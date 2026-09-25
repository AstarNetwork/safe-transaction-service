"""Tests for the incremental ERC-20 balance rollup (token-holdings spec
§4.2, `docs/specs/token-holdings.md` in the workspace root).

Mirrors `test_native_balance_rollup.py`'s shape and conventions (explicit
blocks well above the factories' own sequence, an `initialise()` helper
standing in for the backfill command). The two designed-in departures
from native get their own test groups instead of being folded into the
generic ones: the delta is an UPSERT (Safe-to-Safe nets to zero, a pair
is deleted the moment it nets to zero, a brand-new token for an existing
Safe is created, not skipped), and the new-Safe seed step is driven by an
explicit `(created, address)` marker instead of "no row exists" (a known
Safe funding a counterfactual address, a Safe inserted mid-run).
"""

from datetime import timedelta
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from eth_account import Account
from hexbytes import HexBytes

from safe_transaction_service.analytics.models import (
    AnalyticsSnapshot,
    AnalyticsWatermark,
    SafeTokenBalance,
    TokenHolding,
)
from safe_transaction_service.analytics.tasks import (
    _MAX_ADDRESS,
    ERC20_BALANCE_SAFE_SETTLE,
    ERC20_BALANCE_SAFES_WATERMARK,
    ERC20_BALANCE_WATERMARK,
    apply_erc20_balance_delta,
    compute_erc20_balance_rollup_task,
    count_erc20_balance_orphans,
    erc20_balance_head_block,
    erc20_balance_run_boundary,
    erc20_balance_seed_candidates,
    erc20_balances_upto_block,
    rebuild_token_holdings,
    run_erc20_balance_rollup,
    seed_missing_erc20_balances,
)
from safe_transaction_service.history.models import (
    IndexingStatus,
    IndexingStatusType,
    SafeContract,
)
from safe_transaction_service.history.tests.factories import (
    ERC20TransferFactory,
    EthereumBlockFactory,
    EthereumTxFactory,
    SafeContractFactory,
)
from safe_transaction_service.utils.redis import get_redis
from safe_transaction_service.utils.tasks import get_task_lock_name

# Well clear of `EthereumBlockFactory.number`'s 1-based sequence, and of
# the native tests' own `BASE_BLOCK` (irrelevant across test processes,
# but keeps the two files trivially distinguishable in logs/tracebacks).
BASE_BLOCK = 5_000_000


class Erc20BalanceRollupTestCase(TestCase):
    """Shared scaffolding.

    `ERC20_BALANCE_SAFE_SETTLE` (1 hour) means a Safe created "just now" in
    real wall-clock time is *always* inside the margin -- a fast test can
    never bridge that gap by simply waiting. So this scaffolding runs
    entirely on a fake clock (`self.clock`) instead of real time:
    `timezone.now()` is patched for the whole test to return `self.clock`,
    and `block()` advances it by `CLOCK_STEP` per block, mirroring real
    blocks having increasing timestamps. A normal test's usual "create →
    transfer → advance_head → run" shape comfortably clears the settle
    margin by the time it calls `run_erc20_balance_rollup()` without
    having to think about it; `TestSafeSettleMargin` and
    `TestSafeInsertedMidRun` are the two places that manipulate the clock
    (or freeze the boundary directly) deliberately, to exercise the margin
    itself.
    """

    # 3 blocks (`advance_head`'s default) comfortably clears
    # `ERC20_BALANCE_SAFE_SETTLE` (1h) on their own; a single block does
    # not, by design, so a test that wants "definitely inside the margin"
    # can still get that with a single `self.block()`.
    CLOCK_STEP = timedelta(minutes=25)

    def setUp(self):
        super().setUp()
        self.next_block = BASE_BLOCK
        self.clock = timezone.now()
        patcher = patch(
            "safe_transaction_service.analytics.tasks.timezone.now",
            side_effect=lambda: self.clock,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def advance_clock(self, delta: timedelta = ERC20_BALANCE_SAFE_SETTLE * 2):
        """Move the fake clock forward without touching blocks -- for
        tests that need time to pass on its own (`TestSafeSettleMargin`)."""
        self.clock += delta

    def block(self, confirmed: bool = True):
        self.next_block += 1
        self.advance_clock(self.CLOCK_STEP)
        block = EthereumBlockFactory(number=self.next_block, confirmed=confirmed)
        # `create_indexing_status` (history migration 0069) seeds a
        # `block_number=0` row for `ERC20_721_EVENTS` unconditionally on a
        # fresh test database -- unlike native's `SafeMasterCopy`, which
        # simply has no row when unconfigured. Left at 0, every test would
        # have its head pinned to 0 regardless of the chain it built.
        # Keeping it in step with the tip here is the default "indexer is
        # caught up" state; `TestHeadBlock` overrides it explicitly to
        # exercise the lagging/synced cases.
        IndexingStatus.objects.filter(
            indexing_type=IndexingStatusType.ERC20_721_EVENTS.value
        ).update(block_number=self.next_block)
        return block

    def safe(self, block=None, created=None):
        """A Safe whose creation transaction sits in `block` (a fresh
        confirmed one by default, which also advances the fake clock).
        `created` overrides `auto_now_add` after the fact via `.update()`
        (which, unlike `.save()`, does not re-stamp it) -- the only way to
        get deterministic `(created, address)` ordering in a test."""
        instance = SafeContractFactory(
            ethereum_tx=EthereumTxFactory(block=block or self.block())
        )
        if created is not None:
            SafeContract.objects.filter(address=instance.address).update(
                created=created
            )
            instance.refresh_from_db()
        return instance

    def token(self):
        return Account.create().address

    def transfer(self, block, token, value: int, to=None, _from=None):
        """One ERC-20 `Transfer` of `token` inside `block`."""
        kwargs = {}
        if to is not None:
            kwargs["to"] = to
        if _from is not None:
            kwargs["_from"] = _from
        return ERC20TransferFactory(
            ethereum_tx=EthereumTxFactory(block=block),
            address=token,
            value=value,
            **kwargs,
        )

    def advance_head(self, blocks: int = 3):
        """Push the confirmed chain tip past everything written so far."""
        for _ in range(blocks):
            self.block(confirmed=True)

    def initialise(self, at_block: int = 0):
        """Stand in for `manage.py backfill_erc20_balances` on an empty
        chain: both watermarks set, marker at *now* (not `now -
        ERC20_BALANCE_SAFE_SETTLE` the way `erc20_balance_run_boundary`
        computes B for a real run) so every Safe that already exists is
        treated as already seeded (correct: the backfill would have summed
        their history and found nothing, since none of them have moved a
        token yet in these tests unless a transfer is created after this
        call) -- a test fixture's "as of right now" fiction, not a claim
        about how the real backfill command sets its own marker."""
        AnalyticsWatermark.objects.create(
            name=ERC20_BALANCE_WATERMARK,
            block_number=at_block,
            computed_at=self.clock,
        )
        AnalyticsWatermark.objects.create(
            name=ERC20_BALANCE_SAFES_WATERMARK,
            block_number=at_block,
            computed_at=self.clock,
            address=_MAX_ADDRESS,
        )

    def balance(self, address, token) -> Decimal | None:
        row = SafeTokenBalance.objects.filter(
            safe_address=address, token_address=token
        ).first()
        return row.balance if row else None


class TestHeadBlock(Erc20BalanceRollupTestCase):
    def test_none_when_nothing_confirmed(self):
        self.block(confirmed=False)
        self.assertIsNone(erc20_balance_head_block())

    def test_confirmed_head_bounds_it(self):
        confirmed = self.block(confirmed=True)
        for _ in range(3):
            self.block(confirmed=False)
        self.assertEqual(erc20_balance_head_block(), confirmed.number)

    def test_reorg_depth_bounds_it(self):
        for _ in range(4):
            tip = self.block(confirmed=True)
        self.assertEqual(erc20_balance_head_block(), tip.number - 1)

    def test_head_waits_for_the_erc20_721_events_indexer(self):
        self.advance_head(4)
        unbounded = erc20_balance_head_block()
        behind = unbounded - 2
        IndexingStatus.objects.filter(
            indexing_type=IndexingStatusType.ERC20_721_EVENTS.value
        ).update(block_number=behind)

        self.assertEqual(erc20_balance_head_block(), behind)

    def test_a_synced_indexer_does_not_constrain(self):
        self.advance_head(4)
        head = erc20_balance_head_block()
        IndexingStatus.objects.filter(
            indexing_type=IndexingStatusType.ERC20_721_EVENTS.value
        ).update(block_number=head + 50)

        self.assertEqual(erc20_balance_head_block(), head)


class TestDeltaLegs(Erc20BalanceRollupTestCase):
    """The headline UPSERT behaviour: both signs, and Safe-to-Safe nets to
    zero on each row without netting to zero for the token overall."""

    def test_plus_and_minus_legs_applied_across_two_runs(self):
        safe = self.safe()
        token = self.token()
        self.initialise()

        self.transfer(self.block(), token, 1_000, to=safe.address)
        self.advance_head()
        run_erc20_balance_rollup()
        self.assertEqual(self.balance(safe.address, token), Decimal(1_000))

        self.transfer(self.block(), token, 400, _from=safe.address)
        self.advance_head()
        run_erc20_balance_rollup()
        self.assertEqual(self.balance(safe.address, token), Decimal(600))

    def test_transfer_between_two_safes_moves_both_rows(self):
        sender, receiver = self.safe(), self.safe()
        token = self.token()
        self.initialise()
        block = self.block()
        self.transfer(block, token, 5_000, to=sender.address)
        self.transfer(block, token, 2_000, to=receiver.address, _from=sender.address)
        self.advance_head()

        run_erc20_balance_rollup()

        self.assertEqual(self.balance(sender.address, token), Decimal(3_000))
        self.assertEqual(self.balance(receiver.address, token), Decimal(2_000))
        holding = TokenHolding.objects.get(token_address=token)
        self.assertEqual(holding.holders, 2)
        self.assertEqual(holding.total_balance, Decimal(5_000))

    def test_a_brand_new_token_for_an_already_known_safe_is_created(self):
        """The one place this rollup diverges from native: the delta must
        be able to INSERT, not just UPDATE, because a Safe picking up a
        token it has never held is completely ordinary."""
        safe = self.safe()
        self.initialise()
        run_erc20_balance_rollup()  # nothing to do; just to have a watermark
        self.advance_head()

        new_token = self.token()
        self.transfer(self.block(), new_token, 42, to=safe.address)
        self.advance_head()

        run_erc20_balance_rollup()
        self.assertEqual(self.balance(safe.address, new_token), Decimal(42))


class TestZeroPairsDeleted(Erc20BalanceRollupTestCase):
    def test_pair_is_deleted_once_it_nets_to_zero(self):
        safe = self.safe()
        token = self.token()
        self.initialise()
        self.transfer(self.block(), token, 500, to=safe.address)
        self.advance_head()
        run_erc20_balance_rollup()
        self.assertTrue(
            SafeTokenBalance.objects.filter(
                safe_address=safe.address, token_address=token
            ).exists()
        )

        self.transfer(self.block(), token, 500, _from=safe.address)
        self.advance_head()
        run_erc20_balance_rollup()

        self.assertFalse(
            SafeTokenBalance.objects.filter(
                safe_address=safe.address, token_address=token
            ).exists()
        )
        # And the token drops out of the read model entirely once it has
        # no holders left.
        self.assertFalse(TokenHolding.objects.filter(token_address=token).exists())


class TestCounterfactualSeed(Erc20BalanceRollupTestCase):
    """A transfer to an address that only becomes a Safe later must not be
    lost -- picked up once the seed step reaches it, bounded at the OLD
    watermark so the delta of the same run does not double-count it."""

    def test_transfer_to_a_counterfactual_address_is_picked_up_once_seeded(self):
        existing = self.safe()
        self.initialise()
        token = self.token()

        latecomer_address = Account.create().address
        creation_block = self.block()
        self.transfer(creation_block, token, 40, to=latecomer_address)
        self.transfer(self.block(), token, 100, to=existing.address)
        self.advance_head()

        run_erc20_balance_rollup()
        self.assertIsNone(self.balance(latecomer_address, token))

        watermark = AnalyticsWatermark.objects.get(
            name=ERC20_BALANCE_WATERMARK
        ).block_number
        self.assertGreater(watermark, creation_block.number)

        SafeContractFactory(
            address=latecomer_address,
            ethereum_tx=EthereumTxFactory(block=creation_block),
        )
        self.transfer(self.block(), token, 7, to=latecomer_address)
        self.advance_head()

        run_erc20_balance_rollup()

        # 40 from the seed (blocks <= old watermark), 7 from the delta.
        self.assertEqual(self.balance(latecomer_address, token), Decimal(47))


class TestSafeInsertedMidRun(Erc20BalanceRollupTestCase):
    """Edge case #10e: a Safe the indexer writes *during* a run must be
    excluded from that run entirely (neither step touches it) and picked
    up, once, by the next one -- otherwise the delta counts it this run
    and the next run's seed counts its pre-watermark history again."""

    def test_counted_exactly_once_across_two_runs(self):
        self.safe()
        self.initialise()
        token = self.token()
        self.advance_head()

        # Freeze the boundary this run will use, then create a Safe with a
        # transfer *after* that boundary was captured -- standing in for
        # the indexer writing it mid-run.
        frozen_boundary = erc20_balance_run_boundary()
        with patch(
            "safe_transaction_service.analytics.tasks.erc20_balance_run_boundary",
            return_value=frozen_boundary,
        ):
            midrun = self.safe(created=frozen_boundary[0] + timedelta(seconds=1))
            self.transfer(self.block(), token, 900, to=midrun.address)
            self.advance_head()
            run_erc20_balance_rollup()

        # Neither step touched it this run.
        self.assertIsNone(self.balance(midrun.address, token))

        # The next run's own (later) boundary covers it, and it is seeded
        # from its full history exactly once -- not twice, and not zero.
        self.advance_head()
        run_erc20_balance_rollup()
        self.assertEqual(self.balance(midrun.address, token), Decimal(900))


class TestSafeSettleMargin(Erc20BalanceRollupTestCase):
    """`erc20_balance_run_boundary` backs B off by `ERC20_BALANCE_SAFE_SETTLE`
    rather than using a plain `now()`, because `history_safecontract.created`
    is stamped by the indexer's Python code before its transaction commits
    -- a Safe whose row isn't visible yet can still have `created` inside
    what a plain-`now()` boundary would treat as safe. Excluding anything
    created within the margin, not just anything created after the call,
    is what protects against that."""

    def test_a_safe_created_within_the_margin_is_excluded_until_it_settles(self):
        self.safe()
        self.initialise()
        token = self.token()
        self.advance_head()

        run1_now = timezone.now()
        # Inside the 1-hour margin: close enough to "now" that an
        # in-flight indexer transaction could plausibly still be open.
        recent = self.safe(created=run1_now - timedelta(minutes=10))
        self.transfer(self.block(), token, 900, to=recent.address)
        self.advance_head()

        with patch(
            "safe_transaction_service.analytics.tasks.timezone.now",
            return_value=run1_now,
        ):
            run_erc20_balance_rollup()

        # Neither step touched it: still inside the settle margin, so B
        # (run1_now - 1h) sits *before* its `created`.
        self.assertIsNone(self.balance(recent.address, token))

        # Once `recent` is older than the margin (simulated by moving the
        # clock forward, not by a real sleep), a later run's own boundary
        # covers it and its full pre-watermark history -- including the
        # transfer above, already below this run's watermark -- is summed
        # exactly once by the seed step.
        self.advance_head()
        with patch(
            "safe_transaction_service.analytics.tasks.timezone.now",
            return_value=run1_now + timedelta(hours=2),
        ):
            run_erc20_balance_rollup()
        self.assertEqual(self.balance(recent.address, token), Decimal(900))


class TestSeedCapLowersBoundary(Erc20BalanceRollupTestCase):
    def test_capped_run_lowers_the_boundary_for_both_steps(self):
        self.initialise()
        token = self.token()
        safes = [self.safe() for _ in range(3)]
        self.advance_head()
        for safe in safes:
            self.transfer(self.block(), token, 10, to=safe.address)
        self.advance_head()

        with patch(
            "safe_transaction_service.analytics.tasks.NATIVE_BALANCE_MAX_SEED_PER_RUN",
            1,
        ):
            summary = run_erc20_balance_rollup()

        self.assertEqual(summary["seeded_safes"], 1)
        touched = [s for s in safes if self.balance(s.address, token) is not None]
        # Exactly one Safe was both seeded AND had its delta applied --
        # proving the delta step used the *lowered* boundary, not the
        # original one (which would have upserted flows for all three and
        # left two of them without ever having been seeded).
        self.assertEqual(len(touched), 1)
        marker = AnalyticsWatermark.objects.get(name=ERC20_BALANCE_SAFES_WATERMARK)
        self.assertEqual(HexBytes(marker.address), HexBytes(touched[0].address))

        # A second, uncapped run picks up the remaining two Safes and
        # applies their transfer exactly once each.
        run_erc20_balance_rollup()
        for safe in safes:
            self.assertEqual(self.balance(safe.address, token), Decimal(10))


class TestConcurrentRunRefused(Erc20BalanceRollupTestCase):
    def test_a_second_run_is_refused_without_touching_watermarks(self):
        self.safe()
        self.initialise()
        self.advance_head()

        lock_name = get_task_lock_name(compute_erc20_balance_rollup_task.name)
        redis = get_redis()
        with redis.lock(lock_name, blocking=False):
            result = compute_erc20_balance_rollup_task()

        self.assertIsNone(result)
        watermark = AnalyticsWatermark.objects.get(name=ERC20_BALANCE_WATERMARK)
        # Untouched: `initialise()` set it to 0, and the refused run must
        # not have advanced it.
        self.assertEqual(watermark.block_number, 0)


class TestOrphansExcluded(Erc20BalanceRollupTestCase):
    def test_orphan_pairs_are_counted_and_excluded_from_aggregates(self):
        safe = self.safe()
        token = self.token()
        self.initialise()
        self.transfer(self.block(), token, 600, to=safe.address)
        self.advance_head()
        run_erc20_balance_rollup()
        self.assertEqual(count_erc20_balance_orphans(), 0)

        # A reorg that removes the Safe creation cascades the
        # `history_safecontract` row away, leaving the pair row behind.
        SafeContract.objects.filter(address=safe.address).delete()

        self.assertEqual(count_erc20_balance_orphans(), 1)

        # And the read model must not count it once the next run rebuilds.
        self.advance_head()
        run_erc20_balance_rollup()
        self.assertFalse(TokenHolding.objects.filter(token_address=token).exists())
        snapshot = AnalyticsSnapshot.objects.get(name="token_holdings").payload
        self.assertEqual(snapshot["orphan_pairs"], 1)
        self.assertEqual(snapshot["safes_with_any_erc20"], 0)


class TestHoldersAndTotalPositiveOnly(Erc20BalanceRollupTestCase):
    def test_holders_and_total_balance_exclude_negative_rows(self):
        solvent, underwater = self.safe(), self.safe()
        token = self.token()
        self.initialise()
        block = self.block()
        self.transfer(block, token, 800, to=solvent.address)
        # Only the outgoing leg is indexed -- the matching incoming one is
        # not yet, leaving a negative row.
        self.transfer(block, token, 300, _from=underwater.address)
        self.advance_head()

        run_erc20_balance_rollup()

        self.assertEqual(self.balance(underwater.address, token), Decimal(-300))
        holding = TokenHolding.objects.get(token_address=token)
        self.assertEqual(holding.holders, 1)
        self.assertEqual(holding.total_balance, Decimal(800))
        self.assertEqual(holding.negative_pairs, 1)


class TestPlainFunctionsAreDirectlyTestable(Erc20BalanceRollupTestCase):
    """Each helper is a plain function, callable and assertable without
    going through the Celery task or even `run_erc20_balance_rollup` --
    the point of keeping them out of the task body (task per §12's
    instructions: "Keep plain functions directly testable; the Celery task
    is a thin wrapper")."""

    def test_erc20_balances_upto_block_is_bounded_by_the_block(self):
        safe = self.safe()
        token = self.token()
        early = self.block()
        self.transfer(early, token, 300, to=safe.address)
        late = self.block()
        self.transfer(late, token, 900, to=safe.address)

        # `erc20_balances_upto_block` (like every other raw-SQL helper
        # here) takes and returns *raw* 20-byte addresses, not the
        # checksummed strings the ORM hands back from `EthereumAddressBinaryField`
        # (`from_db_value` converts to a string) -- `HexBytes(...)`, never
        # `bytes(...)`, is the conversion back.
        balances = erc20_balances_upto_block([HexBytes(safe.address)], early.number)
        self.assertEqual(
            balances[(HexBytes(safe.address), HexBytes(token))], Decimal(300)
        )

        balances = erc20_balances_upto_block([HexBytes(safe.address)], late.number)
        self.assertEqual(
            balances[(HexBytes(safe.address), HexBytes(token))], Decimal(1_200)
        )

    def test_seed_candidates_respect_marker_and_boundary(self):
        first = self.safe(created=timezone.now() - timedelta(days=2))
        second = self.safe(created=timezone.now() - timedelta(days=1))
        third = self.safe(created=timezone.now())

        boundary = (timezone.now() + timedelta(seconds=1), b"\xff" * 20)
        candidates = erc20_balance_seed_candidates(
            marker=(first.created, HexBytes(first.address)),
            boundary=boundary,
            limit=10,
        )
        addresses = [bytes(addr) for addr, _created in candidates]
        # `marker` is exclusive: `first` (the marker's own row) must not
        # be re-selected, or a run resumed from a lowered boundary would
        # re-seed the Safe it was lowered to.
        self.assertNotIn(HexBytes(first.address), addresses)
        self.assertIn(HexBytes(second.address), addresses)
        self.assertIn(HexBytes(third.address), addresses)

    def test_seed_missing_erc20_balances_writes_nonzero_pairs_only(self):
        safe = self.safe()
        token = self.token()
        block = self.block()
        self.transfer(block, token, 500, to=safe.address)
        self.transfer(self.block(), token, 500, _from=safe.address)  # nets to 0

        other_token = self.token()
        self.transfer(block, other_token, 15, to=safe.address)

        processed, inserted = seed_missing_erc20_balances(
            [HexBytes(safe.address)], self.next_block
        )
        self.assertEqual(processed, 1)
        self.assertEqual(inserted, 1)  # only the non-zero pair
        self.assertEqual(self.balance(safe.address, other_token), Decimal(15))
        self.assertIsNone(self.balance(safe.address, token))

    def test_apply_erc20_balance_delta_upserts_and_deletes(self):
        safe = self.safe()
        token = self.token()
        # Let the fake clock clear `ERC20_BALANCE_SAFE_SETTLE` before B is
        # captured, or `safe` (just created) would fail its own boundary.
        self.advance_head()
        boundary = erc20_balance_run_boundary()
        block = self.block()
        self.transfer(block, token, 100, to=safe.address)
        self.advance_head()
        head = erc20_balance_head_block()

        upserted, deleted = apply_erc20_balance_delta(0, head, boundary)
        self.assertEqual(upserted, 1)
        self.assertEqual(deleted, 0)
        self.assertEqual(self.balance(safe.address, token), Decimal(100))

        self.transfer(self.block(), token, 100, _from=safe.address)
        self.advance_head()
        new_head = erc20_balance_head_block()
        upserted, deleted = apply_erc20_balance_delta(head, new_head, boundary)
        self.assertEqual(upserted, 1)
        self.assertEqual(deleted, 1)
        self.assertIsNone(self.balance(safe.address, token))

    def test_rebuild_token_holdings_returns_chain_level_counts(self):
        safe = self.safe()
        token = self.token()
        # Same reason as the delta test above: clear the settle margin
        # before capturing B.
        self.advance_head()
        boundary = erc20_balance_run_boundary()
        self.transfer(self.block(), token, 50, to=safe.address)
        self.advance_head()
        head = erc20_balance_head_block()
        apply_erc20_balance_delta(0, head, boundary)

        now = timezone.now()
        counts = rebuild_token_holdings(head, now, now)
        self.assertEqual(counts["tokens_with_holders"], 1)
        self.assertEqual(counts["safes_with_any_erc20"], 1)
        self.assertEqual(counts["negative_pairs_total"], 0)
        self.assertEqual(counts["orphan_pairs"], 0)
        holding = TokenHolding.objects.get(token_address=token)
        self.assertEqual(holding.as_of_block, head)


class TestCeleryTask(Erc20BalanceRollupTestCase):
    def test_task_runs_the_rollup(self):
        safe = self.safe()
        token = self.token()
        self.initialise()
        self.transfer(self.block(), token, 64, to=safe.address)
        self.advance_head()

        compute_erc20_balance_rollup_task.delay()

        self.assertEqual(self.balance(safe.address, token), Decimal(64))


class TestRefusalPaths(Erc20BalanceRollupTestCase):
    def test_no_watermark_is_a_no_op_pointing_at_the_backfill(self):
        self.safe()
        self.advance_head()
        with self.assertLogs(
            "safe_transaction_service.analytics.tasks", level="INFO"
        ) as logs:
            self.assertIsNone(run_erc20_balance_rollup())
        self.assertIn("backfill_erc20_balances", logs.output[0])
        self.assertEqual(SafeTokenBalance.objects.count(), 0)

    def test_watermark_ahead_of_head_refuses(self):
        self.safe()
        self.advance_head()
        head = erc20_balance_head_block()
        AnalyticsWatermark.objects.create(
            name=ERC20_BALANCE_WATERMARK,
            block_number=head + 10,
            computed_at=timezone.now(),
        )
        with self.assertLogs(
            "safe_transaction_service.analytics.tasks", level="ERROR"
        ) as logs:
            self.assertIsNone(run_erc20_balance_rollup())
        self.assertIn("--restart", logs.output[0])

    def test_nothing_confirmed_is_not_an_error(self):
        self.safe(block=self.block(confirmed=False))
        self.initialise()
        self.assertIsNone(run_erc20_balance_rollup())
