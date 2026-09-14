"""Tests for the incremental native-balance rollup.

The property that matters is the first one: *incremental equals full
recompute*. Everything else here defends a specific way that property can
break — a Safe indexed after the transfers it received, a block that gets
reorged out from under an already-applied delta, a crash between applying
the delta and moving the watermark, a negative balance clamped at the
wrong end of the pipeline.

Block numbers are explicit and start well above the factories' own
sequence so that nothing a `SubFactory` creates can become the chain tip
by accident. `ETH_REORG_BLOCKS` is 1 under `config.settings.test`, so a
single trailing confirmed block is enough to lift the safe head past the
blocks a test actually wrote to.
"""

from decimal import Decimal
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from eth_account import Account

from safe_transaction_service.analytics.models import (
    AnalyticsSnapshot,
    AnalyticsWatermark,
    SafeNativeBalance,
)
from safe_transaction_service.analytics.tasks import (
    NATIVE_BALANCE_WATERMARK,
    _calculate_native_balances_from_db,
    check_native_balance_drift,
    check_native_balance_drift_task,
    compute_native_balance_rollup_task,
    compute_tvl_task,
    native_balance_head_block,
    read_native_balance_rollup,
    run_native_balance_rollup,
)
from safe_transaction_service.analytics.tasks_shards import (
    HEX_PREFIXES,
    NATIVE_BALANCE_CURSOR_KEY,
    NATIVE_BALANCE_RUN_KEY_PREFIX,
    backfill_native_balance_chunk,
    latest_native_balance_run_id,
    load_native_balance_run,
)
from safe_transaction_service.history.models import EthereumTxCallType, SafeContract
from safe_transaction_service.history.tests.factories import (
    EthereumBlockFactory,
    EthereumTxFactory,
    InternalTxFactory,
    SafeContractFactory,
    SafeMasterCopyFactory,
)
from safe_transaction_service.utils.redis import get_redis

# Well clear of `EthereumBlockFactory.number`'s 1-based sequence.
BASE_BLOCK = 1_000_000


class NativeBalanceRollupTestCase(TestCase):
    """Shared scaffolding: explicit blocks, explicit confirmation."""

    def setUp(self):
        super().setUp()
        self.next_block = BASE_BLOCK
        # Redis is not rolled back between tests the way the database is,
        # so a run manifest (and the cursor pointing at it) outlives the
        # rows it describes. Same isolation `BackfillRedisMixin` gives the
        # daily-backfill tests.
        redis = get_redis()
        keys = list(redis.scan_iter(match=f"{NATIVE_BALANCE_RUN_KEY_PREFIX}*"))
        keys.append(NATIVE_BALANCE_CURSOR_KEY)
        redis.delete(*keys)

    def block(self, confirmed: bool = True):
        """A new block, one number above the last one this test made."""
        self.next_block += 1
        return EthereumBlockFactory(number=self.next_block, confirmed=confirmed)

    def safe(self, block=None):
        """A Safe whose creation transaction sits in `block` (a fresh
        confirmed one by default), so the Safe's own indexing height is
        under the test's control."""
        return SafeContractFactory(
            ethereum_tx=EthereumTxFactory(block=block or self.block())
        )

    def transfer(self, block, value: int, to=None, _from=None):
        """One successful native-value internal tx inside `block`."""
        kwargs = {}
        if to is not None:
            kwargs["to"] = to
        if _from is not None:
            kwargs["_from"] = _from
        return InternalTxFactory(
            ethereum_tx=EthereumTxFactory(block=block),
            value=value,
            call_type=EthereumTxCallType.CALL.value,
            error=None,
            **kwargs,
        )

    def advance_head(self, blocks: int = 3):
        """Push the confirmed chain tip past everything written so far, so
        `native_balance_head_block()` covers it. Empty blocks — the point
        is only the height."""
        for _ in range(blocks):
            self.block(confirmed=True)

    def initialise(self, at_block: int = 0):
        """Stand in for `manage.py backfill_native_balances` on an empty
        chain: a watermark at `at_block` and no rows. The first run then
        seeds every Safe at `at_block` and applies everything above it."""
        AnalyticsWatermark.objects.create(
            name=NATIVE_BALANCE_WATERMARK,
            block_number=at_block,
            computed_at="2026-01-01T00:00:00+00:00",
        )

    def totals(self) -> tuple[int, int]:
        rollup = read_native_balance_rollup()
        return rollup["balance_wei"], rollup["safes_with_balance"]


class TestHeadBlock(NativeBalanceRollupTestCase):
    """`native_balance_head_block` is the only thing standing between the
    rollup and a reorg it cannot undo."""

    def test_none_when_nothing_confirmed(self):
        self.block(confirmed=False)
        self.assertIsNone(native_balance_head_block())

    def test_none_when_no_blocks_at_all(self):
        self.assertIsNone(native_balance_head_block())

    def test_confirmed_head_bounds_it(self):
        confirmed = self.block(confirmed=True)
        # Three unconfirmed blocks on top: the depth bound would allow
        # `tip - 1`, the confirmation flag does not.
        for _ in range(3):
            self.block(confirmed=False)
        self.assertEqual(native_balance_head_block(), confirmed.number)

    def test_reorg_depth_bounds_it(self):
        # Everything confirmed, so `confirmed` is not the binding term —
        # the depth backstop is, and holds the head one block (
        # ETH_REORG_BLOCKS == 1 in test settings) below the tip.
        for _ in range(4):
            tip = self.block(confirmed=True)
        self.assertEqual(native_balance_head_block(), tip.number - 1)


class TestIncrementalMatchesFullRecompute(NativeBalanceRollupTestCase):
    """The headline property. `_calculate_native_balances_from_db` is the
    reference the 16-shard chord was itself verified against
    (`TestNativeBalanceShards`), so matching it is what "no regression on
    /tvl/" means."""

    def test_matches_after_several_incremental_runs(self):
        safes = [self.safe() for _ in range(4)]
        self.initialise()

        # Three windows, each closed by its own rollup run: incoming,
        # outgoing and a second incoming leg, so the running total has to
        # survive both signs and repeated touches of the same row.
        window_one = self.block()
        for safe in safes:
            self.transfer(window_one, 1_000, to=safe.address)
        self.advance_head()
        run_native_balance_rollup()

        window_two = self.block()
        self.transfer(window_two, 250, _from=safes[0].address)
        self.transfer(window_two, 4_000, to=safes[1].address)
        self.advance_head()
        run_native_balance_rollup()

        window_three = self.block()
        self.transfer(window_three, 7, to=safes[2].address)
        self.advance_head()
        run_native_balance_rollup()

        reference_balance, reference_count = _calculate_native_balances_from_db()
        self.assertEqual(self.totals(), (reference_balance, reference_count))
        self.assertEqual(reference_balance, 1_000 * 4 - 250 + 4_000 + 7)
        self.assertEqual(reference_count, 4)

    def test_transfers_between_two_safes_net_out(self):
        sender, receiver = self.safe(), self.safe()
        self.initialise()
        block = self.block()
        self.transfer(block, 5_000, to=sender.address)
        self.transfer(block, 2_000, to=receiver.address, _from=sender.address)
        self.advance_head()
        run_native_balance_rollup()

        self.assertEqual(self.totals(), _calculate_native_balances_from_db())
        self.assertEqual(self.totals(), (5_000, 2))

    def test_non_safe_counterparties_are_not_stored(self):
        safe = self.safe()
        self.initialise()
        block = self.block()
        # `to` defaults to a random non-Safe address; only the Safe side
        # of this transfer belongs in the rollup.
        self.transfer(block, 900, _from=safe.address)
        self.advance_head()
        run_native_balance_rollup()

        self.assertEqual(SafeNativeBalance.objects.count(), 1)
        self.assertEqual(SafeNativeBalance.objects.get().balance_wei, Decimal(-900))


class TestIdempotency(NativeBalanceRollupTestCase):
    def test_second_run_without_new_blocks_changes_nothing(self):
        safe = self.safe()
        self.initialise()
        self.transfer(self.block(), 3_000, to=safe.address)
        self.advance_head()

        first = run_native_balance_rollup()
        after_first = self.totals()
        watermark_after_first = AnalyticsWatermark.objects.get(
            name=NATIVE_BALANCE_WATERMARK
        ).block_number

        second = run_native_balance_rollup()

        self.assertEqual(after_first, self.totals())
        self.assertEqual(
            watermark_after_first,
            AnalyticsWatermark.objects.get(name=NATIVE_BALANCE_WATERMARK).block_number,
        )
        self.assertEqual(first["watermark_to"], second["watermark_to"])
        # Nothing left to seed and an empty (W, head] range.
        self.assertEqual(second["seeded_safes"], 0)
        self.assertEqual(second["touched_safes"], 0)

    def test_zero_balance_safes_are_not_reseeded_every_run(self):
        """A Safe that has never moved native value still gets a row —
        otherwise "absent from the rollup" would mean "new Safe" for it on
        every single run, and the seed step would re-walk the fleet."""
        self.safe()
        self.initialise()
        self.advance_head()

        first = run_native_balance_rollup()
        self.assertEqual(first["seeded_safes"], 1)
        self.assertEqual(SafeNativeBalance.objects.count(), 1)
        self.assertEqual(SafeNativeBalance.objects.get().balance_wei, Decimal(0))

        self.advance_head()
        self.assertEqual(run_native_balance_rollup()["seeded_safes"], 0)


class TestSafeIndexedMidWindow(NativeBalanceRollupTestCase):
    """A Safe enters `history_safecontract` only when the indexer gets to
    it, which can be well after the transfers it already received. The
    seed step is bounded by the OLD watermark for exactly this case, and
    getting the bound wrong is silent either way: too low and the Safe
    loses its pre-indexing history forever, too high and its first window
    is counted twice.
    """

    def test_new_safe_keeps_transfers_from_before_it_was_indexed(self):
        existing = self.safe()
        self.initialise()

        # Funded while it is still just an address: the internal tx is
        # indexed, the `history_safecontract` row is not written yet.
        latecomer_address = Account.create().address
        creation_block = self.block()
        self.transfer(creation_block, 40, to=latecomer_address)
        self.transfer(self.block(), 100, to=existing.address)
        self.advance_head()

        run_native_balance_rollup()
        watermark = AnalyticsWatermark.objects.get(
            name=NATIVE_BALANCE_WATERMARK
        ).block_number
        self.assertLess(creation_block.number, watermark)
        # Not a Safe yet, so not in the rollup and not in the totals.
        self.assertFalse(
            SafeNativeBalance.objects.filter(safe_address=latecomer_address).exists()
        )

        # The indexer catches up, and a further transfer lands above the
        # watermark in the same run.
        SafeContractFactory(
            address=latecomer_address,
            ethereum_tx=EthereumTxFactory(block=creation_block),
        )
        self.transfer(self.block(), 7, to=latecomer_address)
        self.advance_head()

        summary = run_native_balance_rollup()
        self.assertEqual(summary["seeded_safes"], 1)

        # 40 from the seed (blocks <= W), 7 from the delta ((W, head]).
        self.assertEqual(
            SafeNativeBalance.objects.get(safe_address=latecomer_address).balance_wei,
            Decimal(47),
        )
        self.assertEqual(self.totals(), _calculate_native_balances_from_db())

    def test_the_delta_never_creates_a_row(self):
        """A Safe the indexer writes into `history_safecontract` between
        the seed query and the delta must not get a row from the delta —
        it would hold only `(W, head]` and never be seeded again, losing
        everything below the watermark permanently. Skipping it this run
        and seeding it on the next one is the correct outcome.
        """
        self.initialise()
        self.safe()
        self.advance_head()
        run_native_balance_rollup()

        latecomer = self.safe(block=self.block())
        funding = self.block()
        self.transfer(funding, 800, to=latecomer.address)
        self.advance_head()

        # Stand in for the race: the Safe exists by the time the delta
        # runs, but was not in the seed step's result set.
        with patch(
            "safe_transaction_service.analytics.tasks._unseeded_safe_addresses",
            return_value=[],
        ):
            run_native_balance_rollup()

        self.assertFalse(
            SafeNativeBalance.objects.filter(safe_address=latecomer.address).exists()
        )

        # Next run seeds it, bounded at a watermark that now covers the
        # funding block — so nothing was lost.
        self.advance_head()
        run_native_balance_rollup()
        self.assertEqual(
            SafeNativeBalance.objects.get(safe_address=latecomer.address).balance_wei,
            Decimal(800),
        )
        self.assertEqual(self.totals(), _calculate_native_balances_from_db())

    def test_transfers_inside_the_window_are_counted_once(self):
        """The other side of the same bound. Here the Safe's only transfer
        sits *inside* (W, head], where the delta will read it — a seed
        bounded at `head` rather than at `W` would add it a second time.
        """
        self.initialise()
        self.safe()
        self.advance_head()
        run_native_balance_rollup()
        watermark = AnalyticsWatermark.objects.get(
            name=NATIVE_BALANCE_WATERMARK
        ).block_number

        newcomer = self.safe(block=self.block())
        funding_block = self.block()
        self.transfer(funding_block, 500, to=newcomer.address)
        self.advance_head()
        self.assertGreater(funding_block.number, watermark)

        run_native_balance_rollup()

        self.assertEqual(
            SafeNativeBalance.objects.get(safe_address=newcomer.address).balance_wei,
            Decimal(500),
        )
        self.assertEqual(self.totals(), _calculate_native_balances_from_db())


class TestConfirmationBoundary(NativeBalanceRollupTestCase):
    def test_unconfirmed_blocks_are_not_consumed(self):
        safe = self.safe()
        self.initialise()

        settled = self.block(confirmed=True)
        self.transfer(settled, 1_000, to=safe.address)
        self.advance_head()

        # Above the safe head: confirmed=False, so a reorg could still
        # take it — and with it the row whose value we would have added.
        pending = self.block(confirmed=False)
        self.transfer(pending, 999_999, to=safe.address)

        summary = run_native_balance_rollup()
        self.assertEqual(self.totals(), (1_000, 1))
        self.assertLess(summary["watermark_to"], pending.number)

        # Once it confirms and the tip moves past it, the next run picks
        # it up — no backfill, no gap.
        pending.set_confirmed()
        self.advance_head()
        run_native_balance_rollup()
        self.assertEqual(self.totals(), (1_000_999, 1))
        self.assertEqual(self.totals(), _calculate_native_balances_from_db())


class TestNegativeBalances(NativeBalanceRollupTestCase):
    """Stored signed, clamped on read. Clamping on write would make an
    indexing gap permanent."""

    def test_negative_row_is_stored_signed_and_excluded_from_totals(self):
        solvent, underwater = self.safe(), self.safe()
        self.initialise()
        block = self.block()
        self.transfer(block, 800, to=solvent.address)
        # Only the outgoing leg is indexed — the matching incoming one
        # has not been picked up yet.
        self.transfer(block, 300, _from=underwater.address)
        self.advance_head()
        run_native_balance_rollup()

        self.assertEqual(
            SafeNativeBalance.objects.get(safe_address=underwater.address).balance_wei,
            Decimal(-300),
        )
        # Excluded from the sum and from the count, not floored into it.
        self.assertEqual(self.totals(), (800, 1))
        self.assertEqual(self.totals(), _calculate_native_balances_from_db())

    def test_row_recovers_when_the_missing_transfer_is_indexed(self):
        safe = self.safe()
        self.initialise()
        first = self.block()
        self.transfer(first, 300, _from=safe.address)
        self.advance_head()
        run_native_balance_rollup()
        self.assertEqual(self.totals(), (0, 0))

        second = self.block()
        self.transfer(second, 500, to=safe.address)
        self.advance_head()
        run_native_balance_rollup()

        self.assertEqual(
            SafeNativeBalance.objects.get(safe_address=safe.address).balance_wei,
            Decimal(200),
        )
        self.assertEqual(self.totals(), (200, 1))


class TestAtomicity(NativeBalanceRollupTestCase):
    def test_failure_between_delta_and_watermark_leaves_neither(self):
        safe = self.safe()
        self.initialise()
        self.transfer(self.block(), 2_500, to=safe.address)
        self.advance_head()

        with patch(
            "safe_transaction_service.analytics.models."
            "AnalyticsWatermark.objects.update_or_create",
            side_effect=RuntimeError("boom"),
        ):
            with self.assertRaises(RuntimeError):
                run_native_balance_rollup()

        # The delta rolled back with the watermark write.
        self.assertEqual(self.totals(), (0, 0))
        self.assertEqual(
            AnalyticsWatermark.objects.get(name=NATIVE_BALANCE_WATERMARK).block_number,
            0,
        )

        # And the retry is a clean first application, not a double one.
        run_native_balance_rollup()
        self.assertEqual(self.totals(), (2_500, 1))
        self.assertEqual(self.totals(), _calculate_native_balances_from_db())


class TestRefusalPaths(NativeBalanceRollupTestCase):
    """Every path that declines to run says why at ERROR and leaves the
    rollup untouched — a wrong number here is worse than a stale one."""

    def test_watermark_ahead_of_head_refuses(self):
        self.safe()
        self.advance_head()
        head = native_balance_head_block()
        self.initialise(at_block=head + 10)

        with self.assertLogs(
            "safe_transaction_service.analytics.tasks", level="ERROR"
        ) as logs:
            self.assertIsNone(run_native_balance_rollup())
        self.assertIn("--restart", logs.output[0])
        self.assertEqual(SafeNativeBalance.objects.count(), 0)
        self.assertEqual(
            AnalyticsWatermark.objects.get(name=NATIVE_BALANCE_WATERMARK).block_number,
            head + 10,
        )

    def test_cold_start_over_a_large_fleet_refuses(self):
        """No watermark and a fleet too big to seed here means analytics is
        being switched on over an existing service, not a new network
        coming up. That is the hour-long recompute this rollup exists to
        keep out of a nightly task."""
        for _ in range(3):
            self.safe()
        self.advance_head()

        with patch(
            "safe_transaction_service.analytics.tasks.NATIVE_BALANCE_MAX_SEED_PER_RUN",
            2,
        ):
            with self.assertLogs(
                "safe_transaction_service.analytics.tasks", level="ERROR"
            ) as logs:
                self.assertIsNone(run_native_balance_rollup())

        self.assertIn("backfill_native_balances", logs.output[0])
        self.assertEqual(SafeNativeBalance.objects.count(), 0)
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).exists()
        )

    def test_too_many_unseeded_safes_refuses(self):
        for _ in range(3):
            self.safe()
        self.initialise()
        self.advance_head()

        with patch(
            "safe_transaction_service.analytics.tasks.NATIVE_BALANCE_MAX_SEED_PER_RUN",
            2,
        ):
            with self.assertLogs(
                "safe_transaction_service.analytics.tasks", level="ERROR"
            ) as logs:
                self.assertIsNone(run_native_balance_rollup())
        self.assertIn("backfill_native_balances", logs.output[0])
        self.assertEqual(SafeNativeBalance.objects.count(), 0)

    def test_nothing_confirmed_is_not_an_error(self):
        self.safe(block=self.block(confirmed=False))
        self.initialise()
        self.assertIsNone(run_native_balance_rollup())


class TestColdStart(NativeBalanceRollupTestCase):
    """A newly spun-up transaction service must not need a human to run the
    backfill. The nightly task initialises the rollup itself when the fleet
    is small enough for that to be bounded work."""

    def test_a_new_network_initialises_itself_on_the_first_run(self):
        safe = self.safe()
        self.transfer(self.block(), 4_200, to=safe.address)
        self.advance_head()
        head = native_balance_head_block()
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).exists()
        )

        with self.assertLogs(
            "safe_transaction_service.analytics.tasks", level="INFO"
        ) as logs:
            summary = run_native_balance_rollup()

        self.assertTrue(any("cold start" in line for line in logs.output))
        self.assertEqual(
            AnalyticsWatermark.objects.get(name=NATIVE_BALANCE_WATERMARK).block_number,
            head,
        )
        # Seeded at `head`, so the whole history is in the row already and
        # the delta of this same run had nothing left to add.
        self.assertEqual(summary["watermark_from"], head)
        self.assertEqual(summary["seeded_safes"], 0)
        self.assertEqual(self.totals(), (4_200, 1))
        self.assertEqual(self.totals(), _calculate_native_balances_from_db())

    def test_the_run_after_a_cold_start_is_a_plain_increment(self):
        safe = self.safe()
        self.transfer(self.block(), 100, to=safe.address)
        self.advance_head()
        run_native_balance_rollup()

        self.transfer(self.block(), 25, to=safe.address)
        self.advance_head()
        summary = run_native_balance_rollup()

        self.assertEqual(summary["touched_safes"], 1)
        self.assertEqual(self.totals(), (125, 1))
        self.assertEqual(self.totals(), _calculate_native_balances_from_db())

    def test_cold_start_matches_what_the_backfill_would_have_written(self):
        for _ in range(3):
            safe = self.safe()
            self.transfer(self.block(), 700, to=safe.address)
        self.advance_head()

        run_native_balance_rollup()
        by_cold_start = self.totals()
        stamps = set(
            SafeNativeBalance.objects.values_list("updated_to_block", flat=True)
        )

        # Same shape the command produces: a row per Safe, one stamp.
        self.assertEqual(SafeNativeBalance.objects.count(), 3)
        self.assertEqual(len(stamps), 1)
        self.assertEqual(by_cold_start, _calculate_native_balances_from_db())


class TestIndexerProgressBound(NativeBalanceRollupTestCase):
    """`EthereumBlock` rows are created by any indexer — the ERC20 one makes
    a block the moment it meets a transfer — so block presence can run far
    ahead of the master-copies indexer that actually writes `InternalTx`.
    Consuming such a block moves the watermark past internal transactions
    that have not been written yet, and they are never revisited.

    This is the state every freshly spun-up service is in.
    """

    def test_head_waits_for_the_trace_indexer(self):
        self.advance_head(4)
        unbounded = native_balance_head_block()
        behind = unbounded - 2
        SafeMasterCopyFactory(tx_block_number=behind)

        self.assertEqual(native_balance_head_block(), behind)

    def test_the_furthest_behind_master_copy_wins(self):
        self.advance_head(6)
        head = native_balance_head_block()
        SafeMasterCopyFactory(tx_block_number=head - 1)
        SafeMasterCopyFactory(tx_block_number=head - 4)

        self.assertEqual(native_balance_head_block(), head - 4)

    def test_a_synced_indexer_does_not_constrain(self):
        self.advance_head(4)
        head = native_balance_head_block()
        SafeMasterCopyFactory(tx_block_number=head + 50)

        self.assertEqual(native_balance_head_block(), head)

    def test_transfers_the_indexer_has_not_reached_are_not_consumed(self):
        safe = self.safe()
        early = self.block()
        self.transfer(early, 300, to=safe.address)
        late = self.block()
        self.transfer(late, 900, to=safe.address)
        self.advance_head()

        # The trace indexer has only reached `early`. The 900 is in the
        # database because some other indexer put it there; it is not ours
        # to consume yet.
        master_copy = SafeMasterCopyFactory(tx_block_number=early.number)
        run_native_balance_rollup()
        self.assertEqual(self.totals(), (300, 1))

        # Once it catches up, the next run picks the rest up — no gap.
        master_copy.tx_block_number = late.number + 10
        master_copy.save(update_fields=["tx_block_number"])
        run_native_balance_rollup()
        self.assertEqual(self.totals(), (1_200, 1))
        self.assertEqual(self.totals(), _calculate_native_balances_from_db())


class TestReadHelper(NativeBalanceRollupTestCase):
    def test_none_before_initialisation(self):
        self.assertIsNone(read_native_balance_rollup())

    def test_reports_the_watermark_it_read_at(self):
        safe = self.safe()
        self.initialise()
        self.transfer(self.block(), 11, to=safe.address)
        self.advance_head()
        summary = run_native_balance_rollup()

        rollup = read_native_balance_rollup()
        self.assertEqual(rollup["updated_to_block"], summary["watermark_to"])
        self.assertEqual(rollup["balance_wei"], 11)
        self.assertEqual(rollup["safes_with_balance"], 1)
        self.assertEqual(rollup["safe_rows"], 1)


class TestCeleryTask(NativeBalanceRollupTestCase):
    def test_task_runs_the_rollup(self):
        safe = self.safe()
        self.initialise()
        self.transfer(self.block(), 64, to=safe.address)
        self.advance_head()

        compute_native_balance_rollup_task.delay()

        self.assertEqual(self.totals(), (64, 1))


class TestBackfillCommand(NativeBalanceRollupTestCase):
    """`backfill_native_balances` is the one-time full pass the nightly run
    no longer does. Its hazards are all about *which block* it pins to:
    two passes at two different blocks leave rows that are complete
    through different heights, and a single watermark cannot describe
    both."""

    def backfill(self, **kwargs) -> str:
        out = StringIO()
        call_command("backfill_native_balances", stdout=out, stderr=out, **kwargs)
        return out.getvalue()

    def test_fills_every_safe_and_hands_over_the_watermark(self):
        safes = [self.safe() for _ in range(3)]
        block = self.block()
        self.transfer(block, 700, to=safes[0].address)
        self.transfer(block, 40, _from=safes[1].address)
        self.advance_head()
        head = native_balance_head_block()

        self.backfill()

        # A row per Safe, zero-balance ones included.
        self.assertEqual(SafeNativeBalance.objects.count(), 3)
        self.assertEqual(
            set(SafeNativeBalance.objects.values_list("updated_to_block", flat=True)),
            {head},
        )
        self.assertEqual(
            AnalyticsWatermark.objects.get(name=NATIVE_BALANCE_WATERMARK).block_number,
            head,
        )
        self.assertEqual(self.totals(), _calculate_native_balances_from_db())
        self.assertEqual(self.totals(), (700, 1))

    def test_incremental_takes_over_where_the_backfill_stopped(self):
        safe = self.safe()
        self.transfer(self.block(), 1_000, to=safe.address)
        self.advance_head()
        self.backfill()

        self.transfer(self.block(), 250, to=safe.address)
        self.advance_head()
        run_native_balance_rollup()

        self.assertEqual(self.totals(), (1_250, 1))
        self.assertEqual(self.totals(), _calculate_native_balances_from_db())

    def test_resume_skips_safes_already_computed(self):
        for _ in range(3):
            self.safe()
        self.advance_head()
        self.backfill()

        # A Safe indexed after the first pass; the watermark is already
        # set, so this is a top-up and must not move it.
        watermark = AnalyticsWatermark.objects.get(
            name=NATIVE_BALANCE_WATERMARK
        ).block_number
        self.safe()
        self.advance_head()

        output = self.backfill()

        self.assertIn("3 already done", output)
        self.assertEqual(SafeNativeBalance.objects.count(), 4)
        self.assertEqual(
            AnalyticsWatermark.objects.get(name=NATIVE_BALANCE_WATERMARK).block_number,
            watermark,
        )

    def test_restart_rebuilds_from_scratch(self):
        safe = self.safe()
        self.transfer(self.block(), 88, to=safe.address)
        self.advance_head()
        self.backfill()

        self.advance_head()
        self.backfill(restart=True)

        new_head = native_balance_head_block()
        self.assertEqual(
            AnalyticsWatermark.objects.get(name=NATIVE_BALANCE_WATERMARK).block_number,
            new_head,
        )
        self.assertEqual(self.totals(), (88, 1))

    def test_at_block_above_the_safe_head_is_refused(self):
        self.safe()
        self.advance_head()
        with self.assertRaises(CommandError) as ctx:
            self.backfill(at_block=native_balance_head_block() + 1)
        self.assertIn("safe head", str(ctx.exception))

    def test_topping_up_at_a_different_block_is_refused(self):
        self.safe()
        self.advance_head()
        self.backfill()
        self.advance_head()

        with self.assertRaises(CommandError) as ctx:
            self.backfill(at_block=native_balance_head_block())
        self.assertIn("--restart", str(ctx.exception))

    def test_interrupted_run_resumes_at_its_own_block(self):
        """No watermark yet, rows stamped at an older block: continue
        there. Finishing at today's head instead would leave the early
        rows short of everything in between, and the watermark would then
        claim otherwise."""
        first, second = self.safe(), self.safe()
        self.transfer(self.block(), 500, to=first.address)
        self.advance_head()
        interrupted_head = native_balance_head_block()
        # Stand in for a crash after the first batch: one row written, no
        # watermark.
        SafeNativeBalance.objects.create(
            safe_address=first.address,
            balance_wei=Decimal(500),
            updated_to_block=interrupted_head,
        )

        # The chain moves on before the operator re-runs.
        self.transfer(self.block(), 70, to=second.address)
        self.advance_head()
        self.assertGreater(native_balance_head_block(), interrupted_head)

        output = self.backfill()

        self.assertIn(
            f"Resuming an interrupted run at block {interrupted_head}", output
        )
        self.assertEqual(
            AnalyticsWatermark.objects.get(name=NATIVE_BALANCE_WATERMARK).block_number,
            interrupted_head,
        )
        # The 70 landed above that block, so it is the incremental run's
        # to apply — not the backfill's to quietly absorb.
        self.assertEqual(self.totals(), (500, 1))
        run_native_balance_rollup()
        self.assertEqual(self.totals(), (570, 2))
        self.assertEqual(self.totals(), _calculate_native_balances_from_db())

    def test_inconsistent_stamps_are_refused_rather_than_guessed(self):
        first, second = self.safe(), self.safe()
        self.advance_head()
        head = native_balance_head_block()
        SafeNativeBalance.objects.create(
            safe_address=first.address, balance_wei=Decimal(0), updated_to_block=head
        )
        SafeNativeBalance.objects.create(
            safe_address=second.address,
            balance_wei=Decimal(0),
            updated_to_block=head - 2,
        )

        with self.assertRaises(CommandError) as ctx:
            self.backfill()
        self.assertIn("--restart", str(ctx.exception))

    def test_status_reports_without_writing(self):
        self.safe()
        self.advance_head()

        output = self.backfill(status=True)

        self.assertIn("NOT SET", output)
        self.assertEqual(SafeNativeBalance.objects.count(), 0)

        self.backfill()
        self.assertIn("Watermark", self.backfill(status=True))


class TestTvlReadsTheRollup(NativeBalanceRollupTestCase):
    """`/tvl/`'s payload is the contract with the hub. It may gain keys; it
    may not lose or rename one without the consumer stopping reading it
    first. So the switch has to keep every key it had — `partial_shards`
    in particular, which the hub gates USD pricing on."""

    # Every key `finalize_tvl_snapshot` wrote before the rollup existed.
    PRE_EXISTING_KEYS = {
        "total_safes_with_balance",
        "native_balance_wei",
        "erc20_token_count",
        "top_tokens",
        "partial_shards",
        "total_shards",
        "computed_at",
    }

    def snapshot(self) -> dict:
        return AnalyticsSnapshot.objects.get(name="tvl").payload

    def test_payload_keeps_every_key_and_gains_two(self):
        safe = self.safe()
        self.transfer(self.block(), 12_345, to=safe.address)
        self.advance_head()
        call_command("backfill_native_balances", stdout=StringIO())

        compute_tvl_task()
        payload = self.snapshot()

        self.assertTrue(self.PRE_EXISTING_KEYS.issubset(payload))
        self.assertEqual(payload["native_source"], "rollup")
        self.assertEqual(
            payload["native_updated_to_block"], native_balance_head_block()
        )
        # A rollup run is a complete run — no shards to be partial about.
        self.assertEqual(payload["partial_shards"], 0)
        self.assertEqual(payload["native_balance_wei"], "12345")
        self.assertEqual(payload["total_safes_with_balance"], 1)

    def test_native_numbers_match_the_shard_path_they_replace(self):
        for _ in range(3):
            safe = self.safe()
            self.transfer(self.block(), 4_000, to=safe.address)
        self.advance_head()

        # Chord path first, on an un-backfilled rollup.
        compute_tvl_task()
        via_shards = self.snapshot()
        self.assertEqual(via_shards["native_source"], "shards")
        self.assertEqual(via_shards["total_shards"], len(HEX_PREFIXES))

        call_command("backfill_native_balances", stdout=StringIO())
        compute_tvl_task()
        via_rollup = self.snapshot()

        self.assertEqual(via_rollup["native_source"], "rollup")
        self.assertEqual(
            via_rollup["native_balance_wei"], via_shards["native_balance_wei"]
        )
        self.assertEqual(
            via_rollup["total_safes_with_balance"],
            via_shards["total_safes_with_balance"],
        )

    def test_uninitialised_rollup_falls_back_instead_of_publishing_zero(self):
        """An instance that has migrated but not run the backfill keeps
        serving the number it served before. Publishing the empty rollup's
        zero would look exactly like a fleet holding nothing."""
        safe = self.safe()
        self.transfer(self.block(), 9_000, to=safe.address)
        self.advance_head()
        self.assertIsNone(read_native_balance_rollup())

        compute_tvl_task()
        payload = self.snapshot()

        self.assertEqual(payload["native_balance_wei"], "9000")
        self.assertEqual(payload["native_source"], "shards")
        self.assertIsNone(payload["native_updated_to_block"])


class TestDriftCheck(NativeBalanceRollupTestCase):
    """The rollup has no self-healing property — a batch applied twice
    stays wrong forever and looks like a real number. This check is the
    only thing that would ever notice."""

    def test_clean_rollup_reports_no_drift(self):
        for _ in range(3):
            safe = self.safe()
            self.transfer(self.block(), 600, to=safe.address)
        self.advance_head()
        call_command("backfill_native_balances", stdout=StringIO())

        summary = check_native_balance_drift()

        self.assertEqual(summary["sampled"], 3)
        self.assertEqual(summary["mismatched"], 0)
        self.assertEqual(summary["total_abs_diff_wei"], 0)

    def test_corrupted_row_is_reported_with_its_magnitude(self):
        safe = self.safe()
        self.transfer(self.block(), 600, to=safe.address)
        self.advance_head()
        call_command("backfill_native_balances", stdout=StringIO())

        # Exactly the shape a double-applied delta leaves behind.
        SafeNativeBalance.objects.filter(safe_address=safe.address).update(
            balance_wei=Decimal(1_200)
        )

        with self.assertLogs(
            "safe_transaction_service.analytics.tasks", level="WARNING"
        ) as logs:
            summary = check_native_balance_drift()

        self.assertEqual(summary["mismatched"], 1)
        self.assertEqual(summary["total_abs_diff_wei"], 600)
        self.assertEqual(summary["max_abs_diff_wei"], 600)
        self.assertIn("--restart", logs.output[0])

    def test_blocks_above_the_watermark_are_not_drift(self):
        """The rollup only claims completeness through the watermark, so a
        transfer the next run has yet to apply must not be reported."""
        safe = self.safe()
        self.transfer(self.block(), 600, to=safe.address)
        self.advance_head()
        call_command("backfill_native_balances", stdout=StringIO())

        self.transfer(self.block(), 999, to=safe.address)
        self.advance_head()

        summary = check_native_balance_drift()
        self.assertEqual(summary["mismatched"], 0)

    def test_uninitialised_rollup_is_skipped_quietly(self):
        self.safe()
        self.advance_head()
        self.assertIsNone(check_native_balance_drift())

    def test_task_runs_the_check(self):
        self.safe()
        self.advance_head()
        call_command("backfill_native_balances", stdout=StringIO())

        self.assertEqual(check_native_balance_drift_task.delay().get()["mismatched"], 0)

    def test_orphan_rows_are_reported(self):
        """A reorg that removes a Safe creation cascades the
        `history_safecontract` row away and leaves the rollup row behind,
        still contributing its balance."""
        safe = self.safe()
        self.transfer(self.block(), 600, to=safe.address)
        self.advance_head()
        call_command("backfill_native_balances", stdout=StringIO())

        SafeContract.objects.filter(address=safe.address).delete()

        with self.assertLogs(
            "safe_transaction_service.analytics.tasks", level="WARNING"
        ) as logs:
            summary = check_native_balance_drift()

        self.assertEqual(summary["orphan_rows"], 1)
        self.assertIn("no Safe in history_safecontract", logs.output[0])

    def test_no_orphans_on_a_healthy_rollup(self):
        self.safe()
        self.advance_head()
        call_command("backfill_native_balances", stdout=StringIO())
        self.assertEqual(check_native_balance_drift()["orphan_rows"], 0)


class TestChunkedCeleryBackfill(NativeBalanceRollupTestCase):
    """`--celery` is the same walk driven from the worker. It must land on
    exactly the same rows and the same watermark as the inline mode — the
    only difference is who holds the loop.

    Eager mode runs the whole chain inside the first `apply_async`, so a
    `--celery` call here completes before it returns; `chunk_size` is kept
    small so several chunks actually happen.
    """

    def backfill(self, **kwargs) -> str:
        out = StringIO()
        call_command("backfill_native_balances", stdout=out, stderr=out, **kwargs)
        return out.getvalue()

    def test_celery_mode_matches_inline_mode(self):
        safes = [self.safe() for _ in range(5)]
        block = self.block()
        for safe in safes[:3]:
            self.transfer(block, 900, to=safe.address)
        self.advance_head()
        head = native_balance_head_block()

        self.backfill(celery=True, chunk_size=2)

        # A row per Safe, all stamped at the same block, watermark handed
        # over — byte for byte what the inline mode produces.
        self.assertEqual(SafeNativeBalance.objects.count(), 5)
        self.assertEqual(
            set(SafeNativeBalance.objects.values_list("updated_to_block", flat=True)),
            {head},
        )
        self.assertEqual(
            AnalyticsWatermark.objects.get(name=NATIVE_BALANCE_WATERMARK).block_number,
            head,
        )
        self.assertEqual(self.totals(), _calculate_native_balances_from_db())
        self.assertEqual(self.totals(), (2_700, 3))

    def test_run_manifest_accounts_for_every_safe(self):
        for _ in range(5):
            self.safe()
        self.advance_head()

        self.backfill(celery=True, chunk_size=2)

        run = load_native_balance_run(latest_native_balance_run_id())
        self.assertEqual(run["state"], "finished")
        self.assertEqual(run["safes_seen"], 5)
        self.assertEqual(run["safes_seeded"], 5)
        self.assertEqual(run["safes_already_present"], 0)
        self.assertTrue(run["watermark_written"])
        # 5 Safes at 2 per chunk: three chunks of work, then one that walks
        # off the end and closes the run.
        self.assertEqual(run["chunks_done"], 3)
        self.assertIsNotNone(run["finished_at"])

    def test_celery_mode_resumes_over_rows_already_written(self):
        for _ in range(4):
            self.safe()
        self.advance_head()
        self.backfill(chunk_size=2)  # inline first
        watermark = AnalyticsWatermark.objects.get(
            name=NATIVE_BALANCE_WATERMARK
        ).block_number

        self.safe()
        self.advance_head()
        self.backfill(celery=True, chunk_size=2)

        run = load_native_balance_run(latest_native_balance_run_id())
        self.assertEqual(run["safes_already_present"], 4)
        self.assertEqual(run["safes_seeded"], 1)
        self.assertEqual(SafeNativeBalance.objects.count(), 5)
        # Topping up an already-handed-over rollup must not move the
        # watermark: the Safes it did not touch are only complete through
        # the old one.
        self.assertFalse(run["watermark_written"])
        self.assertEqual(
            AnalyticsWatermark.objects.get(name=NATIVE_BALANCE_WATERMARK).block_number,
            watermark,
        )

    def test_a_failing_chunk_stops_the_chain_and_is_recorded(self):
        for _ in range(6):
            self.safe()
        self.advance_head()

        with patch(
            "safe_transaction_service.analytics.tasks.seed_missing_native_balances",
            side_effect=RuntimeError("pg went away"),
        ):
            self.backfill(celery=True, chunk_size=2)

        run = load_native_balance_run(latest_native_balance_run_id())
        self.assertEqual(run["state"], "failed")
        self.assertIn("pg went away", run["error"])
        self.assertEqual(run["chunks_done"], 0)
        # Nothing written, and crucially no watermark — the incremental
        # task must keep refusing until a run actually completes.
        self.assertEqual(SafeNativeBalance.objects.count(), 0)
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).exists()
        )

        # Re-running finishes the job.
        self.backfill(celery=True, chunk_size=2)
        self.assertEqual(SafeNativeBalance.objects.count(), 6)
        self.assertTrue(
            AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).exists()
        )

    def test_chunk_task_is_inert_once_the_run_is_closed(self):
        """Guards the one way a duplicated message could corrupt a run:
        a late redelivery re-walking a cursor that has already moved."""
        self.safe()
        self.advance_head()
        self.backfill(celery=True, chunk_size=2)
        run_id = latest_native_balance_run_id()
        before = load_native_balance_run(run_id)

        backfill_native_balance_chunk(run_id)

        self.assertEqual(load_native_balance_run(run_id), before)

    def test_status_reports_the_celery_run(self):
        self.safe()
        self.advance_head()
        self.backfill(celery=True, chunk_size=2)

        output = self.backfill(status=True)

        self.assertIn("Most recent --celery run", output)
        self.assertIn("finished", output)

    def test_status_without_a_celery_run_says_so(self):
        self.safe()
        self.advance_head()
        self.assertIn("none recorded", self.backfill(status=True))
