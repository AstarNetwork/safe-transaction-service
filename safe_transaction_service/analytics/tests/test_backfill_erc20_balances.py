"""Tests for `manage.py backfill_erc20_balances` (token-holdings spec
§4.3, `docs/specs/token-holdings.md` in the workspace root).

Scaffolding mirrors `Erc20BalanceRollupTestCase` in
`test_erc20_balance_rollup.py` (fake clock, `block()`/`safe()`/`token()`/
`transfer()`/`advance_head()` helpers) rather than importing it -- same
reason `test_erc20_balance_drift.py` gives: no precedent in this suite for
sharing a `TestCase` base across files, and this file needs its own
`timezone.now()` patch target (the command module, in addition to
`tasks.py`) that the rollup tests do not.
"""

from datetime import timedelta
from decimal import Decimal
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import OperationalError, transaction
from django.test import TestCase
from django.utils import timezone

from eth_account import Account
from hexbytes import HexBytes

from safe_transaction_service.analytics import tasks as tasks_module
from safe_transaction_service.analytics.management.commands import (
    backfill_erc20_balances as backfill_module,
)
from safe_transaction_service.analytics.management.commands.backfill_erc20_balances import (
    _BOUNDARY_WATERMARK,
    _PROGRESS_WATERMARK,
    _WHALE_PROGRESS_WATERMARK,
    apply_whale_range,
    delete_zero_whale_pairs,
    detect_runtime_whales,
    refresh_whale_list,
)
from safe_transaction_service.analytics.models import (
    AnalyticsWatermark,
    Erc20BalanceWhale,
    SafeTokenBalance,
)
from safe_transaction_service.analytics.tasks import (
    ERC20_BALANCE_BACKFILL_STALE_SECONDS,
    ERC20_BALANCE_SAFES_WATERMARK,
    ERC20_BALANCE_WATERMARK,
    build_erc20_balance_backfill_run,
    compute_erc20_balance_rollup_task,
    dispatch_erc20_balance_backfill_run,
    erc20_balance_backfill_looks_stalled,
    erc20_balance_backfill_watchdog_task,
    erc20_balance_head_block,
    erc20_balance_seed_candidates,
    erc20_balances_upto_block,
    latest_erc20_balance_backfill_run_id,
    load_erc20_balance_backfill_run,
    run_erc20_balance_rollup,
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

# Well clear of `test_erc20_balance_rollup.py` (5_000_000) and
# `test_erc20_balance_drift.py` (5_500_000)'s own `BASE_BLOCK`s.
BASE_BLOCK = 6_000_000


class Erc20BalanceBackfillTestCase(TestCase):
    """Same fake-clock shape `Erc20BalanceRollupTestCase` uses -- see its
    docstring for why `ERC20_BALANCE_SAFE_SETTLE` forces a fake clock at
    all. Patches `timezone.now` in *two* places: `tasks.py` (for the
    reused P2 helpers, e.g. `erc20_balance_run_boundary`) and this
    command module itself (for its own `timezone.now()` calls in
    `_resolve_run` / `_finish`) -- both must agree on "now" or a fresh
    run's boundary and its progress-row timestamp would disagree.
    """

    CLOCK_STEP = timedelta(minutes=25)

    def setUp(self):
        super().setUp()
        self.next_block = BASE_BLOCK
        self.clock = timezone.now()
        for target in (
            "safe_transaction_service.analytics.tasks.timezone.now",
            "safe_transaction_service.analytics.management.commands."
            "backfill_erc20_balances.timezone.now",
        ):
            patcher = patch(target, side_effect=lambda: self.clock)
            patcher.start()
            self.addCleanup(patcher.stop)

    def advance_clock(self, delta: timedelta = timedelta(hours=2)):
        self.clock += delta

    def block(self, confirmed: bool = True):
        self.next_block += 1
        self.advance_clock(self.CLOCK_STEP)
        block = EthereumBlockFactory(number=self.next_block, confirmed=confirmed)
        IndexingStatus.objects.filter(
            indexing_type=IndexingStatusType.ERC20_721_EVENTS.value
        ).update(block_number=self.next_block)
        return block

    def safe(self, block=None, created=None):
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
        for _ in range(blocks):
            self.block(confirmed=True)

    def balance(self, address, token) -> Decimal | None:
        row = SafeTokenBalance.objects.filter(
            safe_address=address, token_address=token
        ).first()
        return row.balance if row else None

    def run_backfill(self, **options):
        call_command("backfill_erc20_balances", **options)

    def head_watermark(self) -> int:
        return AnalyticsWatermark.objects.get(name=ERC20_BALANCE_WATERMARK).block_number


class TestBackfillMatchesFromScratchAndNightlyContinues(Erc20BalanceBackfillTestCase):
    def test_result_equals_from_scratch_sum_and_nightly_task_continues(self):
        safe_a, safe_b = self.safe(), self.safe()
        token = self.token()
        block = self.block()
        self.transfer(block, token, 100, to=safe_a.address)
        self.transfer(block, token, 250, to=safe_b.address)
        self.advance_head()

        self.run_backfill(chunk_size=10)

        head = self.head_watermark()
        expected = erc20_balances_upto_block(
            [HexBytes(safe_a.address), HexBytes(safe_b.address)], head
        )
        self.assertEqual(
            self.balance(safe_a.address, token),
            expected[(HexBytes(safe_a.address), HexBytes(token))],
        )
        self.assertEqual(
            self.balance(safe_b.address, token),
            expected[(HexBytes(safe_b.address), HexBytes(token))],
        )
        self.assertTrue(
            AnalyticsWatermark.objects.filter(
                name=ERC20_BALANCE_SAFES_WATERMARK
            ).exists()
        )
        # Progress rows are cleaned up once the run completes.
        self.assertFalse(
            AnalyticsWatermark.objects.filter(
                name__in=[_PROGRESS_WATERMARK, _BOUNDARY_WATERMARK]
            ).exists()
        )

        # The nightly task picks up exactly where the backfill left off.
        self.transfer(self.block(), token, 40, to=safe_a.address)
        self.advance_head()
        run_erc20_balance_rollup()
        self.assertEqual(
            self.balance(safe_a.address, token), Decimal(100) + Decimal(40)
        )


class TestResumeAfterInterruption(Erc20BalanceBackfillTestCase):
    def test_resume_after_a_mid_run_crash_matches_an_uninterrupted_run_with_no_duplicates(
        self,
    ):
        safes = [self.safe() for _ in range(5)]
        token = self.token()
        block = self.block()
        for index, safe in enumerate(safes):
            self.transfer(block, token, (index + 1) * 10, to=safe.address)
        self.advance_head()

        original_seed = backfill_module.seed_missing_erc20_balances
        calls = {"n": 0}

        def flaky(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("simulated crash mid-run")
            return original_seed(*args, **kwargs)

        with patch.object(
            backfill_module, "seed_missing_erc20_balances", side_effect=flaky
        ):
            with self.assertRaises(RuntimeError):
                self.run_backfill(chunk_size=1)

        # The crash happened before the run ever completed: the real
        # watermark must not exist, but the progress row from the chunk
        # that DID commit must still be there for the resume to use.
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )
        progress = AnalyticsWatermark.objects.get(name=_PROGRESS_WATERMARK)
        self.assertIsNotNone(progress.address)

        # Resume: same command, no --restart.
        self.run_backfill(chunk_size=1)

        for index, safe in enumerate(safes):
            self.assertEqual(
                self.balance(safe.address, token), Decimal((index + 1) * 10)
            )
        # No duplicate pairs from the retried chunk.
        self.assertEqual(
            SafeTokenBalance.objects.filter(token_address=token).count(), 5
        )


class TestLockHeldRefuses(Erc20BalanceBackfillTestCase):
    def test_lock_held_by_the_task_refuses_without_touching_anything(self):
        self.safe()
        self.advance_head()

        lock_name = get_task_lock_name(compute_erc20_balance_rollup_task.name)
        redis = get_redis()
        with redis.lock(lock_name, blocking=False):
            with self.assertRaises(CommandError):
                self.run_backfill()

        self.assertFalse(AnalyticsWatermark.objects.exists())
        self.assertEqual(SafeTokenBalance.objects.count(), 0)


class TestAlreadyInitialisedGuard(Erc20BalanceBackfillTestCase):
    def test_refuses_without_restart_once_already_initialised(self):
        self.safe()
        self.advance_head()
        self.run_backfill()

        with self.assertRaises(CommandError):
            self.run_backfill()


class TestRestart(Erc20BalanceBackfillTestCase):
    def test_restart_wipes_and_rebuilds(self):
        safe = self.safe()
        token = self.token()
        self.transfer(self.block(), token, 500, to=safe.address)
        self.advance_head()
        self.run_backfill()
        self.assertEqual(self.balance(safe.address, token), Decimal(500))

        # Tamper the row so a stale value proves --restart actually
        # recomputed it instead of being a no-op.
        SafeTokenBalance.objects.filter(
            safe_address=safe.address, token_address=token
        ).update(balance=Decimal(999_999))

        new_token = self.token()
        self.transfer(self.block(), new_token, 77, to=safe.address)
        self.advance_head()

        self.run_backfill(restart=True)

        self.assertEqual(self.balance(safe.address, token), Decimal(500))
        self.assertEqual(self.balance(safe.address, new_token), Decimal(77))


class TestPositiveIntOptionsValidation(Erc20BalanceBackfillTestCase):
    """`--whale-block-range 0` is the option whose bug actually hung a
    test run (see `TestWhaleWalkResumeAfterInterruption`'s comment and
    `_positive_int` in the command module): a zero step never advances
    `_run_whale_walk`'s loop. `--chunk-size` and `--whale-row-threshold`
    get the same `_positive_int` argparse type, so their own zero/negative
    cases are covered here too rather than left implicit.

    These pass the flag as a raw CLI-style token (`call_command(name,
    "--flag", "0")`), not as a keyword argument, on purpose: Django's
    `call_command` only re-runs a parser action's `type=` callable for
    options it must inject back into `parse_args` (`required=True` ones);
    an ordinary optional kwarg like `self.run_backfill(chunk_size=0)`
    goes straight into the final options dict and never touches
    `_positive_int` at all -- see `parse_args`/`arg_options` in Django's
    own `call_command` source. `TestRuntimeGuardCatchesKwargBypass` below
    covers exactly that bypassed path for `--whale-block-range`, which is
    why `_run_whale_walk` has its own runtime guard as well.
    """

    def test_whale_block_range_zero_is_rejected(self):
        with self.assertRaises(CommandError):
            call_command("backfill_erc20_balances", "--whale-block-range", "0")

    def test_whale_block_range_negative_is_rejected(self):
        with self.assertRaises(CommandError):
            call_command("backfill_erc20_balances", "--whale-block-range", "-1")

    def test_chunk_size_zero_is_rejected(self):
        with self.assertRaises(CommandError):
            call_command("backfill_erc20_balances", "--chunk-size", "0")

    def test_whale_row_threshold_zero_is_rejected(self):
        with self.assertRaises(CommandError):
            call_command("backfill_erc20_balances", "--whale-row-threshold", "0")


class TestRuntimeGuardCatchesKwargBypass(Erc20BalanceBackfillTestCase):
    def test_a_kwarg_supplied_zero_range_is_still_caught_at_runtime(self):
        """`self.run_backfill(whale_block_range=0)` -- an ordinary kwarg,
        exactly how every other test in this file calls the command --
        skips `_positive_int` entirely (see the class docstring above).
        With a whale actually present, `_run_whale_walk` reaches its own
        `range_to <= range_from` guard instead of spinning forever."""
        whale = self.safe()
        token = self.token()
        self.transfer(self.block(), token, 100, to=whale.address)
        self.advance_head()
        Erc20BalanceWhale.objects.create(safe_address=whale.address)

        with self.assertRaises(CommandError):
            self.run_backfill(whale_block_range=0)


class TestWhaleRangeWalkMatchesUnsplitComputation(Erc20BalanceBackfillTestCase):
    """The plain-function proof that block-range accumulation is correct,
    independent of the command's own chunking / detection machinery: apply
    several small ranges by hand and compare the resulting table rows
    against `erc20_balances_upto_block` (the ordinary, address-driven
    reference already proven correct by P2's own tests)."""

    def test_multi_range_walk_matches_erc20_balances_upto_block(self):
        whale = self.safe()
        token = self.token()
        b1 = self.block()
        self.transfer(b1, token, 1_000, to=whale.address)
        b2 = self.block()
        self.transfer(b2, token, 300, _from=whale.address)
        b3 = self.block()
        self.transfer(b3, token, 50, to=whale.address)

        whale_bytes = HexBytes(whale.address)
        # One-block steps force several ranges between the three
        # transfers' blocks -- starting at `b1.number - 2`, not the real
        # walk's absolute 0: `BASE_BLOCK` alone is six million, and a
        # step of 1 from 0 up to `b3.number` is six million-plus
        # iterations, not "several" (this is exactly the hang class the
        # `--whale-block-range` bug was, just self-inflicted here instead
        # of via a bad option value -- there is nothing to sum for this
        # whale before `b1` anyway, so starting just short of it changes
        # nothing about what this test proves).
        range_from = b1.number - 2
        while range_from < b3.number:
            range_to = min(range_from + 1, b3.number)
            with transaction.atomic():
                apply_whale_range([whale_bytes], range_from, range_to)
            range_from = range_to
        delete_zero_whale_pairs([whale_bytes])

        expected = erc20_balances_upto_block([whale_bytes], b3.number)
        self.assertEqual(
            self.balance(whale.address, token),
            expected[(whale_bytes, HexBytes(token))],
        )
        self.assertEqual(self.balance(whale.address, token), Decimal(750))


class TestWhaleIntegratedIntoBackfill(Erc20BalanceBackfillTestCase):
    def test_a_known_whale_is_summed_via_the_whale_path_and_lands_correctly(self):
        whale, regular = self.safe(), self.safe()
        token = self.token()
        block = self.block()
        self.transfer(block, token, 10_000, to=whale.address)
        self.transfer(block, token, 20, to=regular.address)
        self.advance_head()

        Erc20BalanceWhale.objects.create(safe_address=whale.address)

        self.run_backfill(chunk_size=10, whale_block_range=10_000_000)

        head = self.head_watermark()
        expected = erc20_balances_upto_block(
            [HexBytes(whale.address), HexBytes(regular.address)], head
        )
        self.assertEqual(
            self.balance(whale.address, token),
            expected[(HexBytes(whale.address), HexBytes(token))],
        )
        self.assertEqual(
            self.balance(regular.address, token),
            expected[(HexBytes(regular.address), HexBytes(token))],
        )


class TestWhaleWalkRunsOnce(Erc20BalanceBackfillTestCase):
    def test_several_whales_in_different_chunks_produce_exactly_one_walk(self):
        """Whales landing in different chunks must not each trigger their
        own summation -- that would cost one full-history scan per whale
        instead of one walk for the whole run. `chunk_size=1` forces
        `whale_a`, `whale_b` and `regular` into three separate chunks; if
        the walk were still (wrongly) driven from inside the chunk loop,
        `apply_whale_range` would be called once per whale-containing
        chunk instead of once for the whole run."""
        whale_a, whale_b, regular = self.safe(), self.safe(), self.safe()
        token = self.token()
        block = self.block()
        self.transfer(block, token, 10_000, to=whale_a.address)
        self.transfer(block, token, 20_000, to=whale_b.address)
        self.transfer(block, token, 5, to=regular.address)
        self.advance_head()

        Erc20BalanceWhale.objects.create(safe_address=whale_a.address)
        Erc20BalanceWhale.objects.create(safe_address=whale_b.address)

        original_apply = backfill_module.apply_whale_range
        calls = []

        def counting_apply(*args, **kwargs):
            calls.append(args)
            return original_apply(*args, **kwargs)

        with patch.object(
            backfill_module, "apply_whale_range", side_effect=counting_apply
        ):
            # A block range far bigger than this tiny test chain means
            # the whole (0, H] span is covered by a single range.
            self.run_backfill(chunk_size=1, whale_block_range=10_000_000)

        self.assertEqual(len(calls), 1)
        (whale_addresses_arg, _range_from, _range_to) = calls[0]
        self.assertCountEqual(
            whale_addresses_arg,
            [HexBytes(whale_a.address), HexBytes(whale_b.address)],
        )

        head = self.head_watermark()
        expected = erc20_balances_upto_block(
            [HexBytes(whale_a.address), HexBytes(whale_b.address)], head
        )
        self.assertEqual(
            self.balance(whale_a.address, token),
            expected[(HexBytes(whale_a.address), HexBytes(token))],
        )
        self.assertEqual(
            self.balance(whale_b.address, token),
            expected[(HexBytes(whale_b.address), HexBytes(token))],
        )


class TestWhaleWalkResumeAfterInterruption(Erc20BalanceBackfillTestCase):
    def test_walk_interrupted_after_a_range_resumes_without_double_counting(self):
        whale = self.safe()
        token = self.token()
        b1 = self.block()
        self.transfer(b1, token, 1_000, to=whale.address)
        b2 = self.block()
        self.transfer(b2, token, 300, _from=whale.address)
        self.advance_head()

        Erc20BalanceWhale.objects.create(safe_address=whale.address)

        # Computed read-only, before the command runs, so the walk gets
        # *exactly* two ranges: (0, head-1] (succeeds and commits) and
        # (head-1, head] (fails and rolls back) -- proving resume picks
        # up the persisted cursor rather than either redoing range 1 or
        # skipping range 2. That split only makes sense -- and only stays
        # a *positive* --whale-block-range -- if head is at least 3 (see
        # the bug this guards: an earlier version of this test computed
        # `head - 1` without checking head first, and a low head made
        # that 0, which spun `_run_whale_walk` forever). `self.safe()` +
        # two blocks + `advance_head()`'s default 3 comfortably clears
        # this, but assert it rather than assume it.
        head = erc20_balance_head_block()
        self.assertGreaterEqual(
            head,
            3,
            "test setup must produce head >= 3, or whale_block_range = "
            "head - 1 below would not be a positive, genuine 2-range "
            "split",
        )
        whale_block_range = head - 1

        original_apply = backfill_module.apply_whale_range
        calls = {"n": 0}

        def flaky(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("simulated crash mid-walk")
            return original_apply(*args, **kwargs)

        with patch.object(backfill_module, "apply_whale_range", side_effect=flaky):
            with self.assertRaises(RuntimeError):
                self.run_backfill(chunk_size=10, whale_block_range=whale_block_range)

        # Range 1 committed (progress advanced past it); the real
        # watermark was never reached.
        walk_progress = AnalyticsWatermark.objects.get(name=_WHALE_PROGRESS_WATERMARK)
        self.assertEqual(walk_progress.block_number, head - 1)
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )

        # Resume: same command, no --restart. Only range 2 needs to run.
        with patch.object(
            backfill_module, "apply_whale_range", side_effect=original_apply
        ) as resumed_apply:
            self.run_backfill(chunk_size=10, whale_block_range=whale_block_range)
        resumed_apply.assert_called_once_with([HexBytes(whale.address)], head - 1, head)

        expected = erc20_balances_upto_block([HexBytes(whale.address)], head)
        self.assertEqual(
            self.balance(whale.address, token),
            expected[(HexBytes(whale.address), HexBytes(token))],
        )
        self.assertEqual(self.balance(whale.address, token), Decimal(700))
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=_WHALE_PROGRESS_WATERMARK).exists()
        )


class TestResumeDoesNotRefreshWhaleList(Erc20BalanceBackfillTestCase):
    def test_resume_does_not_call_refresh_whale_list(self):
        safes = [self.safe() for _ in range(3)]
        token = self.token()
        block = self.block()
        for safe in safes:
            self.transfer(block, token, 10, to=safe.address)
        self.advance_head()

        original_seed = backfill_module.seed_missing_erc20_balances
        calls = {"n": 0}

        def flaky(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("simulated crash mid-run")
            return original_seed(*args, **kwargs)

        with patch.object(
            backfill_module, "seed_missing_erc20_balances", side_effect=flaky
        ):
            with self.assertRaises(RuntimeError):
                self.run_backfill(chunk_size=1)

        # Resuming: refresh_whale_list must not be called at all.
        with patch.object(backfill_module, "refresh_whale_list") as mocked_refresh:
            self.run_backfill(chunk_size=1)
            mocked_refresh.assert_not_called()


class TestRuntimeWhaleDetection(Erc20BalanceBackfillTestCase):
    def test_detect_runtime_whales_flags_the_heavy_address_only(self):
        heavy = self.safe()
        light = self.safe()
        token = self.token()
        block = self.block()
        for index in range(5):
            self.transfer(block, token, 1 + index, to=heavy.address)
        self.transfer(block, token, 1, to=light.address)

        offenders = detect_runtime_whales(
            [HexBytes(heavy.address), HexBytes(light.address)], row_threshold=2
        )

        self.assertEqual(offenders, [HexBytes(heavy.address)])
        self.assertTrue(
            Erc20BalanceWhale.objects.filter(safe_address=heavy.address).exists()
        )
        self.assertFalse(
            Erc20BalanceWhale.objects.filter(safe_address=light.address).exists()
        )

    def test_runtime_detection_inside_a_full_backfill_run_adds_to_the_whale_table(
        self,
    ):
        """P8 (docs/specs/token-holdings.md §12): detection is no longer
        unconditional -- it only fires once the chunk's own seed hits
        `statement_timeout`. Simulate that by making the first seed
        attempt raise the same `OperationalError` Postgres raises on a
        cancelled statement; the retry (without the whale detection
        found) then uses the real `seed_missing_erc20_balances`."""
        heavy = self.safe()
        light = self.safe()
        token = self.token()
        block = self.block()
        for index in range(5):
            self.transfer(block, token, 1 + index, to=heavy.address)
        self.transfer(block, token, 1, to=light.address)
        self.advance_head()

        self.assertFalse(
            Erc20BalanceWhale.objects.filter(safe_address=heavy.address).exists()
        )

        original_seed = backfill_module.seed_missing_erc20_balances
        calls = {"n": 0}

        def flaky(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise OperationalError("canceling statement due to statement timeout")
            return original_seed(*args, **kwargs)

        with patch.object(
            backfill_module, "seed_missing_erc20_balances", side_effect=flaky
        ):
            self.run_backfill(
                chunk_size=10, whale_row_threshold=2, whale_block_range=10_000_000
            )

        self.assertEqual(calls["n"], 2)
        self.assertTrue(
            Erc20BalanceWhale.objects.filter(safe_address=heavy.address).exists()
        )
        self.assertFalse(
            Erc20BalanceWhale.objects.filter(safe_address=light.address).exists()
        )
        head = self.head_watermark()
        expected = erc20_balances_upto_block(
            [HexBytes(heavy.address), HexBytes(light.address)], head
        )
        self.assertEqual(
            self.balance(heavy.address, token),
            expected[(HexBytes(heavy.address), HexBytes(token))],
        )
        self.assertEqual(
            self.balance(light.address, token),
            expected[(HexBytes(light.address), HexBytes(token))],
        )


class TestNoDetectionOnNormalChunks(Erc20BalanceBackfillTestCase):
    """P8: a chunk whose seed does not time out must never pay for
    run-time whale detection at all -- not `detect_runtime_whales`, and
    not the bounded-count query it is built on."""

    def test_normal_chunks_make_zero_detection_queries(self):
        safe_a, safe_b = self.safe(), self.safe()
        token = self.token()
        block = self.block()
        self.transfer(block, token, 100, to=safe_a.address)
        self.transfer(block, token, 250, to=safe_b.address)
        self.advance_head()

        with (
            patch.object(backfill_module, "detect_runtime_whales") as mocked_detect,
            patch.object(
                backfill_module, "_bounded_transfer_row_count"
            ) as mocked_bounded,
        ):
            self.run_backfill(chunk_size=10)

        mocked_detect.assert_not_called()
        mocked_bounded.assert_not_called()

        head = self.head_watermark()
        expected = erc20_balances_upto_block(
            [HexBytes(safe_a.address), HexBytes(safe_b.address)], head
        )
        self.assertEqual(
            self.balance(safe_a.address, token),
            expected[(HexBytes(safe_a.address), HexBytes(token))],
        )


class TestSeedTimeoutRetryAlsoTimesOut(Erc20BalanceBackfillTestCase):
    def test_second_timeout_raises_command_error_without_advancing_cursor(self):
        safe = self.safe()
        token = self.token()
        self.transfer(self.block(), token, 100, to=safe.address)
        self.advance_head()

        calls = {"n": 0}

        def always_timeout(*args, **kwargs):
            calls["n"] += 1
            raise OperationalError("canceling statement due to statement timeout")

        with patch.object(
            backfill_module, "seed_missing_erc20_balances", side_effect=always_timeout
        ):
            with self.assertRaises(CommandError):
                self.run_backfill(chunk_size=10)

        # Exactly two attempts: the first seed, one detection-driven
        # retry, and no third try -- "never loop more than once".
        self.assertEqual(calls["n"], 2)

        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )
        # The progress watermark was created by `resolve_run` (address is
        # None until a chunk actually commits) but never advanced past
        # that, since both attempts at the only chunk rolled back.
        progress = AnalyticsWatermark.objects.get(name=_PROGRESS_WATERMARK)
        self.assertIsNone(progress.address)
        self.assertEqual(
            SafeTokenBalance.objects.filter(token_address=token).count(), 0
        )


class TestSeedTimeoutInlineAndCeleryMatch(Erc20BalanceBackfillTestCase):
    """The timeout-triggered detect/retry path (P8) must give the same
    result whether the chunk runs under --inline or as one Celery slice
    -- both call the same `run_chunk_slice`, but this proves it end to
    end rather than by code inspection."""

    def _setup_heavy_and_light(self):
        heavy = self.safe()
        light = self.safe()
        token = self.token()
        block = self.block()
        for index in range(5):
            self.transfer(block, token, 1 + index, to=heavy.address)
        self.transfer(block, token, 1, to=light.address)
        self.advance_head()
        return heavy, light, token

    def _flaky_seed(self, calls):
        original_seed = backfill_module.seed_missing_erc20_balances

        def flaky(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise OperationalError("canceling statement due to statement timeout")
            return original_seed(*args, **kwargs)

        return flaky

    def test_inline_timeout_retry_matches_reference(self):
        heavy, light, token = self._setup_heavy_and_light()
        calls = {"n": 0}

        with patch.object(
            backfill_module,
            "seed_missing_erc20_balances",
            side_effect=self._flaky_seed(calls),
        ):
            self.run_backfill(
                chunk_size=10, whale_row_threshold=2, whale_block_range=10_000_000
            )

        self.assertEqual(calls["n"], 2)
        self.assertTrue(
            Erc20BalanceWhale.objects.filter(safe_address=heavy.address).exists()
        )
        head = self.head_watermark()
        expected = erc20_balances_upto_block(
            [HexBytes(heavy.address), HexBytes(light.address)], head
        )
        self.assertEqual(
            self.balance(heavy.address, token),
            expected[(HexBytes(heavy.address), HexBytes(token))],
        )
        self.assertEqual(
            self.balance(light.address, token),
            expected[(HexBytes(light.address), HexBytes(token))],
        )

    def test_celery_timeout_retry_matches_reference(self):
        heavy, light, token = self._setup_heavy_and_light()
        calls = {"n": 0}

        with patch.object(
            backfill_module,
            "seed_missing_erc20_balances",
            side_effect=self._flaky_seed(calls),
        ):
            self.run_backfill(
                celery=True,
                chunk_size=10,
                whale_row_threshold=2,
                whale_block_range=10_000_000,
                task_chunks=10,
            )

        self.assertEqual(calls["n"], 2)
        self.assertTrue(
            Erc20BalanceWhale.objects.filter(safe_address=heavy.address).exists()
        )
        head = self.head_watermark()
        expected = erc20_balances_upto_block(
            [HexBytes(heavy.address), HexBytes(light.address)], head
        )
        self.assertEqual(
            self.balance(heavy.address, token),
            expected[(HexBytes(heavy.address), HexBytes(token))],
        )
        self.assertEqual(
            self.balance(light.address, token),
            expected[(HexBytes(light.address), HexBytes(token))],
        )


class TestRefreshWhaleListFromPgStats(Erc20BalanceBackfillTestCase):
    def test_no_pg_stats_yet_is_a_quiet_noop(self):
        # A per-test transaction never runs ANALYZE, so pg_stats has
        # nothing for history_erc20transfer here -- this only proves the
        # "nothing to add" path doesn't error; the MCV → whale-table path
        # itself is exercised against a real ANALYZE'd database in
        # staging (`optimism-balance-accuracy.paste.sql` section 1w is
        # the same query this seeds from).
        self.assertEqual(refresh_whale_list(min_frequency=0.001), 0)


# ══════════════════════════════ P7: --celery mode ══════════════════════
#
# All Celery paths here run under CELERY_ALWAYS_EAGER (config/settings/
# test.py) -- `.apply_async(...)` executes synchronously and recursively,
# so `dispatch_erc20_balance_backfill_run` runs the WHOLE chain (chunk
# task(s) -> whale task(s) -> finish task) inside one Python call unless
# a task's own exception handling stops it. Same testing shape
# `test_backfill_daily_metrics.py` documents for its own chain.


class TestCeleryModeMatchesInline(Erc20BalanceBackfillTestCase):
    def test_celery_mode_result_equals_inline_mode(self):
        safe_a, safe_b = self.safe(), self.safe()
        token = self.token()
        block = self.block()
        self.transfer(block, token, 100, to=safe_a.address)
        self.transfer(block, token, 250, to=safe_b.address)
        self.advance_head()

        self.run_backfill(celery=True, chunk_size=1, task_chunks=2)

        head = self.head_watermark()
        expected = erc20_balances_upto_block(
            [HexBytes(safe_a.address), HexBytes(safe_b.address)], head
        )
        self.assertEqual(
            self.balance(safe_a.address, token),
            expected[(HexBytes(safe_a.address), HexBytes(token))],
        )
        self.assertEqual(
            self.balance(safe_b.address, token),
            expected[(HexBytes(safe_b.address), HexBytes(token))],
        )
        self.assertTrue(
            AnalyticsWatermark.objects.filter(
                name=ERC20_BALANCE_SAFES_WATERMARK
            ).exists()
        )
        # Same three progress rows --inline uses, and they are gone once
        # the chain's finish task runs -- same invariant as --inline.
        self.assertFalse(
            AnalyticsWatermark.objects.filter(
                name__in=[
                    _PROGRESS_WATERMARK,
                    _BOUNDARY_WATERMARK,
                    _WHALE_PROGRESS_WATERMARK,
                ]
            ).exists()
        )

        run_id = latest_erc20_balance_backfill_run_id()
        run = load_erc20_balance_backfill_run(run_id)
        self.assertEqual(run["run_id"], run_id)
        self.assertEqual(run["state"], "finished")
        self.assertEqual(run["phase"], "done")
        self.assertGreaterEqual(run["slices_done"], 1)

        # The nightly task picks up exactly where the chain left off,
        # same as the --inline handover test.
        self.transfer(self.block(), token, 40, to=safe_a.address)
        self.advance_head()
        run_erc20_balance_rollup()
        self.assertEqual(
            self.balance(safe_a.address, token), Decimal(100) + Decimal(40)
        )

    def test_celery_mode_sums_a_whale_via_the_whale_walk_tasks(self):
        whale, regular = self.safe(), self.safe()
        token = self.token()
        block = self.block()
        self.transfer(block, token, 10_000, to=whale.address)
        self.transfer(block, token, 20, to=regular.address)
        self.advance_head()
        Erc20BalanceWhale.objects.create(safe_address=whale.address)

        # task_chunks=1 forces one whale-walk task per range; head is
        # BASE_BLOCK (6,000,000) plus a handful of blocks, so
        # `whale_block_range=1_000_000` gives ~7 ranges/tasks -- enough
        # to exercise `backfill_erc20_balance_whale_task`'s own
        # self-dispatch without the millions of tiny ranges a small
        # `whale_block_range` would produce against that large absolute
        # head (an earlier version of this test used
        # `whale_block_range=1` and recursed itself into a stack
        # overflow under CELERY_ALWAYS_EAGER -- a test-parameter bug, not
        # a production one; see the P7 report).
        self.run_backfill(
            celery=True,
            chunk_size=10,
            whale_block_range=1_000_000,
            task_chunks=1,
        )

        head = self.head_watermark()
        expected = erc20_balances_upto_block(
            [HexBytes(whale.address), HexBytes(regular.address)], head
        )
        self.assertEqual(
            self.balance(whale.address, token),
            expected[(HexBytes(whale.address), HexBytes(token))],
        )
        self.assertEqual(
            self.balance(regular.address, token),
            expected[(HexBytes(regular.address), HexBytes(token))],
        )


class TestCeleryModeInterruptedChainResumes(Erc20BalanceBackfillTestCase):
    def test_a_chain_interrupted_mid_slice_resumes_on_redispatch_with_no_double_counting(
        self,
    ):
        safes = [self.safe() for _ in range(5)]
        token = self.token()
        block = self.block()
        for index, safe in enumerate(safes):
            self.transfer(block, token, (index + 1) * 10, to=safe.address)
        self.advance_head()

        original_seed = backfill_module.seed_missing_erc20_balances
        calls = {"n": 0}

        def flaky(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("simulated worker crash mid-slice")
            return original_seed(*args, **kwargs)

        with patch.object(
            backfill_module, "seed_missing_erc20_balances", side_effect=flaky
        ):
            # Unlike --inline, a slice's exception is caught INSIDE the
            # task (mirrors `backfill_native_balance_chunk`): the chain
            # records state="failed" and stops dispatching, it does not
            # raise out of the management command.
            self.run_backfill(celery=True, chunk_size=1, task_chunks=1)

        run_id = latest_erc20_balance_backfill_run_id()
        run = load_erc20_balance_backfill_run(run_id)
        self.assertEqual(run["state"], "failed")
        self.assertIn("simulated worker crash", run["error"])

        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )
        progress = AnalyticsWatermark.objects.get(name=_PROGRESS_WATERMARK)
        self.assertIsNotNone(progress.address)

        # Resume: a second --celery is accepted here because the
        # manifest state is "failed", not "running" -- see
        # TestSecondCeleryRefusedWhileRunning for the refusal case.
        self.run_backfill(celery=True, chunk_size=1, task_chunks=1)

        for index, safe in enumerate(safes):
            self.assertEqual(
                self.balance(safe.address, token), Decimal((index + 1) * 10)
            )
        # No duplicate pairs from the retried chunk.
        self.assertEqual(
            SafeTokenBalance.objects.filter(token_address=token).count(), 5
        )

        resumed_run = load_erc20_balance_backfill_run(
            latest_erc20_balance_backfill_run_id()
        )
        self.assertEqual(resumed_run["state"], "finished")


class TestSecondCeleryRefusedWhileRunning(Erc20BalanceBackfillTestCase):
    def _fabricate_in_progress_run(self, **run_kwargs):
        """A manifest and progress rows as they would look mid-chain,
        without letting the chain actually advance: `resolve_run(False)`
        writes the same progress/boundary rows the real chunk task's
        first slice would, and the chunk task's own dispatch is
        short-circuited so nothing more happens. Distinct from
        `TestCeleryModeInterruptedChainResumes`, which crashes a REAL
        chain -- this fabricates the "still running" manifest state that
        a genuinely mid-flight chain (no crash, just hasn't finished
        yet) would have.
        """
        backfill_module.resolve_run(False)
        run = build_erc20_balance_backfill_run(
            chunk_size=run_kwargs.get("chunk_size", 10),
            whale_min_frequency=run_kwargs.get("whale_min_frequency", 0.001),
            whale_row_threshold=run_kwargs.get("whale_row_threshold", 10),
            whale_block_range=run_kwargs.get("whale_block_range", 10_000_000),
            statement_timeout_ms=run_kwargs.get("statement_timeout_ms", 1_000),
            task_chunks=run_kwargs.get("task_chunks", 5),
        )
        with patch.object(
            tasks_module.backfill_erc20_balance_chunk_task, "apply_async"
        ):
            run = dispatch_erc20_balance_backfill_run(run)
        return run

    def test_second_celery_is_refused_while_a_chain_is_running(self):
        self.safe()
        self.advance_head()

        run = self._fabricate_in_progress_run()
        self.assertEqual(run["state"], "running")

        with self.assertRaises(CommandError):
            self.run_backfill(celery=True)

        # Refused before touching anything the fabricated run didn't
        # already write -- no second manifest, no watermark.
        self.assertEqual(latest_erc20_balance_backfill_run_id(), run["run_id"])
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )

    def test_restart_is_allowed_even_while_a_chain_looks_running(self):
        safe = self.safe()
        token = self.token()
        self.transfer(self.block(), token, 500, to=safe.address)
        self.advance_head()

        self._fabricate_in_progress_run()

        # --restart is the documented escape hatch -- must not be
        # blocked by the same guard that refuses a plain --celery.
        self.run_backfill(celery=True, restart=True, chunk_size=10, task_chunks=10)

        run = load_erc20_balance_backfill_run(latest_erc20_balance_backfill_run_id())
        self.assertEqual(run["state"], "finished")
        head = self.head_watermark()
        expected = erc20_balances_upto_block([HexBytes(safe.address)], head)
        self.assertEqual(
            self.balance(safe.address, token),
            expected[(HexBytes(safe.address), HexBytes(token))],
        )


class TestStatusOutput(Erc20BalanceBackfillTestCase):
    def test_status_reports_watermarks_and_the_celery_run(self):
        safe = self.safe()
        token = self.token()
        self.transfer(self.block(), token, 500, to=safe.address)
        self.advance_head()

        self.run_backfill(celery=True, chunk_size=10, task_chunks=10)

        out = StringIO()
        call_command("backfill_erc20_balances", "--status", stdout=out)
        output = out.getvalue()

        self.assertIn("erc20_balance watermark", output)
        self.assertIn("Most recent --celery run:", output)
        self.assertIn("[finished]", output)
        self.assertIn("heartbeat=", output)
        self.assertNotIn("STALLED", output)

    def test_status_writes_nothing(self):
        safe = self.safe()
        token = self.token()
        self.transfer(self.block(), token, 500, to=safe.address)
        self.advance_head()

        rows_before = SafeTokenBalance.objects.count()
        watermark_before = AnalyticsWatermark.objects.exists()

        call_command("backfill_erc20_balances", "--status", stdout=StringIO())

        self.assertEqual(SafeTokenBalance.objects.count(), rows_before)
        self.assertEqual(AnalyticsWatermark.objects.exists(), watermark_before)


class TestNightlyTaskNoOpWhileChainRuns(Erc20BalanceBackfillTestCase):
    def test_nightly_rollup_is_a_noop_while_a_celery_chain_is_mid_run(self):
        safes = [self.safe() for _ in range(3)]
        token = self.token()
        block = self.block()
        for safe in safes:
            self.transfer(block, token, 10, to=safe.address)
        self.advance_head()

        original_seed = backfill_module.seed_missing_erc20_balances
        calls = {"n": 0}

        def flaky(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("simulated crash mid-run")
            return original_seed(*args, **kwargs)

        with patch.object(
            backfill_module, "seed_missing_erc20_balances", side_effect=flaky
        ):
            self.run_backfill(celery=True, chunk_size=1, task_chunks=1)

        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )
        self.assertTrue(
            AnalyticsWatermark.objects.filter(name=_PROGRESS_WATERMARK).exists()
        )

        # `run_erc20_balance_rollup` (the nightly task's body) is
        # unconditionally a no-op until `erc20_balance` exists -- P2
        # behaviour, re-proven here in the P7 context because it is the
        # whole reason overlapping the lock between slices is safe.
        result = run_erc20_balance_rollup()
        self.assertIsNone(result)
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )
        self.assertEqual(
            SafeTokenBalance.objects.filter(token_address=token).count(), 1
        )


# ══════════════════════════ P7 addendum: watchdog ══════════════════════


class Erc20BalanceBackfillWatchdogTestCase(Erc20BalanceBackfillTestCase):
    def fabricate_stalled_run(self, **run_kwargs):
        """Progress rows + a `state="running"` manifest whose heartbeat
        the caller can then age past the threshold by calling
        `self.advance_clock(...)` -- the chain never actually advances
        because the first chunk task's dispatch is short-circuited, the
        same fabrication `TestSecondCeleryRefusedWhileRunning` uses."""
        backfill_module.resolve_run(False)
        run = build_erc20_balance_backfill_run(
            chunk_size=run_kwargs.get("chunk_size", 10),
            whale_min_frequency=run_kwargs.get("whale_min_frequency", 0.001),
            whale_row_threshold=run_kwargs.get("whale_row_threshold", 10),
            whale_block_range=run_kwargs.get("whale_block_range", 10_000_000),
            statement_timeout_ms=run_kwargs.get("statement_timeout_ms", 1_000),
            task_chunks=run_kwargs.get("task_chunks", 10),
        )
        with patch.object(
            tasks_module.backfill_erc20_balance_chunk_task, "apply_async"
        ):
            run = dispatch_erc20_balance_backfill_run(run)
        return run


class TestWatchdogRedispatchesAStalledChain(Erc20BalanceBackfillWatchdogTestCase):
    def test_stalled_chain_is_redispatched_and_runs_to_completion(self):
        safe_a, safe_b = self.safe(), self.safe()
        token = self.token()
        block = self.block()
        self.transfer(block, token, 100, to=safe_a.address)
        self.transfer(block, token, 250, to=safe_b.address)
        self.advance_head()

        self.fabricate_stalled_run()
        self.advance_clock(timedelta(seconds=ERC20_BALANCE_BACKFILL_STALE_SECONDS + 1))

        self.assertIsNotNone(erc20_balance_backfill_looks_stalled())

        # The watchdog's own dispatch is a REAL (unmocked) apply_async,
        # so under CELERY_ALWAYS_EAGER this runs the whole rest of the
        # chain to completion inside this call.
        erc20_balance_backfill_watchdog_task()

        self.assertIsNone(erc20_balance_backfill_looks_stalled())
        self.assertTrue(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )
        head = self.head_watermark()
        expected = erc20_balances_upto_block(
            [HexBytes(safe_a.address), HexBytes(safe_b.address)], head
        )
        self.assertEqual(
            self.balance(safe_a.address, token),
            expected[(HexBytes(safe_a.address), HexBytes(token))],
        )
        run = load_erc20_balance_backfill_run(latest_erc20_balance_backfill_run_id())
        self.assertEqual(run["state"], "finished")


class TestWatchdogLeavesFreshHeartbeatAlone(Erc20BalanceBackfillWatchdogTestCase):
    def test_no_redispatch_while_the_heartbeat_is_still_fresh(self):
        self.safe()
        self.advance_head()

        run = self.fabricate_stalled_run()
        # No `advance_clock`: heartbeat is "now".
        self.assertIsNone(erc20_balance_backfill_looks_stalled())

        erc20_balance_backfill_watchdog_task()

        # Untouched: still the same manifest, still "running", no chunk
        # work happened.
        unchanged = load_erc20_balance_backfill_run(run["run_id"])
        self.assertEqual(unchanged["state"], "running")
        self.assertEqual(unchanged["chunks_done"], 0)
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )


class TestWatchdogLeavesActiveLockAlone(Erc20BalanceBackfillWatchdogTestCase):
    def test_no_redispatch_while_the_rollup_lock_is_held(self):
        self.safe()
        self.advance_head()

        run = self.fabricate_stalled_run()
        self.advance_clock(timedelta(seconds=ERC20_BALANCE_BACKFILL_STALE_SECONDS + 1))
        self.assertIsNotNone(erc20_balance_backfill_looks_stalled())

        lock_name = get_task_lock_name(compute_erc20_balance_rollup_task.name)
        redis = get_redis()
        with redis.lock(lock_name, blocking=False):
            erc20_balance_backfill_watchdog_task()

        # The lock being held means genuine work might be in flight --
        # the watchdog must back off, not redispatch on top of it.
        unchanged = load_erc20_balance_backfill_run(run["run_id"])
        self.assertEqual(unchanged["state"], "running")
        self.assertEqual(unchanged["chunks_done"], 0)
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )


class TestWatchdogNoOpWithNothingInProgress(Erc20BalanceBackfillWatchdogTestCase):
    def test_no_progress_rows_means_nothing_to_check(self):
        self.assertIsNone(erc20_balance_backfill_looks_stalled())
        # Must not raise even with an empty Redis / DB state.
        erc20_balance_backfill_watchdog_task()
        self.assertFalse(AnalyticsWatermark.objects.exists())

    def test_a_finished_run_is_never_flagged(self):
        safe = self.safe()
        token = self.token()
        self.transfer(self.block(), token, 10, to=safe.address)
        self.advance_head()

        self.run_backfill(celery=True, chunk_size=10, task_chunks=10)
        self.assertIsNone(erc20_balance_backfill_looks_stalled())


# ═══════════════════ P7 addendum: candidates query rewrite ═════════════
#
# `_ERC20_BALANCE_SAFE_CANDIDATES_SQL` (analytics/tasks.py, read through
# `erc20_balance_seed_candidates`) was rewritten from a row-constructor
# comparison, `(created, address) > (x, y)`, to the equivalent
# OR-decomposed keyset form -- see that constant's comment for why. These
# tests prove the rewrite preserves marker-exclusive / boundary-inclusive
# semantics and (created, address) ordering, including the tie-break case
# a row-constructor test could gloss over (two Safes sharing one
# `created` instant).


class TestSeedCandidatesKeysetSemantics(Erc20BalanceBackfillTestCase):
    def test_marker_exclusive_boundary_inclusive_with_a_tie_on_created(self):
        tie_time = timezone.now()
        safe_1 = self.safe(created=tie_time)
        safe_2 = self.safe(created=tie_time)
        low_addr, high_addr = sorted([safe_1.address, safe_2.address])
        first = safe_1 if safe_1.address == low_addr else safe_2
        second = safe_2 if first is safe_1 else safe_1
        later = self.safe(created=tie_time + timedelta(seconds=1))

        wide_boundary = (tie_time + timedelta(seconds=1), b"\xff" * 20)

        # From the very start: both tied Safes in address order, then
        # `later`.
        candidates = erc20_balance_seed_candidates(None, wide_boundary, 10)
        self.assertEqual(
            [HexBytes(addr) for addr, _created in candidates],
            [
                HexBytes(first.address),
                HexBytes(second.address),
                HexBytes(later.address),
            ],
        )

        # Marker = `first`'s own tuple: exclusive, so `first` never
        # reappears, but its tie-mate `second` (same `created`, greater
        # `address`) must.
        marker = (tie_time, HexBytes(first.address))
        candidates = erc20_balance_seed_candidates(marker, wide_boundary, 10)
        self.assertEqual(
            [HexBytes(addr) for addr, _created in candidates],
            [HexBytes(second.address), HexBytes(later.address)],
        )

        # Boundary = `second`'s own tuple: inclusive, so `second` is the
        # last one returned; `later` (created strictly after the
        # boundary) is excluded even though it is well inside the
        # overall time window covered by `wide_boundary`.
        tight_boundary = (tie_time, HexBytes(second.address))
        candidates = erc20_balance_seed_candidates(None, tight_boundary, 10)
        self.assertEqual(
            [HexBytes(addr) for addr, _created in candidates],
            [HexBytes(first.address), HexBytes(second.address)],
        )

    def test_full_paginated_walk_returns_every_safe_exactly_once_in_order(self):
        safes = [self.safe() for _ in range(9)]
        boundary = (timezone.now() + timedelta(days=1), b"\xff" * 20)

        seen: list[HexBytes] = []
        marker = None
        while True:
            page = erc20_balance_seed_candidates(marker, boundary, 2)
            if not page:
                break
            seen.extend(HexBytes(addr) for addr, _created in page)
            last_address, last_created = page[-1]
            marker = (last_created, HexBytes(last_address))

        self.assertEqual(len(seen), len(safes))
        self.assertEqual(len(set(seen)), len(safes))
        self.assertCountEqual(seen, [HexBytes(s.address) for s in safes])

        # Ground truth ordering from the DB itself (not a Python-side
        # sort of a checksummed address string, which would not
        # necessarily agree with the underlying bytea comparison the SQL
        # rewrite relies on).
        expected_order = list(
            SafeContract.objects.filter(address__in=[s.address for s in safes])
            .order_by("created", "address")
            .values_list("address", flat=True)
        )
        self.assertEqual(seen, [HexBytes(a) for a in expected_order])
