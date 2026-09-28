"""Tests for the analytics bootstrap (`analytics/bootstrap/`): the stage
interface, the ordered registry (wired to the real `DailyStage` /
`NativeStage` / `Erc20Stage`), the tick algorithm (all 7 steps, including
the retry cooldown/give-up cap), the derived "is everything done" check
and per-stage bookkeeping (`bootstrap/bookkeeping.py`), and the ERC-20
stage built from the watchdog's former auto-start logic.

The registry wires the real `DailyStage`/`NativeStage`, so this file's
ERC-20-focused tests pass their own `stages=[_SpyStage("daily",
done=True), _SpyStage("native", done=True), <real ERC-20 stage>]` to
`run_tick()` instead of relying on the registry, so they exercise
`Erc20Stage` alone, decoupled from `DailyStage`/`NativeStage`'s own real
`is_done()` (covered by their own test modules).

Scaffolding mirrors `Erc20BalanceBackfillTestCase` /
`Erc20BalanceBackfillWatchdogTestCase` in `test_backfill_erc20_balances.py`
(fake clock, `block()`/`safe()`/`token()`/`transfer()`/`advance_head()`/
`fabricate_stalled_run()`) -- duplicated rather than imported, same
reason that file's own docstring gives: no precedent in this suite for
sharing a `TestCase` base across files.
"""

from datetime import timedelta
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase, override_settings
from django.utils import timezone

from eth_account import Account
from hexbytes import HexBytes

from safe_transaction_service.analytics import tasks as tasks_module
from safe_transaction_service.analytics.bootstrap import (
    Stage,
    bootstrap_complete,
    build_default_stages,
    run_tick,
)
from safe_transaction_service.analytics.bootstrap.bookkeeping import (
    get_stage_row,
    mark_completed,
    mark_gave_up,
    record_dispatch,
    record_failure,
    reset_stage,
)
from safe_transaction_service.analytics.bootstrap.daily import DailyStage
from safe_transaction_service.analytics.bootstrap.erc20 import Erc20Stage
from safe_transaction_service.analytics.bootstrap.gate import indexer_caught_up
from safe_transaction_service.analytics.bootstrap.native import NativeStage
from safe_transaction_service.analytics.bootstrap.tick import (
    BOOTSTRAP_RETRY_CAP,
    BOOTSTRAP_RETRY_COOLDOWN,
)
from safe_transaction_service.analytics.catchup.day import DayResult, DayStatus
from safe_transaction_service.analytics.management.commands import (
    backfill_erc20_balances as backfill_module,
)
from safe_transaction_service.analytics.management.commands.backfill_erc20_balances import (
    Command as Erc20BackfillCommand,
)
from safe_transaction_service.analytics.management.commands.backfill_native_balances import (
    Command as NativeBackfillCommand,
)
from safe_transaction_service.analytics.models import AnalyticsWatermark
from safe_transaction_service.analytics.tasks import (
    ERC20_BALANCE_BACKFILL_CURSOR_KEY,
    ERC20_BALANCE_BACKFILL_RUN_KEY_PREFIX,
    ERC20_BALANCE_BACKFILL_STALE_SECONDS,
    ERC20_BALANCE_WATERMARK,
    build_erc20_balance_backfill_run,
    compute_erc20_balance_rollup_task,
    dispatch_erc20_balance_backfill_run,
    erc20_balance_backfill_run_key,
    erc20_balances_upto_block,
    latest_erc20_balance_backfill_run_id,
    load_erc20_balance_backfill_run,
)
from safe_transaction_service.history.models import IndexingStatus, IndexingStatusType
from safe_transaction_service.history.tests.factories import (
    ERC20TransferFactory,
    EthereumBlockFactory,
    EthereumTxFactory,
    SafeContractFactory,
)
from safe_transaction_service.utils.redis import get_redis
from safe_transaction_service.utils.tasks import get_task_lock_name

from .catchup_gate_fixture import SettledGateMixin

# Well clear of every other module's own BASE_BLOCK (see that module's
# comment for the pattern).
BASE_BLOCK = 6_500_000


class AnalyticsBootstrapTestCase(TestCase):
    """Same fake-clock shape `Erc20BalanceBackfillTestCase` uses."""

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

        # Redis is not rolled back between tests the way the database is,
        # so a run manifest (and the cursor pointing at it) would outlive
        # the rows it describes -- same isolation `NativeBootstrapTestCase`
        # gives itself.
        redis = get_redis()
        keys = list(redis.scan_iter(match=f"{ERC20_BALANCE_BACKFILL_RUN_KEY_PREFIX}*"))
        keys.append(ERC20_BALANCE_BACKFILL_CURSOR_KEY)
        redis.delete(*keys)

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

    def safe(self, block=None):
        return SafeContractFactory(
            ethereum_tx=EthereumTxFactory(block=block or self.block())
        )

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

    def fabricate_stalled_run(self, **run_kwargs):
        """Same fabrication `Erc20BalanceBackfillWatchdogTestCase` uses:
        progress rows + a `state="running"` manifest whose heartbeat the
        caller can then age past the threshold via `advance_clock`."""
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


# ═══════════════════════ Tick algorithm (steps 1-5, 7) ═════════════════


class _SpyStage(Stage):
    """A stage the tick can be handed directly, for tests that don't want
    the real ERC-20/daily/native stage in the loop -- an always-done
    stand-in wherever a test only needs an earlier stage out of the way,
    plus a spy for whichever stage the test actually exercises."""

    def __init__(
        self, name, done=True, status="done", raises=None, resumed_externally=False
    ):
        self.name = name
        self._done = done
        self._status = status
        self._raises = raises
        self.resumed_externally = resumed_externally
        self.start_or_resume_called = False
        #: A terminal bookkeeping row must make the tick skip this stage
        #: WITHOUT ever calling `is_done()` again -- a test can assert
        #: this stayed 0.
        self.is_done_called = 0

    def is_done(self):
        self.is_done_called += 1
        if self._raises == "is_done":
            raise RuntimeError("boom from is_done")
        return self._done

    def status(self):
        if self._raises == "status":
            raise RuntimeError("boom from status")
        return self._status

    def start_or_resume(self):
        if self._raises == "start_or_resume":
            raise RuntimeError("boom from start_or_resume")
        self.start_or_resume_called = True


def _mark_fully_complete(daily_depth: int) -> None:
    """Simulate a bootstrap that has already finished every stage at
    ``daily_depth``. Writes all three `AnalyticsBootstrapStage` rows
    directly, the way three real ticks would have left them, without
    running the tick itself."""
    mark_completed("daily", depth=daily_depth)
    mark_completed("native")
    mark_completed("erc20")


def _erc20_only_stages(erc20_stage):
    """`[daily-done, native-done, erc20_stage]` -- what every ERC-20-stage
    test below hands `run_tick(stages=...)` so it exercises `Erc20Stage`
    alone, with the earlier two stages out of the way from the first
    tick (see the module docstring)."""
    return [_SpyStage("daily"), _SpyStage("native"), erc20_stage]


class TestOrderEnforcement(SettledGateMixin, AnalyticsBootstrapTestCase):
    def test_stage_n_plus_1_is_never_started_while_stage_n_is_incomplete(self):
        daily = _SpyStage("daily", done=False, status="pending")
        native = _SpyStage("native", done=True)
        erc20 = _SpyStage("erc20", done=True)

        run_tick(stages=[daily, native, erc20])

        self.assertTrue(daily.start_or_resume_called)
        self.assertFalse(native.start_or_resume_called)
        self.assertFalse(erc20.start_or_resume_called)

    def test_patching_a_done_stage_to_not_done_proves_the_same_thing(self):
        daily = _SpyStage("daily")
        native = _SpyStage("native")
        erc20 = _SpyStage("erc20", done=True)

        with patch.object(native, "is_done", return_value=False):
            run_tick(stages=[daily, native, erc20])

        self.assertFalse(erc20.start_or_resume_called)


class TestIndexerGateBlocksStart(AnalyticsBootstrapTestCase):
    def test_gate_not_caught_up_blocks_start_or_resume(self):
        """No `SettledGateMixin` here: with no `SafeMasterCopy` rows,
        `get_indexer_status()` itself fails, so the gate reports
        "not caught up" and the current stage is never started."""
        erc20 = _SpyStage("erc20", done=False, status="pending")

        run_tick(stages=[_SpyStage("daily"), _SpyStage("native"), erc20])

        self.assertFalse(erc20.start_or_resume_called)


class TestTickNeverRaises(SettledGateMixin, AnalyticsBootstrapTestCase):
    def test_a_stage_raising_from_is_done_does_not_propagate(self):
        bad = _SpyStage("daily", raises="is_done")
        run_tick(
            stages=[bad, _SpyStage("native"), _SpyStage("erc20")]
        )  # must not raise

    def test_a_stage_raising_from_status_does_not_propagate(self):
        bad = _SpyStage("daily", done=False, raises="status")
        run_tick(
            stages=[bad, _SpyStage("native"), _SpyStage("erc20")]
        )  # must not raise

    def test_a_stage_raising_from_start_or_resume_does_not_propagate(self):
        bad = _SpyStage("daily", done=False, status="pending", raises="start_or_resume")
        run_tick(
            stages=[bad, _SpyStage("native"), _SpyStage("erc20")]
        )  # must not raise


class TestCompleteMarker(SettledGateMixin, AnalyticsBootstrapTestCase):
    def test_complete_derived_once_every_stage_is_done_and_later_ticks_are_noop(self):
        depth = 90
        self.assertFalse(bootstrap_complete(depth))

        # All three stages report done -- the daily/native spies, plus a
        # real `Erc20Stage` whose completion watermark already exists. A
        # manual, already-finished backfill satisfies `Erc20Stage.is_done()`.
        self.safe()
        self.advance_head()
        call_command(
            "backfill_erc20_balances", celery=True, chunk_size=10, task_chunks=10
        )
        self.assertTrue(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )

        erc20 = Erc20Stage()
        run_tick(stages=_erc20_only_stages(erc20))

        # Derived straight from the three stages' own bookkeeping rows --
        # no separate marker row any more.
        self.assertTrue(bootstrap_complete(depth))
        self.assertIsNotNone(get_stage_row("erc20").completed_at)
        self.assertIsNotNone(get_stage_row("daily").completed_at)
        self.assertIsNotNone(get_stage_row("native").completed_at)

        # A later tick is a pure no-op: the ERC-20 stage's `start_or_resume`
        # must never be called again once the bootstrap is complete.
        with patch.object(Erc20Stage, "start_or_resume") as spy:
            run_tick(stages=_erc20_only_stages(Erc20Stage()))
        spy.assert_not_called()


# ═══════════════════════════ ERC-20 stage ═══════════════════════════════


class TestErc20StageFreshStartAndAdopt(SettledGateMixin, AnalyticsBootstrapTestCase):
    def test_fresh_start_on_an_empty_instance_reaches_completion(self):
        safe_a, safe_b = self.safe(), self.safe()
        token = self.token()
        block = self.block()
        self.transfer(block, token, 100, to=safe_a.address)
        self.transfer(block, token, 250, to=safe_b.address)
        self.advance_head()

        self.assertFalse(AnalyticsWatermark.objects.exists())

        # daily/native spies done -> Erc20Stage is current -> fresh start
        run_tick(stages=_erc20_only_stages(Erc20Stage()))

        self.assertTrue(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )
        run = load_erc20_balance_backfill_run(latest_erc20_balance_backfill_run_id())
        self.assertEqual(run["state"], "finished")
        self.assertEqual(run["origin"], "auto")
        head = AnalyticsWatermark.objects.get(name=ERC20_BALANCE_WATERMARK).block_number
        expected = erc20_balances_upto_block(
            [HexBytes(safe_a.address), HexBytes(safe_b.address)], head
        )
        self.assertIn((HexBytes(safe_a.address), HexBytes(token)), expected)

    def test_adopts_progress_rows_with_no_manifest_and_finishes(self):
        safe = self.safe()
        token = self.token()
        self.transfer(self.block(), token, 10, to=safe.address)
        self.advance_head()

        backfill_module.resolve_run(False)
        self.assertIsNone(latest_erc20_balance_backfill_run_id())

        run_tick(stages=_erc20_only_stages(Erc20Stage()))

        run_id = latest_erc20_balance_backfill_run_id()
        self.assertIsNotNone(run_id)
        run = load_erc20_balance_backfill_run(run_id)
        self.assertEqual(run["state"], "finished")
        self.assertEqual(run["origin"], "auto")
        self.assertTrue(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )


class TestErc20StageBlockedByRunningManifest(
    SettledGateMixin, AnalyticsBootstrapTestCase
):
    def test_a_fresh_heartbeat_blocks_the_tick_from_superseding_it(self):
        self.safe()
        self.advance_head()

        run = self.fabricate_stalled_run()
        # No `advance_clock`: heartbeat stays fresh -- `status()` reports
        # `"running"`, so the tick's step 4 no-ops before even reaching
        # the indexer gate.

        run_tick(stages=_erc20_only_stages(Erc20Stage()))

        self.assertEqual(latest_erc20_balance_backfill_run_id(), run["run_id"])
        unchanged = load_erc20_balance_backfill_run(run["run_id"])
        self.assertEqual(unchanged["state"], "running")
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )


class TestStalledRunHasExactlyOneResumer(SettledGateMixin, AnalyticsBootstrapTestCase):
    """The watchdog and the tick must never both
    re-dispatch the same stalled `run_id` -- resuming a stall is the
    watchdog's job alone (`Erc20Stage.start_or_resume()` no longer has a
    stalled branch; the tick's step 4 treats `"stalled"` like
    `"running"`, a no-op)."""

    def test_one_watchdog_run_plus_one_tick_dispatches_exactly_once(self):
        safe = self.safe()
        token = self.token()
        self.transfer(self.block(), token, 10, to=safe.address)
        self.advance_head()

        run = self.fabricate_stalled_run()
        self.advance_clock(timedelta(seconds=ERC20_BALANCE_BACKFILL_STALE_SECONDS + 1))
        self.assertIsNotNone(tasks_module.erc20_balance_backfill_looks_stalled())

        with patch.object(
            tasks_module, "_erc20_backfill_dispatch_phase"
        ) as dispatch_spy:
            tasks_module.erc20_balance_backfill_watchdog_task()
            run_tick(stages=_erc20_only_stages(Erc20Stage()))

        self.assertEqual(dispatch_spy.call_count, 1)
        (dispatched_run,), _ = dispatch_spy.call_args
        self.assertEqual(dispatched_run["run_id"], run["run_id"])


class TestErc20StageLockHeldBlocksStart(SettledGateMixin, AnalyticsBootstrapTestCase):
    def test_lock_held_blocks_a_fresh_start(self):
        self.safe()
        self.advance_head()

        lock_name = get_task_lock_name(compute_erc20_balance_rollup_task.name)
        redis = get_redis()
        with redis.lock(lock_name, blocking=False):
            run_tick(stages=_erc20_only_stages(Erc20Stage()))

        self.assertIsNone(latest_erc20_balance_backfill_run_id())
        self.assertFalse(AnalyticsWatermark.objects.exists())

    def test_lock_held_blocks_adopting_an_orphaned_run(self):
        self.safe()
        self.advance_head()
        backfill_module.resolve_run(False)

        lock_name = get_task_lock_name(compute_erc20_balance_rollup_task.name)
        redis = get_redis()
        with redis.lock(lock_name, blocking=False):
            run_tick(stages=_erc20_only_stages(Erc20Stage()))

        self.assertIsNone(latest_erc20_balance_backfill_run_id())
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )


class TestZombieChainExitsWithoutWork(SettledGateMixin, AnalyticsBootstrapTestCase):
    def test_a_superseded_run_ids_task_is_a_noop_once_its_manifest_is_gone(self):
        """At most one live chain: once the bootstrap tick adopts a fresh
        `run_id`, a task still carrying the OLD `run_id` must find
        nothing and do nothing -- never double-apply."""
        safe = self.safe()
        token = self.token()
        self.transfer(self.block(), token, 10, to=safe.address)
        self.advance_head()

        old_run = self.fabricate_stalled_run()
        old_run_id = old_run["run_id"]
        get_redis().delete(erc20_balance_backfill_run_key(old_run_id))

        run_tick(stages=_erc20_only_stages(Erc20Stage()))  # adopts a NEW run_id

        new_run_id = latest_erc20_balance_backfill_run_id()
        self.assertNotEqual(new_run_id, old_run_id)
        self.assertTrue(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )

        result = tasks_module.backfill_erc20_balance_chunk_task(
            old_run_id, old_run["dispatch_seq"]
        )
        self.assertEqual(result, {"run_id": old_run_id, "state": "unknown"})


class TestKillSwitch(SettledGateMixin, AnalyticsBootstrapTestCase):
    def test_kill_switch_off_starts_nothing_on_an_empty_instance(self):
        self.safe()
        self.advance_head()

        with override_settings(ANALYTICS_AUTO_BACKFILL=False):
            run_tick()

        self.assertIsNone(latest_erc20_balance_backfill_run_id())
        self.assertFalse(AnalyticsWatermark.objects.exists())
        self.assertFalse(bootstrap_complete(90))

    def test_kill_switch_off_does_not_adopt_orphaned_progress(self):
        self.safe()
        self.advance_head()
        backfill_module.resolve_run(False)

        with override_settings(ANALYTICS_AUTO_BACKFILL=False):
            run_tick()

        self.assertIsNone(latest_erc20_balance_backfill_run_id())
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )

    def test_analytics_disabled_also_starts_nothing(self):
        self.safe()
        self.advance_head()

        with override_settings(ENABLE_ANALYTICS=False):
            run_tick()

        self.assertIsNone(latest_erc20_balance_backfill_run_id())


class TestDefaultRegistry(AnalyticsBootstrapTestCase):
    def test_build_default_stages_is_daily_native_erc20_in_order(self):
        """`registry.py` wires the real `DailyStage`/`NativeStage`
        alongside the ERC-20 stage, all built for real."""
        stages = build_default_stages()
        self.assertEqual([stage.name for stage in stages], ["daily", "native", "erc20"])
        self.assertIsInstance(stages[0], DailyStage)
        self.assertIsInstance(stages[1], NativeStage)
        self.assertIsInstance(stages[2], Erc20Stage)


# ═══════════════════ Depth growth reopens only daily (step 2) ══════════


class TestDepthGrowthReopensOnlyDaily(SettledGateMixin, AnalyticsBootstrapTestCase):
    """ "Only daily is a candidate after depth growth" is not a separate
    mechanism -- it falls out of the terminal-row-first loop for free.
    `maybe_reopen_daily` clears ONLY daily's row; native and ERC-20 keep
    their own `completed_at`, so the loop skips them as terminal and
    picks daily first."""

    def test_only_daily_is_a_candidate_when_completion_predates_a_larger_depth(self):
        _mark_fully_complete(90)

        daily = _SpyStage("daily", done=False, status="pending")
        native = _SpyStage("native", done=True)
        erc20 = _SpyStage("erc20", done=False, status="pending")

        with override_settings(ANALYTICS_BOOTSTRAP_DAILY_DAYS=730):
            run_tick(stages=[daily, native, erc20])

        self.assertTrue(daily.start_or_resume_called)
        self.assertFalse(erc20.start_or_resume_called)

    def test_daily_done_again_updates_its_row_with_the_new_depth(self):
        _mark_fully_complete(90)

        daily = _SpyStage("daily", done=True)
        native = _SpyStage("native", done=False)  # would matter if ever evaluated
        erc20 = _SpyStage("erc20", done=False)

        with override_settings(ANALYTICS_BOOTSTRAP_DAILY_DAYS=730):
            run_tick(stages=[daily, native, erc20])

        self.assertEqual(get_stage_row("daily").completed_depth, 730)
        self.assertFalse(native.start_or_resume_called)
        self.assertFalse(erc20.start_or_resume_called)


# ═════ A stage completed long ago never redispatches mid-later-stage ════


class TestCompletedStageNeverRedispatchesFromLiveState(
    SettledGateMixin, AnalyticsBootstrapTestCase
):
    """The daily window slides, so a day younger than it can enter the
    window (a new UTC "yesterday") and make `DailyStage.is_done()` go
    back to `False` for a stage the bootstrap already finished.
    Re-deriving "done" from `is_done()` on every tick regardless of the
    row would dispatch a fresh 1-day daily backfill while a LATER stage
    was mid-run -- two backfills running at once. Instead the row
    (`completed_at` set) is checked FIRST and wins: daily is skipped
    without ever calling `is_done()` again."""

    def test_daily_marked_complete_is_skipped_even_though_is_done_would_say_false(
        self,
    ):
        mark_completed("daily", depth=90)

        daily_would_say_not_done = _SpyStage("daily", done=False, status="pending")
        native = _SpyStage("native", done=False, status="pending")  # mid-run

        run_tick(stages=[daily_would_say_not_done, native])

        self.assertFalse(daily_would_say_not_done.start_or_resume_called)
        # The real proof: the terminal row short-circuits BEFORE ever
        # asking the stage whether it's done -- not merely "not started"
        # for some other, coincidental reason.
        self.assertEqual(daily_would_say_not_done.is_done_called, 0)
        self.assertTrue(native.start_or_resume_called)

    def test_a_real_daily_stage_with_a_new_missing_day_is_not_redispatched(self):
        """Same proof with the REAL `DailyStage`, not a spy: `is_done()`
        against the live `DailyMetric` table would report `False` (a new
        day is missing), yet the tick must still leave it alone because
        its row already says complete."""
        mark_completed("daily", depth=90)
        self.assertFalse(DailyStage().is_done())  # the live check disagrees

        native = _SpyStage("native", done=False, status="pending")
        with patch(
            "safe_transaction_service.analytics.bootstrap.daily.start_backfill_run"
        ) as dispatch_spy:
            run_tick(stages=[DailyStage(), native])

        dispatch_spy.assert_not_called()
        self.assertTrue(native.start_or_resume_called)


# ═══ Manual ERC-20 restart: left alone in flight, adopted once orphaned ═══


class TestManualRestartInFlightVsOrphaned(SettledGateMixin, AnalyticsBootstrapTestCase):
    """A manual `--restart` does not stop the bootstrap from coming back:
    `reset_stage(..., data_wiped=True)` makes the row non-terminal again,
    and the tick may help finish it. Safety comes from the SAME guards a
    fresh/adopted run always had -- the lock, the running manifest/lease,
    and `dispatch_seq` -- never from the bootstrap refusing to look."""

    def test_a_restart_with_a_running_manifest_is_left_alone(self):
        mark_completed("erc20")
        reset_stage("erc20", data_wiped=True)
        self.assertIsNone(get_stage_row("erc20").completed_at)

        self.safe()
        self.advance_head()
        run = self.fabricate_stalled_run()  # fresh heartbeat -> "running"

        run_tick(stages=_erc20_only_stages(Erc20Stage()))

        self.assertEqual(latest_erc20_balance_backfill_run_id(), run["run_id"])
        unchanged = load_erc20_balance_backfill_run(run["run_id"])
        self.assertEqual(unchanged["state"], "running")

    def test_a_restart_with_the_rollup_lock_held_is_left_alone(self):
        mark_completed("erc20")
        reset_stage("erc20", data_wiped=True)

        self.safe()
        self.advance_head()
        lock_name = get_task_lock_name(compute_erc20_balance_rollup_task.name)
        with get_redis().lock(lock_name, blocking=False):
            run_tick(stages=_erc20_only_stages(Erc20Stage()))

        self.assertIsNone(latest_erc20_balance_backfill_run_id())

    def test_once_orphaned_the_tick_adopts_the_restarted_run(self):
        mark_completed("erc20")
        reset_stage("erc20", data_wiped=True)

        safe = self.safe()
        token = self.token()
        self.transfer(self.block(), token, 10, to=safe.address)
        self.advance_head()

        # The manual run's Celery manifest is gone (worker restart, TTL
        # expiry, ...) but its progress rows survive -- an orphaned run,
        # same shape `resolve_run(False)` fabricates elsewhere in this
        # file.
        backfill_module.resolve_run(False)
        self.assertIsNone(latest_erc20_balance_backfill_run_id())

        run_tick(stages=_erc20_only_stages(Erc20Stage()))

        run_id = latest_erc20_balance_backfill_run_id()
        self.assertIsNotNone(run_id)
        run = load_erc20_balance_backfill_run(run_id)
        self.assertEqual(run["state"], "finished")
        self.assertTrue(
            AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists()
        )


# ═══════════════════ Indexer gate: unexpected exceptions (finding 3) ════


class TestGateHandlesUnexpectedExceptions(AnalyticsBootstrapTestCase):
    def test_generic_exception_is_treated_as_not_caught_up_and_logged_once(self):
        with (
            patch(
                "safe_transaction_service.analytics.bootstrap.gate.get_indexer_status",
                side_effect=RuntimeError("rpc down"),
            ),
            patch(
                "safe_transaction_service.analytics.bootstrap.gate.logger"
            ) as logger_spy,
        ):
            self.assertFalse(indexer_caught_up())
            # Same failure again -- the state hasn't changed, so no repeat
            # WARNING, but the debug/exc_info line fires every time.
            self.assertFalse(indexer_caught_up())

        self.assertEqual(logger_spy.warning.call_count, 1)
        warning_args = logger_spy.warning.call_args[0]
        self.assertIn("status_unavailable", warning_args)
        self.assertEqual(logger_spy.debug.call_count, 2)
        for call in logger_spy.debug.call_args_list:
            self.assertTrue(call.kwargs.get("exc_info"))

    def test_tick_does_not_raise_or_spam_when_the_gate_blows_up(self):
        erc20 = _SpyStage("erc20", done=False, status="pending")
        with patch(
            "safe_transaction_service.analytics.bootstrap.gate.get_indexer_status",
            side_effect=RuntimeError("rpc down"),
        ):
            run_tick(stages=[_SpyStage("daily"), _SpyStage("native"), erc20])

        self.assertFalse(erc20.start_or_resume_called)


# ═════════════ Duplicate chain after a spurious resume ══════════════════


class TestDuplicateChainAfterASpuriousResume(AnalyticsBootstrapTestCase):
    """The watchdog's grace period only covers the ordinary hand-off gap, and on
    a busy `contracts` queue an originally queued slice can still be
    sitting in the queue when the watchdog decides to resume. Without a
    dispatch-generation guard, that slice would eventually run too,
    continuing a second, parallel chain forever (the shared rollup lock
    only serialises the two, it does not stop either from existing).
    `dispatch_seq` is the fix: a slice whose own argument no longer
    matches the manifest's current value exits without work and without
    dispatching a successor."""

    def test_the_stale_dispatch_seq_slice_does_nothing_and_dispatches_nothing(self):
        self.safe()
        self.advance_head()

        run = self.fabricate_stalled_run()
        stale_seq = run["dispatch_seq"]
        self.assertEqual(stale_seq, 1)

        # Simulate a resume that already happened -- the manifest's
        # generation moved to 2 (the watchdog's own
        # `bump_erc20_balance_dispatch_seq`), but the run is still
        # `"running"`, exactly the window where the ORIGINALLY queued
        # slice -- still carrying seq 1 -- might finally be picked up by
        # a worker.
        tasks_module.bump_erc20_balance_dispatch_seq(run)
        before = load_erc20_balance_backfill_run(run["run_id"])
        self.assertEqual(before["dispatch_seq"], 2)
        self.assertEqual(before["state"], "running")

        with patch.object(
            tasks_module.backfill_erc20_balance_chunk_task, "apply_async"
        ) as spy:
            result = tasks_module.backfill_erc20_balance_chunk_task(
                run["run_id"], stale_seq
            )

        spy.assert_not_called()
        self.assertEqual(result, before)
        self.assertEqual(load_erc20_balance_backfill_run(run["run_id"]), before)


# ═══════════════════ ERC-20 stage reports "failed" ═══════════════════════


class TestErc20StatusReportsFailed(SettledGateMixin, AnalyticsBootstrapTestCase):
    """A manifest with `state="failed"` reports `"failed"` from
    `Erc20Stage.status()`, rather than falling through to
    `"pending"`/adopt, so the retry cooldown/cap (`tick.py` step 6) has
    something to key off."""

    def test_a_failed_manifest_reports_failed_not_pending(self):
        self.safe()
        self.advance_head()

        run = self.fabricate_stalled_run()
        run["state"] = "failed"
        tasks_module._save_erc20_balance_backfill_run(run)

        self.assertEqual(Erc20Stage().status(), "failed")

    def test_start_or_resume_still_starts_a_new_run_over_a_failed_one(self):
        """`start_or_resume()` only ever checks the current manifest's
        state for `"running"` (never `"failed"`) before deciding to
        dispatch -- a failed run is treated as adoptable/startable
        exactly like a lost or never-started one, whether
        `erc20_balance_backfill_auto_start_target()` calls that "adopt"
        or "fresh"."""
        safe = self.safe()
        token = self.token()
        self.transfer(self.block(), token, 10, to=safe.address)
        self.advance_head()

        run = self.fabricate_stalled_run()
        run["state"] = "failed"
        tasks_module._save_erc20_balance_backfill_run(run)

        Erc20Stage().start_or_resume()

        new_run_id = latest_erc20_balance_backfill_run_id()
        self.assertNotEqual(new_run_id, run["run_id"])
        new_run = load_erc20_balance_backfill_run(new_run_id)
        self.assertEqual(new_run["state"], "finished")


# ═══════════════════ Retry cooldown / give-up cap (tick step 6) ═════════


class _TickClockTestCase(SettledGateMixin, TestCase):
    """A fake clock for the tick's OWN retry-cooldown math.

    `AnalyticsBootstrapTestCase`'s fake clock (this module, near the top)
    only patches `timezone.now` for `tasks.py` and
    `backfill_erc20_balances.py` -- it has nothing to do with the tick's
    cooldown arithmetic, which lives in `bootstrap.tick` and
    `bootstrap.bookkeeping` (both call `timezone.now()` directly when
    stamping `last_failure_at`/`last_dispatch_at` and when comparing
    against `BOOTSTRAP_RETRY_COOLDOWN`). Both need the SAME fake clock,
    or the cooldown math is comparing a fake stamp against a real one.
    """

    def setUp(self):
        super().setUp()
        self.clock = timezone.now()
        for target in (
            "safe_transaction_service.analytics.bootstrap.tick.timezone.now",
            "safe_transaction_service.analytics.bootstrap.bookkeeping.timezone.now",
        ):
            patcher = patch(target, side_effect=lambda: self.clock)
            patcher.start()
            self.addCleanup(patcher.stop)

    def advance_clock(self, delta: timedelta) -> None:
        self.clock += delta

    @staticmethod
    def _stages(daily_status="failed"):
        return [
            _SpyStage("daily", done=False, status=daily_status),
            _SpyStage("native"),
            _SpyStage("erc20"),
        ]


class TestFirstFailureWaitsForCooldown(_TickClockTestCase):
    def test_first_failure_is_counted_and_not_dispatched_before_the_cooldown(self):
        stages = self._stages()
        run_tick(stages=stages)

        self.assertFalse(stages[0].start_or_resume_called)
        row = get_stage_row("daily")
        self.assertEqual(row.consecutive_failures, 1)
        self.assertIsNotNone(row.last_failure_at)
        self.assertIsNone(row.gave_up_at)

    def test_dispatched_once_the_cooldown_has_elapsed(self):
        run_tick(stages=self._stages())  # failure #1, cooldown starts

        self.advance_clock(BOOTSTRAP_RETRY_COOLDOWN + timedelta(seconds=1))
        stages = self._stages()
        run_tick(stages=stages)

        self.assertTrue(stages[0].start_or_resume_called)
        # Nothing NEW has failed yet -- this dispatch's own outcome isn't
        # known until the next tick observes it.
        self.assertEqual(get_stage_row("daily").consecutive_failures, 1)


class TestStallNeverCounts(_TickClockTestCase):
    def test_a_stalled_status_does_not_increment_consecutive_failures(self):
        stages = self._stages(daily_status="stalled")
        run_tick(stages=stages)

        row = get_stage_row("daily")
        self.assertEqual(row.consecutive_failures, 0)
        self.assertIsNone(row.last_failure_at)
        # `resumed_externally=False` (the default), so a stall falls
        # through to steps 5-7 like `"pending"` -- `DailyStage`'s own
        # `start_or_resume()` handles redispatching a stalled run itself.
        self.assertTrue(stages[0].start_or_resume_called)


class TestSameFailureNotCountedTwice(_TickClockTestCase):
    def test_the_same_failed_dispatch_is_not_recounted_across_ticks(self):
        run_tick(stages=self._stages())  # failure #1
        self.assertEqual(get_stage_row("daily").consecutive_failures, 1)

        # No new dispatch has happened, so this observes the SAME failure
        # again -- it must not be recounted, only cooled down then
        # dispatched once the cooldown clears.
        self.advance_clock(BOOTSTRAP_RETRY_COOLDOWN + timedelta(seconds=1))
        run_tick(stages=self._stages())
        self.assertEqual(get_stage_row("daily").consecutive_failures, 1)


class TestGiveUpMovesOnToNextStage(_TickClockTestCase):
    def test_three_failures_give_up_with_exactly_one_error_log_then_moves_on(self):
        with patch(
            "safe_transaction_service.analytics.bootstrap.tick.logger"
        ) as logger_spy:
            run_tick(stages=self._stages())  # failure #1
            self.advance_clock(BOOTSTRAP_RETRY_COOLDOWN + timedelta(seconds=1))
            run_tick(stages=self._stages())  # dispatch (still counts 1)
            self.advance_clock(BOOTSTRAP_RETRY_COOLDOWN + timedelta(seconds=1))
            run_tick(stages=self._stages())  # failure #2
            self.advance_clock(BOOTSTRAP_RETRY_COOLDOWN + timedelta(seconds=1))
            run_tick(stages=self._stages())  # dispatch (still counts 2)
            self.advance_clock(BOOTSTRAP_RETRY_COOLDOWN + timedelta(seconds=1))
            run_tick(stages=self._stages())  # failure #3 -> gives up

        row = get_stage_row("daily")
        self.assertEqual(row.consecutive_failures, BOOTSTRAP_RETRY_CAP)
        self.assertIsNotNone(row.gave_up_at)
        self.assertEqual(logger_spy.error.call_count, 1)

        # The next tick moves on: daily is skipped (`gave_up_at` set),
        # native (not done) becomes current instead.
        daily = _SpyStage("daily", done=False, status="failed")
        native = _SpyStage("native", done=False, status="pending")
        erc20 = _SpyStage("erc20", done=True)
        run_tick(stages=[daily, native, erc20])

        self.assertFalse(daily.start_or_resume_called)
        self.assertTrue(native.start_or_resume_called)


# ═══════════════════ Reset paths (manual start / --restart) ═════════════


class TestBookkeepingResetHelpers(TestCase):
    """Direct, tick-free coverage of `bookkeeping.py`'s own contracts."""

    def test_reset_stage_clears_failures_and_gave_up_but_keeps_completed_at(self):
        mark_completed("erc20")
        record_failure("erc20")
        mark_gave_up("erc20")

        reset_stage("erc20")

        row = get_stage_row("erc20")
        self.assertEqual(row.consecutive_failures, 0)
        self.assertIsNone(row.last_failure_at)
        self.assertIsNone(row.gave_up_at)
        self.assertIsNotNone(row.completed_at)  # not wiped -- no data_wiped=True

    def test_reset_stage_with_data_wiped_also_clears_completed_at(self):
        mark_completed("daily", depth=90)
        record_failure("daily")

        reset_stage("daily", data_wiped=True)

        row = get_stage_row("daily")
        self.assertIsNone(row.completed_at)
        self.assertIsNone(row.completed_depth)
        self.assertEqual(row.consecutive_failures, 0)

    def test_record_dispatch_stamps_last_dispatch_at(self):
        self.assertIsNone(get_stage_row("native").last_dispatch_at)
        record_dispatch("native")
        self.assertIsNotNone(get_stage_row("native").last_dispatch_at)

    def test_mark_completed_is_idempotent_on_completed_at(self):
        mark_completed("daily", depth=90)
        first = get_stage_row("daily").completed_at
        mark_completed("daily", depth=90)
        self.assertEqual(get_stage_row("daily").completed_at, first)


class TestManualStartResetsBookkeeping(SettledGateMixin, TestCase):
    """A manual start (or `--restart`) of any of the three backfills
    resets that stage's own bookkeeping row -- exercised here through
    each command's real `handle()`, with only the heavy backfill work
    itself mocked out."""

    def test_backfill_daily_metrics_inline_start_resets_the_daily_row(self):
        mark_gave_up("daily")
        record_failure("daily")
        self.assertIsNotNone(get_stage_row("daily").gave_up_at)

        day = (timezone.now().date() - timedelta(days=2)).isoformat()
        with patch(
            "safe_transaction_service.analytics.management.commands."
            "backfill_daily_metrics.compute_day",
            return_value=DayResult(status=DayStatus.DONE, core_ok=True, failed=()),
        ):
            call_command("backfill_daily_metrics", inline=True, start=day, end=day)

        row = get_stage_row("daily")
        self.assertIsNone(row.gave_up_at)
        self.assertEqual(row.consecutive_failures, 0)

    def test_backfill_native_balances_start_resets_the_native_row(self):
        mark_gave_up("native")
        record_failure("native")
        self.assertIsNotNone(get_stage_row("native").gave_up_at)

        with (
            patch.object(NativeBackfillCommand, "_resolve_head", return_value=100),
            patch.object(NativeBackfillCommand, "_run_inline"),
        ):
            call_command("backfill_native_balances")

        row = get_stage_row("native")
        self.assertIsNone(row.gave_up_at)
        self.assertEqual(row.consecutive_failures, 0)

    def test_backfill_native_balances_restart_also_clears_completed_at(self):
        mark_completed("native")
        self.assertIsNotNone(get_stage_row("native").completed_at)

        with (
            patch.object(NativeBackfillCommand, "_restart"),
            patch.object(NativeBackfillCommand, "_resolve_head", return_value=100),
            patch.object(NativeBackfillCommand, "_run_inline"),
        ):
            call_command("backfill_native_balances", restart=True)

        self.assertIsNone(get_stage_row("native").completed_at)

    def test_backfill_erc20_balances_inline_start_resets_the_erc20_row(self):
        mark_gave_up("erc20")
        record_failure("erc20")
        self.assertIsNotNone(get_stage_row("erc20").gave_up_at)

        with patch.object(Erc20BackfillCommand, "_run"):
            call_command("backfill_erc20_balances")

        row = get_stage_row("erc20")
        self.assertIsNone(row.gave_up_at)
        self.assertEqual(row.consecutive_failures, 0)

    def test_backfill_erc20_balances_restart_also_clears_completed_at(self):
        mark_completed("erc20")
        self.assertIsNotNone(get_stage_row("erc20").completed_at)

        with patch.object(Erc20BackfillCommand, "_run"):
            call_command("backfill_erc20_balances", restart=True)

        self.assertIsNone(get_stage_row("erc20").completed_at)


# ═══════════════════════════ analytics_bootstrap --status ═══════════════


class TestAnalyticsBootstrapStatusCommand(TestCase):
    def test_status_runs_and_prints_the_key_lines(self):
        out = StringIO()
        call_command("analytics_bootstrap", stdout=out)
        output = out.getvalue()

        self.assertIn("ENABLE_ANALYTICS", output)
        self.assertIn("ANALYTICS_AUTO_BACKFILL", output)
        self.assertIn("configured daily depth", output)
        self.assertIn("bootstrap complete", output)
        self.assertIn("indexer gate", output)
        self.assertIn("[daily] status=", output)
        self.assertIn("[native] status=", output)
        self.assertIn("[erc20] status=", output)
        self.assertIn("next tick would act on", output)

    def test_status_flag_accepted_too(self):
        out = StringIO()
        call_command("analytics_bootstrap", "--status", stdout=out)
        self.assertIn("next tick would act on", out.getvalue())

    def test_status_line_comes_from_the_shared_report_not_live_status(self):
        """A stage whose row already reports done (`mark_completed`) must
        print `status=done` even when its own live `status()` is patched
        to disagree -- proof `_print_stage` reads from
        `build_bootstrap_report()` (row-first), not `stage.status()`
        directly, the same rule the tick uses to pick the current stage.
        """
        mark_completed("native")
        with patch.object(NativeStage, "status", return_value="pending"):
            out = StringIO()
            call_command("analytics_bootstrap", stdout=out)
        self.assertIn("[native] status=done", out.getvalue())

    def test_last_recorded_indexer_verdict_line_is_printed(self):
        out = StringIO()
        call_command("analytics_bootstrap", stdout=out)
        self.assertIn("last recorded verdict", out.getvalue())
