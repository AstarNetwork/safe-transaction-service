"""Tests for the native-balance bootstrap stage
(`analytics/bootstrap/native.py`): fresh start, lost-manifest adoption,
resuming a stalled run, the shared-lock check, and `is_done()`.

Scaffolding mirrors `NativeBalanceRollupTestCase` (`test_native_balance_rollup.py`)
for the Safe/block/transfer helpers and Redis isolation, and
`AnalyticsBootstrapTestCase` (`test_analytics_bootstrap.py`) for the fake
clock -- duplicated rather than imported, same reason both of those files'
own docstrings give: no precedent in this suite for sharing a `TestCase`
base across files.

`NativeStage` is exercised directly (`NativeStage().start_or_resume()`),
not through `run_tick()`: testing the stage's own methods is a faster,
more focused way to exercise it on its own than going through the full
registry and tick.
"""

from datetime import timedelta
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from safe_transaction_service.analytics import tasks as tasks_module
from safe_transaction_service.analytics.bootstrap.native import NativeStage
from safe_transaction_service.analytics.models import (
    AnalyticsWatermark,
    SafeNativeBalance,
)
from safe_transaction_service.analytics.tasks import (
    NATIVE_BALANCE_BACKFILL_STALE_SECONDS,
    NATIVE_BALANCE_WATERMARK,
    _cold_start_native_balance_rollup,
    _native_balance_backfill_in_progress,
    compute_native_balance_rollup_task,
    native_balance_head_block,
    seed_missing_native_balances,
    write_native_balance_watermark,
)
from safe_transaction_service.analytics.tasks_shards import (
    NATIVE_BALANCE_CURSOR_KEY,
    NATIVE_BALANCE_RUN_KEY_PREFIX,
    backfill_native_balance_chunk,
    latest_native_balance_run_id,
    load_native_balance_run,
    next_native_balance_dispatch_seq,
    start_native_balance_backfill_run,
)
from safe_transaction_service.history.models import EthereumTxCallType, SafeContract
from safe_transaction_service.history.tests.factories import (
    EthereumBlockFactory,
    EthereumTxFactory,
    InternalTxFactory,
    SafeContractFactory,
)
from safe_transaction_service.utils.redis import get_redis
from safe_transaction_service.utils.tasks import get_task_lock_name

# Well clear of every other module's own BASE_BLOCK (see that module's
# comment for the pattern -- `test_native_balance_rollup.py` uses 1_000_000,
# `test_analytics_bootstrap.py` uses 6_500_000).
BASE_BLOCK = 8_000_000


class NativeBootstrapTestCase(TestCase):
    """Shared scaffolding: explicit blocks/confirmation (`test_native_balance_rollup.py`'s
    shape) plus a fake clock (`test_analytics_bootstrap.py`'s shape), since
    resuming a stalled run needs to move time forward past
    `NATIVE_BALANCE_BACKFILL_STALE_SECONDS` without the run's own chunk
    work also taking real wall-clock time.
    """

    def setUp(self):
        super().setUp()
        self.next_block = BASE_BLOCK
        self.clock = timezone.now()
        for target in (
            "safe_transaction_service.analytics.tasks.timezone.now",
            "safe_transaction_service.analytics.tasks_shards.timezone.now",
        ):
            patcher = patch(target, side_effect=lambda: self.clock)
            patcher.start()
            self.addCleanup(patcher.stop)

        # Redis is not rolled back between tests the way the database is,
        # so a run manifest (and the cursor pointing at it) would outlive
        # the rows it describes -- same isolation `NativeBalanceRollupTestCase`
        # gives itself.
        redis = get_redis()
        keys = list(redis.scan_iter(match=f"{NATIVE_BALANCE_RUN_KEY_PREFIX}*"))
        keys.append(NATIVE_BALANCE_CURSOR_KEY)
        redis.delete(*keys)

    def advance_clock(self, delta: timedelta = timedelta(minutes=25)):
        self.clock += delta

    def block(self, confirmed: bool = True):
        self.next_block += 1
        return EthereumBlockFactory(number=self.next_block, confirmed=confirmed)

    def safe(self, block=None):
        return SafeContractFactory(
            ethereum_tx=EthereumTxFactory(block=block or self.block())
        )

    def transfer(self, block, value: int, to=None, _from=None):
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
        for _ in range(blocks):
            self.block(confirmed=True)

    def fabricate_running_manifest(self, head: int, chunk_size: int = 5000) -> dict:
        """A `state="running"` manifest whose chunk task never actually
        runs -- the caller controls staleness by advancing the fake clock
        and re-checking. Mirrors `AnalyticsBootstrapTestCase.fabricate_stalled_run`'s
        shape for the ERC-20 side, minus the phase/progress-watermark
        machinery native doesn't have."""
        total_safes = SafeContract.objects.count()
        with patch.object(backfill_native_balance_chunk, "apply_async"):
            run = start_native_balance_backfill_run(head, chunk_size, total_safes)
        return run


# ═══════════════════════════ is_done() ══════════════════════════════════


class TestIsDone(NativeBootstrapTestCase):
    def test_flips_once_the_watermark_exists(self):
        stage = NativeStage()
        self.assertFalse(stage.is_done())

        AnalyticsWatermark.objects.create(
            name=NATIVE_BALANCE_WATERMARK, block_number=1, computed_at=timezone.now()
        )

        self.assertTrue(stage.is_done())


# ══════════════════════════ Fresh start ═════════════════════════════════


class TestFreshStart(NativeBootstrapTestCase):
    def test_fresh_start_on_an_empty_instance_reaches_the_watermark(self):
        safe = self.safe()
        self.transfer(self.block(), 100, to=safe.address)
        self.advance_head()
        head = native_balance_head_block()

        self.assertIsNone(latest_native_balance_run_id())
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).exists()
        )

        NativeStage().start_or_resume()

        run = load_native_balance_run(latest_native_balance_run_id())
        self.assertIsNotNone(run)
        self.assertEqual(run["state"], "finished")
        self.assertEqual(run["head"], head)
        self.assertTrue(
            AnalyticsWatermark.objects.filter(
                name=NATIVE_BALANCE_WATERMARK, block_number=head
            ).exists()
        )
        self.assertEqual(SafeNativeBalance.objects.count(), 1)


# ══════════════════════════ Lost-manifest adoption ══════════════════════


class TestAdoptsAnInterruptedRun(NativeBootstrapTestCase):
    def test_adopts_at_the_original_stamp_not_a_later_head(self):
        """Rows already exist at a block with no watermark and no live
        manifest (an interrupted first run, built directly rather than by
        killing a real command mid-flight). Adoption must continue at
        that exact stamp -- `native_balance_backfill_auto_start_target`'s
        whole point, mirroring `_resolve_head`'s own reasoning -- never at
        a newer safe head reached later, which would leave the untouched
        rows complete only up to the *old* stamp forever."""
        safe_a = self.safe()
        safe_b = self.safe()
        self.transfer(self.block(), 100, to=safe_a.address)
        self.advance_head()
        interrupted_stamp = native_balance_head_block()

        seed_missing_native_balances(
            [bytes.fromhex(safe_a.address[2:])], interrupted_stamp
        )
        self.assertEqual(SafeNativeBalance.objects.count(), 1)
        self.assertIsNone(latest_native_balance_run_id())
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).exists()
        )

        # Time -- and the safe head -- moves on before the bootstrap gets
        # to it.
        self.advance_clock(timedelta(hours=1))
        self.advance_head()
        later_head = native_balance_head_block()
        self.assertGreater(later_head, interrupted_stamp)

        NativeStage().start_or_resume()

        run = load_native_balance_run(latest_native_balance_run_id())
        self.assertEqual(run["state"], "finished")
        self.assertEqual(run["head"], interrupted_stamp)  # not later_head
        self.assertTrue(
            AnalyticsWatermark.objects.filter(
                name=NATIVE_BALANCE_WATERMARK, block_number=interrupted_stamp
            ).exists()
        )
        # safe_b, absent from the interrupted rows, is topped up at the
        # same stamp as safe_a -- never at a mismatched block.
        self.assertEqual(SafeNativeBalance.objects.count(), 2)
        self.assertEqual(
            set(SafeNativeBalance.objects.values_list("updated_to_block", flat=True)),
            {interrupted_stamp},
        )
        self.assertTrue(
            SafeNativeBalance.objects.filter(
                safe_address=safe_b.address, updated_to_block=interrupted_stamp
            ).exists()
        )


# ══════════════════════ Resuming a stalled run ══════════════════════════


class TestResumesAStalledRun(NativeBootstrapTestCase):
    """No beat task resumes a stalled native `--celery` run today (see
    `bootstrap/native.py`'s module docstring) -- `NativeStage.resumed_externally`
    is `False`, so `start_or_resume()` must resume it itself. Unlike the
    ERC-20 side, there is no `resumed_externally=True` branch to prove the
    negative of here: native has no watchdog to hand that job to."""

    def test_resumed_externally_is_false(self):
        self.assertFalse(NativeStage.resumed_externally)

    def test_a_stale_running_manifest_is_redispatched(self):
        safe = self.safe()
        self.transfer(self.block(), 100, to=safe.address)
        self.advance_head()
        head = native_balance_head_block()

        run = self.fabricate_running_manifest(head)
        self.advance_clock(timedelta(seconds=NATIVE_BALANCE_BACKFILL_STALE_SECONDS + 1))
        self.assertIsNotNone(tasks_module.native_balance_backfill_looks_stalled())

        with patch.object(backfill_native_balance_chunk, "apply_async") as spy:
            NativeStage().start_or_resume()

        # Resuming bumps the dispatch generation: the fresh manifest
        # started at seq 1, so the redispatch carries seq 2 -- the value
        # the originally queued (but here patched-away) chunk would no
        # longer match if it ran.
        spy.assert_called_once_with((run["run_id"], 2), queue="contracts")
        # Resuming re-dispatches the SAME run_id -- no second manifest is
        # ever created, keeping "at most one live chain".
        self.assertEqual(latest_native_balance_run_id(), run["run_id"])
        self.assertEqual(load_native_balance_run(run["run_id"])["dispatch_seq"], 2)


class TestFreshRunningManifestBlocksDispatch(NativeBootstrapTestCase):
    """The native equivalent of `TestErc20StageBlockedByRunningManifest`.
    The manifest's `heartbeat_at` (fresh here, since `fabricate_running_manifest`
    just wrote it) is well inside `NATIVE_BALANCE_BACKFILL_STALE_SECONDS` --
    `status()` reports `"running"`, not `"stalled"`, and `start_or_resume()`
    must leave it alone."""

    def test_a_freshly_started_running_manifest_blocks_a_fresh_start(self):
        safe = self.safe()
        self.transfer(self.block(), 100, to=safe.address)
        self.advance_head()
        head = native_balance_head_block()

        run = self.fabricate_running_manifest(head)
        # No `advance_clock`: the manifest's heartbeat stays fresh.
        self.assertIsNone(tasks_module.native_balance_backfill_looks_stalled())
        self.assertEqual(NativeStage().status(), "running")

        with patch.object(backfill_native_balance_chunk, "apply_async") as spy:
            NativeStage().start_or_resume()

        spy.assert_not_called()
        unchanged = load_native_balance_run(run["run_id"])
        self.assertEqual(unchanged["state"], "running")
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).exists()
        )


class TestLockHeldMeansRunningHoweverOldTheHeartbeat(NativeBootstrapTestCase):
    """Liveness, not elapsed time, decides "stalled": while a
    chunk holds the shared rollup lock, the run is `"running"`, however
    long it has been running -- simulated here by holding the lock
    ourselves (standing in for a genuinely slow chunk) alongside a
    heartbeat several hours stale, which would have read as `"stalled"`
    under the old elapsed-time-only design."""

    def test_a_lock_held_run_with_a_3h_old_heartbeat_is_running_not_stalled(self):
        safe = self.safe()
        self.transfer(self.block(), 100, to=safe.address)
        self.advance_head()
        head = native_balance_head_block()

        run = self.fabricate_running_manifest(head)
        self.advance_clock(timedelta(hours=3))

        lock_name = get_task_lock_name(compute_native_balance_rollup_task.name)
        redis = get_redis()
        with (
            redis.lock(lock_name, blocking=False),
            patch.object(backfill_native_balance_chunk, "apply_async") as spy,
        ):
            self.assertEqual(NativeStage().status(), "running")
            NativeStage().start_or_resume()

        spy.assert_not_called()
        unchanged = load_native_balance_run(run["run_id"])
        self.assertEqual(unchanged["state"], "running")
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).exists()
        )


# ═══════════════════════ Shared lock blocks dispatch ═════════════════════


class TestLockHeldBlocksDispatch(NativeBootstrapTestCase):
    def test_lock_held_blocks_a_fresh_start(self):
        self.safe()
        self.advance_head()

        lock_name = get_task_lock_name(compute_native_balance_rollup_task.name)
        redis = get_redis()
        with redis.lock(lock_name, blocking=False):
            NativeStage().start_or_resume()

        self.assertIsNone(latest_native_balance_run_id())
        self.assertFalse(AnalyticsWatermark.objects.exists())

    def test_lock_held_blocks_adopting_an_interrupted_run(self):
        safe = self.safe()
        self.transfer(self.block(), 100, to=safe.address)
        self.advance_head()
        stamp = native_balance_head_block()
        seed_missing_native_balances([bytes.fromhex(safe.address[2:])], stamp)

        lock_name = get_task_lock_name(compute_native_balance_rollup_task.name)
        redis = get_redis()
        with redis.lock(lock_name, blocking=False):
            NativeStage().start_or_resume()

        self.assertIsNone(latest_native_balance_run_id())
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).exists()
        )


# ═══════════════════ Zombie / superseded run_id guard ════════════════════


class TestStaleRunIdChainExitsWithoutWork(NativeBootstrapTestCase):
    """`backfill_native_balance_chunk` reads its own manifest by `run_id`
    and returns `{"run_id": ..., "state": "unknown"}` once that manifest
    is gone (`tasks_shards.py`, confirmed by reading the task) -- so a
    superseded `run_id` (its manifest lost or replaced by a fresh
    adoption) does no work when redelivered late. Proven here directly
    against the real task, the same guard
    `TestZombieChainExitsWithoutWork` pins for the ERC-20 side."""

    def test_a_run_ids_task_is_a_noop_once_its_manifest_is_gone(self):
        safe = self.safe()
        self.transfer(self.block(), 100, to=safe.address)
        self.advance_head()
        head = native_balance_head_block()

        old_run = self.fabricate_running_manifest(head)
        get_redis().delete(old_run["run_key"])

        result = backfill_native_balance_chunk(
            old_run["run_id"], old_run["dispatch_seq"]
        )

        self.assertEqual(result, {"run_id": old_run["run_id"], "state": "unknown"})
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).exists()
        )

    def test_adoption_after_a_lost_manifest_never_touches_the_old_run_id(self):
        """Same shape as the ERC-20 side's zombie test: once the bootstrap
        adopts a NEW run_id, a task still carrying the OLD one must find
        nothing and do nothing -- never double-apply."""
        safe_a = self.safe()
        safe_b = self.safe()
        self.transfer(self.block(), 100, to=safe_a.address)
        self.advance_head()
        stamp = native_balance_head_block()
        seed_missing_native_balances([bytes.fromhex(safe_a.address[2:])], stamp)

        old_run = self.fabricate_running_manifest(stamp)
        get_redis().delete(old_run["run_key"])

        NativeStage().start_or_resume()  # adopts a NEW run_id

        new_run_id = latest_native_balance_run_id()
        self.assertNotEqual(new_run_id, old_run["run_id"])
        self.assertTrue(
            AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).exists()
        )

        result = backfill_native_balance_chunk(
            old_run["run_id"], old_run["dispatch_seq"]
        )
        self.assertEqual(result, {"run_id": old_run["run_id"], "state": "unknown"})
        # The new run adopted both Safes, including safe_b, at the same
        # stamp -- never split across the old and new run_ids.
        self.assertTrue(
            SafeNativeBalance.objects.filter(
                safe_address=safe_b.address, updated_to_block=stamp
            ).exists()
        )


# ═══════════════════════ Failed is not stalled or pending ════════════════


class TestFailedIsReportedDistinctly(NativeBootstrapTestCase):
    """A chunk that raises (its own hard time limit, a statement timeout,
    or anything else) must leave the manifest `state="failed"`, which
    `status()` reports as `"failed"` -- never `"stalled"` (the manifest
    isn't `"running"` any more, so `native_balance_backfill_looks_stalled`
    already excludes it) and never silently `"pending"` either, so a
    retry cooldown can key off it."""

    def test_a_raising_chunk_is_reported_failed_not_stalled_or_pending(self):
        safe = self.safe()
        self.transfer(self.block(), 100, to=safe.address)
        self.advance_head()

        with patch(
            "safe_transaction_service.analytics.tasks.seed_missing_native_balances",
            side_effect=RuntimeError("pg went away"),
        ):
            NativeStage().start_or_resume()

        run = load_native_balance_run(latest_native_balance_run_id())
        self.assertEqual(run["state"], "failed")
        self.assertIn("pg went away", run["error"])
        self.assertEqual(NativeStage().status(), "failed")

        # Re-running (the same recovery path a manual re-run of the
        # command gets) still works: the chunk task is not left wedged.
        NativeStage().start_or_resume()
        run = load_native_balance_run(latest_native_balance_run_id())
        self.assertEqual(run["state"], "finished")
        self.assertEqual(NativeStage().status(), "done")


# ═══════════════════════ Idempotent watermark write ══════════════════════


class TestIdempotentWatermarkWrite(NativeBootstrapTestCase):
    """`AnalyticsWatermark.name` is a primary key -- two writers reaching
    "no watermark yet" at the same moment must never surface an
    `IntegrityError`. Both write sites (`write_native_balance_watermark`,
    used by the chunk task's finish step, and
    `_cold_start_native_balance_rollup`, the nightly task's small-fleet
    cold start) are exercised directly here, each simulating the OTHER
    one having already won the race."""

    def test_write_native_balance_watermark_declines_without_raising_if_already_written(
        self,
    ):
        self.safe()
        self.advance_head()
        head = native_balance_head_block()

        first = write_native_balance_watermark(head)
        second = write_native_balance_watermark(head)  # "the other path" won first

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(
            AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).count(),
            1,
        )

    def test_cold_start_declines_without_raising_if_already_written(self):
        self.safe()
        self.advance_head()
        head = native_balance_head_block()

        # Simulate a bootstrap-started `--celery` chunk's finish step
        # winning the watermark race a moment before this cold start's
        # own `get_or_create` runs.
        write_native_balance_watermark(head)

        result = _cold_start_native_balance_rollup(head)

        self.assertIsNone(result)
        self.assertEqual(
            AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).count(),
            1,
        )


# ═════════════ Duplicate chain after a spurious resume ══════════════════


class TestDuplicateChainAfterASpuriousResume(NativeBootstrapTestCase):
    """The grace period only covers the ordinary hand-off gap (a chunk
    released the lock, the next one is queued but not yet running). On a
    busy `contracts` queue that gap can be exceeded, and the bootstrap
    stage would then resume a run whose originally queued chunk is still
    sitting in the queue -- without a generation guard, that chunk would
    eventually run too, continuing a second, parallel chain forever (the
    shared lock only serialises the two, it does not stop either from
    existing). `dispatch_seq` fixes it: a chunk whose own argument no
    longer matches the manifest's current value exits without work and
    without dispatching a successor."""

    def test_the_stale_dispatch_seq_chunk_does_nothing_and_dispatches_nothing(self):
        safe = self.safe()
        self.transfer(self.block(), 100, to=safe.address)
        self.advance_head()
        head = native_balance_head_block()

        run = self.fabricate_running_manifest(head)
        stale_seq = run["dispatch_seq"]
        self.assertEqual(stale_seq, 1)

        # Simulate a resume that already happened -- the manifest's
        # generation moved to 2 (`NativeStage.start_or_resume()`'s own
        # `next_native_balance_dispatch_seq` call), but the run is still
        # `"running"` (the resumed chunk hasn't executed yet in this
        # simulation), exactly the window where the ORIGINALLY queued
        # chunk -- still carrying seq 1 -- might finally be picked up by
        # a worker.
        next_native_balance_dispatch_seq(run)
        before = load_native_balance_run(run["run_id"])
        self.assertEqual(before["dispatch_seq"], 2)
        self.assertEqual(before["state"], "running")

        with patch.object(backfill_native_balance_chunk, "apply_async") as spy:
            result = backfill_native_balance_chunk(run["run_id"], stale_seq)

        spy.assert_not_called()
        self.assertEqual(result, before)
        self.assertEqual(load_native_balance_run(run["run_id"]), before)


# ═══════ Nightly cold start declines while a backfill is in progress ════


class TestColdStartDeclinesWhileABackfillIsInProgress(NativeBootstrapTestCase):
    """The rollup lock is free during the ordinary hand-off gap between
    chunks, so it is not sufficient evidence that no backfill is in
    progress. The nightly cold start must check the backfill's own
    durable state instead."""

    def test_a_running_celery_manifest_makes_the_cold_start_a_noop(self):
        safe = self.safe()
        self.transfer(self.block(), 100, to=safe.address)
        self.advance_head()
        head = native_balance_head_block()

        self.fabricate_running_manifest(head)  # state="running", no rows yet
        self.assertTrue(_native_balance_backfill_in_progress())

        result = _cold_start_native_balance_rollup(head)

        self.assertIsNone(result)
        self.assertEqual(SafeNativeBalance.objects.count(), 0)
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).exists()
        )

    def test_unwatermarked_progress_rows_with_no_live_manifest_make_it_a_noop(self):
        """The inline mode (and a `--celery` run whose manifest has since
        expired from Redis) keeps no live manifest at all -- the rollup
        table's own unwatermarked rows are the only durable signal."""
        safe = self.safe()
        self.transfer(self.block(), 100, to=safe.address)
        self.advance_head()
        head = native_balance_head_block()

        seed_missing_native_balances([bytes.fromhex(safe.address[2:])], head)
        self.assertIsNone(latest_native_balance_run_id())
        self.assertTrue(_native_balance_backfill_in_progress())

        result = _cold_start_native_balance_rollup(head)

        self.assertIsNone(result)
        self.assertFalse(
            AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).exists()
        )

    def test_an_empty_instance_with_no_backfill_is_not_in_progress(self):
        self.safe()
        self.advance_head()
        self.assertFalse(_native_balance_backfill_in_progress())
