"""The native-balance stage.

Always drives ``backfill_native_balances --celery`` (never inline, never
the nightly rollup's own small-fleet cold start) and resumes a stalled
run itself -- there is no native watchdog task.

The shared rollup lock IS the chunk's liveness signal: it's held for
the chunk's whole DB write, so staleness only falls back to
``heartbeat_at``/``started_at`` once the lock is free.
``dispatch_seq`` is bumped before every resume so a chunk still queued
under the old value exits without work instead of racing a second
chain. The nightly cold start declines outright while a backfill (live
manifest or unwatermarked rows) is already under way.
"""

import logging

from safe_transaction_service.utils.tasks import get_task_lock_name

from .locks import lock_is_free
from .stage import Stage, _tasks

logger = logging.getLogger(__name__)


class NativeStage(Stage):
    name = "native"

    #: No beat task resumes a stalled native ``--celery`` run -- see the
    #: module docstring -- so this stage resumes it itself.
    resumed_externally = False

    def _current_run(self) -> dict | None:
        """The latest native-balance backfill run manifest, or `None` if
        there has never been one, it expired, or the cursor key is
        empty."""
        tasks_module = _tasks()
        run_id = tasks_module.tasks_shards.latest_native_balance_run_id()
        return (
            tasks_module.tasks_shards.load_native_balance_run(run_id)
            if run_id
            else None
        )

    def is_done(self) -> bool:
        from safe_transaction_service.analytics.models import AnalyticsWatermark

        tasks_module = _tasks()
        return AnalyticsWatermark.objects.filter(
            name=tasks_module.NATIVE_BALANCE_WATERMARK
        ).exists()

    def status(self) -> str:
        """``"done"`` / ``"stalled"`` / ``"running"`` / ``"failed"`` /
        ``"pending"``. Unlike the ERC-20 stage, ``"stalled"`` here is not
        purely for visibility: this stage resumes it itself, see
        ``start_or_resume()``. ``"failed"`` (a chunk raised) is reported
        distinctly from both, so a retry cooldown can key off it.
        """
        if self.is_done():
            return "done"

        tasks_module = _tasks()
        if tasks_module.native_balance_backfill_looks_stalled() is not None:
            return "stalled"

        run = self._current_run()
        if run is not None:
            if run.get("state") == "running":
                return "running"
            if run.get("state") == "failed":
                return "failed"
        return "pending"

    def progress(self) -> dict | None:
        """Cheap progress straight from the current run's manifest -- no
        counting query. ``None`` if there is no run (never started, or
        the manifest expired)."""
        run = self._current_run()
        if run is None:
            return None
        return {
            "chunks_done": run.get("chunks_done"),
            "safes_seen": run.get("safes_seen"),
            "total_safes_at_start": run.get("total_safes_at_start"),
        }

    def start_or_resume(self) -> None:
        """Resume a stalled run, adopt an interrupted one, or start a
        fresh one -- always through ``backfill_native_balances``'s own
        ``--celery`` dispatch code, never a copy and never the inline
        path. See the module docstring for why this stage, unlike the
        ERC-20 one, owns all three cases.
        """
        tasks_module = _tasks()
        from safe_transaction_service.analytics.management.commands.backfill_native_balances import (
            DEFAULT_CHUNK_SIZE,
        )
        from safe_transaction_service.history.models import SafeContract

        lock_name = get_task_lock_name(
            tasks_module.compute_native_balance_rollup_task.name
        )

        # Resuming a stalled run redispatches the SAME run_id's chunk
        # task, never a new manifest, so "at most one live chain" holds.
        stalled = tasks_module.native_balance_backfill_looks_stalled()
        if stalled is not None:
            if not lock_is_free(lock_name):
                logger.info(
                    "analytics.bootstrap.native: run=%s heartbeat looks "
                    "stale but the rollup lock is held (genuine work in "
                    "flight, or the nightly task); leaving it alone this "
                    "tick",
                    stalled["run_id"],
                )
                return
            # Bump the dispatch generation BEFORE redispatching: the
            # originally queued chunk, wherever it is, still carries the
            # OLD dispatch_seq and will exit without work once it finally
            # runs, instead of continuing a second, parallel chain.
            next_seq = tasks_module.tasks_shards.next_native_balance_dispatch_seq(
                stalled
            )
            logger.warning(
                "analytics.bootstrap.native: run=%s looks stalled "
                "(rollup lock free) -- redispatching its chunk task "
                "dispatch_seq=%d",
                stalled["run_id"],
                next_seq,
            )
            tasks_module.tasks_shards.backfill_native_balance_chunk.apply_async(
                (stalled["run_id"], next_seq), queue="contracts"
            )
            return

        # Defensive re-check only, same redundancy the ERC-20 stage
        # keeps: `status()` already reported something other than
        # "running" for this call to be reached, so this guards nothing
        # but a race between that read and this one.
        current_run = self._current_run()
        if current_run is not None and current_run.get("state") == "running":
            return

        target = tasks_module.native_balance_backfill_auto_start_target()
        if target is None:
            return  # nothing to start: done, inconsistent stamps, or no safe head yet
        action, head = target

        if not lock_is_free(lock_name):
            logger.info(
                "analytics.bootstrap.native: %s candidate found (head=%d) "
                "but the rollup lock is held; leaving it alone this tick",
                action,
                head,
            )
            return

        total_safes = SafeContract.objects.count()
        run = tasks_module.tasks_shards.start_native_balance_backfill_run(
            head, DEFAULT_CHUNK_SIZE, total_safes
        )
        logger.warning(
            "analytics.bootstrap.native: %s run=%s head=%d (rollup lock free)",
            "adopting an interrupted run"
            if action == "adopt"
            else "auto-starting a fresh run",
            run["run_id"],
            head,
        )
