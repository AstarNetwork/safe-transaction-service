"""The ERC-20 balance stage.

``start_or_resume()`` is the watchdog's former auto-start logic: adopt
an orphaned run or start a fresh one, never resume a stalled one -- the
watchdog task (``erc20_balance_backfill_watchdog_task``) is the ONE
resumer, on its own beat, so two resumers could otherwise dispatch the
same run's chain. ``status()`` reports ``"stalled"`` for visibility
only; the tick no-ops on it via ``resumed_externally``.
"""

import logging

from safe_transaction_service.utils.tasks import get_task_lock_name

from .locks import lock_is_free
from .stage import Stage, _tasks

logger = logging.getLogger(__name__)


class Erc20Stage(Stage):
    name = "erc20"
    #: The watchdog task is the one resumer of a stalled ERC-20 run (see
    #: the module docstring) -- the tick must no-op on
    #: ``status() == "stalled"`` exactly like ``"running"`` rather than
    #: falling through to ``start_or_resume()`` itself.
    resumed_externally = True

    def _current_run(self) -> dict | None:
        """The latest ERC-20 backfill run manifest, or `None` if there
        has never been one, it expired, or the cursor key is empty."""
        tasks_module = _tasks()
        run_id = tasks_module.latest_erc20_balance_backfill_run_id()
        return tasks_module.load_erc20_balance_backfill_run(run_id) if run_id else None

    def is_done(self) -> bool:
        from safe_transaction_service.analytics.models import AnalyticsWatermark

        tasks_module = _tasks()
        return AnalyticsWatermark.objects.filter(
            name=tasks_module.ERC20_BALANCE_WATERMARK
        ).exists()

    def status(self) -> str:
        """``"done"`` / ``"stalled"`` / ``"running"`` / ``"failed"`` /
        ``"pending"``. ``"stalled"`` is reported for visibility and so the
        tick no-ops on it, exactly like ``"running"``: resuming a stalled
        run is the watchdog's job alone -- see the module docstring. A
        manifest whose ``state`` is ``"failed"`` is checked first, ahead
        of the stalled check, since a manifest that failed outright never
        had a chance to go stale.
        """
        if self.is_done():
            return "done"

        run = self._current_run()
        if run is not None and run.get("state") == "failed":
            return "failed"

        tasks_module = _tasks()
        if tasks_module.erc20_balance_backfill_looks_stalled() is not None:
            return "stalled"

        if run is not None and run.get("state") == "running":
            return "running"
        return "pending"

    def progress(self) -> dict | None:
        """Cheap progress straight from the current run's manifest -- no
        counting query. ``None`` if there is no run (never started, or
        the manifest expired)."""
        run = self._current_run()
        if run is None:
            return None
        return {
            "phase": run.get("phase"),
            "chunks_done": run.get("chunks_done"),
            "safes_seen": run.get("safes_seen"),
        }

    def start_or_resume(self) -> None:
        """Adopt an orphaned run or start a fresh one. Never resumes a
        stalled run -- see the module docstring. The tick already no-ops
        on ``"stalled"``, so this is only ever reached with
        ``status() in ("pending", "failed")``; the defensive re-check of
        a genuinely ``"running"`` manifest below guards against nothing
        more than a race between the tick's status read and this call.
        """
        tasks_module = _tasks()
        lock_name = get_task_lock_name(
            tasks_module.compute_erc20_balance_rollup_task.name
        )

        current_run = self._current_run()
        if current_run is not None and current_run.get("state") == "running":
            return

        target = tasks_module.erc20_balance_backfill_auto_start_target()
        if target is None:
            return  # the completion watermark landed between the checks above
        action, phase = target

        if not lock_is_free(lock_name):
            logger.info(
                "analytics.bootstrap.erc20: %s candidate found but the "
                "rollup lock is held (genuine work in flight); leaving it "
                "alone this tick",
                action,
            )
            return

        from safe_transaction_service.analytics.management.commands.backfill_erc20_balances import (
            DEFAULT_CHUNK_SIZE,
            DEFAULT_STATEMENT_TIMEOUT_MS,
            DEFAULT_TASK_CHUNKS,
            DEFAULT_WHALE_BLOCK_RANGE,
            DEFAULT_WHALE_MIN_FREQUENCY,
            DEFAULT_WHALE_ROW_THRESHOLD,
        )

        run = tasks_module.build_erc20_balance_backfill_run(
            chunk_size=DEFAULT_CHUNK_SIZE,
            whale_min_frequency=DEFAULT_WHALE_MIN_FREQUENCY,
            whale_row_threshold=DEFAULT_WHALE_ROW_THRESHOLD,
            whale_block_range=DEFAULT_WHALE_BLOCK_RANGE,
            statement_timeout_ms=DEFAULT_STATEMENT_TIMEOUT_MS,
            task_chunks=DEFAULT_TASK_CHUNKS,
            origin="auto",
            **({"phase": phase} if action == "adopt" else {}),
        )
        logger.warning(
            "analytics.bootstrap.erc20: %s run=%s phase=%s (rollup lock free)",
            "adopting an orphaned run"
            if action == "adopt"
            else "auto-starting a fresh run",
            run["run_id"],
            run["phase"],
        )
        tasks_module.dispatch_erc20_balance_backfill_run(run)
