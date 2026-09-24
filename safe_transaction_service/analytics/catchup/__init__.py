"""Analytics catch-up: self-healing for missed/incomplete daily metrics.

See "Analytics catch-up" in ``analytics/implementation-notes.md``.

This package must not import ``safe_transaction_service.analytics.tasks`` at
module level, in this module or any other in the package -- ``tasks.py``
imports ``catchup`` eagerly at module level (it needs ``compute_day`` for
the nightly task), so a module-level import back here would be circular.
Anything that needs ``tasks.py`` imports it lazily, inside a function.
"""

from .day import (
    CORE_INPUTS,
    POPULATORS,
    DayResult,
    DayStatus,
    compute_day,
    log_day_deferred,
    select_failed_days,
)
from .gate import DayNotReady, IndexerStatus, ensure_day_settled, get_indexer_status
from .snapshots import (
    SNAPSHOT_NAMES,
    is_snapshot_stale,
    mark_snapshot_dispatched,
    sweep_snapshots,
)
from .sweeper import run_catchup

__all__ = [
    "CORE_INPUTS",
    "POPULATORS",
    "SNAPSHOT_NAMES",
    "DayNotReady",
    "DayResult",
    "DayStatus",
    "IndexerStatus",
    "compute_day",
    "ensure_day_settled",
    "get_indexer_status",
    "is_snapshot_stale",
    "log_day_deferred",
    "mark_snapshot_dispatched",
    "run_catchup",
    "select_failed_days",
    "sweep_snapshots",
]
