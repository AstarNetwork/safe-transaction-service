"""Day completeness: ``DayResult``, the populator/core registry, and
``compute_day`` -- the single entry point every caller (nightly task,
backfill shard, ``--inline`` backfill, sweeper) goes through to compute or
retry one day.

See "Analytics catch-up" in ``analytics/implementation-notes.md``.
"""

import enum
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

from ..models import DailyMetric
from .gate import (
    INDEXER_STATUS_UNAVAILABLE,
    DayNotReady,
    IndexerStatus,
    ensure_day_settled,
)
from .state import record_day_result

logger = logging.getLogger(__name__)


class DayStatus(enum.Enum):
    """Outcome of one ``compute_day`` call."""

    DONE = "done"
    INCOMPLETE = "incomplete"
    DEFERRED = "deferred"


@dataclass(frozen=True)
class DayResult:
    """Outcome of computing (or attempting to compute) one day.

    ``core_ok`` and ``failed`` describe the day's *current* standing after
    this call -- for a partial (``only=``) run that didn't touch the core or
    a given populator, they carry forward the prior outcome rather than
    describing only what this specific call did (see ``_upsert_daily_metric``
    in ``tasks.py``). That is what lets a caller persist them straight onto
    ``AnalyticsCatchupState`` without re-deriving the merge.
    """

    status: DayStatus
    core_ok: bool
    failed: tuple[str, ...] = ()
    code: str | None = None


#: Order matches the existing populator chain in `tasks.py`: `active_safes` /
#: `active_owners` run first so `_compute_daily_metric_core` can read their
#: rollups instead of recomputing the same aggregates a second time.
POPULATORS = (
    "active_safes",
    "active_owners",
    "token_volume",
    "tx_volume",
    "safe_app_txs",
    "safe_creations",
)

#: `_compute_daily_metric_core` reads its `active_safes` / `active_owners`
#: counts from these two populators' rollups, so retrying either one without
#: also rerunning the core would leave the core's own counts stale.
CORE_INPUTS = frozenset({"active_safes", "active_owners"})


def log_day_deferred(day: date, status: IndexerStatus | None, code: str) -> None:
    """WARNING ``analytics.daily.deferred`` -- dotted-style, matching
    ``analytics.rollup.cold_window``; see "Analytics catch-up" in
    ``analytics/implementation-notes.md`` for why this log's style diverges
    from the rest of ``tasks.py``.

    ``indexed_to_block`` / ``indexed_to_ts`` report whichever of the two
    indexer pipelines is behind -- the one ``IndexerStatus.position_ts``
    comes from, i.e. the pipeline actually holding the day back. Both are
    ``None`` when ``status`` itself couldn't be fetched this run.
    """
    if status is None:
        logger.warning(
            "analytics.daily.deferred day=%s indexed_to_block=%s indexed_to_ts=%s code=%s",
            day,
            None,
            None,
            code,
        )
        return
    if status.erc20_block_timestamp <= status.master_copies_block_timestamp:
        indexed_to_block = status.erc20_block_number
        indexed_to_ts = status.erc20_block_timestamp
    else:
        indexed_to_block = status.master_copies_block_number
        indexed_to_ts = status.master_copies_block_timestamp
    logger.warning(
        "analytics.daily.deferred day=%s indexed_to_block=%s indexed_to_ts=%s code=%s",
        day,
        indexed_to_block,
        indexed_to_ts.isoformat(),
        code,
    )


def compute_day(
    day: date,
    status: IndexerStatus | None,
    *,
    only: tuple[str, ...] | None = None,
    skip_settle_check: bool = False,
) -> DayResult:
    """Compute (or retry) one UTC day -- the single entry point every caller
    goes through: the nightly task, a backfill shard, ``--inline`` backfill,
    and the sweeper.

    ``status`` is one ``get_indexer_status()`` snapshot, fetched once per run
    by the caller and reused across every day it checks -- this function
    never fetches it itself, so every day in a run is judged against the
    same indexer position. ``status is None`` means the caller couldn't
    obtain a snapshot this run at all (it caught ``DayNotReady`` from
    ``get_indexer_status()`` once, outside this call): without
    ``skip_settle_check`` that defers the day exactly like a per-day gate
    failure would; with it, the day is computed anyway -- that is what
    ``--skip-settle-check`` does when the indexer status is unavailable.

    ``only``, when given, retries a subset of ``POPULATORS`` for a day that
    already has a ``DailyMetric`` row -- see ``_upsert_daily_metric`` in
    ``tasks.py`` for the validation, execution and the marks it sets on
    ``DailyMetric``. This function's own job on top of that is: run the
    gate (unless skipped), then persist the result onto the day's
    ``AnalyticsCatchupState`` row, if the sweeper has one.

    ``skip_settle_check`` is ``backfill_daily_metrics --skip-settle-check``'s
    path down to a single day -- the only caller that sets it today, hence
    the fixed ``by=backfill`` on the WARNING below. It computes the day
    without asking whether it's settled at all (regardless of whether
    ``status`` could be fetched), so every call logs WARNING
    ``analytics.daily.settle_check_skipped`` -- this is a deliberate escape
    hatch for a range known to be fully indexed, not a routine path.
    """
    if skip_settle_check:
        logger.warning("analytics.daily.settle_check_skipped day=%s by=backfill", day)
    else:
        if status is None:
            log_day_deferred(day, None, INDEXER_STATUS_UNAVAILABLE)
            return DayResult(
                status=DayStatus.DEFERRED,
                core_ok=False,
                code=INDEXER_STATUS_UNAVAILABLE,
            )
        try:
            ensure_day_settled(day, status)
        except DayNotReady as exc:
            log_day_deferred(day, status, exc.code)
            return DayResult(status=DayStatus.DEFERRED, core_ok=False, code=exc.code)

    # Lazy: `tasks.py` imports this package eagerly at module level (it
    # needs `compute_day` for the nightly task), so importing it back here
    # at module level would be circular. Every module in this package keeps
    # its `tasks` import inside a function body for the same reason.
    from ..tasks import _upsert_daily_metric

    day_start = datetime.combine(day, time.min, tzinfo=UTC)
    day_end = day_start + timedelta(days=1)
    result = _upsert_daily_metric(day_start, day_end, only=only)

    record_day_result(day, result)
    return result


def select_failed_days(dates: list[date]) -> list[date]:
    """Days in ``dates`` without ``completed_at`` -- not (yet) fully
    computed, whether never attempted, deferred by the gate, or a
    populator is still failing.

    ``backfill_daily_metrics --failed-only`` uses this so a re-run only
    touches what a previous run (or the sweeper) hasn't finished yet.
    ``completed_at`` already implies ``core_completed_at`` (see
    ``compute_day``'s docstring above), so checking it alone is a
    complete picture of "not done" -- no separate check is needed.
    """
    if not dates:
        return []
    done = set(
        DailyMetric.objects.filter(
            date__range=(dates[0], dates[-1]), completed_at__isnull=False
        ).values_list("date", flat=True)
    )
    return [d for d in dates if d not in done]
