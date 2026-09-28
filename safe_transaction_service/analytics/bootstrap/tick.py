"""The bootstrap tick -- first match wins, every branch returns in ms.

1. Analytics disabled or the bootstrap switched off -> no-op.
2. Everything done/given-up at the configured depth -> no-op.
3. Pick the first non-terminal stage in registry order, row-first
   (`bookkeeping.pick_current_stage`); none left -> stop.
4. `"running"` -> no-op; `"stalled"` -> no-op only if
   `resumed_externally` (`stage.py`).
5. Indexer gate not caught up -> no-op.
6. `"failed"` -> count it, give up at the cap, else cool down or fall
   through.
7. Otherwise -> `stage.start_or_resume()`.

Never raises -- every step runs inside one try/except.
"""

import logging
from datetime import timedelta

from django.conf import settings
from django.utils import timezone

from . import bookkeeping
from .gate import indexer_caught_up
from .registry import build_default_stages
from .stage import Stage

logger = logging.getLogger(__name__)

#: Retry a failed stage after this cooldown, at most this many times in a
#: row before giving up.
BOOTSTRAP_RETRY_COOLDOWN = timedelta(hours=6)
BOOTSTRAP_RETRY_CAP = 3


def _configured_daily_depth() -> int:
    """The daily stage's currently configured window depth.
    ``config/settings/base.py`` validates ``ANALYTICS_BOOTSTRAP_DAILY_DAYS``
    at settings load time (clamped to >= 1), so this reads it straight --
    no fallback needed here."""
    return settings.ANALYTICS_BOOTSTRAP_DAILY_DAYS


def run_tick(stages: list[Stage] | None = None) -> None:
    """Run one bootstrap tick. ``stages`` defaults to the real registry
    (``build_default_stages()``); a test passes its own list to exercise
    order enforcement or a stage that misbehaves without touching the
    real ERC-20 stage."""
    try:
        _run_tick(stages)
    except Exception:
        logger.exception("analytics.bootstrap.tick: unexpected failure")


def _run_tick(stages: list[Stage] | None) -> None:
    if not settings.ENABLE_ANALYTICS or not settings.ANALYTICS_AUTO_BACKFILL:
        return  # step 1

    depth = _configured_daily_depth()
    if bookkeeping.bootstrap_complete(depth):
        return  # step 2, fully complete for the current depth

    # A no-op unless the daily stage genuinely completed at a smaller
    # depth (see `maybe_reopen_daily`'s own docstring for why a
    # merely-given-up daily row is left alone here).
    bookkeeping.maybe_reopen_daily(depth)

    all_stages = stages if stages is not None else build_default_stages()

    def _mark_stage_done(name: str) -> None:
        bookkeeping.mark_completed(name, depth=depth if name == "daily" else None)

    # Terminal is decided from the ROW FIRST -- see the module
    # docstring's step 3 for why re-deriving "done" from live state on
    # every tick would race (a day sliding into the daily window
    # mid-native-run). `pick_current_stage` is the one place this rule
    # lives; `analytics/bootstrap/report.py` shares it read-only.
    current_stage = bookkeeping.pick_current_stage(
        all_stages, depth, on_stage_done=_mark_stage_done
    )
    if current_stage is None:
        return  # step 3, nothing left to do

    current_row = bookkeeping.get_stage_row(current_stage.name)
    current_status = current_stage.status()

    if current_status == "running":
        return  # step 4, always a no-op
    if current_status == "stalled" and current_stage.resumed_externally:
        return  # step 4, only when something else owns resuming this stage

    if not indexer_caught_up():
        return  # step 5

    if current_status == "failed":
        should_go = _should_dispatch_after_failure(current_stage.name, current_row)
        if not should_go:
            return  # step 6, either given up just now or still cooling down

    bookkeeping.record_dispatch(current_stage.name)
    current_stage.start_or_resume()  # step 7


def _should_dispatch_after_failure(name: str, row) -> bool:
    """Step 6. Counts a new failure (none recorded yet, or a dispatch
    happened since the last one recorded), then gives up at the cap,
    cools down, or clears to retry. A `None` `last_dispatch_at` never
    counts as "dispatched already" -- that would recount the same
    still-cooling-down failure every tick and reach the cap early.
    """
    should_count = row.last_failure_at is None or (
        row.last_dispatch_at is not None and row.last_failure_at < row.last_dispatch_at
    )
    if should_count:
        row = bookkeeping.record_failure(name)

    if row.consecutive_failures >= BOOTSTRAP_RETRY_CAP:
        bookkeeping.mark_gave_up(name)
        logger.error(
            "analytics.bootstrap: stage=%s gave up after %d consecutive failures "
            "-- moving on to the next stage",
            name,
            row.consecutive_failures,
        )
        return False

    if timezone.now() - row.last_failure_at < BOOTSTRAP_RETRY_COOLDOWN:
        return False  # still cooling down

    return True
