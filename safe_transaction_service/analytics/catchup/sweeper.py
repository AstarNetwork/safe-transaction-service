"""The hourly sweeper -- ``run_catchup()``.

See "Analytics catch-up" in ``analytics/implementation-notes.md``. Locking
and the ``ENABLE_ANALYTICS`` gate live in the thin ``analytics_catchup_task``
wrapper in ``tasks.py``, not here: this function assumes whatever lock the
caller wants is already held and just does the work, once, synchronously.

One run does, in order:

1. Read settings (``ImproperlyConfigured`` -> ERROR ``bad_config``, no
   further work, no summary line -- the "missing line" alert is the signal).
2. Fetch the indexer status once, reused for both the day gate and the
   processing-stuck check below (``None`` if unavailable this run).
3. Catch up days: skipped outright while a live backfill run overlaps the
   window; otherwise select candidates, exclude days that have given up
   (logging once) or aren't due for retry yet, exclude days the gate isn't
   ready for (logging ``analytics.daily.deferred``, spending no attempt),
   then take the oldest ``MAX_DAYS_PER_RUN`` ready days, bump each one's
   attempt counter *before* computing it, and call ``compute_day`` --
   retrying only its previously failed populators when the core already
   succeeded.
4. Check the stuck-processing watchdog (same status snapshot as step 2).
5. Check for days that fell out of the window without ever completing.
6. Prune state rows for days more than two windows old.
7. Sweep the three current-state snapshots (``summary`` / ``safe_segments`` /
   ``tvl``): independent of the day catch-up above (and of
   ``skipped_reason``), so a live backfill run blocking day recomputation
   doesn't also block a stale snapshot from refreshing.
8. Log the one summary line every run either finishes with, or is skipped
   for a reason worth recording (``skipped_reason=backfill``) -- everything
   except step 1's ``bad_config`` failure logs this line.
"""

import logging
from datetime import date, datetime, timedelta

from django.core.exceptions import ImproperlyConfigured
from django.db.models import Min
from django.utils import timezone

from safe_transaction_service.history.models import InternalTxDecoded

from ..conf import get_catchup_settings
from ..models import AnalyticsCatchupState, DailyMetric
from .day import DayStatus, compute_day, log_day_deferred, select_failed_days
from .gate import (
    INDEXER_STATUS_UNAVAILABLE,
    DayNotReady,
    ensure_day_settled,
    get_indexer_status,
)
from .snapshots import sweep_snapshots
from .state import (
    ensure_day_states,
    get_day_states,
    mark_day_deferred,
    mark_day_succeeded,
    mark_expired_once,
    mark_gave_up,
    mark_processing_stuck_once,
    prune_day_states_before,
    record_attempt,
    record_processing_observation,
)

logger = logging.getLogger(__name__)


def _bad_config_setting(exc: ImproperlyConfigured) -> str:
    """Pull the variable name back out of ``conf._bad_config``'s fixed
    message shape (``"analytics: invalid value for <VAR>"``) -- the message
    never includes the offending value, so this can't leak one either."""
    message = str(exc)
    return message.rsplit(" ", 1)[-1] if " " in message else message


def _backfill_is_live(cfg, now: datetime) -> bool:
    """Whether the most recent backfill run manifest looks alive: not
    finished, and heartbeating (or, for a manifest written before shards
    wrote heartbeats, merely started) within ``BACKFILL_STALE_HOURS``.

    Any unreadable form of the cursor -- including the chunk-summary shape
    the legacy standalone ``dispatch_backfill`` writes into the same key,
    which has no ``run_id`` at all -- reads back as "no live backfill" via
    ``latest_backfill_run_id()`` itself; nothing extra to special-case here.
    Imports ``tasks_shards`` lazily: a module-level import would pull in
    Celery's ``chord``/``group`` and this package's no-eager-``tasks``-import
    rule extends to it for the same reason it applies to ``tasks.py``.
    """
    from safe_transaction_service.analytics import tasks_shards
    from safe_transaction_service.utils.redis import get_redis

    run_id = tasks_shards.latest_backfill_run_id()
    if run_id is None:
        return False
    run = tasks_shards.load_backfill_run(run_id)
    if run is None or run.get("finished_at") is not None:
        return False

    heartbeat_at: datetime | None = None
    raw = get_redis().get(tasks_shards.backfill_heartbeat_key(run_id))
    if raw:
        try:
            heartbeat_at = datetime.fromisoformat(
                raw.decode() if isinstance(raw, bytes) else raw
            )
        except ValueError:
            heartbeat_at = None

    reference = heartbeat_at
    if reference is None:
        started_raw = run.get("started_at")
        if started_raw:
            try:
                reference = datetime.fromisoformat(started_raw)
            except ValueError:
                reference = None
    if reference is None:
        # Neither a heartbeat nor a readable `started_at` -- nothing to
        # judge liveness against, so don't let it block the sweeper forever.
        return False

    age_hours = (now - reference).total_seconds() / 3600
    if age_hours < cfg.BACKFILL_STALE_HOURS:
        return True

    logger.warning(
        "analytics.catchup.stale_backfill run_id=%s heartbeat_at=%s",
        run_id,
        heartbeat_at.isoformat() if heartbeat_at else None,
    )
    return False


def _refresh_active_window(now: datetime) -> None:
    """Only called when this run actually computed at least one day --
    lazy import of `tasks.py` per the package's import rule."""
    from ..tasks import _refresh_active_window_caches

    today_utc_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        _refresh_active_window_caches(today_utc_midnight)
    except Exception:
        logger.exception("analytics.catchup: rolling-window refresh failed")


def _catch_up_days(
    cfg, now: datetime, status
) -> tuple[list[str], list[str], list[str], list[str], str | None]:
    """Returns ``(days_fixed, days_deferred, days_incomplete, days_given_up,
    skipped_reason)``. ``skipped_reason`` is ``"backfill"`` when a live
    backfill run blocked the whole pass, else ``None``.
    """
    if _backfill_is_live(cfg, now):
        return [], [], [], [], "backfill"

    today = now.date()
    # Oldest -> newest: today-WINDOW_DAYS .. today-1.
    window = [today - timedelta(days=n) for n in range(cfg.WINDOW_DAYS, 0, -1)]

    first_day = DailyMetric.objects.aggregate(Min("date"))["date__min"]
    if first_day is None:
        # Nothing has ever been written -- the nightly task writes the
        # first row; the sweeper has no floor to measure candidates against.
        return [], [], [], [], None
    window = [d for d in window if d >= first_day]
    if not window:
        return [], [], [], [], None

    candidates = select_failed_days(window)
    if not candidates:
        return [], [], [], [], None

    ensure_day_states(candidates)
    states = get_day_states(candidates)

    days_given_up: list[str] = []
    remaining: list[date] = []
    for day in candidates:
        state = states[day]
        if state.attempts >= cfg.MAX_ATTEMPTS:
            if mark_gave_up(day, now):
                logger.error(
                    "analytics.catchup.gave_up day=%s attempts=%s",
                    day,
                    state.attempts,
                )
            days_given_up.append(day.isoformat())
            continue
        remaining.append(day)

    due = [
        day
        for day in remaining
        if states[day].next_attempt_at is None or states[day].next_attempt_at <= now
    ]

    days_deferred: list[str] = []
    ready: list[date] = []
    for day in sorted(due):
        if status is None:
            mark_day_deferred(day, INDEXER_STATUS_UNAVAILABLE)
            log_day_deferred(day, None, INDEXER_STATUS_UNAVAILABLE)
            days_deferred.append(f"{day}:{INDEXER_STATUS_UNAVAILABLE}")
            continue
        try:
            ensure_day_settled(day, status)
        except DayNotReady as exc:
            mark_day_deferred(day, exc.code)
            log_day_deferred(day, status, exc.code)
            days_deferred.append(f"{day}:{exc.code}")
            continue
        ready.append(day)

    selected = ready[: cfg.MAX_DAYS_PER_RUN]

    days_fixed: list[str] = []
    days_incomplete: list[str] = []
    for day in selected:
        state = states[day]
        only = (
            tuple(state.failed_steps) if state.core_ok and state.failed_steps else None
        )
        record_attempt(day, now)
        try:
            result = compute_day(day, status, only=only)
        except Exception:
            # The attempt is already counted (record_attempt above, committed
            # separately) -- an ordinary exception escaping compute_day (e.g.
            # a DB error in the mark-clearing UPDATE) must not abort the rest
            # of this run: processing_stuck, expired and pruning still need
            # to run, and the summary line still needs to be logged. A
            # BaseException (SIGKILL, gevent Timeout) is deliberately left
            # to propagate, same as the nightly task.
            logger.exception("analytics.catchup: day %s failed", day)
            days_incomplete.append(day.isoformat())
            continue
        if result.status == DayStatus.DONE:
            days_fixed.append(day.isoformat())
            mark_day_succeeded(day)
        elif result.status == DayStatus.INCOMPLETE:
            days_incomplete.append(day.isoformat())
        else:
            # Defensive only: `day` already cleared the same gate against
            # this exact `status` snapshot above, so `compute_day` should
            # never re-defer it within the same run.
            days_deferred.append(f"{day}:{result.code}")

    if selected:
        _refresh_active_window(now)

    return days_fixed, days_deferred, days_incomplete, days_given_up, None


def _check_processing_stuck(cfg, now: datetime, status) -> None:
    """One ERROR, once, when the oldest unprocessed `InternalTxDecoded` row
    hasn't changed and the unprocessed count hasn't fallen for
    `PROCESSING_STUCK_HOURS` -- see `record_processing_observation` for the
    id/count bookkeeping this relies on. A `None` status, or an empty queue
    this run, touches nothing (neither resets nor logs) -- see the module
    docstring in `catchup/state.py`.
    """
    if status is None or status.oldest_unprocessed_ts is None:
        return

    # `observed_value` is the oldest unprocessed row's position, not its
    # database id -- reusing `status.oldest_unprocessed_ts` (already fetched
    # once for this run) keeps `get_indexer_status()` at exactly one query;
    # only the count below is a query the sweeper adds on top of it, against
    # the same partial index `get_indexer_status()` uses for the row itself.
    observed_value = int(status.oldest_unprocessed_ts.timestamp())
    count = InternalTxDecoded.objects.not_processed().count()

    state, reset = record_processing_observation(observed_value, count, now)
    if reset:
        return

    since = state.observed_since or now
    hours_stuck = (now - since).total_seconds() / 3600
    if hours_stuck < cfg.PROCESSING_STUCK_HOURS:
        return

    if mark_processing_stuck_once(now):
        logger.error(
            "analytics.catchup.processing_stuck oldest_ts=%s safe=%s since=%s hours=%.1f",
            status.oldest_unprocessed_ts.isoformat(),
            status.oldest_unprocessed_safe,
            since.isoformat(),
            hours_stuck,
        )


def _check_expired(cfg, now: datetime) -> list[str]:
    """ERROR, once per day, for any day in the grace window
    ``[today-WINDOW_DAYS-3, today-WINDOW_DAYS-1]`` that the sweeper has seen
    (has a ``kind="day"`` state row) and that still isn't ``completed_at``.
    A day without a state row was never a candidate the sweeper could have
    caught up -- first deploy, or an instance younger than the window --
    and is silently skipped, not reported as expired.
    """
    today = now.date()
    start = today - timedelta(days=cfg.WINDOW_DAYS + 3)
    end = today - timedelta(days=cfg.WINDOW_DAYS + 1)
    if start > end:
        return []

    keys = [
        (start + timedelta(days=n)).isoformat() for n in range((end - start).days + 1)
    ]
    states = {
        row.key: row
        for row in AnalyticsCatchupState.objects.filter(kind="day", key__in=keys)
    }
    if not states:
        return []

    completed = set(
        DailyMetric.objects.filter(
            date__range=(start, end), completed_at__isnull=False
        ).values_list("date", flat=True)
    )

    days_expired: list[str] = []
    for key, state in states.items():
        day = date.fromisoformat(key)
        if day in completed:
            continue
        days_expired.append(key)
        if mark_expired_once(day, now):
            logger.error(
                "analytics.catchup.expired day=%s last_state=%s code=%s attempts=%s",
                day,
                state.last_state,
                state.last_code,
                state.attempts,
            )
    return sorted(days_expired)


def run_catchup() -> None:
    """The sweeper's body -- see the module docstring for the ordered
    steps. Assumes the caller already holds whatever lock it wants; does
    not take one itself.
    """
    now = timezone.now()
    try:
        cfg = get_catchup_settings()
    except ImproperlyConfigured as exc:
        logger.error(
            "analytics.catchup.bad_config setting=%s reason=%s",
            _bad_config_setting(exc),
            "invalid_value",
        )
        return

    try:
        status = get_indexer_status()
    except DayNotReady:
        status = None

    days_fixed, days_deferred, days_incomplete, days_given_up, skipped_reason = (
        _catch_up_days(cfg, now, status)
    )
    _check_processing_stuck(cfg, now, status)
    days_expired = _check_expired(cfg, now)
    prune_day_states_before(now.date() - timedelta(days=2 * cfg.WINDOW_DAYS))

    snapshots_dispatched, snapshots_given_up = sweep_snapshots(cfg, now)

    logger.info(
        "analytics.catchup: days_fixed=%s days_deferred=%s days_incomplete=%s "
        "days_given_up=%s days_expired=%s skipped_reason=%s "
        "snapshots_dispatched=%s snapshots_given_up=%s",
        days_fixed,
        days_deferred,
        days_incomplete,
        days_given_up,
        days_expired,
        skipped_reason,
        snapshots_dispatched,
        snapshots_given_up,
    )
