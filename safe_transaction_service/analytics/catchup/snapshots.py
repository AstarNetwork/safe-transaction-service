"""Sweeper for the three current-state snapshots -- ``summary``,
``safe_segments`` and ``tvl`` -- called from ``run_catchup()``.

See "Analytics catch-up" in ``analytics/implementation-notes.md``.

Unlike a day, a snapshot has no ``compute_day``-style shared entry point:
the nightly beat schedule and this sweeper both just call one of the three
existing snapshot tasks (``compute_summary_task`` / ``compute_safe_segments_task``
/ ``compute_tvl_task`` in ``tasks.py``). Two independent mechanisms keep them
from duplicating each other's work:

- **The in-flight mark**, a plain Redis key with a TTL
  (``analytics:snapshot:dispatched:<name>``, ``EX 10800`` -- 3h, comfortably
  longer than the TVL chord can run). Each of the three tasks sets it
  themselves at the very start of a run, whoever triggered it (beat or this
  sweeper). ``compute_tvl_task`` in particular releases its
  ``only_one_running_task`` lock right after dispatching the finalize chord
  and returns -- without this mark, nothing would stop the sweeper from
  dispatching a second TVL run while the first chord is still reducing.
  This sweeper never dispatches while the mark is set, and sets it itself
  (via ``SET NX``, to close the small race against a task that started
  between the check and the dispatch) right before calling ``.delay()``.
- **``AnalyticsCatchupState(kind="snapshot", key=<name>)``**, this sweeper's
  own attempt/backoff bookkeeping -- entirely separate from the in-flight
  mark and never touched by the tasks themselves.

Doesn't import ``tasks.py`` at module level -- the package's no-eager-import
rule (see ``catchup/__init__.py``) applies here too.
"""

import logging
from datetime import datetime

from ..conf import CatchupSettings
from ..models import AnalyticsSnapshot
from .state import (
    ensure_snapshot_state,
    mark_snapshot_fresh,
    mark_snapshot_gave_up,
    record_snapshot_attempt,
    reset_stale_snapshot_attempts,
)

logger = logging.getLogger(__name__)

#: Order is fixed and arbitrary but stable, for a deterministic summary line.
SNAPSHOT_NAMES = ("summary", "safe_segments", "tvl")

_IN_FLIGHT_PREFIX = "analytics:snapshot:dispatched:"
#: Longer than any of the three tasks' own Celery lock: `compute_tvl_task`
#: releases its lock as soon as the finalize chord is dispatched, not when
#: the chord itself finishes reducing.
_IN_FLIGHT_TTL_SECONDS = 10800
#: Matches `AnalyticsService._maybe_dispatch_refresh`'s cold-read lock key
#: (`services/analytics_service.py`) -- a view-triggered refresh in flight
#: also means "don't dispatch again".
_REFRESH_LOCK_SUFFIX = ":refresh_lock"


def _in_flight_key(name: str) -> str:
    return f"{_IN_FLIGHT_PREFIX}{name}"


def mark_snapshot_dispatched(name: str) -> None:
    """Set the in-flight mark -- called by each of the three snapshot tasks
    themselves, at the start of every run, regardless of what triggered it.
    """
    _redis().set(_in_flight_key(name), "1", ex=_IN_FLIGHT_TTL_SECONDS)


def _redis():
    from safe_transaction_service.utils.redis import get_redis

    return get_redis()


def _is_in_flight(name: str) -> bool:
    return bool(_redis().exists(_in_flight_key(name)))


def _claim_in_flight(name: str) -> bool:
    """``SET NX`` version of the mark, used only by this sweeper right
    before it dispatches -- ``True`` if this call won the race and set it,
    ``False`` if a task's own start-of-run mark got there first."""
    return bool(
        _redis().set(_in_flight_key(name), "1", nx=True, ex=_IN_FLIGHT_TTL_SECONDS)
    )


def _is_refresh_locked(name: str) -> bool:
    return bool(_redis().exists(f"analytics_snapshot:{name}{_REFRESH_LOCK_SUFFIX}"))


def is_snapshot_stale(name: str, stale_hours: int, now: datetime) -> bool:
    """No row at all, or ``computed_at`` older than ``SNAPSHOT_STALE_HOURS``,
    or -- ``tvl`` only -- the phase-1 placeholder marker
    (``payload["native_source"] is None``; a normal rollup run can
    legitimately write ``0/0`` shards too, so that pair alone is *not* the
    discriminator -- see ``compute_tvl_task`` in ``tasks.py``).

    Public: also used by ``warm_analytics_cache --skip-if-fresh`` so both
    the sweeper and the command decide freshness the same way.
    """
    try:
        snap = AnalyticsSnapshot.objects.get(name=name)
    except AnalyticsSnapshot.DoesNotExist:
        return True
    if name == "tvl" and snap.payload.get("native_source") is None:
        return True
    age_hours = (now - snap.computed_at).total_seconds() / 3600
    return age_hours >= stale_hours


def _dispatch_task(name: str):
    # Lazy: `tasks.py` imports this package eagerly (it needs
    # `mark_snapshot_dispatched`), so a module-level import back here would
    # be circular.
    from .. import tasks

    return {
        "summary": tasks.compute_summary_task,
        "safe_segments": tasks.compute_safe_segments_task,
        "tvl": tasks.compute_tvl_task,
    }[name]


def sweep_snapshots(cfg: CatchupSettings, now: datetime) -> tuple[list[str], list[str]]:
    """One pass over the three snapshots. Returns ``(dispatched, given_up)``,
    each a list of snapshot names in ``SNAPSHOT_NAMES`` order.

    For each snapshot, in order:
    - fresh -> clear any leftover bookkeeping row and move on;
    - stale, but the in-flight mark or the view's ``refresh_lock`` is set ->
      leave it alone this run (something is already refreshing it);
    - stale and clear to dispatch -> a row older than 24h without a fresh
      snapshot gets its attempts reset first (new day, new attempts); then,
      given up (``attempts >= MAX_ATTEMPTS``) -> log once, record in
      ``given_up``, no dispatch; not yet due (``next_attempt_at`` in the
      future) -> skip quietly; otherwise -> bump the attempt counter, claim
      the in-flight mark, dispatch.
    """
    dispatched: list[str] = []
    given_up: list[str] = []

    for name in SNAPSHOT_NAMES:
        if not is_snapshot_stale(name, cfg.SNAPSHOT_STALE_HOURS, now):
            mark_snapshot_fresh(name)
            continue

        if _is_in_flight(name) or _is_refresh_locked(name):
            continue

        state = ensure_snapshot_state(name)
        state = reset_stale_snapshot_attempts(name, state, now)

        if state.attempts >= cfg.MAX_ATTEMPTS:
            if mark_snapshot_gave_up(name, now):
                logger.error(
                    "analytics.catchup.gave_up snapshot=%s attempts=%s",
                    name,
                    state.attempts,
                )
            given_up.append(name)
            continue

        if state.next_attempt_at is not None and state.next_attempt_at > now:
            continue

        record_snapshot_attempt(name, now)
        if not _claim_in_flight(name):
            # A task's own start-of-run mark won a tight race in between --
            # that run already covers this pass; don't dispatch a second one.
            continue

        _dispatch_task(name).delay()
        dispatched.append(name)

    return dispatched, given_up
