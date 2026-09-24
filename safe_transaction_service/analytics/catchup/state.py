"""Persisted catch-up state for a single day -- ``AnalyticsCatchupState(kind="day")``
-- plus the sweeper-side helpers that create/retire those rows and the
single ``kind="processing"`` row the stuck-queue watchdog uses.

See "Analytics catch-up" in ``analytics/implementation-notes.md``. ``compute_day``
is the only writer of ``core_ok`` / ``failed_steps`` here, regardless of which
caller invoked it (nightly task, backfill shard, ``--inline`` backfill, or the
sweeper); the sweeper only reads them, to decide whether a retry should pass
``only=failed_steps``. A row is created only by the sweeper -- every function
here that would otherwise create a row for a day it never saw is a deliberate
no-op instead.

``last_state`` / ``last_code``, unlike ``core_ok`` / ``failed_steps``, are
written from two places: ``compute_day`` (via ``record_day_result``) when it
actually attempts a day, and the sweeper itself (``mark_gave_up`` /
``mark_day_deferred`` below) when it decides *not* to call ``compute_day`` at
all -- a day excluded for having given up, or for failing the gate. That is
what lets ``expired``'s log line report an accurate ``last_state`` even for a
day that has never once reached ``compute_day``.
"""

from datetime import date, datetime
from datetime import timedelta as _timedelta
from typing import TYPE_CHECKING

from django.db import transaction

from ..models import AnalyticsCatchupState

if TYPE_CHECKING:
    from .day import DayResult

_KIND = "day"

#: Not a `DayStatus` value -- `DayStatus` only covers outcomes `compute_day`
#: itself can produce (done/incomplete/deferred). "gave_up" is a sweeper-only
#: verdict: a day excluded from selection for having exhausted its attempts,
#: without ever reaching `compute_day` this run.
GAVE_UP = "gave_up"
#: Matches `DayStatus.DEFERRED.value` -- duplicated as a literal (not
#: imported from `.day`) because `day.py` already imports this module, and a
#: module-level import back would be circular.
_DEFERRED = "deferred"

_PROCESSING_KIND = "processing"
_PROCESSING_KEY = "oldest"

_ONE_HOUR = _timedelta(hours=1)


def get_day_state(day: date) -> AnalyticsCatchupState | None:
    """The existing ``AnalyticsCatchupState("day", day)`` row, or ``None`` if
    the sweeper hasn't created one for this day yet."""
    return AnalyticsCatchupState.objects.filter(kind=_KIND, key=day.isoformat()).first()


def get_day_states(days: list[date]) -> dict[date, AnalyticsCatchupState]:
    """Existing state rows for ``days``, keyed by date. Days without a row
    (never seen by the sweeper) are simply absent from the result."""
    rows = AnalyticsCatchupState.objects.filter(
        kind=_KIND, key__in=[d.isoformat() for d in days]
    )
    return {date.fromisoformat(row.key): row for row in rows}


def ensure_day_states(days: list[date]) -> None:
    """Create a bare ``AnalyticsCatchupState(kind="day")`` row for every day
    in ``days`` that doesn't already have one -- "the sweeper now knows about
    this day", the precondition for it ever counting towards ``expired``.
    Only the sweeper calls this; every other writer in this module only
    updates an existing row.
    """
    existing = set(
        AnalyticsCatchupState.objects.filter(
            kind=_KIND, key__in=[d.isoformat() for d in days]
        ).values_list("key", flat=True)
    )
    to_create = [
        AnalyticsCatchupState(kind=_KIND, key=d.isoformat())
        for d in days
        if d.isoformat() not in existing
    ]
    if to_create:
        AnalyticsCatchupState.objects.bulk_create(to_create)


def reset_core_ok(day: date) -> None:
    """Set ``core_ok=False`` on the existing row, as an immediate, separately
    committed write ahead of a run that recomputes the core -- so a run that
    dies partway through leaves ``core_ok=False`` behind rather than a stale
    ``True`` from an earlier, unrelated success. No-op if there is no row for
    this day.
    """
    AnalyticsCatchupState.objects.filter(kind=_KIND, key=day.isoformat()).update(
        core_ok=False
    )


def record_day_result(day: date, result: "DayResult") -> None:
    """Persist ``result`` onto the existing state row for ``day``.

    ``result.core_ok`` / ``result.failed`` already describe the day's
    current standing -- carried forward from the prior row when this call
    was a partial (``only=``) retry that didn't touch the core or a given
    populator (see ``_upsert_daily_metric`` in ``tasks.py``) -- so this is a
    plain write, not a merge. No-op if the sweeper hasn't created a row for
    this day: a day it has never seen has nothing to update.
    """
    AnalyticsCatchupState.objects.filter(kind=_KIND, key=day.isoformat()).update(
        core_ok=result.core_ok,
        failed_steps=list(result.failed),
        last_state=result.status.value,
        last_code=result.code,
    )


def record_attempt(day: date, now: datetime) -> int:
    """Bump the day's attempt counter and set its next-retry backoff, as one
    commit made *before* ``compute_day`` runs for it -- so a crash mid-compute
    (including a ``BaseException`` such as SIGKILL or a gevent ``Timeout``)
    still counts the attempt. Returns the new ``attempts`` value ``n``; the
    backoff is ``2**(n-1)`` hours, so attempts 1..5 space out 1, 2, 4, 8
    hours apart -- the last of 5 attempts lands at least 15h after the
    first. Returns 0 (no-op) if the day has no state row.
    """
    with transaction.atomic():
        state = (
            AnalyticsCatchupState.objects.select_for_update()
            .filter(kind=_KIND, key=day.isoformat())
            .first()
        )
        if state is None:
            return 0
        n = state.attempts + 1
        state.attempts = n
        state.next_attempt_at = now + (2 ** (n - 1)) * _ONE_HOUR
        state.save(update_fields=["attempts", "next_attempt_at"])
    return n


def mark_day_succeeded(day: date) -> None:
    """A day reached `DayStatus.DONE` -- delete its state row along with the
    attempt counter, backoff, failed-populator list and every marker."""
    AnalyticsCatchupState.objects.filter(kind=_KIND, key=day.isoformat()).delete()


def mark_gave_up(day: date, now: datetime) -> bool:
    """Stamp ``last_state="gave_up"`` on the day's row (every run it stays
    excluded, so ``expired`` reports it accurately even if it never reaches
    ``compute_day`` again) and fire the one-time ERROR marker.

    Returns ``True`` only the first time -- ``rowcount == 1`` on the
    ``WHERE gave_up_logged_at IS NULL`` update -- so callers log the ERROR
    exactly once per day, no matter how many runs keep finding it given up.
    """
    AnalyticsCatchupState.objects.filter(kind=_KIND, key=day.isoformat()).update(
        last_state=GAVE_UP
    )
    return (
        AnalyticsCatchupState.objects.filter(
            kind=_KIND, key=day.isoformat(), gave_up_logged_at__isnull=True
        ).update(gave_up_logged_at=now)
        == 1
    )


def mark_day_deferred(day: date, code: str) -> None:
    """The sweeper decided not to attempt ``day`` this run because the gate
    (or the indexer status itself) said it isn't ready. Recorded here --
    not by ``compute_day``, which is never called for this day this run --
    purely so a later ``expired`` log has an accurate ``last_state``."""
    AnalyticsCatchupState.objects.filter(kind=_KIND, key=day.isoformat()).update(
        last_state=_DEFERRED, last_code=code
    )


def mark_expired_once(day: date, now: datetime) -> bool:
    """Same one-time-marker pattern as ``mark_gave_up``, for
    ``expired_logged_at``."""
    return (
        AnalyticsCatchupState.objects.filter(
            kind=_KIND, key=day.isoformat(), expired_logged_at__isnull=True
        ).update(expired_logged_at=now)
        == 1
    )


def prune_day_states_before(cutoff: date) -> int:
    """Delete every ``kind="day"`` row older than ``cutoff`` -- the sweeper's
    housekeeping step, run every pass, for rows more than two sweep windows
    old (a day that old is long past ``expired`` and gave the alert its one
    ERROR already; keeping the row serves no purpose). Relies on ``key``
    being an ISO ``YYYY-MM-DD`` string, which sorts the same lexicographically
    as chronologically.
    """
    deleted, _ = AnalyticsCatchupState.objects.filter(
        kind=_KIND, key__lt=cutoff.isoformat()
    ).delete()
    return deleted


def record_processing_observation(
    observed_value: int, count: int, now: datetime
) -> tuple[AnalyticsCatchupState, bool]:
    """Update the single ``kind="processing"`` row with this run's oldest
    unprocessed-row observation.

    Returns ``(state, reset)``; ``reset`` is ``True`` when this call cleared
    ``observed_since`` / ``stuck_logged_at`` -- either the observed id
    changed (including the very first observation) or ``count`` dropped
    below the stored minimum -- in which case the caller must not log
    ``processing_stuck`` this run. When ``reset`` is ``False``, the stored
    minimum ``observed_count`` is deliberately left untouched (a growing or
    unchanged count doesn't move it), matching "the count never decreased"
    exactly.
    """
    state, created = AnalyticsCatchupState.objects.get_or_create(
        kind=_PROCESSING_KIND,
        key=_PROCESSING_KEY,
        defaults={
            "observed_value": observed_value,
            "observed_since": now,
            "observed_count": count,
        },
    )
    if created:
        return state, True

    changed_id = state.observed_value != observed_value
    dropped = state.observed_count is None or count < state.observed_count
    if changed_id or dropped:
        state.observed_value = observed_value
        state.observed_since = now
        state.observed_count = count
        state.stuck_logged_at = None
        state.save(
            update_fields=[
                "observed_value",
                "observed_since",
                "observed_count",
                "stuck_logged_at",
            ]
        )
        return state, True
    return state, False


def mark_processing_stuck_once(now: datetime) -> bool:
    """Same one-time-marker pattern as ``mark_gave_up``, for the single
    ``kind="processing"`` row's ``stuck_logged_at``."""
    return (
        AnalyticsCatchupState.objects.filter(
            kind=_PROCESSING_KIND, key=_PROCESSING_KEY, stuck_logged_at__isnull=True
        ).update(stuck_logged_at=now)
        == 1
    )


# ─────────────────────── snapshot bookkeeping (PR 2) ───────────────────────
#
# One row per snapshot name (`"summary"`, `"safe_segments"`, `"tvl"`),
# `kind="snapshot"`. Unlike a day's row, this one is entirely owned by the
# sweeper (`catchup/snapshots.py`): the three snapshot tasks themselves never
# touch `AnalyticsCatchupState` -- they only set the Redis in-flight mark
# described in `catchup/snapshots.py`.

_SNAPSHOT_KIND = "snapshot"
_ONE_DAY = _timedelta(hours=24)


def ensure_snapshot_state(name: str) -> AnalyticsCatchupState:
    """Get-or-create the sweeper's bookkeeping row for this snapshot name.
    ``first_seen_at`` is stamped by ``auto_now_add`` on creation."""
    state, _created = AnalyticsCatchupState.objects.get_or_create(
        kind=_SNAPSHOT_KIND, key=name
    )
    return state


def reset_stale_snapshot_attempts(
    name: str, state: AnalyticsCatchupState, now: datetime
) -> AnalyticsCatchupState:
    """A snapshot row older than 24h that still isn't fresh gets a clean
    slate -- new day, new attempts -- rather than staying permanently given
    up the way a day does. Resets the counter, the backoff, the one-time
    ``gave_up`` marker and the row's own clock (``first_seen_at``) together,
    so the next 24h window starts from this call. No-op (returns ``state``
    unchanged) when the row is younger than 24h.
    """
    first_seen = state.first_seen_at
    if first_seen is None or (now - first_seen) < _ONE_DAY:
        return state
    AnalyticsCatchupState.objects.filter(kind=_SNAPSHOT_KIND, key=name).update(
        attempts=0,
        next_attempt_at=None,
        gave_up_logged_at=None,
        first_seen_at=now,
    )
    state.attempts = 0
    state.next_attempt_at = None
    state.gave_up_logged_at = None
    state.first_seen_at = now
    return state


def record_snapshot_attempt(name: str, now: datetime) -> int:
    """Same shape as ``record_attempt``, for ``kind="snapshot"``: one commit,
    made before the sweeper dispatches the refresh task, so a crash between
    this call and the actual dispatch still counts the attempt. Returns the
    new ``attempts`` value, or 0 (no-op) if the row doesn't exist."""
    with transaction.atomic():
        state = (
            AnalyticsCatchupState.objects.select_for_update()
            .filter(kind=_SNAPSHOT_KIND, key=name)
            .first()
        )
        if state is None:
            return 0
        n = state.attempts + 1
        state.attempts = n
        state.next_attempt_at = now + (2 ** (n - 1)) * _ONE_HOUR
        state.save(update_fields=["attempts", "next_attempt_at"])
    return n


def mark_snapshot_gave_up(name: str, now: datetime) -> bool:
    """Same one-time-marker pattern as ``mark_gave_up``, for a snapshot's
    ``gave_up_logged_at``."""
    return (
        AnalyticsCatchupState.objects.filter(
            kind=_SNAPSHOT_KIND, key=name, gave_up_logged_at__isnull=True
        ).update(gave_up_logged_at=now)
        == 1
    )


def mark_snapshot_fresh(name: str) -> None:
    """A snapshot is fresh again -- delete its bookkeeping row along with
    the attempt counter, backoff and every marker, same as
    ``mark_day_succeeded``. No-op if there is no row."""
    AnalyticsCatchupState.objects.filter(kind=_SNAPSHOT_KIND, key=name).delete()
