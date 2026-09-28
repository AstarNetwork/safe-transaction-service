"""The daily-metrics stage.

Window: the last ``ANALYTICS_BOOTSTRAP_DAILY_DAYS`` complete UTC days,
``[today - N, yesterday]``, recomputed on every call so it slides with
the calendar and reopens when the depth grows. "Done" comes from
``DailyMetric.completed_at``, never a flag of our own; dispatch reuses
``tasks_shards.start_backfill_run``, which already chunks an arbitrary
date list.
"""

import logging
from datetime import date, timedelta

from django.conf import settings
from django.utils import timezone

from safe_transaction_service.analytics.catchup import select_failed_days
from safe_transaction_service.analytics.models import DailyMetric
from safe_transaction_service.analytics.tasks_shards import (
    any_daily_backfill_lease_held,
    backfill_heartbeat_key,
    latest_backfill_run_id,
    load_backfill_run,
    manifest_age_seconds,
    start_backfill_run,
    supersede_backfill_run,
)
from safe_transaction_service.utils.redis import get_redis

from .stage import Stage

logger = logging.getLogger(__name__)

#: Mirrors `backfill_daily_metrics`'s own `--chunk-days` default, so the
#: bootstrap throttles itself the way an operator's own `--celery` run
#: would.
DAILY_STAGE_CHUNK_DAYS = 7

#: Liveness, not elapsed time, decides "stalled": while ANY day-shard
#: lease of the run exists, `status()` reports `"running"` however long
#: the run has taken -- a heavy day or a busy worker must never be
#: superseded just for being slow. This constant is NOT that timeout; it
#: is only the grace period between a chunk being dispatched (or a
#: shard's last heartbeat) and its first shard actually acquiring a
#: lease, covering the dispatch -> broker -> worker pickup gap before
#: there is any lease to check yet.
DAILY_BACKFILL_STALE_SECONDS = 5 * 60


def _dates_in_range(start: date, end: date) -> list[date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def _run_has_dispatch_failure(run: dict) -> bool:
    """Whether any chunk of `run` stopped because it could not even be
    dispatched to the broker (`_dispatch_backfill_chunk` raised inside
    `_advance_backfill_run`, e.g. the broker was down) -- a genuine
    run-level failure, mapped by `status()` to `"failed"`.
    """
    return any(
        chunk.get("state") == "dispatch_failed" for chunk in run.get("chunks", [])
    )


def _run_own_days(run: dict) -> set[date]:
    """Every day this run's chunks ever listed, across the whole run --
    not just the current window. Scopes `_run_has_incomplete_day` to days
    the run actually attempted, so a day that slid into the window only
    AFTER this run started is never blamed on it.
    """
    return {
        date.fromisoformat(day_iso)
        for chunk in run.get("chunks", [])
        for day_iso in chunk.get("days", [])
    }


def _run_has_incomplete_day(run: dict) -> bool:
    """Whether any day `run` itself covered still lacks `completed_at`.

    A finished, non-superseded run that still left one of its OWN days
    incomplete (a shard hit the hard time limit or the statement timeout
    and came back `{"ok": False, ...}`) is a genuine failure, not merely
    "pending" -- reporting it as `"pending"` forever would have the tick
    redispatch the same day, fail the same way, forever, with no
    `"failed"` for the retry cap to count against. One query against the
    run's own day list only, so a day that slid into the window after
    this run started doesn't count.
    """
    run_days = _run_own_days(run)
    if not run_days:
        return False
    completed_days = set(
        DailyMetric.objects.filter(
            date__in=run_days, completed_at__isnull=False
        ).values_list("date", flat=True)
    )
    return not run_days.issubset(completed_days)


def _run_reference_age_seconds(run: dict) -> float | None:
    """Age of `run`'s last known activity, for the grace period
    (`DAILY_BACKFILL_STALE_SECONDS`) only -- not the liveness check
    itself, which is `any_daily_backfill_lease_held()`. Tried in order:
    the heartbeat key, the currently-running chunk's `dispatched_at`, the
    run's `started_at` -- a manifest is always judged against *some*
    timestamp, never treated as eternally fresh.
    """
    heartbeat_raw = get_redis().get(backfill_heartbeat_key(run["run_id"]))
    if heartbeat_raw:
        if isinstance(heartbeat_raw, bytes):
            heartbeat_raw = heartbeat_raw.decode()
        age = manifest_age_seconds(heartbeat_raw)
        if age is not None:
            return age

    running_chunk = next(
        (chunk for chunk in run.get("chunks", []) if chunk.get("state") == "running"),
        None,
    )
    reference = (running_chunk or {}).get("dispatched_at") or run.get("started_at")
    return manifest_age_seconds(reference) if reference else None


class DailyStage(Stage):
    name = "daily"
    # Unlike `Erc20Stage`, nothing external resumes a stalled daily run --
    # this stage's own `start_or_resume()` redispatches the missing days
    # itself, so the tick must NOT no-op on `"stalled"` here. Left as the
    # base class default (`False`) explicitly, for the reader coming from
    # `erc20.py`.
    resumed_externally = False

    def _window(self) -> tuple[date, date]:
        """`[today - N, yesterday]` for the currently configured depth --
        recomputed from live settings/clock on every call, never cached
        (see the module docstring)."""
        today = timezone.now().date()
        depth = settings.ANALYTICS_BOOTSTRAP_DAILY_DAYS
        end = today - timedelta(days=1)
        start = today - timedelta(days=depth)
        return start, end

    def _completed_and_total(self) -> tuple[int, int]:
        """One aggregate query: how many days in the current window
        already have `completed_at`, out of how many the window holds."""
        start, end = self._window()
        total_days = (end - start).days + 1
        completed = DailyMetric.objects.filter(
            date__range=(start, end), completed_at__isnull=False
        ).count()
        return completed, total_days

    def is_done(self) -> bool:
        completed, total_days = self._completed_and_total()
        return completed >= total_days

    def progress(self) -> dict[str, int]:
        """`{"completed_days", "total_days"}` for `/summary/`'s
        `bootstrap` object; not used by the tick itself."""
        completed, total_days = self._completed_and_total()
        return {"completed_days": completed, "total_days": total_days}

    def _current_run(self) -> dict | None:
        """The latest daily-backfill run manifest, or `None` if there has
        never been one, it expired, or the cursor key is empty. Shared by
        `status()` and `start_or_resume()` so both act on the same read."""
        run_id = latest_backfill_run_id()
        return load_backfill_run(run_id) if run_id else None

    def status(self) -> str:
        """`"done"` / `"running"` / `"stalled"` / `"failed"` / `"pending"`.

        Liveness, not elapsed time, decides `"running"` vs `"stalled"`:
        any held lease means `"running"`; past `DAILY_BACKFILL_STALE_SECONDS`
        with none held is `"stalled"`. A finished run with
        `_run_has_dispatch_failure`/`_run_has_incomplete_day` is `"failed"`.
        """
        if self.is_done():
            return "done"

        run = self._current_run()
        if run is None:
            return "pending"
        if run.get("finished_at") is not None:
            if run.get("superseded"):
                return "pending"
            if _run_has_dispatch_failure(run) or _run_has_incomplete_day(run):
                return "failed"
            return "pending"

        if any_daily_backfill_lease_held(run["run_id"]):
            return "running"

        age = _run_reference_age_seconds(run)
        if age is None:
            return "pending"
        if age >= DAILY_BACKFILL_STALE_SECONDS:
            return "stalled"
        return "running"

    def start_or_resume(self) -> None:
        """Dispatch the days in the current window that are still missing
        `completed_at`, as one fresh `start_backfill_run`. Reached with
        `status() in ("pending", "stalled", "failed")` -- `"failed"`
        falls through and redispatches exactly like `"pending"`.

        A `"stalled"` manifest is superseded first
        (`tasks_shards.supersede_backfill_run`): the old chord may only be
        SLOW, not dead, so without this its own next-chunk hand-off would
        still fire once it finally completes, running two daily backfills
        at once. Superseding first means that hand-off now refuses to
        dispatch, regardless of how the race between "we decide to
        supersede" and "the old chord finishes its current chunk"
        resolves.

        The re-check of `status()` below guards only the race between the
        tick's read and this call, the same redundancy `Erc20Stage.start_or_resume()`
        has -- it is not a second resumption mechanism.
        """
        current_status = self.status()
        if current_status in ("done", "running"):
            return

        if current_status == "stalled":
            stale_run = self._current_run()
            if stale_run is not None:
                supersede_backfill_run(stale_run["run_id"])
                logger.warning(
                    "analytics.bootstrap.daily: superseding stalled run=%s "
                    "before starting a fresh one",
                    stale_run["run_id"],
                )

        start, end = self._window()
        missing = select_failed_days(_dates_in_range(start, end))
        if not missing:
            return  # race guard: the window finished between the two checks

        logger.warning(
            "analytics.bootstrap.daily: dispatching %d missing day(s) "
            "(window %s -> %s, was %s)",
            len(missing),
            start.isoformat(),
            end.isoformat(),
            current_status,
        )
        start_backfill_run(missing, DAILY_STAGE_CHUNK_DAYS)
