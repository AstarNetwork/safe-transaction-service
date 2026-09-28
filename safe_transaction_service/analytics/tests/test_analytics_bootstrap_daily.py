"""Tests for the daily-metrics bootstrap stage (`analytics/bootstrap/daily.py`):
the sliding window, missing-days-only dispatch, stall detection/redispatch,
and the depth-growth reopen (tick step 2) applied to a real `DailyStage`.

Fixtures mirror `test_backfill_daily_metrics.py` (the Redis run-manifest
helpers, `DailyMetric` row creation) and `test_analytics_bootstrap.py` (the
tick-level `_SpyStage`, duplicated here rather than imported -- same
reasoning both of those files already give: no precedent in this suite for
sharing a test base across modules).
"""

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from django.test import TestCase, override_settings
from django.utils import timezone

from safe_transaction_service.analytics.bootstrap import Stage, run_tick
from safe_transaction_service.analytics.bootstrap.bookkeeping import mark_completed
from safe_transaction_service.analytics.bootstrap.daily import (
    DAILY_BACKFILL_STALE_SECONDS,
    DAILY_STAGE_CHUNK_DAYS,
    DailyStage,
)
from safe_transaction_service.analytics.catchup.day import DayResult, DayStatus
from safe_transaction_service.analytics.models import DailyMetric
from safe_transaction_service.analytics.tasks_shards import (
    BACKFILL_CURSOR_KEY,
    BACKFILL_RUN_KEY_PREFIX,
    DAILY_BACKFILL_LEASES_KEY_PREFIX,
    _acquire_daily_backfill_lease,
    _advance_backfill_run,
    _save_backfill_run,
    any_daily_backfill_lease_held,
    build_backfill_run,
    compute_daily_metric_shard,
    load_backfill_run,
)
from safe_transaction_service.utils.redis import get_redis

from .catchup_gate_fixture import SettledGateMixin

UPSERT_TARGET = "safe_transaction_service.analytics.tasks._upsert_daily_metric"
DONE_RESULT = DayResult(status=DayStatus.DONE, core_ok=True, failed=())


def _clear_backfill_keys() -> None:
    redis = get_redis()
    keys = list(redis.scan_iter(match=f"{BACKFILL_RUN_KEY_PREFIX}*"))
    keys.extend(redis.scan_iter(match=f"{DAILY_BACKFILL_LEASES_KEY_PREFIX}*"))
    keys.append(BACKFILL_CURSOR_KEY)
    redis.delete(*keys)


class DailyBootstrapRedisMixin:
    """Same Redis-hygiene shape `BackfillRedisMixin` uses in
    `test_backfill_daily_metrics.py`."""

    def setUp(self):
        super().setUp()
        _clear_backfill_keys()

    def tearDown(self):
        _clear_backfill_keys()
        super().tearDown()


def _create_completed_day(day) -> DailyMetric:
    return DailyMetric.objects.create(
        date=day, computed_at=timezone.now(), completed_at=timezone.now()
    )


def _window_dates(stage: DailyStage) -> list:
    start, end = stage._window()
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def _fabricate_manifest(dates, chunk_days: int, run_id: str, dispatched_at: datetime):
    """A run manifest with chunk 0 marked `"running"`, without executing
    anything -- mirrors `TestWaitViaRedis._start_without_executing` in
    `test_backfill_daily_metrics.py`. No heartbeat key and no lease is
    written, so the stage's grace-period check falls back to this
    `dispatched_at` (the same fallback chain
    `erc20_balance_backfill_looks_stalled` uses for a run whose first
    slice never got the chance to heartbeat).
    """
    run = build_backfill_run(dates, chunk_days, run_id=run_id)
    run["chunks"][0]["state"] = "running"
    run["chunks"][0]["dispatched_at"] = dispatched_at.isoformat()
    _save_backfill_run(run)
    get_redis().set(
        BACKFILL_CURSOR_KEY,
        json.dumps({"run_id": run["run_id"], "run_key": run["run_key"]}),
    )
    return run


def _fabricate_dispatch_failed_manifest(dates, chunk_days: int, run_id: str):
    """A run manifest whose chunk 0 failed to even reach the broker --
    what `_advance_backfill_run`'s exception branch leaves behind."""
    run = build_backfill_run(dates, chunk_days, run_id=run_id)
    run["chunks"][0]["state"] = "dispatch_failed"
    run["chunks"][0]["error"] = "boom"
    run["finished_at"] = timezone.now().isoformat()
    _save_backfill_run(run)
    get_redis().set(
        BACKFILL_CURSOR_KEY,
        json.dumps({"run_id": run["run_id"], "run_key": run["run_key"]}),
    )
    return run


def _fabricate_finished_manifest(dates, chunk_days: int, run_id: str):
    """A run manifest whose every chunk finished normally -- not
    `dispatch_failed`, not superseded -- what `_advance_backfill_run`
    leaves behind once every chunk's chord callback has run. Whether any
    of `dates` actually got `completed_at` is up to the caller (that's
    the whole point: a finished run can still have left one of its own
    days incomplete).
    """
    run = build_backfill_run(dates, chunk_days, run_id=run_id)
    for chunk in run["chunks"]:
        chunk["state"] = "done"
    run["finished_at"] = timezone.now().isoformat()
    _save_backfill_run(run)
    get_redis().set(
        BACKFILL_CURSOR_KEY,
        json.dumps({"run_id": run["run_id"], "run_key": run["run_key"]}),
    )
    return run


class _SpyStage(Stage):
    """A stage the tick can be handed directly -- same shape as
    `test_analytics_bootstrap.py`'s own `_SpyStage`, duplicated per that
    file's docstring."""

    def __init__(self, name, done=True, status="done", resumed_externally=False):
        self.name = name
        self._done = done
        self._status = status
        self.resumed_externally = resumed_externally
        self.start_or_resume_called = False

    def is_done(self):
        return self._done

    def status(self):
        return self._status

    def start_or_resume(self):
        self.start_or_resume_called = True


DISPATCH_TARGET = (
    "safe_transaction_service.analytics.bootstrap.daily.start_backfill_run"
)


# ═══════════════════ Missing-days-only dispatch, done, slide ═══════════════


@override_settings(ANALYTICS_BOOTSTRAP_DAILY_DAYS=10)
class TestOnlyMissingDaysAreDispatched(DailyBootstrapRedisMixin, TestCase):
    def test_dispatch_gets_only_the_days_without_completed_at(self):
        stage = DailyStage()
        window = _window_dates(stage)
        already_done = set(window[:3])
        for day in already_done:
            _create_completed_day(day)
        expected_missing = [day for day in window if day not in already_done]

        self.assertFalse(stage.is_done())
        with patch(DISPATCH_TARGET) as dispatch_spy:
            stage.start_or_resume()

        dispatch_spy.assert_called_once_with(expected_missing, DAILY_STAGE_CHUNK_DAYS)


@override_settings(ANALYTICS_BOOTSTRAP_DAILY_DAYS=5)
class TestHandBackfilledWindowCountsAsDone(TestCase):
    def test_every_day_with_completed_at_already_set_is_done(self):
        stage = DailyStage()
        for day in _window_dates(stage):
            _create_completed_day(day)

        self.assertTrue(stage.is_done())
        self.assertEqual(stage.status(), "done")
        self.assertEqual(stage.progress(), {"completed_days": 5, "total_days": 5})

    def test_a_fresh_instance_with_no_rows_is_not_done(self):
        stage = DailyStage()
        self.assertFalse(stage.is_done())
        self.assertEqual(stage.progress(), {"completed_days": 0, "total_days": 5})


@override_settings(ANALYTICS_BOOTSTRAP_DAILY_DAYS=5)
class TestWindowSlidesWithToday(TestCase):
    def test_is_done_and_progress_depend_on_today(self):
        stage = DailyStage()
        day_zero = datetime(2026, 6, 20, 12, 0, tzinfo=UTC)

        with patch(
            "safe_transaction_service.analytics.bootstrap.daily.timezone.now",
            return_value=day_zero,
        ):
            start, end = stage._window()
            for offset in range((end - start).days + 1):
                _create_completed_day(start + timedelta(days=offset))
            self.assertTrue(stage.is_done())

        # A day later the window has slid forward by exactly one day: the
        # oldest previously-completed day drops out and a new day (with no
        # completed_at yet) enters at the other end.
        with patch(
            "safe_transaction_service.analytics.bootstrap.daily.timezone.now",
            return_value=day_zero + timedelta(days=1),
        ):
            new_start, new_end = stage._window()
            self.assertEqual(new_start, start + timedelta(days=1))
            self.assertEqual(new_end, end + timedelta(days=1))
            self.assertFalse(stage.is_done())
            self.assertEqual(stage.progress(), {"completed_days": 4, "total_days": 5})


@override_settings(ANALYTICS_BOOTSTRAP_DAILY_DAYS=3)
class TestZeroActivityDaysStillReachDone(
    SettledGateMixin, DailyBootstrapRedisMixin, TestCase
):
    """A window of days with no chain activity at all
    (a network younger than the window, or genuinely quiet days) must
    still get `completed_at` written -- otherwise `is_done()` never
    becomes true and the stage redispatches forever. Runs the REAL
    dispatch chain (no `_upsert_daily_metric` patch): the test DB has no
    `history_ethereumblock` / `SafeContract` rows at all, so every day in
    the window is an authentic zero-activity day, and each populator's
    `_resolve_block_window`-driven "no blocks in the window" branch
    (`tasks.py`) is what's actually being exercised."""

    def test_a_window_of_zero_activity_days_reaches_done_after_one_dispatch(self):
        stage = DailyStage()
        self.assertFalse(stage.is_done())

        stage.start_or_resume()  # CELERY_ALWAYS_EAGER: runs the whole chain inline

        self.assertTrue(stage.is_done())
        self.assertEqual(stage.status(), "done")


# ═══════════════════ Stall detection and redispatch ════════════════════════


@override_settings(ANALYTICS_BOOTSTRAP_DAILY_DAYS=10)
class TestLeaseDecidesLivenessNotElapsedTime(DailyBootstrapRedisMixin, TestCase):
    """Liveness, not elapsed time, decides "stalled": a run with
    a live day-shard lease is `"running"` no matter how old its manifest
    timestamps are -- a heavy day must never be superseded for being
    slow."""

    def test_a_held_lease_keeps_the_run_running_even_3h_after_dispatch(self):
        stage = DailyStage()
        dates = _window_dates(stage)
        very_old = timezone.now() - timedelta(hours=3)
        run = _fabricate_manifest(
            dates, DAILY_STAGE_CHUNK_DAYS, "run-heavy-day", very_old
        )
        _acquire_daily_backfill_lease(run["run_id"], dates[0].isoformat())

        self.assertEqual(stage.status(), "running")
        with patch(DISPATCH_TARGET) as dispatch_spy:
            stage.start_or_resume()
        dispatch_spy.assert_not_called()

    def test_no_lease_past_the_grace_period_is_stalled_and_superseded_exactly_once(
        self,
    ):
        stage = DailyStage()
        dates = _window_dates(stage)
        stale_at = timezone.now() - timedelta(seconds=DAILY_BACKFILL_STALE_SECONDS + 60)
        run = _fabricate_manifest(dates, DAILY_STAGE_CHUNK_DAYS, "run-dead", stale_at)

        self.assertEqual(stage.status(), "stalled")
        with patch(DISPATCH_TARGET) as dispatch_spy:
            stage.start_or_resume()

        dispatch_spy.assert_called_once_with(dates, DAILY_STAGE_CHUNK_DAYS)
        superseded = load_backfill_run(run["run_id"])
        self.assertTrue(superseded["superseded"])
        # The old run's chunk hand-off is now inert: `backfill_done`
        # advancing it must refuse to dispatch a further chunk.
        with patch(
            "safe_transaction_service.analytics.tasks_shards._dispatch_backfill_chunk"
        ) as redispatch_spy:
            _advance_backfill_run(
                run["run_id"],
                0,
                {
                    "finished_at": timezone.now().isoformat(),
                    "total": 1,
                    "written": 1,
                    "failed": 0,
                    "failures": [],
                },
            )
        redispatch_spy.assert_not_called()

    def test_an_expired_or_missing_manifest_is_pending_not_stalled(self):
        """No manifest at all (never dispatched, or evicted past the 7-day
        TTL) reports `"pending"` -- `start_or_resume()` still redispatches
        the missing days exactly as it would from `"stalled"`."""
        stage = DailyStage()
        self.assertIsNone(get_redis().get(BACKFILL_CURSOR_KEY))

        self.assertEqual(stage.status(), "pending")
        with patch(DISPATCH_TARGET) as dispatch_spy:
            stage.start_or_resume()
        dispatch_spy.assert_called_once()


@override_settings(ANALYTICS_BOOTSTRAP_DAILY_DAYS=10)
class TestDispatchFailureIsFailedNotStalled(DailyBootstrapRedisMixin, TestCase):
    def test_a_dispatch_failed_chunk_is_reported_failed(self):
        stage = DailyStage()
        dates = _window_dates(stage)
        _fabricate_dispatch_failed_manifest(
            dates, DAILY_STAGE_CHUNK_DAYS, "run-broker-down"
        )

        self.assertEqual(stage.status(), "failed")

    def test_failed_still_falls_through_to_a_fresh_dispatch(self):
        """The retry cap lives in the tick, not the stage: a `"failed"`
        stage redispatches its missing days exactly like `"pending"`."""
        stage = DailyStage()
        dates = _window_dates(stage)
        _fabricate_dispatch_failed_manifest(
            dates, DAILY_STAGE_CHUNK_DAYS, "run-broker-down"
        )

        with patch(DISPATCH_TARGET) as dispatch_spy:
            stage.start_or_resume()
        dispatch_spy.assert_called_once_with(dates, DAILY_STAGE_CHUNK_DAYS)


@override_settings(ANALYTICS_BOOTSTRAP_DAILY_DAYS=5)
class TestFinishedRunWithAnOwnDayIncompleteIsFailed(DailyBootstrapRedisMixin, TestCase):
    """A day that always fails (hits the shard's hard time limit or
    statement timeout, comes back `{"ok": False, ...}`) must not read as
    `"pending"` forever -- that would be an infinite redispatch loop."""

    def test_a_day_the_run_covered_but_never_completed_gives_failed(self):
        stage = DailyStage()
        dates = _window_dates(stage)
        _fabricate_finished_manifest(dates, DAILY_STAGE_CHUNK_DAYS, "run-partial")
        for day in dates[:-1]:
            _create_completed_day(day)
        # dates[-1] deliberately left without `completed_at`.

        self.assertFalse(stage.is_done())
        self.assertEqual(stage.status(), "failed")

    def test_a_day_outside_the_run_that_slid_into_the_window_is_pending_not_failed(
        self,
    ):
        stage = DailyStage()
        day_zero = datetime(2026, 6, 20, 12, 0, tzinfo=UTC)

        with patch(
            "safe_transaction_service.analytics.bootstrap.daily.timezone.now",
            return_value=day_zero,
        ):
            dates = _window_dates(stage)
            _fabricate_finished_manifest(dates, DAILY_STAGE_CHUNK_DAYS, "run-complete")
            for day in dates:
                _create_completed_day(day)
            self.assertTrue(stage.is_done())

        # A day later: the window has slid forward by one day, so it now
        # includes a new "yesterday" this run never attempted (it isn't
        # in any of the run's own chunks' `days` lists) and which has no
        # `completed_at` yet -- that day is simply pending, not this run's
        # failure.
        with patch(
            "safe_transaction_service.analytics.bootstrap.daily.timezone.now",
            return_value=day_zero + timedelta(days=1),
        ):
            self.assertFalse(stage.is_done())
            self.assertEqual(stage.status(), "pending")


class TestShardLeaseLifecycle(SettledGateMixin, DailyBootstrapRedisMixin, TestCase):
    """`compute_daily_metric_shard` acquires the lease at start and
    releases it in a `finally`, on both the success and the exception
    path."""

    def test_lease_is_released_after_a_successful_shard(self):
        run_id, day_iso = "lease-ok", "2026-06-10"
        with patch(UPSERT_TARGET, return_value=DONE_RESULT):
            compute_daily_metric_shard(day_iso, run_id=run_id)
        self.assertFalse(any_daily_backfill_lease_held(run_id))

    def test_lease_is_released_even_when_the_shard_raises(self):
        run_id, day_iso = "lease-boom", "2026-06-11"
        with patch(UPSERT_TARGET, side_effect=RuntimeError("boom")):
            result = compute_daily_metric_shard(day_iso, run_id=run_id)
        self.assertFalse(result["ok"])
        self.assertFalse(any_daily_backfill_lease_held(run_id))


class TestTickRedispatchesStalledStagesItResumesItself(SettledGateMixin, TestCase):
    """Task contract change (`stage.py`, `tick.py` step 4): a stage with
    `resumed_externally = False` (the default -- `DailyStage`'s case) is
    resumed by the tick itself when stalled; one with `True` (`Erc20Stage`'s
    case) is left alone."""

    def test_stalled_and_resumed_externally_false_is_resumed_by_the_tick(self):
        stage = _SpyStage(
            "daily", done=False, status="stalled", resumed_externally=False
        )
        run_tick(stages=[stage])
        self.assertTrue(stage.start_or_resume_called)

    def test_stalled_and_resumed_externally_true_is_left_alone_by_the_tick(self):
        stage = _SpyStage(
            "erc20", done=False, status="stalled", resumed_externally=True
        )
        run_tick(stages=[stage])
        self.assertFalse(stage.start_or_resume_called)


# ═══════════════════ Depth growth reopens daily only (tick step 2) ═════════


class TestDepthGrowthReopensDailyOnly(SettledGateMixin, TestCase):
    def test_raising_depth_after_completion_dispatches_only_the_older_missing_days(
        self,
    ):
        old_depth = 5
        new_depth = 8
        # All three stages complete at `old_depth`. Once the depth grows,
        # `maybe_reopen_daily` clears ONLY daily's row -- native/ERC-20
        # keep their own `completed_at`, so the tick's terminal-row-first
        # loop skips them and reaches daily first, without any
        # special-cased candidate restriction.
        mark_completed("daily", depth=old_depth)
        mark_completed("native")
        mark_completed("erc20")

        with override_settings(ANALYTICS_BOOTSTRAP_DAILY_DAYS=old_depth):
            old_start, old_end = DailyStage()._window()
        # The previously-completed window: every day already has
        # `completed_at`, simulating what the earlier bootstrap run (or a
        # hand backfill) left behind.
        for offset in range((old_end - old_start).days + 1):
            _create_completed_day(old_start + timedelta(days=offset))

        erc20_fake = _SpyStage("erc20", done=False, status="pending")

        with override_settings(ANALYTICS_BOOTSTRAP_DAILY_DAYS=new_depth):
            new_start, new_end = DailyStage()._window()
            with patch(DISPATCH_TARGET) as dispatch_spy:
                run_tick(stages=[DailyStage(), erc20_fake])

        expected_missing = [
            new_start + timedelta(days=offset)
            for offset in range((old_start - new_start).days)
        ]
        dispatch_spy.assert_called_once_with(expected_missing, DAILY_STAGE_CHUNK_DAYS)
        self.assertFalse(erc20_fake.start_or_resume_called)
        self.assertEqual(new_end, old_end)  # the right edge never moves
