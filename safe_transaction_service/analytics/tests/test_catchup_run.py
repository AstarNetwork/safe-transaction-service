"""Tests for the hourly sweeper -- ``run_catchup()``
(``analytics/catchup/sweeper.py``) and its thin task wrapper
``analytics_catchup_task`` (``analytics/tasks.py``).

See "Analytics catch-up" in ``analytics/implementation-notes.md``.

Constants below (``WINDOW_DAYS=14``, ``MAX_ATTEMPTS=5``,
``BACKFILL_STALE_HOURS=6``, ``PROCESSING_STUCK_HOURS=6``) mirror
``conf.get_catchup_settings()``'s defaults -- ``.env.test`` doesn't override
any ``ANALYTICS_CATCHUP_*`` variable, so the real settings match these
literals without needing to read them back dynamically.
"""

import dataclasses
import json
from datetime import UTC, date, datetime, timedelta
from unittest.mock import patch

from django.db.utils import OperationalError
from django.test import TestCase, override_settings

from celery import current_app as celery_app
from redis.exceptions import LockError

from safe_transaction_service.analytics import tasks_shards
from safe_transaction_service.analytics.catchup.gate import (
    DAY_NOT_READY,
    INDEXER_STATUS_UNAVAILABLE,
    DayNotReady,
    IndexerStatus,
)
from safe_transaction_service.analytics.catchup.sweeper import run_catchup
from safe_transaction_service.analytics.conf import reset_catchup_settings_cache
from safe_transaction_service.analytics.models import AnalyticsCatchupState, DailyMetric
from safe_transaction_service.analytics.tasks import (
    analytics_catchup_task,
    compute_daily_metrics_task,
)
from safe_transaction_service.utils.redis import get_redis
from safe_transaction_service.utils.tasks import LOCK_TIMEOUT

from .catchup_gate_fixture import SettledGateMixin

SWEEPER_LOGGER = "safe_transaction_service.analytics.catchup.sweeper"
TASKS_LOGGER = "safe_transaction_service.analytics.tasks"
SWEEPER_GATE = "safe_transaction_service.analytics.catchup.sweeper.get_indexer_status"

WINDOW_DAYS = 14
MAX_ATTEMPTS = 5
BACKFILL_STALE_HOURS = 6
PROCESSING_STUCK_HOURS = 6

NOW = datetime(2026, 9, 23, 0, 20, tzinfo=UTC)


def _seed_floor(day: date) -> None:
    """A completed row far enough back that the sweeper's floor
    (``min(DailyMetric.date)``) never itself falls inside a test's window.
    """
    DailyMetric.objects.create(
        date=day, computed_at=NOW, core_completed_at=NOW, completed_at=NOW
    )


def _seed_window_except(*target_days: date) -> None:
    """Complete every day in the 14-day catch-up window except
    ``target_days`` -- so those are the sweeper's only candidates this run,
    regardless of how many days the window actually spans."""
    for offset in range(1, WINDOW_DAYS + 1):
        d = NOW.date() - timedelta(days=offset)
        if d in target_days:
            continue
        DailyMetric.objects.create(
            date=d, computed_at=NOW, core_completed_at=NOW, completed_at=NOW
        )


def _status_not_ready(day: date) -> IndexerStatus:
    """Indexer position sitting right at the start of `day` -- comfortably
    before its settle threshold, so the gate defers with `day_not_ready`."""
    position_ts = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
    return IndexerStatus(
        erc20_block_number=1,
        erc20_block_timestamp=position_ts,
        master_copies_block_number=1,
        master_copies_block_timestamp=position_ts,
        oldest_unprocessed_ts=None,
        oldest_unprocessed_safe=None,
        relevant_master_copies=1,
    )


def _status_with_oldest(ts: datetime, safe: str) -> IndexerStatus:
    """Indexers far ahead (so the day gate, if exercised, never blocks) with
    a given oldest-unprocessed observation -- for the processing-stuck
    tests, which don't care about day settlement at all."""
    far_future = datetime(2100, 1, 1, tzinfo=UTC)
    return IndexerStatus(
        erc20_block_number=1,
        erc20_block_timestamp=far_future,
        master_copies_block_number=1,
        master_copies_block_timestamp=far_future,
        oldest_unprocessed_ts=ts,
        oldest_unprocessed_safe=safe,
        relevant_master_copies=1,
    )


class FrozenNowMixin:
    """Freezes `timezone.now()` for every module that does `from
    django.utils import timezone; timezone.now()` -- patching the shared
    module attribute covers `sweeper.py`, `tasks.py` and Django's own
    `auto_now_add`/`auto_now` machinery in one patch."""

    def setUp(self):
        super().setUp()
        patcher = patch("django.utils.timezone.now", return_value=NOW)
        patcher.start()
        self.addCleanup(patcher.stop)


class TaskRegistrationTestCase(TestCase):
    def test_registered_under_explicit_name_and_routes_to_contracts_queue(self):
        name = "safe_transaction_service.analytics.tasks.analytics_catchup_task"
        self.assertIn(name, celery_app.tasks)
        route = celery_app.amqp.router.route({}, name)
        self.assertEqual(route["queue"].name, "contracts")


class AnalyticsCatchupTaskDisabledAndLockedTestCase(TestCase):
    def test_skipped_disabled_reads_no_settings_and_touches_no_db(self):
        reset_catchup_settings_cache()
        try:
            with override_settings(ENABLE_ANALYTICS=False):
                with patch.dict(
                    "os.environ",
                    {"ANALYTICS_CATCHUP_WINDOW_DAYS": "not-an-int"},
                ):
                    with self.assertNumQueries(0):
                        with self.assertLogs(TASKS_LOGGER, "INFO") as cm:
                            analytics_catchup_task()
        finally:
            reset_catchup_settings_cache()
        self.assertEqual(
            cm.output,
            [
                f"INFO:{TASKS_LOGGER}:analytics.catchup: skipped_reason=analytics_disabled"
            ],
        )
        self.assertEqual(DailyMetric.objects.count(), 0)
        self.assertEqual(AnalyticsCatchupState.objects.count(), 0)

    def test_skipped_locked_does_no_work(self):
        with patch(
            f"{TASKS_LOGGER}.only_one_running_task", side_effect=LockError("locked")
        ):
            with patch(f"{TASKS_LOGGER}.run_catchup") as mock_run:
                with self.assertLogs(TASKS_LOGGER, "INFO") as cm:
                    analytics_catchup_task()
        mock_run.assert_not_called()
        self.assertEqual(
            cm.output, [f"INFO:{TASKS_LOGGER}:analytics.catchup: skipped_reason=locked"]
        )

    def test_lock_uses_same_task_object_and_timeout_as_nightly_task(self):
        with patch(
            f"{TASKS_LOGGER}.only_one_running_task", side_effect=LockError("stop")
        ) as mock_lock:
            with self.assertLogs(TASKS_LOGGER, "INFO"):
                analytics_catchup_task()
        mock_lock.assert_called_once_with(
            compute_daily_metrics_task, lock_timeout=LOCK_TIMEOUT * 4 + 300
        )


class BadConfigTestCase(TestCase):
    def test_bad_env_logs_bad_config_and_skips_everything(self):
        reset_catchup_settings_cache()
        try:
            with patch.dict(
                "os.environ", {"ANALYTICS_CATCHUP_WINDOW_DAYS": "not-an-int"}
            ):
                with self.assertLogs(SWEEPER_LOGGER, "ERROR") as cm:
                    run_catchup()
        finally:
            reset_catchup_settings_cache()
        self.assertEqual(len(cm.output), 1)
        self.assertIn("analytics.catchup.bad_config", cm.output[0])
        self.assertIn("setting=ANALYTICS_CATCHUP_WINDOW_DAYS", cm.output[0])
        # No value leaked, and no final summary line (that's what drives the
        # "no analytics.catchup: line for 2h" alert).
        self.assertNotIn("not-an-int", cm.output[0])
        self.assertEqual(DailyMetric.objects.count(), 0)


class DaySelectionTestCase(FrozenNowMixin, SettledGateMixin, TestCase):
    def test_missing_day_is_computed_and_marked_fixed(self):
        day = NOW.date() - timedelta(days=2)
        _seed_window_except(day)

        with self.assertLogs(SWEEPER_LOGGER, "INFO") as cm:
            run_catchup()

        row = DailyMetric.objects.get(date=day)
        self.assertIsNotNone(row.completed_at)
        self.assertFalse(AnalyticsCatchupState.objects.filter(kind="day").exists())
        self.assertTrue(
            any("days_fixed=" in line and str(day) in line for line in cm.output)
        )

    def test_second_run_after_success_writes_nothing_new(self):
        day = NOW.date() - timedelta(days=2)
        _seed_window_except(day)
        run_catchup()
        row_after_first = DailyMetric.objects.get(date=day)

        with patch(f"{TASKS_LOGGER}._upsert_daily_metric") as mock_upsert:
            run_catchup()
        mock_upsert.assert_not_called()
        row_after_second = DailyMetric.objects.get(date=day)
        self.assertEqual(row_after_first.completed_at, row_after_second.completed_at)

    def test_day_before_instance_floor_is_not_a_candidate(self):
        floor_day = NOW.date() - timedelta(days=5)
        _seed_floor(floor_day)
        before_floor = floor_day - timedelta(days=2)
        # Nothing before `floor_day` should ever be picked up, even though
        # it's technically inside the 14-day window.
        run_catchup()
        self.assertFalse(DailyMetric.objects.filter(date__lt=floor_day).exists())
        self.assertFalse(
            AnalyticsCatchupState.objects.filter(
                kind="day", key=before_floor.isoformat()
            ).exists()
        )

    def test_no_daily_metric_rows_at_all_selects_nothing(self):
        with self.assertLogs(SWEEPER_LOGGER, "INFO") as cm:
            run_catchup()
        self.assertTrue(any("days_fixed=[]" in line for line in cm.output))
        self.assertEqual(DailyMetric.objects.count(), 0)

    def test_limit_of_one_day_per_run_respected(self):
        oldest = NOW.date() - timedelta(days=5)
        newer = NOW.date() - timedelta(days=3)
        _seed_window_except(oldest, newer)
        run_catchup()
        self.assertTrue(
            DailyMetric.objects.filter(date=oldest, completed_at__isnull=False).exists()
        )
        self.assertFalse(DailyMetric.objects.filter(date=newer).exists())
        # The untouched day still got a state row created for it (it was a
        # candidate) but no attempt spent.
        newer_state = AnalyticsCatchupState.objects.get(
            kind="day", key=newer.isoformat()
        )
        self.assertEqual(newer_state.attempts, 0)


class DeferredGateTestCase(FrozenNowMixin, TestCase):
    def test_not_ready_day_defers_and_spends_no_attempt(self):
        _seed_floor(NOW.date() - timedelta(days=WINDOW_DAYS + 5))
        day = NOW.date() - timedelta(days=2)
        status = _status_not_ready(day)

        with patch(SWEEPER_GATE, return_value=status):
            with self.assertLogs(SWEEPER_LOGGER, "INFO") as cm:
                run_catchup()

        self.assertFalse(DailyMetric.objects.filter(date=day).exists())
        state = AnalyticsCatchupState.objects.get(kind="day", key=day.isoformat())
        self.assertEqual(state.attempts, 0)
        self.assertEqual(state.last_state, "deferred")
        self.assertEqual(state.last_code, DAY_NOT_READY)
        self.assertTrue(any(f"{day}:{DAY_NOT_READY}" in line for line in cm.output))

    def test_status_unavailable_defers_every_due_candidate(self):
        _seed_floor(NOW.date() - timedelta(days=WINDOW_DAYS + 5))
        day = NOW.date() - timedelta(days=2)
        with patch(SWEEPER_GATE, side_effect=DayNotReady(INDEXER_STATUS_UNAVAILABLE)):
            run_catchup()
        self.assertFalse(DailyMetric.objects.filter(date=day).exists())
        state = AnalyticsCatchupState.objects.get(kind="day", key=day.isoformat())
        self.assertEqual(state.attempts, 0)
        self.assertEqual(state.last_code, INDEXER_STATUS_UNAVAILABLE)


class GaveUpTestCase(FrozenNowMixin, SettledGateMixin, TestCase):
    def test_gave_up_logs_once_and_excludes_the_day(self):
        day = NOW.date() - timedelta(days=2)
        _seed_window_except(day)
        AnalyticsCatchupState.objects.create(
            kind="day", key=day.isoformat(), attempts=MAX_ATTEMPTS
        )

        with self.assertLogs(SWEEPER_LOGGER, "ERROR") as cm:
            run_catchup()
        self.assertTrue(
            any(
                f"analytics.catchup.gave_up day={day} attempts={MAX_ATTEMPTS}" in line
                for line in cm.output
            )
        )
        self.assertFalse(DailyMetric.objects.filter(date=day).exists())

        with self.assertLogs(SWEEPER_LOGGER, "INFO") as cm2:
            run_catchup()
        self.assertFalse(
            any("analytics.catchup.gave_up" in line for line in cm2.output)
        )

    def test_given_up_old_day_does_not_block_a_new_ready_day(self):
        old_day = NOW.date() - timedelta(days=10)
        new_day = NOW.date() - timedelta(days=1)
        _seed_window_except(old_day, new_day)
        AnalyticsCatchupState.objects.create(
            kind="day", key=old_day.isoformat(), attempts=MAX_ATTEMPTS
        )

        run_catchup()

        self.assertFalse(DailyMetric.objects.filter(date=old_day).exists())
        self.assertTrue(
            DailyMetric.objects.filter(
                date=new_day, completed_at__isnull=False
            ).exists()
        )

    def test_attempt_survives_a_baseexception_and_next_run_gives_up(self):
        day = NOW.date() - timedelta(days=2)
        _seed_window_except(day)
        AnalyticsCatchupState.objects.create(
            kind="day", key=day.isoformat(), attempts=MAX_ATTEMPTS - 1
        )

        class _Crash(BaseException):
            pass

        with patch(f"{SWEEPER_LOGGER}.compute_day", side_effect=_Crash):
            with self.assertRaises(_Crash):
                run_catchup()

        state = AnalyticsCatchupState.objects.get(kind="day", key=day.isoformat())
        self.assertEqual(state.attempts, MAX_ATTEMPTS)

        with self.assertLogs(SWEEPER_LOGGER, "ERROR") as cm:
            run_catchup()
        self.assertTrue(
            any(
                "analytics.catchup.gave_up" in line and str(day) in line
                for line in cm.output
            )
        )
        self.assertFalse(DailyMetric.objects.filter(date=day).exists())


class PartialFailureIsolationTestCase(FrozenNowMixin, SettledGateMixin, TestCase):
    def test_ordinary_exception_from_compute_day_does_not_abort_the_run(self):
        day = NOW.date() - timedelta(days=2)
        _seed_window_except(day)
        # An expired day in the grace window -- proves `_check_expired`
        # still runs (and still fires) in the same run.
        expired_day = NOW.date() - timedelta(days=WINDOW_DAYS + 3)
        AnalyticsCatchupState.objects.create(kind="day", key=expired_day.isoformat())

        with patch(
            f"{SWEEPER_LOGGER}.compute_day",
            side_effect=OperationalError("db gone away"),
        ):
            with self.assertLogs(SWEEPER_LOGGER, "INFO") as cm:
                run_catchup()

        state = AnalyticsCatchupState.objects.get(kind="day", key=day.isoformat())
        self.assertEqual(state.attempts, 1)
        self.assertTrue(
            any(
                "analytics.catchup.expired" in line and str(expired_day) in line
                for line in cm.output
            )
        )
        summary = [
            line for line in cm.output if "analytics.catchup: days_fixed" in line
        ]
        self.assertEqual(len(summary), 1)
        self.assertIn(f"days_incomplete=['{day}']", summary[0])


class BackoffPauseTestCase(FrozenNowMixin, SettledGateMixin, TestCase):
    def test_pauses_are_1_2_4_8_hours(self):
        day = NOW.date() - timedelta(days=2)
        _seed_window_except(day)

        with patch(
            f"{TASKS_LOGGER}._compute_daily_safe_creations", side_effect=RuntimeError
        ):
            run_catchup()
        state = AnalyticsCatchupState.objects.get(kind="day", key=day.isoformat())
        self.assertEqual(state.attempts, 1)
        self.assertEqual(state.next_attempt_at, NOW + timedelta(hours=1))

        # Not due yet 30 minutes later -- no second attempt.
        with patch(
            "django.utils.timezone.now", return_value=NOW + timedelta(minutes=30)
        ):
            with patch(
                f"{TASKS_LOGGER}._compute_daily_safe_creations",
                side_effect=RuntimeError,
            ):
                run_catchup()
        state.refresh_from_db()
        self.assertEqual(state.attempts, 1)

        # Due 1h later -> attempt 2, backoff 2h.
        now2 = NOW + timedelta(hours=1)
        with patch("django.utils.timezone.now", return_value=now2):
            with patch(
                f"{TASKS_LOGGER}._compute_daily_safe_creations",
                side_effect=RuntimeError,
            ):
                run_catchup()
        state.refresh_from_db()
        self.assertEqual(state.attempts, 2)
        self.assertEqual(state.next_attempt_at, now2 + timedelta(hours=2))


class FailedStepsOnlyRetryTestCase(FrozenNowMixin, SettledGateMixin, TestCase):
    def test_stably_failing_populator_is_retried_alone_without_core(self):
        day = NOW.date() - timedelta(days=2)
        _seed_window_except(day)

        with patch(
            f"{TASKS_LOGGER}._compute_daily_safe_creations", side_effect=RuntimeError
        ):
            run_catchup()
        row = DailyMetric.objects.get(date=day)
        self.assertIsNotNone(row.core_completed_at)
        self.assertIsNone(row.completed_at)
        state = AnalyticsCatchupState.objects.get(kind="day", key=day.isoformat())
        self.assertTrue(state.core_ok)
        self.assertEqual(state.failed_steps, ["safe_creations"])

        now2 = NOW + timedelta(hours=1)
        with patch("django.utils.timezone.now", return_value=now2):
            with (
                patch(
                    f"{TASKS_LOGGER}._compute_daily_safe_creations",
                    side_effect=RuntimeError,
                ),
                patch(f"{TASKS_LOGGER}._compute_daily_metric_core") as core_spy,
                patch(
                    f"{TASKS_LOGGER}._compute_daily_active_safes"
                ) as active_safes_spy,
            ):
                run_catchup()
        core_spy.assert_not_called()
        active_safes_spy.assert_not_called()
        row.refresh_from_db()
        self.assertIsNone(row.completed_at)

        now3 = now2 + timedelta(hours=2)
        with patch("django.utils.timezone.now", return_value=now3):
            run_catchup()  # safe_creations succeeds this time (unpatched)
        row.refresh_from_db()
        self.assertIsNotNone(row.completed_at)
        self.assertFalse(AnalyticsCatchupState.objects.filter(kind="day").exists())


class ProcessingStuckTestCase(FrozenNowMixin, TestCase):
    def test_first_observation_seeds_without_logging(self):
        ts = datetime(2026, 9, 1, tzinfo=UTC)
        status = _status_with_oldest(ts, "0xSafe1")
        with patch(SWEEPER_GATE, return_value=status):
            with patch(f"{SWEEPER_LOGGER}.InternalTxDecoded") as mock_model:
                mock_model.objects.not_processed.return_value.count.return_value = 3
                with self.assertLogs(SWEEPER_LOGGER, "INFO") as cm:
                    run_catchup()
        self.assertFalse(any("processing_stuck" in line for line in cm.output))
        state = AnalyticsCatchupState.objects.get(kind="processing", key="oldest")
        self.assertEqual(state.observed_value, int(ts.timestamp()))
        self.assertEqual(state.observed_count, 3)
        self.assertEqual(state.observed_since, NOW)

    def test_same_id_stuck_duration_logs_once(self):
        ts = datetime(2026, 9, 1, tzinfo=UTC)
        AnalyticsCatchupState.objects.create(
            kind="processing",
            key="oldest",
            observed_value=int(ts.timestamp()),
            observed_since=NOW - timedelta(hours=7),
            observed_count=3,
        )
        status = _status_with_oldest(ts, "0xStuckSafe")
        with patch(SWEEPER_GATE, return_value=status):
            with patch(f"{SWEEPER_LOGGER}.InternalTxDecoded") as mock_model:
                mock_model.objects.not_processed.return_value.count.return_value = 3
                with self.assertLogs(SWEEPER_LOGGER, "ERROR") as cm:
                    run_catchup()
        self.assertTrue(
            any(
                "analytics.catchup.processing_stuck" in line and "0xStuckSafe" in line
                for line in cm.output
            )
        )

        with patch(SWEEPER_GATE, return_value=status):
            with patch(f"{SWEEPER_LOGGER}.InternalTxDecoded") as mock_model2:
                mock_model2.objects.not_processed.return_value.count.return_value = 3
                with self.assertLogs(SWEEPER_LOGGER, "INFO") as cm2:
                    run_catchup()
        self.assertFalse(
            any("analytics.catchup.processing_stuck" in line for line in cm2.output)
        )

    def test_count_drop_resets_and_suppresses_log(self):
        ts = datetime(2026, 9, 1, tzinfo=UTC)
        AnalyticsCatchupState.objects.create(
            kind="processing",
            key="oldest",
            observed_value=int(ts.timestamp()),
            observed_since=NOW - timedelta(hours=7),
            observed_count=5,
        )
        status = _status_with_oldest(ts, "0xSafe2")
        with patch(SWEEPER_GATE, return_value=status):
            with patch(f"{SWEEPER_LOGGER}.InternalTxDecoded") as mock_model:
                mock_model.objects.not_processed.return_value.count.return_value = 2
                with self.assertLogs(SWEEPER_LOGGER, "INFO") as cm:
                    run_catchup()
        self.assertFalse(any("processing_stuck" in line for line in cm.output))
        state = AnalyticsCatchupState.objects.get(kind="processing", key="oldest")
        self.assertEqual(state.observed_since, NOW)
        self.assertEqual(state.observed_count, 2)

    def test_empty_queue_does_not_reset_or_log(self):
        ts = datetime(2026, 9, 1, tzinfo=UTC)
        AnalyticsCatchupState.objects.create(
            kind="processing",
            key="oldest",
            observed_value=int(ts.timestamp()),
            observed_since=NOW - timedelta(hours=7),
            observed_count=3,
        )
        empty_status = dataclasses.replace(
            _status_with_oldest(ts, "0xSafe3"),
            oldest_unprocessed_ts=None,
            oldest_unprocessed_safe=None,
        )
        with patch(SWEEPER_GATE, return_value=empty_status):
            with self.assertLogs(SWEEPER_LOGGER, "INFO") as cm:
                run_catchup()
        self.assertFalse(any("processing_stuck" in line for line in cm.output))
        state = AnalyticsCatchupState.objects.get(kind="processing", key="oldest")
        self.assertEqual(state.observed_since, NOW - timedelta(hours=7))
        self.assertIsNone(state.stuck_logged_at)

    def test_id_change_resets_the_clock(self):
        ts_old = datetime(2026, 9, 1, tzinfo=UTC)
        ts_new = datetime(2026, 9, 2, tzinfo=UTC)
        AnalyticsCatchupState.objects.create(
            kind="processing",
            key="oldest",
            observed_value=int(ts_old.timestamp()),
            observed_since=NOW - timedelta(hours=7),
            observed_count=3,
        )
        status = _status_with_oldest(ts_new, "0xNewSafe")
        with patch(SWEEPER_GATE, return_value=status):
            with patch(f"{SWEEPER_LOGGER}.InternalTxDecoded") as mock_model:
                mock_model.objects.not_processed.return_value.count.return_value = 3
                with self.assertLogs(SWEEPER_LOGGER, "INFO") as cm:
                    run_catchup()
        self.assertFalse(any("processing_stuck" in line for line in cm.output))
        state = AnalyticsCatchupState.objects.get(kind="processing", key="oldest")
        self.assertEqual(state.observed_value, int(ts_new.timestamp()))
        self.assertEqual(state.observed_since, NOW)


class ExpiredTestCase(FrozenNowMixin, SettledGateMixin, TestCase):
    def test_never_settled_day_expires_once(self):
        day = NOW.date() - timedelta(days=WINDOW_DAYS + 3)
        AnalyticsCatchupState.objects.create(kind="day", key=day.isoformat())

        with self.assertLogs(SWEEPER_LOGGER, "ERROR") as cm:
            run_catchup()
        self.assertTrue(
            any(f"analytics.catchup.expired day={day}" in line for line in cm.output)
        )

        with self.assertLogs(SWEEPER_LOGGER, "INFO") as cm2:
            run_catchup()
        self.assertFalse(
            any("analytics.catchup.expired" in line for line in cm2.output)
        )

    def test_completed_day_in_grace_window_is_not_expired(self):
        day = NOW.date() - timedelta(days=WINDOW_DAYS + 3)
        AnalyticsCatchupState.objects.create(kind="day", key=day.isoformat())
        DailyMetric.objects.create(
            date=day, computed_at=NOW, core_completed_at=NOW, completed_at=NOW
        )
        with self.assertLogs(SWEEPER_LOGGER, "INFO") as cm:
            run_catchup()
        self.assertFalse(any("analytics.catchup.expired" in line for line in cm.output))

    def test_unseen_boundary_day_is_not_expired_on_first_deploy(self):
        # No state row at all -- the sweeper never saw this day as a
        # candidate (e.g. it predates the deploy).
        with self.assertLogs(SWEEPER_LOGGER, "INFO") as cm:
            run_catchup()
        self.assertFalse(any("analytics.catchup.expired" in line for line in cm.output))

    def test_sweeper_downtime_of_two_days_still_reports_expired(self):
        day = NOW.date() - timedelta(days=WINDOW_DAYS + 1)
        AnalyticsCatchupState.objects.create(kind="day", key=day.isoformat())
        with self.assertLogs(SWEEPER_LOGGER, "ERROR") as cm:
            run_catchup()
        self.assertTrue(
            any(f"analytics.catchup.expired day={day}" in line for line in cm.output)
        )


class BackfillLiveTestCase(FrozenNowMixin, SettledGateMixin, TestCase):
    def tearDown(self):
        super().tearDown()
        redis = get_redis()
        for key in redis.scan_iter(match="analytics_backfill_run:*"):
            redis.delete(key)
        redis.delete(tasks_shards.BACKFILL_CURSOR_KEY)

    def _write_run(self, run_id: str, *, finished: bool, started_at: datetime) -> None:
        run = tasks_shards.build_backfill_run(["2026-01-01"], 1, run_id=run_id)
        run["started_at"] = started_at.isoformat()
        run["finished_at"] = NOW.isoformat() if finished else None
        get_redis().set(
            tasks_shards.backfill_run_key(run_id),
            json.dumps(run),
            ex=tasks_shards.BACKFILL_KEY_TTL_SECONDS,
        )
        get_redis().set(
            tasks_shards.BACKFILL_CURSOR_KEY,
            json.dumps(
                {
                    "run_id": run_id,
                    "run_key": tasks_shards.backfill_run_key(run_id),
                    "started_at": run["started_at"],
                }
            ),
            ex=tasks_shards.BACKFILL_KEY_TTL_SECONDS,
        )

    def test_live_backfill_with_fresh_heartbeat_blocks_days(self):
        _seed_floor(NOW.date() - timedelta(days=WINDOW_DAYS + 5))
        day = NOW.date() - timedelta(days=2)
        self._write_run("live-run", finished=False, started_at=NOW - timedelta(hours=5))
        get_redis().set(
            tasks_shards.backfill_heartbeat_key("live-run"),
            NOW.isoformat(),
            ex=tasks_shards.BACKFILL_KEY_TTL_SECONDS,
        )

        with self.assertLogs(SWEEPER_LOGGER, "INFO") as cm:
            run_catchup()

        self.assertFalse(DailyMetric.objects.filter(date=day).exists())
        self.assertTrue(any("skipped_reason=backfill" in line for line in cm.output))

    def test_stale_heartbeat_does_not_block(self):
        day = NOW.date() - timedelta(days=2)
        _seed_window_except(day)
        self._write_run(
            "stale-run", finished=False, started_at=NOW - timedelta(hours=10)
        )
        get_redis().set(
            tasks_shards.backfill_heartbeat_key("stale-run"),
            (NOW - timedelta(hours=8)).isoformat(),
            ex=tasks_shards.BACKFILL_KEY_TTL_SECONDS,
        )

        with self.assertLogs(SWEEPER_LOGGER, "WARNING") as cm:
            run_catchup()

        self.assertTrue(
            any(
                "analytics.catchup.stale_backfill" in line and "stale-run" in line
                for line in cm.output
            )
        )
        self.assertTrue(
            DailyMetric.objects.filter(date=day, completed_at__isnull=False).exists()
        )

    def test_manifest_without_heartbeat_key_falls_back_to_started_at(self):
        _seed_floor(NOW.date() - timedelta(days=WINDOW_DAYS + 5))
        day = NOW.date() - timedelta(days=2)
        self._write_run(
            "no-heartbeat-run", finished=False, started_at=NOW - timedelta(hours=2)
        )
        # No heartbeat key written at all.

        run_catchup()

        self.assertFalse(DailyMetric.objects.filter(date=day).exists())

    def test_finished_run_does_not_block(self):
        day = NOW.date() - timedelta(days=2)
        _seed_window_except(day)
        self._write_run(
            "finished-run", finished=True, started_at=NOW - timedelta(hours=5)
        )

        run_catchup()

        self.assertTrue(
            DailyMetric.objects.filter(date=day, completed_at__isnull=False).exists()
        )

    def test_legacy_chunk_summary_cursor_does_not_block(self):
        day = NOW.date() - timedelta(days=2)
        _seed_window_except(day)
        # Legacy standalone `dispatch_backfill()` writes a chunk summary
        # (no `run_id` key) straight into BACKFILL_CURSOR_KEY.
        get_redis().set(
            tasks_shards.BACKFILL_CURSOR_KEY,
            json.dumps(
                {
                    "total": 1,
                    "written": 1,
                    "failed": 0,
                    "failures": [],
                    "finished_at": NOW.isoformat(),
                }
            ),
            ex=tasks_shards.BACKFILL_KEY_TTL_SECONDS,
        )

        run_catchup()

        self.assertTrue(
            DailyMetric.objects.filter(date=day, completed_at__isnull=False).exists()
        )


class PruneTestCase(FrozenNowMixin, SettledGateMixin, TestCase):
    def test_prunes_day_state_rows_older_than_two_windows(self):
        old_day = NOW.date() - timedelta(days=2 * WINDOW_DAYS + 5)
        recent_day = NOW.date() - timedelta(days=2 * WINDOW_DAYS - 1)
        AnalyticsCatchupState.objects.create(kind="day", key=old_day.isoformat())
        AnalyticsCatchupState.objects.create(kind="day", key=recent_day.isoformat())

        run_catchup()

        self.assertFalse(
            AnalyticsCatchupState.objects.filter(
                kind="day", key=old_day.isoformat()
            ).exists()
        )
        self.assertTrue(
            AnalyticsCatchupState.objects.filter(
                kind="day", key=recent_day.isoformat()
            ).exists()
        )


class FinalSummaryLineTestCase(FrozenNowMixin, SettledGateMixin, TestCase):
    def test_normal_run_logs_all_eight_fields(self):
        with self.assertLogs(SWEEPER_LOGGER, "INFO") as cm:
            run_catchup()
        summary = [line for line in cm.output if "analytics.catchup:" in line]
        self.assertEqual(len(summary), 1)
        for field in (
            "days_fixed=",
            "days_deferred=",
            "days_incomplete=",
            "days_given_up=",
            "days_expired=",
            "skipped_reason=",
            "snapshots_dispatched=",
            "snapshots_given_up=",
        ):
            self.assertIn(field, summary[0])
        # Order matters -- the two snapshot fields are appended at the end,
        # after the pre-existing day fields.
        self.assertLess(
            summary[0].index("skipped_reason="),
            summary[0].index("snapshots_dispatched="),
        )
        self.assertLess(
            summary[0].index("snapshots_dispatched="),
            summary[0].index("snapshots_given_up="),
        )
