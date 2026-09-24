"""Tests for the T5 callers: the nightly task, the backfill shard and
`--inline` / Celery backfill's own use of the catch-up gate -- on top of
what `test_tasks.py` and `test_backfill_daily_metrics.py` already cover for
these same callers with the shared gate fixture.

See "Analytics catch-up" in `analytics/implementation-notes.md`.
"""

from datetime import date, datetime
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase, override_settings
from django.utils import timezone

from redis.exceptions import LockError

from safe_transaction_service.analytics.catchup.gate import (
    INDEXER_STATUS_UNAVAILABLE,
    DayNotReady,
)
from safe_transaction_service.analytics.models import DailyMetric
from safe_transaction_service.analytics.tasks import compute_daily_metrics_task
from safe_transaction_service.analytics.tasks_shards import (
    backfill_heartbeat_key,
    compute_daily_metric_shard,
)
from safe_transaction_service.utils.redis import get_redis

from .catchup_gate_fixture import SETTLED_INDEXER_STATUS, SettledGateMixin

TASKS_LOGGER = "safe_transaction_service.analytics.tasks"
SHARD_GATE_TARGET = "safe_transaction_service.analytics.tasks_shards.get_indexer_status"


class TestComputeDailyMetricsTaskDisabledAndLocked(TestCase):
    def test_skipped_disabled_when_flag_off_and_nothing_computed(self):
        with override_settings(ENABLE_ANALYTICS=False):
            with self.assertLogs(TASKS_LOGGER, "INFO") as cm:
                result = compute_daily_metrics_task(days_back=1)
        self.assertFalse(result)
        self.assertTrue(
            any("analytics.daily.skipped_disabled" in line for line in cm.output)
        )
        self.assertEqual(DailyMetric.objects.count(), 0)

    def test_skipped_locked_on_lock_error(self):
        with patch(
            f"{TASKS_LOGGER}.only_one_running_task", side_effect=LockError("locked")
        ):
            with self.assertLogs(TASKS_LOGGER, "INFO") as cm:
                result = compute_daily_metrics_task(days_back=1)
        self.assertFalse(result)
        self.assertTrue(
            any("analytics.daily.skipped_locked" in line for line in cm.output)
        )


class TestComputeDailyMetricsTaskSkipsDoneDays(SettledGateMixin, TestCase):
    def test_already_completed_day_is_not_recomputed(self):
        now = timezone.now()
        yesterday = (now - timezone.timedelta(days=1)).date()
        DailyMetric.objects.create(
            date=yesterday,
            computed_at=now,
            core_completed_at=now,
            completed_at=now,
            active_safes=5,
        )
        with patch(f"{TASKS_LOGGER}._upsert_daily_metric") as mock_upsert:
            with self.assertLogs(TASKS_LOGGER, "INFO") as cm:
                result = compute_daily_metrics_task(days_back=1)
        mock_upsert.assert_not_called()
        # Nothing new was written this run -- the only day in range was
        # already done and skipped outright.
        self.assertFalse(result)
        self.assertTrue(any("days_skipped_done=1" in line for line in cm.output))
        row = DailyMetric.objects.get(date=yesterday)
        self.assertEqual(row.active_safes, 5)  # untouched, not recleared/rewritten


class TestComputeDailyMetricShard(TestCase):
    def test_deferred_day_shape_and_no_status_leak(self):
        day = date(2026, 1, 1)
        with patch(
            SHARD_GATE_TARGET, side_effect=DayNotReady(INDEXER_STATUS_UNAVAILABLE)
        ):
            result = compute_daily_metric_shard(day.isoformat())
        self.assertEqual(
            result,
            {
                "date": day.isoformat(),
                "ok": False,
                "status": "deferred",
                "error": INDEXER_STATUS_UNAVAILABLE,
            },
        )
        self.assertFalse(DailyMetric.objects.filter(date=day).exists())

    def test_heartbeat_written_at_start_and_finish_when_run_id_given(self):
        day = date(2026, 1, 2)
        run_id = "shard-heartbeat-test"
        with patch(SHARD_GATE_TARGET, return_value=SETTLED_INDEXER_STATUS):
            result = compute_daily_metric_shard(day.isoformat(), run_id=run_id)
        self.assertTrue(result["ok"])
        raw = get_redis().get(backfill_heartbeat_key(run_id))
        self.assertIsNotNone(raw)
        # Plain ISO timestamp value, not JSON -- parses straight back.
        datetime.fromisoformat(raw.decode())

    def test_no_heartbeat_key_without_run_id(self):
        day = date(2026, 1, 3)
        redis = get_redis()
        before = set(redis.scan_iter(match="analytics_backfill_run:*:heartbeat"))
        with patch(SHARD_GATE_TARGET, return_value=SETTLED_INDEXER_STATUS):
            result = compute_daily_metric_shard(day.isoformat())
        after = set(redis.scan_iter(match="analytics_backfill_run:*:heartbeat"))
        self.assertTrue(result["ok"])
        # A shard dispatched without run_id (bare dispatch_backfill(), no
        # chunk manifest) writes no heartbeat key at all.
        self.assertEqual(before, after)

    def test_exception_reports_class_name_never_the_message(self):
        day = date(2026, 1, 4)
        with (
            patch(SHARD_GATE_TARGET, return_value=SETTLED_INDEXER_STATUS),
            patch(
                f"{TASKS_LOGGER}._upsert_daily_metric",
                side_effect=RuntimeError("rpc failed at https://node.example/secret"),
            ),
        ):
            result = compute_daily_metric_shard(day.isoformat())
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "RuntimeError")
        self.assertNotIn("secret", result["error"])
        self.assertNotIn("node.example", result["error"])


class TestBackfillSkipSettleCheck(TestCase):
    def test_inline_skip_settle_check_computes_with_unavailable_status(self):
        day = date(2026, 1, 5)
        # No SafeMasterCopy rows in this fresh test DB -> get_indexer_status()
        # is genuinely unavailable, unmocked -- exactly the scenario the flag
        # exists for.
        with self.assertLogs(
            "safe_transaction_service.analytics.catchup.day", "WARNING"
        ) as cm:
            call_command(
                "backfill_daily_metrics",
                start=day.isoformat(),
                end=day.isoformat(),
                inline=True,
                skip_settle_check=True,
            )
        self.assertTrue(
            any(
                "analytics.daily.settle_check_skipped" in line
                and f"day={day}" in line
                and "by=backfill" in line
                for line in cm.output
            )
        )
        row = DailyMetric.objects.get(date=day)
        self.assertIsNotNone(row.completed_at)
