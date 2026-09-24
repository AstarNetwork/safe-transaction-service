"""Tests for ``warm_analytics_cache --skip-if-fresh``.

For ``summary`` / ``safe_segments`` / ``tvl`` the command now decides
freshness from Postgres (``AnalyticsSnapshot``), via the same
``is_snapshot_stale`` the snapshot sweeper uses -- see
``test_catchup_snapshots.py`` for that function's own tests. These tests
exercise the command's wiring: ``Command._is_fresh`` for the three snapshot
labels, and one end-to-end ``call_command`` pass showing a fresh snapshot is
skipped while an unrelated, Redis-probed task is untouched by any of this.

See "Analytics catch-up" in ``analytics/implementation-notes.md``.
"""

from datetime import UTC, datetime, timedelta
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase

from safe_transaction_service.analytics.conf import get_catchup_settings
from safe_transaction_service.analytics.management.commands.warm_analytics_cache import (
    Command,
)
from safe_transaction_service.analytics.models import AnalyticsSnapshot
from safe_transaction_service.analytics.services.analytics_service import (
    AnalyticsService,
)
from safe_transaction_service.analytics.tasks import (
    compute_active_owners_task,
    compute_active_safes_task,
    compute_native_balance_rollup_task,
    compute_safe_creations_task,
    compute_safe_segments_task,
    compute_summary_task,
    compute_tvl_task,
    get_transactions_per_safe_app_task,
)
from safe_transaction_service.utils.redis import get_redis

NOW = datetime(2026, 9, 23, 0, 0, tzinfo=UTC)


def _fresh_tvl_payload(**overrides) -> dict:
    payload = {
        "total_safes_with_balance": 1,
        "native_balance_wei": "1",
        "erc20_token_count": 0,
        "top_tokens": [],
        "partial_shards": 0,
        "total_shards": 0,
        "native_source": "rollup",
        "native_updated_to_block": 1,
    }
    payload.update(overrides)
    return payload


class IsFreshForSnapshotsTestCase(TestCase):
    """Direct tests of ``Command._is_fresh`` for the three snapshot labels
    -- the label doubles as the ``AnalyticsSnapshot.name`` / ``SNAPSHOT_NAMES``
    entry, which is what routes it to the Postgres check instead of the
    Redis probe.
    """

    def setUp(self):
        super().setUp()
        self.cfg = get_catchup_settings()

    def test_fresh_snapshot_is_skipped(self):
        AnalyticsSnapshot.objects.create(
            name="summary", payload={}, computed_at=NOW - timedelta(hours=1)
        )
        self.assertTrue(
            Command._is_fresh("summary", None, None, NOW, timedelta(hours=6), self.cfg)
        )

    def test_stale_snapshot_is_not_skipped(self):
        AnalyticsSnapshot.objects.create(
            name="safe_segments",
            payload={},
            computed_at=NOW - timedelta(hours=self.cfg.SNAPSHOT_STALE_HOURS),
        )
        self.assertFalse(
            Command._is_fresh(
                "safe_segments", None, None, NOW, timedelta(hours=6), self.cfg
            )
        )

    def test_missing_row_is_not_skipped(self):
        self.assertFalse(
            Command._is_fresh("tvl", None, None, NOW, timedelta(hours=6), self.cfg)
        )

    def test_tvl_placeholder_native_source_none_is_not_skipped_even_when_fresh(self):
        AnalyticsSnapshot.objects.create(
            name="tvl", payload=_fresh_tvl_payload(native_source=None), computed_at=NOW
        )
        self.assertFalse(
            Command._is_fresh("tvl", None, None, NOW, timedelta(hours=6), self.cfg)
        )

    def test_tvl_with_fresh_computed_at_and_real_native_source_is_skipped(self):
        AnalyticsSnapshot.objects.create(
            name="tvl",
            payload=_fresh_tvl_payload(),
            computed_at=NOW - timedelta(hours=1),
        )
        self.assertTrue(
            Command._is_fresh("tvl", None, None, NOW, timedelta(hours=6), self.cfg)
        )


class SkipIfFreshEndToEndTestCase(TestCase):
    """One full ``call_command`` pass: a fresh `summary` snapshot is
    skipped, while an unrelated task that still uses the Redis probe
    (`active_safes`, no cached payload here) is dispatched as before --
    the two freshness mechanisms don't interfere with each other.
    """

    def setUp(self):
        super().setUp()
        # `active_safes` / `active_owners` are still Redis-probed --
        # a leftover payload from another test file (real Redis, shared
        # across the run) would make them look fresh here regardless of
        # this test's own setup, so start from a clean slate for these
        # specific keys.
        keys = [
            AnalyticsService.REDIS_ACTIVE_SAFES_PREFIX + "30d",
            AnalyticsService.REDIS_ACTIVE_OWNERS_PREFIX + "30d",
        ]
        get_redis().delete(*keys)
        self.addCleanup(lambda: get_redis().delete(*keys))

    def test_fresh_summary_skipped_other_tasks_unaffected(self):
        AnalyticsSnapshot.objects.create(
            name="summary", payload={}, computed_at=NOW - timedelta(hours=1)
        )
        AnalyticsSnapshot.objects.create(
            name="safe_segments", payload={}, computed_at=NOW - timedelta(hours=1)
        )
        AnalyticsSnapshot.objects.create(
            name="tvl",
            payload=_fresh_tvl_payload(),
            computed_at=NOW - timedelta(hours=1),
        )
        out = StringIO()
        with (
            patch("django.utils.timezone.now", return_value=NOW),
            patch.object(compute_summary_task, "delay") as summary_delay,
            patch.object(compute_safe_segments_task, "delay") as segments_delay,
            patch.object(compute_tvl_task, "delay") as tvl_delay,
            patch.object(compute_active_safes_task, "delay") as active_safes_delay,
            patch.object(compute_active_owners_task, "delay") as active_owners_delay,
            patch.object(
                compute_native_balance_rollup_task, "delay"
            ) as native_rollup_delay,
            patch.object(compute_safe_creations_task, "delay") as safe_creations_delay,
            patch.object(
                get_transactions_per_safe_app_task, "delay"
            ) as by_origin_delay,
        ):
            call_command("warm_analytics_cache", "--skip-if-fresh", stdout=out)

        summary_delay.assert_not_called()
        segments_delay.assert_not_called()
        tvl_delay.assert_not_called()
        # No cached Redis payload for these in this test -> `--skip-if-fresh`
        # never skips them, same as before this change.
        active_safes_delay.assert_called_once()
        active_owners_delay.assert_called_once()
        native_rollup_delay.assert_called_once()
        safe_creations_delay.assert_called_once()
        by_origin_delay.assert_called_once()

        output = out.getvalue()
        self.assertIn("summary: skipped (fresh)", output)
        self.assertIn("safe_segments: skipped (fresh)", output)
        self.assertIn("tvl: skipped (fresh)", output)
