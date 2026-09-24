"""Tests for the snapshot sweeper -- ``sweep_snapshots()``
(``analytics/catchup/snapshots.py``) -- and the in-flight mark the three
snapshot tasks (``compute_summary_task`` / ``compute_safe_segments_task`` /
``compute_tvl_task`` in ``analytics/tasks.py``) set themselves.

See "Analytics catch-up" in ``analytics/implementation-notes.md``.

``MAX_ATTEMPTS=5`` / ``SNAPSHOT_STALE_HOURS=26`` mirror
``conf.get_catchup_settings()``'s defaults -- ``.env.test`` doesn't override
either ``ANALYTICS_CATCHUP_*`` variable.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from django.test import TestCase

from safe_transaction_service.analytics.catchup.snapshots import (
    SNAPSHOT_NAMES,
    _in_flight_key,
    mark_snapshot_dispatched,
    sweep_snapshots,
)
from safe_transaction_service.analytics.conf import get_catchup_settings
from safe_transaction_service.analytics.models import (
    AnalyticsCatchupState,
    AnalyticsSnapshot,
)
from safe_transaction_service.analytics.tasks import (
    compute_safe_segments_task,
    compute_summary_task,
    compute_tvl_task,
)
from safe_transaction_service.utils.redis import get_redis

SWEEPER_LOGGER = "safe_transaction_service.analytics.catchup.snapshots"

MAX_ATTEMPTS = 5
SNAPSHOT_STALE_HOURS = 26

NOW = datetime(2026, 9, 23, 0, 20, tzinfo=UTC)


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


class SnapshotTestCase(TestCase):
    """Freezes `timezone.now()` and clears any Redis keys this file's tests
    might set, so a failure mid-test never leaks state into the next one."""

    def setUp(self):
        super().setUp()
        patcher = patch("django.utils.timezone.now", return_value=NOW)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(
            lambda: get_redis().delete(
                *[_in_flight_key(name) for name in ("summary", "safe_segments", "tvl")],
                "analytics_snapshot:summary:refresh_lock",
                "analytics_snapshot:safe_segments:refresh_lock",
                "analytics_snapshot:tvl:refresh_lock",
            )
        )
        self.cfg = get_catchup_settings()

    def _seed_other_fresh(self, *except_names: str) -> None:
        """Fresh rows for every snapshot name not in ``except_names``, so a
        test focused on one snapshot doesn't also see the other two as
        missing (and therefore stale and dispatched)."""
        for name in SNAPSHOT_NAMES:
            if name in except_names:
                continue
            AnalyticsSnapshot.objects.get_or_create(
                name=name,
                defaults={"payload": _fresh_tvl_payload(), "computed_at": NOW},
            )


class FreshnessTestCase(SnapshotTestCase):
    def test_fresh_snapshot_is_not_dispatched(self):
        AnalyticsSnapshot.objects.create(
            name="summary", payload={}, computed_at=NOW - timedelta(hours=1)
        )
        with patch.object(compute_summary_task, "delay") as mock_delay:
            dispatched, given_up = sweep_snapshots(self.cfg, NOW)
        mock_delay.assert_not_called()
        self.assertNotIn("summary", dispatched)
        self.assertEqual(given_up, [])

    def test_missing_snapshot_is_stale_and_dispatched(self):
        with patch.object(compute_summary_task, "delay") as mock_delay:
            dispatched, given_up = sweep_snapshots(self.cfg, NOW)
        mock_delay.assert_called_once()
        self.assertIn("summary", dispatched)
        self.assertEqual(given_up, [])

    def test_snapshot_older_than_stale_hours_is_dispatched(self):
        AnalyticsSnapshot.objects.create(
            name="safe_segments",
            payload={},
            computed_at=NOW - timedelta(hours=SNAPSHOT_STALE_HOURS),
        )
        with patch.object(compute_safe_segments_task, "delay") as mock_delay:
            dispatched, _given_up = sweep_snapshots(self.cfg, NOW)
        mock_delay.assert_called_once()
        self.assertIn("safe_segments", dispatched)

    def test_tvl_rollup_zero_zero_is_not_stale(self):
        AnalyticsSnapshot.objects.create(
            name="tvl",
            payload=_fresh_tvl_payload(),
            computed_at=NOW - timedelta(hours=1),
        )
        with patch.object(compute_tvl_task, "delay") as mock_delay:
            dispatched, _given_up = sweep_snapshots(self.cfg, NOW)
        mock_delay.assert_not_called()
        self.assertNotIn("tvl", dispatched)

    def test_tvl_placeholder_native_source_none_is_stale_even_when_recent(self):
        AnalyticsSnapshot.objects.create(
            name="tvl",
            payload=_fresh_tvl_payload(native_source=None),
            computed_at=NOW,
        )
        with patch.object(compute_tvl_task, "delay") as mock_delay:
            dispatched, _given_up = sweep_snapshots(self.cfg, NOW)
        mock_delay.assert_called_once()
        self.assertIn("tvl", dispatched)


class InFlightAndRefreshLockTestCase(SnapshotTestCase):
    def test_in_flight_mark_blocks_dispatch(self):
        get_redis().set(_in_flight_key("tvl"), "1", ex=10800)
        with patch.object(compute_tvl_task, "delay") as mock_delay:
            dispatched, given_up = sweep_snapshots(self.cfg, NOW)
        mock_delay.assert_not_called()
        self.assertNotIn("tvl", dispatched)
        self.assertEqual(given_up, [])
        # No attempt spent while something else is already refreshing it.
        self.assertFalse(
            AnalyticsCatchupState.objects.filter(kind="snapshot", key="tvl").exists()
        )

    def test_refresh_lock_blocks_dispatch(self):
        get_redis().set("analytics_snapshot:tvl:refresh_lock", "1", ex=1800)
        with patch.object(compute_tvl_task, "delay") as mock_delay:
            dispatched, _given_up = sweep_snapshots(self.cfg, NOW)
        mock_delay.assert_not_called()
        self.assertNotIn("tvl", dispatched)

    def test_nightly_compute_tvl_task_sets_the_mark_the_sweeper_reads(self):
        """The mark is set by the task itself, not by the sweeper's own
        dispatch path -- a plain call to the real task (as beat would make
        it) must leave the same key behind that ``sweep_snapshots`` checks.
        """
        compute_tvl_task.delay()
        self.assertTrue(get_redis().exists(_in_flight_key("tvl")))

    def test_mark_snapshot_dispatched_sets_a_ttl(self):
        mark_snapshot_dispatched("summary")
        ttl = get_redis().ttl(_in_flight_key("summary"))
        self.assertGreater(ttl, 0)
        self.assertLessEqual(ttl, 10800)


class AttemptsAndBackoffTestCase(SnapshotTestCase):
    def test_dispatch_creates_state_row_with_one_hour_backoff(self):
        with patch.object(compute_summary_task, "delay"):
            sweep_snapshots(self.cfg, NOW)
        state = AnalyticsCatchupState.objects.get(kind="snapshot", key="summary")
        self.assertEqual(state.attempts, 1)
        self.assertEqual(state.next_attempt_at, NOW + timedelta(hours=1))

    def test_second_run_before_backoff_elapses_does_not_redispatch(self):
        with patch.object(compute_summary_task, "delay") as mock_delay:
            sweep_snapshots(self.cfg, NOW)
            get_redis().delete(_in_flight_key("summary"))  # first run "finished"
            mock_delay.reset_mock()
            dispatched, _given_up = sweep_snapshots(
                self.cfg, NOW + timedelta(minutes=30)
            )
        mock_delay.assert_not_called()
        self.assertEqual(dispatched, [])
        state = AnalyticsCatchupState.objects.get(kind="snapshot", key="summary")
        self.assertEqual(state.attempts, 1)

    def test_stable_failure_gives_up_after_max_attempts_and_logs_once(self):
        self._seed_other_fresh("tvl")
        AnalyticsCatchupState.objects.create(
            kind="snapshot", key="tvl", attempts=MAX_ATTEMPTS
        )
        with patch.object(compute_tvl_task, "delay") as mock_delay:
            with self.assertLogs(SWEEPER_LOGGER, "ERROR") as cm:
                dispatched, given_up = sweep_snapshots(self.cfg, NOW)
        mock_delay.assert_not_called()
        self.assertEqual(dispatched, [])
        self.assertIn("tvl", given_up)
        self.assertTrue(
            any(
                f"analytics.catchup.gave_up snapshot=tvl attempts={MAX_ATTEMPTS}"
                in line
                for line in cm.output
            )
        )

        with patch.object(compute_tvl_task, "delay"):
            dispatched2, given_up2 = sweep_snapshots(self.cfg, NOW)
        self.assertEqual(dispatched2, [])
        self.assertIn("tvl", given_up2)
        # Second time excluded, no second ERROR.
        state = AnalyticsCatchupState.objects.get(kind="snapshot", key="tvl")
        self.assertIsNotNone(state.gave_up_logged_at)

    def test_row_older_than_24h_without_fresh_snapshot_resets_attempts(self):
        AnalyticsCatchupState.objects.create(
            kind="snapshot", key="tvl", attempts=MAX_ATTEMPTS
        )
        AnalyticsCatchupState.objects.filter(kind="snapshot", key="tvl").update(
            first_seen_at=NOW - timedelta(hours=25),
            gave_up_logged_at=NOW - timedelta(hours=25),
        )
        with patch.object(compute_tvl_task, "delay") as mock_delay:
            dispatched, given_up = sweep_snapshots(self.cfg, NOW)
        mock_delay.assert_called_once()
        self.assertIn("tvl", dispatched)
        self.assertEqual(given_up, [])
        state = AnalyticsCatchupState.objects.get(kind="snapshot", key="tvl")
        self.assertEqual(state.attempts, 1)
        self.assertIsNone(state.gave_up_logged_at)


class SuccessClearsStateTestCase(SnapshotTestCase):
    def test_snapshot_becoming_fresh_deletes_its_state_row(self):
        self._seed_other_fresh("summary")
        AnalyticsCatchupState.objects.create(kind="snapshot", key="summary", attempts=2)
        AnalyticsSnapshot.objects.create(
            name="summary", payload={}, computed_at=NOW - timedelta(hours=1)
        )
        dispatched, given_up = sweep_snapshots(self.cfg, NOW)
        self.assertEqual(dispatched, [])
        self.assertEqual(given_up, [])
        self.assertFalse(
            AnalyticsCatchupState.objects.filter(
                kind="snapshot", key="summary"
            ).exists()
        )


class FinalSummaryLineTestCase(SnapshotTestCase):
    def test_run_catchup_summary_line_includes_snapshot_fields(self):
        from safe_transaction_service.analytics.catchup.sweeper import run_catchup

        RUN_LOGGER = "safe_transaction_service.analytics.catchup.sweeper"
        with (
            patch.object(compute_summary_task, "delay"),
            patch.object(compute_safe_segments_task, "delay"),
            patch.object(compute_tvl_task, "delay"),
        ):
            with self.assertLogs(RUN_LOGGER, "INFO") as cm:
                run_catchup()
        summary = [line for line in cm.output if "analytics.catchup:" in line]
        self.assertEqual(len(summary), 1)
        self.assertIn("snapshots_dispatched=", summary[0])
        self.assertIn("snapshots_given_up=", summary[0])
        for name in ("summary", "safe_segments", "tvl"):
            self.assertIn(name, summary[0])
