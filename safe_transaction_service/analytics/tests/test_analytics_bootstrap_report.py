"""Unit tests for `build_bootstrap_report()` (`analytics/bootstrap/report.py`)
-- the read model behind `/summary/`'s `bootstrap` object and
`analytics_bootstrap --status`. View-level assertions (the `bootstrap`
key showing up in `/summary/`, and the report-raises fallback) live in
`test_views_v2.py`; this file exercises the report function directly,
with fake stages standing in for the real daily/native/ERC-20 ones so
each state can be arranged without their own backend machinery.
"""

from unittest.mock import patch

from django.test import TestCase, override_settings

from safe_transaction_service.analytics.bootstrap import Stage, build_bootstrap_report
from safe_transaction_service.analytics.bootstrap.bookkeeping import (
    mark_completed,
    mark_gave_up,
)
from safe_transaction_service.analytics.models import AnalyticsBootstrapStage

from .catchup_gate_fixture import clear_bootstrap_gate_state


class _FakeStage(Stage):
    def __init__(self, name, status_value="pending", done=False, progress=None):
        self.name = name
        self._status = status_value
        self._done = done
        self._progress = progress

    def is_done(self):
        return self._done

    def status(self):
        return self._status

    def start_or_resume(self):
        raise AssertionError("build_bootstrap_report must never dispatch anything")

    def progress(self):
        return self._progress


def _patched_stages(stages):
    return patch(
        "safe_transaction_service.analytics.bootstrap.report.build_default_stages",
        return_value=stages,
    )


class TestFreshInstanceMakesNoWrites(TestCase):
    def test_no_rows_at_all_and_no_writes_happen(self):
        self.assertEqual(AnalyticsBootstrapStage.objects.count(), 0)
        stages = [_FakeStage("daily"), _FakeStage("native"), _FakeStage("erc20")]

        with _patched_stages(stages):
            report = build_bootstrap_report()

        self.assertEqual(AnalyticsBootstrapStage.objects.count(), 0)
        self.assertFalse(report["complete"])
        self.assertEqual(report["current_stage"], "daily")
        for name in ("daily", "native", "erc20"):
            stage_report = report["stages"][name]
            self.assertEqual(stage_report["state"], "pending")
            self.assertEqual(stage_report["consecutive_failures"], 0)
            self.assertIsNone(stage_report["completed_at"])
            self.assertIsNone(stage_report["gave_up_at"])
            self.assertIsNone(stage_report["last_dispatch_at"])


class TestShape(TestCase):
    def setUp(self):
        super().setUp()
        # Redis isn't rolled back between tests the way the database is;
        # an earlier test's `indexer_caught_up()` call would otherwise
        # leak its verdict into this one's "nothing written yet" assertion.
        clear_bootstrap_gate_state()

    @override_settings(ANALYTICS_BOOTSTRAP_DAILY_DAYS=45)
    def test_top_level_keys_and_enabled_flag(self):
        stages = [_FakeStage("daily"), _FakeStage("native"), _FakeStage("erc20")]

        with _patched_stages(stages):
            report = build_bootstrap_report()

        self.assertEqual(
            set(report),
            {
                "enabled",
                "complete",
                "daily_depth",
                "current_stage",
                "indexer_gate",
                "stages",
            },
        )
        self.assertTrue(report["enabled"])
        self.assertEqual(report["daily_depth"], 45)
        self.assertIsNone(report["indexer_gate"])

    @override_settings(ANALYTICS_AUTO_BACKFILL=False)
    def test_disabled_via_auto_backfill_switch(self):
        stages = [_FakeStage("daily"), _FakeStage("native"), _FakeStage("erc20")]

        with _patched_stages(stages):
            report = build_bootstrap_report()

        self.assertFalse(report["enabled"])


class TestStageStates(TestCase):
    def test_done_gave_up_and_running_states(self):
        mark_completed("daily", depth=90)
        mark_gave_up("native")
        stages = [
            _FakeStage("daily", status_value="pending"),
            _FakeStage("native", status_value="pending"),
            _FakeStage("erc20", status_value="running"),
        ]

        with (
            override_settings(ANALYTICS_BOOTSTRAP_DAILY_DAYS=90),
            _patched_stages(stages),
        ):
            report = build_bootstrap_report()

        self.assertEqual(report["stages"]["daily"]["state"], "done")
        self.assertEqual(report["stages"]["native"]["state"], "gave_up")
        self.assertEqual(report["stages"]["erc20"]["state"], "running")
        # daily and native are terminal (row-first); erc20 -- pending row,
        # live "running" -- is the one the tick would act on next.
        self.assertEqual(report["current_stage"], "erc20")

    def test_daily_depth_growth_reopens_only_daily_without_writing(self):
        mark_completed("daily", depth=30)
        mark_completed("native")
        mark_completed("erc20")
        stages = [
            _FakeStage("daily", status_value="pending"),
            _FakeStage("native", status_value="pending"),
            _FakeStage("erc20", status_value="pending"),
        ]

        with (
            override_settings(ANALYTICS_BOOTSTRAP_DAILY_DAYS=90),
            _patched_stages(stages),
        ):
            report = build_bootstrap_report()

        self.assertFalse(report["complete"])
        self.assertEqual(report["stages"]["daily"]["state"], "pending")
        self.assertEqual(report["stages"]["native"]["state"], "done")
        self.assertEqual(report["stages"]["erc20"]["state"], "done")
        self.assertEqual(report["current_stage"], "daily")
        # Read-only: the row itself stays at its old depth until a real
        # tick (`maybe_reopen_daily`) clears it.
        self.assertEqual(
            AnalyticsBootstrapStage.objects.get(name="daily").completed_depth, 30
        )

    def test_progress_is_surfaced_only_when_the_stage_provides_it(self):
        stages = [
            _FakeStage("daily", progress={"completed_days": 1, "total_days": 90}),
            _FakeStage("native"),
            _FakeStage("erc20"),
        ]

        with _patched_stages(stages):
            report = build_bootstrap_report()

        self.assertEqual(
            report["stages"]["daily"]["progress"],
            {"completed_days": 1, "total_days": 90},
        )
        self.assertIsNone(report["stages"]["native"]["progress"])
