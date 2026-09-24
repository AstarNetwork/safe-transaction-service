"""Tests for safe_transaction_service.analytics.conf — lazy catch-up settings.

No database access: these are pure environment/validation tests, so
``SimpleTestCase`` is enough. ``reset_catchup_settings_cache()`` runs before
and after every test so env-var patches from one test never leak into the
next via ``functools.cache``.
"""

import importlib.util
import os
from dataclasses import FrozenInstanceError
from unittest.mock import patch

from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase

from safe_transaction_service.analytics import conf
from safe_transaction_service.analytics.conf import (
    CatchupSettings,
    get_catchup_settings,
    reset_catchup_settings_cache,
)

_ALL_CATCHUP_ENV_VARS = [
    "ANALYTICS_DAY_SETTLE_MINUTES",
    "ANALYTICS_CATCHUP_WINDOW_DAYS",
    "ANALYTICS_CATCHUP_MAX_DAYS_PER_RUN",
    "ANALYTICS_CATCHUP_MAX_ATTEMPTS",
    "ANALYTICS_CATCHUP_BACKFILL_STALE_HOURS",
    "ANALYTICS_CATCHUP_PROCESSING_STUCK_HOURS",
    "ANALYTICS_CATCHUP_SNAPSHOT_STALE_HOURS",
]


class GetCatchupSettingsTestCase(SimpleTestCase):
    def setUp(self):
        reset_catchup_settings_cache()
        self.addCleanup(reset_catchup_settings_cache)
        # Belt-and-braces: a stray value from the real environment/.env.test
        # must not change what "defaults" means in this test module.
        for var in _ALL_CATCHUP_ENV_VARS:
            os.environ.pop(var, None)

    def test_defaults(self):
        settings = get_catchup_settings()
        self.assertEqual(
            settings,
            CatchupSettings(
                SETTLE_MINUTES=30,
                WINDOW_DAYS=14,
                MAX_DAYS_PER_RUN=1,
                MAX_ATTEMPTS=5,
                BACKFILL_STALE_HOURS=6,
                PROCESSING_STUCK_HOURS=6,
                SNAPSHOT_STALE_HOURS=26,
            ),
        )

    def test_result_is_cached_between_calls(self):
        with patch.dict(os.environ, {"ANALYTICS_CATCHUP_WINDOW_DAYS": "20"}):
            first = get_catchup_settings()
        # Env changes after the first call are ignored until the cache resets.
        with patch.dict(os.environ, {"ANALYTICS_CATCHUP_WINDOW_DAYS": "99"}):
            second = get_catchup_settings()
        self.assertIs(first, second)
        self.assertEqual(second.WINDOW_DAYS, 20)

    def test_reset_cache_rereads_env(self):
        with patch.dict(os.environ, {"ANALYTICS_CATCHUP_WINDOW_DAYS": "20"}):
            get_catchup_settings()
        reset_catchup_settings_cache()
        with patch.dict(os.environ, {"ANALYTICS_CATCHUP_WINDOW_DAYS": "21"}):
            settings = get_catchup_settings()
        self.assertEqual(settings.WINDOW_DAYS, 21)

    def test_settings_are_frozen(self):
        settings = get_catchup_settings()
        with self.assertRaises(FrozenInstanceError):
            settings.WINDOW_DAYS = 1

    # -- one constraint violation per setting --------------------------------

    def test_settle_minutes_negative_raises(self):
        with patch.dict(os.environ, {"ANALYTICS_DAY_SETTLE_MINUTES": "-1"}):
            with self.assertRaises(ImproperlyConfigured) as ctx:
                get_catchup_settings()
        self.assertIn("ANALYTICS_DAY_SETTLE_MINUTES", str(ctx.exception))

    def test_settle_minutes_zero_is_allowed(self):
        with patch.dict(os.environ, {"ANALYTICS_DAY_SETTLE_MINUTES": "0"}):
            settings = get_catchup_settings()
        self.assertEqual(settings.SETTLE_MINUTES, 0)

    def test_window_days_below_minimum_raises(self):
        with patch.dict(os.environ, {"ANALYTICS_CATCHUP_WINDOW_DAYS": "1"}):
            with self.assertRaises(ImproperlyConfigured) as ctx:
                get_catchup_settings()
        self.assertIn("ANALYTICS_CATCHUP_WINDOW_DAYS", str(ctx.exception))

    def test_max_days_per_run_below_minimum_raises(self):
        with patch.dict(os.environ, {"ANALYTICS_CATCHUP_MAX_DAYS_PER_RUN": "0"}):
            with self.assertRaises(ImproperlyConfigured) as ctx:
                get_catchup_settings()
        self.assertIn("ANALYTICS_CATCHUP_MAX_DAYS_PER_RUN", str(ctx.exception))

    def test_max_attempts_below_minimum_raises(self):
        with patch.dict(os.environ, {"ANALYTICS_CATCHUP_MAX_ATTEMPTS": "0"}):
            with self.assertRaises(ImproperlyConfigured) as ctx:
                get_catchup_settings()
        self.assertIn("ANALYTICS_CATCHUP_MAX_ATTEMPTS", str(ctx.exception))

    def test_max_attempts_backoff_exceeding_window_raises(self):
        # Default WINDOW_DAYS=14 -> 336h. MAX_ATTEMPTS=10 -> backoff sum
        # 2^9 - 1 = 511h, past the window: the day could expire mid-retry.
        with patch.dict(os.environ, {"ANALYTICS_CATCHUP_MAX_ATTEMPTS": "10"}):
            with self.assertRaises(ImproperlyConfigured) as ctx:
                get_catchup_settings()
        self.assertIn("ANALYTICS_CATCHUP_MAX_ATTEMPTS", str(ctx.exception))

    def test_max_attempts_backoff_within_window_is_allowed(self):
        with patch.dict(
            os.environ,
            {
                "ANALYTICS_CATCHUP_WINDOW_DAYS": "2",
                "ANALYTICS_CATCHUP_MAX_ATTEMPTS": "6",
            },
        ):
            # 2^5 - 1 = 31h < 2*24 = 48h.
            settings = get_catchup_settings()
        self.assertEqual(settings.MAX_ATTEMPTS, 6)

    def test_backfill_stale_hours_below_minimum_raises(self):
        with patch.dict(os.environ, {"ANALYTICS_CATCHUP_BACKFILL_STALE_HOURS": "0"}):
            with self.assertRaises(ImproperlyConfigured) as ctx:
                get_catchup_settings()
        self.assertIn("ANALYTICS_CATCHUP_BACKFILL_STALE_HOURS", str(ctx.exception))

    def test_processing_stuck_hours_below_minimum_raises(self):
        with patch.dict(os.environ, {"ANALYTICS_CATCHUP_PROCESSING_STUCK_HOURS": "0"}):
            with self.assertRaises(ImproperlyConfigured) as ctx:
                get_catchup_settings()
        self.assertIn("ANALYTICS_CATCHUP_PROCESSING_STUCK_HOURS", str(ctx.exception))

    def test_snapshot_stale_hours_below_minimum_raises(self):
        with patch.dict(os.environ, {"ANALYTICS_CATCHUP_SNAPSHOT_STALE_HOURS": "0"}):
            with self.assertRaises(ImproperlyConfigured) as ctx:
                get_catchup_settings()
        self.assertIn("ANALYTICS_CATCHUP_SNAPSHOT_STALE_HOURS", str(ctx.exception))

    # -- error message contract ----------------------------------------------

    def test_error_message_never_includes_the_bad_value(self):
        with patch.dict(os.environ, {"ANALYTICS_CATCHUP_WINDOW_DAYS": "not-an-int"}):
            with self.assertRaises(ImproperlyConfigured) as ctx:
                get_catchup_settings()
        message = str(ctx.exception)
        self.assertIn("ANALYTICS_CATCHUP_WINDOW_DAYS", message)
        self.assertNotIn("not-an-int", message)

    # -- import / django.setup() must not validate ----------------------------

    def test_importing_the_module_does_not_validate(self):
        """Loading conf.py as a fresh module object — standing in for what
        ``django.setup()`` does to every installed app's modules on startup —
        must not touch the environment at all, even with an unparseable
        value already sitting in os.environ. Only calling
        ``get_catchup_settings()`` may validate.
        """
        spec = importlib.util.spec_from_file_location(
            "_analytics_conf_import_check", conf.__file__
        )
        module = importlib.util.module_from_spec(spec)
        with patch.dict(
            os.environ,
            {
                "ANALYTICS_CATCHUP_WINDOW_DAYS": "not-an-int",
                "ANALYTICS_CATCHUP_MAX_ATTEMPTS": "-5",
            },
        ):
            spec.loader.exec_module(module)  # must not raise
        self.assertTrue(hasattr(module, "get_catchup_settings"))
        self.assertTrue(hasattr(module, "CatchupSettings"))
