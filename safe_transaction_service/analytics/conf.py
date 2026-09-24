"""Lazily-validated settings for the analytics catch-up subsystem.

Reading the ``ANALYTICS_*`` / ``ANALYTICS_CATCHUP_*`` environment variables at
import time (e.g. as module-level constants, the way ``config/settings/base.py``
reads most settings) would raise ``ImproperlyConfigured`` from inside
``INSTALLED_APPS`` on a typo and take down the whole tx-service — API,
indexers, workers — even when ``ENABLE_ANALYTICS`` is False. See "Analytics
catch-up" in ``analytics/implementation-notes.md``.

So validation happens only the first time :func:`get_catchup_settings` is
called (cached afterwards via :func:`functools.cache`), and only analytics
code is expected to call it: ``run_catchup``, ``compute_day``,
``get_indexer_status``, and the ``analytics_settle_status`` /
``backfill_daily_metrics`` management commands. Importing this module, or any
other module in ``analytics/``, must never trigger validation on its own.
"""

from dataclasses import dataclass
from functools import cache

from django.core.exceptions import ImproperlyConfigured

import environ


@dataclass(frozen=True)
class CatchupSettings:
    """Validated analytics catch-up settings, read once from the environment."""

    SETTLE_MINUTES: int
    WINDOW_DAYS: int
    MAX_DAYS_PER_RUN: int
    MAX_ATTEMPTS: int
    BACKFILL_STALE_HOURS: int
    PROCESSING_STUCK_HOURS: int
    SNAPSHOT_STALE_HOURS: int


def _bad_config(var_name: str) -> ImproperlyConfigured:
    # Deliberately does not include the offending value: it may be logged
    # (`analytics.catchup.bad_config setting=… reason=…`) and the value isn't
    # needed to fix a typo'd env var.
    return ImproperlyConfigured(f"analytics: invalid value for {var_name}")


def _read_int(env: "environ.Env", var_name: str, default: int) -> int:
    try:
        return env.int(var_name, default=default)
    except (ValueError, TypeError):
        raise _bad_config(var_name) from None


def _require(condition: bool, var_name: str) -> None:
    if not condition:
        raise _bad_config(var_name) from None


@cache
def get_catchup_settings() -> CatchupSettings:
    """Read, validate and cache the analytics catch-up settings.

    Raises ``django.core.exceptions.ImproperlyConfigured`` (naming the
    offending variable, never its value) if any constraint below is
    violated. Cached via ``functools.cache``; use
    :func:`reset_catchup_settings_cache` to force a re-read (tests only).
    """
    env = environ.Env()

    settle_minutes = _read_int(env, "ANALYTICS_DAY_SETTLE_MINUTES", 30)
    _require(settle_minutes >= 0, "ANALYTICS_DAY_SETTLE_MINUTES")

    window_days = _read_int(env, "ANALYTICS_CATCHUP_WINDOW_DAYS", 14)
    _require(window_days >= 2, "ANALYTICS_CATCHUP_WINDOW_DAYS")

    max_days_per_run = _read_int(env, "ANALYTICS_CATCHUP_MAX_DAYS_PER_RUN", 1)
    _require(max_days_per_run >= 1, "ANALYTICS_CATCHUP_MAX_DAYS_PER_RUN")

    max_attempts = _read_int(env, "ANALYTICS_CATCHUP_MAX_ATTEMPTS", 5)
    _require(max_attempts >= 1, "ANALYTICS_CATCHUP_MAX_ATTEMPTS")
    # Sum of the backoff pauses (1, 2, 4, …, 2^(MAX_ATTEMPTS-2) hours) must fit
    # inside the sweep window, or a day could expire before it runs out of
    # attempts.
    _require(
        (2 ** (max_attempts - 1)) - 1 < window_days * 24,
        "ANALYTICS_CATCHUP_MAX_ATTEMPTS",
    )

    backfill_stale_hours = _read_int(env, "ANALYTICS_CATCHUP_BACKFILL_STALE_HOURS", 6)
    _require(backfill_stale_hours >= 1, "ANALYTICS_CATCHUP_BACKFILL_STALE_HOURS")

    processing_stuck_hours = _read_int(
        env, "ANALYTICS_CATCHUP_PROCESSING_STUCK_HOURS", 6
    )
    _require(processing_stuck_hours >= 1, "ANALYTICS_CATCHUP_PROCESSING_STUCK_HOURS")

    snapshot_stale_hours = _read_int(env, "ANALYTICS_CATCHUP_SNAPSHOT_STALE_HOURS", 26)
    _require(snapshot_stale_hours >= 1, "ANALYTICS_CATCHUP_SNAPSHOT_STALE_HOURS")

    return CatchupSettings(
        SETTLE_MINUTES=settle_minutes,
        WINDOW_DAYS=window_days,
        MAX_DAYS_PER_RUN=max_days_per_run,
        MAX_ATTEMPTS=max_attempts,
        BACKFILL_STALE_HOURS=backfill_stale_hours,
        PROCESSING_STUCK_HOURS=processing_stuck_hours,
        SNAPSHOT_STALE_HOURS=snapshot_stale_hours,
    )


def reset_catchup_settings_cache() -> None:
    """Test-only helper: drop the cached settings so the next call re-reads env."""
    get_catchup_settings.cache_clear()
