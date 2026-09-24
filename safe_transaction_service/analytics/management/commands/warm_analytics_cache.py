"""``--skip-if-fresh`` for `summary` / `safe_segments` / `tvl` decides
freshness from Postgres (`AnalyticsSnapshot.computed_at` / `native_source`),
via the same `is_snapshot_stale` the snapshot sweeper uses, instead of a
Redis probe -- the Redis keys those payloads used to be cached under have
since been dropped, so the old probe never actually skipped anything for
these three. The other tasks in `TASKS` are unaffected.

See "Analytics catch-up" in `analytics/implementation-notes.md`.
"""

import json
from datetime import datetime, timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from safe_transaction_service.analytics.catchup import SNAPSHOT_NAMES, is_snapshot_stale
from safe_transaction_service.analytics.conf import get_catchup_settings
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


class Command(BaseCommand):
    help = (
        "Enqueue all analytics precompute tasks on Celery workers to populate "
        "Redis. Intended for post-deploy warm-up; returns as soon as the tasks "
        "are dispatched (the actual work runs on workers, not in this process)."
    )

    # (label, task_callable, freshness_probe_key, timestamp_field)
    # `timestamp_field` is None for payloads without a timestamp (existence-only
    # check); `freshness_probe_key` is None for work that has no Redis payload to
    # probe at all, which `--skip-if-fresh` therefore never skips based on a
    # probe -- this also covers `summary` / `safe_segments` / `tvl`, whose
    # labels are their `SNAPSHOT_NAMES` entry and are freshness-checked
    # against Postgres instead, ahead of the probe_key/ts_field pair (see
    # `_is_fresh`).
    TASKS = [
        ("summary", compute_summary_task, None, None),
        (
            "transactions_per_safe_app",
            get_transactions_per_safe_app_task,
            AnalyticsService.REDIS_TRANSACTIONS_PER_SAFE_APP,
            None,
        ),
        (
            "active_safes",
            compute_active_safes_task,
            AnalyticsService.REDIS_ACTIVE_SAFES_PREFIX + "30d",
            "computed_at",
        ),
        (
            "active_owners",
            compute_active_owners_task,
            AnalyticsService.REDIS_ACTIVE_OWNERS_PREFIX + "30d",
            "computed_at",
        ),
        ("safe_segments", compute_safe_segments_task, None, None),
        # Before `tvl`, which reads the rollup this advances. Both are
        # fire-and-forget dispatches so the ordering is a hint, not a
        # guarantee — and it does not need to be one: a TVL run that
        # overtakes the rollup just publishes yesterday's native side and
        # the next run catches up.
        (
            "native_balance_rollup",
            compute_native_balance_rollup_task,
            None,
            None,
        ),
        ("tvl", compute_tvl_task, None, None),
        (
            "safe_creations",
            compute_safe_creations_task,
            AnalyticsService.REDIS_SAFE_CREATIONS,
            "computed_at",
        ),
    ]

    def add_arguments(self, parser):
        parser.add_argument(
            "--skip-if-fresh",
            action="store_true",
            help=(
                "Skip tasks whose cached payload is newer than "
                "--fresh-window-hours. Container-restart-safe."
            ),
        )
        parser.add_argument(
            "--fresh-window-hours",
            type=int,
            default=6,
            help="Hours threshold for --skip-if-fresh (default: 6).",
        )

    def handle(self, *args, **options):
        skip_if_fresh = options["skip_if_fresh"]
        threshold = timedelta(hours=options["fresh_window_hours"])
        now = timezone.now()
        # Only read when actually needed -- this can raise `ImproperlyConfigured`
        # on a bad env var, and a plain (non-`--skip-if-fresh`) dispatch run
        # should not be able to fail because of that.
        cfg = get_catchup_settings() if skip_if_fresh else None

        self.stdout.write("Enqueuing analytics warm-up tasks...")
        for label, task, probe_key, ts_field in self.TASKS:
            if skip_if_fresh and self._is_fresh(
                label, probe_key, ts_field, now, threshold, cfg
            ):
                self.stdout.write(f"  {label}: skipped (fresh)")
                continue
            try:
                async_result = task.delay()
                self.stdout.write(
                    self.style.SUCCESS(f"  {label}: enqueued ({async_result.id})")
                )
            except Exception as exc:
                self.stderr.write(self.style.ERROR(f"  {label}: enqueue FAILED: {exc}"))
        self.stdout.write(self.style.SUCCESS("Cache warm-up dispatch complete"))

    @staticmethod
    def _is_fresh(
        label: str,
        probe_key: str | None,
        ts_field: str | None,
        now,
        threshold: timedelta,
        cfg,
    ) -> bool:
        if label in SNAPSHOT_NAMES:
            # Same rule the snapshot sweeper uses: a Postgres row, not a
            # Redis probe -- `cfg` is guaranteed set here since this branch
            # only runs when `skip_if_fresh` is True.
            return not is_snapshot_stale(label, cfg.SNAPSHOT_STALE_HOURS, now)
        if probe_key is None:
            # Nothing to probe — the rollup's freshness lives in a Postgres
            # watermark, not a Redis payload. Never skip it; the task is
            # cheap and no-ops when there is nothing new to consume.
            return False
        blob = get_redis().get(probe_key)
        if not blob:
            return False
        if ts_field is None:
            # No timestamp in the payload (e.g. a list) — existence is enough.
            return True
        try:
            payload = json.loads(blob)
        except json.JSONDecodeError:
            return False
        if not isinstance(payload, dict):
            return False
        ts_str = payload.get(ts_field)
        if not ts_str:
            return False
        try:
            ts = datetime.fromisoformat(ts_str)
        except (TypeError, ValueError):
            return False
        if ts.tzinfo is None:
            # Defensive: payloads have always been written with tz-aware
            # `timezone.now().isoformat()`, but treat naive as stale to be safe.
            return False
        return (now - ts) < threshold
