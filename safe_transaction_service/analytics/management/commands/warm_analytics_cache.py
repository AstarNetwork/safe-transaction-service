import json
from datetime import datetime, timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from safe_transaction_service.analytics.services.analytics_service import (
    AnalyticsService,
)
from safe_transaction_service.analytics.tasks import (
    compute_active_owners_task,
    compute_active_safes_task,
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
    # `timestamp_field` is None for payloads without a timestamp (existence-only check).
    TASKS = [
        (
            "summary",
            compute_summary_task,
            AnalyticsService.REDIS_SUMMARY,
            "computed_at",
        ),
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
        (
            "safe_segments",
            compute_safe_segments_task,
            AnalyticsService.REDIS_SAFE_SEGMENTS,
            "computed_at",
        ),
        ("tvl", compute_tvl_task, AnalyticsService.REDIS_TVL, "computed_at"),
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

        self.stdout.write("Enqueuing analytics warm-up tasks...")
        for label, task, probe_key, ts_field in self.TASKS:
            if skip_if_fresh and self._is_fresh(probe_key, ts_field, now, threshold):
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
        probe_key: str, ts_field: str | None, now, threshold: timedelta
    ) -> bool:
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
