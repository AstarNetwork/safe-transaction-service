import json
import logging
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta
from functools import cache

from django.db.models import Count, DecimalField, Sum, Value
from django.db.models.functions import Coalesce
from django.utils import timezone

from safe_transaction_service import __version__
from safe_transaction_service.history.models import (
    ERC20Transfer,
)
from safe_transaction_service.utils.redis import get_redis

logger = logging.getLogger(__name__)


@cache
def get_analytics_service() -> "AnalyticsService":
    return AnalyticsService()


def _parse_window(window: str) -> int | None:
    """Parse window string like '7d', '30d', '90d' into days. Returns None if invalid."""
    window = window.strip().lower()
    if window.endswith("d"):
        try:
            return int(window[:-1])
        except ValueError:
            return None
    return None


_COMPUTE_LOCK_TTL_SECONDS = 1800  # max expected task duration (30 min)
_COMPUTE_WAIT_SECONDS = 25  # how long a non-leader request will block
_COMPUTE_POLL_INTERVAL_SECONDS = 0.5

# TTL for the SETNX "is a refresh already in flight?" guard used by
# `AnalyticsService._maybe_dispatch_refresh`. Bound at the same max task
# duration as `_COMPUTE_LOCK_TTL_SECONDS` so a crashed worker can't keep
# the lock forever, but long enough that ordinary concurrent miss-reads
# all coalesce onto a single Celery dispatch.
_REFRESH_LOCK_TTL_SECONDS = 1800


# Empty payloads returned on a cold snapshot read. Shapes match the
# legacy `redis-miss` responses so existing API clients see no schema
# change between cold and warm — the only observable difference is that
# cold reads no longer block for 25s on Celery to finish.
EMPTY_SUMMARY_PAYLOAD: dict = {
    "total_safes": 0,
    "total_multisig_txs": 0,
    "total_module_txs": 0,
    "total_erc20_transfers": 0,
    "total_erc721_transfers": 0,
    "first_safe_created": None,
    "last_safe_created": None,
    "computed_at": None,
}
EMPTY_SAFE_SEGMENTS_PAYLOAD: dict = {
    "personal": 0,
    "team": 0,
    "enterprise": 0,
    "with_modules": 0,
    "avg_threshold": 0.0,
    "avg_owners": 0.0,
    "computed_at": None,
}
EMPTY_TVL_PAYLOAD: dict = {
    "total_safes_with_balance": 0,
    "native_balance_wei": "0",
    "erc20_token_count": 0,
    "top_tokens": [],
    # 0/0 distinguishes "snapshot never computed" from a real partial run
    # (where total_shards == 16 and partial_shards > 0).
    "partial_shards": 0,
    "total_shards": 0,
    "computed_at": None,
}


def _redis_get_or_compute(redis_key: str, task_callable: Callable) -> dict | None:
    """
    Read a precomputed analytics payload from Redis. On miss the leader
    *dispatches* the compute to Celery (rather than running it inline) and
    falls into the same polling loop as every concurrent miss-request.

    Running the compute inline in a gunicorn worker guaranteed an nginx 504
    on the first request after a cold cache, because task durations
    legitimately exceed the request timeout on large chains. Handing it to
    Celery lets the request return promptly with the fallback payload while
    the compute lands in the background; subsequent requests pick up the
    warm cache.

    Thundering-herd protection: leadership is taken via Redis SETNX on
    `{redis_key}:compute_lock` so N concurrent miss-requests dispatch the
    task only once. The leader releases the lock as soon as the cache lands
    inside the per-request poll deadline; otherwise the lock TTL bounds
    redispatch in the case of a crashed/stuck worker.

    Returns the parsed JSON dict, or None if no result is available within
    `_COMPUTE_WAIT_SECONDS`.
    """
    redis = get_redis()
    blob = redis.get(redis_key)
    if blob:
        return json.loads(blob)

    lock_key = f"{redis_key}:compute_lock"
    is_leader = redis.set(lock_key, "1", nx=True, ex=_COMPUTE_LOCK_TTL_SECONDS)

    dispatched_async = False
    if is_leader:
        # In tests / non-Celery callers `task_callable` may be a plain
        # function (e.g. a `lambda: None` patched in to simulate a hard
        # failure). Fall back to inline execution in that case so existing
        # test fixtures keep working without dragging a Celery broker in.
        # Exceptions from the dispatch / inline call are swallowed so a
        # broker outage or a buggy inline callable falls through to the
        # view's fallback payload instead of 500-ing.
        try:
            if hasattr(task_callable, "delay"):
                task_callable.delay()
                dispatched_async = True
            else:
                task_callable()
        except Exception:
            pass

        # Eager mode and inline mode: the task has already finished by the
        # time we get here. If it wrote a result, return it; otherwise
        # there's no point polling — release the lock and surface None.
        if not dispatched_async:
            blob = redis.get(redis_key)
            redis.delete(lock_key)
            return json.loads(blob) if blob else None

        # Async leader: a quick early check covers the (rare) case where
        # the Celery worker has already finished by the time we get here,
        # so we don't pay an unnecessary poll interval.
        blob = redis.get(redis_key)
        if blob is not None:
            redis.delete(lock_key)
            return json.loads(blob)

    # Poll for the result. Non-leader callers always end up here; the
    # async leader also waits here for the Celery worker to publish.
    deadline = time.monotonic() + _COMPUTE_WAIT_SECONDS
    while time.monotonic() < deadline:
        time.sleep(_COMPUTE_POLL_INTERVAL_SECONDS)
        blob = redis.get(redis_key)
        if blob:
            if is_leader:
                redis.delete(lock_key)
            return json.loads(blob)
    return None


def _week_key(iso_period: str) -> str:
    """Map an ISO date string to its ISO-week key, e.g. '2026-W20'. Used
    as the deduplication key for bucketing rows."""
    y, w, _ = date.fromisoformat(iso_period).isocalendar()
    return f"{y}-W{w:02d}"


def _week_label(iso_period: str) -> str:
    """Return the Monday of the ISO week containing `iso_period`. The label
    must be stable regardless of which day in the week first appeared in
    the source series, so chart consumers expecting ISO 8601 week-start get
    a Monday every time."""
    d = date.fromisoformat(iso_period)
    return (d - timedelta(days=d.weekday())).isoformat()


def _month_key(iso_period: str) -> str:
    """Map an ISO date string to its calendar month key, e.g. '2026-05'."""
    return iso_period[:7]


def _month_label(iso_period: str) -> str:
    """Return the first day of the month containing `iso_period`. Same
    stability rationale as `_week_label`."""
    return iso_period[:7] + "-01"


def _resample_day_series(series: list[dict], interval: str) -> list[dict]:
    """
    Bucket a day-grain `[{period, count}]` series into week or month bins.
    The `period` label is normalized to the first day of the bucket
    (Monday for week, 1st for month) — independent of which source day
    first landed in that bucket.
    """
    if interval == "day":
        return series
    if interval == "week":
        bucket_fn, label_fn = _week_key, _week_label
    else:
        bucket_fn, label_fn = _month_key, _month_label
    buckets: dict[str, dict] = {}
    for row in series:
        key = bucket_fn(row["period"])
        if key not in buckets:
            buckets[key] = {"period": label_fn(row["period"]), "count": 0}
        buckets[key]["count"] += row["count"]
    return list(buckets.values())


def _in_range(iso_period: str, date_from, date_to) -> bool:
    if not date_from and not date_to:
        return True
    d = date.fromisoformat(iso_period)
    if date_from and d < _as_date(date_from):
        return False
    if date_to and d > _as_date(date_to):
        return False
    return True


def _as_date(value) -> date:
    return value.date() if isinstance(value, datetime) else value


class AnalyticsService:
    REDIS_TRANSACTIONS_PER_SAFE_APP = "analytics_transactions_per_safe_app"
    REDIS_ACTIVE_SAFES_PREFIX = "analytics_active_safes_"
    REDIS_ACTIVE_OWNERS_PREFIX = "analytics_active_owners_"
    REDIS_SAFE_CREATIONS = "analytics_safe_creations"
    # Legacy keys — `summary` / `safe_segments` / `tvl` moved from Redis
    # to the `analytics_analyticssnapshot` table in this release.
    # Constants kept for one release as documentation pointers, then
    # deleted in a follow-up (see `flickering-honking-wand.md`
    # §"Decommissioned"). Don't write to them.
    REDIS_SAFE_SEGMENTS = "analytics_safe_segments"
    REDIS_TVL = "analytics_tvl"
    REDIS_SUMMARY = "analytics_summary"

    def _read_snapshot_or_empty(
        self, name: str, empty: dict, refresh_task: Callable
    ) -> dict:
        """Read the most recent ``AnalyticsSnapshot`` row by name.

        If absent (cold deploy / empty table / mid-deploy), fire-and-forget
        dispatch the refresh task (SETNX-locked to coalesce concurrent
        miss-reads onto one dispatch) and return ``empty`` immediately.

        Crucially: this NEVER blocks the request waiting for the compute.
        That blocking is what produced the 25s gunicorn timeout / 504 on
        BASE before this rewrite — see `flickering-honking-wand.md` §
        Context.
        """
        from safe_transaction_service.analytics.models import AnalyticsSnapshot

        try:
            snap = AnalyticsSnapshot.objects.get(name=name)
        except AnalyticsSnapshot.DoesNotExist:
            logger.info("analytics.snapshot.cold_read name=%s", name)
            self._maybe_dispatch_refresh(name, refresh_task)
            return dict(empty)
        return {**snap.payload, "computed_at": snap.computed_at.isoformat()}

    def _maybe_dispatch_refresh(self, name: str, task: Callable) -> None:
        """Take a SETNX lock on the snapshot name and dispatch the refresh
        task if leader. The lock prevents a herd of concurrent miss-reads
        from all kicking off the same expensive compute.

        In tests / non-Celery callers ``task`` may be a plain function
        (no ``.delay``). In that case skip dispatch — the caller-side
        test will trigger the compute directly. Exceptions are swallowed:
        a broker outage falls through to the next scheduled run.
        """
        lock_key = f"analytics_snapshot:{name}:refresh_lock"
        redis = get_redis()
        try:
            is_leader = redis.set(lock_key, "1", nx=True, ex=_REFRESH_LOCK_TTL_SECONDS)
        except Exception:
            logger.exception("analytics.snapshot.refresh_lock_failed name=%s", name)
            return
        if not is_leader:
            return
        try:
            if hasattr(task, "delay"):
                task.delay()
                logger.info("analytics.snapshot.refresh_dispatched name=%s", name)
        except Exception:
            logger.exception("analytics.snapshot.refresh_dispatch_failed name=%s", name)

    def get_safe_transactions_per_safe_app(self) -> list[dict]:
        """Group multisig tx counts by origin name + URL.

        Totals come from the ``analytics_dailysafeapptx`` rollup
        (constant-time window scan, regardless of
        ``history_multisigtransaction`` size). On a cold rollup (fresh
        deploy / mid-backfill) we fall back to the legacy Redis-cached
        payload populated by ``get_transactions_per_safe_app_task``.
        """
        payload = self._get_safe_app_txs_from_rollup()
        if payload:
            return payload
        logger.info("analytics.rollup.cold_window key=safe_app_txs")

        redis = get_redis()
        analytic_result = redis.get(self.REDIS_TRANSACTIONS_PER_SAFE_APP)
        if analytic_result:
            return json.loads(analytic_result)
        return []

    def _get_safe_app_txs_from_rollup(self) -> list[dict]:
        """Build the `[{name, url, total_tx, tx_last_week, ...}]` payload
        entirely from ``analytics_dailysafeapptx`` — no read-time touch
        of ``history_multisigtransaction``.

        `origin_url` is denormalised onto the rollup (see
        `DailySafeAppTx`). The most-recent non-empty URL wins per name
        when an app ships under multiple URLs in the window.
        """
        from safe_transaction_service.analytics.models import DailySafeAppTx

        today = timezone.now().date()
        week_ago = today - timedelta(days=7)
        month_ago = today - timedelta(days=30)
        year_ago = today - timedelta(days=365)

        rows = list(
            DailySafeAppTx.objects.filter(date__gte=year_ago)
            .order_by("date")
            .values("date", "origin_name", "origin_url", "tx_count")
        )
        if not rows:
            return []

        agg: dict[str, dict] = {}
        for r in rows:
            name = r["origin_name"]
            d = r["date"]
            c = int(r["tx_count"] or 0)
            slot = agg.setdefault(
                name,
                {
                    "name": name,
                    "url": "",
                    "total_tx": 0,
                    "tx_last_week": 0,
                    "tx_last_month": 0,
                    "tx_last_year": 0,
                },
            )
            slot["total_tx"] += c
            if d >= week_ago:
                slot["tx_last_week"] += c
            if d >= month_ago:
                slot["tx_last_month"] += c
            slot["tx_last_year"] += c
            # Rows are ordered by date asc, so the last non-empty URL
            # we see is the most recent one — same collapse the legacy
            # aggregate did silently.
            if r["origin_url"]:
                slot["url"] = r["origin_url"]

        return sorted(agg.values(), key=lambda r: r["total_tx"], reverse=True)

    # ── A.1 Summary (snapshot table, populated by compute_summary_task) ──

    def get_summary(self) -> dict:
        """Read the persisted ``summary`` snapshot, overlay request-time
        fields (`chain_id`, `service_version`).

        Replaces the Redis-cached + 25s-poll read path. ``chain_id`` /
        ``service_version`` stay at request time per spec — cheap, and
        keeps the existing chain_id RPC test mocks working.
        """
        from safe_transaction_service.analytics.tasks import (
            compute_summary_task,
        )
        from safe_transaction_service.utils.ethereum import get_chain_id

        cached = self._read_snapshot_or_empty(
            "summary", EMPTY_SUMMARY_PAYLOAD, compute_summary_task
        )
        return {
            "total_safes": cached.get("total_safes", 0),
            "total_multisig_txs": cached.get("total_multisig_txs", 0),
            "total_module_txs": cached.get("total_module_txs", 0),
            "total_erc20_transfers": cached.get("total_erc20_transfers", 0),
            "total_erc721_transfers": cached.get("total_erc721_transfers", 0),
            "first_safe_created": cached.get("first_safe_created"),
            "last_safe_created": cached.get("last_safe_created"),
            "chain_id": get_chain_id(),
            "service_version": __version__,
            "computed_at": cached.get("computed_at"),
        }

    # ── A.2 Active Safes (Redis-cached) ──────────────────────────────

    def get_active_safes(self, window: str) -> dict:
        """Read the window-distinct active_safes count.

        Single ``COUNT(DISTINCT safe_address)`` over the per-day
        membership table for the requested window — sub-100 ms
        regardless of ``history_*`` size. On a cold rollup we fall
        through to the Redis-cached rolling-window value populated by
        ``compute_daily_metrics_task``.
        """
        from safe_transaction_service.analytics.models import DailyActiveSafe

        days = _parse_window(window) or 30
        today = timezone.now().date()
        since = today - timedelta(days=days)
        windowed = DailyActiveSafe.objects.filter(date__gte=since)
        if windowed.exists():
            count = windowed.values("safe_address").distinct().count()
            return {
                "window": window,
                "active_safes": count,
                "computed_at": timezone.now().isoformat(),
            }
        logger.info("analytics.rollup.cold_window key=active_safes_%s", window)

        from safe_transaction_service.analytics.tasks import (
            compute_daily_metrics_task,
        )

        cached = _redis_get_or_compute(
            self.REDIS_ACTIVE_SAFES_PREFIX + window, compute_daily_metrics_task
        )
        if cached and cached.get("window") == window:
            return cached
        return {"window": window, "active_safes": 0, "computed_at": None}

    # ── A.3 Safe Creations Time Series (Redis-cached, resampled in memory) ──

    def get_safe_creations(self, date_from, date_to, interval: str) -> list[dict]:
        series = self._safe_creations_from_rollup(date_from, date_to)
        if series:
            return _resample_day_series(series, interval)
        logger.info("analytics.rollup.cold_window key=safe_creations")

        from safe_transaction_service.analytics.tasks import (
            compute_safe_creations_task,
        )

        cached = (
            _redis_get_or_compute(
                self.REDIS_SAFE_CREATIONS, compute_safe_creations_task
            )
            or {}
        )
        day_series = cached.get("series", [])
        if date_from or date_to:
            day_series = [
                row
                for row in day_series
                if _in_range(row["period"], date_from, date_to)
            ]
        return _resample_day_series(day_series, interval)

    def _safe_creations_from_rollup(self, date_from, date_to) -> list[dict]:
        from safe_transaction_service.analytics.models import DailySafeCreation

        qs = DailySafeCreation.objects.all().order_by("date")
        if date_from is not None:
            qs = qs.filter(date__gte=_as_date(date_from))
        if date_to is not None:
            qs = qs.filter(date__lte=_as_date(date_to))
        return [{"period": r.date.isoformat(), "count": r.count} for r in qs]

    # ── A.4 Active Owners (Redis-cached) ─────────────────────────────

    def get_active_owners(self, window: str) -> dict:
        """Distinct owners who confirmed any multisig tx executed in the
        window — confirmation-based active-owners semantic.

        Window DAU is a single ``COUNT(DISTINCT owner_address)`` over
        the per-day ``analytics_dailyactiveowner`` rollup — sub-100 ms
        regardless of ``history_*`` size. Replaces the prior
        ``DailyActiveSafe`` → ``SafeLastStatus`` lookup path which on
        BASE took 9–28 s for the 30d window (and 504'd on cold deploys).

        On a cold rollup we fall through to the Redis-cached
        rolling-window value populated by ``compute_daily_metrics_task``.
        """
        from safe_transaction_service.analytics.models import DailyActiveOwner

        days = _parse_window(window) or 30
        today = timezone.now().date()
        since = today - timedelta(days=days)
        windowed = DailyActiveOwner.objects.filter(date__gte=since)
        if windowed.exists():
            count = windowed.values("owner_address").distinct().count()
            return {
                "window": window,
                "active_owners": count,
                "computed_at": timezone.now().isoformat(),
            }
        logger.info("analytics.rollup.cold_window key=active_owners_%s", window)

        from safe_transaction_service.analytics.tasks import (
            compute_daily_metrics_task,
        )

        cached = _redis_get_or_compute(
            self.REDIS_ACTIVE_OWNERS_PREFIX + window, compute_daily_metrics_task
        )
        if cached and cached.get("window") == window:
            return cached
        return {"window": window, "active_owners": 0, "computed_at": None}

    # ── A.5 TX Volume (DailyMetric sum when populated, live fallback) ─

    def get_tx_volume(self, window: str) -> dict:
        """Read the tx-volume window from the `DailyMetric` rollup.

        Pure SUM over ~N day rows — single round-trip to Postgres, no
        live aggregation. The proposal-side count, executed count,
        module count, native value, and the numerator/denominator of
        `avg_confirmations` are all rolled up daily by
        `_compute_daily_tx_volume` + `_compute_daily_metric_core` (see
        `analytics/tasks.py`).

        Windowed `avg_confirmations` is computed as
            SUM(confirmations_count) / SUM(confirmed_tx_count)
        — i.e. average confirmations per (tx, day-bucket). A tx that
        gets confs on multiple days is counted once per day. For most
        txs (signed in a single sitting) this is identical to the
        per-tx average; for long-pending txs the rollup gives a
        slightly smaller number. We surface this as
        `avg_confirmations_approximation: per-tx-day` so callers can
        reason about it.

        When `DailyMetric` coverage is partial (fresh install /
        mid-backfill) the response still returns immediately and
        surfaces `coverage_days` so operators can detect the gap —
        we deliberately do NOT fall back to a 30s live query on the
        request path (that's what caused the 504s this rollup
        replaces).
        """
        days = _parse_window(window)
        if days is None:
            days = 30

        from safe_transaction_service.analytics.models import DailyMetric

        today = timezone.now().date()
        date_from = today - timezone.timedelta(days=days)
        rows = DailyMetric.objects.filter(date__gte=date_from, date__lt=today)

        agg = rows.aggregate(
            proposed=Coalesce(Sum("multisig_txs_proposed"), Value(0)),
            executed=Coalesce(Sum("multisig_txs_executed"), Value(0)),
            module=Coalesce(Sum("module_txs"), Value(0)),
            native=Coalesce(
                Sum("native_value_wei"),
                Value(0),
                output_field=DecimalField(max_digits=80, decimal_places=0),
            ),
            conf_total=Coalesce(Sum("confirmations_count"), Value(0)),
            conf_txs=Coalesce(Sum("confirmed_tx_count"), Value(0)),
            coverage=Count("date"),
        )
        conf_total = int(agg["conf_total"] or 0)
        conf_txs = int(agg["conf_txs"] or 0)
        avg_conf = round(conf_total / conf_txs, 1) if conf_txs else 0.0

        return {
            "window": window,
            "total_multisig_txs": int(agg["proposed"] or 0),
            "executed_multisig_txs": int(agg["executed"] or 0),
            "module_txs": int(agg["module"] or 0),
            "total_value_wei": str(int(agg["native"] or 0)),
            "avg_confirmations": avg_conf,
            "avg_confirmations_approximation": "per-tx-day",
            "coverage_days": int(agg["coverage"] or 0),
            "computed_at": timezone.now(),
            "source": "daily_metric",
        }

    # ── A.6 Safe Segments (Redis-cached) ─────────────────────────────

    def get_safe_segments(self) -> dict:
        """Read the persisted ``safe_segments`` snapshot.

        Replaces the Redis-cached + 25s-poll read path. See
        `flickering-honking-wand.md` Part 2.
        """
        from safe_transaction_service.analytics.tasks import (
            compute_safe_segments_task,
        )

        return self._read_snapshot_or_empty(
            "safe_segments", EMPTY_SAFE_SEGMENTS_PAYLOAD, compute_safe_segments_task
        )

    # ── A.7 TVL (canonical source: compute_tvl_task) ─────────────────

    def get_tvl(self) -> dict:
        """Read the persisted ``tvl`` snapshot (native + ERC20 written
        atomically by ``compute_tvl_task`` — single ``computed_at``, no
        drift between sources).

        Replaces the Redis-cached + 25s-poll read path. Cold reads return
        ``EMPTY_TVL_PAYLOAD`` immediately and fire-and-forget dispatch
        the refresh; the previous ``safe_statistics`` fallback was
        removed with that endpoint.
        """
        from safe_transaction_service.analytics.tasks import compute_tvl_task

        return self._read_snapshot_or_empty("tvl", EMPTY_TVL_PAYLOAD, compute_tvl_task)

    # ── A.8 Token Volume (direct query — fast enough, not cached) ────

    def get_token_volume(self, window: str) -> dict:
        days = _parse_window(window)
        if days is None:
            days = 30

        payload = self._token_volume_from_rollup(window, days)
        if payload is not None:
            return payload
        logger.info("analytics.rollup.cold_window key=token_volume_%s", window)

        cutoff = timezone.now() - timezone.timedelta(days=days)

        qs = ERC20Transfer.objects.filter(timestamp__gte=cutoff)
        total_transfers = qs.count()
        unique_tokens = qs.values("address").distinct().count()

        top_tokens = list(
            qs.values("address")
            .annotate(
                transfer_count=Count("*"),
                total_value=Sum("value"),
            )
            .order_by("-transfer_count")[:20]
        )

        return {
            "window": window,
            "total_erc20_transfers": total_transfers,
            "unique_tokens": unique_tokens,
            "top_tokens": [
                {
                    "address": t["address"],
                    "transfer_count": t["transfer_count"],
                    "total_value": str(t["total_value"] or 0),
                }
                for t in top_tokens
            ],
            "computed_at": timezone.now(),
        }

    def _token_volume_from_rollup(self, window: str, days: int) -> dict | None:
        """Build the same payload from ``analytics_daily_token_volume``.

        Returns None when the rollup is cold for the requested window so
        the caller can fall through to the live aggregation.
        """
        from safe_transaction_service.analytics.models import DailyTokenVolume

        today = timezone.now().date()
        since = today - timedelta(days=days)
        rows = list(
            DailyTokenVolume.objects.filter(date__gte=since)
            .values("token_address")
            .annotate(
                transfer_count=Sum("transfer_count"),
                total_value=Sum("transfer_value"),
            )
            .order_by("-transfer_count")
        )
        if not rows:
            return None
        total_transfers = sum(int(r["transfer_count"] or 0) for r in rows)
        unique_tokens = len(rows)
        top = rows[:20]
        return {
            "window": window,
            "total_erc20_transfers": total_transfers,
            "unique_tokens": unique_tokens,
            "top_tokens": [
                {
                    "address": t["token_address"],
                    "transfer_count": int(t["transfer_count"] or 0),
                    "total_value": str(int(t["total_value"] or 0)),
                }
                for t in top
            ],
            "computed_at": timezone.now(),
        }
