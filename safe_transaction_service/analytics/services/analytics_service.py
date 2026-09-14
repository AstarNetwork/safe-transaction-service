import json
import logging
import time
from collections.abc import Callable, Iterable
from datetime import date, datetime, timedelta
from functools import cache
from itertools import groupby

from django.db.models import Count, DecimalField, F, Sum, Value, Window
from django.db.models.functions import Coalesce, RowNumber
from django.utils import timezone

from safe_transaction_service import __version__
from safe_transaction_service.history.models import (
    ERC20Transfer,
)
from safe_transaction_service.utils.redis import get_redis

logger = logging.getLogger(__name__)

#: How deep every ``top_tokens`` list goes, and the per-day cap under
#: ``breakdown=day`` — where it ships to the consumer as ``days_token_cap``
#: so a summed series is never mistaken for a complete one.
#:
#: One constant for both so the relationship is stated rather than being a
#: coincidence of two literals: a consumer that sums the per-day series to
#: rank tokens over its own range needs each day to be at least as deep as
#: the ranking it draws, and the hub draws a top-10.
#:
#: 20 is also what keeps the breakdown affordable. A day's token entry runs
#: ~140 bytes of JSON, so a 90-day series costs ~250 KB at 20, ~630 KB at 50
#: and ~125 KB at 10. The hub polls ~111 deployments a cycle: 20 buys twice
#: the depth it renders for ~28 MB a cycle, where 50 would spend ~70 MB on
#: tokens nothing displays.
TOP_TOKENS_LIMIT = 20


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


def _daily_distinct_counts(queryset, address_field: str, count_key: str) -> list[dict]:
    """Per-day distinct-address counts over one of the two DAU rollups,
    newest-first, one entry per day *present in the rollup*.

    Powers the opt-in ``breakdown=day`` series on ``/active-safes/`` and
    ``/active-owners/`` (phase-B T9). ``queryset`` is the already
    window-filtered rollup queryset, so the series and the scalar window
    value are read from exactly the same rows. A day absent from the
    rollup is absent here too, never zero-filled: a gap means "not
    computed", not "no activity". Both rollups are unique per
    ``(date, address)``, so a day that has any row has a count of at
    least one and never ``null``.

    **Contract invariant 3.** Every value here is a per-day
    ``COUNT(DISTINCT …)`` and the entries are **not additive**. Summing
    them does not yield the window value — a Safe active on three days
    contributes three entries and one distinct address — so no caller may.
    The window value stays its own ``COUNT(DISTINCT …)`` over this same
    queryset.
    """
    return [
        {"date": row["date"].isoformat(), count_key: row["distinct_count"]}
        for row in queryset.values("date")
        .annotate(distinct_count=Count(address_field, distinct=True))
        .order_by("-date")
    ]


def _rollup_date_filters(since: date | None, until: date | None) -> dict:
    """ORM filter kwargs for the date span of an active-* rollup read.

    Either edge may be ``None``, meaning *unbounded on that side*, and an
    unbounded edge contributes no filter rather than a sentinel date. Two
    shapes reach here (phase-B T10):

    - no range asked for: ``since = today - window``, ``until = None`` — the
      exact ``date__gte``-only filter these reads have always used, which is
      why the ``from``/``to``-absent response stays byte-identical;
    - a range asked for: whichever of ``from`` / ``to`` the caller supplied,
      the other side left unbounded.
    """
    filters = {}
    if since is not None:
        filters["date__gte"] = since
    if until is not None:
        filters["date__lte"] = until
    return filters


def _append_day_breakdown(
    payload: dict, since: date | None, until: date, day_rows: list[dict]
) -> dict:
    """Append the three opt-in ``breakdown=day`` keys to an already-built
    payload, in the order T8 established: ``window_start``,
    ``window_end``, ``days``.

    Called only under ``breakdown == "day"``, and only once the payload is
    otherwise complete, so the parameter-absent response keeps the exact
    key set *and insertion order* it had before — which is what makes it
    byte-identical rather than merely equal as a mapping (spec §4.9).

    ``window_start`` / ``window_end`` are the inclusive UTC bounds of the
    rows the read covers. ``window_start`` is ``null`` in the one case where
    the read has no lower bound at all — a ``to``-only range, phase-B T10 —
    because there is no honest date to name there. They ship even when
    ``day_rows`` is empty:
    they describe the *request*, and a consumer needs them to know which
    days a short series is silent about. ``days`` is therefore
    present-and-empty on a cold or warming read, never omitted — that is
    what keeps "producer predates the parameter" (key absent)
    distinguishable from "producer has no rows yet" (key empty).
    """
    payload["window_start"] = since.isoformat() if since is not None else None
    payload["window_end"] = until.isoformat()
    payload["days"] = day_rows
    return payload


def _token_volume_rollup_queryset(since: date):
    """The one ``DailyTokenVolume`` filter both halves of ``/token-volume/``
    read: the scalar window aggregate and the ``breakdown=day`` series.

    ``date__gte`` with **no upper bound**, which is the read this endpoint
    has always done and is why its window includes a partial current UTC
    day (the asymmetry the workspace contract's endpoint table records;
    ``/tx-volume/`` is the one that stops at ``date__lt=today``).

    It exists so the two halves cannot drift apart: `since` is computed
    once per request and passed down, so a request that crosses UTC
    midnight cannot aggregate one span and report the bounds of another.
    """
    from safe_transaction_service.analytics.models import DailyTokenVolume

    return DailyTokenVolume.objects.filter(date__gte=since)


def _daily_top_tokens(queryset, cap: int) -> list[dict]:
    """Each day's own top-`cap` tokens over the ``DailyTokenVolume``
    rollup, newest-first, one entry per day *present in the rollup*.

    Powers the opt-in ``breakdown=day`` series on ``/token-volume/``.
    Entry shape is ``{"date", "tokens"}`` where every token carries the
    same four keys as a scalar ``top_tokens`` entry — ``address``,
    ``symbol``, ``transfer_count``, ``total_value`` — so a consumer reads
    one shape in both halves of the payload.

    **The cap is per day, and it is lossy in one direction.** The rollup
    holds a row per ``(date, token)``, so an uncapped 90-day series on a
    busy chain would be tens of thousands of entries; the cap is applied
    in SQL (``ROW_NUMBER() OVER (PARTITION BY date …)``) so those rows are
    never fetched, not merely never serialised. The consequence a caller
    must know, and the reason ``days_token_cap`` ships beside the series:
    summing the days is exact for a token that makes its day's top-`cap`
    and **understates** one that never does — the perpetual 21st, busy
    every day and listed on none. It cannot invent volume, only miss it,
    so a range ranking built from this series is right at the top and
    thins out at the tail.

    Ordering is ``transfer_count`` desc with ``token_address`` as the
    tie-break, in the window and again on the outer select, so which
    token survives the cap is deterministic rather than whatever the
    planner returned first.

    Symbols are resolved in one ``IN (...)`` over the whole response
    rather than per day (`get_token_symbols`), and an unknown token is
    ``null`` — never its own address, same contract as `top_tokens`.

    A day absent from the rollup is absent here, never zero-filled: a gap
    means "not computed", not "no transfers". Contract invariant 3 does
    not bite — ``transfer_count`` is an additive count, not a distinct
    one, which is the single property this whole breakdown rests on.
    """
    rows = list(
        queryset.annotate(
            rank=Window(
                expression=RowNumber(),
                partition_by=[F("date")],
                order_by=[F("transfer_count").desc(), F("token_address").asc()],
            )
        )
        .filter(rank__lte=cap)
        .values("date", "token_address", "transfer_count", "transfer_value")
        .order_by("-date", "-transfer_count", "token_address")
    )
    symbols = get_token_symbols(row["token_address"] for row in rows)
    return [
        {
            "date": day.isoformat(),
            "tokens": [
                {
                    "address": row["token_address"],
                    "symbol": symbols.get(row["token_address"]),
                    "transfer_count": int(row["transfer_count"] or 0),
                    "total_value": str(int(row["transfer_value"] or 0)),
                }
                for row in group
            ],
        }
        for day, group in groupby(rows, key=lambda row: row["date"])
    ]


def _append_token_day_breakdown(
    payload: dict, since: date, until: date, day_rows: list[dict], cap: int
) -> dict:
    """`_append_day_breakdown` plus the one key that is specific to
    ``/token-volume/``: ``days_token_cap``, appended last.

    It ships on every ``breakdown=day`` response including a cold one, so
    the key set does not depend on whether there were rows — a consumer
    feature-detecting on ``days_token_cap`` gets the same answer either
    way — and so the cap is never inferred from ``len(tokens)``, which
    would read a quiet day as a shallow one.

    Exists as a wrapper rather than two statements at each call site so
    the emission order (``window_start``, ``window_end``, ``days``,
    ``days_token_cap``) is fixed in one place for both return paths.
    """
    _append_day_breakdown(payload, since, until, day_rows)
    payload["days_token_cap"] = cap
    return payload


def get_token_symbols(addresses: Iterable[str]) -> dict[str, str | None]:
    """Read-time join of ERC20 addresses against the upstream ``tokens_token``
    table (``safe_transaction_service.tokens.models.Token``).

    Read-only by design — the analytics app owns none of that table and
    writes nothing to it. ``Token.address`` is an
    ``EthereumAddressBinaryField`` just like the analytics rollups'
    ``token_address``, so the lookup is a bytes-to-bytes primary-key probe:
    no ``decode(...)``, no per-address query, one ``IN (...)`` for the whole
    (at most 20-entry) top-tokens list.

    Every requested address appears in the returned mapping. A token the
    indexer holds no metadata row for — or whose row carries a blank
    symbol — maps to ``None``, never to its own address: the consumer
    decides how an unknown token renders (the hub renders a truncated
    address), and substituting the address here would make "unknown"
    indistinguishable from a token whose symbol genuinely is a hex string.
    """
    from safe_transaction_service.tokens.models import Token

    unique_addresses = list(dict.fromkeys(addresses))
    if not unique_addresses:
        return {}

    symbols: dict[str, str | None] = dict.fromkeys(unique_addresses)
    for address, symbol in Token.objects.filter(
        address__in=unique_addresses
    ).values_list("address", "symbol"):
        if address in symbols:
            symbols[address] = symbol or None
    return symbols


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
    # Vestigial since the native balance became an incremental rollup —
    # it has no shards and a real run writes 0/0 here too. Kept because
    # the hub reads `partial_shards` (USD pricing is gated on it being 0)
    # and removing a payload key is a breaking change that has to land
    # consumer-first. `computed_at: None` is what marks a cold read.
    "partial_shards": 0,
    "total_shards": 0,
    "native_source": None,
    "native_updated_to_block": None,
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

    def get_active_safes(
        self,
        window: str,
        breakdown: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> dict:
        """Read the window-distinct active_safes count.

        Single ``COUNT(DISTINCT safe_address)`` over the per-day
        membership table for the requested window — sub-100 ms
        regardless of ``history_*`` size. On a cold rollup we fall
        through to the Redis-cached rolling-window value populated by
        ``compute_daily_metrics_task``.

        `breakdown` is opt-in and additive (phase-B T9). It is ``None``
        for every caller that does not ask, and then this method returns
        exactly the payload it always has — same keys, same order, on all
        three return paths. With ``breakdown="day"`` three keys are
        appended: ``window_start`` / ``window_end`` / ``days`` (see
        `_append_day_breakdown`). The per-day values are per-day
        ``COUNT(DISTINCT safe_address)`` and are **not additive**
        (contract invariant 3): the window value below is its own
        ``COUNT(DISTINCT …)`` over the same rows and is never derived by
        summing `days`. `window` is validated by the view (7d/30d/90d)
        and not capped further under ``breakdown=day`` (spec Q21).

        Note the window's right edge: this read filters ``date__gte``
        with **no upper bound**, so — unlike ``/tx-volume/`` — it includes
        a partial current UTC day, and ``window_end`` is therefore today
        rather than yesterday.

        `date_from` / `date_to` are the optional strict ISO range the view
        parses out of ``from`` / ``to`` (phase-B T10). Both ``None`` — every
        caller that does not ask — leaves this read exactly as it was.
        Either one set makes the read **ranged**: the window stops applying
        entirely, the supplied bounds filter the rollup, the unsupplied side
        stays unbounded, and ``window`` is reported as ``None`` so that a
        caller can tell a producer that honoured its range from an older one
        that ignored the parameters. Range length is not capped (spec Q21,
        applied to the range for the same reason).

        A ranged read also does **not** fall through to the Redis
        cold-window scalar: that value is a rolling *window* number, so
        answering a range request with it would be a wrong answer rather
        than a stale one. A cold rollup therefore returns the honest zero
        payload with ``computed_at: None``.
        """
        from safe_transaction_service.analytics.models import DailyActiveSafe

        today = timezone.now().date()
        ranged = date_from is not None or date_to is not None
        if ranged:
            since, until = date_from, date_to
        else:
            since, until = today - timedelta(days=_parse_window(window) or 30), None
        windowed = DailyActiveSafe.objects.filter(**_rollup_date_filters(since, until))
        if windowed.exists():
            # Invariant 3: the reported number is a COUNT(DISTINCT ...) over
            # the rows of the span, ranged or not — never a sum of the
            # per-day series below, which is its own separate distinct count
            # over the same queryset.
            count = windowed.values("safe_address").distinct().count()
            payload = {
                "window": None if ranged else window,
                "active_safes": count,
                "computed_at": timezone.now().isoformat(),
            }
            if breakdown == "day":
                _append_day_breakdown(
                    payload,
                    since,
                    until or today,
                    _daily_distinct_counts(windowed, "safe_address", "active_safes"),
                )
            return payload
        logger.info(
            "analytics.rollup.cold_window key=active_safes_%s",
            f"{since}..{until}" if ranged else window,
        )

        if not ranged:
            from safe_transaction_service.analytics.tasks import (
                compute_daily_metrics_task,
            )

            cached = _redis_get_or_compute(
                self.REDIS_ACTIVE_SAFES_PREFIX + window, compute_daily_metrics_task
            )
            if cached and cached.get("window") == window:
                # The Redis fallback carries a window scalar and no per-day
                # rows, so the series is honestly empty here — present and
                # empty, never omitted.
                if breakdown == "day":
                    _append_day_breakdown(cached, since, today, [])
                return cached
        payload = {
            "window": None if ranged else window,
            "active_safes": 0,
            "computed_at": None,
        }
        if breakdown == "day":
            _append_day_breakdown(payload, since, until or today, [])
        return payload

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

    def get_active_owners(
        self,
        window: str,
        breakdown: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> dict:
        """Distinct owners who confirmed any multisig tx executed in the
        window — confirmation-based active-owners semantic.

        Window DAU is a single ``COUNT(DISTINCT owner_address)`` over
        the per-day ``analytics_dailyactiveowner`` rollup — sub-100 ms
        regardless of ``history_*`` size. Replaces the prior
        ``DailyActiveSafe`` → ``SafeLastStatus`` lookup path which on
        BASE took 9–28 s for the 30d window (and 504'd on cold deploys).

        On a cold rollup we fall through to the Redis-cached
        rolling-window value populated by ``compute_daily_metrics_task``.

        `breakdown` is opt-in and additive (phase-B T9), exactly as on
        ``/active-safes/``: ``None`` returns the payload this method has
        always returned, key-for-key and in the same order on all three
        return paths, and ``breakdown="day"`` appends ``window_start`` /
        ``window_end`` / ``days``. The per-day values are per-day
        ``COUNT(DISTINCT owner_address)`` and are **not additive**
        (contract invariant 3) — the window value stays its own
        ``COUNT(DISTINCT …)`` over the same rows. This read also has no
        upper date bound, so ``window_end`` is today.

        `date_from` / `date_to` are the optional strict ISO range the view
        parses out of ``from`` / ``to`` (phase-B T10). Both ``None`` — every
        caller that does not ask — leaves this read exactly as it was.
        Either one set makes the read **ranged**: the window stops applying
        entirely, the supplied bounds filter the rollup, the unsupplied side
        stays unbounded, and ``window`` is reported as ``None`` so that a
        caller can tell a producer that honoured its range from an older one
        that ignored the parameters. Range length is not capped (spec Q21,
        applied to the range for the same reason).

        A ranged read also does **not** fall through to the Redis
        cold-window scalar: that value is a rolling *window* number, so
        answering a range request with it would be a wrong answer rather
        than a stale one. A cold rollup therefore returns the honest zero
        payload with ``computed_at: None``.

        `date_from` / `date_to` behave exactly as on ``/active-safes/``.
        """
        from safe_transaction_service.analytics.models import DailyActiveOwner

        today = timezone.now().date()
        ranged = date_from is not None or date_to is not None
        if ranged:
            since, until = date_from, date_to
        else:
            since, until = today - timedelta(days=_parse_window(window) or 30), None
        windowed = DailyActiveOwner.objects.filter(**_rollup_date_filters(since, until))
        if windowed.exists():
            # Invariant 3, as on /active-safes/: a COUNT(DISTINCT ...) over
            # the span's rows, never a sum of `days`.
            count = windowed.values("owner_address").distinct().count()
            payload = {
                "window": None if ranged else window,
                "active_owners": count,
                "computed_at": timezone.now().isoformat(),
            }
            if breakdown == "day":
                _append_day_breakdown(
                    payload,
                    since,
                    until or today,
                    _daily_distinct_counts(windowed, "owner_address", "active_owners"),
                )
            return payload
        logger.info(
            "analytics.rollup.cold_window key=active_owners_%s",
            f"{since}..{until}" if ranged else window,
        )

        if not ranged:
            from safe_transaction_service.analytics.tasks import (
                compute_daily_metrics_task,
            )

            cached = _redis_get_or_compute(
                self.REDIS_ACTIVE_OWNERS_PREFIX + window, compute_daily_metrics_task
            )
            if cached and cached.get("window") == window:
                # Window scalar only — no per-day rows behind it.
                if breakdown == "day":
                    _append_day_breakdown(cached, since, today, [])
                return cached
        payload = {
            "window": None if ranged else window,
            "active_owners": 0,
            "computed_at": None,
        }
        if breakdown == "day":
            _append_day_breakdown(payload, since, until or today, [])
        return payload

    # ── A.5 TX Volume (DailyMetric sum when populated, live fallback) ─

    def get_tx_volume(self, window: str, breakdown: str | None = None) -> dict:
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

        API attribution — `executed_multisig_txs_via_api` /
        `executed_multisig_txs_indexed_only` split `executed_multisig_txs`
        by whether the tx carries a `proposer`, i.e. whether it was
        created through this service's proposal API or first seen on
        chain by the indexer. They sum to `executed_multisig_txs` on a
        fully covered window.

        `api_attribution_coverage_days` ships with them and callers must
        use it. The two columns are nullable and `Sum` skips NULL, so on a
        partly backfilled window both halves silently understate; a
        caller that divides without first checking this counter against
        `coverage_days` will publish a wrong ratio.

        `breakdown` is opt-in and additive (phase-B T8). It is ``None``
        for every caller that does not ask, and then this method returns
        exactly the payload it always has — same keys, same order. With
        ``breakdown="day"`` three keys are appended: `window_start` /
        `window_end` (the inclusive UTC bounds of the rows actually read,
        i.e. `today - window` .. `yesterday`) and `days`, newest-first,
        one entry per day *present in the rollup*. Missing days are
        absent rather than zero-filled — a gap in the rollup is not a
        day with no activity — and the two nullable attribution columns
        pass straight through as `null` for the same reason they do in
        the scalar payload. A cold or warming rollup therefore yields
        `"days": []`: present and empty, never omitted, so that a
        consumer can tell "producer predates the parameter" (key absent)
        from "producer has no rows yet" (key empty). `window` is
        deliberately not capped here (spec Q21), so the response grows
        linearly with it.
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
            via_api=Coalesce(Sum("multisig_txs_via_api"), Value(0)),
            indexed_only=Coalesce(Sum("multisig_txs_indexed_only"), Value(0)),
            # Count() skips NULL, so this is "days that actually carry
            # the split", not "days in the window" — that's the whole
            # point of surfacing it next to coverage_days.
            split_coverage=Count("multisig_txs_via_api"),
            coverage=Count("date"),
        )
        conf_total = int(agg["conf_total"] or 0)
        conf_txs = int(agg["conf_txs"] or 0)
        avg_conf = round(conf_total / conf_txs, 1) if conf_txs else 0.0

        payload = {
            "window": window,
            "total_multisig_txs": int(agg["proposed"] or 0),
            "executed_multisig_txs": int(agg["executed"] or 0),
            "executed_multisig_txs_via_api": int(agg["via_api"] or 0),
            "executed_multisig_txs_indexed_only": int(agg["indexed_only"] or 0),
            "api_attribution_coverage_days": int(agg["split_coverage"] or 0),
            "module_txs": int(agg["module"] or 0),
            "total_value_wei": str(int(agg["native"] or 0)),
            "avg_confirmations": avg_conf,
            "avg_confirmations_approximation": "per-tx-day",
            "coverage_days": int(agg["coverage"] or 0),
            "computed_at": timezone.now(),
            "source": "daily_metric",
        }
        if breakdown == "day":
            payload["window_start"] = date_from.isoformat()
            payload["window_end"] = (today - timedelta(days=1)).isoformat()
            payload["days"] = [
                {
                    "date": row["date"].isoformat(),
                    "multisig_txs_executed": row["multisig_txs_executed"],
                    "multisig_txs_via_api": row["multisig_txs_via_api"],
                    "multisig_txs_indexed_only": row["multisig_txs_indexed_only"],
                    "erc20_transfers": row["erc20_transfers"],
                }
                for row in rows.order_by("-date").values(
                    "date",
                    "multisig_txs_executed",
                    "multisig_txs_via_api",
                    "multisig_txs_indexed_only",
                    "erc20_transfers",
                )
            ]
        return payload

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

    def get_token_volume(self, window: str, breakdown: str | None = None) -> dict:
        """Top ERC20 tokens over a window, rollup-first with a live
        cold-window fallback.

        `breakdown` is opt-in and additive, the same contract T8 set on
        `/tx-volume/` and T9 on the two active-* reads: it is ``None``
        for every caller that does not ask, and then this returns exactly
        the payload it always has — same keys, same order. With
        ``breakdown="day"`` four keys are appended, in this order:
        `window_start` / `window_end`, `days`, and `days_token_cap`.

        **`window_end` is today, not yesterday.** This read is
        ``date__gte=since`` with no upper bound
        (`_token_volume_rollup_queryset`), so the window includes a
        partial current UTC day and the bounds say so. `/tx-volume/`'s
        `today-N … yesterday` would be a lie about which days the series
        can contain — the same divergence Part 6 records for
        `/active-safes/`.

        `days` carries each day's **own** top-`TOP_TOKENS_LIMIT` tokens,
        not a per-day slice of the window's ranking, and
        `days_token_cap` ships that depth so a consumer summing the
        series knows where it stops. Summing is exact for a token inside
        its day's top-N and understates a token that is always just
        outside it; it can never overstate. See `_daily_top_tokens`.

        This is the reason the breakdown is possible at all:
        `transfer_count` is an additive count, so days may be summed —
        unlike the DAU series of T9, where contract invariant 3 forbids
        exactly that.

        A cold rollup returns ``"days": []`` — present and empty, never
        omitted — which is what keeps "producer predates the parameter"
        (key absent) apart from "producer has no rows yet" (key empty).
        The scalar half of that response still comes from the live
        `ERC20Transfer` aggregation, whose lower bound is ``now - N
        days`` rather than midnight; `window_start` describes the
        requested window, and with `days` empty there is no series for it
        to disagree with.

        `window` is unvalidated here (`_parse_window` falls back to 30)
        and is not capped under ``breakdown=day`` (spec Q21), so the
        response grows linearly with it — bounded per day by
        `days_token_cap`, not by the window.
        """
        days = _parse_window(window)
        if days is None:
            days = 30

        # One `since` for the whole request: the aggregate, the series and
        # the bounds must describe the same span even across UTC midnight.
        today = timezone.now().date()
        since = today - timedelta(days=days)

        payload = self._token_volume_from_rollup(window, since)
        if payload is not None:
            if breakdown == "day":
                _append_token_day_breakdown(
                    payload,
                    since,
                    today,
                    _daily_top_tokens(
                        _token_volume_rollup_queryset(since), TOP_TOKENS_LIMIT
                    ),
                    TOP_TOKENS_LIMIT,
                )
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
            .order_by("-transfer_count")[:TOP_TOKENS_LIMIT]
        )

        symbols = get_token_symbols(t["address"] for t in top_tokens)

        payload = {
            "window": window,
            "total_erc20_transfers": total_transfers,
            "unique_tokens": unique_tokens,
            "top_tokens": [
                {
                    "address": t["address"],
                    "symbol": symbols.get(t["address"]),
                    "transfer_count": t["transfer_count"],
                    "total_value": str(t["total_value"] or 0),
                }
                for t in top_tokens
            ],
            "computed_at": timezone.now(),
        }
        if breakdown == "day":
            # Cold rollup, not a quiet chain: the series is empty because
            # there are no rollup rows to build it from, and the live
            # aggregation above has no per-day grain to borrow.
            _append_token_day_breakdown(payload, since, today, [], TOP_TOKENS_LIMIT)
        return payload

    def _token_volume_from_rollup(self, window: str, since: date) -> dict | None:
        """Build the same payload from ``analytics_daily_token_volume``.

        Takes `since` from the caller rather than deriving it, so the
        scalar aggregate, the ``breakdown=day`` series and the reported
        window bounds are all the same span.

        Returns None when the rollup is cold for the requested window so
        the caller can fall through to the live aggregation.
        """
        rows = list(
            _token_volume_rollup_queryset(since)
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
        top = rows[:TOP_TOKENS_LIMIT]
        symbols = get_token_symbols(t["token_address"] for t in top)
        return {
            "window": window,
            "total_erc20_transfers": total_transfers,
            "unique_tokens": unique_tokens,
            "top_tokens": [
                {
                    "address": t["token_address"],
                    "symbol": symbols.get(t["token_address"]),
                    "transfer_count": int(t["transfer_count"] or 0),
                    "total_value": str(int(t["total_value"] or 0)),
                }
                for t in top
            ],
            "computed_at": timezone.now(),
        }
