import contextlib
import json
import logging
import time
from datetime import date

from django.db import connection
from django.db.models import Count, F, Max, Min, Q
from django.db.models.functions import Trunc
from django.utils import timezone

from celery import app
from dateutil.relativedelta import relativedelta
from redis.exceptions import LockError

# Force-import `tasks_shards` so its `@app.shared_task` decorators register
# the sharded tasks (`compute_daily_metric_shard`, `backfill_done`,
# `compute_native_balance_shard`, `reduce_native_balance_shards`) with the
# Celery app. Celery's `autodiscover_tasks()` only scans `<app>.tasks` by
# default — without this import, workers consume the `contracts` queue but
# see chord-dispatched shards as `unregistered task` and discard them.
# Side-effect import; intentional. Keep ordering after `LOCK_TIMEOUT` so
# everything `tasks_shards` depends on is in scope.
from safe_transaction_service.analytics import tasks_shards  # noqa: F401
from safe_transaction_service.analytics.models import (
    AnalyticsSnapshot,
    DailyActiveOwner,
    DailyActiveSafe,
    DailyMetric,
    DailySafeCreation,
)
from safe_transaction_service.analytics.services.analytics_service import (
    AnalyticsService,
)
from safe_transaction_service.analytics.services.db import (
    approx_count_or_exact,
    relaxed_statement_timeout,
)
from safe_transaction_service.history.models import (
    ERC20Transfer,
    ERC721Transfer,
    ModuleTransaction,
    MultisigConfirmation,
    MultisigTransaction,
    SafeContract,
)
from safe_transaction_service.utils.celery import task_timeout
from safe_transaction_service.utils.redis import get_redis
from safe_transaction_service.utils.tasks import LOCK_TIMEOUT, only_one_running_task

logger = logging.getLogger(__name__)


def _write_snapshot(name: str, payload: dict) -> None:
    """Upsert one row in `analytics_analyticssnapshot`.

    Replaces the Redis keys used by current-state metrics
    (`summary`, `safe_segments`, `tvl`) — see
    `flickering-honking-wand.md` Part 2. Postgres replaces Redis as the
    durability layer so a Redis flush / pod restart doesn't reset the
    cached payload, and the view's old dispatch-and-poll path is gone
    (a cold read returns the empty payload while fire-and-forget-
    dispatching the refresh, never blocking the request).
    """
    AnalyticsSnapshot.objects.update_or_create(
        name=name,
        defaults={"payload": payload, "computed_at": timezone.now()},
    )


def _iter_safe_addresses_keyset(batch_size: int = 5000):
    """Yield batches of ``SafeContract.address`` using keyset pagination.

    ``SafeContract.address`` is the bytea PK (``EthereumAddressBinaryField``),
    so ``address__gt=last`` produces an indexed range scan and each fetch
    stays O(log N + batch_size). The old ``queryset[offset:offset+batch_size]``
    form compiled to ``OFFSET/LIMIT``, which on a multi-million-Safe chain
    forced PG to scan and discard every preceding row on every batch —
    quadratic over the loop, multi-minute per call once the offset crossed
    ~500k.
    """
    base_qs = SafeContract.objects.values_list("address", flat=True).order_by("pk")
    last: str | None = None
    while True:
        qs = base_qs.filter(address__gt=last) if last is not None else base_qs
        chunk = list(qs[:batch_size])
        if not chunk:
            return
        yield chunk
        last = chunk[-1]


BALANCE_BATCH_SQL = """
    SELECT
        COALESCE(SUM(CASE WHEN balance > 0 THEN balance ELSE 0 END), 0),
        COUNT(*) FILTER (WHERE balance > 0)
    FROM (
        SELECT
            addr,
            SUM(CASE WHEN direction = 1 THEN value ELSE -value END) AS balance
        FROM (
            SELECT it."to" AS addr, it.value, 1 AS direction
            FROM history_internaltx it
            WHERE it."to" = ANY(%s)
              AND it.call_type = 0 AND it.value > 0 AND it.error IS NULL
            UNION ALL
            SELECT it."_from" AS addr, it.value, -1 AS direction
            FROM history_internaltx it
            WHERE it."_from" = ANY(%s)
              AND it.call_type = 0 AND it.value > 0 AND it.error IS NULL
        ) transfers
        GROUP BY addr
    ) safe_balances
"""


def _calculate_native_balances_from_db_sequential() -> tuple[int, int]:
    """
    Sequential reference implementation kept as a fallback / for tests.
    Calculate native token balances using DB aggregation on InternalTx,
    processed in batches to stay within the 50-second statement timeout.

    The default entry point ``_calculate_native_balances_from_db`` now
    fans this work out across 16 hex-prefix shards via Celery (see
    ``tasks_shards.dispatch_native_balance_shards``). On a fresh deploy
    or when called from a worker that *is* the consumer of its own
    shards (single-worker chains), the chord deadlocks — so callers can
    opt into this sequential path by passing ``parallel=False``.
    """
    start_time = time.time()
    batch_size = 5000
    total_balance_wei = 0
    total_safes_with_balance = 0
    processed = 0

    total_safes = SafeContract.objects.count()
    logger.info(
        "native_balance.sequential: starting total_safes=%d batch_size=%d",
        total_safes,
        batch_size,
    )

    batch_number = 0

    # Open the relaxed timeout once for the whole batch loop. Each batch
    # already takes seconds on staging (batch 14 at ~8 s) and the default
    # 50 s request-path timeout was silently dropping batches on planner
    # flips at scale. Per-batch try/except still isolates individual
    # failures from the rest of the run.
    with relaxed_statement_timeout():
        for addresses in _iter_safe_addresses_keyset(batch_size):
            batch_number += 1
            processed += len(addresses)
            batch_start = time.time()

            # Convert checksummed address strings to bytes for bytea comparison
            address_bytes = [bytes.fromhex(addr[2:]) for addr in addresses]

            try:
                with connection.cursor() as cursor:
                    cursor.execute(BALANCE_BATCH_SQL, [address_bytes, address_bytes])
                    row = cursor.fetchone()

                batch_balance = int(row[0]) if row[0] else 0
                batch_count = int(row[1]) if row[1] else 0
                total_balance_wei += batch_balance
                total_safes_with_balance += batch_count

                logger.info(
                    "native_balance.sequential: batch %d done %d/%d in %.2fs "
                    "running_total_wei=%d running_safes_with_balance=%d",
                    batch_number,
                    processed,
                    total_safes,
                    time.time() - batch_start,
                    total_balance_wei,
                    total_safes_with_balance,
                )
            except Exception:
                logger.exception(
                    "native_balance.sequential: batch %d failed after %.2fs "
                    "addresses[0]=%s addresses[-1]=%s",
                    batch_number,
                    time.time() - batch_start,
                    addresses[0] if addresses else "N/A",
                    addresses[-1] if addresses else "N/A",
                )
                # Continue with next batch — partial results still useful

    elapsed = time.time() - start_time
    logger.info(
        "native_balance.sequential: completed in %.2fs total_wei=%d "
        "safes_with_balance=%d/%d",
        elapsed,
        total_balance_wei,
        total_safes_with_balance,
        processed,
    )
    return total_balance_wei, total_safes_with_balance


def _calculate_native_balances_from_db(parallel: bool = False) -> tuple[int, int]:
    """In-process native-balance aggregation.

    The chord path that used to live here has moved into
    ``compute_tvl_task`` (which now dispatches the 16-shard chord directly
    via ``dispatch_tvl_chord``) — the blocking ``.get()`` it required was
    the cause of the silent TVL hangs on chains with gevent workers + a
    Redis result backend. The ``parallel`` kwarg is kept for source
    compatibility but ignored; this entry point now always runs the
    sequential implementation, which is what tests and ad-hoc invocations
    actually want.
    """
    return _calculate_native_balances_from_db_sequential()


@app.shared_task(bind=True)
@task_timeout(timeout_seconds=LOCK_TIMEOUT * 2)
def get_transactions_per_safe_app_task(self):
    """Aggregate multisig txs grouped by origin name + URL and cache to Redis.

    Dual-write: keeps the legacy Redis payload fresh as a cold-rollup
    fallback for the read path, *and* populates ``analytics_dailysafeapptx``
    for every distinct day that holds origin-bearing multisig activity so
    the rollup table is hydrated without a separate backfill pass.

    Guarded by ``only_one_running_task(self)`` so a manual ``.delay()`` while
    the Sunday cron is still running does not race the same UPSERT.
    """
    with contextlib.suppress(LockError):
        with only_one_running_task(self):
            started = time.time()
            logger.info("get_transactions_per_safe_app_task: starting")
            today = timezone.now()
            last_week = today - relativedelta(days=7)
            last_month = today - relativedelta(months=1)
            last_year = today - relativedelta(years=1)

            queryset = (
                MultisigTransaction.objects.filter(origin__name__isnull=False)
                .values(name=F("origin__name"), url=F("origin__url"))
                .annotate(
                    total_tx=Count("origin__name"),
                    tx_last_week=Count("origin__name", filter=Q(created__gt=last_week)),
                    tx_last_month=Count(
                        "origin__name", filter=Q(created__gt=last_month)
                    ),
                    tx_last_year=Count("origin__name", filter=Q(created__gt=last_year)),
                )
                .order_by("-total_tx")
            )

            wrote_redis = False
            redis_rows = 0
            if queryset:
                redis_key = AnalyticsService.REDIS_TRANSACTIONS_PER_SAFE_APP
                redis_payload = list(queryset)
                redis_rows = len(redis_payload)
                redis = get_redis()
                redis.set(redis_key, json.dumps(redis_payload))
                wrote_redis = True

            # Dual-write to the rollup. One SQL pass groups every executed
            # multisig tx by (block.date, origin.name); ON CONFLICT keeps it
            # idempotent.
            rollup_rows = 0
            try:
                with relaxed_statement_timeout(), connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO analytics_dailysafeapptx
                            (date, origin_name, origin_url, tx_count)
                        SELECT date, origin_name, origin_url, tx_count FROM (
                            SELECT
                                (eb.timestamp AT TIME ZONE 'UTC')::date AS date,
                                COALESCE(mt.origin->>'name', '') AS origin_name,
                                COALESCE(MAX(mt.origin->>'url'), '') AS origin_url,
                                COUNT(*) AS tx_count
                            FROM history_multisigtransaction mt
                            JOIN history_ethereumtx etx
                                ON mt.ethereum_tx_id = etx.tx_hash
                            JOIN history_ethereumblock eb
                                ON etx.block_id = eb.number
                            WHERE mt.origin->>'name' IS NOT NULL
                              AND mt.origin->>'name' <> ''
                            GROUP BY date, origin_name
                        ) src
                        ON CONFLICT (date, origin_name) DO UPDATE SET
                            origin_url = EXCLUDED.origin_url,
                            tx_count   = EXCLUDED.tx_count
                        """
                    )
                    rollup_rows = cursor.rowcount
            except Exception:
                logger.exception(
                    "get_transactions_per_safe_app_task: rollup dual-write failed"
                )

            logger.info(
                "get_transactions_per_safe_app_task: completed in %.2fs "
                "redis_rows=%d rollup_rows=%d wrote_redis=%s",
                time.time() - started,
                redis_rows,
                rollup_rows,
                wrote_redis,
            )
            return wrote_redis


@app.shared_task(bind=True)
@task_timeout(timeout_seconds=LOCK_TIMEOUT * 2)
def compute_summary_task(self):
    """Write the ``summary`` snapshot consumed by ``GET /v2/analytics/summary/``.

    Cheap fleet-level counts only: ``total_safes`` via ``SafeContract.count()``
    plus four ``approx_count_or_exact`` reads off ``history_*`` (constant-time
    on big tables; falls back to exact ``COUNT(*)`` for fixtures), and
    first/last creation timestamps via ``Min/Max`` on the indexed
    ``SafeContract.created`` column. No joins, no scans through
    ``MultisigConfirmation``, no native-balance work — that path lives in
    ``compute_tvl_task``.

    Replaces the previous ``get_safe_statistics_task``, which produced both
    the ``safe_statistics`` and ``summary`` snapshots. ``/safe-statistics/``
    is gone (see plan ``robust-wandering-spark.md``); the owner-iteration
    and native-balance phases that endpoint required were removed with it.
    """
    with contextlib.suppress(LockError):
        with only_one_running_task(self):
            started = time.time()
            logger.info("compute_summary_task: starting")
            try:
                total_safes = SafeContract.objects.count()

                with relaxed_statement_timeout():
                    dates = SafeContract.objects.aggregate(
                        first=Min("created"),
                        last=Max("created"),
                    )
                    summary = {
                        "total_safes": total_safes,
                        "total_multisig_txs": approx_count_or_exact(
                            MultisigTransaction, "history_multisigtransaction"
                        ),
                        "total_module_txs": approx_count_or_exact(
                            ModuleTransaction, "history_moduletransaction"
                        ),
                        "total_erc20_transfers": approx_count_or_exact(
                            ERC20Transfer, "history_erc20transfer"
                        ),
                        "total_erc721_transfers": approx_count_or_exact(
                            ERC721Transfer, "history_erc721transfer"
                        ),
                        "first_safe_created": (
                            dates["first"].isoformat() if dates["first"] else None
                        ),
                        "last_safe_created": (
                            dates["last"].isoformat() if dates["last"] else None
                        ),
                        "computed_at": timezone.now().isoformat(),
                    }
                    _write_snapshot("summary", summary)

                logger.info(
                    "compute_summary_task: completed in %.2fs total_safes=%d",
                    time.time() - started,
                    total_safes,
                )
                return True
            except Exception:
                logger.exception(
                    "compute_summary_task: failed after %.2fs",
                    time.time() - started,
                )
                return False


# Per-anchor EXISTS probe: for each Safe address in the batch, ask the
# planner to stop on the first matching transfer row using the
# `(_from, timestamp)` / `(to, timestamp)` covering indexes. The old
# DISTINCT-over-UNION form had to read every transfer row in the window
# for any active Safe; this form is O(addresses_in_batch * 2 index probes)
# regardless of how many transfers each Safe has.
_ERC20_ACTIVE_BATCH_SQL = """
    SELECT a.addr
    FROM unnest(%s::bytea[]) AS a(addr)
    WHERE EXISTS (
        SELECT 1 FROM history_erc20transfer
        WHERE "_from" = a.addr AND timestamp >= %s
        LIMIT 1
    ) OR EXISTS (
        SELECT 1 FROM history_erc20transfer
        WHERE "to" = a.addr AND timestamp >= %s
        LIMIT 1
    )
"""

# Closed-interval variant for per-day DAU computations (C7).
#
# Scans only the day's transfers (bounded by the `timestamp` index range)
# and joins back to `history_safecontract` — one query, two index range
# scans, zero per-Safe probes. The old form looped over all Safes in
# batches of 5000 and ran a paired EXISTS per Safe, which on a 1.5M-Safe
# chain meant ~2M btree probes and ~200 round-trips for what is now a
# single statement.
_ERC20_ACTIVE_BETWEEN_JOIN_SQL = """
    SELECT sc.address
    FROM history_safecontract sc
    JOIN (
        SELECT "_from" AS address FROM history_erc20transfer
        WHERE timestamp >= %s AND timestamp < %s
        UNION
        SELECT "to" AS address FROM history_erc20transfer
        WHERE timestamp >= %s AND timestamp < %s
    ) t ON sc.address = t.address
"""


def _normalize_addr(value) -> str:
    """Coerce a value to a lowercase `0x...` hex string regardless of whether
    it came back as a checksummed string (Django ORM) or raw bytea
    (`cursor.execute`)."""
    if isinstance(value, memoryview):
        value = bytes(value)
    if isinstance(value, (bytes, bytearray)):
        return "0x" + value.hex()
    return value.lower() if isinstance(value, str) else value


def _erc20_active_safe_addrs(cutoff, batch_size: int = 5000) -> set[str]:
    """Return the set of Safe addresses that appear as `_from` or `to` of
    an ERC20 transfer at or after `cutoff`.

    Reverses the direction of the old
    `ERC20Transfer.filter(_from__in=SafeContract)` query, which forced the
    planner to scan every transfer in the window and probe SafeContract per
    row. Here we anchor on batches of 5 000 SafeContract addresses and let
    the `(_from, timestamp)` / `(to, timestamp)` covering indexes do the
    lookup — the same batched-probe pattern that
    `_calculate_native_balances_from_db` uses to stay under the per-statement
    budget on multi-hundred-thousand-Safe chains.
    """
    seen: set[str] = set()
    for addresses in _iter_safe_addresses_keyset(batch_size):
        addr_bytes = [bytes.fromhex(a[2:]) for a in addresses]
        with connection.cursor() as cursor:
            cursor.execute(_ERC20_ACTIVE_BATCH_SQL, [addr_bytes, cutoff, cutoff])
            for row in cursor:
                seen.add(_normalize_addr(row[0]))
    return seen


def _safes_active_in_window(cutoff) -> int:
    """Distinct count of Safes that produced multisig/module activity or
    ERC20 movement at or after `cutoff`."""
    active: set[str] = set()

    # Multisig / module legs filter on block.timestamp / internal_tx.timestamp.
    # Both are bounded by the window, not by the transfers table size, so
    # they finish in tens of ms even on chains with ~1M txs.
    active.update(
        _normalize_addr(a)
        for a in MultisigTransaction.objects.filter(
            ethereum_tx__block__timestamp__gte=cutoff
        )
        .values_list("safe", flat=True)
        .distinct()
    )
    active.update(
        _normalize_addr(a)
        for a in ModuleTransaction.objects.filter(internal_tx__timestamp__gte=cutoff)
        .values_list("safe", flat=True)
        .distinct()
    )
    # ERC20 leg: batched index probe (see `_erc20_active_safe_addrs`).
    active.update(_erc20_active_safe_addrs(cutoff))
    return len(active)


@app.shared_task(bind=True)
@task_timeout(timeout_seconds=LOCK_TIMEOUT * 4)
def compute_active_safes_task(self):
    """Compute active Safes for 7d / 30d / 90d windows and cache in Redis.

    Each window is computed independently inside its own try/except so a
    failure on the heaviest window (90 d on a chain with tens of millions
    of transfers) doesn't strand the 7 d / 30 d results.
    """
    with contextlib.suppress(LockError):
        with only_one_running_task(self):
            started = time.time()
            logger.info("compute_active_safes_task: starting windows=7d,30d,90d")
            redis = get_redis()
            now = timezone.now()

            ok_windows: list[str] = []
            counts: dict[str, int] = {}
            for window_str, days in [("7d", 7), ("30d", 30), ("90d", 90)]:
                window_started = time.time()
                cutoff = now - timezone.timedelta(days=days)
                try:
                    with relaxed_statement_timeout():
                        count = _safes_active_in_window(cutoff)
                except Exception:
                    logger.exception(
                        "compute_active_safes_task: window %s failed after %.2fs",
                        window_str,
                        time.time() - window_started,
                    )
                    continue
                result = {
                    "window": window_str,
                    "active_safes": count,
                    "computed_at": now.isoformat(),
                }
                redis.set(
                    AnalyticsService.REDIS_ACTIVE_SAFES_PREFIX + window_str,
                    json.dumps(result),
                )
                ok_windows.append(window_str)
                counts[window_str] = count
                logger.info(
                    "compute_active_safes_task: window %s took %.2fs active_safes=%d",
                    window_str,
                    time.time() - window_started,
                    count,
                )

            logger.info(
                "compute_active_safes_task: completed in %.2fs ok_windows=%s counts=%s",
                time.time() - started,
                ok_windows,
                counts,
            )
            return bool(ok_windows)


_ACTIVE_OWNERS_FALLBACK_SQL = """
    SELECT COUNT(DISTINCT mc.owner)
    FROM history_ethereumtx et
    JOIN history_multisigtransaction mt
        ON mt.ethereum_tx_id = et.tx_hash
    JOIN history_multisigconfirmation mc
        ON mc.multisig_transaction_id = mt.safe_tx_hash
    WHERE et.block_id >= (
        SELECT MIN(number)
        FROM history_ethereumblock
        WHERE timestamp >= %s
    )
"""
# The 4-table join shape (eb ⋈ et ⋈ mt ⋈ mc) is collapsed by exploiting
# the fact that ``EthereumBlock.number`` is strictly monotonic on a chain:
# resolving the cutoff to a single ``MIN(number)`` via the
# ``history_ethereumblock(timestamp)`` btree index (one row, sub-ms) lets
# us range-scan ``history_ethereumtx`` directly on its FK-indexed
# ``block_id`` column. PG can then nested-loop et → mt → mc through their
# FK indexes, with ``DISTINCT owner`` folded into a hash aggregate. We
# trade the upper-bound predicate on ``eb.timestamp`` for a chain-block
# count, which is fine: timestamps are roughly monotonic with number and
# the worst-case inversion on EVM L2s is ~tens of seconds — irrelevant
# for 7/30/90 d aggregates.


def _active_owners_in_window(cutoff, batch_size: int = 5000) -> int:
    """Distinct count of owners whose signed multisig tx executed on-chain
    in `[cutoff, now]`.

    Fast path: aggregate over the per-day ``DailyActiveOwner`` rollup
    populated by ``compute_daily_metrics_task`` — a single
    ``COUNT(DISTINCT owner_address)`` over the date range, sub-100 ms
    regardless of ``history_*`` size. Matches the semantic of the rollup
    populator (owners with at least one confirmation on a tx executed
    that UTC day).

    Cold-window fallback: if the rollup has no rows covering the window
    (fresh instance / pre-backfill / a long gap since the last daily
    run), drop the whole aggregation to PG. See
    ``_ACTIVE_OWNERS_FALLBACK_SQL``: a single statement, 3-table
    inner join (et ⋈ mt ⋈ mc) gated by a scalar
    ``MIN(history_ethereumblock.number)`` subquery — collapses the
    fourth (block) table to a one-row index probe by exploiting block
    monotonicity, then range-scans ``history_ethereumtx`` on its
    FK-indexed ``block_id`` and nested-loops outward through the
    confirmation FK index. The previous Python form materialised every
    executed ``safe_tx_hash`` in window into a list (later: a streaming
    iterator) and probed ``MultisigConfirmation`` in 5 k-id batches,
    deduping owners in Python — on BASE (1.5 M Safes, 90 d window) that
    pinned a gevent worker past its task timeout and poisoned the
    connection. ``batch_size`` is retained on the signature for callers
    / tests that still pass it, but is unused on the SQL path. Emits
    ``analytics.rollup.cold_window`` when the fallback fires.
    """
    cutoff_date = cutoff.date() if hasattr(cutoff, "date") else cutoff
    rollup_qs = DailyActiveOwner.objects.filter(date__gte=cutoff_date)
    if rollup_qs.exists():
        return rollup_qs.values("owner_address").distinct().count()

    logger.info(
        "analytics.rollup.cold_window key=active_owners cutoff=%s",
        cutoff_date,
    )
    with connection.cursor() as cursor:
        cursor.execute(_ACTIVE_OWNERS_FALLBACK_SQL, [cutoff])
        row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


@app.shared_task(bind=True)
@task_timeout(timeout_seconds=LOCK_TIMEOUT * 4)
def compute_active_owners_task(self):
    """Compute active owners for 7d / 30d / 90d windows and cache in Redis.

    Per-window try/except so the smaller windows still land if the largest
    one overruns. `_active_owners_in_window` runs the heavy join in batched
    form under a relaxed statement timeout.
    """
    with contextlib.suppress(LockError):
        with only_one_running_task(self):
            started = time.time()
            logger.info("compute_active_owners_task: starting windows=7d,30d,90d")
            redis = get_redis()
            now = timezone.now()

            ok_windows: list[str] = []
            counts: dict[str, int] = {}
            for window_str, days in [("7d", 7), ("30d", 30), ("90d", 90)]:
                window_started = time.time()
                cutoff = now - timezone.timedelta(days=days)
                try:
                    with relaxed_statement_timeout():
                        count = _active_owners_in_window(cutoff)
                except Exception:
                    logger.exception(
                        "compute_active_owners_task: window %s failed after %.2fs",
                        window_str,
                        time.time() - window_started,
                    )
                    continue
                result = {
                    "window": window_str,
                    "active_owners": count,
                    "computed_at": now.isoformat(),
                }
                redis.set(
                    AnalyticsService.REDIS_ACTIVE_OWNERS_PREFIX + window_str,
                    json.dumps(result),
                )
                ok_windows.append(window_str)
                counts[window_str] = count
                logger.info(
                    "compute_active_owners_task: window %s took %.2fs active_owners=%d",
                    window_str,
                    time.time() - window_started,
                    count,
                )

            logger.info(
                "compute_active_owners_task: completed in %.2fs ok_windows=%s counts=%s",
                time.time() - started,
                ok_windows,
                counts,
            )
            return bool(ok_windows)


_SAFE_SEGMENTS_SQL = """
    WITH latest AS (
        SELECT DISTINCT ON (address)
            address, threshold, owners, enabled_modules
        FROM history_safestatus
        ORDER BY address, nonce DESC, internal_tx_id DESC
    )
    SELECT
        COUNT(*) FILTER (WHERE COALESCE(array_length(owners, 1), 0) <= 1) AS personal,
        COUNT(*) FILTER (WHERE array_length(owners, 1) BETWEEN 2 AND 5)   AS team,
        COUNT(*) FILTER (WHERE COALESCE(array_length(owners, 1), 0) > 5)  AS enterprise,
        COUNT(*) FILTER (WHERE enabled_modules IS NOT NULL
                              AND array_length(enabled_modules, 1) > 0)   AS with_modules,
        COUNT(*)                                                          AS total,
        AVG(threshold)::float                                             AS avg_threshold,
        AVG(COALESCE(array_length(owners, 1), 0))::float                  AS avg_owners
    FROM latest
"""


@app.shared_task(bind=True)
@task_timeout(timeout_seconds=LOCK_TIMEOUT * 4)
def compute_safe_segments_task(self):
    """Compute Safe segments from latest SafeStatus per address, cache in Redis.

    The previous Python iterator over `SafeStatus.last_for_every_address()`
    took ~10 min on a 250 k-Safe fleet because the DISTINCT-ON QuerySet
    streamed every latest-status row through the ORM. The aggregate is
    pushed entirely into Postgres so a 632 s task collapses to seconds —
    the `(address, -nonce)` index on `history_safestatus` carries the
    DISTINCT-ON.
    """
    with contextlib.suppress(LockError):
        with only_one_running_task(self):
            started = time.time()
            logger.info("compute_safe_segments_task: starting")
            now = timezone.now()

            with relaxed_statement_timeout():
                with connection.cursor() as cursor:
                    cursor.execute(_SAFE_SEGMENTS_SQL)
                    (
                        personal,
                        team,
                        enterprise,
                        with_modules,
                        count,
                        avg_threshold,
                        avg_owners,
                    ) = cursor.fetchone()

            result = {
                "personal": int(personal or 0),
                "team": int(team or 0),
                "enterprise": int(enterprise or 0),
                "with_modules": int(with_modules or 0),
                "avg_threshold": round(float(avg_threshold or 0.0), 1),
                "avg_owners": round(float(avg_owners or 0.0), 1),
                "computed_at": now.isoformat(),
            }
            _write_snapshot("safe_segments", result)
            logger.info(
                "compute_safe_segments_task: completed in %.2fs total=%d "
                "personal=%d team=%d enterprise=%d with_modules=%d",
                time.time() - started,
                int(count or 0),
                int(personal or 0),
                int(team or 0),
                int(enterprise or 0),
                int(with_modules or 0),
            )
            return True


@app.shared_task(bind=True)
@task_timeout(timeout_seconds=LOCK_TIMEOUT)
def compute_tvl_task(self):
    """Fire-and-forget driver for the TVL pipeline.

    Writes a phase-1 placeholder snapshot only if none exists yet, then
    dispatches the chord ``(16 native shards) → reduce → finalize_tvl_snapshot``
    on the ``contracts`` queue and returns immediately. The final
    snapshot is written by ``finalize_tvl_snapshot`` when the chord
    resolves — see ``analytics.tasks_shards.finalize_tvl_snapshot``.

    The previous shape blocked on ``.get()`` waiting for the chord, which
    hung indefinitely on gevent workers + Redis result backend. The new
    shape is event-driven: success of the heavy aggregation is observable
    via the snapshot's ``computed_at`` advancing past the placeholder.
    """
    with contextlib.suppress(LockError):
        with only_one_running_task(self):
            from safe_transaction_service.analytics.tasks_shards import (
                dispatch_tvl_chord,
            )

            started = time.time()
            logger.info("compute_tvl_task: starting")

            # Phase 1 — only seed the placeholder when nothing is there
            # yet. Overwriting a previously-good snapshot would zero out
            # the endpoint for the entire chord duration on every run.
            if not AnalyticsSnapshot.objects.filter(name="tvl").exists():
                placeholder = {
                    "total_safes_with_balance": 0,
                    "native_balance_wei": "0",
                    "erc20_token_count": 0,
                    "top_tokens": [],
                    # 0/0 marks this as a "never-computed" placeholder so
                    # consumers can distinguish it from a real partial run
                    # written by `finalize_tvl_snapshot`.
                    "partial_shards": 0,
                    "total_shards": 0,
                    "computed_at": timezone.now().isoformat(),
                }
                _write_snapshot("tvl", placeholder)
                logger.info("compute_tvl_task: phase1 placeholder snapshot written")

            # Phase 2 — fire-and-forget. The chord callback
            # (`finalize_tvl_snapshot`) does the ERC20 aggregation and
            # writes the real snapshot when shards + reduce resolve.
            dispatch_tvl_chord()
            logger.info(
                "compute_tvl_task: chord dispatched in %.2fs",
                time.time() - started,
            )
            return True


@app.shared_task(bind=True)
@task_timeout(timeout_seconds=LOCK_TIMEOUT * 2)
def compute_safe_creations_task(self):
    """
    Compute Safe creations day-grain series, cache in Redis.

    Post-rollups: reads the series directly from
    ``analytics_dailysafecreation`` when the table is populated (constant-
    time scan of a ~1k-row table on the oldest chains). Falls back to the
    live ``SafeContract → EthereumTx → EthereumBlock`` join when the
    rollup is cold (fresh deploy / mid-backfill) and on success backfills
    the rollup so subsequent runs are instant.

    Only the day-grain series is cached. Week and month buckets are
    derived from it in-memory at request time.

    Guarded by ``only_one_running_task(self)`` so a manual ``.delay()`` while
    the daily cron is still running does not race the rollup upsert loop.
    """
    with contextlib.suppress(LockError):
        with only_one_running_task(self):
            started = time.time()
            logger.info("compute_safe_creations_task: starting")
            rollup_rows = list(DailySafeCreation.objects.order_by("date").all())
            if rollup_rows:
                series = [
                    {"period": r.date.isoformat(), "count": r.count}
                    for r in rollup_rows
                ]
                payload = {
                    "series": series,
                    "computed_at": timezone.now().isoformat(),
                }
                get_redis().set(
                    AnalyticsService.REDIS_SAFE_CREATIONS, json.dumps(payload)
                )
                logger.info(
                    "compute_safe_creations_task: completed in %.2fs "
                    "source=rollup buckets=%d",
                    time.time() - started,
                    len(series),
                )
                return True

            logger.info(
                "analytics.rollup.cold_window key=safe_creations "
                "falling back to live aggregation and backfilling"
            )
            with relaxed_statement_timeout():
                rows = (
                    SafeContract.objects.annotate(
                        period=Trunc("ethereum_tx__block__timestamp", "day")
                    )
                    .values("period")
                    .annotate(count=Count("address"))
                    .order_by("period")
                )
                materialised = [
                    {"period": row["period"].date().isoformat(), "count": row["count"]}
                    for row in rows
                    if row["period"] is not None
                ]
                # Backfill rollup so future calls hit the fast path.
                # Idempotent via update_or_create on the primary-key date
                # column.
                for row in materialised:
                    DailySafeCreation.objects.update_or_create(
                        date=date.fromisoformat(row["period"]),
                        defaults={"count": row["count"]},
                    )
            payload = {
                "series": materialised,
                "computed_at": timezone.now().isoformat(),
                "source": "live",
            }
            get_redis().set(AnalyticsService.REDIS_SAFE_CREATIONS, json.dumps(payload))
            logger.info(
                "compute_safe_creations_task: completed in %.2fs "
                "source=live+backfill buckets=%d",
                time.time() - started,
                len(materialised),
            )
            return True


# ───────────────────────── C7: DailyMetric pipeline ─────────────────────────
#
# `compute_daily_metrics_task` runs incrementally (default: yesterday only),
# upserts one row per day in `analytics_dailymetric`, and refreshes the
# rolling-window distinct active_* Redis caches in the same task run so the
# read-path semantics stay unchanged. The same `_upsert_daily_metric` helper
# is reused by the `backfill_daily_metrics` management command for one-shot
# history loads on a fresh chain.


def _erc20_active_safe_addrs_between(start, end) -> set[str]:
    """Set of Safe addresses with an ERC20 transfer in `[start, end)`.

    Single JOIN: index-range-scan the day's transfers and join back to
    `history_safecontract`. Replaces the prior per-Safe EXISTS batch loop
    that issued ~200 round-trips and ~2M btree probes on a 1.5M-Safe
    chain.
    """
    with connection.cursor() as cursor:
        cursor.execute(_ERC20_ACTIVE_BETWEEN_JOIN_SQL, [start, end, start, end])
        return {_normalize_addr(row[0]) for row in cursor}


def _safes_active_between(start, end) -> int:
    """Closed-interval distinct count of active Safes in `[start, end)`.
    Used by C7 per-day DAU rows; the open-ended form (`_safes_active_in_window`)
    is still used by the standalone active_* tasks and the rolling-window
    refresh in `_refresh_active_window_caches`.

    Thin wrapper around `_safes_active_between_set` — the rollup populator
    needs the membership, the DAU column only needs the cardinality.
    """
    # Forward declaration: the set helper is defined below `_upsert_daily_metric`
    # so it can call into the rollup-populator helpers that themselves use
    # `_erc20_active_safe_addrs_between` (also defined here). The same function
    # is called from both directions; Python resolves at call time so order is
    # fine — but if you reorganize, keep both in this module.
    return len(_safes_active_between_set(start, end))


def _active_owners_between(start, end, batch_size: int = 5000) -> int:
    """Closed-interval distinct count of confirming owners in `[start, end)`."""
    executed_tx_ids = list(
        MultisigTransaction.objects.filter(
            ethereum_tx__block__timestamp__gte=start,
            ethereum_tx__block__timestamp__lt=end,
        ).values_list("safe_tx_hash", flat=True)
    )
    seen: set[str] = set()
    for i in range(0, len(executed_tx_ids), batch_size):
        chunk = executed_tx_ids[i : i + batch_size]
        seen.update(
            _normalize_addr(o)
            for o in MultisigConfirmation.objects.filter(
                multisig_transaction_id__in=chunk
            )
            .values_list("owner", flat=True)
            .distinct()
        )
    return len(seen)


_METRIC_CORE_NEW_SAFES_SQL = """
SELECT COUNT(*)
FROM history_safecontract sc
JOIN history_ethereumtx etx ON sc.ethereum_tx_id = etx.tx_hash
WHERE etx.block_id >= %s AND etx.block_id < %s
"""

# Combined count + native-wei sum in one round-trip. The legacy ORM form
# evaluated `multisig_executed_qs.count()` and
# `multisig_executed_qs.aggregate(Sum)` as two separate queries — same
# join shape, executed twice. One SQL with both aggregates halves the
# wall-clock for this step. The cast to `text` (then to Python int in
# the caller) keeps the value precise for the uint256 sum without
# relying on Django's implicit DecimalField precision.
_METRIC_CORE_MULTISIG_COUNT_SUM_SQL = """
SELECT
    COUNT(*) AS tx_count,
    COALESCE(SUM(mt.value), 0)::text AS native_wei
FROM history_multisigtransaction mt
JOIN history_ethereumtx etx ON mt.ethereum_tx_id = etx.tx_hash
WHERE etx.block_id >= %s AND etx.block_id < %s
"""


def _compute_daily_metric_core(day_start, day_end) -> DailyMetric:
    """Upsert the `DailyMetric` row for `[day_start, day_end)`.

    Carved out of `_upsert_daily_metric` so the per-day write path can be
    sharded across Celery workers (one task per day) without dragging the
    four rollup populators along on every retry path.

    Each of the three timestamp-bounded aggregates (`new_safes`,
    `multisig_txs_executed`, `native_value_wei`) is anchored on
    `etx.block_id` (FK-indexed) after a block-window pre-resolve. The
    prior ORM form (`ethereum_tx__block__timestamp__gte=…`) emitted a
    3-table join with WHERE on `eb.timestamp`, which on busy chains
    produced a plan that did not prune `etx` early — the three queries
    together took ~40 min/day in the original implementation. The
    block-window form puts each in the seconds range.

    `multisig_txs_executed` and `native_value_wei` share the same join
    shape; combined into a single SQL with two aggregates to halve the
    round-trip.

    `module_txs` keeps the simple 2-table ORM filter on
    `internal_tx.timestamp` (directly btree-indexed); `erc20_transfers`
    keeps the 1-table count on its own timestamp index. Neither is the
    bottleneck.
    """
    start_block, end_block = _resolve_block_window(day_start, day_end)
    if start_block is None:
        # Day not yet indexed — write an all-zeros row so consumers can
        # still distinguish "no data yet" via `computed_at`. The cron
        # will re-fire on a later day and idempotently overwrite.
        new_safes = 0
        multisig_txs_executed = 0
        native_value_wei = 0
    else:
        with connection.cursor() as cursor:
            cursor.execute(_METRIC_CORE_NEW_SAFES_SQL, [start_block, end_block])
            new_safes = int(cursor.fetchone()[0] or 0)
            cursor.execute(
                _METRIC_CORE_MULTISIG_COUNT_SUM_SQL,
                [start_block, end_block],
            )
            row = cursor.fetchone()
            multisig_txs_executed = int(row[0] or 0)
            native_value_wei = int(row[1] or 0)

    module_txs = ModuleTransaction.objects.filter(
        internal_tx__timestamp__gte=day_start,
        internal_tx__timestamp__lt=day_end,
    ).count()

    erc20_transfers = ERC20Transfer.objects.filter(
        timestamp__gte=day_start,
        timestamp__lt=day_end,
    ).count()

    # Read active_safes_daily from the `analytics_dailyactivesafe` rollup
    # when `_compute_daily_active_safes` has already populated it for this
    # day — avoids running the expensive 3-leg active-safe union *twice*
    # per day (once for this count, once for the rollup membership rows).
    # `_upsert_daily_metric` orders populators so active_safes lands first.
    # Fallback: compute fresh if the rollup row hasn't been written
    # (shouldn't happen via the canonical entry point, but keeps the
    # function safe to call directly in tests).
    rollup_count = DailyActiveSafe.objects.filter(date=day_start.date()).count()
    if rollup_count > 0:
        active_safes_daily = rollup_count
    else:
        active_safes_daily = _safes_active_between(day_start, day_end)

    # Same trick for active owners — populator runs immediately after
    # `active_safes` in the chain so `analytics_dailyactiveowner` already
    # holds the per-day membership by the time we read it here. Avoids the
    # expensive `_active_owners_between` Python loop (executed-tx hash
    # materialisation + N/5000 confirmation joins) on every daily run.
    owners_rollup_count = DailyActiveOwner.objects.filter(date=day_start.date()).count()
    if owners_rollup_count > 0:
        active_owners_daily = owners_rollup_count
    else:
        active_owners_daily = _active_owners_between(day_start, day_end)

    obj, _ = DailyMetric.objects.update_or_create(
        date=day_start.date(),
        defaults={
            "new_safes": new_safes,
            "active_safes": active_safes_daily,
            "active_owners": active_owners_daily,
            "multisig_txs_executed": multisig_txs_executed,
            "module_txs": module_txs,
            "erc20_transfers": erc20_transfers,
            "native_value_wei": native_value_wei,
            "computed_at": timezone.now(),
        },
    )
    return obj


# ─────────────── Rollup populators (one per narrow rollup table) ───────
#
# All four are idempotent: `INSERT … ON CONFLICT DO UPDATE` for the additive
# rollups, and `ON CONFLICT DO NOTHING` for the (date, safe_address)
# membership table. Safe to re-run for the same day window — the daily
# cron, the backfill management command, and the per-day shard task all
# share these helpers.


def _compute_daily_token_volume(day_start, day_end) -> int:
    """Populate `analytics_daily_token_volume` for `[day_start, day_end)`.

    One row per (date, token_address). Returns the number of (token) rows
    written. Spec §2.1 / §3.
    """
    date_value = day_start.date()
    sql = """
        INSERT INTO analytics_dailytokenvolume
            (date, token_address, transfer_count, transfer_value, computed_at)
        SELECT
            %s::date,
            address,
            COUNT(*),
            COALESCE(SUM(value), 0),
            NOW()
        FROM history_erc20transfer
        WHERE timestamp >= %s AND timestamp < %s
        GROUP BY address
        ON CONFLICT (date, token_address) DO UPDATE SET
            transfer_count = EXCLUDED.transfer_count,
            transfer_value = EXCLUDED.transfer_value,
            computed_at    = NOW()
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, [date_value, day_start, day_end])
        return cursor.rowcount


_DAILY_ACTIVE_SAFES_INSERT_SQL = """
INSERT INTO analytics_dailyactivesafe (date, safe_address)
SELECT %s::date, addr FROM unnest(%s::bytea[]) AS t(addr)
ON CONFLICT (date, safe_address) DO NOTHING
"""

# Multisig leg with block-window pre-resolve. Keyed on `etx.block_id`
# (FK-indexed) — same shape used by `_DAILY_ACTIVE_OWNERS_SQL` and
# `_ACTIVE_OWNERS_FALLBACK_SQL`. The prior ORM form
# (`ethereum_tx__block__timestamp__gte=…`) emitted a 3-table join with the
# WHERE on `eb.timestamp`, which the planner did not always shape into a
# nested-loop anchored on the block-range index → on busy chains it timed
# out before producing rows.
_DAILY_ACTIVE_SAFES_MULTISIG_LEG_SQL = """
SELECT DISTINCT mt.safe
FROM history_multisigtransaction mt
JOIN history_ethereumtx etx ON mt.ethereum_tx_id = etx.tx_hash
WHERE etx.block_id >= %s AND etx.block_id < %s
"""

# Module leg: 2-table join keyed on the directly-indexed
# `InternalTx.timestamp` btree. No need for the block-window trick here
# (which the multisig leg needs to dodge a 3-table planner shape); the
# `(timestamp)` index on `history_internaltx` lets PG range-scan into
# the day window then nested-loop to `mod_tx` via the FK-indexed
# `mod_tx.internal_tx_id`. ModuleTransaction is at most 1-3 orders of
# magnitude less voluminous than MultisigTransaction on production
# chains; this leg is rarely the bottleneck.
_DAILY_ACTIVE_SAFES_MODULE_LEG_SQL = """
SELECT DISTINCT mod_tx.safe
FROM history_moduletransaction mod_tx
JOIN history_internaltx it ON mod_tx.internal_tx_id = it.id
WHERE it.timestamp >= %s AND it.timestamp < %s
"""


def _compute_daily_active_safes(day_start, day_end) -> int:
    """Populate `analytics_dailyactivesafe` for `[day_start, day_end)`.

    Three-leg union (multisig + module + ERC20) producing the membership
    set, then bulk-INSERT with `ON CONFLICT DO NOTHING` for idempotency.

    Each leg uses a planner-friendly shape:
      - Multisig + module legs: block-window pre-resolve (one indexed
        `MIN(EthereumBlock.number)` sub-ms probe per bound), then
        anchored on `etx.block_id` (FK-indexed). Same trick as
        `_DAILY_ACTIVE_OWNERS_SQL` and `_ACTIVE_OWNERS_FALLBACK_SQL`.
      - ERC20 leg: anchored on SafeContract addresses, probing the
        existing `(_from, timestamp)` / `(to, timestamp)` covering
        indexes on `TokenTransfer` in 5000-Safe batches via
        `_erc20_active_safe_addrs_between`.

    History: an earlier revision UNIONed all four legs in one SQL with
    a post-EXISTS filter against `history_safecontract`; that scanned
    the entire ERC20 transfer table for the day window and timed out.
    The follow-up reused `_safes_active_between_set` which still emitted
    the multisig leg through `ethereum_tx__block__timestamp__gte=…` ORM
    join — same bad 3-table-on-timestamp plan, also timed out on busy
    chains. This revision forces the planner's hand for the multisig
    and module legs by pre-resolving the block range.

    Block-window approximation: assumes `EthereumBlock.number` is
    monotonic with `timestamp`. Sub-day L2 sequencer drift is irrelevant
    at this grain; same trade-off the project already accepted in
    `_active_owners_in_window`.

    Returns the number of distinct safe rows inserted (excludes rows
    skipped by `ON CONFLICT`, so reruns return 0).
    """
    date_value = day_start.date()
    start_block, end_block = _resolve_block_window(day_start, day_end)
    if start_block is None:
        return 0

    active: set[str] = set()
    with connection.cursor() as cursor:
        # Multisig + module legs in two short FK-join queries.
        cursor.execute(
            _DAILY_ACTIVE_SAFES_MULTISIG_LEG_SQL,
            [start_block, end_block],
        )
        active.update(_normalize_addr(row[0]) for row in cursor.fetchall())
        cursor.execute(
            _DAILY_ACTIVE_SAFES_MODULE_LEG_SQL,
            [day_start, day_end],
        )
        active.update(_normalize_addr(row[0]) for row in cursor.fetchall())

    # ERC20 leg via the existing Safe-anchored batched-EXISTS helper.
    # Bounded by `len(SafeContract) / batch_size` round-trips × O(1)
    # per-batch index probe; on a 1.5M-Safe chain that's ~300 round-
    # trips, each hitting the covering `(_from, timestamp)` /
    # `(to, timestamp)` index. Tens of seconds on a busy chain.
    active.update(_erc20_active_safe_addrs_between(day_start, day_end))

    if not active:
        return 0

    # Address strings come back lowercase-hex from `_normalize_addr`;
    # `analytics_dailyactivesafe.safe_address` is bytea, so convert once
    # for the bulk INSERT.
    address_bytes_all = [bytes.fromhex(a[2:]) for a in active]
    rows_inserted = 0
    batch_size = 5000
    with connection.cursor() as cursor:
        for i in range(0, len(address_bytes_all), batch_size):
            batch = address_bytes_all[i : i + batch_size]
            cursor.execute(
                _DAILY_ACTIVE_SAFES_INSERT_SQL,
                [date_value, batch],
            )
            rows_inserted += cursor.rowcount
    return rows_inserted


# Block-window range form: pre-resolves day_start / day_end into block-
# number bounds via the indexed `history_ethereumblock(timestamp)` btree
# (two scalar sub-queries, sub-ms each), then range-scans
# `history_ethereumtx.block_id` (FK-indexed) and nested-loops out via
# FK indexes to `mt` and `mc`. Mirrors the open-ended
# `_ACTIVE_OWNERS_FALLBACK_SQL` pattern but adds an upper bound for the
# closed-day range. `COALESCE(..., bigint_max)` keeps the upper-bound
# predicate well-defined when the chain has not yet indexed past the day
# (e.g. the daily cron at 01:00 might race the indexer for the most-
# recent day, in which case we count everything from `start_block`
# onward — same liberal-upper-bound behaviour as the open-ended form).
#
# Approximation: assumes `EthereumBlock.number` is monotonic with
# `timestamp`. On EVM L2s the worst-case sequencer-pause inversion is a
# few seconds; irrelevant at day grain. This is the same trade-off the
# project already accepted in `_active_owners_in_window`.
_DAILY_ACTIVE_OWNERS_SQL = """
INSERT INTO analytics_dailyactiveowner (date, owner_address)
SELECT %s::date, mc.owner
FROM history_multisigconfirmation mc
JOIN history_multisigtransaction mt
    ON mc.multisig_transaction_id = mt.safe_tx_hash
JOIN history_ethereumtx etx
    ON mt.ethereum_tx_id = etx.tx_hash
WHERE etx.block_id >= %s
  AND etx.block_id <  %s
GROUP BY mc.owner
ON CONFLICT (date, owner_address) DO NOTHING
"""

# Sentinel upper bound when the chain has no block at or past `day_end`.
# Postgres `bigint` max is 2**63-1; `history_ethereumblock.number` is a
# `PositiveIntegerField` (32-bit unsigned) so this is comfortably above
# any real block number.
_BLOCK_NUMBER_SENTINEL_MAX = 9223372036854775807


def _resolve_block_window(day_start, day_end) -> tuple[int | None, int]:
    """Resolve a `[day_start, day_end)` datetime window into a
    `[start_block, end_block)` block-number range.

    Two indexed btree probes on `history_ethereumblock(timestamp)`,
    sub-ms each. Returns `(None, _)` when no block has been indexed at
    or after `day_start` (the daily cron racing the indexer on the most-
    recent day, or backfill targeting a date pre-genesis). Returns
    `(start_block, _BLOCK_NUMBER_SENTINEL_MAX)` when `day_end` resolves
    to no block (chain hasn't indexed past the day yet) — callers then
    range-scan from `start_block` to "infinity", same liberal-upper-
    bound behaviour as the open-ended `_ACTIVE_OWNERS_FALLBACK_SQL`.

    Approximation: assumes `EthereumBlock.number` is monotonic with
    `timestamp`. Sub-day L2 sequencer drift (~tens of seconds worst
    case) is irrelevant at day grain. Same trade-off already accepted
    by `_active_owners_in_window`.

    DRY'd here because every populator that touches the
    `ethereum_tx → ethereum_block` join needs the same shape — running
    them all through this helper instead of inlining the two MIN
    queries lets `_upsert_daily_metric` skip work cleanly on chains
    that haven't indexed the day yet, and keeps the block-window
    semantic in one place.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT MIN(number) FROM history_ethereumblock WHERE timestamp >= %s",
            [day_start],
        )
        row = cursor.fetchone()
        start_block = row[0] if row else None
        if start_block is None:
            return None, _BLOCK_NUMBER_SENTINEL_MAX
        cursor.execute(
            "SELECT MIN(number) FROM history_ethereumblock WHERE timestamp >= %s",
            [day_end],
        )
        row = cursor.fetchone()
        end_block = row[0] if row and row[0] is not None else _BLOCK_NUMBER_SENTINEL_MAX
    return start_block, end_block


def _compute_daily_active_owners(day_start, day_end) -> int:
    """Populate `analytics_dailyactiveowner` for `[day_start, day_end)`.

    Confirmation-based semantic: an owner is "active" on day D if at
    least one of their multisig-tx confirmations belongs to a multisig
    tx whose ``EthereumTx.block.timestamp`` lands on day D. One row per
    distinct owner, idempotent via `ON CONFLICT DO NOTHING`.

    Block-window form: pre-resolves the `[day_start, day_end)` timestamp
    window into a `[start_block, end_block)` range against
    `history_ethereumblock` (two indexed sub-ms btree probes), then
    runs a 3-table FK-join `etx → mt → mc` keyed on `etx.block_id`
    (FK-indexed) and groups by `mc.owner`. Replaces the prior 4-table
    join keyed on `eb.timestamp BETWEEN ...` which, on chains with
    substantial daily activity, did not pick a plan that pruned `etx`
    early and hit the 30-min `statement_timeout`.

    Returns the number of distinct owner rows inserted (excludes rows
    skipped by `ON CONFLICT`, so reruns return 0).
    """
    date_value = day_start.date()
    start_block, end_block = _resolve_block_window(day_start, day_end)
    if start_block is None:
        return 0
    with connection.cursor() as cursor:
        cursor.execute(
            _DAILY_ACTIVE_OWNERS_SQL,
            [date_value, start_block, end_block],
        )
        return cursor.rowcount


def _compute_daily_safe_app_txs(day_start, day_end) -> int:
    """Populate `analytics_daily_safe_app_txs` for `[day_start, day_end)`.

    Joins multisig → ethereum_tx → ethereum_block to bound by
    block.timestamp; groups by `origin->>'name'`, skips NULL / empty
    names. Spec §2.3 / §3.

    Early-exit probe: on chains with no Safe Apps origin metadata at
    all (BASE today: 0 rows), the 3-way join scans millions of
    `history_multisigtransaction` rows just to return an empty result.
    A cheap LIMIT-1 probe on the JSONB filter short-circuits to ~5 ms
    for those chains. On chains that *do* have origin data, the probe
    hits the first matching row immediately and the populator runs as
    before.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM history_multisigtransaction "
            "WHERE origin->>'name' IS NOT NULL AND origin->>'name' <> '' "
            "LIMIT 1"
        )
        if cursor.fetchone() is None:
            return 0

    date_value = day_start.date()
    sql = """
        INSERT INTO analytics_dailysafeapptx
            (date, origin_name, origin_url, tx_count)
        SELECT %s::date, origin_name, origin_url, tx_count FROM (
            SELECT
                COALESCE(mt.origin->>'name', '') AS origin_name,
                COALESCE(MAX(mt.origin->>'url'), '') AS origin_url,
                COUNT(*) AS tx_count
            FROM history_multisigtransaction mt
            JOIN history_ethereumtx etx
                ON mt.ethereum_tx_id = etx.tx_hash
            JOIN history_ethereumblock eb
                ON etx.block_id = eb.number
            WHERE eb.timestamp >= %s AND eb.timestamp < %s
              AND mt.origin->>'name' IS NOT NULL
              AND mt.origin->>'name' <> ''
            GROUP BY origin_name
        ) src
        ON CONFLICT (date, origin_name) DO UPDATE SET
            origin_url = EXCLUDED.origin_url,
            tx_count   = EXCLUDED.tx_count
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, [date_value, day_start, day_end])
        return cursor.rowcount


_DAILY_SAFE_CREATIONS_COUNT_SQL = """
SELECT COUNT(*)
FROM history_safecontract sc
JOIN history_ethereumtx etx ON sc.ethereum_tx_id = etx.tx_hash
WHERE etx.block_id >= %s AND etx.block_id < %s
"""


def _compute_daily_safe_creations(day_start, day_end) -> int:
    """Populate `analytics_dailysafecreation` for `[day_start, day_end)`.

    One row per day with the count of new Safes whose creating tx
    landed in the window. Spec §2.4 / §3.

    Block-window form: same shape as `_METRIC_CORE_NEW_SAFES_SQL` (both
    count SafeContracts whose creating EthereumTx lands in the day's
    block range). The prior ORM form
    (`ethereum_tx__block__timestamp__gte=…`) hit the same bad 3-table
    planner shape as the other timestamp-anchored queries — ~9 min/day
    on production data.
    """
    start_block, end_block = _resolve_block_window(day_start, day_end)
    if start_block is None:
        count = 0
    else:
        with connection.cursor() as cursor:
            cursor.execute(_DAILY_SAFE_CREATIONS_COUNT_SQL, [start_block, end_block])
            count = int(cursor.fetchone()[0] or 0)
    DailySafeCreation.objects.update_or_create(
        date=day_start.date(),
        defaults={"count": count},
    )
    return count


def _compute_daily_tx_volume(day_start, day_end) -> int:
    """Populate the proposed/confirmation columns of `analytics_dailymetric`
    for `[day_start, day_end)`.

    Single SQL round-trip — three subselects share one buffered range
    scan on `history_multisigconfirmation.created` (via the CTE) and one
    on `history_multisigtransaction.created`. Idempotent via
    `ON CONFLICT (date) DO UPDATE`.

    Together with `_compute_daily_metric_core` (which writes the
    executed-side columns) these three columns let `get_tx_volume` be a
    pure SUM over the day-rollup — replaces the 5-query live path that
    timed out at 30s on Base.
    """
    date_value = day_start.date()
    sql = """
        INSERT INTO analytics_dailymetric (
            date, new_safes, active_safes, active_owners,
            multisig_txs_executed, module_txs, erc20_transfers,
            native_value_wei,
            multisig_txs_proposed, confirmations_count, confirmed_tx_count,
            computed_at
        )
        SELECT
            %s::date, 0, 0, 0, 0, 0, 0, 0,
            (SELECT COUNT(*) FROM history_multisigtransaction
                WHERE created >= %s AND created < %s),
            c.confs, c.tx_cnt,
            NOW()
        FROM (
            SELECT
                COUNT(*)                                  AS confs,
                COUNT(DISTINCT multisig_transaction_id)   AS tx_cnt
            FROM history_multisigconfirmation
            WHERE created >= %s AND created < %s
        ) c
        ON CONFLICT (date) DO UPDATE SET
            multisig_txs_proposed = EXCLUDED.multisig_txs_proposed,
            confirmations_count   = EXCLUDED.confirmations_count,
            confirmed_tx_count    = EXCLUDED.confirmed_tx_count,
            computed_at           = NOW()
    """
    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            [
                date_value,
                day_start,
                day_end,  # MultisigTransaction subselect
                day_start,
                day_end,  # MultisigConfirmation CTE
            ],
        )
        return cursor.rowcount


def _safes_active_between_set(start, end) -> set[str]:
    """Same body as `_safes_active_between` but returns the set instead of
    just the cardinality. Split out so `_compute_daily_active_safes` can
    persist the membership without re-running the (expensive) union.
    """
    active: set[str] = set()
    active.update(
        _normalize_addr(a)
        for a in MultisigTransaction.objects.filter(
            ethereum_tx__block__timestamp__gte=start,
            ethereum_tx__block__timestamp__lt=end,
        )
        .values_list("safe", flat=True)
        .distinct()
    )
    active.update(
        _normalize_addr(a)
        for a in ModuleTransaction.objects.filter(
            internal_tx__timestamp__gte=start,
            internal_tx__timestamp__lt=end,
        )
        .values_list("safe", flat=True)
        .distinct()
    )
    active.update(_erc20_active_safe_addrs_between(start, end))
    return active


def _upsert_daily_metric(day_start, day_end) -> DailyMetric:
    """Run the 6-step single-day populate path: 5 narrow rollup tables
    plus the `DailyMetric` core upsert.

    Population order matters: the membership populators (`active_safes`,
    `active_owners`) run FIRST so their rollups are populated before
    `_compute_daily_metric_core` reads its `active_safes_daily` /
    `active_owners_daily` counts from them. Without this ordering the
    same expensive aggregates run twice per day (once for the count,
    once for the rollup rows) — on BASE that doubles wall time.

    Per-populator failures are isolated so one slow / failing populator
    does not strand the others.
    """
    # Time each populator so operators can localise slow chains via logs
    # without having to ssh + py-spy. Cheap (one logger call per day per
    # populator).
    populator_order = (
        ("active_safes", _compute_daily_active_safes),
        ("active_owners", _compute_daily_active_owners),
        ("token_volume", _compute_daily_token_volume),
        ("tx_volume", _compute_daily_tx_volume),
        ("safe_app_txs", _compute_daily_safe_app_txs),
        ("safe_creations", _compute_daily_safe_creations),
    )
    day_label = day_start.date()
    overall_started = time.time()
    logger.info(
        "_upsert_daily_metric: starting day=%s populators=%d",
        day_label,
        len(populator_order),
    )
    for name, fn in populator_order:
        step_started = time.time()
        try:
            rows = fn(day_start, day_end)
            logger.info(
                "_upsert_daily_metric: rollup %s took %.2fs day=%s rows=%s",
                name,
                time.time() - step_started,
                day_label,
                rows,
            )
        except Exception:
            logger.exception(
                "_upsert_daily_metric: rollup %s failed in %.2fs day=%s",
                name,
                time.time() - step_started,
                day_label,
            )

    # Core last so it can SELECT COUNT(*) from analytics_dailyactivesafe
    # instead of re-running _safes_active_between.
    core_started = time.time()
    obj = _compute_daily_metric_core(day_start, day_end)
    logger.info(
        "_upsert_daily_metric: metric_core took %.2fs day=%s "
        "active_safes=%d active_owners=%d multisig_txs_executed=%d",
        time.time() - core_started,
        day_label,
        obj.active_safes,
        obj.active_owners,
        obj.multisig_txs_executed,
    )
    logger.info(
        "_upsert_daily_metric: completed in %.2fs day=%s",
        time.time() - overall_started,
        day_label,
    )
    return obj


def _refresh_active_window_caches(now) -> None:
    """Recompute the rolling-window distinct active_safes / active_owners
    counts and write to the existing Redis keys. Preserves today's
    window-distinct semantics on the read path (see plan §C7 / Q1)."""
    redis = get_redis()
    for window_str, days in [("7d", 7), ("30d", 30), ("90d", 90)]:
        cutoff = now - timezone.timedelta(days=days)
        step_started = time.time()
        try:
            safes_count = _safes_active_in_window(cutoff)
            redis.set(
                AnalyticsService.REDIS_ACTIVE_SAFES_PREFIX + window_str,
                json.dumps(
                    {
                        "window": window_str,
                        "active_safes": safes_count,
                        "computed_at": now.isoformat(),
                    }
                ),
            )
            logger.info(
                "_refresh_active_window_caches: active_safes %s took %.2fs count=%d",
                window_str,
                time.time() - step_started,
                safes_count,
            )
        except Exception:
            logger.exception(
                "_refresh_active_window_caches: active_safes %s failed after %.2fs",
                window_str,
                time.time() - step_started,
            )
        step_started = time.time()
        try:
            owners_count = _active_owners_in_window(cutoff)
            redis.set(
                AnalyticsService.REDIS_ACTIVE_OWNERS_PREFIX + window_str,
                json.dumps(
                    {
                        "window": window_str,
                        "active_owners": owners_count,
                        "computed_at": now.isoformat(),
                    }
                ),
            )
            logger.info(
                "_refresh_active_window_caches: active_owners %s took %.2fs count=%d",
                window_str,
                time.time() - step_started,
                owners_count,
            )
        except Exception:
            logger.exception(
                "_refresh_active_window_caches: active_owners %s failed after %.2fs",
                window_str,
                time.time() - step_started,
            )


@app.shared_task(bind=True)
@task_timeout(timeout_seconds=LOCK_TIMEOUT * 4)
def compute_daily_metrics_task(self, days_back: int = 1) -> bool:
    """Upsert `DailyMetric` rows for the last `days_back` complete UTC days
    and refresh the rolling-window distinct active_* Redis keys.

    Default `days_back=1` runs the previous-day metric daily. The same task
    is invoked by the `backfill_daily_metrics` management command with a
    larger range for one-shot history loads. Per-day try/except so a single
    bad day doesn't strand the rest of the run.

    Guarded by ``only_one_running_task(self)`` so the 01:00 cron and a
    manual ``.delay()`` (or a backfill shard for the same date range)
    cannot race the same ``update_or_create`` and trip ``IntegrityError``
    on the ``analytics_dailymetric`` PK.
    """
    with contextlib.suppress(LockError):
        with only_one_running_task(self):
            started = time.time()
            now = timezone.now()
            today_utc_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
            logger.info("compute_daily_metrics_task: starting days_back=%d", days_back)
            written = 0
            with relaxed_statement_timeout():
                for offset in range(1, days_back + 1):
                    day_start = today_utc_midnight - timezone.timedelta(days=offset)
                    day_end = day_start + timezone.timedelta(days=1)
                    try:
                        _upsert_daily_metric(day_start, day_end)
                        written += 1
                    except Exception:
                        logger.exception(
                            "compute_daily_metrics_task: day %s failed",
                            day_start.date(),
                        )
                        continue
                refresh_started = time.time()
                try:
                    _refresh_active_window_caches(today_utc_midnight)
                    logger.info(
                        "compute_daily_metrics_task: rolling-window refresh took %.2fs",
                        time.time() - refresh_started,
                    )
                except Exception:
                    logger.exception(
                        "compute_daily_metrics_task: rolling-window refresh "
                        "failed after %.2fs",
                        time.time() - refresh_started,
                    )
            logger.info(
                "compute_daily_metrics_task: completed in %.2fs days_written=%d/%d",
                time.time() - started,
                written,
                days_back,
            )
            return written > 0
