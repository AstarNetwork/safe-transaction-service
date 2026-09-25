import contextlib
import json
import logging
import time
from datetime import date, datetime, timedelta
from decimal import Decimal

from django.conf import settings
from django.db import connection, transaction
from django.db.models import Count, F, Max, Min, Q
from django.db.models.functions import Trunc
from django.utils import timezone

from celery import app
from dateutil.relativedelta import relativedelta
from hexbytes import HexBytes
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
from safe_transaction_service.analytics.catchup.day import (
    CORE_INPUTS,
    POPULATORS,
    DayResult,
    DayStatus,
    compute_day,
)
from safe_transaction_service.analytics.catchup.gate import (
    DayNotReady,
    get_indexer_status,
)
from safe_transaction_service.analytics.catchup.snapshots import (
    mark_snapshot_dispatched,
)
from safe_transaction_service.analytics.catchup.state import (
    get_day_state,
    reset_core_ok,
)
from safe_transaction_service.analytics.catchup.sweeper import run_catchup
from safe_transaction_service.analytics.models import (
    AnalyticsSnapshot,
    AnalyticsWatermark,
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
    EthereumBlock,
    IndexingStatus,
    ModuleTransaction,
    MultisigConfirmation,
    MultisigTransaction,
    SafeContract,
    SafeMasterCopy,
)
from safe_transaction_service.utils.celery import task_timeout
from safe_transaction_service.utils.redis import get_redis
from safe_transaction_service.utils.tasks import (
    LOCK_TIMEOUT,
    get_task_lock_name,
    only_one_running_task,
)

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
            # In-flight mark for the snapshot sweeper (`catchup/snapshots.py`)
            # -- set here, at the very start, regardless of whether beat or
            # the sweeper triggered this run.
            mark_snapshot_dispatched("summary")
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
    ERC20 movement at or after `cutoff`.

    Fast path: aggregate over the per-day ``DailyActiveSafe`` rollup — a
    single ``COUNT(DISTINCT safe_address)`` over the date range, the same
    number and the same shape the read path computes when it has to. This
    mirrors `_active_owners_in_window`, which has been rollup-first since
    the rollups landed; this one was the odd half of the pair, computing
    from live ``history_*`` every time.

    That asymmetry mattered once the read path started serving the Redis
    scalar this function feeds ahead of the rollup: a Redis value built
    from the live tables and a rollup value built from
    ``analytics_dailyactivesafe`` answer slightly different questions
    (`_compute_daily_active_safes` buckets by UTC day and only covers days
    it has run for), so the same endpoint could return one or the other
    depending on whether the key happened to be warm. Reading the rollup
    here makes the two the same number by construction.

    Cold-window fallback: no rollup row covers the window (fresh instance,
    pre-backfill, or a long gap since the last daily run), so the live
    three-leg union below runs and emits
    ``analytics.rollup.cold_window``. Note the gate is ``date__gte`` on the
    cutoff, not "the table is non-empty": a rollup whose newest row
    predates the window is cold *for that window*.

    Contract invariant 3 applies as everywhere else: this is a distinct
    count over the span, never a sum of the per-day rows.
    """
    cutoff_date = cutoff.date() if hasattr(cutoff, "date") else cutoff
    rollup_qs = DailyActiveSafe.objects.filter(date__gte=cutoff_date)
    if rollup_qs.exists():
        return rollup_qs.aggregate(n=Count("safe_address", distinct=True))["n"] or 0

    logger.info(
        "analytics.rollup.cold_window key=active_safes cutoff=%s",
        cutoff_date,
    )
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
    ``COUNT(DISTINCT owner_address)`` over the date range, orders of
    magnitude cheaper than the live join regardless of ``history_*``
    size, though not free: it is seconds to tens of seconds on a large
    rollup, which is why the read path prefers the Redis scalar this
    function feeds (decision log, 2026-09-20). Matches the semantic of
    the rollup populator (owners with at least one confirmation on a tx
    executed that UTC day).

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
        # `COUNT(DISTINCT owner_address)` rather than COUNT(*) over a
        # SELECT DISTINCT subquery: same number, but the aggregate form
        # goes through the (date, owner_address) unique index instead of
        # sorting the (owner_address, date) one. See the 2026-09-20
        # decision-log entry for the BASE numbers.
        return rollup_qs.aggregate(n=Count("owner_address", distinct=True))["n"] or 0

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
            # In-flight mark for the snapshot sweeper -- see
            # `compute_summary_task`.
            mark_snapshot_dispatched("safe_segments")
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

    Writes a phase-1 placeholder snapshot only if none exists yet, gets
    the native side, and dispatches ``finalize_tvl_snapshot`` on the
    ``contracts`` queue to do the ERC20 aggregation and write the real
    snapshot. Returns immediately either way — success of the heavy work
    is observable via the snapshot's ``computed_at`` advancing past the
    placeholder. (The shape before that was a blocking ``.get()`` on the
    chord, which hung indefinitely on gevent workers + Redis result
    backend.)

    The native side is normally a millisecond read of the incremental
    rollup. On an instance that has migrated but not yet run
    ``manage.py backfill_native_balances`` the rollup has no watermark,
    and we fall back to the 16-shard chord — the number it served before,
    rather than a zero. That fallback is the only reason the shard
    machinery is still here.
    """
    with contextlib.suppress(LockError):
        with only_one_running_task(self):
            from safe_transaction_service.analytics.tasks_shards import (
                dispatch_tvl_chord,
                dispatch_tvl_finalize,
            )

            # In-flight mark for the snapshot sweeper -- see
            # `compute_summary_task`. Set here rather than relying on this
            # task's own `only_one_running_task` lock: that lock is released
            # as soon as this function returns below, well before the
            # dispatched chord/finalize actually finishes.
            mark_snapshot_dispatched("tvl")
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
                    # 0/0 marked this as a "never-computed" placeholder
                    # back when a real run wrote 16 into `total_shards`.
                    # The rollup path writes 0/0 too, so the discriminator
                    # is now `computed_at` (null on a cold read) and
                    # `native_source` (null here, set by a real run).
                    "partial_shards": 0,
                    "total_shards": 0,
                    "native_source": None,
                    "native_updated_to_block": None,
                    "computed_at": timezone.now().isoformat(),
                }
                _write_snapshot("tvl", placeholder)
                logger.info("compute_tvl_task: phase1 placeholder snapshot written")

            # Phase 2 — fire-and-forget. `finalize_tvl_snapshot` does the
            # ERC20 aggregation and writes the real snapshot.
            rollup = read_native_balance_rollup()
            if rollup is None:
                logger.info(
                    "analytics.rollup.cold_window key=native_balance — no "
                    "watermark, falling back to the 16-shard chord. Run "
                    "`manage.py backfill_native_balances` to switch this "
                    "instance over."
                )
                dispatch_tvl_chord()
                logger.info(
                    "compute_tvl_task: chord dispatched in %.2fs",
                    time.time() - started,
                )
                return True

            dispatch_tvl_finalize(
                {
                    "balance_wei": rollup["balance_wei"],
                    "safes_with_balance": rollup["safes_with_balance"],
                    # No shards on this path; 0/0 is what the hub reads as
                    # a complete run. Kept because dropping a payload key
                    # is a breaking change that has to land consumer-first.
                    "partial_shards": 0,
                    "total_shards": 0,
                    "native_source": "rollup",
                    "native_updated_to_block": rollup["updated_to_block"],
                }
            )
            logger.info(
                "compute_tvl_task: finalize dispatched in %.2fs from the "
                "rollup (native_wei=%d safes_with_balance=%d "
                "updated_to_block=%d over %d rows)",
                time.time() - started,
                rollup["balance_wei"],
                rollup["safes_with_balance"],
                rollup["updated_to_block"],
                rollup["safe_rows"],
            )
            return True


# ───────────────── Incremental native-balance rollup ──────────────────
#
#   watermark W ─────────────────────────────────────────► head
#   ├─ (1) SEED   Safes absent from the rollup, balance summed over
#   │             blocks ≤ W only — NOT ≤ head. A Safe created mid-window
#   │             only enters `history_safecontract` when the indexer
#   │             gets to it; seeding it at `head` and then applying the
#   │             delta would count (W, head] twice, seeding it at W and
#   │             applying the delta counts every block exactly once.
#   ├─ (2) DELTA  one pass over (W, head], applied with `+=` ─┐ same
#   └─ (3) MARK   watermark := head ─────────────────────────┴ transaction
#
# (2) and (3) share a transaction because a crash between them either
# loses the range or applies it twice, and a signed running total cannot
# tell the difference afterwards. With them atomic, re-running at the
# same watermark is a no-op — which is the whole idempotency story.

NATIVE_BALANCE_WATERMARK = "native_balance"

# Addresses per round-trip in the seed step. The same 5000 the legacy
# `BALANCE_BATCH_SQL` loop uses — it is what the `= ANY(...)` plan was
# measured on.
NATIVE_BALANCE_SEED_BATCH_SIZE = 5000

# Ceiling on how many never-seen Safes one incremental run will seed.
# A normal day brings hundreds (Ethereum runs ~250-350 new Safes/day).
# Tens of thousands means the rollup was never backfilled or the table
# was truncated — which is exactly the full-history recompute this
# rollup exists to stop doing inside a nightly task. Refuse loudly
# instead of grinding for hours and timing out anyway.
NATIVE_BALANCE_MAX_SEED_PER_RUN = 50_000

# `MAX(number)` over confirmed blocks and over all blocks. The first is
# an index-scan-backward over the PK stopping at the first confirmed row
# (the unconfirmed tail is ETH_REORG_BLOCKS deep at most), the second is
# the PK maximum — both sub-ms.
_NATIVE_BALANCE_HEAD_SQL = """
SELECT
    (SELECT MAX(number) FROM history_ethereumblock WHERE confirmed),
    (SELECT MAX(number) FROM history_ethereumblock)
"""

# Per-address net native flow over blocks <= `%s`, for an explicit
# address list. Same shape as `BALANCE_BATCH_SQL` and the same plan:
# both legs are driven by the partial covering indexes
# `history_internal_transfer_idx` / `history_internal_transfer_from`
# (`(to|_from, timestamp) INCLUDE (ethereum_tx_id, block_number) WHERE
# call_type = 0 AND value > 0`), so the block bound is a residual filter
# on a column the index already carries, not a second access path.
#
# The bound here is `it.block_number`, while the delta below bounds on
# `etx.block_id`. The two are equal by construction — every writer of
# `InternalTx` sets `block_number` from the transaction's own block
# (`history/models.py` `build_from_trace`, `safe_events_indexer`) — and
# the asymmetry is deliberate: this query is driven by the address, so
# the block is a filter; the delta is driven by the block, and
# `InternalTx.block_number` has no index to drive it with.
_NATIVE_BALANCE_SEED_SQL = """
    SELECT addr, SUM(CASE WHEN direction = 1 THEN value ELSE -value END) AS balance
    FROM (
        SELECT it."to" AS addr, it.value, 1 AS direction
        FROM history_internaltx it
        WHERE it."to" = ANY(%s)
          AND it.call_type = 0 AND it.value > 0 AND it.error IS NULL
          AND it.block_number <= %s
        UNION ALL
        SELECT it."_from" AS addr, it.value, -1 AS direction
        FROM history_internaltx it
        WHERE it."_from" = ANY(%s)
          AND it.call_type = 0 AND it.value > 0 AND it.error IS NULL
          AND it.block_number <= %s
    ) transfers
    GROUP BY addr
"""

# Bulk insert of seeded rows. `unnest` of three parallel arrays beats
# both `executemany` (a round-trip per row) and a generated VALUES list
# (a parameter per cell). `DO NOTHING`, not `DO UPDATE`: a row that
# already exists is already correct through its own `updated_to_block`,
# and overwriting it with a balance computed at this run's watermark
# would silently rewind it.
_NATIVE_BALANCE_SEED_INSERT_SQL = """
INSERT INTO analytics_safenativebalance (safe_address, balance_wei, updated_to_block)
SELECT * FROM unnest(%s::bytea[], %s::numeric[], %s::integer[])
ON CONFLICT (safe_address) DO NOTHING
"""

# Safes the rollup has never seen — an anti-join against the rollup PK.
# A row exists for every Safe the rollup has processed, zero-balance
# ones included, so this returns only genuinely new Safes.
_NATIVE_BALANCE_UNSEEDED_SQL = """
SELECT sc.address
FROM history_safecontract sc
WHERE NOT EXISTS (
    SELECT 1 FROM analytics_safenativebalance b WHERE b.safe_address = sc.address
)
LIMIT %s
"""

# The whole incremental step, as one statement.
#
# Driven from `history_ethereumtx.block_id` (FK-indexed), never from
# `history_internaltx.block_number` — that column carries no index
# (`history/models.py`, absent from `Meta.indexes`), so a range filter
# on it is a sequential scan of tens of millions of rows. This is the
# same idiom, and the same reason, as
# `_METRIC_CORE_MULTISIG_COUNT_SUM_SQL` below: anchor on `etx.block_id`
# so the block window prunes `etx` before the join.
#
# **The `LATERAL` is load-bearing, not a stylistic choice.** Written as a
# plain `JOIN … ON it.ethereum_tx_id = etx.tx_hash`, PostgreSQL picks a
# hash join and scans the whole of `history_internaltx` as the probe
# side, which on Ethereum is 53M rows — every night, for a window of one
# day. Measured on the Ethereum production database, 2026-09-12:
#
#   plain JOIN     Parallel Hash Join + Parallel Seq Scan, cost 19.1M
#   + seqscan off  Parallel Hash Join + Bitmap Heap Scan, cost 22.4M
#   LATERAL        Nested Loop + Index Scan on ethereum_tx_id,
#                  1601 ms for 720 blocks (~15 s for a day)
#
# The index it needs (`history_internaltx_ethereum_tx_id_e6ac35ab`)
# exists and always did — the planner simply refuses to use it here,
# because it estimates 621 internal transactions per Ethereum
# transaction where the real number is 5. A 124x cardinality error makes
# the nested loop look five times more expensive than the table scan.
# `LATERAL` does not argue with that estimate; it removes the choice, so
# the estimate stops mattering. Do not "simplify" this back into a JOIN.
#
# An UPDATE, not an upsert, and that is load-bearing: **the delta must
# never create a row**. The seed owns row creation, because only the seed
# knows to compute the balance below the watermark first. If the delta
# could insert, a Safe the indexer writes into `history_safecontract`
# between this run's seed query and this statement would get a row
# holding only `(W, head]` — missing its entire history below W, and
# never seeded again, because a row now exists. A silent, permanent
# undercount. Restricting to rows that already exist means such a Safe
# is simply skipped this run and seeded correctly by the next one.
#
# The join against the rollup also carries the "is it a Safe" filter for
# free: rows only ever come from the seed, which selects from
# `history_safecontract`. It probes the rollup PK once per distinct
# counterparty rather than once per transfer row.
_NATIVE_BALANCE_DELTA_SQL = """
UPDATE analytics_safenativebalance b
SET balance_wei = b.balance_wei + d.delta,
    updated_to_block = %(head)s
FROM (
    SELECT flows.addr AS addr, SUM(flows.signed_value) AS delta
    FROM history_ethereumtx etx
    CROSS JOIN LATERAL (
        SELECT it."to" AS addr, it.value AS signed_value
        FROM history_internaltx it
        WHERE it.ethereum_tx_id = etx.tx_hash
          AND it.call_type = 0 AND it.value > 0 AND it.error IS NULL
          AND it."to" IS NOT NULL
        UNION ALL
        SELECT it."_from" AS addr, -it.value AS signed_value
        FROM history_internaltx it
        WHERE it.ethereum_tx_id = etx.tx_hash
          AND it.call_type = 0 AND it.value > 0 AND it.error IS NULL
          AND it."_from" IS NOT NULL
    ) flows
    WHERE etx.block_id > %(watermark)s AND etx.block_id <= %(head)s
    GROUP BY flows.addr
) d
WHERE b.safe_address = d.addr
"""

# Read side. Clamps negatives to zero in the SUM and excludes them from
# the count — byte for byte the aggregate `BALANCE_BATCH_SQL` produced,
# so switching `/tvl/` onto the rollup does not move the published
# numbers.
_NATIVE_BALANCE_TOTALS_SQL = """
SELECT
    COALESCE(SUM(CASE WHEN balance_wei > 0 THEN balance_wei ELSE 0 END), 0),
    COUNT(*) FILTER (WHERE balance_wei > 0),
    COUNT(*)
FROM analytics_safenativebalance
"""


def _trace_indexer_block() -> int | None:
    """How far the master-copies indexer has processed, or ``None`` when
    the service has no relevant master copies configured.

    ``MIN(SafeMasterCopy.tx_block_number)`` over the master copies this
    network actually indexes — the same expression
    ``IndexService.get_master_copies_current_indexing_block_number`` uses
    to answer "is this service synced".
    """
    return SafeMasterCopy.objects.relevant().aggregate(position=Min("tx_block_number"))[
        "position"
    ]


def native_balance_head_block() -> int | None:
    """Highest block the rollup may consume: the newest one that is both
    beyond a reorg's reach and actually indexed.

    ``min(MAX(number) WHERE confirmed, MAX(number) - ETH_REORG_BLOCKS,
    MIN(SafeMasterCopy.tx_block_number))``.

    The first two are about reorgs: ``confirmed`` is the indexer's own
    statement that it has stopped re-checking a block
    (``reorg_service.check_reorgs`` sets it), while the depth term is an
    independent backstop for deployments whose reorg task is not running
    or is behind — there ``confirmed`` can sit at a stale height, and the
    run simply stalls (correctly) instead of consuming a block that may
    still vanish.

    The third is about a different hazard entirely, and it is the one
    that bites a freshly spun-up service. ``EthereumBlock`` rows are
    created by *any* indexer — the ERC20 indexer makes a block and a
    transaction the moment it meets a transfer — so block presence, and
    ``confirmed`` with it, can run far ahead of the master-copies indexer
    that actually writes ``InternalTx``. Consume a block whose internal
    transactions have not been written yet and the rollup moves its
    watermark past them forever: they are never revisited, and the
    balance is silently short. Bounding on the trace indexer's own
    position means the rollup waits for it instead.

    On a synced service the third term does not bind — it sits at roughly
    the same height as the other two. It only takes effect when the
    indexer is genuinely behind: initial sync, a reindex, an outage.
    ``None`` (no relevant master copies configured) is treated as "no
    constraint": such a service indexes no internal transactions at all,
    so there is nothing for this bound to protect.

    Why this matters more here than anywhere else in analytics:
    ``recover_from_reorg`` deletes ``EthereumBlock`` rows at or above the
    reorg point and the FK cascade takes ``EthereumTx`` and ``InternalTx``
    with them. A running total that had already absorbed those rows could
    not be un-applied afterwards — the rows are gone. Every other
    analytics rollup recomputes a whole day from scratch and self-heals;
    this one does not, so it only ever consumes blocks a reorg cannot
    reach.

    Returns ``None`` when nothing is safe to consume yet (no block
    indexed, none confirmed, or a chain shallower than
    ``ETH_REORG_BLOCKS``).
    """
    with connection.cursor() as cursor:
        cursor.execute(_NATIVE_BALANCE_HEAD_SQL)
        confirmed_head, tip = cursor.fetchone()
    if confirmed_head is None or tip is None:
        return None
    depth_head = int(tip) - settings.ETH_REORG_BLOCKS
    head = min(int(confirmed_head), depth_head)
    indexed_head = _trace_indexer_block()
    if indexed_head is not None and int(indexed_head) < head:
        logger.info(
            "native_balance.head: master-copies indexer is at %d, below the "
            "reorg-safe head %d — consuming only what it has written",
            indexed_head,
            head,
        )
        head = int(indexed_head)
    if head < 0:
        return None
    # When `confirmed` is the binding term and it lags the depth bound
    # badly, say so: a stalled `check_reorgs` shows up here as a rollup
    # that quietly stops advancing, and nothing else would report it.
    lag = depth_head - int(confirmed_head)
    if lag > settings.ETH_REORG_BLOCKS * 10:
        logger.warning(
            "native_balance.head: confirmed head %d is %d blocks behind the "
            "reorg-depth bound %d — is check_reorgs running? The rollup only "
            "advances as fast as blocks get confirmed.",
            confirmed_head,
            lag,
            depth_head,
        )
    return head


_SAFE_ADDRESSES_PAGE_SQL = """
SELECT address FROM history_safecontract
WHERE address > %s
ORDER BY address
LIMIT %s
"""

_SAFE_ADDRESSES_FIRST_PAGE_SQL = """
SELECT address FROM history_safecontract
ORDER BY address
LIMIT %s
"""


def safe_addresses_after(after: bytes | None, limit: int) -> list[bytes]:
    """One keyset page of ``SafeContract.address``, in PK order.

    The single-page form of ``_iter_safe_addresses_keyset``, for the chunked
    Celery backfill: each chunk is its own task, so the walk has to be
    resumable from a cursor carried in the run manifest rather than from a
    generator living in one process.

    Returns address bytes, so the caller can hand them straight to
    ``_seed_native_balances`` without a hex round-trip.
    """
    with connection.cursor() as cursor:
        if after is None:
            cursor.execute(_SAFE_ADDRESSES_FIRST_PAGE_SQL, [limit])
        else:
            cursor.execute(_SAFE_ADDRESSES_PAGE_SQL, [after, limit])
        return [bytes(row[0]) for row in cursor.fetchall()]


def seed_missing_native_balances(
    address_bytes: list[bytes], upto_block: int
) -> tuple[int, int]:
    """Seed only the addresses that have no rollup row yet.

    Returns ``(seeded, already_present)``. Shared by the inline command and
    the chunked Celery task so "resume" means the same thing in both.
    """
    if not address_bytes:
        return 0, 0
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT safe_address FROM analytics_safenativebalance "
            "WHERE safe_address = ANY(%s)",
            [address_bytes],
        )
        present = {bytes(row[0]) for row in cursor.fetchall()}
    todo = [addr for addr in address_bytes if addr not in present]
    _seed_native_balances(todo, upto_block)
    return len(todo), len(present)


def write_native_balance_watermark(head: int) -> bool:
    """Hand the rollup over to the incremental task at ``head``.

    No-op when a watermark already exists: that one belongs to the
    incremental task, and moving it forward here would skip every block
    between it and ``head`` for the Safes this run did not touch. Returns
    whether it wrote.
    """
    if AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).exists():
        return False
    AnalyticsWatermark.objects.create(
        name=NATIVE_BALANCE_WATERMARK,
        block_number=head,
        computed_at=timezone.now(),
    )
    return True


def _unseeded_safe_addresses(limit: int) -> list[bytes] | None:
    """Address bytes of Safes with no ``SafeNativeBalance`` row, at most
    ``limit`` of them. ``None`` means there are more than ``limit`` — see
    ``NATIVE_BALANCE_MAX_SEED_PER_RUN``.
    """
    with connection.cursor() as cursor:
        cursor.execute(_NATIVE_BALANCE_UNSEEDED_SQL, [limit + 1])
        rows = cursor.fetchall()
    if len(rows) > limit:
        return None
    return [bytes(row[0]) for row in rows]


def _seed_native_balances(address_bytes: list[bytes], upto_block: int) -> int:
    """Create rollup rows for ``address_bytes`` holding their balance as of
    ``upto_block``. Returns how many addresses were processed.

    Every address gets a row, including the ones with no native flow at
    all — see ``SafeNativeBalance``'s docstring: an absent row is the
    "new Safe" signal, so a zero-balance Safe that never got one would be
    re-seeded on every single run.
    """
    if not address_bytes:
        return 0
    for offset in range(0, len(address_bytes), NATIVE_BALANCE_SEED_BATCH_SIZE):
        batch = address_bytes[offset : offset + NATIVE_BALANCE_SEED_BATCH_SIZE]
        with connection.cursor() as cursor:
            cursor.execute(
                _NATIVE_BALANCE_SEED_SQL, [batch, upto_block, batch, upto_block]
            )
            balances = {bytes(addr): Decimal(bal) for addr, bal in cursor.fetchall()}
            cursor.execute(
                _NATIVE_BALANCE_SEED_INSERT_SQL,
                [
                    batch,
                    [balances.get(addr, Decimal(0)) for addr in batch],
                    [upto_block] * len(batch),
                ],
            )
    return len(address_bytes)


def _apply_native_balance_delta(watermark: int, head: int) -> int:
    """Apply the net native flow of ``(watermark, head]`` to the rollup.
    Returns the number of Safe rows the statement updated.

    Only ever updates rows that already exist — see the SQL's comment.
    A Safe with flow in this window but no rollup row yet is left for the
    next run's seed step, which is the only step that knows to compute
    its balance below the watermark first.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            _NATIVE_BALANCE_DELTA_SQL, {"watermark": watermark, "head": head}
        )
        return cursor.rowcount


def read_native_balance_rollup() -> dict | None:
    """Clamped native-balance totals straight out of the rollup.

    Returns ``{"balance_wei", "safes_with_balance", "safe_rows",
    "updated_to_block"}``, or ``None`` when the rollup was never
    initialised (no watermark row). Callers must fall back rather than
    publish the zero: an empty rollup and a fleet holding nothing look
    identical from the totals alone.
    """
    watermark = AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).first()
    if watermark is None:
        return None
    with connection.cursor() as cursor:
        cursor.execute(_NATIVE_BALANCE_TOTALS_SQL)
        balance_wei, safes_with_balance, safe_rows = cursor.fetchone()
    return {
        "balance_wei": int(balance_wei or 0),
        "safes_with_balance": int(safes_with_balance or 0),
        "safe_rows": int(safe_rows or 0),
        "updated_to_block": watermark.block_number,
    }


def _cold_start_native_balance_rollup(head: int) -> AnalyticsWatermark | None:
    """Initialise an empty rollup from inside the nightly run, when — and
    only when — doing so is bounded work.

    A newly spun-up transaction service has no Safes yet, or a handful, so
    requiring a human to run `backfill_native_balances` on every new
    network is friction with nothing behind it. Seeding a small fleet at
    ``head`` and setting the watermark there is exactly what the backfill
    command does, and at this size it is a sub-second query.

    The ceiling is what keeps this honest. Above
    ``NATIVE_BALANCE_MAX_SEED_PER_RUN`` we are no longer initialising a new
    network, we are switching analytics on over an existing one with years
    of history — the hour-long full recompute this whole rollup exists to
    stop doing inside a nightly task. That case still refuses, loudly, and
    still wants the command.

    Returns the watermark row it created, or ``None`` if it declined.
    """
    unseeded = _unseeded_safe_addresses(NATIVE_BALANCE_MAX_SEED_PER_RUN)
    if unseeded is None:
        logger.error(
            "native_balance.rollup: no '%s' watermark and more than %d Safes "
            "to seed (~%d). That is analytics being switched on over an "
            "existing service, not a new network coming up, and seeding it "
            "here is the full-history recompute this rollup replaces. Run "
            "`manage.py backfill_native_balances` once — it is inline, so no "
            "task timeout applies to it.",
            NATIVE_BALANCE_WATERMARK,
            NATIVE_BALANCE_MAX_SEED_PER_RUN,
            approx_count_or_exact(SafeContract, "history_safecontract"),
        )
        return None

    # Seeded at `head`, not at 0: every Safe's whole history through `head`
    # goes into its row, and the watermark then says so. Seeding at 0 would
    # be equally correct and would make the first delta re-read the entire
    # chain for nothing.
    with transaction.atomic():
        _seed_native_balances(unseeded, head)
        watermark_row = AnalyticsWatermark.objects.create(
            name=NATIVE_BALANCE_WATERMARK,
            block_number=head,
            computed_at=timezone.now(),
        )
    logger.info(
        "native_balance.rollup: cold start — initialised %d Safes at block "
        "%d. Subsequent runs are incremental.",
        len(unseeded),
        head,
    )
    return watermark_row


def run_native_balance_rollup() -> dict | None:
    """One incremental pass — seed, apply, mark. See the sketch above.

    On an uninitialised rollup this also does the cold start, when the
    fleet is small enough for that to be bounded work — see
    ``_cold_start_native_balance_rollup``. A new network therefore needs no
    manual backfill at all: the first nightly run brings the rollup up.

    Returns a summary dict, or ``None`` when the run declined to do
    anything: nothing safe to consume yet, too many Safes to seed from
    cold, or a watermark ahead of the safe head. Every declining path
    logs; the two that mean something is wrong log at ERROR.

    Separate from the Celery task so the backfill command, the drift
    check and the tests can drive it without a broker.
    """
    started = time.time()
    head = native_balance_head_block()
    if head is None:
        logger.info(
            "native_balance.rollup: no confirmed block within the reorg depth "
            "yet; nothing to consume"
        )
        return None

    watermark_row = AnalyticsWatermark.objects.filter(
        name=NATIVE_BALANCE_WATERMARK
    ).first()
    if watermark_row is None:
        cold_start = _cold_start_native_balance_rollup(head)
        if cold_start is None:
            return None
        watermark_row = cold_start

    watermark = watermark_row.block_number
    if watermark > head:
        logger.error(
            "native_balance.rollup: watermark=%d is ahead of the safe head=%d. "
            "Blocks this rollup already applied have been removed — a reorg "
            "deeper than the confirmation zone, or a database restore — and "
            "the rows needed to undo them went with them. Refusing to run; "
            "rebuild with `manage.py backfill_native_balances --restart`.",
            watermark,
            head,
        )
        return None

    with relaxed_statement_timeout():
        unseeded = _unseeded_safe_addresses(NATIVE_BALANCE_MAX_SEED_PER_RUN)
        if unseeded is None:
            logger.error(
                "native_balance.rollup: more than %d Safes have no rollup row. "
                "That is a cold or truncated table, not a day's worth of new "
                "Safes. Refusing to seed them here; run `manage.py "
                "backfill_native_balances` instead.",
                NATIVE_BALANCE_MAX_SEED_PER_RUN,
            )
            return None

        # Seeded at the OLD watermark, deliberately, and outside the
        # transaction below: seeding is idempotent (`DO NOTHING`), so a
        # failure after it leaves rows the next run simply finds already
        # present.
        seeded = _seed_native_balances(unseeded, watermark)

        with transaction.atomic():
            touched = _apply_native_balance_delta(watermark, head)
            AnalyticsWatermark.objects.update_or_create(
                name=NATIVE_BALANCE_WATERMARK,
                defaults={"block_number": head, "computed_at": timezone.now()},
            )

    summary = {
        "watermark_from": watermark,
        "watermark_to": head,
        "blocks": head - watermark,
        "seeded_safes": seeded,
        "touched_safes": touched,
    }
    logger.info(
        "native_balance.rollup: completed in %.2fs blocks=(%d, %d] seeded=%d "
        "touched=%d",
        time.time() - started,
        watermark,
        head,
        seeded,
        touched,
    )
    return summary


@app.shared_task(bind=True)
@task_timeout(timeout_seconds=LOCK_TIMEOUT)
def compute_native_balance_rollup_task(self):
    """Advance the incremental native-balance rollup (daily at 03:05 UTC,
    ten minutes ahead of ``compute_tvl_task`` so TVL reads a fresh one).

    Lives in ``tasks.py`` rather than a module of its own on purpose:
    ``config/settings/base.py`` routes only ``analytics.tasks.*`` and
    ``analytics.tasks_shards.*`` to the ``contracts`` queue, so a new
    module would land in the default queue and silently never run.
    """
    with contextlib.suppress(LockError):
        with only_one_running_task(self):
            return run_native_balance_rollup()


# ─────────────── Native-balance rollup drift check ────────────────
#
# A running total has no self-healing property: unlike every other
# analytics rollup, nothing here recomputes a window from scratch and
# quietly repairs it. One batch applied twice, or not at all, stays wrong
# forever and looks exactly like a real number. So sample it against an
# independent computation — the same `BALANCE_BATCH_SQL` family the
# nightly chord used — and say so out loud when they disagree.
#
# Observability first, on purpose: this reports, it does not repair. A
# self-healing version would have to decide *which* of the two numbers is
# right, and until we have seen real drift we do not know what causes it.

NATIVE_BALANCE_DRIFT_SAMPLE_SIZE = 2000

# `ORDER BY random()` is a sequential scan plus a sort — tens of ms over
# ~460k rows, once a week. `TABLESAMPLE` would be cheaper and biased
# toward physically clustered rows, which is the wrong trade for a check
# whose whole job is to find an anomaly someone else's bug created.
_NATIVE_BALANCE_SAMPLE_SQL = """
SELECT safe_address, balance_wei
FROM analytics_safenativebalance
ORDER BY random()
LIMIT %s
"""

# Rollup rows whose Safe no longer exists. `SafeContract.ethereum_tx` is
# `on_delete=CASCADE` from `EthereumTx`, which cascades from
# `EthereumBlock` — so `recover_from_reorg` deletes Safes as well as
# transfers, and a rollup row for one of them keeps contributing its
# (real, confirmed-block) balance to a Safe that is no longer a Safe.
# Rare: it needs a reorg that removes a Safe creation while leaving the
# funding below the confirmation zone intact. Counted rather than
# deleted, for the same reason the balance drift is only reported —
# until one is seen in the wild, "delete it" is a guess.
_NATIVE_BALANCE_ORPHANS_SQL = """
SELECT COUNT(*)
FROM analytics_safenativebalance b
WHERE NOT EXISTS (
    SELECT 1 FROM history_safecontract sc WHERE sc.address = b.safe_address
)
"""


def check_native_balance_drift(
    sample_size: int = NATIVE_BALANCE_DRIFT_SAMPLE_SIZE,
) -> dict | None:
    """Compare a random sample of rollup rows against a from-scratch
    recompute at the watermark. Returns a summary, or ``None`` when there
    was nothing to check.

    The recompute is bounded at the watermark, not at the current head:
    the rollup only claims to be complete through the watermark, so
    anything above it is not drift, it is just the next run's work.
    """
    started = time.time()
    watermark_row = AnalyticsWatermark.objects.filter(
        name=NATIVE_BALANCE_WATERMARK
    ).first()
    if watermark_row is None:
        logger.info("native_balance.drift: rollup not initialised, nothing to check")
        return None
    watermark = watermark_row.block_number

    with relaxed_statement_timeout():
        with connection.cursor() as cursor:
            cursor.execute(_NATIVE_BALANCE_SAMPLE_SQL, [sample_size])
            sample = {bytes(addr): Decimal(balance) for addr, balance in cursor}
        if not sample:
            logger.info("native_balance.drift: rollup is empty, nothing to check")
            return None

        addresses = list(sample)
        recomputed: dict[bytes, Decimal] = {}
        for offset in range(0, len(addresses), NATIVE_BALANCE_SEED_BATCH_SIZE):
            batch = addresses[offset : offset + NATIVE_BALANCE_SEED_BATCH_SIZE]
            with connection.cursor() as cursor:
                cursor.execute(
                    _NATIVE_BALANCE_SEED_SQL, [batch, watermark, batch, watermark]
                )
                recomputed.update({bytes(addr): Decimal(bal) for addr, bal in cursor})

    # If the nightly run landed between the sample and the recompute, the
    # rows we read describe a different watermark than the one we
    # recomputed at. That is a race, not drift — say so and come back
    # next week rather than reporting a difference nobody can act on.
    if (
        AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK)
        .values_list("block_number", flat=True)
        .first()
        != watermark
    ):
        logger.info(
            "native_balance.drift: the rollup advanced past block %d while the "
            "check was running; skipping this round",
            watermark,
        )
        return None

    with connection.cursor() as cursor:
        cursor.execute(_NATIVE_BALANCE_ORPHANS_SQL)
        orphans = int(cursor.fetchone()[0] or 0)

    mismatches = []
    total_abs_diff = Decimal(0)
    for address, stored in sample.items():
        expected = recomputed.get(address, Decimal(0))
        diff = stored - expected
        if diff:
            mismatches.append((address, stored, expected, diff))
            total_abs_diff += abs(diff)

    summary = {
        "watermark": watermark,
        "sampled": len(sample),
        "mismatched": len(mismatches),
        "total_abs_diff_wei": int(total_abs_diff),
        "max_abs_diff_wei": (
            int(max(abs(d) for *_, d in mismatches)) if mismatches else 0
        ),
        "orphan_rows": orphans,
        "elapsed": round(time.time() - started, 2),
    }

    if orphans:
        logger.warning(
            "native_balance.drift: %d rollup rows have no Safe in "
            "history_safecontract. A reorg that removed a Safe creation "
            "leaves the row behind, still contributing its balance to the "
            "totals. Rebuild with `manage.py backfill_native_balances "
            "--restart` to drop them.",
            orphans,
        )

    if mismatches:
        worst = sorted(mismatches, key=lambda row: abs(row[3]), reverse=True)[:5]
        logger.warning(
            "native_balance.drift: %d of %d sampled Safes disagree with a "
            "from-scratch recompute at block %d (total |diff| = %d wei, worst "
            "= %d wei). Worst offenders: %s. The rollup cannot self-heal — "
            "rebuild with `manage.py backfill_native_balances --restart` if "
            "this is not a one-off.",
            summary["mismatched"],
            summary["sampled"],
            watermark,
            summary["total_abs_diff_wei"],
            summary["max_abs_diff_wei"],
            ", ".join(
                f"0x{address.hex()} stored={stored} expected={expected}"
                for address, stored, expected, _ in worst
            ),
        )
    else:
        logger.info(
            "native_balance.drift: %d sampled Safes all agree at block %d (%.2fs)",
            summary["sampled"],
            watermark,
            summary["elapsed"],
        )
    return summary


@app.shared_task(bind=True)
@task_timeout(timeout_seconds=LOCK_TIMEOUT * 2)
def check_native_balance_drift_task(self):
    """Weekly sanity check on the incremental rollup (Sundays 05:00 UTC).

    Reports only. See ``check_native_balance_drift``.
    """
    with contextlib.suppress(LockError):
        with only_one_running_task(self):
            return check_native_balance_drift()


# ───────────────── Incremental ERC-20 balance rollup ──────────────────
#
# Token-holdings spec: `docs/specs/token-holdings.md` §4.2 (workspace root).
# Same three-phase shape as the native rollup above (seed / delta / mark,
# one transaction for the last two), with two deliberate departures --
# both are consequences of `SafeTokenBalance` deleting zero-balance rows
# instead of keeping one row per Safe (see its docstring in `models.py`):
#
#   1. "No row" cannot mean "new Safe" here, because a Safe that fully
#      exits a token loses its row too. So the seed step is driven by an
#      explicit marker (`AnalyticsWatermark(name='erc20_balance_safes')`,
#      an ordered `(created, address)` pair) instead of an anti-join
#      against the rollup's own rows.
#   2. The delta step is an UPSERT, not the native delta's plain UPDATE:
#      an existing Safe picking up a token it never held before is a
#      completely ordinary event, not a race only the seed step may win.
#
# Both steps are bounded by one boundary B -- a `(created, address)` pair
# taken at the start of the run, at or before every Safe already committed
# and visible -- so a Safe the indexer inserts *while* the run is
# executing has `created >= B` and is picked up by neither step this time,
# and by both, exactly once, next time (spec edge case #10e).

ERC20_BALANCE_WATERMARK = "erc20_balance"
ERC20_BALANCE_SAFES_WATERMARK = "erc20_balance_safes"

# Sentinel high end of the 20-byte address space, paired with `now() -
# ERC20_BALANCE_SAFE_SETTLE` to build B (see `erc20_balance_run_boundary`)
# -- so a tie on `created` down to the microsecond (two rows genuinely
# created in the same instant) still resolves every *existing* address as
# `<= B`, since every real address sorts at or below `\xff * 20`.
_MAX_ADDRESS = b"\xff" * 20

# `history_safecontract.created` is `auto_now_add`, stamped by the
# indexer's Python code when it builds the row -- but the row is only
# *visible* to another connection once that indexer transaction commits.
# A plain `now()` boundary is therefore unsafe: an indexer transaction
# open when this run starts can have already stamped `created < now()` on
# a Safe that is still invisible to the candidate query, because its
# INSERT hasn't committed yet. B would then sail past it, the marker
# would move beyond it, and neither step would ever see it again -- the
# Safe is seeded never, and every later delta only adds flow *after* the
# watermark, so its pre-watermark balance is lost permanently, not just
# delayed.
#
# Backing B off by a safety margin fixes this the same way
# `native_balance_head_block`'s reorg-depth term does: it must exceed the
# longest indexer transaction can plausibly stay open, so that by the time
# a run reads `created <= B`, every transaction that could have stamped a
# `created` at or before B has committed. A Safe created inside the
# margin is simply excluded from this run and picked up by the next one --
# both steps already handle "not yet reached" Safes correctly, so this
# costs at most one run's delay, never correctness.
ERC20_BALANCE_SAFE_SETTLE = timedelta(hours=1)

# Cap and batch size are intentionally the *same* constants the native
# rollup uses (`NATIVE_BALANCE_MAX_SEED_PER_RUN`,
# `NATIVE_BALANCE_SEED_BATCH_SIZE`), not separate ERC-20 copies with the
# same value -- "reusing the native guard" per spec §4.2, and one knob to
# retune instead of two that can drift apart.

# Candidates for step 1: Safes strictly after the marker, at or below B.
#
# Marker is exclusive (`>`), not `marker <= ...` as the spec's prose reads
# -- the marker this run receives is the *exact* `(created, address)` of
# the last Safe the previous run seeded (the capped case lowers it to
# exactly that tuple; §4.2). An inclusive `>=` would make that same Safe a
# candidate again on the very next run and sum its pre-watermark history
# a second time -- since `address` is a real primary key, "equal to the
# marker" can only ever mean "is the marker's own row", never a
# coincidental tie, so there is nothing an inclusive bound would
# legitimately catch that a strict one misses.
#
# Boundary is inclusive (`<=`), not the spec's `< B` read literally -- and
# it has to be, for the capped case to be internally consistent: when the
# cap bites, B is lowered to exactly the last *seeded* Safe's own tuple
# (§4.2), and `apply_erc20_balance_delta` is given that same B to decide
# which Safes its UPSERT may touch. A strict `<` there would then exclude
# the very Safe step 1 just seeded (its own tuple is not "less than"
# itself), leaving it seeded but never delta-applied. `<=` here and `<=`
# there make one lowered B mean the same set of Safes in both steps, which
# is the actual requirement ("so both steps still agree") -- the marker's
# own strict `>` on the next run is what keeps that Safe from being
# reprocessed, so nothing is lost by not also making this bound strict.
#
# `history_safecontract.created` carries its own `db_index=True`, so the
# `>`/`<=` range and the `ORDER BY` both use it.
#
# **P7 staging finding (Berachain, 2026-09-25):** the "there is no
# composite `(created, address)` index, and there does not need to be
# one, since a whole run only ever pages a few hundred-to-thousand rows"
# claim this comment used to make was wrong at Optimism's scale
# (~3,000 chunks over 15.4M Safes, hours not minutes) — and possibly
# already wrong on Berachain (250k Safes, 50 chunks measured at a flat
# ~8.3s each regardless of how many pairs a chunk inserted, which is
# consistent with, but not proven to be, a per-chunk cost that scales
# with table size rather than chunk size; see the EXPLAIN statements in
# the P7 backfill-celery-mode report). The row-constructor form below,
# `(created, address) > (x, y)`, is NOT guaranteed to be planned as an
# index range scan against a single-column `created` btree the way a
# plain `created > x` is: Postgres *can* derive a loose `created >= x`
# index condition from a row comparison, but whether it does, and
# whether it also uses the index for the symmetric upper bound, is
# planner- and statistics-dependent, unlike a plain single-column
# inequality, which is always sargable. Decomposed into the equivalent
# OR form below (`a > x OR (a = x AND b > y)` for a NULL-free total
# order — true here because `created` is `auto_now_add` and `address` is
# the primary key, so neither column is ever NULL), the `created`
# comparisons are always plain, unambiguously sargable predicates against
# the existing single-column index, with the address tie-break staying a
# cheap residual filter on however many rows share one `created` value
# (`auto_now_add` has microsecond resolution, so genuine ties are rare —
# only from bulk inserts in the same transaction). Provably equivalent to
# the row-constructor form for both bounds — see
# `test_backfill_erc20_balances.py`'s ordering/exclusivity test. An
# actual composite `(created, address)` index would help more but
# touches an upstream table (`history_safecontract`) and is out of scope
# here — noted as an option for whoever owns that migration, not applied.
_ERC20_BALANCE_SAFE_CANDIDATES_SQL = """
SELECT address, created
FROM history_safecontract
WHERE (
        %(marker_created)s::timestamptz IS NULL
        OR created > %(marker_created)s::timestamptz
        OR (
            created = %(marker_created)s::timestamptz
            AND address > %(marker_address)s::bytea
        )
      )
  AND (
        created < %(boundary_created)s::timestamptz
        OR (
            created = %(boundary_created)s::timestamptz
            AND address <= %(boundary_address)s::bytea
        )
      )
ORDER BY created, address
LIMIT %(limit)s
"""

# Per-(Safe, token) net flow for an explicit address list, through
# `upto_block` inclusive. Same shape and the same reason as
# `_NATIVE_BALANCE_SEED_SQL`: driven by the address (`= ANY(...)`), so the
# block bound is a residual filter, not the access path. `HAVING != 0`
# mirrors `SafeTokenBalance`'s "balance = 0 is deleted" rule (§4.1) at the
# source, rather than inserting zero rows and immediately deleting them.
_ERC20_BALANCES_UPTO_BLOCK_SQL = """
    SELECT addr, token, SUM(signed_value) AS balance
    FROM (
        SELECT t."to" AS addr, t.address AS token, t.value AS signed_value
        FROM history_erc20transfer t
        WHERE t."to" = ANY(%s) AND t.block_number <= %s
        UNION ALL
        SELECT t."_from" AS addr, t.address AS token, -t.value AS signed_value
        FROM history_erc20transfer t
        WHERE t."_from" = ANY(%s) AND t.block_number <= %s
    ) transfers
    GROUP BY addr, token
    HAVING SUM(signed_value) != 0
"""

# Bulk insert of seeded pairs. `ON CONFLICT DO NOTHING` guards a retried
# run after a crash before the marker was moved forward -- see
# `seed_missing_erc20_balances`.
_ERC20_BALANCE_SEED_INSERT_SQL = """
INSERT INTO analytics_safetokenbalance (safe_address, token_address, balance)
SELECT * FROM unnest(%s::bytea[], %s::bytea[], %s::numeric[])
ON CONFLICT (safe_address, token_address) DO NOTHING
"""

# The UPSERT half of the incremental step.
#
# Driven from `history_ethereumtx.block_id` via `CROSS JOIN LATERAL`, not
# a plain `JOIN … ON t.ethereum_tx_id = etx.tx_hash`, for the same reason
# `_NATIVE_BALANCE_DELTA_SQL` is (see its comment for the measured
# Ethereum EXPLAIN): `history_erc20transfer` carries no index on
# `block_number` either, and a plain join lets the planner pick a hash
# join with a sequential scan of the whole table as the probe side. Not
# independently re-measured on this table -- if the delta turns out slow
# here, that is the finding to raise, not a detail to fix quietly.
#
# The `JOIN history_safecontract sc` (not an anti-join, not a filter on
# the pair table) is what makes the UPSERT safe despite creating rows: it
# restricts every touched address to a Safe the boundary already covers,
# so a counterparty this run's seed step has not reached yet is simply
# excluded, left for its own seed pass on a later run -- never given a
# partial row here.
#
# Deliberately **not** a single statement with a `DELETE` CTE chained onto
# this `INSERT ... RETURNING` (an earlier draft tried exactly that, "upsert
# then clean up what the upsert just wrote" in one round trip). It looked
# right and it is wrong: Postgres documents that the writable arms of a
# `WITH` "cannot see one another's effects on the target tables" -- they
# all run against the snapshot from the start of the statement. Confirmed
# against this project's own Postgres (2026-09-24): a `DELETE ... USING
# upserted u WHERE ... AND u.balance = 0` chained onto the `INSERT`
# consistently upserts the row but deletes nothing, every time, even with
# nothing else concurrent -- not a race, a documented property of
# multi-CTE writes on one table. Two statements, both inside the same
# `transaction.atomic()` block as the caller, is what makes the second one
# actually see the first one's write.
_ERC20_BALANCE_UPSERT_SQL = """
WITH delta AS (
    SELECT flows.addr AS addr, flows.token AS token, SUM(flows.signed_value) AS delta
    FROM history_ethereumtx etx
    CROSS JOIN LATERAL (
        SELECT t."to" AS addr, t.address AS token, t.value AS signed_value
        FROM history_erc20transfer t
        WHERE t.ethereum_tx_id = etx.tx_hash
        UNION ALL
        SELECT t."_from" AS addr, t.address AS token, -t.value AS signed_value
        FROM history_erc20transfer t
        WHERE t.ethereum_tx_id = etx.tx_hash
    ) flows
    JOIN history_safecontract sc ON sc.address = flows.addr
    WHERE etx.block_id > %(watermark)s AND etx.block_id <= %(head)s
      -- Inclusive (`<=`), matching `_ERC20_BALANCE_SAFE_CANDIDATES_SQL` --
      -- see that constant's comment. A capped run lowers B to exactly the
      -- last *seeded* Safe's own tuple, and this UPSERT must still be
      -- able to touch that Safe.
      AND (sc.created, sc.address) <= (%(boundary_created)s::timestamptz, %(boundary_address)s::bytea)
    GROUP BY flows.addr, flows.token
)
INSERT INTO analytics_safetokenbalance (safe_address, token_address, balance)
SELECT addr, token, delta FROM delta
ON CONFLICT (safe_address, token_address)
DO UPDATE SET balance = analytics_safetokenbalance.balance + EXCLUDED.balance
RETURNING safe_address, token_address, balance
"""

# The cleanup half: only the pairs the UPSERT above just touched, and only
# if they netted to zero -- never a full-table `WHERE balance = 0` scan.
# `unnest` of two parallel arrays is the same idiom `_NATIVE_BALANCE_SEED_INSERT_SQL`
# uses for the reverse (insert) direction.
_ERC20_BALANCE_DELETE_ZERO_SQL = """
DELETE FROM analytics_safetokenbalance b
USING unnest(%s::bytea[], %s::bytea[]) AS touched(safe_address, token_address)
WHERE b.safe_address = touched.safe_address
  AND b.token_address = touched.token_address
  AND b.balance = 0
"""

# Pair rows whose Safe no longer exists -- same cause (a reorg cascade
# through `SafeContract`) and the same "count, don't delete" handling as
# `_NATIVE_BALANCE_ORPHANS_SQL`.
_ERC20_BALANCE_ORPHANS_SQL = """
SELECT COUNT(*)
FROM analytics_safetokenbalance b
WHERE NOT EXISTS (
    SELECT 1 FROM history_safecontract sc WHERE sc.address = b.safe_address
)
"""

# `TokenHolding` is rewritten wholesale (see its docstring), not upserted:
# a token whose last holder just dropped to zero must disappear, not sit
# stale. The `EXISTS` join excludes orphan pairs from both `holders` and
# `total_balance`, same as `safes_with_any_erc20` below excludes them from
# the chain-level count.
_TOKEN_HOLDING_REBUILD_SQL = """
INSERT INTO analytics_tokenholding
    (token_address, holders, negative_pairs, total_balance,
     as_of_block, as_of_timestamp, computed_at)
SELECT
    b.token_address,
    COUNT(*) FILTER (WHERE b.balance > 0),
    COUNT(*) FILTER (WHERE b.balance < 0),
    COALESCE(SUM(b.balance) FILTER (WHERE b.balance > 0), 0),
    %(as_of_block)s,
    %(as_of_timestamp)s,
    %(computed_at)s
FROM analytics_safetokenbalance b
WHERE EXISTS (SELECT 1 FROM history_safecontract sc WHERE sc.address = b.safe_address)
GROUP BY b.token_address
"""

_SAFES_WITH_ANY_ERC20_SQL = """
SELECT COUNT(DISTINCT b.safe_address)
FROM analytics_safetokenbalance b
WHERE b.balance > 0
  AND EXISTS (SELECT 1 FROM history_safecontract sc WHERE sc.address = b.safe_address)
"""


def erc20_balance_head_block() -> int | None:
    """Highest block the ERC-20 balance rollup may consume.

    Same reorg-safety formula as ``native_balance_head_block``
    (``min(confirmed head, tip - ETH_REORG_BLOCKS)``), reusing its SQL —
    but the third term bounds on the ERC20/721 events indexer's own
    progress (``IndexingStatus(indexing_type=ERC20_721_EVENTS)``) instead
    of the master-copies indexer that writes ``InternalTx``: this rollup
    consumes ``history_erc20transfer``, which the ERC20/721 events indexer
    populates, not the trace indexer native depends on. See
    ``native_balance_head_block``'s docstring for why a third term is
    needed at all — block presence in ``EthereumBlock`` can outrun
    whichever indexer actually wrote the rows a rollup consumes.

    Returns ``None`` when nothing is safe to consume yet.
    """
    with connection.cursor() as cursor:
        cursor.execute(_NATIVE_BALANCE_HEAD_SQL)
        confirmed_head, tip = cursor.fetchone()
    if confirmed_head is None or tip is None:
        return None
    depth_head = int(tip) - settings.ETH_REORG_BLOCKS
    head = min(int(confirmed_head), depth_head)
    try:
        indexed_head = (
            IndexingStatus.objects.get_erc20_721_indexing_status().block_number
        )
    except IndexingStatus.DoesNotExist:
        indexed_head = None
    if indexed_head is not None and int(indexed_head) < head:
        logger.info(
            "erc20_balance.head: ERC20/721 events indexer is at %d, below the "
            "reorg-safe head %d — consuming only what it has written",
            indexed_head,
            head,
        )
        head = int(indexed_head)
    if head < 0:
        return None
    return head


def erc20_balance_run_boundary() -> tuple[datetime, bytes]:
    """Boundary B for one incremental run — ``(now - ERC20_BALANCE_SAFE_SETTLE,
    _MAX_ADDRESS)`` — taken once at the start (token-holdings spec §4.2).

    Backed off by ``ERC20_BALANCE_SAFE_SETTLE``, not a plain ``now()`` —
    see that constant's comment for why an uncommitted indexer transaction
    makes a plain ``now()`` boundary lose a Safe's pre-watermark balance
    permanently. Every Safe committed and visible at least
    ``ERC20_BALANCE_SAFE_SETTLE`` ago satisfies ``(created, address) <= B``
    regardless of its own address (the max-address second component
    absorbs a tie on ``created`` down to the microsecond). A Safe created
    more recently than that is excluded from both steps this run and
    picked up cleanly, once, by a later run's own boundary.

    A plain function (not a query) on purpose: unlike native, there is no
    "does at least one Safe exist" question to answer here — an empty
    ``history_safecontract`` just makes both steps below find nothing, the
    same as any other day with no new Safes.
    """
    return timezone.now() - ERC20_BALANCE_SAFE_SETTLE, _MAX_ADDRESS


def erc20_balance_seed_candidates(
    marker: tuple[datetime, bytes] | None,
    boundary: tuple[datetime, bytes],
    limit: int,
) -> list[tuple[bytes, datetime]]:
    """Up to ``limit`` Safes with ``marker < (created, address) <= boundary``,
    in ``(created, address)`` order — the Safes step 1 of one incremental
    run seeds. ``marker`` is exclusive, ``boundary`` is inclusive; see
    ``_ERC20_BALANCE_SAFE_CANDIDATES_SQL``.

    ``marker`` is ``None`` on the very first run (nothing seeded yet, so
    every Safe below ``boundary`` is a candidate). Returns ``(address,
    created)`` pairs so a caller that hits the cap can read the last one
    off the end, for lowering B, without a second query.
    """
    marker_created, marker_address = marker if marker is not None else (None, None)
    boundary_created, boundary_address = boundary
    with connection.cursor() as cursor:
        cursor.execute(
            _ERC20_BALANCE_SAFE_CANDIDATES_SQL,
            {
                "marker_created": marker_created,
                "marker_address": marker_address,
                "boundary_created": boundary_created,
                "boundary_address": boundary_address,
                "limit": limit,
            },
        )
        return [(bytes(row[0]), row[1]) for row in cursor.fetchall()]


def erc20_balances_upto_block(
    address_bytes: list[bytes], upto_block: int
) -> dict[tuple[bytes, bytes], Decimal]:
    """Every non-zero ``(Safe, token)`` balance for ``address_bytes``,
    summed over ``history_erc20transfer`` through ``upto_block`` inclusive.

    The full-history recompute both the nightly seed step and the backfill
    command need — exposed as a plain, reusable function rather than
    folded into a write path. Driven in batches of
    ``NATIVE_BALANCE_SEED_BATCH_SIZE`` addresses, the same batch size
    ``_NATIVE_BALANCE_SEED_SQL`` uses, because that is the ``= ANY(...)``
    plan it was measured on.
    """
    balances: dict[tuple[bytes, bytes], Decimal] = {}
    if not address_bytes:
        return balances
    for offset in range(0, len(address_bytes), NATIVE_BALANCE_SEED_BATCH_SIZE):
        batch = address_bytes[offset : offset + NATIVE_BALANCE_SEED_BATCH_SIZE]
        with connection.cursor() as cursor:
            cursor.execute(
                _ERC20_BALANCES_UPTO_BLOCK_SQL, [batch, upto_block, batch, upto_block]
            )
            for addr, token, balance in cursor.fetchall():
                balances[(bytes(addr), bytes(token))] = Decimal(balance)
    return balances


def seed_missing_erc20_balances(
    address_bytes: list[bytes], upto_block: int
) -> tuple[int, int]:
    """Seed non-zero ``(Safe, token)`` pairs for ``address_bytes`` at
    ``upto_block``. Returns ``(safes_processed, pairs_inserted)``.

    Unlike ``seed_missing_native_balances`` there is no "already present"
    check: a Safe reaches this function once, because the caller selects
    candidates from the ``erc20_balance_safes`` marker rather than from
    "no row exists" (§4.1 — a token pair's absence means "never held or
    fully exited", not "new Safe"). ``ON CONFLICT DO NOTHING`` still
    guards a run retried after a crash before the marker was moved.
    """
    if not address_bytes:
        return 0, 0
    balances = erc20_balances_upto_block(address_bytes, upto_block)
    if not balances:
        return len(address_bytes), 0
    safes, tokens, amounts = [], [], []
    for (safe, token), balance in balances.items():
        safes.append(safe)
        tokens.append(token)
        amounts.append(balance)
    with connection.cursor() as cursor:
        cursor.execute(_ERC20_BALANCE_SEED_INSERT_SQL, [safes, tokens, amounts])
    return len(address_bytes), len(balances)


def apply_erc20_balance_delta(
    watermark: int, head: int, boundary: tuple[datetime, bytes]
) -> tuple[int, int]:
    """Apply the net ERC-20 flow of ``(watermark, head]`` to the pair
    table, restricted to Safes ``(created, address) <= boundary``. Returns
    ``(pairs_upserted, pairs_deleted)``.

    An UPSERT, not the plain UPDATE ``_apply_native_balance_delta`` uses —
    see ``_ERC20_BALANCE_UPSERT_SQL``'s comment for why that is safe here.

    Two statements, not one — see ``_ERC20_BALANCE_UPSERT_SQL``'s comment
    for why a single multi-CTE statement silently deletes nothing here.
    Both still run inside the caller's ``transaction.atomic()``, so the
    pair is as atomic as the old single-statement version would have
    been; only the "one round trip" property is gone.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            _ERC20_BALANCE_UPSERT_SQL,
            {
                "watermark": watermark,
                "head": head,
                "boundary_created": boundary[0],
                "boundary_address": boundary[1],
            },
        )
        upserted_rows = cursor.fetchall()
        zeroed_safes = [row[0] for row in upserted_rows if row[2] == 0]
        zeroed_tokens = [row[1] for row in upserted_rows if row[2] == 0]
        deleted = 0
        if zeroed_safes:
            cursor.execute(
                _ERC20_BALANCE_DELETE_ZERO_SQL, [zeroed_safes, zeroed_tokens]
            )
            deleted = cursor.rowcount
    return len(upserted_rows), deleted


def count_erc20_balance_orphans() -> int:
    """Pair rows whose Safe no longer exists in ``history_safecontract``.
    See ``_ERC20_BALANCE_ORPHANS_SQL``.
    """
    with connection.cursor() as cursor:
        cursor.execute(_ERC20_BALANCE_ORPHANS_SQL)
        return int(cursor.fetchone()[0] or 0)


def rebuild_token_holdings(as_of_block: int, as_of_timestamp, computed_at) -> dict:
    """Rewrite ``TokenHolding`` wholesale from ``SafeTokenBalance`` and
    return the chain-level counts for ``AnalyticsSnapshot(name=
    'token_holdings')``: ``{tokens_with_holders, safes_with_any_erc20,
    negative_pairs_total, orphan_pairs}``.

    A full delete + insert, not an upsert — see ``_TOKEN_HOLDING_REBUILD_SQL``.
    Must run inside the same transaction as ``apply_erc20_balance_delta``
    and the watermark writes (§4.1: "written in the same transaction as
    TokenHolding").
    """
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM analytics_tokenholding")
        cursor.execute(
            _TOKEN_HOLDING_REBUILD_SQL,
            {
                "as_of_block": as_of_block,
                "as_of_timestamp": as_of_timestamp,
                "computed_at": computed_at,
            },
        )
        cursor.execute("SELECT COUNT(*) FROM analytics_tokenholding WHERE holders > 0")
        tokens_with_holders = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COALESCE(SUM(negative_pairs), 0) FROM analytics_tokenholding"
        )
        negative_pairs_total = cursor.fetchone()[0]
        cursor.execute(_SAFES_WITH_ANY_ERC20_SQL)
        safes_with_any_erc20 = cursor.fetchone()[0]
    return {
        "tokens_with_holders": int(tokens_with_holders or 0),
        "safes_with_any_erc20": int(safes_with_any_erc20 or 0),
        "negative_pairs_total": int(negative_pairs_total or 0),
        "orphan_pairs": count_erc20_balance_orphans(),
    }


def run_erc20_balance_rollup() -> dict | None:
    """One incremental pass over the ERC-20 balance rollup — seed new
    Safes, apply the delta, rebuild the read model. See the token-holdings
    spec §4.2.

    Unlike ``run_native_balance_rollup`` this never cold-starts itself:
    the (Safe, token) pair space has no bound small enough to trust
    seeding inline the way a handful of native rows is. A missing
    ``erc20_balance`` watermark always means "run ``manage.py
    backfill_erc20_balances`` once" — see the log line below.

    Returns a summary dict, or ``None`` when the run declined to do
    anything. Every declining path logs; the ones that mean something is
    wrong log at ERROR, mirroring ``run_native_balance_rollup``.
    """
    started = time.time()
    head = erc20_balance_head_block()
    if head is None:
        logger.info(
            "erc20_balance.rollup: no confirmed block within the reorg depth "
            "yet; nothing to consume"
        )
        return None

    watermark_row = AnalyticsWatermark.objects.filter(
        name=ERC20_BALANCE_WATERMARK
    ).first()
    if watermark_row is None:
        logger.info(
            "erc20_balance.rollup: no '%s' watermark yet. Run `manage.py "
            "backfill_erc20_balances` once to initialise the rollup — unlike "
            "native, this task never seeds a fleet from cold: the (Safe, "
            "token) pair space has no bound small enough to trust inline.",
            ERC20_BALANCE_WATERMARK,
        )
        return None

    watermark = watermark_row.block_number
    if watermark > head:
        logger.error(
            "erc20_balance.rollup: watermark=%d is ahead of the safe head=%d. "
            "Blocks this rollup already applied have been removed — a reorg "
            "deeper than the confirmation zone, or a database restore — and "
            "the rows needed to undo them went with them. Refusing to run; "
            "rebuild with `manage.py backfill_erc20_balances --restart`.",
            watermark,
            head,
        )
        return None

    boundary = erc20_balance_run_boundary()

    marker_row = AnalyticsWatermark.objects.filter(
        name=ERC20_BALANCE_SAFES_WATERMARK
    ).first()
    # `AnalyticsWatermark.address` is an `EthereumAddressBinaryField`: the
    # ORM hands back a checksummed hex *string* (`from_db_value`), not raw
    # bytes — `HexBytes(...)`, not `bytes(...)`, is what turns that back
    # into the 20-byte value the raw-SQL helpers below compare against
    # `bytea` columns.
    marker = (
        (marker_row.computed_at, HexBytes(marker_row.address))
        if marker_row is not None and marker_row.address is not None
        else None
    )

    with relaxed_statement_timeout():
        candidates = erc20_balance_seed_candidates(
            marker, boundary, NATIVE_BALANCE_MAX_SEED_PER_RUN + 1
        )
        capped = len(candidates) > NATIVE_BALANCE_MAX_SEED_PER_RUN
        if capped:
            candidates = candidates[:NATIVE_BALANCE_MAX_SEED_PER_RUN]
            last_address, last_created = candidates[-1]
            new_boundary = (last_created, last_address)
            logger.info(
                "erc20_balance.rollup: more new Safes than the %d-per-run "
                "seed cap; lowering this run's boundary to (%s, 0x%s) so the "
                "delta step agrees with what was actually seeded",
                NATIVE_BALANCE_MAX_SEED_PER_RUN,
                last_created,
                last_address.hex(),
            )
        else:
            new_boundary = boundary

        seed_addresses = [addr for addr, _created in candidates]
        # Seeded at the OLD watermark, deliberately, and outside the
        # transaction below — seeding is idempotent (`DO NOTHING`), so a
        # failure after it leaves rows the next run simply finds already
        # present, same reasoning as the native seed step.
        seeded_safes, seeded_pairs = seed_missing_erc20_balances(
            seed_addresses, watermark
        )

        with transaction.atomic():
            touched, deleted = apply_erc20_balance_delta(watermark, head, new_boundary)
            now = timezone.now()
            AnalyticsWatermark.objects.update_or_create(
                name=ERC20_BALANCE_WATERMARK,
                defaults={"block_number": head, "computed_at": now},
            )
            AnalyticsWatermark.objects.update_or_create(
                name=ERC20_BALANCE_SAFES_WATERMARK,
                defaults={
                    "block_number": head,
                    "computed_at": new_boundary[0],
                    "address": new_boundary[1],
                },
            )
            as_of_timestamp = (
                EthereumBlock.objects.filter(number=head)
                .values_list("timestamp", flat=True)
                .first()
                or now
            )
            chain_level = rebuild_token_holdings(head, as_of_timestamp, now)
            _write_snapshot(
                "token_holdings",
                {
                    "as_of_block": head,
                    "as_of_timestamp": as_of_timestamp.isoformat(),
                    **chain_level,
                },
            )

    summary = {
        "watermark_from": watermark,
        "watermark_to": head,
        "blocks": head - watermark,
        "seeded_safes": seeded_safes,
        "seeded_pairs": seeded_pairs,
        "touched_pairs": touched,
        "deleted_pairs": deleted,
        **chain_level,
    }
    logger.info(
        "erc20_balance.rollup: completed in %.2fs blocks=(%d, %d] "
        "seeded_safes=%d seeded_pairs=%d touched=%d deleted=%d "
        "tokens_with_holders=%d safes_with_any_erc20=%d orphan_pairs=%d",
        time.time() - started,
        watermark,
        head,
        seeded_safes,
        seeded_pairs,
        touched,
        deleted,
        chain_level["tokens_with_holders"],
        chain_level["safes_with_any_erc20"],
        chain_level["orphan_pairs"],
    )
    return summary


@app.shared_task(bind=True)
@task_timeout(timeout_seconds=LOCK_TIMEOUT)
def compute_erc20_balance_rollup_task(self):
    """Advance the incremental ERC-20 balance rollup (daily at 03:45 UTC,
    after ``compute_tvl_task``).

    Lives in ``tasks.py`` for the same Celery-routing reason as
    ``compute_native_balance_rollup_task`` — see its docstring.
    """
    with contextlib.suppress(LockError):
        with only_one_running_task(self):
            return run_erc20_balance_rollup()


# ───────────── ERC-20 balance backfill, chunked on Celery (P7) ─────────
#
# `manage.py backfill_erc20_balances --celery` for when the run should
# outlive the shell that started it (token-holdings spec §4.3, §12 P7) —
# the same operator problem `backfill_native_balance_chunk`
# (`tasks_shards.py`) and `backfill_daily_metrics`'s chunked mode solve,
# with two departures forced by this rollup's own design:
#
#   1. **Resumability lives in the `erc20_balance_backfill*` watermark
#      rows, not a Redis cursor.** Those three rows
#      (`erc20_balance_backfill`, `erc20_balance_backfill_boundary`,
#      `erc20_balance_backfill_whales` — see
#      `analytics/management/commands/backfill_erc20_balances.py`'s module
#      docstring) are already the resume journal `--inline` mode writes.
#      Reusing them as the *only* source of truth means inline and Celery
#      modes can interleave freely: an inline run interrupted by Ctrl-C
#      resumes fine under `--celery`, and a Celery run a worker lost
#      resumes fine under `--inline`. The Redis manifest below exists
#      purely for `--status` / `--wait` / refusing a second concurrent
#      `--celery` — it is never read to decide what work is left, only to
#      report it.
#   2. **Every slice holds the shared rollup lock itself, for the
#      duration of that slice only**, released before the next task is
#      dispatched — not held for the whole run the way `--inline` holds
#      it once, nor never taken at all the way
#      `backfill_native_balance_chunk` runs lock-free. `SafeTokenBalance`
#      deletes zero-balance rows and the nightly rollup UPSERTs into the
#      same table (spec edge case #10b), so a chunk-phase or whale-walk
#      slice must never overlap the nightly task's delta step. Between
#      slices the nightly task may take the lock; that is safe, because
#      `run_erc20_balance_rollup` is a no-op until the `erc20_balance`
#      watermark exists, and that is only written by the finish step.
#      A slice that cannot get the lock (the nightly task or another
#      writer has it) retries itself after a short countdown rather than
#      failing the run — there is no existing lock-contention precedent
#      in either reference backfill to mirror (native's chain does not
#      lock at all; see the command module's own note on this deviation).
#
# Shape: `backfill_erc20_balance_chunk_task` runs up to `task_chunks` Safe
# chunks (`run_chunk_slice`, plain-function, shared with `--inline`) and
# dispatches its own successor, exactly like `backfill_native_balance_chunk`
# — until seeding is done, when it hands off to
# `backfill_erc20_balance_whale_task` (up to `task_chunks` whale
# block-ranges per task, `run_whale_slice`), which itself hands off to
# `backfill_erc20_balance_finish_task` (one-shot: writes the real
# `erc20_balance` / `erc20_balance_safes` watermarks and rebuilds
# `TokenHolding`, `finish_run`). Each task is wrapped in
# `@task_timeout(timeout_seconds=LOCK_TIMEOUT)`, the same bound
# `backfill_native_balance_chunk` uses — `task_chunks`'s default is picked
# so a slice's total work comfortably clears it even on a slow chunk (see
# `DEFAULT_TASK_CHUNKS`'s comment in the command module).
#
# **Crash safety (P7 staging addendum, 2026-09-25).** Optimism's ~3,000
# chunks mean this chain runs for hours — long enough to outlast at least
# one ordinary worker restart. Two layers, neither sufficient alone:
#
#   1. `acks_late=True, reject_on_worker_lost=True` on all three tasks
#      (no precedent elsewhere in this codebase — `grep` for
#      `acks_late` before this change found none — but safe to add here
#      specifically because a task's *only* argument is `run_id`; all
#      progress state is read fresh from the DB / manifest at call time,
#      never baked into the task signature the way a keyset cursor would
#      be. A redelivered (possibly duplicate, possibly concurrent) call
#      just re-enters `run_chunk_slice` / `run_whale_slice`, which reads
#      wherever the watermarks currently are and continues forward from
#      there — it cannot rewind and double-apply an already-committed
#      range. Concurrent redeliveries still serialize on the shared
#      rollup lock, so two copies never do DB work at the same instant
#      either. What this does NOT cover: `CELERY_ROUTES` sets
#      `delivery_mode: "transient"` for the `contracts` queue (not
#      changed here — that is a broker-wide, pre-existing choice, not
#      specific to this chain), so a *broker* restart, as opposed to a
#      *worker* restart, still loses an in-flight message outright. Layer
#      2 below is what recovers from that.
#   2. A heartbeat on the run manifest (`heartbeat_at`, refreshed by
#      every `_save_erc20_balance_backfill_run` call — i.e. on every
#      slice's start, success and failure) plus
#      `erc20_balance_backfill_watchdog_task`, a beat task (every 5
#      minutes, `setup_service.py`) that re-dispatches the chain when the
#      progress watermarks say a run is mid-flight, its manifest is still
#      `"running"`, the heartbeat has gone stale
#      (`ERC20_BALANCE_BACKFILL_STALE_SECONDS`), and the shared lock is
#      free right now (proof nothing legitimate is in flight this
#      instant). See `erc20_balance_backfill_looks_stalled` and
#      `erc20_balance_backfill_watchdog_task` below.

ERC20_BALANCE_BACKFILL_RUN_KEY_PREFIX = "analytics_erc20_balance_backfill_run:"
ERC20_BALANCE_BACKFILL_CURSOR_KEY = "analytics_erc20_balance_backfill_cursor"

# How long a slice that lost the lock race waits before trying again. Not
# mirrored from either reference backfill (see the block comment above) —
# short enough that a run does not visibly stall behind one nightly-task
# cycle, long enough not to hammer Redis if the lock is held for a while.
ERC20_BALANCE_BACKFILL_LOCK_RETRY_COUNTDOWN = 15

# How stale `heartbeat_at` must be before the watchdog treats a
# "running" manifest as stalled rather than merely between slices. The
# architect's own figure (P7 addendum, 2026-09-25): long enough that an
# ordinary slice (normally well under a minute; see `DEFAULT_TASK_CHUNKS`)
# never trips it, short enough that a genuine stall does not sit silent
# for hours on an Optimism-sized run.
ERC20_BALANCE_BACKFILL_STALE_SECONDS = 15 * 60


def erc20_balance_backfill_run_key(run_id: str) -> str:
    return f"{ERC20_BALANCE_BACKFILL_RUN_KEY_PREFIX}{run_id}"


def _erc20_backfill_redis_set_json(key: str, value: dict) -> None:
    get_redis().set(key, json.dumps(value), ex=tasks_shards.BACKFILL_KEY_TTL_SECONDS)


def _erc20_backfill_redis_get_json(key: str) -> dict | None:
    blob = get_redis().get(key)
    if not blob:
        return None
    try:
        return json.loads(blob)
    except (TypeError, ValueError):
        logger.warning("erc20_balance.backfill: unreadable JSON at redis key %s", key)
        return None


def load_erc20_balance_backfill_run(run_id: str) -> dict | None:
    """Return the run manifest for ``run_id``, or ``None`` if unknown/expired."""
    return _erc20_backfill_redis_get_json(erc20_balance_backfill_run_key(run_id))


def latest_erc20_balance_backfill_run_id() -> str | None:
    pointer = _erc20_backfill_redis_get_json(ERC20_BALANCE_BACKFILL_CURSOR_KEY)
    if pointer and isinstance(pointer.get("run_id"), str):
        return pointer["run_id"]
    return None


def _save_erc20_balance_backfill_run(run: dict) -> None:
    """Persist the manifest and refresh its heartbeat in the same write.
    Every meaningful state change (slice start, success, failure, finish)
    already calls this, so touching `heartbeat_at` here — rather than at
    each of those call sites individually — is what makes the watchdog's
    staleness check (`erc20_balance_backfill_looks_stalled`) correct by
    construction instead of by remembering to call a separate `_touch()`
    everywhere.
    """
    run["heartbeat_at"] = timezone.now().isoformat()
    _erc20_backfill_redis_set_json(run["run_key"], run)


def _erc20_backfill_options_from_run(run: dict) -> dict:
    """Reconstruct the ``options`` dict ``run_chunk_slice`` / ``run_whale_slice``
    expect, from the fields the manifest was built with."""
    return {
        "chunk_size": run["chunk_size"],
        "whale_min_frequency": run["whale_min_frequency"],
        "whale_row_threshold": run["whale_row_threshold"],
        "whale_block_range": run["whale_block_range"],
        "statement_timeout_ms": run["statement_timeout_ms"],
    }


def build_erc20_balance_backfill_run(
    *,
    chunk_size: int,
    whale_min_frequency: float,
    whale_row_threshold: int,
    whale_block_range: int,
    statement_timeout_ms: int,
    task_chunks: int,
    run_id: str | None = None,
) -> dict:
    """Pure helper: a fresh manifest. Nothing is written or dispatched —
    see ``dispatch_erc20_balance_backfill_run``."""
    run_id = run_id or tasks_shards.new_backfill_run_id()
    return {
        "run_id": run_id,
        "run_key": erc20_balance_backfill_run_key(run_id),
        "phase": "chunks",
        "state": "running",
        "started_at": timezone.now().isoformat(),
        "finished_at": None,
        "error": None,
        "chunk_size": chunk_size,
        "whale_min_frequency": whale_min_frequency,
        "whale_row_threshold": whale_row_threshold,
        "whale_block_range": whale_block_range,
        "statement_timeout_ms": statement_timeout_ms,
        "task_chunks": task_chunks,
        "slices_done": 0,
        "chunks_done": 0,
        "safes_seen": 0,
        "safes_seeded": 0,
        "pairs_touched": 0,
        "whale_skipped": 0,
        "whale_ranges_done": 0,
        "whale_safes_summed": 0,
        "head": None,
    }


def dispatch_erc20_balance_backfill_run(run: dict) -> dict:
    """Persist a fresh manifest, point the cursor key at it and dispatch
    the first chunk-phase task. Later slices dispatch themselves on the
    worker, so the caller (the management command) may exit immediately.
    """
    _save_erc20_balance_backfill_run(run)
    _erc20_backfill_redis_set_json(
        ERC20_BALANCE_BACKFILL_CURSOR_KEY,
        {
            "run_id": run["run_id"],
            "run_key": run["run_key"],
            "started_at": run["started_at"],
        },
    )
    backfill_erc20_balance_chunk_task.apply_async((run["run_id"],), queue="contracts")
    return load_erc20_balance_backfill_run(run["run_id"]) or run


@app.shared_task(acks_late=True, reject_on_worker_lost=True)
@task_timeout(timeout_seconds=LOCK_TIMEOUT)
def backfill_erc20_balance_chunk_task(run_id: str) -> dict:
    """One bounded slice (up to ``task_chunks`` Safe chunks) of the
    backfill's seed phase, then dispatch the next slice — or the
    whale-walk phase once seeding is done. See the block comment above
    ``ERC20_BALANCE_BACKFILL_RUN_KEY_PREFIX`` for the two departures from
    ``backfill_native_balance_chunk``'s shape.
    """
    from safe_transaction_service.analytics.management.commands.backfill_erc20_balances import (
        run_chunk_slice,
    )

    run = load_erc20_balance_backfill_run(run_id)
    if run is None:
        logger.warning(
            "erc20_balance.backfill: run=%s manifest is gone (expired or "
            "flushed); stopping the chain",
            run_id,
        )
        return {"run_id": run_id, "state": "unknown"}
    if run.get("state") != "running":
        logger.info(
            "erc20_balance.backfill: run=%s is %s, not dispatching further chunks",
            run_id,
            run.get("state"),
        )
        return run

    lock = get_redis().lock(
        get_task_lock_name(compute_erc20_balance_rollup_task.name),
        blocking=False,
        timeout=LOCK_TIMEOUT,
    )
    if not lock.acquire(blocking=False):
        logger.info(
            "erc20_balance.backfill: run=%s chunk slice could not get the "
            "rollup lock (nightly task or another writer has it); "
            "retrying in %ds",
            run_id,
            ERC20_BALANCE_BACKFILL_LOCK_RETRY_COUNTDOWN,
        )
        backfill_erc20_balance_chunk_task.apply_async(
            (run_id,),
            queue="contracts",
            countdown=ERC20_BALANCE_BACKFILL_LOCK_RETRY_COUNTDOWN,
        )
        return run

    started = time.time()
    try:
        try:
            result = run_chunk_slice(
                _erc20_backfill_options_from_run(run),
                lock,
                max_chunks=run["task_chunks"],
            )
        finally:
            lock.release()
    except Exception as exc:
        logger.exception("erc20_balance.backfill: run=%s chunk slice failed", run_id)
        run["state"] = "failed"
        run["error"] = str(exc)[:500]
        run["finished_at"] = timezone.now().isoformat()
        _save_erc20_balance_backfill_run(run)
        return run

    run["slices_done"] += 1
    run["chunks_done"] += result["chunks_done"]
    run["safes_seen"] += result["seen"]
    run["safes_seeded"] += result["seeded_safes"]
    run["pairs_touched"] += result["pairs_inserted"]
    run["whale_skipped"] += result["whale_skipped"]
    run["head"] = result["head"]
    # Persisted BEFORE the dispatch below and never after it — same reason
    # as `backfill_native_balance_chunk`'s own comment: under eager mode
    # the rest of the chain runs inside `apply_async`, so a write
    # afterwards would clobber newer state with this stale copy.
    _save_erc20_balance_backfill_run(run)

    logger.info(
        "erc20_balance.backfill: run=%s chunk slice done in %.2fs "
        "chunks=%d seen=%d seeded=%d whale_skipped=%d finished_seeding=%s",
        run_id,
        time.time() - started,
        result["chunks_done"],
        result["seen"],
        result["seeded_safes"],
        result["whale_skipped"],
        result["finished_seeding"],
    )

    if result["finished_seeding"]:
        run["phase"] = "whales"
        _save_erc20_balance_backfill_run(run)
        backfill_erc20_balance_whale_task.apply_async((run_id,), queue="contracts")
    else:
        backfill_erc20_balance_chunk_task.apply_async((run_id,), queue="contracts")
    return run


@app.shared_task(acks_late=True, reject_on_worker_lost=True)
@task_timeout(timeout_seconds=LOCK_TIMEOUT)
def backfill_erc20_balance_whale_task(run_id: str) -> dict:
    """One bounded slice (up to ``task_chunks`` whale block-ranges) of the
    backfill's whale-walk phase, then dispatch the next slice — or the
    finish step once the walk covers ``(0, head]``. The whale address set
    is recomputed at the start of every slice from ``Erc20BalanceWhale``
    and the boundary (``whale_addresses_upto_boundary``) — safe to do on
    every invocation by construction; see that function's docstring.
    """
    from safe_transaction_service.analytics.management.commands.backfill_erc20_balances import (
        read_boundary,
        read_head,
        run_whale_slice,
        whale_addresses_upto_boundary,
    )

    run = load_erc20_balance_backfill_run(run_id)
    if run is None:
        logger.warning(
            "erc20_balance.backfill: run=%s manifest is gone; stopping "
            "before the whale walk",
            run_id,
        )
        return {"run_id": run_id, "state": "unknown"}
    if run.get("state") != "running":
        logger.info(
            "erc20_balance.backfill: run=%s is %s, not continuing the whale walk",
            run_id,
            run.get("state"),
        )
        return run

    lock = get_redis().lock(
        get_task_lock_name(compute_erc20_balance_rollup_task.name),
        blocking=False,
        timeout=LOCK_TIMEOUT,
    )
    if not lock.acquire(blocking=False):
        logger.info(
            "erc20_balance.backfill: run=%s whale slice could not get the "
            "rollup lock; retrying in %ds",
            run_id,
            ERC20_BALANCE_BACKFILL_LOCK_RETRY_COUNTDOWN,
        )
        backfill_erc20_balance_whale_task.apply_async(
            (run_id,),
            queue="contracts",
            countdown=ERC20_BALANCE_BACKFILL_LOCK_RETRY_COUNTDOWN,
        )
        return run

    started = time.time()
    whale_addresses: list = []
    try:
        try:
            boundary = read_boundary()
            head = read_head()
            whale_addresses = whale_addresses_upto_boundary(boundary)
            result = run_whale_slice(
                whale_addresses,
                head,
                _erc20_backfill_options_from_run(run),
                lock,
                max_ranges=run["task_chunks"],
            )
        finally:
            lock.release()
    except Exception as exc:
        logger.exception("erc20_balance.backfill: run=%s whale slice failed", run_id)
        run["state"] = "failed"
        run["error"] = str(exc)[:500]
        run["finished_at"] = timezone.now().isoformat()
        _save_erc20_balance_backfill_run(run)
        return run

    run["whale_ranges_done"] += result["ranges_done"]
    run["pairs_touched"] += result["rows_touched"]
    if result["finished"]:
        run["whale_safes_summed"] = len(whale_addresses)
    _save_erc20_balance_backfill_run(run)

    logger.info(
        "erc20_balance.backfill: run=%s whale slice done in %.2fs "
        "ranges=%d rows_touched=%d finished=%s",
        run_id,
        time.time() - started,
        result["ranges_done"],
        result["rows_touched"],
        result["finished"],
    )

    if result["finished"]:
        run["phase"] = "finish"
        _save_erc20_balance_backfill_run(run)
        backfill_erc20_balance_finish_task.apply_async((run_id,), queue="contracts")
    else:
        backfill_erc20_balance_whale_task.apply_async((run_id,), queue="contracts")
    return run


@app.shared_task(acks_late=True, reject_on_worker_lost=True)
@task_timeout(timeout_seconds=LOCK_TIMEOUT)
def backfill_erc20_balance_finish_task(run_id: str) -> dict:
    """One-shot: write the real ``erc20_balance`` / ``erc20_balance_safes``
    watermarks and rebuild ``TokenHolding`` (``finish_run``), then mark the
    manifest finished. Runs once the chunk and whale-walk phases are both
    done — see ``backfill_erc20_balance_whale_task``.
    """
    from safe_transaction_service.analytics.management.commands.backfill_erc20_balances import (
        finish_run,
        read_boundary,
        read_head,
    )

    run = load_erc20_balance_backfill_run(run_id)
    if run is None:
        logger.warning(
            "erc20_balance.backfill: run=%s manifest is gone; the run may "
            "have finished without recording it — check the watermarks by "
            "hand",
            run_id,
        )
        return {"run_id": run_id, "state": "unknown"}
    if run.get("state") != "running":
        logger.info(
            "erc20_balance.backfill: run=%s is %s, not finishing again",
            run_id,
            run.get("state"),
        )
        return run

    lock = get_redis().lock(
        get_task_lock_name(compute_erc20_balance_rollup_task.name),
        blocking=False,
        timeout=LOCK_TIMEOUT,
    )
    if not lock.acquire(blocking=False):
        logger.info(
            "erc20_balance.backfill: run=%s finish step could not get the "
            "rollup lock; retrying in %ds",
            run_id,
            ERC20_BALANCE_BACKFILL_LOCK_RETRY_COUNTDOWN,
        )
        backfill_erc20_balance_finish_task.apply_async(
            (run_id,),
            queue="contracts",
            countdown=ERC20_BALANCE_BACKFILL_LOCK_RETRY_COUNTDOWN,
        )
        return run

    try:
        try:
            # Read BEFORE `finish_run`, which deletes both progress rows
            # (`_PROGRESS_WATERMARK`, `_BOUNDARY_WATERMARK`) as part of its
            # own transaction — reading them after would find nothing.
            head = read_head()
            boundary = read_boundary()
            chain_level = finish_run(head, boundary)
        finally:
            lock.release()
    except Exception as exc:
        logger.exception("erc20_balance.backfill: run=%s finish step failed", run_id)
        run["state"] = "failed"
        run["error"] = str(exc)[:500]
        run["finished_at"] = timezone.now().isoformat()
        _save_erc20_balance_backfill_run(run)
        return run

    run["state"] = "finished"
    run["phase"] = "done"
    run["finished_at"] = timezone.now().isoformat()
    run["head"] = head
    run["tokens_with_holders"] = chain_level["tokens_with_holders"]
    run["safes_with_any_erc20"] = chain_level["safes_with_any_erc20"]
    _save_erc20_balance_backfill_run(run)

    logger.info(
        "erc20_balance.backfill: run=%s FINISHED head=%d "
        "tokens_with_holders=%d safes_with_any_erc20=%d",
        run_id,
        head,
        chain_level["tokens_with_holders"],
        chain_level["safes_with_any_erc20"],
    )
    return run


def _erc20_backfill_dispatch_phase(run: dict) -> None:
    """Re-enter the chain at whatever phase the manifest currently says,
    without creating a new run. Used by the watchdog's resume — the same
    "read `phase`, dispatch that task" step a legitimate hand-off between
    phases already does inline (see the three tasks above), just driven
    from outside the chain instead of from the end of a slice.
    """
    phase = run.get("phase")
    if phase == "whales":
        backfill_erc20_balance_whale_task.apply_async(
            (run["run_id"],), queue="contracts"
        )
    elif phase == "finish":
        backfill_erc20_balance_finish_task.apply_async(
            (run["run_id"],), queue="contracts"
        )
    else:
        backfill_erc20_balance_chunk_task.apply_async(
            (run["run_id"],), queue="contracts"
        )


def erc20_balance_backfill_looks_stalled() -> dict | None:
    """Pure check, no side effects (does not touch the lock): ``None``
    when nothing needs re-dispatching, else the run manifest the watchdog
    should resume. Split out from the task so `--status`
    (`backfill_erc20_balances.py`'s `_print_status`) can show the same
    verdict the watchdog would act on.

    All three conditions below are required, so a healthy run, an
    inline-mode run (which has no self-dispatch to resume — the
    operator's own shell holds that lock, not a stalled chain), or an
    already-finished/failed one is never flagged:

    1. A backfill is genuinely mid-run: `_PROGRESS_WATERMARK` exists and
       `ERC20_BALANCE_WATERMARK` (written only by `finish_run`) does not.
    2. Its Celery run manifest exists, is `state == "running"`, and
       carries a `heartbeat_at`.
    3. That heartbeat is older than `ERC20_BALANCE_BACKFILL_STALE_SECONDS`.

    Deliberately does NOT check the shared lock here — that check has a
    side effect (acquire-then-release) that belongs in the task, once,
    right before it decides to act, not in a read-only helper `--status`
    also calls on every invocation.
    """
    from safe_transaction_service.analytics.management.commands.backfill_erc20_balances import (
        _PROGRESS_WATERMARK,
    )

    if AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK).exists():
        return None
    if not AnalyticsWatermark.objects.filter(name=_PROGRESS_WATERMARK).exists():
        return None

    run_id = latest_erc20_balance_backfill_run_id()
    if run_id is None:
        return None
    run = load_erc20_balance_backfill_run(run_id)
    if run is None or run.get("state") != "running":
        return None

    heartbeat_at = run.get("heartbeat_at")
    if not heartbeat_at:
        return None
    try:
        age = (timezone.now() - datetime.fromisoformat(heartbeat_at)).total_seconds()
    except ValueError:
        return None
    if age < ERC20_BALANCE_BACKFILL_STALE_SECONDS:
        return None
    return run


@app.shared_task(
    bind=True,
    name="safe_transaction_service.analytics.tasks.erc20_balance_backfill_watchdog_task",
)
@task_timeout(timeout_seconds=LOCK_TIMEOUT)
def erc20_balance_backfill_watchdog_task(self) -> None:
    """Beat task (every 5 minutes, `setup_service.py`): re-dispatch a
    `backfill_erc20_balances --celery` chain that has stopped advancing —
    e.g. because the worker holding its last slice was restarted (or, per
    the module comment above `ERC20_BALANCE_BACKFILL_RUN_KEY_PREFIX`, the
    broker itself was, which `acks_late` alone does not cover) before the
    self-dispatch of the next slice ran. P7 staging note (Berachain,
    2026-09-25): Optimism's ~3,000 chunks mean the chain runs for hours,
    long enough to outlast at least one ordinary deploy.

    `erc20_balance_backfill_looks_stalled` decides WHETHER to act; this
    task adds the one check that belongs at the point of action rather
    than in a read-only helper: the shared rollup lock must be free RIGHT
    NOW. Held means genuine work (a slice, the nightly task, or a
    concurrent `--inline` run) is in flight, not a stall — try again next
    beat rather than race it. Never starts a second concurrent chain: it
    dispatches by `run_id` from the existing manifest's own `phase`
    (`_erc20_backfill_dispatch_phase`), the same re-entry point a normal
    slice-to-slice hand-off uses, and that task will itself take the lock
    before touching anything.
    """
    if not settings.ENABLE_ANALYTICS:
        return

    run = erc20_balance_backfill_looks_stalled()
    if run is None:
        return

    lock = get_redis().lock(
        get_task_lock_name(compute_erc20_balance_rollup_task.name),
        blocking=False,
        timeout=LOCK_TIMEOUT,
    )
    if not lock.acquire(blocking=False):
        logger.info(
            "erc20_balance.backfill.watchdog: run=%s heartbeat looks "
            "stale but the rollup lock is held (genuine work in flight); "
            "leaving it alone this cycle",
            run["run_id"],
        )
        return
    # Immediately release: this task only decides whether to redispatch
    # and does no work of its own. Holding it any longer would just delay
    # the slice task's own acquire for no benefit.
    lock.release()

    logger.warning(
        "erc20_balance.backfill.watchdog: run=%s heartbeat older than "
        "%ds (last seen %s), rollup lock free -- redispatching phase=%s",
        run["run_id"],
        ERC20_BALANCE_BACKFILL_STALE_SECONDS,
        run.get("heartbeat_at"),
        run.get("phase"),
    )
    _erc20_backfill_dispatch_phase(run)


# ─────────────── ERC-20 balance rollup drift check ────────────────
#
# Same rationale as the native drift check above: an incrementally
# maintained running total has no self-healing property, so a batch
# applied twice (or a seed step that silently missed a pair) stays wrong
# forever and looks exactly like a real number. Token-holdings spec §4.4.
#
# Two departures from the native version, both consequences of
# `SafeTokenBalance` deleting zero-balance rows instead of keeping one
# row per pair (see that model's docstring):
#
#   1. The recompute is per-*Safe*, not per sampled pair.
#      `erc20_balances_upto_block` already returns every non-zero
#      (Safe, token) balance for the addresses it is given, so calling it
#      with the *sampled Safes'* addresses -- not the sampled
#      (Safe, token) pairs -- recomputes each Safe's whole token set for
#      free. That is deliberate, not incidental: a pair the rollup should
#      have created but never did has no row to land in the sample in the
#      first place, so sampling stored *pairs* alone could never surface
#      it. Comparing the recomputed set against every row this check can
#      see for the same Safes (not only the sampled row) is what catches
#      a missing pair, the same way a stored pair the recompute no longer
#      sees (an un-deleted zero) is caught by the opposite direction of
#      the same comparison.
#   2. Whale Safes (`Erc20BalanceWhale`) are excluded from the sample --
#      recomputing one of them (tens of millions of transfer rows) would
#      blow the statement timeout on its own (§4.3, §4.4).

ERC20_BALANCE_DRIFT_SAMPLE_SIZE = 2000

# Same `ORDER BY random()` trade-off as `_NATIVE_BALANCE_SAMPLE_SQL` --
# see its comment. Whale Safes are filtered out of the sampling pool
# itself, not out of the Python-side result, so `LIMIT` is still filled
# entirely from non-whale rows.
_ERC20_BALANCE_SAMPLE_SQL = """
SELECT b.safe_address, b.token_address, b.balance
FROM analytics_safetokenbalance b
WHERE NOT EXISTS (
    SELECT 1 FROM analytics_erc20balancewhale w
    WHERE w.safe_address = b.safe_address
)
ORDER BY random()
LIMIT %s
"""

# Every currently-stored pair for a given set of (already sampled,
# non-whale) Safes -- the counterpart `erc20_balances_upto_block` is
# compared against. Not restricted to the sampled token: see departure 1
# above.
_ERC20_BALANCE_EXISTING_FOR_SAFES_SQL = """
SELECT safe_address, token_address, balance
FROM analytics_safetokenbalance
WHERE safe_address = ANY(%s)
"""

_ERC20_BALANCE_NEGATIVE_PAIRS_SQL = """
SELECT COUNT(*) FROM analytics_safetokenbalance WHERE balance < 0
"""


def check_erc20_balance_drift(
    sample_size: int = ERC20_BALANCE_DRIFT_SAMPLE_SIZE,
) -> dict | None:
    """Compare a random sample of ``SafeTokenBalance`` rows -- and every
    other pair the same Safes hold -- against a from-scratch recompute at
    the watermark. Returns a summary, or ``None`` when there was nothing
    to check.

    Report-only, like ``check_native_balance_drift``: repair only ever
    happens through ``manage.py backfill_erc20_balances --restart``
    (§4.4) -- auto-repairing a signed running total from a sample is
    unsafe.
    """
    started = time.time()
    watermark_row = AnalyticsWatermark.objects.filter(
        name=ERC20_BALANCE_WATERMARK
    ).first()
    if watermark_row is None:
        logger.info("erc20_balance.drift: rollup not initialised, nothing to check")
        return None
    watermark = watermark_row.block_number

    with relaxed_statement_timeout():
        with connection.cursor() as cursor:
            cursor.execute(_ERC20_BALANCE_SAMPLE_SQL, [sample_size])
            sample_rows = [
                (bytes(safe), bytes(token), Decimal(balance))
                for safe, token, balance in cursor
            ]
        if not sample_rows:
            logger.info("erc20_balance.drift: rollup is empty, nothing to check")
            return None

        sampled_safes = sorted({safe for safe, _token, _balance in sample_rows})
        recomputed = erc20_balances_upto_block(sampled_safes, watermark)

        with connection.cursor() as cursor:
            cursor.execute(_ERC20_BALANCE_EXISTING_FOR_SAFES_SQL, [sampled_safes])
            existing = {
                (bytes(safe), bytes(token)): Decimal(balance)
                for safe, token, balance in cursor
            }

    # If the nightly run landed between the sample and the recompute, the
    # watermark it recomputed at is no longer the one the sample
    # describes. That is a race, not drift -- same guard as the native
    # check.
    if (
        AnalyticsWatermark.objects.filter(name=ERC20_BALANCE_WATERMARK)
        .values_list("block_number", flat=True)
        .first()
        != watermark
    ):
        logger.info(
            "erc20_balance.drift: the rollup advanced past block %d while the "
            "check was running; skipping this round",
            watermark,
        )
        return None

    with connection.cursor() as cursor:
        cursor.execute(_ERC20_BALANCE_ORPHANS_SQL)
        orphans = int(cursor.fetchone()[0] or 0)
        cursor.execute(_ERC20_BALANCE_NEGATIVE_PAIRS_SQL)
        negative_pairs = int(cursor.fetchone()[0] or 0)

    mismatches = []
    total_abs_diff = Decimal(0)
    for key in set(existing) | set(recomputed):
        stored = existing.get(key, Decimal(0))
        expected = recomputed.get(key, Decimal(0))
        diff = stored - expected
        if diff:
            mismatches.append((*key, stored, expected, diff))
            total_abs_diff += abs(diff)

    compared = len(set(existing) | set(recomputed))
    summary = {
        "watermark": watermark,
        "sampled": len(sample_rows),
        "compared": compared,
        "mismatched": len(mismatches),
        "total_abs_diff": int(total_abs_diff),
        "max_abs_diff": (int(max(abs(d) for *_, d in mismatches)) if mismatches else 0),
        "orphan_rows": orphans,
        "negative_pairs": negative_pairs,
        "elapsed": round(time.time() - started, 2),
    }

    if orphans:
        logger.warning(
            "erc20_balance.drift: %d rollup rows have no Safe in "
            "history_safecontract. A reorg that removed a Safe creation "
            "leaves the row behind, still contributing its balance to the "
            "totals. Rebuild with `manage.py backfill_erc20_balances "
            "--restart` to drop them.",
            orphans,
        )

    if negative_pairs:
        logger.info(
            "erc20_balance.drift: %d pairs in the rollup are currently "
            "negative (incomplete upstream indexing, not necessarily a "
            "rollup bug on its own) -- see TokenHolding.negative_pairs per "
            "token for the breakdown",
            negative_pairs,
        )

    if mismatches:
        worst = sorted(mismatches, key=lambda row: abs(row[4]), reverse=True)[:5]
        logger.warning(
            "erc20_balance.drift: %d of %d compared pairs (sample of %d "
            "Safes) disagree with a from-scratch recompute at block %d "
            "(total |diff| = %d, worst = %d). Worst offenders: %s. The "
            "rollup cannot self-heal -- rebuild with `manage.py "
            "backfill_erc20_balances --restart` if this is not a one-off.",
            summary["mismatched"],
            summary["compared"],
            summary["sampled"],
            watermark,
            summary["total_abs_diff"],
            summary["max_abs_diff"],
            ", ".join(
                f"0x{safe.hex()}/0x{token.hex()} stored={stored} expected={expected}"
                for safe, token, stored, expected, _ in worst
            ),
        )
    else:
        logger.info(
            "erc20_balance.drift: %d compared pairs (sample of %d Safes) "
            "all agree at block %d (%.2fs)",
            summary["compared"],
            summary["sampled"],
            watermark,
            summary["elapsed"],
        )
    return summary


@app.shared_task(bind=True)
@task_timeout(timeout_seconds=LOCK_TIMEOUT * 2)
def check_erc20_balance_drift_task(self):
    """Weekly sanity check on the incremental ERC-20 balance rollup
    (Sundays 05:15 UTC, after ``check_native_balance_drift_task``).

    Reports only. See ``check_erc20_balance_drift``.
    """
    with contextlib.suppress(LockError):
        with only_one_running_task(self):
            return check_erc20_balance_drift()


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
# The two `FILTER` aggregates split the same rows by API attribution:
# `mt.proposer` is written only by the proposal API, so non-NULL means
# the tx was created through this service (`via_api`) and NULL means the
# indexer was the first writer — executed on-chain, never proposed here
# (`indexed_only`). They ride along on the join that was already being
# scanned — no extra round-trip, no extra index need.
_METRIC_CORE_MULTISIG_COUNT_SUM_SQL = """
SELECT
    COUNT(*) AS tx_count,
    COALESCE(SUM(mt.value), 0)::text AS native_wei,
    COUNT(*) FILTER (WHERE mt.proposer IS NOT NULL) AS via_api,
    COUNT(*) FILTER (WHERE mt.proposer IS NULL) AS indexed_only
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

    `multisig_txs_executed`, `native_value_wei` and the
    `multisig_txs_via_api` / `multisig_txs_indexed_only` split
    share the same join shape; combined into a single SQL with four
    aggregates to halve the round-trip.

    `module_txs` keeps the simple 2-table ORM filter on
    `internal_tx.timestamp` (directly btree-indexed); `erc20_transfers`
    keeps the 1-table count on its own timestamp index. Neither is the
    bottleneck.
    """
    start_block, end_block = _resolve_block_window(day_start, day_end)
    if start_block is None:
        # No blocks in the window. This function only ever runs once the
        # catch-up gate has passed for the day (`compute_day` /
        # `ensure_day_settled` -- see "Analytics catch-up" in
        # `analytics/implementation-notes.md`), so this is an honest zero,
        # not "not yet indexed": a quiet network genuinely had no activity.
        # It is also final -- a later reindex will not cause this day to be
        # recomputed, only an explicit `backfill_daily_metrics --start D
        # --end D` does. `multisig_txs_via_api` / `multisig_txs_indexed_only`
        # get a real 0 here too, not NULL, so a quiet day reads as "API
        # attribution complete" rather than "not computed".
        new_safes = 0
        multisig_txs_executed = 0
        native_value_wei = 0
        multisig_txs_via_api = 0
        multisig_txs_indexed_only = 0
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
            multisig_txs_via_api = int(row[2] or 0)
            multisig_txs_indexed_only = int(row[3] or 0)

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
            "multisig_txs_via_api": multisig_txs_via_api,
            "multisig_txs_indexed_only": multisig_txs_indexed_only,
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


def _upsert_daily_metric(
    day_start, day_end, *, only: tuple[str, ...] | None = None
) -> DayResult:
    """Run the single-day populate path -- some or all of the 6 narrow
    rollup populators, plus the `DailyMetric` core upsert -- and report
    what happened as a `DayResult`. Doesn't know about the catch-up gate:
    callers that need "is this day settled yet" go through `compute_day`
    (see "Analytics catch-up" in `analytics/implementation-notes.md`).

    `only`, when given, retries a subset of `POPULATORS` instead of a full
    run: the day must already have a `DailyMetric` row (`ValueError`
    otherwise, along with an empty or unknown-name `only`). The core reruns
    only when `only` overlaps `CORE_INPUTS` (`active_safes` / `active_owners`
    -- the core reads their rollups, so retrying either without the core
    would leave its counts stale); `only=None` is a full run and always
    reruns the core.

    Population order matters: the membership populators (`active_safes`,
    `active_owners`) run FIRST so their rollups are populated before
    `_compute_daily_metric_core` reads its `active_safes_daily` /
    `active_owners_daily` counts from them. Without this ordering the
    same expensive aggregates run twice per day (once for the count,
    once for the rollup rows) — on BASE that doubles wall time.

    Per-populator failures are isolated so one slow / failing populator
    does not strand the others; a core failure is caught here too (rather
    than raised) so a bad day doesn't strand the whole batch caller.

    A run that touches the core clears both `DailyMetric` completion marks
    first, as an immediate, separately committed write (not inside any
    transaction the rest of this function opens) — so a run that dies
    partway through leaves the day looking "not computed" rather than
    stale-but-marked-complete. The day's `AnalyticsCatchupState.core_ok` (if
    the sweeper has a row for it) is reset the same way, for the same
    reason: an `only=` retry that skips the core later trusts that field to
    know whether the core is still good.
    """
    # Name -> populator function, keyed the same as `POPULATORS`. Built here
    # (not module-level) so a test's `unittest.mock.patch` on the module
    # attribute is picked up -- a module-level dict would freeze in the
    # original function objects at import time instead.
    populator_funcs = {
        "active_safes": _compute_daily_active_safes,
        "active_owners": _compute_daily_active_owners,
        "token_volume": _compute_daily_token_volume,
        "tx_volume": _compute_daily_tx_volume,
        "safe_app_txs": _compute_daily_safe_app_txs,
        "safe_creations": _compute_daily_safe_creations,
    }
    day = day_start.date()

    if only is not None:
        if not only:
            raise ValueError("only must not be empty")
        unknown = set(only) - set(POPULATORS)
        if unknown:
            raise ValueError(
                f"only contains unknown populator name(s): {sorted(unknown)}"
            )
        if not DailyMetric.objects.filter(date=day).exists():
            raise ValueError(f"only requires an existing DailyMetric row for {day}")
        run_populators = tuple(name for name in POPULATORS if name in only)
        run_core = bool(set(only) & CORE_INPUTS)
    else:
        run_populators = POPULATORS
        run_core = True

    if run_core:
        DailyMetric.objects.filter(date=day).update(
            core_completed_at=None, completed_at=None
        )
        reset_core_ok(day)

    overall_started = time.time()
    logger.info(
        "_upsert_daily_metric: starting day=%s populators=%d",
        day,
        len(run_populators),
    )
    failed: list[str] = []
    for name in run_populators:
        fn = populator_funcs[name]
        step_started = time.time()
        try:
            rows = fn(day_start, day_end)
            logger.info(
                "_upsert_daily_metric: rollup %s took %.2fs day=%s rows=%s",
                name,
                time.time() - step_started,
                day,
                rows,
            )
        except Exception:
            failed.append(name)
            logger.exception(
                "_upsert_daily_metric: rollup %s failed in %.2fs day=%s",
                name,
                time.time() - step_started,
                day,
            )

    if run_core:
        # Core last so it can SELECT COUNT(*) from analytics_dailyactivesafe
        # instead of re-running _safes_active_between.
        core_started = time.time()
        try:
            obj = _compute_daily_metric_core(day_start, day_end)
            core_ok = True
            logger.info(
                "_upsert_daily_metric: metric_core took %.2fs day=%s "
                "active_safes=%d active_owners=%d multisig_txs_executed=%d "
                "via_api=%s indexed_only=%s",
                time.time() - core_started,
                day,
                obj.active_safes,
                obj.active_owners,
                obj.multisig_txs_executed,
                obj.multisig_txs_via_api,
                obj.multisig_txs_indexed_only,
            )
        except Exception:
            core_ok = False
            logger.exception(
                "_upsert_daily_metric: metric_core failed in %.2fs day=%s",
                time.time() - core_started,
                day,
            )
    else:
        core_ok = None  # not run this call; resolved below from prior state

    if only is None:
        # Full run: this call's own result is the whole story, nothing to
        # carry forward.
        effective_core_ok = core_ok
        effective_failed = set(failed)
    else:
        # Partial (`only=`) retry: populators/core outside `only` weren't
        # touched, so their standing carries forward from the last run
        # that did touch them -- which only the day's AnalyticsCatchupState
        # row (if any) knows. No row -> treat the core as not (yet) known
        # good, so a retry can't silently reveal a day whose core was
        # never actually verified.
        prior = get_day_state(day)
        prior_core_ok = (
            bool(prior.core_ok) if prior and prior.core_ok is not None else False
        )
        effective_core_ok = core_ok if run_core else prior_core_ok
        prior_failed = set(prior.failed_steps) if prior else set()
        effective_failed = (prior_failed - set(only)) | set(failed)

    tx_volume_ok = "tx_volume" not in effective_failed
    all_populators_ok = not effective_failed

    now = timezone.now()
    updates = {}
    if effective_core_ok and tx_volume_ok:
        updates["core_completed_at"] = now
    if effective_core_ok and all_populators_ok:
        updates["completed_at"] = now
    if updates:
        DailyMetric.objects.filter(date=day).update(**updates)

    status = (
        DayStatus.DONE
        if (effective_core_ok and all_populators_ok)
        else DayStatus.INCOMPLETE
    )
    logger.info(
        "_upsert_daily_metric: completed in %.2fs day=%s status=%s",
        time.time() - overall_started,
        day,
        status.value,
    )
    return DayResult(
        status=status,
        core_ok=effective_core_ok,
        failed=tuple(sorted(effective_failed)),
    )


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
    """Compute (or retry) `DailyMetric` rows for the last `days_back`
    complete UTC days through `compute_day` -- the same catch-up gate,
    completion marks and honest-zero handling a backfill shard or the
    sweeper get -- then refresh the rolling-window distinct active_* Redis
    keys. See "Analytics catch-up" in `analytics/implementation-notes.md`.

    Default `days_back=1` runs the previous-day metric daily. The same
    task is invoked by the `backfill_daily_metrics` management command
    with a larger range for one-shot history loads. Per-day try/except so
    a single bad day doesn't strand the rest of the run. The indexer
    status is fetched once for the whole run (`None` if unavailable) and
    reused for every day, exactly like a backfill shard or the sweeper.

    Exits immediately with INFO `analytics.daily.skipped_disabled`,
    without reading any analytics setting or touching the database, when
    `ENABLE_ANALYTICS` is False -- every instance runs this beat task
    regardless of that flag.

    A day that already has `completed_at` is skipped outright: no gate
    check, no populators, not counted as failed. This task doubles as the
    cold-read path behind `/active-safes/` and `/active-owners/`, which
    can compute "yesterday" ahead of the 01:00 cron when `SETTLE_MINUTES`
    is small; without this skip a routine run would clear and recompute
    an already-finished day, hiding it from `/tx-volume/` for as long as
    that takes.

    Guarded by ``only_one_running_task(self, lock_timeout=LOCK_TIMEOUT * 4
    + 300)`` -- the same lock name and timeout the sweeper uses -- so the
    01:00 cron, the hourly sweeper and a manual ``.delay()`` cannot race
    the same ``DailyMetric`` row. A skip due to the lock is not silent:
    INFO ``analytics.daily.skipped_locked``.
    """
    if not settings.ENABLE_ANALYTICS:
        logger.info("analytics.daily.skipped_disabled")
        return False

    try:
        with only_one_running_task(self, lock_timeout=LOCK_TIMEOUT * 4 + 300):
            started = time.time()
            now = timezone.now()
            today_utc_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
            logger.info("compute_daily_metrics_task: starting days_back=%d", days_back)

            try:
                status = get_indexer_status()
            except DayNotReady:
                status = None

            written = 0
            days_skipped_done = 0
            with relaxed_statement_timeout():
                for offset in range(1, days_back + 1):
                    day_start = today_utc_midnight - timezone.timedelta(days=offset)
                    day = day_start.date()
                    if DailyMetric.objects.filter(
                        date=day, completed_at__isnull=False
                    ).exists():
                        days_skipped_done += 1
                        continue
                    try:
                        result = compute_day(day, status)
                        if result.status != DayStatus.DEFERRED:
                            written += 1
                    except Exception:
                        logger.exception(
                            "compute_daily_metrics_task: day %s failed",
                            day,
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
                "compute_daily_metrics_task: completed in %.2fs "
                "days_written=%d/%d days_skipped_done=%d",
                time.time() - started,
                written,
                days_back,
                days_skipped_done,
            )
            return written > 0
    except LockError:
        logger.info("analytics.daily.skipped_locked")
        return False


@app.shared_task(
    bind=True,
    name="safe_transaction_service.analytics.tasks.analytics_catchup_task",
)
@task_timeout(timeout_seconds=LOCK_TIMEOUT * 4)
def analytics_catchup_task(self) -> None:
    """Hourly sweeper: retries unsettled/failed `DailyMetric` days inside
    the catch-up window, gives up loudly after `MAX_ATTEMPTS`, and flags
    days that fall out of the window or a stuck processing queue. See
    "Analytics catch-up" in `analytics/implementation-notes.md`.

    A thin wrapper around `catchup.run_catchup()`, declared here (not in
    the `catchup` package) with an explicit `name=` -- Celery's
    `autodiscover_tasks()` only scans `<app>.tasks`, and the
    `analytics.tasks.*` -> `contracts` queue route matches on this name, not
    on where the function is defined (see the module docstring in
    `catchup/__init__.py`).

    Exits immediately with INFO `analytics.catchup: skipped_reason=analytics_disabled`,
    without reading any analytics setting or touching the database, when
    `ENABLE_ANALYTICS` is False -- every instance runs this beat task
    regardless of that flag, same as `compute_daily_metrics_task`.

    Guarded by the *same* lock `compute_daily_metrics_task` takes --
    `only_one_running_task(compute_daily_metrics_task, lock_timeout=LOCK_TIMEOUT
    * 4 + 300)` derives the lock name from that task's registered name, so
    the 01:00 cron and this hourly sweeper can never write the same
    `DailyMetric` row at once. A skip due to the lock does no work and logs
    the same minimal line, `skipped_reason=locked`.
    """
    if not settings.ENABLE_ANALYTICS:
        logger.info("analytics.catchup: skipped_reason=analytics_disabled")
        return

    try:
        with only_one_running_task(
            compute_daily_metrics_task, lock_timeout=LOCK_TIMEOUT * 4 + 300
        ):
            run_catchup()
    except LockError:
        logger.info("analytics.catchup: skipped_reason=locked")
