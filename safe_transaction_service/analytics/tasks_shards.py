"""Sharded Celery tasks for analytics (Option 5 in
``SCALING_ARCHITECTURE.md``).

Two independent fan-outs live here:

1. **Native-balance sharding** (``compute_native_balance_shard`` +
   ``reduce_native_balance_shards``) — splits the all-Safes native-balance
   compute by first hex nibble of the Safe address so the wall-clock drops
   ~16× on BASE-sized fleets, gated by the worker pool size.
2. **Backfill sharding** (``_compute_daily_metric_shard`` +
   ``_backfill_done``) — one Celery task per UTC day for the
   ``backfill_daily_metrics`` management command.

Both shapes use the celery primitives `group` / `chord`; both are eager-mode
safe so they run inline during tests without a broker.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timedelta

from django.db import connection
from django.db.models import Count, Sum
from django.utils import timezone

from celery import app, chord, group

from safe_transaction_service.analytics.services.db import relaxed_statement_timeout
from safe_transaction_service.history.models import ERC20Transfer, SafeContract
from safe_transaction_service.utils.celery import task_timeout
from safe_transaction_service.utils.redis import get_redis
from safe_transaction_service.utils.tasks import LOCK_TIMEOUT

logger = logging.getLogger(__name__)


# 16 first-nibble shards. The BASE worker pool is on the order of 8–16
# concurrent slots; 16 saturates without queue backlog. Bump to 256
# (two-nibble) only if pool size grows past 16.
HEX_PREFIXES: tuple[str, ...] = tuple("0123456789abcdef")

# Redis key holding the most-recent backfill summary written by
# `backfill_done` — useful for `manage.py backfill_daily_metrics --wait`
# style polling.
BACKFILL_CURSOR_KEY = "analytics_backfill_cursor"


# ────────────────────── Native-balance sharding ────────────────────────


def _safe_addresses_for_prefix(prefix: str) -> list[bytes]:
    """Return the address-bytes for every SafeContract whose first hex
    nibble equals ``prefix``.

    ``SafeContract.address`` is the bytea PK, so the first hex nibble is
    the high 4 bits of byte 0. Filter by an indexed PK range
    ``[N0…00, NF…FF]`` and PG does a single range-scan of ~N/16 rows —
    the prior form iterated every row of ``history_safecontract`` and
    filtered in Python, costing 16 full table scans (one per shard) per
    TVL run.
    """
    nibble = int(prefix, 16)
    lo = bytes([nibble << 4]) + b"\x00" * 19
    hi = bytes([(nibble << 4) | 0x0F]) + b"\xff" * 19
    qs = SafeContract.objects.filter(address__gte=lo, address__lte=hi).values_list(
        "address", flat=True
    )
    return [bytes.fromhex(addr[2:]) for addr in qs.iterator(chunk_size=10_000)]


def _balance_for_addresses(address_bytes: list[bytes]) -> tuple[int, int]:
    """Run the existing batched balance SQL against an explicit address
    list. Returns ``(balance_wei, safes_with_balance)``.
    """
    # Local import — `tasks` imports `tasks_shards` only inside the chord
    # dispatcher (lazy), so we can safely import the legacy SQL constant
    # here.
    from safe_transaction_service.analytics.tasks import BALANCE_BATCH_SQL

    if not address_bytes:
        return 0, 0
    batch_size = 5000
    total_balance_wei = 0
    total_safes_with_balance = 0
    for offset in range(0, len(address_bytes), batch_size):
        batch = address_bytes[offset : offset + batch_size]
        with connection.cursor() as cursor:
            cursor.execute(BALANCE_BATCH_SQL, [batch, batch])
            row = cursor.fetchone()
        total_balance_wei += int(row[0]) if row and row[0] else 0
        total_safes_with_balance += int(row[1]) if row and row[1] else 0
    return total_balance_wei, total_safes_with_balance


@app.shared_task()
@task_timeout(timeout_seconds=LOCK_TIMEOUT)
def compute_native_balance_shard(prefix: str) -> dict:
    """One of the 16 hex-prefix shards of ``_calculate_native_balances_from_db``.

    Each shard owns ~1/16 of the SafeContract address space and runs the
    same batched ``BALANCE_BATCH_SQL`` over its slice. The reduce step
    sums everything back into the same shape the legacy single-task path
    produced — no service-layer changes needed downstream.

    On transient PG failure (statement timeout, broken connection, …) the
    shard returns ``{prefix, balance_wei: 0, safes_with_balance: 0,
    failed: True, error: <str>}`` rather than raising. Raising would
    propagate into the chord header and skip the body
    (``reduce_native_balance_shards | finalize_tvl_snapshot``) entirely,
    leaving the snapshot frozen at the phase-1 placeholder. Returning a
    zero stub lets ``reduce_native_balance_shards`` exclude the shard
    from the sum, surface ``partial_shards`` in the payload, and still
    invoke ``finalize_tvl_snapshot`` so the snapshot stays current.
    """
    started = time.time()
    logger.info("compute_native_balance_shard: starting prefix=%s", prefix)
    try:
        with relaxed_statement_timeout():
            address_bytes = _safe_addresses_for_prefix(prefix)
            balance_wei, safes_with_balance = _balance_for_addresses(address_bytes)
    except Exception as exc:
        logger.exception(
            "compute_native_balance_shard: prefix=%s failed after %.2fs",
            prefix,
            time.time() - started,
        )
        return {
            "prefix": prefix,
            "balance_wei": 0,
            "safes_with_balance": 0,
            "failed": True,
            "error": str(exc)[:500],
        }
    logger.info(
        "compute_native_balance_shard: completed in %.2fs prefix=%s "
        "addresses=%d safes_with_balance=%d balance_wei=%d",
        time.time() - started,
        prefix,
        len(address_bytes),
        safes_with_balance,
        balance_wei,
    )
    return {
        "prefix": prefix,
        "balance_wei": balance_wei,
        "safes_with_balance": safes_with_balance,
    }


@app.shared_task()
@task_timeout(timeout_seconds=LOCK_TIMEOUT)
def reduce_native_balance_shards(shards: list[dict]) -> dict:
    """Sum the shard results and return the same `(balance, count)` pair
    the legacy single-task path produced.

    Skips shards marked ``failed=True`` (see ``compute_native_balance_shard``)
    and reports the partial-shard count via ``partial_shards`` so the
    chord callback (``finalize_tvl_snapshot``) can surface it in the
    snapshot payload. Tolerates ``None`` entries defensively in case a
    future Celery version inserts them for tasks that hit ``task_timeout``
    with ``raise_exception=False``.
    """
    safe_shards = [s for s in shards if isinstance(s, dict) and not s.get("failed")]
    partial_shards = len(shards) - len(safe_shards)
    total = {
        "balance_wei": sum(int(s.get("balance_wei", 0)) for s in safe_shards),
        "safes_with_balance": sum(
            int(s.get("safes_with_balance", 0)) for s in safe_shards
        ),
        "partial_shards": partial_shards,
        "total_shards": len(shards),
    }
    logger.info(
        "reduce_native_balance_shards: completed shards=%d ok=%d failed=%d "
        "safes_with_balance=%d balance_wei=%d",
        len(shards),
        len(safe_shards),
        partial_shards,
        total["safes_with_balance"],
        total["balance_wei"],
    )
    return total


@app.shared_task()
@task_timeout(timeout_seconds=LOCK_TIMEOUT * 2)
def finalize_tvl_snapshot(reduced: dict) -> bool:
    """Chord callback that turns the reduced native balance into the final
    ``tvl`` snapshot.

    Receives ``{"balance_wei", "safes_with_balance"}`` from
    ``reduce_native_balance_shards``, runs the ERC20 net-flow aggregation,
    and overwrites the phase-1 placeholder ``compute_tvl_task`` wrote up
    front. Living in the chord callback (instead of the parent task) is
    what removes the synchronous ``.get()`` block that previously had
    ``compute_tvl_task`` hanging on a result key the gevent worker pool
    sometimes never observed.

    Failure stays local: the placeholder snapshot is left in place so the
    endpoint keeps serving a coherent zero payload until the next run.
    """
    # Local import — ``tasks`` imports ``tasks_shards`` at module load to
    # register the chord members, so the reverse edge has to stay lazy.
    from safe_transaction_service.analytics.tasks import _write_snapshot

    started = time.time()
    native_balance_wei = int(reduced.get("balance_wei", 0))
    total_safes_with_balance = int(reduced.get("safes_with_balance", 0))
    partial_shards = int(reduced.get("partial_shards", 0))
    total_shards = int(reduced.get("total_shards", 0))

    try:
        safe_addrs_subq = SafeContract.objects.values("address")

        with relaxed_statement_timeout():
            erc20_incoming = (
                ERC20Transfer.objects.filter(to__in=safe_addrs_subq)
                .values("address")
                .annotate(total_in=Sum("value"))
            )
            erc20_outgoing = (
                ERC20Transfer.objects.filter(_from__in=safe_addrs_subq)
                .values("address")
                .annotate(total_out=Sum("value"))
            )

            token_balances: dict[str, int] = {}
            for row in erc20_incoming:
                token_balances[row["address"]] = row["total_in"] or 0
            for row in erc20_outgoing:
                addr = row["address"]
                token_balances[addr] = token_balances.get(addr, 0) - (
                    row["total_out"] or 0
                )
            token_balances = {a: b for a, b in token_balances.items() if b > 0}

            token_safe_counts: dict[str, int] = (
                dict(
                    ERC20Transfer.objects.filter(
                        to__in=safe_addrs_subq,
                        address__in=list(token_balances),
                    )
                    .values_list("address")
                    .annotate(safe_count=Count("to", distinct=True))
                    .values_list("address", "safe_count")
                )
                if token_balances
                else {}
            )

        top_tokens = sorted(token_balances.items(), key=lambda x: x[1], reverse=True)[
            :20
        ]
        payload = {
            "total_safes_with_balance": total_safes_with_balance,
            "native_balance_wei": str(native_balance_wei),
            "erc20_token_count": len(token_balances),
            "top_tokens": [
                {
                    "address": addr,
                    "total_balance": str(bal),
                    "safe_count": token_safe_counts.get(addr, 0),
                }
                for addr, bal in top_tokens
            ],
            # Surface partial-shard state so consumers can detect that
            # this snapshot was computed from a subset of the address
            # space (one or more shards failed). When zero shards
            # failed both fields are zero/total and the payload reads
            # as fully-computed.
            "partial_shards": partial_shards,
            "total_shards": total_shards,
            "computed_at": timezone.now().isoformat(),
        }
        _write_snapshot("tvl", payload)
        logger.info(
            "finalize_tvl_snapshot: completed in %.2fs native_wei=%d "
            "safes_with_balance=%d erc20_tokens=%d partial_shards=%d/%d",
            time.time() - started,
            native_balance_wei,
            total_safes_with_balance,
            len(token_balances),
            partial_shards,
            total_shards,
        )
        return True
    except Exception:
        logger.exception(
            "finalize_tvl_snapshot: failed after %.2fs during ERC20 "
            "aggregation; keeping phase-1 placeholder snapshot",
            time.time() - started,
        )
        return False


def dispatch_tvl_chord() -> None:
    """Fire-and-forget the TVL chord.

    Builds ``(16 native shards) → reduce_native_balance_shards →
    finalize_tvl_snapshot`` and submits it to the ``contracts`` queue.
    The parent task (``compute_tvl_task``) does NOT block — the final
    snapshot is written by ``finalize_tvl_snapshot`` when the chord
    resolves. Eager mode runs the whole chain inline.
    """
    job = chord(
        (compute_native_balance_shard.s(p) for p in HEX_PREFIXES),
        reduce_native_balance_shards.s() | finalize_tvl_snapshot.s(),
    )
    job.apply_async(queue="contracts")


# ────────────────────── Backfill sharding ──────────────────────────────


def _parse_iso_date(value: str | date | datetime) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.strptime(value, "%Y-%m-%d").date()


@app.shared_task()
@task_timeout(timeout_seconds=LOCK_TIMEOUT * 4)
def compute_daily_metric_shard(day_iso: str) -> dict:
    """One backfill shard — compute and upsert the DailyMetric row plus all
    four narrow rollup tables for the given UTC day.

    `_upsert_daily_metric` already runs the 5-step inline path; this is a
    thin Celery-task wrapper so the backfill management command can group-
    dispatch one task per day and let the worker pool naturally cap
    concurrency.
    """
    # Local import — keeps the tasks_shards <-> tasks edge lazy so module
    # import order in Celery autodiscovery doesn't matter.
    from safe_transaction_service.analytics.tasks import _upsert_daily_metric

    day = _parse_iso_date(day_iso)
    tz = timezone.get_current_timezone()
    day_start = datetime.combine(day, datetime.min.time(), tzinfo=tz)
    day_end = day_start + timedelta(days=1)
    started = time.time()
    logger.info("compute_daily_metric_shard: starting day=%s", day_iso)
    try:
        with relaxed_statement_timeout():
            _upsert_daily_metric(day_start, day_end)
    except Exception as e:  # noqa: BLE001 — per-day isolation
        logger.exception(
            "compute_daily_metric_shard: failed after %.2fs day=%s",
            time.time() - started,
            day_iso,
        )
        return {"date": day_iso, "ok": False, "error": str(e)}
    elapsed = time.time() - started
    logger.info(
        "compute_daily_metric_shard: completed in %.2fs day=%s",
        elapsed,
        day_iso,
    )
    return {
        "date": day_iso,
        "ok": True,
        "elapsed_seconds": round(elapsed, 2),
    }


@app.shared_task()
@task_timeout(timeout_seconds=LOCK_TIMEOUT)
def backfill_done(shard_results: list[dict], stats_key: str | None = None) -> dict:
    """Chord callback for `backfill_daily_metrics`.

    Aggregates the per-day shard results and writes a small summary blob
    to Redis at `stats_key` (defaults to BACKFILL_CURSOR_KEY) so the
    management command can poll for completion.
    """
    written = sum(1 for r in shard_results if r.get("ok"))
    failed = sum(1 for r in shard_results if not r.get("ok"))
    failures = [r for r in shard_results if not r.get("ok")][:50]
    summary = {
        "total": len(shard_results),
        "written": written,
        "failed": failed,
        "failures": failures,
        "finished_at": timezone.now().isoformat(),
    }
    key = stats_key or BACKFILL_CURSOR_KEY
    get_redis().set(key, json.dumps(summary))
    logger.info(
        "backfill_done: completed days_written=%d/%d failed=%d summary_key=%s",
        written,
        len(shard_results),
        failed,
        key,
    )
    return summary


def dispatch_backfill(dates: list[date], stats_key: str | None = None):
    """Build the backfill chord — one shard per UTC day, summary written
    on completion. Returns the AsyncResult; callers can ``.get(timeout=…)``
    to block on completion.

    Eager mode works identically via the Redis result backend configured
    in `config/settings/base.py`.
    """
    key = stats_key or BACKFILL_CURSOR_KEY
    job = group(
        compute_daily_metric_shard.s(d.isoformat()) for d in dates
    ) | backfill_done.s(key)
    return job.apply_async(queue="contracts")


__all__ = [
    "HEX_PREFIXES",
    "BACKFILL_CURSOR_KEY",
    "compute_native_balance_shard",
    "reduce_native_balance_shards",
    "finalize_tvl_snapshot",
    "dispatch_tvl_chord",
    "compute_daily_metric_shard",
    "backfill_done",
    "dispatch_backfill",
]
