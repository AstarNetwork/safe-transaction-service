"""Sharded Celery tasks for analytics (Option 5 in
``SCALING_ARCHITECTURE.md``).

Two independent fan-outs live here:

1. **Native-balance sharding** (``compute_native_balance_shard`` +
   ``reduce_native_balance_shards``) — splits the all-Safes native-balance
   compute by first hex nibble of the Safe address so the wall-clock drops
   ~16× on BASE-sized fleets, gated by the worker pool size.
2. **Backfill sharding** (``compute_daily_metric_shard`` +
   ``backfill_done``) — one Celery task per UTC day for the
   ``backfill_daily_metrics`` management command, fanned out one chunk at
   a time: the chord callback of chunk *n* dispatches chunk *n+1*.
3. **Native-balance backfill** (``backfill_native_balance_chunk``) — the
   optional ``--celery`` mode of ``backfill_native_balances``. Not a chord:
   the walk is a keyset cursor over Safe addresses, so each chunk
   dispatches its own successor and the manifest carries a cursor rather
   than a precomputed chunk list.

Both shapes use the celery primitives `group` / `chord`; both are eager-mode
safe so they run inline during tests without a broker.
"""

from __future__ import annotations

import json
import logging
import secrets
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

# Redis key pointing at the most recently *started* backfill run
# (`{"run_id", "run_key", "started_at"}`), so `manage.py
# backfill_daily_metrics --status` can find it without arguments. Legacy
# callers of `dispatch_backfill()` without a stats_key still get their
# single-chord summary written here — `--status` tolerates both shapes.
BACKFILL_CURSOR_KEY = "analytics_backfill_cursor"
# Per-run manifest lives at `<prefix><run_id>`; per-chunk summaries at
# `<prefix><run_id>:chunk:<n>`. Both refreshed with this TTL on every write.
BACKFILL_RUN_KEY_PREFIX = "analytics_backfill_run:"
BACKFILL_KEY_TTL_SECONDS = 7 * 24 * 3600
# Cap on the per-day failure records carried in the run aggregate.
BACKFILL_MAX_FAILURES = 200


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

    Receives ``{"balance_wei", "safes_with_balance"}`` from whichever
    producer computed the native side, runs the ERC20 net-flow
    aggregation, and overwrites the phase-1 placeholder
    ``compute_tvl_task`` wrote up front.

    Two producers now feed it. The normal one is
    ``read_native_balance_rollup`` — ``compute_tvl_task`` reads the
    incremental rollup in milliseconds and calls this directly, no chord
    involved. The fallback is the original
    ``reduce_native_balance_shards`` chord, still used on instances whose
    rollup has not been backfilled yet. ``reduced`` says which via
    ``native_source``; when the key is absent the caller was the chord.

    Living outside the parent task is what removes the synchronous
    ``.get()`` block that previously had ``compute_tvl_task`` hanging on a
    result key the gevent worker pool sometimes never observed — true of
    both producers.

    Failure stays local: the placeholder snapshot is left in place so the
    endpoint keeps serving a coherent zero payload until the next run.
    The one failure that does *not* reach that handler is the token
    metadata join — it degrades to null symbols rather than costing the
    reduced snapshot (see the narrow try around ``get_token_symbols``).
    """
    # Local imports — ``tasks`` imports ``tasks_shards`` at module load to
    # register the chord members, so the reverse edge has to stay lazy;
    # ``analytics_service`` is kept lazy alongside it for symmetry (it is
    # only reached on the happy path of this one task).
    from safe_transaction_service.analytics.services.analytics_service import (
        get_token_symbols,
    )
    from safe_transaction_service.analytics.tasks import _write_snapshot

    started = time.time()
    native_balance_wei = int(reduced.get("balance_wei", 0))
    total_safes_with_balance = int(reduced.get("safes_with_balance", 0))
    partial_shards = int(reduced.get("partial_shards", 0))
    total_shards = int(reduced.get("total_shards", 0))
    # Absent means the 16-shard chord called us — it predates the key.
    native_source = reduced.get("native_source", "shards")
    native_updated_to_block = reduced.get("native_updated_to_block")

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
        # Token metadata join. Lexically inside the outer try (belt and
        # braces), but it owns its own except: the outer handler keeps the
        # phase-1 zero placeholder, so letting a `tokens_token` read fall
        # through to it would mean a metadata failure *loses* a
        # fully-reduced TVL snapshot. Degrade the symbols instead — an
        # empty map makes every `symbol` present-and-null, which is
        # already the documented contract for an unknown token.
        try:
            symbols = get_token_symbols(addr for addr, _ in top_tokens)
        except Exception:
            symbols = {}
            logger.warning(
                "finalize_tvl_snapshot: token metadata lookup failed for %d "
                "top tokens after %.2fs (partial_shards=%d/%d); writing the "
                "snapshot with null symbols",
                len(top_tokens),
                time.time() - started,
                partial_shards,
                total_shards,
                exc_info=True,
            )
        payload = {
            "total_safes_with_balance": total_safes_with_balance,
            "native_balance_wei": str(native_balance_wei),
            "erc20_token_count": len(token_balances),
            "top_tokens": [
                {
                    "address": addr,
                    "symbol": symbols.get(addr),
                    "total_balance": str(bal),
                    "safe_count": token_safe_counts.get(addr, 0),
                }
                for addr, bal in top_tokens
            ],
            # `partial_shards` / `total_shards` describe a sharded run
            # that did not fully reduce. The rollup producer has no
            # shards and writes 0/0 — which the hub reads as "complete",
            # exactly as intended (it gates USD pricing on
            # `partial_shards == 0`). The keys stay in the payload
            # because removing one is a breaking change and would have to
            # land consumer-first; on the rollup path they are
            # vestigial, and `native_source` is what actually describes
            # the run.
            "partial_shards": partial_shards,
            "total_shards": total_shards,
            # Additive, so older consumers ignore them: which producer
            # computed the native side, and the block it is complete
            # through. The second answers "how fresh is this number"
            # straight from the payload, without reading worker logs.
            "native_source": native_source,
            "native_updated_to_block": native_updated_to_block,
            "computed_at": timezone.now().isoformat(),
        }
        _write_snapshot("tvl", payload)
        logger.info(
            "finalize_tvl_snapshot: completed in %.2fs native_wei=%d "
            "safes_with_balance=%d erc20_tokens=%d native_source=%s "
            "native_updated_to_block=%s partial_shards=%d/%d",
            time.time() - started,
            native_balance_wei,
            total_safes_with_balance,
            len(token_balances),
            native_source,
            native_updated_to_block,
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


def dispatch_tvl_finalize(reduced: dict) -> None:
    """Fire-and-forget ``finalize_tvl_snapshot`` with an already-computed
    native side.

    The normal path since the native balance became an incremental
    rollup: ``compute_tvl_task`` reads the totals in milliseconds and
    hands them straight here, so there is no chord, no fan-out and no
    result backend involved in a TVL run at all.
    """
    finalize_tvl_snapshot.apply_async((reduced,), queue="contracts")


def dispatch_tvl_chord() -> None:
    """Fire-and-forget the TVL chord — the fallback path.

    Builds ``(16 native shards) → reduce_native_balance_shards →
    finalize_tvl_snapshot`` and submits it to the ``contracts`` queue.
    The parent task (``compute_tvl_task``) does NOT block — the final
    snapshot is written by ``finalize_tvl_snapshot`` when the chord
    resolves. Eager mode runs the whole chain inline.

    Still reached on instances that have migrated but not yet run
    ``manage.py backfill_native_balances``: there the rollup has no
    watermark, and falling back here keeps them serving the number they
    served before rather than a zero. It is also the independent
    reference the weekly drift check compares the rollup against, which
    is why none of this machinery was deleted with the switch.
    """
    job = chord(
        (compute_native_balance_shard.s(p) for p in HEX_PREFIXES),
        reduce_native_balance_shards.s() | finalize_tvl_snapshot.s(),
    )
    job.apply_async(queue="contracts")


# ───────────── Native-balance backfill, chunked on Celery ──────────────
#
# The inline `manage.py backfill_native_balances` is still the default and
# still the one with no task timeout over it. This is the same walk driven
# from the worker instead, for when the run should outlive the shell that
# started it.
#
# Shape: one chunk of `chunk_size` Safe addresses per task, strictly one in
# flight, each task dispatching its own successor. Not a chord — the walk
# is a keyset cursor, so chunk N+1's starting address is not known until
# chunk N has run. That also means the manifest cannot enumerate its chunks
# up front the way `build_backfill_run` does for dates; it carries a cursor
# and a running aggregate instead.
#
# Why this is safe under `task_timeout(LOCK_TIMEOUT)` when the 16 TVL
# shards are not: a shard owns ~1/16 of the address space (~29k addresses
# of full history on Ethereum) and cannot finish in 900 s. A chunk owns
# 5000 addresses, which is the batch size the balance SQL was measured at —
# seconds, not minutes. The unit of work is bounded here and unbounded
# there.

NATIVE_BALANCE_RUN_KEY_PREFIX = "analytics_native_balance_run:"
NATIVE_BALANCE_CURSOR_KEY = "analytics_native_balance_cursor"


def native_balance_run_key(run_id: str) -> str:
    return f"{NATIVE_BALANCE_RUN_KEY_PREFIX}{run_id}"


def load_native_balance_run(run_id: str) -> dict | None:
    """Return the run manifest for ``run_id``, or ``None`` if unknown/expired."""
    return _redis_get_json(native_balance_run_key(run_id))


def latest_native_balance_run_id() -> str | None:
    pointer = _redis_get_json(NATIVE_BALANCE_CURSOR_KEY)
    if pointer and isinstance(pointer.get("run_id"), str):
        return pointer["run_id"]
    return None


def build_native_balance_run(
    head: int, chunk_size: int, total_safes: int, run_id: str | None = None
) -> dict:
    """Pure helper: a fresh manifest. Nothing is written or dispatched.

    `total_safes` is only for the progress line — the fleet grows during a
    long run, so it is a denominator, not a target.
    """
    run_id = run_id or new_backfill_run_id()
    return {
        "run_id": run_id,
        "run_key": native_balance_run_key(run_id),
        "head": int(head),
        "chunk_size": int(chunk_size),
        "total_safes_at_start": int(total_safes),
        "started_at": timezone.now().isoformat(),
        "finished_at": None,
        "state": "running",
        "error": None,
        # Hex of the last address the walk consumed; None = start from the
        # beginning. Carried in Redis rather than in the task signature so a
        # lost message is recoverable by re-dispatching from the manifest.
        "cursor": None,
        "chunks_done": 0,
        "safes_seen": 0,
        "safes_seeded": 0,
        "safes_already_present": 0,
        "watermark_written": False,
    }


def _save_native_balance_run(run: dict) -> None:
    _redis_set_json(run["run_key"], run)


@app.shared_task()
@task_timeout(timeout_seconds=LOCK_TIMEOUT)
def backfill_native_balance_chunk(run_id: str) -> dict:
    """One chunk of the native-balance backfill, then dispatch the next.

    Reads its own starting cursor from the run manifest, so the task
    signature stays `(run_id)` and a chunk can always be re-dispatched from
    Redis after a lost message. Exactly one chunk of a run is ever in
    flight, so the manifest has a single writer and needs no CAS.

    Failure stops the chain rather than raising into a retry storm: the
    manifest records `state="failed"` with the message, `--status` shows it,
    and re-running the command resumes (rows already written are skipped).
    """
    from safe_transaction_service.analytics.tasks import (
        safe_addresses_after,
        seed_missing_native_balances,
        write_native_balance_watermark,
    )

    run = load_native_balance_run(run_id)
    if run is None:
        logger.warning(
            "native_balance.backfill: run=%s manifest is gone (expired or "
            "flushed); stopping the chain",
            run_id,
        )
        return {"run_id": run_id, "state": "unknown"}
    if run.get("state") != "running":
        logger.info(
            "native_balance.backfill: run=%s is %s, not dispatching further chunks",
            run_id,
            run.get("state"),
        )
        return run

    started = time.time()
    cursor_hex = run.get("cursor")
    after = bytes.fromhex(cursor_hex) if cursor_hex else None

    try:
        with relaxed_statement_timeout():
            addresses = safe_addresses_after(after, run["chunk_size"])
            if not addresses:
                # Walked off the end: hand the rollup over and close the run.
                wrote = write_native_balance_watermark(run["head"])
                run["watermark_written"] = wrote
                run["state"] = "finished"
                run["finished_at"] = timezone.now().isoformat()
                _save_native_balance_run(run)
                logger.info(
                    "native_balance.backfill: run=%s finished chunks=%d "
                    "seen=%d seeded=%d already_present=%d watermark=%s",
                    run_id,
                    run["chunks_done"],
                    run["safes_seen"],
                    run["safes_seeded"],
                    run["safes_already_present"],
                    run["head"] if wrote else "left as it was",
                )
                return run

            seeded, present = seed_missing_native_balances(addresses, run["head"])
    except Exception as exc:
        logger.exception(
            "native_balance.backfill: run=%s chunk %d failed after %.2fs",
            run_id,
            run["chunks_done"] + 1,
            time.time() - started,
        )
        run["state"] = "failed"
        run["error"] = str(exc)[:500]
        run["finished_at"] = timezone.now().isoformat()
        _save_native_balance_run(run)
        return run

    run["cursor"] = addresses[-1].hex()
    run["chunks_done"] += 1
    run["safes_seen"] += len(addresses)
    run["safes_seeded"] += seeded
    run["safes_already_present"] += present
    # Persisted BEFORE the dispatch below and never after it: under eager
    # mode the whole remaining chain runs inside `apply_async`, so a write
    # afterwards would clobber newer state with this stale copy. Same
    # lesson as `_dispatch_backfill_chunk`.
    _save_native_balance_run(run)

    logger.info(
        "native_balance.backfill: run=%s chunk %d done in %.2fs seen=%d/%d "
        "seeded=%d present=%d",
        run_id,
        run["chunks_done"],
        time.time() - started,
        run["safes_seen"],
        run["total_safes_at_start"],
        seeded,
        present,
    )
    backfill_native_balance_chunk.apply_async((run_id,), queue="contracts")
    return run


def start_native_balance_backfill_run(
    head: int, chunk_size: int, total_safes: int, run_id: str | None = None
) -> dict:
    """Persist a new manifest, point the cursor key at it and dispatch the
    first chunk. Later chunks dispatch themselves on the worker, so the
    caller may exit immediately.
    """
    run = build_native_balance_run(head, chunk_size, total_safes, run_id=run_id)
    _save_native_balance_run(run)
    _redis_set_json(
        NATIVE_BALANCE_CURSOR_KEY,
        {
            "run_id": run["run_id"],
            "run_key": run["run_key"],
            "started_at": run["started_at"],
        },
    )
    backfill_native_balance_chunk.apply_async((run["run_id"],), queue="contracts")
    return load_native_balance_run(run["run_id"]) or run


# ────────────────────── Backfill sharding ──────────────────────────────
#
# Shape (since 2026-09): a *run* is a manifest blob in Redis describing the
# whole date range split into chunks; exactly one chunk is in flight at any
# time. ``backfill_done`` (the chord callback) records the finished chunk,
# refreshes the run aggregate and dispatches the next chunk itself. The
# management command therefore never holds the throttle — it only starts
# the run and (optionally) polls Redis to report progress. Nothing about
# the concurrency cap depends on the worker pool size, on the result
# backend, or on the command process staying alive.


def _parse_iso_date(value: str | date | datetime) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.strptime(value, "%Y-%m-%d").date()


def _iso(value: str | date | datetime) -> str:
    return value if isinstance(value, str) else _parse_iso_date(value).isoformat()


def backfill_run_key(run_id: str) -> str:
    return f"{BACKFILL_RUN_KEY_PREFIX}{run_id}"


def backfill_chunk_key(run_id: str, chunk_index: int) -> str:
    return f"{BACKFILL_RUN_KEY_PREFIX}{run_id}:chunk:{chunk_index}"


def new_backfill_run_id() -> str:
    return f"{timezone.now().strftime('%Y%m%dT%H%M%S')}-{secrets.token_hex(3)}"


def _redis_set_json(key: str, value: dict) -> None:
    get_redis().set(key, json.dumps(value), ex=BACKFILL_KEY_TTL_SECONDS)


def _redis_get_json(key: str) -> dict | None:
    blob = get_redis().get(key)
    if not blob:
        return None
    try:
        return json.loads(blob)
    except (TypeError, ValueError):
        logger.warning("backfill: unreadable JSON at redis key %s", key)
        return None


def load_backfill_run(run_id: str) -> dict | None:
    """Return the run manifest for ``run_id`` or ``None`` if unknown/expired."""
    return _redis_get_json(backfill_run_key(run_id))


def load_backfill_chunk_summary(run_id: str, chunk_index: int) -> dict | None:
    """Return the per-chunk summary written by ``backfill_done`` or ``None``
    while the chunk is still pending / in flight."""
    return _redis_get_json(backfill_chunk_key(run_id, chunk_index))


def latest_backfill_run_id() -> str | None:
    """``BACKFILL_CURSOR_KEY`` points at the most recently started run."""
    pointer = _redis_get_json(BACKFILL_CURSOR_KEY)
    if pointer and isinstance(pointer.get("run_id"), str):
        return pointer["run_id"]
    return None


def build_backfill_run(
    dates: list[date | str], chunk_days: int, run_id: str | None = None
) -> dict:
    """Pure helper: the manifest for ``dates`` split into ``chunk_days``-sized
    chunks. Nothing is written or dispatched. ``chunk_days <= 0`` → one chunk.
    """
    if not dates:
        raise ValueError("backfill run needs at least one date")
    days = [_iso(d) for d in dates]
    chunk_size = chunk_days if chunk_days and chunk_days > 0 else len(days)
    run_id = run_id or new_backfill_run_id()
    chunks = []
    for index, offset in enumerate(range(0, len(days), chunk_size)):
        chunk_days_list = days[offset : offset + chunk_size]
        chunks.append(
            {
                "index": index,
                "start": chunk_days_list[0],
                "end": chunk_days_list[-1],
                "days": chunk_days_list,
                "key": backfill_chunk_key(run_id, index),
                "state": "pending",
                "dispatched_at": None,
                "finished_at": None,
                "total": 0,
                "written": 0,
                "failed": 0,
                "error": None,
            }
        )
    return {
        "run_id": run_id,
        "run_key": backfill_run_key(run_id),
        "start": days[0],
        "end": days[-1],
        "total_days": len(days),
        "chunk_days": chunk_size,
        "chunk_count": len(chunks),
        "started_at": timezone.now().isoformat(),
        "finished_at": None,
        # Aggregate over *finished* chunks only.
        "total": 0,
        "written": 0,
        "failed": 0,
        "failures": [],
        "chunks": chunks,
    }


def _save_backfill_run(run: dict) -> None:
    _redis_set_json(run["run_key"], run)


def _dispatch_backfill_chunk(run: dict, chunk_index: int):
    """Mark chunk ``chunk_index`` as running, persist the manifest, then
    submit its chord. The manifest is written *before* ``apply_async`` and
    never after it: under eager mode the whole chain (including the nested
    ``backfill_done`` for every later chunk) runs inside ``apply_async``, so
    any write after it would clobber newer state with this stale copy.
    """
    chunk = run["chunks"][chunk_index]
    chunk["state"] = "running"
    chunk["dispatched_at"] = timezone.now().isoformat()
    _save_backfill_run(run)
    logger.info(
        "backfill: run=%s dispatching chunk %d/%d (%s → %s, %d days)",
        run["run_id"],
        chunk_index + 1,
        run["chunk_count"],
        chunk["start"],
        chunk["end"],
        len(chunk["days"]),
    )
    return dispatch_backfill(
        chunk["days"],
        stats_key=chunk["key"],
        run_id=run["run_id"],
        chunk_index=chunk_index,
    )


def start_backfill_run(
    dates: list[date | str], chunk_days: int, run_id: str | None = None
) -> dict:
    """Persist a new run manifest, point ``BACKFILL_CURSOR_KEY`` at it and
    dispatch the first chunk. Returns the manifest as it stands after the
    dispatch call returns (under eager mode that is the finished run).

    Later chunks are dispatched by ``backfill_done`` on the worker, one
    after the other — the caller may exit immediately.
    """
    run = build_backfill_run(dates, chunk_days, run_id=run_id)
    _save_backfill_run(run)
    _redis_set_json(
        BACKFILL_CURSOR_KEY,
        {
            "run_id": run["run_id"],
            "run_key": run["run_key"],
            "started_at": run["started_at"],
        },
    )
    _dispatch_backfill_chunk(run, 0)
    return load_backfill_run(run["run_id"]) or run


def _advance_backfill_run(run_id: str, chunk_index: int, summary: dict) -> None:
    """Record ``summary`` for the finished chunk, refresh the aggregate and
    dispatch the next chunk (or close the run). Only ever called from the
    chord callback, so there is a single writer per run and no CAS needed.
    """
    run = load_backfill_run(run_id)
    if run is None:
        logger.warning(
            "backfill_done: run=%s manifest missing (expired?); chunk %d "
            "summary kept at its own key, no further chunks dispatched",
            run_id,
            chunk_index,
        )
        return
    chunk = run["chunks"][chunk_index]
    chunk.update(
        state="done",
        finished_at=summary["finished_at"],
        total=summary["total"],
        written=summary["written"],
        failed=summary["failed"],
    )
    done = [c for c in run["chunks"] if c["state"] == "done"]
    run["total"] = sum(c["total"] for c in done)
    run["written"] = sum(c["written"] for c in done)
    run["failed"] = sum(c["failed"] for c in done)
    run["failures"] = (run.get("failures") or [])[:BACKFILL_MAX_FAILURES]
    room = BACKFILL_MAX_FAILURES - len(run["failures"])
    if room > 0:
        run["failures"].extend(summary.get("failures", [])[:room])

    next_index = chunk_index + 1
    if next_index >= run["chunk_count"]:
        run["finished_at"] = timezone.now().isoformat()
        _save_backfill_run(run)
        logger.info(
            "backfill: run=%s finished written=%d failed=%d total=%d",
            run_id,
            run["written"],
            run["failed"],
            run["total"],
        )
        return

    try:
        _dispatch_backfill_chunk(run, next_index)
    except Exception as exc:  # noqa: BLE001 — surface via manifest
        logger.exception(
            "backfill: run=%s failed to dispatch chunk %d", run_id, next_index
        )
        # `_dispatch_backfill_chunk` may have persisted "running" before the
        # broker call blew up — reload so we don't resurrect stale state.
        run = load_backfill_run(run_id) or run
        run["chunks"][next_index].update(state="dispatch_failed", error=str(exc)[:500])
        run["finished_at"] = timezone.now().isoformat()
        _save_backfill_run(run)


@app.shared_task()
@task_timeout(timeout_seconds=LOCK_TIMEOUT * 4)
def compute_daily_metric_shard(day_iso: str) -> dict:
    """One backfill shard — compute and upsert the DailyMetric row plus all
    narrow rollup tables for the given UTC day.

    `_upsert_daily_metric` already runs the full inline populate path; this
    is a thin Celery-task wrapper so ``dispatch_backfill`` can fan one chunk
    out as a chord. Concurrency is bounded by the chunk size (one chunk in
    flight per run), not by this task.
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


# `ignore_result=False` overrides the project-wide `CELERY_IGNORE_RESULT =
# True`. Without it the callback's own return value is never stored, so an
# `AsyncResult.get()` on the chord (what `dispatch_backfill(...).get()` and
# the pre-2026-09 management command did) blocks until its timeout even
# though the callback ran to completion. The chord *header* coordination is
# unaffected either way (`on_chord_part_return` runs regardless of
# `ignore_result`), which is why the Redis summary always landed.
@app.shared_task(ignore_result=False)
@task_timeout(timeout_seconds=LOCK_TIMEOUT)
def backfill_done(
    shard_results: list[dict | None],
    stats_key: str | None = None,
    run_id: str | None = None,
    chunk_index: int | None = None,
) -> dict:
    """Chord callback for one backfill chunk.

    Aggregates the per-day shard results and writes the chunk summary to
    Redis at ``stats_key`` (defaults to ``BACKFILL_CURSOR_KEY`` for callers
    that use ``dispatch_backfill`` standalone). When ``run_id`` /
    ``chunk_index`` are given the summary is also folded into the run
    manifest and the *next* chunk of that run is dispatched from here —
    this callback is the throttle.

    A shard that hit ``task_timeout`` returns ``None`` instead of a dict;
    those are counted as failed days (with the date recovered from the
    chunk manifest by position) rather than crashing the callback, which
    would otherwise stall the whole run.
    """
    days: list[str] | None = None
    if run_id is not None and chunk_index is not None:
        run = load_backfill_run(run_id)
        if run is not None:
            days = run["chunks"][chunk_index]["days"]

    normalized: list[dict] = []
    for position, result in enumerate(shard_results or []):
        if not isinstance(result, dict):
            result = {
                "date": days[position] if days and position < len(days) else None,
                "ok": False,
                "error": "shard returned no result (task timeout or worker loss)",
            }
        normalized.append(result)

    written = sum(1 for r in normalized if r.get("ok"))
    failed = len(normalized) - written
    failures = [r for r in normalized if not r.get("ok")][:50]
    summary = {
        "total": len(normalized),
        "written": written,
        "failed": failed,
        "failures": failures,
        "finished_at": timezone.now().isoformat(),
    }
    if run_id is not None:
        summary["run_id"] = run_id
        summary["chunk_index"] = chunk_index
    if days:
        summary["start"] = days[0]
        summary["end"] = days[-1]

    key = stats_key or BACKFILL_CURSOR_KEY
    _redis_set_json(key, summary)
    logger.info(
        "backfill_done: completed run=%s chunk=%s days_written=%d/%d failed=%d "
        "summary_key=%s",
        run_id,
        chunk_index,
        written,
        len(normalized),
        failed,
        key,
    )
    if run_id is not None and chunk_index is not None:
        _advance_backfill_run(run_id, chunk_index, summary)
    return summary


def dispatch_backfill(
    dates: list[date | str],
    stats_key: str | None = None,
    run_id: str | None = None,
    chunk_index: int | None = None,
):
    """Build and submit ONE backfill chord — one shard per UTC day, chunk
    summary written to ``stats_key`` on completion. Returns the AsyncResult.

    This is the low-level primitive; ``start_backfill_run`` is what the
    management command uses so chunks execute one after another. Calling
    this directly for a large range puts every day on the queue at once.
    """
    key = stats_key or BACKFILL_CURSOR_KEY
    job = group(compute_daily_metric_shard.s(_iso(d)) for d in dates) | backfill_done.s(
        key, run_id=run_id, chunk_index=chunk_index
    )
    return job.apply_async(queue="contracts")


__all__ = [
    "HEX_PREFIXES",
    "BACKFILL_CURSOR_KEY",
    "BACKFILL_RUN_KEY_PREFIX",
    "BACKFILL_KEY_TTL_SECONDS",
    "compute_native_balance_shard",
    "reduce_native_balance_shards",
    "finalize_tvl_snapshot",
    "dispatch_tvl_chord",
    "compute_daily_metric_shard",
    "backfill_done",
    "dispatch_backfill",
    "build_backfill_run",
    "start_backfill_run",
    "load_backfill_run",
    "load_backfill_chunk_summary",
    "latest_backfill_run_id",
    "backfill_run_key",
    "backfill_chunk_key",
    "new_backfill_run_id",
]
