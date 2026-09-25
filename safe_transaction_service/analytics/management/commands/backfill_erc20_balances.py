"""Initial fill of the incremental ERC-20 balance rollup (token-holdings
spec §4.3, `docs/specs/token-holdings.md` in the workspace root).

One full pass over every Safe, computing its non-zero (Safe, token)
balances as of one fixed block H, so `compute_erc20_balance_rollup_task`
has something to add deltas to. Mirrors `backfill_native_balances.py`'s
shape (resumable, `--restart` TRUNCATE, guards, per-chunk logging) with
deliberate departures forced by the ERC-20 rollup's own design
(`analytics/tasks.py`, the block above `ERC20_BALANCE_WATERMARK`):

- **A shared Redis lock**, not just a progress table as the resume
  journal. `SafeTokenBalance` deletes zero-balance rows, so unlike
  native's "a row exists = already done" there is no way to tell "not
  seeded yet" from "seeded, balance is zero" by reading the table alone
  — progress lives in dedicated `AnalyticsWatermark` rows instead. The
  lock is the *same* key `compute_erc20_balance_rollup_task` takes
  (`only_one_running_task`), because the nightly task's own delta step
  and this command's seed step both write into `SafeTokenBalance` and
  must never overlap (spec edge case #10b).
- **Chunks are Safes, addressed the same way step 1 of the nightly
  rollup is** (`erc20_balance_seed_candidates`, marker/boundary-driven),
  not a keyset scan over `SafeContract.address` — the boundary B this
  run fixes at the start must be the same B the eventual handover to
  `erc20_balance_safes` uses, so reusing the exact seed-candidate query
  keeps that invariant true by construction instead of by convention.
- **Whales are summed once, in one block-range walk, after the chunk
  loop — never inside it.** A handful of Safes on Optimism each own
  millions of `history_erc20transfer` rows (one alone ~35%, ~60M — see
  `optimism-balance-accuracy.paste.sql`, section 1w). The chunk loop only
  seeds non-whale Safes; a chunk containing a whale still advances the
  progress cursor past it, but the whale itself is left for the walk
  `run_whale_slice` runs over `(0, H]` once every chunk is done, in
  `--whale-block-range` steps through `history_ethereumtx.block_id` (the
  same access path the nightly delta already uses, see
  `_ERC20_BALANCE_WHALE_UPSERT_SQL`). Summing a whale inside its own
  chunk — once per whale, however many chunks it happens to land in —
  would mean N whales cost N full scans of the whole transfer history
  instead of one; walking after the loop, over the *set* of all whales
  found by then, costs exactly one.

Progress lives in three `AnalyticsWatermark` rows, not the rollup table:
`erc20_balance_backfill` (`block_number=H`, `address`/`computed_at` =
the `(created, address)` of the last fully seeded Safe, `address IS
NULL` before the first chunk completes — the same "no marker yet"
convention `ERC20_BALANCE_SAFES_WATERMARK` already uses),
`erc20_balance_backfill_boundary` (`computed_at`/`address` = B, fixed
once at the start of a fresh run), and `erc20_balance_backfill_whales`
(`block_number` = the last `range_to` the whale walk has applied). All
three are deleted on completion, when the real `erc20_balance` /
`erc20_balance_safes` watermarks are written in the same transaction as
the `TokenHolding` rebuild.

**The whale list is only refreshed from `pg_stats` on a fresh run** (no
progress rows yet, or right after `--restart`) — never on a resume. See
`run_chunk_slice`'s comment for why: a `pg_stats` whale discovered
mid-run could already have been seeded as an ordinary Safe by an earlier,
already-committed chunk of this same run, and letting the walk sum it too
would double-count that history. Run-time detection
(`detect_runtime_whales`) stays safe across resumes because it only ever
looks at chunks that have not been seeded yet.

**Run-time detection (P8) fires only when a chunk's seed times out, not
ahead of every chunk.** Berachain staging EXPLAIN showed the bounded-count
detection query reading the same index entries the seed SUM reads right
after it -- 4.3s cold vs. 0.17s warm -- so unconditional detection paid
that cost on every chunk even though 45 of 50 chunks held no whale at
all. `run_chunk_slice` now seeds optimistically inside the chunk's own
`transaction.atomic()` / `SET LOCAL statement_timeout`; only on
`OperationalError` from a cancelled statement (see
`_is_statement_timeout`, `analytics/services/analytics_service.py`) does
it roll back, run `detect_runtime_whales` on the chunk's non-whale
addresses, and retry once without the offenders it finds. A second
timeout on the retry raises `CommandError` without advancing the progress
watermark -- see `analytics/implementation-notes.md` for the numbers.

**Celery mode (P7, `docs/specs/token-holdings.md` §12).** `--celery`
dispatches the same walk as a self-continuing chain of Celery tasks on the
`contracts` queue (`tasks.py`:
`backfill_erc20_balance_chunk_task` / `_whale_task` / `_finish_task`) so
the run survives the shell that started it — mirrors
`backfill_native_balances.py --celery`'s `--wait` / `--status` /
`--run-id` shape. The orchestration below (`resolve_run`,
`run_chunk_slice`, `run_whale_slice`, `finish_run`, `read_boundary`,
`read_head`) is written as plain, module-level functions rather than
`Command` methods for exactly this reason: `--inline` calls them
unbounded (one call each, run to completion, holding the lock for the
whole run — unchanged from before P7); the Celery tasks call
`run_chunk_slice` / `run_whale_slice` bounded by `--task-chunks`, holding
the lock for one slice only and releasing it before dispatching the next
task. Both modes read and write the *same* three progress watermarks
(`_PROGRESS_WATERMARK`, `_BOUNDARY_WATERMARK`, `_WHALE_PROGRESS_WATERMARK`)
as the chain's only state — there is no separate Redis cursor the way
`backfill_native_balance_chunk` carries one, so an inline run interrupted
by Ctrl-C resumes cleanly under `--celery` and vice versa. See `tasks.py`'s
block comment above `ERC20_BALANCE_BACKFILL_RUN_KEY_PREFIX` for the
Redis run-manifest shape (`--status` / `--wait` reporting only) and for
the one place this deliberately does NOT mirror native: every Celery
slice here takes the shared rollup lock for its own duration (native's
chunk task never takes a lock at all), because a chunk-phase or
whale-walk slice must never overlap the nightly task's delta step
(spec edge case #10b).
"""

import argparse
import logging
import time
from datetime import datetime

from django.core.management.base import BaseCommand, CommandError
from django.db import OperationalError, connection, transaction
from django.utils import timezone

from hexbytes import HexBytes

from safe_transaction_service.analytics.models import (
    AnalyticsSnapshot,
    AnalyticsWatermark,
    Erc20BalanceWhale,
    SafeTokenBalance,
    TokenHolding,
)
from safe_transaction_service.analytics.services.analytics_service import (
    _is_statement_timeout,
)
from safe_transaction_service.analytics.tasks import (
    ERC20_BALANCE_BACKFILL_STALE_SECONDS,
    ERC20_BALANCE_SAFES_WATERMARK,
    ERC20_BALANCE_WATERMARK,
    NATIVE_BALANCE_SEED_BATCH_SIZE,
    _write_snapshot,
    build_erc20_balance_backfill_run,
    compute_erc20_balance_rollup_task,
    dispatch_erc20_balance_backfill_run,
    erc20_balance_backfill_looks_stalled,
    erc20_balance_head_block,
    erc20_balance_run_boundary,
    erc20_balance_seed_candidates,
    latest_erc20_balance_backfill_run_id,
    load_erc20_balance_backfill_run,
    rebuild_token_holdings,
    seed_missing_erc20_balances,
)
from safe_transaction_service.history.models import EthereumBlock, SafeContract
from safe_transaction_service.utils.redis import get_redis
from safe_transaction_service.utils.tasks import LOCK_TIMEOUT, get_task_lock_name

logger = logging.getLogger(__name__)

# Own watermark names -- deliberately distinct from `ERC20_BALANCE_WATERMARK`
# / `ERC20_BALANCE_SAFES_WATERMARK`, which are only ever written once, on
# completion, by `_finish`. A crash mid-run must never leave a *partial*
# value under the real watermark names, or the nightly task (or a second
# backfill invocation) could pick it up as "done".
_PROGRESS_WATERMARK = "erc20_balance_backfill"
_BOUNDARY_WATERMARK = "erc20_balance_backfill_boundary"
_WHALE_PROGRESS_WATERMARK = "erc20_balance_backfill_whales"

# Same reasoning as `NATIVE_BALANCE_SEED_BATCH_SIZE` for the default chunk
# size: one knob, not a separate ERC-20 copy of the same number.
DEFAULT_CHUNK_SIZE = NATIVE_BALANCE_SEED_BATCH_SIZE

# `optimism-balance-accuracy.paste.sql` section 1w (`pg_stats` most-common
# values, 2026-09-23): the dominant whale sits at ~35% of all rows; the
# next tier ("other Safes with 1-7.5M rows") is ~0.6%-4.4% of the ~172M-row
# table. 0.1% sits comfortably below that whole tier so pg_stats-visible
# whales are never missed, while staying well above the noise a normal
# active Safe produces. Starting value; needs a first look at a second
# chain (spec §11 Q7's caveat about other starting thresholds applies here
# too).
DEFAULT_WHALE_MIN_FREQUENCY = 0.001

# Run-time whale detection: the bounded-count trick below stops counting
# once a leg passes this many rows for the *whole chunk* (not per Safe).
# A stand-in for "this chunk is clearly whale-tainted, do not try to sum
# it with the ordinary `= ANY(...)` batch" -- generous enough that no
# ordinary chunk of `DEFAULT_CHUNK_SIZE` normal Safes trips it, but far
# below where an `= ANY(...)` batch actually stalls. Starting value; see
# `DEFAULT_WHALE_MIN_FREQUENCY`'s note.
DEFAULT_WHALE_ROW_THRESHOLD = 50_000

# Block span per whale range-query. ~500k blocks is ~11-12 days at
# Optimism's ~2s block time -- small enough that even a very dense whale
# range stays bounded, large enough that summing one whale's full history
# (H ~ a few hundred million blocks worst case) does not need an
# unreasonable number of round trips.
DEFAULT_WHALE_BLOCK_RANGE = 500_000

# Per-chunk `SET LOCAL statement_timeout`. Lower than
# `relaxed_statement_timeout`'s 30-minute default on purpose: a single
# chunk (Safes or one whale block-range) is meant to be bounded work run
# off-peak against a DB shared with the indexer, not a single long-running
# aggregate -- a chunk that needs longer than this is a signal to lower
# `--chunk-size` or `--whale-block-range`, not to raise the timeout first.
DEFAULT_STATEMENT_TIMEOUT_MS = 300_000

# --celery only: Safe chunks (seed phase) or whale block-ranges
# (whale-walk phase) processed inside ONE Celery task before it releases
# the lock and dispatches the next. Each chunk/range is normally "seconds,
# not minutes" (see `DEFAULT_CHUNK_SIZE`'s and `backfill_native_balances.py`'s
# own reasoning) and is itself capped by `--statement-timeout-ms`
# (default 300s); 20 of them per slice keeps a task's total work at a few
# minutes in the ordinary case, comfortably inside the
# `task_timeout(LOCK_TIMEOUT)` bound `tasks.py`'s three Celery tasks share
# with `backfill_native_balance_chunk` (`LOCK_TIMEOUT` defaults to 900s),
# while cutting the per-slice lock-acquire/dispatch overhead to 1/20th of
# doing one chunk per task the way the native chain does. A worst case
# where every one of the 20 sub-chunks hits its own statement timeout
# could still exceed the task timeout -- that is the signal to lower
# --task-chunks, not to raise LOCK_TIMEOUT first, same reasoning as
# --statement-timeout-ms above.
DEFAULT_TASK_CHUNKS = 20


def _positive_int(raw: str) -> int:
    """`argparse` `type=` for an option that becomes a loop step or a
    query `LIMIT` -- 0 or negative must never reach either. `--chunk-size
    0` would be harmless on its own (`LIMIT 0` just returns no
    candidates), but `--whale-block-range 0` is not: it makes
    `range_to == range_from` in `run_whale_slice`'s `while ... range_from
    < head` loop, which then never advances and spins forever (this is
    exactly what happened when a test derived a range of `head - 1` with
    `head == 1` -- see `run_whale_slice`'s own defensive check for the
    second half of the fix). Rejecting <= 0 for all three at the
    argument-parsing stage, uniformly, is cheaper to reason about than
    trusting every call site downstream to guard itself. Raising
    `argparse.ArgumentTypeError` here surfaces as `CommandError` through
    Django's `CommandParser.error()`, not a bare `SystemExit` -- so
    `call_command(...)` in tests can `assertRaises(CommandError)`.
    """
    value = int(raw)
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1, got {value}")
    return value


# ─────────────────── whale detection / summation SQL ───────────────────

# Mirrors `optimism-balance-accuracy.paste.sql` section 1w: most-common
# `to` / `_from` values from `pg_stats`, together with their share of
# `history_erc20transfer`. A sample, not an exact count -- can miss a
# whale ANALYZE hasn't seen yet, which is exactly why run-time detection
# (below) exists as a second line of defence.
_WHALE_MCV_SQL = """
SELECT
    unnest(s.most_common_vals::text::text[]) AS addr_text,
    unnest(s.most_common_freqs) AS freq
FROM pg_stats s
WHERE s.tablename = 'history_erc20transfer' AND s.attname IN ('to', '_from')
"""

_WHALE_SAFE_INTERSECT_SQL = """
SELECT address FROM history_safecontract WHERE address = ANY(%s)
"""

# Bounded row count for a set of addresses, both legs. `LIMIT` inside each
# subquery means the planner stops scanning the moment it has enough rows
# to answer "is this over the threshold", instead of counting a whale's
# full history -- the same trick the spec's design notes describe for
# run-time whale detection. Passing a single address in `addresses`
# repurposes this for the per-Safe follow-up check.
_WHALE_BOUNDED_COUNT_SQL = """
SELECT
    (SELECT COUNT(*) FROM (
        SELECT 1 FROM history_erc20transfer WHERE "to" = ANY(%s) LIMIT %s
    ) x) AS to_count,
    (SELECT COUNT(*) FROM (
        SELECT 1 FROM history_erc20transfer WHERE "_from" = ANY(%s) LIMIT %s
    ) y) AS from_count
"""

# The whale set for one walk: whale Safes bound by this run's boundary B,
# same rule the chunk loop's own candidates obey (`(created, address) <=
# B`) -- a whale Safe the indexer inserts mid-run must be excluded from
# this run's walk exactly like any other Safe past B, and picked up by
# the next run instead.
_WHALE_UPTO_BOUNDARY_SQL = """
SELECT w.safe_address
FROM analytics_erc20balancewhale w
JOIN history_safecontract sc ON sc.address = w.safe_address
WHERE (sc.created, sc.address) <= (%(boundary_created)s::timestamptz, %(boundary_address)s::bytea)
"""

# Whale summation, driven by block ranges through `history_ethereumtx.block_id`
# -- the same `CROSS JOIN LATERAL` access path `_ERC20_BALANCE_UPSERT_SQL`
# (analytics/tasks.py) uses for the nightly delta, filtered to the whale
# addresses instead of joined to every Safe. Deliberately NOT driven by the
# `to`/`_from` index with a `block_number` residual filter (the pattern
# `_ERC20_BALANCES_UPTO_BLOCK_SQL` uses for ordinary Safes): for a whale
# address, that index scan re-reads the whole multi-million-row history on
# every range instead of touching only the transactions inside it.
#
# UPSERTs directly (`balance = balance + delta`), unlike the ordinary
# per-Safe seed path -- there is no separate "insert" step here because a
# whale's balance is built up over many ranges, one call per range, not
# computed once and inserted. Zero-balance cleanup happens once, after
# every range has been applied (`delete_zero_whale_pairs`), not per range:
# a whale pair can legitimately sit at zero mid-walk and become non-zero
# again in a later range.
_ERC20_BALANCE_WHALE_UPSERT_SQL = """
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
    WHERE etx.block_id > %(range_from)s AND etx.block_id <= %(range_to)s
      AND flows.addr = ANY(%(whales)s)
    GROUP BY flows.addr, flows.token
)
INSERT INTO analytics_safetokenbalance (safe_address, token_address, balance)
SELECT addr, token, delta FROM delta
ON CONFLICT (safe_address, token_address)
DO UPDATE SET balance = analytics_safetokenbalance.balance + EXCLUDED.balance
"""

_ERC20_BALANCE_WHALE_DELETE_ZERO_SQL = """
DELETE FROM analytics_safetokenbalance
WHERE safe_address = ANY(%s) AND balance = 0
"""


def _whale_candidates_from_pg_stats(min_frequency: float) -> set[bytes]:
    """Addresses from `pg_stats`' most-common `to`/`_from` values at or
    above `min_frequency`, as raw 20-byte addresses. See `_WHALE_MCV_SQL`.
    """
    with connection.cursor() as cursor:
        cursor.execute(_WHALE_MCV_SQL)
        rows = cursor.fetchall()
    candidates: set[bytes] = set()
    for addr_text, freq in rows:
        if addr_text is None or freq is None or float(freq) < min_frequency:
            continue
        # `bytea::text` renders as Postgres's hex output format, `\x...`.
        hex_part = addr_text[2:] if addr_text.startswith("\\x") else addr_text
        try:
            candidates.add(bytes.fromhex(hex_part))
        except ValueError:
            continue
    return candidates


def refresh_whale_list(min_frequency: float) -> int:
    """Seed `Erc20BalanceWhale` from `pg_stats`, intersected with
    `history_safecontract` (a frequent counterparty that isn't a Safe is
    not a whale *Safe*). Additive -- never removes an existing entry,
    including ones added by run-time detection in an earlier invocation.
    Returns the number of newly added rows.
    """
    candidates = _whale_candidates_from_pg_stats(min_frequency)
    if not candidates:
        return 0
    with connection.cursor() as cursor:
        cursor.execute(_WHALE_SAFE_INTERSECT_SQL, [list(candidates)])
        safe_addresses = [bytes(row[0]) for row in cursor.fetchall()]
    added = 0
    for address in safe_addresses:
        _, created = Erc20BalanceWhale.objects.get_or_create(safe_address=address)
        if created:
            added += 1
    return added


def _bounded_transfer_row_count(addresses: list[bytes], limit: int) -> int:
    """Rows touching `addresses` on either leg, each leg capped at
    `limit` -- see `_WHALE_BOUNDED_COUNT_SQL`."""
    if not addresses:
        return 0
    with connection.cursor() as cursor:
        cursor.execute(_WHALE_BOUNDED_COUNT_SQL, [addresses, limit, addresses, limit])
        to_count, from_count = cursor.fetchone()
    return int(to_count) + int(from_count)


def detect_runtime_whales(addresses: list[bytes], row_threshold: int) -> list[bytes]:
    """Safes in `addresses` whose transfer history alone would blow the
    bounded-count trick, i.e. whales `pg_stats` missed. Only runs the
    expensive per-address pass when the whole-chunk bound is exceeded,
    since that is the rare case. Newly found whales are recorded in
    `Erc20BalanceWhale` so later chunks (and the weekly drift check) see
    them too.
    """
    if not addresses:
        return []
    if _bounded_transfer_row_count(addresses, row_threshold + 1) <= row_threshold:
        return []
    offenders = [
        address
        for address in addresses
        if _bounded_transfer_row_count([address], row_threshold + 1) > row_threshold
    ]
    for address in offenders:
        Erc20BalanceWhale.objects.get_or_create(safe_address=address)
    return offenders


def whale_addresses_upto_boundary(boundary: tuple) -> list[bytes]:
    """Whale Safes (`Erc20BalanceWhale`) that exist in `history_safecontract`
    with `(created, address) <= boundary` -- the fixed set one whale walk
    covers. Re-run at the point the walk starts (after the chunk loop, on
    every invocation of a still-running walk): see the module docstring
    for why this yields the identical set across resumes of the same walk
    without needing to persist it separately.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            _WHALE_UPTO_BOUNDARY_SQL,
            {"boundary_created": boundary[0], "boundary_address": boundary[1]},
        )
        return [bytes(row[0]) for row in cursor.fetchall()]


def apply_whale_range(
    whale_addresses: list[bytes], range_from: int, range_to: int
) -> int:
    """UPSERT the net flow of `(range_from, range_to]` for `whale_addresses`
    into `analytics_safetokenbalance`. Returns the number of pairs
    touched. Must be called inside the caller's own `transaction.atomic()`
    -- see `_ERC20_BALANCE_WHALE_UPSERT_SQL`.
    """
    if not whale_addresses:
        return 0
    with connection.cursor() as cursor:
        cursor.execute(
            _ERC20_BALANCE_WHALE_UPSERT_SQL,
            {
                "range_from": range_from,
                "range_to": range_to,
                "whales": whale_addresses,
            },
        )
        return cursor.rowcount


def delete_zero_whale_pairs(whale_addresses: list[bytes]) -> int:
    """Delete `analytics_safetokenbalance` rows for `whale_addresses` that
    netted to zero, once a whale walk has applied every range. See
    `_ERC20_BALANCE_WHALE_DELETE_ZERO_SQL`."""
    if not whale_addresses:
        return 0
    with connection.cursor() as cursor:
        cursor.execute(_ERC20_BALANCE_WHALE_DELETE_ZERO_SQL, [whale_addresses])
        return cursor.rowcount


# ─────────────── shared orchestration (--inline and --celery) ──────────
#
# Plain, module-level functions -- not `Command` methods -- so
# `tasks.py`'s three Celery tasks can import and call them directly (see
# the module docstring's "Celery mode" section). `--inline` calls each of
# `run_chunk_slice` / `run_whale_slice` exactly once, unbounded
# (`max_chunks=None` / `max_ranges=None`), which reproduces the pre-P7
# behaviour exactly: one call runs the whole phase to completion under
# the single lock `Command.handle` holds for the entire run. The Celery
# tasks call the same functions bounded by `--task-chunks`, under a lock
# they acquire and release themselves once per slice.


def resolve_run(resuming: bool) -> tuple[int, tuple, tuple | None]:
    """Decide `(H, B, cursor)` for one call into the seed phase: fixed
    fresh on a first call (or right after `--restart`), reused from the
    progress watermarks when `resuming` is true. `resuming` is decided by
    the caller (`run_chunk_slice`) before this runs, since it also gates
    whether the whale list gets refreshed.
    """
    if resuming:
        progress = AnalyticsWatermark.objects.get(name=_PROGRESS_WATERMARK)
        boundary_row = AnalyticsWatermark.objects.get(name=_BOUNDARY_WATERMARK)
        head = progress.block_number
        cursor = (
            (progress.computed_at, HexBytes(progress.address))
            if progress.address is not None
            else None
        )
        boundary = (boundary_row.computed_at, HexBytes(boundary_row.address))
        return head, boundary, cursor

    head = erc20_balance_head_block()
    if head is None:
        raise CommandError(
            "No block is confirmed beyond the reorg depth yet -- "
            "nothing can be safely computed. Let the indexer and "
            "check_reorgs catch up first."
        )
    boundary = erc20_balance_run_boundary()
    now = timezone.now()
    AnalyticsWatermark.objects.update_or_create(
        name=_PROGRESS_WATERMARK,
        defaults={"block_number": head, "computed_at": now, "address": None},
    )
    AnalyticsWatermark.objects.update_or_create(
        name=_BOUNDARY_WATERMARK,
        defaults={
            "block_number": head,
            "computed_at": boundary[0],
            "address": boundary[1],
        },
    )
    return head, boundary, None


def read_boundary() -> tuple[datetime, bytes]:
    """The boundary B a still-running backfill fixed at the start, read
    from `_BOUNDARY_WATERMARK`. Used by the whale-walk and finish Celery
    tasks, which run as separate process invocations from the chunk-phase
    task (or the inline call) that first wrote it -- unlike `--inline`,
    they cannot just keep the tuple in a local variable.
    """
    row = AnalyticsWatermark.objects.get(name=_BOUNDARY_WATERMARK)
    return row.computed_at, HexBytes(row.address)


def read_head() -> int:
    """The fixed head H a still-running backfill is computing balances as
    of, read from `_PROGRESS_WATERMARK`. Same cross-invocation reasoning
    as `read_boundary` -- callers must read this BEFORE `finish_run`,
    which deletes the row.
    """
    return AnalyticsWatermark.objects.get(name=_PROGRESS_WATERMARK).block_number


def count_upto_boundary(boundary: tuple) -> int:
    """Progress-display estimate only -- not on any correctness path, so
    an approximate address tie at the boundary is harmless."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM history_safecontract "
            "WHERE (created, address) <= (%s::timestamptz, %s::bytea)",
            [boundary[0], boundary[1]],
        )
        return int(cursor.fetchone()[0] or 0)


def run_chunk_slice(
    options: dict, lock, max_chunks: int | None = None, progress_cb=None
) -> dict:
    """Seed up to `max_chunks` Safe chunks (unbounded when `None` --
    `--inline` runs the whole seed phase in one such call; a Celery
    chunk-phase task passes `--task-chunks`).

    `lock` is threaded through rather than acquired here: the caller owns
    the lock's scope (the whole run for `--inline`; one slice for
    Celery) -- see the module docstring. Every sub-chunk still calls
    `lock.extend(...)`, so a long `--inline` run (many sub-chunks under
    one lock) keeps renewing it exactly as before P7.

    Whale-list refresh (`refresh_whale_list`) only happens when
    `_PROGRESS_WATERMARK` does not exist yet when THIS call starts --
    true both for the very first `--inline` chunk of a fresh run and for
    the very first Celery slice of one, false for every call after (the
    row is created inside `resolve_run` before any chunk is processed).
    A `pg_stats` whale discovered by a LATER call could already have been
    seeded as an ordinary Safe by an earlier, already-committed chunk of
    this same run, and refreshing again would sum its history a second
    time in the whale walk -- exactly the reason resuming skips it.

    `progress_cb(event, data)`, if given, is called with `"resuming"` (no
    data) before a resumed run's first chunk, `"started"` once per call
    right after `(H, B)` are resolved, and `"chunk"` after every
    sub-chunk -- the same three points `--inline`'s stdout output used to
    come from directly.
    """
    resuming = AnalyticsWatermark.objects.filter(name=_PROGRESS_WATERMARK).exists()
    added_whales = 0
    if resuming:
        if progress_cb:
            progress_cb("resuming", {})
    else:
        added_whales = refresh_whale_list(options["whale_min_frequency"])

    head, boundary, cursor = resolve_run(resuming)
    if progress_cb:
        progress_cb(
            "started",
            {
                "head": head,
                "boundary": boundary,
                "resuming": resuming,
                "added_whales": added_whales,
                "chunk_size": options["chunk_size"],
            },
        )

    chunks_done = 0
    seen = 0
    seeded_safes_total = 0
    pairs_inserted_total = 0
    skipped_whales_total = 0
    finished_seeding = False

    while max_chunks is None or chunks_done < max_chunks:
        chunk_started = time.time()
        candidates = erc20_balance_seed_candidates(
            cursor, boundary, options["chunk_size"]
        )
        if not candidates:
            finished_seeding = True
            break

        addresses = [addr for addr, _created in candidates]
        last_address, last_created = candidates[-1]

        # Whales in this chunk are skipped here, not seeded -- see the
        # module docstring. The progress cursor still advances past them
        # below (it always covers the whole chunk), so they are never
        # re-offered to a later chunk; the whale walk is what actually
        # sums them.
        known_whales = {
            HexBytes(a)
            for a in Erc20BalanceWhale.objects.values_list("safe_address", flat=True)
        }
        non_whales = [a for a in addresses if a not in known_whales]
        whale_count = len(addresses) - len(non_whales)

        # P8 (docs/specs/token-holdings.md §12): no run-time whale
        # detection ahead of the seed. Berachain staging EXPLAIN showed
        # the bounded-count detection query (`_WHALE_BOUNDED_COUNT_SQL`)
        # reading the *same* index entries as the seed SUM right after it
        # -- 4.3s cold vs. 0.17s warm -- so paying for detection on every
        # chunk doubled the I/O of the 45 (of 50) chunks that turned out
        # to hold no whale at all. Seed optimistically instead, and only
        # fall back to detection when the seed itself proves there is a
        # problem by hitting `statement_timeout`.
        seed_timed_out = False
        newly_detected: list[bytes] = []
        try:
            with transaction.atomic():
                with connection.cursor() as timeout_cursor:
                    timeout_cursor.execute(
                        "SET LOCAL statement_timeout = %s",
                        [options["statement_timeout_ms"]],
                    )

                seeded, inserted = seed_missing_erc20_balances(non_whales, head)

                AnalyticsWatermark.objects.update_or_create(
                    name=_PROGRESS_WATERMARK,
                    defaults={
                        "block_number": head,
                        "computed_at": last_created,
                        "address": last_address,
                    },
                )
        except OperationalError as exc:
            if not _is_statement_timeout(exc):
                raise
            seed_timed_out = True

        if seed_timed_out:
            # The atomic block above rolled back on the exception -- the
            # progress watermark still points at the previous chunk.
            # Detection now runs once, on exactly the addresses that just
            # timed out (minus whales already known), and any offender it
            # finds is both recorded in `Erc20BalanceWhale` (inside
            # `detect_runtime_whales`) and excluded from the retry.
            newly_detected = detect_runtime_whales(
                non_whales, options["whale_row_threshold"]
            )
            newly_detected_set = set(newly_detected)
            retry_non_whales = [a for a in non_whales if a not in newly_detected_set]
            whale_count += len(newly_detected)
            logger.warning(
                "erc20_balance.backfill: chunk at (%s, 0x%s): seed timed "
                "out, run-time whale detection flagged %d address(es); "
                "retrying without them",
                last_created.isoformat(),
                last_address.hex(),
                len(newly_detected),
            )

            try:
                with transaction.atomic():
                    with connection.cursor() as timeout_cursor:
                        timeout_cursor.execute(
                            "SET LOCAL statement_timeout = %s",
                            [options["statement_timeout_ms"]],
                        )

                    seeded, inserted = seed_missing_erc20_balances(
                        retry_non_whales, head
                    )

                    AnalyticsWatermark.objects.update_or_create(
                        name=_PROGRESS_WATERMARK,
                        defaults={
                            "block_number": head,
                            "computed_at": last_created,
                            "address": last_address,
                        },
                    )
            except OperationalError as exc:
                if not _is_statement_timeout(exc):
                    raise
                # Never loop more than once: a second timeout after
                # run-time whale detection means the chunk itself is too
                # heavy for --statement-timeout-ms, not that another
                # whale is hiding in it.
                raise CommandError(
                    "Chunk at "
                    f"({last_created.isoformat()}, 0x{last_address.hex()}) "
                    "timed out again after run-time whale detection and a "
                    "retry without the offender(s) it found "
                    f"({len(newly_detected)} address(es) flagged this "
                    "attempt). Lower --chunk-size or raise "
                    "--statement-timeout-ms and re-run (the progress "
                    "cursor was not advanced, so this chunk will be "
                    "retried)."
                ) from exc

        cursor = (last_created, last_address)
        chunks_done += 1
        seen += len(addresses)
        seeded_safes_total += seeded
        pairs_inserted_total += inserted
        skipped_whales_total += whale_count

        # Outlives LOCK_TIMEOUT on any run with more than a trivial number
        # of chunks under one lock -- true for the whole `--inline` run,
        # and possible within one Celery slice too when --task-chunks is
        # large -- so it must be renewed after every sub-chunk.
        if lock is not None:
            lock.extend(LOCK_TIMEOUT, replace_ttl=True)

        if progress_cb:
            progress_cb(
                "chunk",
                {
                    "chunk_index": chunks_done,
                    "seen": seen,
                    "seeded": seeded,
                    "whale_skipped": whale_count,
                    "inserted": inserted,
                    "elapsed": time.time() - chunk_started,
                    "newly_detected": len(newly_detected),
                    "seed_timed_out": seed_timed_out,
                },
            )

    return {
        "resuming": resuming,
        "added_whales": added_whales,
        "head": head,
        "boundary": boundary,
        "cursor": cursor,
        "chunks_done": chunks_done,
        "seen": seen,
        "seeded_safes": seeded_safes_total,
        "pairs_inserted": pairs_inserted_total,
        "whale_skipped": skipped_whales_total,
        "finished_seeding": finished_seeding,
    }


def run_whale_slice(
    whale_addresses: list[bytes],
    head: int,
    options: dict,
    lock,
    max_ranges: int | None = None,
    progress_cb=None,
) -> dict:
    """Apply up to `max_ranges` whale block-ranges of `(0, head]`
    (unbounded when `None` -- `--inline` runs the whole walk in one such
    call; a Celery whale-phase task passes `--task-chunks`).

    Resumable via `_WHALE_PROGRESS_WATERMARK` (`block_number` = the last
    `range_to` already applied) exactly like the chunk phase is resumable
    via `_PROGRESS_WATERMARK` -- a crash (or, in Celery mode, simply the
    end of one bounded slice) leaves that row in place, and the next call
    picks the walk back up from there. Deleting the zero-balance pairs
    only happens once the walk reaches `head` (`finished=True` in the
    return value), never mid-walk -- see `apply_whale_range`'s docstring.
    """
    if not whale_addresses:
        AnalyticsWatermark.objects.filter(name=_WHALE_PROGRESS_WATERMARK).delete()
        return {"ranges_done": 0, "rows_touched": 0, "finished": True, "deleted": 0}

    progress = AnalyticsWatermark.objects.filter(name=_WHALE_PROGRESS_WATERMARK).first()
    range_from = progress.block_number if progress is not None else 0
    ranges_done = 0
    rows_touched = 0

    while (max_ranges is None or ranges_done < max_ranges) and range_from < head:
        range_to = min(range_from + options["whale_block_range"], head)
        if range_to <= range_from:
            # Unreachable given --whale-block-range's >= 1 validation
            # (`_positive_int`) -- kept as the last line of defence
            # against a step that never advances `range_from`, which
            # would otherwise spin forever. See `_positive_int`'s
            # docstring for the incident this guards.
            raise CommandError(
                f"Whale walk made no progress: range_from={range_from}, "
                f"range_to={range_to}, --whale-block-range="
                f"{options['whale_block_range']}. Refusing to loop "
                "forever."
            )
        range_started = time.time()
        with transaction.atomic():
            with connection.cursor() as timeout_cursor:
                timeout_cursor.execute(
                    "SET LOCAL statement_timeout = %s",
                    [options["statement_timeout_ms"]],
                )
            rows_touched += apply_whale_range(whale_addresses, range_from, range_to)
            AnalyticsWatermark.objects.update_or_create(
                name=_WHALE_PROGRESS_WATERMARK,
                defaults={"block_number": range_to, "computed_at": timezone.now()},
            )
        range_from = range_to
        ranges_done += 1
        if lock is not None:
            lock.extend(LOCK_TIMEOUT, replace_ttl=True)
        if progress_cb:
            progress_cb(
                "whale_range",
                {
                    "range_from_now": range_from,
                    "head": head,
                    "whale_count": len(whale_addresses),
                    "elapsed": time.time() - range_started,
                },
            )

    finished = range_from >= head
    deleted = delete_zero_whale_pairs(whale_addresses) if finished else None
    return {
        "ranges_done": ranges_done,
        "rows_touched": rows_touched,
        "finished": finished,
        "deleted": deleted,
    }


def finish_run(head: int, boundary: tuple) -> dict:
    """Write the real `erc20_balance` / `erc20_balance_safes` watermarks,
    drop the three progress watermarks, and rebuild `TokenHolding` plus
    the `token_holdings` snapshot -- all in one transaction. Called
    exactly once per run, after both the chunk phase and the whale walk
    report `finished`.
    """
    now = timezone.now()
    as_of_timestamp = (
        EthereumBlock.objects.filter(number=head)
        .values_list("timestamp", flat=True)
        .first()
        or now
    )
    with transaction.atomic():
        AnalyticsWatermark.objects.update_or_create(
            name=ERC20_BALANCE_WATERMARK,
            defaults={"block_number": head, "computed_at": now},
        )
        AnalyticsWatermark.objects.update_or_create(
            name=ERC20_BALANCE_SAFES_WATERMARK,
            defaults={
                "block_number": head,
                "computed_at": boundary[0],
                "address": boundary[1],
            },
        )
        AnalyticsWatermark.objects.filter(
            name__in=[
                _PROGRESS_WATERMARK,
                _BOUNDARY_WATERMARK,
                _WHALE_PROGRESS_WATERMARK,
            ]
        ).delete()
        chain_level = rebuild_token_holdings(head, as_of_timestamp, now)
        _write_snapshot(
            "token_holdings",
            {
                "as_of_block": head,
                "as_of_timestamp": as_of_timestamp.isoformat(),
                **chain_level,
            },
        )
    return {
        "head": head,
        "boundary": boundary,
        "as_of_timestamp": as_of_timestamp,
        **chain_level,
    }


class Command(BaseCommand):
    help = (
        "Fill analytics_safetokenbalance for every Safe as of one fixed "
        "block H, then write the `erc20_balance` / `erc20_balance_safes` "
        "watermarks so the nightly rollup can take over. Resumable: a "
        "crash or Ctrl-C leaves progress in the `erc20_balance_backfill` "
        "watermark, and re-running the same command continues from there "
        "-- H and the boundary B are fixed once, at the start of a fresh "
        "run, and reused on every resume. Runs INLINE by default: holds "
        "the same Redis lock as `compute_erc20_balance_rollup_task` for "
        "the whole run, so it refuses to start (and the nightly task "
        "refuses to run) while the other is in progress. --celery "
        "dispatches the same walk as a self-continuing chain of Celery "
        "tasks on the `contracts` queue instead, each task holding that "
        "lock for one bounded slice only, so the run survives this shell "
        "exiting; --wait N follows it. --status reports progress (the "
        "watermarks, plus the most recent --celery run) without writing "
        "anything. --restart empties the rollup and starts over."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--chunk-size",
            type=_positive_int,
            default=DEFAULT_CHUNK_SIZE,
            help=(
                f"Safes per chunk (default {DEFAULT_CHUNK_SIZE}, the same "
                "batch size the native backfill and the nightly rollup's "
                "own `= ANY(...)` queries use)."
            ),
        )
        parser.add_argument(
            "--restart",
            action="store_true",
            help=(
                "TRUNCATE analytics_safetokenbalance and "
                "analytics_tokenholding, delete the erc20_balance / "
                "erc20_balance_safes / progress watermarks and the "
                "token_holdings snapshot, then compute every Safe from "
                "scratch at a freshly taken H. The whale list "
                "(Erc20BalanceWhale) is refreshed, not wiped -- it is a "
                "durable allow-list the drift check also relies on. Use "
                "after an upstream `reindex_erc20`, a drift mismatch, or "
                "on a new chain."
            ),
        )
        parser.add_argument(
            "--whale-min-frequency",
            type=float,
            default=DEFAULT_WHALE_MIN_FREQUENCY,
            help=(
                "Minimum pg_stats most-common-value frequency (fraction of "
                f"history_erc20transfer rows) to seed the whale list with "
                f"(default {DEFAULT_WHALE_MIN_FREQUENCY}). See the module "
                "docstring / DEFAULT_WHALE_MIN_FREQUENCY comment for how "
                "this was picked from the Optimism probe."
            ),
        )
        parser.add_argument(
            "--whale-row-threshold",
            type=_positive_int,
            default=DEFAULT_WHALE_ROW_THRESHOLD,
            help=(
                "Bounded transfer-row count (per chunk, then per Safe) "
                f"above which a Safe is treated as a whale at run time "
                f"(default {DEFAULT_WHALE_ROW_THRESHOLD}). A starting "
                "value -- see DEFAULT_WHALE_ROW_THRESHOLD's comment."
            ),
        )
        parser.add_argument(
            "--whale-block-range",
            type=_positive_int,
            default=DEFAULT_WHALE_BLOCK_RANGE,
            help=(
                "Block span per whale range-query "
                f"(default {DEFAULT_WHALE_BLOCK_RANGE})."
            ),
        )
        parser.add_argument(
            "--statement-timeout-ms",
            type=int,
            default=DEFAULT_STATEMENT_TIMEOUT_MS,
            help=(
                "Postgres statement_timeout applied (SET LOCAL, so it "
                "only covers one chunk's own transaction) while a chunk "
                f"runs (default {DEFAULT_STATEMENT_TIMEOUT_MS}ms)."
            ),
        )
        parser.add_argument(
            "--status",
            action="store_true",
            help=(
                "Print backfill progress and exit. Writes nothing. Covers "
                "both the resumable progress watermarks (chunk phase, "
                "whale walk) and the most recent --celery run."
            ),
        )
        parser.add_argument(
            "--celery",
            action="store_true",
            help=(
                "Dispatch the walk to the `contracts` queue instead of "
                "running it here: up to --task-chunks Safe chunks (or, "
                "once seeding is done, whale block-ranges) per task, each "
                "task holding the same rollup lock as the nightly task "
                "for its own slice only and releasing it before "
                "dispatching the next. The run survives this shell "
                "exiting -- follow it with --status. Refused while "
                "another --celery run is in progress (progress rows "
                "exist and its manifest says running); pass --restart to "
                "wipe and start over instead."
            ),
        )
        parser.add_argument(
            "--wait",
            type=int,
            default=0,
            help=(
                "--celery only: poll Redis for up to N seconds, printing "
                "progress, then return. 0 (default) starts the run and "
                "returns immediately; the run continues on the worker. "
                "Exits non-zero if the run is still going when N elapses."
            ),
        )
        parser.add_argument(
            "--poll-interval",
            type=int,
            default=15,
            help="--celery with --wait: seconds between polls (default 15).",
        )
        parser.add_argument(
            "--run-id",
            help=(
                "--celery only: explicit run id (default: a timestamp "
                "plus a random suffix). Shown in the output and accepted "
                "by --status."
            ),
        )
        parser.add_argument(
            "--task-chunks",
            type=_positive_int,
            default=DEFAULT_TASK_CHUNKS,
            help=(
                "--celery only: Safe chunks (seed phase) or whale "
                "block-ranges (whale-walk phase) processed inside one "
                f"Celery task before it dispatches the next (default "
                f"{DEFAULT_TASK_CHUNKS}). See DEFAULT_TASK_CHUNKS's "
                "comment for how this was picked."
            ),
        )

    # ────────────────────────────── entry ──────────────────────────────

    def handle(self, *args, **options):
        if options["status"]:
            return self._print_status()

        if options["celery"]:
            return self._run_celery(options)

        lock = get_redis().lock(
            get_task_lock_name(compute_erc20_balance_rollup_task.name),
            blocking=False,
            timeout=LOCK_TIMEOUT,
        )
        if not lock.acquire(blocking=False):
            raise CommandError(
                "Could not acquire the erc20_balance rollup lock -- the "
                "nightly task or another backfill run is already in "
                "progress. Exiting without touching anything."
            )
        try:
            self._run(options, lock)
        finally:
            lock.release()

    # ─────────────────────────────── run ───────────────────────────────

    def _run(self, options, lock):
        if options["restart"]:
            self._restart()

        watermark_present = AnalyticsWatermark.objects.filter(
            name=ERC20_BALANCE_WATERMARK
        ).exists()
        if watermark_present and not options["restart"]:
            raise CommandError(
                "erc20_balance is already initialised; the nightly task "
                "maintains it from here. Use --restart to rebuild from "
                "scratch."
            )

        started = time.time()
        self._chunk_total_estimate = 0

        # `max_chunks=None` / `max_ranges=None`: run each phase to
        # completion in one call, under the single lock `handle()` holds
        # for the whole invocation -- identical to the pre-P7 behaviour.
        # See `run_chunk_slice`'s docstring for the shared shape with
        # Celery mode's bounded calls.
        result = run_chunk_slice(
            options, lock, max_chunks=None, progress_cb=self._chunk_progress_cb
        )

        whale_addresses = whale_addresses_upto_boundary(result["boundary"])
        whale_result = run_whale_slice(
            whale_addresses,
            result["head"],
            options,
            lock,
            max_ranges=None,
            progress_cb=self._whale_progress_cb,
        )
        if whale_result["deleted"] is not None:
            self.stdout.write(
                f"Whale walk done: {whale_result['ranges_done']} range(s) "
                f"this invocation, {whale_result['rows_touched']} pairs "
                f"touched, {whale_result['deleted']} zero pairs deleted."
            )

        chain_level = finish_run(result["head"], result["boundary"])
        self.stdout.write(
            f"Watermarks set: erc20_balance={chain_level['head']}, "
            f"erc20_balance_safes=({chain_level['boundary'][0].isoformat()}, "
            f"0x{chain_level['boundary'][1].hex()}). tokens_with_holders="
            f"{chain_level['tokens_with_holders']} safes_with_any_erc20="
            f"{chain_level['safes_with_any_erc20']}"
        )

        self.stdout.write(
            self.style.SUCCESS(
                f"Done in {time.time() - started:.1f}s: "
                f"{result['seeded_safes'] + len(whale_addresses)} Safes "
                f"seeded ({result['whale_skipped']} whale skip(s) during "
                f"chunking, {len(whale_addresses)} whale(s) summed by the "
                f"walk), {result['pairs_inserted'] + whale_result['rows_touched']} "
                f"pairs touched, {result['chunks_done']} chunks. "
                "`compute_erc20_balance_rollup_task` takes it from here "
                "(daily at 03:45 UTC), or run it now with `manage.py "
                "shell -c 'from safe_transaction_service.analytics.tasks "
                "import compute_erc20_balance_rollup_task as t; "
                "t.delay()'`."
            )
        )

    # ─────────────────────── --inline progress output ────────────────────

    def _chunk_progress_cb(self, event: str, data: dict) -> None:
        if event == "resuming":
            self.stdout.write(
                "Resuming: leaving the whale list as pg_stats last left it "
                "-- a Safe pg_stats now considers a whale could already "
                "have been seeded as an ordinary Safe by an earlier, "
                "already-committed chunk of this run, and refreshing here "
                "would sum its history a second time in the walk below. "
                "Run-time detection during the chunk loop stays safe: it "
                "only ever looks at chunks that have not been seeded yet."
            )
        elif event == "started":
            head, boundary = data["head"], data["boundary"]
            self.stdout.write(
                f"Backfilling ERC-20 balances as of block {head}, boundary "
                f"({boundary[0].isoformat()}, 0x{boundary[1].hex()}), "
                f"{data['chunk_size']} Safes per chunk."
            )
            if data["added_whales"]:
                self.stdout.write(
                    f"Whale list: {data['added_whales']} address(es) added."
                )
            self._chunk_total_estimate = count_upto_boundary(boundary)
            self.stdout.flush()
        elif event == "chunk":
            if data.get("seed_timed_out"):
                self.stdout.write(
                    self.style.WARNING(
                        f"  chunk {data['chunk_index']}: seed timed out, "
                        "run-time whale detection flagged "
                        f"{data['newly_detected']} address(es)."
                    )
                )
            self.stdout.write(
                f"  [{data['seen']}/{self._chunk_total_estimate}] chunk "
                f"{data['chunk_index']}: {data['seeded']} Safes seeded "
                f"({data['whale_skipped']} whale skipped), "
                f"{data['inserted']} pairs inserted, "
                f"{data['elapsed']:.1f}s"
            )
            self.stdout.flush()

    def _whale_progress_cb(self, event: str, data: dict) -> None:
        self.stdout.write(
            f"  whale walk: block {data['range_from_now']}/{data['head']} "
            f"({data['whale_count']} whales), {data['elapsed']:.1f}s"
        )
        self.stdout.flush()

    # ───────────────────────────── restart ─────────────────────────────

    def _restart(self):
        self.stdout.write(
            self.style.WARNING(
                "--restart: emptying analytics_safetokenbalance and "
                "analytics_tokenholding"
            )
        )
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE analytics_safetokenbalance")
            cursor.execute("TRUNCATE TABLE analytics_tokenholding")
        AnalyticsWatermark.objects.filter(
            name__in=[
                ERC20_BALANCE_WATERMARK,
                ERC20_BALANCE_SAFES_WATERMARK,
                _PROGRESS_WATERMARK,
                _BOUNDARY_WATERMARK,
                _WHALE_PROGRESS_WATERMARK,
            ]
        ).delete()
        AnalyticsSnapshot.objects.filter(name="token_holdings").delete()
        # Erc20BalanceWhale is deliberately NOT touched here -- it is
        # refreshed (see refresh_whale_list), not wiped.

    # ─────────────────────────────── celery ────────────────────────────

    def _run_celery(self, options):
        """Start a chunked-then-walked-then-finished run on the
        `contracts` queue and optionally watch it. See the module
        docstring's "Celery mode" section for why this is safe to
        interleave with `--inline`: both write the same three progress
        watermarks.
        """
        if options["restart"]:
            lock = get_redis().lock(
                get_task_lock_name(compute_erc20_balance_rollup_task.name),
                blocking=False,
                timeout=LOCK_TIMEOUT,
            )
            if not lock.acquire(blocking=False):
                raise CommandError(
                    "Could not acquire the erc20_balance rollup lock -- "
                    "the nightly task or another backfill run is already "
                    "in progress. Exiting without touching anything."
                )
            try:
                self._restart()
            finally:
                lock.release()
        else:
            watermark_present = AnalyticsWatermark.objects.filter(
                name=ERC20_BALANCE_WATERMARK
            ).exists()
            if watermark_present:
                raise CommandError(
                    "erc20_balance is already initialised; the nightly "
                    "task maintains it from here. Use --restart to "
                    "rebuild from scratch."
                )
            progress_present = AnalyticsWatermark.objects.filter(
                name=_PROGRESS_WATERMARK
            ).exists()
            if progress_present:
                latest_run_id = latest_erc20_balance_backfill_run_id()
                latest_run = (
                    load_erc20_balance_backfill_run(latest_run_id)
                    if latest_run_id
                    else None
                )
                if latest_run is not None and latest_run.get("state") == "running":
                    raise CommandError(
                        f"A backfill run ({latest_run['run_id']}) is "
                        "already in progress -- progress rows exist and "
                        "its manifest says running. Follow it with "
                        "--status, or pass --restart to wipe and start "
                        "over."
                    )
                self.stdout.write(
                    "Progress rows exist from an earlier, non-running "
                    "invocation (--inline, or a --celery run that failed "
                    "or lost its manifest); resuming them under --celery."
                )

        run = build_erc20_balance_backfill_run(
            chunk_size=options["chunk_size"],
            whale_min_frequency=options["whale_min_frequency"],
            whale_row_threshold=options["whale_row_threshold"],
            whale_block_range=options["whale_block_range"],
            statement_timeout_ms=options["statement_timeout_ms"],
            task_chunks=options["task_chunks"],
            run_id=options.get("run_id"),
        )
        run = dispatch_erc20_balance_backfill_run(run)

        self.stdout.write(
            f"Started run {run['run_id']} on the `contracts` queue: up to "
            f"{options['task_chunks']} chunks (or whale ranges) per task, "
            f"{options['chunk_size']} Safes per chunk."
        )
        self.stdout.write("Follow it with: manage.py backfill_erc20_balances --status")

        if not options["wait"]:
            return

        deadline = time.time() + options["wait"]
        while time.time() < deadline:
            run = load_erc20_balance_backfill_run(run["run_id"]) or run
            self._print_run(run)
            if run.get("state") != "running":
                return
            time.sleep(min(options["poll_interval"], max(deadline - time.time(), 0)))

        run = load_erc20_balance_backfill_run(run["run_id"]) or run
        if run.get("state") == "running":
            raise CommandError(
                f"Run {run['run_id']} is still going after "
                f"{options['wait']}s. It keeps running on the worker -- "
                "check with --status."
            )

    # ───────────────────────────── status ──────────────────────────────

    def _print_status(self):
        total_safes = SafeContract.objects.count()
        pair_rows = SafeTokenBalance.objects.count()
        tokens = TokenHolding.objects.count()
        whales = Erc20BalanceWhale.objects.count()
        watermark = AnalyticsWatermark.objects.filter(
            name=ERC20_BALANCE_WATERMARK
        ).first()
        progress = AnalyticsWatermark.objects.filter(name=_PROGRESS_WATERMARK).first()
        boundary = AnalyticsWatermark.objects.filter(name=_BOUNDARY_WATERMARK).first()
        whale_progress = AnalyticsWatermark.objects.filter(
            name=_WHALE_PROGRESS_WATERMARK
        ).first()

        self.stdout.write(f"Safes in history_safecontract      : {total_safes}")
        self.stdout.write(f"Rows in analytics_safetokenbalance  : {pair_rows}")
        self.stdout.write(f"Tokens in analytics_tokenholding     : {tokens}")
        self.stdout.write(f"Whale addresses tracked              : {whales}")
        if watermark is None:
            self.stdout.write(
                self.style.WARNING(
                    "erc20_balance watermark              : NOT SET -- "
                    "the nightly task will refuse to run until this "
                    "backfill finishes"
                )
            )
        else:
            self.stdout.write(
                f"erc20_balance watermark              : "
                f"{watermark.block_number} (at "
                f"{watermark.computed_at.isoformat()})"
            )
        if progress is None:
            self.stdout.write("Backfill progress (chunk phase)      : none in progress")
        else:
            self.stdout.write(
                f"Backfill progress (chunk phase)      : "
                f"head={progress.block_number} last=("
                f"{progress.computed_at.isoformat() if progress.address else '-'}, "
                f"{progress.address or '-'})"
            )
        if boundary is not None:
            self.stdout.write(
                f"Backfill boundary B                  : "
                f"({boundary.computed_at.isoformat()}, {boundary.address})"
            )
        if whale_progress is not None:
            self.stdout.write(
                f"Whale walk progress (range_to)       : {whale_progress.block_number}"
            )

        run_id = latest_erc20_balance_backfill_run_id()
        run = load_erc20_balance_backfill_run(run_id) if run_id else None
        if run is None:
            self.stdout.write("Celery run                           : none recorded")
        else:
            self.stdout.write("Most recent --celery run:")
            self._print_run(run)
            if erc20_balance_backfill_looks_stalled() is not None:
                self.stdout.write(
                    self.style.WARNING(
                        "  STALLED: heartbeat is older than "
                        f"{ERC20_BALANCE_BACKFILL_STALE_SECONDS}s and the "
                        "rollup lock is not held -- "
                        "erc20_balance_backfill_watchdog_task will "
                        "redispatch it within 5 minutes, or run `manage.py "
                        "backfill_erc20_balances --celery` again to force "
                        "it now (refused only while genuinely running)."
                    )
                )

    def _print_run(self, run: dict) -> None:
        heartbeat_age = "-"
        heartbeat_at = run.get("heartbeat_at")
        if heartbeat_at:
            try:
                heartbeat_age = f"{(timezone.now() - datetime.fromisoformat(heartbeat_at)).total_seconds():.0f}s ago"
            except ValueError:
                heartbeat_age = f"unparseable ({heartbeat_at})"
        line = (
            f"  run {run['run_id']} [{run.get('state')}] "
            f"phase={run.get('phase')} slices={run.get('slices_done', 0)} "
            f"chunks={run.get('chunks_done', 0)} "
            f"seen={run.get('safes_seen', 0)} "
            f"seeded={run.get('safes_seeded', 0)} "
            f"whale_skipped={run.get('whale_skipped', 0)} "
            f"whale_ranges={run.get('whale_ranges_done', 0)} "
            f"pairs_touched={run.get('pairs_touched', 0)} "
            f"head={run.get('head')} heartbeat={heartbeat_age}"
        )
        if run.get("state") == "failed":
            self.stderr.write(self.style.ERROR(line))
            self.stderr.write(self.style.ERROR(f"  error: {run.get('error')}"))
        elif run.get("state") == "finished":
            self.stdout.write(self.style.SUCCESS(line))
        else:
            self.stdout.write(line)
