# Implementation notes — Narrow daily rollup tables + Celery parallelization

Running log of decisions, deviations, and trade-offs made while implementing
[`ROLLUPS_AND_PARALLEL_SPEC.md`](./ROLLUPS_AND_PARALLEL_SPEC.md).

---

## Spec line-number drift

- **§6 "config/settings/base.py (1880)"** — actual file is 810 lines. The
  `ANALYTICS_USE_ROLLUPS` flag is being added near the existing
  `ENABLE_ANALYTICS` flag (~line 55) so it sits with the other analytics
  feature toggles, not deep in Celery routing.
- **§4.1 "Reuses existing BALANCE_BATCH_SQL with a `WHERE substring(...)`"**
  — `BALANCE_BATCH_SQL` filters by `ANY(%s)` of explicit address bytes,
  not by raw table scan. Two options:
  1. Apply the prefix filter at the Python level — only pass addresses whose
     first nibble matches into `ANY(%s)`. Keeps the SQL untouched.
  2. Add a raw `WHERE substring(addr, 1, 1) = …` to a *new* SQL that scans
     `history_internaltx` directly (no SafeContract address list).
  Going with option **(1)**: keeps the existing covering-index plan
  (`it."to" = ANY(...)`) intact, no new query shape to validate, and the
  shard work happens inside the same proven batched loop.

## Model field choice for `safe_address` / `token_address`

Spec says `EthereumAddressBinaryField`. The existing `DailyMetric` model uses
*no* address columns, so this is the first analytics table to embed
addresses. Reused `EthereumAddressBinaryField` from
`safe_transaction_service.history.models` — same storage as
`history_erc20transfer.address`, so a follow-up join is bytes-to-bytes
(no `decode(...)` in queries).

## `analytics_daily_active_safes` row shape

Spec is one-row-per-(date, safe_address). On a 250 k-Safe chain that's
~250 k rows per day, up to ~90 M rows over the rolling 12-month window kept
on disk. Index `(safe_address, date)` is mandatory for the per-Safe lookups
the spec doesn't quite call out — keeping it as specified.

## §3 single-day populate — write order

All five populators run inside one `relaxed_statement_timeout()` block in
`compute_daily_metrics_task`. Order:

1. `_compute_daily_metric_core` (existing — DailyMetric upsert)
2. `_compute_daily_token_volume`
3. `_compute_daily_active_safes`
4. `_compute_daily_safe_app_txs`
5. `_compute_daily_safe_creations`

If one fails the rest still run (per-populator try/except, matching the
existing per-day try/except pattern in `compute_daily_metrics_task`).

## §4.2 backfill — chord vs group

Spec shows a `group(...) | _backfill_done.s(stats_key)` which is the celery
**chord** shape. On the `contracts` queue with eager mode this works in
tests via `CELERY_TASK_ALWAYS_EAGER`. `_backfill_done.s()` only needs to
write a small cursor blob — left as a Redis `SET` of the (start, end,
written, failed) summary so the management command can poll it with
`--wait` if needed. For now `--inline` falls back to the sequential loop
for debugging.

## Cold-window fallback (§5)

Spec says "if a rollup query returns zero rows for the requested window,
the service falls back to the live aggregation **once**, caches the
result, and logs `analytics.rollup.cold_window`."

Implementation: the rollup read returns the rollup result if any rows
exist in the window; otherwise it logs `analytics.rollup.cold_window`
and dispatches the equivalent live compute. The "caches the result"
step writes to the same Redis key the legacy path used — so the next
request gets the cached payload regardless of rollup population state.

## `ANALYTICS_USE_ROLLUPS` flag *(removed)*

Spec §6 / §7 introduced a feature flag so the migration could land
without behavior change, then operators could flip it after backfill.
After review the flag was removed: rollup-first reads with cold-window
fallback to the legacy path are now unconditional. Rationale:

- The cold-window fallback already gives the "no behavior change"
  property on fresh deploys — if the rollup is empty, the service falls
  through to the live/cached path automatically.
- A flag that's flipped to True everywhere within the rollout window
  and then never touched again is just dead conditional code waiting to
  rot.
- One fewer setting to forget in `.env`.

Rollout sequence is now: migrate → backfill → traffic. No flip step,
because there's no flag to flip.

## Tests added

- `test_tasks.py`:
  - `TestDailyRollupPopulators` — one test per rollup populator with
    ON CONFLICT semantics on rerun.
  - `TestNativeBalanceShards` — chord under eager mode matches sequential
    result. Uses 4-shard subset for speed.
  - Idempotency test for the 5-step `compute_daily_metrics_task`.
- `test_views_v2.py`:
  - For the four affected endpoints, rollup-served path + cold-window
    fallback exercised under `ANALYTICS_USE_ROLLUPS=True`.
- `test_backfill_daily_metrics.py`:
  - Group dispatch under eager mode; assert one DailyMetric row per day
    plus rollup rows.

## Why not partition the rollups now

Spec §9 explicitly rules out partitioning. Rollup tables stay
single-table; if growth becomes painful past 12 months we can add a
`DELETE ... WHERE date < now() - interval '12 months'` retention task
later. Not implementing retention now — out of scope.

---

## Decisions made during implementation

### Eager-mode chord bypass — added then removed

First pass: `celery.chord()` requires a result backend. With
`CELERY_RESULT_BACKEND` unset in dev/test, both `dispatch_native_balance_shards`
and `dispatch_backfill` branched on `settings.CELERY_ALWAYS_EAGER` and ran
shards inline as a workaround.

Final pass: deleted the bypass. The root cause was upstream — chord needs
a backend even in eager mode. `config/settings/base.py` now defaults
`CELERY_RESULT_BACKEND` to the same `REDIS_URL` everything else uses, so
chord coordinates on Redis in every environment (eager-mode tests
included). `CELERY_IGNORE_RESULT=True` is orthogonal — non-chord tasks
still don't write results. Production saw this fail in the wild
(`tasks._calculate_native_balances_from_db`: "Sharded native balance
dispatch failed (Starting chords requires a result backend to be
configured.)"), which prompted the cleanup.

### `compute_tvl_task` is fire-and-forget — chord callback owns the snapshot

Earlier shape: `compute_tvl_task` called `_calculate_native_balances_from_db`,
which dispatched the 16-shard chord and blocked on `.get()` for the
reduced result, then ran the ERC20 aggregation, then wrote the snapshot.
On Berachain staging this hung for the full `LOCK_TIMEOUT * 4` window
on every run — gevent worker + Redis result backend never observed the
chord callback's result key — so phase-2 logs never appeared and the
endpoint stayed stuck on the phase-1 placeholder.

Current shape: `compute_tvl_task` writes the placeholder (only if no
prior snapshot exists) and calls `dispatch_tvl_chord()` from
`tasks_shards`, which submits
`(16 native shards) → reduce_native_balance_shards → finalize_tvl_snapshot`
to the `contracts` queue and returns. `finalize_tvl_snapshot` runs the
ERC20 aggregation and writes the real snapshot from the chord callback,
so there is no synchronous `.get()` anywhere in the pipeline.
`_calculate_native_balances_from_db` is kept as a thin alias for the
sequential implementation for tests / ad-hoc use; the `parallel` kwarg
is now ignored.

Failure semantics carry over: if `finalize_tvl_snapshot` raises, the
placeholder snapshot stays, so the endpoint keeps serving a coherent
zero payload until the next run succeeds.

### Address-prefix filter in Python (not SQL)

Spec §4.1 mentioned a `WHERE substring(address from 1 for 1) = ...`
filter. The existing `BALANCE_BATCH_SQL` filters on `ANY(%s)` of an
explicit address-bytes list — there's no `address` column to filter
*on* in that SQL. Two ways to add prefix sharding:
1. Filter the `SafeContract.objects.values_list("address", flat=True)`
   stream in Python by `addr[2].lower() == prefix` (first nibble), then
   feed the survivors into the unchanged `BALANCE_BATCH_SQL`.
2. Add a new SQL that scans `history_internaltx` with a substring
   filter on `_from` / `to`.

Chose **(1)** — keeps the proven covering-index plan intact and is one
local function (`_safe_addresses_for_prefix`). Cost is one stream of
SafeContract.address per shard (small, indexed column).

### `_compute_daily_active_safes` uses bulk_create + ignore_conflicts

Spec §3 shows `INSERT … SELECT … ON CONFLICT DO NOTHING`. Implementation
uses Django's `bulk_create(ignore_conflicts=True)` because
`_safes_active_between` already returns lower-case `0x…` strings and
`EthereumAddressBinaryField` handles the str→bytes encoding via its
descriptor — re-implementing that in raw SQL would have meant either
manually `decode(..., 'hex')` per row or maintaining a parallel encoding
path. `bulk_create` writes in 5k-row batches which is fine for any
realistic per-day DAU count (BASE chain peaks ~50k DAU).

### `_safes_active_between` refactored to a thin wrapper

Originally `_safes_active_between` built the set then returned `len(...)`.
The new `_compute_daily_active_safes` populator also needs the set
(membership goes into the rollup row-by-row). Refactored: extracted
`_safes_active_between_set` containing the union body, and
`_safes_active_between` now does `return len(_safes_active_between_set(...))`.
No behaviour change for existing callers.

### `compute_safe_creations_task` writes a `source` key

Original payload schema had `series` + `computed_at`. The new task adds
a `source: "rollup" | "live"` key so we can observe in production which
path is actually serving. Existing read tests don't pin the keyset (just
check `series`/`computed_at` presence), so this is backwards-compatible.

### `get_transactions_per_safe_app_task` dual-writes via one SQL pass

Spec §6 said "write into `analytics_daily_safe_app_txs` *and* keep
Redis write." Implementation does the Redis write first (unchanged ORM
aggregate), then runs ONE additional SQL with `GROUP BY date,
origin_name` to backfill every distinct day of origin activity into the
rollup. `ON CONFLICT DO UPDATE` keeps it idempotent. Wrapped in a
try/except so a rollup failure doesn't strand the Redis publish.

### Rollup origin_name lookup loses `url` *(superseded — see next note)*

~~Spec §2.3 only stores `(date, origin_name, tx_count)`...~~ Initially
reconstructed `url` via a read-time `MultisigTransaction.objects.filter(
origin__name__in=...)` lookup. **Reverted** — see next entry.

### `origin_url` denormalised onto the rollup (post-review fix)

Reading from a rollup but still hitting `history_multisigtransaction`
for URL contradicts the spec's whole point ("single-digit-ms reads
regardless of `history_*` size"). Added `origin_url CharField(512)` to
`DailySafeAppTx`; both populators (`_compute_daily_safe_app_txs` and
the legacy task's dual-write SQL) now select `MAX(origin->>'url')` per
group; read path returns it straight from the rollup with no JSONB
lookup. Conflict resolution on multi-URL names: most-recent non-empty
URL wins, same collapse the legacy aggregate did silently.

Spec §2.3 was the source of the regression — its column list omitted
URL even though the response payload needs it. Fixed both files.

### Pre-commit triple-quote string fix

`HEX_PREFIXES` and `BACKFILL_CURSOR_KEY` originally had `"""…"""`
string-literal comments under them (a documentation idiom used in
Sphinx). The `check-docstring-first` hook flagged these as additional
module docstrings. Converted both to `#` comments above the constant —
no functional change.

## Test results

`python -m pytest safe_transaction_service/analytics/tests/ -q`:
**82 passed** (under `CELERY_ALWAYS_EAGER=True` test settings).

`pre-commit run --files <all-touched>`: all hooks pass after one
auto-fix pass (ruff format + ruff import sort).

The full repo `./run_tests.sh` was NOT run end-to-end on this machine
(would require ~minutes against the full history/contracts/tokens
suites); only the analytics subtree was validated.

---

(Append as work progresses.)

---

# Part 2 — Rollup-back the 5 remaining analytics endpoints

Running log of decisions for the implementation of
`/home/den/.claude/plans/flickering-honking-wand.md` (DailyActiveOwner
rollup + AnalyticsSnapshot durable cache).

## Spec drift caught during exploration

- **Spec line 250** claims `backfill_daily_metrics` already has
  `--only` / `--skip` flags so operators can backfill just `active_owners`.
  Reality: the command exposes `--inline`, `--wait`, `--chunk-days`
  only — populators are hard-wired into a tuple in `_upsert_daily_metric`
  (`tasks.py:1190–1196`). **No action needed**: the new `active_owners`
  populator runs automatically once added to that tuple, and a full-range
  backfill of the period covers it. If we ever need selective re-runs
  we'd add the flags then.

- **Spec line 252 says "5-step single-day populate path"** in the
  `_upsert_daily_metric` docstring. After adding `active_owners` it's a
  **6-step** path — updated the docstring to match.

## Payload shape — why I didn't strip `computed_at` from snapshots

The spec's `_read_snapshot_or_empty` returns
`{**snap.payload, "computed_at": snap.computed_at.isoformat()}`. I kept
the legacy `computed_at` key inside the *payload* for `summary` /
`safe_segments` / `tvl` rather than stripping it at write time —
column-overlay only writes a freshness stamp on top of whatever the
task produced, so the two stamps stay in sync (both `timezone.now()` at
write time).

(The earlier `safe_statistics` snapshot also carried a legacy
`timestamp` field for backwards-compatibility with one external
consumer. That endpoint was retired — see plan
`robust-wandering-spark.md` — and the `timestamp`/`computed_at`
duplication died with it.)

## Empty payloads — the new "cold cache" contract

The big behaviour change vs `_redis_get_or_compute`:

| Path | Old behaviour | New behaviour |
|---|---|---|
| Cold cache | inline compute (up to 25s blocking) → return data **or** 504 | fire-and-forget dispatch + return empty payload **in <200ms** |
| Warm cache | Redis fetch → return data | Postgres fetch → return data |

Under eager-mode tests, `.delay()` runs synchronously so the snapshot is
populated by the time we return — but `_read_snapshot_or_empty` already
*committed* to returning `empty` before dispatch. So in tests, the FIRST
call gets empty / SECOND call gets data. Several existing tests assumed
single-call inline compute; updated to either pre-warm the snapshot or
to expect the two-call pattern. Documented as
`test_summary_cold_cache_returns_empty_without_blocking`.

## `_compute_daily_metric_core` — owners count reads from `DailyActiveOwner` too

Spec says to mirror the `active_safes_daily` trick (read count from rollup
instead of running the live aggregate). Implemented as:

```python
owners_rollup_count = DailyActiveOwner.objects.filter(date=day_start.date()).count()
if owners_rollup_count > 0:
    active_owners_daily = owners_rollup_count
else:
    active_owners_daily = _active_owners_between(day_start, day_end)
```

Critical: order in the populator tuple matters. `active_owners` runs
**second** (after `active_safes`, before `token_volume`) so it's
populated by the time `_compute_daily_metric_core` reads from it. Without
this, the slow `_active_owners_between` path runs unnecessarily.

The fallback to `_active_owners_between` is intentional — protects
tests that call `_compute_daily_metric_core` directly without going
through `_upsert_daily_metric` (`TestUpsertDailyMetric.test_writes_row_with_correct_counts`
seeds `MultisigTransactionFactory` but doesn't seed
`MultisigConfirmationFactory`, so the owners rollup may be empty for
that day; falling back keeps the test from breaking).

## `_DAILY_ACTIVE_OWNERS_SQL` — confirmation-based, no SafeContract filter

`_compute_daily_active_safes` filters via
`EXISTS (SELECT 1 FROM history_safecontract sc WHERE sc.address = src.addr)`.
The owners populator does **not** — every confirming owner is by
definition an owner of a Safe known to the indexer (the
`MultisigConfirmation` → `MultisigTransaction` → `EthereumTx` →
`EthereumBlock` chain has no orphan rows by construction). Skipping the
existence check saves one indexed PK probe per owner.

## Test that broke from a real bug (good)

`TestUpsertDailyMetricFullStack.test_writes_all_four_rollups` was
renamed to `test_writes_all_rollups` and now asserts
`DailyActiveOwner.objects.filter(date=d).exists()`. To make that pass
I had to seed a `MultisigConfirmationFactory`, not just a
`MultisigTransactionFactory` — surfaced the fact that the new populator
correctly requires both rows (confirmation + tx in window).

## Service-layer cleanup

- Removed the `SafeLastStatus` import — no longer used now that
  `get_active_owners` reads `DailyActiveOwner` directly.
- Kept `_redis_get_or_compute` in the file — still serves the
  cold-window fallback for `get_active_safes` / `get_active_owners`
  (per spec §"Decommissioned"). Its 25s poll path is no longer reached
  by the 4 snapshot endpoints, but the function is unchanged.

## Test environment notes

`./run_tests.sh` requires docker (db + redis + ganache + rabbitmq). I ran
`pytest safe_transaction_service/analytics/tests/` against the local
.venv with `DJANGO_DOT_ENV_FILE=.env.test`. Needed three containers up:

- `db` (postgres:16-alpine on 5432)
- `redis` (redis:alpine on 6379)
- `ganache` (RPC on 8545 — required by `safe_eth` chain_id init)

Result: **93 passed, 109 warnings, 0 failures** in 19.5 s.
The full `./run_tests.sh` (history / contracts / tokens) was NOT run on
this branch — only the analytics subtree was validated.

## Out of scope (carried)

- Historical snapshot/time-series shape (single-row-per-name is intentional).
- `contracts` queue split.
- Cron cadence changes for the 4 snapshot tasks.
- Deletion of orphaned legacy Redis key constants
  (`REDIS_SUMMARY`, `REDIS_SAFE_SEGMENTS`, `REDIS_TVL`) — kept in
  `AnalyticsService` for one release as documentation pointers, then
  deleted. (`REDIS_SAFE_STATISTICS` was removed alongside the
  `/safe-statistics/` endpoint — see plan
  `robust-wandering-spark.md`.)

---

# Part 3 — `backfill_daily_metrics`: waiting, throttling, per-run state

Three defects surfaced on Ethereum staging on 2026-09-08, all reproduced on
live data. Branch `feat/analytics-backfill-throttling`. Only
`analytics/tasks_shards.py`, `analytics/management/commands/backfill_daily_metrics.py`
and tests changed; populators, `_upsert_daily_metric`, SQL and `config/`
are untouched.

## Reproduction (Ethereum staging, 2026-09-08)

```
python manage.py backfill_daily_metrics --start 2026-06-10 --end 2026-08-31 \
    --chunk-days 6 --wait 3600
```

- Chunk 1 (`2026-06-10 → 2026-06-15`) really finished after ~22 minutes:
  `backfill_done` wrote `analytics_backfill_cursor =
  {'total': 6, 'written': 6, 'failed': 0, 'failures': [],
  'finished_at': '2026-09-08T07:04:44+00:00'}`. The command nevertheless sat
  in `result.get(timeout=3600, disable_sync_subtasks=False)` for the full
  hour and exited with `The operation timed out.` Every chunk waited exactly
  `--wait` seconds regardless of its real duration. **(Defect 1)**
- Same range with `--wait 0`: 12 chords were submitted back to back and the
  `contracts` worker started all 77 days at once. `pg_stat_activity` showed
  ~70 concurrent backfill sessions in the same stage (`INSERT INTO
  analytics_dailymetric` from `_compute_daily_tx_volume`), all waiting on
  IPC for Postgres parallel workers, on the shared multi-chain
  `postgres-shared` instance. The help text's "concurrency caps naturally at
  worker pool size" was not a cap on that pool. **(Defect 2)**
- With several chords in flight `analytics_backfill_cursor` was overwritten
  by whichever chunk finished last; there was no per-chunk record and no
  run-level picture. **(Defect 3)**

## Root cause of defect 1 — `CELERY_IGNORE_RESULT = True`

`config/settings/base.py` sets `CELERY_IGNORE_RESULT = True` (project-wide
`task_ignore_result`). In Celery 5.5.3 `backends/base.py::mark_as_done`
does:

```python
if store_result and not _is_request_ignore_result(request):
    self.store_result(...)          # skipped for every task here
if request and request.chord:
    self.on_chord_part_return(...)  # ALWAYS runs
```

So the chord *header* still coordinates through Redis (that is why the
callback ran and the Redis summary landed), but the *callback's own return
value* is never written to the result backend. `chord.apply_async()` hands
back the callback's `AsyncResult`, and `.get()` on it waits for a key that
will never appear — until the timeout. Eager-mode tests never saw this
because `EagerResult` carries the value in-process. This is almost
certainly the same mechanism behind the Berachain TVL hang recorded
above ("gevent worker + Redis result backend never observed the chord
callback's result key") — that one was side-stepped by removing the
`.get()`, which is also what the command does now.

`CELERY_RESULT_BACKEND` itself is fine (defaults to `REDIS_URL`, same env
var on web pod and worker, so both talk to the same Redis); `result_expires`
is Celery's default 1 day and irrelevant here because nothing is stored.

**No change is needed in `config/settings/base.py`.** Two things were done
inside `analytics/` instead:

1. `backfill_done` is declared `@app.shared_task(ignore_result=False)` — a
   task-level override of the global setting, so `dispatch_backfill(...).get()`
   now works for anyone who still calls it that way. It stores one small
   JSON blob per chunk with the default 1-day expiry.
2. The management command no longer uses the result backend at all. Chunk
   completion is detected by the appearance of the chunk's own summary key
   in Redis (see below); `--wait` is only an upper bound per chunk.

If you ever want *every* analytics task result stored, that would be a
`base.py` change (`CELERY_IGNORE_RESULT`, or a per-task override) — not
recommended, the beat tasks return large payloads.

## What changed

### `tasks_shards.py` — run manifest, per-chunk keys, callback-driven chain

A backfill is now a *run* described by a manifest in Redis:

| Key | Content | Written by |
|---|---|---|
| `analytics_backfill_run:<run_id>` | manifest: `run_id`, `start`, `end`, `total_days`, `chunk_days`, `chunk_count`, `started_at`, `finished_at`, aggregate `total/written/failed/failures` (failures capped at 200), and `chunks[]` each with `index/start/end/days/key/state/dispatched_at/finished_at/total/written/failed/error` | `start_backfill_run`, `_dispatch_backfill_chunk`, `_advance_backfill_run` |
| `analytics_backfill_run:<run_id>:chunk:<n>` | that chunk's summary (`total/written/failed/failures/finished_at/start/end/run_id/chunk_index`) | `backfill_done` |
| `analytics_backfill_cursor` | pointer `{"run_id", "run_key", "started_at"}` to the most recently started run | `start_backfill_run` |

All three carry a 7-day TTL refreshed on every write
(`BACKFILL_KEY_TTL_SECONDS`).

Chunk states: `pending → running → done`, or `dispatch_failed` if the broker
call for the *next* chunk raised (the run is then closed with
`finished_at` set and the error recorded; re-run with `--failed-only`).

**Sequencing.** `start_backfill_run(dates, chunk_days)` writes the manifest
and dispatches chunk 0 only. `backfill_done(shard_results, stats_key,
run_id, chunk_index)` writes the chunk key, folds the numbers into the
manifest and then calls `_dispatch_backfill_chunk(run, n+1)` itself. The
callback is the throttle: at most `--chunk-days` days are in flight for a
run, independent of the worker pool size and of whether the command
process is alive. `--wait 0` therefore means "don't block the console",
not "fire everything".

Alternative considered: one big `chord | chord | …` canvas. Rejected —
the whole canvas (all signatures) is serialised into every message, there
is no place to record progress, and a failed link stalls silently. The
manifest gives an inspectable, resumable state machine for the price of
one Redis read/write per chunk.

**Eager-mode subtlety.** Under `CELERY_ALWAYS_EAGER` the entire chain runs
*inside* the first `apply_async` (nested callbacks). `_dispatch_backfill_chunk`
therefore persists the manifest *before* `apply_async` and never after it,
otherwise the stale in-memory copy would clobber the finished state. Same
rule in `_advance_backfill_run`. Tests rely on this — the sequencing test
reads the manifest from inside the patched populator and asserts exactly
one chunk is `running`, earlier ones `done`, later ones `pending`.

**`None` shard results.** `compute_daily_metric_shard` is wrapped in
`task_timeout(raise_exception=False)`, which returns `None` on a gevent
timeout. The old callback did `r.get("ok")` on it and would have crashed,
stalling the chain. `backfill_done` now treats a non-dict result as a
failed day and recovers the date by position from the chunk's `days` list.

`dispatch_backfill(dates, stats_key=None, run_id=None, chunk_index=None)`
is kept as the single-chord primitive (signature extended, backwards
compatible). Called standalone without `stats_key` it still writes its
summary to `analytics_backfill_cursor`; `--status` tolerates that legacy
shape.

### `backfill_daily_metrics.py`

- `--wait N`: per chunk, poll Redis every `--poll-interval` seconds
  (default 15) until the chunk's summary key exists or the chunk is marked
  `dispatch_failed`. Prints a per-chunk line with chunk and cumulative
  counters, then the full run summary. Exceeding `N` raises `CommandError`
  (exit 1) with a `--status <run_id>` hint — the run itself keeps going on
  the worker.
- `--wait 0` (default): start the run, print the run id and the status
  command, return.
- `--status [RUN_ID]`: print the run (defaults to the latest). Reads Redis
  only, no DB. `--start/--end` are not required with it.
- `--failed-only`: restrict the range to days whose `DailyMetric` row is
  missing or has `multisig_txs_via_api IS NULL` (ORM query, no new SQL).
  Works with both Celery and `--inline` modes. Prints `k/n days … need
  (re)running`; exits early with `Nothing to do.` when `k = 0`.
- `--run-id`: optional explicit id (default `YYYYmmddTHHMMSS-<6 hex>`).
- `--chunk-days 0` still means one chunk — in Celery mode that is every
  day on the queue at once; the help now says not to do that on a shared
  DB.
- Help text and docstrings no longer claim a "natural" concurrency cap.
- `--inline` path is byte-for-byte the previous loop (only the optional
  `--failed-only` filter is applied before it). `nohup … --inline` remains
  the no-worker fallback.

### Tests — `tests/test_backfill_daily_metrics.py` (new, 16 tests)

The notes above (Part 1) mention this file; it did not exist in the repo —
the two command tests lived in `test_tasks.py::TestBackfillDailyMetricsCommand`
and are unchanged. New coverage: strict chunk ordering with one chunk in
flight, command dispatches only chunk 0, per-chunk keys + TTL, run
aggregate with failures across chunks, `None` shard result, legacy
standalone `dispatch_backfill`, `--wait` completing on the Redis key with
`start_backfill_run` stubbed to *not* execute (i.e. no result backend
involved), `--wait` timeout → `CommandError` and manifest untouched,
`--wait 0` returns immediately, `--status` latest/explicit/unknown,
argument validation, `select_failed_days`, `--failed-only` inline and
"nothing to do", and one eager end-to-end run with real populators.

## How to run on heavy chains (Ethereum, BASE, Berachain)

```
# 90 days, 6 days in flight at a time, follow it from the console
# (up to 2 h per chunk before the console gives up; the run continues):
python manage.py backfill_daily_metrics --start 2026-06-10 --end 2026-09-07 \
    --chunk-days 6 --wait 7200

# fire-and-forget, then check later:
python manage.py backfill_daily_metrics --start 2026-06-10 --end 2026-09-07 --chunk-days 6
python manage.py backfill_daily_metrics --status            # latest run
python manage.py backfill_daily_metrics --status <run_id>

# redo only the gaps left by a previous run:
python manage.py backfill_daily_metrics --start 2026-06-10 --end 2026-09-07 \
    --chunk-days 6 --failed-only --wait 7200
```

Sizing `--chunk-days`: it is the number of concurrent
`_compute_daily_tx_volume` INSERTs (and the rest of the populators) the
shared Postgres will see from this chain. On Ethereum staging a 6-day chunk
took ~22 min; 6 is a sensible ceiling on `postgres-shared`, go lower if
`pg_stat_activity` shows IPC waits on parallel workers.

## Operational observations (no code change)

1. **Duplicate Celery node names in the worker pod.** The worker pod runs
   several `celery` processes that all register as `celery@<pod>`. `celery
   inspect active_queues` / `inspect active` answer from whichever process
   replies first, so the reported queue set (and task list) looks random
   between calls. It is not a routing bug; when checking whether the
   `contracts` queue is being consumed, either query several times or use
   `--destination` with unique `-n` names once the run scripts set them.
2. **Stay out of the beat window.** The analytics beat chain runs 01:00 →
   ~05:00 UTC (`compute_daily_metrics_task` 01:00 … `compute_safe_creations_task`
   04:30, all on the `contracts` queue). A backfill on a heavy chain in
   that window competes with it for the same pool and the same Postgres,
   and the daily task's own `_upsert_daily_metric` for yesterday can
   interleave with backfill rows. Start heavy backfills after 05:00 UTC and
   size `--chunk-days` × chunk duration so they finish before 01:00.

---

# Part 4 — Token `symbol` on `top_tokens` (phase-B T6)

`phase-b-data-gaps.md` §4.5 / T6. One additive key, `symbol`, on every
`top_tokens` entry of `/token-volume/` and `/tvl/`. Only
`analytics/services/analytics_service.py`, `analytics/tasks_shards.py` and
`analytics/tests/test_views_v2.py` changed; nothing under `tokens/` is
touched — the analytics app *reads* `tokens_token`, it does not own it.

## One helper, three call sites

`get_token_symbols(addresses) -> {address: symbol | None}` lives in
`analytics_service.py` (module level, next to `_parse_window`) and is
imported lazily by `tasks_shards.finalize_tvl_snapshot`. Three callers:
the live token-volume aggregation, the rollup-served token-volume read,
and the TVL snapshot build.

`Token.address` is an `EthereumAddressBinaryField`, same as
`DailyTokenVolume.token_address` and `ERC20Transfer.address`, so the
lookup is a bytes-to-bytes PK probe — one `IN (...)` over at most 20
addresses, no `decode(...)`, no per-address query. `get_prep_value`
normalises before encoding, so a lower-case address matches a
checksummed row and vice versa.

## Unknown is `null`, never the address

Both "no `tokens_token` row" and "row exists with a blank symbol" map to
`None`. Substituting the address would make *unknown* indistinguishable
from a token whose symbol genuinely is a hex string, and the rendering
decision belongs to the consumer (the hub shows a truncated address —
spec Q16). Every requested address is present in the mapping, so the key
is always present in the payload.

`name`, `decimals` and `logo_uri` are deliberately absent (Q15/Q19).
`decimals` becomes necessary the day a token *volume* is displayed —
`total_value` is in raw units — but nothing displays it today.

## TVL: write time, not read time

Spec §4.5 calls this "a read-time join" for both endpoints, and for
`/token-volume/` it is one. `/tvl/` is served from an `AnalyticsSnapshot`,
so its join necessarily happens where the payload is *built* —
`finalize_tvl_snapshot`, as the last statement before `_write_snapshot`.

Consequence: a `tvl` snapshot written *before* this deploy serves
`top_tokens` without `symbol` until the next chord reduces. Additive-key
semantics cover that (the hub ignores unknown keys and treats an absent
one as "store nothing"), so no read-time backfill was added in `get_tvl`
— that would put a `tokens_token` query on a hot snapshot read for the
sake of one cycle.

## The metadata join needs its own `except` (review fix)

T6's acceptance clause is *"the TVL join sits inside the existing
snapshot-writing `try` so that a metadata failure can never lose a TVL
snapshot."* Being lexically inside that `try` satisfies the first half and
**breaks the second**: the outer handler's whole job is to keep the phase-1
zero placeholder, so a `tokens_token` read that fell through to it would
discard a fully-reduced snapshot — precisely the loss the clause forbids.
Placing the lookup last and side-effect-free makes that improbable, not
impossible, which is not what "can never" means.

Shape landed:

```python
try:
    symbols = get_token_symbols(addr for addr, _ in top_tokens)
except Exception:
    symbols = {}
    logger.warning("finalize_tvl_snapshot: token metadata lookup failed …")
```

Still lexically inside the outer `try` (belt and braces, as the spec asks),
but the outer `except` no longer handles metadata failures — it keeps only
the failures it legitimately owns (the ERC20 net-flow aggregation and the
snapshot write). `symbols = {}` needs no other change: `symbols.get(addr)`
already yields `None` per entry, so the payload shape is identical and every
`symbol` key stays present-and-null, which *is* the documented contract for
an unknown token. Degrade the symbols, never the snapshot.

`compute_tvl_task`'s placeholder logic and the reduce are untouched.

## Tests

`test_views_v2.py`, five new cases, all through the reversed route with
the auth header:

- `TestRollupReadPath.test_token_volume_symbol_served_from_rollup` and
  `…_symbol_cold_window_falls_back_to_live` — the existing rollup /
  cold-window pairing, each with one known and one unknown token.
- `TestRollupReadPath.test_token_volume_blank_symbol_reads_as_null` —
  `symbol=""` in `tokens_token` is still unknown.
- `TestTvlSnapshotReadPath.test_tvl_top_tokens_carry_symbol`.
- `TestTvlSnapshotReadPath.test_tvl_snapshot_survives_a_token_metadata_failure`
  — patches `get_token_symbols` to raise and asserts the **reduced**
  payload was written anyway: non-zero `native_balance_wei`, populated
  `top_tokens`, and `"symbol"` *present* and `None` on every entry (asserted
  as key-presence, so dropping the key on the degraded path fails here).
  Its first revision asserted the opposite — placeholder kept, `top_tokens
  == []` — which is the behaviour the review fix above removed. Proof that
  the injected failure really fired moved to the two things that can only
  happen on the degraded path: the patched mock's `called`, and an
  `assertLogs` on the WARNING. Without them a green test would not
  distinguish "degraded correctly" from "patch never ran".

Fixtures use `tokens.tests.factories.TokenFactory`. Note the name clash
in that test module: `Token` there is `rest_framework.authtoken.models.Token`.

---

# Part 5 — `breakdown=day` on `/tx-volume/` (phase-B T8)

`phase-b-data-gaps.md` §4.5 / T8. One opt-in query parameter on
`/tx-volume/` **only**. Only `analytics/views_v2.py`,
`analytics/services/analytics_service.py` and
`analytics/tests/test_views_v2.py` changed.

## `urls_v2.py` is untouched

T8 lists it as a likely file; it needs no edit. `breakdown` is a query
parameter, and `path("tx-volume/", ...)` already matches every query
string. The route name the tests reverse (`analytics-tx-volume`) is
unchanged.

## Absent parameter ⇒ the same dict, built the same way

The scalar payload is now assigned to `payload` and the three new keys are
*appended* after it, so with `breakdown=None` the dict is constructed with
the same keys in the same insertion order as before — which is what makes
the serialised body byte-identical, not merely equal as a mapping. The
test pins `list(body.keys())` as an ordered list against a frozen literal
of the pre-T8 key set for exactly that reason; a set comparison would pass
while the bytes changed.

This is the property §4.9 leans on: the producer half can deploy to the
whole heterogeneous fleet before any hub change exists.

## Validation lives in the view, the shape in the service

`breakdown not in (None, "day")` → 400 in `AnalyticsTxVolumeView`, next to
the `window`/`interval` checks the other views already do. `?breakdown=`
(empty value) is a 400 too — `query_params.get` returns `""`, which is not
`None`, and "any value other than `day`" is the contract. `"Day"` is also a
400: no case folding, matching the existing exact-match style of the
`window` and `interval` guards.

`window` is deliberately left alone: it is still unvalidated on this
endpoint and `_parse_window` still falls back to 30 on anything
unparseable, capped by nothing under `breakdown=day` (spec Q21). A
`test_breakdown_day_window_is_not_capped` case pins that, so a later
"defensive" cap fails a test instead of silently truncating a consumer's
series.

## `days` entries are keyed `date`, not `period`

`/safe-creations/` emits `{"period", "count"}` because that series is
resampled to day/week/month and `period` is honestly the bucket label.
A `breakdown=day` series has exactly one granularity, so the entries carry
`date` (ISO `YYYY-MM-DD`). T9 should use the same key on the two active-\*
endpoints, and T11's hub-side upsert keys `daily_chain_series` on it.

## Window bounds are inclusive and describe the rows read

The read has always been `date__gte=today-window, date__lt=today` — today
is not a completed UTC day. `window_start` / `window_end` therefore report
`today-window` .. `yesterday`, both inclusive, i.e. the range the rows
actually come from rather than the half-open filter as written. They ship
even on a cold rollup: they describe the *request*, and a consumer needs
them to know which days a short `days` list is silent about.

## Nulls and gaps pass through untouched

Per-day rows are read with `.values(...)` (a second, index-only query
against the same filtered queryset — no model instantiation) and mapped
one-to-one. Consequences, all asserted:

- `multisig_txs_via_api` / `multisig_txs_indexed_only` are nullable and a
  pre-backfill day yields `null`, never `0` — the same statement the
  scalar payload makes with `api_attribution_coverage_days`.
- A day missing from the rollup is **absent** from `days`. Zero-filling
  would assert "no activity" where the truth is "not computed".
- A cold rollup gives `"days": []` for free from the empty comprehension —
  present and empty, which is the signal T11's feature detection reads
  ("old producer" is the key being absent).

Contract invariant 3 is not at risk here: all four per-day columns are
additive counts, and nothing sums `days` to produce a window value — the
scalar half still comes from its own `aggregate()` over the same rows.

## Tests

`test_views_v2.py`, two new classes, 12 cases (6 of them subtests):

- `TestTxVolumeDayBreakdown` — the three §5 cases (absent / valid /
  invalid), the series shape (order, gap, exclusion of today and of
  out-of-window days, null pass-through), and the not-capped window.
  `test_breakdown_day_adds_the_series_and_changes_nothing_else` diffs the
  `breakdown=day` body key-by-key against the no-parameter body from the
  same fixture, skipping only `computed_at` (stamped at read time).
- `TestTxVolumeDayBreakdownColdRollup` — the cold half of the rollup
  pairing. `/tx-volume/` has no live fallback (the 504s that removal
  fixed are in Part 1), so "cold" here means the honest zero payload plus
  `"days": []`, and the test seeds a `MultisigTransaction` to prove the
  live path is not consulted.

---

# Part 6 — `breakdown=day` on `/active-safes/` and `/active-owners/` (phase-B T9)

`phase-b-data-gaps.md` §4.5 / T9. The same opt-in parameter Part 5 added
to `/tx-volume/`, now on the two DAU endpoints, read from the
`DailyActiveSafe` / `DailyActiveOwner` rollups. Only
`analytics/views_v2.py`, `analytics/services/analytics_service.py` and
`analytics/tests/test_views_v2.py` changed. `urls_v2.py` needed no edit,
for the reason Part 5 gives: `breakdown` is a query parameter and
`path("active-safes/", …)` already matches every query string.

Part 5's shape is followed deliberately rather than improved on:
validation in the view next to the existing `window` guard, the shape in
the service, keys *appended* to a completed payload, entries keyed
`date`, exact-match validation with no case folding (`?breakdown=` and
`Day` are both 400).

## Contract invariant 3 — the whole point of this task

`active_safes` / `active_owners` are per-day **distinct counts**, so the
two numbers in the response are not two views of one quantity:

- the window value stays `windowed.values(<addr>).distinct().count()`,
  the `COUNT(DISTINCT …)` it has always been;
- each `days` entry is its own per-day `COUNT(DISTINCT …)`, from a second
  `values("date").annotate(Count(<addr>, distinct=True))` pass over the
  **same** filtered queryset.

Neither is derived from the other, nothing sums `days`, and no helper
takes a list of days and returns a total. `_daily_distinct_counts` says so
in its docstring, because it is exactly the function a later "just add up
the series" edit would reach for. The test fixture makes the trap
concrete and fails on it: one Safe active on two days plus two others
gives per-day counts `1, 1, 2` (sum 4) and a window value of 3.
`test_per_day_values_are_not_additive` pins both numbers, so a regression
that computed either from the other cannot pass.

The hub's `_dau` column suffix (spec §4.2) is the consumer-side half of
the same guard.

## `window_end` is **today** here, not yesterday

The one substantive divergence from Part 5, and it is in the data, not in
the shape. `/tx-volume/` reads `date__gte=today-N, date__lt=today`, so its
bounds report `today-N … yesterday`. Both active-\* reads filter
`date__gte=since` with **no upper bound** and therefore include a partial
current UTC day — the asymmetry the workspace contract's endpoint table
records. `window_start` / `window_end` describe the rows the read
actually covers, so they are `since … today` here. Reporting `yesterday`
for consistency's sake would have been a lie about which days the series
can contain, and `test_breakdown_day_series_shape` seeds a row dated
today to pin that the current day really is served.

Realigning that right edge is explicitly out of scope (§6, "gap 1's
semantic half"): it changes numbers already in use.

## Three return paths, all of which must emit `days`

Unlike `/tx-volume/`'s single return, each active-\* read can leave by
three doors: the rollup-served payload, the Redis cold-window fallback
(`_redis_get_or_compute`, populated by `compute_daily_metrics_task`), and
the final honest zero payload. The task's cold-path clause applies to all
three, so `_append_day_breakdown` is called on each. On the two cold doors
the series is `[]` — the Redis fallback carries a window scalar and has no
per-day rows behind it, and inventing them from the scalar would be the
invariant-3 error in another costume.

`days` present-and-empty on a cold read is what keeps "producer predates
the parameter" (key absent) apart from "producer has no rows yet" (key
empty), which T11's hub-side feature detection reads.

Mutating the dict `_redis_get_or_compute` returns is safe: it is a fresh
`json.loads` per request, not a shared object, and the keys land at the
end so the parameter-absent response is untouched.

## The `7d|30d|90d` guard is unchanged, and still runs first

These two endpoints do validate `window`, unlike `/tx-volume/`. Q21's
"`window` is not capped under `breakdown=day`" therefore means *no
further* cap, not a relaxation: `breakdown=day` accepts exactly the three
windows the endpoints already accepted. The `window` check stays above the
`breakdown` check, so `?window=5d&breakdown=week` reports `window` — the
more basic error — and `test_breakdown_day_does_not_relax_window_validation`
pins that a long window is still a 400 rather than being waved through
because a breakdown was asked for. Response size stays bounded by 90 days
here as a side effect, which is not a cap anyone should rely on.

## Per-day values are never `null` on these two

Part 5 has nullable per-day columns (`multisig_txs_via_api` and friends)
and passes them through. There is no analogue here: both rollups are
unique per `(date, address)` and hold no nullable columns, so a day
present in the rollup has a count of at least 1 and a day absent from it
is absent from `days` — never a zero-filled row. The null pass-through
clause of §4.5 is satisfied vacuously, not by coercion.

## Tests

`test_views_v2.py`, four new classes, 24 cases (32 subtests):
`TestActiveSafesDayBreakdown`, `TestActiveSafesDayBreakdownColdRollup`,
`TestActiveOwnersDayBreakdown`, `TestActiveOwnersDayBreakdownColdRollup`.
Each mirrors Part 5's pair: the three §5 cases (absent / valid /
invalid), the series shape, and the rollup / cold-rollup pairing.

The bodies live in two mixins (`_ActiveDayBreakdownMixin`,
`_ActiveDayBreakdownColdMixin`) parametrised by route, rollup model,
address field, count key and Redis prefix. That is the one place Part 5's
shape is not copied literally — the two endpoints differ in exactly those
five values, and two hand-copied 180-line classes would let them drift
apart, which is the opposite of the consistency T9 is asked for. The
concrete classes are still one per endpoint, and the test method names
match Part 5's. Neither mixin inherits `TestCase` and neither is named
`Test*`, so nothing collects them on their own.

`test_breakdown_absent_response_is_unchanged` pins
`list(body.keys()) == ["window", "<count_key>", "computed_at"]` as an
ordered list against a frozen literal — a set would pass while the bytes
changed — and
`test_breakdown_day_adds_the_series_and_changes_nothing_else` diffs the
`breakdown=day` body key-by-key against the no-parameter body from the
same fixture, skipping only `computed_at` (stamped at read time).

`test_breakdown_day_on_cached_fallback_returns_empty_days` seeds the
legacy Redis key with `7`, so a `7` in the response proves the fallback
door is the one that emitted `"days": []` — the cold path is asserted on
directly rather than inferred.

---

# Part 7 — `from`/`to` range on `/active-safes/` and `/active-owners/` (phase-B T10)

`phase-b-data-gaps.md` §4.5 / T10. An optional strict ISO `YYYY-MM-DD`
range on the two DAU endpoints, on top of Part 6's `breakdown=day`. Only
`analytics/views_v2.py`, `analytics/services/analytics_service.py` and
`analytics/tests/test_views_v2.py` changed — `urls_v2.py` again needed no
edit, for the reason Parts 5 and 6 give.

Part 6's shape is followed: parsing and validation in the view beside the
`window` and `breakdown` guards, the span in the service, the tests as one
mixin parametrised by route / model / address field / count key / Redis
prefix with one concrete class per endpoint.

## The parser is strict here, and is not shared with `/safe-creations/`

`date.fromisoformat` alone is too loose for the contract §4.5 writes down:
at Python 3.12 it accepts `20260105` (basic form) and `2026-W01-1` (week
form). `django.utils.dateparse.parse_date` is looser still — it falls back
to a regex that accepts `2026-1-5`. So `_parse_iso_date` in `views_v2.py`
pins the shape with a `\A\d{4}-\d{2}-\d{2}\Z` regex and lets
`fromisoformat` reject impossible dates such as `2026-02-30`. `?from=`
(empty value) is a supplied-and-unparseable bound and therefore a 400,
exactly as `?breakdown=` is a supplied-and-invalid breakdown.

`/safe-creations/`'s lenient `parse_datetime` bounds are **untouched**
(spec §6) and deliberately not refactored to share this parser. The two
endpoints now parse dates differently on purpose.

**Correction to §4.5/§6's wording, found while writing the guard test.**
The spec describes `/safe-creations/` as *silently ignoring a date-only
bound*. At the pinned Django 5.2 / Python 3.12 it does not: since Django
5.0 `parse_datetime` tries `datetime.fromisoformat` first, and that
resolves `2026-05-01` (and `20260501`) to midnight, so a date-only bound
there **is** honoured and does filter. What is silently ignored is a bound
`parse_datetime` cannot use at all — `not-a-date`, `2026-13-01`,
`2026-02-30` (that last returns `None` rather than raising, so there is no
500 either). The out-of-scope decision is unaffected; only the reason
given for it was stale. `TestSafeCreationsRangeStaysLenient` pins both
halves so the endpoint's real contract is written down somewhere.

## Three 400s, each naming its parameter

`_parse_iso_date_range` returns `(date_from, date_to, error)` and the view
answers `{"error": …}` with 400. The messages are asserted on by equality,
not by substring, because "a message naming the offending parameter" is
the acceptance clause:

- `from must be an ISO date (YYYY-MM-DD)`
- `to must be an ISO date (YYYY-MM-DD)`
- `from must not be after to` — `from == to` is a valid single-day range,
  not this error.

Guard order is `window` → `breakdown` → range, cheapest and most basic
first, so a request wrong in two ways reports `window`. A range does
**not** waive the `7d|30d|90d` guard: precedence decides which days are
counted, not which parameters are validated, and
`test_a_range_does_not_waive_the_window_validation` pins that.

## "The range wins" is expressed as `window: null`

Either bound present makes the read *ranged* and the window stops applying
altogether — the supplied bounds filter the rollup, the unsupplied side
stays unbounded (`_rollup_date_filters` contributes no filter for a `None`
edge rather than a sentinel date). A lone `to` therefore means "everything
up to `to`", not "the window, clipped".

The payload reports `window: None` on a ranged read. Reasons, in order:

1. echoing `"30d"` — the default the caller never asked for — next to a
   count over an unrelated span would be an active lie about what the
   number covers;
2. it makes precedence *observable*. A producer that predates T10 ignores
   `from`/`to` and echoes the window string, so `window: null` is how a
   caller tells "range honoured" from "range dropped" — the same
   feature-detection role `days: []` plays for Part 6;
3. it changes no key. Invariant 6 forbids renaming or removing one; the
   value changes only on the new opt-in path, and with both bounds absent
   the response is byte-identical to pre-T10, `window` string included.

Under `breakdown=day` the bounds keys report the span actually read:
`window_start` is the `from` (or `today - window` when unranged) and
`window_end` the `to` (or today). The one case where `window_start` is
`null` is a `to`-only range, where the read has no lower bound and there
is no honest date to name.

## A ranged cold read does not touch the Redis fallback

The `_redis_get_or_compute` fallback holds a *rolling-window* scalar
written by `compute_daily_metrics_task`. It is the right stale answer to a
window question and the wrong answer to a range question, so the ranged
path skips it entirely and returns the honest zero payload with
`computed_at: None`. Cold-plus-ranged is therefore "no rows in that span",
which a caller can act on, rather than a number for some other span.
`test_ranged_cold_read_does_not_serve_the_cached_window_scalar` seeds the
legacy key with `7` and asserts a `0` comes back, so the skip is asserted
directly rather than inferred.

The `analytics.rollup.cold_window` log line carries `since..until` instead
of the window string on a ranged read, so a cold range is not mistaken for
a cold 30d in worker logs.

## Contract invariant 3, again, and the range × `breakdown=day` corner

The ranged number is `windowed.values(<addr>).distinct().count()` over the
range-filtered queryset — the same `COUNT(DISTINCT …)` the windowed number
has always been, with a different `WHERE`. The `from`/`to` path adds a
filter and nothing else; it does not introduce a second way of arriving at
the count, and in particular it cannot bypass the distinct count, because
there is no other expression in the method that produces one.

With a range *and* `breakdown=day`, both halves are read from that one
filtered queryset by two independent passes: `_daily_distinct_counts` for
the per-day series and `.distinct().count()` for the scalar. Neither is
derived from the other and nothing sums `days`.
`test_range_with_breakdown_day_values_are_not_additive` extends Part 6's
`test_per_day_values_are_not_additive` onto a span that is not a window:
over `today-10 … today` the series sums to 5 while the distinct count is
4, because `addr1` is active on two of those days.

## Tests

`test_views_v2.py`, three new classes: `TestActiveSafesDateRange` and
`TestActiveOwnersDateRange` (one `_ActiveRangeMixin`, 13 cases each,
several of them subtests) plus `TestSafeCreationsRangeStaysLenient`.

The mixin reuses Part 6's fixture verbatim — rows at `today`, `today-1`,
`today-2` (two rows), `today-10`, with `today-3` deliberately absent — so
a ranged number can be compared against a windowed one the Part 6 cases
already pin: 7d gives 3, `today-10 … today` gives 4, and the extra address
is the out-of-window one.

Per spec §5, three cases per parameter per endpoint: absent (ordered key
list and the `window` string pinned against a frozen literal), valid (the
distinct count over five spans, including a single-day range and a span
with no rows), invalid (the three forms above, on message equality).
Precedence has its own case asserting the *number* only the range can
produce, not just `window: null`. The rollup / cold-window pairing is kept
as the ranged-cold pair described above.

## No consumer, on purpose

Spec §6: the hub collects on a schedule and its dashboard reads the
database, so consuming a custom range needs an on-demand query path that
does not exist. Nothing was added on the hub side, and no task reads this
yet — custom ranges keep falling back to 90d until such a path exists.
This is the producer half standing alone, which the merge order in the
workspace contract allows precisely because the parameter-absent response
is unchanged.
