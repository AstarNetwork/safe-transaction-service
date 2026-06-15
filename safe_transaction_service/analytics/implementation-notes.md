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
