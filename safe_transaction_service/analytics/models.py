from django.db import models

from safe_eth.eth.django.models import EthereumAddressBinaryField


class DailyMetric(models.Model):
    """Persisted per-day analytics rollup.

    Written incrementally by ``compute_daily_metrics_task`` (one row per
    completed UTC day) and backfilled by the ``backfill_daily_metrics``
    management command. Additive metrics (tx counts, transfers, native
    value) are summed across rows to serve windowed reads; the
    ``active_safes`` / ``active_owners`` columns store *per-day DAU*
    (distinct count within the single day) and are NOT summed to form
    window-distinct counts — the rolling-window distinct values stay on
    the existing Redis keys, refreshed by the same daily task.
    """

    date = models.DateField(primary_key=True)
    new_safes = models.PositiveIntegerField(default=0)
    active_safes = models.PositiveIntegerField(default=0)
    active_owners = models.PositiveIntegerField(default=0)
    multisig_txs_executed = models.PositiveIntegerField(default=0)
    module_txs = models.PositiveIntegerField(default=0)
    erc20_transfers = models.PositiveIntegerField(default=0)
    native_value_wei = models.DecimalField(max_digits=80, decimal_places=0, default=0)
    # tx-volume rollup (populated by _compute_daily_tx_volume, raw SQL).
    # Proposal-side count (by MultisigTransaction.created) and the two
    # numerator/denominator parts of avg_confirmations — sum over a window
    # for windowed avg = SUM(confirmations_count) / SUM(confirmed_tx_count).
    multisig_txs_proposed = models.PositiveIntegerField(default=0)
    confirmations_count = models.PositiveIntegerField(default=0)
    confirmed_tx_count = models.PositiveIntegerField(default=0)
    # API attribution of the executed multisig txs counted in
    # `multisig_txs_executed`, split on `MultisigTransaction.proposer`.
    # That field is only ever written by the proposal API
    # (`history/serializers.py`, inside the `get_or_create` defaults):
    # `via_api` = the tx was created through this service's API before
    # it executed; `indexed_only` = the indexer was the first (and only)
    # writer, i.e. the tx was executed on-chain without ever being
    # proposed here. Both are nullable on purpose: NULL means "not
    # computed for this day" (row written before the columns existed, or
    # the day was not indexed yet), which is a different statement from
    # a real 0. `via_api` + `indexed_only` == `multisig_txs_executed` for
    # any day computed after the columns landed — all three come out of
    # the same aggregate over the same join. Nullable is also required
    # mechanically: `_compute_daily_tx_volume` INSERTs this row with an
    # explicit column list that does not name these two.
    multisig_txs_via_api = models.PositiveIntegerField(null=True)
    multisig_txs_indexed_only = models.PositiveIntegerField(null=True)
    computed_at = models.DateTimeField()
    # Catch-up state — see "Analytics catch-up" in
    # `analytics/implementation-notes.md`. Both NULL until the day is
    # fully settled; neither is touched by populators directly — only
    # `compute_day` (via `_upsert_daily_metric`) sets them, and it clears
    # both to NULL as the *first*, separately committed step of any run
    # that recomputes the core, so a run that dies partway never leaves a
    # stale/rewritten row still marked complete.
    #
    # `core_completed_at`: core + `tx_volume` succeeded — i.e. every
    # column this table itself exposes via `/tx-volume/` is trustworthy.
    # This is what readers (`get_tx_volume`, `breakdown=day`) gate on.
    # `completed_at`: core + all populators succeeded. This is what the
    # sweeper reads to decide "done, never retry again".
    core_completed_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-date"]

    def __str__(self) -> str:
        return f"DailyMetric({self.date})"


class DailyTokenVolume(models.Model):
    """Per-(day, token) ERC20 transfer roll-up.

    Powers ``/v2/analytics/token-volume?window=Nd`` — replaces the live
    aggregation in ``analytics_service.get_token_volume``. Additive: window
    reads SUM across the day rows.
    """

    date = models.DateField()
    token_address = EthereumAddressBinaryField()
    transfer_count = models.PositiveBigIntegerField(default=0)
    transfer_value = models.DecimalField(max_digits=80, decimal_places=0, default=0)
    computed_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["date", "token_address"],
                name="analytics_daily_token_volume_uniq",
            ),
        ]
        indexes = [
            models.Index(fields=["date"], name="analytics_dtv_date_idx"),
            models.Index(
                fields=["token_address", "date"],
                name="analytics_dtv_token_date_idx",
            ),
        ]


class DailyActiveSafe(models.Model):
    """One row per (day, safe) where the Safe had any activity that day.

    Powers ``/v2/analytics/active-safes`` (window DAU is a clean
    ``COUNT(DISTINCT safe_address)`` over the date range) and the
    driving-set lookup for ``/active-owners``.
    """

    date = models.DateField()
    safe_address = EthereumAddressBinaryField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["date", "safe_address"],
                name="analytics_daily_active_safes_uniq",
            ),
        ]
        indexes = [
            models.Index(fields=["date"], name="analytics_das_date_idx"),
            models.Index(
                fields=["safe_address", "date"],
                name="analytics_das_safe_date_idx",
            ),
        ]


class DailySafeAppTx(models.Model):
    """Per-(day, origin_name) executed multisig tx count.

    Powers ``/v2/analytics/multisig-transactions/by-origin/`` — replaces
    the weekly live aggregation in ``get_transactions_per_safe_app_task``.

    `origin_url` is denormalised onto the rollup so the read path is a
    pure rollup scan — no read-time fall-back into
    `history_multisigtransaction.origin` JSONB just to recover the URL.
    Populators take ``MAX(origin->>'url')`` per group; if a name ships
    under multiple URLs on the same day we pick one (same collapse the
    legacy aggregate did silently).
    """

    date = models.DateField()
    origin_name = models.CharField(max_length=255)
    origin_url = models.CharField(max_length=512, blank=True, default="")
    tx_count = models.PositiveIntegerField(default=0)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["date", "origin_name"],
                name="analytics_daily_safe_app_txs_uniq",
            ),
        ]
        indexes = [
            models.Index(fields=["date"], name="analytics_dsat_date_idx"),
        ]


class DailySafeCreation(models.Model):
    """One row per UTC day — count of Safes whose ``EthereumTx.block.timestamp``
    falls in ``[day, day+1)``.

    Powers ``/v2/analytics/safe-creations?from&to&interval`` — replaces the
    full-history aggregation in ``compute_safe_creations_task``. Interval
    resampling (week / month) stays in Python.
    """

    date = models.DateField(primary_key=True)
    count = models.PositiveIntegerField(default=0)


class DailyActiveOwner(models.Model):
    """One row per (day, owner) where the owner confirmed any multisig tx
    whose ``EthereumTx.block.timestamp`` lands in that UTC day.

    Powers ``/v2/analytics/active-owners?window=Nd`` — windowed DAU is a
    clean ``COUNT(DISTINCT owner_address)`` over the date range, no
    ``SafeLastStatus`` lookup needed at read time.
    """

    date = models.DateField()
    owner_address = EthereumAddressBinaryField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["date", "owner_address"],
                name="analytics_daily_active_owner_uniq",
            ),
        ]
        indexes = [
            models.Index(fields=["date"], name="analytics_dao_date_idx"),
            models.Index(
                fields=["owner_address", "date"],
                name="analytics_dao_owner_date_idx",
            ),
        ]


class AnalyticsSnapshot(models.Model):
    """Single-row-per-name cache for current-state analytics that don't fit
    the per-day rollup shape (``summary`` / ``safe-segments`` / ``tvl``).

    Replaces the Redis cache for those metrics. Tasks upsert here on each
    compute; views read the most-recent row by ``name``. Postgres replaces
    Redis as the durability layer so reads survive Redis flush / pod
    restart, and the view's dispatch-and-poll path is gone — a missing
    snapshot returns an empty payload and fire-and-forget-dispatches the
    refresh task, never blocking the request.
    """

    name = models.CharField(max_length=64, primary_key=True)
    payload = models.JSONField()
    computed_at = models.DateTimeField()

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return f"AnalyticsSnapshot({self.name})"


class SafeNativeBalance(models.Model):
    """Per-Safe native-token balance, maintained incrementally.

    Replaces the nightly full recompute of every Safe's native balance
    (the 16-shard chord over ``BALANCE_BATCH_SQL``, which summed the
    *entire* transfer history on every run and stopped finishing at all
    on Ethereum-sized chains). One row per Safe; each incremental run
    applies only the net native flow of the blocks it has not consumed
    yet, so a run costs O(new rows) instead of O(all history).

    ``balance_wei`` is the **signed** net flow, not the clamped one.
    Incomplete indexing can make it negative (the outgoing transfer is
    indexed, the matching incoming one is not yet); clamping at write
    time would make that permanent, because a later incoming row could
    never lift the row back above zero. The clamp therefore lives at
    *read* time — ``SUM(CASE WHEN balance_wei > 0 …)`` /
    ``COUNT(*) FILTER (WHERE balance_wei > 0)`` — which is exactly what
    ``BALANCE_BATCH_SQL`` did per-Safe, so the numbers on
    ``/api/v2/analytics/tvl/`` do not move.

    ``updated_to_block`` is the block this row's balance is complete
    through. It is a per-row provenance stamp and the backfill's resume
    journal, *not* the authority on what has been consumed — that is the
    single ``AnalyticsWatermark(name='native_balance')`` row. A Safe with
    no native flow in a given window keeps its old ``updated_to_block``
    and is still correct.

    A row exists for **every** ``SafeContract``, including Safes that
    have never moved native value (``balance_wei = 0``). "Absent from
    this table" therefore means "created since the last run" and is the
    signal the incremental seed step keys on; if zero-balance Safes were
    omitted, every run would try to re-seed the whole fleet.

    No index beyond the PK on purpose: the only read is a full-table
    ``SUM`` + ``COUNT FILTER``, which is a sequence scan whatever indexes
    exist (tens of ms over ~460k rows on Ethereum). Add one only with a
    measurement to point at.
    """

    safe_address = EthereumAddressBinaryField(primary_key=True)
    balance_wei = models.DecimalField(max_digits=80, decimal_places=0, default=0)
    updated_to_block = models.PositiveIntegerField()

    def __str__(self) -> str:
        return f"SafeNativeBalance({self.safe_address}@{self.updated_to_block})"


class SafeTokenBalance(models.Model):
    """Per-(Safe, ERC-20 token) balance, maintained incrementally from
    indexed ``Transfer`` events — the token analogue of
    ``SafeNativeBalance``, kept as a **separate** table rather than folded
    into it (source, indexer cursor, error profile and drift check all
    differ; see ``analytics/implementation-notes.md``).

    ``balance`` is the **signed** net flow (inflow minus outflow), kept
    unclamped so negative rows can be counted (``negative_pairs`` on
    ``TokenHolding``) instead of being silently hidden by a clamp, unlike
    native's read-time clamp which only ever needs the sign, not the
    count.

    Two deliberate departures from ``SafeNativeBalance``:

    - **Rows with ``balance = 0`` are deleted**, not kept at zero. Unlike
      native (one row per ``SafeContract``, always present), the space of
      (Safe, token) pairs is unbounded — most Safes hold nothing — so
      "no row" here means "never held or fully exited", *not* "new Safe".
      New Safes are found through a separate marker
      (``AnalyticsWatermark(name='erc20_balance_safes')``), because the
      indexer can record a transfer to a not-yet-created Safe address
      before that address becomes a ``SafeContract``.
    - **``balance`` is an effectively unbounded signed numeric**
      (``max_digits=1000``), not ``numeric(80, 0)`` like
      ``balance_wei``. Native value is bounded by real chain supply;
      an ERC-20 contract can emit a ``Transfer`` of up to 2**256 - 1,
      and a spam token doing that a few hundred times overflows 80
      digits. If it did, the transaction would abort mid-upsert and the
      watermark would stop moving — freezing every token's rollup, not
      just the spam one. ``max_digits=1000`` is Postgres's own numeric
      precision ceiling, so there is no realistic value this can't
      store. Not ``Uint256Field``: that type is unsigned and its
      ``pre_save`` rejects negative values outright.
    """

    safe_address = EthereumAddressBinaryField()
    token_address = EthereumAddressBinaryField()
    balance = models.DecimalField(max_digits=1000, decimal_places=0)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["safe_address", "token_address"],
                name="analytics_safe_token_balance_uniq",
            ),
        ]
        indexes = [
            models.Index(fields=["token_address"], name="analytics_stb_token_idx"),
        ]

    def __str__(self) -> str:
        return (
            f"SafeTokenBalance({self.safe_address}/{self.token_address}={self.balance})"
        )


class TokenHolding(models.Model):
    """Per-token read model over ``SafeTokenBalance`` — one row per token,
    rewritten wholesale in the same transaction as each incremental
    delta (see ``compute_erc20_balance_rollup_task``).

    ``holders`` = ``COUNT(*) WHERE balance > 0`` and ``total_balance`` =
    ``SUM(balance) WHERE balance > 0`` over ``SafeTokenBalance`` rows for
    this token — both exclude negative rows so a token with only
    negative-balance pairs (incomplete indexing, never a real holding)
    doesn't count as held.

    ``total_balance`` carries the same unbounded signed numeric type as
    ``SafeTokenBalance.balance`` for the same overflow reason, even
    though in practice it only ever sums positive rows.

    ``negative_pairs`` is the count of ``SafeTokenBalance`` rows for this
    token with ``balance < 0`` (incomplete indexing, not clamped away —
    see ``SafeTokenBalance``), surfaced as a data-quality signal rather
    than hidden.

    ``as_of_block`` / ``as_of_timestamp`` declare the snapshot's right
    edge explicitly, unlike the older endpoints in this app where the
    window boundary is implicit (see the workspace contract notes on
    this).
    """

    token_address = EthereumAddressBinaryField(primary_key=True)
    holders = models.PositiveIntegerField(default=0)
    negative_pairs = models.PositiveIntegerField(default=0)
    total_balance = models.DecimalField(max_digits=1000, decimal_places=0, default=0)
    as_of_block = models.PositiveIntegerField()
    as_of_timestamp = models.DateTimeField()
    computed_at = models.DateTimeField()

    def __str__(self) -> str:
        return f"TokenHolding({self.token_address}@{self.as_of_block})"


class Erc20BalanceWhale(models.Model):
    """Safes whose ERC-20 transfer history is too large to sum in one
    backfill chunk (one Optimism Safe alone accounts for ~35% of all
    ``history_erc20transfer`` rows — see §4.3 of the token-holdings spec).

    A small, chain-specific allow-list, not a setting: it is filled by
    the backfill command (seeded from ``pg_stats`` most-common values,
    plus any Safe whose chunk exceeds a row threshold at run time) and
    read by both the backfill (to split a whale's chunk by block range)
    and the weekly drift check (to exclude whales from the recompute
    sample, since resumming a whale's full history would blow the
    statement timeout).
    """

    safe_address = EthereumAddressBinaryField(primary_key=True)

    def __str__(self) -> str:
        return f"Erc20BalanceWhale({self.safe_address})"


class AnalyticsWatermark(models.Model):
    """How far an incremental analytics rollup has consumed the chain.

    One row per rollup: ``name='native_balance'``, written by
    ``compute_native_balance_rollup_task`` and seeded by the
    ``backfill_native_balances`` management command; ``name='erc20_balance'``
    and ``name='erc20_balance_safes'``, the ERC-20 analogues (see
    ``run_erc20_balance_rollup`` in ``analytics/tasks.py``).

    Deliberately *not* a key inside ``AnalyticsSnapshot``: that table is
    the cache of payloads the views hand back, keyed by endpoint name and
    overwritten wholesale by each compute. A watermark is compute state —
    read before the work, written after it, inside the same transaction
    as the work — and conflating the two would put a correctness-critical
    cursor inside a blob that any view refresh may replace.

    ``block_number`` is the **last block already applied** (inclusive), so
    the next run consumes ``(block_number, head]``. For ``name=
    'erc20_balance_safes'`` this column is unused (set equal to the sibling
    ``erc20_balance`` watermark's block for readability) — that row's
    payload is the ``(computed_at, address)`` pair instead: the
    ``(created, address)`` boundary the new-Safe seed step last reached
    (token-holdings spec §4.2). ``address`` is ``NULL`` for every other
    watermark; a plain block cursor has no address component.
    """

    name = models.CharField(max_length=64, primary_key=True)
    block_number = models.PositiveIntegerField()
    computed_at = models.DateTimeField()
    address = EthereumAddressBinaryField(null=True, blank=True, default=None)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return f"AnalyticsWatermark({self.name}={self.block_number})"


class AnalyticsCatchupState(models.Model):
    """Sweeper bookkeeping for the analytics catch-up mechanism.

    See "Analytics catch-up" in `analytics/implementation-notes.md` for the
    full design. One table replaces the seven kinds of Redis keys the
    design first considered: attempt counters and "already logged" markers
    now survive a Redis flush, so `gave_up` / `expired` don't re-fire after
    one.

    ``kind`` + ``key`` (unique together) identify what is being tracked:
    ``("day", "2026-09-22")`` for a `DailyMetric` day, ``("snapshot",
    "tvl")`` for a PR-2 snapshot, ``("processing", "oldest")`` for the
    single row the gate uses to detect a stuck `InternalTxDecoded` queue.

    Every write here is committed immediately, outside any transaction
    wrapping the actual compute — an attempt is counted *before* the work
    it pays for runs, so it survives a SIGKILL or a gevent `Timeout`
    mid-compute. Success (a day/snapshot fully catching up) deletes the
    row; the sweeper itself prunes `kind="day"` rows older than two
    windows.

    ``core_ok`` and ``failed_steps`` are written **only** by `compute_day`
    (whichever caller invoked it — nightly task, backfill shard,
    `--inline` backfill, or the sweeper itself); the sweeper only reads
    them to decide whether a retry should pass `only=failed_steps`.
    Keeping the sweeper out of writing these two fields is what stops a
    manual backfill's `failed_steps` from silently diverging from the
    sweeper's.

    ``observed_value`` / ``observed_since`` / ``observed_count`` are only
    meaningful for ``kind="processing"``: the id of the oldest unprocessed
    `InternalTxDecoded` row the sweeper last saw, since when that id last
    changed, and how many unprocessed rows there were at that observation
    — the "same id, same-or-growing count" combination is what lets the
    sweeper tell a stuck queue apart from `fix_out_of_order` reshuffling
    within an otherwise-draining one.
    """

    kind = models.CharField(max_length=32)
    key = models.CharField(max_length=64)
    attempts = models.PositiveIntegerField(default=0)
    next_attempt_at = models.DateTimeField(null=True, blank=True)
    failed_steps = models.JSONField(default=list, blank=True)
    # Nullable: None = core has never been attempted (or its outcome is
    # not yet known) for this state row; only `compute_day` sets it.
    core_ok = models.BooleanField(null=True, blank=True)
    last_state = models.CharField(max_length=32, null=True, blank=True)
    last_code = models.CharField(max_length=64, null=True, blank=True)
    gave_up_logged_at = models.DateTimeField(null=True, blank=True)
    expired_logged_at = models.DateTimeField(null=True, blank=True)
    stuck_logged_at = models.DateTimeField(null=True, blank=True)
    first_seen_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    # `kind="processing"` only — see docstring.
    observed_value = models.BigIntegerField(null=True, blank=True)
    observed_since = models.DateTimeField(null=True, blank=True)
    observed_count = models.BigIntegerField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["kind", "key"],
                name="analytics_catchup_state_kind_key_uniq",
            ),
        ]
        indexes = [
            models.Index(
                fields=["kind", "next_attempt_at"], name="analytics_acs_kind_next_idx"
            ),
        ]

    def __str__(self) -> str:
        return f"AnalyticsCatchupState({self.kind}:{self.key})"
