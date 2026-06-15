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
    computed_at = models.DateTimeField()

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
