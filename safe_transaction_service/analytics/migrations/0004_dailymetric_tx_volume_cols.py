from django.db import migrations, models


class Migration(migrations.Migration):
    """Adds the three tx-volume rollup columns to ``analytics_dailymetric``
    so ``/v2/analytics/tx-volume/`` can serve from the rollup instead of
    running a 30s live aggregation per request.

    - ``multisig_txs_proposed``: per-day COUNT on
      ``history_multisigtransaction.created``. Summed over the window for
      ``total_multisig_txs``.
    - ``confirmations_count``: per-day COUNT on
      ``history_multisigconfirmation.created``. Numerator of
      ``avg_confirmations``.
    - ``confirmed_tx_count``: per-day COUNT(DISTINCT multisig_transaction_id)
      on the same window. Denominator of ``avg_confirmations``.

    All three default to 0 — Postgres ``ADD COLUMN ... DEFAULT 0`` is a
    metadata-only operation on PG ≥ 11, so this migration is instant on
    the production table. Backfill of historical days is run via the
    existing ``backfill_daily_metrics`` management command.
    """

    dependencies = [
        ("analytics", "0003_dailyactiveowner_and_analyticssnapshot"),
    ]

    operations = [
        migrations.AddField(
            model_name="dailymetric",
            name="multisig_txs_proposed",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="dailymetric",
            name="confirmations_count",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="dailymetric",
            name="confirmed_tx_count",
            field=models.PositiveIntegerField(default=0),
        ),
    ]
