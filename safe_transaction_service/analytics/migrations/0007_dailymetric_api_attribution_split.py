from django.db import migrations, models


class Migration(migrations.Migration):
    """Splits the executed multisig-tx count of ``analytics_dailymetric`` by
    whether the transaction came through this service's proposal API.

    - ``multisig_txs_via_api``: per-day count of executed multisig txs
      whose ``history_multisigtransaction.proposer`` is set. That field is
      written only by the proposal API, so it marks the tx as "created
      through this service".
    - ``multisig_txs_indexed_only``: the complement — executed on-chain,
      first seen by the indexer, never proposed here.

    Both are ``NULL``-able rather than ``DEFAULT 0``: an existing row gets
    NULL, which reads as "this day was never split" instead of claiming a
    real zero. ``ADD COLUMN ... NULL`` with no default is metadata-only,
    so this is instant on the production table. Historical days are filled
    by re-running the existing ``backfill_daily_metrics`` command.

    Caveat for whoever reads the numbers: ``proposer`` was added to
    ``history`` by migration ``0075_multisigtransaction_proposer``
    (2023-10-05). Days before that land in ``indexed_only`` even when the
    tx did come through the API, because the column did not exist yet.
    """

    dependencies = [
        ("analytics", "0006_create_analytics_auth_token"),
    ]

    operations = [
        migrations.AddField(
            model_name="dailymetric",
            name="multisig_txs_via_api",
            field=models.PositiveIntegerField(null=True),
        ),
        migrations.AddField(
            model_name="dailymetric",
            name="multisig_txs_indexed_only",
            field=models.PositiveIntegerField(null=True),
        ),
    ]
