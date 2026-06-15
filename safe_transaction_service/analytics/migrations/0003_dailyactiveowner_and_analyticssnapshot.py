from django.db import migrations, models

import safe_eth.eth.django.models


class Migration(migrations.Migration):
    """Adds the two tables described in
    ``flickering-honking-wand.md`` Parts 1 & 2:

    - ``analytics_dailyactiveowner`` — per-day confirmation-based active
      owners rollup. Replaces the ``SafeLastStatus`` lookup in
      ``analytics_service.get_active_owners``.
    - ``analytics_analyticssnapshot`` — single-row-per-name durable
      cache replacing the four Redis keys used by ``summary`` /
      ``safe-statistics`` / ``safe-segments`` / ``tvl``.

    Additive only — no ``history_*`` schema touches. Rollback = drop
    tables (``migrate analytics 0002``).
    """

    dependencies = [
        ("analytics", "0002_rollup_tables"),
    ]

    operations = [
        migrations.CreateModel(
            name="DailyActiveOwner",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("date", models.DateField()),
                (
                    "owner_address",
                    safe_eth.eth.django.models.EthereumAddressBinaryField(),
                ),
            ],
            options={},
        ),
        migrations.AddConstraint(
            model_name="dailyactiveowner",
            constraint=models.UniqueConstraint(
                fields=("date", "owner_address"),
                name="analytics_daily_active_owner_uniq",
            ),
        ),
        migrations.AddIndex(
            model_name="dailyactiveowner",
            index=models.Index(fields=["date"], name="analytics_dao_date_idx"),
        ),
        migrations.AddIndex(
            model_name="dailyactiveowner",
            index=models.Index(
                fields=["owner_address", "date"],
                name="analytics_dao_owner_date_idx",
            ),
        ),
        migrations.CreateModel(
            name="AnalyticsSnapshot",
            fields=[
                (
                    "name",
                    models.CharField(max_length=64, primary_key=True, serialize=False),
                ),
                ("payload", models.JSONField()),
                ("computed_at", models.DateTimeField()),
            ],
            options={"ordering": ["name"]},
        ),
    ]
