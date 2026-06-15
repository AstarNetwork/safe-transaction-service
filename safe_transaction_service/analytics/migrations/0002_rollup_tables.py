from django.db import migrations, models

import safe_eth.eth.django.models


class Migration(migrations.Migration):
    """Adds the four narrow per-day rollup tables described in
    ROLLUPS_AND_PARALLEL_SPEC.md §2. All additive — no `history_*` schema
    touches. Rollback = drop tables (Django can do this with ``migrate
    analytics 0001``).
    """

    dependencies = [
        ("analytics", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="DailyTokenVolume",
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
                    "token_address",
                    safe_eth.eth.django.models.EthereumAddressBinaryField(),
                ),
                ("transfer_count", models.PositiveBigIntegerField(default=0)),
                (
                    "transfer_value",
                    models.DecimalField(decimal_places=0, default=0, max_digits=80),
                ),
                ("computed_at", models.DateTimeField(auto_now=True)),
            ],
            options={},
        ),
        migrations.AddConstraint(
            model_name="dailytokenvolume",
            constraint=models.UniqueConstraint(
                fields=("date", "token_address"),
                name="analytics_daily_token_volume_uniq",
            ),
        ),
        migrations.AddIndex(
            model_name="dailytokenvolume",
            index=models.Index(fields=["date"], name="analytics_dtv_date_idx"),
        ),
        migrations.AddIndex(
            model_name="dailytokenvolume",
            index=models.Index(
                fields=["token_address", "date"],
                name="analytics_dtv_token_date_idx",
            ),
        ),
        migrations.CreateModel(
            name="DailyActiveSafe",
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
                    "safe_address",
                    safe_eth.eth.django.models.EthereumAddressBinaryField(),
                ),
            ],
            options={},
        ),
        migrations.AddConstraint(
            model_name="dailyactivesafe",
            constraint=models.UniqueConstraint(
                fields=("date", "safe_address"),
                name="analytics_daily_active_safes_uniq",
            ),
        ),
        migrations.AddIndex(
            model_name="dailyactivesafe",
            index=models.Index(fields=["date"], name="analytics_das_date_idx"),
        ),
        migrations.AddIndex(
            model_name="dailyactivesafe",
            index=models.Index(
                fields=["safe_address", "date"],
                name="analytics_das_safe_date_idx",
            ),
        ),
        migrations.CreateModel(
            name="DailySafeAppTx",
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
                ("origin_name", models.CharField(max_length=255)),
                (
                    "origin_url",
                    models.CharField(blank=True, default="", max_length=512),
                ),
                ("tx_count", models.PositiveIntegerField(default=0)),
            ],
            options={},
        ),
        migrations.AddConstraint(
            model_name="dailysafeapptx",
            constraint=models.UniqueConstraint(
                fields=("date", "origin_name"),
                name="analytics_daily_safe_app_txs_uniq",
            ),
        ),
        migrations.AddIndex(
            model_name="dailysafeapptx",
            index=models.Index(fields=["date"], name="analytics_dsat_date_idx"),
        ),
        migrations.CreateModel(
            name="DailySafeCreation",
            fields=[
                (
                    "date",
                    models.DateField(primary_key=True, serialize=False),
                ),
                ("count", models.PositiveIntegerField(default=0)),
            ],
            options={},
        ),
    ]
