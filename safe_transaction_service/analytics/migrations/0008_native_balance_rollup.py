import safe_eth.eth.django.models
from django.db import migrations, models


class Migration(migrations.Migration):
    """Storage for the incremental native-balance rollup.

    ``analytics_safenativebalance`` holds one signed net-flow row per
    Safe; ``analytics_analyticswatermark`` holds the single cursor
    (``name='native_balance'``) saying which block the rollup has
    consumed up to. Together they replace the nightly full recompute of
    every Safe's native balance — see the model docstrings and
    ``implementation-notes.md`` Part 8.

    Both tables are new and empty; nothing reads them until
    ``backfill_native_balances`` has filled them, so this migration is
    safe to apply ahead of the rollout. Rollback = ``migrate analytics
    0007``.

    Deliberately omitted: the ``AutoField`` → ``BigAutoField`` change on
    the four ``Daily*`` rollup PKs that ``makemigrations`` also wants to
    emit. That drift predates this branch (``DEFAULT_AUTO_FIELD`` moved
    under already-created models) and rewriting four rollup tables is not
    this change's business.
    """

    dependencies = [
        ("analytics", "0007_dailymetric_api_attribution_split"),
    ]

    operations = [
        migrations.CreateModel(
            name="AnalyticsWatermark",
            fields=[
                (
                    "name",
                    models.CharField(max_length=64, primary_key=True, serialize=False),
                ),
                ("block_number", models.PositiveIntegerField()),
                ("computed_at", models.DateTimeField()),
            ],
            options={
                "ordering": ["name"],
            },
        ),
        migrations.CreateModel(
            name="SafeNativeBalance",
            fields=[
                (
                    "safe_address",
                    safe_eth.eth.django.models.EthereumAddressBinaryField(
                        primary_key=True, serialize=False
                    ),
                ),
                (
                    "balance_wei",
                    models.DecimalField(decimal_places=0, default=0, max_digits=80),
                ),
                ("updated_to_block", models.PositiveIntegerField()),
            ],
        ),
    ]
