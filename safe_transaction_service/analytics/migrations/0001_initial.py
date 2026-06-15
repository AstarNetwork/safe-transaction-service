from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="DailyMetric",
            fields=[
                (
                    "date",
                    models.DateField(primary_key=True, serialize=False),
                ),
                ("new_safes", models.PositiveIntegerField(default=0)),
                ("active_safes", models.PositiveIntegerField(default=0)),
                ("active_owners", models.PositiveIntegerField(default=0)),
                ("multisig_txs_executed", models.PositiveIntegerField(default=0)),
                ("module_txs", models.PositiveIntegerField(default=0)),
                ("erc20_transfers", models.PositiveIntegerField(default=0)),
                (
                    "native_value_wei",
                    models.DecimalField(decimal_places=0, default=0, max_digits=80),
                ),
                ("computed_at", models.DateTimeField()),
            ],
            options={
                "ordering": ["-date"],
            },
        ),
    ]
