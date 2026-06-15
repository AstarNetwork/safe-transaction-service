"""Drop the ``safe_statistics`` row from ``analytics_analyticssnapshot``.

The ``/v2/analytics/safe-statistics/`` endpoint was retired in favour of
``/summary/`` (see plan ``robust-wandering-spark.md``). On already-deployed
instances ``compute_summary_task`` will keep writing the ``summary`` row;
the leftover ``safe_statistics`` row would just sit dead-weight in the
table forever. This is a one-shot delete; reverse is a no-op because the
schema isn't changing — only one application-managed row by ``name``.
"""

from django.db import migrations


def drop_safe_statistics_row(apps, schema_editor):
    AnalyticsSnapshot = apps.get_model("analytics", "AnalyticsSnapshot")
    AnalyticsSnapshot.objects.filter(name="safe_statistics").delete()


class Migration(migrations.Migration):
    dependencies = [
        ("analytics", "0004_dailymetric_tx_volume_cols"),
    ]

    operations = [
        migrations.RunPython(drop_safe_statistics_row, migrations.RunPython.noop),
    ]
