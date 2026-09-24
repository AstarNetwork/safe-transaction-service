import datetime

from django.db import migrations, models

# Day of the 2026-09-22/23 Postgres-connection incident that left several
# chains' `DailyMetric` rows unwritten and left Ethereum / quiet chains with
# a genuine-looking zero row that is indistinguishable from an honest zero
# (see "Analytics catch-up" in `analytics/implementation-notes.md`).
# Deliberately a constant baked into this migration file, not
# `timezone.now()` or an env var: the data migration below must produce the
# same result however many times or whenever it is (re)applied, on any
# machine, with any env set.
CATCHUP_MIGRATION_CUTOFF = datetime.date(2026, 9, 22)


def backfill_catchup_columns(apps, schema_editor):
    """Set `core_completed_at` / `completed_at` on existing `DailyMetric` rows.

    Every existing row gets both columns set to its own `computed_at`
    (it was written by the pre-catch-up nightly task, which only ever
    wrote a fully-computed row) — **except** rows dated on or after
    `CATCHUP_MIGRATION_CUTOFF` with `multisig_txs_via_api IS NULL`. Those
    are the incident's zero placeholders (a day written — or never
    written and later backfilled as a stub — without the tx-volume
    populator having actually run), and the whole point of this migration
    is to turn them into catch-up candidates rather than let the sweeper
    believe they are done. They are left with both columns NULL.

    `multisig_txs_via_api IS NULL` (rather than "row is all zero") is the
    signal the migration data itself carries forward from
    `0007_dailymetric_api_attribution_split`: that column is only ever
    NULL for a row the tx-volume populator has never touched.
    """
    DailyMetric = apps.get_model("analytics", "DailyMetric")
    DailyMetric.objects.exclude(
        date__gte=CATCHUP_MIGRATION_CUTOFF,
        multisig_txs_via_api__isnull=True,
    ).update(
        core_completed_at=models.F("computed_at"),
        completed_at=models.F("computed_at"),
    )


class Migration(migrations.Migration):
    """Catch-up state schema — see "Analytics catch-up" in
    `analytics/implementation-notes.md`.

    Two nullable `DailyMetric` columns (`core_completed_at`,
    `completed_at`) plus a new `AnalyticsCatchupState` table that holds
    all of the sweeper's attempt bookkeeping — see the model docstrings
    for what each field means and who is allowed to write it.

    The data migration backfills the two new columns for every existing
    `DailyMetric` row so the sweeper does not treat pre-existing history
    as "never computed"; the one deliberate exception is the incident's
    zero-placeholder rows (see `backfill_catchup_columns`), which are left
    NULL so the sweeper picks them up. It is a pure, deterministic
    function of the existing rows and the `CATCHUP_MIGRATION_CUTOFF`
    constant above — no env var, no `timezone.now()` — so it is safe to
    apply at any point after deploy and gives the same result every time.

    Reverse is a no-op for the data (nothing to undo: the forward
    migration only ever fills columns this migration itself adds, and
    `migrations.RunPython.noop` drops them along with the schema reversal
    below).

    Deliberately **not** included here: the `AutoField` → `BigAutoField`
    alter Django's migration autodetector also wants to emit on
    `dailyactiveowner` / `dailyactivesafe` / `dailysafeapptx` /
    `dailytokenvolume`. That drift predates this branch (see
    `0008_native_balance_rollup`'s docstring) and is still out of scope
    here; `makemigrations --check` will keep reporting those four
    unrelated `AlterField`s until someone picks that up on purpose.
    """

    dependencies = [
        ("analytics", "0008_native_balance_rollup"),
    ]

    operations = [
        migrations.AddField(
            model_name="dailymetric",
            name="core_completed_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="dailymetric",
            name="completed_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.CreateModel(
            name="AnalyticsCatchupState",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("kind", models.CharField(max_length=32)),
                ("key", models.CharField(max_length=64)),
                ("attempts", models.PositiveIntegerField(default=0)),
                ("next_attempt_at", models.DateTimeField(blank=True, null=True)),
                ("failed_steps", models.JSONField(blank=True, default=list)),
                ("core_ok", models.BooleanField(blank=True, null=True)),
                ("last_state", models.CharField(blank=True, max_length=32, null=True)),
                ("last_code", models.CharField(blank=True, max_length=64, null=True)),
                ("gave_up_logged_at", models.DateTimeField(blank=True, null=True)),
                ("expired_logged_at", models.DateTimeField(blank=True, null=True)),
                ("stuck_logged_at", models.DateTimeField(blank=True, null=True)),
                ("first_seen_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("observed_value", models.BigIntegerField(blank=True, null=True)),
                ("observed_since", models.DateTimeField(blank=True, null=True)),
                ("observed_count", models.BigIntegerField(blank=True, null=True)),
            ],
            options={
                "indexes": [
                    models.Index(
                        fields=["kind", "next_attempt_at"],
                        name="analytics_acs_kind_next_idx",
                    )
                ],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("kind", "key"),
                        name="analytics_catchup_state_kind_key_uniq",
                    )
                ],
            },
        ),
        migrations.RunPython(backfill_catchup_columns, migrations.RunPython.noop),
    ]
