"""Tests for the `0009_catchup_state` data migration and its admin.

Covers the acceptance criteria in `docs/specs/analytics-catchup.md` T2:
the data migration must set `core_completed_at` / `completed_at` to
`computed_at` for every existing `DailyMetric` row except the incident's
zero placeholders (`date >= CATCHUP_MIGRATION_CUTOFF` with
`multisig_txs_via_api IS NULL`), deterministically — independent of the
env and of "now" — and `AnalyticsCatchupState` must be registered in the
admin as fully read-only.
"""

import importlib
import os
from datetime import UTC, date, datetime
from unittest.mock import patch

from django.apps import apps as real_apps
from django.contrib.admin.sites import site
from django.test import RequestFactory, TestCase

from ..admin import AnalyticsCatchupStateAdmin
from ..models import AnalyticsCatchupState, DailyMetric

# Migration modules are named e.g. `0009_catchup_state.py` — not a valid
# Python identifier, so they can't be `import`-ed with a normal `import`
# statement and must be loaded by dotted string path instead.
_migration = importlib.import_module(
    "safe_transaction_service.analytics.migrations.0009_catchup_state"
)
CATCHUP_MIGRATION_CUTOFF = _migration.CATCHUP_MIGRATION_CUTOFF
backfill_catchup_columns = _migration.backfill_catchup_columns


class TestCatchupMigrationCutoff(TestCase):
    def test_cutoff_is_the_documented_constant(self) -> None:
        self.assertEqual(CATCHUP_MIGRATION_CUTOFF, date(2026, 9, 22))


class TestBackfillCatchupColumns(TestCase):
    """Runs the migration's forward function against real rows.

    Uses the real app registry (`django.apps.apps`) rather than
    `MigrationExecutor` — `backfill_catchup_columns(apps, schema_editor)`
    only calls `apps.get_model(...)`, which the real registry satisfies
    identically, and this keeps the test fast (no migration replay).
    """

    @classmethod
    def setUpTestData(cls) -> None:
        computed = datetime(2026, 1, 1, 2, 0, tzinfo=UTC)

        # Old row, long before the cutoff, with a real (non-NULL) split —
        # a normal fully-computed historical day.
        cls.old_split = DailyMetric.objects.create(
            date=date(2026, 1, 1),
            computed_at=computed,
            multisig_txs_via_api=3,
            multisig_txs_indexed_only=1,
        )
        # Old row, before the cutoff, but NEVER split (written before
        # 0007 landed) — still a genuine historical zero, not an incident
        # placeholder, so it should still be marked complete.
        cls.old_unsplit = DailyMetric.objects.create(
            date=date(2020, 5, 1),
            computed_at=computed,
            multisig_txs_via_api=None,
            multisig_txs_indexed_only=None,
        )
        # Cutoff day itself, unsplit -> incident placeholder, must stay NULL.
        cls.cutoff_unsplit = DailyMetric.objects.create(
            date=CATCHUP_MIGRATION_CUTOFF,
            computed_at=computed,
            multisig_txs_via_api=None,
            multisig_txs_indexed_only=None,
        )
        # Day after the cutoff, but split -> a legitimately computed day
        # on/after the cutoff, must be marked complete.
        cls.cutoff_split = DailyMetric.objects.create(
            date=date(2026, 9, 25),
            computed_at=computed,
            multisig_txs_via_api=5,
            multisig_txs_indexed_only=2,
        )
        # After the cutoff, unsplit -> placeholder, must stay NULL.
        cls.after_unsplit = DailyMetric.objects.create(
            date=date(2026, 9, 23),
            computed_at=computed,
            multisig_txs_via_api=None,
            multisig_txs_indexed_only=None,
        )
        # After the cutoff, split -> complete.
        cls.after_split = DailyMetric.objects.create(
            date=date(2026, 9, 24),
            computed_at=computed,
            multisig_txs_via_api=7,
            multisig_txs_indexed_only=0,
        )

    def _run(self) -> None:
        backfill_catchup_columns(apps=real_apps, schema_editor=None)

    def test_rows_before_cutoff_are_marked_complete_regardless_of_split(
        self,
    ) -> None:
        self._run()
        for row in (self.old_split, self.old_unsplit):
            row.refresh_from_db()
            self.assertEqual(row.core_completed_at, row.computed_at)
            self.assertEqual(row.completed_at, row.computed_at)

    def test_unsplit_rows_on_or_after_cutoff_stay_null(self) -> None:
        self._run()
        for row in (self.cutoff_unsplit, self.after_unsplit):
            row.refresh_from_db()
            self.assertIsNone(row.core_completed_at)
            self.assertIsNone(row.completed_at)

    def test_split_rows_on_or_after_cutoff_are_marked_complete(self) -> None:
        self._run()
        for row in (self.cutoff_split, self.after_split):
            row.refresh_from_db()
            self.assertEqual(row.core_completed_at, row.computed_at)
            self.assertEqual(row.completed_at, row.computed_at)

    def test_result_independent_of_now_and_env(self) -> None:
        """Same input rows -> same output, whatever `now()` or the env say.

        The migration must not read `timezone.now()` or any env var to
        decide the cutoff; it is baked into the module as a constant.
        """
        fake_now = datetime(2030, 1, 1, tzinfo=UTC)
        with (
            patch("django.utils.timezone.now", return_value=fake_now),
            patch.dict(
                os.environ, {"CATCHUP_MIGRATION_CUTOFF": "2020-01-01"}, clear=False
            ),
        ):
            self._run()

        self.cutoff_unsplit.refresh_from_db()
        self.after_unsplit.refresh_from_db()
        self.old_split.refresh_from_db()

        self.assertIsNone(self.cutoff_unsplit.core_completed_at)
        self.assertIsNone(self.after_unsplit.core_completed_at)
        self.assertEqual(self.old_split.core_completed_at, self.old_split.computed_at)


class TestAnalyticsCatchupStateAdmin(TestCase):
    request_factory = RequestFactory()

    def setUp(self) -> None:
        self.model_admin = AnalyticsCatchupStateAdmin(AnalyticsCatchupState, site)
        self.request = self.request_factory.get("/")

    def test_registered_in_admin_site(self) -> None:
        self.assertIn(AnalyticsCatchupState, site._registry)

    def test_add_permission_is_false(self) -> None:
        self.assertFalse(self.model_admin.has_add_permission(self.request))

    def test_change_permission_is_false(self) -> None:
        self.assertFalse(self.model_admin.has_change_permission(self.request))
        row = AnalyticsCatchupState.objects.create(kind="day", key="2026-09-22")
        self.assertFalse(self.model_admin.has_change_permission(self.request, row))

    def test_delete_permission_is_false(self) -> None:
        self.assertFalse(self.model_admin.has_delete_permission(self.request))
        row = AnalyticsCatchupState.objects.create(kind="day", key="2026-09-22")
        self.assertFalse(self.model_admin.has_delete_permission(self.request, row))
