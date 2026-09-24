"""Tests for `DayResult` / `compute_day` (analytics/catchup/day.py) and the
`only=` retry path on `_upsert_daily_metric` (analytics/tasks.py).

See "Analytics catch-up" in `analytics/implementation-notes.md`.
"""

from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime, timedelta
from unittest.mock import patch

from django.test import TestCase

from safe_transaction_service.analytics.catchup.day import (
    CORE_INPUTS,
    POPULATORS,
    DayResult,
    DayStatus,
    compute_day,
)
from safe_transaction_service.analytics.catchup.gate import (
    DAY_NOT_READY,
    INDEXER_STATUS_UNAVAILABLE,
    IndexerStatus,
)
from safe_transaction_service.analytics.models import AnalyticsCatchupState, DailyMetric
from safe_transaction_service.analytics.tasks import _upsert_daily_metric

TASKS_MODULE = "safe_transaction_service.analytics.tasks"


def _settled_status(day: date) -> IndexerStatus:
    """An `IndexerStatus` comfortably past `day`'s settle threshold, with no
    unprocessed rows -- `ensure_day_settled` accepts it unconditionally."""
    position_ts = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(
        days=2
    )
    return IndexerStatus(
        erc20_block_number=1,
        erc20_block_timestamp=position_ts,
        master_copies_block_number=1,
        master_copies_block_timestamp=position_ts,
        oldest_unprocessed_ts=None,
        oldest_unprocessed_safe=None,
        relevant_master_copies=1,
    )


def _day_window(day: date) -> tuple[datetime, datetime]:
    day_start = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
    return day_start, day_start + timedelta(days=1)


class DayResultTestCase(TestCase):
    def test_frozen_and_status_is_dayresult_status(self):
        result = DayResult(status=DayStatus.DONE, core_ok=True)
        with self.assertRaises(FrozenInstanceError):
            result.status = DayStatus.INCOMPLETE
        self.assertIsInstance(result.status, DayStatus)

    def test_populators_and_core_inputs(self):
        self.assertEqual(
            POPULATORS,
            (
                "active_safes",
                "active_owners",
                "token_volume",
                "tx_volume",
                "safe_app_txs",
                "safe_creations",
            ),
        )
        self.assertEqual(CORE_INPUTS, frozenset({"active_safes", "active_owners"}))
        self.assertTrue(CORE_INPUTS.issubset(set(POPULATORS)))


class UpsertDailyMetricOnlyValidationTestCase(TestCase):
    def test_empty_only_raises(self):
        day_start, day_end = _day_window(date(2026, 9, 10))
        with self.assertRaises(ValueError):
            _upsert_daily_metric(day_start, day_end, only=[])

    def test_unknown_name_raises(self):
        day_start, day_end = _day_window(date(2026, 9, 10))
        with self.assertRaises(ValueError):
            _upsert_daily_metric(day_start, day_end, only=["not_a_populator"])

    def test_only_without_existing_row_raises(self):
        day_start, day_end = _day_window(date(2026, 9, 10))
        self.assertFalse(DailyMetric.objects.filter(date=day_start.date()).exists())
        with self.assertRaises(ValueError):
            _upsert_daily_metric(day_start, day_end, only=["tx_volume"])


class UpsertDailyMetricFullRunTestCase(TestCase):
    """No factory data -> every populator and the core succeed trivially
    (empty rollups, honest-zero core row), so a full run is the DONE
    happy path -- both completion marks land."""

    def test_full_run_success_sets_both_marks_and_done(self):
        day = date(2026, 9, 10)
        day_start, day_end = _day_window(day)

        result = _upsert_daily_metric(day_start, day_end)

        self.assertEqual(result.status, DayStatus.DONE)
        self.assertTrue(result.core_ok)
        self.assertEqual(result.failed, ())

        row = DailyMetric.objects.get(date=day)
        self.assertIsNotNone(row.core_completed_at)
        self.assertIsNotNone(row.completed_at)
        # Honest zero, not NULL -- no blocks in the window.
        self.assertEqual(row.multisig_txs_via_api, 0)
        self.assertEqual(row.multisig_txs_indexed_only, 0)

    def test_safe_app_txs_failure_leaves_day_visible_but_incomplete(self):
        """A populator unrelated to tx-volume fails: core_completed_at
        still lands (the day stays visible to `get_tx_volume`), but
        completed_at does not (the sweeper keeps retrying)."""
        day = date(2026, 9, 11)
        day_start, day_end = _day_window(day)

        with patch(
            f"{TASKS_MODULE}._compute_daily_safe_app_txs", side_effect=RuntimeError
        ):
            result = _upsert_daily_metric(day_start, day_end)

        self.assertEqual(result.status, DayStatus.INCOMPLETE)
        self.assertTrue(result.core_ok)
        self.assertEqual(result.failed, ("safe_app_txs",))

        row = DailyMetric.objects.get(date=day)
        self.assertIsNotNone(row.core_completed_at)
        self.assertIsNone(row.completed_at)

    def test_tx_volume_failure_hides_the_day_from_core_completed(self):
        day = date(2026, 9, 12)
        day_start, day_end = _day_window(day)

        with patch(
            f"{TASKS_MODULE}._compute_daily_tx_volume", side_effect=RuntimeError
        ):
            result = _upsert_daily_metric(day_start, day_end)

        self.assertEqual(result.status, DayStatus.INCOMPLETE)
        self.assertTrue(result.core_ok)
        self.assertEqual(result.failed, ("tx_volume",))

        row = DailyMetric.objects.get(date=day)
        self.assertIsNone(row.core_completed_at)
        self.assertIsNone(row.completed_at)

    def test_core_failure_leaves_both_marks_null(self):
        day = date(2026, 9, 13)
        day_start, day_end = _day_window(day)

        with patch(
            f"{TASKS_MODULE}._compute_daily_metric_core", side_effect=RuntimeError
        ):
            result = _upsert_daily_metric(day_start, day_end)

        self.assertEqual(result.status, DayStatus.INCOMPLETE)
        self.assertFalse(result.core_ok)
        self.assertEqual(result.failed, ())

        row = DailyMetric.objects.get(date=day)
        self.assertIsNone(row.core_completed_at)
        self.assertIsNone(row.completed_at)

    def test_recompute_of_a_done_day_that_fails_core_clears_both_marks(self):
        """A full recompute of an already-`completed_at` day that fails
        midway must not leave the day looking complete on stale data."""
        day = date(2026, 9, 14)
        day_start, day_end = _day_window(day)

        first = _upsert_daily_metric(day_start, day_end)
        self.assertEqual(first.status, DayStatus.DONE)

        with patch(
            f"{TASKS_MODULE}._compute_daily_metric_core", side_effect=RuntimeError
        ):
            second = _upsert_daily_metric(day_start, day_end)

        self.assertEqual(second.status, DayStatus.INCOMPLETE)
        row = DailyMetric.objects.get(date=day)
        self.assertIsNone(row.core_completed_at)
        self.assertIsNone(row.completed_at)

    #: Populator name -> the function `_upsert_daily_metric` dispatches to,
    #: for tests that need to patch one specific step.
    _FN_NAME = {
        "active_safes": "_compute_daily_active_safes",
        "active_owners": "_compute_daily_active_owners",
        "token_volume": "_compute_daily_token_volume",
        "tx_volume": "_compute_daily_tx_volume",
        "safe_app_txs": "_compute_daily_safe_app_txs",
        "safe_creations": "_compute_daily_safe_creations",
    }

    def test_hard_rule_completed_at_never_set_without_core_completed_at(self):
        """Sweep every single-populator failure and assert the invariant
        holds in each case: completed_at implies core_completed_at."""
        for offset, name in enumerate(POPULATORS):
            with self.subTest(populator=name):
                day = date(2026, 8, 1) + timedelta(days=offset)
                day_start, day_end = _day_window(day)
                with patch(
                    f"{TASKS_MODULE}.{self._FN_NAME[name]}", side_effect=RuntimeError
                ):
                    _upsert_daily_metric(day_start, day_end)
                row = DailyMetric.objects.get(date=day)
                if row.completed_at is not None:
                    self.assertIsNotNone(row.core_completed_at)


class UpsertDailyMetricOnlyRetryTestCase(TestCase):
    """`only=` retries against a day the sweeper already knows about --
    i.e. one with both a `DailyMetric` row and an `AnalyticsCatchupState`
    row, mirroring how `compute_day` / the sweeper actually drive this."""

    def _seed_state(self, day: date, *, core_ok, failed_steps):
        return AnalyticsCatchupState.objects.create(
            kind="day",
            key=day.isoformat(),
            core_ok=core_ok,
            failed_steps=failed_steps,
        )

    def test_only_core_inputs_reruns_core_and_merges_prior_failed(self):
        day = date(2026, 9, 15)
        day_start, day_end = _day_window(day)
        with patch(
            f"{TASKS_MODULE}._compute_daily_safe_creations", side_effect=RuntimeError
        ):
            _upsert_daily_metric(day_start, day_end)
        self._seed_state(day, core_ok=True, failed_steps=["safe_creations"])

        result = _upsert_daily_metric(day_start, day_end, only=["active_safes"])

        # active_safes reruns the core too (CORE_INPUTS); safe_creations
        # was never touched by this call, so it must still show up as
        # outstanding via the merge with the prior state row.
        self.assertTrue(result.core_ok)
        self.assertEqual(result.failed, ("safe_creations",))
        self.assertEqual(result.status, DayStatus.INCOMPLETE)
        row = DailyMetric.objects.get(date=day)
        self.assertIsNone(row.completed_at)
        self.assertIsNotNone(row.core_completed_at)

    def test_only_non_core_input_does_not_rerun_core(self):
        day = date(2026, 9, 16)
        day_start, day_end = _day_window(day)
        _upsert_daily_metric(day_start, day_end)
        self._seed_state(day, core_ok=True, failed_steps=[])

        with patch(f"{TASKS_MODULE}._compute_daily_metric_core") as mock_core:
            _upsert_daily_metric(day_start, day_end, only=["safe_creations"])
        mock_core.assert_not_called()

    def test_only_tx_volume_with_core_ok_sets_core_completed_at_without_core(self):
        """§0.14: a bare `tx_volume` retry, with the state row saying the
        core already succeeded, sets `core_completed_at` without rerunning
        the core."""
        day = date(2026, 9, 17)
        day_start, day_end = _day_window(day)
        with patch(
            f"{TASKS_MODULE}._compute_daily_tx_volume", side_effect=RuntimeError
        ):
            _upsert_daily_metric(day_start, day_end)
        row = DailyMetric.objects.get(date=day)
        self.assertIsNone(row.core_completed_at)
        self._seed_state(day, core_ok=True, failed_steps=["tx_volume"])

        with patch(f"{TASKS_MODULE}._compute_daily_metric_core") as mock_core:
            result = _upsert_daily_metric(day_start, day_end, only=["tx_volume"])
        mock_core.assert_not_called()

        self.assertTrue(result.core_ok)
        self.assertEqual(result.failed, ())
        self.assertEqual(result.status, DayStatus.DONE)
        row.refresh_from_db()
        self.assertIsNotNone(row.core_completed_at)
        self.assertIsNotNone(row.completed_at)

    def test_only_without_state_row_treats_core_as_not_known_good(self):
        """No `AnalyticsCatchupState` row (e.g. it was pruned) -> a
        non-core `only=` retry can't silently vouch for a core it never
        actually re-verified."""
        day = date(2026, 9, 18)
        day_start, day_end = _day_window(day)
        _upsert_daily_metric(day_start, day_end)
        self.assertFalse(
            AnalyticsCatchupState.objects.filter(
                kind="day", key=day.isoformat()
            ).exists()
        )

        result = _upsert_daily_metric(day_start, day_end, only=["safe_creations"])

        self.assertFalse(result.core_ok)
        self.assertEqual(result.status, DayStatus.INCOMPLETE)


class ComputeDayGateTestCase(TestCase):
    """`compute_day`'s own job on top of `_upsert_daily_metric`: run the
    gate (unless skipped) and persist the outcome onto the state row."""

    def test_status_none_without_skip_defers(self):
        day = date(2026, 9, 19)
        with self.assertLogs(
            "safe_transaction_service.analytics.catchup.day", "WARNING"
        ) as cm:
            result = compute_day(day, None)
        self.assertEqual(result.status, DayStatus.DEFERRED)
        self.assertEqual(result.code, INDEXER_STATUS_UNAVAILABLE)
        self.assertFalse(result.core_ok)
        self.assertTrue(any("analytics.daily.deferred" in line for line in cm.output))
        self.assertFalse(DailyMetric.objects.filter(date=day).exists())

    def test_status_none_with_skip_computes_anyway(self):
        day = date(2026, 9, 20)
        result = compute_day(day, None, skip_settle_check=True)
        self.assertEqual(result.status, DayStatus.DONE)
        self.assertTrue(DailyMetric.objects.filter(date=day).exists())

    def test_day_not_ready_defers_without_writing(self):
        day = date(2026, 9, 21)
        # A status whose position is before the day even ends.
        status = IndexerStatus(
            erc20_block_number=1,
            erc20_block_timestamp=datetime.combine(
                day, datetime.min.time(), tzinfo=UTC
            ),
            master_copies_block_number=1,
            master_copies_block_timestamp=datetime.combine(
                day, datetime.min.time(), tzinfo=UTC
            ),
            oldest_unprocessed_ts=None,
            oldest_unprocessed_safe=None,
            relevant_master_copies=1,
        )
        with self.assertLogs(
            "safe_transaction_service.analytics.catchup.day", "WARNING"
        ):
            result = compute_day(day, status)
        self.assertEqual(result.status, DayStatus.DEFERRED)
        self.assertEqual(result.code, DAY_NOT_READY)
        self.assertFalse(DailyMetric.objects.filter(date=day).exists())

    def test_settled_status_computes_the_day(self):
        day = date(2026, 9, 22)
        status = _settled_status(day)
        result = compute_day(day, status)
        self.assertEqual(result.status, DayStatus.DONE)
        self.assertTrue(DailyMetric.objects.filter(date=day).exists())


class ComputeDayStateWritebackTestCase(TestCase):
    """`compute_day` persists `DayResult` onto the existing
    `AnalyticsCatchupState` row -- and only an existing one."""

    def test_no_state_row_is_a_noop(self):
        day = date(2026, 9, 23)
        status = _settled_status(day)
        compute_day(day, status)
        self.assertFalse(
            AnalyticsCatchupState.objects.filter(
                kind="day", key=day.isoformat()
            ).exists()
        )

    def test_existing_state_row_is_updated(self):
        day = date(2026, 9, 24)
        status = _settled_status(day)
        AnalyticsCatchupState.objects.create(
            kind="day", key=day.isoformat(), core_ok=None, failed_steps=[]
        )

        with patch(
            f"{TASKS_MODULE}._compute_daily_safe_app_txs", side_effect=RuntimeError
        ):
            result = compute_day(day, status)

        state = AnalyticsCatchupState.objects.get(kind="day", key=day.isoformat())
        self.assertEqual(state.core_ok, result.core_ok)
        self.assertEqual(state.failed_steps, list(result.failed))
        self.assertEqual(state.last_state, DayStatus.INCOMPLETE.value)

    def test_review_scenario_full_recompute_then_only_retry_reaches_done(self):
        """Review-2 scenario: the sweeper previously failed on
        `safe_creations`; a manual full recompute then fails `tx_volume`
        instead -- `completed_at` must not appear until a further
        `only=["tx_volume"]` retry actually succeeds, and never without
        `core_completed_at`."""
        day = date(2026, 9, 25)
        status = _settled_status(day)
        AnalyticsCatchupState.objects.create(
            kind="day",
            key=day.isoformat(),
            core_ok=True,
            failed_steps=["safe_creations"],
        )

        with patch(
            f"{TASKS_MODULE}._compute_daily_tx_volume", side_effect=RuntimeError
        ):
            first = compute_day(day, status)
        self.assertEqual(first.status, DayStatus.INCOMPLETE)
        self.assertEqual(first.failed, ("tx_volume",))
        row = DailyMetric.objects.get(date=day)
        self.assertIsNone(row.core_completed_at)
        self.assertIsNone(row.completed_at)
        state = AnalyticsCatchupState.objects.get(kind="day", key=day.isoformat())
        self.assertEqual(state.failed_steps, ["tx_volume"])

        second = compute_day(day, status, only=["tx_volume"])

        self.assertEqual(second.status, DayStatus.DONE)
        row.refresh_from_db()
        self.assertIsNotNone(row.core_completed_at)
        self.assertIsNotNone(row.completed_at)
