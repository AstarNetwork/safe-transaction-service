"""Tests for the ``analytics_settle_status`` read-only diagnostics command.

Most scenarios mock ``get_indexer_status`` at the command's own module
attribute (``unittest.mock.patch`` targets the name where it's looked up)
rather than exercising the real gate against ganache -- that's already
covered by ``test_catchup_gate.py``. ``_diagnose_unavailable`` is tested
directly against the real test database, the same way ``get_indexer_status``
itself is tested in ``test_catchup_gate.py``, since its whole job is to
re-derive *why* that function failed.
"""

import os
from datetime import UTC, date, datetime
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from eth_account import Account

from safe_transaction_service.analytics.catchup.gate import (
    DAY_NOT_READY,
    INDEXER_STATUS_UNAVAILABLE,
    DayNotReady,
    IndexerStatus,
)
from safe_transaction_service.analytics.conf import reset_catchup_settings_cache
from safe_transaction_service.analytics.management.commands.analytics_settle_status import (
    _diagnose_unavailable,
)
from safe_transaction_service.analytics.models import AnalyticsCatchupState, DailyMetric
from safe_transaction_service.history.models import IndexingStatus, SafeMasterCopy

_COMMAND = (
    "safe_transaction_service.analytics.management.commands.analytics_settle_status"
)

# Both indexer pipelines parked at 2026-09-20 00:30 UTC: exactly the
# settled threshold (day_end + default 30 min) for day 2026-09-19, and one
# threshold short for 2026-09-20 -- so, walking the default 14-day window
# ending 2026-09-22, every day up to and including 2026-09-19 passes the
# gate and every day from 2026-09-20 on fails it with DAY_NOT_READY.
_POSITION_TS = datetime(2026, 9, 20, 0, 30, tzinfo=UTC)
# Well after every window day's end, so it never blocks any of them on
# PROCESSING_PENDING -- only exercises the header's "oldest_unprocessed"
# line.
_OLDEST_UNPROCESSED_TS = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
_OLDEST_UNPROCESSED_SAFE = "0xAAAABBBBCCCCDDDDEEEEFFFF0000111122223333"

_SETTLED_STATUS = IndexerStatus(
    erc20_block_number=555,
    erc20_block_timestamp=_POSITION_TS,
    master_copies_block_number=555,
    master_copies_block_timestamp=_POSITION_TS,
    oldest_unprocessed_ts=_OLDEST_UNPROCESSED_TS,
    oldest_unprocessed_safe=_OLDEST_UNPROCESSED_SAFE,
    relevant_master_copies=3,
)

_NOW = datetime(2026, 9, 23, 5, 0, tzinfo=UTC)


def _run_command() -> str:
    out = StringIO()
    call_command("analytics_settle_status", stdout=out)
    return out.getvalue()


class DiagnoseUnavailableTestCase(TestCase):
    """``_diagnose_unavailable()`` against the real test database -- each
    branch mirrors a way ``get_indexer_status()`` itself can fail."""

    def _make_master_copy(self) -> SafeMasterCopy:
        return SafeMasterCopy.objects.create(
            address=Account.create().address,
            initial_block_number=0,
            tx_block_number=1,
            version="1.4.1",
            deployer="test",
            l2=True,
        )

    def test_no_relevant_master_copies(self):
        # No SafeMasterCopy rows at all.
        self.assertEqual(_diagnose_unavailable(), "no relevant master copies")

    def test_no_indexing_status_row(self):
        self._make_master_copy()
        # Migration 0069 always leaves one ERC20_721_EVENTS row behind;
        # remove it so the check sees a truly empty table.
        IndexingStatus.objects.all().delete()
        self.assertEqual(_diagnose_unavailable(), "no IndexingStatus row")

    def test_rpc_error_reports_class_name_without_leaking_text(self):
        self._make_master_copy()
        secret = "https://secret-node.example/API_KEY=abcd1234"
        with patch(f"{_COMMAND}.IndexServiceProvider") as mock_provider:
            mock_provider.return_value.get_indexing_status.side_effect = (
                ConnectionError(secret)
            )
            reason = _diagnose_unavailable()
        self.assertEqual(reason, "RPC/DB error: ConnectionError")
        self.assertNotIn(secret, reason)


class AnalyticsSettleStatusCommandTestCase(TestCase):
    def setUp(self):
        reset_catchup_settings_cache()
        self.addCleanup(reset_catchup_settings_cache)

    def test_indexer_status_unavailable_shows_fixed_code_and_detail(self):
        # No SafeMasterCopy rows -> get_indexer_status() itself raises, and
        # the command's own diagnosis lands on the same reason.
        with patch(
            f"{_COMMAND}.get_indexer_status",
            side_effect=DayNotReady(INDEXER_STATUS_UNAVAILABLE),
        ):
            output = _run_command()
        self.assertIn(
            f"indexer_status: unavailable ({INDEXER_STATUS_UNAVAILABLE}) -- "
            "no relevant master copies",
            output,
        )
        self.assertIn(
            "gate: indexer status unavailable, cannot evaluate window", output
        )

    def test_bad_config_shows_setting_name_and_skips_gate(self):
        with patch(f"{_COMMAND}.get_indexer_status", return_value=_SETTLED_STATUS):
            with patch.dict(os.environ, {"ANALYTICS_CATCHUP_WINDOW_DAYS": "1"}):
                output = _run_command()
        self.assertIn(
            "settings: bad_config setting=ANALYTICS_CATCHUP_WINDOW_DAYS", output
        )
        self.assertIn("gate: skipped (settings invalid", output)
        self.assertNotIn("window (", output)

    def test_full_report(self):
        # A completed day the sweeper has retired its state row for.
        DailyMetric.objects.create(
            date=date(2026, 9, 10),
            computed_at=_NOW,
            core_completed_at=_NOW,
            completed_at=_NOW,
        )
        # A day with a row that exists but is unmarked -- written, still
        # incomplete, distinct from "never written at all" below.
        DailyMetric.objects.create(date=date(2026, 9, 12), computed_at=_NOW)
        # A day still being retried: no DailyMetric row at all, a state row
        # with counters, and an expired marker already fired.
        AnalyticsCatchupState.objects.create(
            kind="day",
            key="2026-09-21",
            attempts=3,
            next_attempt_at=_NOW,
            failed_steps=["tx_volume"],
            core_ok=False,
            last_state="deferred",
            last_code=DAY_NOT_READY,
            expired_logged_at=_NOW,
        )
        AnalyticsCatchupState.objects.create(
            kind="processing",
            key="oldest",
            observed_value=1_758_000_000,
            observed_since=_NOW,
            observed_count=5,
        )

        with patch(f"{_COMMAND}.get_indexer_status", return_value=_SETTLED_STATUS):
            with patch(f"{_COMMAND}.timezone") as mock_timezone:
                mock_timezone.now.return_value = _NOW
                with CaptureQueriesContext(connection) as ctx:
                    output = _run_command()

        for query in ctx.captured_queries:
            sql = query["sql"].strip().upper()
            self.assertFalse(
                sql.startswith(("INSERT", "UPDATE", "DELETE")),
                msg=f"unexpected write query: {query['sql']}",
            )

        self.assertIn("erc20: block=555 ts=2026-09-20T00:30:00+00:00", output)
        self.assertIn(
            "master_copies: block=555 ts=2026-09-20T00:30:00+00:00 relevant_count=3",
            output,
        )
        self.assertIn(
            f"oldest_unprocessed: ts={_OLDEST_UNPROCESSED_TS.isoformat()} "
            f"safe={_OLDEST_UNPROCESSED_SAFE}",
            output,
        )
        self.assertIn("settings: ok", output)
        self.assertIn(
            "gate: latest_passing_day=2026-09-19 first_failing_day=2026-09-20 "
            f"reason={DAY_NOT_READY}",
            output,
        )
        self.assertIn("window (14 days, 2026-09-09 .. 2026-09-22):", output)
        self.assertIn(
            "  2026-09-10: core_completed_at=2026-09-23T05:00:00+00:00 "
            "completed_at=2026-09-23T05:00:00+00:00 (no catch-up state row)",
            output,
        )
        self.assertIn(
            "  2026-09-12: core_completed_at=None completed_at=None "
            "(no catch-up state row)",
            output,
        )
        self.assertIn(
            "  2026-09-21: row=missing attempts=3 "
            "next_attempt_at=2026-09-23T05:00:00+00:00 failed_steps=['tx_volume'] "
            "last_state=deferred last_code=day_not_ready gave_up=no expired=yes",
            output,
        )
        self.assertIn(
            "processing_watchdog: observed_value=1758000000 observed_count=5 "
            "observed_since=2026-09-23T05:00:00+00:00 stuck_logged_at=None",
            output,
        )

    def test_no_processing_observation_yet(self):
        with patch(f"{_COMMAND}.get_indexer_status", return_value=_SETTLED_STATUS):
            with patch(f"{_COMMAND}.timezone") as mock_timezone:
                mock_timezone.now.return_value = _NOW
                output = _run_command()
        self.assertIn("processing_watchdog: no observation yet", output)
