"""Tests for the indexer-status gate: ``get_indexer_status``, ``ensure_day_settled``,
``DayNotReady``.

Two kinds of test here. ``ensure_day_settled`` is a pure function, so its
boundary cases are plain unit tests against hand-built ``IndexerStatus``
values -- no database, no chain. ``get_indexer_status`` does real I/O (one
query to the database, one call through ``IndexServiceProvider`` to the
chain), so those tests run against the real test database and ganache, the
same way ``history/tests/test_index_service.py`` does.
"""

import subprocess
import sys
from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch

from django.db import connection
from django.test import SimpleTestCase, TestCase
from django.test.utils import CaptureQueriesContext

from eth_account import Account
from safe_eth.eth.tests.ethereum_test_case import EthereumTestCaseMixin
from safe_eth.eth.utils import fast_keccak_text
from safe_eth.util.util import to_0x_hex_str

from safe_transaction_service.analytics.catchup.gate import (
    DAY_NOT_READY,
    INDEXER_STATUS_UNAVAILABLE,
    PROCESSING_PENDING,
    DayNotReady,
    IndexerStatus,
    ensure_day_settled,
    get_indexer_status,
)
from safe_transaction_service.analytics.conf import reset_catchup_settings_cache
from safe_transaction_service.history.models import (
    EthereumBlock,
    EthereumTx,
    EthereumTxCallType,
    IndexingStatus,
    IndexingStatusType,
    InternalTx,
    InternalTxDecoded,
    InternalTxType,
    ProxyFactory,
    SafeMasterCopy,
)
from safe_transaction_service.history.services.index_service import (
    IndexServiceProvider,
)

_UTC = UTC


def _status(
    *,
    position_ts: datetime,
    oldest_unprocessed_ts: datetime | None = None,
    oldest_unprocessed_safe: str | None = None,
) -> IndexerStatus:
    """An ``IndexerStatus`` with both indexer positions equal to ``position_ts``."""
    return IndexerStatus(
        erc20_block_number=1,
        erc20_block_timestamp=position_ts,
        master_copies_block_number=1,
        master_copies_block_timestamp=position_ts,
        oldest_unprocessed_ts=oldest_unprocessed_ts,
        oldest_unprocessed_safe=oldest_unprocessed_safe,
        relevant_master_copies=1,
    )


class EnsureDaySettledTestCase(SimpleTestCase):
    """Pure-function boundary tests: no DB, no mocking of settings needed
    beyond making sure no other test module's env patch leaked into the
    cached ``SETTLE_MINUTES`` (default 30)."""

    def setUp(self):
        reset_catchup_settings_cache()
        self.addCleanup(reset_catchup_settings_cache)

    day = date(2026, 9, 20)
    day_end = datetime(2026, 9, 21, 0, 0, tzinfo=_UTC)
    threshold = datetime(2026, 9, 21, 0, 30, tzinfo=_UTC)  # day_end + 30 min default

    def test_settled_exactly_at_threshold(self):
        status = _status(position_ts=self.threshold)
        ensure_day_settled(self.day, status)  # does not raise

    def test_not_settled_one_second_before_threshold(self):
        status = _status(position_ts=self.threshold - datetime.resolution)
        with self.assertRaises(DayNotReady) as ctx:
            ensure_day_settled(self.day, status)
        self.assertEqual(ctx.exception.code, DAY_NOT_READY)

    def test_processing_pending_when_unprocessed_row_inside_day(self):
        status = _status(
            position_ts=self.threshold,
            oldest_unprocessed_ts=self.day_end - datetime.resolution,
        )
        with self.assertRaises(DayNotReady) as ctx:
            ensure_day_settled(self.day, status)
        self.assertEqual(ctx.exception.code, PROCESSING_PENDING)

    def test_unprocessed_row_exactly_at_day_end_does_not_block(self):
        status = _status(position_ts=self.threshold, oldest_unprocessed_ts=self.day_end)
        ensure_day_settled(self.day, status)  # does not raise

    def test_unprocessed_row_after_day_end_does_not_block(self):
        status = _status(
            position_ts=self.threshold,
            oldest_unprocessed_ts=self.day_end + datetime.resolution,
        )
        ensure_day_settled(self.day, status)  # does not raise

    def test_no_unprocessed_rows_does_not_block(self):
        status = _status(position_ts=self.threshold, oldest_unprocessed_ts=None)
        ensure_day_settled(self.day, status)  # does not raise

    def test_position_ts_is_min_of_the_two_pipelines(self):
        earlier = datetime(2026, 9, 21, 0, 0, tzinfo=_UTC)
        later = datetime(2026, 9, 22, 0, 0, tzinfo=_UTC)
        status = IndexerStatus(
            erc20_block_number=1,
            erc20_block_timestamp=later,
            master_copies_block_number=1,
            master_copies_block_timestamp=earlier,
            oldest_unprocessed_ts=None,
            oldest_unprocessed_safe=None,
            relevant_master_copies=1,
        )
        self.assertEqual(status.position_ts, earlier)

    def test_indexer_status_is_frozen(self):
        status = _status(position_ts=self.threshold)
        with self.assertRaises(FrozenInstanceError):
            status.relevant_master_copies = 2

    def test_day_not_ready_carries_code_and_no_secret_text(self):
        exc = DayNotReady(INDEXER_STATUS_UNAVAILABLE)
        self.assertEqual(exc.code, INDEXER_STATUS_UNAVAILABLE)
        self.assertEqual(str(exc), INDEXER_STATUS_UNAVAILABLE)


class GetIndexerStatusTestCase(EthereumTestCaseMixin, TestCase):
    """``get_indexer_status`` against the real test DB and ganache."""

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        IndexServiceProvider.del_singleton()

    def _make_master_copy(self):
        return SafeMasterCopy.objects.create(
            address=Account.create().address,
            initial_block_number=0,
            tx_block_number=1,
            version="1.4.1",
            deployer="test",
            l2=True,  # included by `.relevant()` whether or not the network is L2
        )

    def _make_indexing_status(self, block_number=1):
        # A data migration (history/migrations/0069) always leaves one
        # ERC20_721_EVENTS row behind, so upsert rather than create.
        status, _ = IndexingStatus.objects.update_or_create(
            indexing_type=IndexingStatusType.ERC20_721_EVENTS.value,
            defaults={"block_number": block_number},
        )
        return status

    def _make_internal_tx_decoded(
        self, *, safe_address, timestamp, processed, seq
    ) -> InternalTxDecoded:
        block = EthereumBlock.objects.create(
            number=1000 + seq,
            gas_limit=30_000_000,
            gas_used=100_000,
            timestamp=timestamp,
            block_hash=to_0x_hex_str(fast_keccak_text(f"gate-block-{seq}")),
            parent_hash=to_0x_hex_str(fast_keccak_text(f"gate-parent-{seq}")),
        )
        ethereum_tx = EthereumTx.objects.create(
            block=block,
            tx_hash=to_0x_hex_str(fast_keccak_text(f"gate-tx-{seq}")),
            _from=safe_address,
            gas=100_000,
            gas_price=1,
            data=b"",
            nonce=seq,
            to=Account.create().address,
            value=0,
            type=0,
        )
        internal_tx = InternalTx.objects.create(
            ethereum_tx=ethereum_tx,
            timestamp=timestamp,
            block_number=block.number,
            _from=safe_address,
            gas=100_000,
            data=b"",
            to=Account.create().address,
            value=0,
            gas_used=100_000,
            tx_type=InternalTxType.CALL.value,
            call_type=EthereumTxCallType.CALL.value,
            trace_address=str(seq),
        )
        return InternalTxDecoded.objects.create(
            internal_tx=internal_tx,
            function_name="execTransaction",
            arguments={},
            processed=processed,
            safe_address=safe_address,
        )

    def test_no_relevant_master_copies_is_unavailable(self):
        # No SafeMasterCopy rows at all -- upstream would otherwise stand in
        # for a chain head and report "synced" with nothing behind it.
        with self.assertRaises(DayNotReady) as ctx:
            get_indexer_status()
        self.assertEqual(ctx.exception.code, INDEXER_STATUS_UNAVAILABLE)
        self.assertIsNone(ctx.exception.__cause__)
        self.assertTrue(ctx.exception.__suppress_context__)

    def test_no_indexing_status_row_is_unavailable(self):
        self._make_master_copy()
        # A data migration (history/migrations/0069) always leaves one
        # ERC20_721_EVENTS row behind; remove it so upstream's `.get()`
        # raises DoesNotExist, as it would on a truly empty table.
        IndexingStatus.objects.all().delete()
        with self.assertRaises(DayNotReady) as ctx:
            get_indexer_status()
        self.assertEqual(ctx.exception.code, INDEXER_STATUS_UNAVAILABLE)
        self.assertIsNone(ctx.exception.__cause__)
        self.assertTrue(ctx.exception.__suppress_context__)

    def test_rpc_error_is_unavailable_without_leaking_error_text(self):
        self._make_master_copy()
        self._make_indexing_status()
        secret = "https://secret-node.example/API_KEY=abcd1234"

        with patch(
            "safe_transaction_service.analytics.catchup.gate.IndexServiceProvider"
        ) as mock_provider:
            mock_provider.return_value.get_indexing_status.side_effect = (
                ConnectionError(secret)
            )
            with self.assertRaises(DayNotReady) as ctx:
                get_indexer_status()

        self.assertEqual(ctx.exception.code, INDEXER_STATUS_UNAVAILABLE)
        self.assertIsNone(ctx.exception.__cause__)
        self.assertTrue(ctx.exception.__suppress_context__)
        self.assertNotIn(secret, str(ctx.exception))
        self.assertNotIn(secret, repr(ctx.exception.args))

    def test_proxy_factory_position_does_not_affect_status(self):
        self._make_master_copy()
        self._make_indexing_status()
        # A frozen proxy-factory position (index_new_proxies_task is
        # disabled) must not stop the gate from getting a status at all.
        ProxyFactory.objects.create(
            address=Account.create().address,
            initial_block_number=0,
            tx_block_number=0,
        )
        status = get_indexer_status()
        self.assertIsInstance(status, IndexerStatus)

    def test_no_unprocessed_rows_gives_none(self):
        self._make_master_copy()
        self._make_indexing_status()
        safe_address = Account.create().address
        self._make_internal_tx_decoded(
            safe_address=safe_address,
            timestamp=datetime(2026, 9, 20, tzinfo=_UTC),
            processed=True,
            seq=1,
        )

        status = get_indexer_status()

        self.assertIsNone(status.oldest_unprocessed_ts)
        self.assertIsNone(status.oldest_unprocessed_safe)
        self.assertEqual(status.relevant_master_copies, 1)

    def test_oldest_unprocessed_row_and_exactly_one_query(self):
        self._make_master_copy()
        self._make_indexing_status()

        older_safe = Account.create().address
        newer_safe = Account.create().address
        older = self._make_internal_tx_decoded(
            safe_address=older_safe,
            timestamp=datetime(2026, 9, 18, tzinfo=_UTC),
            processed=False,
            seq=1,
        )
        self._make_internal_tx_decoded(
            safe_address=newer_safe,
            timestamp=datetime(2026, 9, 19, tzinfo=_UTC),
            processed=False,
            seq=2,
        )
        # A processed row, even an older one, must not count.
        self._make_internal_tx_decoded(
            safe_address=Account.create().address,
            timestamp=datetime(2026, 9, 1, tzinfo=_UTC),
            processed=True,
            seq=3,
        )

        with CaptureQueriesContext(connection) as captured:
            status = get_indexer_status()

        decoded_queries = [
            q["sql"]
            for q in captured.captured_queries
            if "history_internaltxdecoded" in q["sql"]
        ]
        self.assertEqual(len(decoded_queries), 1, decoded_queries)

        self.assertEqual(status.oldest_unprocessed_ts, older.internal_tx.timestamp)
        self.assertEqual(status.oldest_unprocessed_safe, older_safe)


class CatchupPackageImportTestCase(SimpleTestCase):
    def test_importing_catchup_does_not_import_tasks(self):
        script = (
            "import django, os, sys\n"
            "os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.test')\n"
            "django.setup()\n"
            "import safe_transaction_service.analytics.catchup\n"
            "assert 'safe_transaction_service.analytics.tasks' not in sys.modules, sys.modules.keys()\n"
            "print('OK')\n"
        )
        repo_root = Path(__file__).resolve().parents[3]
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(repo_root),
        )
        self.assertEqual(
            result.returncode,
            0,
            f"stdout={result.stdout!r} stderr={result.stderr!r}",
        )
        self.assertIn("OK", result.stdout)
