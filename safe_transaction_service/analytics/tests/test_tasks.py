import json
from unittest.mock import patch

from django.test import TestCase

from safe_transaction_service.analytics.models import (
    AnalyticsSnapshot,
    DailyActiveOwner,
    DailyMetric,
)
from safe_transaction_service.analytics.services.analytics_service import (
    AnalyticsService,
)
from safe_transaction_service.analytics.services.db import approx_count_or_exact
from safe_transaction_service.analytics.tasks import (
    _active_owners_in_window,
    _calculate_native_balances_from_db,
    _safes_active_in_window,
    _upsert_daily_metric,
    compute_active_owners_task,
    compute_active_safes_task,
    compute_daily_metrics_task,
    compute_safe_creations_task,
    compute_summary_task,
    get_transactions_per_safe_app_task,
)
from safe_transaction_service.history.models import (
    EthereumTxCallType,
    MultisigTransaction,
    SafeContract,
)
from safe_transaction_service.history.tests.factories import (
    ERC20TransferFactory,
    ERC721TransferFactory,
    InternalTxFactory,
    ModuleTransactionFactory,
    MultisigConfirmationFactory,
    MultisigTransactionFactory,
    SafeContractFactory,
    SafeStatusFactory,
)
from safe_transaction_service.utils.redis import get_redis


class TestCalculateNativeBalancesFromDb(TestCase):
    def test_empty_safe_contracts(self):
        total_balance, safes_with_balance = _calculate_native_balances_from_db()
        self.assertEqual(total_balance, 0)
        self.assertEqual(safes_with_balance, 0)

    def test_safe_with_only_incoming(self):
        safe = SafeContractFactory()
        InternalTxFactory(
            to=safe.address,
            value=1000,
            call_type=EthereumTxCallType.CALL.value,
            error=None,
        )
        total_balance, safes_with_balance = _calculate_native_balances_from_db()
        self.assertEqual(total_balance, 1000)
        self.assertEqual(safes_with_balance, 1)

    def test_safe_with_only_outgoing(self):
        safe = SafeContractFactory()
        InternalTxFactory(
            _from=safe.address,
            value=500,
            call_type=EthereumTxCallType.CALL.value,
            error=None,
        )
        total_balance, safes_with_balance = _calculate_native_balances_from_db()
        self.assertEqual(total_balance, 0)
        self.assertEqual(safes_with_balance, 0)

    def test_safe_with_positive_net_balance(self):
        safe = SafeContractFactory()
        InternalTxFactory(
            to=safe.address,
            value=1000,
            call_type=EthereumTxCallType.CALL.value,
            error=None,
        )
        InternalTxFactory(
            _from=safe.address,
            value=300,
            call_type=EthereumTxCallType.CALL.value,
            error=None,
        )
        total_balance, safes_with_balance = _calculate_native_balances_from_db()
        self.assertEqual(total_balance, 700)
        self.assertEqual(safes_with_balance, 1)

    def test_safe_with_zero_net_balance(self):
        safe = SafeContractFactory()
        InternalTxFactory(
            to=safe.address,
            value=500,
            call_type=EthereumTxCallType.CALL.value,
            error=None,
        )
        InternalTxFactory(
            _from=safe.address,
            value=500,
            call_type=EthereumTxCallType.CALL.value,
            error=None,
        )
        total_balance, safes_with_balance = _calculate_native_balances_from_db()
        self.assertEqual(total_balance, 0)
        self.assertEqual(safes_with_balance, 0)

    def test_error_transactions_excluded(self):
        safe = SafeContractFactory()
        # Successful incoming
        InternalTxFactory(
            to=safe.address,
            value=1000,
            call_type=EthereumTxCallType.CALL.value,
            error=None,
        )
        # Failed incoming — should be excluded
        InternalTxFactory(
            to=safe.address,
            value=5000,
            call_type=EthereumTxCallType.CALL.value,
            error="Reverted",
        )
        total_balance, safes_with_balance = _calculate_native_balances_from_db()
        self.assertEqual(total_balance, 1000)
        self.assertEqual(safes_with_balance, 1)

    def test_non_safe_address_excluded(self):
        """InternalTx for addresses not in SafeContract should not be counted."""
        safe = SafeContractFactory()
        # Incoming to the Safe
        InternalTxFactory(
            to=safe.address,
            value=100,
            call_type=EthereumTxCallType.CALL.value,
            error=None,
        )
        # Incoming to a non-Safe address (no SafeContract record)
        InternalTxFactory(
            value=9999,
            call_type=EthereumTxCallType.CALL.value,
            error=None,
        )
        total_balance, safes_with_balance = _calculate_native_balances_from_db()
        self.assertEqual(total_balance, 100)
        self.assertEqual(safes_with_balance, 1)

    def test_multiple_safes(self):
        safe1 = SafeContractFactory()
        safe2 = SafeContractFactory()
        InternalTxFactory(
            to=safe1.address,
            value=2000,
            call_type=EthereumTxCallType.CALL.value,
            error=None,
        )
        InternalTxFactory(
            to=safe2.address,
            value=3000,
            call_type=EthereumTxCallType.CALL.value,
            error=None,
        )
        InternalTxFactory(
            _from=safe2.address,
            value=1000,
            call_type=EthereumTxCallType.CALL.value,
            error=None,
        )
        total_balance, safes_with_balance = _calculate_native_balances_from_db()
        # safe1: 2000, safe2: 3000-1000=2000
        self.assertEqual(total_balance, 4000)
        self.assertEqual(safes_with_balance, 2)


class TestTasks(TestCase):
    def test_get_transactions_per_safe_apps(self):
        redis = get_redis()
        redis.flushall()
        redis_key = AnalyticsService.REDIS_TRANSACTIONS_PER_SAFE_APP
        origin_1 = {"url": "https://example1.com", "name": "SafeApp1"}
        origin_2 = {"url": "https://example2.com", "name": "SafeApp2"}
        string_origin = "test"
        expected = [
            {
                "name": "SafeApp2",
                "url": "https://example2.com",
                "total_tx": 7,
                "tx_last_week": 7,
                "tx_last_month": 7,
                "tx_last_year": 7,
            },
            {
                "name": "SafeApp1",
                "url": "https://example1.com",
                "total_tx": 3,
                "tx_last_week": 3,
                "tx_last_month": 3,
                "tx_last_year": 3,
            },
        ]
        for _ in range(3):
            MultisigTransactionFactory(origin=origin_1)
        for _ in range(7):
            MultisigTransactionFactory(origin=origin_2)
        MultisigTransactionFactory(origin=string_origin)

        self.assertEqual(MultisigTransaction.objects.count(), 11)
        value = redis.get(redis_key)
        self.assertIsNone(value)
        # Execute the task to get data from database
        get_transactions_per_safe_app_task()
        # Get the result from redis
        value = redis.get(redis_key)
        analytic = json.loads(value)

        self.assertEqual(analytic, expected)

    def test_compute_summary_task_writes_snapshot(self):
        """`compute_summary_task` populates the `summary` row in
        `analytics_analyticssnapshot` and writes nothing to Redis."""
        redis = get_redis()
        redis.flushall()

        SafeContractFactory()
        SafeContractFactory()
        MultisigTransactionFactory()
        ModuleTransactionFactory()
        ERC20TransferFactory()
        ERC721TransferFactory()

        compute_summary_task.delay()

        summary = AnalyticsSnapshot.objects.get(name="summary").payload
        self.assertEqual(summary["total_safes"], 2)
        self.assertEqual(summary["total_multisig_txs"], 1)
        self.assertEqual(summary["total_module_txs"], 1)
        self.assertEqual(summary["total_erc20_transfers"], 1)
        self.assertEqual(summary["total_erc721_transfers"], 1)
        self.assertIsNotNone(summary["first_safe_created"])
        self.assertIsNotNone(summary["last_safe_created"])
        self.assertIsNotNone(summary["computed_at"])

        # Legacy Redis key must not be written.
        self.assertIsNone(redis.get(AnalyticsService.REDIS_SUMMARY))
        # `safe_statistics` snapshot must not be produced — the endpoint
        # was retired and this task no longer writes that row.
        self.assertFalse(
            AnalyticsSnapshot.objects.filter(name="safe_statistics").exists()
        )

    def test_compute_summary_task_empty_db_writes_snapshot_with_zeros(self):
        redis = get_redis()
        redis.flushall()
        AnalyticsSnapshot.objects.all().delete()

        compute_summary_task.delay()

        summary = AnalyticsSnapshot.objects.get(name="summary").payload
        self.assertEqual(summary["total_safes"], 0)
        self.assertEqual(summary["total_multisig_txs"], 0)
        self.assertEqual(summary["total_module_txs"], 0)
        self.assertIsNone(summary["first_safe_created"])
        self.assertIsNone(summary["last_safe_created"])
        self.assertIsNotNone(summary["computed_at"])

    def test_compute_safe_creations_task_writes_day_series(self):
        redis = get_redis()
        redis.flushall()

        SafeContractFactory()
        SafeContractFactory()
        SafeContractFactory()

        compute_safe_creations_task()

        payload = json.loads(redis.get(AnalyticsService.REDIS_SAFE_CREATIONS))
        self.assertIn("series", payload)
        self.assertIn("computed_at", payload)
        self.assertIsNotNone(payload["computed_at"])

        total = sum(row["count"] for row in payload["series"])
        self.assertEqual(total, 3)
        for row in payload["series"]:
            self.assertIn("period", row)
            self.assertIn("count", row)
            # period is an ISO date string, not a datetime
            self.assertEqual(len(row["period"]), 10)

    def test_compute_safe_creations_task_empty_writes_empty_series(self):
        redis = get_redis()
        redis.flushall()

        compute_safe_creations_task()

        payload = json.loads(redis.get(AnalyticsService.REDIS_SAFE_CREATIONS))
        self.assertEqual(payload["series"], [])
        self.assertIsNotNone(payload["computed_at"])


class TestApproxCountOrExact(TestCase):
    """`approx_count_or_exact` should serve the planner estimate on large
    tables and fall back to exact `COUNT(*)` on small / freshly-fixtured
    tables so unit tests don't depend on ANALYZE having been run."""

    def test_falls_back_to_exact_when_reltuples_below_threshold(self):
        SafeContractFactory()
        SafeContractFactory()
        SafeContractFactory()
        # pg_class.reltuples on a freshly-populated table is 0 or -1 until
        # ANALYZE runs, so the helper must fall back to exact COUNT(*).
        self.assertEqual(
            approx_count_or_exact(SafeContract, "history_safecontract", threshold=1000),
            3,
        )

    def test_returns_reltuples_when_above_threshold(self):
        # Force the approx path by lowering the threshold below the real
        # estimate. ANALYZE on a 0-row table leaves reltuples at 0, so we
        # patch the cursor to simulate a populated table.
        SafeContractFactory()

        class _FakeCursorCM:
            def __init__(self, value):
                self._value = value

            def __enter__(self):
                self._exec_count = 0
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, sql, params):
                self._sql = sql
                self._params = params

            def fetchone(self):
                return (self._value,)

        from safe_transaction_service.analytics.services import db as db_helpers

        with patch.object(
            db_helpers.connection, "cursor", lambda: _FakeCursorCM(12_345_678)
        ):
            self.assertEqual(
                approx_count_or_exact(
                    SafeContract, "history_safecontract", threshold=1000
                ),
                12_345_678,
            )


class TestActiveSafesTask(TestCase):
    """The rewritten compute_active_safes_task: batched ERC20 probe + per-
    window resilience."""

    def test_erc20_active_safe_addresses_counted(self):
        """A SafeContract that appears as `_from` of an ERC20 transfer
        inside the window must show up in the active count, exercising the
        new batched index-probe path."""
        redis = get_redis()
        redis.flushall()
        safe = SafeContractFactory()
        ERC20TransferFactory(_from=safe.address)

        compute_active_safes_task.delay()

        for window in ("7d", "30d", "90d"):
            blob = redis.get(AnalyticsService.REDIS_ACTIVE_SAFES_PREFIX + window)
            self.assertIsNotNone(blob, f"window {window} not cached")
            payload = json.loads(blob)
            self.assertGreaterEqual(payload["active_safes"], 1)

    def test_distinct_across_all_three_legs(self):
        """The same Safe touching all three legs (multisig / module / ERC20)
        is counted once, not three times. Validates the set-union semantics."""
        redis = get_redis()
        redis.flushall()
        safe = SafeContractFactory()
        MultisigTransactionFactory(safe=safe.address)
        ModuleTransactionFactory(safe=safe.address)
        ERC20TransferFactory(_from=safe.address)

        compute_active_safes_task.delay()

        payload = json.loads(
            redis.get(AnalyticsService.REDIS_ACTIVE_SAFES_PREFIX + "7d")
        )
        self.assertEqual(payload["active_safes"], 1)

    def test_per_window_failure_isolated(self):
        """If a single window's compute raises, the other windows still
        land in Redis. Simulated by patching `_safes_active_in_window` to
        raise only on the 30d cutoff."""
        redis = get_redis()
        redis.flushall()
        SafeContractFactory()

        real = _safes_active_in_window
        call_count = {"n": 0}

        def fake(cutoff):
            call_count["n"] += 1
            # Order is 7d, 30d, 90d — fail the middle window.
            if call_count["n"] == 2:
                raise RuntimeError("simulated planner timeout")
            return real(cutoff)

        with patch(
            "safe_transaction_service.analytics.tasks._safes_active_in_window",
            side_effect=fake,
        ):
            compute_active_safes_task.delay()

        self.assertIsNotNone(
            redis.get(AnalyticsService.REDIS_ACTIVE_SAFES_PREFIX + "7d")
        )
        self.assertIsNone(redis.get(AnalyticsService.REDIS_ACTIVE_SAFES_PREFIX + "30d"))
        self.assertIsNotNone(
            redis.get(AnalyticsService.REDIS_ACTIVE_SAFES_PREFIX + "90d")
        )


class TestActiveOwnersTask(TestCase):
    """The rewritten compute_active_owners_task: two-step batched join."""

    def test_counts_owners_of_executed_multisig_txs(self):
        redis = get_redis()
        redis.flushall()
        MultisigConfirmationFactory()
        MultisigConfirmationFactory()

        compute_active_owners_task.delay()

        for window in ("7d", "30d", "90d"):
            blob = redis.get(AnalyticsService.REDIS_ACTIVE_OWNERS_PREFIX + window)
            self.assertIsNotNone(blob, f"window {window} not cached")
            payload = json.loads(blob)
            self.assertGreaterEqual(payload["active_owners"], 1)

    def test_batched_path_with_explicit_helper(self):
        """Exercise `_active_owners_in_window` directly to make sure the
        chunked join surfaces all distinct owners (not just those in the
        first chunk)."""
        from django.utils import timezone

        # Two confirmations on two different multisig txs → two distinct
        # owners. The function should return 2 regardless of how we slice
        # the executed-tx IDs.
        c1 = MultisigConfirmationFactory()
        c2 = MultisigConfirmationFactory()
        # Force a tiny batch size so we exercise the chunking branch even
        # with this small fixture.
        cutoff = timezone.now() - timezone.timedelta(days=30)
        count = _active_owners_in_window(cutoff, batch_size=1)
        self.assertGreaterEqual(count, 2)
        # Sanity: both confirmations' owners are present in the underlying
        # data even though the helper only returns a count.
        self.assertNotEqual(c1.owner, c2.owner)


class TestUpsertDailyMetric(TestCase):
    """`_upsert_daily_metric` writes one row per day-window and re-runs it
    idempotently. Tested directly (rather than through the Celery task) so
    we can control the day window and seed factory data inside it without
    racing the "yesterday-only" default."""

    def test_writes_row_with_correct_counts(self):
        from django.utils import timezone

        safe = SafeContractFactory()
        MultisigTransactionFactory(safe=safe.address, value=1500)
        ModuleTransactionFactory(safe=safe.address)
        ERC20TransferFactory(_from=safe.address)

        # Window that wraps "now" so the factory rows fall inside it.
        day_start = timezone.now() - timezone.timedelta(hours=1)
        day_end = day_start + timezone.timedelta(days=1)
        _upsert_daily_metric(day_start, day_end)

        row = DailyMetric.objects.get(date=day_start.date())
        self.assertGreaterEqual(row.multisig_txs_executed, 1)
        self.assertGreaterEqual(row.module_txs, 1)
        self.assertGreaterEqual(row.erc20_transfers, 1)
        self.assertGreaterEqual(row.active_safes, 1)
        self.assertEqual(int(row.native_value_wei), 1500)
        self.assertIsNotNone(row.computed_at)

    def test_idempotent_upsert(self):
        from django.utils import timezone

        safe = SafeContractFactory()
        MultisigTransactionFactory(safe=safe.address, value=42)
        day_start = timezone.now() - timezone.timedelta(hours=1)
        day_end = day_start + timezone.timedelta(days=1)

        _upsert_daily_metric(day_start, day_end)
        first = DailyMetric.objects.get(date=day_start.date())
        first_computed_at = first.computed_at

        # Second call must update the same row, not create a new one.
        _upsert_daily_metric(day_start, day_end)
        self.assertEqual(DailyMetric.objects.count(), 1)
        refreshed = DailyMetric.objects.get(date=day_start.date())
        self.assertGreaterEqual(refreshed.computed_at, first_computed_at)


class TestComputeDailyMetricsTask(TestCase):
    """End-to-end task: the rolling-window cache refresh must populate the
    same Redis keys the read path consumes, and a per-day failure must not
    strand the rolling-window refresh that runs after the day loop."""

    def test_refresh_active_window_caches_populates_redis(self):
        redis = get_redis()
        redis.flushall()

        safe = SafeContractFactory()
        MultisigTransactionFactory(safe=safe.address)

        compute_daily_metrics_task(days_back=1)

        for window in ("7d", "30d", "90d"):
            blob = redis.get(AnalyticsService.REDIS_ACTIVE_SAFES_PREFIX + window)
            self.assertIsNotNone(blob, f"active_safes {window} missing")
            payload = json.loads(blob)
            self.assertGreaterEqual(payload["active_safes"], 1)
            self.assertEqual(payload["window"], window)

    def test_per_day_failure_isolated(self):
        """If `_upsert_daily_metric` raises for one day, the rolling-window
        refresh still runs so the read path doesn't go cold."""
        redis = get_redis()
        redis.flushall()
        SafeContractFactory()

        with patch(
            "safe_transaction_service.analytics.tasks._upsert_daily_metric",
            side_effect=RuntimeError("simulated"),
        ):
            compute_daily_metrics_task(days_back=2)

        # Day loop failed for every day; no DailyMetric rows.
        self.assertEqual(DailyMetric.objects.count(), 0)
        # But the rolling-window cache was refreshed regardless.
        self.assertIsNotNone(
            redis.get(AnalyticsService.REDIS_ACTIVE_SAFES_PREFIX + "7d")
        )


class TestBackfillDailyMetricsCommand(TestCase):
    """`manage.py backfill_daily_metrics` should upsert one DailyMetric row
    per day in the inclusive range, reusing the same task helper."""

    def test_populates_date_range_inline(self):
        from datetime import timedelta

        from django.core.management import call_command
        from django.utils import timezone

        SafeContractFactory()
        today = timezone.now().date()
        # Three-day window ending yesterday so we don't collide with the
        # incremental "yesterday-only" daily task semantics.
        start = today - timedelta(days=3)
        end = today - timedelta(days=1)

        call_command(
            "backfill_daily_metrics",
            start=start.isoformat(),
            end=end.isoformat(),
            inline=True,
        )

        self.assertEqual(DailyMetric.objects.count(), 3)
        for offset in range(3):
            d = start + timedelta(days=offset)
            self.assertTrue(
                DailyMetric.objects.filter(date=d).exists(),
                f"missing DailyMetric for {d}",
            )

    def test_populates_date_range_celery_eager(self):
        """Default (no --inline) dispatches one Celery shard per day.
        Under CELERY_ALWAYS_EAGER the dispatch runs shards inline and
        returns the summary dict; we just check the row count lands."""
        from datetime import timedelta

        from django.core.management import call_command
        from django.utils import timezone

        SafeContractFactory()
        today = timezone.now().date()
        start = today - timedelta(days=3)
        end = today - timedelta(days=1)

        call_command(
            "backfill_daily_metrics",
            start=start.isoformat(),
            end=end.isoformat(),
        )

        self.assertEqual(DailyMetric.objects.count(), 3)


class TestDailyRollupPopulators(TestCase):
    """One test per narrow rollup populator. Asserts the populator writes
    the expected (date, key) rows AND that a second call is idempotent —
    `ON CONFLICT DO UPDATE` / `ignore_conflicts=True` semantics.
    """

    def setUp(self):
        super().setUp()
        from django.utils import timezone

        self.day_start = timezone.now() - timezone.timedelta(hours=1)
        self.day_end = self.day_start + timezone.timedelta(days=1)
        self.date_value = self.day_start.date()

    def test_compute_daily_token_volume(self):
        from eth_account import Account

        from safe_transaction_service.analytics.models import DailyTokenVolume
        from safe_transaction_service.analytics.tasks import (
            _compute_daily_token_volume,
        )

        token = Account.create().address
        ERC20TransferFactory(address=token, value=100)
        ERC20TransferFactory(address=token, value=250)

        _compute_daily_token_volume(self.day_start, self.day_end)
        row = DailyTokenVolume.objects.get(date=self.date_value, token_address=token)
        self.assertEqual(row.transfer_count, 2)
        self.assertEqual(int(row.transfer_value), 350)

        # Rerun should leave a single row with the same totals — ON CONFLICT
        # DO UPDATE.
        _compute_daily_token_volume(self.day_start, self.day_end)
        self.assertEqual(
            DailyTokenVolume.objects.filter(
                date=self.date_value, token_address=token
            ).count(),
            1,
        )

    def test_compute_daily_active_safes(self):
        from safe_transaction_service.analytics.models import DailyActiveSafe
        from safe_transaction_service.analytics.tasks import (
            _compute_daily_active_safes,
        )

        safe = SafeContractFactory()
        MultisigTransactionFactory(safe=safe.address)

        _compute_daily_active_safes(self.day_start, self.day_end)
        self.assertEqual(
            DailyActiveSafe.objects.filter(date=self.date_value).count(), 1
        )

        # Rerun is a no-op via ignore_conflicts=True (membership table).
        _compute_daily_active_safes(self.day_start, self.day_end)
        self.assertEqual(
            DailyActiveSafe.objects.filter(date=self.date_value).count(), 1
        )

    def test_compute_daily_safe_app_txs(self):
        from safe_transaction_service.analytics.models import DailySafeAppTx
        from safe_transaction_service.analytics.tasks import (
            _compute_daily_safe_app_txs,
        )

        origin = {"url": "https://example.com", "name": "MyDapp"}
        MultisigTransactionFactory(origin=origin)
        MultisigTransactionFactory(origin=origin)

        _compute_daily_safe_app_txs(self.day_start, self.day_end)
        row = DailySafeAppTx.objects.get(date=self.date_value, origin_name="MyDapp")
        self.assertEqual(row.tx_count, 2)
        # origin_url is denormalised onto the rollup so the read path
        # doesn't have to re-touch history_multisigtransaction.
        self.assertEqual(row.origin_url, "https://example.com")

        _compute_daily_safe_app_txs(self.day_start, self.day_end)
        self.assertEqual(
            DailySafeAppTx.objects.filter(
                date=self.date_value, origin_name="MyDapp"
            ).count(),
            1,
        )

    def test_compute_daily_safe_app_txs_short_circuits_when_no_origin_data(self):
        """On chains with zero multisig txs carrying origin metadata
        (BASE today), the populator must return immediately without
        touching the 3-way join. The probe is bounded by `LIMIT 1` —
        no need to actually count the chain's origin rows."""
        from safe_transaction_service.analytics.models import DailySafeAppTx
        from safe_transaction_service.analytics.tasks import (
            _compute_daily_safe_app_txs,
        )

        # Seed multisig txs with NO origin name — the probe should see
        # nothing and bail before doing the heavy join.
        MultisigTransactionFactory(origin={})
        MultisigTransactionFactory(origin={"url": "https://x"})  # no name

        result = _compute_daily_safe_app_txs(self.day_start, self.day_end)
        self.assertEqual(result, 0)
        self.assertEqual(DailySafeAppTx.objects.filter(date=self.date_value).count(), 0)

    def test_compute_daily_safe_creations(self):
        from safe_transaction_service.analytics.models import (
            DailySafeCreation,
        )
        from safe_transaction_service.analytics.tasks import (
            _compute_daily_safe_creations,
        )

        SafeContractFactory()
        SafeContractFactory()

        _compute_daily_safe_creations(self.day_start, self.day_end)
        row = DailySafeCreation.objects.get(date=self.date_value)
        self.assertEqual(row.count, 2)

        _compute_daily_safe_creations(self.day_start, self.day_end)
        self.assertEqual(
            DailySafeCreation.objects.filter(date=self.date_value).count(), 1
        )

    def test_compute_daily_tx_volume(self):
        from safe_transaction_service.analytics.tasks import (
            _compute_daily_tx_volume,
        )

        # Three proposed multisig txs, one with two confirmations and one
        # with one confirmation — third has none.
        tx_with_two = MultisigTransactionFactory()
        tx_with_one = MultisigTransactionFactory()
        MultisigTransactionFactory()
        MultisigConfirmationFactory(multisig_transaction=tx_with_two)
        MultisigConfirmationFactory(multisig_transaction=tx_with_two)
        MultisigConfirmationFactory(multisig_transaction=tx_with_one)

        _compute_daily_tx_volume(self.day_start, self.day_end)
        row = DailyMetric.objects.get(date=self.date_value)
        self.assertEqual(row.multisig_txs_proposed, 3)
        self.assertEqual(row.confirmations_count, 3)
        self.assertEqual(row.confirmed_tx_count, 2)
        self.assertIsNotNone(row.computed_at)

        # Idempotent — re-run yields the same counts, one row.
        _compute_daily_tx_volume(self.day_start, self.day_end)
        self.assertEqual(DailyMetric.objects.filter(date=self.date_value).count(), 1)
        refreshed = DailyMetric.objects.get(date=self.date_value)
        self.assertEqual(refreshed.multisig_txs_proposed, 3)
        self.assertEqual(refreshed.confirmations_count, 3)
        self.assertEqual(refreshed.confirmed_tx_count, 2)

    def test_compute_daily_tx_volume_empty_window(self):
        """No data in the window → row inserted with zeros (caller can
        sum it safely)."""
        from safe_transaction_service.analytics.tasks import (
            _compute_daily_tx_volume,
        )

        _compute_daily_tx_volume(self.day_start, self.day_end)
        row = DailyMetric.objects.get(date=self.date_value)
        self.assertEqual(row.multisig_txs_proposed, 0)
        self.assertEqual(row.confirmations_count, 0)
        self.assertEqual(row.confirmed_tx_count, 0)

    def test_compute_daily_tx_volume_preserves_core_columns(self):
        """When `_compute_daily_metric_core` has already written the row,
        the tx-volume upsert must update ONLY the three new columns and
        leave the executed-side counts alone (ON CONFLICT clause is
        explicit about which fields to overwrite)."""
        from safe_transaction_service.analytics.tasks import (
            _compute_daily_tx_volume,
        )

        DailyMetric.objects.create(
            date=self.date_value,
            multisig_txs_executed=7,
            module_txs=3,
            erc20_transfers=11,
            native_value_wei=12345,
            active_safes=5,
            active_owners=4,
            new_safes=2,
            computed_at=self.day_start,
        )
        MultisigTransactionFactory()

        _compute_daily_tx_volume(self.day_start, self.day_end)
        row = DailyMetric.objects.get(date=self.date_value)
        self.assertEqual(row.multisig_txs_proposed, 1)
        # Executed-side columns untouched.
        self.assertEqual(row.multisig_txs_executed, 7)
        self.assertEqual(row.module_txs, 3)
        self.assertEqual(row.erc20_transfers, 11)
        self.assertEqual(int(row.native_value_wei), 12345)
        self.assertEqual(row.active_safes, 5)
        self.assertEqual(row.active_owners, 4)
        self.assertEqual(row.new_safes, 2)


class TestUpsertDailyMetricFullStack(TestCase):
    """The 5-step `_upsert_daily_metric` populates DailyMetric + all four
    rollups inside a single call. Failure of any one rollup must not
    strand the others or the DailyMetric row (per-populator try/except)."""

    def test_writes_all_rollups(self):
        from datetime import timedelta

        from django.utils import timezone

        from safe_transaction_service.analytics.models import (
            DailyActiveSafe,
            DailySafeAppTx,
            DailySafeCreation,
            DailyTokenVolume,
        )

        safe = SafeContractFactory()
        MultisigConfirmationFactory(
            multisig_transaction=MultisigTransactionFactory(
                safe=safe.address, origin={"url": "u", "name": "App"}
            )
        )
        ERC20TransferFactory()

        day_start = timezone.now() - timedelta(hours=1)
        day_end = day_start + timedelta(days=1)
        _upsert_daily_metric(day_start, day_end)

        d = day_start.date()
        self.assertTrue(DailyMetric.objects.filter(date=d).exists())
        self.assertTrue(DailyTokenVolume.objects.filter(date=d).exists())
        self.assertTrue(DailyActiveSafe.objects.filter(date=d).exists())
        self.assertTrue(DailyActiveOwner.objects.filter(date=d).exists())
        self.assertTrue(DailySafeAppTx.objects.filter(date=d).exists())
        self.assertTrue(DailySafeCreation.objects.filter(date=d).exists())

    def test_one_rollup_failure_isolated(self):
        """Patch one populator to raise; the others (and the DailyMetric
        row) must still land."""
        from datetime import timedelta

        from django.utils import timezone

        from safe_transaction_service.analytics.models import (
            DailySafeCreation,
        )

        safe = SafeContractFactory()
        MultisigTransactionFactory(safe=safe.address)

        day_start = timezone.now() - timedelta(hours=1)
        day_end = day_start + timedelta(days=1)

        with patch(
            "safe_transaction_service.analytics.tasks._compute_daily_active_safes",
            side_effect=RuntimeError("boom"),
        ):
            _upsert_daily_metric(day_start, day_end)

        d = day_start.date()
        self.assertTrue(DailyMetric.objects.filter(date=d).exists())
        # The non-failing rollups still landed.
        self.assertTrue(DailySafeCreation.objects.filter(date=d).exists())


class TestNativeBalanceShards(TestCase):
    """Sharded native-balance compute under eager mode must match the
    sequential reference implementation."""

    def test_chord_matches_sequential(self):
        from celery import chord

        from safe_transaction_service.analytics.tasks import (
            _calculate_native_balances_from_db_sequential,
        )
        from safe_transaction_service.analytics.tasks_shards import (
            HEX_PREFIXES,
            compute_native_balance_shard,
            reduce_native_balance_shards,
        )

        for _ in range(3):
            safe = SafeContractFactory()
            InternalTxFactory(
                to=safe.address,
                value=1_000,
                call_type=0,
                error=None,
            )

        seq_balance, seq_count = _calculate_native_balances_from_db_sequential()
        # Eager mode runs the chord inline; bypass the fire-and-forget
        # `dispatch_tvl_chord` so we can read the reduced result back here.
        reduced = (
            chord(
                (compute_native_balance_shard.s(p) for p in HEX_PREFIXES),
                reduce_native_balance_shards.s(),
            )
            .apply_async()
            .get()
        )
        self.assertEqual(seq_balance, reduced["balance_wei"])
        self.assertEqual(seq_count, reduced["safes_with_balance"])
        self.assertGreater(reduced["balance_wei"], 0)
        self.assertEqual(reduced["safes_with_balance"], 3)

    def test_shards_partition_address_space(self):
        """Every Safe is hit by exactly one shard. Sum of shard counts ==
        total safes with balance."""
        from safe_transaction_service.analytics.tasks_shards import (
            HEX_PREFIXES,
            compute_native_balance_shard,
        )

        # Seed a small but predictable set.
        for _ in range(4):
            safe = SafeContractFactory()
            InternalTxFactory(
                to=safe.address,
                value=500,
                call_type=0,
                error=None,
            )

        total = 0
        for prefix in HEX_PREFIXES:
            shard = compute_native_balance_shard(prefix)
            total += shard["safes_with_balance"]
        # Each seeded Safe lands in exactly one shard.
        self.assertEqual(total, 4)


class TestKeysetPagination(TestCase):
    """`_iter_safe_addresses_keyset` is the load-bearing replacement for
    the old OFFSET/LIMIT loop. The bug it fixes is silent (correctness
    holds, performance collapses on multi-million-Safe chains), so the
    unit guarantee that survives is: every Safe is yielded exactly once,
    in PK order, regardless of how small the batch is."""

    def test_yields_every_address_exactly_once(self):
        from safe_transaction_service.analytics.tasks import (
            _iter_safe_addresses_keyset,
        )

        # Seed 7 Safes; iterate with batch_size=2 to force `address__gt`
        # boundary crossings and exercise the keyset cursor explicitly.
        for _ in range(7):
            SafeContractFactory()
        expected = list(
            SafeContract.objects.values_list("address", flat=True).order_by("pk")
        )

        seen = []
        for chunk in _iter_safe_addresses_keyset(batch_size=2):
            self.assertLessEqual(len(chunk), 2)
            seen.extend(chunk)

        self.assertEqual(seen, expected)

    def test_empty_safe_contracts_terminates(self):
        from safe_transaction_service.analytics.tasks import (
            _iter_safe_addresses_keyset,
        )

        self.assertEqual(list(_iter_safe_addresses_keyset(batch_size=10)), [])


class TestErc20ActiveSafeAddrsBetween(TestCase):
    """The bounded-window helper used to issue ~200 batched EXISTS probes
    per call; it now resolves to a single JOIN over the day's transfers.
    Behaviour contract: returns the set of Safe addresses with any
    ERC20 transfer (_from OR to) strictly inside `[start, end)`."""

    def test_includes_safes_with_transfer_inside_window(self):
        from django.utils import timezone

        from safe_transaction_service.analytics.tasks import (
            _erc20_active_safe_addrs_between,
        )

        start = timezone.now() - timezone.timedelta(hours=1)
        end = start + timezone.timedelta(days=1)

        safe_in = SafeContractFactory()
        ERC20TransferFactory(to=safe_in.address)

        result = _erc20_active_safe_addrs_between(start, end)
        self.assertIn(safe_in.address.lower(), {a.lower() for a in result})

    def test_excludes_safes_with_no_transfers(self):
        from django.utils import timezone

        from safe_transaction_service.analytics.tasks import (
            _erc20_active_safe_addrs_between,
        )

        start = timezone.now() - timezone.timedelta(hours=1)
        end = start + timezone.timedelta(days=1)

        SafeContractFactory()  # no transfers — must not appear
        safe_in = SafeContractFactory()
        ERC20TransferFactory(_from=safe_in.address)

        result = {a.lower() for a in _erc20_active_safe_addrs_between(start, end)}
        self.assertEqual(len(result), 1)
        self.assertIn(safe_in.address.lower(), result)

    def test_excludes_non_safe_addresses(self):
        """Transfers whose endpoints are not SafeContracts must not be
        returned — the JOIN against `history_safecontract` is what
        enforces this."""
        from django.utils import timezone

        from safe_transaction_service.analytics.tasks import (
            _erc20_active_safe_addrs_between,
        )

        start = timezone.now() - timezone.timedelta(hours=1)
        end = start + timezone.timedelta(days=1)

        # Transfer between two non-Safe addresses: the JOIN drops it.
        ERC20TransferFactory()

        self.assertEqual(_erc20_active_safe_addrs_between(start, end), set())


class TestDailyActiveOwnersPopulator(TestCase):
    """`_compute_daily_active_owners` writes one row per (date, owner)
    where the owner confirmed any multisig tx whose tx block.timestamp
    landed in the day window. Mirror of `_compute_daily_active_safes`
    — single SQL, idempotent via `ON CONFLICT DO NOTHING`."""

    def setUp(self):
        super().setUp()
        from django.utils import timezone

        self.day_start = timezone.now() - timezone.timedelta(hours=1)
        self.day_end = self.day_start + timezone.timedelta(days=1)
        self.date_value = self.day_start.date()

    def test_writes_one_row_per_distinct_confirming_owner(self):
        from safe_transaction_service.analytics.tasks import (
            _compute_daily_active_owners,
        )

        c1 = MultisigConfirmationFactory()
        c2 = MultisigConfirmationFactory()
        # Sanity: factory uses fresh owner per confirmation.
        self.assertNotEqual(c1.owner, c2.owner)

        _compute_daily_active_owners(self.day_start, self.day_end)
        self.assertEqual(
            DailyActiveOwner.objects.filter(date=self.date_value).count(), 2
        )

    def test_idempotent_on_rerun(self):
        """`ON CONFLICT DO NOTHING` keeps the table single-row-per-
        (date, owner) on a second invocation."""
        from safe_transaction_service.analytics.tasks import (
            _compute_daily_active_owners,
        )

        MultisigConfirmationFactory()

        _compute_daily_active_owners(self.day_start, self.day_end)
        first_count = DailyActiveOwner.objects.filter(date=self.date_value).count()
        self.assertGreaterEqual(first_count, 1)

        _compute_daily_active_owners(self.day_start, self.day_end)
        self.assertEqual(
            DailyActiveOwner.objects.filter(date=self.date_value).count(),
            first_count,
        )

    def test_distinct_owners_counted_once(self):
        """A single owner confirming on two multisig txs in the window
        appears once — distinct semantic, same shape as DailyActiveSafe."""
        from safe_transaction_service.analytics.tasks import (
            _compute_daily_active_owners,
        )

        c1 = MultisigConfirmationFactory()
        # Second confirmation from the same owner on a different multisig
        # tx must NOT create a duplicate row.
        MultisigConfirmationFactory(owner=c1.owner)

        _compute_daily_active_owners(self.day_start, self.day_end)
        self.assertEqual(
            DailyActiveOwner.objects.filter(
                date=self.date_value, owner_address=c1.owner
            ).count(),
            1,
        )


class TestSnapshotWritingTasks(TestCase):
    """Each of the four current-state task functions must persist to
    `analytics_analyticssnapshot` rather than Redis after the Part 2
    cut (see `flickering-honking-wand.md`)."""

    def test_compute_safe_segments_task_writes_snapshot(self):
        from eth_account import Account

        from safe_transaction_service.analytics.tasks import (
            compute_safe_segments_task,
        )

        redis = get_redis()
        redis.flushall()
        AnalyticsSnapshot.objects.all().delete()
        # Personal Safe (1 owner) + Team Safe (3 owners).
        SafeStatusFactory(owners=[Account.create().address], threshold=1)
        SafeStatusFactory(
            owners=[Account.create().address for _ in range(3)], threshold=2
        )

        compute_safe_segments_task.delay()

        snap = AnalyticsSnapshot.objects.get(name="safe_segments")
        self.assertEqual(snap.payload["personal"], 1)
        self.assertEqual(snap.payload["team"], 1)
        self.assertIsNotNone(snap.computed_at)
        # Legacy Redis key must not be written.
        self.assertIsNone(redis.get(AnalyticsService.REDIS_SAFE_SEGMENTS))

    def test_compute_tvl_task_writes_snapshot(self):
        from safe_transaction_service.analytics.tasks import compute_tvl_task

        redis = get_redis()
        redis.flushall()
        AnalyticsSnapshot.objects.all().delete()
        safe = SafeContractFactory()
        InternalTxFactory(to=safe.address, value=1_000_000, call_type=0, error=None)

        compute_tvl_task.delay()

        snap = AnalyticsSnapshot.objects.get(name="tvl")
        self.assertIn("native_balance_wei", snap.payload)
        self.assertIn("top_tokens", snap.payload)
        self.assertIsNotNone(snap.computed_at)
        # Legacy Redis key must not be written.
        self.assertIsNone(redis.get(AnalyticsService.REDIS_TVL))

    def test_compute_tvl_task_snapshot_lands_when_balance_calc_fails(self):
        """The phase-1 placeholder snapshot must persist even when the chord
        callback raises during the ERC20 aggregation — endpoint should
        return a coherent zero-valued payload instead of all-null."""
        from safe_transaction_service.analytics.tasks import compute_tvl_task

        AnalyticsSnapshot.objects.all().delete()
        SafeContractFactory()

        # `finalize_tvl_snapshot` runs in the chord callback after reduce
        # and is where the heavy ERC20 aggregates + final snapshot write
        # happen. Patching it to raise leaves the phase-1 placeholder in
        # place — same end-user contract as before.
        with patch(
            "safe_transaction_service.analytics.tasks_shards.finalize_tvl_snapshot.run",
            side_effect=RuntimeError("simulated ERC20 aggregate failure"),
        ):
            compute_tvl_task.delay()

        snap = AnalyticsSnapshot.objects.get(name="tvl")
        self.assertEqual(snap.payload["native_balance_wei"], "0")
        self.assertEqual(snap.payload["total_safes_with_balance"], 0)
        self.assertEqual(snap.payload["erc20_token_count"], 0)
        self.assertEqual(snap.payload["top_tokens"], [])
        self.assertIsNotNone(snap.computed_at)
