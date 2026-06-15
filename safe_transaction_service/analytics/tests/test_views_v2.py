import json
from unittest.mock import patch

from django.contrib.auth.models import User
from django.urls import reverse

from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase

from safe_transaction_service.analytics.models import AnalyticsSnapshot
from safe_transaction_service.analytics.services.analytics_service import (
    AnalyticsService,
)
from safe_transaction_service.analytics.tasks import (
    compute_active_owners_task,
    compute_active_safes_task,
    compute_daily_metrics_task,
    compute_safe_segments_task,
    compute_summary_task,
    compute_tvl_task,
    get_transactions_per_safe_app_task,
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


class AnalyticsTestMixin:
    """Common setup for analytics test classes."""

    def setUp(self):
        super().setUp()
        self.redis = get_redis()
        self.redis.flushall()
        self.user, _ = User.objects.get_or_create(username="test", password="12345")
        self.token, _ = Token.objects.get_or_create(user=self.user)
        self.auth_header = {"HTTP_AUTHORIZATION": "Token " + self.token.key}


class TestViewsV2(AnalyticsTestMixin, APITestCase):
    def test_analytics_multisig_txs_by_origin_view(self):
        response = self.client.get(
            reverse("v2:analytics:analytics-multisig-txs-by-origin")
        )
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

        response = self.client.get(
            reverse("v2:analytics:analytics-multisig-txs-by-origin"),
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, [])

        origin_1 = {"url": "https://example1.com", "name": "SafeApp1"}
        origin_2 = {"url": "https://example2.com", "name": "SafeApp2"}

        MultisigTransactionFactory(origin=origin_1)
        get_transactions_per_safe_app_task()
        response = self.client.get(
            reverse("v2:analytics:analytics-multisig-txs-by-origin"),
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        expected = [
            {
                "name": origin_1["name"],
                "url": origin_1["url"],
                "total_tx": 1,
                "tx_last_month": 1,
                "tx_last_week": 1,
                "tx_last_year": 1,
            },
        ]
        self.assertEqual(response.data, expected)

        for _ in range(3):
            MultisigTransactionFactory(origin=origin_2)

        get_transactions_per_safe_app_task()

        response = self.client.get(
            reverse("v2:analytics:analytics-multisig-txs-by-origin"),
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        expected = [
            {
                "name": origin_2["name"],
                "url": origin_2["url"],
                "total_tx": 3,
                "tx_last_month": 3,
                "tx_last_week": 3,
                "tx_last_year": 3,
            },
            {
                "name": origin_1["name"],
                "url": origin_1["url"],
                "total_tx": 1,
                "tx_last_month": 1,
                "tx_last_week": 1,
                "tx_last_year": 1,
            },
        ]
        self.assertEqual(response.data, expected)

        for _ in range(3):
            MultisigTransactionFactory(origin=origin_1)

        get_transactions_per_safe_app_task()
        response = self.client.get(
            reverse("v2:analytics:analytics-multisig-txs-by-origin"),
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        expected = [
            {
                "name": origin_1["name"],
                "url": origin_1["url"],
                "total_tx": 4,
                "tx_last_month": 4,
                "tx_last_week": 4,
                "tx_last_year": 4,
            },
            {
                "name": origin_2["name"],
                "url": origin_2["url"],
                "total_tx": 3,
                "tx_last_month": 3,
                "tx_last_week": 3,
                "tx_last_year": 3,
            },
        ]
        self.assertEqual(response.data, expected)


class TestSummaryView(AnalyticsTestMixin, APITestCase):
    def test_auth_required(self):
        response = self.client.get(reverse("v2:analytics:analytics-summary"))
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    @patch(
        "safe_transaction_service.utils.ethereum.get_chain_id",
        return_value=84532,
    )
    def test_summary_empty(self, mock_chain_id):
        response = self.client.get(
            reverse("v2:analytics:analytics-summary"), **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        self.assertEqual(data["total_safes"], 0)
        self.assertEqual(data["total_multisig_txs"], 0)
        self.assertEqual(data["total_module_txs"], 0)
        self.assertEqual(data["total_erc20_transfers"], 0)
        self.assertEqual(data["total_erc721_transfers"], 0)
        self.assertIsNone(data["first_safe_created"])
        self.assertIsNone(data["last_safe_created"])
        self.assertEqual(data["chain_id"], 84532)

    @patch(
        "safe_transaction_service.utils.ethereum.get_chain_id",
        return_value=84532,
    )
    def test_summary_with_data(self, mock_chain_id):
        SafeContractFactory()
        SafeContractFactory()
        MultisigTransactionFactory()
        ModuleTransactionFactory()
        ERC20TransferFactory()
        ERC721TransferFactory()

        # Pre-warm the snapshot — the view no longer blocks on compute.
        # In production the daily cron warms this; in tests we run the
        # task synchronously instead.
        compute_summary_task()

        response = self.client.get(
            reverse("v2:analytics:analytics-summary"), **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        self.assertEqual(data["total_safes"], 2)
        self.assertEqual(data["total_multisig_txs"], 1)
        self.assertEqual(data["total_module_txs"], 1)
        self.assertEqual(data["total_erc20_transfers"], 1)
        self.assertEqual(data["total_erc721_transfers"], 1)
        self.assertIsNotNone(data["first_safe_created"])
        self.assertIsNotNone(data["last_safe_created"])


class TestActiveSafesView(AnalyticsTestMixin, APITestCase):
    def test_auth_required(self):
        response = self.client.get(reverse("v2:analytics:analytics-active-safes"))
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_invalid_window(self):
        response = self.client.get(
            reverse("v2:analytics:analytics-active-safes"),
            {"window": "5d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_empty_cache(self):
        response = self.client.get(
            reverse("v2:analytics:analytics-active-safes"),
            {"window": "30d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["active_safes"], 0)

    def test_with_cached_data(self):
        safe = SafeContractFactory()
        MultisigTransactionFactory(safe=safe.address)

        compute_active_safes_task()

        response = self.client.get(
            reverse("v2:analytics:analytics-active-safes"),
            {"window": "30d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertGreaterEqual(response.data["active_safes"], 1)
        self.assertIsNotNone(response.data["computed_at"])


class TestSafeCreationsView(AnalyticsTestMixin, APITestCase):
    def test_auth_required(self):
        response = self.client.get(reverse("v2:analytics:analytics-safe-creations"))
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_invalid_interval(self):
        response = self.client.get(
            reverse("v2:analytics:analytics-safe-creations"),
            {"interval": "hour"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_empty(self):
        response = self.client.get(
            reverse("v2:analytics:analytics-safe-creations"),
            {"interval": "day"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, [])

    def test_with_data(self):
        SafeContractFactory()
        SafeContractFactory()

        response = self.client.get(
            reverse("v2:analytics:analytics-safe-creations"),
            {"interval": "day"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertGreater(len(response.data), 0)
        for entry in response.data:
            self.assertIn("period", entry)
            self.assertIn("count", entry)


class TestActiveOwnersView(AnalyticsTestMixin, APITestCase):
    def test_auth_required(self):
        response = self.client.get(reverse("v2:analytics:analytics-active-owners"))
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_invalid_window(self):
        response = self.client.get(
            reverse("v2:analytics:analytics-active-owners"),
            {"window": "1y"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_with_cached_data(self):
        MultisigConfirmationFactory()

        compute_active_owners_task()

        response = self.client.get(
            reverse("v2:analytics:analytics-active-owners"),
            {"window": "30d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertGreaterEqual(response.data["active_owners"], 1)
        self.assertIsNotNone(response.data["computed_at"])


class TestTxVolumeView(AnalyticsTestMixin, APITestCase):
    def test_auth_required(self):
        response = self.client.get(reverse("v2:analytics:analytics-tx-volume"))
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_empty(self):
        response = self.client.get(
            reverse("v2:analytics:analytics-tx-volume"),
            {"window": "30d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        self.assertEqual(data["total_multisig_txs"], 0)
        self.assertEqual(data["executed_multisig_txs"], 0)
        self.assertEqual(data["module_txs"], 0)
        self.assertEqual(data["total_value_wei"], "0")

    def test_with_data(self):
        """The view reads from the DailyMetric rollup. Today's row is
        excluded (`date__lt=today`) because today isn't a completed day
        yet — same semantics as `safe-creations` and `token-volume`.

        Seed a row at today-1 directly; the populator's correctness
        against factory data is covered by `test_compute_daily_tx_volume`
        in `test_tasks.py`.
        """
        from datetime import timedelta

        from django.utils import timezone

        from safe_transaction_service.analytics.models import DailyMetric

        DailyMetric.objects.create(
            date=timezone.now().date() - timedelta(days=1),
            multisig_txs_proposed=2,
            multisig_txs_executed=2,
            module_txs=1,
            native_value_wei=3000,
            confirmations_count=4,
            confirmed_tx_count=2,
            computed_at=timezone.now(),
        )

        response = self.client.get(
            reverse("v2:analytics:analytics-tx-volume"),
            {"window": "30d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        self.assertEqual(data["total_multisig_txs"], 2)
        self.assertEqual(data["executed_multisig_txs"], 2)
        self.assertEqual(data["module_txs"], 1)
        self.assertEqual(data["total_value_wei"], "3000")
        self.assertEqual(data["avg_confirmations"], 2.0)
        self.assertEqual(data.get("source"), "daily_metric")
        self.assertEqual(data["coverage_days"], 1)


class TestSafeSegmentsView(AnalyticsTestMixin, APITestCase):
    def test_auth_required(self):
        response = self.client.get(reverse("v2:analytics:analytics-safe-segments"))
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_empty_cache(self):
        response = self.client.get(
            reverse("v2:analytics:analytics-safe-segments"), **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        self.assertEqual(data["personal"], 0)
        self.assertEqual(data["team"], 0)
        self.assertEqual(data["enterprise"], 0)

    def test_with_cached_data(self):
        from eth_account import Account

        # Personal Safe (1 owner)
        SafeStatusFactory(owners=[Account.create().address], threshold=1)
        # Team Safe (3 owners)
        SafeStatusFactory(
            owners=[Account.create().address for _ in range(3)], threshold=2
        )

        compute_safe_segments_task()

        response = self.client.get(
            reverse("v2:analytics:analytics-safe-segments"), **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        self.assertEqual(data["personal"], 1)
        self.assertEqual(data["team"], 1)
        self.assertEqual(data["enterprise"], 0)
        self.assertIsNotNone(data["computed_at"])


class TestTvlView(AnalyticsTestMixin, APITestCase):
    def test_auth_required(self):
        response = self.client.get(reverse("v2:analytics:analytics-tvl"))
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_empty_cache(self):
        response = self.client.get(
            reverse("v2:analytics:analytics-tvl"), **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        self.assertEqual(data["total_safes_with_balance"], 0)
        self.assertEqual(data["native_balance_wei"], "0")
        self.assertEqual(data["erc20_token_count"], 0)
        self.assertEqual(data["top_tokens"], [])

    def test_with_cached_data(self):
        safe = SafeContractFactory()
        InternalTxFactory(to=safe.address, value=1000000)

        compute_tvl_task()

        response = self.client.get(
            reverse("v2:analytics:analytics-tvl"), **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        self.assertIsNotNone(data["computed_at"])


class TestTokenVolumeView(AnalyticsTestMixin, APITestCase):
    def test_auth_required(self):
        response = self.client.get(reverse("v2:analytics:analytics-token-volume"))
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_empty(self):
        response = self.client.get(
            reverse("v2:analytics:analytics-token-volume"),
            {"window": "30d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        self.assertEqual(data["total_erc20_transfers"], 0)
        self.assertEqual(data["unique_tokens"], 0)
        self.assertEqual(data["top_tokens"], [])

    def test_with_data(self):
        from eth_account import Account

        token_address = Account.create().address
        ERC20TransferFactory(address=token_address, value=100)
        ERC20TransferFactory(address=token_address, value=200)

        response = self.client.get(
            reverse("v2:analytics:analytics-token-volume"),
            {"window": "30d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        self.assertEqual(data["total_erc20_transfers"], 2)
        self.assertEqual(data["unique_tokens"], 1)
        self.assertEqual(len(data["top_tokens"]), 1)
        self.assertEqual(data["top_tokens"][0]["transfer_count"], 2)
        self.assertEqual(data["top_tokens"][0]["total_value"], "300")


class TestSafeCreationsResampling(AnalyticsTestMixin, APITestCase):
    """Verify week/month buckets are derived from cached day-grain series."""

    def _seed_day_series(self, series):
        payload = {"series": series, "computed_at": "2026-05-18T00:00:00+00:00"}
        self.redis.set(AnalyticsService.REDIS_SAFE_CREATIONS, json.dumps(payload))

    def test_day_passthrough(self):
        self._seed_day_series(
            [
                {"period": "2026-05-04", "count": 3},
                {"period": "2026-05-05", "count": 5},
            ]
        )
        response = self.client.get(
            reverse("v2:analytics:analytics-safe-creations"),
            {"interval": "day"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data,
            [
                {"period": "2026-05-04", "count": 3},
                {"period": "2026-05-05", "count": 5},
            ],
        )

    def test_week_resample(self):
        # 2026-05-04 (Mon) and 2026-05-05 (Tue) → ISO week 2026-W19
        # 2026-05-11 (Mon) → ISO week 2026-W20
        self._seed_day_series(
            [
                {"period": "2026-05-04", "count": 3},
                {"period": "2026-05-05", "count": 5},
                {"period": "2026-05-11", "count": 7},
            ]
        )
        response = self.client.get(
            reverse("v2:analytics:analytics-safe-creations"),
            {"interval": "week"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Two buckets total, counts summed within each ISO week
        self.assertEqual(len(response.data), 2)
        counts = sorted(row["count"] for row in response.data)
        self.assertEqual(counts, [7, 8])

    def test_month_resample(self):
        self._seed_day_series(
            [
                {"period": "2026-04-30", "count": 2},
                {"period": "2026-05-01", "count": 4},
                {"period": "2026-05-31", "count": 1},
            ]
        )
        response = self.client.get(
            reverse("v2:analytics:analytics-safe-creations"),
            {"interval": "month"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # April → 2, May → 5
        by_month = {row["period"][:7]: row["count"] for row in response.data}
        self.assertEqual(by_month["2026-04"], 2)
        self.assertEqual(by_month["2026-05"], 5)

    def test_week_label_normalizes_to_monday(self):
        # Series starts Tue 2026-05-05; the bucket label should still be
        # Mon 2026-05-04 (Monday of ISO week 2026-W19), not the first
        # day encountered in the source.
        self._seed_day_series(
            [
                {"period": "2026-05-05", "count": 5},  # Tue
                {"period": "2026-05-08", "count": 2},  # Fri, same week
            ]
        )
        response = self.client.get(
            reverse("v2:analytics:analytics-safe-creations"),
            {"interval": "week"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, [{"period": "2026-05-04", "count": 7}])

    def test_month_label_normalizes_to_first(self):
        # Mid-month start; label must be 2026-05-01.
        self._seed_day_series(
            [
                {"period": "2026-05-15", "count": 4},
                {"period": "2026-05-22", "count": 1},
            ]
        )
        response = self.client.get(
            reverse("v2:analytics:analytics-safe-creations"),
            {"interval": "month"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, [{"period": "2026-05-01", "count": 5}])

    def test_date_range_filter(self):
        self._seed_day_series(
            [
                {"period": "2026-04-15", "count": 1},
                {"period": "2026-05-04", "count": 3},
                {"period": "2026-05-20", "count": 9},
            ]
        )
        response = self.client.get(
            reverse("v2:analytics:analytics-safe-creations"),
            {
                "interval": "day",
                "from": "2026-05-01T00:00:00Z",
                "to": "2026-05-15T00:00:00Z",
            },
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Only the 2026-05-04 entry falls inside the half-open window
        self.assertEqual(response.data, [{"period": "2026-05-04", "count": 3}])

    def test_cache_miss_triggers_sync_compute(self):
        # No seed; ensure the view triggers compute_safe_creations_task itself
        SafeContractFactory()
        response = self.client.get(
            reverse("v2:analytics:analytics-safe-creations"),
            {"interval": "day"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertGreater(len(response.data), 0)
        # Cache must now be populated
        self.assertIsNotNone(self.redis.get(AnalyticsService.REDIS_SAFE_CREATIONS))


class TestTvlSnapshotReadPath(AnalyticsTestMixin, APITestCase):
    """`compute_tvl_task` is the canonical source — native + ERC20 are
    computed atomically inside it. Cold reads return the empty payload
    immediately and fire-and-forget dispatch the refresh."""

    def test_tvl_payload_served_from_snapshot(self):
        from django.utils import timezone

        AnalyticsSnapshot.objects.create(
            name="tvl",
            payload={
                "total_safes_with_balance": 99,
                "native_balance_wei": "1",
                "erc20_token_count": 2,
                "top_tokens": [{"address": "0xabc"}],
            },
            computed_at=timezone.now(),
        )
        response = self.client.get(
            reverse("v2:analytics:analytics-tvl"), **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["total_safes_with_balance"], 99)
        self.assertEqual(response.data["native_balance_wei"], "1")
        self.assertEqual(response.data["erc20_token_count"], 2)
        self.assertEqual(response.data["top_tokens"], [{"address": "0xabc"}])
        self.assertIsNotNone(response.data["computed_at"])

    def test_tvl_cold_snapshot_returns_empty(self):
        # No `tvl` row yet — view returns the empty payload immediately
        # and fire-and-forget dispatches a refresh. Patch the task to a
        # no-op so the dispatch-on-miss path short-circuits and doesn't
        # accidentally write the snapshot under eager mode.
        with patch(
            "safe_transaction_service.analytics.tasks.compute_tvl_task",
            lambda: None,
        ):
            response = self.client.get(
                reverse("v2:analytics:analytics-tvl"), **self.auth_header
            )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["total_safes_with_balance"], 0)
        self.assertEqual(response.data["native_balance_wei"], "0")
        self.assertEqual(response.data["erc20_token_count"], 0)
        self.assertEqual(response.data["top_tokens"], [])
        self.assertIsNone(response.data["computed_at"])


class TestSummaryCache(AnalyticsTestMixin, APITestCase):
    @patch(
        "safe_transaction_service.utils.ethereum.get_chain_id",
        return_value=84532,
    )
    def test_summary_reads_from_snapshot(self, mock_chain_id):
        from django.utils import timezone

        snap_time = timezone.now()
        AnalyticsSnapshot.objects.create(
            name="summary",
            payload={
                "total_safes": 42,
                "total_multisig_txs": 100,
                "total_module_txs": 5,
                "total_erc20_transfers": 200,
                "total_erc721_transfers": 3,
                "first_safe_created": "2025-01-01T00:00:00+00:00",
                "last_safe_created": "2026-05-18T00:00:00+00:00",
            },
            computed_at=snap_time,
        )
        response = self.client.get(
            reverse("v2:analytics:analytics-summary"), **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        self.assertEqual(data["total_safes"], 42)
        self.assertEqual(data["total_multisig_txs"], 100)
        self.assertEqual(data["total_erc20_transfers"], 200)
        self.assertEqual(data["chain_id"], 84532)
        self.assertEqual(data["computed_at"], snap_time.isoformat())

    @patch(
        "safe_transaction_service.utils.ethereum.get_chain_id",
        return_value=84532,
    )
    def test_summary_cold_cache_returns_empty_without_blocking(self, mock_chain_id):
        """Cold snapshot read returns the empty payload IMMEDIATELY — no
        25s `_redis_get_or_compute` poll, no 504. Under eager mode the
        fire-and-forget dispatch then populates the snapshot
        synchronously so a *second* request returns real data.

        Headline behaviour change for operators (see
        `flickering-honking-wand.md` Part 2 / §"Cold-deploy first request").
        """
        SafeContractFactory()
        self.assertFalse(AnalyticsSnapshot.objects.filter(name="summary").exists())

        first = self.client.get(
            reverse("v2:analytics:analytics-summary"), **self.auth_header
        )
        self.assertEqual(first.status_code, status.HTTP_200_OK)
        # Cold read: empty payload.
        self.assertEqual(first.data["total_safes"], 0)
        self.assertIsNone(first.data["computed_at"])
        # Under eager mode the dispatched `.delay()` ran synchronously,
        # so the snapshot is now populated and the SECOND request hits it.
        self.assertTrue(
            AnalyticsSnapshot.objects.filter(name="summary").exists(),
            "fire-and-forget dispatch should have populated the snapshot",
        )
        second = self.client.get(
            reverse("v2:analytics:analytics-summary"), **self.auth_header
        )
        self.assertEqual(second.status_code, status.HTTP_200_OK)
        self.assertEqual(second.data["total_safes"], 1)
        self.assertIsNotNone(second.data["computed_at"])


class TestActiveSafesViewDailyTask(AnalyticsTestMixin, APITestCase):
    """C7 read-path contract: the active_safes view must serve the
    window-distinct count written by `compute_daily_metrics_task`, not a
    sum of per-day DAU rows. A Safe touching activity in the 7d window
    contributes exactly 1, regardless of how many days it was active."""

    def test_serves_window_distinct_after_daily_task(self):
        safe = SafeContractFactory()
        MultisigTransactionFactory(safe=safe.address)

        compute_daily_metrics_task(days_back=1)

        response = self.client.get(
            reverse("v2:analytics:analytics-active-safes"),
            {"window": "7d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["active_safes"], 1)
        self.assertIsNotNone(response.data["computed_at"])


class TestRollupReadPath(AnalyticsTestMixin, APITestCase):
    """Spec §5: each of the four affected endpoints reads from rollups
    when populated and falls back to the legacy live/cached path on a
    cold rollup."""

    def test_token_volume_served_from_rollup(self):
        from datetime import timedelta

        from django.utils import timezone

        from eth_account import Account

        from safe_transaction_service.analytics.models import DailyTokenVolume

        token = Account.create().address
        today = timezone.now().date()
        DailyTokenVolume.objects.create(
            date=today - timedelta(days=1),
            token_address=token,
            transfer_count=3,
            transfer_value=900,
        )
        DailyTokenVolume.objects.create(
            date=today - timedelta(days=2),
            token_address=token,
            transfer_count=2,
            transfer_value=100,
        )

        response = self.client.get(
            reverse("v2:analytics:analytics-token-volume"),
            {"window": "30d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["total_erc20_transfers"], 5)
        self.assertEqual(response.data["unique_tokens"], 1)
        self.assertEqual(response.data["top_tokens"][0]["transfer_count"], 5)
        self.assertEqual(response.data["top_tokens"][0]["total_value"], "1000")

    def test_token_volume_cold_window_falls_back_to_live(self):
        from eth_account import Account

        token = Account.create().address
        ERC20TransferFactory(address=token, value=100)

        response = self.client.get(
            reverse("v2:analytics:analytics-token-volume"),
            {"window": "30d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["total_erc20_transfers"], 1)
        # Live path doesn't tag source.
        self.assertNotIn("source", response.data)

    def test_active_safes_served_from_rollup(self):
        from datetime import timedelta

        from django.utils import timezone

        from eth_account import Account

        from safe_transaction_service.analytics.models import DailyActiveSafe

        today = timezone.now().date()
        addr1 = Account.create().address
        addr2 = Account.create().address
        DailyActiveSafe.objects.create(
            date=today - timedelta(days=1), safe_address=addr1
        )
        DailyActiveSafe.objects.create(
            date=today - timedelta(days=2), safe_address=addr1
        )
        DailyActiveSafe.objects.create(
            date=today - timedelta(days=2), safe_address=addr2
        )

        response = self.client.get(
            reverse("v2:analytics:analytics-active-safes"),
            {"window": "7d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # addr1 counted once even though it appears on two days.
        self.assertEqual(response.data["active_safes"], 2)

    def test_active_safes_cold_window_falls_back_to_cached(self):
        # Pre-populate the legacy Redis key the fallback reads from.
        self.redis.set(
            AnalyticsService.REDIS_ACTIVE_SAFES_PREFIX + "30d",
            json.dumps(
                {
                    "window": "30d",
                    "active_safes": 7,
                    "computed_at": "2026-05-18T00:00:00+00:00",
                }
            ),
        )

        response = self.client.get(
            reverse("v2:analytics:analytics-active-safes"),
            {"window": "30d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["active_safes"], 7)

    def test_safe_creations_served_from_rollup(self):
        from datetime import timedelta

        from django.utils import timezone

        from safe_transaction_service.analytics.models import DailySafeCreation

        today = timezone.now().date()
        DailySafeCreation.objects.create(date=today - timedelta(days=1), count=4)
        DailySafeCreation.objects.create(date=today - timedelta(days=2), count=1)

        response = self.client.get(
            reverse("v2:analytics:analytics-safe-creations"),
            {"interval": "day"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Two day rows, total = 5
        self.assertEqual(len(response.data), 2)
        self.assertEqual(sum(r["count"] for r in response.data), 5)

    def test_safe_app_txs_served_from_rollup(self):
        from datetime import timedelta

        from django.utils import timezone

        from safe_transaction_service.analytics.models import DailySafeAppTx

        today = timezone.now().date()
        DailySafeAppTx.objects.create(
            date=today - timedelta(days=1),
            origin_name="App1",
            origin_url="https://app1.example",
            tx_count=4,
        )
        DailySafeAppTx.objects.create(
            date=today - timedelta(days=10),
            origin_name="App1",
            origin_url="https://app1-older.example",
            tx_count=1,
        )
        DailySafeAppTx.objects.create(
            date=today - timedelta(days=2),
            origin_name="App2",
            origin_url="https://app2.example",
            tx_count=2,
        )

        response = self.client.get(
            reverse("v2:analytics:analytics-multisig-txs-by-origin"),
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        by_name = {r["name"]: r for r in response.data}
        # App1 total = 5 (4 from last week + 1 from 10 days ago)
        self.assertEqual(by_name["App1"]["total_tx"], 5)
        self.assertEqual(by_name["App1"]["tx_last_week"], 4)
        self.assertEqual(by_name["App1"]["tx_last_month"], 5)
        self.assertEqual(by_name["App2"]["total_tx"], 2)
        # URL comes straight from the rollup, no history_* lookup.
        # Most-recent non-empty URL wins for App1.
        self.assertEqual(by_name["App1"]["url"], "https://app1.example")
        self.assertEqual(by_name["App2"]["url"], "https://app2.example")

    def test_safe_app_txs_cold_window_falls_back_to_redis(self):
        self.redis.set(
            AnalyticsService.REDIS_TRANSACTIONS_PER_SAFE_APP,
            json.dumps(
                [
                    {
                        "name": "Legacy",
                        "url": "https://legacy",
                        "total_tx": 9,
                        "tx_last_week": 3,
                        "tx_last_month": 9,
                        "tx_last_year": 9,
                    }
                ]
            ),
        )

        response = self.client.get(
            reverse("v2:analytics:analytics-multisig-txs-by-origin"),
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data[0]["name"], "Legacy")
        self.assertEqual(response.data[0]["total_tx"], 9)


class TestTxVolumeDailyMetricSource(AnalyticsTestMixin, APITestCase):
    """C7 read-path: when DailyMetric covers the requested window, the
    response should sum the persisted rows (and mark `source=daily_metric`).
    When coverage is sparse, the live ORM path is the fallback."""

    def test_returns_daily_metric_sum_when_table_populated(self):
        from datetime import timedelta

        from django.utils import timezone

        from safe_transaction_service.analytics.models import DailyMetric

        # Populate 8 rows covering the 7d window (date < today, date >= today-7).
        today = timezone.now().date()
        for offset in range(1, 9):
            DailyMetric.objects.create(
                date=today - timedelta(days=offset),
                multisig_txs_executed=2,
                module_txs=1,
                erc20_transfers=3,
                native_value_wei=100,
                multisig_txs_proposed=4,
                confirmations_count=6,
                confirmed_tx_count=3,
                computed_at=timezone.now(),
            )

        response = self.client.get(
            reverse("v2:analytics:analytics-tx-volume"),
            {"window": "7d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        # 7 days of 2 multisig + 1 module = 14 + 7 = 21 from the table
        # rows in the [today-7, today) range. Row at offset=8 is excluded.
        self.assertEqual(data["executed_multisig_txs"], 14)
        self.assertEqual(data["module_txs"], 7)
        self.assertEqual(data["total_value_wei"], "700")
        # New rollup columns: proposed = 4 × 7 = 28; avg_confirmations =
        # SUM(confirmations_count) / SUM(confirmed_tx_count) = 42 / 21 = 2.0
        self.assertEqual(data["total_multisig_txs"], 28)
        self.assertEqual(data["avg_confirmations"], 2.0)
        self.assertEqual(data["avg_confirmations_approximation"], "per-tx-day")
        self.assertEqual(data["coverage_days"], 7)
        self.assertEqual(data.get("source"), "daily_metric")

    def test_sparse_rollup_returns_zeros_not_live_count(self):
        """No DailyMetric rows → response must return zeros and
        `coverage_days=0` rather than falling back to a live ORM count.
        The live path was removed because it could not finish in 30s on
        Base; sparse coverage is surfaced honestly via `coverage_days`.
        """
        # Seed live data the legacy fallback would have summed — it
        # must NOT show up in the response now.
        MultisigTransactionFactory(value=1000)
        MultisigTransactionFactory(value=2000)

        response = self.client.get(
            reverse("v2:analytics:analytics-tx-volume"),
            {"window": "30d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        self.assertEqual(data["total_multisig_txs"], 0)
        self.assertEqual(data["executed_multisig_txs"], 0)
        self.assertEqual(data["total_value_wei"], "0")
        self.assertEqual(data["avg_confirmations"], 0.0)
        self.assertEqual(data["coverage_days"], 0)
        self.assertEqual(data.get("source"), "daily_metric")


class TestActiveOwnersRollupReadPath(AnalyticsTestMixin, APITestCase):
    """`active-owners` reads `DailyActiveOwner` directly — no
    `SafeLastStatus` lookup at request time (see
    `flickering-honking-wand.md` Part 1)."""

    def test_active_owners_served_from_rollup_via_daily_active_owner(self):
        from datetime import timedelta

        from django.utils import timezone

        from eth_account import Account

        from safe_transaction_service.analytics.models import DailyActiveOwner

        today = timezone.now().date()
        owner1 = Account.create().address
        owner2 = Account.create().address
        DailyActiveOwner.objects.create(
            date=today - timedelta(days=1), owner_address=owner1
        )
        # Same owner on a different day — must count once.
        DailyActiveOwner.objects.create(
            date=today - timedelta(days=2), owner_address=owner1
        )
        DailyActiveOwner.objects.create(
            date=today - timedelta(days=2), owner_address=owner2
        )

        response = self.client.get(
            reverse("v2:analytics:analytics-active-owners"),
            {"window": "7d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["active_owners"], 2)
        self.assertIsNotNone(response.data["computed_at"])


class TestSnapshotReadPath(AnalyticsTestMixin, APITestCase):
    """The four current-state endpoints read from
    `analytics_analyticssnapshot`. Each test verifies (a) snapshot-served
    response, (b) cold-snapshot response is empty (not a 504)."""

    def _make_snapshot(self, name: str, payload: dict):
        from django.utils import timezone

        AnalyticsSnapshot.objects.create(
            name=name, payload=payload, computed_at=timezone.now()
        )

    def test_safe_segments_served_from_snapshot(self):
        self._make_snapshot(
            "safe_segments",
            {
                "personal": 3,
                "team": 2,
                "enterprise": 1,
                "with_modules": 1,
                "avg_threshold": 1.5,
                "avg_owners": 2.0,
            },
        )
        response = self.client.get(
            reverse("v2:analytics:analytics-safe-segments"), **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["personal"], 3)
        self.assertEqual(response.data["team"], 2)
        self.assertEqual(response.data["enterprise"], 1)
        self.assertIsNotNone(response.data["computed_at"])

    def test_tvl_served_from_snapshot(self):
        self._make_snapshot(
            "tvl",
            {
                "total_safes_with_balance": 7,
                "native_balance_wei": "100",
                "erc20_token_count": 3,
                "top_tokens": [],
            },
        )
        response = self.client.get(
            reverse("v2:analytics:analytics-tvl"), **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["total_safes_with_balance"], 7)
        self.assertEqual(response.data["native_balance_wei"], "100")
        self.assertEqual(response.data["erc20_token_count"], 3)
        self.assertIsNotNone(response.data["computed_at"])
