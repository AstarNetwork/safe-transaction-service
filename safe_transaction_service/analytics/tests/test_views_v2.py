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
from safe_transaction_service.tokens.tests.factories import TokenFactory
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
        # Attribution keys are always present, zero on an empty window.
        self.assertEqual(data["executed_multisig_txs_via_api"], 0)
        self.assertEqual(data["executed_multisig_txs_indexed_only"], 0)
        self.assertEqual(data["api_attribution_coverage_days"], 0)

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
            multisig_txs_via_api=1,
            multisig_txs_indexed_only=1,
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
        # API attribution: the executed split adds up to
        # `executed_multisig_txs` and the coverage counter equals
        # `coverage_days` on a fully covered window, which is the only
        # condition under which a consumer may divide.
        self.assertEqual(data["executed_multisig_txs_via_api"], 1)
        self.assertEqual(data["executed_multisig_txs_indexed_only"], 1)
        self.assertEqual(
            data["executed_multisig_txs_via_api"]
            + data["executed_multisig_txs_indexed_only"],
            data["executed_multisig_txs"],
        )
        self.assertEqual(data["api_attribution_coverage_days"], data["coverage_days"])

    def test_api_attribution_partial_coverage(self):
        """A window that is only partly backfilled: `Sum` skips the NULL
        days, so both halves understate. `api_attribution_coverage_days`
        falling short of `coverage_days` is what tells the consumer not
        to divide.
        """
        from datetime import timedelta

        from django.utils import timezone

        from safe_transaction_service.analytics.models import DailyMetric

        today = timezone.now().date()
        # Fully computed day.
        DailyMetric.objects.create(
            date=today - timedelta(days=1),
            multisig_txs_proposed=5,
            multisig_txs_executed=4,
            multisig_txs_via_api=3,
            multisig_txs_indexed_only=1,
            computed_at=timezone.now(),
        )
        # Pre-backfill day: executed split NULL.
        DailyMetric.objects.create(
            date=today - timedelta(days=2),
            multisig_txs_proposed=7,
            multisig_txs_executed=6,
            computed_at=timezone.now(),
        )
        # Fully computed day with every executed tx attributed to the API.
        DailyMetric.objects.create(
            date=today - timedelta(days=3),
            multisig_txs_proposed=0,
            multisig_txs_executed=2,
            multisig_txs_via_api=2,
            multisig_txs_indexed_only=0,
            computed_at=timezone.now(),
        )

        response = self.client.get(
            reverse("v2:analytics:analytics-tx-volume"),
            {"window": "30d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data

        self.assertEqual(data["coverage_days"], 3)
        # Keys are still numbers, never null — they just don't add up.
        self.assertEqual(data["executed_multisig_txs_via_api"], 5)
        self.assertEqual(data["executed_multisig_txs_indexed_only"], 1)
        self.assertEqual(data["executed_multisig_txs"], 12)
        self.assertLess(data["api_attribution_coverage_days"], data["coverage_days"])
        self.assertEqual(data["api_attribution_coverage_days"], 2)


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

    def test_tvl_top_tokens_carry_symbol(self):
        """`finalize_tvl_snapshot` joins `tokens_token` while building the
        payload, so `/tvl/` serves a `symbol` on every top-tokens entry.
        A token the indexer has no metadata row for is present-and-null —
        never its own address."""
        from eth_account import Account

        AnalyticsSnapshot.objects.all().delete()
        safe = SafeContractFactory()
        known = TokenFactory(symbol="WETH")
        unknown_address = Account.create().address
        ERC20TransferFactory(address=known.address, to=safe.address, value=500)
        ERC20TransferFactory(address=unknown_address, to=safe.address, value=400)

        compute_tvl_task()

        response = self.client.get(
            reverse("v2:analytics:analytics-tvl"), **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        symbols = {t["address"]: t["symbol"] for t in response.data["top_tokens"]}
        self.assertEqual(symbols[known.address], "WETH")
        self.assertIn(unknown_address, symbols)
        self.assertIsNone(symbols[unknown_address])

    def test_tvl_snapshot_survives_a_token_metadata_failure(self):
        """The metadata join has its own `except`, so a `tokens_token` read
        that blows up must not cost us the reduced TVL snapshot — it may
        only cost the symbols.

        The failure must NOT fall through to the outer handler, which keeps
        the phase-1 zero placeholder: that would lose a fully-reduced
        snapshot, which is what T6's "can never lose a TVL snapshot" clause
        forbids."""
        AnalyticsSnapshot.objects.all().delete()
        safe = SafeContractFactory()
        InternalTxFactory(to=safe.address, value=1_000_000, call_type=0, error=None)
        ERC20TransferFactory(to=safe.address, value=500)

        with patch(
            "safe_transaction_service.analytics.services."
            "analytics_service.get_token_symbols",
            side_effect=RuntimeError("simulated tokens_token failure"),
        ) as mock_get_token_symbols:
            with self.assertLogs(
                "safe_transaction_service.analytics.tasks_shards", level="WARNING"
            ) as logs:
                compute_tvl_task()

        # Proof the injected failure actually fired and was swallowed
        # where we intended — this is what the old `top_tokens == []`
        # assertion was standing in for.
        self.assertTrue(mock_get_token_symbols.called)
        self.assertTrue(
            any("token metadata lookup failed" in line for line in logs.output),
            logs.output,
        )

        # The *reduced* payload was written, not the placeholder.
        snap = AnalyticsSnapshot.objects.get(name="tvl")
        self.assertIsNotNone(snap.computed_at)
        self.assertNotEqual(snap.payload["native_balance_wei"], "0")
        self.assertTrue(snap.payload["top_tokens"])
        for entry in snap.payload["top_tokens"]:
            # Present-and-null, not absent: a later change that drops the
            # key on the degraded path has to fail here.
            self.assertIn("symbol", entry)
            self.assertIsNone(entry["symbol"])

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

    def test_token_volume_symbol_served_from_rollup(self):
        """Rollup-served read joins `tokens_token` for the `symbol` key:
        a known token gets its symbol, an unknown one is present-and-null
        rather than falling back to its address."""
        from datetime import timedelta

        from django.utils import timezone

        from eth_account import Account

        from safe_transaction_service.analytics.models import DailyTokenVolume

        known = TokenFactory(symbol="USDC")
        unknown_address = Account.create().address
        today = timezone.now().date()
        DailyTokenVolume.objects.create(
            date=today - timedelta(days=1),
            token_address=known.address,
            transfer_count=9,
            transfer_value=900,
        )
        DailyTokenVolume.objects.create(
            date=today - timedelta(days=1),
            token_address=unknown_address,
            transfer_count=2,
            transfer_value=100,
        )

        response = self.client.get(
            reverse("v2:analytics:analytics-token-volume"),
            {"window": "30d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        top_tokens = response.data["top_tokens"]
        self.assertEqual(len(top_tokens), 2)
        symbols = {t["address"]: t["symbol"] for t in top_tokens}
        self.assertEqual(symbols[known.address], "USDC")
        self.assertIn(unknown_address, symbols)
        self.assertIsNone(symbols[unknown_address])

    def test_token_volume_symbol_cold_window_falls_back_to_live(self):
        """Same `symbol` contract on the cold-window live aggregation —
        the key must not appear only on the rollup path."""
        from eth_account import Account

        known = TokenFactory(symbol="DAI")
        unknown_address = Account.create().address
        ERC20TransferFactory(address=known.address, value=100)
        ERC20TransferFactory(address=unknown_address, value=100)

        response = self.client.get(
            reverse("v2:analytics:analytics-token-volume"),
            {"window": "30d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        symbols = {t["address"]: t["symbol"] for t in response.data["top_tokens"]}
        self.assertEqual(symbols[known.address], "DAI")
        self.assertIn(unknown_address, symbols)
        self.assertIsNone(symbols[unknown_address])

    def test_token_volume_blank_symbol_reads_as_null(self):
        """A `tokens_token` row exists but carries an empty symbol — that is
        still "unknown", so it must be null and not an empty string."""
        blank = TokenFactory(symbol="")
        ERC20TransferFactory(address=blank.address, value=100)

        response = self.client.get(
            reverse("v2:analytics:analytics-token-volume"),
            {"window": "30d"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIsNone(response.data["top_tokens"][0]["symbol"])

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


class TestTxVolumeDayBreakdown(AnalyticsTestMixin, APITestCase):
    """phase-B T8 — the opt-in `breakdown=day` parameter on `/tx-volume/`.

    Three cases per spec §5 (absent / valid / invalid) plus the
    rollup-served / cold-rollup pairing every rollup-backed read carries.
    """

    # The scalar payload as it shipped before T8, in emission order. The
    # `breakdown`-absent response must still be exactly this — that is
    # the property that lets the producer half deploy on its own (§4.9),
    # so it is pinned as an ordered list, not as a set.
    KEYS_WITHOUT_BREAKDOWN = [
        "window",
        "total_multisig_txs",
        "executed_multisig_txs",
        "executed_multisig_txs_via_api",
        "executed_multisig_txs_indexed_only",
        "api_attribution_coverage_days",
        "module_txs",
        "total_value_wei",
        "avg_confirmations",
        "avg_confirmations_approximation",
        "coverage_days",
        "computed_at",
        "source",
    ]

    def setUp(self):
        super().setUp()
        from datetime import timedelta

        from django.utils import timezone

        from safe_transaction_service.analytics.models import DailyMetric

        self.today = timezone.now().date()
        # Fully computed day.
        DailyMetric.objects.create(
            date=self.today - timedelta(days=1),
            multisig_txs_proposed=5,
            multisig_txs_executed=4,
            multisig_txs_via_api=3,
            multisig_txs_indexed_only=1,
            erc20_transfers=7,
            module_txs=2,
            native_value_wei=1000,
            confirmations_count=8,
            confirmed_tx_count=4,
            computed_at=timezone.now(),
        )
        # today-2 is deliberately absent from the rollup.
        # Pre-backfill day: the executed split is NULL, not 0.
        DailyMetric.objects.create(
            date=self.today - timedelta(days=3),
            multisig_txs_proposed=1,
            multisig_txs_executed=1,
            erc20_transfers=2,
            computed_at=timezone.now(),
        )
        # Today is not a completed UTC day (`date__lt=today`), and
        # today-40 sits outside the 30d window. Neither may appear.
        DailyMetric.objects.create(
            date=self.today,
            multisig_txs_executed=99,
            computed_at=timezone.now(),
        )
        DailyMetric.objects.create(
            date=self.today - timedelta(days=40),
            multisig_txs_executed=77,
            computed_at=timezone.now(),
        )

    def _get(self, params):
        return self.client.get(
            reverse("v2:analytics:analytics-tx-volume"),
            params,
            **self.auth_header,
        )

    def test_breakdown_absent_response_is_unchanged(self):
        """Case 1 of 3: no parameter, no new keys — in the same order."""
        response = self._get({"window": "30d"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)
        self.assertEqual(list(body.keys()), self.KEYS_WITHOUT_BREAKDOWN)
        self.assertNotIn("days", body)
        self.assertNotIn("window_start", body)
        self.assertNotIn("window_end", body)
        # The pre-T8 numbers for this fixture, unchanged.
        self.assertEqual(body["total_multisig_txs"], 6)
        self.assertEqual(body["executed_multisig_txs"], 5)
        self.assertEqual(body["executed_multisig_txs_via_api"], 3)
        self.assertEqual(body["executed_multisig_txs_indexed_only"], 1)
        self.assertEqual(body["api_attribution_coverage_days"], 1)
        self.assertEqual(body["coverage_days"], 2)
        self.assertEqual(body["total_value_wei"], "1000")
        self.assertEqual(body["source"], "daily_metric")

    def test_breakdown_day_adds_the_series_and_changes_nothing_else(self):
        """Case 2 of 3: the parameter is purely additive. Compared
        key-by-key against the response the same fixture produces
        without it, so a change to any existing value fails here."""
        scalar = json.loads(self._get({"window": "30d"}).content)
        response = self._get({"window": "30d", "breakdown": "day"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)

        self.assertEqual(
            list(body.keys()),
            self.KEYS_WITHOUT_BREAKDOWN + ["window_start", "window_end", "days"],
        )
        for key in self.KEYS_WITHOUT_BREAKDOWN:
            if key == "computed_at":
                # Stamped at read time; equal values would be a coincidence.
                continue
            self.assertEqual(body[key], scalar[key], key)

    def test_breakdown_day_series_shape(self):
        from datetime import timedelta

        response = self._get({"window": "30d", "breakdown": "day"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)

        self.assertEqual(
            body["window_start"], (self.today - timedelta(days=30)).isoformat()
        )
        self.assertEqual(
            body["window_end"], (self.today - timedelta(days=1)).isoformat()
        )

        days = body["days"]
        # Newest-first; one entry per day *present in the rollup*, so
        # today-2 is absent rather than a zero-filled row, today is not a
        # completed day, and today-40 is outside the window.
        self.assertEqual(
            [d["date"] for d in days],
            [
                (self.today - timedelta(days=1)).isoformat(),
                (self.today - timedelta(days=3)).isoformat(),
            ],
        )
        self.assertEqual(
            days[0],
            {
                "date": (self.today - timedelta(days=1)).isoformat(),
                "multisig_txs_executed": 4,
                "multisig_txs_via_api": 3,
                "multisig_txs_indexed_only": 1,
                "erc20_transfers": 7,
            },
        )
        # The two nullable columns pass through as null — present keys,
        # never coerced to 0, exactly as in the scalar payload.
        self.assertIn("multisig_txs_via_api", days[1])
        self.assertIsNone(days[1]["multisig_txs_via_api"])
        self.assertIn("multisig_txs_indexed_only", days[1])
        self.assertIsNone(days[1]["multisig_txs_indexed_only"])
        self.assertEqual(days[1]["multisig_txs_executed"], 1)
        self.assertEqual(days[1]["erc20_transfers"], 2)

    def test_breakdown_day_window_is_not_capped(self):
        """Spec Q21 — a long window is served, not rejected or clamped,
        and `window` itself is echoed back verbatim (it is not validated
        on this endpoint)."""
        from datetime import timedelta

        response = self._get({"window": "365d", "breakdown": "day"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)
        self.assertEqual(body["window"], "365d")
        self.assertEqual(
            body["window_start"], (self.today - timedelta(days=365)).isoformat()
        )
        # today-40 is inside a 365d window and outside a 30d one.
        self.assertEqual(len(body["days"]), 3)

    def test_breakdown_invalid_returns_400(self):
        """Case 3 of 3. Any value but `day`, including an empty one."""
        for value in ("week", "month", "hour", "", "Day", "day,week"):
            with self.subTest(breakdown=value):
                response = self._get({"window": "30d", "breakdown": value})
                self.assertEqual(
                    response.status_code, status.HTTP_400_BAD_REQUEST, value
                )
                self.assertIn("breakdown", json.loads(response.content)["error"])


class TestTxVolumeDayBreakdownColdRollup(AnalyticsTestMixin, APITestCase):
    """The cold half of the rollup pairing: `days` is present and empty,
    never omitted. That is what keeps "old producer, key absent"
    distinguishable from "new producer, no rows yet" for the hub's
    feature detection (T11)."""

    def test_breakdown_day_on_cold_rollup_returns_empty_days(self):
        # Live data the read path must not touch — `/tx-volume/` has no
        # live fallback, it reports the empty rollup honestly.
        MultisigTransactionFactory(value=1000)

        response = self.client.get(
            reverse("v2:analytics:analytics-tx-volume"),
            {"window": "30d", "breakdown": "day"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)
        self.assertIn("days", body)
        self.assertEqual(body["days"], [])
        # The window bounds describe the request, so they still ship.
        self.assertIn("window_start", body)
        self.assertIn("window_end", body)
        self.assertEqual(body["coverage_days"], 0)
        self.assertEqual(body["executed_multisig_txs"], 0)


class _ActiveDayBreakdownMixin(AnalyticsTestMixin):
    """Shared body for the `breakdown=day` cases on `/active-safes/` and
    `/active-owners/` (phase-B T9). The two endpoints read different
    rollups through the same code path, so the assertions are identical
    apart from the rollup model, the address field and the count key —
    the three things each subclass supplies.

    Three cases per spec §5 (absent / valid / invalid) plus the
    rollup-served / cold-rollup pairing every rollup-backed read carries
    (the cold half lives in the companion class below).
    """

    route = None  # reversed route name
    model = None  # DailyActiveSafe | DailyActiveOwner
    address_field = None  # "safe_address" | "owner_address"
    count_key = None  # "active_safes" | "active_owners"

    @property
    def keys_without_breakdown(self):
        """The payload as it shipped before T9, in emission order. The
        `breakdown`-absent response must still be exactly this — that is
        the property that lets the producer half deploy on its own
        (§4.9) — so it is pinned as an ordered list, not as a set."""
        return ["window", self.count_key, "computed_at"]

    def setUp(self):
        super().setUp()
        from datetime import timedelta

        from django.utils import timezone

        from eth_account import Account

        self.today = timezone.now().date()
        self.addr1 = Account.create().address
        self.addr2 = Account.create().address
        self.addr3 = Account.create().address
        self.addr4 = Account.create().address

        def row(days_ago, address):
            self.model.objects.create(
                **{
                    "date": self.today - timedelta(days=days_ago),
                    self.address_field: address,
                }
            )

        # These reads filter `date__gte` with no upper bound, so the
        # partial current UTC day is in the window (unlike /tx-volume/).
        row(0, self.addr3)
        row(1, self.addr1)
        # Two rows on the same day; addr1 spans two days and is one
        # distinct address across the window.
        row(2, self.addr1)
        row(2, self.addr2)
        # today-3 is deliberately absent from the rollup.
        # today-10 sits outside the 7d window used below.
        row(10, self.addr4)

    def _get(self, params):
        return self.client.get(reverse(self.route), params, **self.auth_header)

    def test_breakdown_absent_response_is_unchanged(self):
        """Case 1 of 3: no parameter, no new keys — in the same order."""
        response = self._get({"window": "7d"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)
        self.assertEqual(list(body.keys()), self.keys_without_breakdown)
        self.assertNotIn("days", body)
        self.assertNotIn("window_start", body)
        self.assertNotIn("window_end", body)
        # The pre-T9 number for this fixture, unchanged: distinct over
        # the window, not a per-day sum.
        self.assertEqual(body[self.count_key], 3)
        self.assertIsNotNone(body["computed_at"])

    def test_breakdown_day_adds_the_series_and_changes_nothing_else(self):
        """Case 2 of 3: the parameter is purely additive. Compared
        key-by-key against the response the same fixture produces
        without it, so a change to any existing value fails here."""
        scalar = json.loads(self._get({"window": "7d"}).content)
        response = self._get({"window": "7d", "breakdown": "day"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)

        self.assertEqual(
            list(body.keys()),
            self.keys_without_breakdown + ["window_start", "window_end", "days"],
        )
        for key in self.keys_without_breakdown:
            if key == "computed_at":
                # Stamped at read time; equal values would be a coincidence.
                continue
            self.assertEqual(body[key], scalar[key], key)

    def test_breakdown_day_series_shape(self):
        from datetime import timedelta

        response = self._get({"window": "7d", "breakdown": "day"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)

        self.assertEqual(
            body["window_start"], (self.today - timedelta(days=7)).isoformat()
        )
        # No upper date bound on this read, so the right edge is today.
        self.assertEqual(body["window_end"], self.today.isoformat())

        days = body["days"]
        # Newest-first; one entry per day *present in the rollup*, so
        # today-3 is absent rather than a zero-filled row and today-10
        # is outside the window.
        self.assertEqual(
            [d["date"] for d in days],
            [
                self.today.isoformat(),
                (self.today - timedelta(days=1)).isoformat(),
                (self.today - timedelta(days=2)).isoformat(),
            ],
        )
        self.assertEqual(
            days,
            [
                {"date": self.today.isoformat(), self.count_key: 1},
                {
                    "date": (self.today - timedelta(days=1)).isoformat(),
                    self.count_key: 1,
                },
                {
                    "date": (self.today - timedelta(days=2)).isoformat(),
                    self.count_key: 2,
                },
            ],
        )

    def test_per_day_values_are_not_additive(self):
        """Contract invariant 3, pinned. The per-day entries are per-day
        distinct counts: summing them (4 here) does not give the window
        value (3), because addr1 is active on two of the days. A regression
        that derived either number from the other fails here."""
        response = self._get({"window": "7d", "breakdown": "day"})
        body = json.loads(response.content)
        self.assertEqual(sum(d[self.count_key] for d in body["days"]), 4)
        self.assertEqual(body[self.count_key], 3)

    def test_breakdown_day_does_not_relax_window_validation(self):
        """The 7d|30d|90d guard is unchanged and still runs first."""
        for window in ("5d", "365d", "1y", ""):
            with self.subTest(window=window):
                response = self._get({"window": window, "breakdown": "day"})
                self.assertEqual(
                    response.status_code, status.HTTP_400_BAD_REQUEST, window
                )
                self.assertIn("window", json.loads(response.content)["error"])

    def test_breakdown_day_accepts_every_valid_window_uncapped(self):
        """Spec Q21 — `breakdown=day` caps nothing; each of the three
        allowed windows is served."""
        from datetime import timedelta

        for window, expected in (("7d", 3), ("30d", 4), ("90d", 4)):
            with self.subTest(window=window):
                response = self._get({"window": window, "breakdown": "day"})
                self.assertEqual(response.status_code, status.HTTP_200_OK)
                body = json.loads(response.content)
                self.assertEqual(body["window"], window)
                self.assertEqual(
                    body["window_start"],
                    (self.today - timedelta(days=int(window[:-1]))).isoformat(),
                )
                # today-10 joins the series from 30d up.
                self.assertEqual(len(body["days"]), expected)

    def test_breakdown_invalid_returns_400(self):
        """Case 3 of 3. Any value but `day`, including an empty one."""
        for value in ("week", "month", "hour", "", "Day", "day,week"):
            with self.subTest(breakdown=value):
                response = self._get({"window": "7d", "breakdown": value})
                self.assertEqual(
                    response.status_code, status.HTTP_400_BAD_REQUEST, value
                )
                self.assertIn("breakdown", json.loads(response.content)["error"])


class _ActiveDayBreakdownColdMixin(AnalyticsTestMixin):
    """The cold half of the rollup pairing: `days` is present and empty,
    never omitted, on **both** paths a cold rollup can take — the Redis
    cached-scalar fallback and the final zero payload. That is what keeps
    "old producer, key absent" distinguishable from "new producer, no
    rows yet" for the hub's feature detection (T11)."""

    route = None
    count_key = None
    redis_prefix = None

    def test_breakdown_day_on_cold_rollup_returns_empty_days(self):
        response = self.client.get(
            reverse(self.route),
            {"window": "30d", "breakdown": "day"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)
        self.assertIn("days", body)
        self.assertEqual(body["days"], [])
        # The window bounds describe the request, so they still ship.
        self.assertIn("window_start", body)
        self.assertIn("window_end", body)
        self.assertEqual(body[self.count_key], 0)

    def test_breakdown_day_on_cached_fallback_returns_empty_days(self):
        """The cold-window fallback serves a Redis window scalar with no
        per-day rows behind it. It must still emit the key."""
        self.redis.set(
            self.redis_prefix + "30d",
            json.dumps(
                {
                    "window": "30d",
                    self.count_key: 7,
                    "computed_at": "2026-05-18T00:00:00+00:00",
                }
            ),
        )

        response = self.client.get(
            reverse(self.route),
            {"window": "30d", "breakdown": "day"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)
        # 7 can only have come from the cached fallback, so this pins
        # which path emitted the key.
        self.assertEqual(body[self.count_key], 7)
        self.assertEqual(body["computed_at"], "2026-05-18T00:00:00+00:00")
        self.assertIn("days", body)
        self.assertEqual(body["days"], [])
        self.assertIn("window_start", body)
        self.assertIn("window_end", body)


class TestActiveSafesDayBreakdown(_ActiveDayBreakdownMixin, APITestCase):
    route = "v2:analytics:analytics-active-safes"
    address_field = "safe_address"
    count_key = "active_safes"

    @property
    def model(self):
        from safe_transaction_service.analytics.models import DailyActiveSafe

        return DailyActiveSafe


class TestActiveSafesDayBreakdownColdRollup(_ActiveDayBreakdownColdMixin, APITestCase):
    route = "v2:analytics:analytics-active-safes"
    count_key = "active_safes"
    redis_prefix = AnalyticsService.REDIS_ACTIVE_SAFES_PREFIX


class TestActiveOwnersDayBreakdown(_ActiveDayBreakdownMixin, APITestCase):
    route = "v2:analytics:analytics-active-owners"
    address_field = "owner_address"
    count_key = "active_owners"

    @property
    def model(self):
        from safe_transaction_service.analytics.models import DailyActiveOwner

        return DailyActiveOwner


class TestActiveOwnersDayBreakdownColdRollup(_ActiveDayBreakdownColdMixin, APITestCase):
    route = "v2:analytics:analytics-active-owners"
    count_key = "active_owners"
    redis_prefix = AnalyticsService.REDIS_ACTIVE_OWNERS_PREFIX


class _ActiveRangeMixin(AnalyticsTestMixin):
    """Shared body for the optional `from`/`to` range on `/active-safes/`
    and `/active-owners/` (phase-B T10). Same arrangement as T9's
    breakdown mixin — the two endpoints differ only in the rollup model,
    the address field, the count key and the Redis prefix — and the same
    fixture, so the ranged numbers can be compared against the windowed
    ones the T9 cases already pin.

    Three cases per spec §5: range absent (byte-identical to pre-T10),
    range valid, range invalid (400). "Invalid" has three distinct forms
    here — an unparseable `from`, an unparseable `to`, and `from > to` —
    and each is asserted on its *message*, not only on the status, since
    the task requires the offending parameter to be named.
    """

    route = None  # reversed route name
    model = None  # DailyActiveSafe | DailyActiveOwner
    address_field = None  # "safe_address" | "owner_address"
    count_key = None  # "active_safes" | "active_owners"
    redis_prefix = None  # legacy rolling-window cache key prefix

    # Bounds that are not a strict ISO `YYYY-MM-DD` calendar date. The last
    # three are the interesting ones: `date.fromisoformat` alone accepts the
    # basic and week forms, and `parse_date` accepts `2026-1-5` — this
    # endpoint accepts none of them, unlike `/safe-creations/`'s lenient
    # `parse_datetime` bounds, which are deliberately left alone (spec §6).
    unparseable_bounds = (
        "not-a-date",
        "2026-13-01",
        "2026-02-30",
        "01-02-2026",
        "",
        "20260105",
        "2026-W01-1",
        "2026-1-5",
        "2026-01-05T00:00:00+00:00",
    )

    @property
    def keys_without_range(self):
        """The payload as it shipped before T10, in emission order — the
        range adds no key, it only changes `window` to null when it wins.
        Pinned as an ordered list, not a set, for the reason T9 gives."""
        return ["window", self.count_key, "computed_at"]

    def setUp(self):
        super().setUp()
        from datetime import timedelta

        from django.utils import timezone

        from eth_account import Account

        self.today = timezone.now().date()
        self.addr1 = Account.create().address
        self.addr2 = Account.create().address
        self.addr3 = Account.create().address
        self.addr4 = Account.create().address

        def row(days_ago, address):
            self.model.objects.create(
                **{
                    "date": self.today - timedelta(days=days_ago),
                    self.address_field: address,
                }
            )

        # Same fixture as the T9 breakdown cases: addr1 is active on two
        # days, so distinct-over-the-span and sum-of-days differ.
        row(0, self.addr3)
        row(1, self.addr1)
        row(2, self.addr1)
        row(2, self.addr2)
        # today-3 is absent from the rollup; today-10 is outside 7d.
        row(10, self.addr4)

    def _get(self, params):
        return self.client.get(reverse(self.route), params, **self.auth_header)

    def _day(self, days_ago):
        from datetime import timedelta

        return (self.today - timedelta(days=days_ago)).isoformat()

    # ── case 1 of 3: absent ──────────────────────────────────────────

    def test_range_absent_response_is_unchanged(self):
        """No `from`, no `to` — the pre-T10 response, keys in order and
        `window` still the requested window string rather than null."""
        response = self._get({"window": "7d"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)
        self.assertEqual(list(body.keys()), self.keys_without_range)
        self.assertEqual(body["window"], "7d")
        # Distinct over the 7d window: addr1 (two days), addr2, addr3.
        self.assertEqual(body[self.count_key], 3)
        self.assertIsNotNone(body["computed_at"])

    # ── case 2 of 3: valid ───────────────────────────────────────────

    def test_range_returns_the_distinct_count_over_exactly_that_range(self):
        """A COUNT(DISTINCT …) over the requested span and nothing else:
        the closed range excludes both the days after `to` and the days
        before `from`, and counts a repeat address once."""
        for date_from, date_to, expected in (
            (10, 0, 4),  # every seeded day: addr1..addr4
            (10, 2, 3),  # drops addr3 (today) and addr1's today-1 row
            (2, 2, 2),  # single day, two rows: addr1 + addr2
            (10, 10, 1),  # single day, one row: addr4
            (9, 3, 0),  # a span with no rows at all
        ):
            with self.subTest(date_from=date_from, date_to=date_to):
                response = self._get(
                    {"from": self._day(date_from), "to": self._day(date_to)}
                )
                self.assertEqual(response.status_code, status.HTTP_200_OK)
                body = json.loads(response.content)
                self.assertEqual(body[self.count_key], expected)
                # The range won, so no window applies to this number.
                self.assertIsNone(body["window"])
                self.assertEqual(list(body.keys()), self.keys_without_range)

    def test_one_sided_range_leaves_the_other_side_unbounded(self):
        """Either bound alone is valid, and the window stops applying as
        soon as one of them is supplied."""
        from_only = json.loads(self._get({"from": self._day(1)}).content)
        # today-1 and today: addr1 + addr3.
        self.assertEqual(from_only[self.count_key], 2)
        self.assertIsNone(from_only["window"])

        to_only = json.loads(self._get({"to": self._day(2)}).content)
        # Everything up to today-2, including the out-of-window today-10.
        self.assertEqual(to_only[self.count_key], 3)
        self.assertIsNone(to_only["window"])

    def test_range_wins_over_window(self):
        """§4.5: "when both a range and `window` are supplied the range
        wins". Asserted on the number, which only the range can produce —
        7d alone gives 3 — and on `window` being reported as null."""
        windowed = json.loads(self._get({"window": "7d"}).content)
        self.assertEqual(windowed[self.count_key], 3)

        response = self._get(
            {"window": "7d", "from": self._day(10), "to": self._day(0)}
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)
        # addr4 is 10 days old: inside the range, outside the 7d window.
        self.assertEqual(body[self.count_key], 4)
        self.assertIsNone(body["window"])

        # And the other way round: a range narrower than the window.
        narrow = json.loads(
            self._get(
                {"window": "90d", "from": self._day(2), "to": self._day(2)}
            ).content
        )
        self.assertEqual(narrow[self.count_key], 2)

    def test_range_length_is_not_capped(self):
        """Q21, applied to the range: no cap, in either direction."""
        response = self._get({"from": self._day(3650), "to": self._day(0)})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)
        self.assertEqual(body[self.count_key], 4)

    def test_ranged_cold_read_does_not_serve_the_cached_window_scalar(self):
        """The Redis cold-window fallback holds a rolling *window* number.
        Serving it for a range request would answer a different question,
        so a range that selects no rows returns the honest zero payload."""
        self.redis.set(
            self.redis_prefix + "30d",
            json.dumps(
                {
                    "window": "30d",
                    self.count_key: 7,
                    "computed_at": "2026-05-18T00:00:00+00:00",
                }
            ),
        )

        response = self._get({"from": self._day(9), "to": self._day(3)})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)
        self.assertEqual(body[self.count_key], 0)
        self.assertIsNone(body["computed_at"])
        self.assertIsNone(body["window"])

    # ── case 3 of 3: invalid, in its three distinct forms ────────────

    def test_unparseable_from_returns_400_naming_from(self):
        for value in self.unparseable_bounds:
            with self.subTest(value=value):
                response = self._get({"from": value, "to": self._day(0)})
                self.assertEqual(
                    response.status_code, status.HTTP_400_BAD_REQUEST, value
                )
                error = json.loads(response.content)["error"]
                self.assertEqual(error, "from must be an ISO date (YYYY-MM-DD)")

    def test_unparseable_to_returns_400_naming_to(self):
        for value in self.unparseable_bounds:
            with self.subTest(value=value):
                response = self._get({"from": self._day(10), "to": value})
                self.assertEqual(
                    response.status_code, status.HTTP_400_BAD_REQUEST, value
                )
                error = json.loads(response.content)["error"]
                self.assertEqual(error, "to must be an ISO date (YYYY-MM-DD)")

    def test_from_after_to_returns_400_naming_from_and_to(self):
        for date_from, date_to in ((0, 1), (2, 10), (0, 3650)):
            with self.subTest(date_from=date_from, date_to=date_to):
                response = self._get(
                    {"from": self._day(date_from), "to": self._day(date_to)}
                )
                self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
                error = json.loads(response.content)["error"]
                self.assertEqual(error, "from must not be after to")

    def test_equal_bounds_are_a_valid_single_day_range(self):
        """`from == to` is the boundary of the previous case and is not an
        error: a one-day closed range."""
        response = self._get({"from": self._day(2), "to": self._day(2)})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(json.loads(response.content)[self.count_key], 2)

    def test_a_range_does_not_waive_the_window_validation(self):
        """Precedence decides which days are counted, not which parameters
        are checked: an invalid `window` is still a 400, and it is reported
        before the range because it is the more basic error."""
        response = self._get(
            {"window": "5d", "from": self._day(10), "to": self._day(0)}
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("window", json.loads(response.content)["error"])

        # Two things wrong at once still reports `window` first.
        response = self._get({"window": "5d", "from": "not-a-date"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("window", json.loads(response.content)["error"])

    # ── range × breakdown=day (T9's series over a T10 span) ──────────

    def test_range_with_breakdown_day_series_covers_exactly_the_range(self):
        response = self._get(
            {"from": self._day(10), "to": self._day(2), "breakdown": "day"}
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)

        self.assertEqual(
            list(body.keys()),
            self.keys_without_range + ["window_start", "window_end", "days"],
        )
        # The bounds are the range asked for, not a window off today.
        self.assertEqual(body["window_start"], self._day(10))
        self.assertEqual(body["window_end"], self._day(2))
        # Newest-first, one entry per day present in the rollup: today and
        # today-1 are past `to`, today-3 is absent rather than zero-filled.
        self.assertEqual(
            body["days"],
            [
                {"date": self._day(2), self.count_key: 2},
                {"date": self._day(10), self.count_key: 1},
            ],
        )

    def test_range_with_breakdown_day_values_are_not_additive(self):
        """Contract invariant 3 over a range — T9's
        `test_per_day_values_are_not_additive`, extended to a span that is
        not a window. Neither number may be derived from the other: the
        series sums to 5 while the distinct count over the same span is 4,
        because addr1 is active on two of those days."""
        response = self._get(
            {"from": self._day(10), "to": self._day(0), "breakdown": "day"}
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)
        self.assertEqual(len(body["days"]), 4)
        self.assertEqual(sum(d[self.count_key] for d in body["days"]), 5)
        self.assertEqual(body[self.count_key], 4)

    def test_to_only_range_with_breakdown_day_reports_no_lower_bound(self):
        """The one case where `window_start` is null: nothing bounds the
        read below, so there is no honest date to name."""
        response = self._get({"to": self._day(2), "breakdown": "day"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)
        self.assertIsNone(body["window_start"])
        self.assertEqual(body["window_end"], self._day(2))
        self.assertEqual(
            [d["date"] for d in body["days"]], [self._day(2), self._day(10)]
        )
        self.assertEqual(body[self.count_key], 3)

    def test_ranged_cold_read_with_breakdown_day_returns_empty_days(self):
        """A range with no rows keeps T9's cold-read contract: `days`
        present and empty, bounds still describing the request."""
        response = self._get(
            {"from": self._day(9), "to": self._day(3), "breakdown": "day"}
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = json.loads(response.content)
        self.assertEqual(body["days"], [])
        self.assertEqual(body["window_start"], self._day(9))
        self.assertEqual(body["window_end"], self._day(3))
        self.assertEqual(body[self.count_key], 0)
        self.assertIsNone(body["computed_at"])


class TestActiveSafesDateRange(_ActiveRangeMixin, APITestCase):
    route = "v2:analytics:analytics-active-safes"
    address_field = "safe_address"
    count_key = "active_safes"
    redis_prefix = AnalyticsService.REDIS_ACTIVE_SAFES_PREFIX

    @property
    def model(self):
        from safe_transaction_service.analytics.models import DailyActiveSafe

        return DailyActiveSafe


class TestActiveOwnersDateRange(_ActiveRangeMixin, APITestCase):
    route = "v2:analytics:analytics-active-owners"
    address_field = "owner_address"
    count_key = "active_owners"
    redis_prefix = AnalyticsService.REDIS_ACTIVE_OWNERS_PREFIX

    @property
    def model(self):
        from safe_transaction_service.analytics.models import DailyActiveOwner

        return DailyActiveOwner


class TestSafeCreationsRangeStaysLenient(AnalyticsTestMixin, APITestCase):
    """`/safe-creations/` keeps its lenient `parse_datetime` bounds: an
    unusable bound is silently *ignored*, never a 400. That is existing
    contract and spec §6 explicitly leaves it alone, so T10's strict
    `YYYY-MM-DD` parsing was deliberately **not** shared with this
    endpoint. Pinned here so a later "harmonise the date parsing" edit
    fails a test instead of quietly changing a deployed contract.

    Correction to the wording in §4.5/§6, established while writing this:
    at Django 5.2 + Python 3.12 a *date-only* bound is not ignored — since
    Django 5.0 `parse_datetime` tries `datetime.fromisoformat` first, and
    that accepts `2026-05-01` (and `20260501`), so a date-only bound here
    is honoured as midnight. What is silently ignored is a bound
    `parse_datetime` cannot use at all (`not-a-date`, `2026-13-01`,
    `2026-02-30` — the last returns `None` rather than raising, so there is
    no 500 either). Both halves are pinned below; the out-of-scope
    decision is unaffected, only the reason given for it.
    """

    def _seed_day_series(self, series):
        payload = {"series": series, "computed_at": "2026-05-18T00:00:00+00:00"}
        self.redis.set(AnalyticsService.REDIS_SAFE_CREATIONS, json.dumps(payload))

    def _both_days(self):
        self._seed_day_series(
            [
                {"period": "2026-04-15", "count": 1},
                {"period": "2026-05-04", "count": 3},
            ]
        )

    def test_unusable_bounds_are_ignored_not_rejected(self):
        """The lenient half. Each of these would be a 400 naming the
        parameter on `/active-safes/` and `/active-owners/`; here they are
        dropped and filter nothing."""
        for value in ("not-a-date", "2026-13-01", "2026-02-30", ""):
            with self.subTest(value=value):
                self._both_days()
                response = self.client.get(
                    reverse("v2:analytics:analytics-safe-creations"),
                    {"interval": "day", "from": value, "to": value},
                    **self.auth_header,
                )
                self.assertEqual(response.status_code, status.HTTP_200_OK, value)
                self.assertEqual(
                    response.data,
                    [
                        {"period": "2026-04-15", "count": 1},
                        {"period": "2026-05-04", "count": 3},
                    ],
                )

    def test_date_only_bounds_are_honoured_as_midnight(self):
        """The correction. `parse_datetime` resolves a date-only bound at
        this Django/Python pin, so the range does filter — which is why
        this endpoint is left exactly as it is rather than being described
        as ignoring such bounds."""
        self._both_days()
        response = self.client.get(
            reverse("v2:analytics:analytics-safe-creations"),
            {"interval": "day", "from": "2026-05-01", "to": "2026-05-15"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, [{"period": "2026-05-04", "count": 3}])


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
