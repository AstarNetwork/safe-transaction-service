"""Tests for `GET /api/v2/analytics/token-holdings/` (token-holdings spec
`docs/specs/token-holdings.md` §5, task P5 in the workspace root).

`test_views_v2.py` is already large (3000+ lines), so P5 gets its own file
per the task instructions.

Covers: every row of the §5 param table (`min_holders`, `limit`, `cursor`,
`tokens`), 400 on each malformed input, 409 on a stale cursor, a full
keyset pagination walk with ties, the warming shape + fire-and-forget
dispatch, 401 without a token, a token missing its `tokens_token` row, and
`safes_holding_requested` (orphan-excluding).
"""

import base64
import json
from unittest.mock import patch

from django.contrib.auth.models import User
from django.urls import reverse
from django.utils import timezone

from eth_account import Account
from rest_framework import status
from rest_framework.authtoken.models import Token as AuthToken
from rest_framework.test import APITestCase

from safe_transaction_service.analytics.models import (
    AnalyticsSnapshot,
    SafeTokenBalance,
    TokenHolding,
)
from safe_transaction_service.history.tests.factories import SafeContractFactory
from safe_transaction_service.tokens.tests.factories import TokenFactory
from safe_transaction_service.utils.redis import get_redis

URL_NAME = "v2:analytics:analytics-token-holdings"


def _write_snapshot(
    as_of_block: int,
    tokens_with_holders: int = 0,
    safes_with_any_erc20: int = 0,
    negative_pairs_total: int = 0,
    orphan_pairs: int = 0,
    as_of_timestamp=None,
) -> None:
    as_of_timestamp = as_of_timestamp or timezone.now()
    AnalyticsSnapshot.objects.update_or_create(
        name="token_holdings",
        defaults={
            "payload": {
                "as_of_block": as_of_block,
                "as_of_timestamp": as_of_timestamp.isoformat(),
                "tokens_with_holders": tokens_with_holders,
                "safes_with_any_erc20": safes_with_any_erc20,
                "negative_pairs_total": negative_pairs_total,
                "orphan_pairs": orphan_pairs,
            },
            "computed_at": timezone.now(),
        },
    )


def _write_token_holding(
    token_address: str,
    holders: int,
    total_balance: int = 0,
    negative_pairs: int = 0,
    as_of_block: int = 100,
    as_of_timestamp=None,
) -> TokenHolding:
    return TokenHolding.objects.create(
        token_address=token_address,
        holders=holders,
        negative_pairs=negative_pairs,
        total_balance=total_balance,
        as_of_block=as_of_block,
        as_of_timestamp=as_of_timestamp or timezone.now(),
        computed_at=timezone.now(),
    )


def _encode_cursor(as_of_block: int, holders: int, token_address: str) -> str:
    raw = json.dumps(
        {"b": as_of_block, "h": holders, "a": token_address}, separators=(",", ":")
    ).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


class TokenHoldingsTestMixin:
    def setUp(self):
        super().setUp()
        self.redis = get_redis()
        self.redis.flushall()
        self.user, _ = User.objects.get_or_create(username="test", password="12345")
        self.auth_token, _ = AuthToken.objects.get_or_create(user=self.user)
        self.auth_header = {"HTTP_AUTHORIZATION": "Token " + self.auth_token.key}
        self.url = reverse(URL_NAME)


class TestAuth(TokenHoldingsTestMixin, APITestCase):
    def test_401_without_token(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)


class TestWarming(TokenHoldingsTestMixin, APITestCase):
    @patch(
        "safe_transaction_service.analytics.tasks.compute_erc20_balance_rollup_task.delay"
    )
    def test_missing_snapshot_returns_warming_and_dispatches_refresh(self, mock_delay):
        self.assertFalse(
            AnalyticsSnapshot.objects.filter(name="token_holdings").exists()
        )

        response = self.client.get(self.url, **self.auth_header)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["tokens"], [])
        self.assertIsNone(response.data["computed_at"])
        self.assertIsNone(response.data["as_of_block"])
        self.assertIsNone(response.data["as_of_timestamp"])
        self.assertIsNone(response.data["tokens_with_holders"])
        self.assertIsNone(response.data["safes_with_any_erc20"])
        self.assertIsNone(response.data["next_cursor"])
        mock_delay.assert_called_once()

    @patch(
        "safe_transaction_service.analytics.tasks.compute_erc20_balance_rollup_task.delay"
    )
    def test_snapshot_present_but_token_holding_empty_is_still_warming(
        self, mock_delay
    ):
        """§5 warming applies when *either* the snapshot is missing *or*
        `TokenHolding` has no rows — a snapshot can exist from a previous
        run while every row was since deleted (every token's last holder
        exited)."""
        _write_snapshot(as_of_block=100)
        self.assertFalse(TokenHolding.objects.exists())

        response = self.client.get(self.url, **self.auth_header)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["tokens"], [])
        self.assertIsNone(response.data["computed_at"])
        mock_delay.assert_called_once()


class TestParamValidation(TokenHoldingsTestMixin, APITestCase):
    def setUp(self):
        super().setUp()
        _write_snapshot(as_of_block=100, tokens_with_holders=1, safes_with_any_erc20=1)
        _write_token_holding(Account.create().address, holders=5, as_of_block=100)

    def test_min_holders_non_integer(self):
        response = self.client.get(self.url, {"min_holders": "abc"}, **self.auth_header)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("error", response.data)

    def test_min_holders_zero(self):
        response = self.client.get(self.url, {"min_holders": "0"}, **self.auth_header)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_min_holders_negative(self):
        response = self.client.get(self.url, {"min_holders": "-1"}, **self.auth_header)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_limit_non_integer(self):
        response = self.client.get(self.url, {"limit": "abc"}, **self.auth_header)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_limit_zero(self):
        response = self.client.get(self.url, {"limit": "0"}, **self.auth_header)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_limit_over_1000(self):
        response = self.client.get(self.url, {"limit": "1001"}, **self.auth_header)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_limit_boundaries_are_accepted(self):
        for limit in ("1", "1000", "500"):
            response = self.client.get(self.url, {"limit": limit}, **self.auth_header)
            self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_cursor_not_base64(self):
        response = self.client.get(
            self.url, {"cursor": "!!!not-base64!!!"}, **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_cursor_not_json(self):
        bad = base64.urlsafe_b64encode(b"not json").decode("ascii").rstrip("=")
        response = self.client.get(self.url, {"cursor": bad}, **self.auth_header)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_cursor_missing_key(self):
        bad = (
            base64.urlsafe_b64encode(json.dumps({"b": 1, "h": 2}).encode())
            .decode("ascii")
            .rstrip("=")
        )
        response = self.client.get(self.url, {"cursor": bad}, **self.auth_header)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_cursor_wrong_types(self):
        bad = (
            base64.urlsafe_b64encode(
                json.dumps(
                    {"b": "not-int", "h": 2, "a": Account.create().address}
                ).encode()
            )
            .decode("ascii")
            .rstrip("=")
        )
        response = self.client.get(self.url, {"cursor": bad}, **self.auth_header)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_cursor_bool_rejected_as_int(self):
        bad = (
            base64.urlsafe_b64encode(
                json.dumps({"b": True, "h": 2, "a": Account.create().address}).encode()
            )
            .decode("ascii")
            .rstrip("=")
        )
        response = self.client.get(self.url, {"cursor": bad}, **self.auth_header)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_cursor_invalid_address(self):
        bad = (
            base64.urlsafe_b64encode(
                json.dumps({"b": 1, "h": 2, "a": "not-an-address"}).encode()
            )
            .decode("ascii")
            .rstrip("=")
        )
        response = self.client.get(self.url, {"cursor": bad}, **self.auth_header)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_tokens_invalid_address(self):
        response = self.client.get(
            self.url, {"tokens": "not-an-address"}, **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_tokens_empty_value(self):
        response = self.client.get(self.url, {"tokens": ""}, **self.auth_header)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_tokens_at_80_after_dedupe_is_200(self):
        addresses = [Account.create().address for _ in range(80)]
        response = self.client.get(
            self.url, {"tokens": ",".join(addresses)}, **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_tokens_over_80_after_dedupe(self):
        addresses = [Account.create().address for _ in range(81)]
        response = self.client.get(
            self.url, {"tokens": ",".join(addresses)}, **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_tokens_81_entries_collapsing_to_80_after_dedupe_is_200(self):
        addresses = [Account.create().address for _ in range(80)]
        # Dedupe happens before the cap check (spec P9): 81 raw entries that
        # collapse to 80 unique addresses must pass, not 400.
        response = self.client.get(
            self.url,
            {"tokens": ",".join(addresses + [addresses[0]])},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_tokens_one_malformed_item_among_valid_ones_is_400(self):
        addr = Account.create().address
        response = self.client.get(
            self.url, {"tokens": f"{addr},not-an-address"}, **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)


class TestStaleCursor(TokenHoldingsTestMixin, APITestCase):
    def test_stale_as_of_block_is_409(self):
        _write_snapshot(as_of_block=100, tokens_with_holders=1, safes_with_any_erc20=1)
        token_address = Account.create().address
        _write_token_holding(token_address, holders=5, as_of_block=100)

        stale_cursor = _encode_cursor(99, 10, Account.create().address)
        response = self.client.get(
            self.url, {"cursor": stale_cursor}, **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_409_CONFLICT)
        self.assertIn("error", response.data)


class TestPagination(TokenHoldingsTestMixin, APITestCase):
    def test_min_holders_filters_the_tail(self):
        _write_snapshot(as_of_block=100, tokens_with_holders=2, safes_with_any_erc20=2)
        low = Account.create().address
        high = Account.create().address
        _write_token_holding(low, holders=1, as_of_block=100)
        _write_token_holding(high, holders=10, as_of_block=100)

        response = self.client.get(self.url, {"min_holders": "2"}, **self.auth_header)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        returned = {t["token_address"] for t in response.data["tokens"]}
        self.assertEqual(returned, {high})
        self.assertIsNone(response.data["next_cursor"])

    def test_response_shape_and_total_balance_is_a_string(self):
        _write_snapshot(
            as_of_block=100,
            tokens_with_holders=1,
            safes_with_any_erc20=1,
            negative_pairs_total=3,
            orphan_pairs=2,
        )
        token_address = Account.create().address
        _write_token_holding(
            token_address,
            holders=7,
            total_balance=1_300_000_000,
            negative_pairs=3,
            as_of_block=100,
        )

        response = self.client.get(self.url, **self.auth_header)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["as_of_block"], 100)
        self.assertEqual(response.data["tokens_with_holders"], 1)
        self.assertEqual(response.data["safes_with_any_erc20"], 1)
        self.assertEqual(response.data["negative_pairs_total"], 3)
        self.assertEqual(response.data["orphan_pairs"], 2)
        self.assertIsNotNone(response.data["computed_at"])
        self.assertEqual(len(response.data["tokens"]), 1)
        row = response.data["tokens"][0]
        self.assertEqual(row["token_address"], token_address)
        self.assertEqual(row["holders"], 7)
        self.assertEqual(row["total_balance"], "1300000000")
        self.assertIsInstance(row["total_balance"], str)
        self.assertEqual(row["negative_pairs"], 3)

    def test_missing_tokens_token_row_gives_null_symbol_and_decimals(self):
        _write_snapshot(as_of_block=100, tokens_with_holders=1, safes_with_any_erc20=1)
        token_address = Account.create().address
        _write_token_holding(token_address, holders=5, as_of_block=100)
        # No `tokens_token` row is ever created for `token_address`.

        response = self.client.get(self.url, **self.auth_header)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        row = response.data["tokens"][0]
        self.assertIsNone(row["symbol"])
        self.assertIsNone(row["decimals"])

    def test_metadata_join_uses_tokens_token_when_present(self):
        _write_snapshot(as_of_block=100, tokens_with_holders=1, safes_with_any_erc20=1)
        token = TokenFactory(symbol="USDC", decimals=6)
        _write_token_holding(token.address, holders=5, as_of_block=100)

        response = self.client.get(self.url, **self.auth_header)
        row = response.data["tokens"][0]
        self.assertEqual(row["symbol"], "USDC")
        self.assertEqual(row["decimals"], 6)

    def test_full_pagination_walk_with_ties_returns_every_token_exactly_once(self):
        _write_snapshot(as_of_block=100, tokens_with_holders=5, safes_with_any_erc20=5)
        # Two ties on `holders` (10 and 5) among 5 tokens, to exercise the
        # `token_address` tie-break.
        addresses = [Account.create().address for _ in range(5)]
        holders_by_index = [10, 10, 10, 5, 5]
        for address, holders in zip(addresses, holders_by_index, strict=False):
            _write_token_holding(address, holders=holders, as_of_block=100)

        # Ground truth: the full order with a limit wide enough for one page.
        full = self.client.get(self.url, {"limit": "1000"}, **self.auth_header)
        self.assertEqual(full.status_code, status.HTTP_200_OK)
        expected_order = [t["token_address"] for t in full.data["tokens"]]
        self.assertEqual(len(expected_order), 5)
        self.assertIsNone(full.data["next_cursor"])

        # Walk with limit=2.
        seen = []
        cursor = None
        pages = 0
        while True:
            params = {"limit": "2"}
            if cursor is not None:
                params["cursor"] = cursor
            response = self.client.get(self.url, params, **self.auth_header)
            self.assertEqual(response.status_code, status.HTTP_200_OK)
            pages += 1
            page_tokens = [t["token_address"] for t in response.data["tokens"]]
            self.assertLessEqual(len(page_tokens), 2)
            seen.extend(page_tokens)
            cursor = response.data["next_cursor"]
            if cursor is None:
                break
            self.assertLess(pages, 10)  # guard against an infinite loop

        self.assertEqual(seen, expected_order)
        self.assertEqual(len(seen), len(set(seen)), "every token exactly once")
        self.assertEqual(pages, 3)  # 2 + 2 + 1


class TestTokensParam(TokenHoldingsTestMixin, APITestCase):
    def test_returns_exactly_requested_tokens_ignoring_min_holders_and_cursor(self):
        _write_snapshot(as_of_block=100, tokens_with_holders=2, safes_with_any_erc20=2)
        requested = Account.create().address
        other = Account.create().address
        # `requested` has only 1 holder -- below a high `min_holders`, and
        # would never come up in the paginated walk.
        _write_token_holding(requested, holders=1, as_of_block=100)
        _write_token_holding(other, holders=100, as_of_block=100)

        response = self.client.get(
            self.url,
            {
                "tokens": requested,
                "min_holders": "1000",
                # Malformed cursor: ignored entirely in `tokens=` mode.
                "cursor": "!!!garbage!!!",
            },
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        returned = [t["token_address"] for t in response.data["tokens"]]
        self.assertEqual(returned, [requested])
        self.assertIsNone(response.data["next_cursor"])

    def test_tokens_not_present_in_token_holding_are_simply_omitted(self):
        _write_snapshot(as_of_block=100, tokens_with_holders=0, safes_with_any_erc20=0)
        never_held = Account.create().address

        response = self.client.get(self.url, {"tokens": never_held}, **self.auth_header)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["tokens"], [])

    def test_dedupe_is_case_insensitive(self):
        _write_snapshot(as_of_block=100, tokens_with_holders=1, safes_with_any_erc20=1)
        token_address = Account.create().address
        _write_token_holding(token_address, holders=3, as_of_block=100)

        response = self.client.get(
            self.url,
            {"tokens": f"{token_address},{token_address.lower()}"},
            **self.auth_header,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["tokens"]), 1)

    def test_safes_holding_requested_excludes_orphans_and_non_positive_balances(self):
        _write_snapshot(as_of_block=100, tokens_with_holders=1, safes_with_any_erc20=1)
        token_address = Account.create().address
        _write_token_holding(token_address, holders=1, as_of_block=100)

        real_safe = SafeContractFactory()
        orphan_address = Account.create().address  # never a SafeContract row

        SafeTokenBalance.objects.create(
            safe_address=real_safe.address, token_address=token_address, balance=100
        )
        SafeTokenBalance.objects.create(
            safe_address=orphan_address, token_address=token_address, balance=100
        )
        # A real Safe with a non-positive balance never counts either.
        zero_balance_safe = SafeContractFactory()
        SafeTokenBalance.objects.create(
            safe_address=zero_balance_safe.address,
            token_address=token_address,
            balance=0,
        )

        response = self.client.get(
            self.url, {"tokens": token_address}, **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["safes_holding_requested"], 1)

    def test_safes_holding_requested_null_when_warming(self):
        never_written_token = Account.create().address
        response = self.client.get(
            self.url, {"tokens": never_written_token}, **self.auth_header
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIsNone(response.data["safes_holding_requested"])
        self.assertEqual(response.data["tokens"], [])
