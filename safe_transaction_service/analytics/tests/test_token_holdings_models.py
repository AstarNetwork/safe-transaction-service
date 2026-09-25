"""Tests for the token-holdings storage models: ``SafeTokenBalance``,
``TokenHolding`` and ``Erc20BalanceWhale`` (spec §4.1, task P1).

Scope is deliberately narrow — this only proves the schema holds what it
must:

- a negative ``balance`` round-trips (unclamped negative net flow, §4.1);
- a value past ``numeric(80, 0)`` (80 digits) round-trips exactly, up to
  900 * (2**256 - 1) as named in the task's "done when" (~1.04e80,
  ~81 digits) — the whole reason this isn't ``numeric(80, 0)`` like
  ``SafeNativeBalance.balance_wei``, per §4.1 edge case 10f: a spam
  token emitting near-2**256 transfers must not overflow and abort the
  rollup transaction;
- the unique constraint on (safe_address, token_address) is enforced.

The incremental rollup task that actually populates these tables is P2;
nothing here exercises it.
"""

from decimal import Decimal

from django.db import IntegrityError, transaction
from django.test import TestCase
from django.utils import timezone

from eth_account import Account

from safe_transaction_service.analytics.models import (
    Erc20BalanceWhale,
    SafeTokenBalance,
    TokenHolding,
)

# 900 * (2**256 - 1), as named in the P1 "done when" clause. ~1.04e80,
# one digit past `numeric(80, 0)`'s ceiling — the value that would
# overflow `SafeNativeBalance.balance_wei`'s type and, per spec §4.1
# edge case 10f, abort the whole rollup transaction if this table used
# the same bound.
HUGE_VALUE = 900 * (2**256 - 1)


class SafeTokenBalanceTestCase(TestCase):
    def test_negative_balance_round_trips(self):
        safe_address = Account.create().address
        token_address = Account.create().address

        SafeTokenBalance.objects.create(
            safe_address=safe_address,
            token_address=token_address,
            balance=Decimal(-42),
        )

        stored = SafeTokenBalance.objects.get(
            safe_address=safe_address, token_address=token_address
        )
        self.assertEqual(stored.balance, Decimal(-42))

    def test_huge_value_round_trips_exactly(self):
        safe_address = Account.create().address
        token_address = Account.create().address

        SafeTokenBalance.objects.create(
            safe_address=safe_address,
            token_address=token_address,
            balance=Decimal(HUGE_VALUE),
        )

        stored = SafeTokenBalance.objects.get(
            safe_address=safe_address, token_address=token_address
        )
        self.assertEqual(stored.balance, Decimal(HUGE_VALUE))
        # Sanity: this is genuinely past the 80-digit ceiling
        # `SafeNativeBalance.balance_wei` uses.
        self.assertGreater(len(str(HUGE_VALUE)), 80)

    def test_huge_negative_value_round_trips_exactly(self):
        safe_address = Account.create().address
        token_address = Account.create().address

        SafeTokenBalance.objects.create(
            safe_address=safe_address,
            token_address=token_address,
            balance=Decimal(-HUGE_VALUE),
        )

        stored = SafeTokenBalance.objects.get(
            safe_address=safe_address, token_address=token_address
        )
        self.assertEqual(stored.balance, Decimal(-HUGE_VALUE))

    def test_unique_safe_token_constraint_enforced(self):
        safe_address = Account.create().address
        token_address = Account.create().address

        SafeTokenBalance.objects.create(
            safe_address=safe_address,
            token_address=token_address,
            balance=Decimal(10),
        )

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                SafeTokenBalance.objects.create(
                    safe_address=safe_address,
                    token_address=token_address,
                    balance=Decimal(20),
                )

    def test_same_safe_different_token_is_allowed(self):
        safe_address = Account.create().address
        token_address_a = Account.create().address
        token_address_b = Account.create().address

        SafeTokenBalance.objects.create(
            safe_address=safe_address,
            token_address=token_address_a,
            balance=Decimal(1),
        )
        SafeTokenBalance.objects.create(
            safe_address=safe_address,
            token_address=token_address_b,
            balance=Decimal(2),
        )

        self.assertEqual(
            SafeTokenBalance.objects.filter(safe_address=safe_address).count(), 2
        )


class TokenHoldingTestCase(TestCase):
    def test_huge_total_balance_round_trips_exactly(self):
        token_address = Account.create().address
        now = timezone.now()

        TokenHolding.objects.create(
            token_address=token_address,
            holders=3,
            negative_pairs=1,
            total_balance=Decimal(HUGE_VALUE),
            as_of_block=157_266_731,
            as_of_timestamp=now,
            computed_at=now,
        )

        stored = TokenHolding.objects.get(token_address=token_address)
        self.assertEqual(stored.total_balance, Decimal(HUGE_VALUE))
        self.assertEqual(stored.holders, 3)
        self.assertEqual(stored.negative_pairs, 1)
        self.assertEqual(stored.as_of_block, 157_266_731)

    def test_token_address_is_primary_key(self):
        token_address = Account.create().address
        now = timezone.now()

        TokenHolding.objects.create(
            token_address=token_address,
            as_of_block=1,
            as_of_timestamp=now,
            computed_at=now,
        )

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                TokenHolding.objects.create(
                    token_address=token_address,
                    as_of_block=2,
                    as_of_timestamp=now,
                    computed_at=now,
                )


class Erc20BalanceWhaleTestCase(TestCase):
    def test_safe_address_round_trips_and_is_unique(self):
        safe_address = Account.create().address

        Erc20BalanceWhale.objects.create(safe_address=safe_address)

        self.assertTrue(
            Erc20BalanceWhale.objects.filter(safe_address=safe_address).exists()
        )

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                Erc20BalanceWhale.objects.create(safe_address=safe_address)
