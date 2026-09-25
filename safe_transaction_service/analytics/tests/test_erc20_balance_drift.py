"""Tests for the weekly ERC-20 balance rollup drift check (token-holdings
spec §4.4, `docs/specs/token-holdings.md` in the workspace root).

Mirrors `TestDriftCheck` in `test_native_balance_rollup.py`'s shape and
conventions (a corrupted row is reported with its magnitude, the check
never writes, whales/orphans/negative pairs are data-quality signals, not
repairs). Scaffolding is copied from `Erc20BalanceRollupTestCase` in
`test_erc20_balance_rollup.py` rather than imported cross-file — there is
no precedent in this test suite for sharing a `TestCase` base across
files, and the drift check's own tests need only a slice of it (no
seed-cap / mid-run-insert machinery).

Two departures from the native drift check get their own tests instead of
being folded into the generic ones: the recompute is per-*Safe* (every
pair `erc20_balances_upto_block` finds for a sampled Safe, not only the
sampled row), so a pair present in the recompute but missing from
`SafeTokenBalance` entirely is caught too; and whale Safes
(`Erc20BalanceWhale`) are excluded from the sampling pool itself.
"""

from datetime import timedelta
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from eth_account import Account

from safe_transaction_service.analytics.models import (
    AnalyticsWatermark,
    Erc20BalanceWhale,
    SafeTokenBalance,
)
from safe_transaction_service.analytics.tasks import (
    _MAX_ADDRESS,
    ERC20_BALANCE_SAFES_WATERMARK,
    ERC20_BALANCE_WATERMARK,
    check_erc20_balance_drift,
    check_erc20_balance_drift_task,
    run_erc20_balance_rollup,
)
from safe_transaction_service.history.models import (
    IndexingStatus,
    IndexingStatusType,
    SafeContract,
)
from safe_transaction_service.history.tests.factories import (
    ERC20TransferFactory,
    EthereumBlockFactory,
    EthereumTxFactory,
    SafeContractFactory,
)

# Well clear of the other analytics test files' own `BASE_BLOCK`s
# (irrelevant across test processes, but keeps this file trivially
# distinguishable in logs/tracebacks).
BASE_BLOCK = 5_500_000


class Erc20BalanceDriftTestCase(TestCase):
    """Shared scaffolding — the same fake-clock shape
    `Erc20BalanceRollupTestCase` uses, trimmed to what the drift tests
    need: build a real, rollup-produced `SafeTokenBalance` row (rather
    than hand-inserting one that never went through the seed/delta path),
    then let each test corrupt or delete it.
    """

    CLOCK_STEP = timedelta(minutes=25)

    def setUp(self):
        super().setUp()
        self.next_block = BASE_BLOCK
        self.clock = timezone.now()
        patcher = patch(
            "safe_transaction_service.analytics.tasks.timezone.now",
            side_effect=lambda: self.clock,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def advance_clock(self, delta: timedelta = timedelta(hours=2)):
        self.clock += delta

    def block(self, confirmed: bool = True):
        self.next_block += 1
        self.advance_clock(self.CLOCK_STEP)
        block = EthereumBlockFactory(number=self.next_block, confirmed=confirmed)
        IndexingStatus.objects.filter(
            indexing_type=IndexingStatusType.ERC20_721_EVENTS.value
        ).update(block_number=self.next_block)
        return block

    def safe(self, block=None):
        return SafeContractFactory(
            ethereum_tx=EthereumTxFactory(block=block or self.block())
        )

    def token(self):
        return Account.create().address

    def transfer(self, block, token, value: int, to=None, _from=None):
        kwargs = {}
        if to is not None:
            kwargs["to"] = to
        if _from is not None:
            kwargs["_from"] = _from
        return ERC20TransferFactory(
            ethereum_tx=EthereumTxFactory(block=block),
            address=token,
            value=value,
            **kwargs,
        )

    def advance_head(self, blocks: int = 3):
        for _ in range(blocks):
            self.block(confirmed=True)

    def initialise(self, at_block: int = 0):
        """Stand in for `manage.py backfill_erc20_balances` on an empty
        chain — same fiction `Erc20BalanceRollupTestCase.initialise` uses:
        both watermarks set, the Safe marker at *now* so every Safe that
        already exists counts as already seeded."""
        AnalyticsWatermark.objects.create(
            name=ERC20_BALANCE_WATERMARK,
            block_number=at_block,
            computed_at=self.clock,
        )
        AnalyticsWatermark.objects.create(
            name=ERC20_BALANCE_SAFES_WATERMARK,
            block_number=at_block,
            computed_at=self.clock,
            address=_MAX_ADDRESS,
        )

    def balance(self, address, token) -> Decimal | None:
        row = SafeTokenBalance.objects.filter(
            safe_address=address, token_address=token
        ).first()
        return row.balance if row else None


class TestDriftCheck(Erc20BalanceDriftTestCase):
    """The rollup has no self-healing property — a batch applied twice,
    or a pair the seed/delta step silently never created, stays wrong
    forever and looks like a real number. This check is the only thing
    that would ever notice."""

    def test_clean_rollup_reports_no_drift(self):
        token = self.token()
        self.initialise()
        for _ in range(3):
            safe = self.safe()
            self.transfer(self.block(), token, 600, to=safe.address)
        self.advance_head()
        run_erc20_balance_rollup()

        summary = check_erc20_balance_drift()

        self.assertEqual(summary["sampled"], 3)
        self.assertEqual(summary["mismatched"], 0)
        self.assertEqual(summary["total_abs_diff"], 0)

    def test_corrupted_row_is_reported_and_never_modified(self):
        safe = self.safe()
        token = self.token()
        self.initialise()
        self.transfer(self.block(), token, 600, to=safe.address)
        self.advance_head()
        run_erc20_balance_rollup()
        self.assertEqual(self.balance(safe.address, token), Decimal(600))

        # Exactly the shape a double-applied delta leaves behind.
        tampered = Decimal(1_200)
        SafeTokenBalance.objects.filter(
            safe_address=safe.address, token_address=token
        ).update(balance=tampered)

        with self.assertLogs(
            "safe_transaction_service.analytics.tasks", level="WARNING"
        ) as logs:
            summary = check_erc20_balance_drift()

        self.assertEqual(summary["mismatched"], 1)
        self.assertEqual(summary["total_abs_diff"], 600)
        self.assertEqual(summary["max_abs_diff"], 600)
        self.assertIn("--restart", logs.output[0])

        # Report-only: the corrupted row is exactly as this test left it.
        self.assertEqual(self.balance(safe.address, token), tampered)

    def test_missing_pair_row_is_caught_via_per_safe_recompute(self):
        """The reason the recompute is per-*Safe*, not per sampled pair:
        a row the rollup should have but doesn't has nothing to land in
        the sample, so sampling stored pairs alone could never see it.
        Recomputing everything the sampled Safe holds, and comparing that
        against every row this check can see for it (not only the
        sampled one), is what catches it."""
        safe = self.safe()
        token_a, token_b = self.token(), self.token()
        self.initialise()
        block = self.block()
        self.transfer(block, token_a, 500, to=safe.address)
        self.transfer(block, token_b, 300, to=safe.address)
        self.advance_head()
        run_erc20_balance_rollup()
        self.assertEqual(self.balance(safe.address, token_a), Decimal(500))
        self.assertEqual(self.balance(safe.address, token_b), Decimal(300))

        # Simulate the exact bug this strategy exists to catch: transfers
        # moved, but the rollup has no row for one of the pairs.
        SafeTokenBalance.objects.filter(
            safe_address=safe.address, token_address=token_b
        ).delete()

        with self.assertLogs(
            "safe_transaction_service.analytics.tasks", level="WARNING"
        ) as logs:
            summary = check_erc20_balance_drift()

        # Only token_a's row exists to be drawn into the sample...
        self.assertEqual(summary["sampled"], 1)
        # ...but the per-Safe recompute still surfaces both pairs.
        self.assertEqual(summary["compared"], 2)
        self.assertEqual(summary["mismatched"], 1)
        self.assertEqual(summary["total_abs_diff"], 300)
        self.assertIn("--restart", logs.output[0])
        # Still report-only: the one row that does exist is untouched, and
        # nothing was inserted for the missing pair.
        self.assertEqual(self.balance(safe.address, token_a), Decimal(500))
        self.assertIsNone(self.balance(safe.address, token_b))

    def test_whale_safes_are_never_sampled(self):
        whale, regular = self.safe(), self.safe()
        token = self.token()
        self.initialise()
        block = self.block()
        self.transfer(block, token, 10_000, to=whale.address)
        self.transfer(block, token, 10, to=regular.address)
        self.advance_head()
        run_erc20_balance_rollup()
        Erc20BalanceWhale.objects.create(safe_address=whale.address)

        # Tamper the whale's row so it would unmistakably show up as a
        # mismatch if the sample ever touched it.
        SafeTokenBalance.objects.filter(safe_address=whale.address).update(
            balance=Decimal(999_999)
        )

        # sample_size comfortably covers the whole (2-row) pool, so if the
        # whale were eligible it would certainly be drawn.
        summary = check_erc20_balance_drift(sample_size=10)

        self.assertEqual(summary["sampled"], 1)
        self.assertEqual(summary["compared"], 1)
        self.assertEqual(summary["mismatched"], 0)
        # The whale's corrupted row is left exactly as this test made it —
        # excluded from sampling, not "checked and found fine".
        self.assertEqual(self.balance(whale.address, token), Decimal(999_999))

    def test_negative_pairs_are_logged(self):
        safe = self.safe()
        token = self.token()
        self.initialise()
        # A negative pair — incomplete upstream indexing, not produced via
        # the rollup's own delta path here, to isolate the data-quality
        # signal from drift itself (§8 edge case 2b/5).
        SafeTokenBalance.objects.create(
            safe_address=safe.address, token_address=token, balance=Decimal(-42)
        )

        with self.assertLogs(
            "safe_transaction_service.analytics.tasks", level="INFO"
        ) as logs:
            summary = check_erc20_balance_drift()

        self.assertEqual(summary["negative_pairs"], 1)
        self.assertTrue(any("negative" in line for line in logs.output))

    def test_orphan_rows_are_reported(self):
        """A reorg that removes a Safe creation leaves the rollup row
        behind, still contributing its balance."""
        safe = self.safe()
        token = self.token()
        self.initialise()
        self.transfer(self.block(), token, 600, to=safe.address)
        self.advance_head()
        run_erc20_balance_rollup()

        SafeContract.objects.filter(address=safe.address).delete()

        with self.assertLogs(
            "safe_transaction_service.analytics.tasks", level="WARNING"
        ) as logs:
            summary = check_erc20_balance_drift()

        self.assertEqual(summary["orphan_rows"], 1)
        self.assertEqual(summary["mismatched"], 0)
        self.assertIn("no Safe in history_safecontract", logs.output[0])

    def test_uninitialised_rollup_is_skipped_quietly(self):
        self.safe()
        self.advance_head()
        self.assertIsNone(check_erc20_balance_drift())

    def test_empty_rollup_is_skipped_quietly(self):
        self.initialise()
        self.assertIsNone(check_erc20_balance_drift())

    def test_task_runs_the_check(self):
        safe = self.safe()
        token = self.token()
        self.initialise()
        self.transfer(self.block(), token, 600, to=safe.address)
        self.advance_head()
        run_erc20_balance_rollup()

        self.assertEqual(check_erc20_balance_drift_task.delay().get()["mismatched"], 0)
