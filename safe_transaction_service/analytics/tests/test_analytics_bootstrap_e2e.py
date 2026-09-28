"""End-to-end test for the analytics bootstrap (`analytics/bootstrap/`):
on an empty instance with a little real chain data, consecutive
`run_tick()` calls through the REAL stage registry
(`build_default_stages()` -- no spies, no `stages=` override) must run
daily, then native, then ERC-20, in that fixed order; leave all three
`AnalyticsBootstrapStage` rows completed and `bootstrap_complete()` True;
make a further tick a pure no-op; and surface `bootstrap.complete: True`
on `/summary/`.

Order is proven two ways: a spy around
`bookkeeping.mark_completed` records the sequence it's called in (it can
only ever be daily, then native, then erc20 -- the tick's own order
enforcement, `tick.py` step 3, makes any other order impossible), and the
three rows' own `completed_at` timestamps are asserted non-decreasing in
the same order, in case a stage happens to finish inside the same tick
that dispatched it (eager Celery mode) -- `mark_completed` for that stage
then only lands on a *later* tick's `pick_current_stage` pass.

Fixtures mirror `AnalyticsBootstrapTestCase` (`test_analytics_bootstrap.py`
-- `block()`/`safe()`/`transfer()`/`advance_head()`, updating
`IndexingStatus(ERC20_721_EVENTS)` on every block so
`erc20_balance_head_block()` has a real head) plus one `InternalTxFactory`
native transfer (`test_analytics_bootstrap_native.py`'s shape) so
`NativeStage.start_or_resume()` seeds a real balance, not just an empty
fleet -- duplicated rather than imported, the same reason both of those
files' own docstrings give: no precedent in this suite for sharing a
`TestCase` base across modules. The daily window is shrunk to 2 days
(`ANALYTICS_BOOTSTRAP_DAILY_DAYS=2`) purely to keep the daily stage's
dispatch small; it reaches `is_done()` the same way on an all-zero window
as `test_analytics_bootstrap_daily.py`'s `TestZeroActivityDaysStillReachDone`
already proves, real chain data or none.
"""

from unittest.mock import patch

from django.contrib.auth.models import User
from django.test import override_settings
from django.urls import reverse

from eth_account import Account
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase

from safe_transaction_service.analytics.bootstrap import bookkeeping, run_tick
from safe_transaction_service.analytics.bootstrap.bookkeeping import (
    bootstrap_complete,
    get_stage_row,
)
from safe_transaction_service.analytics.bootstrap.daily import DailyStage
from safe_transaction_service.analytics.bootstrap.erc20 import Erc20Stage
from safe_transaction_service.analytics.bootstrap.native import NativeStage
from safe_transaction_service.history.models import (
    EthereumTxCallType,
    IndexingStatus,
    IndexingStatusType,
)
from safe_transaction_service.history.tests.factories import (
    ERC20TransferFactory,
    EthereumBlockFactory,
    EthereumTxFactory,
    InternalTxFactory,
    SafeContractFactory,
)
from safe_transaction_service.utils.redis import get_redis

from .catchup_gate_fixture import SettledGateMixin

# Well clear of every other bootstrap test module's own BASE_BLOCK (each
# picks its own range -- see those modules' own comments for the pattern).
BASE_BLOCK = 9_500_000


@override_settings(ANALYTICS_BOOTSTRAP_DAILY_DAYS=2)
class AnalyticsBootstrapEndToEndTestCase(SettledGateMixin, APITestCase):
    def setUp(self):
        super().setUp()
        # Redis isn't rolled back between tests the way the database is --
        # same isolation `AnalyticsTestMixin` (`test_views_v2.py`) and the
        # per-stage bootstrap test modules give themselves.
        get_redis().flushall()
        self.next_block = BASE_BLOCK
        self.user, _ = User.objects.get_or_create(username="bootstrap-e2e")
        self.token, _ = Token.objects.get_or_create(user=self.user)
        self.auth_header = {"HTTP_AUTHORIZATION": "Token " + self.token.key}

    def _block(self, confirmed: bool = True):
        self.next_block += 1
        block = EthereumBlockFactory(number=self.next_block, confirmed=confirmed)
        IndexingStatus.objects.filter(
            indexing_type=IndexingStatusType.ERC20_721_EVENTS.value
        ).update(block_number=self.next_block)
        return block

    def _advance_head(self, blocks: int = 3):
        for _ in range(blocks):
            self._block(confirmed=True)

    def _seed_chain_data(self):
        """One Safe with a native transfer and an ERC-20 transfer -- just
        enough real data for the native and ERC-20 stages to have
        something to seed, plus enough confirmed blocks past
        `ETH_REORG_BLOCKS` (1, in test settings) for both stages' head
        queries to resolve."""
        safe = SafeContractFactory(ethereum_tx=EthereumTxFactory(block=self._block()))
        token_address = Account.create().address
        block = self._block()
        InternalTxFactory(
            ethereum_tx=EthereumTxFactory(block=block),
            value=100,
            call_type=EthereumTxCallType.CALL.value,
            error=None,
            to=safe.address,
        )
        ERC20TransferFactory(
            ethereum_tx=EthereumTxFactory(block=block),
            address=token_address,
            value=100,
            to=safe.address,
        )
        self._advance_head()

    def test_daily_then_native_then_erc20_in_order_then_complete_and_on_summary(self):
        self._seed_chain_data()
        depth = 2

        completion_order = []
        real_mark_completed = bookkeeping.mark_completed

        def _spy_mark_completed(name, depth=None):
            completion_order.append(name)
            return real_mark_completed(name, depth=depth)

        with patch.object(
            bookkeeping, "mark_completed", side_effect=_spy_mark_completed
        ):
            for _ in range(12):
                if bootstrap_complete(depth):
                    break
                run_tick()
            else:
                self.fail("bootstrap did not reach completion within 12 ticks")

        # 1. Order: daily, then native, then ERC-20 -- the only order the
        # tick's own step 3 (`tick.py`) permits, since a later stage is
        # never even looked at while an earlier one is non-terminal.
        self.assertEqual(completion_order, ["daily", "native", "erc20"])

        # 2. Same proof from the durable rows themselves, in case a stage
        # happened to finish inside the very tick that dispatched it
        # (eager mode) -- `completed_at` is still stamped no earlier than
        # an already-completed earlier stage's own timestamp.
        daily_row = get_stage_row("daily")
        native_row = get_stage_row("native")
        erc20_row = get_stage_row("erc20")
        self.assertIsNotNone(daily_row.completed_at)
        self.assertIsNotNone(native_row.completed_at)
        self.assertIsNotNone(erc20_row.completed_at)
        self.assertLessEqual(daily_row.completed_at, native_row.completed_at)
        self.assertLessEqual(native_row.completed_at, erc20_row.completed_at)

        # 3. Everything is done: the derived complete check, and each
        # stage's own live `is_done()`.
        self.assertTrue(bootstrap_complete(depth))
        self.assertTrue(DailyStage().is_done())
        self.assertTrue(NativeStage().is_done())
        self.assertTrue(Erc20Stage().is_done())

        # 4. One further tick is a pure no-op: none of the three real
        # stages' `start_or_resume()` is ever called again.
        with (
            patch.object(DailyStage, "start_or_resume") as daily_spy,
            patch.object(NativeStage, "start_or_resume") as native_spy,
            patch.object(Erc20Stage, "start_or_resume") as erc20_spy,
        ):
            run_tick()
        daily_spy.assert_not_called()
        native_spy.assert_not_called()
        erc20_spy.assert_not_called()

        # 5. `/summary/`'s additive `bootstrap` object reports completion
        # too (`AnalyticsService._get_bootstrap_report()`).
        with patch(
            "safe_transaction_service.utils.ethereum.get_chain_id",
            return_value=84532,
        ):
            response = self.client.get(
                reverse("v2:analytics:analytics-summary"), **self.auth_header
            )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        bootstrap = response.data["bootstrap"]
        self.assertIsNotNone(bootstrap)
        self.assertTrue(bootstrap["complete"])
        for name in ("daily", "native", "erc20"):
            self.assertEqual(bootstrap["stages"][name]["state"], "done")
