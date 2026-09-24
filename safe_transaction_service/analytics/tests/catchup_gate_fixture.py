"""Shared indexer-status gate fixture for tests that exercise a caller now
routed through the catch-up gate (`compute_daily_metrics_task`, a backfill
shard, `--inline` backfill) without themselves testing the gate.

Not a `test_*` module -- pytest won't collect it -- same convention as
`history/tests/factories.py`. See "Analytics catch-up" in
`analytics/implementation-notes.md`.
"""

from datetime import UTC, datetime
from unittest.mock import patch

from safe_transaction_service.analytics.catchup.gate import IndexerStatus

#: Far enough in the future that `ensure_day_settled` accepts any day one
#: of these tests computes, regardless of which UTC date the test itself
#: freezes `timezone.now()` at -- these tests aren't exercising day-boundary
#: behaviour, that's `test_catchup_gate.py` / `test_catchup_day.py`'s job.
_SETTLED_POSITION = datetime(2100, 1, 1, tzinfo=UTC)

SETTLED_INDEXER_STATUS = IndexerStatus(
    erc20_block_number=1,
    erc20_block_timestamp=_SETTLED_POSITION,
    master_copies_block_number=1,
    master_copies_block_timestamp=_SETTLED_POSITION,
    oldest_unprocessed_ts=None,
    oldest_unprocessed_safe=None,
    relevant_master_copies=1,
)

#: Every module-level name a T5 caller binds `get_indexer_status` to --
#: `unittest.mock.patch` targets the name where it's looked up, not where
#: it's defined, so each caller needs its own entry here.
GATE_PATCH_TARGETS = (
    "safe_transaction_service.analytics.tasks.get_indexer_status",
    "safe_transaction_service.analytics.tasks_shards.get_indexer_status",
    "safe_transaction_service.analytics.management.commands."
    "backfill_daily_metrics.get_indexer_status",
    "safe_transaction_service.analytics.catchup.sweeper.get_indexer_status",
)


class SettledGateMixin:
    """Patches every caller's `get_indexer_status()` to return a status
    that clears the gate unconditionally, for the duration of the test --
    mix in wherever a test drives `compute_daily_metrics_task`, a backfill
    shard, or `--inline` backfill and cares about the day actually being
    computed rather than deferred.
    """

    def setUp(self):
        super().setUp()
        for target in GATE_PATCH_TARGETS:
            patcher = patch(target, return_value=SETTLED_INDEXER_STATUS)
            patcher.start()
            self.addCleanup(patcher.stop)
