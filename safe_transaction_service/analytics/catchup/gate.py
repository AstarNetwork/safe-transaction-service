"""Indexer-status gate: is a day settled enough to compute yet.

See "Analytics catch-up" in ``analytics/implementation-notes.md``.

``get_indexer_status()`` takes one read-only snapshot of where the indexers
and internal-tx processing stand; ``ensure_day_settled()`` is a pure
function that decides, from that snapshot alone, whether a given UTC day is
safe to compute. Callers fetch the snapshot once per run and pass it to
every day they check, so all of a run's decisions are made against the same
position.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

from safe_transaction_service.history.models import InternalTxDecoded, SafeMasterCopy
from safe_transaction_service.history.services.index_service import (
    IndexServiceProvider,
)

from ..conf import get_catchup_settings

logger = logging.getLogger(__name__)

#: Fixed set of reasons a day can fail the gate, or the status snapshot
#: itself can't be obtained. Alerting is keyed on these literals; keep them
#: stable.
DAY_NOT_READY = "day_not_ready"
PROCESSING_PENDING = "processing_pending"
INDEXER_STATUS_UNAVAILABLE = "indexer_status_unavailable"


class DayNotReady(Exception):
    """A day isn't settled yet, or the indexer status couldn't be read.

    ``code`` is always one of ``DAY_NOT_READY``, ``PROCESSING_PENDING`` or
    ``INDEXER_STATUS_UNAVAILABLE`` -- never free text, so it's safe to log
    and to alert on.
    """

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class IndexerStatus:
    """A single read-only snapshot of indexer position and processing lag."""

    erc20_block_number: int
    erc20_block_timestamp: datetime
    master_copies_block_number: int
    master_copies_block_timestamp: datetime
    #: Timestamp of the oldest ``InternalTxDecoded(processed=False)`` row, or
    #: ``None`` if processing is fully caught up.
    oldest_unprocessed_ts: datetime | None
    #: Safe address of that same oldest row, or ``None``.
    oldest_unprocessed_safe: str | None
    #: How many master copies are relevant on this network (0 means the
    #: upstream indexing-status call can't be trusted: it falls back to
    #: reporting the chain head as "synced").
    relevant_master_copies: int

    @property
    def position_ts(self) -> datetime:
        """The indexers' combined position: the slower of the two pipelines."""
        return min(self.erc20_block_timestamp, self.master_copies_block_timestamp)


def get_indexer_status() -> IndexerStatus:
    """Fetch one read-only snapshot of indexer position and processing lag.

    Makes exactly one query against ``InternalTxDecoded`` (the oldest
    unprocessed row and its Safe address, in a single ``ORDER BY ... LIMIT
    1``) plus the existing upstream indexing-status call.

    Any failure -- no relevant master copies, no indexing-status row, or an
    RPC/DB error while asking the node -- becomes
    ``DayNotReady(INDEXER_STATUS_UNAVAILABLE)`` raised ``from None``. That
    keeps an RPC error's text (node URL, API key) out of the exception a
    caller might log, and out of anything derived from it (task manifests,
    command output).
    """
    try:
        relevant_master_copies = SafeMasterCopy.objects.relevant().count()
        if not relevant_master_copies:
            raise DayNotReady(INDEXER_STATUS_UNAVAILABLE)

        all_status = IndexServiceProvider().get_indexing_status()

        oldest_unprocessed = (
            InternalTxDecoded.objects.not_processed()
            .order_by("internal_tx__timestamp")
            .values("internal_tx__timestamp", "safe_address")
            .first()
        )
    except Exception:
        raise DayNotReady(INDEXER_STATUS_UNAVAILABLE) from None

    return IndexerStatus(
        erc20_block_number=all_status.erc20_block_number,
        erc20_block_timestamp=datetime.fromtimestamp(
            all_status.erc20_block_timestamp, tz=UTC
        ),
        master_copies_block_number=all_status.master_copies_block_number,
        master_copies_block_timestamp=datetime.fromtimestamp(
            all_status.master_copies_block_timestamp, tz=UTC
        ),
        oldest_unprocessed_ts=(
            oldest_unprocessed["internal_tx__timestamp"] if oldest_unprocessed else None
        ),
        oldest_unprocessed_safe=(
            oldest_unprocessed["safe_address"] if oldest_unprocessed else None
        ),
        relevant_master_copies=relevant_master_copies,
    )


def ensure_day_settled(day: date, status: IndexerStatus) -> None:
    """Raise ``DayNotReady`` unless ``day`` (a UTC calendar day) is settled.

    A day is settled when both hold, against the already-fetched ``status``:

    1. indexed: the indexers' combined position is at or past the end of
       the day plus ``SETTLE_MINUTES`` of slack;
    2. processed: no ``InternalTxDecoded(processed=False)`` row has an
       internal-tx timestamp before the end of the day -- an unprocessed
       row dated *after* the day doesn't hold it up.

    Pure function of its arguments: no I/O, no clock read. A caller that
    checks several days in one run fetches ``status`` once and reuses it,
    so every day in that run is judged against the same position.
    """
    day_end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=UTC)

    settle_minutes = get_catchup_settings().SETTLE_MINUTES
    if status.position_ts < day_end + timedelta(minutes=settle_minutes):
        raise DayNotReady(DAY_NOT_READY)

    if (
        status.oldest_unprocessed_ts is not None
        and status.oldest_unprocessed_ts < day_end
    ):
        raise DayNotReady(PROCESSING_PENDING)
