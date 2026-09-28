"""Is the indexer caught up enough to start or resume a stage? Reuses
``analytics/catchup/gate.py``'s settle check rather than inventing a
second notion of "caught up".

Asks about **yesterday**, the most recent UTC day that could ever be
complete -- a new network still mid-sync fails this immediately, which
is what keeps the bootstrap from freezing incomplete history under a
watermark.

Split in two so a read-only caller (`--status`) can get the verdict
without inheriting a Redis write: `check_indexer()` is pure;
`indexer_caught_up()` adds `_log_on_change()`.
"""

import logging
from datetime import timedelta

from django.utils import timezone

from safe_transaction_service.utils.redis import get_redis

from ..catchup.gate import DayNotReady, ensure_day_settled, get_indexer_status

logger = logging.getLogger(__name__)

#: Redis key remembering the gate's last verdict, so "not caught up" logs
#: at INFO once per state change rather than every 5-minute tick. Not a
#: real watermark -- transient, no TTL needed beyond "until the state
#: flips back".
_LAST_STATE_KEY = "analytics_bootstrap:indexer_gate_last_state"


#: Code used when `get_indexer_status()` (or `ensure_day_settled`) raises
#: something other than `DayNotReady` -- an RPC/DB error the gate itself
#: doesn't already turn into a stable literal (unlike `DayNotReady.code`,
#: see `catchup/gate.py`'s own `INDEXER_STATUS_UNAVAILABLE` handling).
#: Kept distinct from that literal so a log line can tell "the gate raised
#: cleanly" from "something below it blew up" -- both mean "not caught
#: up" to the tick either way.
_STATUS_UNAVAILABLE_CODE = "status_unavailable"


def check_indexer() -> tuple[bool, str | None]:
    """``(True, None)`` when the indexer has settled past yesterday and
    processing isn't stuck; ``(False, code)`` on a ``DayNotReady``, or
    ``(False, "status_unavailable")`` on any other exception -- an
    outage never escapes as a raised exception. Pure: no Redis I/O; see
    ``indexer_caught_up()`` for the logging wrapper.
    """
    try:
        status = get_indexer_status()
        yesterday = timezone.now().date() - timedelta(days=1)
        ensure_day_settled(yesterday, status)
        return True, None
    except DayNotReady as exc:
        return False, exc.code
    except Exception:
        logger.debug(
            "analytics.bootstrap.gate: unexpected error checking the indexer gate",
            exc_info=True,
        )
        return False, _STATUS_UNAVAILABLE_CODE


def last_indexer_gate_state() -> str | None:
    """The verdict ``_log_on_change`` last recorded, read straight from
    Redis -- ``None`` if never written. Callers needing a live verdict
    call ``check_indexer()`` instead."""
    raw = get_redis().get(_LAST_STATE_KEY)
    return raw.decode() if isinstance(raw, bytes) else raw


def indexer_caught_up() -> bool:
    """``check_indexer()`` plus log-on-change: logs at INFO/WARNING only
    when the verdict changes, via a Redis-remembered last state."""
    caught_up, code = check_indexer()
    _log_on_change(caught_up, code)
    return caught_up


def _log_on_change(caught_up: bool, code: str | None) -> None:
    redis = get_redis()
    state = "caught_up" if caught_up else f"not_caught_up:{code}"
    previous = redis.get(_LAST_STATE_KEY)
    previous = previous.decode() if isinstance(previous, bytes) else previous
    if previous == state:
        return
    redis.set(_LAST_STATE_KEY, state)
    if caught_up:
        logger.info("analytics.bootstrap.gate: indexer caught up")
    elif code == _STATUS_UNAVAILABLE_CODE:
        logger.warning(
            "analytics.bootstrap.gate: indexer status unavailable (%s)", code
        )
    else:
        logger.info("analytics.bootstrap.gate: indexer not caught up (%s)", code)
