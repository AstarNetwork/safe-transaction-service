"""Shared lock-liveness check: releases immediately -- neither stage
does real work here, so holding it would only delay the real task."""

from safe_transaction_service.utils.redis import get_redis
from safe_transaction_service.utils.tasks import LOCK_TIMEOUT


def lock_is_free(lock_name: str) -> bool:
    """Whether the lock was free at the moment of the check."""
    lock = get_redis().lock(lock_name, blocking=False, timeout=LOCK_TIMEOUT)
    if not lock.acquire(blocking=False):
        return False
    lock.release()
    return True
