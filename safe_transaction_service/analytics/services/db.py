"""Database helpers for analytics compute tasks.

`config/settings/base.py` runs every connection with
`statement_timeout = DB_STATEMENT_TIMEOUT` (50 s by default) as a safety belt
for the request path. Analytics computes legitimately need minutes — wrap them
with `relaxed_statement_timeout(...)` to lift the cap only inside the compute
body. `approx_count_or_exact(...)` swaps a multi-second exact `COUNT(*)` over a
tens-of-millions-of-rows table for an O(1) read from `pg_class.reltuples`,
falling back to exact for tiny tables / fresh fixtures so unit tests don't
depend on ANALYZE having been run.
"""

import logging
from contextlib import contextmanager

from django.conf import settings
from django.db import DatabaseError, ProgrammingError, connection

logger = logging.getLogger(__name__)


@contextmanager
def relaxed_statement_timeout(ms: int = 1_800_000):
    """Set PG `statement_timeout` to `ms` for the duration of the block.

    Default 30 minutes — high enough to absorb the heaviest analytics
    aggregates on a multi-million-Safe chain doing month-old backfills
    (the `_DAILY_ACTIVE_SAFES_SQL` 4-leg UNION + EXISTS lookup against
    `history_safecontract` legitimately runs 5–10 min per day on BASE
    once we reach data older than ~30 days; the old 10 min default
    timed out the Feb/March slice of a 90-day backfill on BASE dev).
    Still bounded so a stuck statement gets killed eventually instead
    of pinning a worker forever.

    On exit we restore the value explicitly to `settings.DB_STATEMENT_TIMEOUT`
    rather than `SET ... = DEFAULT`. Django applies the configured value via
    the connection startup packet, but `SET DEFAULT` resets to PG's compiled
    default (no timeout), not to the startup-packet value — so an analytics
    task running on a pooled connection would otherwise leave the request
    path's safety belt removed for the next caller.

    If the body throws via a gevent `Timeout` mid-query, the underlying
    psycopg connection is left in `ACTIVE` state (the protocol never saw
    the query finish or error). The reset `SET statement_timeout` then
    raises `OperationalError: another command is already in progress`,
    which without care would propagate out of `finally` and mask the
    original exception. Treat that case by dropping the connection so
    the pool hands out a fresh one (with the startup-packet timeout
    intact) on the next checkout, and let the original exception
    propagate.
    """
    default_ms = getattr(settings, "DB_STATEMENT_TIMEOUT", 50_000)
    with connection.cursor() as cursor:
        cursor.execute(f"SET statement_timeout = {int(ms)}")
    try:
        yield
    finally:
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SET statement_timeout = {int(default_ms)}")
        except DatabaseError as exc:
            # Catch DatabaseError (parent of OperationalError, InternalError,
            # …) so an aborted-transaction state from a body-side failure
            # cannot raise on the reset SET and mask the original exception.
            logger.warning(
                "relaxed_statement_timeout: failed to reset statement_timeout "
                "(%s); closing connection to guarantee clean state",
                exc,
            )
            try:
                connection.close()
            except Exception:
                logger.exception(
                    "relaxed_statement_timeout: failed to close busy connection"
                )


def approx_count_or_exact(model, table_name: str, threshold: int = 1000) -> int:
    """Return `pg_class.reltuples` for `table_name`, or an exact `COUNT(*)`
    when the estimate is below `threshold`.

    `reltuples` is updated by VACUUM / ANALYZE, so for fleet-level summary
    metrics where exact precision is unnecessary it's a constant-time
    substitute for `COUNT(*)` on tables with tens of millions of rows.

    The threshold fallback exists so unit tests against a freshly created
    fixture (no ANALYZE has run, reltuples is 0) still get correct numbers
    — and the cost of `COUNT(*)` on a tiny table is negligible anyway.
    """
    # `oid = %s::regclass` resolves the name through PG's search_path and is
    # unique per table — filtering by `relname` alone can collide across
    # schemas (multi-tenant partitions, test schemas, …) and return the
    # wrong row. `regclass` raises if the table doesn't exist, so fall back
    # to the exact count for not-yet-created tables (e.g. mid-migration).
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT reltuples::bigint FROM pg_class WHERE oid = %s::regclass",
                [table_name],
            )
            row = cursor.fetchone()
    except ProgrammingError:
        return model.objects.count()
    approx = int(row[0]) if row and row[0] is not None else 0
    if approx < threshold:
        return model.objects.count()
    return approx
