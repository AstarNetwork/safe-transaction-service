"""Read-only diagnostics for the analytics catch-up gate and sweeper state.

See "Analytics catch-up" in ``analytics/implementation-notes.md``. Every
value printed here comes from a plain read: this command makes no writes to
the database. Unlike the sweeper's own logs, which only ever report one of
three fixed gate codes, this command is allowed to look further when the
indexer status itself is unavailable -- see ``_diagnose_unavailable`` below
-- because an operator staring at broken analytics on one network needs more
than "indexer_status_unavailable" to know where to look. It never prints the
node URL or a raw RPC error string: an unexpected RPC/DB failure is reported
only by its exception class name.
"""

from datetime import date, timedelta

from django.core.exceptions import ImproperlyConfigured
from django.core.management.base import BaseCommand
from django.utils import timezone

from safe_transaction_service.analytics.catchup import (
    DayNotReady,
    ensure_day_settled,
    get_indexer_status,
)
from safe_transaction_service.analytics.conf import get_catchup_settings
from safe_transaction_service.analytics.models import AnalyticsCatchupState, DailyMetric
from safe_transaction_service.history.models import (
    IndexingStatus,
    IndexingStatusType,
    InternalTxDecoded,
    SafeMasterCopy,
)
from safe_transaction_service.history.services.index_service import (
    IndexServiceProvider,
)

#: `AnalyticsCatchupState.kind` / `.key` values this command reads. Not
#: re-exported from the `catchup` package (they're private to
#: `catchup/state.py`), so duplicated here as literals -- read-only lookups,
#: no behaviour depends on getting them wrong beyond a blank report.
_DAY_KIND = "day"
_PROCESSING_KIND = "processing"
_PROCESSING_KEY = "oldest"


def _diagnose_unavailable() -> str:
    """A human-readable reason ``get_indexer_status()`` couldn't produce a
    snapshot -- for this command's output only. ``get_indexer_status()``
    itself always collapses every failure to the one fixed
    ``indexer_status_unavailable`` code; this re-runs the same read-only
    checks, in the same order, so the detail matches whichever one actually
    failed: no relevant master copies, no ``IndexingStatus`` row, or an
    RPC/DB error (reported by exception class name only, never its text).
    """
    try:
        if not SafeMasterCopy.objects.relevant().exists():
            return "no relevant master copies"
    except Exception as exc:
        return f"RPC/DB error: {type(exc).__name__}"

    try:
        has_indexing_status = IndexingStatus.objects.filter(
            indexing_type=IndexingStatusType.ERC20_721_EVENTS.value
        ).exists()
    except Exception as exc:
        return f"RPC/DB error: {type(exc).__name__}"
    if not has_indexing_status:
        return "no IndexingStatus row"

    try:
        IndexServiceProvider().get_indexing_status()
        InternalTxDecoded.objects.not_processed().order_by(
            "internal_tx__timestamp"
        ).values("internal_tx__timestamp", "safe_address").first()
    except Exception as exc:
        return f"RPC/DB error: {type(exc).__name__}"

    # Every read `get_indexer_status()` itself makes just succeeded above,
    # yet it still raised -- shouldn't happen outside a race between the two
    # calls; report it rather than claim a false "ok".
    return "unavailable for an undetermined reason"


def _setting_name(exc: ImproperlyConfigured) -> str:
    """Pull the variable name back out of ``conf._bad_config``'s fixed
    message shape (``"analytics: invalid value for <VAR>"``) -- that message
    never includes the offending value, so this can't leak one either."""
    message = str(exc)
    return message.rsplit(" ", 1)[-1] if " " in message else message


class Command(BaseCommand):
    help = (
        "Read-only diagnostics for the analytics catch-up mechanism: "
        "indexer positions, the oldest unprocessed row, where the "
        "settlement gate currently stands across the catch-up window, each "
        "window day's completion state and retry counters, the "
        "stuck-processing watchdog row, and whether the catch-up settings "
        "are valid. Makes no writes and never prints the node URL or a raw "
        "RPC error."
    )

    def handle(self, *args, **options):
        today = timezone.now().date()

        try:
            status = get_indexer_status()
            status_error = None
        except DayNotReady as exc:
            status = None
            status_error = exc
        self._print_indexer_status(status, status_error)

        try:
            cfg = get_catchup_settings()
        except ImproperlyConfigured as exc:
            self.stdout.write(f"settings: bad_config setting={_setting_name(exc)}")
            self.stdout.write(
                "gate: skipped (settings invalid -- the catch-up window "
                "size itself comes from settings)"
            )
            return
        self.stdout.write("settings: ok")

        window = [today - timedelta(days=n) for n in range(cfg.WINDOW_DAYS, 0, -1)]
        self._print_gate(window, status)
        self._print_window_days(window)
        self._print_processing_watchdog()

    def _print_indexer_status(self, status, status_error: DayNotReady | None) -> None:
        if status is None:
            self.stdout.write(
                f"indexer_status: unavailable ({status_error.code}) -- "
                f"{_diagnose_unavailable()}"
            )
            return
        self.stdout.write(
            f"erc20: block={status.erc20_block_number} "
            f"ts={status.erc20_block_timestamp.isoformat()}"
        )
        self.stdout.write(
            f"master_copies: block={status.master_copies_block_number} "
            f"ts={status.master_copies_block_timestamp.isoformat()} "
            f"relevant_count={status.relevant_master_copies}"
        )
        if status.oldest_unprocessed_ts is None:
            self.stdout.write("oldest_unprocessed: none (processing caught up)")
        else:
            self.stdout.write(
                f"oldest_unprocessed: ts={status.oldest_unprocessed_ts.isoformat()} "
                f"safe={status.oldest_unprocessed_safe}"
            )

    def _print_gate(self, window: list[date], status) -> None:
        if status is None:
            self.stdout.write(
                "gate: indexer status unavailable, cannot evaluate window"
            )
            return
        latest_passing: date | None = None
        first_failure: tuple[date, str] | None = None
        for day in window:
            try:
                ensure_day_settled(day, status)
            except DayNotReady as exc:
                if first_failure is None:
                    first_failure = (day, exc.code)
                continue
            latest_passing = day
        self.stdout.write(
            "gate: latest_passing_day="
            f"{latest_passing.isoformat() if latest_passing else None} "
            "first_failing_day="
            f"{first_failure[0].isoformat() if first_failure else None} "
            f"reason={first_failure[1] if first_failure else None}"
        )

    def _print_window_days(self, window: list[date]) -> None:
        metrics = {row.date: row for row in DailyMetric.objects.filter(date__in=window)}
        states = {
            date.fromisoformat(row.key): row
            for row in AnalyticsCatchupState.objects.filter(
                kind=_DAY_KIND, key__in=[d.isoformat() for d in window]
            )
        }
        self.stdout.write(f"window ({len(window)} days, {window[0]} .. {window[-1]}):")
        for day in window:
            metric = metrics.get(day)
            state = states.get(day)
            if metric is None:
                # Distinct from a row that exists but has neither mark set
                # yet -- "never written" and "written, still incomplete"
                # are different diagnoses for an operator.
                marks = "row=missing"
            else:
                core_completed_at = (
                    metric.core_completed_at.isoformat()
                    if metric.core_completed_at
                    else None
                )
                completed_at = (
                    metric.completed_at.isoformat() if metric.completed_at else None
                )
                marks = (
                    f"core_completed_at={core_completed_at} completed_at={completed_at}"
                )
            if state is None:
                self.stdout.write(
                    f"  {day.isoformat()}: {marks} (no catch-up state row)"
                )
                continue
            self.stdout.write(
                f"  {day.isoformat()}: {marks} attempts={state.attempts} "
                "next_attempt_at="
                f"{state.next_attempt_at.isoformat() if state.next_attempt_at else None} "
                f"failed_steps={state.failed_steps} last_state={state.last_state} "
                f"last_code={state.last_code} "
                f"gave_up={'yes' if state.gave_up_logged_at else 'no'} "
                f"expired={'yes' if state.expired_logged_at else 'no'}"
            )

    def _print_processing_watchdog(self) -> None:
        state = AnalyticsCatchupState.objects.filter(
            kind=_PROCESSING_KIND, key=_PROCESSING_KEY
        ).first()
        if state is None:
            self.stdout.write("processing_watchdog: no observation yet")
            return
        self.stdout.write(
            f"processing_watchdog: observed_value={state.observed_value} "
            f"observed_count={state.observed_count} "
            "observed_since="
            f"{state.observed_since.isoformat() if state.observed_since else None} "
            "stuck_logged_at="
            f"{state.stuck_logged_at.isoformat() if state.stuck_logged_at else None}"
        )
