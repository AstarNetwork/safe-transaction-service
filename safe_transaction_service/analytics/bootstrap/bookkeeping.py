"""Durable per-stage bootstrap bookkeeping: one row per stage in
``AnalyticsBootstrapStage``, with no separate "bootstrap complete"
marker row -- ``bootstrap_complete()`` derives that from the three rows.
``STAGE_NAMES`` mirrors the registry order without importing it.
"""

from collections.abc import Callable

from django.utils import timezone

from ..models import AnalyticsBootstrapStage
from .stage import Stage

#: The three stages, in the fixed order the tick enforces. Kept here, not
#: imported from ``registry.py`` -- see the module docstring.
STAGE_NAMES = ("daily", "native", "erc20")


def get_stage_row(name: str) -> AnalyticsBootstrapStage:
    """The stage's row, created lazily (all defaults) on first use."""
    row, _ = AnalyticsBootstrapStage.objects.get_or_create(name=name)
    return row


def peek_stage_row(name: str) -> AnalyticsBootstrapStage:
    """The stage's row without creating one -- for a read-only caller
    (`--status`) that must make no writes."""
    return AnalyticsBootstrapStage.objects.filter(name=name).first() or (
        AnalyticsBootstrapStage(name=name)
    )


def mark_completed(name: str, depth: int | None = None) -> None:
    """Record that ``name``'s stage reported ``is_done() == True``.
    Sets ``completed_at`` only the first time; a new ``depth`` (daily,
    after a reopen) updates ``completed_depth`` and clears any leftover
    failure/give-up bookkeeping. Idempotent.
    """
    row = get_stage_row(name)
    update_fields: list[str] = []
    if row.completed_at is None:
        row.completed_at = timezone.now()
        update_fields.append("completed_at")
    if depth is not None and row.completed_depth != depth:
        row.completed_depth = depth
        update_fields.append("completed_depth")
    if row.consecutive_failures or row.last_failure_at or row.gave_up_at:
        row.consecutive_failures = 0
        row.last_failure_at = None
        row.gave_up_at = None
        update_fields.extend(["consecutive_failures", "last_failure_at", "gave_up_at"])
    if update_fields:
        row.save(update_fields=[*update_fields, "updated_at"])


def record_failure(name: str) -> AnalyticsBootstrapStage:
    """Increment ``consecutive_failures`` and stamp ``last_failure_at``;
    returns the saved row."""
    row = get_stage_row(name)
    row.consecutive_failures += 1
    row.last_failure_at = timezone.now()
    row.save()
    return row


def record_dispatch(name: str) -> None:
    """Stamps ``last_dispatch_at`` on every ``start_or_resume()`` call,
    so a later failure can tell "new" from "already counted"."""
    row = get_stage_row(name)
    row.last_dispatch_at = timezone.now()
    row.save()


def mark_gave_up(name: str) -> None:
    """Retry cap reached: park this stage until reset or a depth grow
    (daily only)."""
    row = get_stage_row(name)
    row.gave_up_at = timezone.now()
    row.save()


def reset_stage(name: str, *, data_wiped: bool = False) -> None:
    """Reset a stage's retry bookkeeping for a manual start or
    ``--restart``. ``data_wiped=True`` (``--restart`` only) also clears
    ``completed_at`` (and, for daily, ``completed_depth``), since the
    data was truncated. Safe to leave non-terminal: the lock,
    manifest/lease and ``dispatch_seq`` guard already protect a live run.
    """
    row = get_stage_row(name)
    row.consecutive_failures = 0
    row.last_failure_at = None
    row.gave_up_at = None
    if data_wiped:
        row.completed_at = None
        if name == "daily":
            row.completed_depth = None
    row.save()


def maybe_reopen_daily(configured_depth: int) -> None:
    """Clear the daily row's completion/failure bookkeeping once
    ``configured_depth`` exceeds ``completed_depth``. Keyed on
    ``completed_at``, not ``gave_up_at`` alone: a stage that gave up
    without ever completing has no baseline to grow past.
    """
    row = get_stage_row("daily")
    if row.completed_at is None or row.completed_depth is None:
        return
    if configured_depth <= row.completed_depth:
        return
    row.completed_at = None
    row.completed_depth = None
    row.gave_up_at = None
    row.consecutive_failures = 0
    row.last_failure_at = None
    row.save()


def bootstrap_complete(configured_depth: int) -> bool:
    """One query over at most 3 rows: every stage done or given up, with
    daily additionally needing ``completed_depth >= configured_depth``.
    A given-up daily row counts as complete regardless of depth, even
    though it never reached one -- see ``maybe_reopen_daily``.
    """
    rows = {
        row.name: row
        for row in AnalyticsBootstrapStage.objects.filter(name__in=STAGE_NAMES)
    }
    for name in STAGE_NAMES:
        row = rows.get(name)
        if row is None:
            return False
        if row.gave_up_at is not None:
            continue
        if row.completed_at is None:
            return False
        if name == "daily" and (
            row.completed_depth is None or row.completed_depth < configured_depth
        ):
            return False
    return True


def row_reports_gave_up(row: AnalyticsBootstrapStage) -> bool:
    return row.gave_up_at is not None


def row_reports_done(
    name: str, row: AnalyticsBootstrapStage, configured_depth: int
) -> bool:
    """Whether ``row`` counts as completed for ``configured_depth`` --
    depth-gated for daily only. Shared by ``pick_current_stage`` and the
    status report."""
    if row.completed_at is None:
        return False
    if name != "daily":
        return True
    return row.completed_depth is not None and row.completed_depth >= configured_depth


def pick_current_stage(
    stages: list[Stage],
    configured_depth: int,
    *,
    on_stage_done: Callable[[str], None] | None = None,
) -> Stage | None:
    """The first stage, in order, whose row is not terminal. A
    non-terminal stage whose ``is_done()`` is ``True`` is marked via
    ``on_stage_done`` (the tick passes ``mark_completed``) and skipped."""
    for stage in stages:
        row = peek_stage_row(stage.name)
        if row_reports_gave_up(row) or row_reports_done(
            stage.name, row, configured_depth
        ):
            continue
        if stage.is_done():
            if on_stage_done is not None:
                on_stage_done(stage.name)
            continue
        return stage
    return None
