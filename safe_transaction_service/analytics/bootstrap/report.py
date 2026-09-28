"""The single read model behind `/summary/`'s `bootstrap` object and
`analytics_bootstrap --status`, so neither keeps its own copy of how a
stage's state or the bootstrap's current stage is decided.

Read-only: every row comes from `peek_stage_row`, never `get_stage_row`,
so a fresh instance with no `AnalyticsBootstrapStage` rows at all reports
a full `"pending"` bootstrap without creating a single one. No RPC, no
gate check, no dispatch -- `check_indexer()` (the live verdict) is left to
callers that can afford a Redis/DB/RPC round trip, such as the management
command.
"""

from django.conf import settings

from .bookkeeping import (
    bootstrap_complete,
    peek_stage_row,
    pick_current_stage,
    row_reports_done,
    row_reports_gave_up,
)
from .gate import last_indexer_gate_state
from .registry import build_default_stages
from .stage import Stage


def _iso(value):
    return value.isoformat() if value is not None else None


def _stage_state(stage: Stage, row, depth: int) -> str:
    """Row-first, the same rule `pick_current_stage` uses to skip a
    terminal stage: a row that reports done (depth-gated for daily) is
    `"done"`, a gave-up one is `"gave_up"`, regardless of what the
    stage's own live `status()` would say right now -- e.g. daily's
    sliding window pulling a completed day back into view. Only once
    neither is true does the live `status()` decide."""
    if row_reports_done(stage.name, row, depth):
        return "done"
    if row_reports_gave_up(row):
        return "gave_up"
    return stage.status()


def build_bootstrap_report() -> dict:
    """Everything `/summary/`'s `bootstrap` object and `--status` show,
    built fresh on every call -- never cached, so it's always current."""
    depth = settings.ANALYTICS_BOOTSTRAP_DAILY_DAYS
    stages = build_default_stages()
    rows = {stage.name: peek_stage_row(stage.name) for stage in stages}
    current_stage = pick_current_stage(stages, depth)

    return {
        "enabled": settings.ENABLE_ANALYTICS and settings.ANALYTICS_AUTO_BACKFILL,
        "complete": bootstrap_complete(depth),
        "daily_depth": depth,
        "current_stage": current_stage.name if current_stage else None,
        "indexer_gate": last_indexer_gate_state(),
        "stages": {
            stage.name: {
                "state": _stage_state(stage, rows[stage.name], depth),
                "progress": stage.progress(),
                "consecutive_failures": rows[stage.name].consecutive_failures,
                "completed_at": _iso(rows[stage.name].completed_at),
                "gave_up_at": _iso(rows[stage.name].gave_up_at),
                "last_dispatch_at": _iso(rows[stage.name].last_dispatch_at),
            }
            for stage in stages
        },
    }
