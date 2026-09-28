"""Read-only status for the analytics bootstrap (`analytics/bootstrap/`).

`--status` is the only mode: this command never dispatches anything and
never writes a row. It prints `build_bootstrap_report()` (`report.py`) --
the same read model `/summary/`'s `bootstrap` object uses -- plus two
things the report deliberately leaves out: the live indexer-gate check
(`check_indexer()`, an RPC/DB call a read model must not make on its own)
and each failing stage's retry-cooldown end time. Style mirrors
`backfill_erc20_balances --status` -- plain `self.stdout.write` lines,
one fact per line, `self.style.WARNING` for anything an operator should
notice.
"""

from django.conf import settings
from django.core.management.base import BaseCommand

from safe_transaction_service.analytics.bootstrap.bookkeeping import peek_stage_row
from safe_transaction_service.analytics.bootstrap.gate import check_indexer
from safe_transaction_service.analytics.bootstrap.report import build_bootstrap_report
from safe_transaction_service.analytics.bootstrap.tick import (
    BOOTSTRAP_RETRY_CAP,
    BOOTSTRAP_RETRY_COOLDOWN,
)


class Command(BaseCommand):
    help = (
        "Read-only status for the analytics bootstrap: whether it's "
        "enabled, the configured daily depth, whether it's complete, "
        "the indexer gate verdict, and each stage's status/progress/"
        "retry bookkeeping. Makes no writes and dispatches nothing."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--status",
            action="store_true",
            help="Print the bootstrap's status and exit. The only supported mode.",
        )

    def handle(self, *args, **options):
        report = build_bootstrap_report()

        self.stdout.write(f"ENABLE_ANALYTICS          : {settings.ENABLE_ANALYTICS}")
        self.stdout.write(
            f"ANALYTICS_AUTO_BACKFILL   : {settings.ANALYTICS_AUTO_BACKFILL}"
        )
        self.stdout.write(f"configured daily depth    : {report['daily_depth']}")
        self.stdout.write(f"bootstrap complete        : {report['complete']}")

        daily_row = peek_stage_row("daily")
        self.stdout.write(
            "  daily completed_depth   : "
            f"{daily_row.completed_depth} (recorded on last completion)"
        )

        if not report["enabled"]:
            self.stdout.write(
                self.style.WARNING(
                    "Bootstrap is switched off (analytics disabled or "
                    "ANALYTICS_AUTO_BACKFILL=False) -- the tick no-ops every "
                    "cycle regardless of what follows below."
                )
            )

        caught_up, code = check_indexer()
        self.stdout.write(
            f"indexer gate              : "
            f"{'caught up' if caught_up else f'NOT caught up ({code})'}"
        )
        self.stdout.write(f"  last recorded verdict   : {report['indexer_gate']}")
        self.stdout.write(
            "  (a real tick logs a state change at INFO/WARNING -- this "
            "command's own check above does not, to stay read-only)"
        )

        self.stdout.write(f"stages ({', '.join(report['stages'])}):")
        for name, stage_report in report["stages"].items():
            self._print_stage(name, stage_report)

        current_stage = report["current_stage"]
        self.stdout.write(
            f"next tick would act on    : {current_stage or 'nothing (complete)'}"
        )

    def _print_stage(self, name: str, stage_report: dict) -> None:
        self.stdout.write(f"  [{name}] status={stage_report['state']}")

        if stage_report["progress"] is not None:
            self.stdout.write(f"    progress={stage_report['progress']}")

        row = peek_stage_row(name)
        self.stdout.write(
            f"    consecutive_failures={stage_report['consecutive_failures']}/{BOOTSTRAP_RETRY_CAP} "
            f"last_failure_at={row.last_failure_at} "
            f"last_dispatch_at={stage_report['last_dispatch_at']}"
        )
        if row.last_failure_at is not None and row.gave_up_at is None:
            cooldown_ends = row.last_failure_at + BOOTSTRAP_RETRY_COOLDOWN
            self.stdout.write(f"    retry cooldown ends at={cooldown_ends}")
        if stage_report["gave_up_at"] is not None:
            self.stdout.write(
                self.style.WARNING(f"    GAVE UP at={stage_report['gave_up_at']}")
            )
        if stage_report["completed_at"] is not None:
            self.stdout.write(f"    completed_at={stage_report['completed_at']}")
