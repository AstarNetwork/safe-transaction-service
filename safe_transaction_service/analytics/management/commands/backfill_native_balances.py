"""Initial fill of the incremental native-balance rollup.

One full pass over every Safe, computing its native balance as of a fixed
block, so that `compute_native_balance_rollup_task` has something to add
deltas to. This is the same work the nightly 16-shard chord used to do —
the point of this command is that it is done **once**, not every night.

Runs INLINE, in this process, with no Celery involved: the shards fail on
Ethereum-sized chains because each one is a task under
`task_timeout(LOCK_TIMEOUT)` and 900 s is not enough to sum the whole
history of ~29k addresses. Nothing here is under a task timeout, so the
run simply takes as long as it takes (`nohup` it).

Progress lives in the rollup table itself: a row with `updated_to_block`
set is a Safe already computed. A crash mid-run loses nothing — re-run
and it skips what is done. No Redis manifest, unlike
`backfill_daily_metrics`; the table is its own journal.
"""

import time

from django.core.management.base import BaseCommand, CommandError
from django.db import connection

from safe_transaction_service.analytics.models import (
    AnalyticsWatermark,
    SafeNativeBalance,
)
from safe_transaction_service.analytics.services.db import relaxed_statement_timeout
from safe_transaction_service.analytics.tasks import (
    NATIVE_BALANCE_WATERMARK,
    _iter_safe_addresses_keyset,
    native_balance_head_block,
    seed_missing_native_balances,
    write_native_balance_watermark,
)
from safe_transaction_service.analytics.tasks_shards import (
    latest_native_balance_run_id,
    load_native_balance_run,
    start_native_balance_backfill_run,
)
from safe_transaction_service.history.models import SafeContract


def _stamp_range() -> tuple[int | None, int | None]:
    """`(MIN, MAX)` of `updated_to_block` over the rollup, or `(None, None)`
    when it is empty."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT MIN(updated_to_block), MAX(updated_to_block) "
            "FROM analytics_safenativebalance"
        )
        low, high = cursor.fetchone()
    if low is None:
        return None, None
    return int(low), int(high)


class Command(BaseCommand):
    help = (
        "Fill analytics_safenativebalance for every Safe as of one fixed "
        "block, then write the `native_balance` watermark so the nightly "
        "incremental task can take over. Runs INLINE by default (no Celery, "
        "no task timeout) and resumes — a row in the table is a Safe already "
        "done. --celery runs the same walk on the `contracts` queue instead, "
        "one chunk in flight at a time, so the run outlives this shell; "
        "--wait N follows it. --status reports progress (the table, plus the "
        "most recent Celery run) without writing anything; --restart empties "
        "the table and starts over."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--chunk-size",
            type=int,
            default=5000,
            help=(
                "Safe addresses per batch (default 5000). The same size the "
                "existing balance SQL was measured at; lower it if a batch "
                "outruns the 30-minute relaxed statement timeout."
            ),
        )
        parser.add_argument(
            "--resume",
            action="store_true",
            default=True,
            help=(
                "Skip Safes that already have a rollup row (the default). "
                "Present for symmetry with --restart."
            ),
        )
        parser.add_argument(
            "--restart",
            action="store_true",
            help=(
                "Empty the rollup and its watermark first, then compute "
                "every Safe from scratch. Use after a reorg deeper than the "
                "confirmation zone, or when the stamps are inconsistent."
            ),
        )
        parser.add_argument(
            "--status",
            action="store_true",
            help=(
                "Print rollup progress and exit. Writes nothing. Covers both "
                "the table and the most recent --celery run."
            ),
        )
        parser.add_argument(
            "--celery",
            action="store_true",
            help=(
                "Dispatch the walk to the `contracts` queue instead of "
                "running it here: one chunk of --chunk-size Safes per task, "
                "strictly one in flight, each dispatching the next. The run "
                "survives this shell exiting. Safe under the task timeout "
                "because a chunk is bounded work, unlike the 16 TVL shards "
                "which each own an unbounded slice of history."
            ),
        )
        parser.add_argument(
            "--wait",
            type=int,
            default=0,
            help=(
                "--celery only: poll Redis for up to N seconds, printing "
                "progress, then return. 0 (default) starts the run and "
                "returns immediately; the run continues on the worker. Exits "
                "non-zero if the run is still going when N elapses."
            ),
        )
        parser.add_argument(
            "--poll-interval",
            type=int,
            default=15,
            help="--celery with --wait: seconds between polls (default 15).",
        )
        parser.add_argument(
            "--run-id",
            help=(
                "--celery only: explicit run id (default: a timestamp plus a "
                "random suffix). Shown in the output and accepted by --status."
            ),
        )
        parser.add_argument(
            "--at-block",
            type=int,
            help=(
                "Pin the run to this block instead of the current safe head. "
                "Must not exceed the safe head — blocks a reorg can still "
                "take must never enter the rollup. Required to be the block "
                "an interrupted run was already using, if there is one."
            ),
        )

    # ────────────────────────────── entry ──────────────────────────────

    def handle(self, *args, **options):
        if options["status"]:
            return self._print_status()

        if options["restart"]:
            self._restart()

        head = self._resolve_head(options.get("at_block"))
        if options["celery"]:
            return self._run_celery(
                head,
                chunk_size=options["chunk_size"],
                wait_seconds=options["wait"],
                poll_interval=options["poll_interval"],
                run_id=options.get("run_id"),
            )
        self._run_inline(head, options["chunk_size"])

    # ───────────────────────────── status ──────────────────────────────

    def _print_status(self):
        total_safes = SafeContract.objects.count()
        rows = SafeNativeBalance.objects.count()
        low, high = _stamp_range()
        watermark = AnalyticsWatermark.objects.filter(
            name=NATIVE_BALANCE_WATERMARK
        ).first()
        safe_head = native_balance_head_block()

        self.stdout.write(f"Safes in history_safecontract : {total_safes}")
        self.stdout.write(f"Rows in the rollup            : {rows}")
        self.stdout.write(
            f"Remaining (approx)            : {max(total_safes - rows, 0)}"
        )
        self.stdout.write(
            f"updated_to_block range        : {low} → {high}"
            if low is not None
            else "updated_to_block range        : (empty)"
        )
        if watermark is None:
            self.stdout.write(
                self.style.WARNING(
                    "Watermark                     : NOT SET — the incremental "
                    "task will refuse to run until this backfill finishes"
                )
            )
        else:
            self.stdout.write(
                f"Watermark                     : {watermark.block_number} "
                f"(at {watermark.computed_at.isoformat()})"
            )
        self.stdout.write(f"Safe head right now           : {safe_head}")

        run_id = latest_native_balance_run_id()
        run = load_native_balance_run(run_id) if run_id else None
        if run is None:
            self.stdout.write("Celery run                    : none recorded")
        else:
            self.stdout.write("Most recent --celery run:")
            self._print_run(run)

    # ──────────────────────────── restart ──────────────────────────────

    def _restart(self):
        self.stdout.write(self.style.WARNING("--restart: emptying the rollup"))
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE analytics_safenativebalance")
        AnalyticsWatermark.objects.filter(name=NATIVE_BALANCE_WATERMARK).delete()

    # ───────────────────────── head resolution ─────────────────────────

    def _resolve_head(self, at_block: int | None) -> int:
        """Decide which block this run computes balances as of.

        The hazard this guards is a *second* run at a newer block over a
        table the first run already stamped: the untouched rows would
        still only be complete through the old block, and moving the
        watermark to the new one would drop everything in between for
        them, silently and permanently. So a resumed run is pinned to the
        block the interrupted one was using, and a run over an
        already-watermarked rollup tops up missing Safes at the existing
        watermark without moving it.
        """
        safe_head = native_balance_head_block()
        if safe_head is None:
            raise CommandError(
                "No block is confirmed beyond the reorg depth yet — nothing "
                "can be safely computed. Let the indexer and check_reorgs "
                "catch up first."
            )

        watermark = AnalyticsWatermark.objects.filter(
            name=NATIVE_BALANCE_WATERMARK
        ).first()
        low, high = _stamp_range()

        if watermark is not None:
            # Already handed over to the incremental task. Anything this
            # command does now is a top-up of Safes it missed, and those
            # have to land at the watermark so the next delta covers them
            # exactly once.
            if at_block is not None and at_block != watermark.block_number:
                raise CommandError(
                    f"--at-block {at_block} conflicts with the existing "
                    f"watermark at {watermark.block_number}. A completed "
                    f"rollup can only be topped up at its own watermark; use "
                    f"--restart to rebuild at a different block."
                )
            self.stdout.write(
                f"Rollup already has a watermark at {watermark.block_number}; "
                f"topping up missing Safes there and leaving it in place."
            )
            return watermark.block_number

        if low is not None and low != high:
            raise CommandError(
                f"The rollup holds rows stamped at different blocks "
                f"({low} and {high}) with no watermark to reconcile them. "
                f"Neither block is safe to continue at — the lower one would "
                f"double-count the range between them, the higher one would "
                f"skip it. Rebuild with --restart."
            )

        if low is not None:
            # Interrupted first run: continue at exactly its block.
            if at_block is not None and at_block != low:
                raise CommandError(
                    f"--at-block {at_block} does not match the block an "
                    f"interrupted run already used ({low}). Pass --at-block "
                    f"{low} to resume it, or --restart to start over."
                )
            self.stdout.write(f"Resuming an interrupted run at block {low}.")
            return low

        head = at_block if at_block is not None else safe_head
        if head > safe_head:
            raise CommandError(
                f"--at-block {head} is above the safe head {safe_head}. Blocks "
                f"a reorg can still remove must not enter the rollup."
            )
        return head

    # ────────────────────────────── run ────────────────────────────────

    def _run_inline(self, head: int, chunk_size: int):
        total_safes = SafeContract.objects.count()
        started = time.time()
        seen = 0
        seeded = 0
        chunk_index = 0

        self.stdout.write(
            f"Backfilling native balances for {total_safes} Safes as of block "
            f"{head}, {chunk_size} per batch."
        )
        self.stdout.flush()

        with relaxed_statement_timeout():
            for addresses in _iter_safe_addresses_keyset(chunk_size):
                chunk_index += 1
                chunk_started = time.time()
                address_bytes = [bytes.fromhex(addr[2:]) for addr in addresses]
                seen += len(address_bytes)

                computed, present = seed_missing_native_balances(address_bytes, head)
                seeded += computed

                self.stdout.write(
                    f"  [{seen}/{total_safes}] batch {chunk_index}: "
                    f"{computed} computed, {present} already done, "
                    f"{time.time() - chunk_started:.1f}s"
                )
                self.stdout.flush()

        if write_native_balance_watermark(head):
            self.stdout.write(self.style.SUCCESS(f"Watermark set to {head}."))
        else:
            # Top-up of an already-handed-over rollup: the watermark is the
            # incremental task's, and moving it here would skip every block
            # between it and `head` for the Safes this run did not touch.
            existing = AnalyticsWatermark.objects.get(name=NATIVE_BALANCE_WATERMARK)
            self.stdout.write(f"Watermark left at {existing.block_number}.")

        self.stdout.write(
            self.style.SUCCESS(
                f"Done in {time.time() - started:.1f}s: {seeded} Safes computed, "
                f"{seen} scanned, {chunk_index} batches. Next: "
                f"`compute_native_balance_rollup_task` takes it from here "
                f"(daily at 03:05 UTC), or run it now with "
                f"`manage.py shell -c 'from safe_transaction_service.analytics."
                f"tasks import compute_native_balance_rollup_task as t; t.delay()'`."
            )
        )

    # ─────────────────────────── celery ────────────────────────────────

    def _run_celery(
        self,
        head: int,
        chunk_size: int,
        wait_seconds: int,
        poll_interval: int,
        run_id: str | None,
    ):
        """Start a chunked run on the `contracts` queue and optionally watch it.

        Each chunk dispatches its own successor, so exactly one is ever in
        flight and the cap does not depend on the worker pool size or on
        this process staying alive. Progress lives in the same two places
        as the inline mode: the rollup table (what is done) and, for the
        run itself, a Redis manifest (where the cursor is).
        """
        total_safes = SafeContract.objects.count()
        run = start_native_balance_backfill_run(
            head, chunk_size, total_safes, run_id=run_id
        )
        self.stdout.write(
            f"Started run {run['run_id']} on the `contracts` queue: "
            f"{total_safes} Safes as of block {head}, {chunk_size} per chunk."
        )
        self.stdout.write("Follow it with: manage.py backfill_native_balances --status")

        if not wait_seconds:
            return

        deadline = time.time() + wait_seconds
        while time.time() < deadline:
            run = load_native_balance_run(run["run_id"]) or run
            self._print_run(run)
            if run.get("state") != "running":
                return
            time.sleep(min(poll_interval, max(deadline - time.time(), 0)))

        run = load_native_balance_run(run["run_id"]) or run
        if run.get("state") == "running":
            raise CommandError(
                f"Run {run['run_id']} is still going after {wait_seconds}s. "
                f"It keeps running on the worker — check with --status."
            )

    def _print_run(self, run: dict) -> None:
        seen = run.get("safes_seen", 0)
        total = run.get("total_safes_at_start") or 0
        pct = f"{100 * seen / total:.1f}%" if total else "?"
        line = (
            f"  run {run['run_id']} [{run.get('state')}] "
            f"chunks={run.get('chunks_done', 0)} "
            f"seen={seen}/{total} ({pct}) "
            f"seeded={run.get('safes_seeded', 0)} "
            f"present={run.get('safes_already_present', 0)} "
            f"head={run.get('head')}"
        )
        if run.get("state") == "failed":
            self.stderr.write(self.style.ERROR(line))
            self.stderr.write(self.style.ERROR(f"  error: {run.get('error')}"))
        elif run.get("state") == "finished":
            self.stdout.write(self.style.SUCCESS(line))
        else:
            self.stdout.write(line)
