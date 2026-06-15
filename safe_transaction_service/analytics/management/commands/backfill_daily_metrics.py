import json
from datetime import date, datetime, timedelta

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from safe_transaction_service.analytics.services.db import relaxed_statement_timeout
from safe_transaction_service.analytics.tasks import _upsert_daily_metric
from safe_transaction_service.analytics.tasks_shards import (
    BACKFILL_CURSOR_KEY,
    dispatch_backfill,
)
from safe_transaction_service.utils.redis import get_redis


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as e:
        raise CommandError(f"Invalid date {value!r}: expected YYYY-MM-DD") from e


class Command(BaseCommand):
    help = (
        "Backfill DailyMetric rows and rollup tables for a closed date "
        "range. Default mode group-dispatches one Celery task per day on "
        "the `contracts` queue; concurrency caps naturally at worker pool "
        "size. Use --inline for the legacy sequential loop (debugging)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--start",
            required=True,
            help="First day to backfill, inclusive (YYYY-MM-DD, UTC)",
        )
        parser.add_argument(
            "--end",
            required=True,
            help="Last day to backfill, inclusive (YYYY-MM-DD, UTC)",
        )
        parser.add_argument(
            "--batch-days",
            type=int,
            default=7,
            help=(
                "Inline mode only: cosmetic progress-print interval "
                "(default 7). Ignored when dispatching via Celery."
            ),
        )
        parser.add_argument(
            "--inline",
            action="store_true",
            help=(
                "Run sequentially inside this process instead of group-"
                "dispatching one Celery task per day. Useful for debugging "
                "and for fresh installs without a running worker pool."
            ),
        )
        parser.add_argument(
            "--wait",
            type=int,
            default=0,
            help=(
                "Celery mode only: block for up to N seconds waiting for "
                "the chord callback to land. 0 (default) dispatches and "
                "returns immediately; check the Redis cursor for results."
            ),
        )
        parser.add_argument(
            "--chunk-days",
            type=int,
            default=7,
            help=(
                "Process the date range in chunks of N days. After each "
                "chunk the command prints a checkpoint summary with "
                "cumulative written/failed counts and elapsed time, then "
                "moves to the next chunk. Set to 0 to disable chunking "
                "(one continuous run). Default 7 — chunk boundaries are "
                "natural restart points since every day's writes commit "
                "independently."
            ),
        )

    def handle(self, *args, **options):
        start = _parse_date(options["start"])
        end = _parse_date(options["end"])
        if end < start:
            raise CommandError("--end must be on or after --start")

        total_days = (end - start).days + 1
        dates = [start + timedelta(days=offset) for offset in range(total_days)]

        if options["inline"]:
            return self._run_inline(dates, options["batch_days"], options["chunk_days"])
        return self._run_celery(dates, options["wait"], options["chunk_days"])

    def _run_inline(self, dates, batch_days, chunk_days):
        tz = timezone.get_current_timezone()
        total_days = len(dates)
        written = 0
        failed = 0

        self.stdout.write(
            f"Backfilling {total_days} days INLINE: "
            f"{dates[0].isoformat()} → {dates[-1].isoformat()}"
        )

        import time as _time

        # Slice the run into chunks. chunk_days=0 → single chunk (legacy).
        chunk_size = chunk_days if chunk_days and chunk_days > 0 else total_days
        chunks = [dates[i : i + chunk_size] for i in range(0, total_days, chunk_size)]
        total_chunks = len(chunks)
        run_started = _time.time()

        with relaxed_statement_timeout():
            for chunk_idx, chunk in enumerate(chunks):
                chunk_started = _time.time()
                chunk_written = 0
                chunk_failed = 0
                self.stdout.write(
                    f"== Chunk {chunk_idx + 1}/{total_chunks}: "
                    f"{chunk[0].isoformat()} → {chunk[-1].isoformat()} "
                    f"({len(chunk)} days)"
                )
                self.stdout.flush()
                for chunk_offset, day in enumerate(chunk):
                    offset = chunk_idx * chunk_size + chunk_offset
                    day_start = datetime.combine(day, datetime.min.time(), tzinfo=tz)
                    day_end = day_start + timedelta(days=1)
                    self.stdout.write(
                        f"  [{offset + 1}/{total_days}] Starting {day.isoformat()} …"
                    )
                    self.stdout.flush()
                    started = _time.time()
                    try:
                        _upsert_daily_metric(day_start, day_end)
                        written += 1
                        chunk_written += 1
                        self.stdout.write(
                            f"  [{offset + 1}/{total_days}] {day.isoformat()} "
                            f"done in {_time.time() - started:.1f}s"
                        )
                    except Exception as e:  # noqa: BLE001 — per-day isolation
                        failed += 1
                        chunk_failed += 1
                        self.stderr.write(
                            self.style.ERROR(
                                f"  [{offset + 1}/{total_days}] "
                                f"{day.isoformat()} FAILED after "
                                f"{_time.time() - started:.1f}s: {e}"
                            )
                        )
                    self.stdout.flush()
                    if batch_days and (offset + 1) % batch_days == 0:
                        self.stdout.write(
                            f"  …{offset + 1}/{total_days} processed "
                            f"(written={written}, failed={failed})"
                        )

                self.stdout.write(
                    self.style.SUCCESS(
                        f"== Chunk {chunk_idx + 1}/{total_chunks} done in "
                        f"{_time.time() - chunk_started:.1f}s "
                        f"(chunk: written={chunk_written}, failed={chunk_failed}; "
                        f"cumulative: written={written}, failed={failed})"
                    )
                )
                self.stdout.flush()

        self.stdout.write(
            self.style.SUCCESS(
                f"Backfill done in {_time.time() - run_started:.1f}s: "
                f"written={written}, failed={failed}, total={total_days}, "
                f"chunks={total_chunks}"
            )
        )

    def _run_celery(self, dates, wait_seconds: int, chunk_days: int):
        """Celery path with optional chunking.

        chunk_days=0 → single chord for the whole range (legacy behaviour).
        chunk_days>0 → dispatch one chord per chunk. With --wait>0, we
        block on each chord serially before queueing the next, so the
        contracts queue never has more than `chunk_days` tasks in flight.
        With --wait=0 we fire all chords up front and return.
        """
        total_days = len(dates)
        chunk_size = chunk_days if chunk_days and chunk_days > 0 else total_days
        chunks = [dates[i : i + chunk_size] for i in range(0, total_days, chunk_size)]
        total_chunks = len(chunks)

        self.stdout.write(
            f"Dispatching backfill: {total_days} days in {total_chunks} chunk(s) "
            f"of up to {chunk_size} days each "
            f"({dates[0].isoformat()} → {dates[-1].isoformat()}) on queue=contracts"
        )

        for chunk_idx, chunk in enumerate(chunks):
            result = dispatch_backfill(chunk)
            self.stdout.write(
                self.style.SUCCESS(
                    f"== Chunk {chunk_idx + 1}/{total_chunks}: "
                    f"{chunk[0].isoformat()} → {chunk[-1].isoformat()} "
                    f"dispatched (task {result.id})"
                )
            )
            if not wait_seconds:
                continue

            self.stdout.write(f"  Waiting up to {wait_seconds}s for chunk to finish…")
            try:
                summary = result.get(timeout=wait_seconds, disable_sync_subtasks=False)
                self.stdout.write(self.style.SUCCESS(f"  Chunk finished: {summary}"))
            except Exception as e:  # noqa: BLE001 — surface to operator
                blob = get_redis().get(BACKFILL_CURSOR_KEY)
                partial = json.loads(blob) if blob else None
                self.stderr.write(
                    self.style.WARNING(
                        f"  Chunk did not finish within {wait_seconds}s: {e}. "
                        f"Last cursor: {partial}"
                    )
                )
                # Don't abort — next chunk's dispatch is independent.

        if not wait_seconds:
            self.stdout.write(
                f"All {total_chunks} chunk(s) dispatched. "
                f"Cursor key: {BACKFILL_CURSOR_KEY}"
            )
