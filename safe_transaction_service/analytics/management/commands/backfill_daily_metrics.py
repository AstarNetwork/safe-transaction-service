import json
import time
from datetime import date, datetime, timedelta

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from safe_transaction_service.analytics.catchup import (
    DayNotReady,
    DayStatus,
    compute_day,
    get_indexer_status,
    select_failed_days,
)
from safe_transaction_service.analytics.services.db import relaxed_statement_timeout
from safe_transaction_service.analytics.tasks_shards import (
    BACKFILL_CURSOR_KEY,
    latest_backfill_run_id,
    load_backfill_chunk_summary,
    load_backfill_run,
    start_backfill_run,
)
from safe_transaction_service.utils.redis import get_redis


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as e:
        raise CommandError(f"Invalid date {value!r}: expected YYYY-MM-DD") from e


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    seconds = int(max(seconds, 0))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


class Command(BaseCommand):
    help = (
        "Backfill DailyMetric rows and rollup tables for a closed date range. "
        "Every day goes through the same indexer-status gate as the nightly "
        "task and the sweeper -- a day that isn't settled yet is deferred, "
        "not computed on partial data. Default mode splits the range into "
        "--chunk-days chunks and runs them STRICTLY ONE AT A TIME on the "
        "`contracts` queue: each chunk is a Celery chord (one task per day) "
        "whose callback dispatches the next chunk, so at most --chunk-days "
        "days are ever in flight regardless of the worker pool size and "
        "regardless of whether this process is still alive -- but this "
        "process's --inline run is invisible to the sweeper, which only "
        "sees the most recent Celery run's manifest; avoid a long --inline "
        "run on a large network once the sweeper is active. --wait N polls "
        "Redis and reports each chunk (N is an upper bound per chunk); "
        "--wait 0 returns right after starting the run. --status [RUN_ID] "
        "prints the progress of a run without touching the database. "
        "--failed-only re-runs only days without completed_at. Use --inline "
        "for the sequential in-process loop (debugging, no worker needed). "
        "--skip-settle-check bypasses the indexer-status gate for this range "
        "-- only for a range you already know is fully indexed; every "
        "skipped day is logged."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--start",
            help="First day to backfill, inclusive (YYYY-MM-DD, UTC)",
        )
        parser.add_argument(
            "--end",
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
                "Run sequentially inside this process instead of dispatching "
                "Celery chunks. Useful for debugging and for fresh installs "
                "without a running worker pool. Safe under nohup."
            ),
        )
        parser.add_argument(
            "--wait",
            type=int,
            default=0,
            help=(
                "Celery mode only: poll Redis and block for up to N seconds "
                "PER CHUNK, printing each chunk's summary as it lands and the "
                "run aggregate at the end. 0 (default) starts the run and "
                "returns immediately — chunks still execute one after another "
                "on the worker; inspect with --status. Exits non-zero if a "
                "chunk exceeds N seconds (the run itself keeps going)."
            ),
        )
        parser.add_argument(
            "--poll-interval",
            type=int,
            default=15,
            help="Celery mode with --wait: seconds between Redis polls (default 15).",
        )
        parser.add_argument(
            "--chunk-days",
            type=int,
            default=7,
            help=(
                "Process the date range in chunks of N days. Celery mode: N is "
                "the hard cap on concurrently running days (one chunk in "
                "flight at a time). Inline mode: checkpoint-print interval. "
                "0 disables chunking (one chunk for the whole range — in "
                "Celery mode that means every day on the queue at once; "
                "don't do that on a shared database). Default 7."
            ),
        )
        parser.add_argument(
            "--failed-only",
            action="store_true",
            help=(
                "Only (re)run days in the range without completed_at -- "
                "missing a DailyMetric row entirely, deferred by the gate, "
                "or still short a populator. Works with both Celery and "
                "--inline modes."
            ),
        )
        parser.add_argument(
            "--skip-settle-check",
            action="store_true",
            help=(
                "Bypass the indexer-status gate for this range and compute "
                "every day regardless of whether it looks settled yet. Only "
                "for a one-shot backfill of history you already know is "
                "fully indexed -- a day computed this way gets completed_at "
                "on possibly-partial data and the sweeper will never retry "
                "it. Logs WARNING analytics.daily.settle_check_skipped for "
                "every day. Works with both Celery and --inline modes."
            ),
        )
        parser.add_argument(
            "--run-id",
            help=(
                "Celery mode: explicit run id (default: timestamp + random "
                "suffix). Shown in the output and accepted by --status."
            ),
        )
        parser.add_argument(
            "--status",
            nargs="?",
            const="latest",
            metavar="RUN_ID",
            help=(
                "Print the state of a Celery backfill run and exit. Without a "
                "value, the most recently started run. Reads Redis only."
            ),
        )

    # ────────────────────────────── entry ──────────────────────────────

    def handle(self, *args, **options):
        if options.get("status"):
            return self._print_status(options["status"])

        if not options.get("start") or not options.get("end"):
            raise CommandError("--start and --end are required (or use --status)")
        start = _parse_date(options["start"])
        end = _parse_date(options["end"])
        if end < start:
            raise CommandError("--end must be on or after --start")

        total_days = (end - start).days + 1
        dates = [start + timedelta(days=offset) for offset in range(total_days)]

        if options["failed_only"]:
            selected = select_failed_days(dates)
            self.stdout.write(
                f"--failed-only: {len(selected)}/{len(dates)} days in "
                f"{start.isoformat()} → {end.isoformat()} need (re)running"
            )
            if not selected:
                self.stdout.write(self.style.SUCCESS("Nothing to do."))
                return
            dates = selected

        skip_settle_check = options["skip_settle_check"]
        if skip_settle_check:
            self.stdout.write(
                self.style.WARNING(
                    "--skip-settle-check: bypassing the indexer-status gate "
                    "for this range. Only for a range you already know is "
                    "fully indexed -- see --help."
                )
            )

        if options["inline"]:
            return self._run_inline(
                dates, options["batch_days"], options["chunk_days"], skip_settle_check
            )
        return self._run_celery(
            dates,
            wait_seconds=options["wait"],
            chunk_days=options["chunk_days"],
            poll_interval=options["poll_interval"],
            run_id=options.get("run_id"),
            skip_settle_check=skip_settle_check,
        )

    # ────────────────────────────── inline ─────────────────────────────

    def _run_inline(self, dates, batch_days, chunk_days, skip_settle_check):
        total_days = len(dates)
        written = 0
        deferred = 0
        failed = 0

        self.stdout.write(
            f"Backfilling {total_days} days INLINE: "
            f"{dates[0].isoformat()} → {dates[-1].isoformat()}"
        )

        import time as _time

        # One indexer-status snapshot for the whole run, reused for every
        # day -- same as a Celery shard or the nightly task. `None` means
        # the snapshot itself couldn't be fetched this run; every day then
        # defers unless `skip_settle_check` says to compute anyway.
        try:
            status = get_indexer_status()
        except DayNotReady:
            status = None

        # Slice the run into chunks. chunk_days=0 → single chunk (legacy).
        chunk_size = chunk_days if chunk_days and chunk_days > 0 else total_days
        chunks = [dates[i : i + chunk_size] for i in range(0, total_days, chunk_size)]
        total_chunks = len(chunks)
        run_started = _time.time()

        with relaxed_statement_timeout():
            for chunk_idx, chunk in enumerate(chunks):
                chunk_started = _time.time()
                chunk_written = 0
                chunk_deferred = 0
                chunk_failed = 0
                self.stdout.write(
                    f"== Chunk {chunk_idx + 1}/{total_chunks}: "
                    f"{chunk[0].isoformat()} → {chunk[-1].isoformat()} "
                    f"({len(chunk)} days)"
                )
                self.stdout.flush()
                for chunk_offset, day in enumerate(chunk):
                    offset = chunk_idx * chunk_size + chunk_offset
                    self.stdout.write(
                        f"  [{offset + 1}/{total_days}] Starting {day.isoformat()} …"
                    )
                    self.stdout.flush()
                    started = _time.time()
                    try:
                        result = compute_day(
                            day, status, skip_settle_check=skip_settle_check
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
                        continue
                    if result.status == DayStatus.DEFERRED:
                        deferred += 1
                        chunk_deferred += 1
                        self.stdout.write(
                            f"  [{offset + 1}/{total_days}] {day.isoformat()} "
                            f"deferred ({result.code}) in "
                            f"{_time.time() - started:.1f}s"
                        )
                    else:
                        written += 1
                        chunk_written += 1
                        self.stdout.write(
                            f"  [{offset + 1}/{total_days}] {day.isoformat()} "
                            f"done in {_time.time() - started:.1f}s "
                            f"status={result.status.value}"
                        )
                    self.stdout.flush()
                    if batch_days and (offset + 1) % batch_days == 0:
                        self.stdout.write(
                            f"  …{offset + 1}/{total_days} processed "
                            f"(written={written}, deferred={deferred}, "
                            f"failed={failed})"
                        )

                self.stdout.write(
                    self.style.SUCCESS(
                        f"== Chunk {chunk_idx + 1}/{total_chunks} done in "
                        f"{_time.time() - chunk_started:.1f}s "
                        f"(chunk: written={chunk_written}, "
                        f"deferred={chunk_deferred}, failed={chunk_failed}; "
                        f"cumulative: written={written}, deferred={deferred}, "
                        f"failed={failed})"
                    )
                )
                self.stdout.flush()

        self.stdout.write(
            self.style.SUCCESS(
                f"Backfill done in {_time.time() - run_started:.1f}s: "
                f"written={written}, deferred={deferred}, failed={failed}, "
                f"total={total_days}, chunks={total_chunks}"
            )
        )

    # ────────────────────────────── celery ─────────────────────────────

    def _run_celery(
        self,
        dates: list[date],
        wait_seconds: int,
        chunk_days: int,
        poll_interval: int,
        run_id: str | None,
        skip_settle_check: bool = False,
    ):
        """Start a sequential chunked run on the worker and optionally watch it.

        The throttle lives on the Celery side (``tasks_shards.backfill_done``
        dispatches chunk n+1 when chunk n's chord resolves), so this method
        only *observes*: completion of a chunk is detected by the appearance
        of its summary key in Redis — never via the chord's ``AsyncResult``,
        whose value is not stored under ``CELERY_IGNORE_RESULT=True``.

        ``skip_settle_check`` rides on the run manifest, not just this call
        -- every chunk after the first is dispatched by a shard's chord
        callback, not by this process (see ``build_backfill_run``).
        """
        run = start_backfill_run(
            dates, chunk_days, run_id=run_id, skip_settle_check=skip_settle_check
        )
        run_id = run["run_id"]
        total_chunks = run["chunk_count"]

        self.stdout.write(
            f"Started backfill run {run_id}: {run['total_days']} days in "
            f"{total_chunks} chunk(s) of up to {run['chunk_days']} days "
            f"({run['start']} → {run['end']}), one chunk in flight at a time "
            f"on queue=contracts"
        )
        self.stdout.write(
            f"  Progress: manage.py backfill_daily_metrics --status {run_id}"
        )
        self.stdout.flush()

        if not wait_seconds:
            self.stdout.write(
                "Not waiting (--wait 0). Chunks continue sequentially on the "
                "worker; use --status to follow the run."
            )
            return

        watch_started = time.monotonic()
        for chunk_index in range(total_chunks):
            chunk = run["chunks"][chunk_index]
            self.stdout.write(
                f"== Chunk {chunk_index + 1}/{total_chunks}: "
                f"{chunk['start']} → {chunk['end']} ({len(chunk['days'])} days) — "
                f"waiting up to {wait_seconds}s…"
            )
            self.stdout.flush()
            chunk_wait_started = time.monotonic()
            run, summary = self._wait_for_chunk(
                run_id, chunk_index, wait_seconds, poll_interval
            )
            chunk = run["chunks"][chunk_index]
            if summary is None:
                if chunk["state"] == "dispatch_failed":
                    self._print_run(run)
                    raise CommandError(
                        f"Chunk {chunk_index + 1}/{total_chunks} could not be "
                        f"dispatched: {chunk.get('error')}. Re-run with "
                        f"--failed-only once the broker is healthy."
                    )
                self._print_run(run)
                raise CommandError(
                    f"Chunk {chunk_index + 1}/{total_chunks} did not finish "
                    f"within {wait_seconds}s (state={chunk['state']}). The run "
                    f"keeps going on the worker; follow it with --status "
                    f"{run_id}."
                )
            self.stdout.write(
                self.style.SUCCESS(
                    f"== Chunk {chunk_index + 1}/{total_chunks} finished in "
                    f"{_fmt_duration(time.monotonic() - chunk_wait_started)} "
                    f"(chunk: written={summary['written']}, "
                    f"failed={summary['failed']}; cumulative: "
                    f"written={run['written']}, failed={run['failed']}, "
                    f"elapsed={_fmt_duration(time.monotonic() - watch_started)})"
                )
            )
            for failure in summary.get("failures", []):
                self.stderr.write(
                    self.style.ERROR(
                        f"   FAILED {failure.get('date')}: {failure.get('error')}"
                    )
                )
            self.stdout.flush()

        run = load_backfill_run(run_id) or run
        self._print_run(run)

    def _wait_for_chunk(
        self, run_id: str, chunk_index: int, timeout: int, poll_interval: int
    ) -> tuple[dict, dict | None]:
        """Poll Redis until the chunk's summary key exists, the chunk is
        marked ``dispatch_failed`` in the manifest, or ``timeout`` elapses.
        Returns ``(run_manifest, chunk_summary_or_None)``.
        """
        deadline = time.monotonic() + timeout
        poll_interval = max(1, poll_interval)
        while True:
            run = load_backfill_run(run_id)
            if run is None:
                raise CommandError(
                    f"Run manifest for {run_id} disappeared from Redis "
                    f"(expired or flushed); cannot follow it."
                )
            summary = load_backfill_chunk_summary(run_id, chunk_index)
            if summary is not None:
                return run, summary
            if run["chunks"][chunk_index]["state"] == "dispatch_failed":
                return run, None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return run, None
            time.sleep(min(poll_interval, remaining))

    # ────────────────────────────── status ─────────────────────────────

    def _print_status(self, run_id: str):
        if run_id == "latest":
            resolved = latest_backfill_run_id()
            if resolved is None:
                # Legacy shape: a bare single-chord summary at the cursor key.
                blob = get_redis().get(BACKFILL_CURSOR_KEY)
                if blob:
                    self.stdout.write(
                        f"No run manifest; legacy summary at {BACKFILL_CURSOR_KEY}: "
                        f"{json.loads(blob)}"
                    )
                    return
                raise CommandError("No backfill run recorded in Redis.")
            run_id = resolved
        run = load_backfill_run(run_id)
        if run is None:
            raise CommandError(f"Unknown or expired backfill run: {run_id}")
        self._print_run(run)

    def _print_run(self, run: dict) -> None:
        started = _parse_ts(run.get("started_at"))
        finished = _parse_ts(run.get("finished_at"))
        now = timezone.now()
        elapsed = None
        if started is not None:
            elapsed = ((finished or now) - started).total_seconds()
        done = sum(1 for c in run["chunks"] if c["state"] == "done")
        if finished is None:
            state = "IN PROGRESS"
        elif any(c["state"] == "dispatch_failed" for c in run["chunks"]):
            state = "STOPPED (dispatch failed)"
        else:
            state = "FINISHED"

        self.stdout.write(
            f"Backfill run {run['run_id']}: {run['start']} → {run['end']} "
            f"({run['total_days']} days, chunk_days={run['chunk_days']}, "
            f"{run['chunk_count']} chunks) — {state}"
        )
        self.stdout.write(
            f"  started_at={run.get('started_at')} "
            f"finished_at={run.get('finished_at') or '-'} "
            f"elapsed={_fmt_duration(elapsed)}"
        )
        self.stdout.write(
            f"  chunks done={done}/{run['chunk_count']}  days: "
            f"written={run['written']} failed={run['failed']} "
            f"processed={run['total']}/{run['total_days']}"
        )
        self.stdout.write("  chunks:")
        for chunk in run["chunks"]:
            label = (
                f"    [{chunk['index'] + 1:>3}/{run['chunk_count']}] "
                f"{chunk['start']} → {chunk['end']}  {chunk['state']:<15}"
            )
            if chunk["state"] == "done":
                dispatched = _parse_ts(chunk.get("dispatched_at"))
                chunk_finished = _parse_ts(chunk.get("finished_at"))
                took = (
                    (chunk_finished - dispatched).total_seconds()
                    if dispatched and chunk_finished
                    else None
                )
                label += (
                    f" written={chunk['written']} failed={chunk['failed']} "
                    f"took={_fmt_duration(took)}"
                )
            elif chunk["state"] == "running":
                dispatched = _parse_ts(chunk.get("dispatched_at"))
                since = (now - dispatched).total_seconds() if dispatched else None
                label += (
                    f" since {chunk.get('dispatched_at')} ({_fmt_duration(since)} ago)"
                )
            elif chunk["state"] == "dispatch_failed":
                label += f" error={chunk.get('error')}"
            self.stdout.write(label)
        failures = run.get("failures") or []
        if failures:
            self.stdout.write(f"  failures ({len(failures)}):")
            for failure in failures:
                self.stdout.write(f"    {failure.get('date')}: {failure.get('error')}")
        self.stdout.flush()
