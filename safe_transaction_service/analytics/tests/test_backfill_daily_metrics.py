"""Tests for the chunked, sequential Celery backfill
(`manage.py backfill_daily_metrics` + `tasks_shards` run manifest).

All Celery paths run under ``CELERY_ALWAYS_EAGER`` — a chord's header and
callback execute inline inside ``apply_async``, so the *worker-side*
dispatch of chunk n+1 from ``backfill_done`` is observable as nested,
strictly ordered calls. Redis is the real one from ``.env.test``.
"""

import json
from datetime import date, timedelta
from io import StringIO
from unittest.mock import patch

from django.core.management import CommandError, call_command
from django.test import TestCase
from django.utils import timezone

from safe_transaction_service.analytics.management.commands.backfill_daily_metrics import (
    select_failed_days,
)
from safe_transaction_service.analytics.models import DailyMetric
from safe_transaction_service.analytics.tasks_shards import (
    BACKFILL_CURSOR_KEY,
    BACKFILL_RUN_KEY_PREFIX,
    _save_backfill_run,
    backfill_chunk_key,
    backfill_done,
    build_backfill_run,
    dispatch_backfill,
    latest_backfill_run_id,
    load_backfill_chunk_summary,
    load_backfill_run,
    start_backfill_run,
)
from safe_transaction_service.history.tests.factories import SafeContractFactory
from safe_transaction_service.utils.redis import get_redis

UPSERT_TARGET = "safe_transaction_service.analytics.tasks._upsert_daily_metric"
COMMAND_MODULE = (
    "safe_transaction_service.analytics.management.commands.backfill_daily_metrics"
)

START = date(2026, 6, 10)


def _dates(n: int, start: date = START) -> list[date]:
    return [start + timedelta(days=i) for i in range(n)]


def _clear_backfill_keys() -> None:
    redis = get_redis()
    keys = list(redis.scan_iter(match=f"{BACKFILL_RUN_KEY_PREFIX}*"))
    keys.append(BACKFILL_CURSOR_KEY)
    redis.delete(*keys)


class BackfillRedisMixin:
    def setUp(self):
        super().setUp()
        _clear_backfill_keys()

    def tearDown(self):
        _clear_backfill_keys()
        super().tearDown()


class TestSequentialChunks(BackfillRedisMixin, TestCase):
    """Defect 2 (2026-09-08, Ethereum staging): with `--wait 0` the command
    fired every chunk's chord at once and the worker took all 77 days
    concurrently. Now chunk n+1 is dispatched by chunk n's callback."""

    def test_chunks_execute_one_at_a_time_in_order(self):
        dates = _dates(6)
        seen: list[date] = []
        violations: list[str] = []

        def recording_upsert(day_start, day_end):
            day = day_start.date()
            seen.append(day)
            run_id = latest_backfill_run_id()
            run = load_backfill_run(run_id)
            states = [c["state"] for c in run["chunks"]]
            running = [i for i, s in enumerate(states) if s == "running"]
            if len(running) != 1:
                violations.append(f"{day}: running chunks={running}")
                return
            current = running[0]
            if any(s != "done" for s in states[:current]):
                violations.append(f"{day}: earlier chunk not done {states}")
            if any(s != "pending" for s in states[current + 1 :]):
                violations.append(f"{day}: later chunk already started {states}")
            if day.isoformat() not in run["chunks"][current]["days"]:
                violations.append(f"{day}: not in running chunk {current}")

        with patch(UPSERT_TARGET, side_effect=recording_upsert):
            run = start_backfill_run(dates, chunk_days=2)

        self.assertEqual(violations, [])
        self.assertEqual(seen, dates)
        self.assertEqual(run["chunk_count"], 3)
        self.assertEqual([c["state"] for c in run["chunks"]], ["done"] * 3)
        self.assertIsNotNone(run["finished_at"])

    def test_command_dispatches_only_the_first_chunk(self):
        """`--wait 0` must not fan out every chunk from the command process;
        with the callback stubbed out, exactly one chord goes on the queue."""
        dates = _dates(6)
        target = "safe_transaction_service.analytics.tasks_shards.dispatch_backfill"
        with patch(target) as dispatch_mock:
            call_command(
                "backfill_daily_metrics",
                start=dates[0].isoformat(),
                end=dates[-1].isoformat(),
                chunk_days=2,
                wait=0,
                stdout=StringIO(),
            )
        self.assertEqual(dispatch_mock.call_count, 1)
        _, kwargs = dispatch_mock.call_args
        first_days = dispatch_mock.call_args[0][0]
        self.assertEqual(first_days, [d.isoformat() for d in dates[:2]])
        self.assertEqual(kwargs["chunk_index"], 0)

        run = load_backfill_run(latest_backfill_run_id())
        self.assertEqual(
            [c["state"] for c in run["chunks"]], ["running", "pending", "pending"]
        )

    def test_chunk_days_zero_is_a_single_chunk(self):
        with patch(UPSERT_TARGET):
            run = start_backfill_run(_dates(5), chunk_days=0)
        self.assertEqual(run["chunk_count"], 1)
        self.assertEqual(run["chunk_days"], 5)
        self.assertEqual(run["written"], 5)


class TestPerChunkKeysAndAggregate(BackfillRedisMixin, TestCase):
    """Defect 3: a single shared cursor key was overwritten by every chunk.
    Now every chunk has its own key and the run manifest aggregates."""

    def test_each_chunk_writes_its_own_key(self):
        dates = _dates(5)
        with patch(UPSERT_TARGET):
            run = start_backfill_run(dates, chunk_days=2, run_id="test-run")

        self.assertEqual(run["run_id"], "test-run")
        self.assertEqual(latest_backfill_run_id(), "test-run")
        expected = [
            ("2026-06-10", "2026-06-11", 2),
            ("2026-06-12", "2026-06-13", 2),
            ("2026-06-14", "2026-06-14", 1),
        ]
        for index, (start, end, total) in enumerate(expected):
            summary = load_backfill_chunk_summary("test-run", index)
            self.assertIsNotNone(summary, f"chunk {index} summary missing")
            self.assertEqual(summary["run_id"], "test-run")
            self.assertEqual(summary["chunk_index"], index)
            self.assertEqual((summary["start"], summary["end"]), (start, end))
            self.assertEqual(summary["total"], total)
            self.assertEqual(summary["written"], total)
            self.assertEqual(summary["failed"], 0)
            self.assertEqual(
                run["chunks"][index]["key"], backfill_chunk_key("test-run", index)
            )
        # Keys are distinct and carry a TTL.
        keys = {c["key"] for c in run["chunks"]}
        self.assertEqual(len(keys), 3)
        for key in keys:
            self.assertGreater(get_redis().ttl(key), 0)
        self.assertGreater(get_redis().ttl(run["run_key"]), 0)

    def test_run_aggregate_counts_failures_across_chunks(self):
        dates = _dates(6)
        bad = {dates[1], dates[4]}

        def flaky_upsert(day_start, day_end):
            if day_start.date() in bad:
                raise RuntimeError(f"boom {day_start.date()}")

        with patch(UPSERT_TARGET, side_effect=flaky_upsert):
            run = start_backfill_run(dates, chunk_days=3)

        self.assertEqual(run["start"], "2026-06-10")
        self.assertEqual(run["end"], "2026-06-15")
        self.assertEqual(run["total_days"], 6)
        self.assertEqual(run["total"], 6)
        self.assertEqual(run["written"], 4)
        self.assertEqual(run["failed"], 2)
        self.assertEqual(
            sorted(f["date"] for f in run["failures"]),
            ["2026-06-11", "2026-06-14"],
        )
        self.assertIn("boom", run["failures"][0]["error"])
        self.assertIsNotNone(run["started_at"])
        self.assertIsNotNone(run["finished_at"])
        self.assertEqual([c["failed"] for c in run["chunks"]], [1, 1])
        self.assertEqual([c["written"] for c in run["chunks"]], [2, 2])

    def test_none_shard_result_is_a_failed_day_not_a_crash(self):
        """`task_timeout` makes a shard return None; the callback must count
        it as a failure (recovering the date by position) and still advance."""
        run = build_backfill_run(_dates(4), chunk_days=2, run_id="none-run")
        run["chunks"][0]["state"] = "running"
        _save_backfill_run(run)

        with patch(UPSERT_TARGET):
            summary = backfill_done(
                [None, {"date": "2026-06-11", "ok": True}],
                stats_key=run["chunks"][0]["key"],
                run_id="none-run",
                chunk_index=0,
            )

        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["failures"][0]["date"], "2026-06-10")
        self.assertIn("no result", summary["failures"][0]["error"])
        # The callback dispatched chunk 1, which (eagerly) completed the run.
        run = load_backfill_run("none-run")
        self.assertEqual([c["state"] for c in run["chunks"]], ["done", "done"])
        self.assertEqual(run["failed"], 1)
        self.assertEqual(run["written"], 3)
        self.assertIsNotNone(run["finished_at"])

    def test_standalone_dispatch_backfill_keeps_legacy_cursor_summary(self):
        with patch(UPSERT_TARGET):
            dispatch_backfill(_dates(2))
        summary = get_redis().get(BACKFILL_CURSOR_KEY)
        self.assertIsNotNone(summary)
        self.assertIn(b'"written": 2', summary)
        # No run manifest → the pointer helper returns None, not garbage.
        self.assertIsNone(latest_backfill_run_id())


class TestWaitViaRedis(BackfillRedisMixin, TestCase):
    """Defect 1: `AsyncResult.get()` never returned because the callback's
    result is dropped under CELERY_IGNORE_RESULT=True. The command now
    detects completion by the chunk summary key appearing in Redis."""

    def _start_without_executing(self, dates, chunk_days, run_id=None):
        """Stand-in for `start_backfill_run` that writes the manifest with
        chunk 0 running but never executes anything — like a real broker
        where the worker hasn't picked the chord up yet."""
        run = build_backfill_run(dates, chunk_days, run_id=run_id)
        run["chunks"][0]["state"] = "running"
        run["chunks"][0]["dispatched_at"] = timezone.now().isoformat()
        _save_backfill_run(run)
        get_redis().set(
            BACKFILL_CURSOR_KEY,
            json.dumps({"run_id": run["run_id"], "run_key": run["run_key"]}),
        )
        return run

    def test_wait_completes_when_chunk_key_appears(self):
        dates = _dates(4)
        sleeps: list[float] = []

        def worker_finishes_chunk(seconds):
            """Simulate the worker: chunk 0's callback lands on the first
            poll, chunk 1's on the next."""
            sleeps.append(seconds)
            run_id = latest_backfill_run_id()
            run = load_backfill_run(run_id)
            index = next(
                i for i, c in enumerate(run["chunks"]) if c["state"] == "running"
            )
            results = [{"date": d, "ok": True} for d in run["chunks"][index]["days"]]
            with patch(
                "safe_transaction_service.analytics.tasks_shards.dispatch_backfill",
                side_effect=lambda *a, **k: None,
            ):
                backfill_done(
                    results,
                    stats_key=run["chunks"][index]["key"],
                    run_id=run_id,
                    chunk_index=index,
                )

        out = StringIO()
        with (
            patch(
                f"{COMMAND_MODULE}.start_backfill_run",
                side_effect=self._start_without_executing,
            ),
            patch(f"{COMMAND_MODULE}.time.sleep", side_effect=worker_finishes_chunk),
        ):
            call_command(
                "backfill_daily_metrics",
                start=dates[0].isoformat(),
                end=dates[-1].isoformat(),
                chunk_days=2,
                wait=600,
                poll_interval=5,
                stdout=out,
            )

        output = out.getvalue()
        self.assertEqual(len(sleeps), 2, output)
        self.assertTrue(all(s <= 5 for s in sleeps))
        self.assertIn("Chunk 1/2 finished", output)
        self.assertIn("Chunk 2/2 finished", output)
        self.assertIn("FINISHED", output)
        self.assertIn("written=4 failed=0", output)

    def test_wait_times_out_with_status_hint_and_run_keeps_going(self):
        dates = _dates(2)
        out = StringIO()
        with (
            patch(
                f"{COMMAND_MODULE}.start_backfill_run",
                side_effect=self._start_without_executing,
            ),
            patch(f"{COMMAND_MODULE}.time.sleep"),
            self.assertRaises(CommandError) as ctx,
        ):
            call_command(
                "backfill_daily_metrics",
                start=dates[0].isoformat(),
                end=dates[-1].isoformat(),
                chunk_days=2,
                wait=1,
                poll_interval=1,
                stdout=out,
            )
        self.assertIn("did not finish within 1s", str(ctx.exception))
        self.assertIn("--status", str(ctx.exception))
        # Manifest untouched: the chunk is still running on the "worker".
        run = load_backfill_run(latest_backfill_run_id())
        self.assertEqual(run["chunks"][0]["state"], "running")

    def test_wait_zero_returns_immediately(self):
        out = StringIO()
        with (
            patch(
                f"{COMMAND_MODULE}.start_backfill_run",
                side_effect=self._start_without_executing,
            ),
            patch(f"{COMMAND_MODULE}.time.sleep") as sleep_mock,
        ):
            call_command(
                "backfill_daily_metrics",
                start="2026-06-10",
                end="2026-06-15",
                chunk_days=3,
                wait=0,
                stdout=out,
            )
        sleep_mock.assert_not_called()
        self.assertIn("Not waiting", out.getvalue())
        self.assertIn("--status", out.getvalue())


class TestStatusAndFailedOnly(BackfillRedisMixin, TestCase):
    def test_status_prints_latest_and_explicit_run(self):
        with patch(UPSERT_TARGET):
            start_backfill_run(_dates(3), chunk_days=2, run_id="status-run")

        out = StringIO()
        call_command("backfill_daily_metrics", status="latest", stdout=out)
        text = out.getvalue()
        self.assertIn("Backfill run status-run", text)
        self.assertIn("FINISHED", text)
        self.assertIn("chunks done=2/2", text)
        self.assertIn("written=3 failed=0", text)

        out = StringIO()
        call_command("backfill_daily_metrics", status="status-run", stdout=out)
        self.assertIn("Backfill run status-run", out.getvalue())

        with self.assertRaises(CommandError):
            call_command("backfill_daily_metrics", status="nope", stdout=StringIO())

    def test_status_requires_no_dates_but_run_requires_them(self):
        with self.assertRaises(CommandError):
            call_command("backfill_daily_metrics", stdout=StringIO())
        with self.assertRaises(CommandError):
            call_command("backfill_daily_metrics", status="latest", stdout=StringIO())

    def test_select_failed_days_missing_or_null_split(self):
        now = timezone.now()
        complete, stale, missing = _dates(3)
        DailyMetric.objects.create(
            date=complete, computed_at=now, multisig_txs_via_api=0
        )
        DailyMetric.objects.create(date=stale, computed_at=now)  # split NULL
        self.assertEqual(
            select_failed_days([complete, stale, missing]), [stale, missing]
        )
        self.assertEqual(select_failed_days([]), [])

    def test_failed_only_inline_touches_only_selected_days(self):
        now = timezone.now()
        complete, stale, missing = _dates(3)
        DailyMetric.objects.create(
            date=complete, computed_at=now, multisig_txs_via_api=1
        )
        DailyMetric.objects.create(date=stale, computed_at=now)
        touched: list[date] = []
        out = StringIO()
        with patch(
            f"{COMMAND_MODULE}._upsert_daily_metric",
            side_effect=lambda s, e: touched.append(s.date()),
        ):
            call_command(
                "backfill_daily_metrics",
                start=complete.isoformat(),
                end=missing.isoformat(),
                inline=True,
                failed_only=True,
                stdout=out,
            )
        self.assertEqual(touched, [stale, missing])
        self.assertIn("2/3 days", out.getvalue())

    def test_failed_only_nothing_to_do(self):
        now = timezone.now()
        d = START
        DailyMetric.objects.create(date=d, computed_at=now, multisig_txs_via_api=0)
        out = StringIO()
        with patch(f"{COMMAND_MODULE}.start_backfill_run") as start_mock:
            call_command(
                "backfill_daily_metrics",
                start=d.isoformat(),
                end=d.isoformat(),
                failed_only=True,
                stdout=out,
            )
        start_mock.assert_not_called()
        self.assertIn("Nothing to do", out.getvalue())


class TestEndToEndEager(BackfillRedisMixin, TestCase):
    """Real populators, real Redis, eager Celery: the whole chunked run
    lands one DailyMetric row per day and a finished manifest."""

    def test_celery_run_writes_rows_and_manifest(self):
        SafeContractFactory()
        today = timezone.now().date()
        start = today - timedelta(days=3)
        end = today - timedelta(days=1)
        out = StringIO()
        call_command(
            "backfill_daily_metrics",
            start=start.isoformat(),
            end=end.isoformat(),
            chunk_days=2,
            wait=60,
            stdout=out,
        )
        self.assertEqual(DailyMetric.objects.count(), 3)
        run = load_backfill_run(latest_backfill_run_id())
        self.assertEqual(run["chunk_count"], 2)
        self.assertEqual(run["written"], 3)
        self.assertEqual(run["failed"], 0)
        self.assertIsNotNone(run["finished_at"])
        self.assertIn("FINISHED", out.getvalue())
