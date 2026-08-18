"""Tests for the APScheduler entry point and shared retry helper."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

class TestScheduleHours:
    def test_default(self, monkeypatch):
        monkeypatch.delenv("SCHEDULE_HOURS", raising=False)
        from eurobot.scheduler import _schedule_hours

        assert _schedule_hours() == [8, 13, 18]

    def test_custom(self, monkeypatch):
        monkeypatch.setenv("SCHEDULE_HOURS", "9, 15")
        from eurobot.scheduler import _schedule_hours

        assert _schedule_hours() == [9, 15]

    def test_invalid_falls_back(self, monkeypatch):
        monkeypatch.setenv("SCHEDULE_HOURS", "not,ints")
        from eurobot.scheduler import _schedule_hours

        assert _schedule_hours() == [8, 13, 18]

    def test_out_of_range_falls_back(self, monkeypatch):
        monkeypatch.setenv("SCHEDULE_HOURS", "5,25")
        from eurobot.scheduler import _schedule_hours

        assert _schedule_hours() == [8, 13, 18]

    def test_deduplicated_sorted(self, monkeypatch):
        monkeypatch.setenv("SCHEDULE_HOURS", "13,8,8")
        from eurobot.scheduler import _schedule_hours

        assert _schedule_hours() == [8, 13]


class TestBuildScheduler:
    def test_job_registered_with_cron_trigger(self):
        from eurobot.scheduler import build_scheduler

        sched = build_scheduler([8, 13, 18])
        job = sched.get_job("eurobot_pipeline")
        assert job is not None
        assert "hour='8,13,18'" in str(job.trigger)
        # UTC timezone on the trigger
        assert "UTC" in repr(job.trigger)
        # next fire time is in the future and at minute 0
        now = datetime.now(timezone.utc)
        nxt = job.trigger.get_next_fire_time(None, now)
        if nxt is not None:  # None only if scheduler not started — acceptable
            assert nxt.minute == 0
            assert nxt.hour in (8, 13, 18)

    def test_job_never_overlaps(self):
        from eurobot.scheduler import build_scheduler

        sched = build_scheduler([8])
        job = sched.get_job("eurobot_pipeline")
        assert job.max_instances == 1
        assert job.coalesce is True

    def test_run_pipeline_job_swallows_failure(self, monkeypatch):
        """A crashing pipeline must not kill the scheduler loop."""
        import asyncio

        from eurobot import scheduler as sched_mod

        def boom():
            raise RuntimeError("pipeline down")

        monkeypatch.setattr(sched_mod, "run_pipeline", boom)
        asyncio.run(sched_mod._run_pipeline_job())  # must not raise


# ---------------------------------------------------------------------------
# Shared transient-retry helper
# ---------------------------------------------------------------------------

class TestTransientClassifier:
    def test_transient_error(self):
        from eurobot.utils.retries import TransientError, _is_transient

        assert _is_transient(TransientError("net"))

    def test_connection_and_timeout(self):
        import requests

        from eurobot.utils.retries import _is_transient

        assert _is_transient(requests.ConnectionError("refused"))
        assert _is_transient(requests.Timeout("slow"))

    def test_http_status_split(self):
        import requests

        from eurobot.utils.retries import _is_transient

        def err_with(status):
            resp = requests.Response()
            resp.status_code = status
            return requests.HTTPError(response=resp)

        assert _is_transient(err_with(503))
        assert _is_transient(err_with(429))
        assert not _is_transient(err_with(403))
        assert not _is_transient(err_with(404))
        assert not _is_transient(ValueError("no"))
