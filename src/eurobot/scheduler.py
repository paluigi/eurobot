"""APScheduler-based entry point — replaces the supercronic cron container.

Runs the eurobot pipeline on a cron schedule (default 08:00/13:00/18:00 UTC,
same cadence the crontab used) inside a long-lived asyncio event loop,
mirroring the scheduler pattern of congiuntura-live.

Set ``RUN_ON_START=1`` to trigger one run immediately at startup (useful for
manual ``docker compose run`` invocations), and ``SCHEDULE_HOURS=8,13,18`` to
change the daily run hours.
"""

from __future__ import annotations

import asyncio
import logging
import os

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from eurobot import config
from eurobot.main import run as run_pipeline

logger = logging.getLogger("eurobot.scheduler")

JOB_ID = "eurobot_pipeline"


def _schedule_hours() -> list[int]:
    """Parse SCHEDULE_HOURS (comma-separated ints, UTC)."""
    raw = os.getenv("SCHEDULE_HOURS", "8,13,18")
    try:
        hours = sorted({int(h.strip()) for h in raw.split(",") if h.strip()})
    except ValueError:
        logger.error(
            "SCHEDULE_HOURS=%r is not comma-separated ints — using default", raw
        )
        hours = [8, 13, 18]
    if not hours or any(h < 0 or h > 23 for h in hours):
        logger.error("SCHEDULE_HOURS=%r out of range — using default", raw)
        hours = [8, 13, 18]
    return hours


async def _run_pipeline_job() -> None:
    """Job wrapper — runs the (sync) pipeline in a worker thread.

    The pipeline's own tenacity loop already bounds in-run retries; a raise
    here would only propagate to APScheduler's error logger, so failures are
    logged and swallowed to keep the scheduler alive.
    """
    logger.info("Scheduled run triggered")
    try:
        exit_code = await asyncio.to_thread(run_pipeline)
        logger.info("Scheduled run finished with exit code %d", exit_code)
    except Exception:
        logger.exception("Scheduled run crashed")


def build_scheduler(hours: list[int] | None = None) -> AsyncIOScheduler:
    """Build the scheduler with one cron job at the given UTC hours."""
    if hours is None:
        hours = _schedule_hours()
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        _run_pipeline_job,
        trigger=CronTrigger(
            hour=",".join(str(h) for h in hours), minute=0, timezone="UTC"
        ),
        id=JOB_ID,
        replace_existing=True,
        # Never overlap runs; skip missed fires (e.g. laptop asleep) but allow
        # a small grace window so a missed slot still fires shortly after.
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    return scheduler


async def main() -> None:
    config.setup_logging()
    hours = _schedule_hours()
    scheduler = build_scheduler(hours)
    scheduler.start()
    logger.info(
        "eurobot scheduler started — daily runs at %s UTC (job '%s')",
        ", ".join(f"{h:02d}:00" for h in hours),
        JOB_ID,
    )

    if os.getenv("RUN_ON_START", "").lower() in ("1", "true", "yes"):
        logger.info("RUN_ON_START set — running one pipeline execution now")
        await _run_pipeline_job()

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutting down scheduler")
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
