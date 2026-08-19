"""Windmill flow step 4 — publish, archive, clean up.

1. Loads the assembled payload from the MongoDB helper document.
2. POSTs it to the zzboard endpoint (bounded tenacity retries).
3. On success ONLY: archives the final post JSON into the persistent Mongo
   ``posts`` collection, marks news/macro dedup state in PostgreSQL.
4. Drops the Mongo helper collection ``flow_state`` — the inter-step
   handoff data never outlives a completed flow.

This script is fully self-contained: the PostgreSQL schema and the Mongo
handoff helpers are defined below — no shared module and no external SQL
file — so it can be pasted into Windmill and run standalone.

Windmill wiring:
    inputs: postgres_dsn, mongo_uri, run_id, zzboard_api_token,
            zzboard_api_endpoint, max_attempts=3
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime

import asyncpg
import requests as req_mod
from pymongo import AsyncMongoClient
from tenacity import (
    AsyncRetrying,
    before_sleep_log,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("eurobot.windmill.publish")

HTTP_TIMEOUT = 30

# MongoDB collections used by the flow.
DB_NAME = "eurobot"
HELPER_COLLECTION = "flow_state"  # per-run handoff docs, dropped at the end
ARCHIVE_COLLECTION = "posts"  # final published post JSONs (persistent)

# PostgreSQL data-layer schema (series registry, observations, news, dedup
# state, run log, stats). Idempotent — applied on every connect.
SCHEMA_SQL = """
-- eurobot Windmill flow — PostgreSQL schema (data layer)
--
-- Raw + structured data lives here: series registry, observations, news
-- items, dedup state and per-run stats/logs. Applied idempotently at the
-- start of every flow run (CREATE TABLE IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS series (
    tag         TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    source      TEXT NOT NULL,               -- ECB | ESTAT | YAHOO | COMPUTED
    frequency   TEXT NOT NULL,               -- D | M | Q
    unit        TEXT NOT NULL,
    description TEXT,
    first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS observations (
    tag        TEXT NOT NULL REFERENCES series(tag) ON DELETE CASCADE,
    obs_date   DATE   NOT NULL,
    value      DOUBLE PRECISION NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tag, obs_date)
);

CREATE TABLE IF NOT EXISTS news_items (
    hash       TEXT PRIMARY KEY,             -- sha256(title|link)[:16]
    title      TEXT NOT NULL,
    summary    TEXT,
    link       TEXT NOT NULL,
    source     TEXT NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dedup state — written ONLY after a successful publish (publish_archive).
CREATE TABLE IF NOT EXISTS news_seen (
    hash         TEXT PRIMARY KEY,
    first_seen   TIMESTAMPTZ NOT NULL,
    times_posted INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS macro_releases (
    series_key         TEXT PRIMARY KEY,
    latest_obs_period  TEXT,
    release_timestamp  TIMESTAMPTZ,
    first_presented    TIMESTAMPTZ,
    value_hash         TEXT
);

CREATE TABLE IF NOT EXISTS run_log (
    run_id     UUID PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    step       TEXT NOT NULL,                -- fetch | stats | llm | publish | done
    status     TEXT NOT NULL,                -- running | succeeded | failed | skipped
    detail     TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stats (
    run_id        UUID NOT NULL REFERENCES run_log(run_id) ON DELETE CASCADE,
    tag          TEXT NOT NULL,
    latest_value DOUBLE PRECISION,
    latest_date  DATE,
    delta_prev   DOUBLE PRECISION,
    delta_prev_pct DOUBLE PRECISION,
    yoy          DOUBLE PRECISION,
    yoy_pct      DOUBLE PRECISION,
    summary      TEXT,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, tag)
);

CREATE INDEX IF NOT EXISTS idx_observations_tag_date ON observations (tag, obs_date DESC);
CREATE INDEX IF NOT EXISTS idx_stats_run ON stats (run_id);
"""


def utc_now_naive() -> datetime:
    """Current UTC time as a naive datetime (PyMongo convention)."""
    return datetime.now(UTC).replace(tzinfo=None)


async def pg_connect(postgres_dsn: str) -> asyncpg.Connection:
    """Open a raw asyncpg connection (timezone UTC)."""
    conn = await asyncpg.connect(postgres_dsn)
    await conn.execute("SET TIME ZONE 'UTC'")
    return conn


def mongo_client(mongo_uri: str) -> AsyncMongoClient:
    """Build the async Mongo client (caller must ``await client.close()``)."""
    return AsyncMongoClient(mongo_uri)


class FlowStore:
    """Helper collection that passes run documents between flow steps.

    One document per ``run_id``::

        { _id: run_id, step: "fetch", docs: {...}, created_at, updated_at }

    ``docs`` holds whatever the previous step produced for the next one.
    """

    def __init__(self, client: AsyncMongoClient, run_id: str):
        self._coll = client[DB_NAME][HELPER_COLLECTION]
        self.run_id = run_id

    async def create(self, first_step: str = "fetch") -> None:
        now = utc_now_naive()
        await self._coll.insert_one(
            {
                "_id": self.run_id,
                "step": first_step,
                "docs": {},
                "created_at": now,
                "updated_at": now,
            }
        )

    async def update(self, step: str, docs: dict) -> None:
        """Replace the carried docs and advance the step marker."""
        await self._coll.update_one(
            {"_id": self.run_id},
            {
                "$set": {
                    "step": step,
                    "docs": docs,
                    "updated_at": utc_now_naive(),
                }
            },
        )

    async def load(self) -> dict | None:
        doc = await self._coll.find_one({"_id": self.run_id})
        if doc is not None:
            doc.pop("_id", None)
        return doc

    @staticmethod
    async def drop(client: AsyncMongoClient) -> None:
        """Drop the helper collection (called after successful publish)."""
        await client[DB_NAME][HELPER_COLLECTION].drop()
        logger.info("Mongo helper collection '%s' dropped", HELPER_COLLECTION)


class PublishError(Exception):
    """zzboard POST failed — worth retrying (network/5xx)."""


class PermanentPublishError(Exception):
    """zzboard rejected the payload (4xx) — do NOT retry."""


def _is_retryable(exc: BaseException) -> bool:
    return isinstance(exc, PublishError) and not isinstance(exc, PermanentPublishError)


async def _publish(payload: dict, api_token: str, endpoint: str) -> None:
    """POST the payload; raise PublishError (retryable) or PermanentPublishError."""

    def _post() -> None:
        headers = {"X-API-Key": api_token, "Content-Type": "application/json"}
        try:
            resp = req_mod.post(
                endpoint, json=payload, headers=headers, timeout=HTTP_TIMEOUT
            )
        except req_mod.RequestException as exc:
            raise PublishError(f"network failure: {exc}") from exc
        if resp.status_code in (200, 201):
            return
        if 400 <= resp.status_code < 500 and resp.status_code != 429:
            raise PermanentPublishError(f"HTTP {resp.status_code}: {resp.text[:300]}")
        raise PublishError(f"HTTP {resp.status_code}: {resp.text[:300]}")

    await asyncio.to_thread(_post)


async def _main(
    postgres_dsn: str,
    mongo_uri: str,
    run_id: str,
    zzboard_api_token: str,
    zzboard_api_endpoint: str = "https://roll.by.gg8.eu/api/new",
    max_attempts: int = 3,
    dry_run: bool = False,
) -> dict:
    """Publish → archive → mark dedup → drop the helper collection."""
    conn = await pg_connect(postgres_dsn)
    client = mongo_client(mongo_uri)
    try:
        await conn.execute(SCHEMA_SQL)  # idempotent
        store = FlowStore(client, run_id)
        state = await store.load()
        if state is None:
            raise RuntimeError(f"flow helper document for run {run_id} not found")
        carried = state["docs"]
        if carried.get("skip"):
            await FlowStore.drop(client)
            await conn.execute(
                "UPDATE run_log SET step='done', status='skipped',"
                " detail='nothing fresh', updated_at=now() WHERE run_id=$1",
                uuid.UUID(run_id),
            )
            return {"run_id": run_id, "published": False, "skipped": True}

        payload = carried["payload"]
        await conn.execute(
            "UPDATE run_log SET step='publish', status='running', updated_at=now()"
            " WHERE run_id=$1",
            uuid.UUID(run_id),
        )

        # ── 1. Publish (bounded retries; permanent errors abort) ───────
        if dry_run:
            logger.warning("DRY RUN — skipping zzboard POST")
        else:
            try:
                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(max_attempts),
                    wait=wait_exponential(multiplier=2, min=5, max=30),
                    retry=retry_if_exception(_is_retryable),
                    before_sleep=before_sleep_log(logger, logging.WARNING),
                    reraise=True,
                ):
                    with attempt:
                        await _publish(payload, zzboard_api_token, zzboard_api_endpoint)
            except PermanentPublishError:
                raise
            except Exception as exc:
                logger.error("Publish failed after %d attempts: %s", max_attempts, exc)
                raise

        # ── 2. Archive final JSON to the persistent posts collection ───
        archive_doc = {
            "run_id": run_id,
            "published_at": utc_now_naive(),
            "endpoint": zzboard_api_endpoint if not dry_run else None,
            "dry_run": dry_run,
            "payload": payload,
        }
        result = await client[DB_NAME][ARCHIVE_COLLECTION].insert_one(archive_doc)
        logger.info(
            "Post archived to '%s' (_id=%s)", ARCHIVE_COLLECTION, result.inserted_id
        )

        # ── 3. Mark dedup state (only after successful publish) ────────
        for n in carried["fresh_news"]:
            await conn.execute(
                """
                INSERT INTO news_seen (hash, first_seen, times_posted)
                VALUES ($1, now(), 1)
                ON CONFLICT (hash) DO UPDATE SET times_posted = news_seen.times_posted + 1
                """,
                n["hash"],
            )

        for tag in carried["fresh_tags"]:
            stat = next((s for s in carried["stats"] if s["tag"] == tag), None)
            if stat is None:
                continue
            await conn.execute(
                """
                INSERT INTO macro_releases
                    (series_key, latest_obs_period, release_timestamp,
                     first_presented, value_hash)
                VALUES ($1, $2, now(), now(), $3)
                ON CONFLICT (series_key) DO UPDATE SET
                    latest_obs_period = EXCLUDED.latest_obs_period,
                    release_timestamp = now(),
                    first_presented = EXCLUDED.first_presented,
                    value_hash = EXCLUDED.value_hash
                """,
                tag,
                stat["latest_date"],
                _series_hash(tag, carried),
            )

        await conn.execute(
            "UPDATE run_log SET step='done', status='succeeded', updated_at=now()"
            " WHERE run_id=$1",
            uuid.UUID(run_id),
        )

        # ── 4. Drop the helper collection — flow complete ──────────────
        await FlowStore.drop(client)

        logger.info(
            "publish_archive done (run %s): published=%s, helper dropped",
            run_id,
            not dry_run,
        )
        return {
            "run_id": run_id,
            "published": not dry_run,
            "archived_doc_id": str(result.inserted_id),
            "title": payload.get("title", "?"),
        }
    finally:
        await conn.close()
        await client.close()


def main(
    postgres_dsn: str,
    mongo_uri: str,
    run_id: str,
    zzboard_api_token: str,
    zzboard_api_endpoint: str = "https://roll.by.gg8.eu/api/new",
    max_attempts: int = 3,
    dry_run: bool = False,
) -> dict:
    """Sync Windmill entrypoint (workers call ``main`` without awaiting)."""
    return asyncio.run(
        _main(
            postgres_dsn,
            mongo_uri,
            run_id,
            zzboard_api_token,
            zzboard_api_endpoint,
            max_attempts,
            dry_run,
        )
    )


def _series_hash(tag: str, carried: dict) -> str | None:
    """Deterministic per-series value hash carried from step 2, if present."""
    h = carried.get("value_hashes", {}).get(tag)
    return h
