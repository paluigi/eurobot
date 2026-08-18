"""Shared helpers for the eurobot Windmill flow scripts.

Every flow script imports this module. It provides:

- ``pg_connect`` / ``pg_apply_schema`` — asyncpg connection + idempotent
  schema application (raw asyncpg ``execute`` handles the multi-statement
  schema file as a single batch).
- ``mongo_client`` — native PyMongo ≥4.9 ``AsyncMongoClient`` (NOT Motor).
- ``FlowStore`` — the MongoDB helper collection used to pass intermediate
  documents between flow steps. The helper collection is created when a run
  starts and DROPPED by the final step once the post is published and
  archived (it never outlives a successful flow).
- ``utc_now_naive`` — Mongo stores naive UTC; always write naive datetimes.

Connection strings come from Windmill variables/resources passed as script
arguments (``postgres_dsn``, ``mongo_uri``), never hardcoded.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
from pymongo import AsyncMongoClient

logger = logging.getLogger("eurobot.windmill")

SCHEMA_PATH = Path(__file__).with_name("schema.sql")

# MongoDB collections used by the flow.
DB_NAME = "eurobot"
HELPER_COLLECTION = "flow_state"  # per-run handoff docs, dropped at the end
ARCHIVE_COLLECTION = "posts"  # final published post JSONs (persistent)


def utc_now_naive() -> datetime:
    """Current UTC time as a naive datetime (PyMongo convention)."""
    return datetime.now(UTC).replace(tzinfo=None)


async def pg_connect(postgres_dsn: str) -> asyncpg.Connection:
    """Open a raw asyncpg connection (timezone UTC)."""
    conn = await asyncpg.connect(postgres_dsn)
    await conn.execute("SET TIME ZONE 'UTC'")
    return conn


async def pg_apply_schema(conn: asyncpg.Connection) -> None:
    """Apply windmill/schema.sql (idempotent, multi-statement batch)."""
    sql = SCHEMA_PATH.read_text()
    await conn.execute(sql)
    logger.info("Postgres schema applied (idempotent)")


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
