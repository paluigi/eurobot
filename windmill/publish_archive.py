"""Windmill flow step 4 — publish, archive, clean up.

1. Loads the assembled payload from the MongoDB helper document.
2. POSTs it to the zzboard endpoint (bounded tenacity retries).
3. On success ONLY: archives the final post JSON into the persistent Mongo
   ``posts`` collection, marks news/macro dedup state in PostgreSQL.
4. Drops the Mongo helper collection ``flow_state`` — the inter-step
   handoff data never outlives a completed flow.

Windmill wiring:
    inputs: postgres_dsn, mongo_uri, run_id, zzboard_api_token,
            zzboard_api_endpoint, max_attempts=3
"""

from __future__ import annotations

import asyncio
import logging
import uuid

import requests as req_mod
from common import (
    ARCHIVE_COLLECTION,
    DB_NAME,
    FlowStore,
    mongo_client,
    pg_connect,
    utc_now_naive,
)
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


async def main(
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


def _series_hash(tag: str, carried: dict) -> str | None:
    """Deterministic per-series value hash carried from step 2, if present."""
    h = carried.get("value_hashes", {}).get(tag)
    return h
