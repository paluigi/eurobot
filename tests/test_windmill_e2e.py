"""E2E test — full Windmill flow against real PostgreSQL + MongoDB.

Requires Docker. Starts throwaway ``postgres:16`` and ``mongo:7``
containers, runs the four flow scripts end to end (fetchers monkeypatched
to deterministic fakes, LLM monkeypatched, publish in dry_run mode), and
asserts:

- PostgreSQL: schema applied, observations/stats/run_log rows written,
  dedup marked only after publish.
- MongoDB: helper doc carries state between steps; final payload archived
  to ``posts``; ``flow_state`` dropped at the end.
- Second run with the same data: nothing fresh → skip path → helper still
  dropped.
- Always-failing publish: bounded retries, exactly max_attempts POSTs.

Run via pytest:   pytest tests/test_windmill_e2e.py -v
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

WINDMILL_DIR = Path(__file__).resolve().parent.parent / "windmill"
sys.path.insert(0, str(WINDMILL_DIR))

PG_PORT = 55432
MONGO_PORT = 57017
PG_CONTAINER = "eurobot-e2e-pg"
MONGO_CONTAINER = "eurobot-e2e-mongo"
PG_DSN = "postgres://eurobot:eurobot@localhost:" + str(PG_PORT) + "/eurobot"
MONGO_URI = "mongodb://localhost:" + str(MONGO_PORT)


def _docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True, capture_output=True)


def _docker_available() -> bool:
    try:
        subprocess.run(["docker", "info"], check=True, capture_output=True, timeout=30)
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def databases():
    _docker("rm", "-f", PG_CONTAINER)
    _docker("rm", "-f", MONGO_CONTAINER)
    _docker(
        "run",
        "-d",
        "--name",
        PG_CONTAINER,
        "-e",
        "POSTGRES_USER=eurobot",
        "-e",
        "POSTGRES_PASSWORD=eurobot",
        "-e",
        "POSTGRES_DB=eurobot",
        "-p",
        str(PG_PORT) + ":5432",
        "postgres:16",
    )
    _docker(
        "run",
        "-d",
        "--name",
        MONGO_CONTAINER,
        "-p",
        str(MONGO_PORT) + ":27017",
        "mongo:7",
    )
    try:
        import pymongo

        for _ in range(60):
            try:
                pymongo.MongoClient(
                    MONGO_URI, serverSelectionTimeoutMS=1000
                ).admin.command("ping")
                break
            except Exception:
                time.sleep(1)

        import asyncpg

        for _ in range(60):
            try:
                asyncio.run(asyncpg.connect(PG_DSN))
                break
            except Exception:
                time.sleep(1)
        yield
    finally:
        _docker("rm", "-f", PG_CONTAINER)
        _docker("rm", "-f", MONGO_CONTAINER)


# ---------------------------------------------------------------------------
# Deterministic fakes for the fetchers and the LLM
# ---------------------------------------------------------------------------


def _fake_fetchers(monkeypatch, seed: int = 42):
    """Deterministic fakes. The SAME seed always yields the SAME data, so a
    second flow run sees 'nothing fresh' (dedup skip path). A DIFFERENT seed
    produces new values → fresh series."""
    import numpy as np
    import pandas as pd

    from eurobot.fetchers.market_fetcher import MarketSpec

    def make_dates():
        return pd.date_range("2026-05-01", periods=60, freq="D")

    def fake_fetch_all_macro():
        rng = np.random.default_rng(seed)
        dates = make_dates()
        return [
            {
                "tag": "CISS",
                "title": "CISS",
                "unit": "index",
                "frequency": "D",
                "description": "stress",
                "spec": None,
                "series": pd.Series(rng.random(60).round(4), index=dates),
            },
            {
                "tag": "IT_10Y_YIELD",
                "title": "IT 10Y",
                "unit": "%",
                "frequency": "M",
                "description": "it",
                "spec": None,
                "series": pd.Series((3.5 + rng.random(60) / 5).round(4), index=dates),
            },
            {
                "tag": "DE_10Y_YIELD",
                "title": "DE 10Y",
                "unit": "%",
                "frequency": "M",
                "description": "de",
                "spec": None,
                "series": pd.Series((2.3 + rng.random(60) / 10).round(4), index=dates),
            },
        ]

    def fake_fetch_all_markets(range_days="6mo"):
        rng = np.random.default_rng(seed + 1)
        return [
            {
                "tag": "FTSE_MIB",
                "title": "FTSE MIB",
                "unit": "index points",
                "frequency": "D",
                "spec": MarketSpec("FTSE_MIB", "FTSEMIB.MI", "FTSE MIB", "idx"),
                "series": pd.Series(
                    (38000 + rng.random(60) * 500).round(2), index=make_dates()
                ),
            }
        ]

    class FakeNews:
        def __init__(self, tag, title, summary, link, source, hsh):
            self.tag, self.title, self.summary = tag, title, summary
            self.link, self.source, self.hash = link, source, hsh

        def to_prompt_line(self):
            return f"[{self.tag}] {self.source}: {self.title} — {self.summary[:50]}"

    def fake_get_latest_news(max_items=15):
        return [
            FakeNews(
                "NEWS_001",
                "ECB holds rates",
                "Council keeps rates.",
                "https://ecb.europa.eu/pr1",
                "ECB Press",
                "aaaa1111bbbb2222",
            )
        ]

    import fetch_data

    monkeypatch.setattr(fetch_data, "fetch_all_macro", fake_fetch_all_macro)
    monkeypatch.setattr(fetch_data, "fetch_all_markets", fake_fetch_all_markets)
    monkeypatch.setattr(fetch_data, "get_latest_news", fake_get_latest_news)


def _fake_llm(monkeypatch):
    import llm_report

    # keys normally come from Windmill variables — stub the fetcher
    monkeypatch.setattr(llm_report.wmill, "get_variable", lambda path: "test-key")

    responses = [
        '["DATA_CISS", "DATA_IT_10Y_YIELD", "NEWS_001"]',
        '{"title": "Italian yields steady", "summary": "sum", "tags": ["bonds"],'
        ' "content_markdown": "Italian yields held while stress stayed low."}',
        '{"approved": true, "errors": [], "corrected_markdown": null}',
    ]

    async def fake_query(user_prompt, system_prompt, cascade_config):
        assert isinstance(cascade_config, dict)
        return responses.pop(0)

    monkeypatch.setattr(llm_report, "query_llm_dict", fake_query)


def _run_flow(dry_run: bool = True) -> dict:
    import compute_stats
    import fetch_data
    import llm_report
    import publish_archive

    r1 = fetch_data.main(PG_DSN, MONGO_URI)
    r2 = compute_stats.main(PG_DSN, MONGO_URI, r1["run_id"])
    r3 = llm_report.main(MONGO_URI, r1["run_id"])
    r4 = publish_archive.main(
        PG_DSN,
        MONGO_URI,
        r1["run_id"],
        zzboard_api_token="test-token",
        zzboard_api_endpoint="http://localhost:1/never-called",
        dry_run=dry_run,
    )
    return {"fetch": r1, "stats": r2, "llm": r3, "publish": r4}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _docker_available(), reason="Docker not available")
def test_full_flow(databases, monkeypatch):
    import asyncpg

    _fake_fetchers(monkeypatch)
    _fake_llm(monkeypatch)

    results = _run_flow(dry_run=True)
    run_id = results["fetch"]["run_id"]

    assert results["stats"]["skip"] is False
    assert results["stats"]["fresh_series"] >= 2
    assert results["llm"]["skip"] is False
    assert results["publish"]["published"] is False  # dry run
    assert results["publish"]["archived_doc_id"]

    async def check_pg():
        conn = await asyncpg.connect(PG_DSN)
        try:
            obs = await conn.fetchval("SELECT count(*) FROM observations")
            stats = await conn.fetchval(
                "SELECT count(*) FROM stats WHERE run_id=$1", uuid.UUID(run_id)
            )
            runlog = await conn.fetchrow(
                "SELECT step, status FROM run_log WHERE run_id=$1", uuid.UUID(run_id)
            )
            seen = await conn.fetchval("SELECT count(*) FROM news_seen")
            macro = await conn.fetchval("SELECT count(*) FROM macro_releases")
            return obs, stats, runlog, seen, macro
        finally:
            await conn.close()

    obs, stats_n, runlog, seen, macro = asyncio.run(check_pg())
    assert obs >= 120, "observations upserted"
    assert stats_n >= 2, "stats rows written"
    assert runlog["step"] == "done" and runlog["status"] == "succeeded"
    assert seen == 1, "news marked seen only after publish"
    assert macro >= 2, "macro releases marked"

    from pymongo import AsyncMongoClient

    async def check_mongo():
        client = AsyncMongoClient(MONGO_URI)
        try:
            colls = await client["eurobot"].list_collection_names()
            posts = await client["eurobot"]["posts"].count_documents({})
            doc = await client["eurobot"]["posts"].find_one({})
            return colls, posts, doc
        finally:
            await client.close()

    colls, posts, doc = asyncio.run(check_mongo())
    assert "flow_state" not in colls, "helper collection dropped"
    assert posts == 1
    assert doc["run_id"] == run_id
    assert doc["dry_run"] is True
    assert doc["payload"]["title"] == "Italian yields steady"
    assert any(c["title"] == "CISS" for c in doc["payload"]["charts"])

    # ── Second run: same data → nothing fresh → skip path ─────────────
    results2 = _run_flow(dry_run=True)
    assert results2["stats"]["skip"] is True
    assert results2["llm"]["skip"] is True
    assert results2["publish"]["skipped"] is True

    async def colls_now():
        client = AsyncMongoClient(MONGO_URI)
        try:
            return await client["eurobot"].list_collection_names()
        finally:
            await client.close()

    colls2 = asyncio.run(colls_now())
    assert "flow_state" not in colls2, "helper dropped on skip path too"

    # news dedup: the same item must not be re-marked (times_posted stays 1)
    async def seen_twice():
        conn = await asyncpg.connect(PG_DSN)
        try:
            return await conn.fetchval(
                "SELECT times_posted FROM news_seen WHERE hash=$1", "aaaa1111bbbb2222"
            )
        finally:
            await conn.close()

    assert asyncio.run(seen_twice()) == 1


@pytest.mark.skipif(not _docker_available(), reason="Docker not available")
def test_publish_bounded_retries(databases, monkeypatch):
    """Always-failing publish (transient 503) stops at max_attempts."""
    import fetch_data
    import llm_report
    import publish_archive

    _fake_fetchers(monkeypatch, seed=99)  # different data → fresh series
    _fake_llm(monkeypatch)

    calls = {"n": 0}

    def always_503(*args, **kwargs):
        calls["n"] += 1

        class R:
            status_code = 503
            text = "service unavailable"

        return R()

    monkeypatch.setattr(publish_archive.req_mod, "post", always_503)

    def flow():
        import compute_stats

        r1 = fetch_data.main(PG_DSN, MONGO_URI)
        compute_stats.main(PG_DSN, MONGO_URI, r1["run_id"])
        llm_report.main(MONGO_URI, r1["run_id"])
        return r1

    r1 = flow()

    try:
        publish_archive.main(
            PG_DSN,
            MONGO_URI,
            r1["run_id"],
            zzboard_api_token="t",
            zzboard_api_endpoint="http://x/",
            dry_run=False,
            max_attempts=3,
        )
        outcome = "no-error"
    except Exception as exc:  # PublishError after retries exhausted
        outcome = "stopped:" + type(exc).__name__
    assert outcome.startswith("stopped:"), outcome
    assert calls["n"] == 3, "exactly max_attempts POSTs — bounded, no infinite loop"

    # helper collection still there — publish failed, no cleanup
    from pymongo import AsyncMongoClient

    async def helper_exists():
        client = AsyncMongoClient(MONGO_URI)
        try:
            return "flow_state" in await client["eurobot"].list_collection_names()
        finally:
            await client.close()

    assert asyncio.run(helper_exists()) is True
