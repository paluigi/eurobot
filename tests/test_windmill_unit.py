"""Unit tests for the Windmill flow scripts (no DB required).

Covers the pure logic: retry classification, LLM payload assembly with a
fake cascade, publish error semantics and the FlowStore doc shape (with a
stub client).
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

WINDMILL_DIR = Path(__file__).resolve().parent.parent / "windmill"
sys.path.insert(0, str(WINDMILL_DIR))

import llm_report
import publish_archive
from common import FlowStore

# ---------------------------------------------------------------------------
# Retry classification (publish step)
# ---------------------------------------------------------------------------


class TestPublishRetryClassification:
    def test_permanent_not_retryable(self):
        assert not publish_archive._is_retryable(
            publish_archive.PermanentPublishError("HTTP 400")
        )
        assert not publish_archive._is_retryable(
            publish_archive.PermanentPublishError("HTTP 401")
        )

    def test_transient_retryable(self):
        assert publish_archive._is_retryable(publish_archive.PublishError("HTTP 503"))
        assert publish_archive._is_retryable(
            publish_archive.PublishError("network failure")
        )

    def test_other_errors_not_retryable(self):
        assert not publish_archive._is_retryable(ValueError("boom"))


# ---------------------------------------------------------------------------
# Transient retry helper (shared with the classic pipeline)
# ---------------------------------------------------------------------------


class TestTransientRetryHelper:
    def test_retries_then_succeeds(self):
        from eurobot.utils.retries import call_with_retries

        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            if calls["n"] < 3:
                raise llm_report.StageFailure("transient-ish")
            return "ok"

        # StageFailure is NOT transient-classified -> would fail; use TransientError
        from eurobot.utils.retries import TransientError

        def flaky2():
            calls["m"] = calls.get("m", 0) + 1
            if calls["m"] < 3:
                raise TransientError("boom")
            return "ok"

        assert call_with_retries(flaky2) == "ok"
        assert calls["m"] == 3

    def test_bounded_no_infinite_loop(self):
        from eurobot.utils.retries import TransientError, call_with_retries

        calls = {"n": 0}

        def always_fails():
            calls["n"] += 1
            raise TransientError("always")

        with pytest.raises(TransientError):
            call_with_retries(always_fails, max_attempts=3)
        assert calls["n"] == 3  # hard cap, no infinite loop


# ---------------------------------------------------------------------------
# LLM payload assembly with a fake cascade
# ---------------------------------------------------------------------------

CARRIED = {
    "skip": False,
    "data_summaries": [
        "[DATA_CISS] CISS: latest 0.12 Δ +0.01 (unit: index)",
        "[DATA_EUR_USD] EUR/USD: latest 1.08 Δ -0.01 (unit: USD per EUR)",
    ],
    "news_summaries": [
        "[NEWS_001] ECB Press: ECB holds rates — summary",
    ],
    "fresh_news": [
        {
            "hash": "abc123",
            "tag": "NEWS_001",
            "title": "ECB holds rates",
            "summary": "The ECB kept rates unchanged.",
            "link": "https://ecb.europa.eu/1",
            "source": "ECB Press",
        },
    ],
    "fresh_tags": ["CISS", "EUR_USD"],
    "value_hashes": {"CISS": "h1", "EUR_USD": "h2"},
    "charts": {
        "CISS": {"title": "CISS", "spec": {"data": []}},
        "EUR_USD": {"title": "EUR/USD", "spec": {"data": []}},
    },
    "tables": {
        "CISS": {"title": "CISS", "rows": [["v", "0.12"]]},
        "EUR_USD": {"title": "EUR/USD", "rows": [["v", "1.08"]]},
    },
    "stats": [
        {"tag": "CISS", "summary": "CISS: latest 0.12", "latest_date": "2026-08-15"},
        {
            "tag": "EUR_USD",
            "summary": "EUR/USD: latest 1.08",
            "latest_date": "2026-08-15",
        },
    ],
}

FAKE_RESPONSES = [
    '["DATA_CISS", "NEWS_001"]',  # stage 1 selection
    '{"title": "Stress calm", "summary": "s", "tags": ["t"],'
    ' "content_markdown": "Body about CISS."}',  # stage 2 draft
    '{"approved": true, "errors": [], "corrected_markdown": null}',  # stage 3
]


class TestLlmReportAssembly:
    def test_full_assembly(self):
        """_report with monkeypatched cascade → payload shape."""
        queue = list(FAKE_RESPONSES)

        async def fake_query(user_prompt, system_prompt, cascade_config):
            return queue.pop(0)

        orig = llm_report.query_llm_dict
        llm_report.query_llm_dict = fake_query
        try:
            payload = asyncio.run(
                llm_report._report(CARRIED, {"providers": {}, "cascades": {}}, {})
            )
        finally:
            llm_report.query_llm_dict = orig

        assert payload["title"] == "Stress calm"
        assert payload["author"] == "eurobot"
        assert "CISS" in payload["content_markdown"]
        # only selected series' charts/tables are included
        assert [c["title"] for c in payload["charts"]] == ["CISS"]
        assert [t["title"] for t in payload["tables"]] == ["CISS"]
        # links built from selected news
        assert payload["links"][0]["url"] == "https://ecb.europa.eu/1"

    def test_stage1_failure_raises(self):
        async def bad_query(user_prompt, system_prompt, cascade_config):
            return "no json here at all"

        orig = llm_report.query_llm_dict
        llm_report.query_llm_dict = bad_query
        try:
            with pytest.raises(llm_report.StageFailure):
                asyncio.run(
                    llm_report._report(CARRIED, {"providers": {}, "cascades": {}}, {})
                )
        finally:
            llm_report.query_llm_dict = orig

    def test_rejected_draft_redrafts(self):
        """Unfixable self-review rejection pops the draft checkpoint."""
        responses = [
            '["DATA_CISS", "NEWS_001"]',
            '{"title": "T", "summary": "s", "tags": [], "content_markdown": "B."}',
            '{"approved": false, "errors": ["x"], "corrected_markdown": null}',
        ]
        checkpoints: dict = {}

        async def query(user_prompt, system_prompt, cascade_config):
            return responses.pop(0)

        orig = llm_report.query_llm_dict
        llm_report.query_llm_dict = query
        try:
            with pytest.raises(llm_report.StageFailure):
                asyncio.run(llm_report._report(CARRIED, {}, checkpoints))
            # selection kept, draft dropped for the next attempt
            assert checkpoints["selected_ids"] == ["DATA_CISS", "NEWS_001"]
            assert "draft" not in checkpoints
        finally:
            llm_report.query_llm_dict = orig


# ---------------------------------------------------------------------------
# FlowStore doc shape (stub async client)
# ---------------------------------------------------------------------------


class StubColl:
    def __init__(self):
        self.docs: dict = {}
        self.dropped = False

    async def insert_one(self, doc):
        self.docs[doc["_id"]] = doc
        return type("R", (), {"inserted_id": doc["_id"]})()

    async def update_one(self, q, update):
        doc = self.docs[q["_id"]]
        doc.update(update["$set"])

    async def find_one(self, q):
        return self.docs.get(q["_id"])

    async def drop(self):
        self.dropped = True
        self.docs.clear()


class StubDb:
    def __init__(self):
        self.flow_state = StubColl()
        self.posts = StubColl()

    def __getitem__(self, name):
        return {"flow_state": self.flow_state, "posts": self.posts}[name]


class StubClient:
    def __init__(self):
        self.db = StubDb()

    def __getitem__(self, name):
        return self.db

    async def close(self):
        pass


class TestFlowStore:
    def test_lifecycle(self):
        client = StubClient()

        async def scenario():
            store = FlowStore(client, "run-1")
            await store.create()
            await store.update("stats", {"fresh_tags": ["CISS"]})
            doc = await store.load()
            await FlowStore.drop(client)
            return doc

        doc = asyncio.run(scenario())
        assert doc["step"] == "stats"
        assert doc["docs"]["fresh_tags"] == ["CISS"]
        assert client.db.flow_state.dropped is True
