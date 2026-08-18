"""Tests for the retry/checkpoint logic around main._report_and_publish.

Verifies that tenacity retries pick up from the last valid step: stage
results are cached in ``checkpoints`` and never re-computed, an unfixable
self-review rejection redrafts (keeping the selection), and a failed
publish raises ``StageFailure`` without discarding earlier stages.
"""

import pytest

from eurobot import main
from eurobot.main import StageFailure, _report_and_publish


class _Stat:
    def __init__(self, tag, summary):
        self.tag = tag
        self.summary = summary


class _News:
    def __init__(self, tag):
        self.tag = tag
        self.title = f"Headline {tag}"
        self.link = "https://example.org"
        self.source = "TestFeed"

    def to_prompt_line(self):
        return f"[{self.tag}] {self.source}: {self.title}"


@pytest.fixture
def pipeline_inputs():
    return {
        "fresh_data": [{"tag": "CISS"}],
        "fresh_news": [_News("NEWS_001")],
        "data_summaries": ["[DATA_CISS] CISS index, latest 0.10 (unit: index)"],
        "news_summaries": ["[NEWS_001] TestFeed: Headline NEWS_001"],
        "all_stats": [_Stat("CISS", "CISS index, latest 0.10")],
        "charts_pool": {"CISS": {"title": "CISS", "spec": {"x": [1], "y": [2]}}},
        "tables_pool": {"CISS": {"title": "CISS", "rows": [["a", "b"]]}},
    }


@pytest.fixture(autouse=True)
def stub_side_effects(monkeypatch, tmp_path):
    """Keep the test off the network, the real DB and the posts dir."""
    monkeypatch.setattr(main, "save_payload", lambda payload, posts_dir=None: str(tmp_path / "post.json"))
    monkeypatch.setattr(main.dedup, "mark_news_seen", lambda news: None)
    monkeypatch.setattr(main.dedup, "mark_macro_presented", lambda items: None)


SELECTION = '["DATA_CISS"]'
DRAFT = '{"title": "T", "summary": "s", "tags": ["x"], "content_markdown": "body"}'
APPROVE = '{"approved": true}'
REJECT = '{"approved": false}'


def _run(monkeypatch, inputs, checkpoints, responses, publish_results):
    """Run one attempt with a scripted query_llm.

    ``responses`` is consumed in stage order (selection, draft, review);
    ``publish_results`` is consumed one per publish call. Values may be
    exceptions, which are raised. Returns (exit_code, call_log).
    """
    calls = {"llm": [], "publish": 0}
    queue = list(responses)

    def fake_query(user_prompt, system_prompt=""):
        calls["llm"].append(user_prompt[:16])
        item = queue.pop(0) if queue else "[]"
        if isinstance(item, Exception):
            raise item
        return item

    def fake_publish(payload):
        idx = calls["publish"]
        calls["publish"] += 1
        return publish_results[idx] if idx < len(publish_results) else True

    monkeypatch.setattr(main, "query_llm", fake_query)
    monkeypatch.setattr(main, "publish_payload", fake_publish)

    code = _report_and_publish(
        inputs["fresh_data"], inputs["fresh_news"],
        inputs["data_summaries"], inputs["news_summaries"],
        inputs["all_stats"], inputs["charts_pool"], inputs["tables_pool"],
        checkpoints,
    )
    return code, calls


def test_stage_results_are_checkpointed(monkeypatch, pipeline_inputs):
    """A second attempt with the same checkpoints makes no LLM calls."""
    checkpoints = {}
    code, calls = _run(
        monkeypatch, pipeline_inputs, checkpoints,
        [SELECTION, DRAFT, APPROVE], [True],
    )
    assert code == 0
    assert len(calls["llm"]) == 3

    assert checkpoints["selected_ids"] == ["DATA_CISS"]
    assert checkpoints["draft"]["title"] == "T"
    assert checkpoints["reviewed"] is True

    # Second attempt: everything LLM-side is checkpointed → zero LLM calls,
    # only the publish POST is re-attempted.
    code2, calls2 = _run(monkeypatch, pipeline_inputs, checkpoints, [], [True])
    assert code2 == 0
    assert calls2["llm"] == []
    assert calls2["publish"] == 1


def test_review_rejection_redrafts_keeps_selection(monkeypatch, pipeline_inputs):
    """Unfixable rejection drops the draft but keeps the selection."""
    checkpoints = {}
    with pytest.raises(StageFailure):
        _run(monkeypatch, pipeline_inputs, checkpoints,
             [SELECTION, DRAFT, REJECT], [True])

    assert "selected_ids" in checkpoints
    assert "draft" not in checkpoints
    assert not checkpoints.get("reviewed")

    # Next attempt: selection is NOT re-run, only draft + review.
    code, calls = _run(
        monkeypatch, pipeline_inputs, checkpoints,
        [DRAFT, APPROVE], [True],
    )
    assert code == 0
    assert len(calls["llm"]) == 2


def test_publish_failure_keeps_stage_checkpoints(monkeypatch, pipeline_inputs):
    """A failed POST raises StageFailure without losing stage results."""
    checkpoints = {}
    with pytest.raises(StageFailure):
        _run(monkeypatch, pipeline_inputs, checkpoints,
             [SELECTION, DRAFT, APPROVE], [False])

    assert "selected_ids" in checkpoints
    assert "draft" in checkpoints
    assert checkpoints["reviewed"] is True
