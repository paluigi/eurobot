"""Tests for payload builder — zzboard flat-array assembly."""

import json

import pytest

from eurobot.fetchers.rss_fetcher import NewsItem
from eurobot.pipeline.payload_builder import assemble_payload


@pytest.fixture
def sample_news():
    return [
        NewsItem(tag="NEWS_001", title="ECB cuts rates", summary="...", link="https://ecb.eu", source="ECB", hash="abc123"),
        NewsItem(tag="NEWS_002", title="Inflation rises", summary="...", link="https://reuters.com", source="Reuters", hash="def456"),
    ]


@pytest.fixture
def sample_charts():
    return [
        {"title": "CISS trend", "spec": {"data": [{"x": [1, 2], "y": [3, 4], "type": "scatter"}]}},
        {"title": "HICP trend", "spec": {"data": [{"x": [1, 2], "y": [5, 6], "type": "bar"}]}},
    ]


@pytest.fixture
def sample_tables():
    return [
        {"title": "CISS summary", "rows": [{"Metric": "Latest", "Value": "0.08"}]},
    ]


def test_payload_structure(sample_news, sample_charts, sample_tables):
    """Payload has all required zzboard top-level keys."""
    payload = assemble_payload(
        title="Test Post",
        summary="A summary",
        tags=["test", "macro"],
        content_markdown="# Hello",
        charts=sample_charts,
        tables=sample_tables,
        selected_news=sample_news,
    )
    assert set(payload.keys()) == {
        "title", "summary", "author", "content_markdown",
        "tables", "charts", "links", "tags"
    }


def test_payload_links_from_news(sample_news):
    """Links array is built from selected news items."""
    payload = assemble_payload(
        title="T", summary="S", tags=[],
        content_markdown="", charts=[], tables=[],
        selected_news=sample_news,
    )
    assert len(payload["links"]) == 2
    assert payload["links"][0]["url"] == "https://ecb.eu"
    assert payload["links"][0]["label"] == "ECB cuts rates"
    assert payload["links"][0]["description"] == "ECB"


def test_payload_empty_news():
    """Empty news list produces empty links array."""
    payload = assemble_payload(
        title="T", summary="S", tags=[],
        content_markdown="", charts=[], tables=[],
        selected_news=[],
    )
    assert payload["links"] == []


def test_payload_charts_format(sample_charts):
    """Charts have title + spec keys."""
    payload = assemble_payload(
        title="T", summary="S", tags=[],
        content_markdown="", charts=sample_charts, tables=[],
        selected_news=[],
    )
    assert len(payload["charts"]) == 2
    assert payload["charts"][0]["title"] == "CISS trend"
    assert "data" in payload["charts"][0]["spec"]


def test_payload_json_serializable(sample_news, sample_charts, sample_tables):
    """Payload must be JSON serializable for the HTTP POST."""
    payload = assemble_payload(
        title="T", summary="S", tags=["x"],
        content_markdown="# Hi", charts=sample_charts, tables=sample_tables,
        selected_news=sample_news,
    )
    json_str = json.dumps(payload, default=str)
    assert len(json_str) > 0
