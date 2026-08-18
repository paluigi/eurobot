"""Tests for dedup logic — news cooldown + macro release freshness."""

from datetime import datetime, timedelta

import pandas as pd
import pytest

from eurobot import config
from eurobot.fetchers.rss_fetcher import NewsItem
from eurobot.pipeline import dedup


@pytest.fixture(autouse=True)
def temp_db(tmp_path, monkeypatch):
    """Use a temporary SQLite database for each test."""
    db_path = tmp_path / "test.db"
    monkeypatch.setattr(config, "DB_PATH", db_path)
    yield db_path


# ---------------------------------------------------------------------------
# News dedup
# ---------------------------------------------------------------------------

def test_news_fresh_first_time(temp_db):
    """First sighting of a news item is always fresh."""
    items = [NewsItem("NEWS_001", "Title", "Sum", "http://a", "SRC", "hash1")]
    fresh = dedup.filter_fresh_news(items, cooldown_hours=48)
    assert len(fresh) == 1


def test_news_filtered_in_cooldown(temp_db):
    """Item seen recently is filtered out."""
    items = [NewsItem("NEWS_001", "Title", "Sum", "http://a", "SRC", "hash1")]
    dedup.mark_news_seen(items)
    fresh = dedup.filter_fresh_news(items, cooldown_hours=48)
    assert len(fresh) == 0


def test_news_resurfaces_after_cooldown(temp_db):
    """Item resurfaces after cooldown window expires."""
    items = [NewsItem("NEWS_001", "Title", "Sum", "http://a", "SRC", "hash1")]
    dedup.mark_news_seen(items)

    # Manually backdate the first_seen timestamp
    with dedup._get_conn() as conn:
        old_time = (datetime.now() - timedelta(hours=72)).isoformat()
        conn.execute("UPDATE news_seen SET first_seen = ? WHERE hash = ?", (old_time, "hash1"))

    fresh = dedup.filter_fresh_news(items, cooldown_hours=48)
    assert len(fresh) == 1


# ---------------------------------------------------------------------------
# Macro release freshness
# ---------------------------------------------------------------------------

def test_macro_fresh_first_time(temp_db):
    """First sighting of a macro series is fresh."""
    series = pd.Series([1.0, 2.0], index=pd.date_range("2024-01", periods=2, freq="MS"))
    items = [{"tag": "TEST", "title": "Test", "series": series, "spec": None}]
    fresh = dedup.filter_fresh_macro(items)
    assert len(fresh) == 1


def test_macro_filtered_after_presentation(temp_db):
    """Series already presented with same period is not fresh."""
    series = pd.Series([1.0, 2.0], index=pd.date_range("2024-01", periods=2, freq="MS"))
    items = [{"tag": "TEST", "title": "Test", "series": series, "spec": None}]
    dedup.mark_macro_presented(items)
    fresh = dedup.filter_fresh_macro(items)
    assert len(fresh) == 0


def test_macro_fresh_on_new_release(temp_db):
    """Series with a new observation period is fresh."""
    series = pd.Series([1.0, 2.0], index=pd.date_range("2024-01", periods=2, freq="MS"))
    items = [{"tag": "TEST", "title": "Test", "series": series, "spec": None}]
    dedup.mark_macro_presented(items)

    # New observation appended
    series2 = pd.Series([1.0, 2.0, 3.0], index=pd.date_range("2024-01", periods=3, freq="MS"))
    items2 = [{"tag": "TEST", "title": "Test", "series": series2, "spec": None}]
    fresh = dedup.filter_fresh_macro(items2)
    assert len(fresh) == 1
