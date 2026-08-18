"""Tests for Plotly chart/table generation."""

import pandas as pd
import pytest

from eurobot.stats.compute import compute_stats_for_series
from eurobot.viz.plotly_charts import (
    make_delta_bar_chart,
    make_line_chart,
    make_summary_table,
)


@pytest.fixture
def sample_series():
    dates = pd.date_range("2024-01", periods=24, freq="MS")
    return pd.Series([100 + i * 0.5 for i in range(24)], index=dates)


@pytest.fixture
def sample_stats(sample_series):
    return compute_stats_for_series(sample_series, "TEST", "Test Series", "M")


def test_line_chart_structure(sample_series):
    """Line chart has required zzboard keys."""
    chart = make_line_chart(sample_series, "TEST", "Test Title", y_title="index")
    assert "tag" in chart
    assert chart["tag"] == "CHART_TEST"
    assert chart["title"] == "Test Title"
    assert "spec" in chart
    assert "data" in chart["spec"]
    assert "layout" in chart["spec"]


def test_line_chart_trace_is_scatter(sample_series):
    """Line chart produces a scatter trace."""
    chart = make_line_chart(sample_series, "TEST", "T", y_title="y")
    trace = chart["spec"]["data"][0]
    assert trace["type"] == "scatter"


def test_summary_table_structure(sample_stats):
    """Summary table has rows with Metric/Value keys."""
    table = make_summary_table(sample_stats, "TEST")
    assert table["tag"] == "TABLE_TEST"
    assert "Latest value" in [r["Metric"] for r in table["rows"]]
    assert "Δ vs previous" in [r["Metric"] for r in table["rows"]]


def test_delta_bar_chart_multiple_stats(sample_stats):
    """Delta bar chart works with multiple series."""
    stats_list = [sample_stats, sample_stats]
    chart = make_delta_bar_chart(stats_list)
    assert chart["tag"] == "CHART_ALL_DELTA"
    assert chart["spec"]["data"][0]["type"] == "bar"
