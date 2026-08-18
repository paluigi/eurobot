"""Tests for stats computation (Δ prev + YoY)."""

import pandas as pd
import pytest

from eurobot.stats.compute import compute_stats_for_series


@pytest.fixture
def monthly_series() -> pd.Series:
    """Synthetic monthly series: 24 months of values."""
    dates = pd.date_range("2024-01", periods=24, freq="MS")
    values = [100.0 + i * 0.5 for i in range(24)]  # steady growth
    return pd.Series(values, index=dates, name="TEST")


@pytest.fixture
def daily_series() -> pd.Series:
    """Synthetic daily series."""
    dates = pd.date_range("2024-01-01", periods=300, freq="B")
    values = [50.0 + i * 0.1 for i in range(300)]
    return pd.Series(values, index=dates, name="TEST_D")


def test_compute_stats_monthly(monthly_series):
    """Test Δ-prev and YoY for a monthly series."""
    stats = compute_stats_for_series(monthly_series, "TEST", "Test Series", "M")
    assert stats is not None
    assert stats.tag == "TEST"
    assert stats.latest_value == pytest.approx(111.5)
    # Δ prev = 0.5 (last increment)
    assert stats.delta_prev == pytest.approx(0.5)
    assert stats.delta_prev_pct == pytest.approx(0.5 / 111.0 * 100, rel=1e-3)
    # YoY = 6.0 (12 increments of 0.5)
    assert stats.yoy == pytest.approx(6.0)
    assert stats.yoy_pct is not None


def test_compute_stats_daily(daily_series):
    """Test daily series with 252-period YoY."""
    stats = compute_stats_for_series(daily_series, "TEST_D", "Test Daily", "D")
    assert stats is not None
    assert stats.latest_value == pytest.approx(50.0 + 299 * 0.1)
    assert stats.delta_prev == pytest.approx(0.1)


def test_compute_stats_too_short():
    """Series with < 2 observations returns None."""
    s = pd.Series([100.0], index=pd.date_range("2024-01", periods=1, freq="MS"))
    stats = compute_stats_for_series(s, "SHORT", "Short", "M")
    assert stats is None


def test_compute_stats_empty():
    """Empty series returns None."""
    s = pd.Series([], dtype=float)
    stats = compute_stats_for_series(s, "EMPTY", "Empty", "M")
    assert stats is None


def test_stats_summary_string(monthly_series):
    """Summary string contains tag and key figures."""
    stats = compute_stats_for_series(monthly_series, "TEST", "Test Series", "M")
    assert "Test Series" in stats.summary
    assert "Δ" in stats.summary or "111" in stats.summary


def test_compute_stats_no_yoy_short_series():
    """Series with < 13 months has no YoY."""
    dates = pd.date_range("2024-01", periods=6, freq="MS")
    s = pd.Series([100 + i for i in range(6)], index=dates)
    stats = compute_stats_for_series(s, "SHORT6", "Short 6", "M")
    assert stats is not None
    assert stats.delta_prev is not None
    assert stats.yoy is None
