"""Market-data fetcher — free Yahoo Finance API (no key required).

Retrieves daily OHLCV for European financial instruments.  The BTP–Bund spread
is computed from the SDMX sovereign yield series.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd
import requests

from eurobot.utils.retries import call_with_retries

logger = logging.getLogger(__name__)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

HTTP_TIMEOUT = 15


@dataclass(frozen=True)
class MarketSpec:
    """Specification for a single market instrument."""

    tag: str
    yahoo_symbol: str
    title: str
    unit: str


# Yahoo Finance tickers — verified live on 2026-08-13.
MARKET_SPECS: list[MarketSpec] = [
    MarketSpec(
        "FTSE_MIB", "FTSEMIB.MI", "FTSE MIB — Italian equity benchmark", "index points"
    ),
    MarketSpec("BRENT", "BZ=F", "Brent crude oil futures", "USD/bbl"),
    # EUR/USD is also fetched from ECB SDMX; Yahoo is a cross-check
    MarketSpec("EUR_USD_MKT", "EURUSD=X", "EUR/USD (market source)", "USD per EUR"),
]


def _download_yahoo(symbol: str, range_days: str = "6mo") -> pd.DataFrame | None:
    """Download historical daily data from Yahoo Finance.

    Uses the undocumented chart API (no key needed).  Returns a DataFrame with
    columns: Open, High, Low, Close, Adj Close, Volume.  The HTTP call is
    retried on transient failures (network / 5xx / 429) with bounded tenacity.
    """

    def _fetch() -> pd.DataFrame:
        resp = requests.get(
            YAHOO_CHART_URL.format(symbol=symbol),
            params={"range": range_days, "interval": "1d"},
            headers={"User-Agent": "eurobot/0.1"},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        result = data["chart"]["result"][0]
        timestamps = result["timestamp"]
        quote = result["indicators"]["quote"][0]

        df = pd.DataFrame(
            {
                "Open": quote["open"],
                "High": quote["high"],
                "Low": quote["low"],
                "Close": quote["close"],
                "Volume": quote["volume"],
            },
            index=pd.to_datetime(timestamps, unit="s"),
        )
        df.index.name = "Date"
        return df.dropna(subset=["Close"])

    try:
        df = call_with_retries(_fetch)
        logger.info(
            "Yahoo: %s — %d obs [%s → %s]",
            symbol,
            len(df),
            df.index[0].date(),
            df.index[-1].date(),
        )
        return df
    except Exception as exc:
        logger.warning("Yahoo: failed %s — %s", symbol, exc)
        return None


def fetch_all_markets(range_days: str = "6mo") -> list[dict]:
    """Fetch all market instruments and return tagged data items.

    Returns a list of dicts with keys: ``tag``, ``title``, ``unit``, ``df``
    (pd.DataFrame with OHLCV), and ``series`` (pd.Series of Close prices).
    """
    items: list[dict] = []
    for spec in MARKET_SPECS:
        df = _download_yahoo(spec.yahoo_symbol, range_days)
        if df is not None and not df.empty:
            items.append(
                {
                    "tag": spec.tag,
                    "title": spec.title,
                    "unit": spec.unit,
                    "df": df,
                    "series": df["Close"],
                    "spec": spec,
                }
            )
        else:
            logger.warning("Yahoo: skipping %s (no data)", spec.tag)
    logger.info("Yahoo: %d/%d instruments fetched", len(items), len(MARKET_SPECS))
    return items


def compute_btp_bund_spread(
    btp_yield: pd.Series,
    bund_yield: pd.Series,
) -> pd.Series | None:
    """Compute the BTP–Bund yield spread (percentage points).

    Aligns the two series on their common dates and subtracts.
    """
    if btp_yield is None or bund_yield is None:
        return None
    aligned = pd.concat([btp_yield, bund_yield], axis=1, join="inner").dropna()
    if aligned.empty:
        return None
    aligned.columns = ["BTP", "Bund"]
    spread = aligned["BTP"] - aligned["Bund"]
    spread.name = "BTP_BUND_SPREAD"
    logger.info("BTP-Bund spread: %d obs, latest %.2f pp", len(spread), spread.iloc[-1])
    return spread
