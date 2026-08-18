"""SDMX data fetcher — ECB Data Portal + Eurostat via sdmx1.

Fetches euro-area macroeconomic and financial series.  All geo codes use the
EA changing-composition code — never the static EA19/EA20/EA21.

All series keys in this module have been **verified live** against the ECB and
Eurostat SDMX endpoints (August 2026).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import pandas as pd
import sdmx

from eurobot.utils.retries import call_with_retries

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Series registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SeriesSpec:
    """Specification for a single macro/financial series."""

    tag: str
    agency: str  # "ECB" or "ESTAT"
    flow: str  # SDMX dataflow ref
    key: str  # Full series key (dot-separated dimensions)
    title: str
    frequency: str  # "D", "M", "Q"
    unit: str
    description: str = ""


# Registry of all tracked series.  Keys verified live on 2026-08-13.
SERIES_SPECS: list[SeriesSpec] = [
    # ── ECB financial / monetary ──────────────────────────────────────────
    SeriesSpec(
        tag="CISS",
        agency="ECB",
        flow="CISS",
        key="D.U2.Z0Z.4F.EC.SS_CIN.IDX",
        title="CISS — Composite Indicator of Systemic Stress",
        frequency="D",
        unit="index (0–1)",
        description="Daily measure of financial system stress in the euro area.",
    ),
    SeriesSpec(
        tag="EUR_USD",
        agency="ECB",
        flow="EXR",
        key="D.USD.EUR.SP00.A",
        title="EUR/USD exchange rate",
        frequency="D",
        unit="USD per EUR",
        description="Daily spot euro-dollar reference rate.",
    ),
    SeriesSpec(
        tag="M3",
        agency="ECB",
        flow="BSI",
        key="M.U2.N.V.M30.X.1.U2.2300.Z01.E",
        title="M3 monetary aggregate",
        frequency="M",
        unit="€ bn (stock)",
        description="Broad money stock for the euro area.",
    ),
    SeriesSpec(
        tag="DFR",
        agency="ECB",
        flow="FM",
        key="D.U2.EUR.4F.KR.DFR.LEV",
        title="ECB Deposit Facility Rate",
        frequency="D",
        unit="% p.a.",
        description="ECB key interest rate — deposit facility.",
    ),
    SeriesSpec(
        tag="EA_10Y_YIELD",
        agency="ECB",
        flow="FM",
        key="M.U2.EUR.4F.BB.U2_10Y.YLD",
        title="Euro area 10-year benchmark bond yield",
        frequency="M",
        unit="% p.a.",
        description="EA aggregate government benchmark bond yield, 10Y maturity.",
    ),
    # ── Eurostat macro ────────────────────────────────────────────────────
    # Country-specific 10Y convergence yields (Maastricht) — monthly
    SeriesSpec(
        tag="DE_10Y_YIELD",
        agency="ESTAT",
        flow="irt_lt_mcby_m",
        key="M.MCBY.DE",
        title="Germany 10-year long-term interest rate",
        frequency="M",
        unit="% p.a.",
        description="German 10-year benchmark bond yield (Maastricht convergence).",
    ),
    SeriesSpec(
        tag="IT_10Y_YIELD",
        agency="ESTAT",
        flow="irt_lt_mcby_m",
        key="M.MCBY.IT",
        title="Italy 10-year long-term interest rate",
        frequency="M",
        unit="% p.a.",
        description="Italian 10-year benchmark bond yield (Maastricht convergence).",
    ),
]


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------


def _fetch_ecb_series(spec: SeriesSpec) -> pd.Series | None:
    """Fetch a single series from ECB via sdmx1 (with transient retries)."""

    def _fetch() -> pd.Series:
        client = sdmx.Client("ECB")
        msg = client.data(
            resource_id=spec.flow,
            key=spec.key,
            params={"startPeriod": "2020-01"},
        )
        s = sdmx.to_pandas(msg)
        # Drop multi-index dimensions, keep only the time index
        if isinstance(s.index, pd.MultiIndex):
            s = s.reset_index().set_index("TIME_PERIOD")["value"]
        s.index = pd.to_datetime(s.index)
        return s.sort_index().dropna()

    try:
        s = call_with_retries(_fetch)
        s.name = spec.tag
        logger.info(
            "ECB: %s — %d obs [%s → %s]",
            spec.tag,
            len(s),
            s.index[0].date(),
            s.index[-1].date(),
        )
        return s
    except Exception as exc:
        logger.warning("ECB: failed %s — %s", spec.tag, exc)
        return None


def _fetch_eurostat_series(spec: SeriesSpec) -> pd.Series | None:
    """Fetch a single series from Eurostat via REST API (CSV format).

    The SDMX-CSV endpoint returns all countries by default; we filter
    client-side based on the geo code embedded in the key (e.g. "M.MCBY.IT"
    → filter where geo == "IT").
    """
    import io
    import requests

    def _fetch() -> pd.Series:
        # Parse the key to extract the geo filter
        # Key format: "M.MCBY.DE" → freq=M, int_rt=MCBY, geo=DE
        parts = spec.key.split(".")
        geo_filter = parts[-1] if len(parts) >= 3 else None

        url = f"https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{spec.flow}/"
        resp = requests.get(
            url,
            params={
                "format": "SDMX-CSV",
                "startPeriod": "2020-01",
            },
            timeout=30,
        )
        resp.raise_for_status()

        df = pd.read_csv(io.StringIO(resp.text))
        if "OBS_VALUE" not in df.columns or "TIME_PERIOD" not in df.columns:
            raise ValueError(f"unexpected columns: {list(df.columns)[:8]}")

        # Filter for specific country if key has a geo dimension
        if geo_filter and "geo" in df.columns:
            df = df[df["geo"] == geo_filter]

        s = df.set_index(pd.to_datetime(df["TIME_PERIOD"]))["OBS_VALUE"]
        return s[~s.index.duplicated(keep="last")].sort_index().dropna()

    try:
        s = call_with_retries(_fetch)
        s.name = spec.tag
        logger.info(
            "Eurostat: %s — %d obs [%s → %s]",
            spec.tag,
            len(s),
            s.index[0].date(),
            s.index[-1].date(),
        )
        return s
    except Exception as exc:
        logger.warning("Eurostat: failed %s — %s", spec.tag, exc)
        return None


def _fetch_series(spec: SeriesSpec) -> pd.Series | None:
    """Fetch a single series from the appropriate source."""
    if spec.agency == "ECB":
        return _fetch_ecb_series(spec)
    elif spec.agency == "ESTAT":
        return _fetch_eurostat_series(spec)
    else:
        logger.error("Unknown agency: %s", spec.agency)
        return None


def fetch_all_macro() -> list[dict[str, Any]]:
    """Fetch all macro series, returning a list of tagged data items."""
    items: list[dict[str, Any]] = []
    for spec in SERIES_SPECS:
        series = _fetch_series(spec)
        if series is not None and not series.empty:
            items.append(
                {
                    "tag": spec.tag,
                    "title": spec.title,
                    "unit": spec.unit,
                    "frequency": spec.frequency,
                    "description": spec.description,
                    "series": series,
                    "spec": spec,
                }
            )
    logger.info(
        "SDMX: %d/%d series fetched successfully", len(items), len(SERIES_SPECS)
    )
    return items
