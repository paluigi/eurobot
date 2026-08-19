"""Windmill flow step 1 — fetch data.

Fetches macro (SDMX), market (Yahoo) and news (RSS) data using the fetchers
embedded below (resilient per series, with bounded transient retries), then
upserts everything into PostgreSQL and opens the MongoDB helper document
that carries run state between flow steps.

This script is fully self-contained: the PostgreSQL schema, the Mongo
handoff helpers and all fetching code (SDMX/Yahoo/RSS) are defined below —
no eurobot package, no shared module, no external files. Its only
dependencies are published PyPI libraries, so it can be pasted into
Windmill and run standalone.

Windmill wiring:
    inputs: postgres_dsn, mongo_uri  (Windmill variables)
    output: {"run_id": ..., "fetched": {...}} — stored in the Mongo helper.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import asyncpg
import feedparser
import pandas as pd
import requests
import sdmx
from pymongo import AsyncMongoClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("eurobot.windmill.fetch")

# MongoDB collections used by the flow.
DB_NAME = "eurobot"
HELPER_COLLECTION = "flow_state"  # per-run handoff docs, dropped at the end

# PostgreSQL data-layer schema (series registry, observations, news, dedup
# state, run log, stats). Idempotent — applied on every connect.
SCHEMA_SQL = """
-- eurobot Windmill flow — PostgreSQL schema (data layer)
--
-- Raw + structured data lives here: series registry, observations, news
-- items, dedup state and per-run stats/logs. Applied idempotently at the
-- start of every flow run (CREATE TABLE IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS series (
    tag         TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    source      TEXT NOT NULL,               -- ECB | ESTAT | YAHOO | COMPUTED
    frequency   TEXT NOT NULL,               -- D | M | Q
    unit        TEXT NOT NULL,
    description TEXT,
    first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS observations (
    tag        TEXT NOT NULL REFERENCES series(tag) ON DELETE CASCADE,
    obs_date   DATE   NOT NULL,
    value      DOUBLE PRECISION NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tag, obs_date)
);

CREATE TABLE IF NOT EXISTS news_items (
    hash       TEXT PRIMARY KEY,             -- sha256(title|link)[:16]
    title      TEXT NOT NULL,
    summary    TEXT,
    link       TEXT NOT NULL,
    source     TEXT NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dedup state — written ONLY after a successful publish (publish_archive).
CREATE TABLE IF NOT EXISTS news_seen (
    hash         TEXT PRIMARY KEY,
    first_seen   TIMESTAMPTZ NOT NULL,
    times_posted INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS macro_releases (
    series_key         TEXT PRIMARY KEY,
    latest_obs_period  TEXT,
    release_timestamp  TIMESTAMPTZ,
    first_presented    TIMESTAMPTZ,
    value_hash         TEXT
);

CREATE TABLE IF NOT EXISTS run_log (
    run_id     UUID PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    step       TEXT NOT NULL,                -- fetch | stats | llm | publish | done
    status     TEXT NOT NULL,                -- running | succeeded | failed | skipped
    detail     TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stats (
    run_id        UUID NOT NULL REFERENCES run_log(run_id) ON DELETE CASCADE,
    tag          TEXT NOT NULL,
    latest_value DOUBLE PRECISION,
    latest_date  DATE,
    delta_prev   DOUBLE PRECISION,
    delta_prev_pct DOUBLE PRECISION,
    yoy          DOUBLE PRECISION,
    yoy_pct      DOUBLE PRECISION,
    summary      TEXT,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, tag)
);

CREATE INDEX IF NOT EXISTS idx_observations_tag_date ON observations (tag, obs_date DESC);
CREATE INDEX IF NOT EXISTS idx_stats_run ON stats (run_id);
"""


def utc_now_naive() -> datetime:
    """Current UTC time as a naive datetime (PyMongo convention)."""
    return datetime.now(UTC).replace(tzinfo=None)


async def pg_connect(postgres_dsn: str) -> asyncpg.Connection:
    """Open a raw asyncpg connection (timezone UTC)."""
    conn = await asyncpg.connect(postgres_dsn)
    await conn.execute("SET TIME ZONE 'UTC'")
    return conn


def mongo_client(mongo_uri: str) -> AsyncMongoClient:
    """Build the async Mongo client (caller must ``await client.close()``)."""
    return AsyncMongoClient(mongo_uri)


class FlowStore:
    """Helper collection that passes run documents between flow steps.

    One document per ``run_id``::

        { _id: run_id, step: "fetch", docs: {...}, created_at, updated_at }

    ``docs`` holds whatever the previous step produced for the next one.
    """

    def __init__(self, client: AsyncMongoClient, run_id: str):
        self._coll = client[DB_NAME][HELPER_COLLECTION]
        self.run_id = run_id

    async def create(self, first_step: str = "fetch") -> None:
        now = utc_now_naive()
        await self._coll.insert_one(
            {
                "_id": self.run_id,
                "step": first_step,
                "docs": {},
                "created_at": now,
                "updated_at": now,
            }
        )

    async def update(self, step: str, docs: dict) -> None:
        """Replace the carried docs and advance the step marker."""
        await self._coll.update_one(
            {"_id": self.run_id},
            {
                "$set": {
                    "step": step,
                    "docs": docs,
                    "updated_at": utc_now_naive(),
                }
            },
        )

    async def load(self) -> dict | None:
        doc = await self._coll.find_one({"_id": self.run_id})
        if doc is not None:
            doc.pop("_id", None)
        return doc

    @staticmethod
    async def drop(client: AsyncMongoClient) -> None:
        """Drop the helper collection (called after successful publish)."""
        await client[DB_NAME][HELPER_COLLECTION].drop()
        logger.info("Mongo helper collection '%s' dropped", HELPER_COLLECTION)


# ---------------------------------------------------------------------------
# Bounded transient retries (network / 5xx / 429)
# ---------------------------------------------------------------------------

# Default cap for transient retries (initial attempt + 2 retries).
TRANSIENT_MAX_ATTEMPTS = 3

# HTTP status codes worth retrying: server errors + rate limiting + timeouts.
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class TransientError(Exception):
    """Wraps a failure that is worth retrying (network, 5xx, 429)."""


def _is_transient(exc: BaseException) -> bool:
    """True for network-level failures and retryable HTTP statuses."""
    if isinstance(exc, TransientError):
        return True
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError):
        status = getattr(exc.response, "status_code", None)
        return status in _RETRYABLE_STATUS
    return False


def call_with_retries(func, *args, max_attempts: int = TRANSIENT_MAX_ATTEMPTS, **kwargs):
    """Run ``func(*args, **kwargs)`` with bounded transient-error retries.

    Non-transient exceptions propagate immediately on the first attempt.
    """
    from tenacity import (
        Retrying,
        before_sleep_log,
        retry_if_exception,
        stop_after_attempt,
        wait_exponential,
    )

    retryer = Retrying(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception(_is_transient),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    return retryer(func, *args, **kwargs)


# ---------------------------------------------------------------------------
# SDMX fetcher — ECB Data Portal + Eurostat via sdmx1
#
# All geo codes use the EA changing-composition code — never the static
# EA19/EA20/EA21. All series keys verified live (August 2026).
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


# ---------------------------------------------------------------------------
# Market-data fetcher — free Yahoo Finance API (no key required)
# ---------------------------------------------------------------------------

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


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

    Uses the undocumented chart API (no key needed).  Returns a DataFrame
    with columns: Open, High, Low, Close, Volume.  The HTTP call is retried
    on transient failures (network / 5xx / 429) with bounded tenacity.
    """

    def _fetch() -> pd.DataFrame:
        resp = requests.get(
            YAHOO_CHART_URL.format(symbol=symbol),
            params={"range": range_days, "interval": "1d"},
            headers={"User-Agent": "eurobot/0.1"},
            timeout=15,
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


# ---------------------------------------------------------------------------
# RSS news fetcher — euro-area economic and financial feeds
#
# Collects items from an official source (ECB), international press (The
# Economist, The Guardian, DW), Italian press (ANSA, Il Sole 24 Ore) and
# think tanks (Bruegel).  Items are tagged ``[NEWS_xxx]`` for LLM selection.
# All feeds are filtered for euro-area economic/financial relevance.
# ---------------------------------------------------------------------------

FEEDS: dict[str, str] = {
    # ── Official sources ──────────────────────────────────────────────────
    "ECB Press": "https://www.ecb.europa.eu/rss/press.html",
    # ── International press ───────────────────────────────────────────────
    "The Economist (Finance)": "https://www.economist.com/finance-and-economics/rss.xml",
    "The Guardian (Business)": "https://www.theguardian.com/business/rss",
    "DW (Economy)": "https://rss.dw.com/rdf/rss-en-bus",
    # ── Italian press ─────────────────────────────────────────────────────
    "ANSA Economy": "https://www.ansa.it/sito/notizie/economia/economia_rss.xml",
    "Il Sole 24 Ore": "https://www.ilsole24ore.com/rss/economia.xml",
    # ── Think tanks ───────────────────────────────────────────────────────
    "Bruegel": "https://www.bruegel.org/rss.xml",
}

# Keyword filter — only keep items matching these terms in title or summary.
# Case-insensitive.  This is a pre-LLM filter to reduce noise.
RELEVANCE_KEYWORDS = [
    # English
    "euro area",
    "eurozone",
    "eurozone",
    "euro zone",
    "european central bank",
    "ecb",
    "european commission",
    "inflation",
    "hicp",
    "gdp",
    "recession",
    "monetary policy",
    "interest rate",
    "bond yield",
    "spread",
    "sovereign",
    "btp",
    "bund",
    "mib",
    "italian",
    "italy",
    "euro",
    "eur",
    "currency",
    "forex",
    "unemployment",
    "labour market",
    "labor market",
    "energy",
    "oil",
    "gas",
    "brent",
    "economic",
    "economy",
    "financial",
    # Italian
    "economia",
    "inflazione",
    "spread",
    "bcc",
    "bce",
    "pil",
    "disoccupazione",
    "titoli di stato",
    "rendimenti",
    "area euro",
    "zona euro",
    # French / German
    "wirtschaft",
    "inflation",
    "économie",
    "inflation",
]


@dataclass
class NewsItem:
    """A single news item extracted from RSS."""

    tag: str  # e.g. "NEWS_001"
    title: str
    summary: str
    link: str
    source: str  # feed name
    hash: str  # dedup hash of title+link

    def to_prompt_line(self) -> str:
        """One-line representation for the LLM selection prompt."""
        # Truncate summary to keep context lean
        summary = self.summary[:200].replace("\n", " ")
        return f"[{self.tag}] {self.source}: {self.title} — {summary}"


def _is_relevant(title: str, summary: str) -> bool:
    """Check if a news item matches the euro-area economic relevance filter."""
    text = (title + " " + summary).lower()
    return any(kw in text for kw in RELEVANCE_KEYWORDS)


def _clean_html(text: str) -> str:
    """Strip HTML tags from RSS summary."""
    clean = re.sub(r"<[^>]+>", "", text)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def _make_hash(title: str, link: str) -> str:
    """Create a deterministic hash for deduplication."""
    return hashlib.sha256(f"{title}|{link}".encode()).hexdigest()[:16]


def get_latest_news(max_items: int = 15) -> list[NewsItem]:
    """Fetch news from all feeds, filter for relevance, tag, and deduplicate.

    Returns at most ``max_items`` items.  Cross-run dedup against the
    PostgreSQL ``news_seen`` table happens downstream (compute_stats /
    publish_archive), not here — this function returns all relevant items.
    """
    items: list[NewsItem] = []
    seen_hashes: set[str] = set()

    for source_name, feed_url in FEEDS.items():
        try:
            # Use requests with a proper User-Agent, then pass content to
            # feedparser. Retried on transient failures with bounded tenacity.
            def _get(url: str):
                resp = requests.get(
                    url, timeout=15, headers={"User-Agent": "eurobot/0.1"}
                )
                resp.raise_for_status()
                return resp.content

            content = call_with_retries(_get, feed_url)
            parsed = feedparser.parse(content)
            if parsed.bozo and not parsed.entries:
                logger.warning(
                    "RSS: feed error for %s — %s", source_name, parsed.bozo_exception
                )
                continue

            for entry in parsed.entries[:10]:  # max 10 per source
                title = entry.get("title", "").strip()
                summary = _clean_html(
                    entry.get("summary", entry.get("description", ""))
                )
                link = entry.get("link", "")

                if not title or not link:
                    continue

                # Relevance filter
                if not _is_relevant(title, summary):
                    continue

                h = _make_hash(title, link)
                if h in seen_hashes:
                    continue
                seen_hashes.add(h)

                items.append(
                    NewsItem(
                        tag="",  # assigned after collection
                        title=title,
                        summary=summary,
                        link=link,
                        source=source_name,
                        hash=h,
                    )
                )
        except Exception as exc:
            logger.warning("RSS: failed %s — %s", source_name, exc)

    # Assign sequential tags
    for i, item in enumerate(items[:max_items], start=1):
        item.tag = f"NEWS_{i:03d}"

    logger.info(
        "RSS: collected %d relevant items (from %d feeds)",
        len(items[:max_items]),
        len(FEEDS),
    )
    return items[:max_items]


# ---------------------------------------------------------------------------
# Step 1 — fetch → PostgreSQL + Mongo helper doc
# ---------------------------------------------------------------------------


async def upsert_series(
    conn,
    tag: str,
    title: str,
    source: str,
    frequency: str,
    unit: str,
    description: str,
    values: dict,
) -> int:
    """Upsert one series + its observations. ``values`` maps ISO date → float."""
    await conn.execute(
        """
        INSERT INTO series (tag, title, source, frequency, unit, description, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, now())
        ON CONFLICT (tag) DO UPDATE SET
            title = EXCLUDED.title,
            source = EXCLUDED.source,
            frequency = EXCLUDED.frequency,
            unit = EXCLUDED.unit,
            description = EXCLUDED.description,
            updated_at = now()
        """,
        tag,
        title,
        source,
        frequency,
        unit,
        description,
    )
    if not values:
        return 0
    rows = [(tag, date.fromisoformat(d), float(v)) for d, v in values.items()]
    await conn.executemany(
        """
        INSERT INTO observations (tag, obs_date, value, fetched_at)
        VALUES ($1, $2, $3, now())
        ON CONFLICT (tag, obs_date) DO UPDATE SET value = EXCLUDED.value
        """,
        rows,
    )
    return len(rows)


async def _main(
    postgres_dsn: str,
    mongo_uri: str,
    run_id: str | None = None,
    max_news_items: int = 15,
) -> dict:
    """Fetch all sources → PostgreSQL + open the Mongo flow document."""
    run_id = run_id or str(uuid.uuid4())

    conn = await pg_connect(postgres_dsn)
    client = mongo_client(mongo_uri)
    try:
        await conn.execute(SCHEMA_SQL)  # idempotent
        logger.info("Postgres schema applied (idempotent)")
        await conn.execute(
            "INSERT INTO run_log (run_id, started_at, step, status, detail)"
            " VALUES ($1, now(), 'fetch', 'running', NULL)"
            " ON CONFLICT (run_id) DO UPDATE SET step='running', status='running',"
            " updated_at=now()",
            uuid.UUID(run_id),
        )

        # ── Fetch (CPU/IO bound libs are sync → run in worker threads) ──
        macro_items = await asyncio.to_thread(fetch_all_macro)
        market_items = await asyncio.to_thread(fetch_all_markets)
        all_news = await asyncio.to_thread(get_latest_news, max_news_items)

        # BTP–Bund spread from the SDMX sovereign yields
        macro_by_tag = {it["tag"]: it for it in macro_items}
        btp = macro_by_tag.get("IT_10Y_YIELD", {}).get("series")
        bund = macro_by_tag.get("DE_10Y_YIELD", {}).get("series")
        if btp is not None and bund is not None:
            spread = await asyncio.to_thread(compute_btp_bund_spread, btp, bund)
            if spread is not None:
                macro_items.append(
                    {
                        "tag": "BTP_BUND_SPREAD",
                        "title": "BTP–Bund spread",
                        "unit": "percentage points",
                        "frequency": "D",
                        "description": "Italian-German 10-year sovereign yield spread.",
                        "series": spread,
                        "spec": None,
                    }
                )

        # ── PostgreSQL upserts ──────────────────────────────────────────
        series_meta = {}
        obs_counts = {}
        for item in macro_items + market_items:
            s = item.get("series")
            if s is None or s.empty:
                continue
            source = getattr(item.get("spec"), "agency", None) or "YAHOO"
            values = {str(ts.date()): float(v) for ts, v in s.tail(400).items()}
            n = await upsert_series(
                conn,
                item["tag"],
                item["title"],
                source,
                item.get("frequency", "M"),
                item.get("unit", ""),
                item.get("description", ""),
                values,
            )
            series_meta[item["tag"]] = {
                "title": item["title"],
                "unit": item.get("unit", ""),
                "frequency": item.get("frequency", "M"),
                "source": source,
            }
            obs_counts[item["tag"]] = n

        news_rows = 0
        for n in all_news:
            await conn.execute(
                """
                INSERT INTO news_items (hash, title, summary, link, source, first_seen)
                VALUES ($1, $2, $3, $4, $5, now())
                ON CONFLICT (hash) DO UPDATE SET title = EXCLUDED.title,
                                                 summary = EXCLUDED.summary
                """,
                n.hash,
                n.title,
                n.summary,
                n.link,
                n.source,
            )
            news_rows += 1

        await conn.execute(
            "UPDATE run_log SET status='succeeded', detail=$2, updated_at=now()"
            " WHERE run_id=$1",
            uuid.UUID(run_id),
            f"series={len(series_meta)} obs={sum(obs_counts.values())} news={news_rows}",
        )

        # ── Mongo helper doc: carry tags + news hashes to the next step ──
        store = FlowStore(client, run_id)
        await store.create(first_step="fetch")
        await store.update(
            "fetch",
            {
                "run_id": run_id,
                "series_tags": sorted(series_meta.keys()),
                "series_meta": series_meta,
                "news": [
                    {
                        "hash": n.hash,
                        "tag": n.tag,
                        "title": n.title,
                        "summary": n.summary,
                        "link": n.link,
                        "source": n.source,
                    }
                    for n in all_news
                ],
                "fetched_at": utc_now_naive().isoformat(),
            },
        )

        logger.info(
            "fetch_data done: %d series, %d news, run_id=%s",
            len(series_meta),
            news_rows,
            run_id,
        )
        return {"run_id": run_id, "series": len(series_meta), "news": news_rows}
    finally:
        await conn.close()
        await client.close()


def main(
    postgres_dsn: str,
    mongo_uri: str,
    run_id: str | None = None,
    max_news_items: int = 15,
) -> dict:
    """Sync Windmill entrypoint (workers call ``main`` without awaiting)."""
    return asyncio.run(
        _main(postgres_dsn, mongo_uri, run_id, max_news_items)
    )
