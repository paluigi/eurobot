"""Windmill flow step 2 — compute stats, charts, tables + freshness filter.

Reads the observations stored in PostgreSQL by step 1, applies the same
freshness logic as the classic pipeline (news dedup window + macro-release
freshness, both backed by PostgreSQL tables), computes Δ-prev + YoY stats,
builds Plotly charts/tables and writes everything the LLM needs into the
MongoDB helper document.

This script is fully self-contained: the PostgreSQL schema, the Mongo
handoff helpers and all stats/chart code are defined below — no eurobot
package, no shared module, no external files. Its only dependencies are
published PyPI libraries, so it can be pasted into Windmill and run
standalone.

Windmill wiring:
    inputs: postgres_dsn, mongo_uri, run_id
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pandas as pd
import plotly.graph_objects as go
from pymongo import AsyncMongoClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("eurobot.windmill.stats")

# Defaults mirror the classic pipeline config.
NEWS_COOLDOWN_HOURS = 48

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
# Deterministic statistics — Δ vs previous period + YoY
# ---------------------------------------------------------------------------


@dataclass
class SeriesStats:
    """Computed statistics for a single series.

    Attributes:
        tag: Series tag (e.g. ``"CISS"``).
        title: Human-readable title.
        latest_value: Most recent observation.
        latest_date: Date of most recent observation.
        delta_prev: Change vs previous period.
        delta_prev_pct: Percentage change vs previous period.
        yoy: Change vs same period 12 months ago (levels or pp).
        yoy_pct: YoY percentage change.
        summary: One-sentence deterministic summary for the LLM.
    """

    tag: str
    title: str
    latest_value: float
    latest_date: pd.Timestamp
    delta_prev: float
    delta_prev_pct: float | None
    yoy: float | None
    yoy_pct: float | None
    summary: str


# Number of periods to shift for YoY
_FREQ_YOY_PERIODS = {
    "D": 252,  # trading days in a year
    "M": 12,
    "Q": 4,
}


def compute_stats_for_series(
    series: pd.Series,
    tag: str,
    title: str,
    frequency: str = "M",
) -> SeriesStats | None:
    """Compute Δ-prev + YoY for a single series.

    Returns ``None`` if the series is too short (< 2 obs for Δ, < 13 for YoY).
    """
    if series is None or len(series) < 2:
        logger.warning("Stats: %s — too few observations (%d)", tag, len(series) if series is not None else 0)
        return None

    latest = series.iloc[-1]
    latest_date = series.index[-1]

    # Δ vs previous period
    prev = series.iloc[-2]
    delta_prev = latest - prev
    delta_prev_pct = (delta_prev / abs(prev) * 100) if prev != 0 else None

    # YoY
    yoy_periods = _FREQ_YOY_PERIODS.get(frequency, 12)
    yoy = None
    yoy_pct = None
    if len(series) > yoy_periods:
        yoy_base = series.iloc[-1 - yoy_periods]
        yoy = latest - yoy_base
        yoy_pct = (yoy / abs(yoy_base) * 100) if yoy_base != 0 else None

    # Build summary string
    delta_str = f"{delta_prev:+.2f}" if abs(delta_prev) < 100 else f"{delta_prev:+.1f}"
    summary_parts = [f"{title}: latest {latest:.2f} ({latest_date.strftime('%Y-%m-%d')})"]
    summary_parts.append(f"Δ {delta_str}")
    if yoy is not None:
        summary_parts.append(f"YoY {yoy:+.2f}")
    summary = " ".join(summary_parts)

    return SeriesStats(
        tag=tag,
        title=title,
        latest_value=float(latest),
        latest_date=latest_date,
        delta_prev=float(delta_prev),
        delta_prev_pct=delta_prev_pct,
        yoy=float(yoy) if yoy is not None else None,
        yoy_pct=float(yoy_pct) if yoy_pct is not None else None,
        summary=summary,
    )


# ---------------------------------------------------------------------------
# Plotly chart and table generation
#
# For every data series: a line chart (time-series path), a summary table
# (current value, Δ prev, YoY) and — across series — a Δ bar chart. Each is
# returned as a ready-to-embed dict conforming to the zzboard ``charts`` or
# ``tables`` array entry format.
# ---------------------------------------------------------------------------

_PLOTLY_TEMPLATE = "plotly_white"


def _fmt(val: float | None, unit: str = "") -> str:
    """Format a value for table display."""
    if val is None:
        return "—"
    if abs(val) >= 100:
        return f"{val:,.1f} {unit}".strip()
    return f"{val:+.2f} {unit}".strip() if "Δ" not in unit else f"{val:+.2f} {unit}".strip()


def make_line_chart(
    series: pd.Series,
    tag: str,
    title: str,
    y_title: str = "",
) -> dict[str, Any]:
    """Create a line-chart zzboard chart entry from a time-series.

    Returns ``{"title": ..., "spec": {Plotly spec}}``.
    """
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=series.index,
        y=series.values,
        mode="lines+markers",
        line={"color": "#0d6efd", "width": 2},
        marker={"size": 4},
        name=title,
    ))
    fig.update_layout(
        template=_PLOTLY_TEMPLATE,
        xaxis={"title": "Date"},
        yaxis={"title": y_title or title},
        margin={"l": 40, "r": 20, "t": 30, "b": 30},
        height=350,
    )
    return {
        "tag": f"CHART_{tag}",
        "title": title,
        "spec": fig.to_dict(),
    }


def make_summary_table(
    stats: SeriesStats,
    tag: str,
) -> dict[str, Any]:
    """Create a summary-table zzboard entry from computed stats.

    Returns ``{"tag": ..., "title": ..., "rows": [...]}``.
    """
    rows = [
        {"Metric": "Latest value", "Value": f"{stats.latest_value:.2f}"},
        {"Metric": "Date", "Value": stats.latest_date.strftime("%Y-%m-%d")},
        {"Metric": "Δ vs previous", "Value": _fmt(stats.delta_prev)},
        {"Metric": "Δ vs previous (%)", "Value": _fmt(stats.delta_prev_pct)},
        {"Metric": "YoY change", "Value": _fmt(stats.yoy)},
        {"Metric": "YoY change (%)", "Value": _fmt(stats.yoy_pct)},
    ]
    return {
        "tag": f"TABLE_{tag}",
        "title": f"{stats.title} — summary",
        "rows": rows,
    }


def make_delta_bar_chart(
    all_stats: list[SeriesStats],
    tag: str = "ALL_DELTA",
    title: str = "Euro-area indicators — latest change vs previous period",
) -> dict[str, Any]:
    """Create a bar chart comparing Δ-prev across all series.

    Useful as a single overview chart in the post.
    """
    if not all_stats:
        return {}

    fig = go.Figure()
    tags = [s.tag for s in all_stats]
    deltas = [s.delta_prev for s in all_stats]

    colors = ["#198754" if d >= 0 else "#dc3545" for d in deltas]

    fig.add_trace(go.Bar(
        x=tags,
        y=deltas,
        marker_color=colors,
        text=[f"{d:+.2f}" for d in deltas],
        textposition="outside",
    ))
    fig.update_layout(
        template=_PLOTLY_TEMPLATE,
        xaxis={"title": "Indicator"},
        yaxis={"title": "Δ vs previous period"},
        margin={"l": 40, "r": 20, "t": 30, "b": 40},
        height=350,
    )
    tag_id = f"CHART_{tag}"
    return {
        "tag": tag_id,
        "title": title,
        "spec": fig.to_dict(),
    }


# ---------------------------------------------------------------------------
# Step 2 — stats/charts/tables + freshness filter
# ---------------------------------------------------------------------------


def _value_hash(series: pd.Series) -> str:
    return hashlib.sha256(series.tail(5).round(4).to_json().encode()).hexdigest()[:16]


async def _main(
    postgres_dsn: str,
    mongo_uri: str,
    run_id: str,
    news_cooldown_hours: int = NEWS_COOLDOWN_HOURS,
) -> dict:
    """Read PG observations → stats/charts/tables → Mongo helper doc."""
    conn = await pg_connect(postgres_dsn)
    client = mongo_client(mongo_uri)
    try:
        await conn.execute(SCHEMA_SQL)  # idempotent
        await conn.execute(
            "UPDATE run_log SET step='stats', status='running', updated_at=now()"
            " WHERE run_id=$1",
            uuid.UUID(run_id),
        )
        store = FlowStore(client, run_id)
        state = await store.load()
        if state is None:
            raise RuntimeError(f"flow helper document for run {run_id} not found")
        carried = state["docs"]

        # ── Load series + observations from PostgreSQL ─────────────────
        tags = carried["series_tags"]
        meta = carried["series_meta"]
        cutoff = datetime.now(UTC) - timedelta(hours=news_cooldown_hours)

        rows = await conn.fetch(
            "SELECT tag, obs_date, value FROM observations"
            " WHERE tag = ANY($1::text[]) ORDER BY tag, obs_date",
            tags,
        )
        frames: dict[str, pd.Series] = {}
        for r in rows:
            frames.setdefault(r["tag"], []).append((r["obs_date"], r["value"]))
        series_map: dict[str, pd.Series] = {}
        for tag, obs in frames.items():
            idx = pd.to_datetime([d for d, _ in obs])
            s = pd.Series([v for _, v in obs], index=idx, name=tag).sort_index()
            series_map[tag] = s
        logger.info("Loaded %d series from PostgreSQL", len(series_map))

        # ── Freshness filter (same logic as the classic SQLite dedup) ──
        fresh_tags: list[str] = []
        for tag, s in series_map.items():
            latest_period = str(s.index[-1].date())
            value_hash = _value_hash(s)
            row = await conn.fetchrow(
                "SELECT latest_obs_period, value_hash FROM macro_releases"
                " WHERE series_key=$1",
                tag,
            )
            if (
                row is None
                or row["latest_obs_period"] != latest_period
                or row["value_hash"] != value_hash
            ):
                fresh_tags.append(tag)

        fresh_news = []
        for n in carried["news"]:
            seen = await conn.fetchrow(
                "SELECT first_seen FROM news_seen WHERE hash=$1",
                n["hash"],
            )
            if seen is None or seen["first_seen"] < cutoff:
                fresh_news.append(n)
        logger.info(
            "Fresh: %d/%d series, %d/%d news items",
            len(fresh_tags),
            len(series_map),
            len(fresh_news),
            len(carried["news"]),
        )

        if not fresh_tags and not fresh_news:
            await conn.execute(
                "UPDATE run_log SET status='skipped', detail='nothing fresh',"
                " updated_at=now() WHERE run_id=$1",
                uuid.UUID(run_id),
            )
            await store.update("stats", {"skip": True})
            return {"run_id": run_id, "skip": True, "fresh_series": 0, "fresh_news": 0}

        # ── Stats + charts + tables ────────────────────────────────────
        data_summaries: list[str] = []
        stats_rows = []
        stats_objs = []  # SeriesStats objects (for the overview chart)
        charts_pool: dict[str, dict] = {}
        tables_pool: dict[str, dict] = {}

        for tag in fresh_tags:
            s = series_map[tag]
            m = meta.get(tag, {})
            stats = await asyncio.to_thread(
                compute_stats_for_series,
                s,
                tag,
                m.get("title", tag),
                frequency=m.get("frequency", "M"),
            )
            if stats is None:
                continue
            stats_objs.append(stats)
            stats_rows.append(
                (
                    uuid.UUID(run_id),
                    tag,
                    stats.latest_value,
                    (
                        stats.latest_date.date()
                        if hasattr(stats.latest_date, "date")
                        else stats.latest_date
                    ),
                    stats.delta_prev,
                    stats.delta_prev_pct,
                    stats.yoy,
                    stats.yoy_pct,
                    stats.summary,
                )
            )
            charts_pool[tag] = await asyncio.to_thread(
                make_line_chart,
                s,
                tag,
                m.get("title", tag),
                y_title=m.get("unit", ""),
            )
            tables_pool[tag] = await asyncio.to_thread(make_summary_table, stats, tag)
            data_summaries.append(
                f"[DATA_{tag}] {stats.summary} (unit: {m.get('unit', '')})"
            )

        if len(stats_objs) >= 2:
            overview = await asyncio.to_thread(make_delta_bar_chart, stats_objs)
            charts_pool["ALL_DELTA"] = overview
            data_summaries.append(
                f"[DATA_ALL_DELTA] Overview bar chart: Δ vs previous period"
                f" across {len(stats_objs)} indicators"
            )

        await conn.executemany(
            """
            INSERT INTO stats (run_id, tag, latest_value, latest_date, delta_prev,
                               delta_prev_pct, yoy, yoy_pct, summary, computed_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
            ON CONFLICT (run_id, tag) DO UPDATE SET
                latest_value=EXCLUDED.latest_value, latest_date=EXCLUDED.latest_date,
                delta_prev=EXCLUDED.delta_prev, delta_prev_pct=EXCLUDED.delta_prev_pct,
                yoy=EXCLUDED.yoy, yoy_pct=EXCLUDED.yoy_pct, summary=EXCLUDED.summary
            """,
            stats_rows,
        )
        await conn.execute(
            "UPDATE run_log SET status='succeeded', updated_at=now() WHERE run_id=$1",
            uuid.UUID(run_id),
        )

        # ── Carry the LLM-ready bundle in the Mongo helper doc ─────────
        import numpy as np

        def _norm(obj):
            """Make stats/charts JSON-safe for Mongo (numpy → native)."""
            if isinstance(obj, dict):
                return {k: _norm(v) for k, v in obj.items()}
            if isinstance(obj, (list, tuple)):
                return [_norm(v) for v in obj]
            if isinstance(obj, np.ndarray):
                return _norm(obj.tolist())
            if isinstance(obj, np.generic):
                return obj.item()
            if isinstance(obj, (pd.Timestamp, datetime)):
                return obj.isoformat()
            return obj

        news_summaries = [
            f"[{n['tag']}] {n['source']}: {n['title']} — {n['summary'][:200]}"
            for n in fresh_news
        ]

        await store.update(
            "stats",
            {
                "skip": False,
                "data_summaries": data_summaries,
                "news_summaries": news_summaries,
                "fresh_news": fresh_news,
                "fresh_tags": fresh_tags,
                "value_hashes": {t: _value_hash(series_map[t]) for t in fresh_tags},
                "charts": _norm(charts_pool),
                "tables": _norm(tables_pool),
                "stats": [
                    {
                        "tag": r[1],
                        "latest_value": r[2],
                        "latest_date": (
                            r[3].isoformat()
                            if hasattr(r[3], "isoformat")
                            else str(r[3])
                        ),
                        "delta_prev": r[4],
                        "delta_prev_pct": r[5],
                        "yoy": r[6],
                        "yoy_pct": r[7],
                        "summary": r[8],
                    }
                    for r in stats_rows
                ],
            },
        )

        logger.info(
            "compute_stats done: %d summaries, %d fresh news (run %s)",
            len(data_summaries),
            len(fresh_news),
            run_id,
        )
        return {
            "run_id": run_id,
            "skip": False,
            "fresh_series": len(fresh_tags),
            "fresh_news": len(fresh_news),
        }
    finally:
        await conn.close()
        await client.close()


def main(
    postgres_dsn: str,
    mongo_uri: str,
    run_id: str,
    news_cooldown_hours: int = NEWS_COOLDOWN_HOURS,
) -> dict:
    """Sync Windmill entrypoint (workers call ``main`` without awaiting)."""
    return asyncio.run(
        _main(postgres_dsn, mongo_uri, run_id, news_cooldown_hours)
    )
