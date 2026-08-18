"""Windmill flow step 2 — compute stats, charts, tables + freshness filter.

Reads the observations stored in PostgreSQL by step 1, applies the same
freshness logic as the classic pipeline (news dedup window + macro-release
freshness, both backed by PostgreSQL tables), computes Δ-prev + YoY stats,
builds Plotly charts/tables and writes everything the LLM needs into the
MongoDB helper document.

Windmill wiring:
    inputs: postgres_dsn, mongo_uri, run_id
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import uuid
from datetime import UTC, datetime, timedelta

import pandas as pd
from common import FlowStore, mongo_client, pg_connect

from eurobot.stats.compute import compute_stats_for_series
from eurobot.viz.plotly_charts import (
    make_delta_bar_chart,
    make_line_chart,
    make_summary_table,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("eurobot.windmill.stats")

# Defaults mirror the classic pipeline config.
NEWS_COOLDOWN_HOURS = 48


def _value_hash(series: pd.Series) -> str:
    return hashlib.sha256(series.tail(5).round(4).to_json().encode()).hexdigest()[:16]


async def main(
    postgres_dsn: str,
    mongo_uri: str,
    run_id: str,
    news_cooldown_hours: int = NEWS_COOLDOWN_HOURS,
) -> dict:
    """Read PG observations → stats/charts/tables → Mongo helper doc."""
    conn = await pg_connect(postgres_dsn)
    client = mongo_client(mongo_uri)
    try:
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
