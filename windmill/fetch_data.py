"""Windmill flow step 1 — fetch data.

Fetches macro (SDMX), market (Yahoo) and news (RSS) data using the eurobot
fetchers (already resilient per series, with bounded transient retries),
then upserts everything into PostgreSQL and opens the MongoDB helper
document that carries run state between flow steps.

Windmill wiring:
    inputs: postgres_dsn, mongo_uri  (Windmill resources/variables)
    output: {"run_id": ..., "fetched": {...}} — stored in the Mongo helper.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import date

from common import FlowStore, mongo_client, pg_apply_schema, pg_connect, utc_now_naive

from eurobot.fetchers.market_fetcher import compute_btp_bund_spread, fetch_all_markets
from eurobot.fetchers.rss_fetcher import get_latest_news
from eurobot.fetchers.sdmx_fetcher import fetch_all_macro

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("eurobot.windmill.fetch")


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


async def main(
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
        await pg_apply_schema(conn)
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
