"""Main orchestrator — coordinates the full end-to-end pipeline.

Flow:
  1. Fetch macro (SDMX), market (Yahoo Finance), and news (RSS) data.
  2. Filter for freshness (dedup) and compute statistics (Δ prev + YoY).
  3. Generate Plotly charts and summary tables.
  4. Stage 1 LLM: Select items forming a cohesive theme.
  5. Stage 2 LLM: Draft narrative with title, summary, tags.
  6. Stage 3 LLM: Self-review for numeric accuracy.
  7. Assemble zzboard payload and publish.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sys

from tenacity import Retrying, before_sleep_log, stop_after_attempt, wait_exponential

from eurobot import config
from eurobot.fetchers.sdmx_fetcher import fetch_all_macro
from eurobot.fetchers.market_fetcher import fetch_all_markets, compute_btp_bund_spread
from eurobot.fetchers.rss_fetcher import get_latest_news
from eurobot.stats.compute import compute_stats_for_series
from eurobot.viz.plotly_charts import make_line_chart, make_summary_table, make_delta_bar_chart
from eurobot.pipeline import dedup
from eurobot.pipeline.payload_builder import assemble_payload, save_payload
from eurobot.publish.zzboard_client import publish_payload
from eurobot.llm.cascade_runner import query_llm
from eurobot.llm.parser import parse_json_safe as _parse_json_safe
from eurobot.llm import prompts

logger = config.setup_logging()


def run() -> int:
    """Execute the full pipeline. Returns exit code (0=success)."""
    logger.info("=" * 60)
    logger.info("eurobot pipeline started")
    logger.info("=" * 60)

    # ── 1. Fetch data ────────────────────────────────────────────────────
    logger.info("STEP 1: Fetching data sources")
    macro_items = fetch_all_macro()
    market_items = fetch_all_markets()
    all_news = get_latest_news(max_items=config.MAX_NEWS_ITEMS)

    # Compute BTP-Bund spread from sovereign yields
    macro_by_tag = {it["tag"]: it for it in macro_items}
    btp = macro_by_tag.get("IT_10Y_YIELD", {}).get("series")
    bund = macro_by_tag.get("DE_10Y_YIELD", {}).get("series")
    if btp is not None and bund is not None:
        spread = compute_btp_bund_spread(btp, bund)
        if spread is not None:
            macro_items.append({
                "tag": "BTP_BUND_SPREAD",
                "title": "BTP–Bund spread",
                "unit": "percentage points",
                "frequency": "D",
                "description": "Italian-German 10-year sovereign yield spread.",
                "series": spread,
                "spec": None,
            })

    # Combine macro + market into a single list
    all_data = macro_items + market_items
    logger.info("Total data items: %d macro + %d market = %d",
                len(macro_items), len(market_items), len(all_data))

    if not all_data and not all_news:
        logger.warning("No data or news collected — nothing to report. Exiting.")
        return 0

    # ── 2. Filter for freshness ──────────────────────────────────────────
    logger.info("STEP 2: Filtering for fresh content")
    fresh_macro = dedup.filter_fresh_macro(macro_items)
    fresh_news = dedup.filter_fresh_news(all_news)

    # Market data is always fresh (daily updates)
    fresh_data = fresh_macro + market_items

    if not fresh_data and not fresh_news:
        logger.info("No fresh content today — nothing to report. Exiting.")
        return 0

    # ── 3. Compute statistics + generate charts/tables ───────────────────
    logger.info("STEP 3: Computing stats and generating visualizations")
    data_summaries: list[str] = []
    all_stats = []
    charts_pool: dict[str, dict] = {}  # tag → chart dict
    tables_pool: dict[str, dict] = {}  # tag → table dict

    for item in fresh_data:
        series = item.get("series")
        if series is None or series.empty:
            continue
        tag = item["tag"]
        title = item["title"]
        freq = item.get("frequency", "M")
        unit = item.get("unit", "")

        stats = compute_stats_for_series(series, tag, title, frequency=freq)
        if stats is None:
            continue
        all_stats.append(stats)

        # Generate both chart and table for this series
        chart = make_line_chart(series, tag, title, y_title=unit)
        table = make_summary_table(stats, tag)
        charts_pool[tag] = chart
        tables_pool[tag] = table

        data_summaries.append(
            f"[DATA_{tag}] {stats.summary} (unit: {unit})"
        )

    # Add a cross-indicator bar chart if we have multiple series
    if len(all_stats) >= 2:
        overview = make_delta_bar_chart(all_stats)
        charts_pool["ALL_DELTA"] = overview
        data_summaries.append(
            f"[DATA_ALL_DELTA] Overview bar chart: Δ vs previous period across {len(all_stats)} indicators"
        )

    news_summaries = [item.to_prompt_line() for item in fresh_news]

    logger.info("Prepared %d data items + %d news items for LLM",
                len(data_summaries), len(news_summaries))

    if not data_summaries and not news_summaries:
        logger.info("Nothing to present to LLM. Exiting.")
        return 0

    # ── 4-8. LLM stages + publish, with in-run retries ──────────────────
    # Tenacity retries the whole reporting block, but completed stages are
    # stored in `checkpoints`, so each attempt picks up from the last valid
    # step instead of restarting the pipeline. Fetch/stats stay outside the
    # retry loop (they are already per-series resilient).
    max_attempts = config.PIPELINE_MAX_ATTEMPTS
    retryer = Retrying(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=5, min=10, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    checkpoints: dict = {}
    try:
        return retryer(
            _report_and_publish,
            fresh_data, fresh_news, data_summaries, news_summaries,
            all_stats, charts_pool, tables_pool, checkpoints,
        )
    except Exception as exc:
        logger.error(
            "Pipeline failed after %d attempts — giving up until the next "
            "scheduled run (%s: %s)",
            max_attempts, type(exc).__name__, exc,
        )
        return 1


class StageFailure(Exception):
    """A pipeline stage produced no usable result — retry from this stage."""


def _report_and_publish(
    fresh_data: list,
    fresh_news: list,
    data_summaries: list[str],
    news_summaries: list[str],
    all_stats: list,
    charts_pool: dict,
    tables_pool: dict,
    checkpoints: dict,
) -> int:
    """LLM stages (select → draft → review) + payload assembly and publish.

    Runs under tenacity retry. Stage results are cached in ``checkpoints``:
    a retry attempt skips every stage that already succeeded and re-runs
    only the failed one and those after it.
    """
    # ── Stage 1 — Selection ─────────────────────────────────────────────
    if "selected_ids" not in checkpoints:
        logger.info("STEP 4: LLM Stage 1 — Selection")
        sys_prompt, usr_prompt = prompts.build_selection_prompt(
            data_summaries, news_summaries
        )
        raw_response = query_llm(usr_prompt, system_prompt=sys_prompt)
        selected_ids = _parse_json_safe(raw_response, prefer=list)

        if not selected_ids:
            raise StageFailure("Stage 1 returned no valid selections")
        checkpoints["selected_ids"] = selected_ids
        logger.info("Stage 1 selected: %s", selected_ids)
    selected_ids = checkpoints["selected_ids"]

    # Build selected context for Stage 2
    selected_context_parts = []
    selected_news: list = []
    selected_tags: set[str] = set()

    for item_id in selected_ids:
        if item_id.startswith("NEWS_"):
            # Find the matching news item
            for n in fresh_news:
                if n.tag == item_id:
                    selected_news.append(n)
                    selected_context_parts.append(n.to_prompt_line())
                    break
        elif item_id.startswith("DATA_"):
            tag = item_id.replace("DATA_", "")
            selected_tags.add(tag)
            # Find the matching data summary
            for s in data_summaries:
                if s.startswith(f"[{item_id}]"):
                    selected_context_parts.append(s)
                    break

    selected_context = "\n".join(selected_context_parts)

    # ── Stage 2 — Drafting ──────────────────────────────────────────────
    if "draft" not in checkpoints:
        logger.info("STEP 5: LLM Stage 2 — Drafting")
        sys_prompt, usr_prompt = prompts.build_drafting_prompt(selected_context)
        raw_response = query_llm(usr_prompt, system_prompt=sys_prompt)
        draft = _parse_json_safe(raw_response, prefer=dict)

        if not draft or "content_markdown" not in draft:
            raise StageFailure("Stage 2 returned an invalid draft")
        checkpoints["draft"] = draft
        logger.info("Draft: title='%s', %d chars markdown",
                    draft.get("title", "?"), len(draft["content_markdown"]))
    draft = checkpoints["draft"]

    # ── Stage 3 — Self-review ───────────────────────────────────────────
    if not checkpoints.get("reviewed"):
        logger.info("STEP 6: LLM Stage 3 — Self-review")
        source_summaries = "\n".join(
            [s.summary for s in all_stats if s.tag in selected_tags]
        )
        sys_prompt, usr_prompt = prompts.build_review_prompt(
            json.dumps(draft, indent=2), source_summaries
        )
        raw_response = query_llm(usr_prompt, system_prompt=sys_prompt)
        review = _parse_json_safe(raw_response, prefer=dict)

        if review and review.get("approved") is False:
            if review.get("corrected_markdown"):
                logger.warning("Self-review found errors — applying corrections")
                draft["content_markdown"] = review["corrected_markdown"]
            else:
                # Unfixable: drop the draft so the next attempt redrafts
                # from the (still valid) selection.
                checkpoints.pop("draft", None)
                raise StageFailure(
                    "Self-review rejected the draft — redrafting on next attempt"
                )
        else:
            logger.info("Self-review approved the draft")
        checkpoints["reviewed"] = True

    # ── Assemble charts/tables for selected series ──────────────────────
    selected_charts = []
    selected_tables = []
    for tag in selected_tags:
        if tag in charts_pool:
            selected_charts.append(charts_pool[tag])
        if tag in tables_pool:
            selected_tables.append(tables_pool[tag])

    # ── Assemble payload + publish ──────────────────────────────────────
    logger.info("STEP 7: Assembling and publishing payload")
    payload = assemble_payload(
        title=draft.get("title", "Euro-area economic update"),
        summary=draft.get("summary", ""),
        tags=draft.get("tags", []),
        content_markdown=draft["content_markdown"],
        charts=selected_charts,
        tables=selected_tables,
        selected_news=selected_news,
    )

    # Save for audit (every attempt keeps its own copy)
    payload_path = save_payload(payload)
    logger.info("Payload saved to %s", payload_path)

    # Publish — retried on failure like any other stage
    success = publish_payload(payload)
    if not success:
        raise StageFailure("Publish POST failed")

    # Mark items as seen/presented only after a successful publish, so a
    # failed run retries the same content at the next scheduled run.
    dedup.mark_news_seen(selected_news)
    dedup.mark_macro_presented(
        [item for item in fresh_data if item["tag"] in selected_tags]
    )
    logger.info("=" * 60)
    logger.info("eurobot pipeline completed successfully")
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(run())
