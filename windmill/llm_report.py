"""Windmill flow step 3 — run the 3-stage LLM workflow.

Loads the run bundle from the MongoDB helper document, runs the same
selection → drafting → self-review cascade as the classic pipeline, and
stores the assembled payload back into the helper document.

New in llm-pycascade 0.2.0: the cascade is configured from a **dict**
(``cascade_config`` — a Windmill variable) via ``config_from_dict`` with a
``:memory:`` database, so no TOML file or persistent state is needed on the
ephemeral Windmill worker.

Wrapped in bounded tenacity retries (``max_attempts``), resuming from the
last valid stage via checkpoints — identical semantics to the classic
pipeline (an unfixable self-review rejection redrafts, keeping the
selection).

Windmill wiring:
    inputs: mongo_uri, run_id, cascade_config (dict), max_attempts=4
"""

from __future__ import annotations

import json
import logging

from common import FlowStore, mongo_client

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("eurobot.windmill.llm")


class StageFailure(Exception):
    """A pipeline stage produced no usable result — retry from this stage."""


async def query_llm_dict(
    user_prompt: str,
    system_prompt: str,
    cascade_config: dict,
) -> str:
    """One cascade query using a dict-built config (llm-pycascade ≥0.2.0)."""
    from llm_pycascade import config_from_dict, init_db, run_cascade
    from llm_pycascade.models import Conversation, Message, MessageRole

    cfg = config_from_dict(cascade_config)
    messages = []
    if system_prompt:
        messages.append(Message(role=MessageRole.SYSTEM, content=system_prompt))
    messages.append(Message(role=MessageRole.USER, content=user_prompt))
    conversation = Conversation(messages=messages)

    conn = await init_db(cascade_config.get("database", {}).get("path", ":memory:"))
    try:
        response = await run_cascade("default", conversation, cfg, conn)
    finally:
        await conn.close()

    text_parts = []
    for block in response.content:
        if hasattr(block, "text"):
            text_parts.append(block.text)
        elif hasattr(block, "content"):
            text_parts.append(block.content)
    return "\n".join(text_parts)


async def _report(
    carried: dict,
    cascade_config: dict,
    checkpoints: dict,
) -> dict:
    """LLM stages (select → draft → review) → assembled payload dict."""
    from eurobot.llm import prompts
    from eurobot.llm.parser import parse_json_safe

    data_summaries = carried["data_summaries"]
    news_summaries = carried["news_summaries"]

    # ── Stage 1 — Selection ─────────────────────────────────────────────
    if "selected_ids" not in checkpoints:
        sys_prompt, usr_prompt = prompts.build_selection_prompt(
            data_summaries, news_summaries
        )
        raw = await query_llm_dict(usr_prompt, sys_prompt, cascade_config)
        selected_ids = parse_json_safe(raw, prefer=list)
        if not selected_ids:
            raise StageFailure("Stage 1 returned no valid selections")
        checkpoints["selected_ids"] = selected_ids
        logger.info("Stage 1 selected: %s", selected_ids)
    selected_ids = checkpoints["selected_ids"]

    selected_context_parts = []
    selected_news = []
    selected_tags: list[str] = []
    for item_id in selected_ids:
        if item_id.startswith("NEWS_"):
            for n in carried["fresh_news"]:
                if n["tag"] == item_id:
                    selected_news.append(n)
                    selected_context_parts.append(
                        f"[{n['tag']}] {n['source']}: {n['title']} — {n['summary'][:200]}"
                    )
                    break
        elif item_id.startswith("DATA_"):
            tag = item_id.replace("DATA_", "")
            if tag in carried["fresh_tags"]:
                selected_tags.append(tag)
            for s in data_summaries:
                if s.startswith(f"[{item_id}]"):
                    selected_context_parts.append(s)
                    break
    selected_context = "\n".join(selected_context_parts)

    # ── Stage 2 — Drafting ──────────────────────────────────────────────
    if "draft" not in checkpoints:
        sys_prompt, usr_prompt = prompts.build_drafting_prompt(selected_context)
        raw = await query_llm_dict(usr_prompt, sys_prompt, cascade_config)
        draft = parse_json_safe(raw, prefer=dict)
        if not draft or "content_markdown" not in draft:
            raise StageFailure("Stage 2 returned an invalid draft")
        checkpoints["draft"] = draft
        logger.info(
            "Draft: title='%s', %d chars markdown",
            draft.get("title", "?"),
            len(draft["content_markdown"]),
        )
    draft = checkpoints["draft"]

    # ── Stage 3 — Self-review ───────────────────────────────────────────
    if not checkpoints.get("reviewed"):
        source_summaries = "\n".join(
            s["summary"] for s in carried["stats"] if s["tag"] in selected_tags
        )
        sys_prompt, usr_prompt = prompts.build_review_prompt(
            json.dumps(draft, indent=2), source_summaries
        )
        raw = await query_llm_dict(usr_prompt, sys_prompt, cascade_config)
        review = parse_json_safe(raw, prefer=dict)

        if review and review.get("approved") is False:
            if review.get("corrected_markdown"):
                logger.warning("Self-review found errors — applying corrections")
                draft["content_markdown"] = review["corrected_markdown"]
            else:
                checkpoints.pop("draft", None)
                raise StageFailure(
                    "Self-review rejected the draft — redrafting on next attempt"
                )
        else:
            logger.info("Self-review approved the draft")
        checkpoints["reviewed"] = True

    # ── Assemble payload (charts/tables for selected series) ───────────
    charts = [carried["charts"][t] for t in selected_tags if t in carried["charts"]]
    tables = [carried["tables"][t] for t in selected_tags if t in carried["tables"]]

    links = [
        {"label": n["title"], "url": n["link"], "description": n["source"]}
        for n in selected_news
    ]
    payload = {
        "title": draft.get("title", "Euro-area economic update"),
        "summary": draft.get("summary", ""),
        "author": "eurobot",
        "content_markdown": draft["content_markdown"],
        "tables": tables,
        "charts": charts,
        "links": links,
        "tags": draft.get("tags", []),
    }
    return payload


async def main(
    mongo_uri: str,
    run_id: str,
    cascade_config: dict,
    max_attempts: int = 4,
) -> dict:
    """Run the LLM stages; write the payload into the Mongo helper doc."""
    client = mongo_client(mongo_uri)
    try:
        store = FlowStore(client, run_id)
        state = await store.load()
        if state is None:
            raise RuntimeError(f"flow helper document for run {run_id} not found")
        carried = state["docs"]
        if carried.get("skip"):
            await store.update("llm", {"skip": True})
            return {"run_id": run_id, "skip": True}

        # Bounded tenacity retry, resuming from last valid stage.
        from tenacity import (
            AsyncRetrying,
            before_sleep_log,
            stop_after_attempt,
            wait_exponential,
        )

        checkpoints: dict = {}
        payload: dict | None = None
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential(multiplier=5, min=10, max=60),
                before_sleep=before_sleep_log(logger, logging.WARNING),
                reraise=True,
            ):
                with attempt:
                    payload = await _report(carried, cascade_config, checkpoints)
        except Exception as exc:
            logger.error(
                "LLM stages failed after %d attempts: %s: %s",
                max_attempts,
                type(exc).__name__,
                exc,
            )
            raise
        assert payload is not None  # reraise=True guarantees success here
        result = payload

        # Merge the payload into the carried docs (keep fresh_news/stats/
        # value_hashes from step 2 — step 4 needs them post-publish).
        carried["payload"] = result
        carried["selected_tags"] = checkpoints.get("selected_ids", [])
        await store.update("llm", carried)
        logger.info(
            "llm_report done (run %s): payload %d chars, %d charts",
            run_id,
            len(result["content_markdown"]),
            len(result["charts"]),
        )
        return {"run_id": run_id, "skip": False, "title": result["title"]}
    finally:
        await client.close()
