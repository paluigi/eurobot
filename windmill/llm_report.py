"""Windmill flow step 3 — run the 3-stage LLM workflow.

Loads the run bundle from the MongoDB helper document, runs the same
selection → drafting → self-review cascade as the classic pipeline, and
stores the assembled payload back into the helper document.

This script is fully self-contained: the Mongo handoff helpers, the whole
cascade config and all LLM prompt/parser code are defined below — no
eurobot package, no shared module, no external files. Its only dependencies
are published PyPI libraries, so it can be pasted into Windmill and run
standalone.

The llm-pycascade (≥0.2.0) config is built from a dict via
``config_from_dict`` with a ``:memory:`` database, so no TOML file or
persistent state is needed on the ephemeral Windmill worker. The provider
API keys are Windmill **secret variables**, fetched at runtime with
``wmill.get_variable()`` and injected with ``"api_key_literal": true``
(stored masked as ``SecretStr``) — see the ``PROVIDERS`` table below.

Wrapped in bounded tenacity retries (``max_attempts``), resuming from the
last valid stage via checkpoints — identical semantics to the classic
pipeline (an unfixable self-review rejection redrafts, keeping the
selection).

Windmill wiring:
    inputs: mongo_uri, run_id, max_attempts=4
    variables: the API-key variables listed in PROVIDERS below are fetched
               by the script itself via ``wmill.get_variable`` — no need to
               pass them as arguments in the flow.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import UTC, datetime

import wmill
from pymongo import AsyncMongoClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("eurobot.windmill.llm")

# MongoDB collections used by the flow.
DB_NAME = "eurobot"
HELPER_COLLECTION = "flow_state"  # per-run handoff docs, dropped at the end

# ---------------------------------------------------------------------------
# LLM providers & cascade — edit this section to add providers or models.
#
# Each PROVIDERS entry maps a provider name to its base URL and the Windmill
# variable holding its API key (Settings → Variables; create it as a
# secret). Keys are fetched at runtime with ``wmill.get_variable`` and
# injected with ``api_key_literal`` (llm-pycascade masks them as SecretStr).
# CASCADE_ENTRIES is the fallback order: the first entry is tried first.
# ---------------------------------------------------------------------------

PROVIDERS: dict[str, dict] = {
    "groq": {
        "base_url": "https://api.groq.com/openai/v1",
        "api_key_variable": "u/eurobot/groq_api_key",
    },
    "together": {
        "base_url": "https://api.together.xyz/v1",
        "api_key_variable": "u/eurobot/together_api_key",
    },
}

CASCADE_ENTRIES = [
    {"provider": "together", "model": "deepseek-ai/DeepSeek-V4-Flash-0731"},
    {"provider": "together", "model": "Prism-ML/Ternary-Bonsai-27B"},
    {"provider": "groq", "model": "qwen/qwen3.6-27b"},
    {"provider": "groq", "model": "openai/gpt-oss-120b"},
]


def build_cascade_config() -> dict:
    """Build the llm-pycascade dict config (API keys from Windmill)."""
    providers = {
        name: {
            "type": "openai",
            "api_key": wmill.get_variable(spec["api_key_variable"]),
            "api_key_literal": True,
            "base_url": spec["base_url"],
        }
        for name, spec in PROVIDERS.items()
    }
    return {
        "providers": providers,
        "cascades": {"default": {"entries": CASCADE_ENTRIES}},
        "database": {"path": ":memory:"},
        "failure_persistence": {"dir": "/tmp/llm-pycascade/failed_prompts"},
    }


def utc_now_naive() -> datetime:
    """Current UTC time as a naive datetime (PyMongo convention)."""
    return datetime.now(UTC).replace(tzinfo=None)


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
# LLM-response JSON parsing
#
# Tolerates reasoning-model ``<think>`` blocks (even with the JSON inside
# them), markdown code fences, and surrounding prose: collects every complete
# JSON value found and returns the last one — models place the final answer at
# the end. If ``prefer`` (list or dict) is given, only candidates of that type
# are considered.
# ---------------------------------------------------------------------------


def parse_json_safe(text: str, prefer: type | None = None) -> list | dict | None:
    """Extract and parse the last complete JSON value from an LLM response."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    decoder = json.JSONDecoder()
    candidates = []
    for match in re.finditer(r"[\[{]", text):
        try:
            obj, _ = decoder.raw_decode(text, match.start())
            candidates.append(obj)
        except json.JSONDecodeError:
            continue
    if prefer is not None:
        candidates = [c for c in candidates if isinstance(c, prefer)]
    if candidates:
        return candidates[-1]
    logger.error("Could not parse JSON from LLM response: %s", text[:200])
    return None


# ---------------------------------------------------------------------------
# Prompt templates for the three-stage LLM workflow
#
# Stage 1 — Selection:    Pick 3–4 data points + 2–3 news items forming a theme.
# Stage 2 — Drafting:     Write 2–3 paragraph narrative + title/summary/tags.
# Stage 3 — Self-review:  Verify numeric claims against source data.
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are an expert economic analyst specialising in the euro area. \
You write for a professional audience of economists and financial analysts. \
Your tone is precise, analytical, and concise — never sensational. \
You never fabricate data; you only reference figures provided to you. \
Your output must be valid JSON when requested."""


SELECTION_SYSTEM = SYSTEM_PROMPT + "\n\n" + """\
STAGE 1 — SELECTION. You are given a list of data summaries and news items. \
Select items that together form a COHESIVE euro-area economic theme for today. \
Prefer fresh macro releases and breaking news that explains market moves."""

SELECTION_USER_TEMPLATE = """\
Here are today's candidate items:

## Data items (charts & tables)

{data_items}

## News items

{news_items}

---

Select 3–4 data items and 2–3 news items that together tell a coherent \
euro-area economic story. Respond with ONLY a JSON array of the IDs you \
selected, in the order they should appear in the narrative. Example:

["DATA_001", "NEWS_002", "DATA_003", "DATA_002", "NEWS_001"]

Note: each [DATA_xxx] represents both a chart and a table for that series. \
You are selecting the series; the presentation format (chart vs table) will be \
chosen in the next stage. Respond with ONLY the JSON array, no explanation."""


DRAFTING_SYSTEM = SYSTEM_PROMPT + "\n\n" + """\
STAGE 2 — DRAFTING. Write a short economic blog post (2–3 paragraphs) for \
economists. Embed the selected data by referencing each series by name. \
You MUST provide title, summary, tags, and content_markdown."""

DRAFTING_USER_TEMPLATE = """\
You have selected the following items for today's post:

{selected_context}

---

Write a 2–3 paragraph blog post for a professional economist audience. \
Rules:
1. Open with the most significant development.
2. Connect the data to the news contextually.
3. Reference each data series by name when discussing it.
4. Use markdown formatting (## subheadings, **bold** for key figures).
5. Do NOT fabricate any numbers — use only the figures in the context above.
6. The post should be ~150–250 words.

Respond with ONLY a JSON object with this exact structure:
{{
  "title": "A concise headline (max 12 words)",
  "summary": "1-sentence summary of the post",
  "tags": ["tag1", "tag2", ...],
  "content_markdown": "The full markdown body"
}}"""


REVIEW_SYSTEM = SYSTEM_PROMPT + "\n\n" + """\
STAGE 3 — SELF-REVIEW. You are a fact-checker. Verify every numeric claim in \
the draft against the provided source data. Flag any discrepancy. If a claim \
is wrong and cannot be corrected, reject the post."""

REVIEW_USER_TEMPLATE = """\
Draft post:
{draft_json}

Source data summaries (ground truth):
{source_summaries}

---

Verify every numeric figure mentioned in the draft against the source data. \
Check that:
1. Every number matches the source exactly (or is a correct arithmetic derivation).
2. No figures are fabricated or attributed to the wrong series.
3. The direction of change (↑/↓) is correct.

Respond with ONLY a JSON object:
{{
  "approved": true | false,
  "errors": ["description of each error found"],
  "corrected_markdown": "the corrected content_markdown, or null if approved"
}}"""


def build_selection_prompt(
    data_summaries: list[str],
    news_summaries: list[str],
) -> tuple[str, str]:
    """Build (system, user) prompt pair for Stage 1.

    Args:
        data_summaries: One-line summaries, each prefixed with [DATA_xxx].
        news_summaries: One-line summaries, each prefixed with [NEWS_xxx].

    Returns:
        (system_prompt, user_prompt) tuple.
    """
    data_text = "\n".join(data_summaries) if data_summaries else "(none)"
    news_text = "\n".join(news_summaries) if news_summaries else "(none)"
    user = SELECTION_USER_TEMPLATE.format(
        data_items=data_text,
        news_items=news_text,
    )
    return SELECTION_SYSTEM, user


def build_drafting_prompt(selected_context: str) -> tuple[str, str]:
    """Build (system, user) prompt pair for Stage 2."""
    return DRAFTING_SYSTEM, DRAFTING_USER_TEMPLATE.format(
        selected_context=selected_context
    )


def build_review_prompt(
    draft_json: str,
    source_summaries: str,
) -> tuple[str, str]:
    """Build (system, user) prompt pair for Stage 3."""
    return REVIEW_SYSTEM, REVIEW_USER_TEMPLATE.format(
        draft_json=draft_json,
        source_summaries=source_summaries,
    )


# ---------------------------------------------------------------------------
# Step 3 — the 3-stage LLM workflow
# ---------------------------------------------------------------------------


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
    data_summaries = carried["data_summaries"]
    news_summaries = carried["news_summaries"]

    # ── Stage 1 — Selection ─────────────────────────────────────────────
    if "selected_ids" not in checkpoints:
        sys_prompt, usr_prompt = build_selection_prompt(data_summaries, news_summaries)
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
        sys_prompt, usr_prompt = build_drafting_prompt(selected_context)
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
        sys_prompt, usr_prompt = build_review_prompt(
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


async def _main(
    mongo_uri: str,
    run_id: str,
    max_attempts: int = 4,
) -> dict:
    """Run the LLM stages; write the payload into the Mongo helper doc."""
    cascade_config = build_cascade_config()
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


def main(
    mongo_uri: str,
    run_id: str,
    max_attempts: int = 4,
) -> dict:
    """Sync Windmill entrypoint (workers call ``main`` without awaiting)."""
    return asyncio.run(_main(mongo_uri, run_id, max_attempts))
