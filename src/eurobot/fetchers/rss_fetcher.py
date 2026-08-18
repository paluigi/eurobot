"""RSS news fetcher — euro-area economic and financial feeds.

Collects items from an official source (ECB), international press (The
Economist, The Guardian, DW), Italian press (ANSA, Il Sole 24 Ore) and
think tanks (Bruegel).  Items are tagged ``[NEWS_xxx]`` for LLM selection.
All feeds are filtered for euro-area economic/financial relevance.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass

import feedparser

from eurobot.utils.retries import call_with_retries

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Feed registry — URLs of euro-area economic/financial RSS feeds.
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


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------


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

    Returns at most ``max_items`` items.  Dedup against the SQLite ``news_seen``
    table is handled by the dedup module, not here — this function returns
    all relevant fresh items.
    """
    items: list[NewsItem] = []
    seen_hashes: set[str] = set()

    for source_name, feed_url in FEEDS.items():
        try:
            # Use requests with a proper User-Agent, then pass content to
            # feedparser. Retried on transient failures with bounded tenacity.
            import requests as req_mod

            def _get(url: str):
                resp = req_mod.get(
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
