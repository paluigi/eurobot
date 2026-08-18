"""LLM-response JSON parsing — shared by the classic pipeline and Windmill.

Tolerates reasoning-model ``<think>`` blocks (even with the JSON inside
them), markdown code fences, and surrounding prose: collects every complete
JSON value found and returns the last one — models place the final answer at
the end. If ``prefer`` (list or dict) is given, only candidates of that type
are considered.
"""

from __future__ import annotations

import json
import logging
import re

logger = logging.getLogger(__name__)


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
