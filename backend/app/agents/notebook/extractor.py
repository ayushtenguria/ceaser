"""Notebook Extractor Agent — intelligently extracts reusable steps from conversations.

Smart extraction:
- Skips corrections ("sorry", "no not that", "wrong")
- Skips failed queries (messages with errors)
- Skips duplicate/repeated questions
- Only keeps the FINAL version when user refined a query
- Groups related follow-ups into single steps
- Returns a DRAFT for user review before saving
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

logger = logging.getLogger(__name__)

# Patterns that indicate a correction/throwaway message
_CORRECTION_PATTERNS = [
    r"^(sorry|oops|wait|no |nope|not that|wrong|ignore|scratch|never\s?mind|my bad|actually\s*,?\s*$)",
    r"^(ok|okay|yes|yeah|sure|thanks|thank you|cool|great|perfect|got it|nice)[\s!.]*$",
]
_CORRECTION_RE = [re.compile(p, re.IGNORECASE) for p in _CORRECTION_PATTERNS]

# Minimum message length to be considered a real analysis step
_MIN_STEP_LENGTH = 10


_EXTRACT_PROMPT = """\
You are a notebook designer. Analyze this conversation and extract ONLY the meaningful
analysis steps — the questions that produced useful, correct results.

Conversation (each message marked as [USER] or [ASSISTANT] with status):
{conversation}

Your job:
1. SKIP:
   - Corrections ("sorry", "no not that", "wrong one")
   - Greetings and small talk
   - Failed queries (marked as FAILED)
   - Duplicate questions (keep only the FINAL refined version)
   - Vague/incomplete messages

2. KEEP:
   - Questions that got successful data/chart/analysis results
   - Only the FINAL version if the user refined a question multiple times
   - Each step should be SELF-CONTAINED (make sense without previous context)

3. For each step, provide:
   - A clean, reusable prompt (generic enough to work with different data)
   - A short label (2-5 words)
   - Whether it produces a chart

Return JSON:
{{
  "title": "Notebook title based on the analysis theme",
  "description": "One-line description",
  "steps": [
    {{
      "label": "Short label",
      "prompt": "The reusable analysis prompt",
      "produces_chart": false,
      "original_question": "What the user actually asked"
    }}
  ]
}}
"""


async def extract_notebook_draft(
    messages: list[dict[str, Any]],
    llm: BaseChatModel,
    notebook_name: str = "",
) -> dict[str, Any]:
    """Extract a notebook DRAFT from conversation — for user review before saving.

    Returns:
        dict with "title", "description", "steps" (list of proposed steps),
        and "skipped" (list of messages that were excluded with reasons)
    """
    # Pre-filter: deterministic cleanup
    filtered, skipped = _prefilter_messages(messages)

    if not filtered:
        return {
            "title": notebook_name or "Analysis Notebook",
            "description": "No meaningful analysis steps found",
            "steps": [],
            "skipped": skipped,
        }

    # Build conversation text for LLM
    conv_lines: list[str] = []
    for i, msg in enumerate(filtered):
        role = "USER" if msg.get("role") == "user" else "ASSISTANT"
        content = msg.get("content", "")[:400]
        status = "OK"
        if msg.get("error"):
            status = "FAILED"
        elif msg.get("table_data") or msg.get("plotly_figure"):
            status = "HAS_DATA"

        conv_lines.append(f"[{i}] {role} ({status}): {content}")

    conversation_text = "\n".join(conv_lines)

    # LLM extraction
    try:
        prompt_messages = [
            SystemMessage(content=_EXTRACT_PROMPT.format(conversation=conversation_text)),
            HumanMessage(content="Extract the notebook steps."),
        ]

        response = await llm.ainvoke(prompt_messages)
        raw: str = response.content.strip()  # type: ignore[union-attr]

        if raw.startswith("```"):
            lines = raw.split("\n")
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            raw = "\n".join(lines).strip()

        data = json.loads(raw)

        steps = []
        for s in data.get("steps", []):
            steps.append({
                "label": s.get("label", "Analysis Step"),
                "prompt": s.get("prompt", ""),
                "produces_chart": s.get("produces_chart", False),
                "original_question": s.get("original_question", ""),
                "cell_type": "prompt",
                "included": True,  # User can toggle this off in preview
            })

        result = {
            "title": data.get("title", notebook_name or "Analysis Notebook"),
            "description": data.get("description", ""),
            "steps": steps,
            "skipped": skipped,
        }

        logger.info("Extracted draft: %d steps from %d messages (%d skipped)",
                     len(steps), len(messages), len(skipped))
        return result

    except Exception as exc:
        logger.warning("LLM extraction failed, using fallback: %s", exc)
        return _fallback_extraction(filtered, skipped, notebook_name)


def _prefilter_messages(messages: list[dict[str, Any]]) -> tuple[list[dict], list[dict]]:
    """Deterministic pre-filter: remove corrections, duplicates, failures.

    Returns (kept_messages, skipped_with_reasons)
    """
    kept: list[dict] = []
    skipped: list[dict] = []

    # Track user questions to detect duplicates/refinements
    seen_questions: list[str] = []

    for i, msg in enumerate(messages):
        role = msg.get("role", "")
        content = (msg.get("content") or "").strip()

        # Always keep assistant messages (they carry data)
        if role == "assistant":
            # Skip assistant errors
            if msg.get("error") and not msg.get("table_data") and not msg.get("plotly_figure"):
                skipped.append({"index": i, "content": content[:80], "reason": "Failed query"})
                continue
            kept.append(msg)
            continue

        if role != "user":
            continue

        # Skip empty/short messages
        if len(content) < _MIN_STEP_LENGTH:
            skipped.append({"index": i, "content": content, "reason": "Too short"})
            continue

        # Skip corrections
        if _is_correction(content):
            skipped.append({"index": i, "content": content[:80], "reason": "Correction/acknowledgment"})
            continue

        # Detect duplicate/refined questions
        is_duplicate = False
        content_lower = content.lower().strip()
        for prev_q in seen_questions:
            similarity = _quick_similarity(content_lower, prev_q)
            if similarity > 0.7:
                # This is a refinement — remove the old version, keep the new one
                # Find and remove the old user message from kept
                kept = [m for m in kept if (m.get("content") or "").lower().strip() != prev_q]
                skipped.append({"index": i, "content": f"(Refined version of earlier question)", "reason": "Superseded by refinement"})
                # Actually keep the NEW version
                is_duplicate = False  # Keep this one
                break

        seen_questions.append(content_lower)

        # Check if the NEXT assistant message has useful output
        has_useful_response = False
        for next_msg in messages[i + 1:]:
            if next_msg.get("role") == "assistant":
                if (next_msg.get("table_data") or next_msg.get("plotly_figure") or
                        len(next_msg.get("content", "")) > 100):
                    has_useful_response = True
                break

        if not has_useful_response:
            skipped.append({"index": i, "content": content[:80], "reason": "No useful response"})
            continue

        kept.append(msg)

    return kept, skipped


def _is_correction(text: str) -> bool:
    """Check if a message is a correction/throwaway."""
    for pattern in _CORRECTION_RE:
        if pattern.search(text.strip()):
            return True
    return False


def _quick_similarity(a: str, b: str) -> float:
    """Quick word-overlap similarity. O(n) with sets."""
    words_a = set(a.split())
    words_b = set(b.split())
    if not words_a or not words_b:
        return 0.0
    overlap = len(words_a & words_b)
    return overlap / min(len(words_a), len(words_b))


def _fallback_extraction(
    filtered: list[dict], skipped: list[dict], name: str
) -> dict[str, Any]:
    """Fallback: create steps from filtered user messages."""
    steps = []
    for msg in filtered:
        if msg.get("role") != "user":
            continue
        content = (msg.get("content") or "").strip()
        if len(content) < _MIN_STEP_LENGTH:
            continue

        steps.append({
            "label": content[:40],
            "prompt": content,
            "produces_chart": any(w in content.lower() for w in ("chart", "plot", "graph", "trend", "visuali")),
            "original_question": content,
            "cell_type": "prompt",
            "included": True,
        })

    return {
        "title": name or "Analysis Notebook",
        "description": "Auto-extracted analysis steps",
        "steps": steps[:10],
        "skipped": skipped,
    }
