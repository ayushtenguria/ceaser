"""Conversation memory — compress exchanges and build relevance-scored history.

Instead of dumping raw messages into the LLM prompt, this module:
1. Compresses each Q&A exchange into a ~50 token summary
2. Scores historical summaries by relevance to the current question
3. Picks the top matches within a token budget

Raw messages stay in DB for the UI and audit trail. The LLM only sees
compressed, relevant context.
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

_HISTORY_TOKEN_BUDGET = 4000  # chars (~1000 tokens) — much smaller than raw 12K
_MAX_HISTORY_ITEMS = 15


# ── Exchange Summarization ──────────────────────────────────────────


def summarize_exchange(
    user_message: str,
    assistant_message: str,
    sql_query: str | None = None,
    code_block: str | None = None,
    table_data: dict | None = None,
    error: str | None = None,
) -> tuple[str, str]:
    """Compress a Q&A exchange into short summaries.

    Returns (user_summary, assistant_summary).
    Template-based — no LLM call, deterministic, free.
    """
    # User summary: just the question, truncated
    user_summary = user_message.strip()[:120]

    # Assistant summary: structured extraction
    parts: list[str] = []

    # What method was used
    if sql_query:
        tables = _extract_tables(sql_query)
        if tables:
            parts.append(f"SQL on {', '.join(tables[:3])}")
        aggs = _extract_aggregations(sql_query)
        if aggs:
            parts.append(f"({', '.join(aggs)})")
    elif code_block:
        libs = _extract_libraries(code_block)
        parts.append("Python" + (f" ({', '.join(libs)})" if libs else ""))

    # Key numbers from the response
    numbers = _extract_key_numbers(assistant_message)
    if numbers:
        parts.append(f"Results: {', '.join(numbers[:5])}")

    # Row count from table data
    if table_data:
        total = table_data.get("total_rows", 0)
        cols = table_data.get("columns", [])
        if total:
            parts.append(f"{total} rows")
        if cols:
            parts.append(f"cols: {', '.join(cols[:5])}")

    # Error
    if error and not parts:
        parts.append(f"Error: {error[:80]}")

    # Fallback: first sentence of response
    if not parts:
        first_sentence = assistant_message.split(".")[0][:100]
        parts.append(first_sentence)

    assistant_summary = " | ".join(parts)

    return user_summary, assistant_summary


def summarize_user_message(message: str) -> str:
    """Create a summary for a standalone user message."""
    return message.strip()[:120]


def summarize_assistant_message(
    content: str,
    sql_query: str | None = None,
    code_block: str | None = None,
    table_data: dict | None = None,
    error: str | None = None,
) -> str:
    """Create a summary for a standalone assistant message."""
    _, summary = summarize_exchange(
        "",
        content,
        sql_query,
        code_block,
        table_data,
        error,
    )
    return summary


# ── Relevance-Scored History ────────────────────────────────────────


def build_relevant_history(
    prev_msgs: list,
    current_question: str,
    max_chars: int = _HISTORY_TOKEN_BUDGET,
    max_items: int = _MAX_HISTORY_ITEMS,
) -> list[dict[str, str]]:
    """Build conversation history scored by relevance to the current question.

    Uses summaries when available, falls back to truncated raw content.
    Scores by: keyword relevance (50%), recency (30%), correction boost (20%).
    """
    if not prev_msgs:
        return []

    current_keywords = _extract_keywords(current_question)

    # Score each message
    scored: list[tuple[Any, float]] = []
    total_msgs = len(prev_msgs)

    for i, msg in enumerate(prev_msgs[:-1]):  # Exclude the current user message
        content = msg.summary if msg.summary else (msg.content or "")[:150]

        # Recency score: newer messages score higher (0.0 to 1.0)
        position = i / max(total_msgs - 1, 1)
        recency = position  # 0 = oldest, 1 = newest

        # Relevance score: keyword overlap with current question
        msg_keywords = _extract_keywords(content)
        if current_keywords and msg_keywords:
            overlap = len(current_keywords & msg_keywords)
            relevance = overlap / max(len(current_keywords), 1)
        else:
            relevance = 0.0

        # Correction boost: corrections are always worth keeping
        correction_boost = 0.0
        if msg.summary and any(
            kw in msg.summary.lower() for kw in ("error", "wrong", "fix", "correction")
        ):
            correction_boost = 1.0
        if msg.role == "user" and any(
            kw in (msg.content or "").lower()
            for kw in ("no ", "not ", "wrong", "actually", "i meant")
        ):
            correction_boost = 1.0

        # Combined score
        score = recency * 0.3 + relevance * 0.5 + correction_boost * 0.2

        scored.append((msg, score))

    # Sort by score descending
    scored.sort(key=lambda x: x[1], reverse=True)

    # Build history within budget
    history: list[dict[str, str]] = []
    total_chars = 0

    for msg, score in scored:
        if len(history) >= max_items:
            break

        # Use summary if available, else truncated content
        content = msg.summary if msg.summary else (msg.content or "")[:150]

        # Add context markers for assistant messages
        if msg.role == "assistant" and not msg.summary:
            if msg.table_data:
                cols = msg.table_data.get("columns", [])
                total = msg.table_data.get("total_rows", 0)
                content += f" [Data: {total} rows, cols: {', '.join(cols[:5])}]"
            if msg.plotly_figure:
                content += " [Chart generated]"

        msg_chars = len(content)
        if total_chars + msg_chars > max_chars:
            # Try to fit a truncated version
            remaining = max_chars - total_chars
            if remaining > 50:
                content = content[:remaining]
            else:
                break

        history.append({"role": msg.role, "content": content})
        total_chars += msg_chars

    # Re-sort by chronological order for the LLM (oldest first)
    # We need to track original indices
    {id(msg): i for i, (msg, _) in enumerate(scored)}
    history_with_idx = []
    for h in history:
        # Find the original message by matching content
        for msg, _ in scored:
            c = msg.summary if msg.summary else (msg.content or "")[:150]
            if h["content"].startswith(c[:50]) and h["role"] == msg.role:
                history_with_idx.append((msg.created_at, h))
                break

    history_with_idx.sort(key=lambda x: x[0])
    history = [h for _, h in history_with_idx]

    logger.info(
        "Relevant history: %d/%d messages selected, %d chars, keywords=%s",
        len(history),
        total_msgs,
        total_chars,
        list(current_keywords)[:5],
    )

    return history


# ── Private Helpers ─────────────────────────────────────────────────

_STOP_WORDS = frozenset(
    {
        "the",
        "a",
        "an",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "have",
        "has",
        "had",
        "do",
        "does",
        "did",
        "will",
        "would",
        "can",
        "could",
        "should",
        "may",
        "might",
        "must",
        "i",
        "me",
        "my",
        "we",
        "our",
        "you",
        "your",
        "show",
        "me",
        "give",
        "tell",
        "find",
        "get",
        "list",
        "display",
        "please",
        "also",
        "just",
        "like",
        "want",
        "need",
        "what",
        "which",
        "who",
        "how",
        "when",
        "where",
        "why",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "with",
        "by",
        "from",
        "and",
        "or",
        "but",
        "not",
        "no",
        "all",
        "each",
        "every",
    }
)


def _extract_keywords(text: str) -> set[str]:
    """Extract meaningful keywords from text."""
    words = re.findall(r"[a-zA-Z_]+", text.lower())
    return {w for w in words if w not in _STOP_WORDS and len(w) > 2}


def _extract_tables(sql: str) -> list[str]:
    """Extract table names from SQL."""
    tables = set()
    for m in re.finditer(r"\bFROM\s+(\w+)", sql, re.IGNORECASE):
        tables.add(m.group(1))
    for m in re.finditer(r"\bJOIN\s+(\w+)", sql, re.IGNORECASE):
        tables.add(m.group(1))
    return sorted(tables)


def _extract_aggregations(sql: str) -> list[str]:
    """Extract aggregation functions from SQL."""
    aggs = set()
    for func in ("COUNT", "SUM", "AVG", "MAX", "MIN"):
        if re.search(rf"\b{func}\s*\(", sql, re.IGNORECASE):
            aggs.add(func)
    if re.search(r"\bGROUP\s+BY\b", sql, re.IGNORECASE):
        aggs.add("GROUP BY")
    return sorted(aggs)


def _extract_libraries(code: str) -> list[str]:
    """Extract imported library names from Python code."""
    libs = set()
    for m in re.finditer(r"(?:import|from)\s+(\w+)", code):
        lib = m.group(1)
        if lib not in ("json", "os", "sys", "re", "math"):
            libs.add(lib)
    return sorted(libs)[:3]


def _extract_key_numbers(text: str) -> list[str]:
    """Extract notable numbers from response text."""
    numbers = []
    # Currency amounts: $1,234.56, ₹2.4Cr
    for m in re.finditer(r"[\$₹€£]\s?[\d,]+\.?\d*\s?[KMBCr]*", text):
        numbers.append(m.group().strip())
    # Percentages
    for m in re.finditer(r"\d+\.?\d*\s?%", text):
        numbers.append(m.group().strip())
    # Large formatted numbers: 1,234 or 12,345,678
    for m in re.finditer(r"\b\d{1,3}(?:,\d{3})+\b", text):
        numbers.append(m.group())
    return numbers[:5]
