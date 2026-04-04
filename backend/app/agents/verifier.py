"""Result verification node — LLM checks if query results answer the question."""

from __future__ import annotations

import json
import logging
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import SystemMessage, HumanMessage

from app.agents.state import AgentState

logger = logging.getLogger(__name__)

_VERIFY_PROMPT = """\
You are a SQL result verifier. Given a user's question, the SQL query that was executed,
and the results, determine if the SQL QUERY LOGIC is correct for the question asked.

Check for these issues in the QUERY LOGIC (not the data):
1. Wrong table or column used (e.g., querying "deals" when asked about "support tickets")
2. Wrong filter values (e.g., filtering status='active' when schema shows values are 'open'/'resolved')
3. Missing GROUP BY when user asked "by X" or "per X"
4. Wrong aggregation (e.g., COUNT when user asked for SUM)
5. Missing JOIN when data from multiple tables is needed

IMPORTANT:
- If the query logic is correct but returns NULL, zero, or empty results, that is NOT an error.
  The data simply doesn't exist for that filter. Respond "correct".
- If the query uses the right tables, right columns, right filters, and right aggregation,
  respond "correct" even if results are empty.
- Only respond "retry" if the SQL logic itself is wrong for the question.

Respond with EXACTLY one line in this format:
- "correct | high" — exact column matches, clear join path, unambiguous intent
- "correct | medium" — correct but used inferred joins or column aliases (≈ hints)
- "correct | low" — plausible but ambiguous terms (e.g., "revenue" could mean multiple columns)
- "retry: <explanation>" — if the query logic is wrong
"""


async def verify_results(state: AgentState, llm: BaseChatModel) -> AgentState:
    """Check if SQL results actually answer the user's question."""
    table_data = state.get("table_data")
    sql_query = state.get("sql_query", "")
    query = state.get("query", "")

    if not table_data or state.get("retry_count", 0) >= 1:
        return state

    rows = table_data.get("rows", [])
    columns = table_data.get("columns", [])
    total = table_data.get("total_rows", 0)

    preview = json.dumps(rows[:10], default=str) if rows else "No rows returned"

    messages = [
        SystemMessage(content=_VERIFY_PROMPT),
        HumanMessage(content=(
            f"User question: {query}\n\n"
            f"SQL query executed:\n{sql_query}\n\n"
            f"Result columns: {columns}\n"
            f"Total rows: {total}\n"
            f"Result preview (first 10 rows):\n{preview}"
        )),
    ]

    response = await llm.ainvoke(messages)
    verdict: str = response.content.strip().lower()  # type: ignore[union-attr]

    if verdict.startswith("correct"):
        # Parse confidence level: "correct | high" → "high"
        confidence = "high"  # default
        if "|" in verdict:
            conf_part = verdict.split("|", 1)[1].strip()
            if conf_part in ("high", "medium", "low"):
                confidence = conf_part

        logger.info("Verification passed (confidence: %s).", confidence)
        return {**state, "confidence": confidence}

    if verdict.startswith("retry"):
        explanation = verdict.split(":", 1)[1].strip() if ":" in verdict else verdict
        logger.warning("Verification failed: %s", explanation)
        return {
            **state,
            "error": f"Result verification failed: {explanation}",
            "retry_count": state.get("retry_count", 0) + 1,
            "confidence": "low",
        }

    logger.info("Verification ambiguous ('%s'), trusting results.", verdict[:50])
    return {**state, "confidence": "medium"}
