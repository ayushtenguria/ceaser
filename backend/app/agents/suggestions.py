"""Suggestion agent — generates smart query suggestions based on the connected schema."""

from __future__ import annotations

import json
import logging

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

logger = logging.getLogger(__name__)

_SUGGEST_PROMPT = """\
You are a data analyst assistant. Based on the database schema below, generate 6 smart
questions that a business user would likely want to ask about this data.

Rules:
1. Make questions specific to the actual tables and columns — reference real column names
2. Mix different types: aggregations, trends, comparisons, top-N, anomalies
3. Include 2 questions that would produce charts (mention "plot", "chart", or "trend")
4. Make questions business-relevant, not technical
5. Keep each question under 60 characters
6. Return ONLY a JSON array of strings, no markdown

Database schema:
{schema_context}

Example output format:
["What is our total revenue this quarter?", "Plot monthly customer growth trend", ...]
"""


async def generate_suggestions(
    schema_context: str,
    llm: BaseChatModel,
) -> list[str]:
    """Generate smart query suggestions based on the database schema."""
    if not schema_context:
        return _DEFAULT_SUGGESTIONS

    messages = [
        SystemMessage(content=_SUGGEST_PROMPT.format(schema_context=schema_context)),
        HumanMessage(content="Generate 6 relevant questions for this database."),
    ]

    try:
        response = await llm.ainvoke(messages)
        raw: str = response.content.strip()  # type: ignore[union-attr]

        if raw.startswith("```"):
            lines = raw.split("\n")
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            raw = "\n".join(lines).strip()

        suggestions = json.loads(raw)
        if isinstance(suggestions, list) and len(suggestions) >= 3:
            return [str(s).strip() for s in suggestions[:6]]
    except Exception as exc:
        logger.warning("Suggestion generation failed: %s", exc)

    return _DEFAULT_SUGGESTIONS


_DEFAULT_SUGGESTIONS = [
    "Show me the top 10 customers by revenue",
    "What are the trends in monthly revenue?",
    "Which department has the highest headcount?",
    "Plot a chart of deals by stage",
    "How many open support tickets by priority?",
    "What is our current MRR breakdown?",
]
