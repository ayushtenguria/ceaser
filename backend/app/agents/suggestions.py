"""Suggestion Agent — generates intelligent follow-up suggestions based on conversation context.

Uses the schema, conversation history, last response, and defined metrics to
suggest relevant follow-up questions after each assistant response.
"""

from __future__ import annotations

import json
import logging

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

logger = logging.getLogger(__name__)

_SUGGEST_PROMPT = """\
You are a data analysis assistant. Generate exactly 3 smart follow-up questions
the user might want to ask next.

Database schema:
{schema_context}

Conversation so far:
{conversation_history}

Last question: {last_question}
Last answer summary: {last_answer}

Rules:
1. Questions must be DIRECTLY related to what was just discussed
2. Progress the analysis deeper — drill down, compare, visualize, find causes
3. Reference actual tables/columns from the schema
4. Keep each question under 50 characters
5. Include at least 1 visualization suggestion (chart/plot/trend)
6. Don't repeat questions already asked in the conversation
7. Return ONLY a JSON array of 3 strings, no markdown

Examples of good follow-ups:
- After "top customers by revenue" → ["Plot revenue trend by month", "Which customers churned?", "Revenue by industry breakdown"]
- After "open tickets by priority" → ["Show ticket resolution times", "Plot ticket trend over time", "Which team handles most tickets?"]
"""

_INITIAL_PROMPT = """\
You are a data analysis assistant. Based on the database schema below, suggest 6
questions a business user would want to ask first.

Database schema:
{schema_context}

Rules:
1. Make questions specific to the actual tables and columns
2. Mix types: aggregations, trends, comparisons, top-N
3. Include 2 that produce charts
4. Keep each under 50 characters
5. Return ONLY a JSON array of strings

{metrics_context}
"""


async def generate_follow_up_suggestions(
    schema_context: str,
    conversation_history: list[dict[str, str]],
    last_question: str,
    last_answer: str,
    llm: BaseChatModel,
) -> list[str]:
    """Generate 3 contextual follow-up suggestions based on the conversation."""
    conv_summary = ""
    for msg in conversation_history[-6:]:
        role = "User" if msg["role"] == "user" else "Assistant"
        content = msg["content"][:100]
        conv_summary += f"{role}: {content}\n"

    messages = [
        SystemMessage(content=_SUGGEST_PROMPT.format(
            schema_context=schema_context[:2000],
            conversation_history=conv_summary or "No previous messages",
            last_question=last_question,
            last_answer=last_answer[:300],
        )),
        HumanMessage(content="Generate 3 follow-up questions."),
    ]

    try:
        response = await llm.ainvoke(messages)
        raw: str = response.content.strip()  # type: ignore[union-attr]

        if raw.startswith("```"):
            lines = raw.split("\n")
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            raw = "\n".join(lines).strip()

        suggestions = json.loads(raw)
        if isinstance(suggestions, list) and len(suggestions) >= 2:
            return [str(s).strip() for s in suggestions[:3]]
    except Exception as exc:
        logger.warning("Follow-up suggestion generation failed: %s", exc)

    return []


async def generate_initial_suggestions(
    schema_context: str,
    metrics_context: str,
    llm: BaseChatModel,
) -> list[str]:
    """Generate initial suggestions for a new conversation (empty state)."""
    if not schema_context:
        return _DEFAULT_SUGGESTIONS

    messages = [
        SystemMessage(content=_INITIAL_PROMPT.format(
            schema_context=schema_context[:3000],
            metrics_context=f"Defined business metrics:\n{metrics_context}" if metrics_context else "",
        )),
        HumanMessage(content="Generate 6 relevant starter questions."),
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
        logger.warning("Initial suggestion generation failed: %s", exc)

    return _DEFAULT_SUGGESTIONS


async def generate_suggestions(
    schema_context: str,
    llm: BaseChatModel,
) -> list[str]:
    """Generate smart query suggestions based on the database schema (backward compat)."""
    return await generate_initial_suggestions(schema_context, "", llm)


_DEFAULT_SUGGESTIONS = [
    "Show top 10 customers by revenue",
    "What is our monthly revenue trend?",
    "Which department has most employees?",
    "Plot deals by stage as a chart",
    "How many open support tickets?",
    "What is our current MRR?",
]
