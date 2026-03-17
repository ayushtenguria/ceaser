"""Router node — classifies the user query into an action type."""

from __future__ import annotations

import logging

from langchain_core.messages import SystemMessage
from langchain_core.language_models import BaseChatModel

from app.agents.state import AgentState

logger = logging.getLogger(__name__)

_ROUTER_SYSTEM_PROMPT = """\
You are a query router for a data analysis assistant. A database IS connected.

Given the user's query and the database schema below, decide which action to take:

1. "sql" — ANY question that can be answered by a SINGLE database query — lookups,
   aggregations, filters, joins, rankings, comparisons, counts, sums, averages.
   Choose sql when the answer is a fact that exists in the data.

2. "sql_then_viz" — same as sql, but when the user also wants a chart / plot / graph.
   Choose this when they say "plot", "chart", "graph", "visualise", "trend".

3. "analyze" — for STRATEGIC or ADVISORY questions that need MULTIPLE analyses to answer.
   Choose this when the user asks "what should we do", "how can we improve",
   "why is X happening", "what are our biggest risks", "give me insights about",
   "analyze our performance", "recommend strategies", "what's going wrong".
   The analyst agent will automatically run multiple queries and synthesize insights.
   WHEN IN DOUBT between sql and analyze, choose analyze for broad/strategic questions.

4. "python" — only for computation on an uploaded file, or pure math.

5. "respond" — ONLY for greetings ("hi"), thank-yous, or questions completely
   unrelated to the data.

Database schema:
{schema_context}

Respond with EXACTLY ONE word: sql, sql_then_viz, analyze, python, or respond.
"""


async def route_query(state: AgentState, llm: BaseChatModel) -> AgentState:
    """Classify the user's intent and set ``next_action`` accordingly."""
    schema_context = state.get("schema_context", "")
    query = state["query"]

    # If there is no data source at all, default to a direct response.
    has_connection = bool(state.get("connection_id"))
    has_file = bool(state.get("file_id"))

    if not has_connection and not has_file:
        # Check if this looks like a data question — if so, tell user to connect
        data_keywords = (
            "revenue", "sales", "customer", "employee", "ticket", "order",
            "report", "top", "total", "average", "count", "how many",
            "show me", "list", "find", "query", "data", "table", "department",
            "product", "pipeline", "churn", "mrr", "arr", "deal",
            "plot", "chart", "graph", "visuali", "trend",
        )
        if any(kw in query.lower() for kw in data_keywords):
            from langchain_core.messages import AIMessage
            return {
                **state,
                "next_action": "respond",
                "error": "NO_DATA_SOURCE",
            }
        return {**state, "next_action": "respond"}

    messages = [
        SystemMessage(content=_ROUTER_SYSTEM_PROMPT.format(schema_context=schema_context)),
        *state["messages"],
    ]

    response = await llm.ainvoke(messages)
    action = response.content.strip().lower()  # type: ignore[union-attr]

    if action not in ("sql", "sql_then_viz", "analyze", "python", "respond"):
        logger.warning("Router returned unexpected action '%s', defaulting to 'sql'.", action)
        action = "sql"

    # If the action requires SQL but only a file is attached (no DB), switch to python.
    if action in ("sql", "sql_then_viz", "analyze") and not has_connection and has_file:
        action = "python"

    logger.info("Router decision: %s", action)
    return {**state, "next_action": action}
