"""Router node — classifies the user query into an action type."""

from __future__ import annotations

import logging

from langchain_core.messages import SystemMessage
from langchain_core.language_models import BaseChatModel

from app.agents.state import AgentState

logger = logging.getLogger(__name__)

_ROUTER_SYSTEM_PROMPT = """\
You are a query router for a data analysis assistant. Data IS available.

Given the user's query and the data context below, decide which action to take:

1. "sql" — question answerable by a SINGLE database query — lookups, aggregations,
   filters, joins, rankings, comparisons. Choose only when a DATABASE is connected.

2. "sql_then_viz" — same as sql + a chart. Choose when database connected AND user
   wants a visualization.

3. "analyze" — STRATEGIC or ADVISORY questions needing MULTIPLE analyses.
   "what should we do", "how can we improve", "give me insights", "recommend".

4. "python" — ANY question about uploaded files (Excel/CSV), data exploration,
   "what is this data", "describe the data", "show columns", "summarize",
   computation, visualization on file data, or when DATAFRAMES are available.
   WHEN IN DOUBT and the context mentions DataFrames or Excel, choose python.

5. "respond" — ONLY for greetings or questions completely unrelated to data.

Data context:
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
    has_file_context = any(
        marker in schema_context
        for marker in ("EXCEL DATA CONTEXT", "FILE DATA SUMMARY", "AVAILABLE DATAFRAMES", "CODE PREAMBLE")
    )

    # Effective data source check
    has_data = has_connection or has_file or has_file_context

    if not has_data:
        # No data at all — tell user to connect or upload
        data_keywords = (
            "revenue", "sales", "customer", "employee", "ticket", "order",
            "report", "top", "total", "average", "count", "how many",
            "show me", "list", "find", "query", "data", "table", "department",
            "product", "pipeline", "churn", "mrr", "arr", "deal",
            "plot", "chart", "graph", "visuali", "trend", "what", "which",
            "analyze", "about", "summary", "describe",
        )
        if any(kw in query.lower() for kw in data_keywords):
            return {
                **state,
                "next_action": "respond",
                "error": "NO_DATA_SOURCE",
            }
        return {**state, "next_action": "respond"}

    # If only file data (no DB), always route to python
    if not has_connection and (has_file or has_file_context):
        logger.info("Router: file-only mode → python")
        return {**state, "next_action": "python"}

    # DB connected — use LLM to decide
    messages = [
        SystemMessage(content=_ROUTER_SYSTEM_PROMPT.format(schema_context=schema_context)),
        *state["messages"],
    ]

    response = await llm.ainvoke(messages)
    action = response.content.strip().lower()  # type: ignore[union-attr]

    if action not in ("sql", "sql_then_viz", "analyze", "python", "respond"):
        logger.warning("Router returned unexpected action '%s', defaulting to 'sql'.", action)
        action = "sql"

    logger.info("Router decision: %s", action)
    return {**state, "next_action": action}
