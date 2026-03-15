"""Router node — classifies the user query into an action type."""

from __future__ import annotations

import logging

from langchain_core.messages import SystemMessage
from langchain_core.language_models import BaseChatModel

from app.agents.state import AgentState

logger = logging.getLogger(__name__)

_ROUTER_SYSTEM_PROMPT = """\
You are a query router for a data analysis assistant.

Given the user's query and the available context, decide which action to take:

1. "sql" — if the query requires fetching or analysing data from a connected database
   and the result should be a table.  Use this for aggregations, filters, joins, lookups.

2. "sql_then_viz" — if the query needs data from the database AND a chart / plot /
   visualisation.  This first fetches data via SQL then generates a Plotly chart.
   Choose this whenever the user asks to "plot", "chart", "graph", "visualise", or
   "show me a chart of" something from the database.

3. "python" — if the query requires computation, visualisation, or analysis on an
   uploaded file, or general computation that does not need a database.

4. "respond" — if the query is a greeting, general knowledge question, or can be
   answered directly without touching any data source.

Available context:
{schema_context}

Respond with EXACTLY ONE word: sql, sql_then_viz, python, or respond.
"""


async def route_query(state: AgentState, llm: BaseChatModel) -> AgentState:
    """Classify the user's intent and set ``next_action`` accordingly."""
    schema_context = state.get("schema_context", "")
    query = state["query"]

    # If there is no data source at all, default to a direct response.
    has_connection = bool(state.get("connection_id"))
    has_file = bool(state.get("file_id"))

    if not has_connection and not has_file:
        # Without any data source, only python (for general computation) or
        # respond make sense.
        if any(
            keyword in query.lower()
            for keyword in ("plot", "chart", "graph", "calculate", "compute", "visuali")
        ):
            return {**state, "next_action": "python"}
        return {**state, "next_action": "respond"}

    messages = [
        SystemMessage(content=_ROUTER_SYSTEM_PROMPT.format(schema_context=schema_context)),
        *state["messages"],
    ]

    response = await llm.ainvoke(messages)
    action = response.content.strip().lower()  # type: ignore[union-attr]

    if action not in ("sql", "sql_then_viz", "python", "respond"):
        logger.warning("Router returned unexpected action '%s', defaulting to 'respond'.", action)
        action = "respond"

    # If the action requires SQL but only a file is attached (no DB), switch to python.
    if action in ("sql", "sql_then_viz") and not has_connection and has_file:
        action = "python"

    logger.info("Router decision: %s", action)
    return {**state, "next_action": action}
