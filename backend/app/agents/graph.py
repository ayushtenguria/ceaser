"""LangGraph state-graph definition and entry-point for the analysis agent.

Graph topology:

    Entry -> Router -> (SQL Agent -> Execute SQL)   \
                    -> (Python Agent -> Execute Py)   > -> Respond
                    -> Respond directly               /
                         ^           ^
                         |           |
                         +-- retry --+
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncGenerator
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph
from sqlalchemy.ext.asyncio import AsyncSession

from app.agents.analyst import run_analyst
from app.agents.decomposer import decompose_query
from app.agents.executor import execute_code, execute_sql
from app.agents.python_agent import generate_python
from app.agents.router import route_query
from app.agents.sql_agent import generate_sql
from app.agents.state import AgentState
from app.agents.validator import validate_sql
from app.agents.verifier import verify_results

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3

# ---------------------------------------------------------------------------
# Response generator
# ---------------------------------------------------------------------------

_RESPONSE_SYSTEM_PROMPT = """\
You are Ceaser, a friendly and expert AI data analyst.

Summarise the analysis result for the user in clear, concise language.
Reference specific numbers / columns when available.

IMPORTANT rules for your response:
- If the query returned NULL values, empty results, or zero rows, tell the user clearly:
  "No data found for this query." Then explain WHY — e.g., "The database has no revenue
  records for 2026. The latest data is from March 2025. Try asking for 2024 or 2025 instead."
- If there was an error, explain what went wrong in plain language and suggest a fix.
- Never say "null" or "None" without explanation — always translate technical results
  into a clear business message.
- If results look correct, present them with key insights and highlight notable patterns.
- NEVER generate SQL code, Python code, or code blocks in your response. You are summarizing
  results, not writing code. If no data source is connected, tell the user:
  "Please select a database connection from the top bar to query your data."
- If a chart/visualisation was generated (plotly figure exists in context), acknowledge it:
  "Here's the chart" or "I've created a visualisation showing..." — do NOT say you cannot
  create charts, because you already did.
- For advice/strategy questions ("what should we do", "how to improve"), provide data-driven
  suggestions based on the database schema. Suggest specific queries the user can ask to
  find insights. Example: "To understand revenue growth opportunities, try asking:
  1) Which customers have the lowest health scores? 2) What's our revenue trend by month?
  3) Which industries have the highest deal values?"
- Keep responses concise — 2-4 sentences max for simple queries.

{context}
"""


async def _respond(state: AgentState, llm: BaseChatModel) -> AgentState:
    """Generate a natural-language response summarising the analysis."""
    context_parts: list[str] = []
    if state.get("sql_query"):
        context_parts.append(f"SQL executed:\n{state['sql_query']}")
    if state.get("execution_result"):
        context_parts.append(f"Execution output:\n{state['execution_result']}")
    if state.get("table_data"):
        preview = json.dumps(state["table_data"], default=str)[:2000]
        context_parts.append(f"Table data (preview):\n{preview}")
    if state.get("plotly_figure"):
        context_parts.append("A chart/visualisation HAS BEEN successfully generated and will be displayed to the user.")
    if state.get("error"):
        context_parts.append(f"Error:\n{state['error']}")

    context = "\n\n".join(context_parts) if context_parts else ""

    messages = [
        SystemMessage(content=_RESPONSE_SYSTEM_PROMPT.format(context=context)),
        *state["messages"],
    ]

    response = await llm.ainvoke(messages)
    content: str = response.content  # type: ignore[assignment]

    return {
        **state,
        "messages": [AIMessage(content=content)],
    }


# ---------------------------------------------------------------------------
# Conditional edges
# ---------------------------------------------------------------------------

def _after_router(state: AgentState) -> str:
    return state["next_action"]


def _after_validate(state: AgentState) -> str:
    """After SQL validation: retry on error, otherwise proceed to execute."""
    if state.get("error") and state.get("retry_count", 0) < _MAX_RETRIES:
        return "sql_agent"
    return "sql_execute"


def _after_sql_execute(state: AgentState) -> str:
    """After SQL execution: retry on error, otherwise proceed to verification."""
    if state.get("error") and state.get("retry_count", 0) < _MAX_RETRIES:
        return "sql_agent"
    return "verify_results"


def _after_verify(state: AgentState) -> str:
    """After result verification: retry on error, chain to Python for viz, or respond."""
    if state.get("error") and state.get("retry_count", 0) < _MAX_RETRIES:
        return "sql_agent"
    if state.get("next_action") == "sql_then_viz" and not state.get("error"):
        return "python_agent"
    return "respond"


def _after_code_execute(state: AgentState) -> str:
    if state.get("error") and state.get("retry_count", 0) < _MAX_RETRIES:
        return "python_agent"
    return "respond"


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------

def build_graph(llm: BaseChatModel, db: AsyncSession) -> StateGraph:
    """Construct and return the compiled LangGraph agent."""

    async def router_node(state: AgentState) -> AgentState:
        return await route_query(state, llm)

    async def sql_agent_node(state: AgentState) -> AgentState:
        return await generate_sql(state, llm)

    async def python_agent_node(state: AgentState) -> AgentState:
        return await generate_python(state, llm)

    async def validate_node(state: AgentState) -> AgentState:
        return validate_sql(state)

    async def sql_execute_node(state: AgentState) -> AgentState:
        return await execute_sql(state, db)

    async def verify_node(state: AgentState) -> AgentState:
        return await verify_results(state, llm)

    async def code_execute_node(state: AgentState) -> AgentState:
        return await execute_code(state)

    async def respond_node(state: AgentState) -> AgentState:
        return await _respond(state, llm)

    async def analyst_node(state: AgentState) -> AgentState:
        return await run_analyst(state, llm, db)

    graph = StateGraph(AgentState)

    # Nodes
    graph.add_node("router", router_node)
    graph.add_node("sql_agent", sql_agent_node)
    graph.add_node("validate_sql", validate_node)
    graph.add_node("python_agent", python_agent_node)
    graph.add_node("sql_execute", sql_execute_node)
    graph.add_node("verify_results", verify_node)
    graph.add_node("code_execute", code_execute_node)
    graph.add_node("respond", respond_node)
    graph.add_node("analyst", analyst_node)

    # Edges
    graph.set_entry_point("router")

    graph.add_conditional_edges(
        "router",
        _after_router,
        {
            "sql": "sql_agent",
            "python": "python_agent",
            "sql_then_viz": "sql_agent",
            "analyze": "analyst",
            "respond": "respond",
            "error": "respond",
        },
    )

    # Analyst goes straight to respond (it does its own execution internally)
    graph.add_edge("analyst", "respond")

    graph.add_edge("sql_agent", "validate_sql")
    graph.add_conditional_edges(
        "validate_sql",
        _after_validate,
        {"sql_agent": "sql_agent", "sql_execute": "sql_execute"},
    )
    graph.add_conditional_edges(
        "sql_execute",
        _after_sql_execute,
        {"sql_agent": "sql_agent", "verify_results": "verify_results"},
    )
    graph.add_conditional_edges(
        "verify_results",
        _after_verify,
        {"sql_agent": "sql_agent", "python_agent": "python_agent", "respond": "respond"},
    )

    graph.add_edge("python_agent", "code_execute")
    graph.add_conditional_edges(
        "code_execute",
        _after_code_execute,
        {
            "python_agent": "python_agent",
            "respond": "respond",
        },
    )

    graph.add_edge("respond", END)

    return graph


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------

async def _run_single_query(
    compiled: Any,
    query: str,
    connection_id: str | None,
    file_id: str | None,
    schema_context: str,
    history_messages: list,
    timeout_seconds: int = 60,
) -> AsyncGenerator[dict[str, Any], None]:
    """Run a single query through the compiled graph and yield stream chunks."""
    messages = list(history_messages) + [HumanMessage(content=query)]

    initial_state: AgentState = {
        "messages": messages,
        "query": query,
        "connection_id": connection_id,
        "file_id": file_id,
        "schema_context": schema_context,
        "sql_query": None,
        "code_block": None,
        "execution_result": None,
        "table_data": None,
        "plotly_figure": None,
        "error": None,
        "retry_count": 0,
        "next_action": "",
    }

    import asyncio as _asyncio
    import time as _time

    final_state: AgentState | None = None
    start_time = _time.monotonic()

    async for event in compiled.astream(initial_state, stream_mode="updates"):
        # Hard timeout guard — prevent infinite loops from killing the stream
        if _time.monotonic() - start_time > timeout_seconds:
            yield {"type": "error", "content": "Analysis timed out. Try a simpler query."}
            return
        for node_name, node_state in event.items():
            logger.debug("Node '%s' completed.", node_name)

            if node_name == "router":
                action = node_state.get("next_action", "")
                yield {"type": "status", "content": f"Decided to use: {action}"}

            elif node_name == "sql_agent":
                sql = node_state.get("sql_query")
                if sql:
                    yield {"type": "sql", "content": sql}

            elif node_name == "validate_sql":
                if node_state.get("error"):
                    yield {"type": "status", "content": f"SQL validation issue: {node_state['error'][:100]}"}

            elif node_name == "sql_execute":
                if node_state.get("error"):
                    yield {"type": "status", "content": f"SQL error, retrying... ({node_state.get('retry_count', 0)}/{_MAX_RETRIES})"}
                elif node_state.get("table_data"):
                    yield {"type": "table", "content": node_state["table_data"]}

            elif node_name == "verify_results":
                if node_state.get("error"):
                    yield {"type": "status", "content": "Verifying results... re-trying for better accuracy"}

            elif node_name == "analyst":
                yield {"type": "status", "content": "Running deep analysis..."}
                if node_state.get("table_data"):
                    yield {"type": "table", "content": node_state["table_data"]}

            elif node_name == "python_agent":
                code = node_state.get("code_block")
                if code:
                    yield {"type": "code", "content": code}

            elif node_name == "code_execute":
                if node_state.get("error"):
                    yield {"type": "status", "content": f"Code error, retrying... ({node_state.get('retry_count', 0)}/{_MAX_RETRIES})"}
                else:
                    if node_state.get("plotly_figure"):
                        yield {"type": "plotly", "content": node_state["plotly_figure"]}
                    if node_state.get("table_data") and node_state["table_data"] != initial_state.get("table_data"):
                        yield {"type": "table", "content": node_state["table_data"]}

            elif node_name == "respond":
                msgs = node_state.get("messages", [])
                for msg in msgs:
                    if isinstance(msg, AIMessage):
                        yield {"type": "text", "content": msg.content}

            final_state = node_state

    if final_state and final_state.get("error"):
        yield {"type": "error", "content": final_state["error"]}


async def run_agent(
    *,
    query: str,
    connection_id: str | None,
    file_id: str | None,
    schema_context: str,
    llm: BaseChatModel,
    db: AsyncSession,
    history: list[dict[str, str]] | None = None,
) -> AsyncGenerator[dict[str, Any], None]:
    """Run the analysis agent, handling compound queries via decomposition.

    If the user asks multiple independent questions in one message, the
    decomposer splits them and each sub-query is executed separately.
    """
    graph = build_graph(llm, db)
    compiled = graph.compile()

    # Build message history for conversation context
    history_messages: list = []
    for msg in (history or []):
        if msg["role"] == "user":
            history_messages.append(HumanMessage(content=msg["content"]))
        else:
            history_messages.append(AIMessage(content=msg["content"]))

    yield {"type": "status", "content": "Analysing your question..."}

    # Step 1: Decompose the query
    sub_queries = await decompose_query(query, llm)

    if len(sub_queries) == 1:
        # Single query — run directly
        async for chunk in _run_single_query(
            compiled, query, connection_id, file_id, schema_context, history_messages,
        ):
            yield chunk
    else:
        # Multiple sub-queries — run each sequentially
        yield {"type": "status", "content": f"Breaking into {len(sub_queries)} parts..."}

        all_texts: list[str] = []

        for i, sub_q in enumerate(sub_queries, 1):
            yield {"type": "status", "content": f"Part {i}/{len(sub_queries)}: {sub_q}"}

            sub_text = ""
            async for chunk in _run_single_query(
                compiled, sub_q, connection_id, file_id, schema_context, history_messages,
            ):
                # Yield all artifacts (tables, charts, sql, code)
                if chunk["type"] in ("table", "plotly", "sql", "code", "chart"):
                    yield chunk
                elif chunk["type"] == "text":
                    sub_text = chunk["content"]
                elif chunk["type"] == "error":
                    sub_text = f"Error: {chunk['content']}"
                # Skip status/done for sub-queries to avoid noise

            if sub_text:
                all_texts.append(f"**{sub_q}**\n{sub_text}")

        # Combine all text responses
        if all_texts:
            yield {"type": "text", "content": "\n\n---\n\n".join(all_texts)}
