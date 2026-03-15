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

from app.agents.executor import execute_code, execute_sql
from app.agents.python_agent import generate_python
from app.agents.router import route_query
from app.agents.sql_agent import generate_sql
from app.agents.state import AgentState

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3

# ---------------------------------------------------------------------------
# Response generator
# ---------------------------------------------------------------------------

_RESPONSE_SYSTEM_PROMPT = """\
You are Ceaser, a friendly and expert AI data analyst.

Summarise the analysis result for the user in clear, concise language.
If there was an error, explain what went wrong and suggest a fix.
Reference specific numbers / columns when available.

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


def _after_sql_execute(state: AgentState) -> str:
    if state.get("error") and state.get("retry_count", 0) < _MAX_RETRIES:
        return "sql_agent"
    return "respond"


def _after_sql_execute_or_viz(state: AgentState) -> str:
    """After SQL execution: retry on error, chain to Python for viz, or respond."""
    if state.get("error") and state.get("retry_count", 0) < _MAX_RETRIES:
        return "sql_agent"
    # If the original intent was sql_then_viz, chain to Python agent for visualization.
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

    async def sql_execute_node(state: AgentState) -> AgentState:
        return await execute_sql(state, db)

    async def code_execute_node(state: AgentState) -> AgentState:
        return await execute_code(state)

    async def respond_node(state: AgentState) -> AgentState:
        return await _respond(state, llm)

    graph = StateGraph(AgentState)

    # Nodes
    graph.add_node("router", router_node)
    graph.add_node("sql_agent", sql_agent_node)
    graph.add_node("python_agent", python_agent_node)
    graph.add_node("sql_execute", sql_execute_node)
    graph.add_node("code_execute", code_execute_node)
    graph.add_node("respond", respond_node)

    # Edges
    graph.set_entry_point("router")

    graph.add_conditional_edges(
        "router",
        _after_router,
        {
            "sql": "sql_agent",
            "python": "python_agent",
            # sql_then_viz: fetch data via SQL first, then visualise with Python
            "sql_then_viz": "sql_agent",
            "respond": "respond",
            "error": "respond",
        },
    )

    graph.add_edge("sql_agent", "sql_execute")
    graph.add_conditional_edges(
        "sql_execute",
        _after_sql_execute_or_viz,
        {
            "sql_agent": "sql_agent",
            "python_agent": "python_agent",
            "respond": "respond",
        },
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

async def run_agent(
    *,
    query: str,
    connection_id: str | None,
    file_id: str | None,
    schema_context: str,
    llm: BaseChatModel,
    db: AsyncSession,
) -> AsyncGenerator[dict[str, Any], None]:
    """Run the analysis agent and yield ``StreamChunk``-style dicts as it progresses.

    Each yielded dict has at minimum a ``type`` key:
      - ``"status"``  — progress update text
      - ``"sql"``     — generated SQL query
      - ``"code"``    — generated Python code
      - ``"text"``    — assistant's natural-language response
      - ``"table"``   — table_data dict
      - ``"plotly"``  — plotly figure JSON
      - ``"error"``   — error message
    """
    graph = build_graph(llm, db)
    compiled = graph.compile()

    initial_state: AgentState = {
        "messages": [HumanMessage(content=query)],
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

    yield {"type": "status", "content": "Analysing your question..."}

    final_state: AgentState | None = None

    async for event in compiled.astream(initial_state, stream_mode="updates"):
        for node_name, node_state in event.items():
            logger.debug("Node '%s' completed.", node_name)

            if node_name == "router":
                action = node_state.get("next_action", "")
                yield {"type": "status", "content": f"Decided to use: {action}"}

            elif node_name == "sql_agent":
                sql = node_state.get("sql_query")
                if sql:
                    yield {"type": "sql", "content": sql}

            elif node_name == "sql_execute":
                if node_state.get("error"):
                    yield {"type": "status", "content": f"SQL error, retrying... ({node_state.get('retry_count', 0)}/{_MAX_RETRIES})"}
                elif node_state.get("table_data"):
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
                # Extract the AI message from the messages list.
                msgs = node_state.get("messages", [])
                for msg in msgs:
                    if isinstance(msg, AIMessage):
                        yield {"type": "text", "content": msg.content}

            final_state = node_state

    # If we ended with an error and no text was produced, emit the error.
    if final_state and final_state.get("error"):
        yield {"type": "error", "content": final_state["error"]}
