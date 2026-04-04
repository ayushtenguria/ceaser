"""LangGraph agent state definition."""

from __future__ import annotations

from typing import Annotated, Any, TypedDict

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages


class AgentState(TypedDict):
    """Shared state flowing through the LangGraph agent nodes.

    ``messages`` uses the LangGraph ``add_messages`` reducer so that each
    node can simply return new messages and they will be appended.
    """

    messages: Annotated[list[BaseMessage], add_messages]

    query: str
    connection_id: str | None
    file_id: str | None
    schema_context: str

    sql_query: str | None
    code_block: str | None
    execution_result: str | None
    table_data: dict[str, Any] | None
    plotly_figure: dict[str, Any] | None
    error: str | None

    retry_count: int
    next_action: str

    # Trust & transparency
    query_reasoning: str  # WHY specific tables/joins were chosen
    confidence: str  # "high", "medium", "low"

    # Disambiguation
    disambiguation: dict  # disambiguation question + options (if ambiguous)
    disambiguation_resolution: str  # user's chosen interpretation
