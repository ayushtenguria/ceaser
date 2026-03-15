"""SQL agent node — generates a SELECT query from natural language."""

from __future__ import annotations

import logging

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import SystemMessage

from app.agents.state import AgentState

logger = logging.getLogger(__name__)

_SQL_SYSTEM_PROMPT = """\
You are an expert SQL analyst.  Generate a SINGLE, read-only SQL query that answers the user's question.

Rules:
1. Only produce SELECT statements.  Never generate INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, or any DDL/DML.
2. Always qualify column names with table aliases when joining.
3. Use standard SQL syntax compatible with the database dialect described below.
4. Return ONLY the SQL query — no explanations, no markdown fences, no surrounding text.
5. Limit results to 1000 rows unless the user explicitly asks for more.
6. Use meaningful column aliases so the result set is human-readable.

Database schema:
{schema_context}
"""


async def generate_sql(state: AgentState, llm: BaseChatModel) -> AgentState:
    """Use the LLM to generate a SQL query for the user's question."""
    schema_context = state.get("schema_context", "")

    messages = [
        SystemMessage(content=_SQL_SYSTEM_PROMPT.format(schema_context=schema_context)),
        *state["messages"],
    ]

    response = await llm.ainvoke(messages)
    raw_sql: str = response.content.strip()  # type: ignore[union-attr]

    # Strip common markdown fencing the LLM might include despite instructions.
    if raw_sql.startswith("```"):
        lines = raw_sql.split("\n")
        # Remove first and last fence lines.
        lines = [ln for ln in lines if not ln.strip().startswith("```")]
        raw_sql = "\n".join(lines).strip()

    logger.info("Generated SQL:\n%s", raw_sql)
    return {**state, "sql_query": raw_sql}
