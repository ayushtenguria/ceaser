"""SQL Repair Agent — fixes broken SQL queries using the exact database error message.

Instead of regenerating SQL from scratch (which often produces the same bug),
this agent takes the failed SQL + the exact error and surgically fixes it.
"""

from __future__ import annotations

import logging

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import SystemMessage, HumanMessage

from app.agents.state import AgentState

logger = logging.getLogger(__name__)

_REPAIR_PROMPT = """\
You are a SQL repair specialist. A SQL query failed with a database error.
Your job is to FIX the query — not rewrite it from scratch.

Failed SQL:
{sql}

Database error:
{error}

Database dialect: {dialect}

Schema context:
{schema}

Common fixes:
- UNION type mismatch → CAST columns to same type, or use separate queries
- Column not found → check schema for correct column name
- Syntax error → fix the specific syntax issue
- Function not found → use dialect-appropriate function
- Ambiguous column → add table alias
- Division by zero → add NULLIF or CASE WHEN

Rules:
1. Return ONLY the fixed SQL query — no explanations, no markdown fences
2. Keep the query as close to the original as possible
3. Only fix the specific error — don't restructure the whole query
4. Must be a SELECT/WITH statement
"""


async def repair_sql(state: AgentState, llm: BaseChatModel) -> AgentState:
    """Attempt to repair a failed SQL query using the error message."""
    sql = state.get("sql_query", "")
    error = state.get("error", "")
    schema = state.get("schema_context", "")

    if not sql or not error:
        return state

    dialect = "PostgreSQL"
    if "MYSQL" in schema.upper():
        dialect = "MySQL"
    elif "SQLITE" in schema.upper():
        dialect = "SQLite"

    logger.info("Repair agent: fixing SQL error: %s", error[:100])

    messages = [
        SystemMessage(content=_REPAIR_PROMPT.format(
            sql=sql, error=error, dialect=dialect, schema=schema[:3000],
        )),
        HumanMessage(content=f"Fix this SQL query. The error was: {error}"),
    ]

    response = await llm.ainvoke(messages)
    fixed_sql: str = response.content.strip()  # type: ignore[union-attr]

    if fixed_sql.startswith("```"):
        lines = fixed_sql.split("\n")
        lines = [ln for ln in lines if not ln.strip().startswith("```")]
        fixed_sql = "\n".join(lines).strip()

    if not fixed_sql.upper().startswith(("SELECT", "WITH")):
        logger.warning("Repair agent produced non-SELECT: %s", fixed_sql[:50])
        return state

    logger.info("Repair agent: fixed SQL (%d chars)", len(fixed_sql))
    return {
        **state,
        "sql_query": fixed_sql,
        "error": None,
    }
