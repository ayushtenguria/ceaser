"""Execution node — runs SQL queries or Python code and captures results."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agents.state import AgentState
from app.connectors.factory import get_connector
from app.db.models import DatabaseConnection
from app.sandbox.executor import execute_python

logger = logging.getLogger(__name__)

_MAX_TABLE_ROWS = 500


async def execute_sql(state: AgentState, db: AsyncSession) -> AgentState:
    """Execute the SQL query in ``state['sql_query']`` against the connected DB."""
    sql = state.get("sql_query")
    connection_id = state.get("connection_id")

    if not sql or not connection_id:
        return {**state, "error": "Missing SQL query or connection_id.", "next_action": "error"}

    stmt = select(DatabaseConnection).where(
        DatabaseConnection.id == uuid.UUID(connection_id)
    )
    result = await db.execute(stmt)
    connection = result.scalar_one_or_none()
    if connection is None:
        return {**state, "error": "Database connection not found.", "next_action": "error"}

    connector = get_connector(connection)
    try:
        await connector.connect()
        columns, rows = await connector.execute_query(sql)

        truncated = rows[:_MAX_TABLE_ROWS]
        table_data: dict[str, Any] = {
            "columns": columns,
            "rows": truncated,
            "total_rows": len(rows),
            "truncated": len(rows) > _MAX_TABLE_ROWS,
        }

        execution_result = (
            f"Query returned {len(rows)} row(s) with columns: {', '.join(columns)}"
        )

        return {
            **state,
            "table_data": table_data,
            "execution_result": execution_result,
            "error": None,
        }
    except Exception as exc:
        retry = state.get("retry_count", 0)
        logger.warning("SQL execution failed (attempt %d): %s", retry + 1, exc)
        return {
            **state,
            "error": str(exc),
            "retry_count": retry + 1,
            "next_action": "sql" if retry < 2 else "error",
        }
    finally:
        await connector.disconnect()


async def execute_code(state: AgentState) -> AgentState:
    """Execute the Python code in ``state['code_block']`` inside the sandbox."""
    code = state.get("code_block")
    if not code:
        return {**state, "error": "No Python code to execute.", "next_action": "error"}

    result = await execute_python(code)

    if not result.success:
        retry = state.get("retry_count", 0)
        logger.warning("Python execution failed (attempt %d): %s", retry + 1, result.error)
        return {
            **state,
            "error": result.error,
            "execution_result": result.stderr,
            "retry_count": retry + 1,
            "next_action": "python" if retry < 2 else "error",
        }

    plotly_figure = result.plotly_figure
    execution_result = result.stdout

    table_data: dict[str, Any] | None = state.get("table_data")
    if execution_result:
        try:
            parsed = json.loads(execution_result)
            if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
                columns = list(parsed[0].keys())
                table_data = {
                    "columns": columns,
                    "rows": parsed[:_MAX_TABLE_ROWS],
                    "total_rows": len(parsed),
                    "truncated": len(parsed) > _MAX_TABLE_ROWS,
                }
        except (json.JSONDecodeError, IndexError, KeyError):
            pass

    return {
        **state,
        "execution_result": execution_result,
        "plotly_figure": plotly_figure or state.get("plotly_figure"),
        "table_data": table_data,
        "error": None,
    }
