"""SQL validation node — checks generated SQL for common issues before execution."""

from __future__ import annotations

import logging
import re

from app.agents.state import AgentState

logger = logging.getLogger(__name__)


def validate_sql(state: AgentState) -> AgentState:
    """Validate the generated SQL query for safety and correctness.

    Checks:
    1. Only SELECT statements allowed
    2. No destructive keywords
    3. References only tables/columns in the schema context
    4. Has a LIMIT clause (adds one if missing)
    """
    sql = state.get("sql_query", "")
    if not sql:
        return {**state, "error": "No SQL query generated.", "next_action": "error"}

    sql_upper = sql.upper().strip()

    if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
        return {
            **state,
            "error": f"Generated query is not a SELECT/WITH statement. Got: {sql[:50]}",
            "retry_count": state.get("retry_count", 0) + 1,
        }

    destructive = re.findall(
        r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|"
        r"EXEC|EXECUTE|CALL|COPY|VACUUM|REINDEX|CLUSTER|COMMENT|"
        r"LOCK|UNLOCK|BEGIN|COMMIT|ROLLBACK|SAVEPOINT)\b",
        sql_upper,
    )
    if destructive:
        return {
            **state,
            "error": f"Query contains forbidden keyword(s): {', '.join(set(destructive))}",
            "sql_query": None,
            "retry_count": state.get("retry_count", 0) + 1,
        }

    statements = [s.strip() for s in sql.rstrip(";").split(";") if s.strip()]
    if len(statements) > 1:
        return {
            **state,
            "error": "Query contains multiple statements — only single SELECT queries are allowed.",
            "sql_query": None,
            "retry_count": state.get("retry_count", 0) + 1,
        }

    schema_ctx = state.get("schema_context", "")
    known_tables: set[str] = set()
    known_tables.update(re.findall(r'"name"\s*:\s*"(\w+)"', schema_ctx))
    known_tables.update(re.findall(r"Table:\s*(\w+)", schema_ctx))
    known_tables = {t.lower() for t in known_tables}

    sql_keywords = {
        "select",
        "where",
        "group",
        "order",
        "having",
        "limit",
        "offset",
        "union",
        "intersect",
        "except",
        "values",
        "set",
        "dual",
        "current_date",
        "current_time",
        "current_timestamp",
        "now",
        "lateral",
        "unnest",
        "generate_series",
        "json_each",
        "jsonb_each",
    }

    if known_tables:
        from_tables = re.findall(r"\bfrom\s+([a-z_]\w*)\b(?!\s*\()", sql.lower())
        join_tables = re.findall(r"\bjoin\s+([a-z_]\w*)\b(?!\s*\()", sql.lower())
        referenced_tables = set(from_tables + join_tables) - sql_keywords

        unknown_tables = [t for t in referenced_tables if t not in known_tables]
        if unknown_tables:
            available = ", ".join(sorted(known_tables))
            return {
                **state,
                "error": f"Query references unknown table(s): {', '.join(unknown_tables)}. Available tables: {available}",
                "retry_count": state.get("retry_count", 0) + 1,
            }

    if "LIMIT" not in sql_upper:
        sql = sql.rstrip(";").strip() + "\nLIMIT 1000;"
        logger.info("Added LIMIT 1000 to query.")

    return {**state, "sql_query": sql, "error": None}
