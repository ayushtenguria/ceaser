"""Data Analyst agent — runs multiple queries autonomously to answer strategic questions.

When a user asks "what should we do to increase revenue", this agent:
1. Plans which analyses are needed
2. Generates and executes SQL queries for each
3. Synthesizes all results into data-backed recommendations
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agents.state import AgentState
from app.connectors.factory import get_connector
from app.db.models import DatabaseConnection

logger = logging.getLogger(__name__)

_PLAN_PROMPT = """\
You are a senior data analyst. The user asked a strategic question that requires
analysing data from multiple angles to provide a data-backed answer.

User question: {question}

Database schema:
{schema_context}

Plan 3-5 specific SQL queries that will gather the data needed to answer this question.
Each query should answer a different analytical angle.

Return a JSON array of objects, each with:
- "label": short description of what this query answers (e.g., "Revenue by month trend")
- "sql": the complete SELECT query (PostgreSQL syntax, read-only, LIMIT 100)

RULES:
- Only SELECT statements
- Use the exact table/column names from the schema
- Each query should provide a different insight relevant to the question
- Keep queries focused — one aggregation per query
- Use meaningful column aliases

Return ONLY the JSON array, no markdown fences, no explanation.
"""

_SYNTHESIZE_PROMPT = """\
You are a senior data analyst presenting findings to a business leader.

The user asked: "{question}"

You ran the following analyses and got these results:

{analysis_results}

Now provide a CLEAR, DATA-BACKED answer to the user's question.

RULES:
- Lead with the key insight/recommendation
- Reference SPECIFIC numbers from the data (e.g., "$48,000 MRR", "5 at-risk customers")
- Provide 3-5 actionable recommendations, each backed by data from the results above
- Use bullet points for clarity
- If some queries returned no data or errors, acknowledge it briefly and move on
- NEVER suggest the user run more queries — YOU are the analyst, give them the answer
- Be concise but thorough — this should read like an executive briefing
"""


async def run_analyst(
    state: AgentState,
    llm: BaseChatModel,
    db: AsyncSession,
) -> AgentState:
    """Run the data analyst agent — plans queries, executes them, synthesizes results."""
    query = state["query"]
    schema_context = state.get("schema_context", "")
    connection_id = state.get("connection_id")

    if not connection_id:
        from langchain_core.messages import AIMessage
        return {
            **state,
            "execution_result": "No database connected.",
            "error": "NO_DATA_SOURCE",
        }

    # Step 1: Plan the analyses
    logger.info("Analyst: Planning analyses for '%s'", query[:80])
    plan_messages = [
        SystemMessage(content=_PLAN_PROMPT.format(
            question=query,
            schema_context=schema_context,
        )),
        HumanMessage(content=query),
    ]
    plan_response = await llm.ainvoke(plan_messages)
    raw_plan: str = plan_response.content.strip()  # type: ignore[union-attr]

    # Strip markdown fences
    if raw_plan.startswith("```"):
        lines = raw_plan.split("\n")
        lines = [ln for ln in lines if not ln.strip().startswith("```")]
        raw_plan = "\n".join(lines).strip()

    try:
        analyses = json.loads(raw_plan)
        if not isinstance(analyses, list):
            analyses = [analyses]
        analyses = analyses[:5]  # Cap at 5
    except (json.JSONDecodeError, ValueError):
        logger.warning("Analyst: Failed to parse plan: %s", raw_plan[:200])
        analyses = []

    if not analyses:
        return {
            **state,
            "execution_result": "Could not plan analyses for this question.",
        }

    # Step 2: Execute each query
    logger.info("Analyst: Executing %d analyses", len(analyses))

    # Load the connection
    stmt = select(DatabaseConnection).where(
        DatabaseConnection.id == uuid.UUID(connection_id)
    )
    result = await db.execute(stmt)
    connection = result.scalar_one_or_none()
    if connection is None:
        return {**state, "error": "Database connection not found."}

    analysis_results: list[dict[str, Any]] = []

    for i, analysis in enumerate(analyses):
        label = analysis.get("label", f"Analysis {i + 1}")
        sql = analysis.get("sql", "")

        if not sql or not sql.strip().upper().startswith(("SELECT", "WITH")):
            analysis_results.append({
                "label": label,
                "sql": sql,
                "error": "Invalid or non-SELECT SQL blocked",
                "rows": [],
            })
            continue

        connector = get_connector(connection)
        try:
            await connector.connect()
            columns, rows = await connector.execute_query(sql)
            await connector.disconnect()

            analysis_results.append({
                "label": label,
                "sql": sql,
                "columns": columns,
                "rows": rows[:20],  # Cap preview
                "total_rows": len(rows),
            })
            logger.info("Analyst: '%s' returned %d rows", label, len(rows))

        except Exception as exc:
            await connector.disconnect()
            logger.warning("Analyst: '%s' failed: %s", label, exc)
            analysis_results.append({
                "label": label,
                "sql": sql,
                "error": str(exc),
                "rows": [],
            })

    # Step 3: Synthesize results into a data-backed answer
    logger.info("Analyst: Synthesizing %d analysis results", len(analysis_results))

    results_text = ""
    for r in analysis_results:
        results_text += f"\n### {r['label']}\n"
        if r.get("error"):
            results_text += f"Error: {r['error']}\n"
        else:
            results_text += f"SQL: {r['sql']}\n"
            results_text += f"Returned {r.get('total_rows', 0)} rows.\n"
            preview = json.dumps(r.get("rows", [])[:10], default=str)
            results_text += f"Data: {preview}\n"

    synth_messages = [
        SystemMessage(content=_SYNTHESIZE_PROMPT.format(
            question=query,
            analysis_results=results_text,
        )),
        HumanMessage(content=query),
    ]

    synth_response = await llm.ainvoke(synth_messages)
    answer: str = synth_response.content.strip()  # type: ignore[union-attr]

    # Build combined table data from the most relevant analysis
    best_table = None
    for r in analysis_results:
        if not r.get("error") and r.get("rows"):
            best_table = {
                "columns": r.get("columns", []),
                "rows": r["rows"],
                "total_rows": r.get("total_rows", len(r["rows"])),
            }
            break

    return {
        **state,
        "execution_result": answer,
        "table_data": best_table,
        "error": None,
    }
