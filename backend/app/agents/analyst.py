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
You are a senior data analyst at a top management consulting firm (like Bain, McKinsey, BCG).
The user asked a strategic question that requires multi-dimensional data analysis.

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
- ALWAYS JOIN to get human-readable names: join products for product names, join customers
  for customer names/cities, join categories for category names. Never return just IDs.
- Each query should provide a different insight relevant to the question
- Keep queries focused — one aggregation per query
- Use meaningful column aliases
- For consulting questions about buying potential, use: total_spend, order_count,
  avg_order_value, recency (days since last order), category breadth (distinct categories)
- For cross-sell/upsell: find categories each customer buys vs all available categories
- For lapsed accounts: customers whose last order was >180 days ago
- For wallet share: customer spend vs avg spend in their segment

Return ONLY the JSON array, no markdown fences, no explanation.
"""

_SYNTHESIZE_PROMPT = """\
You are a senior data analyst presenting findings to a business leader.

The user asked: "{question}"

You ran the following analyses and got these results:

{analysis_results}

Now provide a CLEAR, DATA-BACKED answer to the user's question.

RULES:
- Start with METHODOLOGY: briefly explain how you calculated any scores or derived metrics
- Lead with the key insight/recommendation
- EVERY claim MUST have a specific number. NEVER say "significant", "substantial", "notable",
  "considerable", or "a large portion". Instead say "$248,000 (32% of total revenue)" or
  "1,247 out of 5,000 customers (24.9%)". If you don't have the exact number, calculate it
  from the data provided. No vague language allowed.
- Provide 3-5 actionable recommendations, each backed by data with exact numbers
- Use bullet points for clarity
- If some queries failed or returned errors, IGNORE THEM COMPLETELY. Focus only on the
  successful results. Do NOT mention errors, failed queries, or technical issues.
- NEVER suggest the user run more queries — YOU are the analyst, give them the answer
- Be concise but thorough — this should read like an executive briefing from Bain/McKinsey
"""


async def run_analyst(
    state: AgentState,
    llm: BaseChatModel,
    db: AsyncSession,
) -> AgentState:
    """Run the data analyst agent — plans queries, executes them, synthesizes results.

    Supports two modes:
    - SQL mode: when connection_id is set (queries a database)
    - DataFrame mode: when schema_context contains CODE PREAMBLE (uploaded files)
    """
    query = state["query"]
    schema_context = state.get("schema_context", "")
    connection_id = state.get("connection_id")
    has_dataframes = "CODE PREAMBLE" in schema_context or "AVAILABLE DATAFRAMES" in schema_context

    if not connection_id and not has_dataframes:
        from langchain_core.messages import AIMessage
        return {
            **state,
            "execution_result": "No database connected.",
            "error": "NO_DATA_SOURCE",
        }

    # Route to DataFrame mode if no SQL connection but files are available
    if not connection_id and has_dataframes:
        return await _run_dataframe_analyst(state, llm, query, schema_context)

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

    # Step 2b: Retry failed queries with error context (like the repair agent)
    for i, r in enumerate(analysis_results):
        if not r.get("error"):
            continue
        # Try to fix the SQL using the error message
        original_sql = r.get("sql", "")
        error_msg = r["error"]
        if not original_sql:
            continue

        logger.info("Analyst: Retrying failed query '%s' with error: %s", r["label"], error_msg[:100])
        fix_messages = [
            SystemMessage(content=(
                f"Fix this SQL query that failed with an error.\n"
                f"Database schema:\n{schema_context[:3000]}\n\n"
                f"Original SQL:\n{original_sql}\n\n"
                f"Error: {error_msg}\n\n"
                f"Return ONLY the fixed SQL query. No explanations."
            )),
            HumanMessage(content=f"Fix this query: {original_sql}"),
        ]
        try:
            fix_response = await llm.ainvoke(fix_messages)
            fixed_sql: str = fix_response.content.strip()
            if fixed_sql.startswith("```"):
                lines = fixed_sql.split("\n")
                lines = [ln for ln in lines if not ln.strip().startswith("```")]
                fixed_sql = "\n".join(lines).strip()

            if fixed_sql and fixed_sql.upper().startswith(("SELECT", "WITH")):
                connector = get_connector(connection)
                await connector.connect()
                columns, rows = await connector.execute_query(fixed_sql)
                await connector.disconnect()

                analysis_results[i] = {
                    "label": r["label"],
                    "sql": fixed_sql,
                    "columns": columns,
                    "rows": rows[:20],
                    "total_rows": len(rows),
                }
                logger.info("Analyst: Retry succeeded for '%s' — %d rows", r["label"], len(rows))
        except Exception as exc:
            logger.warning("Analyst: Retry also failed for '%s': %s", r["label"], exc)
            # Keep original error — will be filtered out below

    # Step 3: Synthesize results into a data-backed answer
    # Only include SUCCESSFUL results — filter out failures completely
    successful_results = [r for r in analysis_results if not r.get("error")]
    logger.info("Analyst: Synthesizing %d/%d successful results", len(successful_results), len(analysis_results))

    if not successful_results:
        return {
            **state,
            "execution_result": "I couldn't complete this analysis. Please try a more specific question.",
            "error": "All analysis queries failed. Try a simpler question.",
        }

    results_text = ""
    for r in successful_results:
        results_text += f"\n### {r['label']}\n"
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


# ---------------------------------------------------------------------------
# DataFrame analyst — for Excel/file-based complex queries
# ---------------------------------------------------------------------------

_DF_PLAN_PROMPT = """\
You are a senior data analyst at a top consulting firm.
The user uploaded data files and asked a strategic question.

User question: {question}

Available DataFrames and their columns:
{schema_context}

Plan a comprehensive Python analysis to answer this question.
Generate a SINGLE Python script that:
1. Uses the pre-loaded DataFrames (they are already available as variables)
2. Performs 3-5 analytical steps (merge, aggregate, compute metrics)
3. Prints key findings with print() statements
4. Creates a Plotly visualization stored in variable `fig`

RULES:
- DataFrames are ALREADY loaded — do NOT read files or create sample data
- Use pd.merge() to join DataFrames on shared columns
- For buying potential: compute total_spend, order_count, avg_order_value, recency
- For lapsed accounts: customers with last order > 180 days ago
- For cross-sell: compare categories each customer buys vs all available categories
- Always convert numeric columns: pd.to_numeric(col, errors='coerce')
- NEVER use pd.to_numeric(errors='ignore') — it's removed in pandas 3.0
- Store the final chart in variable named exactly `fig`
- Print a clear summary of findings

Return ONLY the Python code — no markdown fences, no explanations.
"""


async def _run_dataframe_analyst(
    state: AgentState,
    llm: BaseChatModel,
    query: str,
    schema_context: str,
) -> AgentState:
    """Run the analyst in DataFrame mode — generates and executes Python code."""
    from app.sandbox.executor import execute_python

    logger.info("DataFrame Analyst: Planning for '%s'", query[:80])

    plan_messages = [
        SystemMessage(content=_DF_PLAN_PROMPT.format(
            question=query,
            schema_context=schema_context[:4000],
        )),
        HumanMessage(content=query),
    ]

    plan_response = await llm.ainvoke(plan_messages)
    code: str = plan_response.content.strip()

    # Strip markdown fences
    if code.startswith("```"):
        lines = code.split("\n")
        lines = [ln for ln in lines if not ln.strip().startswith("```")]
        code = "\n".join(lines).strip()

    # Prepend the code preamble (loads DataFrames from files)
    preamble = ""
    if "CODE PREAMBLE" in schema_context:
        marker = "CODE PREAMBLE (prepend to all Python code):\n"
        if marker in schema_context:
            raw_preamble = schema_context.split(marker, 1)[1].strip()
            # Keep ONLY valid Python lines: imports and DataFrame reads
            # Stop at first blank line or non-code line to avoid picking up relationship text
            preamble_lines = []
            for line in raw_preamble.split("\n"):
                stripped = line.strip()
                if not stripped:
                    if preamble_lines:
                        break  # Stop at first blank line after code starts
                    continue
                # Only keep actual Python: imports and pd.read_parquet assignments
                if stripped.startswith(("import ", "from ")) or "= pd.read_parquet(" in stripped:
                    preamble_lines.append(line)
                elif "→" in stripped or "CROSS" in stripped or "RELATIONSHIP" in stripped:
                    break  # Hit relationship text — stop
                else:
                    break  # Unknown line — stop to be safe
            preamble = "\n".join(preamble_lines) + "\n\n" if preamble_lines else ""

    full_code = preamble + code
    logger.info("DataFrame Analyst: Executing %d chars of code", len(full_code))

    # Execute with retry
    result = await execute_python(full_code)

    if not result.success:
        # Retry with error context
        logger.warning("DataFrame Analyst: First attempt failed: %s", result.error[:200] if result.error else "")
        fix_messages = [
            SystemMessage(content=(
                f"The Python code failed. Fix it.\n\n"
                f"Available DataFrames:\n{schema_context[:3000]}\n\n"
                f"Original code:\n{code}\n\n"
                f"Error: {result.error}\n\n"
                f"Return ONLY the fixed Python code."
            )),
            HumanMessage(content=f"Fix this code. Error: {result.error}"),
        ]
        fix_response = await llm.ainvoke(fix_messages)
        fixed_code: str = fix_response.content.strip()
        if fixed_code.startswith("```"):
            lines = fixed_code.split("\n")
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            fixed_code = "\n".join(lines).strip()

        full_code = preamble + fixed_code
        result = await execute_python(full_code)

    if not result.success:
        logger.warning("DataFrame Analyst: Both attempts failed")
        return {
            **state,
            "code_block": full_code,
            "error": result.error or "Analysis failed",
        }

    # Synthesize the stdout output into a clean answer
    stdout_text = result.stdout or ""
    synth_messages = [
        SystemMessage(content=_SYNTHESIZE_PROMPT.format(
            question=query,
            analysis_results=f"Python analysis output:\n{stdout_text[:3000]}",
        )),
        HumanMessage(content=query),
    ]
    synth_response = await llm.ainvoke(synth_messages)
    answer: str = synth_response.content.strip()

    return {
        **state,
        "execution_result": answer,
        "code_block": full_code,
        "plotly_figure": result.plotly_figure,
        "error": None,
    }
