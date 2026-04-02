"""SQL agent node — generates a SELECT query from natural language."""

from __future__ import annotations

import logging

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import SystemMessage

from app.agents.state import AgentState

logger = logging.getLogger(__name__)

_SQL_SYSTEM_PROMPT = """\
You are an expert SQL analyst for a B2B SaaS analytics platform. Generate a SINGLE, \
read-only SQL query that precisely answers the user's question.

CRITICAL RULES:
1. Only produce SELECT statements. Never INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE.
2. Always qualify column names with table aliases when joining.
3. Use the EXACT column values shown in the schema below. For example, if the schema \
   shows status values: ['open', 'resolved'], use 'open' not 'Open' or 'OPEN'.
4. Return ONLY the raw SQL query — no explanations, no markdown fences, no surrounding text.
5. Add LIMIT 1000 unless the user explicitly asks for all rows.
6. Use meaningful column aliases (e.g., "total_revenue" not "sum1").
7. When the user asks for "by X" or "per X", always include GROUP BY.
8. When filtering on string columns, use the EXACT values listed in the schema. Case matters.
9. For date/time operations, use dialect-appropriate functions:
   - PostgreSQL: DATE_TRUNC('month', col), EXTRACT(YEAR FROM col), TO_CHAR(), interval
   - MySQL: DATE_FORMAT(), YEAR(), MONTH()
   - SQLite: strftime()
10. When counting, use COUNT(*) for row counts, COUNT(DISTINCT col) for unique counts.
11. For "top N" queries, always ORDER BY the relevant metric DESC and add LIMIT N.
12. For JOINs, prefer LEFT JOIN unless an INNER JOIN is clearly needed.
13. When the schema shows foreign keys (FK), use those for joins — do NOT guess join columns.

FUZZY MATCHING RULES (for dirty/inconsistent data):
14. When filtering text/categorical columns, ALWAYS use LOWER(TRIM(col)) for comparison \
    to handle case differences and whitespace: WHERE LOWER(TRIM(status)) = 'open'
15. For city/location/name columns, use ILIKE with wildcards to catch abbreviations and \
    spelling variations: WHERE city ILIKE '%gurgaon%' OR city ILIKE '%gurugram%'
16. If the schema shows a column marked as [DIRTY DATA] with known variations, include \
    ALL listed variations in your WHERE clause using OR / ILIKE ANY.
17. For columns with low unique counts (< 20 distinct values), prefer GROUP BY with \
    LOWER(TRIM(col)) to merge case variants: GROUP BY LOWER(TRIM(status))
18. When the agent memory mentions aliases (e.g., "GGN = Gurgaon"), include all aliases.

VISUALIZATION RULES:
19. When the user asks for a HISTOGRAM or DISTRIBUTION, return RAW individual values — \
    do NOT bucket or aggregate with CASE/WHEN. Return: SELECT col FROM table LIMIT 1000. \
    The Python agent will handle binning with px.histogram(). Pre-aggregated data breaks histograms.
20. When the user asks for a bar chart, pie chart, or comparison, aggregation is fine \
    (GROUP BY, COUNT, SUM, AVG).

SMART JOIN RULES (for multi-table analysis):
19. When the question asks about "what are they selling" or "who are the customers", \
    ALWAYS JOIN across order_items → products → categories AND orders → customers to get \
    human-readable names. Never return just IDs when names/descriptions are available.
20. For consulting-style questions (buying potential, cross-sell, lapsed accounts, retention), \
    build comprehensive queries that JOIN customers + orders + order_items + products. \
    Include: customer name/company, total spend, order count, recency, product categories purchased.
21. When the question implies analysis of customer behavior, always include: \
    total_orders, total_spend, avg_order_value, last_order_date, categories_purchased.

{schema_context}
"""


async def generate_sql(state: AgentState, llm: BaseChatModel) -> AgentState:
    """Use the LLM to generate a SQL query for the user's question."""
    schema_context = state.get("schema_context", "")

    messages = [
        SystemMessage(content=_SQL_SYSTEM_PROMPT.format(schema_context=schema_context)),
        *state["messages"],
    ]

    if state.get("error") and state.get("sql_query"):
        from langchain_core.messages import HumanMessage
        messages.append(HumanMessage(
            content=f"The previous SQL query failed with this error:\n"
                    f"Query: {state['sql_query']}\n"
                    f"Error: {state['error']}\n\n"
                    f"Please fix the query and try again."
        ))

    response = await llm.ainvoke(messages)
    raw_sql: str = response.content.strip()  # type: ignore[union-attr]

    if raw_sql.startswith("```"):
        lines = raw_sql.split("\n")
        lines = [ln for ln in lines if not ln.strip().startswith("```")]
        raw_sql = "\n".join(lines).strip()

    logger.info("Generated SQL:\n%s", raw_sql)
    return {**state, "sql_query": raw_sql}
