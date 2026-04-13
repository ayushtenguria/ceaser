"""Cross-DB Query Planner — plans individual queries per database.

Takes a user question + multi-DB schema, identifies which DBs have the
relevant data, and produces separate SQL queries per DB plus a join plan.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from app.agents.crossdb.schema_loader import MultiDbSchema

logger = logging.getLogger(__name__)


@dataclass
class SubQuery:
    """A query to run against one specific data source."""

    connection_id: str
    connection_name: str
    sql: str
    python_code: str = ""
    source_type: str = "database"
    purpose: str = ""
    result_alias: str = ""
    parquet_paths: dict[str, str] = field(default_factory=dict)


@dataclass
class JoinStep:
    """How to join two intermediate results."""

    left_alias: str
    right_alias: str
    left_on: str
    right_on: str
    how: str = "left"


@dataclass
class CrossDbQueryPlan:
    """Complete plan for a cross-database query."""

    queries: list[SubQuery] = field(default_factory=list)
    joins: list[JoinStep] = field(default_factory=list)
    post_join_operations: str = ""
    explanation: str = ""
    is_single_db: bool = False


_PLAN_PROMPT = """\
You are a cross-database query planner. The user's data is spread across multiple databases.

User question: {question}

Available databases and their schemas:
{schema}

Your job:
1. Identify which database(s) contain the tables needed to answer the question
2. Write a separate SQL query for EACH database (use that DB's dialect)
3. Plan how to join the results in pandas
4. Each SQL should only fetch the columns needed (minimize data transfer)

RULES:
- Each query MUST be a SELECT statement
- Include LIMIT 10000 on each query (safety)
- Use table aliases in SQL
- If only ONE database is needed, set is_single_db=true
- For joins, identify the matching columns across databases

Return JSON:
{{
  "is_single_db": false,
  "explanation": "Brief explanation of the plan",
  "queries": [
    {{
      "connection_id": "uuid-here",
      "connection_name": "DB Name",
      "sql": "SELECT ...",
      "purpose": "What this query fetches",
      "result_alias": "df_something"
    }}
  ],
  "joins": [
    {{
      "left_alias": "df_users",
      "right_alias": "df_orders",
      "left_on": "id",
      "right_on": "user_id",
      "how": "left"
    }}
  ],
  "post_join_operations": "Optional pandas code for final sort/filter"
}}
"""


async def plan_cross_db_query(
    question: str,
    multi_schema: MultiDbSchema,
    llm: BaseChatModel,
) -> CrossDbQueryPlan:
    """Plan how to answer a question using multiple databases."""
    messages = [
        SystemMessage(
            content=_PLAN_PROMPT.format(
                question=question,
                schema=multi_schema.combined_context[:6000],
            )
        ),
        HumanMessage(content=question),
    ]

    try:
        response = await llm.ainvoke(messages)
        raw: str = response.content.strip()  # type: ignore[union-attr]

        if raw.startswith("```"):
            lines = raw.split("\n")
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            raw = "\n".join(lines).strip()

        data = json.loads(raw)

        plan = CrossDbQueryPlan(
            is_single_db=data.get("is_single_db", False),
            explanation=data.get("explanation", ""),
            post_join_operations=data.get("post_join_operations", ""),
        )

        for q in data.get("queries", []):
            sql = q.get("sql", "")
            if not sql:
                continue
            if not sql.strip().upper().startswith(("SELECT", "WITH")):
                logger.warning("Cross-DB planner produced non-SELECT: %s", sql[:50])
                continue

            plan.queries.append(
                SubQuery(
                    connection_id=q.get("connection_id", ""),
                    connection_name=q.get("connection_name", ""),
                    sql=sql,
                    purpose=q.get("purpose", ""),
                    result_alias=q.get("result_alias", f"df_{len(plan.queries)}"),
                )
            )

        for j in data.get("joins", []):
            plan.joins.append(
                JoinStep(
                    left_alias=j.get("left_alias", ""),
                    right_alias=j.get("right_alias", ""),
                    left_on=j.get("left_on", ""),
                    right_on=j.get("right_on", ""),
                    how=j.get("how", "left"),
                )
            )

        logger.info(
            "Cross-DB plan: %d queries, %d joins, single_db=%s",
            len(plan.queries),
            len(plan.joins),
            plan.is_single_db,
        )
        return plan

    except Exception as exc:
        logger.warning("Cross-DB planning failed: %s", exc)
        return CrossDbQueryPlan(explanation=f"Planning failed: {exc}")
