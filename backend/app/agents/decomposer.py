"""Query decomposer agent — breaks compound questions into independent sub-queries.

When a user asks "show me top customers AND plot revenue trend", this agent
splits it into two sub-queries that can be executed independently.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

logger = logging.getLogger(__name__)

_DECOMPOSE_PROMPT = """\
You are a query decomposer for a data analysis system.

Given a user's question, determine if it contains MULTIPLE independent data requests.

Examples of compound queries:
- "Show me top customers and also plot revenue trend" → 2 sub-queries
- "What is our MRR and how many open tickets do we have" → 2 sub-queries
- "Plot a chart of deals by stage and another chart of revenue by month" → 2 sub-queries

Examples of SINGLE queries (do NOT split):
- "Show me top customers by revenue" → 1 query (just one request)
- "Plot revenue trend for the last 12 months" → 1 query
- "What is total revenue grouped by department" → 1 query
- "Show me customers with health score below 60 and their ARR" → 1 query (it's one dataset)
- "Who are our top employees by salary" → 1 query

Rules:
1. Only split if there are clearly INDEPENDENT requests joined by "and", "also", "plus",
   "as well as", "another", "additionally".
2. Do NOT split a single query that has multiple columns/conditions — that's just one query.
3. Maximum 3 sub-queries.
4. Each sub-query MUST be fully self-contained. Never create a sub-query like "plot graph
   for both" or "do the same for that" — these have no context on their own.
5. If the user says "plot/chart/graph for both" or "for all of them", expand each into
   a complete sub-query with the specific topic included.
6. If the user asks to show data AND plot it, combine into one sub-query per topic
   (e.g., "show top customers and plot it" = "show top customers with a chart").

Respond with a JSON array of strings. If it's a single query, return a single-element array.

Examples:
- Input: "show top customers and plot revenue by month"
  Output: ["show top customers", "plot revenue by month as a chart"]
- Input: "what is our MRR"
  Output: ["what is our MRR"]
- Input: "show me top deal crackers and revenue by month also plot graph for both"
  Output: ["plot a chart of top deal crackers", "plot a chart of revenue by month"]
- Input: "show me employees by department and customers by plan with charts"
  Output: ["show employees by department with a chart", "show customers by plan with a chart"]
- Input: "what is total revenue and how many open tickets"
  Output: ["what is total revenue", "how many open tickets are there"]
"""


async def decompose_query(
    query: str,
    llm: BaseChatModel,
) -> list[str]:
    """Break a compound question into independent sub-queries.

    Returns a list of 1-3 sub-query strings. If the question is simple,
    returns a single-element list with the original query.
    """
    messages = [
        SystemMessage(content=_DECOMPOSE_PROMPT),
        HumanMessage(content=query),
    ]

    response = await llm.ainvoke(messages)
    raw: str = response.content.strip()  # type: ignore[union-attr]

    # Parse the JSON array
    try:
        # Strip markdown fences if present
        if raw.startswith("```"):
            lines = raw.split("\n")
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            raw = "\n".join(lines).strip()

        sub_queries = json.loads(raw)

        if not isinstance(sub_queries, list):
            logger.warning("Decomposer returned non-list: %s", raw[:100])
            return [query]

        # Validate and cap at 3
        sub_queries = [str(q).strip() for q in sub_queries if str(q).strip()][:3]

        if not sub_queries:
            return [query]

        logger.info("Decomposed into %d sub-queries: %s", len(sub_queries), sub_queries)
        return sub_queries

    except (json.JSONDecodeError, ValueError):
        logger.warning("Decomposer returned invalid JSON: %s", raw[:100])
        return [query]
