"""Cross-Database Query Engine — queries across multiple microservice databases.

Handles: multi-DB schema loading, query planning, parallel execution, cross-DB joins.
"""

from app.agents.crossdb.schema_loader import load_all_schemas, MultiDbSchema
from app.agents.crossdb.planner import plan_cross_db_query, CrossDbQueryPlan
from app.agents.crossdb.executor import execute_parallel_queries
from app.agents.crossdb.joiner import join_results

__all__ = [
    "load_all_schemas", "MultiDbSchema",
    "plan_cross_db_query", "CrossDbQueryPlan",
    "execute_parallel_queries",
    "join_results",
]
