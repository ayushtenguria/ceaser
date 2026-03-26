"""Parallel Executor — runs queries across multiple DBs simultaneously.

Uses asyncio.gather for true parallel execution. Each DB gets its own
timeout and failure is isolated (one DB down doesn't kill the others).
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agents.crossdb.planner import CrossDbQueryPlan, SubQuery
from app.connectors.factory import get_connector
from app.db.models import DatabaseConnection

logger = logging.getLogger(__name__)

_PER_QUERY_TIMEOUT = 30  # seconds per individual query


@dataclass
class QueryResult:
    """Result from executing one sub-query."""
    alias: str
    connection_name: str
    df: pd.DataFrame | None = None
    row_count: int = 0
    columns: list[str] = field(default_factory=list)
    execution_ms: int = 0
    success: bool = True
    error: str | None = None


async def execute_parallel_queries(
    plan: CrossDbQueryPlan,
    db: AsyncSession,
    timeout_per_query: int = _PER_QUERY_TIMEOUT,
) -> dict[str, QueryResult]:
    """Execute all sub-queries in parallel and return results.

    Returns: dict of alias -> QueryResult
    """
    if not plan.queries:
        return {}

    async def _execute_one(sub: SubQuery) -> QueryResult:
        result = QueryResult(
            alias=sub.result_alias,
            connection_name=sub.connection_name,
        )
        start = time.monotonic()

        try:
            # File source — load from parquet, no SQL needed
            if sub.source_type == "file":
                return await _execute_file_query(sub, result, start)

            # Database source — execute SQL
            stmt = select(DatabaseConnection).where(
                DatabaseConnection.id == uuid.UUID(sub.connection_id)
            )
            res = await db.execute(stmt)
            connection = res.scalar_one_or_none()

            if connection is None:
                result.success = False
                result.error = f"Connection '{sub.connection_name}' not found"
                return result

            connector = get_connector(connection)

            try:
                await connector.connect()
                columns, rows = await asyncio.wait_for(
                    connector.execute_query(sub.sql),
                    timeout=timeout_per_query,
                )
                await connector.disconnect()

                # Convert to DataFrame
                df = pd.DataFrame(rows, columns=columns)

                # Convert non-serializable types
                for col in df.columns:
                    try:
                        if df[col].dtype == object:
                            # Try numeric conversion for string numbers
                            numeric = pd.to_numeric(df[col], errors="coerce")
                            if numeric.notna().sum() > df[col].notna().sum() * 0.8:
                                df[col] = numeric
                    except Exception:
                        pass

                result.df = df
                result.row_count = len(df)
                result.columns = list(df.columns)
                result.success = True

            except asyncio.TimeoutError:
                result.success = False
                result.error = f"Query timed out after {timeout_per_query}s"
                logger.warning("Query timeout on %s: %s", sub.connection_name, sub.sql[:80])
            finally:
                try:
                    await connector.disconnect()
                except Exception:
                    pass

        except Exception as exc:
            result.success = False
            result.error = str(exc)
            logger.warning("Query failed on %s: %s", sub.connection_name, exc)

        result.execution_ms = int((time.monotonic() - start) * 1000)
        return result

    # Execute ALL queries in parallel
    raw_results = await asyncio.gather(
        *[_execute_one(sub) for sub in plan.queries],
        return_exceptions=False,
    )

    results: dict[str, QueryResult] = {}
    for r in raw_results:
        results[r.alias] = r

    successful = sum(1 for r in results.values() if r.success)
    total_rows = sum(r.row_count for r in results.values() if r.success)
    logger.info(
        "Parallel execution: %d/%d succeeded, %d total rows",
        successful, len(results), total_rows,
    )

    return results


async def _execute_file_query(
    sub: SubQuery,
    result: QueryResult,
    start: float,
) -> QueryResult:
    """Execute a query against a file source (load from parquet)."""
    try:
        if not sub.parquet_paths:
            result.success = False
            result.error = "No parquet files found for this file source"
            return result

        # Load parquet files (via storage backend for signed URLs / local paths)
        from app.services.storage import get_storage
        storage = get_storage()
        dfs = []
        for var_name, path in sub.parquet_paths.items():
            try:
                url = await storage.download_url(path)
                df = pd.read_parquet(url)
                dfs.append(df)
            except Exception as exc:
                logger.warning("Failed to read parquet %s: %s", path, exc)

        if not dfs:
            result.success = False
            result.error = "Failed to load any data from file"
            return result

        # If multiple sheets, concat them (user can filter later)
        df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

        # If there's pandas code to execute, run it
        if sub.python_code:
            local_vars = {"df": df, "pd": pd}
            exec(sub.python_code, {"__builtins__": {}}, local_vars)
            if "result_df" in local_vars and isinstance(local_vars["result_df"], pd.DataFrame):
                df = local_vars["result_df"]

        result.df = df
        result.row_count = len(df)
        result.columns = list(df.columns)
        result.success = True

    except Exception as exc:
        result.success = False
        result.error = str(exc)
        logger.warning("File query failed on %s: %s", sub.connection_name, exc)

    result.execution_ms = int((time.monotonic() - start) * 1000)
    return result
