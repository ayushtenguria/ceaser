"""Cross-DB Joiner — merges results from multiple databases in pandas.

Handles type mismatches, missing data, partial results, and generates
the final output DataFrame.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from app.agents.crossdb.executor import QueryResult
from app.agents.crossdb.planner import CrossDbQueryPlan

logger = logging.getLogger(__name__)


def join_results(
    results: dict[str, QueryResult],
    plan: CrossDbQueryPlan,
) -> dict[str, Any]:
    """Join results from multiple databases into a single output.

    Returns:
        dict with "df" (final DataFrame), "table_data" (for frontend),
        "warnings" (list of issues), "execution_summary" (text)
    """
    warnings: list[str] = []
    summary_parts: list[str] = []

    for alias, result in results.items():
        if result.success:
            summary_parts.append(
                f"  {result.connection_name}: {result.row_count} rows ({result.execution_ms}ms)"
            )
        else:
            warnings.append(f"{result.connection_name}: {result.error}")
            summary_parts.append(f"  {result.connection_name}: FAILED — {result.error}")

    if len(plan.joins) == 0:
        for alias, result in results.items():
            if result.success and result.df is not None:
                return _format_output(result.df, warnings, summary_parts)
        return _empty_output(warnings, summary_parts)

    dfs: dict[str, pd.DataFrame] = {}
    for alias, result in results.items():
        if result.success and result.df is not None:
            dfs[alias] = result.df

    if not dfs:
        return _empty_output(warnings, summary_parts)

    current_df: pd.DataFrame | None = None

    for join in plan.joins:
        left_df = dfs.get(join.left_alias) if current_df is None else current_df
        right_df = dfs.get(join.right_alias)

        if left_df is None:
            warnings.append(f"Missing data for {join.left_alias}")
            continue
        if right_df is None:
            warnings.append(f"Missing data for {join.right_alias}, using partial results")
            current_df = left_df
            continue

        try:
            left_col = join.left_on
            right_col = join.right_on

            if left_col not in left_df.columns:
                left_col = _find_column(left_df, join.left_on)
            if right_col not in right_df.columns:
                right_col = _find_column(right_df, join.right_on)

            if not left_col or not right_col:
                warnings.append(
                    f"Join column not found: {join.left_alias}.{join.left_on} "
                    f"↔ {join.right_alias}.{join.right_on}"
                )
                current_df = left_df
                continue

            left_df = left_df.copy()
            right_df = right_df.copy()
            left_df[left_col] = left_df[left_col].astype(str)
            right_df[right_col] = right_df[right_col].astype(str)

            how = join.how if join.how in ("left", "right", "inner", "outer") else "left"

            current_df = pd.merge(
                left_df,
                right_df,
                left_on=left_col,
                right_on=right_col,
                how=how,
                suffixes=("", f"_{join.right_alias}"),
            )

            logger.info(
                "Joined %s.%s ↔ %s.%s (%s): %d rows",
                join.left_alias,
                left_col,
                join.right_alias,
                right_col,
                how,
                len(current_df),
            )

        except Exception as exc:
            warnings.append(f"Join failed: {exc}")
            logger.warning("Join failed: %s", exc)
            if left_df is not None:
                current_df = left_df

    if current_df is None:
        return _empty_output(warnings, summary_parts)

    if plan.post_join_operations:
        try:
            local_vars = {"df": current_df, "pd": pd}
            exec(plan.post_join_operations, {"__builtins__": {}}, local_vars)
            if "result" in local_vars and isinstance(local_vars["result"], pd.DataFrame):
                current_df = local_vars["result"]
        except Exception as exc:
            warnings.append(f"Post-join operation failed: {exc}")

    return _format_output(current_df, warnings, summary_parts)


def _format_output(
    df: pd.DataFrame,
    warnings: list[str],
    summary_parts: list[str],
) -> dict[str, Any]:
    """Format the final DataFrame into the standard output format."""
    display_df = df.head(500)

    columns = list(display_df.columns)
    rows = []
    for _, row in display_df.iterrows():
        row_dict = {}
        for col in columns:
            val = row[col]
            if pd.isna(val):
                row_dict[col] = None
            elif hasattr(val, "item"):
                row_dict[col] = val.item()
            else:
                row_dict[col] = str(val) if not isinstance(val, (int, float, bool)) else val
        rows.append(row_dict)

    return {
        "df": df,
        "table_data": {
            "columns": columns,
            "rows": rows,
            "total_rows": len(df),
            "truncated": len(df) > 500,
        },
        "warnings": warnings,
        "execution_summary": "\n".join(summary_parts),
    }


def _empty_output(warnings: list[str], summary_parts: list[str]) -> dict[str, Any]:
    """Return an empty result when no data is available."""
    return {
        "df": pd.DataFrame(),
        "table_data": {"columns": [], "rows": [], "total_rows": 0},
        "warnings": warnings + ["No data available from any database."],
        "execution_summary": "\n".join(summary_parts),
    }


def _find_column(df: pd.DataFrame, name: str) -> str | None:
    """Find a column by case-insensitive match."""
    name_lower = name.lower()
    for col in df.columns:
        if col.lower() == name_lower:
            return col
    return None
