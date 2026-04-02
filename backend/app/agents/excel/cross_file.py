"""Cross-file relationship discovery — finds links between DataFrames from different files."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def discover_cross_file_relationships(
    file_contexts: list[dict[str, Any]],
) -> list[dict[str, str]]:
    """Find shared columns across DataFrames from different files.

    Args:
        file_contexts: list of {"filename": str, "parquet_paths": {var: path}, "column_info": {...}}

    Returns:
        list of {"source_var": str, "source_col": str, "target_var": str, "target_col": str, "match_type": str}
    """
    if len(file_contexts) < 2:
        return []

    var_columns: dict[str, set[str]] = {}
    var_to_file: dict[str, str] = {}

    for ctx in file_contexts:
        filename = ctx.get("filename", "unknown")
        col_info = ctx.get("column_info") or {}
        columns = col_info.get("columns", [])
        parquet_paths = ctx.get("parquet_paths") or {}

        for var_name in parquet_paths:
            col_names = {c["name"].lower() for c in columns if isinstance(c, dict)}
            var_columns[var_name] = col_names
            var_to_file[var_name] = filename

    relationships = []
    vars_list = list(var_columns.keys())

    for i, var_a in enumerate(vars_list):
        for var_b in vars_list[i + 1:]:
            if var_to_file[var_a] == var_to_file[var_b]:
                continue

            shared = var_columns[var_a] & var_columns[var_b]
            for col in shared:
                is_key = any(kw in col for kw in ("id", "key", "code", "name", "email", "sku"))
                relationships.append({
                    "source_var": var_a,
                    "source_col": col,
                    "target_var": var_b,
                    "target_col": col,
                    "match_type": "exact_name_key" if is_key else "exact_name",
                    "confidence": 0.95 if is_key else 0.7,
                })

    logger.info("Cross-file discovery: %d relationships across %d files",
                len(relationships), len(file_contexts))
    return relationships


def format_cross_file_context(relationships: list[dict]) -> str:
    """Format cross-file relationships for injection into LLM prompt."""
    if not relationships:
        return ""

    lines = [
        "\nCROSS-FILE RELATIONSHIPS (use pd.merge to join these DataFrames):",
        "=" * 60,
    ]
    for rel in relationships:
        conf = rel.get("confidence", 0.7)
        lines.append(
            f"  {rel['source_var']}.{rel['source_col']} → {rel['target_var']}.{rel['target_col']}"
            f"  ({conf:.0%} confidence)"
        )
        lines.append(
            f"    Code: pd.merge({rel['source_var']}, {rel['target_var']}, "
            f"on='{rel['source_col']}')"
        )

    return "\n".join(lines)
