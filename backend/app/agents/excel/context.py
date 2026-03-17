"""Context Builder Agent — builds LLM-ready descriptions and saves DataFrames as parquet.

No SQLite — DataFrames are saved as parquet for fast loading in the sandbox,
and a text schema description is generated for the LLM prompt.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from app.agents.excel.parser import SheetResult, WorkbookResult
from app.agents.excel.relationships import Relationship

logger = logging.getLogger(__name__)

_PARQUET_DIR = Path(__file__).resolve().parent.parent.parent.parent / "uploads" / "parquet"


def save_dataframes_to_parquet(
    workbooks: list[WorkbookResult],
) -> dict[str, str]:
    """Save all DataFrames as parquet files for fast sandbox loading.

    Returns a mapping: df_variable_name -> parquet_file_path
    """
    _PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    paths: dict[str, str] = {}

    for wb in workbooks:
        file_prefix = Path(wb.file_name).stem.lower().replace(" ", "_").replace("-", "_")

        for sheet in wb.sheets:
            # Variable name: df_filename_sheetname
            var_name = f"df_{file_prefix}_{sheet.name.lower().replace(' ', '_')}"
            var_name = "".join(c if c.isalnum() or c == "_" else "_" for c in var_name)

            parquet_path = _PARQUET_DIR / f"{var_name}.parquet"
            sheet.df.to_parquet(str(parquet_path), index=False)
            paths[var_name] = str(parquet_path)

            logger.info("Saved %s: %d rows -> %s", var_name, len(sheet.df), parquet_path)

    return paths


def build_excel_context(
    workbooks: list[WorkbookResult],
    relationships: list[Relationship],
    parquet_paths: dict[str, str],
) -> str:
    """Build a text description of the Excel data for the LLM prompt.

    Similar to the DB schema context but for DataFrames.
    """
    lines: list[str] = [
        "EXCEL DATA CONTEXT",
        "=" * 50,
        f"Files loaded: {len(workbooks)}",
        f"Total sheets: {sum(len(wb.sheets) for wb in workbooks)}",
        f"Total rows: {sum(wb.total_rows for wb in workbooks):,}",
        "",
    ]

    # DataFrames available
    lines.append("AVAILABLE DATAFRAMES (pre-loaded in Python):")
    lines.append("-" * 40)

    for wb in workbooks:
        file_prefix = Path(wb.file_name).stem.lower().replace(" ", "_").replace("-", "_")
        for sheet in wb.sheets:
            var_name = f"df_{file_prefix}_{sheet.name.lower().replace(' ', '_')}"
            var_name = "".join(c if c.isalnum() or c == "_" else "_" for c in var_name)

            lines.append(f"\n{var_name}  ({sheet.row_count:,} rows, {sheet.column_count} columns)")
            lines.append(f"  Source: {wb.file_name} -> {sheet.name}")

            for col in sheet.df.columns:
                col_type = sheet.column_types.get(col, "unknown")
                parts = [f"    {col}: {col_type}"]

                # Sample values (max 5)
                samples = sheet.sample_values.get(col, [])
                if samples:
                    sample_str = ", ".join(repr(v) for v in samples[:5])
                    parts.append(f"  values: [{sample_str}]")

                lines.append(" ".join(parts))

    # Relationships
    if relationships:
        lines.append("\n\nRELATIONSHIPS (use pd.merge for JOINs):")
        lines.append("=" * 50)
        for rel in relationships:
            src_var = _sheet_to_var(workbooks, rel.source_sheet)
            tgt_var = _sheet_to_var(workbooks, rel.target_sheet)
            lines.append(
                f"  {src_var}.{rel.source_column} -> {tgt_var}.{rel.target_column}"
                f"  ({rel.relationship_type}, {rel.confidence:.0%} confidence)"
            )
            lines.append(
                f"    Code: pd.merge({src_var}, {tgt_var}, "
                f"left_on='{rel.source_column}', right_on='{rel.target_column}')"
            )

    return "\n".join(lines)


def generate_code_preamble(parquet_paths: dict[str, str]) -> str:
    """Generate Python code that pre-loads all DataFrames.

    This code is prepended to every sandbox execution for Excel queries.
    """
    lines = ["import pandas as pd", "import plotly.express as px", ""]

    for var_name, path in parquet_paths.items():
        lines.append(f'{var_name} = pd.read_parquet("{path}")')

    lines.append("")
    return "\n".join(lines)


def _sheet_to_var(workbooks: list[WorkbookResult], sheet_name: str) -> str:
    """Convert a sheet name to its DataFrame variable name."""
    for wb in workbooks:
        file_prefix = Path(wb.file_name).stem.lower().replace(" ", "_").replace("-", "_")
        for sheet in wb.sheets:
            if sheet.name == sheet_name:
                var_name = f"df_{file_prefix}_{sheet.name.lower().replace(' ', '_')}"
                return "".join(c if c.isalnum() or c == "_" else "_" for c in var_name)
    return f"df_{sheet_name.lower()}"
