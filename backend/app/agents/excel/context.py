"""Context Builder Agent — builds LLM-ready descriptions and saves DataFrames as parquet.

Parquet files are saved via the storage backend (local or Supabase).
Code preamble uses safe ceaser:// aliases — never exposes real URLs.
Real URLs are resolved only at sandbox execution time, server-side.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from app.agents.excel.relationship_mapper import Relationship

logger = logging.getLogger(__name__)

# Protocol prefix for safe file references (never contains real URLs/tokens)
CEASER_PROTOCOL = "ceaser://"


def _make_var_name(file_name: str, sheet_name: str, sheet_count: int) -> str:
    """Create a clean, short DataFrame variable name."""
    import re
    prefix = Path(file_name).stem.lower().replace(" ", "_").replace("-", "_")
    prefix = re.sub(r'^[a-f0-9]{20,}_', '', prefix)  # strip UUID prefix
    prefix = prefix[:30]
    sheet_clean = sheet_name.lower().replace(' ', '_')[:30]

    if sheet_count == 1:
        var = f"df_{prefix}" if prefix else f"df_{sheet_clean}"
    else:
        var = f"df_{prefix}_{sheet_clean}" if prefix else f"df_{sheet_clean}"

    var = "".join(c if c.isalnum() or c == "_" else "_" for c in var)
    var = re.sub(r'_+', '_', var).strip('_')
    return var


def save_dataframes_to_parquet(
    workbooks: list[Any],
    org_id: str = "default",
) -> dict[str, str]:
    """Save all DataFrames as parquet via storage backend.

    Returns a mapping: df_variable_name -> remote_path (storage key).
    The remote_path is a storage key like 'parquet/org/df_name.parquet',
    NOT a URL. URLs are only generated at execution time.
    """
    import asyncio
    from app.services.storage import get_storage
    storage = get_storage()

    paths: dict[str, str] = {}

    try:
        loop = asyncio.get_event_loop()
        has_loop = loop.is_running()
    except RuntimeError:
        has_loop = False

    for wb in workbooks:
        for sheet in wb.sheets:
            var_name = _make_var_name(wb.file_name, sheet.name, len(wb.sheets))
            remote_path = f"parquet/{org_id}/{var_name}.parquet"

            buf = sheet.df.to_parquet(index=False)

            if has_loop:
                future = asyncio.run_coroutine_threadsafe(
                    storage.upload(buf, remote_path), loop
                )
                future.result(timeout=60)
            else:
                asyncio.run(storage.upload(buf, remote_path))

            paths[var_name] = remote_path
            logger.info("Saved %s: %d rows -> %s", var_name, len(sheet.df), remote_path)

    return paths


def generate_code_preamble(parquet_paths: dict[str, str]) -> str:
    """Generate Python code that pre-loads all DataFrames using safe aliases.

    The code uses ceaser:// protocol references instead of real URLs:
        df_sales = pd.read_parquet("ceaser://parquet/org/df_sales.parquet")

    These are resolved to real URLs/paths only at sandbox execution time
    by the sandbox executor. This ensures:
    - LLMs never see real storage URLs or tokens
    - Frontend never sees signed URLs in code blocks
    - Database never stores signed URLs
    """
    lines = ["import pandas as pd", "import plotly.express as px", ""]

    for var_name, remote_path in parquet_paths.items():
        safe_ref = f"{CEASER_PROTOCOL}{remote_path}"
        lines.append(f'{var_name} = pd.read_parquet("{safe_ref}")')

    lines.append("")
    return "\n".join(lines)


async def resolve_ceaser_refs(code: str) -> str:
    """Replace ceaser:// references with real storage URLs/paths.

    Called ONLY by the sandbox executor right before execution.
    The resolved code is never stored, sent to LLMs, or returned to clients.
    """
    import re
    from app.services.storage import get_storage
    storage = get_storage()

    pattern = re.compile(r'ceaser://([^\s"\']+)')
    matches = pattern.findall(code)

    if not matches:
        return code

    resolved = code
    for remote_path in set(matches):
        try:
            real_url = await storage.download_url(remote_path)
            resolved = resolved.replace(f"{CEASER_PROTOCOL}{remote_path}", real_url)
        except Exception as exc:
            logger.warning("Could not resolve ceaser://%s: %s", remote_path, exc)

    return resolved


def build_excel_context(
    workbooks: list[Any],
    relationships: list[Any],
    parquet_paths: dict[str, str],
) -> str:
    """Build a text description of the Excel data for the LLM prompt."""
    lines: list[str] = [
        "EXCEL DATA CONTEXT",
        "=" * 50,
        f"Files loaded: {len(workbooks)}",
        f"Total sheets: {sum(len(wb.sheets) for wb in workbooks)}",
        f"Total rows: {sum(wb.total_rows for wb in workbooks):,}",
        "",
    ]

    lines.append("AVAILABLE DATAFRAMES (pre-loaded in Python):")
    lines.append("-" * 40)

    for wb in workbooks:
        for sheet in wb.sheets:
            var_name = _make_var_name(wb.file_name, sheet.name, len(wb.sheets))

            lines.append(f"\n{var_name}  ({sheet.row_count:,} rows, {sheet.column_count} columns)")
            lines.append(f"  Source: {wb.file_name} -> {sheet.name}")

            for col in sheet.df.columns:
                col_type = sheet.column_types.get(col, "unknown")
                parts = [f"    {col}: {col_type}"]

                samples = sheet.sample_values.get(col, [])
                if samples:
                    sample_str = ", ".join(repr(v) for v in samples[:5])
                    parts.append(f"  values: [{sample_str}]")

                lines.append(" ".join(parts))

    if relationships:
        lines.append("\n\nRELATIONSHIPS (use pd.merge for JOINs):")
        lines.append("=" * 50)
        for rel in relationships:
            src_var = _sheet_to_var(workbooks, rel.source_sheet)
            tgt_var = _sheet_to_var(workbooks, rel.target_sheet)
            lines.append(
                f"  {src_var}.{rel.source_column} -> {tgt_var}.{rel.target_column}"
                f"  ({getattr(rel, 'relationship_type', getattr(rel, 'rel_type', 'unknown'))}, {rel.confidence:.0%} confidence)"
            )
            lines.append(
                f"    Code: pd.merge({src_var}, {tgt_var}, "
                f"left_on='{rel.source_column}', right_on='{rel.target_column}')"
            )

    return "\n".join(lines)


def _sheet_to_var(workbooks: list[Any], sheet_name: str) -> str:
    """Convert a sheet name to its DataFrame variable name."""
    for wb in workbooks:
        for sheet in wb.sheets:
            if sheet.name == sheet_name:
                return _make_var_name(wb.file_name, sheet.name, len(wb.sheets))
    return f"df_{sheet_name.lower()}"
