"""Parse uploaded files (CSV / Excel) and generate summaries for the LLM."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

_SUPPORTED_EXTENSIONS: dict[str, str] = {
    ".csv": "csv",
    ".xlsx": "excel",
    ".xls": "excel",
}

_MAX_ROWS_FOR_SUMMARY = 1_000  # Keep lightweight — only need column metadata


def parse_file(file_path: str, file_type: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Read a CSV or Excel file into a DataFrame and extract column metadata.

    Parameters
    ----------
    file_path:
        Absolute path to the file on disk.
    file_type:
        One of ``"csv"``, ``"excel"``.

    Returns
    -------
    tuple[pd.DataFrame, dict]:
        The parsed dataframe and a column-info dict suitable for storing in
        ``FileUpload.column_info``.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    if file_type == "csv":
        df = pd.read_csv(path, nrows=_MAX_ROWS_FOR_SUMMARY)
    elif file_type in ("excel", "xlsx", "xls"):
        df = pd.read_excel(path, nrows=_MAX_ROWS_FOR_SUMMARY)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    column_info = _extract_column_info(df)
    return df, column_info


def _extract_column_info(df: pd.DataFrame) -> dict[str, Any]:
    """Build a JSON-serialisable dict describing each column."""
    columns: list[dict[str, Any]] = []
    for col in df.columns:
        series = df[col]
        col_meta: dict[str, Any] = {
            "name": str(col),
            "dtype": str(series.dtype),
            "null_count": int(series.isna().sum()),
            "unique_count": int(series.nunique()),
        }
        try:
            samples = series.dropna().unique()[:5].tolist()
            safe = []
            for v in samples:
                if hasattr(v, "isoformat"):
                    safe.append(v.isoformat())
                elif hasattr(v, "item"):
                    safe.append(v.item())
                elif isinstance(v, (str, int, float, bool, type(None))):
                    safe.append(v)
                else:
                    safe.append(str(v))
            col_meta["sample_values"] = safe
        except Exception:
            col_meta["sample_values"] = []

        columns.append(col_meta)

    return {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": columns,
    }


def get_file_summary(df: pd.DataFrame) -> str:
    """Generate a concise text summary of a DataFrame for use as LLM context.

    Includes column names, types, null counts, and basic statistics.
    """
    lines: list[str] = [
        "FILE DATA SUMMARY",
        "=" * 50,
        f"Rows: {len(df):,}  |  Columns: {len(df.columns)}",
        "",
        "COLUMNS:",
        "-" * 40,
    ]

    for col in df.columns:
        series = df[col]
        dtype = str(series.dtype)
        nulls = series.isna().sum()
        unique = series.nunique()
        lines.append(f"  {col} ({dtype}) — {unique:,} unique, {nulls:,} nulls")

    numeric_cols = df.select_dtypes(include="number")
    if not numeric_cols.empty:
        lines.append("")
        lines.append("NUMERIC STATISTICS:")
        lines.append("-" * 40)
        desc = numeric_cols.describe().round(2)
        lines.append(desc.to_string())

    lines.append("")
    lines.append("FIRST 5 ROWS:")
    lines.append("-" * 40)
    lines.append(df.head(5).to_string(index=False))

    return "\n".join(lines)
