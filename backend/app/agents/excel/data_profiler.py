"""Data Profiler Agent — profiles each column's data quality.

Single job: given an ExtractedSheet, produce quality metrics per column.
All operations are vectorized pandas — no row-level loops.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from app.agents.excel.sheet_extractor import ExtractedSheet

logger = logging.getLogger(__name__)


@dataclass
class ColumnProfile:
    """Quality profile for one column."""
    name: str
    dtype: str
    null_count: int = 0
    null_pct: float = 0.0
    unique_count: int = 0
    min_val: str | None = None
    max_val: str | None = None
    mean_val: float | None = None
    outlier_count: int = 0
    top_values: list[str] = field(default_factory=list)


@dataclass
class SheetProfile:
    """Quality profile for one sheet."""
    sheet_name: str
    row_count: int = 0
    column_count: int = 0
    duplicate_rows: int = 0
    columns: list[ColumnProfile] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def profile_sheet(sheet: ExtractedSheet) -> SheetProfile:
    """Profile a single sheet's data quality. All vectorized."""
    df = sheet.df
    profile = SheetProfile(
        sheet_name=sheet.name,
        row_count=len(df),
        column_count=len(df.columns),
    )

    if df.empty:
        return profile

    # Duplicate rows
    profile.duplicate_rows = int(df.duplicated().sum())
    if profile.duplicate_rows > 0:
        profile.warnings.append(f"{profile.duplicate_rows} duplicate rows")

    # Per-column profiles
    for col in df.columns:
        cp = _profile_column(df[col], col)
        profile.columns.append(cp)
        if cp.null_pct > 20:
            profile.warnings.append(f"{col}: {cp.null_pct:.0f}% null")

    return profile


def profile_all_sheets(sheets: list[ExtractedSheet]) -> list[SheetProfile]:
    """Profile all sheets."""
    return [profile_sheet(s) for s in sheets]


def _profile_column(series: pd.Series, name: str) -> ColumnProfile:
    """Profile a single column."""
    cp = ColumnProfile(name=name, dtype=str(series.dtype))
    total = len(series)
    if total == 0:
        return cp

    cp.null_count = int(series.isna().sum())
    cp.null_pct = round(cp.null_count / total * 100, 1)
    cp.unique_count = int(series.nunique())

    non_null = series.dropna()
    if non_null.empty:
        return cp

    # Top values
    try:
        cp.top_values = [str(v) for v in non_null.value_counts().head(5).index.tolist()]
    except Exception:
        pass

    # Numeric stats
    if pd.api.types.is_numeric_dtype(non_null):
        try:
            cp.min_val = str(non_null.min())
            cp.max_val = str(non_null.max())
            cp.mean_val = float(non_null.mean())

            # Outlier detection (IQR)
            q1 = non_null.quantile(0.25)
            q3 = non_null.quantile(0.75)
            iqr = q3 - q1
            if iqr > 0:
                cp.outlier_count = int(((non_null < q1 - 1.5 * iqr) | (non_null > q3 + 1.5 * iqr)).sum())
        except Exception:
            pass
    elif pd.api.types.is_datetime64_any_dtype(non_null):
        try:
            cp.min_val = str(non_null.min())
            cp.max_val = str(non_null.max())
        except Exception:
            pass

    return cp
