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
    suspected_typos: list[tuple[str, str]] = field(default_factory=list)


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

    profile.duplicate_rows = int(df.duplicated().sum())
    if profile.duplicate_rows > 0:
        profile.warnings.append(f"{profile.duplicate_rows} duplicate rows")

    for col in df.columns:
        cp = _profile_column(df[col], col)
        profile.columns.append(cp)
        if cp.null_pct > 20:
            profile.warnings.append(f"{col}: {cp.null_pct:.0f}% null")
        for typo, correct in cp.suspected_typos:
            profile.warnings.append(f"{col}: probable typo '{typo}' (did you mean '{correct}'?)")

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

    try:
        cp.top_values = [str(v) for v in non_null.value_counts().head(5).index.tolist()]
    except Exception:
        pass

    if pd.api.types.is_numeric_dtype(non_null):
        try:
            cp.min_val = str(non_null.min())
            cp.max_val = str(non_null.max())
            cp.mean_val = float(non_null.mean())

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

    if non_null.dtype == "object" and cp.unique_count <= 50:
        cp.suspected_typos = _detect_fuzzy_duplicates(non_null)

    return cp


def _detect_fuzzy_duplicates(series: pd.Series) -> list[tuple[str, str]]:
    """Find values that are likely typos of other values using edit distance.

    Returns list of (suspected_typo, likely_correct_value) tuples.
    """
    value_counts = series.value_counts()
    values = [str(v) for v in value_counts.index if isinstance(v, str) and len(str(v)) > 1]

    if len(values) < 2 or len(values) > 100:
        return []

    typos: list[tuple[str, str]] = []

    for i, val_a in enumerate(values):
        for val_b in values[i + 1:]:
            count_a = value_counts.get(val_a, 0)
            count_b = value_counts.get(val_b, 0)
            if min(count_a, count_b) >= max(count_a, count_b) * 0.8:
                continue

            dist = _edit_distance(val_a.lower(), val_b.lower())
            max_len = max(len(val_a), len(val_b))

            threshold = 2 if max_len <= 5 else (3 if max_len <= 10 else max(3, max_len // 4))
            if max_len > 2 and dist <= threshold:
                typo = val_a if count_a < count_b else val_b
                correct = val_b if count_a < count_b else val_a
                typos.append((typo, correct))

    return typos[:10]


def _edit_distance(s1: str, s2: str) -> int:
    """Levenshtein edit distance between two strings."""
    if len(s1) < len(s2):
        return _edit_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)

    prev_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            ins = prev_row[j + 1] + 1
            dele = curr_row[j] + 1
            sub = prev_row[j] + (0 if c1 == c2 else 1)
            curr_row.append(min(ins, dele, sub))
        prev_row = curr_row

    return prev_row[-1]
