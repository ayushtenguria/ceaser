"""Data Quality Agent — scans Excel data for issues before user queries.

All checks are vectorized pandas operations — no Python loops over rows.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from app.agents.excel.parser import SheetResult
from app.agents.excel.relationships import Relationship

logger = logging.getLogger(__name__)


@dataclass
class QualityReport:
    """Quality scan results."""
    total_issues: int = 0
    severity: str = "clean"        # clean, minor, major
    duplicate_rows: dict[str, int] = field(default_factory=dict)
    null_columns: dict[str, dict[str, int]] = field(default_factory=dict)
    type_issues: list[str] = field(default_factory=list)
    outliers: dict[str, dict[str, int]] = field(default_factory=dict)
    orphan_records: dict[str, int] = field(default_factory=dict)
    summary_items: list[str] = field(default_factory=list)


def run_quality_scan(
    sheets: list[SheetResult],
    relationships: list[Relationship] | None = None,
) -> QualityReport:
    """Run all quality checks across sheets. Returns a QualityReport."""
    report = QualityReport()

    for sheet in sheets:
        df = sheet.df
        name = sheet.name

        # Duplicates
        dup_count = int(df.duplicated().sum())
        if dup_count > 0:
            report.duplicate_rows[name] = dup_count
            report.summary_items.append(f"{dup_count} duplicate rows in {name}")

        # Nulls
        null_cols: dict[str, int] = {}
        for col in df.columns:
            null_count = int(df[col].isna().sum())
            if null_count > 0:
                pct = null_count / len(df) * 100
                if pct > 5:  # Only flag >5% null
                    null_cols[col] = null_count
                    report.summary_items.append(f"{name}.{col}: {pct:.0f}% null ({null_count} rows)")
        if null_cols:
            report.null_columns[name] = null_cols

        # Outliers in numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        sheet_outliers: dict[str, int] = {}
        for col in numeric_cols:
            series = df[col].dropna()
            if len(series) < 10:
                continue
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1
            if iqr == 0:
                continue
            outlier_count = int(((series < q1 - 1.5 * iqr) | (series > q3 + 1.5 * iqr)).sum())
            if outlier_count > 0:
                sheet_outliers[col] = outlier_count
        if sheet_outliers:
            report.outliers[name] = sheet_outliers

    # Orphan records
    if relationships:
        for rel in relationships:
            src_sheet = next((s for s in sheets if s.name == rel.source_sheet), None)
            tgt_sheet = next((s for s in sheets if s.name == rel.target_sheet), None)
            if src_sheet is not None and tgt_sheet is not None:
                src_vals = set(src_sheet.df[rel.source_column].dropna().astype(str))
                tgt_vals = set(tgt_sheet.df[rel.target_column].dropna().astype(str))
                orphans = len(src_vals - tgt_vals)
                if orphans > 0:
                    report.orphan_records[f"{rel.source_sheet}.{rel.source_column}"] = orphans
                    report.summary_items.append(
                        f"{orphans} orphan records: {rel.source_sheet}.{rel.source_column} "
                        f"references missing in {rel.target_sheet}.{rel.target_column}"
                    )

    report.total_issues = (
        sum(report.duplicate_rows.values())
        + sum(sum(v.values()) for v in report.null_columns.values())
        + len(report.orphan_records)
    )

    if report.total_issues == 0:
        report.severity = "clean"
    elif report.total_issues < 100:
        report.severity = "minor"
    else:
        report.severity = "major"

    return report
