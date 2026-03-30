"""Shared state for the stateful Excel processing pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TypedDict

import pandas as pd


class ExcelPipelineState(TypedDict, total=False):
    """Shared state flowing through the Excel LangGraph pipeline.

    Every node reads from and writes to this state. Downstream nodes
    can adapt their behavior based on what upstream nodes discovered.
    """

    # ── Input ─────────────────────────────────────────────────────
    file_path: str
    org_id: str

    # ── Inspector output ──────────────────────────────────────────
    file_name: str
    file_type: str  # "csv", "xlsx", "xls"
    sheet_count: int
    encoding: str | None
    file_size_bytes: int

    # ── Sheet Extractor output ────────────────────────────────────
    sheets: list[dict[str, Any]]
    # Each sheet: {"name": str, "df": DataFrame, "row_count": int,
    #              "column_count": int, "column_types": dict, "sample_values": dict,
    #              "warnings": list}

    # ── Formula Extractor output ──────────────────────────────────
    formulas: dict[str, Any] | None
    # {"total_formulas": int, "cross_sheet_references": list, "formula_cells": dict}

    # ── Relationship Mapper output ────────────────────────────────
    relationships: list[dict[str, Any]]
    # Each: {"source_sheet": str, "source_column": str, "target_sheet": str,
    #        "target_column": str, "confidence": float, "rel_type": str}

    # ── Data Profiler output ──────────────────────────────────────
    profiles: list[dict[str, Any]]
    # Each: {"sheet_name": str, "row_count": int, "column_count": int,
    #        "duplicate_rows": int, "columns": list[ColumnProfile], "warnings": list}

    # ── Quality Gate output ───────────────────────────────────────
    quality_issues: list[str]
    quality_severity: str  # "clean", "minor", "major"
    auto_fixes_applied: list[str]  # "normalized 'opne'→'open' in status column"

    # ── Context Builder output ────────────────────────────────────
    parquet_paths: dict[str, str]  # var_name → remote storage path
    excel_context: str  # LLM-ready text description
    code_preamble: str  # Python import code with ceaser:// refs

    # ── Insight Generator output ──────────────────────────────────
    insight_summary: str
    insight_suggestions: list[str]

    # ── Pipeline control ──────────────────────────────────────────
    warnings: list[str]  # accumulated warnings across all steps
    failed_steps: list[str]  # names of steps that failed
    retry_count: int
    pipeline_time_seconds: float
