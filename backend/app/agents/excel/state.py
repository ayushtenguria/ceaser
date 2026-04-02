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

    file_path: str
    org_id: str

    file_name: str
    file_type: str
    sheet_count: int
    encoding: str | None
    file_size_bytes: int

    sheets: list[dict[str, Any]]

    formulas: dict[str, Any] | None

    relationships: list[dict[str, Any]]

    profiles: list[dict[str, Any]]

    quality_issues: list[str]
    quality_severity: str
    auto_fixes_applied: list[str]

    parquet_paths: dict[str, str]
    excel_context: str
    code_preamble: str

    insight_summary: str
    insight_suggestions: list[str]

    warnings: list[str]
    failed_steps: list[str]
    retry_count: int
    pipeline_time_seconds: float
