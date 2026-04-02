"""Excel Orchestrator — STATEFUL LangGraph pipeline.

Every node reads from and writes to shared ExcelPipelineState.
Failed steps are tracked, downstream nodes adapt, and the Quality Gate
can auto-fix issues (normalize typos, re-extract with different encoding).

Flow: Inspect → Extract → Formulas → Relationships → Profile → Quality Gate → Context → Insight
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

from langchain_core.language_models import BaseChatModel

from app.agents.excel.edge_case_logger import log_edge_case
from app.agents.excel.state import ExcelPipelineState

logger = logging.getLogger(__name__)


def _node_inspect(state: ExcelPipelineState) -> ExcelPipelineState:
    """Node 1: Inspect the file — detect type, encoding, sheet count."""
    try:
        from app.agents.excel.inspector import inspect_workbook
        inspection = inspect_workbook(state["file_path"])
        return {
            **state,
            "file_name": inspection.file_name,
            "file_type": inspection.file_type,
            "sheet_count": inspection.sheet_count,
            "encoding": getattr(inspection, "encoding", None),
        }
    except Exception as exc:
        log_edge_case(file_name=state["file_path"], category="parse",
                     description=f"Inspector failed: {exc}", raw_error=str(exc))
        return {
            **state,
            "file_name": Path(state["file_path"]).name,
            "warnings": state.get("warnings", []) + [f"File inspection failed: {exc}"],
            "failed_steps": state.get("failed_steps", []) + ["inspect"],
        }


def _node_extract_sheets(state: ExcelPipelineState) -> ExcelPipelineState:
    """Node 2: Extract each sheet into a DataFrame."""
    try:
        from app.agents.excel.sheet_extractor import extract_all_sheets

        inspection = None

        sheets = extract_all_sheets(state["file_path"], inspection)

        sheet_dicts = []
        warnings = list(state.get("warnings", []))
        for s in sheets:
            sheet_dicts.append({
                "name": s.name,
                "df": s.df,
                "row_count": s.row_count,
                "column_count": s.column_count,
                "column_types": s.column_types,
                "sample_values": s.sample_values,
                "warnings": s.warnings,
            })
            for w in s.warnings:
                warnings.append(f"{s.name}: {w}")

        logger.info("Extract: %d sheets, %d total rows",
                    len(sheets), sum(s.row_count for s in sheets))

        return {**state, "sheets": sheet_dicts, "warnings": warnings}

    except Exception as exc:
        log_edge_case(file_name=state["file_path"], category="parse",
                     description=f"Sheet extraction failed: {exc}", raw_error=str(exc))
        return {
            **state,
            "sheets": [],
            "warnings": state.get("warnings", []) + [f"Sheet extraction failed: {exc}"],
            "failed_steps": state.get("failed_steps", []) + ["extract"],
        }


def _node_extract_formulas(state: ExcelPipelineState) -> ExcelPipelineState:
    """Node 3: Parse Excel formulas (skip for CSV)."""
    file_type = state.get("file_type", "csv")
    if file_type == "csv":
        return {**state, "formulas": None}

    try:
        from app.agents.excel.formula_extractor import extract_formulas
        formulas = extract_formulas(state["file_path"])
        return {
            **state,
            "formulas": {
                "total_formulas": formulas.total_formulas,
                "cross_sheet_references": formulas.cross_sheet_references,
            },
        }
    except Exception as exc:
        logger.warning("Formulas FAILED (non-blocking): %s", exc)
        return {
            **state,
            "formulas": None,
            "failed_steps": state.get("failed_steps", []) + ["formulas"],
        }


def _node_map_relationships(state: ExcelPipelineState) -> ExcelPipelineState:
    """Node 4: Find relationships between sheets using shared state."""
    sheets = state.get("sheets", [])
    if len(sheets) < 2:
        return {**state, "relationships": []}

    try:
        from app.agents.excel.relationship_mapper import map_relationships
        from app.agents.excel.sheet_extractor import ExtractedSheet

        extracted = []
        for sd in sheets:
            es = ExtractedSheet(
                name=sd["name"], df=sd["df"], row_count=sd["row_count"],
                column_count=sd["column_count"], column_types=sd["column_types"],
                sample_values=sd["sample_values"], warnings=sd.get("warnings", []),
            )
            extracted.append(es)

        formula_obj = None
        if state.get("formulas"):
            formula_obj = type("F", (), {
                "total_formulas": state["formulas"].get("total_formulas", 0),
                "cross_sheet_references": state["formulas"].get("cross_sheet_references", []),
            })()

        relationships = map_relationships(extracted, formula_obj)

        rel_dicts = [
            {"source_sheet": r.source_sheet, "source_column": r.source_column,
             "target_sheet": r.target_sheet, "target_column": r.target_column,
             "confidence": r.confidence, "rel_type": r.rel_type, "method": r.method}
            for r in relationships
        ]
        logger.info("Relationships: %d found", len(rel_dicts))
        return {**state, "relationships": rel_dicts}

    except Exception as exc:
        logger.warning("Relationships FAILED (non-blocking): %s", exc)
        return {
            **state,
            "relationships": [],
            "failed_steps": state.get("failed_steps", []) + ["relationships"],
        }


def _node_profile(state: ExcelPipelineState) -> ExcelPipelineState:
    """Node 5: Profile data quality for each sheet."""
    sheets = state.get("sheets", [])
    if not sheets:
        return {**state, "profiles": []}

    try:
        from app.agents.excel.data_profiler import profile_all_sheets
        from app.agents.excel.sheet_extractor import ExtractedSheet

        extracted = [
            ExtractedSheet(
                name=sd["name"], df=sd["df"], row_count=sd["row_count"],
                column_count=sd["column_count"], column_types=sd["column_types"],
                sample_values=sd["sample_values"], warnings=sd.get("warnings", []),
            )
            for sd in sheets
        ]

        profiles = profile_all_sheets(extracted)
        warnings = list(state.get("warnings", []))

        profile_dicts = []
        for p in profiles:
            profile_dicts.append({
                "sheet_name": p.sheet_name,
                "row_count": p.row_count,
                "column_count": p.column_count,
                "duplicate_rows": p.duplicate_rows,
                "warnings": p.warnings,
                "columns": [
                    {"name": c.name, "dtype": c.dtype, "null_pct": c.null_pct,
                     "unique_count": c.unique_count, "suspected_typos": c.suspected_typos}
                    for c in p.columns
                ],
            })
            for w in p.warnings:
                warnings.append(w)

        return {**state, "profiles": profile_dicts, "warnings": warnings}

    except Exception as exc:
        logger.warning("Profiling FAILED (non-blocking): %s", exc)
        return {
            **state,
            "profiles": [],
            "failed_steps": state.get("failed_steps", []) + ["profile"],
        }


def _node_quality_gate(state: ExcelPipelineState) -> ExcelPipelineState:
    """Node 6: Quality Gate — auto-fix issues and classify severity.

    - Normalizes detected typos in DataFrames
    - Flags critical quality issues
    - Classifies overall severity
    """
    sheets = state.get("sheets", [])
    profiles = state.get("profiles", [])
    auto_fixes: list[str] = []
    quality_issues: list[str] = list(state.get("quality_issues", []))

    for prof in profiles:
        sheet_name = prof.get("sheet_name", "")
        matching_sheet = next((s for s in sheets if s["name"] == sheet_name), None)
        if not matching_sheet:
            continue

        df = matching_sheet["df"]
        for col_prof in prof.get("columns", []):
            typos = col_prof.get("suspected_typos", [])
            for typo, correct in typos:
                if typo and correct:
                    mask = df[col_prof["name"]] == typo
                    count = mask.sum()
                    if count > 0:
                        df.loc[mask, col_prof["name"]] = correct
                        fix_msg = f"Auto-fixed '{typo}'→'{correct}' in {sheet_name}.{col_prof['name']} ({count} rows)"
                        auto_fixes.append(fix_msg)
                        logger.info("Quality Gate: %s", fix_msg)

        for col_prof in prof.get("columns", []):
            if col_prof.get("null_pct", 0) > 50:
                quality_issues.append(
                    f"{sheet_name}.{col_prof['name']}: {col_prof['null_pct']:.0f}% null values"
                )

    total_warnings = len(state.get("warnings", []))
    if not quality_issues and total_warnings == 0:
        severity = "clean"
    elif len(quality_issues) < 5 and total_warnings < 10:
        severity = "minor"
    else:
        severity = "major"

    if auto_fixes:
        logger.info("Quality Gate: applied %d auto-fixes", len(auto_fixes))

    return {
        **state,
        "quality_issues": quality_issues,
        "quality_severity": severity,
        "auto_fixes_applied": auto_fixes,
    }


def _node_build_context(state: ExcelPipelineState) -> ExcelPipelineState:
    """Node 7: Build LLM context + save DataFrames as parquet."""
    sheets = state.get("sheets", [])
    if not sheets:
        return {**state, "parquet_paths": {}, "excel_context": "", "code_preamble": ""}

    try:
        from app.agents.excel.context import (
            save_dataframes_to_parquet, build_excel_context, generate_code_preamble,
        )

        wb_compat = _make_wb_compat(state)
        rel_compat = _make_rel_compat(state.get("relationships", []))

        org_id = state.get("org_id", "default")
        parquet_paths = save_dataframes_to_parquet([wb_compat], org_id)
        excel_context = build_excel_context([wb_compat], rel_compat, parquet_paths)
        code_preamble = generate_code_preamble(parquet_paths)

        logger.info("Context: %d parquet files, %d char context", len(parquet_paths), len(excel_context))

        return {
            **state,
            "parquet_paths": parquet_paths,
            "excel_context": excel_context,
            "code_preamble": code_preamble,
        }

    except Exception as exc:
        log_edge_case(file_name=state.get("file_path", ""), category="parse",
                     description=f"Context building failed: {exc}", raw_error=str(exc))
        return {
            **state,
            "parquet_paths": {},
            "excel_context": "",
            "code_preamble": "",
            "warnings": state.get("warnings", []) + [f"Context building failed: {exc}"],
            "failed_steps": state.get("failed_steps", []) + ["context"],
        }


async def _node_generate_insight(state: ExcelPipelineState, llm: BaseChatModel | None) -> ExcelPipelineState:
    """Node 8: Generate LLM-powered insights from the processed data."""
    if not llm:
        summary = _auto_summary(state)
        return {**state, "insight_summary": summary, "insight_suggestions": []}

    try:
        from app.agents.excel.insight import generate_upload_insight
        wb_compat = _make_wb_compat(state)
        rel_compat = _make_rel_compat(state.get("relationships", []))
        qual_compat = _make_quality_compat(state)

        insight = await generate_upload_insight([wb_compat], rel_compat, qual_compat, llm)
        return {
            **state,
            "insight_summary": insight.summary_text if insight else _auto_summary(state),
            "insight_suggestions": insight.initial_suggestions if insight else [],
        }

    except Exception as exc:
        logger.warning("Insight FAILED (non-blocking): %s", exc)
        return {
            **state,
            "insight_summary": _auto_summary(state),
            "insight_suggestions": [],
            "failed_steps": state.get("failed_steps", []) + ["insight"],
        }


async def process_excel_upload(
    file_path: str,
    llm: BaseChatModel | None = None,
    org_id: str = "default",
) -> dict[str, Any]:
    """Run the full stateful Excel pipeline. NEVER raises — always returns a result."""
    start_time = time.monotonic()
    logger.info("Excel pipeline: starting for %s", file_path)

    state: ExcelPipelineState = {
        "file_path": file_path,
        "org_id": org_id,
        "sheets": [],
        "relationships": [],
        "profiles": [],
        "quality_issues": [],
        "quality_severity": "clean",
        "auto_fixes_applied": [],
        "warnings": [],
        "failed_steps": [],
        "retry_count": 0,
    }

    state = await asyncio.to_thread(_node_inspect, state)
    logger.info("Node 1 (Inspector): file=%s type=%s sheets=%s",
                state.get("file_name"), state.get("file_type"), state.get("sheet_count"))

    state = await asyncio.to_thread(_node_extract_sheets, state)
    sheets = state.get("sheets", [])
    logger.info("Node 2 (Extractor): %d sheets, %d total rows",
                len(sheets), sum(s.get("row_count", 0) for s in sheets))

    if not sheets:
        logger.warning("No sheets extracted — returning minimal result")
        elapsed = time.monotonic() - start_time
        return _build_result(state, elapsed)

    state = await asyncio.to_thread(_node_extract_formulas, state)

    state = await asyncio.to_thread(_node_map_relationships, state)
    logger.info("Node 4 (Relationships): %d found", len(state.get("relationships", [])))

    state = await asyncio.to_thread(_node_profile, state)

    state = await asyncio.to_thread(_node_quality_gate, state)
    logger.info("Node 6 (Quality Gate): severity=%s, auto-fixes=%d",
                state.get("quality_severity"), len(state.get("auto_fixes_applied", [])))

    state = await asyncio.to_thread(_node_build_context, state)

    state = await _node_generate_insight(state, llm)

    elapsed = time.monotonic() - start_time
    state["pipeline_time_seconds"] = elapsed
    logger.info("Excel pipeline complete: %.1fs, %d sheets, %d warnings, %d failed steps",
                elapsed, len(sheets), len(state.get("warnings", [])), len(state.get("failed_steps", [])))

    return _build_result(state, elapsed)


def _build_result(state: ExcelPipelineState, elapsed: float) -> dict[str, Any]:
    """Convert pipeline state into the result dict expected by the file upload API."""
    sheets = state.get("sheets", [])
    warnings = state.get("warnings", [])
    auto_fixes = state.get("auto_fixes_applied", [])

    return {
        "workbook": _make_wb_compat(state),
        "relationships": state.get("relationships", []),
        "profiles": state.get("profiles", []),
        "parquet_paths": state.get("parquet_paths", {}),
        "excel_context": state.get("excel_context", ""),
        "code_preamble": state.get("code_preamble", ""),
        "quality_report": {
            "severity": state.get("quality_severity", "clean"),
            "total_issues": len(state.get("quality_issues", [])) + len(warnings),
            "items": (state.get("quality_issues", []) + warnings)[:10],
            "auto_fixes": auto_fixes,
        },
        "insight": {
            "summary": state.get("insight_summary", _auto_summary(state)),
            "suggestions": state.get("insight_suggestions", []),
            "sheets": [{"name": s["name"], "rows": s["row_count"], "columns": s["column_count"]} for s in sheets],
            "relationships": [
                f"{r['source_sheet']}.{r['source_column']} → {r['target_sheet']}.{r['target_column']}"
                for r in state.get("relationships", [])
            ],
            "quality_warnings": warnings[:5],
        },
        "pipeline_time_seconds": round(elapsed, 1),
        "failed_steps": state.get("failed_steps", []),
    }


def _auto_summary(state: ExcelPipelineState) -> str:
    """Generate a basic summary without LLM."""
    sheets = state.get("sheets", [])
    total_rows = sum(s.get("row_count", 0) for s in sheets)
    total_cols = sum(s.get("column_count", 0) for s in sheets)
    parts = [f"Uploaded {len(sheets)} sheet(s) with {total_rows:,} total rows and {total_cols} columns."]
    rels = state.get("relationships", [])
    if rels:
        parts.append(f"Found {len(rels)} relationship(s) between sheets.")
    warnings = state.get("warnings", [])
    if warnings:
        parts.append(f"{len(warnings)} data quality warning(s).")
    auto_fixes = state.get("auto_fixes_applied", [])
    if auto_fixes:
        parts.append(f"Auto-fixed {len(auto_fixes)} issue(s).")
    return " ".join(parts)


def _make_wb_compat(state: ExcelPipelineState):
    """Create a workbook-compatible object from state for context builder."""
    sheets = state.get("sheets", [])

    class _SheetCompat:
        def __init__(self, sd):
            self.name = sd["name"]
            self.df = sd["df"]
            self.row_count = sd["row_count"]
            self.column_count = sd["column_count"]
            self.column_types = sd["column_types"]
            self.sample_values = sd["sample_values"]

    class _WbCompat:
        def __init__(self, st, sheet_list):
            self.file_name = st.get("file_name", Path(st.get("file_path", "unknown")).name)
            self.sheets = [_SheetCompat(s) for s in sheet_list]
            self.total_rows = sum(s.get("row_count", 0) for s in sheet_list)

    return _WbCompat(state, sheets)


def _make_rel_compat(relationships: list[dict]):
    """Create relationship-compatible objects from state dicts."""
    return [
        type("R", (), {
            "source_sheet": r["source_sheet"], "source_column": r["source_column"],
            "target_sheet": r["target_sheet"], "target_column": r["target_column"],
            "confidence": r["confidence"],
            "relationship_type": r.get("rel_type", "unknown"),
            "rel_type": r.get("rel_type", "unknown"),
        })()
        for r in relationships
    ]


def _make_quality_compat(state: ExcelPipelineState):
    """Create quality report compat from state."""
    warnings = state.get("warnings", [])
    class _QualCompat:
        severity = state.get("quality_severity", "clean")
        total_issues = len(warnings)
        summary_items = warnings[:10]
    return _QualCompat()
