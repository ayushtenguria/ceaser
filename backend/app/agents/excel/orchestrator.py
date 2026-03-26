# NOTE: Requires DB columns on file_uploads table:
# ALTER TABLE file_uploads ADD COLUMN IF NOT EXISTS excel_context TEXT;
# ALTER TABLE file_uploads ADD COLUMN IF NOT EXISTS code_preamble TEXT;
# ALTER TABLE file_uploads ADD COLUMN IF NOT EXISTS parquet_paths JSONB;
# ALTER TABLE file_uploads ADD COLUMN IF NOT EXISTS excel_metadata JSONB;

"""Excel Orchestrator — wires all 7 agents into a DEFENSIVE pipeline.

NEVER fails completely. If any agent crashes, the pipeline continues
with whatever data it has. Edge cases are logged for future review.

Flow: Inspect → Extract sheets → Extract formulas → Map relationships →
      Profile data → Build context → Generate insight
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from langchain_core.language_models import BaseChatModel

from app.agents.excel.edge_case_logger import log_edge_case

logger = logging.getLogger(__name__)


async def process_excel_upload(
    file_path: str,
    llm: BaseChatModel | None = None,
    org_id: str = "default",
) -> dict[str, Any]:
    """Run the full 7-agent Excel pipeline. NEVER raises — always returns a result."""
    start_time = time.monotonic()
    logger.info("Excel pipeline: starting for %s", file_path)

    pipeline_warnings: list[str] = []

    # ── Agent 1: Inspector ──────────────────────────────────────────
    inspection = None
    try:
        from app.agents.excel.inspector import inspect_workbook
        inspection = await asyncio.to_thread(inspect_workbook, file_path)
        logger.info("Agent 1 (Inspector): %d sheets, type=%s", inspection.sheet_count, inspection.file_type)
    except Exception as exc:
        log_edge_case(file_name=file_path, category="parse",
                     description=f"Inspector failed: {exc}", raw_error=str(exc))
        pipeline_warnings.append(f"File inspection failed: {exc}")
        logger.warning("Agent 1 (Inspector) FAILED: %s", exc)

    # ── Agent 2: Extract sheets ─────────────────────────────────────
    sheets = []
    try:
        from app.agents.excel.sheet_extractor import extract_all_sheets
        sheets = await asyncio.to_thread(extract_all_sheets, file_path, inspection)
        logger.info("Agent 2 (Extractor): %d sheets extracted, %d total rows",
                    len(sheets), sum(s.row_count for s in sheets))
        # Collect per-sheet warnings
        for s in sheets:
            for w in s.warnings:
                pipeline_warnings.append(f"{s.name}: {w}")
    except Exception as exc:
        log_edge_case(file_name=file_path, category="parse",
                     description=f"Sheet extraction failed: {exc}", raw_error=str(exc))
        pipeline_warnings.append(f"Sheet extraction failed: {exc}")
        logger.warning("Agent 2 (Extractor) FAILED: %s", exc)

    if not sheets:
        logger.warning("No sheets extracted — returning minimal result")
        return _empty_result(file_path, pipeline_warnings)

    # ── Agent 3: Extract formulas ───────────────────────────────────
    formulas = None
    try:
        from app.agents.excel.formula_extractor import extract_formulas
        formulas = await asyncio.to_thread(extract_formulas, file_path)
        logger.info("Agent 3 (Formulas): %d formulas, %d cross-sheet refs",
                    formulas.total_formulas, len(formulas.cross_sheet_references))
    except Exception as exc:
        log_edge_case(file_name=file_path, category="parse",
                     description=f"Formula extraction failed: {exc}", raw_error=str(exc))
        logger.warning("Agent 3 (Formulas) FAILED (non-blocking): %s", exc)

    # ── Agent 4: Map relationships ──────────────────────────────────
    relationships = []
    try:
        from app.agents.excel.relationship_mapper import map_relationships
        relationships = await asyncio.to_thread(map_relationships, sheets, formulas)
        logger.info("Agent 4 (Relationships): %d relationships", len(relationships))
    except Exception as exc:
        log_edge_case(file_name=file_path, category="parse",
                     description=f"Relationship mapping failed: {exc}", raw_error=str(exc))
        logger.warning("Agent 4 (Relationships) FAILED (non-blocking): %s", exc)

    # ── Agent 5: Profile data quality ───────────────────────────────
    profiles = []
    try:
        from app.agents.excel.data_profiler import profile_all_sheets
        profiles = await asyncio.to_thread(profile_all_sheets, sheets)
        total_warnings = sum(len(p.warnings) for p in profiles)
        logger.info("Agent 5 (Profiler): %d warnings", total_warnings)
        for p in profiles:
            pipeline_warnings.extend(p.warnings)
    except Exception as exc:
        log_edge_case(file_name=file_path, category="parse",
                     description=f"Data profiling failed: {exc}", raw_error=str(exc))
        logger.warning("Agent 5 (Profiler) FAILED (non-blocking): %s", exc)

    # ── Agent 6: Build context + save parquet ───────────────────────
    parquet_paths: dict[str, str] = {}
    excel_context = ""
    code_preamble = ""
    try:
        wb_compat = _make_wb_compat(inspection, sheets, file_path)
        rel_compat = _make_rel_compat(relationships)

        from app.agents.excel.context import save_dataframes_to_parquet, build_excel_context, generate_code_preamble
        parquet_paths = await asyncio.to_thread(save_dataframes_to_parquet, [wb_compat], org_id)
        excel_context = await asyncio.to_thread(build_excel_context, [wb_compat], rel_compat, parquet_paths)
        code_preamble = generate_code_preamble(parquet_paths)
        logger.info("Agent 6 (Context): %d parquet files, %d char context", len(parquet_paths), len(excel_context))
    except Exception as exc:
        log_edge_case(file_name=file_path, category="parse",
                     description=f"Context building failed: {exc}", raw_error=str(exc))
        pipeline_warnings.append(f"Context building failed: {exc}")
        logger.warning("Agent 6 (Context) FAILED: %s", exc)

    # ── Agent 7: Generate insight (LLM) ─────────────────────────────
    insight = None
    if llm:
        try:
            wb_compat = _make_wb_compat(inspection, sheets, file_path)
            rel_compat = _make_rel_compat(relationships)
            qual_compat = _make_quality_compat(profiles, pipeline_warnings)

            from app.agents.excel.insight import generate_upload_insight
            insight = await generate_upload_insight([wb_compat], rel_compat, qual_compat, llm)
            logger.info("Agent 7 (Insight): generated")
        except Exception as exc:
            log_edge_case(file_name=file_path, category="parse",
                         description=f"Insight generation failed: {exc}", raw_error=str(exc))
            logger.warning("Agent 7 (Insight) FAILED (non-blocking): %s", exc)

            # Log novel edge case with LLM for future analysis
            try:
                from app.agents.excel.edge_case_logger import describe_edge_case_with_llm
                await describe_edge_case_with_llm(str(exc), file_path, "", {}, llm)
            except Exception:
                pass

    elapsed = time.monotonic() - start_time
    logger.info("Excel pipeline complete: %.1fs, %d sheets, %d rows, %d warnings",
                elapsed, len(sheets), sum(s.row_count for s in sheets), len(pipeline_warnings))

    return {
        "workbook": _make_wb_compat(inspection, sheets, file_path),
        "relationships": [
            {
                "source_sheet": r.source_sheet, "source_column": r.source_column,
                "target_sheet": r.target_sheet, "target_column": r.target_column,
                "confidence": r.confidence, "type": r.rel_type, "method": r.method,
            }
            for r in relationships
        ],
        "profiles": [
            {
                "sheet": p.sheet_name, "rows": p.row_count, "columns": p.column_count,
                "duplicates": p.duplicate_rows, "warnings": p.warnings,
            }
            for p in profiles
        ],
        "parquet_paths": parquet_paths,
        "excel_context": excel_context,
        "code_preamble": code_preamble,
        "quality_report": {
            "severity": "clean" if not pipeline_warnings else "minor" if len(pipeline_warnings) < 10 else "major",
            "total_issues": len(pipeline_warnings),
            "items": pipeline_warnings[:10],
        },
        "insight": {
            "summary": insight.summary_text if insight else _auto_summary(sheets, relationships, pipeline_warnings),
            "suggestions": insight.initial_suggestions if insight else [],
            "sheets": [{"name": s.name, "rows": s.row_count, "columns": s.column_count} for s in sheets],
            "relationships": [f"{r.source_sheet}.{r.source_column} → {r.target_sheet}.{r.target_column}" for r in relationships],
            "quality_warnings": pipeline_warnings[:5],
        },
        "pipeline_time_seconds": round(elapsed, 1),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _empty_result(file_path: str, warnings: list[str]) -> dict[str, Any]:
    """Return a minimal result when no data could be extracted."""
    from pathlib import Path
    return {
        "workbook": type("W", (), {"file_name": Path(file_path).name, "sheets": [], "total_rows": 0})(),
        "relationships": [],
        "profiles": [],
        "parquet_paths": {},
        "excel_context": "",
        "code_preamble": "",
        "quality_report": {"severity": "major", "total_issues": len(warnings), "items": warnings},
        "insight": {
            "summary": f"Could not extract data from file. Issues: {'; '.join(warnings[:3])}",
            "suggestions": [],
            "sheets": [],
            "relationships": [],
            "quality_warnings": warnings[:5],
        },
        "pipeline_time_seconds": 0,
    }


def _auto_summary(sheets, relationships, warnings) -> str:
    """Generate a basic summary without LLM."""
    total_rows = sum(s.row_count for s in sheets)
    total_cols = sum(s.column_count for s in sheets)
    parts = [f"Uploaded {len(sheets)} sheet(s) with {total_rows:,} total rows and {total_cols} columns."]
    if relationships:
        parts.append(f"Found {len(relationships)} relationship(s) between sheets.")
    if warnings:
        parts.append(f"{len(warnings)} data quality warning(s).")
    return " ".join(parts)


def _make_wb_compat(inspection, sheets, file_path):
    """Create a compatible workbook object for context builder."""
    from pathlib import Path

    class _SheetCompat:
        def __init__(self, es):
            self.name = es.name
            self.df = es.df
            self.row_count = es.row_count
            self.column_count = es.column_count
            self.column_types = es.column_types
            self.sample_values = es.sample_values

    class _WbCompat:
        def __init__(self, insp, extracted, fpath):
            self.file_name = insp.file_name if insp else Path(fpath).name
            self.sheets = [_SheetCompat(s) for s in extracted]
            self.total_rows = sum(s.row_count for s in extracted)

    return _WbCompat(inspection, sheets, file_path)


def _make_rel_compat(relationships):
    """Create compatible relationship objects."""
    return [
        type("R", (), {
            "source_sheet": r.source_sheet, "source_column": r.source_column,
            "target_sheet": r.target_sheet, "target_column": r.target_column,
            "confidence": r.confidence,
            "relationship_type": r.rel_type,
            "rel_type": r.rel_type,
        })()
        for r in relationships
    ]


def _make_quality_compat(profiles, warnings):
    """Create compatible quality report object."""
    class _QualCompat:
        severity = "clean" if not warnings else "minor" if len(warnings) < 10 else "major"
        total_issues = len(warnings)
        summary_items = warnings[:10]
    return _QualCompat()
