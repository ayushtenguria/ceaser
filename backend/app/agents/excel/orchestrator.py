# NOTE: Requires DB columns on file_uploads table:
# ALTER TABLE file_uploads ADD COLUMN IF NOT EXISTS excel_context TEXT;
# ALTER TABLE file_uploads ADD COLUMN IF NOT EXISTS code_preamble TEXT;
# ALTER TABLE file_uploads ADD COLUMN IF NOT EXISTS parquet_paths JSONB;
# ALTER TABLE file_uploads ADD COLUMN IF NOT EXISTS excel_metadata JSONB;

"""Excel Orchestrator — runs the full Excel processing pipeline on upload.

Flow: Parse → Discover Relationships → Save Parquet → Quality Scan → Generate Insight
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from langchain_core.language_models import BaseChatModel

from app.agents.excel.parser import parse_workbook, WorkbookResult
from app.agents.excel.relationships import discover_relationships, Relationship
from app.agents.excel.context import (
    save_dataframes_to_parquet,
    build_excel_context,
    generate_code_preamble,
)
from app.agents.excel.quality import run_quality_scan, QualityReport
from app.agents.excel.insight import generate_upload_insight, UploadInsight

logger = logging.getLogger(__name__)


async def process_excel_upload(
    file_path: str,
    llm: BaseChatModel | None = None,
) -> dict[str, Any]:
    """Run the full Excel processing pipeline.

    Returns a dict with all results:
    - workbook: WorkbookResult
    - relationships: list[Relationship]
    - parquet_paths: dict[var_name, path]
    - excel_context: str (for LLM prompt)
    - code_preamble: str (Python imports + reads)
    - quality_report: QualityReport
    - insight: UploadInsight | None
    """
    logger.info("Processing Excel upload: %s", file_path)

    # Step 1: Parse (run in thread — CPU-bound)
    workbook = await asyncio.to_thread(parse_workbook, file_path)
    logger.info("Parsed: %d sheets, %d total rows", len(workbook.sheets), workbook.total_rows)

    # Step 2: Discover relationships
    relationships = await asyncio.to_thread(discover_relationships, workbook.sheets)
    logger.info("Found %d relationships", len(relationships))

    # Step 3: Save as parquet + build context
    parquet_paths = await asyncio.to_thread(save_dataframes_to_parquet, [workbook])
    excel_context = await asyncio.to_thread(
        build_excel_context, [workbook], relationships, parquet_paths
    )
    code_preamble = generate_code_preamble(parquet_paths)

    # Step 4: Quality scan
    quality_report = await asyncio.to_thread(run_quality_scan, workbook.sheets, relationships)
    logger.info("Quality: %s (%d issues)", quality_report.severity, quality_report.total_issues)

    # Step 5: Generate insight (needs LLM)
    insight: UploadInsight | None = None
    if llm:
        try:
            insight = await generate_upload_insight([workbook], relationships, quality_report, llm)
            logger.info("Insight generated: %d suggestions", len(insight.initial_suggestions))
        except Exception as exc:
            logger.warning("Insight generation failed: %s", exc)

    return {
        "workbook": workbook,
        "relationships": [
            {
                "source_sheet": r.source_sheet,
                "source_column": r.source_column,
                "target_sheet": r.target_sheet,
                "target_column": r.target_column,
                "confidence": r.confidence,
                "type": r.relationship_type,
            }
            for r in relationships
        ],
        "parquet_paths": parquet_paths,
        "excel_context": excel_context,
        "code_preamble": code_preamble,
        "quality_report": {
            "severity": quality_report.severity,
            "total_issues": quality_report.total_issues,
            "items": quality_report.summary_items[:10],
        },
        "insight": {
            "summary": insight.summary_text if insight else f"Uploaded {workbook.file_name}: {workbook.total_rows:,} rows across {len(workbook.sheets)} sheets.",
            "suggestions": insight.initial_suggestions if insight else [],
            "sheets": [
                {"name": s.name, "rows": s.row_count, "columns": s.column_count}
                for s in workbook.sheets
            ],
            "relationships": [
                f"{r.source_sheet}.{r.source_column} → {r.target_sheet}.{r.target_column}"
                for r in relationships
            ],
            "quality_warnings": quality_report.summary_items[:5],
        },
    }
