"""Excel Intelligence Engine — specialized agents for Excel file analysis."""

from app.agents.excel.parser import parse_workbook, WorkbookResult, SheetResult
from app.agents.excel.relationships import discover_relationships, Relationship
from app.agents.excel.context import build_excel_context, save_dataframes_to_parquet
from app.agents.excel.quality import run_quality_scan, QualityReport
from app.agents.excel.insight import generate_upload_insight, UploadInsight

__all__ = [
    "parse_workbook", "WorkbookResult", "SheetResult",
    "discover_relationships", "Relationship",
    "build_excel_context", "save_dataframes_to_parquet",
    "run_quality_scan", "QualityReport",
    "generate_upload_insight", "UploadInsight",
]
