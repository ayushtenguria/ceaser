"""Excel Intelligence Engine — 7 specialized microservice agents.

Agent 1: inspector      — workbook metadata inspection
Agent 2: sheet_extractor — per-sheet DataFrame extraction
Agent 3: formula_extractor — formula parsing and cross-sheet refs
Agent 4: relationship_mapper — FK-like relationship discovery
Agent 5: data_profiler  — per-column data quality profiling
Agent 6: context        — LLM context builder + parquet storage (existing)
Agent 7: insight        — LLM-powered summary generator (existing)

Orchestrator wires them all together.
"""

from app.agents.excel.context import build_excel_context, save_dataframes_to_parquet
from app.agents.excel.data_profiler import SheetProfile, profile_all_sheets, profile_sheet
from app.agents.excel.formula_extractor import FormulaExtractionResult, extract_formulas
from app.agents.excel.insight import UploadInsight, generate_upload_insight
from app.agents.excel.inspector import SheetInfo, WorkbookInspection, inspect_workbook
from app.agents.excel.relationship_mapper import Relationship, map_relationships
from app.agents.excel.sheet_extractor import ExtractedSheet, extract_all_sheets, extract_sheet

__all__ = [
    "inspect_workbook",
    "WorkbookInspection",
    "SheetInfo",
    "extract_sheet",
    "extract_all_sheets",
    "ExtractedSheet",
    "extract_formulas",
    "FormulaExtractionResult",
    "map_relationships",
    "Relationship",
    "profile_sheet",
    "profile_all_sheets",
    "SheetProfile",
    "build_excel_context",
    "save_dataframes_to_parquet",
    "generate_upload_insight",
    "UploadInsight",
]
