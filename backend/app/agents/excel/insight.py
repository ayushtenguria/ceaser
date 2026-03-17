"""Excel Insight Agent — generates upload summary and initial suggestions."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from app.agents.excel.parser import WorkbookResult
from app.agents.excel.quality import QualityReport
from app.agents.excel.relationships import Relationship

logger = logging.getLogger(__name__)


@dataclass
class UploadInsight:
    """Summary generated after Excel upload."""
    file_count: int = 0
    total_sheets: int = 0
    total_rows: int = 0
    total_columns: int = 0
    data_type: str = ""
    sheet_summaries: list[str] = field(default_factory=list)
    relationships_found: list[str] = field(default_factory=list)
    quality_warnings: list[str] = field(default_factory=list)
    initial_suggestions: list[str] = field(default_factory=list)
    summary_text: str = ""


_INSIGHT_PROMPT = """\
You are a data analyst. A user just uploaded Excel file(s). Based on the data summary below,
write a brief, friendly welcome message (3-4 sentences) that:
1. Confirms what was uploaded (file name, sheets, row counts)
2. Highlights the most interesting data relationships found
3. Mentions any data quality issues briefly
4. Suggests what analysis they could start with

Also generate 6 specific questions they could ask about THIS data.

Data summary:
{summary}

Quality issues:
{quality}

Respond as JSON:
{{"summary": "your welcome message", "suggestions": ["question1", "question2", ...]}}
"""


async def generate_upload_insight(
    workbooks: list[WorkbookResult],
    relationships: list[Relationship],
    quality_report: QualityReport,
    llm: BaseChatModel,
) -> UploadInsight:
    """Generate a complete insight summary after file upload."""
    insight = UploadInsight(
        file_count=len(workbooks),
        total_sheets=sum(len(wb.sheets) for wb in workbooks),
        total_rows=sum(wb.total_rows for wb in workbooks),
    )

    # Build sheet summaries
    for wb in workbooks:
        for sheet in wb.sheets:
            insight.sheet_summaries.append(
                f"{sheet.name}: {sheet.row_count:,} rows, {sheet.column_count} columns"
            )

    # Relationship summaries
    for rel in relationships:
        insight.relationships_found.append(
            f"{rel.source_sheet}.{rel.source_column} -> {rel.target_sheet}.{rel.target_column}"
        )

    # Quality warnings
    insight.quality_warnings = quality_report.summary_items[:5]

    # LLM summary
    summary_text = "\n".join([
        f"Files: {', '.join(wb.file_name for wb in workbooks)}",
        f"Sheets: {', '.join(insight.sheet_summaries)}",
        f"Relationships: {', '.join(insight.relationships_found) or 'None found'}",
    ])

    quality_text = "\n".join(quality_report.summary_items[:5]) or "No issues found"

    try:
        import json
        messages = [
            SystemMessage(content=_INSIGHT_PROMPT.format(summary=summary_text, quality=quality_text)),
            HumanMessage(content="Generate the insight."),
        ]
        response = await llm.ainvoke(messages)
        raw = response.content.strip()  # type: ignore[union-attr]

        if raw.startswith("```"):
            lines = raw.split("\n")
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            raw = "\n".join(lines).strip()

        data = json.loads(raw)
        insight.summary_text = data.get("summary", "")
        insight.initial_suggestions = data.get("suggestions", [])[:6]
    except Exception as exc:
        logger.warning("Insight generation failed: %s", exc)
        insight.summary_text = (
            f"Uploaded {insight.file_count} file(s) with {insight.total_sheets} sheets "
            f"and {insight.total_rows:,} total rows."
        )

    return insight
