"""Report Writer Agent — produces professional narrative from conversation data.

Takes the report plan + original messages and writes a complete report with
executive summary, per-section narrative + data, and recommendations.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from app.agents.report.planner import ReportPlan, SectionPlan

logger = logging.getLogger(__name__)


@dataclass
class ReportSection:
    """A section in the generated report."""
    order: int
    title: str
    narrative: str                        # Markdown text with insights
    table_data: dict[str, Any] | None = None
    chart_data: dict[str, Any] | None = None
    source_message_ids: list[str] = field(default_factory=list)
    key_metrics: list[dict[str, str]] = field(default_factory=list)


@dataclass
class GeneratedReport:
    """Complete generated report."""
    title: str
    subtitle: str
    executive_summary: str
    key_metrics: list[dict[str, str]] = field(default_factory=list)  # [{"label": "MRR", "value": "$157K"}]
    sections: list[ReportSection] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)
    conversation_id: str = ""
    total_messages_analyzed: int = 0


_SUMMARY_PROMPT = """\
You are a senior data analyst writing an executive summary for a report.

Report title: {title}

Key findings from the analysis:
{findings}

Write a 3-5 sentence executive summary that:
1. States the main purpose of the analysis
2. Highlights the most important findings with specific numbers
3. Notes any risks or opportunities discovered
4. Is written in professional business language

Also extract 4-6 key metrics as label-value pairs (e.g., "Total Revenue": "$3.2M").

Return JSON:
{{"summary": "The executive summary text...", "metrics": [{{"label": "Metric Name", "value": "Value"}}]}}
"""

_SECTION_PROMPT = """\
You are a data analyst writing one section of a professional report.

Section title: {title}
Section topic: {description}

Data from the analysis:
{data_context}

Key data points to reference:
{key_points}

Write 2-4 paragraphs of professional narrative that:
1. Explains what was analyzed and why it matters
2. References SPECIFIC numbers from the data (don't be vague)
3. Highlights trends, patterns, or notable findings
4. Provides interpretation — what do the numbers mean for the business?

Return ONLY the markdown text (no JSON wrapping). Use **bold** for key numbers.
"""

_RECOMMENDATIONS_PROMPT = """\
You are a senior data analyst writing actionable recommendations.

Report findings:
{findings}

Recommendation topics to cover:
{topics}

Write 3-5 specific, actionable recommendations that:
1. Are directly backed by data from the analysis
2. Include specific numbers or thresholds
3. Prioritize by impact (most important first)
4. Are practical and achievable

Return a JSON array of recommendation strings.
"""


async def write_report(
    plan: ReportPlan,
    messages: list[dict[str, Any]],
    llm: BaseChatModel,
) -> GeneratedReport:
    """Write the complete report from the plan and message data."""
    report = GeneratedReport(
        title=plan.title,
        subtitle=plan.subtitle,
        executive_summary="",
        total_messages_analyzed=plan.total_messages_analyzed,
    )

    # Collect all findings for summary/recommendations
    all_findings: list[str] = []

    # Step 1: Write each section
    for i, section_plan in enumerate(plan.sections):
        logger.info("Writing section %d/%d: %s", i + 1, len(plan.sections), section_plan.title)

        # Gather data from source messages
        data_context = _build_section_data(section_plan, messages)
        key_points = "\n".join(f"- {p}" for p in section_plan.key_data_points) or "None specified"

        # Write narrative
        try:
            section_msgs = [
                SystemMessage(content=_SECTION_PROMPT.format(
                    title=section_plan.title,
                    description=section_plan.description,
                    data_context=data_context[:3000],
                    key_points=key_points,
                )),
                HumanMessage(content="Write this section."),
            ]
            response = await llm.ainvoke(section_msgs)
            narrative: str = response.content.strip()  # type: ignore[union-attr]
        except Exception as exc:
            logger.warning("Section writing failed: %s", exc)
            narrative = section_plan.description

        # Get table/chart from source messages
        table_data, chart_data, msg_ids = _extract_artifacts(section_plan, messages)

        section = ReportSection(
            order=i,
            title=section_plan.title,
            narrative=narrative,
            table_data=table_data,
            chart_data=chart_data,
            source_message_ids=msg_ids,
        )
        report.sections.append(section)
        all_findings.append(f"{section_plan.title}: {narrative[:200]}")

    # Step 2: Write executive summary
    findings_text = "\n".join(all_findings)
    try:
        summary_msgs = [
            SystemMessage(content=_SUMMARY_PROMPT.format(
                title=plan.title,
                findings=findings_text,
            )),
            HumanMessage(content="Write the executive summary."),
        ]
        response = await llm.ainvoke(summary_msgs)
        raw = response.content.strip()  # type: ignore[union-attr]

        if raw.startswith("```"):
            lines = raw.split("\n")
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            raw = "\n".join(lines).strip()

        summary_data = json.loads(raw)
        report.executive_summary = summary_data.get("summary", "")
        report.key_metrics = summary_data.get("metrics", [])
    except Exception as exc:
        logger.warning("Summary writing failed: %s", exc)
        report.executive_summary = f"This report analyzes {plan.total_messages_analyzed} data points across {len(plan.sections)} areas."

    # Step 3: Write recommendations
    try:
        topics = "\n".join(f"- {t}" for t in plan.recommendation_topics) or "General improvements"
        rec_msgs = [
            SystemMessage(content=_RECOMMENDATIONS_PROMPT.format(
                findings=findings_text,
                topics=topics,
            )),
            HumanMessage(content="Write recommendations."),
        ]
        response = await llm.ainvoke(rec_msgs)
        raw = response.content.strip()  # type: ignore[union-attr]

        if raw.startswith("```"):
            lines = raw.split("\n")
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            raw = "\n".join(lines).strip()

        recs = json.loads(raw)
        if isinstance(recs, list):
            report.recommendations = [str(r) for r in recs[:5]]
    except Exception as exc:
        logger.warning("Recommendations writing failed: %s", exc)
        report.recommendations = ["Review the findings and identify action items."]

    logger.info("Report written: %d sections, %d recommendations", len(report.sections), len(report.recommendations))
    return report


def _build_section_data(plan: SectionPlan, messages: list[dict[str, Any]]) -> str:
    """Build context string from source messages for a section."""
    parts: list[str] = []
    for idx in plan.source_message_indices:
        if 0 <= idx < len(messages):
            msg = messages[idx]
            content = msg.get("content", "")
            parts.append(f"Message {idx} ({msg.get('role', 'unknown')}): {content[:500]}")

            if msg.get("sql_query"):
                parts.append(f"SQL used: {msg['sql_query'][:200]}")

            if msg.get("table_data"):
                table = msg["table_data"]
                cols = table.get("columns", [])
                rows = table.get("rows", [])
                parts.append(f"Table: {len(rows)} rows, columns: {', '.join(str(c) for c in cols)}")
                if rows:
                    preview = json.dumps(rows[:5], default=str)
                    parts.append(f"Data preview: {preview[:500]}")

    return "\n".join(parts) if parts else "No specific data available for this section."


def _extract_artifacts(
    plan: SectionPlan,
    messages: list[dict[str, Any]],
) -> tuple[dict | None, dict | None, list[str]]:
    """Extract the best table and chart from source messages."""
    table_data = None
    chart_data = None
    msg_ids: list[str] = []

    for idx in plan.source_message_indices:
        if 0 <= idx < len(messages):
            msg = messages[idx]
            msg_ids.append(str(msg.get("id", "")))

            if msg.get("table_data") and table_data is None:
                table_data = msg["table_data"]
            if msg.get("plotly_figure") and chart_data is None:
                chart_data = msg["plotly_figure"]

    return table_data, chart_data, msg_ids
