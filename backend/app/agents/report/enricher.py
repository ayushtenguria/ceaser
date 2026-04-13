"""Report Enricher Agent — fills gaps and adds summary metrics.

Optionally runs additional queries to provide context that wasn't
explicitly asked during the conversation but is relevant to the report.
"""

from __future__ import annotations

import json
import logging

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from app.agents.report.writer import GeneratedReport

logger = logging.getLogger(__name__)

_ENRICH_PROMPT = """\
You are a report reviewer. Review this report and suggest improvements.

Report title: {title}
Executive summary: {summary}
Sections: {section_titles}
Current recommendations: {recommendations}

Are there any:
1. Missing key metrics that should be in the summary?
2. Sections that could use better titles?
3. Additional recommendations based on the data?
4. An improved executive summary?

Return JSON:
{{
  "improved_summary": "Better executive summary or null if current is fine",
  "additional_metrics": [{{"label": "Metric", "value": "Value"}}],
  "additional_recommendations": ["New recommendation"],
  "section_improvements": {{"section_index": "suggested improvement"}}
}}
"""


async def enrich_report(
    report: GeneratedReport,
    llm: BaseChatModel,
) -> GeneratedReport:
    """Review and enrich the report with additional insights."""
    try:
        section_titles = ", ".join(s.title for s in report.sections)
        recommendations = "\n".join(f"- {r}" for r in report.recommendations)

        messages = [
            SystemMessage(
                content=_ENRICH_PROMPT.format(
                    title=report.title,
                    summary=report.executive_summary[:500],
                    section_titles=section_titles,
                    recommendations=recommendations,
                )
            ),
            HumanMessage(content="Review and enrich."),
        ]

        response = await llm.ainvoke(messages)
        raw: str = response.content.strip()  # type: ignore[union-attr]

        if raw.startswith("```"):
            lines = raw.split("\n")
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            raw = "\n".join(lines).strip()

        data = json.loads(raw)

        if data.get("improved_summary"):
            report.executive_summary = data["improved_summary"]

        if data.get("additional_metrics"):
            for m in data["additional_metrics"]:
                if isinstance(m, dict) and "label" in m and "value" in m:
                    existing_labels = {em["label"].lower() for em in report.key_metrics}
                    if m["label"].lower() not in existing_labels:
                        report.key_metrics.append(m)

        if data.get("additional_recommendations"):
            for r in data["additional_recommendations"]:
                if isinstance(r, str) and r not in report.recommendations:
                    report.recommendations.append(r)

        logger.info(
            "Report enriched: %d metrics, %d recommendations",
            len(report.key_metrics),
            len(report.recommendations),
        )

    except Exception as exc:
        logger.warning("Report enrichment failed (using original): %s", exc)

    return report
