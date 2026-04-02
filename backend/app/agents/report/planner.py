"""Report Planner Agent — analyzes conversation and plans report structure.

Reads all messages, identifies distinct analysis topics, groups them into
sections, and determines which charts/tables to include.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

logger = logging.getLogger(__name__)


@dataclass
class SectionPlan:
    """Plan for one report section."""
    title: str
    description: str
    source_message_indices: list[int]
    has_table: bool = False
    has_chart: bool = False
    key_data_points: list[str] = field(default_factory=list)


@dataclass
class ReportPlan:
    """Complete plan for the report."""
    title: str
    subtitle: str
    sections: list[SectionPlan] = field(default_factory=list)
    executive_summary_points: list[str] = field(default_factory=list)
    recommendation_topics: list[str] = field(default_factory=list)
    total_messages_analyzed: int = 0


_PLAN_PROMPT = """\
You are a report planner. Analyze this conversation between a user and a data analyst AI,
and plan a professional analytical report.

Conversation messages:
{conversation}

Your job:
1. Give the report a professional title based on the data discussed
2. Identify 2-6 distinct analysis topics from the conversation
3. For each topic, create a section with:
   - A clear title
   - Which message numbers contain the relevant data
   - Whether there's table data or chart data
   - Key numbers/findings to highlight
4. List 3-5 points for the executive summary
5. List 2-4 recommendation topics based on the findings

Return JSON:
{{
  "title": "Company Name — Analysis Report",
  "subtitle": "Key findings from data analysis",
  "sections": [
    {{
      "title": "Section Title",
      "description": "What this section covers",
      "source_message_indices": [2, 3],
      "has_table": true,
      "has_chart": true,
      "key_data_points": ["Revenue grew 12%", "$157K MRR"]
    }}
  ],
  "executive_summary_points": ["Point 1", "Point 2"],
  "recommendation_topics": ["Focus on X", "Improve Y"]
}}
"""


async def plan_report(
    messages: list[dict[str, Any]],
    llm: BaseChatModel,
) -> ReportPlan:
    """Analyze conversation messages and produce a report plan."""
    conv_lines: list[str] = []
    for i, msg in enumerate(messages):
        role = "USER" if msg.get("role") == "user" else "ASSISTANT"
        content = msg.get("content", "")[:300]
        has_table = "yes" if msg.get("table_data") else "no"
        has_chart = "yes" if msg.get("plotly_figure") else "no"
        conv_lines.append(
            f"[Message {i}] {role}: {content}\n"
            f"  (table: {has_table}, chart: {has_chart})"
        )

    conversation_text = "\n".join(conv_lines)

    prompt_messages = [
        SystemMessage(content=_PLAN_PROMPT.format(conversation=conversation_text)),
        HumanMessage(content="Plan the report."),
    ]

    try:
        response = await llm.ainvoke(prompt_messages)
        raw: str = response.content.strip()  # type: ignore[union-attr]

        if raw.startswith("```"):
            lines = raw.split("\n")
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            raw = "\n".join(lines).strip()

        data = json.loads(raw)

        plan = ReportPlan(
            title=data.get("title", "Analysis Report"),
            subtitle=data.get("subtitle", ""),
            executive_summary_points=data.get("executive_summary_points", []),
            recommendation_topics=data.get("recommendation_topics", []),
            total_messages_analyzed=len(messages),
        )

        for sec in data.get("sections", []):
            plan.sections.append(SectionPlan(
                title=sec.get("title", "Section"),
                description=sec.get("description", ""),
                source_message_indices=sec.get("source_message_indices", []),
                has_table=sec.get("has_table", False),
                has_chart=sec.get("has_chart", False),
                key_data_points=sec.get("key_data_points", []),
            ))

        if not plan.sections:
            for i, msg in enumerate(messages):
                if msg.get("role") == "assistant" and (msg.get("table_data") or msg.get("plotly_figure")):
                    plan.sections.append(SectionPlan(
                        title=f"Analysis {len(plan.sections) + 1}",
                        description=msg.get("content", "")[:100],
                        source_message_indices=[i],
                        has_table=bool(msg.get("table_data")),
                        has_chart=bool(msg.get("plotly_figure")),
                    ))

        logger.info("Report plan: %d sections, %d summary points", len(plan.sections), len(plan.executive_summary_points))
        return plan

    except Exception as exc:
        logger.warning("Report planning failed: %s", exc)
        return _fallback_plan(messages)


def _fallback_plan(messages: list[dict[str, Any]]) -> ReportPlan:
    """Create a simple plan when LLM fails."""
    plan = ReportPlan(
        title="Data Analysis Report",
        subtitle="Generated from conversation analysis",
        total_messages_analyzed=len(messages),
    )
    for i, msg in enumerate(messages):
        if msg.get("role") == "assistant" and msg.get("content"):
            plan.sections.append(SectionPlan(
                title=f"Finding {len(plan.sections) + 1}",
                description=msg.get("content", "")[:100],
                source_message_indices=[i],
                has_table=bool(msg.get("table_data")),
                has_chart=bool(msg.get("plotly_figure")),
            ))
    return plan
