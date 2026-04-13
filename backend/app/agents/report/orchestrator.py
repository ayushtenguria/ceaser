"""Report Orchestrator — runs the full report generation pipeline.

Flow: Load messages -> Plan -> Write -> Enrich -> Save
Streams progress events via SSE.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from typing import Any

from langchain_core.language_models import BaseChatModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agents.report.enricher import enrich_report
from app.agents.report.planner import plan_report
from app.agents.report.writer import write_report
from app.db.models import Message

logger = logging.getLogger(__name__)


async def generate_report_from_conversation(
    conversation_id: str,
    db: AsyncSession,
    llm: BaseChatModel,
) -> AsyncGenerator[dict[str, Any], None]:
    """Generate a report from a conversation, streaming progress events.

    Yields events:
    - {"type": "report_status", "stage": "...", "progress": 0-100}
    - {"type": "report_section", "section": {...}}
    - {"type": "report_complete", "report": {...}}
    """
    import uuid

    yield {"type": "report_status", "stage": "Loading conversation", "progress": 5}

    stmt = (
        select(Message)
        .where(Message.conversation_id == uuid.UUID(conversation_id))
        .order_by(Message.created_at)
    )
    result = await db.execute(stmt)
    db_messages = list(result.scalars().all())

    if not db_messages:
        yield {"type": "report_error", "error": "No messages found in this conversation."}
        return

    messages: list[dict[str, Any]] = []
    for msg in db_messages:
        messages.append(
            {
                "id": str(msg.id),
                "role": msg.role,
                "content": msg.content or "",
                "message_type": msg.message_type,
                "sql_query": msg.sql_query,
                "table_data": msg.table_data,
                "plotly_figure": msg.plotly_figure,
                "error": msg.error,
            }
        )

    yield {"type": "report_status", "stage": f"Analyzing {len(messages)} messages", "progress": 15}

    yield {"type": "report_status", "stage": "Planning report structure", "progress": 25}
    plan = await plan_report(messages, llm)
    yield {
        "type": "report_status",
        "stage": f"Planned {len(plan.sections)} sections",
        "progress": 35,
    }

    total_sections = len(plan.sections) or 1
    for i in range(total_sections):
        pct = 35 + int((i / total_sections) * 40)
        yield {
            "type": "report_status",
            "stage": f"Writing section {i+1}/{total_sections}: {plan.sections[i].title if i < len(plan.sections) else ''}",
            "progress": pct,
        }

    report = await write_report(plan, messages, llm)
    report.conversation_id = conversation_id

    yield {"type": "report_status", "stage": "Writing complete", "progress": 80}

    for section in report.sections:
        yield {
            "type": "report_section",
            "section": {
                "order": section.order,
                "title": section.title,
                "narrative": section.narrative,
                "hasTable": section.table_data is not None,
                "hasChart": section.chart_data is not None,
            },
        }

    yield {"type": "report_status", "stage": "Enriching with additional insights", "progress": 90}
    report = await enrich_report(report, llm)

    yield {"type": "report_status", "stage": "Report complete", "progress": 100}

    yield {
        "type": "report_complete",
        "report": {
            "title": report.title,
            "subtitle": report.subtitle,
            "executiveSummary": report.executive_summary,
            "keyMetrics": report.key_metrics,
            "sections": [
                {
                    "order": s.order,
                    "title": s.title,
                    "narrative": s.narrative,
                    "tableData": s.table_data,
                    "chartData": s.chart_data,
                    "sourceMessageIds": s.source_message_ids,
                    "keyMetrics": s.key_metrics,
                }
                for s in report.sections
            ],
            "recommendations": report.recommendations,
            "conversationId": report.conversation_id,
            "totalMessagesAnalyzed": report.total_messages_analyzed,
        },
    }
