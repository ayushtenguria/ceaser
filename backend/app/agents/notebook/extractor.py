"""Notebook Extractor Agent — extracts a reusable notebook skeleton from a conversation.

Analyzes the conversation messages, identifies the analysis steps (prompts + queries),
and creates a notebook with:
1. A file/connection cell at the top (for swapping data source)
2. Prompt cells for each analysis step (preserving the working query patterns)
3. Text cells for section headers
"""

from __future__ import annotations

import json
import logging
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

logger = logging.getLogger(__name__)

_EXTRACT_PROMPT = """\
You are a notebook designer. Analyze this conversation between a user and a data analyst AI.
Extract the REUSABLE analysis steps — the questions the user asked that produced useful results.

Conversation:
{conversation}

Your job:
1. Identify each distinct analysis step (user question that got a meaningful answer)
2. For each step, write a GENERIC prompt that would work with any similar dataset
   - Replace specific table/column names with generic references where possible
   - But keep the analytical intent clear
3. Group related steps under section headers
4. Add a file upload cell at the very beginning
5. Skip greetings, small talk, failed queries, and clarification messages

Return a JSON array of cells:
[
  {{"cell_type": "file", "content": "Upload your data file", "config": {{"accepted_types": [".xlsx", ".csv"], "description": "Upload the dataset to analyze"}}, "output_variable": ""}},
  {{"cell_type": "text", "content": "# Section Title", "config": null, "output_variable": ""}},
  {{"cell_type": "prompt", "content": "The analysis prompt here", "config": null, "output_variable": "result_1"}},
  ...
]

Rules:
- 1 file cell at the top (always)
- Group with text cells as section headers
- Each prompt should be self-contained and reusable
- Keep the original analytical intent but make it work with new data
- Include 3-8 prompt cells (skip redundant/failed queries)
- If a query produced a chart, mention "plot" or "chart" in the prompt
- Set output_variable for prompts that later steps reference
"""


async def extract_notebook_from_conversation(
    messages: list[dict[str, Any]],
    llm: BaseChatModel,
    notebook_name: str = "",
) -> dict[str, Any]:
    """Extract a reusable notebook skeleton from conversation messages.

    Returns:
        dict with "name", "description", "cells" (list of cell dicts)
    """
    # Build conversation summary — focus on user questions and assistant results
    conv_lines: list[str] = []
    analysis_count = 0

    for i, msg in enumerate(messages):
        role = msg.get("role", "unknown")
        content = msg.get("content", "")[:400]
        has_table = bool(msg.get("table_data"))
        has_chart = bool(msg.get("plotly_figure"))
        sql = msg.get("sql_query", "")

        if role == "user":
            conv_lines.append(f"[{i}] USER: {content}")
        elif role == "assistant":
            extras = []
            if has_table:
                extras.append("produced table")
            if has_chart:
                extras.append("produced chart")
            if sql:
                extras.append(f"SQL: {sql[:150]}")
            extra_str = f" ({', '.join(extras)})" if extras else ""
            conv_lines.append(f"[{i}] ASSISTANT: {content[:200]}{extra_str}")
            if has_table or has_chart:
                analysis_count += 1

    conversation_text = "\n".join(conv_lines)

    # Generate notebook name if not provided
    if not notebook_name:
        # Use first user message as name hint
        for msg in messages:
            if msg.get("role") == "user" and msg.get("content"):
                notebook_name = msg["content"][:60]
                break
        notebook_name = notebook_name or "Analysis Notebook"

    try:
        prompt_messages = [
            SystemMessage(content=_EXTRACT_PROMPT.format(conversation=conversation_text)),
            HumanMessage(content="Extract the notebook."),
        ]

        response = await llm.ainvoke(prompt_messages)
        raw: str = response.content.strip()  # type: ignore[union-attr]

        # Strip markdown fences
        if raw.startswith("```"):
            lines = raw.split("\n")
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            raw = "\n".join(lines).strip()

        cells = json.loads(raw)

        if not isinstance(cells, list) or len(cells) < 2:
            cells = _fallback_extraction(messages)

        # Validate cells
        valid_types = {"text", "file", "input", "prompt", "code"}
        cleaned_cells: list[dict[str, Any]] = []
        for cell in cells:
            if not isinstance(cell, dict):
                continue
            ct = cell.get("cell_type", "")
            if ct not in valid_types:
                continue
            cleaned_cells.append({
                "cell_type": ct,
                "content": cell.get("content", ""),
                "config": cell.get("config"),
                "output_variable": cell.get("output_variable", ""),
            })

        if not cleaned_cells:
            cleaned_cells = _fallback_extraction(messages)

        logger.info("Extracted notebook: %d cells from %d messages (%d analyses)",
                     len(cleaned_cells), len(messages), analysis_count)

        return {
            "name": notebook_name,
            "description": f"Extracted from conversation with {len(messages)} messages and {analysis_count} analyses",
            "cells": cleaned_cells,
        }

    except Exception as exc:
        logger.warning("Notebook extraction failed: %s", exc)
        return {
            "name": notebook_name,
            "description": "Auto-extracted analysis notebook",
            "cells": _fallback_extraction(messages),
        }


def _fallback_extraction(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Fallback: create cells directly from user messages that got results."""
    cells: list[dict[str, Any]] = [
        {
            "cell_type": "file",
            "content": "Upload your data file",
            "config": {"accepted_types": [".xlsx", ".csv"], "description": "Upload the dataset to analyze"},
            "output_variable": "",
        },
    ]

    seen_prompts: set[str] = set()

    for msg in messages:
        if msg.get("role") != "user":
            continue
        content = (msg.get("content") or "").strip()
        if not content or len(content) < 5:
            continue

        # Skip duplicates and greetings
        content_lower = content.lower()
        if content_lower in seen_prompts:
            continue
        if any(content_lower.startswith(g) for g in ("hi", "hello", "hey", "thanks", "ok", "yes", "no")):
            continue

        # Check if the next assistant message had useful output
        msg_idx = messages.index(msg)
        has_useful_output = False
        for next_msg in messages[msg_idx + 1:]:
            if next_msg.get("role") == "assistant":
                if next_msg.get("table_data") or next_msg.get("plotly_figure") or len(next_msg.get("content", "")) > 100:
                    has_useful_output = True
                break

        if has_useful_output:
            seen_prompts.add(content_lower)
            cells.append({
                "cell_type": "prompt",
                "content": content,
                "config": None,
                "output_variable": f"result_{len(cells)}",
            })

    # Add at least one prompt if none found
    if len(cells) == 1:
        cells.append({
            "cell_type": "prompt",
            "content": "Analyze the uploaded data",
            "config": None,
            "output_variable": "result_1",
        })

    return cells
