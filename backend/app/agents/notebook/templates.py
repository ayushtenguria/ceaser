"""Notebook Template Agent — auto-generates notebooks from descriptions."""

from __future__ import annotations

import json
import logging
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

logger = logging.getLogger(__name__)

_GENERATE_PROMPT = """\
You are a data analysis notebook designer. Given a description of what analysis the user
wants, generate a notebook structure with cells.

Description: {description}

Available cell types:
- "text": markdown text (headers, notes). Content is markdown.
- "file": file upload placeholder. Config: {{"accepted_types": [".xlsx", ".csv"], "description": "..."}}
- "input": user parameter. Config: {{"input_type": "text|number|select|date", "label": "...", "default": "...", "options": [...]}}
- "prompt": AI analysis instruction. Content is the natural language prompt. Can use {{variable}} for inputs.
- "code": Python code cell. Content is raw Python.

Rules:
1. Start with a text cell (title/description)
2. Add file cell if data upload is needed
3. Add input cells for parameters the user might want to change
4. Add 3-5 prompt cells for the actual analysis steps
5. End with a text cell (summary section)
6. Each prompt cell should be a single, focused analysis step
7. Use {{variable}} syntax to reference input cell values
8. Set output_variable for cells whose results are used later

Return a JSON array of cell objects:
[{{"cell_type": "text", "content": "...", "config": null, "output_variable": ""}}]
"""


async def generate_notebook_from_description(
    description: str,
    llm: BaseChatModel,
) -> list[dict[str, Any]]:
    """Auto-generate a notebook structure from a plain text description."""
    messages = [
        SystemMessage(content=_GENERATE_PROMPT.format(description=description)),
        HumanMessage(content=description),
    ]

    try:
        response = await llm.ainvoke(messages)
        raw: str = response.content.strip()  # type: ignore[union-attr]

        if raw.startswith("```"):
            lines = raw.split("\n")
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            raw = "\n".join(lines).strip()

        cells = json.loads(raw)
        if not isinstance(cells, list):
            return _default_cells(description)

        valid_types = {"text", "file", "input", "prompt", "code"}
        cleaned = []
        for cell in cells:
            if not isinstance(cell, dict):
                continue
            ct = cell.get("cell_type", "")
            if ct not in valid_types:
                continue
            cleaned.append({
                "cell_type": ct,
                "content": cell.get("content", ""),
                "config": cell.get("config"),
                "output_variable": cell.get("output_variable", ""),
            })

        if len(cleaned) < 2:
            return _default_cells(description)

        return cleaned

    except Exception as exc:
        logger.warning("Notebook generation failed: %s", exc)
        return _default_cells(description)


def _default_cells(description: str) -> list[dict[str, Any]]:
    """Fallback notebook structure."""
    return [
        {"cell_type": "text", "content": f"# {description}", "config": None, "output_variable": ""},
        {"cell_type": "file", "content": "Upload your data file", "config": {"accepted_types": [".xlsx", ".csv"], "description": "Upload data"}, "output_variable": ""},
        {"cell_type": "prompt", "content": f"Analyze the uploaded data: {description}", "config": None, "output_variable": "analysis"},
        {"cell_type": "prompt", "content": "Create a visualization of the key findings", "config": None, "output_variable": "chart"},
        {"cell_type": "text", "content": "## Summary\nAnalysis complete.", "config": None, "output_variable": ""},
    ]
