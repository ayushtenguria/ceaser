"""Edge Case Logger — records novel/unhandled situations for future improvement.

Uses LLM to describe novel edge cases in human-readable form.
Writes to a log file that developers review periodically.
Does NOT block the pipeline — fire-and-forget async logging.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_EDGE_CASE_LOG = Path(__file__).resolve().parent.parent.parent.parent / "logs" / "edge_cases.jsonl"


def log_edge_case(
    *,
    file_name: str,
    sheet_name: str = "",
    category: str,
    description: str,
    raw_error: str = "",
    context: dict[str, Any] | None = None,
) -> None:
    """Log an edge case to the JSONL file for future review.

    Categories:
    - "header" — header detection issues
    - "encoding" — encoding detection failures
    - "type" — column type detection problems
    - "structure" — sheet structure issues (merged cells, pivots, etc.)
    - "data" — data quality issues (mixed types, formula errors, etc.)
    - "memory" — file too large, OOM risk
    - "parse" — general parse failures
    - "unknown" — truly novel issues
    """
    _EDGE_CASE_LOG.parent.mkdir(parents=True, exist_ok=True)

    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "file_name": file_name,
        "sheet_name": sheet_name,
        "category": category,
        "description": description,
        "raw_error": raw_error[:500],  # Truncate long errors
        "context": context or {},
    }

    try:
        with open(_EDGE_CASE_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as exc:
        logger.debug("Could not write edge case log: %s", exc)

    logger.info("Edge case [%s] %s: %s — %s", category, file_name, sheet_name, description)


async def describe_edge_case_with_llm(
    error: str,
    file_name: str,
    sheet_name: str,
    context: dict[str, Any],
    llm: Any = None,
) -> str | None:
    """Use LLM to describe a novel edge case. Fire-and-forget — never blocks pipeline."""
    if not llm:
        return None

    try:
        from langchain_core.messages import HumanMessage, SystemMessage

        messages = [
            SystemMessage(content=(
                "You are a data engineering assistant. A novel edge case was encountered "
                "while processing an Excel/CSV file. Describe what happened and suggest "
                "how to handle it in the future. Be concise (2-3 sentences)."
            )),
            HumanMessage(content=(
                f"File: {file_name}\n"
                f"Sheet: {sheet_name}\n"
                f"Error: {error}\n"
                f"Context: {json.dumps(context, default=str)[:500]}"
            )),
        ]

        response = await llm.ainvoke(messages)
        description = response.content.strip()  # type: ignore[union-attr]

        log_edge_case(
            file_name=file_name,
            sheet_name=sheet_name,
            category="unknown",
            description=f"LLM analysis: {description}",
            raw_error=error,
            context=context,
        )

        return description

    except Exception:
        return None


def get_recent_edge_cases(limit: int = 50) -> list[dict]:
    """Read recent edge cases for developer review."""
    if not _EDGE_CASE_LOG.exists():
        return []

    entries = []
    try:
        with open(_EDGE_CASE_LOG, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except Exception:
        pass

    return entries[-limit:]
