"""Genomics-specific code repair — fixes bioinformatics errors.

Knows pydeseq2/gseapy/scanpy API patterns and common genomics mistakes
that the generic repair agent wouldn't understand.
"""

from __future__ import annotations

import logging

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import SystemMessage, HumanMessage

from app.agents.genomics.prompts import GENOMICS_REPAIR_PROMPT
from app.agents.state import AgentState

logger = logging.getLogger(__name__)


async def repair_genomics_code(state: AgentState, llm: BaseChatModel) -> AgentState:
    """Attempt to repair failed genomics Python code."""
    code = state.get("code_block", "")
    error = state.get("error", "")
    schema = state.get("schema_context", "")

    if not code or not error:
        return state

    logger.info("Genomics repair: fixing error: %s", error[:150])

    messages = [
        SystemMessage(content=GENOMICS_REPAIR_PROMPT.format(
            code=code,
            error=error,
            schema=schema[:5000],
        )),
        HumanMessage(content=f"Fix this genomics code. The error was: {error}"),
    ]

    response = await llm.ainvoke(messages)
    fixed_code: str = response.content.strip()  # type: ignore[union-attr]

    # Strip markdown fences
    if fixed_code.startswith("```"):
        lines = fixed_code.split("\n")
        lines = [ln for ln in lines if not ln.strip().startswith("```")]
        fixed_code = "\n".join(lines).strip()

    if not fixed_code or len(fixed_code) < 30:
        logger.warning("Genomics repair produced too-short code, keeping original")
        return state

    logger.info("Genomics repair: fixed code (%d → %d chars)", len(code), len(fixed_code))
    return {
        **state,
        "code_block": fixed_code,
        "error": None,
    }
