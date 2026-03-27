"""Memory Extractor — extracts atomic facts from conversation turns.

Runs ASYNC after each chat response (non-blocking).
Uses a small/fast LLM call to extract corrections, preferences, and domain terms.
"""

from __future__ import annotations

import json
import logging
import uuid

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import SystemMessage, HumanMessage
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.memory import save_memory

logger = logging.getLogger(__name__)

_EXTRACTION_PROMPT = """You are a memory extraction agent for a data analytics platform.

Analyze the user message and assistant response below. Extract any useful facts that should be remembered for future conversations.

EXTRACT these types of memories:
- "correction": User corrected the AI (wrong table, wrong column, wrong interpretation)
- "preference": User expressed a preference (chart type, format, metric they care about)
- "domain_term": User defined a business term ("churn means...", "MRR is...")
- "business_rule": A business rule was revealed ("revenue is in cents", "fiscal year starts April")
- "column_alias": A column mapping was clarified ("cust_id = customer_id in the other table")
- "learned_fact": The AI discovered something useful ("the status column uses 1,2,3 not text")

RULES:
- Only extract NEW, non-obvious facts
- Keep each memory as a single, clear sentence
- Do NOT extract greetings, thanks, or generic statements
- Do NOT extract facts already obvious from the database schema
- If nothing worth remembering, return an empty array

Return a JSON array (no markdown, no explanation):
[{"type": "correction", "content": "...", "confidence": 0.8}, ...]

Return [] if nothing to extract."""


async def extract_memories(
    user_message: str,
    assistant_response: str,
    sql_query: str | None,
    llm: BaseChatModel,
    db: AsyncSession,
    org_id: str,
    user_id: uuid.UUID | None = None,
    conversation_id: uuid.UUID | None = None,
) -> int:
    """Extract and save memories from a conversation turn.

    Returns the number of memories saved. Non-blocking — errors are logged, not raised.
    """
    try:
        # Skip very short or empty exchanges
        if len(user_message) < 10 or len(assistant_response) < 20:
            return 0

        context = f"USER: {user_message}\n\nASSISTANT: {assistant_response}"
        if sql_query:
            context += f"\n\nSQL GENERATED: {sql_query}"

        messages = [
            SystemMessage(content=_EXTRACTION_PROMPT),
            HumanMessage(content=context),
        ]

        response = await llm.ainvoke(messages)
        text = response.content.strip()

        # Parse JSON response
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]

        memories = json.loads(text)
        if not isinstance(memories, list):
            return 0

        saved = 0
        for mem in memories:
            if not isinstance(mem, dict):
                continue
            content = mem.get("content", "").strip()
            mem_type = mem.get("type", "learned_fact")
            confidence = mem.get("confidence", 0.7)

            if not content or len(content) < 5:
                continue

            # Validate type
            valid_types = {"correction", "preference", "column_alias", "domain_term", "business_rule", "learned_fact"}
            if mem_type not in valid_types:
                mem_type = "learned_fact"

            # Determine scope: corrections and preferences are user-level,
            # domain terms and business rules are org-level
            mem_user_id = user_id if mem_type in ("correction", "preference") else None

            await save_memory(
                db,
                org_id=org_id,
                content=content,
                memory_type=mem_type,
                user_id=mem_user_id,
                source="auto_extracted",
                source_conversation_id=conversation_id,
                confidence=float(confidence),
            )
            saved += 1

        if saved:
            logger.info("Extracted %d memories from conversation %s", saved, conversation_id)
        return saved

    except json.JSONDecodeError:
        logger.debug("Memory extraction returned non-JSON (no memories to extract)")
        return 0
    except Exception as exc:
        logger.warning("Memory extraction failed (non-blocking): %s", exc)
        return 0
