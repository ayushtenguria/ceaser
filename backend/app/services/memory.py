"""Agentic Memory Service — load, inject, and manage agent memories.

Three tiers:
  1. Working Memory: conversation history (handled by chat.py, not here)
  2. Episodic Memory: per-user corrections, preferences (expires after 90 days)
  3. Semantic Memory: per-org domain terms, business rules (permanent)

Memories are loaded per query and injected into the LLM system prompt.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta

from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import AgentMemory

logger = logging.getLogger(__name__)

# Max memories to inject per query (keeps prompt size bounded)
_MAX_MEMORIES_PER_QUERY = 20

# Default TTL for user-level episodic memories
_USER_MEMORY_TTL_DAYS = 90


async def load_memories(
    db: AsyncSession,
    org_id: str,
    user_id: uuid.UUID | None = None,
) -> list[AgentMemory]:
    """Load active, non-expired memories for injection into the LLM prompt.

    Returns org-level memories + user-specific memories, scored by relevance.
    """
    now = datetime.utcnow()

    # Base filter: active, not expired, belongs to this org
    filters = [
        AgentMemory.organization_id == org_id,
        AgentMemory.is_active == True,  # noqa: E712
    ]

    # Load org-level (user_id IS NULL) + this user's memories
    if user_id:
        filters.append(
            (AgentMemory.user_id == None) | (AgentMemory.user_id == user_id)  # noqa: E711
        )
    else:
        filters.append(AgentMemory.user_id == None)  # noqa: E711

    stmt = (
        select(AgentMemory)
        .where(*filters)
        .order_by(
            # Corrections first, then by confidence desc, then newest
            AgentMemory.memory_type == "correction",  # corrections are highest priority
            AgentMemory.confidence.desc(),
            AgentMemory.created_at.desc(),
        )
        .limit(_MAX_MEMORIES_PER_QUERY)
    )

    result = await db.execute(stmt)
    memories = list(result.scalars().all())

    # Filter out expired memories
    active = [m for m in memories if not m.expires_at or m.expires_at > now]

    # Update access counts (fire-and-forget)
    if active:
        mem_ids = [m.id for m in active]
        await db.execute(
            update(AgentMemory)
            .where(AgentMemory.id.in_(mem_ids))
            .values(access_count=AgentMemory.access_count + 1, last_accessed_at=now)
        )

    return active


def format_memories_for_prompt(memories: list[AgentMemory]) -> str:
    """Format memories as a text block for injection into the LLM system prompt.

    Returns empty string if no memories.
    """
    if not memories:
        return ""

    lines = [
        "",
        "AGENT MEMORY (use these facts silently — do NOT mention them to the user):",
        "=" * 60,
    ]

    # Group by type for readability
    org_memories = [m for m in memories if m.user_id is None]
    user_memories = [m for m in memories if m.user_id is not None]

    if org_memories:
        lines.append("\n[ORGANIZATION KNOWLEDGE]")
        for m in org_memories:
            tag = m.memory_type.upper().replace("_", " ")
            lines.append(f"  [{tag}] {m.content}")

    if user_memories:
        lines.append("\n[USER PREFERENCES]")
        for m in user_memories:
            tag = m.memory_type.upper().replace("_", " ")
            lines.append(f"  [{tag}] {m.content}")

    lines.append("")
    return "\n".join(lines)


async def save_memory(
    db: AsyncSession,
    *,
    org_id: str,
    content: str,
    memory_type: str,
    user_id: uuid.UUID | None = None,
    source: str = "auto_extracted",
    source_conversation_id: uuid.UUID | None = None,
    confidence: float = 0.7,
) -> AgentMemory:
    """Save a new memory, deduplicating against existing ones."""
    # Check for duplicate (same org, same type, similar content)
    existing_stmt = select(AgentMemory).where(
        AgentMemory.organization_id == org_id,
        AgentMemory.memory_type == memory_type,
        AgentMemory.content == content,
        AgentMemory.is_active == True,  # noqa: E712
    )
    if user_id:
        existing_stmt = existing_stmt.where(AgentMemory.user_id == user_id)

    existing = await db.execute(existing_stmt)
    if existing.scalar_one_or_none():
        logger.debug("Duplicate memory skipped: %s", content[:80])
        return existing.scalar_one_or_none()

    # Set expiry for user-level memories
    expires_at = None
    if user_id and memory_type in ("preference", "correction"):
        expires_at = datetime.utcnow() + timedelta(days=_USER_MEMORY_TTL_DAYS)

    memory = AgentMemory(
        organization_id=org_id,
        user_id=user_id,
        memory_type=memory_type,
        content=content,
        source=source,
        source_conversation_id=source_conversation_id,
        confidence=confidence,
        expires_at=expires_at,
    )
    db.add(memory)
    await db.flush()
    logger.info("Memory saved: [%s] %s (org=%s, user=%s)", memory_type, content[:60], org_id, user_id)
    return memory
