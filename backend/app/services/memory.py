"""Agentic Memory Service — enterprise-grade with Graph RAG.

Hybrid retrieval:
  1. Neo4j graph (keyword + relationship traversal)
  2. Neo4j vector index (semantic similarity)
  3. PostgreSQL fallback (if Neo4j unavailable)

Memories are scored with temporal decay, conflict resolution, and consolidation.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta

from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import AgentMemory

logger = logging.getLogger(__name__)

_USER_MEMORY_TTL_DAYS = 90


async def load_memories(
    db: AsyncSession,
    org_id: str,
    user_id: uuid.UUID | None = None,
    question: str = "",
) -> list[dict]:
    """Load relevant memories using Graph RAG with PostgreSQL fallback.

    Args:
        db: database session
        org_id: organization ID
        user_id: optional user ID for user-specific memories
        question: the user's question (for relevance filtering)
    """
    try:
        from app.services.memory_graph import retrieve_memories, update_memory_access
        memories = await retrieve_memories(
            question=question,
            org_id=org_id,
            user_id=str(user_id) if user_id else None,
        )
        if memories:
            mem_ids = [m["id"] for m in memories if m.get("id")]
            await update_memory_access(mem_ids)
            logger.info("Graph RAG memories: %d loaded for org %s", len(memories), org_id)
            return memories
    except Exception as exc:
        logger.debug("Graph memory retrieval failed, falling back to PostgreSQL: %s", exc)

    return await _load_memories_flat(db, org_id, user_id)


async def _load_memories_flat(
    db: AsyncSession,
    org_id: str,
    user_id: uuid.UUID | None,
) -> list[dict]:
    """Flat PostgreSQL loading — fallback when Neo4j unavailable."""
    now = datetime.utcnow()

    filters = [
        AgentMemory.organization_id == org_id,
        AgentMemory.is_active == True,  # noqa: E712
    ]

    if user_id:
        filters.append(
            (AgentMemory.user_id == None) | (AgentMemory.user_id == user_id)  # noqa: E711
        )
    else:
        filters.append(AgentMemory.user_id == None)  # noqa: E711

    stmt = (
        select(AgentMemory)
        .where(*filters)
        .order_by(AgentMemory.confidence.desc(), AgentMemory.created_at.desc())
        .limit(20)
    )

    result = await db.execute(stmt)
    memories = list(result.scalars().all())

    active = [m for m in memories if not m.expires_at or m.expires_at > now]

    if active:
        mem_ids = [m.id for m in active]
        await db.execute(
            update(AgentMemory)
            .where(AgentMemory.id.in_(mem_ids))
            .values(access_count=AgentMemory.access_count + 1, last_accessed_at=now)
        )

    return [
        {
            "id": str(m.id),
            "content": m.content,
            "type": m.memory_type,
            "confidence": m.confidence,
            "user_id": str(m.user_id) if m.user_id else None,
            "access_count": m.access_count,
            "final_score": m.confidence,
        }
        for m in active
    ]


def format_memories_for_prompt(memories: list[dict]) -> str:
    """Format memories for LLM prompt injection. Works with both graph and flat memories."""
    if not memories:
        return ""

    try:
        from app.services.memory_graph import format_memories_for_prompt as graph_format
        return graph_format(memories)
    except ImportError:
        pass

    lines = [
        "",
        "AGENT MEMORY (use these facts silently — do NOT mention them to the user):",
        "=" * 60,
    ]

    org_memories = [m for m in memories if not m.get("user_id")]
    user_memories = [m for m in memories if m.get("user_id")]

    if org_memories:
        lines.append("\n[ORGANIZATION KNOWLEDGE]")
        for m in org_memories:
            tag = m.get("type", "fact").upper().replace("_", " ")
            lines.append(f"  [{tag}] {m['content']}")

    if user_memories:
        lines.append("\n[USER PREFERENCES]")
        for m in user_memories:
            tag = m.get("type", "fact").upper().replace("_", " ")
            lines.append(f"  [{tag}] {m['content']}")

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
    related_tables: list[str] | None = None,
    related_columns: list[dict] | None = None,
) -> AgentMemory:
    """Save a memory to both PostgreSQL and Neo4j graph."""
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

    try:
        from app.services.memory_graph import save_memory_to_graph
        await save_memory_to_graph(
            memory_id=str(memory.id),
            org_id=org_id,
            user_id=str(user_id) if user_id else None,
            memory_type=memory_type,
            content=content,
            source=source,
            confidence=confidence,
            source_conversation_id=str(source_conversation_id) if source_conversation_id else None,
            related_tables=related_tables,
            related_columns=related_columns,
        )
    except Exception as exc:
        logger.debug("Graph memory save failed (non-blocking): %s", exc)

    logger.info("Memory saved: [%s] %s", memory_type, content[:60])
    return memory
