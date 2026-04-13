"""Memory endpoints — view, manage, and admin agent memories."""

from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import func, select

from app.core.deps import CurrentUser, DbSession
from app.core.permissions import Permission, require_permission
from app.db.models import AgentMemory
from app.services.memory import save_memory

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/memories", tags=["memories"])


@router.get("/")
async def list_memories(
    current_user: CurrentUser,
    db: DbSession,
    memory_type: str | None = Query(None),
    scope: str = Query("all", pattern="^(all|user|org)$"),
    limit: int = Query(50, ge=1, le=200),
) -> list[dict]:
    """List memories for the current user's organization.

    scope: 'all' = org + user, 'user' = user-only, 'org' = org-only
    """
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    org_id = user.organization_id or ""

    filters = [
        AgentMemory.organization_id == org_id,
        AgentMemory.is_active == True,  # noqa: E712
    ]

    if scope == "user":
        filters.append(AgentMemory.user_id == user.id)
    elif scope == "org":
        filters.append(AgentMemory.user_id == None)  # noqa: E711
    else:
        filters.append(
            (AgentMemory.user_id == None) | (AgentMemory.user_id == user.id)  # noqa: E711
        )

    if memory_type:
        filters.append(AgentMemory.memory_type == memory_type)

    stmt = select(AgentMemory).where(*filters).order_by(AgentMemory.created_at.desc()).limit(limit)
    result = await db.execute(stmt)
    memories = list(result.scalars().all())

    return [_serialize(m) for m in memories]


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_memory(
    body: dict,
    current_user: CurrentUser,
    db: DbSession,
) -> dict:
    """Manually create a memory (admin or user adding a domain term)."""
    user = await require_permission(Permission.QUERY_DATA, current_user, db)
    org_id = user.organization_id or ""

    content = body.get("content", "").strip()
    memory_type = body.get("memoryType", "domain_term")
    scope = body.get("scope", "org")

    if not content:
        raise HTTPException(status_code=400, detail="Content is required.")

    valid_types = {
        "correction",
        "preference",
        "column_alias",
        "domain_term",
        "business_rule",
        "table_note",
        "learned_fact",
    }
    if memory_type not in valid_types:
        raise HTTPException(status_code=400, detail=f"Invalid type. Valid: {sorted(valid_types)}")

    mem = await save_memory(
        db,
        org_id=org_id,
        content=content,
        memory_type=memory_type,
        user_id=user.id if scope == "user" else None,
        source="admin_manual",
        confidence=1.0,
    )
    await db.commit()
    return _serialize(mem)


@router.delete("/{memory_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_memory(
    memory_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> None:
    """Delete (soft-delete) a memory. Users can delete their own, admins can delete org memories."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    org_id = user.organization_id or ""

    stmt = select(AgentMemory).where(
        AgentMemory.id == memory_id,
        AgentMemory.organization_id == org_id,
    )
    result = await db.execute(stmt)
    mem = result.scalar_one_or_none()

    if mem is None:
        raise HTTPException(status_code=404, detail="Memory not found.")

    if mem.user_id and mem.user_id != user.id and user.role not in ("admin", "super_admin"):
        raise HTTPException(status_code=403, detail="Cannot delete another user's memory.")

    mem.is_active = False
    await db.flush()


@router.get("/stats")
async def memory_stats(current_user: CurrentUser, db: DbSession) -> dict:
    """Get memory statistics for the organization."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    org_id = user.organization_id or ""

    stmt = (
        select(AgentMemory.memory_type, func.count())
        .where(
            AgentMemory.organization_id == org_id,
            AgentMemory.is_active == True,  # noqa: E712
        )
        .group_by(AgentMemory.memory_type)
    )
    result = await db.execute(stmt)
    by_type = {row[0]: row[1] for row in result.all()}

    org_count_stmt = (
        select(func.count())
        .select_from(AgentMemory)
        .where(
            AgentMemory.organization_id == org_id,
            AgentMemory.is_active == True,  # noqa: E712
            AgentMemory.user_id == None,  # noqa: E711
        )
    )
    user_count_stmt = (
        select(func.count())
        .select_from(AgentMemory)
        .where(
            AgentMemory.organization_id == org_id,
            AgentMemory.is_active == True,  # noqa: E712
            AgentMemory.user_id != None,  # noqa: E711
        )
    )

    org_count = (await db.execute(org_count_stmt)).scalar() or 0
    user_count = (await db.execute(user_count_stmt)).scalar() or 0

    return {
        "total": org_count + user_count,
        "orgLevel": org_count,
        "userLevel": user_count,
        "byType": by_type,
    }


def _serialize(m: AgentMemory) -> dict:
    return {
        "id": str(m.id),
        "memoryType": m.memory_type,
        "content": m.content,
        "scope": "user" if m.user_id else "org",
        "source": m.source,
        "confidence": m.confidence,
        "accessCount": m.access_count,
        "lastAccessedAt": m.last_accessed_at.isoformat() if m.last_accessed_at else None,
        "expiresAt": m.expires_at.isoformat() if m.expires_at else None,
        "createdAt": m.created_at.isoformat() if m.created_at else None,
    }
