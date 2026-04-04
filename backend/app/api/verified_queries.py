"""Verified queries API — CRUD for org-level saved SQL patterns."""

from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from app.api.schemas import to_camel, _CamelModel
from app.core.deps import CurrentUser, DbSession
from app.core.permissions import Permission, require_permission
from app.db.models import VerifiedQuery

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/verified-queries", tags=["verified-queries"])


class VerifiedQueryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)

    id: uuid.UUID
    original_question: str
    question_pattern: str
    sql_template: str
    use_count: int
    confidence: float
    is_active: bool
    created_at: str


class VerifiedQueryCreate(_CamelModel):
    connection_id: uuid.UUID
    original_question: str = Field(..., min_length=1)
    sql_template: str = Field(..., min_length=1)


class VerifiedQueryUpdate(_CamelModel):
    sql_template: str | None = None
    is_active: bool | None = None


@router.get("/", response_model=list[VerifiedQueryResponse])
async def list_verified_queries(
    current_user: CurrentUser,
    db: DbSession,
    connection_id: uuid.UUID | None = None,
) -> list[VerifiedQuery]:
    """List verified queries for the org, optionally filtered by connection."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    org_id = user.organization_id or ""

    stmt = select(VerifiedQuery).where(
        VerifiedQuery.organization_id == org_id,
    ).order_by(VerifiedQuery.use_count.desc())

    if connection_id:
        stmt = stmt.where(VerifiedQuery.connection_id == connection_id)

    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.post("/", response_model=VerifiedQueryResponse, status_code=status.HTTP_201_CREATED)
async def create_verified_query(
    body: VerifiedQueryCreate,
    current_user: CurrentUser,
    db: DbSession,
) -> VerifiedQuery:
    """Manually create a verified query (admin)."""
    user = await require_permission(Permission.QUERY_DATA, current_user, db)
    org_id = user.organization_id or ""

    from app.services.verified_queries import create_verified_query as _create
    vq = await _create(
        db, org_id=org_id,
        connection_id=body.connection_id,
        question=body.original_question,
        sql=body.sql_template,
        verified_by=user.id,
    )
    if not vq:
        raise HTTPException(status_code=400, detail="Could not create verified query.")

    await db.commit()
    await db.refresh(vq)
    return vq


@router.patch("/{query_id}", response_model=VerifiedQueryResponse)
async def update_verified_query(
    query_id: uuid.UUID,
    body: VerifiedQueryUpdate,
    current_user: CurrentUser,
    db: DbSession,
) -> VerifiedQuery:
    """Update a verified query's SQL template or active status."""
    user = await require_permission(Permission.QUERY_DATA, current_user, db)
    org_id = user.organization_id or ""

    stmt = select(VerifiedQuery).where(
        VerifiedQuery.id == query_id,
        VerifiedQuery.organization_id == org_id,
    )
    result = await db.execute(stmt)
    vq = result.scalar_one_or_none()
    if not vq:
        raise HTTPException(status_code=404, detail="Verified query not found.")

    if body.sql_template is not None:
        vq.sql_template = body.sql_template
    if body.is_active is not None:
        vq.is_active = body.is_active

    await db.commit()
    await db.refresh(vq)
    return vq


@router.delete("/{query_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_verified_query(
    query_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> None:
    """Delete a verified query."""
    user = await require_permission(Permission.QUERY_DATA, current_user, db)
    org_id = user.organization_id or ""

    stmt = select(VerifiedQuery).where(
        VerifiedQuery.id == query_id,
        VerifiedQuery.organization_id == org_id,
    )
    result = await db.execute(stmt)
    vq = result.scalar_one_or_none()
    if not vq:
        raise HTTPException(status_code=404, detail="Verified query not found.")

    await db.delete(vq)
    await db.commit()
