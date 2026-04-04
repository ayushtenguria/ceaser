"""Join rules API — org-level overrides for table join paths."""

from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from app.api.schemas import to_camel, _CamelModel
from app.core.deps import CurrentUser, DbSession
from app.core.permissions import Permission, require_permission
from app.db.models import JoinRule

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/join-rules", tags=["join-rules"])


class JoinRuleCreate(_CamelModel):
    connection_id: uuid.UUID
    source_table: str = Field(..., min_length=1)
    source_column: str = Field(..., min_length=1)
    target_table: str = Field(..., min_length=1)
    target_column: str = Field(..., min_length=1)
    join_type: str = Field("LEFT JOIN", pattern=r"^(LEFT JOIN|INNER JOIN|RIGHT JOIN|FULL OUTER JOIN)$")
    description: str = ""


class JoinRuleUpdate(_CamelModel):
    join_type: str | None = None
    description: str | None = None
    is_active: bool | None = None


class JoinRuleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)

    id: uuid.UUID
    connection_id: uuid.UUID
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    join_type: str
    description: str
    is_active: bool
    created_at: str


@router.get("/", response_model=list[JoinRuleResponse])
async def list_join_rules(
    current_user: CurrentUser,
    db: DbSession,
    connection_id: uuid.UUID | None = None,
) -> list[JoinRule]:
    """List join rules for the org, optionally filtered by connection."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    org_id = user.organization_id or ""

    stmt = select(JoinRule).where(JoinRule.organization_id == org_id)
    if connection_id:
        stmt = stmt.where(JoinRule.connection_id == connection_id)

    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.post("/", response_model=JoinRuleResponse, status_code=status.HTTP_201_CREATED)
async def create_join_rule(
    body: JoinRuleCreate,
    current_user: CurrentUser,
    db: DbSession,
) -> JoinRule:
    """Create a new join rule."""
    user = await require_permission(Permission.QUERY_DATA, current_user, db)
    org_id = user.organization_id or ""

    rule = JoinRule(
        organization_id=org_id,
        connection_id=body.connection_id,
        source_table=body.source_table,
        source_column=body.source_column,
        target_table=body.target_table,
        target_column=body.target_column,
        join_type=body.join_type,
        description=body.description,
        created_by=user.id,
    )
    db.add(rule)
    await db.flush()
    await db.refresh(rule)
    await db.commit()

    logger.info("Join rule created: %s.%s → %s.%s",
                body.source_table, body.source_column,
                body.target_table, body.target_column)
    return rule


@router.patch("/{rule_id}", response_model=JoinRuleResponse)
async def update_join_rule(
    rule_id: uuid.UUID,
    body: JoinRuleUpdate,
    current_user: CurrentUser,
    db: DbSession,
) -> JoinRule:
    """Update a join rule."""
    user = await require_permission(Permission.QUERY_DATA, current_user, db)
    org_id = user.organization_id or ""

    stmt = select(JoinRule).where(JoinRule.id == rule_id, JoinRule.organization_id == org_id)
    result = await db.execute(stmt)
    rule = result.scalar_one_or_none()
    if not rule:
        raise HTTPException(status_code=404, detail="Join rule not found.")

    if body.join_type is not None:
        rule.join_type = body.join_type
    if body.description is not None:
        rule.description = body.description
    if body.is_active is not None:
        rule.is_active = body.is_active

    await db.commit()
    await db.refresh(rule)
    return rule


@router.delete("/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_join_rule(
    rule_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> None:
    """Delete a join rule."""
    user = await require_permission(Permission.QUERY_DATA, current_user, db)
    org_id = user.organization_id or ""

    stmt = select(JoinRule).where(JoinRule.id == rule_id, JoinRule.organization_id == org_id)
    result = await db.execute(stmt)
    rule = result.scalar_one_or_none()
    if not rule:
        raise HTTPException(status_code=404, detail="Join rule not found.")

    await db.delete(rule)
    await db.commit()
