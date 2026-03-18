"""Semantic layer — business metric definitions for consistent analytics."""

from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from app.api.schemas import MetricCreate, MetricUpdate, MetricResponse
from app.core.deps import CurrentUser, DbSession
from app.core.permissions import Permission, require_permission
from app.db.models import MetricDefinition, User

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/metrics", tags=["metrics"])


async def _get_user(db: DbSession, clerk_id: str) -> User:
    stmt = select(User).where(User.clerk_id == clerk_id)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()
    if user is None:
        if clerk_id == "dev_user":
            user = User(clerk_id="dev_user", email=get_settings().dev_fallback_email, first_name="Dev", last_name="User", organization_id="dev_org")
            db.add(user)
            await db.flush()
            await db.refresh(user)
            return user
        raise HTTPException(status_code=404, detail="User not found.")
    return user


@router.post("/", response_model=MetricResponse, status_code=status.HTTP_201_CREATED)
async def create_metric(body: MetricCreate, current_user: CurrentUser, db: DbSession) -> MetricDefinition:
    """Define a new business metric."""
    user = await require_permission(Permission.MANAGE_METRICS, current_user, db)
    metric = MetricDefinition(
        name=body.name,
        description=body.description,
        sql_expression=body.sql_expression,
        category=body.category,
        connection_id=body.connection_id,
        organization_id=user.organization_id or current_user.org_id or "",
        user_id=user.id,
    )
    db.add(metric)
    await db.flush()
    await db.refresh(metric)
    logger.info("Created metric '%s'", body.name)
    return metric


@router.get("/", response_model=list[MetricResponse])
async def list_metrics(current_user: CurrentUser, db: DbSession) -> list[MetricDefinition]:
    """List all metric definitions for the organization."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    stmt = (
        select(MetricDefinition)
        .where(MetricDefinition.organization_id == (user.organization_id or current_user.org_id or ""))
        .order_by(MetricDefinition.category, MetricDefinition.name)
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.patch("/{metric_id}", response_model=MetricResponse)
async def update_metric(
    metric_id: uuid.UUID, body: MetricUpdate, current_user: CurrentUser, db: DbSession
) -> MetricDefinition:
    """Update a metric definition."""
    user = await require_permission(Permission.MANAGE_METRICS, current_user, db)
    stmt = select(MetricDefinition).where(MetricDefinition.id == metric_id, MetricDefinition.user_id == user.id)
    result = await db.execute(stmt)
    metric = result.scalar_one_or_none()
    if metric is None:
        raise HTTPException(status_code=404, detail="Metric not found.")

    if body.name is not None: metric.name = body.name
    if body.description is not None: metric.description = body.description
    if body.sql_expression is not None: metric.sql_expression = body.sql_expression
    if body.category is not None: metric.category = body.category

    await db.flush()
    await db.refresh(metric)
    return metric


@router.delete("/{metric_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_metric(metric_id: uuid.UUID, current_user: CurrentUser, db: DbSession) -> None:
    """Delete a metric definition."""
    user = await require_permission(Permission.MANAGE_METRICS, current_user, db)
    stmt = select(MetricDefinition).where(MetricDefinition.id == metric_id, MetricDefinition.user_id == user.id)
    result = await db.execute(stmt)
    metric = result.scalar_one_or_none()
    if metric is None:
        raise HTTPException(status_code=404, detail="Metric not found.")
    await db.delete(metric)
