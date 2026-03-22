"""Audit log — tracks all user actions for compliance."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime

from fastapi import APIRouter, Query
from sqlalchemy import select, func

from app.api.schemas import AuditLogResponse
from app.core.deps import CurrentUser, DbSession
from app.core.permissions import Permission, require_permission
from app.db.models import AuditLog, User

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("/", response_model=list[AuditLogResponse])
async def list_audit_logs(
    current_user: CurrentUser,
    db: DbSession,
    action: str | None = Query(None, description="Filter by action type"),
    resource_type: str | None = Query(None, description="Filter by resource type"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> list[AuditLog]:
    """List audit logs for the organization. Newest first."""
    user = await require_permission(Permission.VIEW_AUDIT, current_user, db)

    # Filter by org: only show logs from users in the same organization
    org_user_ids = select(User.id).where(User.organization_id == (user.organization_id or ""))

    stmt = select(AuditLog).where(
        AuditLog.user_id.in_(org_user_ids)
    ).order_by(AuditLog.created_at.desc())

    if action:
        stmt = stmt.where(AuditLog.action == action)
    if resource_type:
        stmt = stmt.where(AuditLog.resource_type == resource_type)

    stmt = stmt.offset(offset).limit(limit)
    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.get("/stats")
async def audit_stats(current_user: CurrentUser, db: DbSession) -> dict:
    """Get summary stats of recent activity."""
    await require_permission(Permission.VIEW_AUDIT, current_user, db)
    # Count by action type in the last 24 hours
    since = datetime.utcnow().replace(hour=0, minute=0, second=0)
    stmt = (
        select(AuditLog.action, func.count())
        .where(AuditLog.created_at >= since)
        .group_by(AuditLog.action)
    )
    result = await db.execute(stmt)
    action_counts = {row[0]: row[1] for row in result.all()}

    # Total queries today
    total = sum(action_counts.values())

    return {
        "today": {
            "total_actions": total,
            "by_action": action_counts,
        }
    }
