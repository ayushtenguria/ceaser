"""Plan enforcement — checks usage limits before allowing actions.

Checks:
- max_queries_per_day: count today's chat queries from audit_logs
- max_connections: count org's connections
- max_seats: count org's users
- max_reports: count org's reports this month
- file_size_limit: check upload size
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from fastapi import HTTPException, status
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import AuditLog, DatabaseConnection, FileUpload, Report, User, OrganizationPlan

logger = logging.getLogger(__name__)


async def get_org_plan(db: AsyncSession, org_id: str) -> OrganizationPlan | None:
    """Get the plan for an organization. Returns None if no plan (unlimited)."""
    if not org_id:
        return None
    stmt = select(OrganizationPlan).where(OrganizationPlan.organization_id == org_id)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def check_query_limit(db: AsyncSession, org_id: str) -> None:
    """Check if org has exceeded daily query limit. Raises 429 if exceeded."""
    plan = await get_org_plan(db, org_id)
    if not plan:
        return  # No plan = no limits

    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    stmt = (
        select(func.count())
        .select_from(AuditLog)
        .where(
            AuditLog.action == "chat_query",
            AuditLog.created_at >= today,
        )
    )
    result = await db.execute(stmt)
    count = result.scalar() or 0

    if count >= plan.max_queries_per_day:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Daily query limit reached ({count}/{plan.max_queries_per_day}). Upgrade your plan for more queries.",
        )


async def check_connection_limit(db: AsyncSession, org_id: str) -> None:
    """Check if org can add another connection."""
    plan = await get_org_plan(db, org_id)
    if not plan:
        return

    stmt = (
        select(func.count())
        .select_from(DatabaseConnection)
        .where(DatabaseConnection.organization_id == org_id)
    )
    result = await db.execute(stmt)
    count = result.scalar() or 0

    if count >= plan.max_connections:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail=f"Connection limit reached ({count}/{plan.max_connections}). Upgrade your plan.",
        )


async def check_report_limit(db: AsyncSession, org_id: str) -> None:
    """Check if org can generate another report this month."""
    plan = await get_org_plan(db, org_id)
    if not plan:
        return

    month_start = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    stmt = (
        select(func.count())
        .select_from(Report)
        .where(
            Report.organization_id == org_id,
            Report.created_at >= month_start,
        )
    )
    result = await db.execute(stmt)
    count = result.scalar() or 0

    if count >= plan.max_reports:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail=f"Monthly report limit reached ({count}/{plan.max_reports}). Upgrade your plan.",
        )


def check_file_size(file_size: int, plan_name: str = "free") -> None:
    """Check if file size is within plan limits."""
    limits = {
        "free": 5 * 1024 * 1024,       # 5 MB
        "starter": 50 * 1024 * 1024,    # 50 MB
        "business": 200 * 1024 * 1024,  # 200 MB
        "enterprise": 1024 * 1024 * 1024, # 1 GB
    }
    max_size = limits.get(plan_name, limits["free"])

    if file_size > max_size:
        max_mb = max_size / (1024 * 1024)
        file_mb = file_size / (1024 * 1024)
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File too large ({file_mb:.1f} MB). Your plan allows up to {max_mb:.0f} MB.",
        )
