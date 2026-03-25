"""Auth endpoints — Clerk user sync and current-user info."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from app.api.schemas import UserResponse, UserSyncRequest
from app.core.config import get_settings
from app.core.deps import CurrentUser, DbSession
from app.core.permissions import get_permissions, get_user_with_role
from app.db.models import User

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/sync", response_model=UserResponse, status_code=status.HTTP_200_OK)
async def sync_user(payload: UserSyncRequest, current_user: CurrentUser, db: DbSession) -> User:
    """Create or update a local user record from Clerk data.

    Called by the frontend after Clerk sign-in/sign-up, or by a Clerk webhook.
    """
    # Verify the sync request matches the authenticated user
    if payload.clerk_id != current_user.user_id and current_user.user_id != "dev_user":
        raise HTTPException(status_code=403, detail="Cannot sync a different user's data.")

    stmt = select(User).where(User.clerk_id == payload.clerk_id)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()

    settings = get_settings()
    is_admin = payload.email in settings.super_admin_emails

    if user is None:
        user = User(
            clerk_id=payload.clerk_id,
            email=payload.email,
            first_name=payload.first_name,
            last_name=payload.last_name,
            organization_id=payload.organization_id,
            image_url=payload.image_url,
            role="super_admin" if is_admin else "member",
            is_super_admin=is_admin,
        )
        db.add(user)
        logger.info("Created new user: %s (%s) admin=%s", payload.email, payload.clerk_id, is_admin)
    else:
        user.email = payload.email
        user.first_name = payload.first_name
        user.last_name = payload.last_name
        # Only update org_id if provided (don't overwrite with null)
        if payload.organization_id:
            user.organization_id = payload.organization_id
        user.image_url = payload.image_url
        # Update admin status in case config changed
        user.is_super_admin = is_admin
        if is_admin:
            user.role = "super_admin"
        logger.info("Updated existing user: %s", payload.clerk_id)

    await db.flush()
    await db.refresh(user)

    # Auto-create Free plan for org if none exists
    if user.organization_id:
        from app.db.models import OrganizationPlan
        plan_stmt = select(OrganizationPlan).where(
            OrganizationPlan.organization_id == user.organization_id
        )
        plan_result = await db.execute(plan_stmt)
        existing_plan = plan_result.scalar_one_or_none()
        if existing_plan is None:
            free_plan = OrganizationPlan(
                organization_id=user.organization_id,
                plan_name="free",
                max_seats=5,
                max_connections=1,
                max_queries_per_day=50,
                max_reports=5,
            )
            db.add(free_plan)
            logger.info("Auto-created Free plan for org %s", user.organization_id)

    return user


@router.get("/me", response_model=UserResponse)
async def get_me(current_user: CurrentUser, db: DbSession) -> User:
    """Return the currently authenticated user's profile."""
    stmt = select(User).where(User.clerk_id == current_user.user_id)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found. Please call /auth/sync first.",
        )
    return user


@router.get("/me/plan")
async def get_my_plan(current_user: CurrentUser, db: DbSession) -> dict:
    """Return the current user's organization plan and usage."""
    from app.db.models import OrganizationPlan, AuditLog, DatabaseConnection, Report
    from datetime import datetime
    from sqlalchemy import func

    user = await get_user_with_role(db, current_user.user_id)
    org_id = user.organization_id or ""

    # Super admin = unlimited everything
    is_admin = user.is_super_admin

    # Get plan
    plan_stmt = select(OrganizationPlan).where(OrganizationPlan.organization_id == org_id)
    plan_result = await db.execute(plan_stmt)
    plan = plan_result.scalar_one_or_none()

    if is_admin:
        plan_name = "enterprise"
        max_queries = -1  # unlimited
        max_connections = -1
        max_reports = -1
        max_seats = -1
    else:
        plan_name = plan.plan_name if plan else "free"
        max_queries = plan.max_queries_per_day if plan else 50
        max_connections = plan.max_connections if plan else 1
        max_reports = plan.max_reports if plan else 5
        max_seats = plan.max_seats if plan else 5

    # Get today's query count
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    query_count_stmt = (
        select(func.count()).select_from(AuditLog)
        .where(AuditLog.action == "chat_query", AuditLog.created_at >= today)
    )
    queries_today = (await db.execute(query_count_stmt)).scalar() or 0

    # Get connection count
    conn_count_stmt = (
        select(func.count()).select_from(DatabaseConnection)
        .where(DatabaseConnection.organization_id == org_id)
    )
    connections_used = (await db.execute(conn_count_stmt)).scalar() or 0

    # Get monthly report count
    month_start = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    report_count_stmt = (
        select(func.count()).select_from(Report)
        .where(Report.organization_id == org_id, Report.created_at >= month_start)
    )
    reports_this_month = (await db.execute(report_count_stmt)).scalar() or 0

    # Get seat count
    from app.db.models import User as UserModel
    seat_count_stmt = (
        select(func.count()).select_from(UserModel)
        .where(UserModel.organization_id == org_id)
    )
    seats_used = (await db.execute(seat_count_stmt)).scalar() or 0

    # Get feature flags
    from app.core.features import get_all_features
    features = await get_all_features(db, org_id)

    return {
        "planName": plan_name,
        "usage": {
            "queriesToday": {"used": queries_today, "limit": max_queries},
            "connections": {"used": connections_used, "limit": max_connections},
            "reportsThisMonth": {"used": reports_this_month, "limit": max_reports},
            "seats": {"used": seats_used, "limit": max_seats},
        },
        "features": features,
        "upgrades": {
            "starter": {"price": "$79/mo", "queries": 100, "connections": 3, "reports": 30, "seats": 3},
            "business": {"price": "$249/mo", "queries": 500, "connections": 10, "reports": -1, "seats": 10},
        },
    }


@router.get("/me/permissions")
async def get_my_permissions(current_user: CurrentUser, db: DbSession) -> dict:
    """Return the current user's role and permissions."""
    user = await get_user_with_role(db, current_user.user_id)
    perms = get_permissions(user.role)
    return {
        "role": user.role,
        "isSuperAdmin": user.is_super_admin,
        "permissions": sorted([p.value for p in perms]),
    }
