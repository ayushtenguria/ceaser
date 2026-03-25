"""Super Admin API — manage organizations, users, and platform settings."""

from __future__ import annotations

import logging
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select, func

from app.api.schemas import _CamelModel
from app.core.config import get_settings
from app.core.deps import CurrentUser, DbSession
from app.db.models import AuditLog, Conversation, DatabaseConnection, Message, Report, User

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["admin"])

_CLERK_API = "https://api.clerk.com/v1"


def _clerk_headers() -> dict[str, str]:
    settings = get_settings()
    return {
        "Authorization": f"Bearer {settings.clerk_secret_key}",
        "Content-Type": "application/json",
    }


async def _require_admin(current_user: CurrentUser, db: DbSession) -> User:
    """Check if the current user is a super admin. Raises 403 if not."""
    settings = get_settings()
    # Look up user email
    stmt = select(User).where(User.clerk_id == current_user.user_id)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()

    if user is None:
        if settings.dev_mode and current_user.user_id == "dev_user":
            user = User(
                clerk_id="dev_user", email=settings.super_admin_emails[0] if settings.super_admin_emails else "admin@ceaser.local",
                first_name="Admin", last_name="(Dev)",
                organization_id="dev_org",
                role="super_admin", is_super_admin=True,
            )
            db.add(user)
            await db.flush()
            await db.refresh(user)
            return user
        raise HTTPException(status_code=403, detail="Not authorized. Admin account not found.")

    # Check both DB flag and config list
    if not user.is_super_admin and user.email not in settings.super_admin_emails:
        raise HTTPException(status_code=403, detail="Super admin access required.")

    # Auto-upgrade if in config but not yet flagged in DB
    if not user.is_super_admin and user.email in settings.super_admin_emails:
        user.is_super_admin = True
        user.role = "super_admin"
        await db.flush()

    return user


# ---------------------------------------------------------------------------
# Dashboard stats
# ---------------------------------------------------------------------------

@router.get("/stats")
async def get_platform_stats(current_user: CurrentUser, db: DbSession) -> dict:
    """Platform-wide stats for the admin dashboard."""
    admin = await _require_admin(current_user, db)

    total_users = (await db.execute(select(func.count()).select_from(User))).scalar() or 0
    total_conversations = (await db.execute(select(func.count()).select_from(Conversation))).scalar() or 0
    total_messages = (await db.execute(select(func.count()).select_from(Message))).scalar() or 0
    total_connections = (await db.execute(select(func.count()).select_from(DatabaseConnection))).scalar() or 0
    total_reports = (await db.execute(select(func.count()).select_from(Report))).scalar() or 0

    # Get distinct organizations
    orgs_result = await db.execute(
        select(User.organization_id, func.count(User.id))
        .where(User.organization_id.isnot(None))
        .group_by(User.organization_id)
    )
    orgs = [{"org_id": row[0], "user_count": row[1]} for row in orgs_result.all()]

    return {
        "totalUsers": total_users,
        "totalConversations": total_conversations,
        "totalMessages": total_messages,
        "totalConnections": total_connections,
        "totalReports": total_reports,
        "organizations": orgs,
    }


# ---------------------------------------------------------------------------
# Organization management (via Clerk API)
# ---------------------------------------------------------------------------

class OrgCreate(_CamelModel):
    name: str
    slug: str = ""

class InviteUser(_CamelModel):
    email: str
    role: str = "basic_member"  # or "admin"


@router.post("/organizations")
async def create_organization(body: OrgCreate, current_user: CurrentUser, db: DbSession) -> dict:
    """Create a new organization in Clerk."""
    await _require_admin(current_user, db)

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{_CLERK_API}/organizations",
            headers=_clerk_headers(),
            json={"name": body.name},
        )
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.json())
        return resp.json()


@router.get("/organizations")
async def list_organizations(current_user: CurrentUser, db: DbSession) -> list[dict]:
    """List all organizations from Clerk."""
    await _require_admin(current_user, db)

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{_CLERK_API}/organizations?limit=100",
            headers=_clerk_headers(),
        )
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.json())
        data = resp.json()
        return data.get("data", data) if isinstance(data, dict) else data


@router.post("/organizations/{org_id}/invite")
async def invite_user(org_id: str, body: InviteUser, current_user: CurrentUser, db: DbSession) -> dict:
    """Invite a user to an organization via Clerk."""
    await _require_admin(current_user, db)

    # Check seat limit
    from app.db.models import OrganizationPlan
    plan_stmt = select(OrganizationPlan).where(OrganizationPlan.organization_id == org_id)
    plan_result = await db.execute(plan_stmt)
    plan = plan_result.scalar_one_or_none()

    if plan:
        # Count current members
        member_count_stmt = select(func.count()).select_from(User).where(User.organization_id == org_id)
        member_count = (await db.execute(member_count_stmt)).scalar() or 0

        if member_count >= plan.max_seats:
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                detail=f"Seat limit reached ({member_count}/{plan.max_seats}). Upgrade your plan to add more users.",
            )

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{_CLERK_API}/organizations/{org_id}/invitations",
            headers=_clerk_headers(),
            json={"email_address": body.email, "role": body.role},
        )
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.json())
        return resp.json()


# ---------------------------------------------------------------------------
# User management
# ---------------------------------------------------------------------------

@router.get("/users")
async def list_all_users(current_user: CurrentUser, db: DbSession) -> list[dict]:
    """List all users in the platform (from our DB)."""
    await _require_admin(current_user, db)

    stmt = select(User).order_by(User.created_at.desc())
    result = await db.execute(stmt)
    users = result.scalars().all()

    return [
        {
            "id": str(u.id),
            "email": u.email,
            "firstName": u.first_name,
            "lastName": u.last_name,
            "organizationId": u.organization_id,
            "createdAt": u.created_at.isoformat() if u.created_at else None,
        }
        for u in users
    ]


# ---------------------------------------------------------------------------
# Plan management
# ---------------------------------------------------------------------------

class PlanUpdate(_CamelModel):
    plan_name: str | None = None
    max_seats: int | None = None
    max_connections: int | None = None
    max_queries_per_day: int | None = None
    max_reports: int | None = None


@router.get("/organizations/{org_id}/plan")
async def get_org_plan(org_id: str, current_user: CurrentUser, db: DbSession) -> dict:
    """Get an organization's current plan."""
    await _require_admin(current_user, db)

    from app.db.models import OrganizationPlan
    stmt = select(OrganizationPlan).where(OrganizationPlan.organization_id == org_id)
    result = await db.execute(stmt)
    plan = result.scalar_one_or_none()

    if plan is None:
        # Create default free plan
        plan = OrganizationPlan(organization_id=org_id, plan_name="free", max_seats=5)
        db.add(plan)
        await db.flush()
        await db.refresh(plan)

    from app.core.features import get_all_features
    features = await get_all_features(db, org_id)

    return {
        "organizationId": org_id,
        "planName": plan.plan_name,
        "maxSeats": plan.max_seats,
        "maxConnections": plan.max_connections,
        "maxQueriesPerDay": plan.max_queries_per_day,
        "maxReports": plan.max_reports,
        "features": features,
        "isActive": plan.is_active,
        "trialEndsAt": plan.trial_ends_at.isoformat() if plan.trial_ends_at else None,
    }


@router.patch("/organizations/{org_id}/plan")
async def update_org_plan(org_id: str, body: PlanUpdate, current_user: CurrentUser, db: DbSession) -> dict:
    """Update an organization's plan (super admin only)."""
    await _require_admin(current_user, db)

    from app.db.models import OrganizationPlan
    stmt = select(OrganizationPlan).where(OrganizationPlan.organization_id == org_id)
    result = await db.execute(stmt)
    plan = result.scalar_one_or_none()

    if plan is None:
        plan = OrganizationPlan(organization_id=org_id)
        db.add(plan)

    if body.plan_name is not None: plan.plan_name = body.plan_name
    if body.max_seats is not None: plan.max_seats = body.max_seats
    if body.max_connections is not None: plan.max_connections = body.max_connections
    if body.max_queries_per_day is not None: plan.max_queries_per_day = body.max_queries_per_day
    if body.max_reports is not None: plan.max_reports = body.max_reports

    await db.flush()
    await db.refresh(plan)

    from app.core.features import get_all_features
    features = await get_all_features(db, org_id)

    return {
        "organizationId": org_id,
        "planName": plan.plan_name,
        "maxSeats": plan.max_seats,
        "maxConnections": plan.max_connections,
        "maxQueriesPerDay": plan.max_queries_per_day,
        "maxReports": plan.max_reports,
        "features": features,
        "isActive": plan.is_active,
    }


@router.patch("/organizations/{org_id}/features")
async def update_org_features(org_id: str, body: dict, current_user: CurrentUser, db: DbSession) -> dict:
    """Toggle feature flags per org (super admin only).

    Body: {"notebooks": true, "advanced_analytics": false, ...}
    Only specified features are overridden; omitted ones fall back to plan defaults.
    Pass null to remove an override.
    """
    await _require_admin(current_user, db)

    from app.db.models import OrganizationPlan
    from app.core.features import Feature, get_all_features

    # Validate feature names
    valid_features = {f.value for f in Feature}
    for key in body:
        if key not in valid_features:
            raise HTTPException(status_code=400, detail=f"Unknown feature: '{key}'. Valid: {sorted(valid_features)}")

    stmt = select(OrganizationPlan).where(OrganizationPlan.organization_id == org_id)
    result = await db.execute(stmt)
    plan = result.scalar_one_or_none()
    if plan is None:
        raise HTTPException(status_code=404, detail="Organization plan not found.")

    # Merge with existing overrides
    current = plan.features or {}
    for key, value in body.items():
        if value is None:
            current.pop(key, None)  # Remove override → fall back to plan default
        else:
            current[key] = bool(value)

    plan.features = current
    from sqlalchemy.orm.attributes import flag_modified
    flag_modified(plan, "features")
    await db.flush()

    features = await get_all_features(db, org_id)
    return {"organizationId": org_id, "features": features, "overrides": current}
