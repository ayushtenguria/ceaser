"""Role-based permission checking for API endpoints."""

from __future__ import annotations

from enum import Enum
from functools import wraps
from typing import Any

from fastapi import HTTPException, status

from app.core.deps import CurrentUser, DbSession
from app.db.models import User
from sqlalchemy import select


class Permission(str, Enum):
    """Available permissions in the platform."""
    QUERY_DATA = "query_data"
    VIEW_DATA = "view_data"
    SAVE_REPORTS = "save_reports"
    MANAGE_CONNECTIONS = "manage_connections"
    MANAGE_METRICS = "manage_metrics"
    UPLOAD_FILES = "upload_files"
    DELETE_FILES = "delete_files"
    VIEW_AUDIT = "view_audit"
    INVITE_USERS = "invite_users"
    ADMIN_DASHBOARD = "admin_dashboard"
    MANAGE_ORGS = "manage_orgs"


ROLE_PERMISSIONS: dict[str, set[Permission]] = {
    "super_admin": set(Permission),
    "admin": {
        Permission.QUERY_DATA,
        Permission.VIEW_DATA,
        Permission.SAVE_REPORTS,
        Permission.MANAGE_CONNECTIONS,
        Permission.MANAGE_METRICS,
        Permission.UPLOAD_FILES,
        Permission.DELETE_FILES,
        Permission.VIEW_AUDIT,
        Permission.INVITE_USERS,
    },
    "member": {
        Permission.QUERY_DATA,
        Permission.VIEW_DATA,
        Permission.SAVE_REPORTS,
        Permission.UPLOAD_FILES,
    },
    "viewer": {
        Permission.VIEW_DATA,
    },
}


def get_permissions(role: str) -> set[Permission]:
    """Get the set of permissions for a given role."""
    return ROLE_PERMISSIONS.get(role, ROLE_PERMISSIONS["viewer"])


def has_permission(role: str, permission: Permission) -> bool:
    """Check if a role has a specific permission."""
    return permission in get_permissions(role)


async def get_user_with_role(db: DbSession, clerk_id: str) -> User:
    """Fetch user and return. Raises 404 if not found."""
    stmt = select(User).where(User.clerk_id == clerk_id)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()
    if user is None:
        from app.core.config import get_settings
        settings = get_settings()
        if settings.dev_mode and clerk_id == "dev_user":
            user = User(
                clerk_id="dev_user",
                email="admin@ceaser.local",
                first_name="Dev", last_name="User",
                organization_id="dev_org",
                role="super_admin", is_super_admin=True,
            )
            db.add(user)
            await db.flush()
            await db.refresh(user)
            return user
        raise HTTPException(status_code=404, detail="User not found. Please sign in again.")
    return user


async def require_permission(
    permission: Permission,
    current_user: CurrentUser,
    db: DbSession,
) -> User:
    """Check if the current user has the required permission. Returns the User if allowed."""
    user = await get_user_with_role(db, current_user.user_id)

    if not has_permission(user.role, permission):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Your role '{user.role}' does not have permission: {permission.value}. "
                   f"Contact your organization admin to upgrade your access.",
        )
    return user
