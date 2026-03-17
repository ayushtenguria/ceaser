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
async def sync_user(payload: UserSyncRequest, db: DbSession) -> User:
    """Create or update a local user record from Clerk data.

    Called by the frontend after Clerk sign-in/sign-up, or by a Clerk webhook.
    """
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
