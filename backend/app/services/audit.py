"""Audit logging utility — call from API endpoints to record actions."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import AuditLog

logger = logging.getLogger(__name__)


async def log_action(
    db: AsyncSession,
    *,
    user_id: str | None = None,
    action: str,
    resource_type: str,
    resource_id: str = "",
    details: dict[str, Any] | None = None,
    ip_address: str = "",
) -> None:
    """Record an audit log entry."""
    from sqlalchemy import select
    from app.db.models import User

    # Resolve clerk_id to DB user UUID
    resolved_user_id = None
    if user_id:
        try:
            stmt = select(User.id).where(User.clerk_id == user_id)
            result = await db.execute(stmt)
            resolved_user_id = result.scalar_one_or_none()
        except Exception:
            pass

    entry = AuditLog(
        user_id=resolved_user_id,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        details=details,
        ip_address=ip_address,
    )
    db.add(entry)
    logger.debug("Audit: %s %s %s", action, resource_type, resource_id)
