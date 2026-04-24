"""OAuth2 endpoints for Meta Ads and Google Ads integrations."""

from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import RedirectResponse
from sqlalchemy import select

from app.core.deps import CurrentUser, DbSession
from app.core.permissions import Permission, require_permission
from app.db.models import DatabaseConnection
from app.services.oauth import (
    google_auth_url,
    google_exchange_code,
    google_get_accessible_customers,
    meta_auth_url,
    meta_exchange_code,
    meta_get_ad_accounts,
    store_tokens,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/oauth", tags=["oauth"])


# ── Meta Ads ─────────────────────────────────────────────────────────────────


@router.get("/meta/initiate")
async def meta_initiate(
    connection_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> dict:
    """Start Meta OAuth flow. Returns the consent URL."""
    await require_permission(Permission.MANAGE_CONNECTIONS, current_user, db)

    stmt = select(DatabaseConnection).where(DatabaseConnection.id == connection_id)
    result = await db.execute(stmt)
    connection = result.scalar_one_or_none()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    url = meta_auth_url(state=str(connection_id))
    return {"authUrl": url}


@router.get("/meta/callback")
async def meta_callback(
    code: str = Query(...),
    state: str = Query(...),
    db: DbSession = ...,
) -> RedirectResponse:
    """Handle Meta OAuth callback — exchange code for tokens."""
    try:
        connection_id = uuid.UUID(state)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid state parameter")

    stmt = select(DatabaseConnection).where(DatabaseConnection.id == connection_id)
    result = await db.execute(stmt)
    connection = result.scalar_one_or_none()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    try:
        token_data = await meta_exchange_code(code)
        access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 5184000)

        store_tokens(connection, access_token=access_token, expires_in=expires_in)

        # Fetch ad accounts and store in schema_cache
        ad_accounts = await meta_get_ad_accounts(access_token)
        if ad_accounts:
            connection.database = ad_accounts[0]["id"]
        connection.schema_cache = {"ad_accounts": ad_accounts}

        await db.flush()
        logger.info(
            "Meta OAuth complete for connection %s: %d ad accounts", connection_id, len(ad_accounts)
        )

    except Exception as exc:
        logger.exception("Meta OAuth failed: %s", exc)
        raise HTTPException(status_code=400, detail=f"OAuth failed: {exc}")

    from app.core.config import get_settings

    frontend_url = (
        get_settings().cors_origins[0] if get_settings().cors_origins else "http://localhost:5173"
    )
    return RedirectResponse(url=f"{frontend_url}/connections?oauth=success&provider=meta")


# ── Google Ads ───────────────────────────────────────────────────────────────


@router.get("/google/initiate")
async def google_initiate(
    connection_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> dict:
    """Start Google Ads OAuth flow. Returns the consent URL."""
    await require_permission(Permission.MANAGE_CONNECTIONS, current_user, db)

    stmt = select(DatabaseConnection).where(DatabaseConnection.id == connection_id)
    result = await db.execute(stmt)
    connection = result.scalar_one_or_none()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    url = google_auth_url(state=str(connection_id))
    return {"authUrl": url}


@router.get("/google/callback")
async def google_callback(
    code: str = Query(...),
    state: str = Query(...),
    db: DbSession = ...,
) -> RedirectResponse:
    """Handle Google Ads OAuth callback — exchange code for tokens."""
    try:
        connection_id = uuid.UUID(state)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid state parameter")

    stmt = select(DatabaseConnection).where(DatabaseConnection.id == connection_id)
    result = await db.execute(stmt)
    connection = result.scalar_one_or_none()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    try:
        token_data = await google_exchange_code(code)
        access_token = token_data["access_token"]
        refresh_token = token_data.get("refresh_token")
        expires_in = token_data.get("expires_in", 3600)

        store_tokens(
            connection,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=expires_in,
        )

        # Fetch accessible customer IDs
        customers = await google_get_accessible_customers(access_token)
        if customers:
            connection.database = customers[0].split("/")[-1]
        connection.schema_cache = {"customers": customers}

        await db.flush()
        logger.info(
            "Google OAuth complete for connection %s: %d customers", connection_id, len(customers)
        )

    except Exception as exc:
        logger.exception("Google OAuth failed: %s", exc)
        raise HTTPException(status_code=400, detail=f"OAuth failed: {exc}")

    from app.core.config import get_settings

    frontend_url = (
        get_settings().cors_origins[0] if get_settings().cors_origins else "http://localhost:5173"
    )
    return RedirectResponse(url=f"{frontend_url}/connections?oauth=success&provider=google")
