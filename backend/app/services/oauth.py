"""OAuth2 helpers for Meta Ads and Google Ads integrations."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.core.config import get_settings
from app.services.encryption import decrypt_value, encrypt_value

logger = logging.getLogger(__name__)


# ── Meta (Facebook) OAuth ────────────────────────────────────────────────────

META_AUTH_URL = "https://www.facebook.com/v21.0/dialog/oauth"
META_TOKEN_URL = "https://graph.facebook.com/v21.0/oauth/access_token"
META_GRAPH_URL = "https://graph.facebook.com/v21.0"


def meta_auth_url(state: str) -> str:
    """Build the Meta OAuth consent URL."""
    settings = get_settings()
    params = {
        "client_id": settings.meta_app_id,
        "redirect_uri": settings.meta_redirect_uri,
        "scope": "ads_read,ads_management,business_management",
        "response_type": "code",
        "state": state,
    }
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{META_AUTH_URL}?{qs}"


async def meta_exchange_code(code: str) -> dict[str, Any]:
    """Exchange auth code for access token."""
    settings = get_settings()
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            META_TOKEN_URL,
            params={
                "client_id": settings.meta_app_id,
                "client_secret": settings.meta_app_secret,
                "redirect_uri": settings.meta_redirect_uri,
                "code": code,
            },
        )
        resp.raise_for_status()
        data = resp.json()

    # Exchange short-lived token for long-lived (60 days)
    short_token = data["access_token"]
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            META_TOKEN_URL,
            params={
                "grant_type": "fb_exchange_token",
                "client_id": settings.meta_app_id,
                "client_secret": settings.meta_app_secret,
                "fb_exchange_token": short_token,
            },
        )
        resp.raise_for_status()
        long_data = resp.json()

    return {
        "access_token": long_data["access_token"],
        "expires_in": long_data.get("expires_in", 5184000),  # 60 days default
    }


async def meta_get_ad_accounts(access_token: str) -> list[dict[str, str]]:
    """Fetch ad accounts the user has access to."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{META_GRAPH_URL}/me/adaccounts",
            params={
                "access_token": access_token,
                "fields": "id,name,account_status,currency,timezone_name",
                "limit": 100,
            },
        )
        resp.raise_for_status()
        data = resp.json()
    return data.get("data", [])


async def meta_refresh_token(access_token: str) -> dict[str, Any]:
    """Refresh a long-lived Meta token (returns new token valid for 60 days)."""
    settings = get_settings()
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            META_TOKEN_URL,
            params={
                "grant_type": "fb_exchange_token",
                "client_id": settings.meta_app_id,
                "client_secret": settings.meta_app_secret,
                "fb_exchange_token": access_token,
            },
        )
        resp.raise_for_status()
        return resp.json()


# ── Google Ads OAuth ─────────────────────────────────────────────────────────

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"


def google_auth_url(state: str) -> str:
    """Build the Google OAuth consent URL."""
    settings = get_settings()
    params = {
        "client_id": settings.google_ads_client_id,
        "redirect_uri": settings.google_ads_redirect_uri,
        "scope": "https://www.googleapis.com/auth/adwords",
        "response_type": "code",
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{GOOGLE_AUTH_URL}?{qs}"


async def google_exchange_code(code: str) -> dict[str, Any]:
    """Exchange auth code for access + refresh tokens."""
    settings = get_settings()
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            GOOGLE_TOKEN_URL,
            data={
                "client_id": settings.google_ads_client_id,
                "client_secret": settings.google_ads_client_secret,
                "redirect_uri": settings.google_ads_redirect_uri,
                "code": code,
                "grant_type": "authorization_code",
            },
        )
        resp.raise_for_status()
        return resp.json()


async def google_refresh_access_token(refresh_token: str) -> dict[str, Any]:
    """Refresh a Google access token using the refresh token."""
    settings = get_settings()
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            GOOGLE_TOKEN_URL,
            data={
                "client_id": settings.google_ads_client_id,
                "client_secret": settings.google_ads_client_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            },
        )
        resp.raise_for_status()
        return resp.json()


async def google_get_accessible_customers(access_token: str) -> list[str]:
    """Fetch customer IDs (ad accounts) the user has access to."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            "https://googleads.googleapis.com/v18/customers:listAccessibleCustomers",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        resp.raise_for_status()
        data = resp.json()
    return data.get("resourceNames", [])


# ── Shared helpers ───────────────────────────────────────────────────────────


def store_tokens(
    connection,
    access_token: str,
    refresh_token: str | None = None,
    expires_in: int = 3600,
) -> None:
    """Encrypt and store OAuth tokens on a DatabaseConnection."""
    connection.oauth_access_token = encrypt_value(access_token)
    if refresh_token:
        connection.oauth_refresh_token = encrypt_value(refresh_token)
    connection.oauth_expires_at = datetime.now(UTC) + timedelta(seconds=expires_in)
    connection.is_connected = True


def get_access_token(connection) -> str:
    """Decrypt and return the stored access token."""
    if not connection.oauth_access_token:
        raise ValueError("No access token stored for this connection")
    return decrypt_value(connection.oauth_access_token)


def get_refresh_token(connection) -> str | None:
    """Decrypt and return the stored refresh token."""
    if not connection.oauth_refresh_token:
        return None
    return decrypt_value(connection.oauth_refresh_token)


def is_token_expired(connection) -> bool:
    """Check if the stored token has expired."""
    if not connection.oauth_expires_at:
        return True
    return datetime.now(UTC) >= connection.oauth_expires_at
