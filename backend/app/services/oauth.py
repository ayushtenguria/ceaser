"""Token refresh helpers for Meta Ads and Google Ads connectors.

Tokens are stored encrypted in DatabaseConnection.encrypted_password.
These helpers handle refreshing expired tokens via the platform APIs.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


async def meta_refresh_token(access_token: str, app_id: str, app_secret: str) -> dict[str, Any]:
    """Refresh a Meta long-lived token. Returns new token valid for 60 days."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            "https://graph.facebook.com/v21.0/oauth/access_token",
            params={
                "grant_type": "fb_exchange_token",
                "client_id": app_id,
                "client_secret": app_secret,
                "fb_exchange_token": access_token,
            },
        )
        resp.raise_for_status()
        return resp.json()


async def google_refresh_access_token(
    refresh_token: str, client_id: str, client_secret: str
) -> dict[str, Any]:
    """Refresh a Google access token using the refresh token."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            },
        )
        resp.raise_for_status()
        return resp.json()
