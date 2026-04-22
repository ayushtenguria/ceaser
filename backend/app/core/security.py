"""Clerk JWT verification and JWKS caching."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

import httpx
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

from app.core.config import Settings, get_settings

logger = logging.getLogger(__name__)

_bearer_scheme = HTTPBearer(auto_error=False)


_JWKS_CACHE_TTL_SECONDS = 3600


@dataclass
class _JwksCache:
    """In-memory cache for the Clerk JSON Web Key Set."""

    keys: list[dict[str, Any]] = field(default_factory=list)
    fetched_at: float = 0.0


_jwks_cache = _JwksCache()


async def _fetch_jwks(jwks_url: str) -> list[dict[str, Any]]:
    """Fetch JWKS from Clerk and update the module-level cache."""
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(jwks_url)
        response.raise_for_status()
        data = response.json()
    keys: list[dict[str, Any]] = data.get("keys", [])
    _jwks_cache.keys = keys
    _jwks_cache.fetched_at = time.time()
    logger.info("Refreshed JWKS cache (%d key(s))", len(keys))
    return keys


async def _get_jwks(settings: Settings) -> list[dict[str, Any]]:
    """Return cached JWKS, refreshing if stale."""
    if time.time() - _jwks_cache.fetched_at > _JWKS_CACHE_TTL_SECONDS or not _jwks_cache.keys:
        return await _fetch_jwks(settings.clerk_jwks_url)
    return _jwks_cache.keys


@dataclass
class AuthenticatedUser:
    """Lightweight representation of the authenticated caller."""

    user_id: str
    org_id: str | None


async def verify_token(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
    settings: Settings = Depends(get_settings),
) -> AuthenticatedUser:
    """Decode and verify a Clerk-issued JWT.

    Returns an ``AuthenticatedUser`` with the subject (user_id) and optional
    org_id extracted from token claims.

    Raises ``HTTPException(401)`` on any verification failure.
    """
    if settings.clerk_jwks_url.startswith("https://your-clerk"):
        if not settings.dev_mode:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Clerk JWKS URL not configured.",
            )
        logger.warning("DEV_MODE: Using dev user (Clerk not configured).")
        return AuthenticatedUser(user_id="dev_user", org_id="dev_org")

    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authorization header.",
        )

    token = credentials.credentials

    try:
        jwks = await _get_jwks(settings)
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get("kid")
        rsa_key: dict[str, Any] = {}
        for key in jwks:
            if key.get("kid") == kid:
                rsa_key = key
                break

        if not rsa_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unable to find matching signing key.",
            )

        payload = jwt.decode(
            token,
            rsa_key,
            algorithms=["RS256"],
            options={"verify_aud": False},
        )

        user_id: str | None = payload.get("sub")
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token missing 'sub' claim.",
            )

        org_id: str | None = payload.get("org_id")

        return AuthenticatedUser(user_id=user_id, org_id=org_id)

    except JWTError as exc:
        logger.warning("JWT verification failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token.",
        ) from exc
