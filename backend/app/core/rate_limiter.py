"""Simple in-memory rate limiter for API abuse prevention.

Not distributed — for single-server deployments. Use Redis for multi-server.
"""

from __future__ import annotations

import time
import logging
from collections import defaultdict

from fastapi import HTTPException, status

logger = logging.getLogger(__name__)

_requests: dict[str, list[float]] = defaultdict(list)
_CLEANUP_INTERVAL = 300
_last_cleanup = time.time()


def check_rate_limit(
    key: str,
    max_requests: int = 100,
    window_seconds: int = 60,
) -> None:
    """Check if a key has exceeded the rate limit.

    Args:
        key: Unique identifier (e.g., user_id, IP, org_id)
        max_requests: Maximum requests allowed in the window
        window_seconds: Time window in seconds
    """
    global _last_cleanup

    now = time.time()
    cutoff = now - window_seconds

    if now - _last_cleanup > _CLEANUP_INTERVAL:
        _cleanup(cutoff)
        _last_cleanup = now

    _requests[key] = [t for t in _requests[key] if t > cutoff]

    if len(_requests[key]) >= max_requests:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Too many requests. Please wait before trying again.",
        )

    _requests[key].append(now)


def _cleanup(cutoff: float) -> None:
    """Remove expired entries from all keys."""
    keys_to_delete = []
    for key in _requests:
        _requests[key] = [t for t in _requests[key] if t > cutoff]
        if not _requests[key]:
            keys_to_delete.append(key)
    for key in keys_to_delete:
        del _requests[key]
