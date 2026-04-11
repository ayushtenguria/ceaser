"""Enqueue files for async processing on AWS Lambda via SQS.

When ``settings.sqs_queue_url`` is empty, ``enqueue_file`` is a no-op so the
caller can fall back to inline processing for small files (useful in local dev
and as a safety net when the Lambda pipeline is unavailable).
"""

from __future__ import annotations

import json
import logging
from typing import Any

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_client = None


def _get_sqs():
    global _client
    if _client is None:
        import boto3

        _client = boto3.client("sqs", region_name=get_settings().aws_region)
    return _client


def enqueue_file(
    *,
    file_id: str,
    storage_url: str,
    file_type: str,
    filename: str,
    org_id: str,
) -> bool:
    """Send a processing job to SQS. Returns True if enqueued, False if disabled/failed."""
    queue_url = get_settings().sqs_queue_url
    if not queue_url:
        logger.debug("SQS queue not configured — skipping enqueue for %s", file_id)
        return False

    body = {
        "file_id": str(file_id),
        "storage_url": storage_url,
        "file_type": file_type,
        "filename": filename,
        "org_id": org_id,
    }
    try:
        _get_sqs().send_message(QueueUrl=queue_url, MessageBody=json.dumps(body))
        logger.info("Enqueued %s (%s) for processing", filename, file_id)
        return True
    except Exception as exc:
        logger.exception("SQS enqueue failed: %s", exc)
        return False


def verify_signature(body: bytes, signature: str) -> bool:
    """Verify the HMAC-SHA256 signature sent by the Lambda callback."""
    import hashlib
    import hmac

    secret = get_settings().hmac_shared_secret
    if not secret:
        logger.warning("hmac_shared_secret is empty — rejecting callback")
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature or "")


def get_signed_download_url(remote_path: str, expires_in: int = 3600) -> str | None:
    """Return a temporary URL the Lambda can use to fetch the file from Supabase."""
    try:
        from app.services.storage import get_storage

        storage = get_storage()
    except Exception:
        return None

    # Supabase storage exposes a `.sign_url()` helper; fall back to download_url.
    try:
        signer = getattr(storage, "sign_url", None)
        if signer is None:
            import asyncio

            return asyncio.get_event_loop().run_until_complete(
                storage.download_url(remote_path)
            )
        return signer(remote_path, expires_in)
    except Exception as exc:
        logger.warning("Could not sign download URL for %s: %s", remote_path, exc)
        return None
