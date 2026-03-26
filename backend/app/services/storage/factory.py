"""Storage backend factory — returns the configured backend singleton."""

from __future__ import annotations

import logging

from app.services.storage.base import StorageBackend

logger = logging.getLogger(__name__)

_instance: StorageBackend | None = None


def get_storage() -> StorageBackend:
    """Return the configured storage backend (singleton)."""
    global _instance
    if _instance is not None:
        return _instance

    from app.core.config import get_settings
    settings = get_settings()
    backend = settings.storage_backend.lower()

    if backend == "supabase":
        from app.services.storage.supabase import SupabaseStorage
        _instance = SupabaseStorage(
            url=settings.supabase_url,
            service_key=settings.supabase_service_key,
            bucket=settings.supabase_bucket,
        )
        logger.info("Storage: Supabase (%s/%s)", settings.supabase_url, settings.supabase_bucket)
    else:
        from app.services.storage.local import LocalStorage
        _instance = LocalStorage()
        logger.info("Storage: local filesystem")

    return _instance
