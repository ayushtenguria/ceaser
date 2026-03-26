"""Pluggable file storage — local filesystem or Supabase.

Configure via env:
    STORAGE_BACKEND=local        # default — uses uploads/ directory
    STORAGE_BACKEND=supabase     # uses Supabase Storage
"""

from app.services.storage.base import StorageBackend
from app.services.storage.factory import get_storage

__all__ = ["StorageBackend", "get_storage"]
