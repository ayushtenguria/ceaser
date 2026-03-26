"""Abstract storage backend interface."""

from __future__ import annotations

import abc


class StorageBackend(abc.ABC):
    """Interface for file storage backends."""

    @abc.abstractmethod
    async def upload(self, data: bytes, remote_path: str) -> str:
        """Upload bytes to storage. Returns the canonical path/key."""

    @abc.abstractmethod
    async def download_url(self, remote_path: str) -> str:
        """Return a URL or local path that pandas can read directly.

        - Local backend: returns absolute filesystem path
        - Supabase backend: returns a signed URL (valid ~10 min)
        """

    @abc.abstractmethod
    async def delete(self, remote_path: str) -> None:
        """Delete a file from storage."""
