"""Local filesystem storage backend — default for development."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from app.services.storage.base import StorageBackend

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent.parent.parent


class LocalStorage(StorageBackend):
    """Store files under backend/uploads/."""

    def __init__(self, root: Path | None = None):
        self._root = root or _ROOT

    def _full_path(self, remote_path: str) -> Path:
        if os.path.isabs(remote_path):
            return Path(remote_path)
        return self._root / remote_path

    async def upload(self, data: bytes, remote_path: str) -> str:
        path = self._full_path(remote_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        logger.info("Local upload: %s (%d bytes)", remote_path, len(data))
        return str(path)

    async def download_url(self, remote_path: str) -> str:
        """For local storage, return the absolute filesystem path."""
        path = self._full_path(remote_path)
        if path.exists():
            return str(path)
        if os.path.isabs(remote_path) and os.path.exists(remote_path):
            return remote_path
        raise FileNotFoundError(f"File not found: {remote_path}")

    async def delete(self, remote_path: str) -> None:
        path = self._full_path(remote_path)
        if path.exists():
            path.unlink()
            logger.info("Local delete: %s", remote_path)
