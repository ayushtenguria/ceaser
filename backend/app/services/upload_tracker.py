"""Upload progress tracker — tracks file processing status for frontend polling.

In-memory store of upload processing stages. Frontend polls GET /files/{id}/status
to show progressive feedback: "Reading file... Detecting columns... 80% complete."

TTL: 10 minutes (auto-cleaned after processing completes).
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

_TTL_SECONDS = 600  # 10 minutes


class UploadProgress:
    """Track processing progress for a single file upload."""

    def __init__(self, file_id: str, filename: str):
        self.file_id = file_id
        self.filename = filename
        self.stage = "uploading"
        self.progress_pct = 0
        self.message = "Uploading file..."
        self.started_at = time.monotonic()
        self.completed = False
        self.error: str | None = None

    def update(self, stage: str, progress_pct: int, message: str) -> None:
        self.stage = stage
        self.progress_pct = min(progress_pct, 100)
        self.message = message

    def complete(self) -> None:
        self.stage = "done"
        self.progress_pct = 100
        self.message = "Processing complete"
        self.completed = True

    def fail(self, error: str) -> None:
        self.stage = "error"
        self.message = error
        self.error = error
        self.completed = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "fileId": self.file_id,
            "filename": self.filename,
            "stage": self.stage,
            "progressPct": self.progress_pct,
            "message": self.message,
            "completed": self.completed,
            "error": self.error,
            "elapsedSeconds": round(time.monotonic() - self.started_at, 1),
        }


# In-memory store
_active: dict[str, UploadProgress] = {}


def start_tracking(file_id: str, filename: str) -> UploadProgress:
    """Start tracking progress for a file upload."""
    # Clean up old entries
    _cleanup()
    progress = UploadProgress(file_id, filename)
    _active[file_id] = progress
    return progress


def get_progress(file_id: str) -> UploadProgress | None:
    """Get current progress for a file upload."""
    return _active.get(file_id)


def remove_tracking(file_id: str) -> None:
    """Remove tracking for a completed upload."""
    _active.pop(file_id, None)


def _cleanup() -> None:
    """Remove stale entries older than TTL."""
    now = time.monotonic()
    stale = [fid for fid, p in _active.items() if now - p.started_at > _TTL_SECONDS]
    for fid in stale:
        del _active[fid]
