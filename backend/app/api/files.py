"""File upload and management endpoints."""

from __future__ import annotations

import logging
import os
import uuid
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, status
from sqlalchemy import select

from app.api.schemas import FileUploadResponse
from app.core.deps import CurrentUser, DbSession
from app.db.models import FileUpload, User
from app.services.file_parser import parse_file

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/files", tags=["files"])

_UPLOAD_DIR = Path(__file__).resolve().parent.parent.parent / "uploads"
_MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB

_ALLOWED_EXTENSIONS: dict[str, str] = {
    ".csv": "csv",
    ".xlsx": "excel",
    ".xls": "excel",
}


async def _get_user(db: DbSession, clerk_id: str) -> User:
    """Fetch user by clerk_id, auto-creating in dev mode if needed."""
    stmt = select(User).where(User.clerk_id == clerk_id)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()
    if user is None:
        if clerk_id == "dev_user":
            user = User(
                clerk_id="dev_user",
                email="dev@ceaser.local",
                first_name="Dev",
                last_name="User",
                organization_id="dev_org",
            )
            db.add(user)
            await db.flush()
            await db.refresh(user)
            return user
        raise HTTPException(status_code=404, detail="User not found.")
    return user


@router.post("/upload", response_model=FileUploadResponse, status_code=status.HTTP_201_CREATED)
async def upload_file(
    file: UploadFile,
    current_user: CurrentUser,
    db: DbSession,
) -> FileUpload:
    """Upload a CSV or Excel file for analysis.

    The file is persisted to ``uploads/<user_id>/<uuid>_<filename>`` and its
    column metadata is extracted and stored.
    """
    user = await _get_user(db, current_user.user_id)

    # ── Validate ────────────────────────────────────────────────────
    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename is required.")

    ext = Path(file.filename).suffix.lower()
    file_type = _ALLOWED_EXTENSIONS.get(ext)
    if file_type is None:
        allowed = ", ".join(sorted(_ALLOWED_EXTENSIONS.keys()))
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{ext}'. Allowed: {allowed}",
        )

    # ── Save to disk ────────────────────────────────────────────────
    user_dir = _UPLOAD_DIR / str(user.id)
    user_dir.mkdir(parents=True, exist_ok=True)

    safe_name = f"{uuid.uuid4().hex}_{file.filename}"
    dest_path = user_dir / safe_name

    contents = await file.read()
    if len(contents) > _MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="File exceeds 100 MB limit.")

    dest_path.write_bytes(contents)
    logger.info("Saved uploaded file: %s (%d bytes)", dest_path, len(contents))

    # ── Parse & extract metadata ────────────────────────────────────
    column_info: dict | None = None
    try:
        _, column_info = parse_file(str(dest_path), file_type)
    except Exception as exc:
        logger.warning("Could not parse uploaded file: %s", exc)
        # File is saved; we just won't have column_info.

    # ── Persist record ──────────────────────────────────────────────
    upload = FileUpload(
        filename=file.filename,
        file_type=file_type,
        file_path=str(dest_path),
        size_bytes=len(contents),
        organization_id=current_user.org_id or "",
        user_id=user.id,
        column_info=column_info,
    )
    db.add(upload)
    await db.flush()
    await db.refresh(upload)
    return upload


@router.get("/", response_model=list[FileUploadResponse])
async def list_files(current_user: CurrentUser, db: DbSession) -> list[FileUpload]:
    """List all files uploaded by the current user."""
    user = await _get_user(db, current_user.user_id)
    stmt = (
        select(FileUpload)
        .where(FileUpload.user_id == user.id)
        .order_by(FileUpload.created_at.desc())
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.delete("/{file_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_file(
    file_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> None:
    """Delete an uploaded file (both DB record and disk file)."""
    user = await _get_user(db, current_user.user_id)
    stmt = select(FileUpload).where(FileUpload.id == file_id, FileUpload.user_id == user.id)
    result = await db.execute(stmt)
    upload = result.scalar_one_or_none()
    if upload is None:
        raise HTTPException(status_code=404, detail="File not found.")

    # Remove from disk.
    try:
        if os.path.exists(upload.file_path):
            os.remove(upload.file_path)
            logger.info("Deleted file from disk: %s", upload.file_path)
    except OSError as exc:
        logger.warning("Could not delete file from disk: %s", exc)

    await db.delete(upload)
