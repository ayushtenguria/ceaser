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
from app.core.permissions import Permission, require_permission
from app.db.models import FileUpload, User
from app.services.file_parser import parse_file

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/files", tags=["files"])

_UPLOAD_DIR = Path(__file__).resolve().parent.parent.parent / "uploads"
_MAX_FILE_SIZE = 100 * 1024 * 1024

_ALLOWED_EXTENSIONS: dict[str, str] = {
    ".csv": "csv",
    ".xlsx": "excel",
    ".xls": "excel",
    ".tsv": "tsv",
    ".gct": "genomics",
    ".tab": "tsv",
}


async def _get_user(db: DbSession, clerk_id: str) -> User:
    """Fetch user by clerk_id, auto-creating in dev mode if needed."""
    stmt = select(User).where(User.clerk_id == clerk_id)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()
    if user is None:
        from app.core.config import get_settings
        if get_settings().dev_mode and clerk_id == "dev_user":
            user = User(
                clerk_id="dev_user",
                email=get_settings().dev_fallback_email,
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
    user = await require_permission(Permission.UPLOAD_FILES, current_user, db)

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

    from app.services.storage import get_storage

    clean_filename = Path(file.filename).name
    safe_name = f"{uuid.uuid4().hex}_{clean_filename}"
    org_id = user.organization_id or "default"
    remote_path = f"uploads/{org_id}/{user.id}/{safe_name}"

    contents = await file.read()
    if len(contents) > _MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="File exceeds 100 MB limit.")

    storage = get_storage()
    stored_path = await storage.upload(contents, remote_path)
    logger.info("Saved uploaded file: %s (%d bytes)", stored_path, len(contents))

    local_path = await storage.download_url(remote_path)
    dest_path = Path(local_path) if not local_path.startswith("http") else None

    import tempfile
    _temp_dir = None
    parse_path = str(dest_path) if dest_path else ""
    if not dest_path:
        _temp_dir = tempfile.mkdtemp(prefix="ceaser_upload_")
        parse_path = str(Path(_temp_dir) / safe_name)
        Path(parse_path).write_bytes(contents)

    column_info: dict | None = None
    try:
        _, column_info = parse_file(parse_path, file_type)
    except Exception as exc:
        logger.warning("Could not parse uploaded file: %s", exc)

    excel_context = None
    code_preamble = None
    parquet_paths_data = None
    excel_metadata = None

    # Detect if this is a genomics file (TSV/GCT or CSV with gene IDs)
    is_genomics = file_type in ("genomics", "tsv")
    if not is_genomics and file_type == "csv":
        try:
            from app.agents.genomics.detector import detect_format
            detection = detect_format(parse_path)
            is_genomics = detection.get("file_format") in ("count_matrix", "deseq2_result", "gct")
        except Exception:
            pass

    if is_genomics:
        try:
            from app.core.features import check_feature, Feature
            await check_feature(Feature.GENOMICS, db, org_id)

            from app.agents.genomics.orchestrator import process_genomics_upload
            from app.core.deps import get_llm
            llm = get_llm()
            genomics_result = await process_genomics_upload(parse_path, llm, org_id=org_id)
            excel_context = genomics_result.get("genomics_context")
            code_preamble = genomics_result.get("code_preamble")
            parquet_paths_data = genomics_result.get("parquet_paths")
            excel_metadata = {
                "genomics": True,
                "insight": genomics_result.get("insight"),
                "quality_report": genomics_result.get("quality_report"),
                "gene_count": genomics_result.get("gene_count"),
                "sample_count": genomics_result.get("sample_count"),
            }
            logger.info("Genomics processing complete for %s", file.filename)
        except Exception as exc:
            logger.warning("Genomics processing failed, falling back to Excel: %s", exc)
            is_genomics = False  # Fall through to Excel pipeline

    if not is_genomics:
        try:
            from app.agents.excel.orchestrator import process_excel_upload
            from app.core.deps import get_llm
            llm = get_llm()
            excel_result = await process_excel_upload(parse_path, llm, org_id=org_id)
            excel_context = excel_result.get("excel_context")
            code_preamble = excel_result.get("code_preamble")
            parquet_paths_data = excel_result.get("parquet_paths")
            excel_metadata = {
                "insight": excel_result.get("insight"),
                "quality_report": excel_result.get("quality_report"),
                "relationships": excel_result.get("relationships"),
            }
            logger.info("Excel processing complete for %s", file.filename)
        except Exception as exc:
            logger.warning("Excel processing failed (file still saved): %s", exc)

    if _temp_dir:
        import shutil
        shutil.rmtree(_temp_dir, ignore_errors=True)

    upload = FileUpload(
        filename=file.filename,
        file_type=file_type,
        file_path=remote_path,
        size_bytes=len(contents),
        organization_id=user.organization_id or current_user.org_id or "",
        user_id=user.id,
        column_info=column_info,
        excel_context=excel_context,
        code_preamble=code_preamble,
        parquet_paths=parquet_paths_data,
        excel_metadata=excel_metadata,
    )
    db.add(upload)
    await db.flush()
    await db.refresh(upload)

    try:
        from app.services.schema_graph import build_file_graph
        await build_file_graph(
            file_id=str(upload.id),
            org_id=upload.organization_id,
            filename=file.filename,
            conversation_id=None,
            uploaded_by=str(user.id),
            column_info=column_info,
            parquet_paths=parquet_paths_data,
            row_count=column_info.get("row_count", 0) if column_info else 0,
        )
    except Exception as exc:
        logger.warning("File graph build failed (non-blocking): %s", exc)

    return upload


@router.get("/", response_model=list[FileUploadResponse])
async def list_files(current_user: CurrentUser, db: DbSession) -> list[FileUpload]:
    """List all files uploaded by the current user."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    stmt = (
        select(FileUpload)
        .where(FileUpload.organization_id == (user.organization_id or current_user.org_id or ""))
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
    user = await require_permission(Permission.DELETE_FILES, current_user, db)
    stmt = select(FileUpload).where(FileUpload.id == file_id, FileUpload.user_id == user.id)
    result = await db.execute(stmt)
    upload = result.scalar_one_or_none()
    if upload is None:
        raise HTTPException(status_code=404, detail="File not found.")

    from app.services.storage import get_storage
    try:
        storage = get_storage()
        await storage.delete(upload.file_path)
        if upload.parquet_paths:
            for path in upload.parquet_paths.values():
                try:
                    await storage.delete(path)
                except Exception:
                    pass
        logger.info("Deleted file from storage: %s", upload.file_path)
    except Exception as exc:
        logger.warning("Could not delete file from storage: %s", exc)

    await db.delete(upload)
