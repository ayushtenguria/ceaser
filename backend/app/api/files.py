"""File upload and management endpoints."""

from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request, UploadFile, status
from sqlalchemy import select

from app.api.schemas import FileUploadResponse
from app.core.deps import CurrentUser, DbSession
from app.core.permissions import Permission, require_permission
from app.db.models import FileUpload, User
from app.services.file_parser import parse_file
from app.services.processing_queue import verify_signature

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/files", tags=["files"])

_UPLOAD_DIR = Path(__file__).resolve().parent.parent.parent / "uploads"
_MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024
_UPLOAD_CHUNK = 1 * 1024 * 1024

_PREVIEW_ROWS = 50


def _generate_preview(file_path: str, file_type: str) -> dict | None:
    """Generate a data preview with first N rows + column statistics.

    Returns: {columns: [{name, dtype, null_count, unique_count, sample}], rows: [...], total_rows: N}
    """
    import pandas as pd

    try:
        if file_type in ("csv", "tsv"):
            sep = "\t" if file_type == "tsv" else ","
            df = pd.read_csv(file_path, sep=sep, nrows=_PREVIEW_ROWS + 1)
        elif file_type in ("excel",):
            df = pd.read_excel(file_path, nrows=_PREVIEW_ROWS + 1)
        else:
            return None
    except Exception:
        return None

    if df.empty:
        return None

    # Column stats
    col_stats = []
    for col in df.columns:
        col_stats.append(
            {
                "name": str(col),
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isna().sum()),
                "unique_count": int(df[col].nunique()),
                "sample": str(df[col].dropna().iloc[0]) if not df[col].dropna().empty else None,
            }
        )

    # Rows as list of dicts (preview only)
    preview_rows = df.head(_PREVIEW_ROWS).fillna("").to_dict(orient="records")
    for row in preview_rows:
        for k, v in row.items():
            if not isinstance(v, (str, int, float, bool, type(None))):
                row[k] = str(v)

    return {
        "columns": col_stats,
        "rows": preview_rows,
        "total_rows": len(df),
        "preview_rows": min(len(df), _PREVIEW_ROWS),
    }


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


async def _launch_fargate_task(
    file_id: str,
    s3_bucket: str,
    s3_key: str,
    org_id: str,
    settings: Any = None,
) -> None:
    """Launch a Fargate task to run the full Excel processing pipeline."""
    import asyncio

    import boto3

    if settings is None:
        from app.core.config import get_settings

        settings = get_settings()

    client = boto3.client("ecs", region_name=settings.aws_region)

    subnets = [s.strip() for s in settings.fargate_subnets.split(",") if s.strip()]
    security_groups = [settings.fargate_security_group] if settings.fargate_security_group else []

    await asyncio.to_thread(
        client.run_task,
        cluster=settings.fargate_cluster,
        taskDefinition=settings.fargate_task_definition,
        launchType="FARGATE",
        count=1,
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": subnets,
                "securityGroups": security_groups,
                "assignPublicIp": "ENABLED",
            }
        },
        overrides={
            "containerOverrides": [
                {
                    "name": "processor",
                    "command": ["python", "-m", "app.tasks.process_file"],
                    "environment": [
                        {"name": "FILE_ID", "value": file_id},
                        {"name": "S3_BUCKET", "value": s3_bucket},
                        {"name": "S3_KEY", "value": s3_key},
                        {"name": "ORG_ID", "value": org_id},
                    ],
                }
            ]
        },
    )


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
    db_org_id = user.organization_id or current_user.org_id or ""
    storage_org_id = db_org_id or "default"  # S3 paths can't have empty segments

    # Pre-assign the file_id so we can encode it into the S3 key — the Lambda
    # parses the file_id from the key when S3 notifies SQS of ObjectCreated.
    file_id = uuid.uuid4()
    remote_path = f"uploads/{storage_org_id}/{file_id}/{clean_filename}"

    # Stream the upload to a temp file on disk, aborting as soon as we exceed the cap.
    # This prevents OOM on the 2 GB instance when users upload huge files.
    import tempfile

    _temp_dir = tempfile.mkdtemp(prefix="ceaser_upload_")
    parse_path = str(Path(_temp_dir) / clean_filename)
    total_bytes = 0
    try:
        with open(parse_path, "wb") as out:
            while True:
                chunk = await file.read(_UPLOAD_CHUNK)
                if not chunk:
                    break
                total_bytes += len(chunk)
                if total_bytes > _MAX_FILE_SIZE:
                    import shutil

                    shutil.rmtree(_temp_dir, ignore_errors=True)
                    raise HTTPException(
                        status_code=413,
                        detail=(f"File exceeds {_MAX_FILE_SIZE // 1024 // 1024 // 1024} GB limit."),
                    )
                out.write(chunk)
    except HTTPException:
        raise
    except Exception as exc:
        import shutil

        shutil.rmtree(_temp_dir, ignore_errors=True)
        logger.exception("Upload stream failed: %s", exc)
        raise HTTPException(status_code=500, detail="Upload failed.")

    storage = get_storage()
    # S3 backend supports streaming upload from disk (no in-memory bytes).
    # Other backends fall back to reading bytes.
    if hasattr(storage, "upload_file"):
        await storage.upload_file(parse_path, remote_path)
    else:
        with open(parse_path, "rb") as f:
            contents = f.read()
        await storage.upload(contents, remote_path)
        del contents
    logger.info("Uploaded %s (%d bytes) → %s", file.filename, total_bytes, remote_path)

    # ── Processing strategy ───────────────────────────────────────────
    # Fargate: Full 8-step orchestrator runs as a Fargate task (production).
    # Inline:  Full orchestrator runs in-process (local dev fallback).
    # The upload returns immediately; Fargate processes in the background.
    from app.core.config import get_settings

    settings = get_settings()
    use_fargate = bool(settings.fargate_cluster and settings.fargate_task_definition)

    column_info: dict | None = None
    excel_context = None
    code_preamble = None
    parquet_paths_data = None
    excel_metadata = None

    # Lightweight preview for instant UI feedback (safe on any file size)
    preview_data = None
    try:
        preview_data = _generate_preview(parse_path, file_type)
    except Exception as exc:
        logger.debug("Preview generation skipped: %s", exc)

    if not use_fargate:
        # Local dev: run full orchestrator inline
        try:
            _, column_info = parse_file(parse_path, file_type)
        except Exception as exc:
            logger.warning("Could not parse uploaded file: %s", exc)

        try:
            from app.agents.excel.orchestrator import process_excel_upload
            from app.core.deps import get_llm

            llm = get_llm()
            excel_result = await process_excel_upload(parse_path, llm, org_id=db_org_id)
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

    import shutil

    shutil.rmtree(_temp_dir, ignore_errors=True)

    upload = FileUpload(
        id=file_id,
        filename=file.filename,
        file_type=file_type,
        file_path=remote_path,
        size_bytes=total_bytes,
        organization_id=db_org_id,
        user_id=user.id,
        column_info=column_info,
        excel_context=excel_context,
        code_preamble=code_preamble,
        parquet_paths=parquet_paths_data,
        excel_metadata=excel_metadata,
        processing_status="processing" if use_fargate else "ready",
    )
    db.add(upload)
    await db.flush()
    await db.refresh(upload)

    if not use_fargate and column_info:
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

    # Launch Fargate task for async processing
    if use_fargate:
        try:
            await _launch_fargate_task(
                file_id=str(file_id),
                s3_bucket=settings.parquet_s3_bucket or "",
                s3_key=remote_path,
                org_id=db_org_id,
                settings=settings,
            )
            logger.info("Fargate task launched for file %s", file_id)
        except Exception as exc:
            logger.exception("Failed to launch Fargate task: %s", exc)
            upload.processing_status = "failed"
            upload.processing_error = f"Failed to launch processing: {exc}"
            await db.flush()

    return {
        "id": upload.id,
        "filename": upload.filename,
        "file_type": upload.file_type,
        "size_bytes": upload.size_bytes,
        "column_info": upload.column_info,
        "excel_metadata": upload.excel_metadata,
        "preview_data": preview_data,
        "processing_status": upload.processing_status,
        "created_at": upload.created_at,
    }


@router.post("/{file_id}/processed", include_in_schema=False)
async def processed_callback(
    file_id: uuid.UUID,
    request: Request,
    db: DbSession,
) -> dict:
    """Fargate/Lambda → backend callback with full orchestrator results.

    Auth: HMAC-SHA256 of the raw body in X-Ceaser-Signature header.
    """
    body = await request.body()
    sig = request.headers.get("X-Ceaser-Signature", "")
    if not verify_signature(body, sig):
        raise HTTPException(status_code=401, detail="Invalid signature")

    import json

    try:
        payload = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    stmt = select(FileUpload).where(FileUpload.id == file_id)
    result = await db.execute(stmt)
    upload = result.scalar_one_or_none()
    if upload is None:
        raise HTTPException(status_code=404, detail="File not found")

    status_val = payload.get("status", "ready")
    upload.processing_status = status_val

    if status_val == "failed":
        error_msg = payload.get("error", "unknown")
        # Track retry count in excel_metadata
        meta = upload.excel_metadata or {}
        retries = meta.get("_retries", 0)

        if retries < 2:
            # Auto-retry: re-launch Fargate task
            meta["_retries"] = retries + 1
            upload.excel_metadata = meta
            upload.processing_status = "processing"
            upload.processing_error = None
            from sqlalchemy.orm.attributes import flag_modified

            flag_modified(upload, "excel_metadata")
            await db.flush()
            logger.info(
                "Retrying Fargate for file %s (attempt %d/2): %s",
                file_id,
                retries + 1,
                error_msg,
            )
            try:
                from app.core.config import get_settings

                settings = get_settings()
                await _launch_fargate_task(
                    file_id=str(file_id),
                    s3_bucket=settings.parquet_s3_bucket or "",
                    s3_key=upload.file_path,
                    org_id=upload.organization_id,
                    settings=settings,
                )
                return {"ok": True, "retried": True}
            except Exception as exc:
                logger.warning("Retry launch failed: %s", exc)
                # Fall through to mark as failed

        upload.processing_error = error_msg
        upload.processing_status = "failed"
        logger.warning(
            "Processing failed for %s (after %d retries): %s", file_id, retries, error_msg
        )
    else:
        # Store ALL orchestrator fields
        ci = payload.get("column_info")
        if ci:
            upload.column_info = ci

        ec = payload.get("excel_context")
        if ec:
            upload.excel_context = ec

        cp = payload.get("code_preamble")
        if cp:
            upload.code_preamble = cp

        pp = payload.get("parquet_paths")
        if pp:
            upload.parquet_paths = pp

        em = payload.get("excel_metadata")
        if em:
            upload.excel_metadata = em

        pk = payload.get("parquet_s3_key")
        if pk:
            upload.parquet_s3_key = pk

        # Build file graph for Graph RAG
        if ci:
            try:
                from app.services.schema_graph import build_file_graph

                await build_file_graph(
                    file_id=str(upload.id),
                    org_id=upload.organization_id,
                    filename=upload.filename,
                    conversation_id=None,
                    uploaded_by=str(upload.user_id),
                    column_info=ci,
                    parquet_paths=pp,
                    row_count=ci.get("row_count", 0) if ci else 0,
                )
            except Exception as exc:
                logger.warning("File graph build failed (non-blocking): %s", exc)

    await db.flush()
    logger.info(
        "Processing callback for file %s: status=%s fields=%s",
        file_id,
        status_val,
        [
            k
            for k in (
                "column_info",
                "excel_context",
                "code_preamble",
                "parquet_paths",
                "excel_metadata",
            )
            if payload.get(k)
        ],
    )
    return {"ok": True}


@router.get("/", response_model=list[FileUploadResponse])
async def list_files(current_user: CurrentUser, db: DbSession) -> list[FileUpload]:
    """List all files uploaded by the current user."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    org_id = user.organization_id or current_user.org_id or ""
    if org_id:
        org_filter = FileUpload.organization_id == org_id
    else:
        org_filter = (FileUpload.organization_id == "") | (FileUpload.organization_id.is_(None))
    stmt = select(FileUpload).where(org_filter).order_by(FileUpload.created_at.desc())
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


@router.get("/{file_id}/status")
async def get_upload_status(
    file_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> dict:
    """Poll processing status for a file upload.

    Returns stage, progress percentage, and message for progressive UI feedback.
    """
    await require_permission(Permission.VIEW_DATA, current_user, db)

    from app.services.upload_tracker import get_progress

    progress = get_progress(str(file_id))
    if progress:
        return progress.to_dict()

    # Check if file exists in DB (already processed)
    stmt = select(FileUpload).where(FileUpload.id == file_id)
    result = await db.execute(stmt)
    upload = result.scalar_one_or_none()

    if upload:
        if upload.processing_status == "ready":
            return {
                "fileId": str(file_id),
                "filename": upload.filename,
                "stage": "done",
                "progressPct": 100,
                "message": "Processing complete",
                "completed": True,
                "error": None,
                "processingStatus": "ready",
            }
        elif upload.processing_status == "failed":
            return {
                "fileId": str(file_id),
                "filename": upload.filename,
                "stage": "failed",
                "progressPct": 0,
                "message": upload.processing_error or "Processing failed",
                "completed": True,
                "error": upload.processing_error or "Processing failed",
                "processingStatus": "failed",
            }
        else:
            # Map processing_stage to user-friendly message and percentage
            stage_map = {
                "inspecting": ("Inspecting file structure", 10),
                "extracting": ("Extracting sheets", 20),
                "formulas": ("Analyzing formulas", 35),
                "relationships": ("Detecting relationships", 50),
                "profiling": ("Profiling data quality", 65),
                "quality": ("Running quality checks", 75),
                "parquet": ("Saving optimized data", 85),
                "insight": ("Generating insights", 95),
            }
            stage = upload.processing_stage or "processing"
            friendly, pct = stage_map.get(stage, ("Analyzing your file...", 30))
            return {
                "fileId": str(file_id),
                "filename": upload.filename,
                "stage": stage,
                "progressPct": pct,
                "message": friendly,
                "completed": False,
                "error": None,
                "processingStatus": "processing",
            }

    raise HTTPException(status_code=404, detail="Upload not found.")


@router.put("/{file_id}/processing-stage", include_in_schema=False)
async def update_processing_stage(
    file_id: uuid.UUID,
    request: Request,
    db: DbSession,
) -> dict:
    """Fargate task calls this to report pipeline progress. Auth: HMAC signature."""
    body = await request.body()
    sig = request.headers.get("X-Ceaser-Signature", "")
    if not verify_signature(body, sig):
        raise HTTPException(status_code=401, detail="Invalid signature")

    import json

    try:
        payload = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    stmt = select(FileUpload).where(FileUpload.id == file_id)
    result = await db.execute(stmt)
    upload = result.scalar_one_or_none()
    if upload is None:
        raise HTTPException(status_code=404, detail="File not found")

    upload.processing_stage = payload.get("stage", "processing")
    await db.flush()
    return {"ok": True}
