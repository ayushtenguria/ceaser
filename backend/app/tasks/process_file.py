"""Fargate task entrypoint: run the full Excel processing pipeline.

Usage (as Fargate container override):
    python -m app.tasks.process_file

Reads configuration from environment variables:
    FILE_ID              - UUID of the FileUpload record (passed via container override)
    S3_BUCKET            - Source S3 bucket name (passed via container override)
    S3_KEY               - S3 key of the uploaded file (passed via container override)
    ORG_ID               - Organization ID for storage paths (passed via container override)
    BACKEND_CALLBACK_URL - Base URL for the callback (set in task definition)
    HMAC_SHARED_SECRET   - Shared secret for callback authentication (set in task definition)
    STORAGE_BACKEND      - "s3" (set in task definition)
    PARQUET_S3_BUCKET    - S3 bucket for parquet output (set in task definition)
    AWS_REGION           - AWS region (set in task definition, default: us-east-1)
    LLM_PROVIDER         - "bedrock" (default) — uses IAM role for auth, no API key needed

The task:
1. Downloads the uploaded file from S3
2. Runs the full 8-step Excel processing pipeline (process_excel_upload)
3. POSTs all results to the backend callback endpoint
4. Exits
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import sys
import tempfile
from pathlib import Path

import boto3
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
)
logger = logging.getLogger("ceaser.fargate")

# ── Environment ─────────────────────────────────────────────────────────────
FILE_ID = os.environ.get("FILE_ID", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_KEY = os.environ.get("S3_KEY", "")
ORG_ID = os.environ.get("ORG_ID", "default")
CALLBACK_URL = os.environ.get("BACKEND_CALLBACK_URL", "")
HMAC_SECRET = os.environ.get("HMAC_SHARED_SECRET", "").encode()
REGION = os.environ.get("AWS_REGION", "us-east-1")


def _sign(body: bytes) -> str:
    """HMAC-SHA256 signature for callback authentication."""
    return hmac.new(HMAC_SECRET, body, hashlib.sha256).hexdigest()


def _report_stage(file_id: str, stage: str) -> None:
    """Report current pipeline stage to the backend for progress tracking."""
    try:
        body = json.dumps({"stage": stage}).encode()
        sig = _sign(body)
        url = f"{CALLBACK_URL.rstrip('/')}/files/{file_id}/processing-stage"
        requests.put(
            url,
            data=body,
            headers={"Content-Type": "application/json", "X-Ceaser-Signature": sig},
            timeout=5,
        )
    except Exception:
        pass  # Non-critical — don't fail the pipeline for progress reporting


def _callback(file_id: str, payload: dict) -> None:
    """POST results to the backend callback endpoint."""
    body = json.dumps(payload, default=str).encode()
    sig = _sign(body)
    url = f"{CALLBACK_URL.rstrip('/')}/files/{file_id}/processed"
    logger.info("Callback → %s", url)
    r = requests.post(
        url,
        data=body,
        headers={"Content-Type": "application/json", "X-Ceaser-Signature": sig},
        timeout=30,
    )
    logger.info("Callback response: HTTP %d", r.status_code)
    r.raise_for_status()


async def _run() -> None:
    """Download file from S3, run orchestrator, callback with results."""
    if not all([FILE_ID, S3_BUCKET, S3_KEY, CALLBACK_URL]):
        logger.error(
            "Missing required env vars. FILE_ID=%s S3_BUCKET=%s S3_KEY=%s CALLBACK_URL=%s",
            FILE_ID,
            S3_BUCKET,
            S3_KEY,
            CALLBACK_URL,
        )
        sys.exit(1)

    s3 = boto3.client("s3", region_name=REGION)
    filename = Path(S3_KEY).name

    with tempfile.TemporaryDirectory(prefix="ceaser_fargate_") as tmp:
        local_path = str(Path(tmp) / filename)

        # Step 1: Download from S3
        logger.info("Downloading s3://%s/%s → %s", S3_BUCKET, S3_KEY, local_path)
        s3.download_file(S3_BUCKET, S3_KEY, local_path)
        size = Path(local_path).stat().st_size
        logger.info("Downloaded %d bytes", size)

        # Step 2: Initialize LLM
        try:
            from app.core.deps import get_llm

            llm = get_llm()
            logger.info("LLM initialized: %s", type(llm).__name__)
        except Exception as exc:
            logger.warning("LLM init failed (will use auto-summary): %s", exc)
            llm = None

        # Step 3: Run the full 8-step orchestrator with a timeout
        logger.info("Starting Excel processing pipeline for file %s", FILE_ID)
        pipeline_timeout = int(os.environ.get("PIPELINE_TIMEOUT", "1200"))  # 20 min default
        try:
            from app.agents.excel.orchestrator import process_excel_upload

            result = await asyncio.wait_for(
                process_excel_upload(
                    local_path,
                    llm=llm,
                    org_id=ORG_ID,
                    on_stage=lambda stage: _report_stage(FILE_ID, stage),
                ),
                timeout=pipeline_timeout,
            )
            logger.info(
                "Pipeline complete: %.1fs, %d failed steps",
                result.get("pipeline_time_seconds", 0),
                len(result.get("failed_steps", [])),
            )
        except TimeoutError:
            logger.error("Pipeline timed out after %ds for file %s", pipeline_timeout, FILE_ID)
            _callback(
                FILE_ID,
                {
                    "status": "failed",
                    "error": f"Processing timed out after {pipeline_timeout}s. Try a smaller file.",
                },
            )
            return
        except Exception as exc:
            logger.exception("Pipeline failed: %s", exc)
            _callback(
                FILE_ID,
                {
                    "status": "failed",
                    "error": f"pipeline_error: {exc}",
                },
            )
            return

    # Step 4: Build callback payload with ALL orchestrator fields
    payload = {
        "status": "ready",
        "column_info": None,  # Will be populated from orchestrator workbook
        "excel_context": result.get("excel_context", ""),
        "code_preamble": result.get("code_preamble", ""),
        "parquet_paths": result.get("parquet_paths", {}),
        "excel_metadata": {
            "insight": result.get("insight"),
            "quality_report": result.get("quality_report"),
            "relationships": result.get("relationships", []),
        },
    }

    # Extract column_info from the workbook object
    wb = result.get("workbook")
    if wb and hasattr(wb, "sheets"):
        sheets = wb.sheets if hasattr(wb, "sheets") else []
        all_columns = []
        total_rows = 0
        for sheet in sheets:
            total_rows += getattr(sheet, "row_count", 0)
            if hasattr(sheet, "df"):
                for col in sheet.df.columns:
                    series = sheet.df[col]
                    all_columns.append(
                        {
                            "name": str(col),
                            "dtype": str(series.dtype),
                            "null_count": int(series.isna().sum()),
                            "unique_count": int(series.nunique()),
                            "sample_values": [],
                        }
                    )
        payload["column_info"] = {
            "row_count": total_rows,
            "column_count": len(all_columns),
            "columns": all_columns,
        }
    elif isinstance(wb, dict) and "sheets" in wb:
        # Compat dict format
        sheets_list = wb["sheets"]
        all_columns = []
        total_rows = 0
        for s in sheets_list:
            total_rows += s.get("row_count", 0)
            for col_name in s.get("columns", []):
                col_type = s.get("column_types", {}).get(col_name, "unknown")
                all_columns.append(
                    {
                        "name": col_name,
                        "dtype": col_type,
                        "null_count": 0,
                        "unique_count": 0,
                        "sample_values": s.get("sample_values", {}).get(col_name, [])[:5],
                    }
                )
        payload["column_info"] = {
            "row_count": total_rows,
            "column_count": len(all_columns),
            "columns": all_columns,
        }

    # Extract parquet_s3_key (first parquet path if available)
    parquet_paths = result.get("parquet_paths", {})
    if parquet_paths:
        payload["parquet_s3_key"] = next(iter(parquet_paths.values()), None)

    # Step 5: Callback to backend
    try:
        _callback(FILE_ID, payload)
        logger.info("File %s processing complete and reported to backend", FILE_ID)
    except Exception as exc:
        logger.exception("Callback failed: %s", exc)
        sys.exit(1)


def main() -> None:
    """Sync entrypoint for Fargate."""
    asyncio.run(_run())


if __name__ == "__main__":
    main()
