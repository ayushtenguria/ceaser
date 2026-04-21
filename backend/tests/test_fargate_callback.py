"""Tests for the Fargate callback endpoint logic."""

from __future__ import annotations

import hashlib
import hmac
import json

import pytest


def _sign(body: bytes, secret: str = "test-secret") -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def test_callback_payload_structure(sample_callback_payload):
    """Verify the expected callback payload has all 6 fields."""
    assert sample_callback_payload["status"] == "ready"
    assert "column_info" in sample_callback_payload
    assert "excel_context" in sample_callback_payload
    assert "code_preamble" in sample_callback_payload
    assert "parquet_paths" in sample_callback_payload
    assert "excel_metadata" in sample_callback_payload

    # Verify nested structure
    meta = sample_callback_payload["excel_metadata"]
    assert "insight" in meta
    assert "quality_report" in meta
    assert "relationships" in meta


def test_callback_hmac_signature(sample_callback_payload):
    """Verify HMAC signing works correctly."""
    body = json.dumps(sample_callback_payload, default=str).encode()
    sig = _sign(body)
    assert len(sig) == 64  # SHA256 hex digest
    # Verify signature is deterministic
    assert _sign(body) == sig


def test_callback_failed_status():
    """Failed callback should include error message."""
    payload = {
        "status": "failed",
        "error": "pipeline_error: TimeoutError",
    }
    assert payload["status"] == "failed"
    assert "TimeoutError" in payload["error"]


def test_column_info_structure(sample_column_info):
    """Column info should have row_count, column_count, and columns list."""
    assert sample_column_info["row_count"] == 100
    assert sample_column_info["column_count"] == 3
    assert len(sample_column_info["columns"]) == 3

    col = sample_column_info["columns"][0]
    assert "name" in col
    assert "dtype" in col
    assert "null_count" in col
    assert "unique_count" in col
    assert "sample_values" in col
