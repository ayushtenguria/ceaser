"""Tests for the sandbox Python executor (local subprocess mode)."""

from __future__ import annotations

import pytest

from app.sandbox.executor import ExecutionResult, _build_runner_script, _sanitize_error


def test_sanitize_error_strips_urls():
    text = 'Error loading https://s3.amazonaws.com/bucket/key?token=secret123'
    result = _sanitize_error(text)
    assert "s3.amazonaws.com" not in result
    assert "[STORAGE_URL]" in result


def test_sanitize_error_strips_paths():
    text = 'File "/Users/ayush/hacktiger/ceaser/backend/app/main.py", line 10'
    result = _sanitize_error(text)
    assert "/Users/ayush" not in result
    assert "[PATH]" in result


def test_sanitize_error_strips_ceaser_refs():
    text = 'Error loading ceaser://parquet/org/file.parquet'
    result = _sanitize_error(text)
    assert "ceaser://" not in result
    assert "[FILE_REF]" in result


def test_build_runner_script_includes_prefix():
    script = _build_runner_script("print('hello')", "/tmp/fig.json")
    assert "import json" in script
    assert "import pandas as _pd_internal" in script
    assert "_col(df, name)" in script
    assert "print('hello')" in script


def test_build_runner_script_includes_postfix():
    script = _build_runner_script("fig = None", "/tmp/fig.json")
    assert "fig.to_json()" in script
    assert "except (NameError, AttributeError, TypeError):" in script


@pytest.mark.asyncio
async def test_execute_python_simple():
    """Test basic code execution via local subprocess."""
    # Only test in local mode (no Lambda configured)
    import os

    if os.environ.get("SANDBOX_LAMBDA_FUNCTION"):
        pytest.skip("Lambda sandbox configured — skipping local test")

    from app.sandbox.executor import execute_python

    result = await execute_python("print(2 + 2)")
    assert result.success is True
    assert "4" in result.stdout


@pytest.mark.asyncio
async def test_execute_python_error():
    """Test that errors are captured properly."""
    import os

    if os.environ.get("SANDBOX_LAMBDA_FUNCTION"):
        pytest.skip("Lambda sandbox configured — skipping local test")

    from app.sandbox.executor import execute_python

    result = await execute_python("raise ValueError('test error')")
    assert result.success is False
    assert "test error" in (result.error or "")
