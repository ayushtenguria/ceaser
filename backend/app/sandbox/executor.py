"""Sandboxed Python code execution — Lambda or local subprocess.

When SANDBOX_LAMBDA_FUNCTION is configured, code is executed on AWS Lambda
(up to 10GB RAM). Otherwise falls back to a local subprocess on EC2.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import tempfile
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _sandbox_timeout() -> int:
    try:
        from app.core.config import get_settings

        return get_settings().sandbox_timeout_seconds
    except Exception:
        return 30


_TIMEOUT_SECONDS = _sandbox_timeout()

_PYTHON_EXECUTABLE = sys.executable


def _sanitize_error(text: str) -> str:
    """Strip sensitive info from error text before returning to the user."""
    import re

    # URLs (signed S3, Supabase, etc.)
    text = re.sub(r'https?://[^\s"\')\]]+', "[STORAGE_URL]", text)
    # Token/key parameters
    text = re.sub(r'token=[^\s&"\']+', "token=***", text)
    # Internal file paths
    text = re.sub(r'/(?:Users|home|var|tmp|private|app)[^\s"\')\]:]+', "[PATH]", text)
    # ceaser:// protocol refs
    text = re.sub(r'ceaser://[^\s"\')\]]+', "[FILE_REF]", text)
    # Database connection strings (postgresql://, mysql://, etc.)
    text = re.sub(r'(?:postgresql|mysql|sqlite|redis)(?:\+\w+)?://[^\s"\')\]]+', "[DB_URL]", text)
    # AWS ARNs
    text = re.sub(r'arn:aws:[^\s"\')\]]+', "[AWS_ARN]", text)
    # IP addresses with ports
    text = re.sub(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(?::\d+)?\b", "[HOST]", text)
    # AWS access key IDs
    text = re.sub(r"(?:AKIA|ASIA)[A-Z0-9]{16}", "[AWS_KEY]", text)
    return text


@dataclass
class ExecutionResult:
    """Captured output from a sandboxed code execution."""

    stdout: str = ""
    stderr: str = ""
    plotly_figure: dict[str, Any] | None = None
    error: str | None = None
    success: bool = True


def _build_runner_script(user_code: str, figure_path: str) -> str:
    """Wrap user code with runtime safety helpers and plotly figure capture."""
    prefix = textwrap.dedent("""\
        import json
        import pandas as _pd_internal

        def _col(df, name):
            if name in df.columns:
                return name
            lower_map = {c.lower(): c for c in df.columns}
            if name.lower() in lower_map:
                return lower_map[name.lower()]
            norm = name.lower().replace(" ", "_").replace("-", "_")
            norm_map = {c.lower().replace(" ", "_").replace("-", "_"): c for c in df.columns}
            if norm in norm_map:
                return norm_map[norm]
            matches = [c for c in df.columns if name.lower() in c.lower()]
            if len(matches) == 1:
                return matches[0]
            matches = [c for c in df.columns if c.lower() in name.lower()]
            if len(matches) == 1:
                return matches[0]
            raise KeyError(
                f"Column '{name}' not found. "
                f"Available columns: {', '.join(df.columns.tolist()[:30])}"
            )

        def _safe_numeric(series):
            return _pd_internal.to_numeric(series, errors='coerce')

        _orig_getitem = _pd_internal.DataFrame.__getitem__
        def _patched_getitem(self, key):
            if isinstance(key, str) and key not in self.columns:
                try:
                    key = _col(self, key)
                except KeyError:
                    pass
            return _orig_getitem(self, key)
        _pd_internal.DataFrame.__getitem__ = _patched_getitem

        try:
            import duckdb as _duckdb

            def query_parquet(sql, parquet_path=None):
                if parquet_path and 'read_parquet' not in sql:
                    sql = sql.replace('data', f"read_parquet('{parquet_path}')", 1)
                return _duckdb.sql(sql).fetchdf()

        except ImportError:
            def query_parquet(sql, parquet_path=None):
                raise RuntimeError("DuckDB not available. Use pandas instead.")

    """)

    postfix = textwrap.dedent(f"""\


        try:
            fig  # noqa: F821
            import json as _json
            import base64 as _b64
            import numpy as _np

            def _decode_bdata(obj):
                if isinstance(obj, dict):
                    if 'bdata' in obj and 'dtype' in obj:
                        dt = _np.dtype(obj['dtype'])
                        raw = _b64.b64decode(obj['bdata'])
                        return _np.frombuffer(raw, dtype=dt).tolist()
                    return {{k: _decode_bdata(v) for k, v in obj.items()}}
                if isinstance(obj, (list, tuple)):
                    return [_decode_bdata(v) for v in obj]
                if isinstance(obj, _np.ndarray):
                    return obj.tolist()
                if isinstance(obj, (_np.integer, _np.floating)):
                    return obj.item()
                return obj

            _fig_dict = _json.loads(fig.to_json())
            _fig_clean = _decode_bdata(_fig_dict)
            with open({figure_path!r}, "w") as _f:
                _json.dump(_fig_clean, _f, default=str)
        except (NameError, AttributeError, TypeError):
            pass
    """)

    return prefix + user_code + "\n" + postfix


async def _execute_via_lambda(code: str, timeout: int) -> ExecutionResult:
    """Invoke the sandbox Lambda function synchronously."""
    from app.core.config import get_settings

    settings = get_settings()
    result = ExecutionResult()

    try:
        import boto3

        client = boto3.client("lambda", region_name=settings.aws_region)

        payload = json.dumps({"code": code, "timeout": timeout})

        response = await asyncio.to_thread(
            client.invoke,
            FunctionName=settings.sandbox_lambda_function,
            InvocationType="RequestResponse",
            Payload=payload.encode(),
        )

        response_payload = json.loads(response["Payload"].read().decode())

        # Check for Lambda-level errors (not code execution errors)
        if "errorMessage" in response_payload:
            result.success = False
            result.error = _sanitize_error(response_payload["errorMessage"])
            return result

        result.success = response_payload.get("success", False)
        result.stdout = response_payload.get("stdout", "")
        result.stderr = response_payload.get("stderr", "")
        result.plotly_figure = response_payload.get("plotly_figure")
        result.error = response_payload.get("error")

        logger.info(
            "Lambda sandbox: success=%s stdout=%d chars figure=%s",
            result.success,
            len(result.stdout),
            result.plotly_figure is not None,
        )

    except Exception as exc:
        result.success = False
        result.error = _sanitize_error(f"Lambda invocation failed: {exc}")
        logger.exception("Lambda sandbox invocation error")

    return result


async def _execute_via_subprocess(code: str, timeout: int) -> ExecutionResult:
    """Execute code in a local subprocess (EC2 fallback)."""
    result = ExecutionResult()

    with tempfile.TemporaryDirectory() as tmp_dir:
        script_path = Path(tmp_dir) / "runner.py"
        figure_path = Path(tmp_dir) / "figure.json"

        runner = _build_runner_script(code, str(figure_path))
        script_path.write_text(runner)

        try:
            proc = await asyncio.create_subprocess_exec(
                _PYTHON_EXECUTABLE,
                str(script_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=tmp_dir,
            )

            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(),
                timeout=timeout,
            )

            result.stdout = stdout_bytes.decode(errors="replace").strip()
            raw_stderr = stderr_bytes.decode(errors="replace").strip()
            result.stderr = _sanitize_error(raw_stderr)

            if proc.returncode != 0:
                result.success = False
                result.error = result.stderr or f"Process exited with code {proc.returncode}"

            if figure_path.exists():
                try:
                    fig_data = json.loads(figure_path.read_text())
                    result.plotly_figure = fig_data
                except json.JSONDecodeError:
                    logger.warning("Plotly figure file was not valid JSON.")

        except TimeoutError:
            proc.kill()  # type: ignore[union-attr]
            result.success = False
            result.error = f"Execution timed out after {timeout}s."
            logger.warning("Sandbox execution timed out for code snippet.")
        except Exception as exc:
            result.success = False
            result.error = _sanitize_error(str(exc))
            logger.exception("Sandbox execution error.")

    return result


async def execute_python(code: str) -> ExecutionResult:
    """Execute Python code in a sandbox — Lambda if configured, else subprocess.

    * ``ceaser://`` file references are resolved to real URLs/paths server-side
      before execution. The resolved code is never stored or returned.
    * Stdout and stderr are captured.
    * If the code defines a variable ``fig`` (Plotly figure), it is serialised
      to JSON and included in the result.
    """
    # Resolve ceaser:// aliases to real storage URLs before execution
    resolved_code = code
    if "ceaser://" in code:
        try:
            from app.agents.excel.context import resolve_ceaser_refs

            resolved_code = await resolve_ceaser_refs(code)
        except Exception as exc:
            logger.warning("Failed to resolve ceaser:// refs: %s", exc)

    # Route to Lambda or local subprocess
    from app.core.config import get_settings

    settings = get_settings()

    if settings.sandbox_lambda_function:
        logger.info("Executing code via Lambda: %s", settings.sandbox_lambda_function)
        return await _execute_via_lambda(resolved_code, timeout=_TIMEOUT_SECONDS)
    else:
        return await _execute_via_subprocess(resolved_code, timeout=_TIMEOUT_SECONDS)
