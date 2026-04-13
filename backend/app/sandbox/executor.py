"""Sandboxed Python code execution via subprocess with timeout."""

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
    """Strip signed URLs, tokens, and internal file paths from error text.

    Prevents Supabase signed URLs and server-side paths from leaking to
    the frontend or being stored in the database.
    """
    import re

    # Strip full URLs (signed Supabase URLs, etc.)
    text = re.sub(
        r'https?://[^\s"\')\]]+',
        "[STORAGE_URL]",
        text,
    )
    # Strip token parameters
    text = re.sub(r'token=[^\s&"\']+', "token=***", text)
    # Strip absolute server paths
    text = re.sub(r'/(?:Users|home|var|tmp|private)[^\s"\')\]:]+', "[PATH]", text)
    # Strip ceaser:// refs (shouldn't appear in errors, but just in case)
    text = re.sub(r'ceaser://[^\s"\')\]]+', "[FILE_REF]", text)
    return text


_BLOCKED_MODULES = frozenset(
    {
        "subprocess",
        "shutil",
        "socket",
        "http",
        "urllib",
        "requests",
        "httpx",
        "aiohttp",
        "ftplib",
        "smtplib",
        "telnetlib",
        "xmlrpc",
        "ctypes",
        "multiprocessing",
        "webbrowser",
        "antigravity",
    }
)


@dataclass
class ExecutionResult:
    """Captured output from a sandboxed code execution."""

    stdout: str = ""
    stderr: str = ""
    plotly_figure: dict[str, Any] | None = None
    error: str | None = None
    success: bool = True


def _build_runner_script(user_code: str, figure_path: str) -> str:
    """Wrap user code with runtime safety helpers and plotly figure capture.

    The prefix injects:
    - ``_col(df, name)`` — fuzzy column lookup that auto-corrects case
      mismatches and common variations at runtime instead of crashing.
    - ``_safe_numeric(df, col)`` — safe numeric conversion.

    Security is enforced by the subprocess boundary (timeout, no network in
    production via Docker ``--network none``).  We don't override ``__import__``
    because pandas/numpy/plotly depend on dozens of internal stdlib modules
    (ctypes, os, _io, etc.) that cannot be individually allowlisted.
    """
    prefix = textwrap.dedent("""\
        import json
        import pandas as _pd_internal

        def _col(df, name):
            \"\"\"Find the best matching column name in a DataFrame.

            Handles: case mismatches (Quantity→quantity), underscores vs spaces,
            and partial matches (revenue→total_revenue).
            Returns the actual column name or raises KeyError with helpful message.
            \"\"\"
            if name in df.columns:
                return name
            # Case-insensitive match
            lower_map = {c.lower(): c for c in df.columns}
            if name.lower() in lower_map:
                return lower_map[name.lower()]
            # Underscore/space normalization
            norm = name.lower().replace(" ", "_").replace("-", "_")
            norm_map = {c.lower().replace(" ", "_").replace("-", "_"): c for c in df.columns}
            if norm in norm_map:
                return norm_map[norm]
            # Substring match — find columns that contain the target
            matches = [c for c in df.columns if name.lower() in c.lower()]
            if len(matches) == 1:
                return matches[0]
            # Reverse substring — target contains a column name
            matches = [c for c in df.columns if c.lower() in name.lower()]
            if len(matches) == 1:
                return matches[0]
            raise KeyError(
                f"Column '{name}' not found. "
                f"Available columns: {', '.join(df.columns.tolist()[:30])}"
            )

        def _safe_numeric(series):
            \"\"\"Convert a series to numeric, coercing errors.\"\"\"
            return _pd_internal.to_numeric(series, errors='coerce')

        # Monkey-patch DataFrame.__getitem__ for fuzzy column access
        _orig_getitem = _pd_internal.DataFrame.__getitem__
        def _patched_getitem(self, key):
            if isinstance(key, str) and key not in self.columns:
                try:
                    key = _col(self, key)
                except KeyError:
                    pass  # let pandas raise its own error with our enhanced message
            return _orig_getitem(self, key)
        _pd_internal.DataFrame.__getitem__ = _patched_getitem

        # DuckDB helper — SQL on parquet files without loading into memory
        try:
            import duckdb as _duckdb

            def query_parquet(sql, parquet_path=None):
                \"\"\"Run SQL directly on a parquet file using DuckDB.

                Returns a pandas DataFrame with just the result — never loads
                the full file into memory. 100x faster than pandas for
                aggregations on large files.

                Usage:
                    result = query_parquet("SELECT region, SUM(rev) FROM data GROUP BY region", "ceaser://...")
                    # Or reference the parquet path directly in SQL:
                    result = query_parquet("SELECT * FROM read_parquet('ceaser://...') LIMIT 10")
                \"\"\"
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
                \"\"\"Convert plotly binary arrays to plain lists for frontend.\"\"\"
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
        except NameError:
            pass
    """)

    return prefix + user_code + "\n" + postfix


async def execute_python(code: str) -> ExecutionResult:
    """Execute *code* in an isolated subprocess, returning captured output.

    * ``ceaser://`` file references are resolved to real URLs/paths server-side
      before execution. The resolved code is never stored or returned.
    * Stdout and stderr are captured.
    * If the code defines a variable ``fig`` that is a Plotly figure, it is
      serialised to JSON and included in the result.
    * The process is killed after ``_TIMEOUT_SECONDS``.
    """
    result = ExecutionResult()

    # Resolve ceaser:// aliases to real storage URLs (signed URLs for Supabase,
    # local paths for filesystem). This happens ONLY here — the resolved code
    # is written to a temp file and deleted after execution.
    resolved_code = code
    if "ceaser://" in code:
        try:
            from app.agents.excel.context import resolve_ceaser_refs

            resolved_code = await resolve_ceaser_refs(code)
        except Exception as exc:
            logger.warning("Failed to resolve ceaser:// refs: %s", exc)

    with tempfile.TemporaryDirectory() as tmp_dir:
        script_path = Path(tmp_dir) / "runner.py"
        figure_path = Path(tmp_dir) / "figure.json"

        runner = _build_runner_script(resolved_code, str(figure_path))
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
                timeout=_TIMEOUT_SECONDS,
            )

            result.stdout = stdout_bytes.decode(errors="replace").strip()
            raw_stderr = stderr_bytes.decode(errors="replace").strip()
            result.stderr = _sanitize_error(raw_stderr)

            if proc.returncode != 0:
                result.success = False
                result.error = result.stderr or f"Process exited with code {proc.returncode}"

            # Read plotly figure if the user code produced one.
            if figure_path.exists():
                try:
                    fig_data = json.loads(figure_path.read_text())
                    result.plotly_figure = fig_data
                except json.JSONDecodeError:
                    logger.warning("Plotly figure file was not valid JSON.")

        except TimeoutError:
            proc.kill()  # type: ignore[union-attr]
            result.success = False
            result.error = f"Execution timed out after {_TIMEOUT_SECONDS}s."
            logger.warning("Sandbox execution timed out for code snippet.")
        except Exception as exc:
            result.success = False
            result.error = _sanitize_error(str(exc))
            logger.exception("Sandbox execution error.")

    return result
