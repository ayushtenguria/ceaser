"""Lambda: execute sandboxed Python code for data analysis.

Invoked synchronously by the backend via `lambda:InvokeFunction`.
Receives Python code (with storage URLs already resolved), executes it,
and returns stdout, stderr, and any Plotly figure JSON.

This offloads heavy pandas/numpy/plotly compute from the EC2 instance
(2GB RAM) to Lambda (up to 10GB RAM), preventing OOM on large files.
"""

from __future__ import annotations

import json
import logging
import sys
import tempfile
import textwrap
from pathlib import Path
from typing import Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _build_runner_script(user_code: str, figure_path: str) -> str:
    """Wrap user code with runtime helpers and plotly figure capture."""
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


def _sanitize_error(text: str) -> str:
    """Strip signed URLs, tokens, and internal paths from error text."""
    import re

    text = re.sub(r'https?://[^\s"\')\]]+', "[STORAGE_URL]", text)
    text = re.sub(r'token=[^\s&"\']+', "token=***", text)
    text = re.sub(r'/(?:Users|home|var|tmp|private)[^\s"\')\]:]+', "[PATH]", text)
    text = re.sub(r'ceaser://[^\s"\')\]]+', "[FILE_REF]", text)
    return text


def lambda_handler(event: dict, context: Any) -> dict:
    """Execute Python code and return results.

    Input event:
        {"code": "import pandas as pd\\n...", "timeout": 30}

    The code should have ceaser:// refs already resolved to real URLs
    by the backend before invoking this Lambda.

    Returns:
        {"success": bool, "stdout": str, "stderr": str,
         "plotly_figure": dict|null, "error": str|null}
    """
    code = event.get("code", "")
    timeout = event.get("timeout", 60)

    if not code.strip():
        return {
            "success": False,
            "stdout": "",
            "stderr": "",
            "plotly_figure": None,
            "error": "No code provided",
        }

    import asyncio
    import subprocess

    with tempfile.TemporaryDirectory() as tmp_dir:
        script_path = Path(tmp_dir) / "runner.py"
        figure_path = Path(tmp_dir) / "figure.json"

        runner = _build_runner_script(code, str(figure_path))
        script_path.write_text(runner)

        try:
            proc = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                timeout=timeout,
                cwd=tmp_dir,
            )

            stdout = proc.stdout.decode(errors="replace").strip()
            stderr = _sanitize_error(proc.stderr.decode(errors="replace").strip())

            plotly_figure = None
            if figure_path.exists():
                try:
                    plotly_figure = json.loads(figure_path.read_text())
                except json.JSONDecodeError:
                    logger.warning("Plotly figure file was not valid JSON")

            success = proc.returncode == 0
            error = None if success else (stderr or f"Process exited with code {proc.returncode}")

            logger.info(
                "Sandbox execution: success=%s stdout=%d chars stderr=%d chars figure=%s",
                success, len(stdout), len(stderr), plotly_figure is not None,
            )

            return {
                "success": success,
                "stdout": stdout,
                "stderr": stderr,
                "plotly_figure": plotly_figure,
                "error": error,
            }

        except subprocess.TimeoutExpired:
            logger.warning("Sandbox execution timed out after %ds", timeout)
            return {
                "success": False,
                "stdout": "",
                "stderr": "",
                "plotly_figure": None,
                "error": f"Execution timed out after {timeout}s",
            }
        except Exception as exc:
            logger.exception("Sandbox execution error: %s", exc)
            return {
                "success": False,
                "stdout": "",
                "stderr": "",
                "plotly_figure": None,
                "error": _sanitize_error(str(exc)),
            }
