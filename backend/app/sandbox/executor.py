"""Sandboxed Python code execution via subprocess with timeout."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import tempfile
import textwrap
from dataclasses import dataclass, field
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

# Use the current Python executable (from the venv) so the sandbox subprocess
# can import pandas, plotly, numpy, etc.  Do NOT resolve() — that follows the
# symlink to the system Python and loses the venv site-packages.
_PYTHON_EXECUTABLE = sys.executable

# Blocklist of dangerous modules — network access, process spawning, etc.
# We deliberately allow os/sys/pathlib because numpy/pandas/plotly need them
# internally. The subprocess is the real execution sandbox boundary.
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
    """Wrap user code with a plotly figure capture postfix.

    Security is enforced by the subprocess boundary (timeout, no network in
    production via Docker ``--network none``).  We don't override ``__import__``
    because pandas/numpy/plotly depend on dozens of internal stdlib modules
    (ctypes, os, _io, etc.) that cannot be individually allowlisted.
    """
    prefix = textwrap.dedent("""\
        import json

        # ── User code ──────────────────────────────────────────
    """)

    postfix = textwrap.dedent(f"""\

        # ── End user code ──────────────────────────────────────

        # Capture any plotly figure written to a variable named `fig`.
        try:
            fig  # noqa: F821
            fig_json = fig.to_json()
            with open({figure_path!r}, "w") as _f:
                _f.write(fig_json)
        except NameError:
            pass
    """)

    return prefix + user_code + "\n" + postfix


async def execute_python(code: str) -> ExecutionResult:
    """Execute *code* in an isolated subprocess, returning captured output.

    * Stdout and stderr are captured.
    * If the code defines a variable ``fig`` that is a Plotly figure, it is
      serialised to JSON and included in the result.
    * The process is killed after ``_TIMEOUT_SECONDS``.
    """
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
                timeout=_TIMEOUT_SECONDS,
            )

            result.stdout = stdout_bytes.decode(errors="replace").strip()
            result.stderr = stderr_bytes.decode(errors="replace").strip()

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

        except asyncio.TimeoutError:
            proc.kill()  # type: ignore[union-attr]
            result.success = False
            result.error = f"Execution timed out after {_TIMEOUT_SECONDS}s."
            logger.warning("Sandbox execution timed out for code snippet.")
        except Exception as exc:
            result.success = False
            result.error = str(exc)
            logger.exception("Sandbox execution error.")

    return result
