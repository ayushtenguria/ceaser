"""Notebook Orchestrator — executes cells sequentially, streaming results.

Each cell type is handled by a specialized executor:
- text: passthrough (no execution)
- file: parse via Excel Parser Agent, save as parquet
- input: register user value in context
- prompt: execute via the main agent pipeline (Router → SQL/Python → Execute)
- code: execute directly in sandbox
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import AsyncGenerator
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agents.notebook.context import CellOutput, NotebookContext
from app.db.models import DatabaseConnection, FileUpload, NotebookCell

logger = logging.getLogger(__name__)

def _cell_timeout() -> int:
    try:
        from app.core.config import get_settings
        return get_settings().cell_timeout_seconds
    except Exception:
        return 120

_CELL_TIMEOUT_SECONDS = _cell_timeout()  # Hard timeout per cell


async def execute_notebook_cells(
    *,
    cells: list[NotebookCell],
    user_inputs: dict[str, Any] | None = None,
    file_uploads: dict[str, str] | None = None,
    connection_id: str | None = None,
    db: AsyncSession,
) -> AsyncGenerator[dict[str, Any], None]:
    """Execute notebook cells sequentially and yield SSE events.

    Yields events:
    - {"type": "cell_start", "cellId": ..., "cellOrder": ..., "cellType": ...}
    - {"type": "cell_progress", "cellId": ..., "message": ...}
    - {"type": "cell_complete", "cellId": ..., "status": ..., "text": ..., ...}
    """
    ctx = NotebookContext()
    user_inputs = user_inputs or {}
    file_uploads = file_uploads or {}

    # Load connection schema if available
    if connection_id:
        try:
            from app.api.chat import _build_schema_context
            schema = await _build_schema_context(db, uuid.UUID(connection_id), None)
            ctx.set_connection_schema(schema)
        except Exception as exc:
            logger.warning("Failed to load connection schema: %s", exc)

    for cell in cells:
        cell_id = str(cell.id)
        cell_type = cell.cell_type
        cell_order = cell.order

        yield {
            "type": "cell_start",
            "cellId": cell_id,
            "cellOrder": cell_order,
            "cellType": cell_type,
        }

        start_time = time.monotonic()

        try:
            if cell_type == "text":
                result = await _execute_text_cell(cell, ctx)
            elif cell_type == "file":
                result = await _execute_file_cell(cell, ctx, file_uploads, db)
            elif cell_type == "input":
                result = await _execute_input_cell(cell, ctx, user_inputs)
            elif cell_type == "prompt":
                result = await _execute_prompt_cell(cell, ctx, connection_id, db)
            elif cell_type == "code":
                result = await _execute_code_cell(cell, ctx)
            else:
                result = _error_result(cell_id, f"Unknown cell type: {cell_type}")

        except Exception as exc:
            logger.exception("Cell %s (%s) failed", cell_id, cell_type)
            result = _error_result(cell_id, str(exc))

        elapsed_ms = int((time.monotonic() - start_time) * 1000)
        result["cellId"] = cell_id
        result["cellOrder"] = cell_order
        result["executionMs"] = elapsed_ms
        result["type"] = "cell_complete"

        # Register output in context
        var_name = cell.output_variable or f"result_{cell_order}"
        output = CellOutput(
            cell_id=cell_id,
            cell_type=cell_type,
            variable_name=var_name,
            text=result.get("text", ""),
            table_data=result.get("table"),
            chart_data=result.get("chart"),
            code=result.get("code"),
            error=result.get("error"),
        )
        ctx.add_cell_output(output)

        yield result


# ---------------------------------------------------------------------------
# Cell executors
# ---------------------------------------------------------------------------

async def _execute_text_cell(cell: NotebookCell, ctx: NotebookContext) -> dict:
    """Text cells just resolve templates and pass through."""
    content = ctx.resolve_template(cell.content)
    return {"status": "success", "text": content}


async def _execute_file_cell(
    cell: NotebookCell,
    ctx: NotebookContext,
    file_uploads: dict[str, str],
    db: AsyncSession,
) -> dict:
    """File cells load uploaded files into DataFrames."""
    cell_id = str(cell.id)
    file_id = file_uploads.get(cell_id)

    if not file_id:
        return {"status": "skipped", "text": "No file provided for this cell."}

    # Load file record
    stmt = select(FileUpload).where(FileUpload.id == uuid.UUID(file_id))
    result = await db.execute(stmt)
    upload = result.scalar_one_or_none()

    if upload is None:
        return _error_result(cell_id, "File not found.")

    # If Excel processing already done, use safe ceaser:// aliases
    # (resolved to real URLs at sandbox execution time only)
    if upload.parquet_paths:
        from app.agents.excel.context import CEASER_PROTOCOL
        for var_name, remote_path in upload.parquet_paths.items():
            safe_ref = f"{CEASER_PROTOCOL}{remote_path}"
            info = {"columns": [], "rows": 0, "path": safe_ref}
            if upload.column_info:
                info["columns"] = [c["name"] for c in upload.column_info.get("columns", [])]
                info["rows"] = upload.column_info.get("row_count", 0)
            ctx.add_file(cell_id, var_name, safe_ref, info)

        text = f"Loaded {upload.filename}: {len(upload.parquet_paths)} sheet(s)"
        return {"status": "success", "text": text}

    # Fallback: basic file load
    try:
        import asyncio
        from app.agents.excel.orchestrator import process_excel_upload
        from app.core.deps import get_llm

        excel_result = await process_excel_upload(upload.file_path, llm=None)
        for var_name, path in excel_result.get("parquet_paths", {}).items():
            ctx.add_file(cell_id, var_name, path, {"columns": [], "rows": 0})

        text = f"Loaded {upload.filename}"
        return {"status": "success", "text": text}
    except Exception as exc:
        return _error_result(str(cell.id), f"Failed to load file: {exc}")


async def _execute_input_cell(
    cell: NotebookCell,
    ctx: NotebookContext,
    user_inputs: dict[str, Any],
) -> dict:
    """Input cells register user-provided values."""
    cell_id = str(cell.id)
    config = cell.config or {}
    label = config.get("label", cell.output_variable or f"input_{cell.order}")
    value = user_inputs.get(cell_id, config.get("default", ""))

    ctx.add_user_input(cell_id, label, value)

    return {"status": "success", "text": f"{label} = {value}"}


async def _execute_prompt_cell(
    cell: NotebookCell,
    ctx: NotebookContext,
    connection_id: str | None,
    db: AsyncSession,
) -> dict:
    """Prompt cells execute through the main agent pipeline."""
    from app.agents.graph import run_agent
    from app.core.deps import get_llm

    # Resolve templates in prompt
    prompt = ctx.resolve_template(cell.content)

    # Build schema context from notebook context
    schema_context = ctx.build_prompt_context()

    # If notebook has file DataFrames, include the code preamble in schema context
    # so the agent knows about the uploaded files' data
    has_files = ctx.dataframe_count > 0
    if has_files:
        code_preamble = ctx.build_code_preamble()
        schema_context += f"\n\nCODE PREAMBLE (prepend to all Python code):\n{code_preamble}"
        schema_context += "\nIMPORTANT: The DataFrames listed above are from UPLOADED FILES in this notebook."
        schema_context += " Use Python/pandas to query these DataFrames — NOT SQL."
        schema_context += " If the user's question is about this uploaded data, use Python mode."

    # If only files (no DB connection), force Python mode
    # If both files AND a connection exist, still pass connection — the router + schema
    # context will help the agent decide (SQL for DB questions, Python for file questions)
    effective_connection = None if (has_files and not connection_id) else connection_id

    llm = get_llm(tier="heavy")

    # Collect results from the agent run
    collected_text = ""
    collected_table = None
    collected_chart = None
    collected_code = None
    collected_error = None

    try:
        async for chunk in run_agent(
            query=prompt,
            connection_id=effective_connection,
            file_id=None,
            schema_context=schema_context,
            llm=llm,
            db=db,
        ):
            chunk_type = chunk.get("type", "")
            if chunk_type == "text":
                collected_text += chunk.get("content", "")
            elif chunk_type == "sql":
                collected_code = chunk.get("content")
            elif chunk_type == "code":
                collected_code = chunk.get("content")
            elif chunk_type in ("table", "plotly"):
                data = chunk.get("content") or chunk.get("data")
                if chunk_type == "table":
                    collected_table = data
                else:
                    collected_chart = data
            elif chunk_type == "error":
                collected_error = chunk.get("content")

    except Exception as exc:
        collected_error = str(exc)

    if collected_error and not collected_text:
        return _error_result(str(cell.id), collected_error)

    return {
        "status": "success",
        "text": collected_text,
        "table": collected_table,
        "chart": collected_chart,
        "code": collected_code,
    }


async def _execute_code_cell(cell: NotebookCell, ctx: NotebookContext) -> dict:
    """Code cells execute Python directly in the sandbox."""
    from app.sandbox.executor import execute_python

    # Prepend context preamble
    preamble = ctx.build_code_preamble()
    full_code = preamble + cell.content

    result = await execute_python(full_code)

    if not result.success:
        return _error_result(str(cell.id), result.error or "Code execution failed")

    return {
        "status": "success",
        "text": result.stdout,
        "chart": result.plotly_figure,
        "code": cell.content,
    }


def _error_result(cell_id: str, error: str) -> dict:
    """Create a standard error result."""
    return {"status": "error", "text": "", "error": error}
