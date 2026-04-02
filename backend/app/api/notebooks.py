"""Notebook API — CRUD, cell management, execution, and templates."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.api.schemas import (
    CellReorderRequest,
    NotebookCellCreate,
    NotebookCreate,
    NotebookResponse,
    NotebookRunRequest,
    NotebookRunResponse,
    NotebookUpdate,
)
from app.core.deps import CurrentUser, DbSession
from app.core.permissions import Permission, require_permission
from app.db.models import (
    Notebook,
    NotebookCell,
    NotebookCellResult,
    NotebookRun,
    User,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/notebooks", tags=["notebooks"])


async def _load_notebook(db: DbSession, notebook_id: uuid.UUID, user: User) -> Notebook:
    """Load notebook with cells, verifying org access."""
    stmt = (
        select(Notebook)
        .options(selectinload(Notebook.cells))
        .where(Notebook.id == notebook_id)
    )
    result = await db.execute(stmt)
    notebook = result.scalar_one_or_none()
    if notebook is None:
        raise HTTPException(status_code=404, detail="Notebook not found.")
    if notebook.organization_id and notebook.organization_id != (user.organization_id or ""):
        if not notebook.is_public:
            raise HTTPException(status_code=404, detail="Notebook not found.")
    return notebook


@router.post("/", response_model=NotebookResponse, status_code=status.HTTP_201_CREATED)
async def create_notebook(
    body: NotebookCreate, current_user: CurrentUser, db: DbSession,
) -> Notebook:
    """Create a new notebook with optional initial cells."""
    user = await require_permission(Permission.SAVE_REPORTS, current_user, db)
    from app.core.features import check_feature, Feature
    await check_feature(Feature.NOTEBOOKS, db, user.organization_id or "")

    notebook = Notebook(
        name=body.name,
        description=body.description,
        user_id=user.id,
        organization_id=user.organization_id or "",
        connection_id=body.connection_id,
    )
    db.add(notebook)
    await db.flush()

    for i, cell_data in enumerate(body.cells):
        cell = NotebookCell(
            notebook_id=notebook.id,
            order=cell_data.order if cell_data.order > 0 else i,
            cell_type=cell_data.cell_type,
            content=cell_data.content,
            config=cell_data.config,
            output_variable=cell_data.output_variable,
        )
        db.add(cell)

    await db.flush()
    await db.refresh(notebook)

    stmt = select(Notebook).options(selectinload(Notebook.cells)).where(Notebook.id == notebook.id)
    result = await db.execute(stmt)
    notebook = result.scalar_one()

    logger.info("Created notebook '%s' with %d cells", body.name, len(body.cells))
    return notebook


@router.get("/", response_model=list[NotebookResponse])
async def list_notebooks(current_user: CurrentUser, db: DbSession) -> list[Notebook]:
    """List notebooks for the current user's organization."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    org_id = user.organization_id or ""
    stmt = (
        select(Notebook)
        .options(selectinload(Notebook.cells))
        .where(Notebook.organization_id == org_id)
        .order_by(Notebook.updated_at.desc())
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.get("/templates", response_model=list[NotebookResponse])
async def list_templates(current_user: CurrentUser, db: DbSession) -> list[Notebook]:
    """List public notebook templates."""
    await require_permission(Permission.VIEW_DATA, current_user, db)
    stmt = (
        select(Notebook)
        .options(selectinload(Notebook.cells))
        .where(Notebook.is_template == True, Notebook.is_public == True)  # noqa: E712
        .order_by(Notebook.run_count.desc())
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.get("/{notebook_id}", response_model=NotebookResponse)
async def get_notebook(
    notebook_id: uuid.UUID, current_user: CurrentUser, db: DbSession,
) -> Notebook:
    """Get a notebook with all cells."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    return await _load_notebook(db, notebook_id, user)


@router.patch("/{notebook_id}", response_model=NotebookResponse)
async def update_notebook(
    notebook_id: uuid.UUID, body: NotebookUpdate, current_user: CurrentUser, db: DbSession,
) -> Notebook:
    """Update notebook metadata."""
    user = await require_permission(Permission.SAVE_REPORTS, current_user, db)
    notebook = await _load_notebook(db, notebook_id, user)

    if body.name is not None:
        notebook.name = body.name
    if body.description is not None:
        notebook.description = body.description
    if body.connection_id is not None:
        notebook.connection_id = body.connection_id
    if body.is_template is not None:
        notebook.is_template = body.is_template
    if body.is_public is not None:
        notebook.is_public = body.is_public
    if body.template_category is not None:
        notebook.template_category = body.template_category

    await db.flush()
    await db.refresh(notebook)
    return notebook


@router.delete("/{notebook_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_notebook(
    notebook_id: uuid.UUID, current_user: CurrentUser, db: DbSession,
) -> None:
    """Delete a notebook and all its cells/runs."""
    user = await require_permission(Permission.SAVE_REPORTS, current_user, db)
    notebook = await _load_notebook(db, notebook_id, user)
    await db.delete(notebook)


@router.post("/{notebook_id}/cells", response_model=NotebookResponse)
async def add_cell(
    notebook_id: uuid.UUID, body: NotebookCellCreate, current_user: CurrentUser, db: DbSession,
) -> Notebook:
    """Add a cell to a notebook."""
    user = await require_permission(Permission.SAVE_REPORTS, current_user, db)
    notebook = await _load_notebook(db, notebook_id, user)

    max_order = max((c.order for c in notebook.cells), default=-1)
    order = body.order if body.order > 0 else max_order + 1

    cell = NotebookCell(
        notebook_id=notebook.id,
        order=order,
        cell_type=body.cell_type,
        content=body.content,
        config=body.config,
        output_variable=body.output_variable,
    )
    db.add(cell)
    await db.flush()

    return await _load_notebook(db, notebook_id, user)


@router.patch("/{notebook_id}/cells/{cell_id}", response_model=NotebookResponse)
async def update_cell(
    notebook_id: uuid.UUID, cell_id: uuid.UUID, body: NotebookCellCreate,
    current_user: CurrentUser, db: DbSession,
) -> Notebook:
    """Update a cell's content/config."""
    user = await require_permission(Permission.SAVE_REPORTS, current_user, db)
    notebook = await _load_notebook(db, notebook_id, user)

    cell = next((c for c in notebook.cells if c.id == cell_id), None)
    if cell is None:
        raise HTTPException(status_code=404, detail="Cell not found.")

    cell.cell_type = body.cell_type
    cell.content = body.content
    cell.config = body.config
    cell.output_variable = body.output_variable
    if body.order >= 0:
        cell.order = body.order

    await db.flush()
    return await _load_notebook(db, notebook_id, user)


@router.delete("/{notebook_id}/cells/{cell_id}", response_model=NotebookResponse)
async def delete_cell(
    notebook_id: uuid.UUID, cell_id: uuid.UUID, current_user: CurrentUser, db: DbSession,
) -> Notebook:
    """Delete a cell from a notebook."""
    user = await require_permission(Permission.SAVE_REPORTS, current_user, db)
    notebook = await _load_notebook(db, notebook_id, user)

    cell = next((c for c in notebook.cells if c.id == cell_id), None)
    if cell is None:
        raise HTTPException(status_code=404, detail="Cell not found.")

    await db.delete(cell)
    await db.flush()
    return await _load_notebook(db, notebook_id, user)


@router.post("/{notebook_id}/cells/reorder", response_model=NotebookResponse)
async def reorder_cells(
    notebook_id: uuid.UUID, body: CellReorderRequest, current_user: CurrentUser, db: DbSession,
) -> Notebook:
    """Reorder cells by providing an ordered list of cell IDs."""
    user = await require_permission(Permission.SAVE_REPORTS, current_user, db)
    notebook = await _load_notebook(db, notebook_id, user)

    cell_map = {c.id: c for c in notebook.cells}
    for i, cid in enumerate(body.cell_ids):
        if cid in cell_map:
            cell_map[cid].order = i

    await db.flush()
    return await _load_notebook(db, notebook_id, user)


@router.post("/{notebook_id}/run")
async def run_notebook(
    notebook_id: uuid.UUID, body: NotebookRunRequest,
    current_user: CurrentUser, db: DbSession,
) -> StreamingResponse:
    """Run a notebook and stream cell results as SSE."""
    user = await require_permission(Permission.QUERY_DATA, current_user, db)
    notebook = await _load_notebook(db, notebook_id, user)

    if not notebook.cells:
        raise HTTPException(status_code=400, detail="Notebook has no cells.")

    run = NotebookRun(
        notebook_id=notebook.id,
        user_id=user.id,
        status="running",
        user_inputs=body.inputs or None,
        file_uploads=body.files or None,
        started_at=datetime.utcnow(),
    )
    db.add(run)
    await db.flush()
    await db.commit()

    run_id = run.id
    cells = sorted(notebook.cells, key=lambda c: c.order)

    async def event_stream():
        """Execute cells sequentially and stream results."""
        from app.agents.notebook.orchestrator import execute_notebook_cells

        yield _sse({"type": "run_start", "runId": str(run_id), "totalCells": len(cells)})

        total_ms = 0
        has_error = False

        async for cell_event in execute_notebook_cells(
            cells=cells,
            user_inputs=body.inputs,
            file_uploads=body.files,
            connection_id=str(notebook.connection_id) if notebook.connection_id else None,
            db=db,
        ):
            yield _sse(cell_event)

            if cell_event.get("type") == "cell_complete":
                cell_result = NotebookCellResult(
                    run_id=run_id,
                    cell_id=uuid.UUID(cell_event["cellId"]),
                    cell_order=cell_event.get("cellOrder", 0),
                    status=cell_event.get("status", "success"),
                    output_text=cell_event.get("text", ""),
                    output_table=cell_event.get("table"),
                    output_chart=cell_event.get("chart"),
                    output_code=cell_event.get("code"),
                    error=cell_event.get("error"),
                    execution_time_ms=cell_event.get("executionMs", 0),
                )
                db.add(cell_result)
                total_ms += cell_event.get("executionMs", 0)

                if cell_event.get("status") == "error":
                    has_error = True

        stmt = select(NotebookRun).where(NotebookRun.id == run_id)
        result = await db.execute(stmt)
        run_record = result.scalar_one_or_none()
        if run_record:
            run_record.status = "failed" if has_error else "completed"
            run_record.completed_at = datetime.utcnow()
            run_record.total_execution_ms = total_ms

        stmt2 = select(Notebook).where(Notebook.id == notebook_id)
        result2 = await db.execute(stmt2)
        nb = result2.scalar_one_or_none()
        if nb:
            nb.run_count = (nb.run_count or 0) + 1
            nb.last_run_at = datetime.utcnow()

        await db.commit()

        yield _sse({"type": "run_complete", "runId": str(run_id), "status": "completed" if not has_error else "failed", "totalMs": total_ms})

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.get("/{notebook_id}/runs", response_model=list[NotebookRunResponse])
async def list_runs(
    notebook_id: uuid.UUID, current_user: CurrentUser, db: DbSession,
) -> list[NotebookRun]:
    """List past runs of a notebook."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    await _load_notebook(db, notebook_id, user)
    stmt = (
        select(NotebookRun)
        .options(selectinload(NotebookRun.cell_results))
        .where(NotebookRun.notebook_id == notebook_id)
        .order_by(NotebookRun.created_at.desc())
        .limit(20)
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.get("/{notebook_id}/runs/{run_id}", response_model=NotebookRunResponse)
async def get_run(
    notebook_id: uuid.UUID, run_id: uuid.UUID, current_user: CurrentUser, db: DbSession,
) -> NotebookRun:
    """Get a specific run with all cell results."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    await _load_notebook(db, notebook_id, user)
    stmt = (
        select(NotebookRun)
        .options(selectinload(NotebookRun.cell_results))
        .where(NotebookRun.id == run_id, NotebookRun.notebook_id == notebook_id)
    )
    result = await db.execute(stmt)
    run = result.scalar_one_or_none()
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found.")
    return run


def _sse(data: dict) -> str:
    """Format as SSE event."""
    return f"data: {json.dumps(data, default=str)}\n\n"
