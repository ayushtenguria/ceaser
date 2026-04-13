"""Saved reports — CRUD and refresh endpoints."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from app.agents.graph import run_agent
from app.api.schemas import ReportCreate, ReportResponse, ReportUpdate
from app.core.deps import CurrentUser, DbSession, get_llm
from app.core.permissions import Permission, require_permission
from app.db.models import Report, User

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/reports", tags=["reports"])


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


def _compute_next_run(schedule: str | None) -> datetime | None:
    """Compute the next run time based on schedule."""
    if not schedule:
        return None
    now = datetime.utcnow()
    if schedule == "hourly":
        return now + timedelta(hours=1)
    elif schedule == "daily":
        return now + timedelta(days=1)
    elif schedule == "weekly":
        return now + timedelta(weeks=1)
    return None


@router.post("/", response_model=ReportResponse, status_code=status.HTTP_201_CREATED)
async def create_report(
    body: ReportCreate,
    current_user: CurrentUser,
    db: DbSession,
) -> Report:
    """Save a new report from chat results."""
    user = await require_permission(Permission.SAVE_REPORTS, current_user, db)
    from app.core.features import Feature, check_feature

    await check_feature(Feature.REPORTS, db, user.organization_id or "")

    report = Report(
        name=body.name,
        description=body.description,
        connection_id=body.connection_id,
        file_id=body.file_id,
        sql_query=body.sql_query,
        python_code=body.python_code,
        original_question=body.original_question,
        table_data=body.table_data,
        plotly_figure=body.plotly_figure,
        summary_text=body.summary_text,
        schedule=body.schedule,
        last_run_at=datetime.utcnow(),
        next_run_at=_compute_next_run(body.schedule),
        user_id=user.id,
        organization_id=user.organization_id or current_user.org_id or "",
    )
    db.add(report)
    await db.flush()
    await db.refresh(report)
    logger.info("Created report '%s' for user %s", body.name, user.id)
    return report


@router.get("/", response_model=list[ReportResponse])
async def list_reports(current_user: CurrentUser, db: DbSession) -> list[Report]:
    """List all reports for the current user's organization."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    stmt = (
        select(Report)
        .where(Report.organization_id == (user.organization_id or current_user.org_id or ""))
        .order_by(Report.is_pinned.desc(), Report.updated_at.desc())
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.get("/{report_id}", response_model=ReportResponse)
async def get_report(
    report_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> Report:
    """Get a single report."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    report = await _load_report(db, report_id, user.id)
    return report


@router.patch("/{report_id}", response_model=ReportResponse)
async def update_report(
    report_id: uuid.UUID,
    body: ReportUpdate,
    current_user: CurrentUser,
    db: DbSession,
) -> Report:
    """Update a report's metadata (name, description, schedule, pinned)."""
    user = await require_permission(Permission.SAVE_REPORTS, current_user, db)
    report = await _load_report(db, report_id, user.id)

    if body.name is not None:
        report.name = body.name
    if body.description is not None:
        report.description = body.description
    if body.schedule is not None:
        report.schedule = body.schedule or None
        report.next_run_at = _compute_next_run(body.schedule)
    if body.is_pinned is not None:
        report.is_pinned = body.is_pinned
    if body.is_active is not None:
        report.is_active = body.is_active

    await db.flush()
    await db.refresh(report)
    return report


@router.post("/{report_id}/refresh", response_model=ReportResponse)
async def refresh_report(
    report_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> Report:
    """Re-run a report's query and update cached results."""
    user = await require_permission(Permission.SAVE_REPORTS, current_user, db)
    report = await _load_report(db, report_id, user.id)

    if not report.sql_query and not report.original_question:
        raise HTTPException(status_code=400, detail="Report has no query to refresh.")

    schema_context = ""
    if report.connection_id:
        from app.api.chat import _build_schema_context

        schema_context = await _build_schema_context(db, report.connection_id, report.file_id)

    llm = get_llm()

    new_table = None
    new_plotly = None
    new_text = ""
    new_sql = report.sql_query

    async for chunk in run_agent(
        query=report.original_question or f"Run this SQL: {report.sql_query}",
        connection_id=str(report.connection_id) if report.connection_id else None,
        file_id=str(report.file_id) if report.file_id else None,
        schema_context=schema_context,
        llm=llm,
        db=db,
    ):
        chunk_type = chunk.get("type", "")
        if chunk_type == "table":
            new_table = chunk.get("content") or chunk.get("data")
        elif chunk_type == "plotly":
            new_plotly = chunk.get("content") or chunk.get("data")
        elif chunk_type == "text":
            new_text = chunk.get("content", "")
        elif chunk_type == "sql":
            new_sql = chunk.get("content")

    if new_table is not None:
        report.table_data = new_table
    if new_plotly is not None:
        report.plotly_figure = new_plotly
    if new_text:
        report.summary_text = new_text
    if new_sql:
        report.sql_query = new_sql
    report.last_run_at = datetime.utcnow()
    report.next_run_at = _compute_next_run(report.schedule)

    await db.flush()
    await db.refresh(report)
    logger.info("Refreshed report '%s'", report.name)
    return report


@router.delete("/{report_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_report(
    report_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> None:
    """Delete a report."""
    user = await require_permission(Permission.SAVE_REPORTS, current_user, db)
    report = await _load_report(db, report_id, user.id)
    await db.delete(report)


async def _load_report(db: DbSession, report_id: uuid.UUID, user_id: uuid.UUID) -> Report:
    """Load a report and verify ownership."""
    stmt = select(Report).where(Report.id == report_id, Report.user_id == user_id)
    result = await db.execute(stmt)
    report = result.scalar_one_or_none()
    if report is None:
        raise HTTPException(status_code=404, detail="Report not found.")
    return report
