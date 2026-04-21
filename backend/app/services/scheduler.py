"""Background notebook scheduler — checks for due notebooks every 60 seconds.

Uses a simple asyncio loop instead of APScheduler to avoid extra dependencies.
Runs inside the FastAPI process via the lifespan context manager.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from croniter import croniter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Notebook, NotebookRun
from app.db.session import async_session_factory

logger = logging.getLogger(__name__)

_CHECK_INTERVAL = 60  # seconds


async def _compute_next_run(cron_expr: str) -> datetime:
    """Compute the next run time from a cron expression."""
    now = datetime.now(UTC)
    cron = croniter(cron_expr, now)
    return cron.get_next(datetime).replace(tzinfo=UTC)


async def _execute_notebook(notebook_id, session: AsyncSession) -> None:
    """Execute all cells of a notebook and record the run."""
    from app.agents.notebook.orchestrator import execute_notebook_cells
    from app.core.deps import get_llm

    stmt = select(Notebook).where(Notebook.id == notebook_id)
    result = await session.execute(stmt)
    notebook = result.scalar_one_or_none()
    if not notebook:
        logger.warning("Scheduled notebook %s not found", notebook_id)
        return

    llm = get_llm(tier="heavy")

    # Create a run record
    run = NotebookRun(
        notebook_id=notebook.id,
        status="running",
        triggered_by="scheduler",
    )
    session.add(run)
    await session.flush()

    logger.info("Scheduler: executing notebook '%s' (run %s)", notebook.name, run.id)

    try:
        cell_results = []
        async for chunk in execute_notebook_cells(
            notebook=notebook,
            run_id=run.id,
            llm=llm,
            db=session,
        ):
            cell_results.append(chunk)

        run.status = "completed"
        run.completed_at = datetime.now(UTC)
        notebook.last_run_at = run.completed_at
        notebook.run_count = (notebook.run_count or 0) + 1

        # Compute next run
        if notebook.schedule:
            notebook.next_run_at = await _compute_next_run(notebook.schedule)

        logger.info(
            "Scheduler: notebook '%s' completed (%d cell results)",
            notebook.name,
            len(cell_results),
        )

    except Exception as exc:
        run.status = "failed"
        run.error = str(exc)[:1000]
        logger.exception("Scheduler: notebook '%s' failed: %s", notebook.name, exc)

    await session.flush()


async def scheduler_loop() -> None:
    """Main scheduler loop — runs forever, checks for due notebooks every 60s."""
    logger.info("Notebook scheduler started (checking every %ds)", _CHECK_INTERVAL)

    while True:
        try:
            async with async_session_factory() as session:
                # First check if any notebooks are scheduled at all (cheap query)
                count_stmt = select(Notebook).where(
                    Notebook.is_scheduled.is_(True),
                    Notebook.schedule.isnot(None),
                )
                count_result = await session.execute(count_stmt)
                scheduled = list(count_result.scalars().all())

                if not scheduled:
                    await asyncio.sleep(_CHECK_INTERVAL)
                    continue

                # Check which are due
                now = datetime.now(UTC)
                due_notebooks = [
                    nb
                    for nb in scheduled
                    if nb.next_run_at is not None and nb.next_run_at.replace(tzinfo=UTC) <= now
                ]

                if due_notebooks:
                    logger.info("Scheduler: %d notebook(s) due for execution", len(due_notebooks))

                for nb in due_notebooks:
                    try:
                        await _execute_notebook(nb.id, session)
                    except Exception as exc:
                        logger.exception("Scheduler: failed to execute notebook %s: %s", nb.id, exc)

                await session.commit()

        except Exception as exc:
            logger.warning("Scheduler loop error (will retry): %s", exc)

        await asyncio.sleep(_CHECK_INTERVAL)
