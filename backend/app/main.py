"""FastAPI application entry-point for the Ceaser backend."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api import (
    admin,
    audit,
    auth,
    billing,
    chat,
    connections,
    feedback,
    files,
    join_rules,
    memories,
    metrics,
    notebooks,
    onboarding,
    reports,
    verified_queries,
)
from app.core.config import get_settings
from app.db.session import Base, engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Create database tables on startup (dev convenience) and dispose on shutdown."""
    logger.info("Starting Ceaser backend...")
    os.makedirs("uploads", exist_ok=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Idempotent column additions — create_all only adds new tables, not
        # new columns on existing tables. These run on every startup but are
        # no-ops once the columns exist.
        from sqlalchemy import text

        await conn.execute(
            text(
                "ALTER TABLE file_uploads "
                "ADD COLUMN IF NOT EXISTS processing_status VARCHAR(32) NOT NULL DEFAULT 'pending'"
            )
        )
        await conn.execute(
            text("ALTER TABLE file_uploads ADD COLUMN IF NOT EXISTS processing_error TEXT")
        )
        await conn.execute(
            text("ALTER TABLE file_uploads ADD COLUMN IF NOT EXISTS parquet_s3_key TEXT")
        )
    logger.info("Database tables ensured.")
    try:
        from app.services.memory_graph import ensure_vector_index

        await ensure_vector_index()
    except Exception as exc:
        logger.debug("Neo4j vector index setup skipped: %s", exc)
    # Start background notebook scheduler
    from app.services.scheduler import scheduler_loop

    scheduler_task = asyncio.create_task(scheduler_loop())

    yield

    # Shutdown
    scheduler_task.cancel()
    await engine.dispose()
    try:
        from app.services.schema_graph import close_graph_driver

        await close_graph_driver()
    except Exception:
        pass
    logger.info("Ceaser backend shut down.")


settings = get_settings()

app = FastAPI(
    title="Ceaser",
    description="AI-powered data analysis platform API",
    version="0.1.0",
    lifespan=lifespan,
    redirect_slashes=False,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)

_API_PREFIX = "/api/v1"

app.include_router(auth.router, prefix=_API_PREFIX)
app.include_router(chat.router, prefix=_API_PREFIX)
app.include_router(connections.router, prefix=_API_PREFIX)
app.include_router(files.router, prefix=_API_PREFIX)
app.include_router(metrics.router, prefix=_API_PREFIX)
app.include_router(audit.router, prefix=_API_PREFIX)
app.include_router(reports.router, prefix=_API_PREFIX)
app.include_router(onboarding.router, prefix=_API_PREFIX)
app.include_router(notebooks.router, prefix=_API_PREFIX)
app.include_router(billing.router, prefix=_API_PREFIX)
app.include_router(memories.router, prefix=_API_PREFIX)
app.include_router(feedback.router, prefix=_API_PREFIX)
app.include_router(verified_queries.router, prefix=_API_PREFIX)
app.include_router(join_rules.router, prefix=_API_PREFIX)
app.include_router(admin.router, prefix=_API_PREFIX)


@app.get("/health", tags=["health"])
async def health_check() -> dict[str, str]:
    """Lightweight liveness probe."""
    return {"status": "ok"}


@app.exception_handler(ValueError)
async def value_error_handler(_request: Request, exc: ValueError) -> JSONResponse:
    """Return 400 for ValueError raised in business logic."""
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": str(exc)},
    )


@app.exception_handler(Exception)
async def generic_error_handler(_request: Request, exc: Exception) -> JSONResponse:
    """Catch-all for unhandled exceptions — log and return 500."""
    logger.exception("Unhandled exception: %s", exc)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "An internal error occurred."},
    )
