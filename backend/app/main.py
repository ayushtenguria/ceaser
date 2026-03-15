"""FastAPI application entry-point for the Ceaser backend."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api import auth, chat, connections, files
from app.core.config import get_settings
from app.db.session import Base, engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Create database tables on startup (dev convenience) and dispose on shutdown."""
    logger.info("Starting Ceaser backend...")
    os.makedirs("uploads", exist_ok=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables ensured.")
    yield
    await engine.dispose()
    logger.info("Ceaser backend shut down.")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

settings = get_settings()

app = FastAPI(
    title="Ceaser",
    description="AI-powered data analysis platform API",
    version="0.1.0",
    lifespan=lifespan,
)

# ── CORS ────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ─────────────────────────────────────────────────────────────────
_API_PREFIX = "/api/v1"

app.include_router(auth.router, prefix=_API_PREFIX)
app.include_router(chat.router, prefix=_API_PREFIX)
app.include_router(connections.router, prefix=_API_PREFIX)
app.include_router(files.router, prefix=_API_PREFIX)


# ── Health check ────────────────────────────────────────────────────────────

@app.get("/health", tags=["health"])
async def health_check() -> dict[str, str]:
    """Lightweight liveness probe."""
    return {"status": "ok"}


# ── Exception handlers ─────────────────────────────────────────────────────

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
