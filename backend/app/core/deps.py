"""Shared FastAPI dependencies."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends
from langchain_anthropic import ChatAnthropic
from langchain_core.language_models import BaseChatModel
from langchain_google_genai import ChatGoogleGenerativeAI
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings, get_settings
from app.core.security import AuthenticatedUser, verify_token
from app.db.session import async_session_factory


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Yield an async SQLAlchemy session, rolling back on error."""
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def get_llm(
    model: str = "gemini",
    tier: str = "heavy",
    settings: Settings | None = None,
) -> BaseChatModel:
    """Return a configured LangChain chat model.

    Parameters
    ----------
    model:
        ``"gemini"`` (default) or ``"claude"`` — selects the provider.
    tier:
        ``"heavy"`` (default) — best quality model for SQL, code, analysis.
        ``"light"`` — fast/cheap model for routing, verification, extraction.
    settings:
        Optional override; falls back to the cached singleton.

    Model mapping:
        heavy  → gemini-3-flash (or claude if selected)
        light  → gemini-3.1-flash-lite
    """
    settings = settings or get_settings()

    if model == "claude":
        return ChatAnthropic(
            model=settings.claude_model,
            anthropic_api_key=settings.anthropic_api_key,
            temperature=0,
            max_tokens=4096,
        )

    model_name = settings.gemini_model_light if tier == "light" else settings.gemini_model

    return ChatGoogleGenerativeAI(
        model=model_name,
        google_api_key=settings.gemini_api_key,
        temperature=0,
        max_output_tokens=4096,
    )


CurrentUser = Annotated[AuthenticatedUser, Depends(verify_token)]
DbSession = Annotated[AsyncSession, Depends(get_db)]
