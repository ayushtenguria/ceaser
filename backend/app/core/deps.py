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
    settings: Settings | None = None,
) -> BaseChatModel:
    """Return a configured LangChain chat model.

    Parameters
    ----------
    model:
        ``"gemini"`` (default) or ``"claude"``.
    settings:
        Optional override; falls back to the cached singleton.
    """
    settings = settings or get_settings()

    if model == "claude":
        return ChatAnthropic(
            model="claude-sonnet-4-20250514",
            anthropic_api_key=settings.anthropic_api_key,
            temperature=0,
            max_tokens=4096,
        )

    # Default: Gemini
    return ChatGoogleGenerativeAI(
        model="gemini-2.0-flash",
        google_api_key=settings.gemini_api_key,
        temperature=0,
        max_output_tokens=4096,
    )


# Annotated shortcuts for use in route signatures
CurrentUser = Annotated[AuthenticatedUser, Depends(verify_token)]
DbSession = Annotated[AsyncSession, Depends(get_db)]
