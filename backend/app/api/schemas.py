"""Pydantic v2 request / response schemas for the API layer."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, EmailStr, Field


def to_camel(s: str) -> str:
    """Convert snake_case to camelCase for JSON serialisation."""
    parts = s.split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class _CamelModel(BaseModel):
    """Base model that accepts both camelCase and snake_case input."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class UserSyncRequest(_CamelModel):
    """Payload sent by Clerk webhook or frontend to sync user data."""

    clerk_id: str
    email: EmailStr
    first_name: str
    last_name: str = ""
    organization_id: str | None = None
    image_url: str | None = None


class UserResponse(BaseModel):
    """Public representation of a user."""

    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)

    id: uuid.UUID
    clerk_id: str
    email: str
    first_name: str
    last_name: str
    organization_id: str | None
    image_url: str | None
    created_at: datetime


# ---------------------------------------------------------------------------
# Chat
# ---------------------------------------------------------------------------

class ChatRequest(_CamelModel):
    """Incoming chat message from the frontend."""

    message: str = Field(..., min_length=1, max_length=10_000)
    conversation_id: uuid.UUID | None = None
    connection_id: uuid.UUID | None = None
    file_id: uuid.UUID | None = None
    model: str = Field(default="gemini", pattern="^(gemini|claude)$")


class StreamChunk(BaseModel):
    """A single server-sent event chunk."""

    type: str  # status, sql, code, text, table, plotly, error
    content: Any


class ChatResponse(BaseModel):
    """Final response after the stream completes (used by non-streaming callers)."""

    conversation_id: uuid.UUID
    message_id: uuid.UUID
    content: str
    message_type: str
    sql_query: str | None = None
    code_block: str | None = None
    table_data: dict[str, Any] | None = None
    plotly_figure: dict[str, Any] | None = None
    error: str | None = None


# ---------------------------------------------------------------------------
# Conversations
# ---------------------------------------------------------------------------

class ConversationResponse(BaseModel):
    """Conversation list / detail representation."""

    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)

    id: uuid.UUID
    title: str
    connection_id: uuid.UUID | None
    file_id: uuid.UUID | None
    created_at: datetime
    updated_at: datetime


class MessageResponse(BaseModel):
    """Single message inside a conversation."""

    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)

    id: uuid.UUID
    conversation_id: uuid.UUID
    role: str
    content: str
    message_type: str
    sql_query: str | None = None
    code_block: str | None = None
    plotly_figure: dict[str, Any] | None = None
    table_data: dict[str, Any] | None = None
    error: str | None = None
    created_at: datetime


# ---------------------------------------------------------------------------
# Database connections
# ---------------------------------------------------------------------------

class ConnectionCreate(_CamelModel):
    """Payload to register a new external database connection."""

    name: str = Field(..., min_length=1, max_length=255)
    db_type: str = Field(..., pattern="^(postgresql|mysql|sqlite|bigquery|snowflake)$")
    host: str = ""
    port: int = 5432
    database: str = Field(..., min_length=1)
    username: str = ""
    password: str = ""  # plaintext; encrypted before storage


class ConnectionResponse(BaseModel):
    """Public representation of a database connection (no password)."""

    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)

    id: uuid.UUID
    name: str
    db_type: str
    host: str
    port: int
    database: str
    username: str
    is_connected: bool
    organization_id: str
    schema_cache: dict[str, Any] | None = None
    created_at: datetime
    updated_at: datetime


class ConnectionTestResult(BaseModel):
    """Result of testing a database connection."""

    success: bool
    message: str
    schema_info: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# File uploads
# ---------------------------------------------------------------------------

class FileUploadResponse(BaseModel):
    """Public representation of an uploaded file."""

    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)

    id: uuid.UUID
    filename: str
    file_type: str
    size_bytes: int
    column_info: dict[str, Any] | None = None
    created_at: datetime
