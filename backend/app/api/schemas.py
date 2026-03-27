"""Pydantic v2 request / response schemas for the API layer."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator


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
    role: str = "member"
    is_super_admin: bool = False
    created_at: datetime


# ---------------------------------------------------------------------------
# Chat
# ---------------------------------------------------------------------------

class ChatRequest(_CamelModel):
    """Incoming chat message from the frontend."""

    message: str = Field(..., min_length=1, max_length=10_000)
    conversation_id: uuid.UUID | None = None
    connection_id: uuid.UUID | None = None
    connection_ids: list[uuid.UUID] | None = None  # For cross-DB queries
    file_id: uuid.UUID | None = None
    model: str = Field(default="gemini", pattern="^(gemini|claude)$")

    @field_validator("message")
    @classmethod
    def strip_null_bytes(cls, v: str) -> str:
        """Remove null bytes that PostgreSQL text columns cannot store."""
        return v.replace("\x00", "")


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
    excel_metadata: dict[str, Any] | None = None
    created_at: datetime


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------

class ReportCreate(_CamelModel):
    """Create a saved report from a chat result."""
    name: str = Field(..., min_length=1, max_length=500)
    description: str = ""
    connection_id: uuid.UUID | None = None
    file_id: uuid.UUID | None = None
    sql_query: str | None = None
    python_code: str | None = None
    original_question: str = ""
    table_data: dict[str, Any] | None = None
    plotly_figure: dict[str, Any] | None = None
    summary_text: str = ""
    schedule: str | None = Field(None, pattern="^(hourly|daily|weekly)?$")

class ReportUpdate(_CamelModel):
    """Update a saved report."""
    name: str | None = None
    description: str | None = None
    schedule: str | None = None
    is_pinned: bool | None = None
    is_active: bool | None = None

class ReportResponse(BaseModel):
    """Public representation of a saved report."""
    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)

    id: uuid.UUID
    name: str
    description: str
    connection_id: uuid.UUID | None
    file_id: uuid.UUID | None
    sql_query: str | None
    python_code: str | None
    original_question: str
    table_data: dict[str, Any] | None
    plotly_figure: dict[str, Any] | None
    summary_text: str
    schedule: str | None
    last_run_at: datetime | None
    next_run_at: datetime | None
    is_active: bool
    is_pinned: bool
    organization_id: str
    created_at: datetime
    updated_at: datetime


# ---------------------------------------------------------------------------
# Metrics (Semantic Layer)
# ---------------------------------------------------------------------------

class MetricCreate(_CamelModel):
    """Define a new business metric."""
    name: str = Field(..., min_length=1, max_length=200)
    description: str = ""
    sql_expression: str = Field(..., min_length=1)
    category: str = "general"
    connection_id: uuid.UUID | None = None

class MetricUpdate(_CamelModel):
    """Update a metric definition."""
    name: str | None = None
    description: str | None = None
    sql_expression: str | None = None
    category: str | None = None
    is_locked: bool | None = None

class MetricResponse(BaseModel):
    """Public representation of a metric definition."""
    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)

    id: uuid.UUID
    name: str
    description: str
    sql_expression: str
    category: str
    connection_id: uuid.UUID | None
    organization_id: str
    is_locked: bool = False
    created_at: datetime
    updated_at: datetime


# ---------------------------------------------------------------------------
# Audit Log
# ---------------------------------------------------------------------------

class AuditLogResponse(BaseModel):
    """Public representation of an audit log entry."""
    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)

    id: uuid.UUID
    user_id: uuid.UUID | None
    action: str
    resource_type: str
    resource_id: str
    details: dict[str, Any] | None
    ip_address: str
    created_at: datetime


# ---------------------------------------------------------------------------
# Notebooks
# ---------------------------------------------------------------------------

class NotebookCellCreate(_CamelModel):
    """Create/update a notebook cell."""
    cell_type: str = Field(..., pattern="^(text|file|input|prompt|code)$")
    content: str = ""
    config: dict[str, Any] | None = None
    output_variable: str = ""
    order: int = 0

class NotebookCreate(_CamelModel):
    """Create a new notebook."""
    name: str = Field(..., min_length=1, max_length=500)
    description: str = ""
    connection_id: uuid.UUID | None = None
    cells: list[NotebookCellCreate] = []

class NotebookUpdate(_CamelModel):
    """Update notebook metadata."""
    name: str | None = None
    description: str | None = None
    connection_id: uuid.UUID | None = None
    is_template: bool | None = None
    is_public: bool | None = None
    template_category: str | None = None

class NotebookCellResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)
    id: uuid.UUID
    notebook_id: uuid.UUID
    order: int
    cell_type: str
    content: str
    config: dict[str, Any] | None
    output_variable: str

class NotebookResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)
    id: uuid.UUID
    name: str
    description: str
    organization_id: str
    connection_id: uuid.UUID | None
    is_template: bool
    is_public: bool
    template_category: str
    last_run_at: datetime | None
    run_count: int
    cells: list[NotebookCellResponse] = []
    created_at: datetime
    updated_at: datetime

class NotebookCellResultResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)
    id: uuid.UUID
    cell_id: uuid.UUID
    cell_order: int
    status: str
    output_text: str
    output_table: dict[str, Any] | None
    output_chart: dict[str, Any] | None
    output_code: str | None
    error: str | None
    execution_time_ms: int

class NotebookRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)
    id: uuid.UUID
    notebook_id: uuid.UUID
    status: str
    user_inputs: dict[str, Any] | None
    file_uploads: dict[str, Any] | None
    started_at: datetime | None
    completed_at: datetime | None
    total_execution_ms: int
    error: str | None
    cell_results: list[NotebookCellResultResponse] = []
    created_at: datetime

class NotebookRunRequest(_CamelModel):
    """Request to run a notebook."""
    inputs: dict[str, Any] = {}      # {cell_id: value}
    files: dict[str, str] = {}       # {cell_id: file_id}

class CellReorderRequest(_CamelModel):
    """Reorder cells."""
    cell_ids: list[uuid.UUID]  # ordered list of cell IDs
