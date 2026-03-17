"""SQLAlchemy 2.0 ORM models for the Ceaser platform."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import ForeignKey, Index, JSON, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.session import Base


class User(Base):
    """Platform user synced from Clerk."""

    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    clerk_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    email: Mapped[str] = mapped_column(String(255), unique=True)
    first_name: Mapped[str] = mapped_column(String(255))
    last_name: Mapped[str] = mapped_column(String(255), default="")
    organization_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    image_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    role: Mapped[str] = mapped_column(String(50), default="member")  # "super_admin", "admin", "member", "viewer"
    is_super_admin: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())

    # Relationships
    conversations: Mapped[list[Conversation]] = relationship(back_populates="user")
    connections: Mapped[list[DatabaseConnection]] = relationship(back_populates="user")
    file_uploads: Mapped[list[FileUpload]] = relationship(back_populates="user")


class OrganizationPlan(Base):
    """Subscription plan and seat limits per organization."""

    __tablename__ = "organization_plans"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    plan_name: Mapped[str] = mapped_column(String(100), default="free")  # free, starter, business, enterprise
    max_seats: Mapped[int] = mapped_column(default=5)
    max_connections: Mapped[int] = mapped_column(default=1)
    max_queries_per_day: Mapped[int] = mapped_column(default=50)
    max_reports: Mapped[int] = mapped_column(default=5)
    features: Mapped[dict | None] = mapped_column(JSON, nullable=True)  # Feature flags
    is_active: Mapped[bool] = mapped_column(default=True)
    trial_ends_at: Mapped[datetime | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())


class DatabaseConnection(Base):
    """Encrypted reference to a client's external database."""

    __tablename__ = "database_connections"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255))
    db_type: Mapped[str] = mapped_column(String(50))  # postgresql, mysql, sqlite, bigquery, snowflake
    host: Mapped[str] = mapped_column(String(255), default="")
    port: Mapped[int] = mapped_column(default=5432)
    database: Mapped[str] = mapped_column(String(255))
    username: Mapped[str] = mapped_column(String(255), default="")
    encrypted_password: Mapped[str] = mapped_column(Text, default="")
    is_connected: Mapped[bool] = mapped_column(default=False)
    organization_id: Mapped[str] = mapped_column(String(255), index=True)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    schema_cache: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())

    user: Mapped[User] = relationship(back_populates="connections")
    conversations: Mapped[list[Conversation]] = relationship(back_populates="connection")


class Conversation(Base):
    """A chat session between a user and the AI agent."""

    __tablename__ = "conversations"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    title: Mapped[str] = mapped_column(String(500), default="New Conversation")
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    connection_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("database_connections.id"), nullable=True
    )
    file_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("file_uploads.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())

    user: Mapped[User] = relationship(back_populates="conversations")
    connection: Mapped[DatabaseConnection | None] = relationship(back_populates="conversations")
    file: Mapped[FileUpload | None] = relationship()
    messages: Mapped[list[Message]] = relationship(
        back_populates="conversation", order_by="Message.created_at"
    )


class Message(Base):
    """Single message (user or assistant) inside a conversation."""

    __tablename__ = "messages"
    __table_args__ = (Index("ix_messages_conversation_created", "conversation_id", "created_at"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    conversation_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("conversations.id"))
    role: Mapped[str] = mapped_column(String(20))  # "user" or "assistant"
    content: Mapped[str] = mapped_column(Text, default="")
    message_type: Mapped[str] = mapped_column(
        String(50), default="text"
    )  # text, sql_result, visualization, code_execution, error
    sql_query: Mapped[str | None] = mapped_column(Text, nullable=True)
    code_block: Mapped[str | None] = mapped_column(Text, nullable=True)
    plotly_figure: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    table_data: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    conversation: Mapped[Conversation] = relationship(back_populates="messages")


class FileUpload(Base):
    """Metadata for a user-uploaded CSV / Excel / PDF file."""

    __tablename__ = "file_uploads"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    filename: Mapped[str] = mapped_column(String(500))
    file_type: Mapped[str] = mapped_column(String(50))
    file_path: Mapped[str] = mapped_column(Text)
    size_bytes: Mapped[int] = mapped_column()
    organization_id: Mapped[str] = mapped_column(String(255))
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    column_info: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    user: Mapped[User] = relationship(back_populates="file_uploads")


class Report(Base):
    """A saved report created from chat analysis results."""

    __tablename__ = "reports"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(500))
    description: Mapped[str] = mapped_column(Text, default="")

    # Source
    connection_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("database_connections.id"), nullable=True)
    file_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("file_uploads.id"), nullable=True)

    # The saved query/analysis
    sql_query: Mapped[str | None] = mapped_column(Text, nullable=True)
    python_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    original_question: Mapped[str] = mapped_column(Text, default="")

    # Cached results (last run)
    table_data: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    plotly_figure: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    summary_text: Mapped[str] = mapped_column(Text, default="")

    # Schedule (cron-like)
    schedule: Mapped[str | None] = mapped_column(String(50), nullable=True)  # "hourly", "daily", "weekly", or None
    last_run_at: Mapped[datetime | None] = mapped_column(nullable=True)
    next_run_at: Mapped[datetime | None] = mapped_column(nullable=True)
    is_active: Mapped[bool] = mapped_column(default=True)

    # Ownership
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    organization_id: Mapped[str] = mapped_column(String(255), default="")
    is_pinned: Mapped[bool] = mapped_column(default=False)

    created_at: Mapped[datetime] = mapped_column(default=func.now())
    updated_at: Mapped[datetime] = mapped_column(default=func.now(), onupdate=func.now())

    # Relationships
    user: Mapped["User"] = relationship()
    connection: Mapped["DatabaseConnection | None"] = relationship()


class AuditLog(Base):
    """Audit trail for user actions across the platform."""

    __tablename__ = "audit_logs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    action: Mapped[str] = mapped_column(String(100))  # "chat_query", "report_created", "connection_created", "file_uploaded", etc.
    resource_type: Mapped[str] = mapped_column(String(100))  # "conversation", "report", "connection", "file"
    resource_id: Mapped[str] = mapped_column(String(255), default="")
    details: Mapped[dict | None] = mapped_column(JSON, nullable=True)  # Stores query text, SQL, etc.
    ip_address: Mapped[str] = mapped_column(String(50), default="")
    created_at: Mapped[datetime] = mapped_column(default=func.now())


class MetricDefinition(Base):
    """A named metric in the semantic layer."""

    __tablename__ = "metric_definitions"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(200), index=True)
    description: Mapped[str] = mapped_column(Text, default="")
    sql_expression: Mapped[str] = mapped_column(Text)  # e.g., "SUM(revenue.amount) WHERE revenue.type = 'subscription'"
    category: Mapped[str] = mapped_column(String(100), default="general")
    connection_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("database_connections.id"), nullable=True)
    organization_id: Mapped[str] = mapped_column(String(255), default="")
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    created_at: Mapped[datetime] = mapped_column(default=func.now())
    updated_at: Mapped[datetime] = mapped_column(default=func.now(), onupdate=func.now())
