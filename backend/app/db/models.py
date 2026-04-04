"""SQLAlchemy 2.0 ORM models for the Ceaser platform."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import ForeignKey, Index, JSON, String, Text, UniqueConstraint, func
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
    role: Mapped[str] = mapped_column(String(50), default="member")
    is_super_admin: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())

    conversations: Mapped[list[Conversation]] = relationship(back_populates="user")
    connections: Mapped[list[DatabaseConnection]] = relationship(back_populates="user")
    file_uploads: Mapped[list[FileUpload]] = relationship(back_populates="user")


class OrganizationPlan(Base):
    """Subscription plan and seat limits per organization."""

    __tablename__ = "organization_plans"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    plan_name: Mapped[str] = mapped_column(String(100), default="free")
    max_seats: Mapped[int] = mapped_column(default=5)
    max_connections: Mapped[int] = mapped_column(default=1)
    max_queries_per_day: Mapped[int] = mapped_column(default=50)
    max_reports: Mapped[int] = mapped_column(default=5)
    features: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    is_active: Mapped[bool] = mapped_column(default=True)
    trial_ends_at: Mapped[datetime | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())


class DatabaseConnection(Base):
    """Encrypted reference to a client's external database."""

    __tablename__ = "database_connections"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255))
    db_type: Mapped[str] = mapped_column(String(50))
    host: Mapped[str] = mapped_column(String(255), default="")
    port: Mapped[int] = mapped_column(default=5432)
    database: Mapped[str] = mapped_column(String(255))
    username: Mapped[str] = mapped_column(String(255), default="")
    encrypted_password: Mapped[str] = mapped_column(Text, default="")
    is_connected: Mapped[bool] = mapped_column(default=False)
    organization_id: Mapped[str] = mapped_column(String(255), index=True)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    schema_cache: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    metrics_scanned: Mapped[bool] = mapped_column(default=False)
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
    file_ids: Mapped[list | None] = mapped_column(JSON, nullable=True)
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
    role: Mapped[str] = mapped_column(String(20))
    content: Mapped[str] = mapped_column(Text, default="")
    message_type: Mapped[str] = mapped_column(
        String(50), default="text"
    )
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
    excel_context: Mapped[str | None] = mapped_column(Text, nullable=True)
    code_preamble: Mapped[str | None] = mapped_column(Text, nullable=True)
    parquet_paths: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    excel_metadata: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    user: Mapped[User] = relationship(back_populates="file_uploads")


class Report(Base):
    """A saved report created from chat analysis results."""

    __tablename__ = "reports"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(500))
    description: Mapped[str] = mapped_column(Text, default="")

    connection_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("database_connections.id"), nullable=True)
    file_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("file_uploads.id"), nullable=True)

    sql_query: Mapped[str | None] = mapped_column(Text, nullable=True)
    python_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    original_question: Mapped[str] = mapped_column(Text, default="")

    table_data: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    plotly_figure: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    summary_text: Mapped[str] = mapped_column(Text, default="")

    schedule: Mapped[str | None] = mapped_column(String(50), nullable=True)
    last_run_at: Mapped[datetime | None] = mapped_column(nullable=True)
    next_run_at: Mapped[datetime | None] = mapped_column(nullable=True)
    is_active: Mapped[bool] = mapped_column(default=True)

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    organization_id: Mapped[str] = mapped_column(String(255), default="")
    is_pinned: Mapped[bool] = mapped_column(default=False)

    created_at: Mapped[datetime] = mapped_column(default=func.now())
    updated_at: Mapped[datetime] = mapped_column(default=func.now(), onupdate=func.now())

    user: Mapped["User"] = relationship()
    connection: Mapped["DatabaseConnection | None"] = relationship()


class AuditLog(Base):
    """Audit trail for user actions across the platform."""

    __tablename__ = "audit_logs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    action: Mapped[str] = mapped_column(String(100))
    resource_type: Mapped[str] = mapped_column(String(100))
    resource_id: Mapped[str] = mapped_column(String(255), default="")
    details: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    ip_address: Mapped[str] = mapped_column(String(50), default="")
    created_at: Mapped[datetime] = mapped_column(default=func.now())


class MetricDefinition(Base):
    """A named metric in the semantic layer."""

    __tablename__ = "metric_definitions"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(200), index=True)
    description: Mapped[str] = mapped_column(Text, default="")
    sql_expression: Mapped[str] = mapped_column(Text)
    category: Mapped[str] = mapped_column(String(100), default="general")
    connection_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("database_connections.id"), nullable=True)
    organization_id: Mapped[str] = mapped_column(String(255), default="")
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    is_locked: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(default=func.now())
    updated_at: Mapped[datetime] = mapped_column(default=func.now(), onupdate=func.now())


class Notebook(Base):
    """Reusable analysis notebook — a sequence of cells that execute top-to-bottom."""

    __tablename__ = "notebooks"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(500))
    description: Mapped[str] = mapped_column(Text, default="")

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    organization_id: Mapped[str] = mapped_column(String(255), default="")

    connection_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("database_connections.id"), nullable=True)

    is_template: Mapped[bool] = mapped_column(default=False)
    is_public: Mapped[bool] = mapped_column(default=False)
    template_category: Mapped[str] = mapped_column(String(100), default="")

    last_run_at: Mapped[datetime | None] = mapped_column(nullable=True)
    run_count: Mapped[int] = mapped_column(default=0)

    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())

    cells: Mapped[list["NotebookCell"]] = relationship(
        back_populates="notebook", cascade="all, delete-orphan",
        order_by="NotebookCell.order",
    )
    runs: Mapped[list["NotebookRun"]] = relationship(
        back_populates="notebook", cascade="all, delete-orphan",
    )


class NotebookCell(Base):
    """A single cell within a notebook — text, file, input, prompt, or code."""

    __tablename__ = "notebook_cells"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    notebook_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("notebooks.id", ondelete="CASCADE"))

    order: Mapped[int] = mapped_column(default=0)
    cell_type: Mapped[str] = mapped_column(String(20))

    content: Mapped[str] = mapped_column(Text, default="")

    config: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    output_variable: Mapped[str] = mapped_column(String(100), default="")

    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())

    notebook: Mapped["Notebook"] = relationship(back_populates="cells")


class NotebookRun(Base):
    """A single execution of a notebook with specific inputs/files."""

    __tablename__ = "notebook_runs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    notebook_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("notebooks.id", ondelete="CASCADE"))
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))

    status: Mapped[str] = mapped_column(String(20), default="pending")

    user_inputs: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    file_uploads: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    started_at: Mapped[datetime | None] = mapped_column(nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    total_execution_ms: Mapped[int] = mapped_column(default=0)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    notebook: Mapped["Notebook"] = relationship(back_populates="runs")
    cell_results: Mapped[list["NotebookCellResult"]] = relationship(
        cascade="all, delete-orphan",
        order_by="NotebookCellResult.cell_order",
    )


class Subscription(Base):
    """Payment subscription linked to an organization."""

    __tablename__ = "subscriptions"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[str] = mapped_column(String(255), index=True)
    provider: Mapped[str] = mapped_column(String(50))
    provider_subscription_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    plan_name: Mapped[str] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(50), default="incomplete")
    current_period_end: Mapped[datetime | None] = mapped_column(nullable=True)
    cancel_at_period_end: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())


class Payment(Base):
    """Individual payment record for audit trail."""

    __tablename__ = "payments"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[str] = mapped_column(String(255), index=True)
    provider: Mapped[str] = mapped_column(String(50))
    provider_payment_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    amount: Mapped[int] = mapped_column(default=0)
    currency: Mapped[str] = mapped_column(String(10), default="usd")
    status: Mapped[str] = mapped_column(String(50), default="pending")
    plan_name: Mapped[str] = mapped_column(String(100), default="")
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())


class AgentMemory(Base):
    """Persistent memory for the AI agent — org-level and user-level knowledge.

    Memories are injected into the LLM prompt to improve accuracy over time.
    Three tiers: working (conversation), episodic (user), semantic (org).
    """

    __tablename__ = "agent_memories"
    __table_args__ = (
        Index("ix_agent_memories_org_active", "organization_id", "is_active"),
        Index("ix_agent_memories_user", "user_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[str] = mapped_column(String(255), index=True)
    user_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"), nullable=True)

    memory_type: Mapped[str] = mapped_column(String(50))

    content: Mapped[str] = mapped_column(Text)

    source: Mapped[str] = mapped_column(String(50), default="auto_extracted")
    source_conversation_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("conversations.id", ondelete="SET NULL"), nullable=True
    )

    confidence: Mapped[float] = mapped_column(default=0.7)
    access_count: Mapped[int] = mapped_column(default=0)
    last_accessed_at: Mapped[datetime | None] = mapped_column(nullable=True)

    expires_at: Mapped[datetime | None] = mapped_column(nullable=True)
    is_active: Mapped[bool] = mapped_column(default=True)

    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())


class NotebookCellResult(Base):
    """Output of a single cell from a specific notebook run."""

    __tablename__ = "notebook_cell_results"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("notebook_runs.id", ondelete="CASCADE"))
    cell_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("notebook_cells.id", ondelete="CASCADE"))
    cell_order: Mapped[int] = mapped_column(default=0)

    status: Mapped[str] = mapped_column(String(20), default="pending")

    output_text: Mapped[str] = mapped_column(Text, default="")
    output_table: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    output_chart: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    output_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    output_data: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    execution_time_ms: Mapped[int] = mapped_column(default=0)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())


class MessageFeedback(Base):
    """User feedback on assistant messages — thumbs up/down with optional correction.

    Separate from Message to keep the hot table lean and support analytics
    on feedback patterns per connection/org.
    """

    __tablename__ = "message_feedback"
    __table_args__ = (
        Index("ix_message_feedback_org", "organization_id"),
        Index("ix_message_feedback_conn", "connection_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    message_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("messages.id", ondelete="CASCADE"), unique=True,
    )
    conversation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE"),
    )
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    organization_id: Mapped[str] = mapped_column(String(255))
    connection_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("database_connections.id", ondelete="SET NULL"), nullable=True,
    )

    rating: Mapped[str] = mapped_column(String(10))  # "up" or "down"
    correction_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    category: Mapped[str | None] = mapped_column(String(50), nullable=True)
    # "wrong_data", "wrong_join", "wrong_metric", "wrong_filter", "other"

    # Snapshot for analytics (query + SQL at feedback time)
    user_query: Mapped[str] = mapped_column(Text, default="")
    sql_generated: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())


class JoinRule(Base):
    """Org-level join rules — override or supplement FK constraints.

    Injected as MANDATORY JOIN RULES in the SQL agent prompt so the LLM
    uses exact join paths instead of guessing.
    """

    __tablename__ = "join_rules"
    __table_args__ = (
        Index("ix_join_rules_org_conn", "organization_id", "connection_id"),
        UniqueConstraint(
            "connection_id", "source_table", "source_column",
            "target_table", "target_column",
            name="uq_join_rule_path",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[str] = mapped_column(String(255))
    connection_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("database_connections.id", ondelete="CASCADE"),
    )

    source_table: Mapped[str] = mapped_column(String(255))
    source_column: Mapped[str] = mapped_column(String(255))
    target_table: Mapped[str] = mapped_column(String(255))
    target_column: Mapped[str] = mapped_column(String(255))
    join_type: Mapped[str] = mapped_column(String(20), default="LEFT JOIN")

    description: Mapped[str] = mapped_column(Text, default="")
    is_active: Mapped[bool] = mapped_column(default=True)
    created_by: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))

    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())


class VerifiedQuery(Base):
    """Saved verified queries — reuse proven SQL patterns for similar questions.

    When a user thumbs-up a query, the (question_pattern → SQL) mapping is
    saved org-wide. Future similar questions skip the LLM pipeline and use
    the verified SQL directly.
    """

    __tablename__ = "verified_queries"
    __table_args__ = (
        Index("ix_verified_queries_org_conn", "organization_id", "connection_id"),
        Index("ix_verified_queries_active", "is_active"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[str] = mapped_column(String(255))
    connection_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("database_connections.id", ondelete="CASCADE"),
    )

    original_question: Mapped[str] = mapped_column(Text)
    question_pattern: Mapped[str] = mapped_column(Text)
    """Normalized pattern for matching (lowercase, specifics replaced with placeholders)."""
    sql_template: Mapped[str] = mapped_column(Text)
    """The verified SQL query."""

    verified_by: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    source_message_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("messages.id", ondelete="SET NULL"), nullable=True,
    )

    use_count: Mapped[int] = mapped_column(default=0)
    last_used_at: Mapped[datetime | None] = mapped_column(nullable=True)
    confidence: Mapped[float] = mapped_column(default=1.0)

    is_active: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())
