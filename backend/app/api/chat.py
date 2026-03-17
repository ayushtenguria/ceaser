"""Chat endpoints — conversation management and AI agent invocation."""

from __future__ import annotations

import json
import logging
import uuid

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.agents.graph import run_agent
from app.api.schemas import (
    ChatRequest,
    ConversationResponse,
    MessageResponse,
)
from app.core.deps import CurrentUser, DbSession, get_llm
from app.core.permissions import Permission, require_permission
from app.db.models import (
    Conversation,
    DatabaseConnection,
    FileUpload,
    Message,
    MetricDefinition,
    User,
)
from app.services.file_parser import get_file_summary, parse_file
from app.services.schema import format_schema_for_llm, introspect_schema, SchemaInfo

logger = logging.getLogger(__name__)
router = APIRouter(tags=["chat"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
                email="dev@ceaser.local",
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


async def _build_schema_context(
    db: DbSession,
    connection_id: uuid.UUID | None,
    file_id: uuid.UUID | None,
) -> str:
    """Build the text context the LLM needs to know about the data source."""
    parts: list[str] = []

    if connection_id:
        stmt = select(DatabaseConnection).where(DatabaseConnection.id == connection_id)
        result = await db.execute(stmt)
        conn = result.scalar_one_or_none()
        if conn:
            parts.append(f"DATABASE DIALECT: {conn.db_type.upper()}")
            if conn.schema_cache:
                # Reconstruct SchemaInfo from cache and format for LLM.
                from app.services.schema import SchemaInfo, TableInfo, ColumnInfo
                tables = []
                for t in conn.schema_cache.get("tables", []):
                    cols = [
                        ColumnInfo(
                            name=c["name"],
                            data_type=c.get("data_type", "unknown"),
                            nullable=c.get("nullable", True),
                            primary_key=c.get("primary_key", False),
                            foreign_key=c.get("foreign_key"),
                            sample_values=c.get("sample_values", []),
                        )
                        for c in t.get("columns", [])
                    ]
                    tables.append(TableInfo(name=t["name"], columns=cols, row_count=t.get("row_count")))
                parts.append(format_schema_for_llm(SchemaInfo(tables=tables)))
            else:
                schema = await introspect_schema(conn)
                parts.append(format_schema_for_llm(schema))

    if file_id:
        stmt = select(FileUpload).where(FileUpload.id == file_id)
        result = await db.execute(stmt)
        upload = result.scalar_one_or_none()
        if upload:
            try:
                df, _ = parse_file(upload.file_path, upload.file_type)
                parts.append(get_file_summary(df))
                parts.append(f"\nFile path for code: {upload.file_path}")
            except Exception as exc:
                logger.warning("Could not parse file for context: %s", exc)

    # Append metric definitions (semantic layer) if any exist.
    if connection_id:
        metrics_stmt = select(MetricDefinition).where(
            MetricDefinition.connection_id == connection_id
        )
        metrics_result = await db.execute(metrics_stmt)
        metrics = list(metrics_result.scalars().all())
        if metrics:
            metric_lines = ["\n\nBUSINESS METRICS (use these exact definitions when the user references these terms)", "=" * 50]
            for m in metrics:
                metric_lines.append(f"\n{m.name} ({m.category})")
                if m.description:
                    metric_lines.append(f"  Description: {m.description}")
                metric_lines.append(f"  SQL: {m.sql_expression}")
            parts.append("\n".join(metric_lines))

    return "\n\n".join(parts) if parts else ""


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/suggestions")
async def get_suggestions(
    current_user: CurrentUser,
    db: DbSession,
    connection_id: uuid.UUID | None = None,
) -> dict:
    """Generate smart query suggestions based on the connected database schema."""
    from app.agents.suggestions import generate_suggestions
    from app.core.permissions import require_permission, Permission

    user = await require_permission(Permission.VIEW_DATA, current_user, db)

    schema_context = ""
    if connection_id:
        schema_context = await _build_schema_context(db, connection_id, None)

    llm = get_llm()
    suggestions = await generate_suggestions(schema_context, llm)

    return {"suggestions": suggestions}


@router.post("/chat")
async def chat(
    body: ChatRequest,
    current_user: CurrentUser,
    db: DbSession,
) -> StreamingResponse:
    """Accept a user message and stream the AI agent's response as SSE.

    If ``conversation_id`` is null, a new conversation is created.
    """
    user = await require_permission(Permission.QUERY_DATA, current_user, db)

    # ── Resolve or create conversation ──────────────────────────────
    if body.conversation_id:
        # Look up conversation — allow access if user owns it or is in same org
        stmt = select(Conversation).where(Conversation.id == body.conversation_id)
        result = await db.execute(stmt)
        conversation = result.scalar_one_or_none()
        if conversation is None:
            raise HTTPException(status_code=404, detail="Conversation not found.")
        # For now allow the conversation owner to continue their own conversations
        # In production, add proper org-level check here
    else:
        title = body.message[:80] if len(body.message) > 0 else "New Conversation"
        conversation = Conversation(
            user_id=user.id,
            title=title,
            connection_id=body.connection_id,
            file_id=body.file_id,
        )
        db.add(conversation)
        await db.flush()
        await db.refresh(conversation)

    # ── Save user message ───────────────────────────────────────────
    user_msg = Message(
        conversation_id=conversation.id,
        role="user",
        content=body.message,
        message_type="text",
    )
    db.add(user_msg)
    await db.flush()

    # Commit conversation + user message NOW so follow-up requests can find it.
    # After commit, session stays open for the SSE generator's later writes.
    await db.commit()
    # Re-attach the conversation object to this session after commit
    await db.refresh(conversation)

    # ── Build context ───────────────────────────────────────────────
    effective_connection_id = body.connection_id or conversation.connection_id
    effective_file_id = body.file_id or conversation.file_id
    schema_context = await _build_schema_context(db, effective_connection_id, effective_file_id)

    # ── Load conversation history for follow-up context ──────────
    history_messages: list[dict[str, str]] = []
    if body.conversation_id:
        hist_stmt = (
            select(Message)
            .where(Message.conversation_id == conversation.id)
            .order_by(Message.created_at)
        )
        hist_result = await db.execute(hist_stmt)
        prev_msgs = list(hist_result.scalars().all())
        # Take last 10 messages (exclude the one we just added)
        for msg in prev_msgs[-11:-1]:
            content = msg.content or ""
            # For assistant messages, add a brief data summary (not raw SQL/JSON)
            if msg.role == "assistant":
                if msg.table_data:
                    cols = msg.table_data.get("columns", [])
                    rows = msg.table_data.get("rows", [])
                    total = msg.table_data.get("total_rows", len(rows))
                    content += f"\n(Data returned: {total} rows, columns: {', '.join(cols[:8])})"
                if msg.plotly_figure:
                    content += "\n(A chart was generated)"
            history_messages.append({"role": msg.role, "content": content})

    llm = get_llm(model=body.model)

    # ── Stream SSE ──────────────────────────────────────────────────
    conversation_id = conversation.id

    async def event_stream():  # noqa: ANN202
        """Yield SSE-formatted events from the agent run."""
        # Send conversation_id first so the frontend can track it.
        yield _sse({"type": "conversation_id", "content": str(conversation_id)})

        collected_text = ""
        collected_sql: str | None = None
        collected_code: str | None = None
        collected_table: dict | None = None
        collected_plotly: dict | None = None
        collected_error: str | None = None

        async for chunk in run_agent(
            query=body.message,
            connection_id=str(effective_connection_id) if effective_connection_id else None,
            file_id=str(effective_file_id) if effective_file_id else None,
            schema_context=schema_context,
            history=history_messages,
            llm=llm,
            db=db,
        ):
            # Transform chunk types to match frontend expectations.
            chunk_type = chunk.get("type", "")
            if chunk_type == "plotly":
                sse_chunk = {"type": "chart", "content": "", "data": chunk["content"]}
            elif chunk_type == "table":
                sse_chunk = {"type": "table", "content": "", "data": chunk["content"]}
            else:
                sse_chunk = chunk
            yield _sse(sse_chunk)

            # Collect artifacts for the final persisted message.
            sse_type = sse_chunk.get("type", "")
            if sse_type == "text":
                collected_text += chunk.get("content", "")
            elif sse_type == "sql":
                collected_sql = chunk.get("content")
            elif sse_type == "code":
                collected_code = chunk.get("content")
            elif sse_type == "table":
                collected_table = chunk.get("content") or sse_chunk.get("data")
            elif sse_type == "chart":
                collected_plotly = chunk.get("content") or sse_chunk.get("data")
            elif sse_type == "error":
                collected_error = chunk.get("content")

        # ── Determine message type ──────────────────────────────────
        if collected_error and not collected_text:
            msg_type = "error"
        elif collected_plotly:
            msg_type = "visualization"
        elif collected_sql:
            msg_type = "sql_result"
        elif collected_code:
            msg_type = "code_execution"
        else:
            msg_type = "text"

        # ── Save assistant message ──────────────────────────────────
        assistant_msg = Message(
            conversation_id=conversation_id,
            role="assistant",
            content=collected_text or collected_error or "",
            message_type=msg_type,
            sql_query=collected_sql,
            code_block=collected_code,
            table_data=collected_table,
            plotly_figure=collected_plotly,
            error=collected_error,
        )
        db.add(assistant_msg)
        await db.flush()

        from app.services.audit import log_action
        await log_action(
            db,
            user_id=current_user.user_id,
            action="chat_query",
            resource_type="conversation",
            resource_id=str(conversation_id),
            details={"question": body.message, "sql": collected_sql, "model": body.model},
        )

        yield _sse({"type": "done", "content": str(assistant_msg.id)})

    return StreamingResponse(event_stream(), media_type="text/event-stream")


def _sse(data: dict) -> str:
    """Format a dict as a server-sent event line."""
    return f"data: {json.dumps(data, default=str)}\n\n"


# ---------------------------------------------------------------------------
# Conversation CRUD
# ---------------------------------------------------------------------------

@router.get("/conversations", response_model=list[ConversationResponse])
async def list_conversations(current_user: CurrentUser, db: DbSession) -> list[Conversation]:
    """List all conversations for the authenticated user, newest first."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    # Filter by org instead of user for shared access within org
    stmt = (
        select(Conversation)
        .join(User, Conversation.user_id == User.id)
        .where(User.organization_id == user.organization_id)
        .order_by(Conversation.updated_at.desc())
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.get("/conversations/{conversation_id}", response_model=ConversationResponse)
async def get_conversation(
    conversation_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> Conversation:
    """Get a single conversation by ID."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    stmt = select(Conversation).where(
        Conversation.id == conversation_id,
        Conversation.user_id == user.id,
    )
    result = await db.execute(stmt)
    conversation = result.scalar_one_or_none()
    if conversation is None:
        raise HTTPException(status_code=404, detail="Conversation not found.")
    return conversation


@router.get("/conversations/{conversation_id}/messages", response_model=list[MessageResponse])
async def list_messages(
    conversation_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> list[Message]:
    """Get all messages in a conversation, ordered by creation time."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)

    # Verify ownership.
    conv_stmt = select(Conversation).where(
        Conversation.id == conversation_id,
        Conversation.user_id == user.id,
    )
    conv_result = await db.execute(conv_stmt)
    if conv_result.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Conversation not found.")

    stmt = (
        select(Message)
        .where(Message.conversation_id == conversation_id)
        .order_by(Message.created_at)
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.delete("/conversations/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_conversation(
    conversation_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> None:
    """Delete a conversation and all its messages."""
    user = await require_permission(Permission.QUERY_DATA, current_user, db)
    stmt = select(Conversation).where(
        Conversation.id == conversation_id,
        Conversation.user_id == user.id,
    )
    result = await db.execute(stmt)
    conversation = result.scalar_one_or_none()
    if conversation is None:
        raise HTTPException(status_code=404, detail="Conversation not found.")

    # Delete messages first (cascade could handle this, but explicit is clearer).
    msg_stmt = select(Message).where(Message.conversation_id == conversation_id)
    msg_result = await db.execute(msg_stmt)
    for msg in msg_result.scalars().all():
        await db.delete(msg)

    await db.delete(conversation)
