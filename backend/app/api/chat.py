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
from app.db.models import (
    Conversation,
    DatabaseConnection,
    FileUpload,
    Message,
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
        if clerk_id == "dev_user":
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
            if conn.schema_cache:
                # Reconstruct SchemaInfo from cache for formatting.
                parts.append(json.dumps(conn.schema_cache, default=str)[:4000])
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

    return "\n\n".join(parts) if parts else ""


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/chat")
async def chat(
    body: ChatRequest,
    current_user: CurrentUser,
    db: DbSession,
) -> StreamingResponse:
    """Accept a user message and stream the AI agent's response as SSE.

    If ``conversation_id`` is null, a new conversation is created.
    """
    user = await _get_user(db, current_user.user_id)

    # ── Resolve or create conversation ──────────────────────────────
    if body.conversation_id:
        stmt = select(Conversation).where(
            Conversation.id == body.conversation_id,
            Conversation.user_id == user.id,
        )
        result = await db.execute(stmt)
        conversation = result.scalar_one_or_none()
        if conversation is None:
            raise HTTPException(status_code=404, detail="Conversation not found.")
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

    # ── Build context ───────────────────────────────────────────────
    effective_connection_id = body.connection_id or conversation.connection_id
    effective_file_id = body.file_id or conversation.file_id
    schema_context = await _build_schema_context(db, effective_connection_id, effective_file_id)

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
    user = await _get_user(db, current_user.user_id)
    stmt = (
        select(Conversation)
        .where(Conversation.user_id == user.id)
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
    user = await _get_user(db, current_user.user_id)
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
    user = await _get_user(db, current_user.user_id)

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
    user = await _get_user(db, current_user.user_id)
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
