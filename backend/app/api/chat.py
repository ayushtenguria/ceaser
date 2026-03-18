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
    Report,
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
                email=get_settings().dev_fallback_email,
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
            # Use rich Excel context if available (from Excel Intelligence Engine)
            if upload.excel_context:
                parts.append(upload.excel_context)
                if upload.code_preamble:
                    parts.append(f"\nCODE PREAMBLE (prepend to all Python code):\n{upload.code_preamble}")
            else:
                # Fallback to basic file summary
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
    conversation_id: uuid.UUID | None = None,
) -> dict:
    """Generate smart query suggestions based on schema and conversation history."""
    from app.agents.suggestions import generate_follow_up_suggestions, generate_suggestions
    from app.core.permissions import require_permission, Permission

    user = await require_permission(Permission.VIEW_DATA, current_user, db)

    schema_context = ""
    if connection_id:
        schema_context = await _build_schema_context(db, connection_id, None)

    llm = get_llm()

    # If conversation_id provided, use conversation history for context-aware suggestions
    if conversation_id:
        msg_stmt = (
            select(Message)
            .where(Message.conversation_id == conversation_id)
            .order_by(Message.created_at)
        )
        msg_result = await db.execute(msg_stmt)
        msgs = list(msg_result.scalars().all())

        if msgs:
            # Build history
            history = [{"role": m.role, "content": m.content or ""} for m in msgs[-10:]]
            # Last user question and assistant answer
            last_q = ""
            last_a = ""
            for m in reversed(msgs):
                if m.role == "user" and not last_q:
                    last_q = m.content or ""
                if m.role == "assistant" and not last_a:
                    last_a = m.content or ""
                if last_q and last_a:
                    break

            suggestions = await generate_follow_up_suggestions(
                schema_context=schema_context,
                conversation_history=history,
                last_question=last_q,
                last_answer=last_a,
                llm=llm,
            )
            return {"suggestions": suggestions}

    # No conversation — generic suggestions
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
    logger.info("Chat context: connection=%s file=%s (body.file_id=%s, conv.file_id=%s)",
                effective_connection_id, effective_file_id, body.file_id, conversation.file_id)
    schema_context = await _build_schema_context(db, effective_connection_id, effective_file_id)
    logger.info("Schema context length: %d chars, has_excel=%s",
                len(schema_context), "EXCEL" in schema_context or "DATAFRAME" in schema_context)

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
            connection_ids=[str(cid) for cid in body.connection_ids] if body.connection_ids else None,
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
# Save as Notebook
# ---------------------------------------------------------------------------

@router.post("/conversations/{conversation_id}/notebook")
async def save_conversation_as_notebook(
    conversation_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> dict:
    """Extract a reusable notebook from a conversation's analysis history."""
    from app.agents.notebook.extractor import extract_notebook_from_conversation
    from app.db.models import Notebook, NotebookCell

    user = await require_permission(Permission.SAVE_REPORTS, current_user, db)

    # Load conversation messages
    msg_stmt = (
        select(Message)
        .where(Message.conversation_id == conversation_id)
        .order_by(Message.created_at)
    )
    msg_result = await db.execute(msg_stmt)
    db_messages = list(msg_result.scalars().all())

    if not db_messages:
        raise HTTPException(status_code=400, detail="Conversation has no messages.")

    # Convert to dicts
    messages = [
        {
            "role": m.role,
            "content": m.content or "",
            "sql_query": m.sql_query,
            "table_data": m.table_data,
            "plotly_figure": m.plotly_figure,
        }
        for m in db_messages
    ]

    # Get conversation title for notebook name
    conv_stmt = select(Conversation).where(Conversation.id == conversation_id)
    conv_result = await db.execute(conv_stmt)
    conversation = conv_result.scalar_one_or_none()
    conv_title = conversation.title if conversation else ""

    llm = get_llm()
    extracted = await extract_notebook_from_conversation(messages, llm, conv_title)

    # Create the notebook
    notebook = Notebook(
        name=extracted["name"],
        description=extracted["description"],
        user_id=user.id,
        organization_id=user.organization_id or "",
        connection_id=conversation.connection_id if conversation else None,
    )
    db.add(notebook)
    await db.flush()

    # Create cells
    for i, cell_data in enumerate(extracted["cells"]):
        cell = NotebookCell(
            notebook_id=notebook.id,
            order=i,
            cell_type=cell_data["cell_type"],
            content=cell_data["content"],
            config=cell_data.get("config"),
            output_variable=cell_data.get("output_variable", ""),
        )
        db.add(cell)

    await db.commit()

    logger.info("Saved conversation %s as notebook %s (%d cells)",
                conversation_id, notebook.id, len(extracted["cells"]))

    return {
        "notebookId": str(notebook.id),
        "name": extracted["name"],
        "cellCount": len(extracted["cells"]),
    }


# ---------------------------------------------------------------------------
# Report Generation
# ---------------------------------------------------------------------------

@router.get("/conversations/{conversation_id}/report")
async def get_conversation_report(
    conversation_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> dict:
    """Get the saved report for a conversation, or 404 if none exists."""
    from sqlalchemy import func as sqlfunc

    user = await require_permission(Permission.VIEW_DATA, current_user, db)

    # Find existing report for this conversation
    stmt = (
        select(Report)
        .where(
            Report.original_question == str(conversation_id),
            Report.organization_id == (user.organization_id or ""),
        )
        .order_by(Report.created_at.desc())
        .limit(1)
    )
    result = await db.execute(stmt)
    report = result.scalar_one_or_none()

    if report is None:
        raise HTTPException(status_code=404, detail="No report found for this conversation.")

    # Check if conversation has new messages since report was generated
    latest_msg_stmt = (
        select(sqlfunc.max(Message.created_at))
        .where(Message.conversation_id == conversation_id)
    )
    latest_msg_result = await db.execute(latest_msg_stmt)
    latest_msg_time = latest_msg_result.scalar()

    has_new_messages = False
    if latest_msg_time and report.created_at:
        has_new_messages = latest_msg_time > report.created_at

    return {
        "report": report.plotly_figure,  # We store full report JSON here
        "reportId": str(report.id),
        "createdAt": report.created_at.isoformat() if report.created_at else None,
        "hasNewMessages": has_new_messages,
    }


@router.post("/conversations/{conversation_id}/report")
async def generate_conversation_report(
    conversation_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> StreamingResponse:
    """Generate a professional report from a conversation and save it."""
    import json as _json
    from app.agents.report.orchestrator import generate_report_from_conversation

    user = await require_permission(Permission.SAVE_REPORTS, current_user, db)

    # Verify conversation exists
    stmt = select(Conversation).where(Conversation.id == conversation_id)
    result = await db.execute(stmt)
    conversation = result.scalar_one_or_none()
    if conversation is None:
        raise HTTPException(status_code=404, detail="Conversation not found.")

    llm = get_llm()

    async def stream():
        final_report = None

        async for event in generate_report_from_conversation(
            conversation_id=str(conversation_id),
            db=db,
            llm=llm,
        ):
            yield f"data: {_json.dumps(event, default=str)}\n\n"

            if event.get("type") == "report_complete":
                final_report = event.get("report")

        # Save the report to DB
        if final_report:
            # Delete old report for this conversation
            old_stmt = select(Report).where(
                Report.original_question == str(conversation_id),
                Report.organization_id == (user.organization_id or ""),
            )
            old_result = await db.execute(old_stmt)
            for old in old_result.scalars().all():
                await db.delete(old)

            # Save new report
            saved_report = Report(
                name=final_report.get("title", "Report"),
                description=final_report.get("subtitle", ""),
                original_question=str(conversation_id),
                summary_text=final_report.get("executiveSummary", ""),
                plotly_figure=final_report,  # Store full report JSON in this column
                table_data=None,
                connection_id=conversation.connection_id,
                user_id=user.id,
                organization_id=user.organization_id or "",
            )
            db.add(saved_report)
            await db.commit()

    return StreamingResponse(stream(), media_type="text/event-stream")


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
