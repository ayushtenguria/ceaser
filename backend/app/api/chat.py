"""Chat endpoints — conversation management and AI agent invocation."""

from __future__ import annotations

import json
import logging
import uuid

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import StreamingResponse
from sqlalchemy import select

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
from app.services.schema import format_schema_for_llm, introspect_schema

logger = logging.getLogger(__name__)
router = APIRouter(tags=["chat"])


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


async def _verify_org_connection(
    db: DbSession, connection_id: uuid.UUID, org_id: str
) -> DatabaseConnection:
    """Load a connection and verify it belongs to the given org. Raises 404 if not."""
    stmt = select(DatabaseConnection).where(
        DatabaseConnection.id == connection_id,
        DatabaseConnection.organization_id == org_id,
    )
    result = await db.execute(stmt)
    conn = result.scalar_one_or_none()
    if conn is None:
        raise HTTPException(status_code=404, detail="Connection not found.")
    return conn


async def _verify_org_file(db: DbSession, file_id: uuid.UUID, org_id: str) -> FileUpload:
    """Load a file and verify it belongs to the given org. Raises 404 if not."""
    stmt = select(FileUpload).where(
        FileUpload.id == file_id,
        FileUpload.organization_id == org_id,
    )
    result = await db.execute(stmt)
    upload = result.scalar_one_or_none()
    if upload is None:
        raise HTTPException(status_code=404, detail="File not found.")
    return upload


async def _verify_org_conversation(
    db: DbSession, conversation_id: uuid.UUID, org_id: str
) -> Conversation:
    """Load a conversation and verify it belongs to a user in the given org."""
    org_filter = (
        User.organization_id.is_(None) | (User.organization_id == "")
        if not org_id
        else User.organization_id == org_id
    )
    stmt = (
        select(Conversation)
        .join(User, Conversation.user_id == User.id)
        .where(Conversation.id == conversation_id, org_filter)
    )
    result = await db.execute(stmt)
    conv = result.scalar_one_or_none()
    if conv is None:
        raise HTTPException(status_code=404, detail="Conversation not found.")
    return conv


async def _build_schema_context(
    db: DbSession,
    connection_id: uuid.UUID | None,
    file_id: uuid.UUID | None,
    org_id: str = "",
    user_question: str = "",
) -> str:
    """Build the text context the LLM needs to know about the data source."""
    parts: list[str] = []

    graph_rag_used = False
    if connection_id:
        try:
            from app.services.schema_graph import select_relevant_schema

            graph_context = await select_relevant_schema(user_question, str(connection_id), org_id)
            if graph_context:
                parts.append(graph_context)
                graph_rag_used = True
                logger.info("Graph RAG: selected relevant tables (%d chars)", len(graph_context))
        except Exception as exc:
            logger.warning("Graph RAG failed (falling back to full schema): %s", exc)

    if connection_id and not graph_rag_used:
        stmt = (
            select(DatabaseConnection).where(
                DatabaseConnection.id == connection_id,
                DatabaseConnection.organization_id == org_id,
            )
            if org_id
            else select(DatabaseConnection).where(DatabaseConnection.id == connection_id)
        )
        result = await db.execute(stmt)
        conn = result.scalar_one_or_none()
        if conn:
            parts.append(f"DATABASE DIALECT: {conn.db_type.upper()}")
            if conn.schema_cache:
                from app.services.schema import ColumnInfo, SchemaInfo, TableInfo

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
                    tables.append(
                        TableInfo(name=t["name"], columns=cols, row_count=t.get("row_count"))
                    )
                parts.append(format_schema_for_llm(SchemaInfo(tables=tables)))
            else:
                schema = await introspect_schema(conn)
                parts.append(format_schema_for_llm(schema))

    if file_id:
        stmt = (
            select(FileUpload).where(
                FileUpload.id == file_id,
                FileUpload.organization_id == org_id,
            )
            if org_id
            else select(FileUpload).where(FileUpload.id == file_id)
        )
        result = await db.execute(stmt)
        upload = result.scalar_one_or_none()
        if upload:
            preamble = upload.code_preamble or ""

            if upload.excel_context:
                from app.agents.excel.sheet_selector import (
                    build_compact_summary,
                    build_selected_context,
                    parse_excel_context_to_sheets,
                    select_relevant_sheets,
                )

                all_sheet_metas = parse_excel_context_to_sheets(upload.excel_context)

                if all_sheet_metas and len(all_sheet_metas) > 3:
                    parts.append(build_compact_summary(all_sheet_metas))
                    selected = select_relevant_sheets(user_question, all_sheet_metas, max_sheets=3)
                    parts.append(build_selected_context(selected, preamble))

                    logger.info(
                        "Smart context: %d/%d sheets selected, ~%d chars",
                        len(selected),
                        len(all_sheet_metas),
                        sum(len(p) for p in parts),
                    )
                else:
                    parts.append(upload.excel_context)
                    if preamble:
                        parts.append(f"\nCODE PREAMBLE (prepend to all Python code):\n{preamble}")
            else:
                try:
                    from app.services.storage import get_storage

                    storage = get_storage()
                    local_path = await storage.download_url(upload.file_path)
                    df, _ = parse_file(local_path, upload.file_type)
                    parts.append(get_file_summary(df))
                    parts.append(f"\nFile path for code: ceaser://{upload.file_path}")
                except Exception as exc:
                    logger.warning("Could not parse file for context: %s", exc)

    if connection_id:
        metrics_filter = [MetricDefinition.connection_id == connection_id]
        if org_id:
            metrics_filter.append(MetricDefinition.organization_id == org_id)
        metrics_stmt = select(MetricDefinition).where(*metrics_filter)
        metrics_result = await db.execute(metrics_stmt)
        metrics = list(metrics_result.scalars().all())
        if metrics:
            locked = [m for m in metrics if m.is_locked]
            unlocked = [m for m in metrics if not m.is_locked]

            metric_lines: list[str] = []

            if locked:
                metric_lines.append(
                    "\n\nMANDATORY METRIC DEFINITIONS (use these EXACTLY as written — DO NOT deviate or generate alternative SQL)"
                )
                metric_lines.append("=" * 70)
                for m in locked:
                    metric_lines.append(f"\n  {m.name} ({m.category})")
                    if m.description:
                        metric_lines.append(f"    Description: {m.description}")
                    metric_lines.append(f"    SQL: {m.sql_expression}")
                    metric_lines.append(
                        "    ** THIS DEFINITION IS LOCKED — you MUST use it exactly as shown **"
                    )

            if unlocked:
                metric_lines.append(
                    "\n\nSUGGESTED METRIC DEFINITIONS (use as guidance when user references these terms)"
                )
                metric_lines.append("=" * 70)
                for m in unlocked:
                    metric_lines.append(f"\n  {m.name} ({m.category})")
                    if m.description:
                        metric_lines.append(f"    Description: {m.description}")
                    metric_lines.append(f"    SQL: {m.sql_expression}")

            if metric_lines:
                parts.append("\n".join(metric_lines))

    # ── Inject org-level join rules ──────────────────────────────────
    if connection_id:
        from app.db.models import JoinRule

        jr_stmt = select(JoinRule).where(
            JoinRule.connection_id == connection_id,
            JoinRule.is_active == True,
        )
        if org_id:
            jr_stmt = jr_stmt.where(JoinRule.organization_id == org_id)
        jr_result = await db.execute(jr_stmt)
        join_rules = list(jr_result.scalars().all())
        if join_rules:
            rule_lines = [
                "\n\nMANDATORY JOIN RULES (use these EXACTLY — do NOT infer alternative joins for these table pairs)",
                "=" * 70,
            ]
            for jr in join_rules:
                rule_lines.append(
                    f"  {jr.source_table}.{jr.source_column} → {jr.target_table}.{jr.target_column} "
                    f"(use {jr.join_type})"
                )
                if jr.description:
                    rule_lines.append(f"    Note: {jr.description}")
            parts.append("\n".join(rule_lines))

    return "\n\n".join(parts) if parts else ""


async def _build_multi_file_context(
    db: DbSession,
    file_ids: list[str],
    org_id: str,
    user_question: str = "",
) -> tuple[str, list[dict]]:
    """Build unified context from ALL files in a conversation.

    Returns (context_string, cross_file_relationships).
    """
    parts: list[str] = []
    file_contexts: list[dict] = []
    all_preamble_lines: list[str] = ["import pandas as pd", "import plotly.express as px", ""]

    for fid in file_ids:
        try:
            fid_uuid = uuid.UUID(fid)
        except ValueError:
            continue

        stmt = select(FileUpload).where(FileUpload.id == fid_uuid)
        if org_id:
            stmt = stmt.where(FileUpload.organization_id == org_id)
        result = await db.execute(stmt)
        upload = result.scalar_one_or_none()
        if not upload:
            continue

        file_contexts.append(
            {
                "filename": upload.filename,
                "parquet_paths": upload.parquet_paths or {},
                "column_info": upload.column_info or {},
            }
        )

        if upload.code_preamble:
            for line in upload.code_preamble.split("\n"):
                stripped = line.strip()
                if "= pd.read_parquet(" in stripped and stripped not in all_preamble_lines:
                    all_preamble_lines.append(stripped)

        if upload.excel_context:
            from app.agents.excel.sheet_selector import (
                build_compact_summary,
                build_selected_context,
                parse_excel_context_to_sheets,
                select_relevant_columns,
                select_relevant_sheets,
            )

            all_sheet_metas = parse_excel_context_to_sheets(upload.excel_context)

            if all_sheet_metas and len(all_sheet_metas) > 3:
                parts.append(build_compact_summary(all_sheet_metas))
                selected = select_relevant_sheets(user_question, all_sheet_metas, max_sheets=3)
                parts.append(build_selected_context(selected, "", question=user_question))
            else:
                # ≤3 sheets — still apply column filtering for large sheets
                filtered = [select_relevant_columns(user_question, s) for s in all_sheet_metas]
                parts.append(build_selected_context(filtered, "", question=""))

    if len(all_preamble_lines) > 3:
        unified_preamble = "\n".join(all_preamble_lines)
        parts.append(f"\nCODE PREAMBLE (prepend to all Python code):\n{unified_preamble}\n")

    cross_rels: list[dict] = []
    if len(file_contexts) > 1:
        from app.agents.excel.cross_file import (
            discover_cross_file_relationships,
            format_cross_file_context,
        )

        cross_rels = discover_cross_file_relationships(file_contexts)
        rel_context = format_cross_file_context(cross_rels)
        if rel_context:
            parts.append(rel_context)

    context = "\n\n".join(parts) if parts else ""
    return context, cross_rels


def _build_adaptive_history(
    prev_msgs: list,
    max_chars: int = 4000,
    current_question: str = "",
) -> list[dict[str, str]]:
    """Build conversation history scored by relevance to the current question.

    Uses compressed summaries when available (stored on Message.summary),
    falls back to truncated raw content. Scores by keyword relevance +
    recency + correction boost. Much smaller token budget than raw dumping.
    """
    from app.services.conversation_memory import build_relevant_history

    return build_relevant_history(prev_msgs, current_question, max_chars=max_chars)


@router.get("/suggestions")
async def get_suggestions(
    current_user: CurrentUser,
    db: DbSession,
    connection_id: uuid.UUID | None = None,
    conversation_id: uuid.UUID | None = None,
) -> dict:
    """Generate smart query suggestions based on schema and conversation history."""
    from app.agents.suggestions import generate_follow_up_suggestions, generate_suggestions
    from app.core.permissions import Permission, require_permission

    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    org_id = user.organization_id or ""

    schema_context = ""
    if connection_id:
        await _verify_org_connection(db, connection_id, org_id)
        schema_context = await _build_schema_context(db, connection_id, None, org_id=org_id)

    llm = get_llm(tier="light")

    if conversation_id:
        await _verify_org_conversation(db, conversation_id, org_id)
        msg_stmt = (
            select(Message)
            .where(Message.conversation_id == conversation_id)
            .order_by(Message.created_at)
        )
        msg_result = await db.execute(msg_stmt)
        msgs = list(msg_result.scalars().all())

        if msgs:
            history = [{"role": m.role, "content": m.content or ""} for m in msgs[-10:]]
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

    if not user.is_super_admin:
        from app.core.rate_limiter import check_rate_limit

        check_rate_limit(current_user.user_id, max_requests=30, window_seconds=60)

        from app.core.plan_enforcement import check_query_limit

        await check_query_limit(db, user.organization_id or "")

    org_id = user.organization_id or ""

    from app.core.features import Feature, check_feature

    if body.model == "claude":
        await check_feature(Feature.CLAUDE_MODEL, db, org_id)
    if body.connection_ids and len(body.connection_ids) > 1:
        await check_feature(Feature.MULTI_DB, db, org_id)
    if body.file_id:
        await check_feature(Feature.FILE_UPLOAD, db, org_id)

    if body.connection_id:
        await _verify_org_connection(db, body.connection_id, org_id)

    if body.file_id:
        await _verify_org_file(db, body.file_id, org_id)

    if body.conversation_id:
        conversation = await _verify_org_conversation(db, body.conversation_id, org_id)
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

    user_msg = Message(
        conversation_id=conversation.id,
        role="user",
        content=body.message,
        message_type="text",
    )
    db.add(user_msg)
    await db.flush()

    await db.commit()
    await db.refresh(conversation)

    logger.info(
        "Chat request: file_id=%s file_ids=%s connection_id=%s",
        body.file_id,
        body.file_ids,
        body.connection_id,
    )
    incoming_file_ids: list[str] = []
    if body.file_ids:
        incoming_file_ids = [str(fid) for fid in body.file_ids]
    elif body.file_id:
        incoming_file_ids = [str(body.file_id)]

    if incoming_file_ids:
        existing_ids = conversation.file_ids or []
        changed = False
        for fid in incoming_file_ids:
            if fid not in existing_ids:
                existing_ids.append(fid)
                changed = True
        if changed:
            conversation.file_ids = existing_ids
            conversation.file_id = uuid.UUID(incoming_file_ids[-1])
            from sqlalchemy.orm.attributes import flag_modified

            flag_modified(conversation, "file_ids")

            try:
                from app.services.schema_graph import get_graph_driver

                driver = get_graph_driver()
                if driver:
                    async with driver.session() as neo_session:
                        for fid in incoming_file_ids:
                            await neo_session.run(
                                """
                                MATCH (f:FileNode {file_id: $file_id})
                                SET f.conversation_id = $conv_id
                            """,
                                file_id=fid,
                                conv_id=str(conversation.id),
                            )
            except Exception:
                pass
            await db.flush()
            await db.commit()
            await db.refresh(conversation)

    # ── Build context (all files in conversation) ─────────────────
    effective_connection_id = body.connection_id or conversation.connection_id
    all_file_ids = conversation.file_ids or []
    if not all_file_ids and conversation.file_id:
        all_file_ids = [str(conversation.file_id)]

    # Auto-detect: if no connection AND no files, check if org has recent uploads
    if not effective_connection_id and not all_file_ids:
        recent_files_stmt = (
            select(FileUpload.id)
            .where(FileUpload.organization_id == org_id)
            .order_by(FileUpload.created_at.desc())
            .limit(5)
        )
        recent_result = await db.execute(recent_files_stmt)
        recent_ids = [str(r[0]) for r in recent_result.all()]
        if recent_ids:
            all_file_ids = recent_ids
            logger.info("Auto-linked %d org files to conversation", len(recent_ids))

    # Validate file references — remove deleted files, track processing ones
    valid_file_ids = []
    files_still_processing = []
    for fid in all_file_ids:
        try:
            fid_uuid = uuid.UUID(fid)
        except ValueError:
            continue
        stmt = select(FileUpload).where(FileUpload.id == fid_uuid)
        fresult = await db.execute(stmt)
        fupload = fresult.scalar_one_or_none()
        if not fupload:
            logger.info("File %s referenced in conversation was deleted — skipping", fid)
            continue
        if fupload.processing_status == "processing":
            files_still_processing.append(fupload.filename)
        valid_file_ids.append(fid)
    all_file_ids = valid_file_ids

    # If ALL files are still processing, return early with a clear message
    schema_context = ""
    if files_still_processing and len(files_still_processing) == len(all_file_ids):
        processing_names = ", ".join(files_still_processing)

        async def _processing_stream():
            yield _sse({"type": "conversation_id", "content": str(conversation.id)})
            yield _sse(
                {
                    "type": "text",
                    "content": f"Your file(s) ({processing_names}) are still being analyzed. "
                    "This usually takes 1-2 minutes. Please check the Files page for progress "
                    "and try again once processing is complete.",
                }
            )
            yield _sse({"type": "done", "content": ""})

        return StreamingResponse(_processing_stream(), media_type="text/event-stream")

    if effective_connection_id or all_file_ids:
        # Connection context
        schema_context = await _build_schema_context(
            db, effective_connection_id, None, org_id=org_id, user_question=body.message
        )
        # Multi-file context — try Graph RAG first, fallback to flat loading
        if all_file_ids:
            file_graph_context = ""
            try:
                from app.services.schema_graph import select_relevant_files

                file_graph_context = await select_relevant_files(
                    question=body.message,
                    org_id=org_id,
                    conversation_id=str(conversation.id) if conversation else None,
                    connection_id=str(effective_connection_id) if effective_connection_id else None,
                )
                if file_graph_context:
                    logger.info(
                        "File Graph RAG: selected relevant files (%d chars)",
                        len(file_graph_context),
                    )
            except Exception as exc:
                logger.debug("File graph selection failed: %s", exc)

            if file_graph_context:
                schema_context = (
                    (schema_context + "\n\n" + file_graph_context)
                    if schema_context
                    else file_graph_context
                )
            else:
                # Fallback to flat multi-file context
                file_context, cross_rels = await _build_multi_file_context(
                    db, all_file_ids, org_id, body.message
                )
                if file_context:
                    schema_context = (
                        (schema_context + "\n\n" + file_context) if schema_context else file_context
                    )

            # Append CODE PREAMBLE — only load sheets that the sheet selector
            # picked (max 3-5), not all 32. This keeps the prompt small and
            # prevents the agent from generating code that loads every sheet.
            max_preamble_reads = 5  # match sheet selector's typical output
            preamble_lines: list[str] = ["import pandas as pd", "import plotly.express as px", ""]
            read_count = 0
            for fid in all_file_ids:
                try:
                    fid_uuid = uuid.UUID(fid)
                except ValueError:
                    continue
                stmt = select(FileUpload).where(FileUpload.id == fid_uuid)
                if org_id:
                    stmt = stmt.where(FileUpload.organization_id == org_id)
                result = await db.execute(stmt)
                upload = result.scalar_one_or_none()
                if not upload:
                    continue
                if upload.processing_status == "processing":
                    logger.info("File %s still processing — skipping preamble", fid)
                    continue
                if upload.code_preamble:
                    # Extract sheet names from schema_context to filter preamble
                    selected_vars = set()
                    for sc_line in schema_context.split("\n"):
                        sc_stripped = sc_line.strip()
                        if sc_stripped.startswith("df_") and ":" in sc_stripped:
                            selected_vars.add(
                                sc_stripped.split(":")[0].strip().split("(")[0].strip()
                            )
                        elif sc_stripped.startswith("('df_"):
                            var = sc_stripped.split("'")[1] if "'" in sc_stripped else ""
                            if var:
                                selected_vars.add(var)

                    for line in upload.code_preamble.split("\n"):
                        stripped = line.strip()
                        if not stripped or stripped in preamble_lines:
                            continue
                        if stripped.startswith(("import ", "from ")) or stripped.startswith("#"):
                            preamble_lines.append(stripped)
                        elif "= pd.read_" in stripped or "duckdb" in stripped:
                            # If we know selected sheets, only include matching ones
                            if selected_vars:
                                var_name = stripped.split("=")[0].strip()
                                if var_name in selected_vars:
                                    preamble_lines.append(stripped)
                                    read_count += 1
                            elif read_count < max_preamble_reads:
                                preamble_lines.append(stripped)
                                read_count += 1
            if len(preamble_lines) > 3:
                unified_preamble = "\n".join(preamble_lines)
                schema_context += (
                    f"\n\nCODE PREAMBLE (prepend to all Python code):\n{unified_preamble}\n"
                )

    logger.info("Chat context: connection=%s files=%d", effective_connection_id, len(all_file_ids))

    # ── Load agent memories (org + user) ──────────────────────────
    from app.services.memory import format_memories_for_prompt, load_memories

    memories = await load_memories(db, org_id, user.id, question=body.message)
    memory_context = format_memories_for_prompt(memories)
    if memory_context:
        schema_context = schema_context + "\n" + memory_context

    # ── Inject disambiguation resolution if provided ───────────────
    if body.disambiguation_choice:
        clarifications = []
        for term, chosen in body.disambiguation_choice.items():
            clarifications.append(f'When I say "{term}", I mean {chosen}.')
        schema_context += "\n\nUSER CLARIFICATION: " + " ".join(clarifications)

        # Save as memory for future queries
        try:
            from app.services.memory import save_memory

            for term, chosen in body.disambiguation_choice.items():
                await save_memory(
                    db,
                    org_id=org_id,
                    content=f'When user says "{term}", they mean {chosen}',
                    memory_type="domain_term",
                    user_id=user.id,
                    source="disambiguation",
                    confidence=0.95,
                )
        except Exception as exc:
            logger.debug("Failed to save disambiguation memory: %s", exc)

    logger.info(
        "Schema context length: %d chars, memories=%d, has_excel=%s",
        len(schema_context),
        len(memories),
        "EXCEL" in schema_context or "DATAFRAME" in schema_context,
    )

    # ── Load conversation history (adaptive — fits as many as token budget allows)
    history_messages: list[dict[str, str]] = []
    if body.conversation_id:
        hist_stmt = (
            select(Message)
            .where(Message.conversation_id == conversation.id)
            .order_by(Message.created_at)
        )
        hist_result = await db.execute(hist_stmt)
        prev_msgs = list(hist_result.scalars().all())
        history_messages = _build_adaptive_history(
            prev_msgs,
            max_chars=4000,
            current_question=body.message,
        )
        logger.info(
            "Relevant history: %d messages loaded (of %d total)",
            len(history_messages),
            len(prev_msgs),
        )

    # ── Inject previous query results for follow-up reference ────────
    if body.conversation_id:
        try:
            from app.services.result_store import build_result_context, load_conversation_results

            prev_results = await load_conversation_results(db, str(conversation.id))
            result_context = build_result_context(prev_results)
            if result_context:
                schema_context += result_context
                logger.info("Injected %d previous results into context", len(prev_results))
        except Exception as exc:
            logger.debug("Result context loading skipped: %s", exc)

    # Only override provider if user explicitly selected "claude" in the UI.
    # "gemini" is the frontend default — treat it as "use server default" (Bedrock).
    llm_model = body.model if body.model == "claude" else None
    llm = get_llm(model=llm_model, tier="heavy")
    llm_light = get_llm(tier="light")

    # ── Stream SSE ──────────────────────────────────────────────────
    conversation_id = conversation.id

    async def event_stream():  # noqa: ANN202
        """Yield SSE-formatted events from the agent run."""
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
            connection_ids=[str(cid) for cid in body.connection_ids]
            if body.connection_ids
            else None,
            file_id=str(body.file_id)
            if body.file_id
            else (str(conversation.file_id) if conversation.file_id else None),
            schema_context=schema_context,
            history=history_messages,
            llm=llm,
            llm_light=llm_light,
            db=db,
        ):
            chunk_type = chunk.get("type", "")
            if chunk_type == "plotly":
                sse_chunk = {"type": "chart", "content": "", "data": chunk["content"]}
            elif chunk_type == "table":
                sse_chunk = {"type": "table", "content": "", "data": chunk["content"]}
            else:
                sse_chunk = chunk
            yield _sse(sse_chunk)

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

        # Generate compressed summaries for history injection
        from app.services.conversation_memory import summarize_exchange

        user_summary, assistant_summary = summarize_exchange(
            user_message=body.message,
            assistant_message=collected_text or collected_error or "",
            sql_query=collected_sql,
            code_block=collected_code,
            table_data=collected_table,
            error=collected_error,
        )

        # Update user message with summary
        user_msg.summary = user_summary
        from sqlalchemy.orm.attributes import flag_modified

        flag_modified(user_msg, "summary")

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
            summary=assistant_summary,
        )
        # Save intermediate result as parquet for follow-up queries
        if collected_table and not collected_error:
            try:
                from app.services.result_store import save_query_result

                result_ref = await save_query_result(
                    collected_table,
                    org_id,
                    str(conversation_id),
                    body.message,
                )
                if result_ref and assistant_msg.table_data:
                    assistant_msg.table_data["_result_ref"] = result_ref
                    flag_modified(assistant_msg, "table_data")
            except Exception as exc:
                logger.debug("Result persistence skipped: %s", exc)

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

        try:
            from app.agents.memory_extractor import extract_memories

            await extract_memories(
                user_message=body.message,
                assistant_response=collected_text or "",
                sql_query=collected_sql,
                llm=llm_light,
                db=db,
                org_id=org_id,
                user_id=user.id,
                conversation_id=conversation_id,
            )
        except Exception as exc:
            logger.debug("Memory extraction skipped: %s", exc)

        yield _sse({"type": "done", "content": str(assistant_msg.id)})

    return StreamingResponse(event_stream(), media_type="text/event-stream")


def _sse(data: dict) -> str:
    """Format a dict as a server-sent event line."""
    return f"data: {json.dumps(data, default=str)}\n\n"


@router.post("/conversations/{conversation_id}/notebook/draft")
async def get_notebook_draft(
    conversation_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> dict:
    """Extract a notebook DRAFT from conversation — returns steps for user review."""
    from app.agents.notebook.extractor import extract_notebook_draft

    user = await require_permission(Permission.SAVE_REPORTS, current_user, db)
    org_id = user.organization_id or ""
    conversation = await _verify_org_conversation(db, conversation_id, org_id)

    msg_stmt = (
        select(Message)
        .where(Message.conversation_id == conversation_id)
        .order_by(Message.created_at)
    )
    msg_result = await db.execute(msg_stmt)
    db_messages = list(msg_result.scalars().all())

    if not db_messages:
        raise HTTPException(status_code=400, detail="Conversation has no messages.")

    messages = [
        {
            "role": m.role,
            "content": m.content or "",
            "sql_query": m.sql_query,
            "table_data": m.table_data,
            "plotly_figure": m.plotly_figure,
            "error": m.error,
        }
        for m in db_messages
    ]

    llm = get_llm(tier="light")
    draft = await extract_notebook_draft(messages, llm, conversation.title if conversation else "")

    return {
        "conversationId": str(conversation_id),
        "connectionId": str(conversation.connection_id)
        if conversation and conversation.connection_id
        else None,
        **draft,
    }


@router.post("/conversations/{conversation_id}/notebook")
async def save_conversation_as_notebook(
    conversation_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
    body: dict | None = None,
) -> dict:
    """Save a notebook from a reviewed draft. Accepts the steps the user approved."""
    from app.agents.notebook.extractor import extract_notebook_draft
    from app.db.models import Notebook, NotebookCell

    user = await require_permission(Permission.SAVE_REPORTS, current_user, db)
    org_id = user.organization_id or ""
    conversation = await _verify_org_conversation(db, conversation_id, org_id)

    if body and body.get("steps"):
        title = body.get("title", "Analysis Notebook")
        description = body.get("description", "")
        steps = [s for s in body["steps"] if s.get("included", True)]
    else:
        msg_stmt = (
            select(Message)
            .where(Message.conversation_id == conversation_id)
            .order_by(Message.created_at)
        )
        msg_result = await db.execute(msg_stmt)
        db_messages = list(msg_result.scalars().all())

        if not db_messages:
            raise HTTPException(status_code=400, detail="No messages.")

        messages = [
            {
                "role": m.role,
                "content": m.content or "",
                "sql_query": m.sql_query,
                "table_data": m.table_data,
                "plotly_figure": m.plotly_figure,
                "error": m.error,
            }
            for m in db_messages
        ]

        llm = get_llm(tier="light")
        draft = await extract_notebook_draft(
            messages, llm, conversation.title if conversation else ""
        )
        title = draft["title"]
        description = draft["description"]
        steps = [s for s in draft["steps"] if s.get("included", True)]

    notebook = Notebook(
        name=title,
        description=description,
        user_id=user.id,
        organization_id=user.organization_id or "",
        connection_id=conversation.connection_id if conversation else None,
    )
    db.add(notebook)
    await db.flush()

    file_cell = NotebookCell(
        notebook_id=notebook.id,
        order=0,
        cell_type="file",
        content="Upload your data file",
        config={"accepted_types": [".xlsx", ".csv"], "description": "Upload data"},
    )
    db.add(file_cell)

    for i, step in enumerate(steps):
        cell = NotebookCell(
            notebook_id=notebook.id,
            order=i + 1,
            cell_type=step.get("cell_type", "prompt"),
            content=step.get("prompt", ""),
            output_variable=f"result_{i + 1}",
        )
        db.add(cell)

    await db.commit()

    logger.info(
        "Saved conversation %s as notebook %s (%d steps)", conversation_id, notebook.id, len(steps)
    )

    return {
        "notebookId": str(notebook.id),
        "name": title,
        "cellCount": len(steps) + 1,
    }


@router.get("/conversations/{conversation_id}/report")
async def get_conversation_report(
    conversation_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> dict:
    """Get the saved report for a conversation, or 404 if none exists."""
    from sqlalchemy import func as sqlfunc

    user = await require_permission(Permission.VIEW_DATA, current_user, db)

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

    latest_msg_stmt = select(sqlfunc.max(Message.created_at)).where(
        Message.conversation_id == conversation_id
    )
    latest_msg_result = await db.execute(latest_msg_stmt)
    latest_msg_time = latest_msg_result.scalar()

    has_new_messages = False
    if latest_msg_time and report.created_at:
        has_new_messages = latest_msg_time > report.created_at

    return {
        "report": report.plotly_figure,
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
    org_id = user.organization_id or ""

    from app.core.plan_enforcement import check_report_limit

    await check_report_limit(db, org_id)

    conversation = await _verify_org_conversation(db, conversation_id, org_id)

    llm = get_llm(tier="heavy")

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

        if final_report:
            old_stmt = select(Report).where(
                Report.original_question == str(conversation_id),
                Report.organization_id == (user.organization_id or ""),
            )
            old_result = await db.execute(old_stmt)
            for old in old_result.scalars().all():
                await db.delete(old)

            saved_report = Report(
                name=final_report.get("title", "Report"),
                description=final_report.get("subtitle", ""),
                original_question=str(conversation_id),
                summary_text=final_report.get("executiveSummary", ""),
                plotly_figure=final_report,
                table_data=None,
                connection_id=conversation.connection_id,
                user_id=user.id,
                organization_id=user.organization_id or "",
            )
            db.add(saved_report)
            await db.commit()

    return StreamingResponse(stream(), media_type="text/event-stream")


@router.get("/conversations", response_model=list[ConversationResponse])
async def list_conversations(current_user: CurrentUser, db: DbSession) -> list[Conversation]:
    """List all conversations for the authenticated user, newest first."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
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
    return await _verify_org_conversation(db, conversation_id, user.organization_id or "")


@router.get("/conversations/{conversation_id}/messages", response_model=list[MessageResponse])
async def list_messages(
    conversation_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> list[Message]:
    """Get all messages in a conversation, ordered by creation time."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    await _verify_org_conversation(db, conversation_id, user.organization_id or "")

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
    conversation = await _verify_org_conversation(db, conversation_id, user.organization_id or "")

    msg_stmt = select(Message).where(Message.conversation_id == conversation_id)
    msg_result = await db.execute(msg_stmt)
    for msg in msg_result.scalars().all():
        await db.delete(msg)

    await db.delete(conversation)
