"""Message feedback endpoints — thumbs up/down on assistant responses.

Feedback drives:
1. Correction memories (thumbs-down → save_memory with high confidence)
2. Verified queries (thumbs-up on SQL → auto-create VerifiedQuery)
3. Accuracy analytics per connection/org
"""

from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import func as sa_func
from sqlalchemy import select

from app.api.schemas import AccuracyStats, FeedbackCreate, FeedbackResponse
from app.core.deps import CurrentUser, DbSession
from app.core.permissions import Permission, require_permission
from app.db.models import Conversation, Message, MessageFeedback

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/messages", tags=["feedback"])


@router.post(
    "/{message_id}/feedback",
    response_model=FeedbackResponse,
    status_code=status.HTTP_201_CREATED,
)
async def submit_feedback(
    message_id: uuid.UUID,
    body: FeedbackCreate,
    current_user: CurrentUser,
    db: DbSession,
) -> MessageFeedback:
    """Submit or update feedback on an assistant message.

    - Thumbs-down: auto-creates a correction memory.
    - Thumbs-up: auto-creates a VerifiedQuery if the message has SQL.
    Upsert on message_id (one feedback per message per user).
    """
    user = await require_permission(Permission.QUERY_DATA, current_user, db)
    org_id = user.organization_id or ""

    # Verify message exists and belongs to org
    msg_stmt = (
        select(Message)
        .join(Conversation, Message.conversation_id == Conversation.id)
        .where(Message.id == message_id, Message.role == "assistant")
    )
    result = await db.execute(msg_stmt)
    message = result.scalar_one_or_none()
    if not message:
        raise HTTPException(status_code=404, detail="Assistant message not found.")

    # Get conversation for context
    conv_stmt = select(Conversation).where(Conversation.id == message.conversation_id)
    conv_result = await db.execute(conv_stmt)
    conversation = conv_result.scalar_one_or_none()

    # Get the preceding user message (the question that triggered this response)
    user_msg_stmt = (
        select(Message)
        .where(
            Message.conversation_id == message.conversation_id,
            Message.role == "user",
            Message.created_at < message.created_at,
        )
        .order_by(Message.created_at.desc())
        .limit(1)
    )
    user_msg_result = await db.execute(user_msg_stmt)
    user_msg = user_msg_result.scalar_one_or_none()
    user_query = user_msg.content if user_msg else ""

    # Upsert feedback
    existing_stmt = select(MessageFeedback).where(MessageFeedback.message_id == message_id)
    existing_result = await db.execute(existing_stmt)
    existing = existing_result.scalar_one_or_none()

    if existing:
        existing.rating = body.rating
        existing.correction_note = body.correction_note
        existing.category = body.category
        existing.user_query = user_query
        existing.sql_generated = message.sql_query
        feedback = existing
    else:
        feedback = MessageFeedback(
            message_id=message_id,
            conversation_id=message.conversation_id,
            user_id=user.id,
            organization_id=org_id,
            connection_id=conversation.connection_id if conversation else None,
            rating=body.rating,
            correction_note=body.correction_note,
            category=body.category,
            user_query=user_query,
            sql_generated=message.sql_query,
        )
        db.add(feedback)

    await db.flush()
    await db.refresh(feedback)

    # Side effects
    if body.rating == "down":
        await _create_correction_memory(
            db,
            org_id,
            user.id,
            message.conversation_id,
            user_query,
            message.sql_query,
            body.correction_note,
            body.category,
        )

    if body.rating == "up" and message.sql_query and conversation and conversation.connection_id:
        await _create_verified_query(
            db,
            org_id,
            conversation.connection_id,
            user.id,
            user_query,
            message.sql_query,
            message_id,
        )

    # Deactivate verified query on thumbs-down if one exists
    if body.rating == "down" and message.sql_query:
        await _deactivate_matching_verified_query(
            db,
            org_id,
            user_query,
        )

    await db.commit()
    await db.refresh(feedback)

    logger.info(
        "Feedback submitted: message=%s rating=%s category=%s",
        message_id,
        body.rating,
        body.category,
    )
    return feedback


@router.get("/{message_id}/feedback", response_model=FeedbackResponse | None)
async def get_feedback(
    message_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> MessageFeedback | None:
    """Get feedback for a specific message."""
    await require_permission(Permission.VIEW_DATA, current_user, db)

    stmt = select(MessageFeedback).where(MessageFeedback.message_id == message_id)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


@router.delete("/{message_id}/feedback", status_code=status.HTTP_204_NO_CONTENT)
async def delete_feedback(
    message_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> None:
    """Retract feedback on a message."""
    user = await require_permission(Permission.QUERY_DATA, current_user, db)

    stmt = select(MessageFeedback).where(
        MessageFeedback.message_id == message_id,
        MessageFeedback.user_id == user.id,
    )
    result = await db.execute(stmt)
    feedback = result.scalar_one_or_none()
    if feedback:
        await db.delete(feedback)
        await db.commit()


@router.get("/feedback/stats", response_model=AccuracyStats)
async def get_accuracy_stats(
    current_user: CurrentUser,
    db: DbSession,
    connection_id: uuid.UUID | None = None,
) -> AccuracyStats:
    """Get accuracy stats for a connection or the whole org."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    org_id = user.organization_id or ""

    base = select(MessageFeedback).where(MessageFeedback.organization_id == org_id)
    if connection_id:
        base = base.where(MessageFeedback.connection_id == connection_id)

    # Total counts
    up_stmt = base.where(MessageFeedback.rating == "up")
    down_stmt = base.where(MessageFeedback.rating == "down")

    up_result = await db.execute(select(sa_func.count()).select_from(up_stmt.subquery()))
    down_result = await db.execute(select(sa_func.count()).select_from(down_stmt.subquery()))
    total_result = await db.execute(select(sa_func.count()).select_from(base.subquery()))

    up_count = up_result.scalar() or 0
    down_count = down_result.scalar() or 0
    total = total_result.scalar() or 0

    accuracy = (up_count / total * 100) if total > 0 else 0.0

    # By category (for thumbs-down)
    cat_stmt = (
        select(MessageFeedback.category, sa_func.count())
        .where(
            MessageFeedback.organization_id == org_id,
            MessageFeedback.rating == "down",
            MessageFeedback.category.is_not(None),
        )
        .group_by(MessageFeedback.category)
    )
    if connection_id:
        cat_stmt = cat_stmt.where(MessageFeedback.connection_id == connection_id)
    cat_result = await db.execute(cat_stmt)
    by_category = {row[0]: row[1] for row in cat_result.all() if row[0]}

    return AccuracyStats(
        total_rated=total,
        thumbs_up=up_count,
        thumbs_down=down_count,
        accuracy_pct=round(accuracy, 1),
        by_category=by_category,
    )


# ── Side effects ────────────────────────────────────────────────────


async def _create_correction_memory(
    db: DbSession,
    org_id: str,
    user_id: uuid.UUID,
    conversation_id: uuid.UUID,
    user_query: str,
    sql_query: str | None,
    correction_note: str | None,
    category: str | None,
) -> None:
    """Create a high-confidence correction memory from thumbs-down feedback."""
    content_parts = [f"User query: {user_query}"]
    if sql_query:
        content_parts.append(f"Generated SQL was incorrect: {sql_query[:500]}")
    if correction_note:
        content_parts.append(f"User correction: {correction_note}")
    if category:
        content_parts.append(f"Error type: {category}")

    content = "\n".join(content_parts)

    try:
        from app.services.memory import save_memory

        await save_memory(
            db,
            org_id=org_id,
            content=content,
            memory_type="correction",
            user_id=user_id,
            source="user_feedback",
            source_conversation_id=conversation_id,
            confidence=0.95,
        )
        logger.info("Correction memory created from feedback")
    except Exception as exc:
        logger.warning("Failed to create correction memory: %s", exc)


async def _create_verified_query(
    db: DbSession,
    org_id: str,
    connection_id: uuid.UUID,
    user_id: uuid.UUID,
    user_query: str,
    sql_query: str,
    message_id: uuid.UUID,
) -> None:
    """Auto-create a VerifiedQuery from thumbs-up feedback on SQL."""
    if not user_query or not sql_query:
        return

    from app.db.models import VerifiedQuery

    # Normalize question pattern
    pattern = _normalize_question(user_query)

    # Check if already exists
    existing_stmt = select(VerifiedQuery).where(
        VerifiedQuery.organization_id == org_id,
        VerifiedQuery.connection_id == connection_id,
        VerifiedQuery.question_pattern == pattern,
        VerifiedQuery.is_active == True,
    )
    existing_result = await db.execute(existing_stmt)
    if existing_result.scalar_one_or_none():
        logger.debug("Verified query already exists for pattern: %s", pattern[:50])
        return

    vq = VerifiedQuery(
        organization_id=org_id,
        connection_id=connection_id,
        original_question=user_query,
        question_pattern=pattern,
        sql_template=sql_query,
        verified_by=user_id,
        source_message_id=message_id,
    )
    db.add(vq)
    logger.info("Verified query auto-created from thumbs-up: %s", pattern[:50])


async def _deactivate_matching_verified_query(
    db: DbSession,
    org_id: str,
    user_query: str,
) -> None:
    """Deactivate any verified query matching this question pattern."""
    pattern = _normalize_question(user_query)

    from app.db.models import VerifiedQuery

    stmt = select(VerifiedQuery).where(
        VerifiedQuery.organization_id == org_id,
        VerifiedQuery.question_pattern == pattern,
        VerifiedQuery.is_active == True,
    )
    result = await db.execute(stmt)
    vq = result.scalar_one_or_none()
    if vq:
        vq.is_active = False
        logger.info("Deactivated verified query from thumbs-down: %s", pattern[:50])


def _normalize_question(question: str) -> str:
    """Normalize a question into a matchable pattern.

    - Lowercase
    - Remove specific dates, numbers, proper nouns
    - Replace with placeholders
    """
    import re

    pattern = question.lower().strip()
    # Replace dates
    pattern = re.sub(r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b", "{date}", pattern)
    # Replace Q1-Q4 year references
    pattern = re.sub(r"\bq[1-4]\s*\d{4}\b", "{quarter}", pattern)
    pattern = re.sub(r"\bq[1-4]\b", "{quarter}", pattern)
    # Replace year references
    pattern = re.sub(r"\b20\d{2}\b", "{year}", pattern)
    # Replace numbers
    pattern = re.sub(r"\b\d+\b", "{N}", pattern)
    # Collapse whitespace
    pattern = re.sub(r"\s+", " ", pattern).strip()
    return pattern
