"""Verified Queries service — match, create, and validate saved SQL patterns.

When a user thumbs-up a query, the (question → SQL) mapping is saved org-wide.
Future similar questions skip the LLM pipeline and use the verified SQL directly.

Matching uses token-level Jaccard similarity on normalized question patterns.
No vector DB needed — volume is small (< 100 per connection typically).
"""

from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import VerifiedQuery

logger = logging.getLogger(__name__)

_SIMILARITY_THRESHOLD = 0.80

# Stop words to remove during normalization
_STOP_WORDS = frozenset(
    {
        "the",
        "a",
        "an",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "have",
        "has",
        "had",
        "do",
        "does",
        "did",
        "will",
        "would",
        "can",
        "could",
        "should",
        "may",
        "might",
        "must",
        "i",
        "me",
        "my",
        "we",
        "our",
        "you",
        "your",
        "show",
        "me",
        "give",
        "tell",
        "find",
        "get",
        "list",
        "display",
        "please",
        "also",
        "just",
        "like",
        "want",
        "need",
        "what",
        "which",
        "who",
        "how",
        "when",
        "where",
        "why",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "with",
        "by",
        "from",
        "and",
        "or",
        "but",
        "not",
        "no",
    }
)


def normalize_question(question: str) -> str:
    """Normalize a question into a matchable pattern.

    - Lowercase, strip punctuation
    - Remove dates, numbers, quarters, years
    - Remove stop words
    - Sort tokens for order-independence
    """
    pattern = question.lower().strip()

    # Replace dates (2024-01-15, 01/15/2024, etc.)
    pattern = re.sub(r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b", "", pattern)
    pattern = re.sub(r"\b\d{1,2}[-/]\d{1,2}[-/]\d{4}\b", "", pattern)

    # Replace Q1-Q4 references
    pattern = re.sub(r"\bq[1-4]\s*\d{4}\b", "", pattern)
    pattern = re.sub(r"\bq[1-4]\b", "", pattern)

    # Replace years
    pattern = re.sub(r"\b20\d{2}\b", "", pattern)

    # Replace standalone numbers
    pattern = re.sub(r"\b\d+\b", "", pattern)

    # Remove punctuation
    pattern = re.sub(r"[^\w\s]", " ", pattern)

    # Tokenize, remove stop words, sort
    tokens = [t for t in pattern.split() if t and t not in _STOP_WORDS and len(t) > 1]
    tokens = sorted(set(tokens))

    return " ".join(tokens)


def _jaccard_similarity(a: str, b: str) -> float:
    """Token-level Jaccard similarity between two normalized patterns."""
    tokens_a = set(a.split())
    tokens_b = set(b.split())

    if not tokens_a or not tokens_b:
        return 0.0

    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b

    return len(intersection) / len(union) if union else 0.0


async def find_matching_verified_query(
    db: AsyncSession,
    question: str,
    connection_id: uuid.UUID,
    org_id: str,
    threshold: float = _SIMILARITY_THRESHOLD,
) -> VerifiedQuery | None:
    """Find a matching verified query for the given question.

    Returns the best match above the threshold, or None.
    """
    normalized = normalize_question(question)
    if not normalized:
        return None

    # Load all active verified queries for this connection
    stmt = select(VerifiedQuery).where(
        VerifiedQuery.organization_id == org_id,
        VerifiedQuery.connection_id == connection_id,
        VerifiedQuery.is_active == True,
    )
    result = await db.execute(stmt)
    candidates = list(result.scalars().all())

    if not candidates:
        return None

    # Score each candidate
    best_match: VerifiedQuery | None = None
    best_score = 0.0

    for vq in candidates:
        score = _jaccard_similarity(normalized, vq.question_pattern)
        if score > best_score:
            best_score = score
            best_match = vq

    if best_score >= threshold and best_match:
        logger.info(
            "Verified query match: score=%.2f pattern='%s' → '%s'",
            best_score,
            normalized[:50],
            best_match.question_pattern[:50],
        )
        # Update usage stats
        best_match.use_count += 1
        best_match.last_used_at = datetime.utcnow()
        await db.flush()
        return best_match

    logger.debug("No verified query match (best=%.2f < %.2f)", best_score, threshold)
    return None


async def create_verified_query(
    db: AsyncSession,
    org_id: str,
    connection_id: uuid.UUID,
    question: str,
    sql: str,
    verified_by: uuid.UUID,
    source_message_id: uuid.UUID | None = None,
) -> VerifiedQuery | None:
    """Create a new verified query, deduplicating by pattern."""
    pattern = normalize_question(question)
    if not pattern or not sql:
        return None

    # Check for existing
    stmt = select(VerifiedQuery).where(
        VerifiedQuery.organization_id == org_id,
        VerifiedQuery.connection_id == connection_id,
        VerifiedQuery.question_pattern == pattern,
    )
    result = await db.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing:
        # Reactivate if it was deactivated, update SQL
        existing.is_active = True
        existing.sql_template = sql
        existing.verified_by = verified_by
        existing.source_message_id = source_message_id
        existing.confidence = 1.0
        await db.flush()
        logger.info("Verified query updated: %s", pattern[:50])
        return existing

    vq = VerifiedQuery(
        organization_id=org_id,
        connection_id=connection_id,
        original_question=question,
        question_pattern=pattern,
        sql_template=sql,
        verified_by=verified_by,
        source_message_id=source_message_id,
    )
    db.add(vq)
    await db.flush()
    await db.refresh(vq)
    logger.info("Verified query created: %s", pattern[:50])
    return vq


async def validate_verified_queries(
    db: AsyncSession,
    connection_id: uuid.UUID,
    schema_cache: dict[str, Any],
) -> dict[str, int]:
    """Validate all verified queries against current schema.

    Deactivates queries that reference tables/columns no longer in the schema.
    Returns: {"valid": N, "invalid": N}
    """
    known_tables = {t["name"].lower() for t in schema_cache.get("tables", [])}

    stmt = select(VerifiedQuery).where(
        VerifiedQuery.connection_id == connection_id,
        VerifiedQuery.is_active == True,
    )
    result = await db.execute(stmt)
    queries = list(result.scalars().all())

    valid = 0
    invalid = 0

    for vq in queries:
        # Extract table references from SQL
        sql_lower = vq.sql_template.lower()
        tables_in_sql = set(re.findall(r"\bfrom\s+(\w+)", sql_lower))
        tables_in_sql |= set(re.findall(r"\bjoin\s+(\w+)", sql_lower))

        # Check if all referenced tables still exist
        missing = tables_in_sql - known_tables
        if missing:
            vq.is_active = False
            invalid += 1
            logger.warning("Deactivated verified query %s: missing tables %s", vq.id, missing)
        else:
            valid += 1

    await db.flush()
    return {"valid": valid, "invalid": invalid}
