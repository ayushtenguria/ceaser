"""Result store — persist intermediate query results as parquet.

After each successful query that produces tabular data, the result is saved
as a parquet file. Follow-up queries can reference previous results directly
instead of recomputing from the original data.

Storage: Same Supabase/local backend as file uploads.
Protocol: ceaser://results/{org_id}/{conversation_id}/{result_id}.parquet
TTL: 24 hours (auto-cleaned).
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

_CEASER_PROTOCOL = "ceaser://"
_MAX_RESULT_ROWS = 100_000  # Don't persist huge results


async def save_query_result(
    table_data: dict[str, Any],
    org_id: str,
    conversation_id: str,
    query_text: str,
) -> dict[str, str] | None:
    """Save a query result as parquet for follow-up reference.

    Returns {"result_id": str, "parquet_ref": str, "row_count": int, "columns": [...]}
    or None if the result is too small to bother persisting.
    """
    rows = table_data.get("rows", [])
    columns = table_data.get("columns", [])

    if not rows or not columns or len(rows) < 2:
        return None  # Don't persist trivially small results

    try:
        df = pd.DataFrame(rows, columns=columns if len(rows[0]) == len(columns) else None)
    except Exception:
        try:
            df = pd.DataFrame(rows)
        except Exception as exc:
            logger.debug("Could not create DataFrame from result: %s", exc)
            return None

    if len(df) > _MAX_RESULT_ROWS:
        df = df.head(_MAX_RESULT_ROWS)

    result_id = hashlib.md5(f"{conversation_id}:{query_text}:{len(rows)}".encode()).hexdigest()[:12]

    remote_path = f"results/{org_id}/{conversation_id}/{result_id}.parquet"
    buf = df.to_parquet(index=False)

    from app.services.storage import get_storage

    storage = get_storage()

    try:
        await storage.upload(buf, remote_path)
        logger.info(
            "Saved query result: %s (%d rows × %d cols)",
            result_id,
            len(df),
            len(df.columns),
        )
        return {
            "result_id": result_id,
            "parquet_ref": f"{_CEASER_PROTOCOL}{remote_path}",
            "row_count": len(df),
            "columns": list(df.columns),
        }
    except Exception as exc:
        logger.warning("Failed to save query result: %s", exc)
        return None


def build_result_context(
    previous_results: list[dict[str, str]],
) -> str:
    """Build context string for follow-up queries referencing previous results.

    Injected into schema_context so the Python/SQL agent knows about
    available intermediate results.
    """
    if not previous_results:
        return ""

    lines = [
        "\nPREVIOUS QUERY RESULTS (available as parquet files for follow-up analysis):",
        "=" * 60,
        "Use these to reference results from earlier queries without recomputing.",
        "",
    ]

    for i, result in enumerate(previous_results, 1):
        ref = result.get("parquet_ref", "")
        cols = result.get("columns", [])
        rows = result.get("row_count", 0)
        query = result.get("query", "")[:100]

        var_name = f"prev_result_{i}"
        col_list = ", ".join(cols[:15])
        if len(cols) > 15:
            col_list += f", ... ({len(cols) - 15} more)"

        lines.append(f'  {var_name}: {rows} rows — from: "{query}"')
        lines.append(f"    Columns: {col_list}")
        lines.append(f'    Load: {var_name} = pd.read_parquet("{ref}")')
        lines.append(f"    DuckDB: duckdb.sql(\"SELECT ... FROM read_parquet('{ref}')\")")
        lines.append("")

    return "\n".join(lines)


async def load_conversation_results(
    db: Any,
    conversation_id: str,
) -> list[dict[str, str]]:
    """Load all saved results for a conversation from message metadata.

    Reads the result_ref field from assistant messages in this conversation.
    """
    from sqlalchemy import select

    from app.db.models import Message

    stmt = (
        select(Message)
        .where(
            Message.conversation_id == conversation_id,
            Message.role == "assistant",
            Message.table_data.is_not(None),
        )
        .order_by(Message.created_at)
    )
    result = await db.execute(stmt)
    messages = list(result.scalars().all())

    results = []
    for msg in messages:
        # Check if this message has a stored result ref
        result_meta = msg.table_data.get("_result_ref") if msg.table_data else None
        if result_meta:
            result_meta["query"] = msg.summary or (msg.content or "")[:100]
            results.append(result_meta)

    return results
