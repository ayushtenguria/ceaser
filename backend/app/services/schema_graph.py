"""Graph RAG Schema Layer — Neo4j-backed schema intelligence.

Builds a knowledge graph from database schemas AND uploaded files.
At query time, traverses the graph to find only relevant tables/DataFrames + exact JOIN paths.

Supports:
  - Database tables (:Table nodes)
  - Uploaded file DataFrames (:FileNode nodes)
  - Cross-source links (file columns matching DB columns)
  - Conversation-scoped file relationships
  - Version superseding (newer file replaces older with same name)
  - Tiered retrieval (conversation files > recent org files > old files)

Usage:
    from app.services.schema_graph import (
        get_graph_driver, build_schema_graph, build_file_graph,
        select_relevant_schema, select_relevant_files,
    )
"""

from __future__ import annotations

import logging
import re
from typing import Any

from neo4j import AsyncDriver, AsyncGraphDatabase

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_driver: AsyncDriver | None = None


def get_graph_driver() -> AsyncDriver | None:
    """Return the Neo4j async driver singleton. None if not configured."""
    global _driver
    if _driver is not None:
        return _driver

    settings = get_settings()
    if not settings.neo4j_uri:
        return None

    _driver = AsyncGraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_username, settings.neo4j_password),
    )
    logger.info("Neo4j driver initialized: %s", settings.neo4j_uri)
    return _driver


async def close_graph_driver():
    """Close the Neo4j driver on shutdown."""
    global _driver
    if _driver:
        await _driver.close()
        _driver = None


def _classify_domain(col_name: str, col_type: str) -> str:
    """Classify a column into a business domain."""
    name = col_name.lower()
    dtype = col_type.lower()

    if name == "id" or name.endswith("_id") or name.endswith("_key"):
        return "identifier"
    if any(
        kw in name
        for kw in (
            "price",
            "amount",
            "revenue",
            "cost",
            "total",
            "spend",
            "fee",
            "salary",
            "budget",
            "mrr",
            "arr",
        )
    ):
        return "monetary"
    if "decimal" in dtype or "money" in dtype or "numeric" in dtype:
        return "monetary"
    if any(
        kw in name
        for kw in ("city", "state", "country", "region", "zip", "pincode", "address", "location")
    ):
        return "location"
    if any(
        kw in name
        for kw in (
            "date",
            "time",
            "created",
            "updated",
            "_at",
            "when",
            "start",
            "end",
            "born",
            "expires",
        )
    ):
        return "temporal"
    if "timestamp" in dtype or "date" in dtype:
        return "temporal"
    if any(kw in name for kw in ("email", "phone", "mobile")):
        return "contact"
    if any(
        kw in name
        for kw in ("status", "type", "category", "level", "tier", "plan", "priority", "stage")
    ):
        return "categorical"
    if any(kw in name for kw in ("count", "quantity", "qty", "num", "number", "size")):
        return "quantity"
    if any(kw in name for kw in ("name", "title", "label", "description", "subject")):
        return "descriptive"
    if any(kw in name for kw in ("rating", "score", "rank", "weight", "pct", "percent")):
        return "metric"

    return "text"


def _is_numeric(col_type: str) -> bool:
    dtype = col_type.lower()
    return any(
        kw in dtype
        for kw in ("int", "decimal", "numeric", "float", "double", "real", "money", "serial")
    )


def _is_temporal(col_type: str) -> bool:
    dtype = col_type.lower()
    return any(kw in dtype for kw in ("timestamp", "date", "time", "interval"))


def _is_categorical(unique_count: int, row_count: int) -> bool:
    if row_count == 0:
        return False
    return unique_count < 50 and (unique_count / max(row_count, 1)) < 0.1


async def build_schema_graph(
    connection_id: str,
    org_id: str,
    schema_data: dict[str, Any],
    db_type: str = "postgresql",
) -> int:
    """Build the schema graph in Neo4j from introspected schema data.

    Args:
        connection_id: UUID of the database connection
        org_id: organization ID for isolation
        schema_data: the schema_cache dict from DatabaseConnection
        db_type: postgresql, mysql, sqlite, etc.

    Returns:
        Number of table nodes created.
    """
    driver = get_graph_driver()
    if not driver:
        logger.warning("Neo4j not configured — skipping graph build")
        return 0

    tables = schema_data.get("tables", [])
    if not tables:
        return 0

    async with driver.session() as session:
        await session.run(
            "MATCH (t:Table {connection_id: $conn_id}) DETACH DELETE t",
            conn_id=connection_id,
        )
        await session.run(
            "MATCH (c:Column {connection_id: $conn_id}) DETACH DELETE c",
            conn_id=connection_id,
        )

        for table in tables:
            table_name = table.get("name", "")
            row_count = table.get("row_count", 0)
            columns = table.get("columns", [])

            await session.run(
                """
                CREATE (t:Table {
                    name: $name, connection_id: $conn_id, org_id: $org_id,
                    db_type: $db_type, row_count: $rows, column_count: $col_count
                })
            """,
                name=table_name,
                conn_id=connection_id,
                org_id=org_id,
                db_type=db_type,
                rows=row_count or 0,
                col_count=len(columns),
            )

            # Create column nodes
            for col in columns:
                col_name = col.get("name", "")
                col_type = col.get("data_type", "unknown")
                samples = col.get("sample_values", [])
                unique_count = len(set(str(s) for s in samples)) if samples else 0

                domain = _classify_domain(col_name, col_type)
                is_pk = col.get("primary_key", False)
                fk = col.get("foreign_key")

                await session.run(
                    """
                    MATCH (t:Table {name: $table, connection_id: $conn_id})
                    CREATE (t)-[:HAS_COLUMN]->(c:Column {
                        name: $name, connection_id: $conn_id,
                        data_type: $dtype, domain: $domain,
                        is_numeric: $numeric, is_temporal: $temporal,
                        is_categorical: $categorical,
                        is_pk: $pk, is_fk: $fk,
                        nullable: $nullable,
                        sample_values: $samples
                    })
                """,
                    table=table_name,
                    conn_id=connection_id,
                    name=col_name,
                    dtype=col_type,
                    domain=domain,
                    numeric=_is_numeric(col_type),
                    temporal=_is_temporal(col_type),
                    categorical=_is_categorical(unique_count, row_count or 0),
                    pk=is_pk,
                    fk=fk is not None,
                    nullable=col.get("nullable", True),
                    samples=[str(s) for s in (samples or [])[:10]],
                )

                # Create FK relationship
                if fk:
                    parts = fk.split(".")
                    if len(parts) == 2:
                        ref_table, ref_col = parts
                        await session.run(
                            """
                            MATCH (from_t:Table {name: $from_table, connection_id: $conn_id})
                            MATCH (to_t:Table {name: $to_table, connection_id: $conn_id})
                            MERGE (from_t)-[:FK {
                                from_column: $from_col, to_column: $to_col,
                                join_sql: $join_sql
                            }]->(to_t)
                        """,
                            from_table=table_name,
                            to_table=ref_table,
                            conn_id=connection_id,
                            from_col=col_name,
                            to_col=ref_col,
                            join_sql=f"{table_name}.{col_name} = {ref_table}.{ref_col}",
                        )

        # Infer relationships from shared column names across tables
        await session.run(
            """
            MATCH (t1:Table {connection_id: $conn_id})-[:HAS_COLUMN]->(c1:Column)
            MATCH (t2:Table {connection_id: $conn_id})-[:HAS_COLUMN]->(c2:Column)
            WHERE t1 <> t2 AND c1.name = c2.name AND c1.domain = 'identifier'
            AND NOT EXISTS { (t1)-[:FK]-(t2) }
            MERGE (t1)-[:INFERRED_REL {
                shared_column: c1.name, confidence: 0.85, method: 'name_match'
            }]->(t2)
        """,
            conn_id=connection_id,
        )

        # Cross-domain links (city ↔ shipping_city)
        await session.run(
            """
            MATCH (c1:Column {connection_id: $conn_id})
            MATCH (c2:Column {connection_id: $conn_id})
            WHERE c1 <> c2 AND c1.domain = c2.domain
            AND c1.domain IN ['location', 'contact']
            AND c1.name <> c2.name
            MERGE (c1)-[:SAME_DOMAIN {domain: c1.domain, similarity: 0.7}]->(c2)
        """,
            conn_id=connection_id,
        )

    logger.info("Schema graph built: %d tables for connection %s", len(tables), connection_id)
    return len(tables)


# ---------------------------------------------------------------------------
# Entity extraction — no LLM, pure keyword/pattern matching
# ---------------------------------------------------------------------------

TEMPORAL_KEYWORDS = frozenset(
    {
        "monthly",
        "weekly",
        "daily",
        "yearly",
        "quarterly",
        "trend",
        "over time",
        "by month",
        "by week",
        "by year",
        "time series",
        "growth",
        "by date",
    }
)
NUMERIC_KEYWORDS = frozenset(
    {
        "revenue",
        "sales",
        "amount",
        "total",
        "average",
        "sum",
        "count",
        "price",
        "cost",
        "profit",
        "margin",
        "spend",
        "budget",
        "salary",
    }
)
CHART_KEYWORDS = {
    "histogram": frozenset({"histogram", "distribution", "spread"}),
    "pie": frozenset({"pie", "proportion", "share", "breakdown"}),
    "line": frozenset({"line", "trend", "over time", "time series"}),
    "scatter": frozenset({"scatter", "correlation", "vs", "versus", "relationship"}),
    "bar": frozenset({"bar chart", "bar graph", "compare", "comparison", "top"}),
}


def extract_entities(question: str) -> dict[str, Any]:
    """Extract searchable entities from the question. No LLM — pure code."""
    q = question.lower()
    words = set(q.split())

    entities = {
        "keywords": [],
        "table_hints": [],
        "column_hints": [],
        "needs_temporal": any(kw in q for kw in TEMPORAL_KEYWORDS),
        "needs_numeric": any(kw in q for kw in NUMERIC_KEYWORDS),
        "chart_type": None,
        "is_complex": q.count("?") >= 3 or len(question) > 300,
    }

    for chart_type, keywords in CHART_KEYWORDS.items():
        if any(kw in q for kw in keywords):
            entities["chart_type"] = chart_type
            break

    stop_words = {
        "the",
        "a",
        "an",
        "is",
        "are",
        "was",
        "were",
        "show",
        "me",
        "give",
        "please",
        "can",
        "you",
        "what",
        "how",
        "many",
        "much",
        "do",
        "does",
        "for",
        "by",
        "in",
        "of",
        "to",
        "and",
        "or",
        "with",
        "as",
        "from",
    }
    entities["keywords"] = [w for w in words if w not in stop_words and len(w) > 2]

    return entities


_TRAVERSAL_CYPHER = """
// Step 1: Find tables with columns matching any keyword
MATCH (t:Table {connection_id: $conn_id})-[:HAS_COLUMN]->(c:Column)
WHERE ANY(kw IN $keywords WHERE
    toLower(c.name) CONTAINS kw OR
    toLower(t.name) CONTAINS kw OR
    ANY(sv IN c.sample_values WHERE toLower(sv) CONTAINS kw)
)
WITH COLLECT(DISTINCT t) AS matched_tables

// Step 2: Expand to connected tables (1-2 hops)
UNWIND matched_tables AS mt
OPTIONAL MATCH (mt)-[:FK|INFERRED_REL*1..2]-(neighbor:Table {connection_id: $conn_id})
WITH matched_tables, COLLECT(DISTINCT neighbor) AS neighbors
WITH [x IN matched_tables + neighbors WHERE x IS NOT NULL] AS all_tables
UNWIND all_tables AS t
WITH DISTINCT t

// Step 3: Get columns
MATCH (t)-[:HAS_COLUMN]->(c:Column)
WITH t, COLLECT({
    name: c.name, type: c.data_type, domain: c.domain,
    numeric: c.is_numeric, temporal: c.is_temporal,
    categorical: c.is_categorical, pk: c.is_pk, fk: c.is_fk,
    nullable: c.nullable, samples: c.sample_values
}) AS columns

// Step 4: Get JOIN paths
OPTIONAL MATCH (t)-[fk:FK]->(related:Table {connection_id: $conn_id})
WITH t, columns, COLLECT(CASE WHEN fk IS NOT NULL
    THEN {to_table: related.name, join_sql: fk.join_sql}
    ELSE NULL END) AS raw_joins
WITH t, columns, [j IN raw_joins WHERE j IS NOT NULL] AS joins

RETURN t.name AS table_name,
       t.row_count AS row_count,
       t.db_type AS db_type,
       columns,
       joins
"""


async def select_relevant_schema(
    question: str,
    connection_id: str,
    org_id: str,
    token_budget: int = 4000,
) -> str:
    """Graph-based schema selection. Returns prompt-ready context string.

    Falls back to full schema dump if Neo4j is unavailable.
    """
    driver = get_graph_driver()
    if not driver:
        return ""  # Caller should fall back to _build_schema_context

    entities = extract_entities(question)

    if not entities["keywords"]:
        # No useful keywords — return all tables (small DB) or empty
        return ""

    try:
        async with driver.session() as session:
            result = await session.run(
                _TRAVERSAL_CYPHER,
                conn_id=connection_id,
                keywords=entities["keywords"],
            )
            records = [record.data() async for record in result]

        if not records:
            return ""  # No matches — fall back

        # Score and select tables within token budget
        scored = _score_tables(records, entities)
        selected = _apply_token_budget(scored, token_budget)

        if not selected:
            return ""

        # Format for SQL agent
        return _format_graph_context(selected, entities)

    except Exception as exc:
        logger.warning("Graph traversal failed (falling back): %s", exc)
        return ""  # Caller falls back to full schema dump


def _score_tables(records: list[dict], entities: dict) -> list[tuple[dict, float]]:
    """Score tables by relevance to the question."""
    scored = []
    for record in records:
        score = 0.3

        table_name = record.get("table_name", "").lower()
        columns = record.get("columns", [])

        for kw in entities["keywords"]:
            if kw in table_name:
                score += 0.4

        for col in columns:
            col_name = col.get("name", "").lower()
            for kw in entities["keywords"]:
                if kw in col_name:
                    score += 0.3

        if entities["needs_temporal"]:
            if any(col.get("temporal") for col in columns):
                score += 0.2

        if entities["needs_numeric"]:
            if any(col.get("numeric") for col in columns):
                score += 0.2

        scored.append((record, min(score, 2.0)))

    scored.sort(key=lambda x: x[1], reverse=True)
    return scored


def _apply_token_budget(scored: list[tuple[dict, float]], budget: int) -> list[dict]:
    """Select tables that fit within the token budget."""
    selected = []
    total_tokens = 0

    for record, score in scored:
        col_count = len(record.get("columns", []))
        estimated = 30 + (col_count * 20) + 20

        if total_tokens + estimated > budget and selected:
            break
        if score < 0.1:
            break

        selected.append(record)
        total_tokens += estimated

    return selected


def _format_graph_context(tables: list[dict], entities: dict) -> str:
    """Format selected tables into SQL agent prompt context."""
    lines = ["DATABASE SCHEMA (selected for this query)", "=" * 55]

    all_joins = []

    for record in tables:
        table_name = record["table_name"]
        row_count = record.get("row_count", 0)
        columns = record.get("columns", [])
        joins = [j for j in record.get("joins", []) if j]

        lines.append(f"\nTable: {table_name}  (~{row_count:,} rows)")
        lines.append("-" * 40)

        for col in columns:
            if not col.get("name"):
                continue
            parts = [f"  {col['name']}: {col.get('type', 'unknown')}"]
            if col.get("pk"):
                parts.append("[PK]")
            if col.get("fk"):
                parts.append("[FK]")
            if not col.get("nullable", True):
                parts.append("[NOT NULL]")
            if col.get("temporal"):
                parts.append("[TEMPORAL]")
            if col.get("numeric"):
                parts.append("[NUMERIC]")
            samples = col.get("samples", [])
            if samples:
                vals = ", ".join(f"'{v}'" for v in samples[:8])
                parts.append(f"  values: [{vals}]")
            lines.append(" ".join(parts))

        for j in joins:
            if j.get("join_sql"):
                all_joins.append(j["join_sql"])

    if all_joins:
        lines.append("\n\nJOIN PATHS (use these exact conditions)")
        lines.append("=" * 55)
        for join_sql in set(all_joins):
            lines.append(f"  {join_sql}")

    hints = []
    if entities.get("chart_type") == "histogram":
        hints.append(
            "HISTOGRAM: Return RAW individual values — do NOT aggregate with CASE/GROUP BY."
        )
    if entities.get("needs_temporal"):
        hints.append("TEMPORAL: Use DATE_TRUNC('month', col) for monthly grouping.")
    if hints:
        lines.append("\n\nQUERY HINTS")
        lines.append("=" * 55)
        for h in hints:
            lines.append(f"  {h}")

    return "\n".join(lines)


async def build_file_graph(
    file_id: str,
    org_id: str,
    filename: str,
    conversation_id: str | None,
    uploaded_by: str | None,
    column_info: dict | None,
    parquet_paths: dict | None,
    row_count: int = 0,
    project_tag: str = "",
) -> int:
    """Build file nodes in Neo4j from uploaded file metadata.

    Each file gets its own :FileNode with file-scoped :FileColumn nodes.
    Cross-file relationships are scoped to the same conversation.
    """
    driver = get_graph_driver()
    if not driver:
        return 0

    columns = (column_info or {}).get("columns", [])
    if not columns:
        return 0

    try:
        async with driver.session() as session:
            await session.run(
                """
                MATCH (old:FileNode {org_id: $org_id, filename: $filename, is_active: true})
                WHERE old.file_id <> $file_id
                SET old.is_active = false
                WITH old
                MATCH (new:FileNode {file_id: $file_id})
                WHERE new IS NOT NULL
                CREATE (new)-[:SUPERSEDES {reason: 'newer_upload'}]->(old)
            """,
                org_id=org_id,
                filename=filename,
                file_id=file_id,
            )

            # Create file node
            await session.run(
                """
                MERGE (f:FileNode {file_id: $file_id})
                SET f.org_id = $org_id,
                    f.filename = $filename,
                    f.conversation_id = $conv_id,
                    f.uploaded_by = $user_id,
                    f.uploaded_at = datetime(),
                    f.row_count = $rows,
                    f.is_active = true,
                    f.project_tag = $tag
            """,
                file_id=file_id,
                org_id=org_id,
                filename=filename,
                conv_id=conversation_id or "",
                user_id=uploaded_by or "",
                rows=row_count,
                tag=project_tag,
            )

            # Create column nodes (scoped to this file)
            for col in columns:
                col_name = col.get("name", "")
                col_dtype = col.get("dtype", "object")
                samples = col.get("sample_values", [])
                unique = col.get("unique_count", 0)

                domain = _classify_domain(col_name, col_dtype)

                await session.run(
                    """
                    MATCH (f:FileNode {file_id: $file_id})
                    CREATE (f)-[:HAS_COLUMN]->(c:FileColumn {
                        name: $name, file_id: $file_id,
                        data_type: $dtype, domain: $domain,
                        is_numeric: $numeric, is_temporal: $temporal,
                        is_categorical: $categorical,
                        sample_values: $samples
                    })
                """,
                    file_id=file_id,
                    name=col_name,
                    dtype=col_dtype,
                    domain=domain,
                    numeric=_is_numeric(col_dtype) or col_dtype in ("int64", "float64"),
                    temporal="date" in col_name.lower() or "time" in col_name.lower(),
                    categorical=_is_categorical(unique, row_count) if row_count > 0 else False,
                    samples=[str(s) for s in (samples or [])[:10]],
                )

            # Discover cross-file relationships WITHIN same conversation
            if conversation_id:
                await _build_cross_file_links(session, file_id, org_id, conversation_id)

            # Discover cross-source links (file ↔ database tables)
            await _build_cross_source_links(session, file_id, org_id)

        logger.info(
            "File graph built: %s (%d columns) for file %s", filename, len(columns), file_id
        )
        return len(columns)

    except Exception as exc:
        logger.warning("File graph build failed: %s", exc)
        return 0


async def _build_cross_file_links(session, file_id: str, org_id: str, conversation_id: str):
    """Find shared columns between files in the same conversation."""
    try:
        await session.run(
            """
            MATCH (f1:FileNode {file_id: $file_id})-[:HAS_COLUMN]->(c1:FileColumn)
            MATCH (f2:FileNode {org_id: $org_id, conversation_id: $conv_id, is_active: true})
                  -[:HAS_COLUMN]->(c2:FileColumn)
            WHERE f1 <> f2 AND c1.name = c2.name AND c1.domain = 'identifier'
            AND NOT EXISTS { (f1)-[:SHARED_KEY]-(f2) }
            MERGE (f1)-[:SHARED_KEY {
                shared_column: c1.name, confidence: 0.9
            }]->(f2)
        """,
            file_id=file_id,
            org_id=org_id,
            conv_id=conversation_id,
        )
    except Exception as exc:
        logger.debug("Cross-file link failed: %s", exc)


async def _build_cross_source_links(session, file_id: str, org_id: str):
    """Find file columns that match database table columns (same org)."""
    try:
        await session.run(
            """
            MATCH (f:FileNode {file_id: $file_id})-[:HAS_COLUMN]->(fc:FileColumn)
            MATCH (t:Table {org_id: $org_id})-[:HAS_COLUMN]->(tc:Column)
            WHERE fc.name = tc.name AND fc.domain = tc.domain
              AND fc.domain IN ['identifier', 'monetary', 'location']
            MERGE (fc)-[:CROSS_SOURCE_LINK {
                confidence: 0.8,
                file_column: fc.name,
                table_name: t.name,
                table_column: tc.name
            }]->(tc)
        """,
            file_id=file_id,
            org_id=org_id,
        )
    except Exception as exc:
        logger.debug("Cross-source link failed: %s", exc)


# ---------------------------------------------------------------------------
# File retrieval — query-time file selection
# ---------------------------------------------------------------------------

_FILE_TRAVERSAL_CYPHER = """
// Tier 1: Files in THIS conversation (highest priority)
MATCH (f:FileNode {conversation_id: $conv_id, org_id: $org_id, is_active: true})
      -[:HAS_COLUMN]->(c:FileColumn)
WHERE ANY(kw IN $keywords WHERE
    toLower(c.name) CONTAINS kw OR
    toLower(f.filename) CONTAINS kw OR
    ANY(sv IN c.sample_values WHERE toLower(sv) CONTAINS kw)
)
WITH f, COLLECT({
    name: c.name, type: c.data_type, domain: c.domain,
    numeric: c.is_numeric, temporal: c.is_temporal,
    samples: c.sample_values
}) AS columns, 1.0 AS tier_score

// Get cross-file relationships
OPTIONAL MATCH (f)-[sk:SHARED_KEY]-(other:FileNode {conversation_id: $conv_id, is_active: true})
WITH f, columns, tier_score,
     COLLECT(CASE WHEN sk IS NOT NULL
         THEN {to_file: other.filename, to_file_id: other.file_id,
               shared_column: sk.shared_column, confidence: sk.confidence}
         ELSE NULL END) AS raw_links
WITH f, columns, tier_score, [l IN raw_links WHERE l IS NOT NULL] AS file_links

// Get cross-source links (file ↔ database)
OPTIONAL MATCH (f)-[:HAS_COLUMN]->(fc:FileColumn)-[csl:CROSS_SOURCE_LINK]->(tc:Column)
WITH f, columns, tier_score, file_links,
     COLLECT(CASE WHEN csl IS NOT NULL
         THEN {table_name: csl.table_name, table_column: csl.table_column,
               file_column: csl.file_column}
         ELSE NULL END) AS raw_db_links
WITH f, columns, tier_score, file_links, [l IN raw_db_links WHERE l IS NOT NULL] AS db_links

RETURN f.file_id AS file_id,
       f.filename AS filename,
       f.row_count AS row_count,
       f.conversation_id AS conversation_id,
       f.project_tag AS project_tag,
       columns,
       file_links,
       db_links,
       tier_score
ORDER BY tier_score DESC
"""

_FILE_ORG_CYPHER = """
// Tier 2: Recent org files matching keywords (fallback)
MATCH (f:FileNode {org_id: $org_id, is_active: true})
      -[:HAS_COLUMN]->(c:FileColumn)
WHERE f.conversation_id <> $conv_id
  AND ANY(kw IN $keywords WHERE
      toLower(c.name) CONTAINS kw OR toLower(f.filename) CONTAINS kw)
  AND f.uploaded_at > datetime() - duration('P30D')
WITH f, COLLECT({
    name: c.name, type: c.data_type, domain: c.domain,
    numeric: c.is_numeric, temporal: c.is_temporal,
    samples: c.sample_values
}) AS columns, 0.5 AS tier_score
RETURN f.file_id AS file_id,
       f.filename AS filename,
       f.row_count AS row_count,
       f.conversation_id AS conversation_id,
       f.project_tag AS project_tag,
       columns,
       [] AS file_links,
       [] AS db_links,
       tier_score
ORDER BY f.uploaded_at DESC
LIMIT 5
"""


async def select_relevant_files(
    question: str,
    org_id: str,
    conversation_id: str | None = None,
    connection_id: str | None = None,
    token_budget: int = 3000,
) -> str:
    """Graph-based file selection. Returns prompt-ready context for DataFrames.

    Tiered retrieval:
      Tier 1: Files in this conversation (score 1.0)
      Tier 2: Recent org files (score 0.5, last 30 days)
    """
    driver = get_graph_driver()
    if not driver:
        return ""

    entities = extract_entities(question)
    if not entities["keywords"]:
        return ""

    try:
        all_records = []
        async with driver.session() as session:
            # Tier 1: conversation files
            if conversation_id:
                result = await session.run(
                    _FILE_TRAVERSAL_CYPHER,
                    conv_id=conversation_id,
                    org_id=org_id,
                    keywords=entities["keywords"],
                )
                tier1 = [r.data() async for r in result]
                all_records.extend(tier1)

            # Tier 2: org files (only if Tier 1 has < 3 files)
            if len(all_records) < 3:
                result = await session.run(
                    _FILE_ORG_CYPHER,
                    conv_id=conversation_id or "",
                    org_id=org_id,
                    keywords=entities["keywords"],
                )
                tier2 = [r.data() async for r in result]
                all_records.extend(tier2)

        if not all_records:
            return ""

        # Deduplicate by file_id
        seen = set()
        unique = []
        for r in all_records:
            fid = r.get("file_id")
            if fid not in seen:
                seen.add(fid)
                unique.append(r)

        # Score and select within budget
        scored = _score_files(unique, entities)
        selected = _apply_file_token_budget(scored, token_budget)

        if not selected:
            return ""

        return _format_file_context(selected, entities)

    except Exception as exc:
        logger.warning("File graph traversal failed: %s", exc)
        return ""


def _score_files(records: list[dict], entities: dict) -> list[tuple[dict, float]]:
    """Score files by relevance."""
    scored = []
    for record in records:
        score = record.get("tier_score", 0.3)
        filename = (record.get("filename") or "").lower()
        columns = record.get("columns", [])

        for kw in entities["keywords"]:
            if kw in filename:
                score += 0.3

        for col in columns:
            for kw in entities["keywords"]:
                if kw in (col.get("name") or "").lower():
                    score += 0.2

        if record.get("db_links"):
            score += 0.2

        if record.get("file_links"):
            score += 0.1

        scored.append((record, min(score, 2.5)))

    scored.sort(key=lambda x: x[1], reverse=True)
    return scored


def _apply_file_token_budget(scored: list[tuple[dict, float]], budget: int) -> list[dict]:
    """Select files within token budget."""
    selected = []
    total_tokens = 0

    for record, score in scored:
        col_count = len(record.get("columns", []))
        estimated = 40 + (col_count * 15) + 30
        if total_tokens + estimated > budget and selected:
            break
        if score < 0.1:
            break
        selected.append(record)
        total_tokens += estimated

    return selected


def _format_file_context(files: list[dict], entities: dict) -> str:
    """Format selected files into prompt context for Python/DataFrame agent."""
    lines = ["UPLOADED FILE DATA (use Python/pandas to analyze)", "=" * 55]

    all_file_links = []
    all_db_links = []

    for record in files:
        filename = record.get("filename", "unknown")
        row_count = record.get("row_count", 0)
        columns = record.get("columns", [])
        file_links = record.get("file_links", [])
        db_links = record.get("db_links", [])

        stem = filename.rsplit(".", 1)[0].lower()
        stem = re.sub(r"[^a-z0-9_]", "_", stem)
        stem = re.sub(r"_+", "_", stem).strip("_")[:30]
        var_name = f"df_{stem}"

        lines.append(f"\n{var_name}  (from: {filename}, ~{row_count:,} rows)")
        lines.append("-" * 40)

        for col in columns:
            if not col.get("name"):
                continue
            parts = [f"  {col['name']}: {col.get('type', 'object')}"]
            if col.get("numeric"):
                parts.append("[NUMERIC]")
            if col.get("temporal"):
                parts.append("[TEMPORAL]")
            samples = col.get("samples", [])
            if samples:
                vals = ", ".join(f"'{v}'" for v in samples[:5])
                parts.append(f"  values: [{vals}]")
            lines.append(" ".join(parts))

        all_file_links.extend(file_links)
        all_db_links.extend(db_links)

    if all_file_links:
        lines.append("\n\nCROSS-FILE MERGE PATHS (use pd.merge)")
        lines.append("=" * 55)
        seen = set()
        for link in all_file_links:
            if not link:
                continue
            key = f"{link.get('shared_column', '')}_{link.get('to_file', '')}"
            if key in seen:
                continue
            seen.add(key)
            lines.append(f"  pd.merge(on='{link['shared_column']}')  → {link.get('to_file', '?')}")

    if all_db_links:
        lines.append("\n\nCROSS-SOURCE LINKS (file data ↔ database)")
        lines.append("=" * 55)
        for link in all_db_links:
            if not link:
                continue
            lines.append(
                f"  file.{link.get('file_column', '?')} ↔ {link.get('table_name', '?')}.{link.get('table_column', '?')}"
            )

    return "\n".join(lines)
