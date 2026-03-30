"""Graph RAG Schema Layer — Neo4j-backed schema intelligence.

Builds a knowledge graph from database schemas. At query time, traverses the graph
to find only relevant tables + exact JOIN paths. Replaces full-schema dumping.

Usage:
    from app.services.schema_graph import get_graph_driver, build_schema_graph, select_relevant_schema

    # On connection create/refresh:
    await build_schema_graph(connection_id, schema, org_id)

    # On each query:
    context = await select_relevant_schema(question, connection_id, org_id)
"""

from __future__ import annotations

import logging
import re
from typing import Any

from neo4j import AsyncGraphDatabase, AsyncDriver

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_driver: AsyncDriver | None = None


# ---------------------------------------------------------------------------
# Driver management
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Domain classification
# ---------------------------------------------------------------------------

def _classify_domain(col_name: str, col_type: str) -> str:
    """Classify a column into a business domain."""
    name = col_name.lower()
    dtype = col_type.lower()

    if name == "id" or name.endswith("_id") or name.endswith("_key"):
        return "identifier"
    if any(kw in name for kw in ("price", "amount", "revenue", "cost", "total", "spend", "fee", "salary", "budget", "mrr", "arr")):
        return "monetary"
    if "decimal" in dtype or "money" in dtype or "numeric" in dtype:
        return "monetary"
    if any(kw in name for kw in ("city", "state", "country", "region", "zip", "pincode", "address", "location")):
        return "location"
    if any(kw in name for kw in ("date", "time", "created", "updated", "_at", "when", "start", "end", "born", "expires")):
        return "temporal"
    if "timestamp" in dtype or "date" in dtype:
        return "temporal"
    if any(kw in name for kw in ("email", "phone", "mobile")):
        return "contact"
    if any(kw in name for kw in ("status", "type", "category", "level", "tier", "plan", "priority", "stage")):
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
    return any(kw in dtype for kw in ("int", "decimal", "numeric", "float", "double", "real", "money", "serial"))


def _is_temporal(col_type: str) -> bool:
    dtype = col_type.lower()
    return any(kw in dtype for kw in ("timestamp", "date", "time", "interval"))


def _is_categorical(unique_count: int, row_count: int) -> bool:
    if row_count == 0:
        return False
    return unique_count < 50 and (unique_count / max(row_count, 1)) < 0.1


# ---------------------------------------------------------------------------
# Graph builder — runs on connection create/refresh
# ---------------------------------------------------------------------------

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
        # Clear old graph for this connection
        await session.run(
            "MATCH (t:Table {connection_id: $conn_id}) DETACH DELETE t",
            conn_id=connection_id,
        )
        await session.run(
            "MATCH (c:Column {connection_id: $conn_id}) DETACH DELETE c",
            conn_id=connection_id,
        )

        # Create table + column nodes
        for table in tables:
            table_name = table.get("name", "")
            row_count = table.get("row_count", 0)
            columns = table.get("columns", [])

            # Create table node
            await session.run("""
                CREATE (t:Table {
                    name: $name, connection_id: $conn_id, org_id: $org_id,
                    db_type: $db_type, row_count: $rows, column_count: $col_count
                })
            """, name=table_name, conn_id=connection_id, org_id=org_id,
                db_type=db_type, rows=row_count or 0, col_count=len(columns))

            # Create column nodes
            for col in columns:
                col_name = col.get("name", "")
                col_type = col.get("data_type", "unknown")
                samples = col.get("sample_values", [])
                unique_count = len(set(str(s) for s in samples)) if samples else 0

                domain = _classify_domain(col_name, col_type)
                is_pk = col.get("primary_key", False)
                fk = col.get("foreign_key")

                await session.run("""
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
                """, table=table_name, conn_id=connection_id,
                    name=col_name, dtype=col_type, domain=domain,
                    numeric=_is_numeric(col_type), temporal=_is_temporal(col_type),
                    categorical=_is_categorical(unique_count, row_count or 0),
                    pk=is_pk, fk=fk is not None,
                    nullable=col.get("nullable", True),
                    samples=[str(s) for s in (samples or [])[:10]])

                # Create FK relationship
                if fk:
                    parts = fk.split(".")
                    if len(parts) == 2:
                        ref_table, ref_col = parts
                        await session.run("""
                            MATCH (from_t:Table {name: $from_table, connection_id: $conn_id})
                            MATCH (to_t:Table {name: $to_table, connection_id: $conn_id})
                            MERGE (from_t)-[:FK {
                                from_column: $from_col, to_column: $to_col,
                                join_sql: $join_sql
                            }]->(to_t)
                        """, from_table=table_name, to_table=ref_table, conn_id=connection_id,
                            from_col=col_name, to_col=ref_col,
                            join_sql=f"{table_name}.{col_name} = {ref_table}.{ref_col}")

        # Infer relationships from shared column names across tables
        await session.run("""
            MATCH (t1:Table {connection_id: $conn_id})-[:HAS_COLUMN]->(c1:Column)
            MATCH (t2:Table {connection_id: $conn_id})-[:HAS_COLUMN]->(c2:Column)
            WHERE t1 <> t2 AND c1.name = c2.name AND c1.domain = 'identifier'
            AND NOT EXISTS { (t1)-[:FK]-(t2) }
            MERGE (t1)-[:INFERRED_REL {
                shared_column: c1.name, confidence: 0.85, method: 'name_match'
            }]->(t2)
        """, conn_id=connection_id)

        # Cross-domain links (city ↔ shipping_city)
        await session.run("""
            MATCH (c1:Column {connection_id: $conn_id})
            MATCH (c2:Column {connection_id: $conn_id})
            WHERE c1 <> c2 AND c1.domain = c2.domain
            AND c1.domain IN ['location', 'contact']
            AND c1.name <> c2.name
            MERGE (c1)-[:SAME_DOMAIN {domain: c1.domain, similarity: 0.7}]->(c2)
        """, conn_id=connection_id)

    logger.info("Schema graph built: %d tables for connection %s", len(tables), connection_id)
    return len(tables)


# ---------------------------------------------------------------------------
# Entity extraction — no LLM, pure keyword/pattern matching
# ---------------------------------------------------------------------------

TEMPORAL_KEYWORDS = frozenset({
    "monthly", "weekly", "daily", "yearly", "quarterly", "trend", "over time",
    "by month", "by week", "by year", "time series", "growth", "by date",
})
NUMERIC_KEYWORDS = frozenset({
    "revenue", "sales", "amount", "total", "average", "sum", "count",
    "price", "cost", "profit", "margin", "spend", "budget", "salary",
})
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

    # Detect chart type
    for chart_type, keywords in CHART_KEYWORDS.items():
        if any(kw in q for kw in keywords):
            entities["chart_type"] = chart_type
            break

    # Extract meaningful keywords (skip stop words)
    stop_words = {"the", "a", "an", "is", "are", "was", "were", "show", "me", "give",
                  "please", "can", "you", "what", "how", "many", "much", "do", "does",
                  "for", "by", "in", "of", "to", "and", "or", "with", "as", "from"}
    entities["keywords"] = [w for w in words if w not in stop_words and len(w) > 2]

    return entities


# ---------------------------------------------------------------------------
# Graph traversal — query-time schema selection
# ---------------------------------------------------------------------------

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
        score = 0.3  # base score for being connected

        table_name = record.get("table_name", "").lower()
        columns = record.get("columns", [])

        # Table name match
        for kw in entities["keywords"]:
            if kw in table_name:
                score += 0.4

        # Column name match
        for col in columns:
            col_name = col.get("name", "").lower()
            for kw in entities["keywords"]:
                if kw in col_name:
                    score += 0.3

        # Temporal boost
        if entities["needs_temporal"]:
            if any(col.get("temporal") for col in columns):
                score += 0.2

        # Numeric boost
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
        # Estimate tokens: ~20 per column + ~30 for table header + ~20 for joins
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

    # JOIN paths section
    if all_joins:
        lines.append("\n\nJOIN PATHS (use these exact conditions)")
        lines.append("=" * 55)
        for join_sql in set(all_joins):
            lines.append(f"  {join_sql}")

    # Query hints
    hints = []
    if entities.get("chart_type") == "histogram":
        hints.append("HISTOGRAM: Return RAW individual values — do NOT aggregate with CASE/GROUP BY.")
    if entities.get("needs_temporal"):
        hints.append("TEMPORAL: Use DATE_TRUNC('month', col) for monthly grouping.")
    if hints:
        lines.append("\n\nQUERY HINTS")
        lines.append("=" * 55)
        for h in hints:
            lines.append(f"  {h}")

    return "\n".join(lines)
