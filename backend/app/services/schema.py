"""Introspect a client database and produce schema info for the LLM."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import create_engine, inspect, text

from app.connectors.factory import get_connector
from app.db.models import DatabaseConnection
from app.services.encryption import decrypt_value

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema data structures
# ---------------------------------------------------------------------------

@dataclass
class ColumnInfo:
    """Metadata for a single column."""

    name: str
    data_type: str
    nullable: bool
    primary_key: bool = False
    foreign_key: str | None = None
    sample_values: list[str] = field(default_factory=list)  # distinct values for low-cardinality columns


@dataclass
class TableInfo:
    """Metadata for a single table."""

    name: str
    columns: list[ColumnInfo] = field(default_factory=list)
    row_count: int | None = None


@dataclass
class SchemaInfo:
    """Full schema representation for one database."""

    tables: list[TableInfo] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------

def _build_sync_url(conn: DatabaseConnection) -> str:
    """Build a synchronous SQLAlchemy connection URL for introspection."""
    password = decrypt_value(conn.encrypted_password)
    db_type = conn.db_type.lower()

    if db_type == "postgresql":
        return f"postgresql+psycopg2://{conn.username}:{password}@{conn.host}:{conn.port}/{conn.database}"
    if db_type == "mysql":
        return f"mysql+pymysql://{conn.username}:{password}@{conn.host}:{conn.port}/{conn.database}"
    if db_type == "sqlite":
        return f"sqlite:///{conn.database}"

    raise ValueError(f"Unsupported db_type for introspection: {db_type}")


def _introspect_schema_sync(connection: DatabaseConnection) -> SchemaInfo:
    """Synchronous introspection — meant to be called via ``asyncio.to_thread``."""
    url = _build_sync_url(connection)
    sync_engine = create_engine(url, pool_pre_ping=True)

    schema = SchemaInfo()

    try:
        inspector = inspect(sync_engine)
        table_names = inspector.get_table_names()

        # Build a set of PKs / FKs per table for quick lookup.
        for table_name in table_names:
            pk_cols = {c for c in inspector.get_pk_constraint(table_name).get("constrained_columns", [])}
            fk_map: dict[str, str] = {}
            for fk in inspector.get_foreign_keys(table_name):
                for col in fk.get("constrained_columns", []):
                    referred = f"{fk['referred_table']}.{fk['referred_columns'][0]}" if fk.get("referred_columns") else fk["referred_table"]
                    fk_map[col] = referred

            columns: list[ColumnInfo] = []
            for col in inspector.get_columns(table_name):
                columns.append(
                    ColumnInfo(
                        name=col["name"],
                        data_type=str(col["type"]),
                        nullable=col.get("nullable", True),
                        primary_key=col["name"] in pk_cols,
                        foreign_key=fk_map.get(col["name"]),
                    )
                )

            # Approximate row count (fast, non-locking).
            row_count: int | None = None
            try:
                with sync_engine.connect() as conn:
                    result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
                    row_count = result.scalar()
            except Exception:
                logger.debug("Could not get row count for %s", table_name)

            # Fetch distinct values for low-cardinality string/categorical columns.
            for col_info in columns:
                col_name = col_info.name
                dtype_lower = col_info.data_type.lower()

                # Only for string-like and boolean columns
                if not any(t in dtype_lower for t in ("char", "text", "varchar", "bool", "enum")):
                    continue

                try:
                    with sync_engine.connect() as conn_:
                        # Check cardinality first (fast)
                        count_result = conn_.execute(
                            text(f'SELECT COUNT(DISTINCT "{col_name}") FROM "{table_name}"')
                        )
                        distinct_count = count_result.scalar()

                        if distinct_count is not None and distinct_count <= 25:
                            values_result = conn_.execute(
                                text(f'SELECT DISTINCT "{col_name}" FROM "{table_name}" WHERE "{col_name}" IS NOT NULL ORDER BY "{col_name}" LIMIT 25')
                            )
                            col_info.sample_values = [str(row[0]) for row in values_result]
                except Exception:
                    logger.debug("Could not get distinct values for %s.%s", table_name, col_name)

            schema.tables.append(
                TableInfo(name=table_name, columns=columns, row_count=row_count)
            )
    finally:
        sync_engine.dispose()

    return schema


async def introspect_schema(connection: DatabaseConnection) -> SchemaInfo:
    """Connect to the client's DB and read table / column / row-count metadata.

    This uses a *synchronous* SQLAlchemy engine because ``inspect()`` does not
    support async.  The blocking work is offloaded to a thread so it does not
    stall the event loop.
    """
    return await asyncio.to_thread(_introspect_schema_sync, connection)


# ---------------------------------------------------------------------------
# LLM formatting
# ---------------------------------------------------------------------------

def format_schema_for_llm(schema: SchemaInfo) -> str:
    """Return a human-readable text representation of the schema.

    The output is designed to fit inside an LLM system prompt so the model
    knows the available tables and columns.
    """
    if not schema.tables:
        return "No tables found in the connected database."

    lines: list[str] = ["DATABASE SCHEMA", "=" * 50]

    for table in schema.tables:
        row_info = f"  (~{table.row_count:,} rows)" if table.row_count is not None else ""
        lines.append(f"\nTable: {table.name}{row_info}")
        lines.append("-" * 40)

        for col in table.columns:
            parts: list[str] = [f"  {col.name}: {col.data_type}"]
            if col.primary_key:
                parts.append("[PK]")
            if col.foreign_key:
                parts.append(f"[FK -> {col.foreign_key}]")
            if not col.nullable:
                parts.append("[NOT NULL]")
            if col.sample_values:
                vals = ", ".join(f"'{v}'" for v in col.sample_values[:15])
                parts.append(f"  values: [{vals}]")
                # Flag columns that appear to have inconsistent values
                # (case variations, similar strings that might be typos)
                str_vals = [str(v).lower().strip() for v in col.sample_values if v]
                unique_lower = set(str_vals)
                if len(unique_lower) < len(col.sample_values) and col.data_type.upper() in (
                    "VARCHAR", "TEXT", "STRING", "CHARACTER VARYING",
                    "VARCHAR(50)", "VARCHAR(100)", "VARCHAR(200)", "VARCHAR(255)",
                ):
                    parts.append("[DIRTY DATA - use LOWER(TRIM()) for comparison]")
            lines.append(" ".join(parts))

    # Relationships section
    relationships: list[str] = []
    for table in schema.tables:
        for col in table.columns:
            if col.foreign_key:
                relationships.append(f"  {table.name}.{col.name} -> {col.foreign_key}")

    if relationships:
        lines.append("\n\nRELATIONSHIPS (JOIN hints)")
        lines.append("=" * 50)
        for rel in relationships:
            lines.append(rel)

    return "\n".join(lines)
