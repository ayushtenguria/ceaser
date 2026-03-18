"""Multi-DB Schema Loader — loads and merges schemas from all active connections.

Builds a unified schema context that labels each table with its source database,
so the LLM knows which tables live where.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import DatabaseConnection
from app.services.schema import introspect_schema, format_schema_for_llm, SchemaInfo

logger = logging.getLogger(__name__)


@dataclass
class DbSchemaEntry:
    """Schema for one data source (database OR file)."""
    connection_id: str
    connection_name: str
    db_type: str                    # "postgresql", "mysql", "sqlite", "excel", "csv"
    source_type: str = "database"   # "database" or "file"
    schema: SchemaInfo | None = None
    schema_text: str = ""
    is_available: bool = True
    error: str | None = None
    table_names: list[str] = field(default_factory=list)
    # File-specific fields
    file_id: str | None = None
    parquet_paths: dict[str, str] = field(default_factory=dict)  # var_name → path
    excel_context: str | None = None


@dataclass
class MultiDbSchema:
    """Combined schema across all connections."""
    entries: list[DbSchemaEntry] = field(default_factory=list)
    combined_context: str = ""
    total_tables: int = 0
    total_connections: int = 0

    def get_connection_for_table(self, table_name: str) -> str | None:
        """Find which connection owns a given table."""
        table_lower = table_name.lower()
        for entry in self.entries:
            if table_lower in [t.lower() for t in entry.table_names]:
                return entry.connection_id
        return None

    def get_available_connections(self) -> list[DbSchemaEntry]:
        """Return only connections that loaded successfully."""
        return [e for e in self.entries if e.is_available]


async def load_all_schemas(
    connection_ids: list[str],
    db: AsyncSession,
    file_ids: list[str] | None = None,
    timeout_per_db: int = 15,
) -> MultiDbSchema:
    """Load schemas from multiple database connections in parallel.

    Handles partial failures — if one DB is down, others still load.
    Each DB gets its own timeout.
    """
    import uuid

    result = MultiDbSchema()

    if not connection_ids:
        return result

    # Load connection records
    connections: list[DatabaseConnection] = []
    for cid in connection_ids:
        try:
            stmt = select(DatabaseConnection).where(DatabaseConnection.id == uuid.UUID(cid))
            res = await db.execute(stmt)
            conn = res.scalar_one_or_none()
            if conn:
                connections.append(conn)
        except Exception as exc:
            logger.warning("Failed to load connection %s: %s", cid, exc)

    if not connections:
        return result

    # Load schemas in parallel with per-DB timeout
    async def _load_one(conn: DatabaseConnection) -> DbSchemaEntry:
        entry = DbSchemaEntry(
            connection_id=str(conn.id),
            connection_name=conn.name,
            db_type=conn.db_type,
        )
        try:
            # Use cached schema if available
            if conn.schema_cache:
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
                entry.schema = SchemaInfo(tables=tables)
            else:
                # Introspect with timeout
                entry.schema = await asyncio.wait_for(
                    introspect_schema(conn),
                    timeout=timeout_per_db,
                )

            if entry.schema:
                entry.table_names = [t.name for t in entry.schema.tables]
                entry.schema_text = format_schema_for_llm(entry.schema)
                entry.is_available = True

        except asyncio.TimeoutError:
            entry.is_available = False
            entry.error = f"Connection timed out after {timeout_per_db}s"
            logger.warning("Schema load timeout for %s", conn.name)
        except Exception as exc:
            entry.is_available = False
            entry.error = str(exc)
            logger.warning("Schema load failed for %s: %s", conn.name, exc)

        return entry

    # Execute all DB loads in parallel
    entries = await asyncio.gather(
        *[_load_one(conn) for conn in connections],
        return_exceptions=False,
    )
    result.entries = list(entries)

    # Load file sources
    if file_ids:
        from app.db.models import FileUpload
        for fid in file_ids:
            try:
                stmt = select(FileUpload).where(FileUpload.id == uuid.UUID(fid))
                res = await db.execute(stmt)
                upload = res.scalar_one_or_none()
                if upload and upload.excel_context:
                    file_entry = DbSchemaEntry(
                        connection_id=fid,
                        connection_name=upload.filename,
                        db_type=upload.file_type or "excel",
                        source_type="file",
                        file_id=fid,
                        parquet_paths=upload.parquet_paths or {},
                        excel_context=upload.excel_context,
                        is_available=True,
                        table_names=[f"df_{k}" for k in (upload.parquet_paths or {}).keys()],
                    )
                    result.entries.append(file_entry)
            except Exception as exc:
                logger.warning("Failed to load file %s: %s", fid, exc)

    result.total_connections = len([e for e in result.entries if e.is_available])
    result.total_tables = sum(len(e.table_names) for e in result.entries if e.is_available)

    # Build combined context
    result.combined_context = _build_combined_context(result.entries)

    logger.info(
        "Loaded %d sources (%d DB + %d files), %d total tables",
        result.total_connections, len(connection_ids), len(file_ids or []), result.total_tables,
    )
    return result


def _build_combined_context(entries: list[DbSchemaEntry]) -> str:
    """Build a unified schema context with DB and file labels."""
    db_count = sum(1 for e in entries if e.is_available and e.source_type == "database")
    file_count = sum(1 for e in entries if e.is_available and e.source_type == "file")

    lines: list[str] = [
        "MULTI-SOURCE DATA SCHEMA",
        "=" * 50,
        f"Connected sources: {db_count} databases, {file_count} files",
        "",
    ]

    for entry in entries:
        if not entry.is_available:
            lines.append(f"\nSOURCE: {entry.connection_name} ({entry.db_type}) — UNAVAILABLE")
            lines.append(f"  Error: {entry.error}")
            continue

        # File sources — use their excel_context directly
        if entry.source_type == "file":
            lines.append(f"\nFILE: {entry.connection_name} ({entry.db_type})")
            lines.append(f"Source ID: {entry.connection_id}")
            lines.append(f"Query method: Python/pandas (NOT SQL)")
            lines.append("-" * 40)
            if entry.excel_context:
                lines.append(entry.excel_context[:2000])
            continue

        # Database sources
        lines.append(f"\nDATABASE: {entry.connection_name} ({entry.db_type})")
        lines.append(f"Connection ID: {entry.connection_id}")
        lines.append(f"Query method: SQL")
        lines.append("-" * 40)

        if entry.schema:
            for table in entry.schema.tables:
                row_info = f"  (~{table.row_count:,} rows)" if table.row_count else ""
                lines.append(f"\n  Table: {table.name}{row_info}")
                for col in table.columns:
                    parts = [f"    {col.name}: {col.data_type}"]
                    if col.primary_key:
                        parts.append("[PK]")
                    if col.foreign_key:
                        parts.append(f"[FK -> {col.foreign_key}]")
                    if col.sample_values:
                        vals = ", ".join(f"'{v}'" for v in col.sample_values[:5])
                        parts.append(f"  values: [{vals}]")
                    lines.append(" ".join(parts))

    # Cross-DB relationship hints
    lines.append("\n\nCROSS-DATABASE JOIN HINTS:")
    lines.append("=" * 50)
    lines.append("When joining across databases, Ceaser will:")
    lines.append("  1. Run separate queries per database")
    lines.append("  2. Merge results in memory using pandas")
    lines.append("  3. Match columns by name (e.g., user_id in DB-A = user_id in DB-B)")
    lines.append("For cross-DB queries, use the format: database_name.table_name.column_name")

    return "\n".join(lines)
