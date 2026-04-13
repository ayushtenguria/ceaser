"""Introspect a client database and produce schema info for the LLM."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from sqlalchemy import create_engine, inspect, text

from app.db.models import DatabaseConnection
from app.services.encryption import decrypt_value

logger = logging.getLogger(__name__)


@dataclass
class ColumnInfo:
    """Metadata for a single column."""

    name: str
    data_type: str
    nullable: bool
    primary_key: bool = False
    foreign_key: str | None = None
    sample_values: list[str] = field(default_factory=list)


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


def _quote_ident(name: str) -> str:
    """Safely quote a SQL identifier (table/column name).

    Prevents SQL injection by escaping embedded double-quotes and wrapping
    in double quotes. Only allows alphanumeric, underscore, and space chars.
    """
    # Reject names with dangerous characters
    import re

    if not re.match(r"^[\w\s.]+$", name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return '"' + name.replace('"', '""') + '"'


def _introspect_schema_sync(connection: DatabaseConnection) -> SchemaInfo:
    """Synchronous introspection — meant to be called via ``asyncio.to_thread``."""
    url = _build_sync_url(connection)
    sync_engine = create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 15} if "postgresql" in url else {},
    )

    schema = SchemaInfo()

    try:
        inspector = inspect(sync_engine)
        table_names = inspector.get_table_names()

        for table_name in table_names:
            pk_cols = {
                c for c in inspector.get_pk_constraint(table_name).get("constrained_columns", [])
            }
            fk_map: dict[str, str] = {}
            for fk in inspector.get_foreign_keys(table_name):
                for col in fk.get("constrained_columns", []):
                    referred = (
                        f"{fk['referred_table']}.{fk['referred_columns'][0]}"
                        if fk.get("referred_columns")
                        else fk["referred_table"]
                    )
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

            row_count: int | None = None
            try:
                safe_table = _quote_ident(table_name)
                with sync_engine.connect() as conn:
                    # Set statement timeout to prevent long-running COUNT(*) on huge tables
                    if "postgresql" in url:
                        conn.execute(text("SET statement_timeout = '10s'"))
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {safe_table}"))
                    row_count = result.scalar()
            except Exception:
                logger.debug("Could not get row count for %s", table_name)

            for col_info in columns:
                col_name = col_info.name
                dtype_lower = col_info.data_type.lower()

                if not any(t in dtype_lower for t in ("char", "text", "varchar", "bool", "enum")):
                    continue

                try:
                    safe_table = _quote_ident(table_name)
                    safe_col = _quote_ident(col_name)
                    with sync_engine.connect() as conn_:
                        if "postgresql" in url:
                            conn_.execute(text("SET statement_timeout = '5s'"))
                        count_result = conn_.execute(
                            text(f"SELECT COUNT(DISTINCT {safe_col}) FROM {safe_table}")
                        )
                        distinct_count = count_result.scalar()

                        if distinct_count is not None and distinct_count <= 25:
                            values_result = conn_.execute(
                                text(
                                    f"SELECT DISTINCT {safe_col} FROM {safe_table} WHERE {safe_col} IS NOT NULL ORDER BY {safe_col} LIMIT 25"
                                )
                            )
                            col_info.sample_values = [str(row[0]) for row in values_result]
                except Exception:
                    logger.debug("Could not get distinct values for %s.%s", table_name, col_name)

            schema.tables.append(TableInfo(name=table_name, columns=columns, row_count=row_count))
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


def _humanize_column_name(name: str) -> str | None:
    """Generate a human-readable alias for a cryptic column name.

    Returns None if the column name is already readable.
    Examples:
        rev_amt_q4_adj  → "Q4 Adjusted Revenue Amount"
        cust_acq_dt_utc → "Customer Acquisition Date (UTC)"
        ord_cnt         → "Order Count"
        is_actv_flg     → "Is Active Flag"
    """
    # Skip already readable names (single word, or clean multi-word)
    if len(name) <= 4 and "_" not in name:
        return None

    # Common abbreviation mapping
    abbrevs = {
        "amt": "amount",
        "qty": "quantity",
        "cnt": "count",
        "num": "number",
        "dt": "date",
        "ts": "timestamp",
        "tm": "time",
        "desc": "description",
        "nm": "name",
        "addr": "address",
        "cust": "customer",
        "usr": "user",
        "emp": "employee",
        "mgr": "manager",
        "org": "organization",
        "dept": "department",
        "div": "division",
        "prod": "product",
        "cat": "category",
        "inv": "invoice",
        "ord": "order",
        "txn": "transaction",
        "pmt": "payment",
        "acct": "account",
        "rev": "revenue",
        "mrr": "MRR",
        "arr": "ARR",
        "flg": "flag",
        "ind": "indicator",
        "sts": "status",
        "st": "status",
        "pct": "percent",
        "avg": "average",
        "tot": "total",
        "max": "maximum",
        "min": "minimum",
        "prev": "previous",
        "cur": "current",
        "ytd": "year-to-date",
        "mtd": "month-to-date",
        "qtd": "quarter-to-date",
        "q1": "Q1",
        "q2": "Q2",
        "q3": "Q3",
        "q4": "Q4",
        "yy": "year",
        "mm": "month",
        "dd": "day",
        "adj": "adjusted",
        "unadj": "unadjusted",
        "calc": "calculated",
        "src": "source",
        "tgt": "target",
        "dst": "destination",
        "actv": "active",
        "inactv": "inactive",
        "del": "deleted",
        "id": "ID",
        "pk": "PK",
        "fk": "FK",
        "utc": "UTC",
        "lbl": "label",
        "grp": "group",
        "lvl": "level",
        "typ": "type",
        "seq": "sequence",
        "ref": "reference",
        "ext": "external",
        "int": "internal",
        "pri": "primary",
        "sec": "secondary",
    }

    parts = name.lower().replace("-", "_").split("_")
    expanded = []
    any_expanded = False

    for part in parts:
        if part in abbrevs:
            expanded.append(abbrevs[part])
            any_expanded = True
        elif part.startswith("is_") or part == "is":
            expanded.append("Is")
            any_expanded = True
        else:
            expanded.append(part)

    if not any_expanded:
        return None

    result = " ".join(expanded).title()
    # Fix known acronyms back to uppercase
    for acr in ("Id", "Pk", "Fk", "Utc", "Mrr", "Arr", "Q1", "Q2", "Q3", "Q4"):
        result = result.replace(acr, acr.upper() if len(acr) <= 3 else acr)

    return result


def format_schema_for_llm(schema: SchemaInfo) -> str:
    """Return a human-readable text representation of the schema.

    The output is designed to fit inside an LLM system prompt so the model
    knows the available tables and columns. Includes auto-generated
    human-readable aliases for cryptic column names.
    """
    if not schema.tables:
        return "No tables found in the connected database."

    lines: list[str] = ["DATABASE SCHEMA", "=" * 50]

    for table in schema.tables:
        row_info = f"  (~{table.row_count:,} rows)" if table.row_count is not None else ""
        table_alias = _humanize_column_name(table.name)
        alias_hint = f"  (≈ {table_alias})" if table_alias else ""
        lines.append(f"\nTable: {table.name}{row_info}{alias_hint}")
        lines.append("-" * 40)

        for col in table.columns:
            parts: list[str] = [f"  {col.name}: {col.data_type}"]
            # Auto-alias for cryptic names
            col_alias = _humanize_column_name(col.name)
            if col_alias:
                parts.append(f"(≈ {col_alias})")
            if col.primary_key:
                parts.append("[PK]")
            if col.foreign_key:
                parts.append(f"[FK -> {col.foreign_key}]")
            if not col.nullable:
                parts.append("[NOT NULL]")
            if col.sample_values:
                vals = ", ".join(f"'{v}'" for v in col.sample_values[:15])
                parts.append(f"  values: [{vals}]")
                str_vals = [str(v).lower().strip() for v in col.sample_values if v]
                unique_lower = set(str_vals)
                if len(unique_lower) < len(col.sample_values) and col.data_type.upper() in (
                    "VARCHAR",
                    "TEXT",
                    "STRING",
                    "CHARACTER VARYING",
                    "VARCHAR(50)",
                    "VARCHAR(100)",
                    "VARCHAR(200)",
                    "VARCHAR(255)",
                ):
                    parts.append("[DIRTY DATA - use LOWER(TRIM()) for comparison]")
            lines.append(" ".join(parts))

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
