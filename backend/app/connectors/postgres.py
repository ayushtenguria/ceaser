"""PostgreSQL connector using asyncpg."""

from __future__ import annotations

import logging
from typing import Any

import asyncpg

from app.connectors.base import BaseConnector
from app.db.models import DatabaseConnection
from app.services.encryption import decrypt_value

logger = logging.getLogger(__name__)


class PostgresConnector(BaseConnector):
    """Async PostgreSQL connector backed by a single ``asyncpg`` connection."""

    def __init__(self, connection: DatabaseConnection) -> None:
        self._meta = connection
        self._password = decrypt_value(connection.encrypted_password)
        self._conn: asyncpg.Connection | None = None

    # ------------------------------------------------------------------
    # BaseConnector interface
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        """Open an asyncpg connection to the target PostgreSQL database."""
        try:
            self._conn = await asyncpg.connect(
                host=self._meta.host,
                port=self._meta.port,
                user=self._meta.username,
                password=self._password,
                database=self._meta.database,
                timeout=10,
            )
            logger.info("Connected to PostgreSQL: %s", self._meta.name)
            return True
        except Exception as exc:
            logger.error("PostgreSQL connection failed: %s", exc)
            raise

    async def execute_query(self, query: str) -> tuple[list[str], list[dict[str, Any]]]:
        """Execute a read-only query and return ``(columns, rows)``."""
        if self._conn is None:
            await self.connect()
        assert self._conn is not None

        # HARD BLOCK: reject non-SELECT at connector level
        stripped = query.strip().upper()
        if not stripped.startswith("SELECT") and not stripped.startswith("WITH"):
            raise PermissionError(
                f"Only SELECT/WITH queries are allowed. Received: {stripped[:30]}"
            )

        # Execute in a read-only transaction for extra safety
        async with self._conn.transaction(readonly=True):
            stmt = await self._conn.prepare(query)
            columns = [attr.name for attr in stmt.get_attributes()]
            records = await stmt.fetch()

        rows = [dict(record) for record in records]
        # Convert non-serialisable types to strings.
        for row in rows:
            for key, value in row.items():
                if not isinstance(value, (str, int, float, bool, type(None), list, dict)):
                    row[key] = str(value)

        return columns, rows

    async def get_schema(self) -> dict[str, Any]:
        """Introspect tables and columns via information_schema."""
        if self._conn is None:
            await self.connect()
        assert self._conn is not None

        query = """
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position;
        """
        records = await self._conn.fetch(query)

        tables: dict[str, list[dict[str, Any]]] = {}
        for rec in records:
            table = rec["table_name"]
            tables.setdefault(table, []).append(
                {
                    "name": rec["column_name"],
                    "type": rec["data_type"],
                    "nullable": rec["is_nullable"] == "YES",
                }
            )
        return {"tables": tables}

    async def disconnect(self) -> None:
        """Close the asyncpg connection."""
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
            logger.info("Disconnected from PostgreSQL: %s", self._meta.name)

    def get_connection_string(self) -> str:
        """Return a masked connection string."""
        return (
            f"postgresql://{self._meta.username}:***@"
            f"{self._meta.host}:{self._meta.port}/{self._meta.database}"
        )
