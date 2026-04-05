"""PostgreSQL connector using asyncpg with connection pooling."""

from __future__ import annotations

import logging
from typing import Any

import asyncpg

from app.connectors.base import BaseConnector
from app.db.models import DatabaseConnection
from app.services.encryption import decrypt_value

logger = logging.getLogger(__name__)

_QUERY_TIMEOUT_S = 60
_MAX_ROWS = 5000
_POOL_MIN = 1
_POOL_MAX = 5


class PostgresConnector(BaseConnector):
    """Async PostgreSQL connector backed by an ``asyncpg`` connection pool."""

    def __init__(self, connection: DatabaseConnection) -> None:
        self._meta = connection
        self._password = decrypt_value(connection.encrypted_password)
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> bool:
        """Create an asyncpg connection pool to the target PostgreSQL database."""
        try:
            async def _init_conn(conn: asyncpg.Connection) -> None:
                await conn.execute(f"SET statement_timeout = '{_QUERY_TIMEOUT_S * 1000}'")

            self._pool = await asyncpg.create_pool(
                host=self._meta.host,
                port=self._meta.port,
                user=self._meta.username,
                password=self._password,
                database=self._meta.database,
                min_size=_POOL_MIN,
                max_size=_POOL_MAX,
                timeout=10,
                command_timeout=_QUERY_TIMEOUT_S,
                init=_init_conn,
            )
            logger.info("Connected to PostgreSQL (pool %d-%d): %s",
                        _POOL_MIN, _POOL_MAX, self._meta.name)
            return True
        except Exception as exc:
            logger.error("PostgreSQL connection failed: %s", exc)
            raise

    async def _execute_query_impl(self, query: str) -> tuple[list[str], list[dict[str, Any]]]:
        """Execute a read-only query and return ``(columns, rows)``."""
        if self._pool is None:
            await self.connect()
        if self._pool is None:
            raise RuntimeError("Failed to establish database connection.")

        stripped = query.strip().upper()
        if not stripped.startswith("SELECT") and not stripped.startswith("WITH"):
            raise PermissionError(
                f"Only SELECT/WITH queries are allowed. Received: {stripped[:30]}"
            )

        async with self._pool.acquire() as conn:
            async with conn.transaction(readonly=True):
                stmt = await conn.prepare(query)
                columns = [attr.name for attr in stmt.get_attributes()]
                records = await stmt.fetch(_MAX_ROWS)

        rows = [dict(record) for record in records]
        for row in rows:
            for key, value in row.items():
                if not isinstance(value, (str, int, float, bool, type(None), list, dict)):
                    row[key] = str(value)

        return columns, rows

    async def get_schema(self) -> dict[str, Any]:
        """Introspect tables and columns via information_schema."""
        if self._pool is None:
            await self.connect()
        if self._pool is None:
            raise RuntimeError("Failed to establish database connection.")

        query = """
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position;
        """
        async with self._pool.acquire() as conn:
            records = await conn.fetch(query)

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
        """Close the connection pool."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            logger.info("Disconnected from PostgreSQL: %s", self._meta.name)

    def get_connection_string(self) -> str:
        """Return a masked connection string."""
        return (
            f"postgresql://{self._meta.username}:***@"
            f"{self._meta.host}:{self._meta.port}/{self._meta.database}"
        )
