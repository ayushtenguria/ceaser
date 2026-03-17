"""SQLite connector using aiosqlite."""

from __future__ import annotations

import logging
from typing import Any

import aiosqlite

from app.connectors.base import BaseConnector
from app.db.models import DatabaseConnection

logger = logging.getLogger(__name__)


class SQLiteConnector(BaseConnector):
    """Async SQLite connector backed by ``aiosqlite``."""

    def __init__(self, connection: DatabaseConnection) -> None:
        self._meta = connection
        self._db_path = connection.database  # file path for SQLite
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> bool:
        try:
            self._conn = await aiosqlite.connect(self._db_path)
            self._conn.row_factory = aiosqlite.Row
            logger.info("Connected to SQLite: %s", self._db_path)
            return True
        except Exception as exc:
            logger.error("SQLite connection failed: %s", exc)
            raise

    async def execute_query(self, query: str) -> tuple[list[str], list[dict[str, Any]]]:
        if self._conn is None:
            await self.connect()
        assert self._conn is not None

        # HARD BLOCK: reject non-SELECT at connector level
        stripped = query.strip().upper()
        if not stripped.startswith("SELECT") and not stripped.startswith("WITH"):
            raise PermissionError(
                f"Only SELECT/WITH queries are allowed. Received: {stripped[:30]}"
            )

        async with self._conn.execute(query) as cursor:
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            raw_rows = await cursor.fetchall()
            rows = [dict(row) for row in raw_rows]

            for row in rows:
                for key, value in row.items():
                    if not isinstance(value, (str, int, float, bool, type(None), list, dict)):
                        row[key] = str(value)

        return columns, rows

    async def get_schema(self) -> dict[str, Any]:
        if self._conn is None:
            await self.connect()
        assert self._conn is not None

        tables: dict[str, list[dict[str, Any]]] = {}

        async with self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
        ) as cursor:
            table_rows = await cursor.fetchall()

        for table_row in table_rows:
            table_name = table_row[0] if isinstance(table_row, tuple) else table_row["name"]
            async with self._conn.execute(f'PRAGMA table_info("{table_name}")') as cursor:
                col_rows = await cursor.fetchall()

            cols: list[dict[str, Any]] = []
            for col in col_rows:
                if isinstance(col, tuple):
                    cols.append({"name": col[1], "type": col[2], "nullable": not col[3]})
                else:
                    cols.append(
                        {"name": col["name"], "type": col["type"], "nullable": not col["notnull"]}
                    )
            tables[table_name] = cols

        return {"tables": tables}

    async def disconnect(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
            logger.info("Disconnected from SQLite: %s", self._db_path)

    def get_connection_string(self) -> str:
        return f"sqlite:///{self._db_path}"
