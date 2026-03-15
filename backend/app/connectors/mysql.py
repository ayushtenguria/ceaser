"""MySQL connector using PyMySQL (sync, executed in a thread pool)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pymysql

from app.connectors.base import BaseConnector
from app.db.models import DatabaseConnection
from app.services.encryption import decrypt_value

logger = logging.getLogger(__name__)


class MySQLConnector(BaseConnector):
    """Sync PyMySQL connector wrapped in ``asyncio.to_thread`` for async compat."""

    def __init__(self, connection: DatabaseConnection) -> None:
        self._meta = connection
        self._password = decrypt_value(connection.encrypted_password)
        self._conn: pymysql.connections.Connection | None = None

    # ------------------------------------------------------------------

    def _connect_sync(self) -> pymysql.connections.Connection:
        return pymysql.connect(
            host=self._meta.host,
            port=self._meta.port,
            user=self._meta.username,
            password=self._password,
            database=self._meta.database,
            connect_timeout=10,
            cursorclass=pymysql.cursors.DictCursor,
        )

    async def connect(self) -> bool:
        try:
            self._conn = await asyncio.to_thread(self._connect_sync)
            logger.info("Connected to MySQL: %s", self._meta.name)
            return True
        except Exception as exc:
            logger.error("MySQL connection failed: %s", exc)
            raise

    async def execute_query(self, query: str) -> tuple[list[str], list[dict[str, Any]]]:
        if self._conn is None:
            await self.connect()
        assert self._conn is not None

        def _run() -> tuple[list[str], list[dict[str, Any]]]:
            with self._conn.cursor() as cursor:  # type: ignore[union-attr]
                cursor.execute(query)
                rows: list[dict[str, Any]] = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []

                # Serialise non-native types.
                for row in rows:
                    for key, value in row.items():
                        if not isinstance(value, (str, int, float, bool, type(None), list, dict)):
                            row[key] = str(value)
                return columns, rows

        return await asyncio.to_thread(_run)

    async def get_schema(self) -> dict[str, Any]:
        if self._conn is None:
            await self.connect()
        assert self._conn is not None

        def _introspect() -> dict[str, Any]:
            with self._conn.cursor() as cursor:  # type: ignore[union-attr]
                cursor.execute(
                    """
                    SELECT table_name, column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s
                    ORDER BY table_name, ordinal_position;
                    """,
                    (self._meta.database,),
                )
                records = cursor.fetchall()

            tables: dict[str, list[dict[str, Any]]] = {}
            for rec in records:
                table = rec["TABLE_NAME"]
                tables.setdefault(table, []).append(
                    {
                        "name": rec["COLUMN_NAME"],
                        "type": rec["DATA_TYPE"],
                        "nullable": rec["IS_NULLABLE"] == "YES",
                    }
                )
            return {"tables": tables}

        return await asyncio.to_thread(_introspect)

    async def disconnect(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            logger.info("Disconnected from MySQL: %s", self._meta.name)

    def get_connection_string(self) -> str:
        return (
            f"mysql://{self._meta.username}:***@"
            f"{self._meta.host}:{self._meta.port}/{self._meta.database}"
        )
