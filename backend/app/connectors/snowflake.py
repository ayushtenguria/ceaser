"""Snowflake connector using snowflake-sqlalchemy.

Credentials stored in standard fields:
- host = account URL (abc123.us-east-1.snowflakecomputing.com)
- database = database name
- username = Snowflake username
- encrypted_password = Snowflake password (Fernet encrypted)
- schema_cache = {"warehouse": "...", "schema": "...", "role": "..."} for extra params
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.connectors.base import BaseConnector
from app.db.models import DatabaseConnection
from app.services.encryption import decrypt_value

logger = logging.getLogger(__name__)


class SnowflakeConnector(BaseConnector):
    """Connector for Snowflake Data Cloud."""

    def __init__(self, connection: DatabaseConnection) -> None:
        super().__init__(connection)
        self._conn = None

    async def connect(self) -> bool:
        import snowflake.connector

        password = decrypt_value(self._connection.encrypted_password)
        extra = self._connection.schema_cache or {}

        account = self._connection.host.replace(".snowflakecomputing.com", "")

        connect_params = {
            "account": account,
            "user": self._connection.username,
            "password": password,
            "database": self._connection.database,
            "warehouse": extra.get("warehouse", "COMPUTE_WH"),
            "schema": extra.get("schema", "PUBLIC"),
        }
        role = extra.get("role")
        if role:
            connect_params["role"] = role

        self._conn = await asyncio.to_thread(snowflake.connector.connect, **connect_params)
        logger.info(
            "Snowflake connected: account=%s, database=%s, warehouse=%s",
            account,
            self._connection.database,
            connect_params["warehouse"],
        )
        return True

    async def get_schema(self) -> dict[str, Any]:
        extra = self._connection.schema_cache or {}
        schema_name = extra.get("schema", "PUBLIC")

        query = f"""
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
                   ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema_name}'
            ORDER BY TABLE_NAME, ORDINAL_POSITION
        """

        cursor = await asyncio.to_thread(self._conn.cursor)
        await asyncio.to_thread(cursor.execute, query)
        rows = await asyncio.to_thread(cursor.fetchall)

        tables: dict[str, dict] = {}
        for row in rows:
            table_name = row[0]
            if table_name not in tables:
                tables[table_name] = {"columns": [], "row_count": None}
            tables[table_name]["columns"].append(
                {
                    "name": row[1],
                    "data_type": row[2],
                    "nullable": row[3] == "YES",
                    "primary_key": False,
                    "foreign_key": None,
                }
            )

        return {"tables": tables}

    async def _execute_query_impl(self, query: str) -> tuple[list[str], list[dict[str, Any]]]:
        sql = query.strip()
        if not sql.upper().startswith(("SELECT", "WITH")):
            raise ValueError("Only SELECT queries are allowed")

        cursor = await asyncio.to_thread(self._conn.cursor)
        await asyncio.to_thread(cursor.execute, sql)
        columns = [desc[0] for desc in cursor.description]
        raw_rows = await asyncio.to_thread(cursor.fetchall)

        rows = []
        for raw in raw_rows:
            row = {}
            for i, col in enumerate(columns):
                val = raw[i]
                if hasattr(val, "isoformat"):
                    row[col] = val.isoformat()
                elif hasattr(val, "item"):
                    row[col] = val.item()
                else:
                    row[col] = val
            rows.append(row)

        return columns, rows

    async def disconnect(self) -> None:
        if self._conn:
            await asyncio.to_thread(self._conn.close)

    def get_connection_string(self) -> str:
        account = self._connection.host.replace(".snowflakecomputing.com", "")
        return f"snowflake://{account}/{self._connection.database}"
