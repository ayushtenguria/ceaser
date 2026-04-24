"""BigQuery connector using sqlalchemy-bigquery.

Credentials are stored as encrypted service account JSON in
DatabaseConnection.encrypted_password.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from app.connectors.base import BaseConnector
from app.db.models import DatabaseConnection
from app.services.encryption import decrypt_value

logger = logging.getLogger(__name__)


class BigQueryConnector(BaseConnector):
    """Connector for Google BigQuery."""

    def __init__(self, connection: DatabaseConnection) -> None:
        super().__init__(connection)
        self._engine = None

    async def connect(self) -> bool:
        from google.cloud import bigquery
        from google.oauth2 import service_account

        creds_json = decrypt_value(self._connection.encrypted_password)
        creds_dict = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(creds_dict)

        project_id = (
            self._connection.database.split(".")[0]
            if "." in self._connection.database
            else self._connection.database
        )
        self._client = bigquery.Client(project=project_id, credentials=credentials)

        # Verify connection by listing datasets
        datasets = await asyncio.to_thread(lambda: list(self._client.list_datasets(max_results=1)))
        logger.info("BigQuery connected: project=%s, datasets=%d+", project_id, len(datasets))
        return True

    async def get_schema(self) -> dict[str, Any]:
        parts = self._connection.database.split(".")
        project_id = parts[0]
        dataset_id = parts[1] if len(parts) > 1 else ""

        if not dataset_id:
            datasets = await asyncio.to_thread(
                lambda: list(self._client.list_datasets(max_results=20))
            )
            if datasets:
                dataset_id = datasets[0].dataset_id

        if not dataset_id:
            return {"tables": {}}

        tables_ref = await asyncio.to_thread(
            lambda: list(self._client.list_tables(f"{project_id}.{dataset_id}", max_results=50))
        )

        tables = {}
        for table_ref in tables_ref:
            table = await asyncio.to_thread(lambda t=table_ref: self._client.get_table(t))
            columns = []
            for field in table.schema:
                columns.append(
                    {
                        "name": field.name,
                        "data_type": field.field_type,
                        "nullable": field.mode != "REQUIRED",
                        "primary_key": False,
                        "foreign_key": None,
                    }
                )
            tables[table.table_id] = {
                "columns": columns,
                "row_count": table.num_rows,
            }

        return {"tables": tables}

    async def _execute_query_impl(self, query: str) -> tuple[list[str], list[dict[str, Any]]]:
        if not query.strip().upper().startswith(("SELECT", "WITH")):
            raise ValueError("Only SELECT queries are allowed")

        result = await asyncio.to_thread(lambda: self._client.query(query).result())

        rows = []
        columns = [field.name for field in result.schema]
        for row in result:
            row_dict = {}
            for col in columns:
                val = row[col]
                if hasattr(val, "isoformat"):
                    row_dict[col] = val.isoformat()
                elif hasattr(val, "item"):
                    row_dict[col] = val.item()
                else:
                    row_dict[col] = val
            rows.append(row_dict)

        return columns, rows

    async def disconnect(self) -> None:
        if self._client:
            self._client.close()

    def get_connection_string(self) -> str:
        return f"bigquery://{self._connection.database}"
