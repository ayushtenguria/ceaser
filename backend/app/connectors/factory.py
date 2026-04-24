"""Factory function to instantiate the correct database connector."""

from __future__ import annotations

from app.connectors.base import BaseConnector
from app.connectors.bigquery import BigQueryConnector
from app.connectors.google_ads import GoogleAdsConnector
from app.connectors.meta_ads import MetaAdsConnector
from app.connectors.mysql import MySQLConnector
from app.connectors.postgres import PostgresConnector
from app.connectors.snowflake import SnowflakeConnector
from app.connectors.sqlite_conn import SQLiteConnector
from app.db.models import DatabaseConnection

_CONNECTOR_MAP: dict[str, type[BaseConnector]] = {
    "postgresql": PostgresConnector,
    "mysql": MySQLConnector,
    "sqlite": SQLiteConnector,
    "bigquery": BigQueryConnector,
    "snowflake": SnowflakeConnector,
    "meta_ads": MetaAdsConnector,
    "google_ads": GoogleAdsConnector,
}


def get_connector(connection: DatabaseConnection) -> BaseConnector:
    """Return a ``BaseConnector`` subclass matching the connection's ``db_type``."""
    db_type = connection.db_type.lower()
    cls = _CONNECTOR_MAP.get(db_type)
    if cls is None:
        supported = ", ".join(sorted(_CONNECTOR_MAP.keys()))
        raise ValueError(f"Unsupported database type '{db_type}'. Supported: {supported}")
    return cls(connection)
