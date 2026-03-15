"""Factory function to instantiate the correct database connector."""

from __future__ import annotations

from app.connectors.base import BaseConnector
from app.connectors.mysql import MySQLConnector
from app.connectors.postgres import PostgresConnector
from app.connectors.sqlite_conn import SQLiteConnector
from app.db.models import DatabaseConnection

_CONNECTOR_MAP: dict[str, type[BaseConnector]] = {
    "postgresql": PostgresConnector,
    "mysql": MySQLConnector,
    "sqlite": SQLiteConnector,
}


def get_connector(connection: DatabaseConnection) -> BaseConnector:
    """Return a ``BaseConnector`` subclass matching the connection's ``db_type``.

    Raises ``ValueError`` if the database type is not supported.
    """
    db_type = connection.db_type.lower()
    cls = _CONNECTOR_MAP.get(db_type)

    if cls is None:
        supported = ", ".join(sorted(_CONNECTOR_MAP.keys()))
        raise ValueError(
            f"Unsupported database type '{db_type}'. Supported types: {supported}"
        )

    return cls(connection)
