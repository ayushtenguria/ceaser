"""Abstract base class for database connectors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseConnector(ABC):
    """Contract that every database connector must implement.

    Each connector wraps a single client-database connection and exposes
    a small surface: connect, execute read-only queries, introspect schema,
    and disconnect.
    """

    @abstractmethod
    async def connect(self) -> bool:
        """Establish a connection.  Returns ``True`` on success."""
        ...

    @abstractmethod
    async def execute_query(self, query: str) -> tuple[list[str], list[dict[str, Any]]]:
        """Execute a read-only SQL query.

        Returns
        -------
        tuple[list[str], list[dict]]:
            A tuple of ``(column_names, rows)`` where each row is a dict
            mapping column name to value.
        """
        ...

    @abstractmethod
    async def get_schema(self) -> dict[str, Any]:
        """Return a JSON-serialisable dict describing tables and columns."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Close the underlying connection / pool."""
        ...

    @abstractmethod
    def get_connection_string(self) -> str:
        """Return the connection string (password masked)."""
        ...
