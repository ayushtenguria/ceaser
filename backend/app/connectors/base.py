"""Abstract base class for database connectors with retry logic."""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)

_MAX_RETRIES = 2
_BASE_DELAY = 0.5  # seconds


class BaseConnector(ABC):
    """Contract that every database connector must implement.

    Each connector wraps a client-database connection/pool and exposes
    a small surface: connect, execute read-only queries, introspect schema,
    and disconnect.

    Includes built-in retry with exponential backoff for transient failures.
    """

    @abstractmethod
    async def connect(self) -> bool:
        """Establish a connection.  Returns ``True`` on success."""
        ...

    @abstractmethod
    async def _execute_query_impl(self, query: str) -> tuple[list[str], list[dict[str, Any]]]:
        """Internal query execution — subclasses implement this."""
        ...

    async def execute_query(self, query: str) -> tuple[list[str], list[dict[str, Any]]]:
        """Execute a read-only SQL query with retry on transient failures.

        Retries up to _MAX_RETRIES times with exponential backoff for
        connection errors and timeouts. Does NOT retry permission errors
        or SQL syntax errors.
        """
        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES + 1):
            try:
                return await self._execute_query_impl(query)
            except (ConnectionError, OSError, TimeoutError) as exc:
                last_exc = exc
                if attempt < _MAX_RETRIES:
                    delay = _BASE_DELAY * (2**attempt)
                    logger.warning(
                        "Query failed (attempt %d/%d), retrying in %.1fs: %s",
                        attempt + 1,
                        _MAX_RETRIES + 1,
                        delay,
                        exc,
                    )
                    await asyncio.sleep(delay)
                    # Force reconnect on connection errors
                    try:
                        await self.disconnect()
                        await self.connect()
                    except Exception:
                        pass
            except (PermissionError, ValueError):
                raise  # Don't retry auth/validation errors
            except Exception as exc:
                # Check if it's a transient error worth retrying
                err_str = str(exc).lower()
                if any(kw in err_str for kw in ("connection", "timeout", "reset", "broken pipe")):
                    last_exc = exc
                    if attempt < _MAX_RETRIES:
                        delay = _BASE_DELAY * (2**attempt)
                        logger.warning(
                            "Transient error (attempt %d), retrying: %s", attempt + 1, exc
                        )
                        await asyncio.sleep(delay)
                        try:
                            await self.disconnect()
                            await self.connect()
                        except Exception:
                            pass
                        continue
                raise  # Non-transient error — don't retry

        raise last_exc or RuntimeError("Query failed after retries")

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
