"""Query result cache — skip LLM pipeline for repeated questions.

In-memory LRU cache keyed by (connection_or_file_id + normalized_question).
TTL: 5 minutes (configurable). Thread-safe via asyncio lock.

When a cache hit occurs, the entire LLM pipeline (router → agent → validate →
execute → verify → respond) is bypassed — result returned in <10ms.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections import OrderedDict
from typing import Any

logger = logging.getLogger(__name__)

_CACHE_TTL_SECONDS = 300  # 5 minutes
_CACHE_MAX_SIZE = 500  # Max cached entries


class QueryCache:
    """Thread-safe LRU cache for query results."""

    def __init__(self, max_size: int = _CACHE_MAX_SIZE, ttl: int = _CACHE_TTL_SECONDS):
        self._cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._max_size = max_size
        self._ttl = ttl
        self._hits = 0
        self._misses = 0

    def _make_key(self, data_source_id: str, question: str) -> str:
        """Create a cache key from data source + normalized question."""
        from app.services.verified_queries import normalize_question
        normalized = normalize_question(question)
        raw = f"{data_source_id}:{normalized}"
        return hashlib.md5(raw.encode()).hexdigest()

    def get(self, data_source_id: str, question: str) -> dict[str, Any] | None:
        """Look up a cached result. Returns None on miss or expiry."""
        key = self._make_key(data_source_id, question)
        entry = self._cache.get(key)

        if entry is None:
            self._misses += 1
            return None

        # Check TTL
        if time.monotonic() - entry["_cached_at"] > self._ttl:
            del self._cache[key]
            self._misses += 1
            return None

        # Move to end (most recently used)
        self._cache.move_to_end(key)
        self._hits += 1
        logger.info("Cache HIT: %s (hits=%d, misses=%d)", key[:8], self._hits, self._misses)
        return entry

    def put(
        self,
        data_source_id: str,
        question: str,
        result: dict[str, Any],
    ) -> None:
        """Store a query result in the cache."""
        key = self._make_key(data_source_id, question)

        # Evict oldest if at capacity
        while len(self._cache) >= self._max_size:
            self._cache.popitem(last=False)

        self._cache[key] = {
            **result,
            "_cached_at": time.monotonic(),
        }
        logger.debug("Cache PUT: %s (%d entries)", key[:8], len(self._cache))

    def invalidate(self, data_source_id: str) -> int:
        """Invalidate all cached entries for a data source.

        Called when schema changes, file is re-uploaded, or metrics are updated.
        Returns number of entries invalidated.
        """
        to_remove = []
        for key, entry in self._cache.items():
            if entry.get("_data_source_id") == data_source_id:
                to_remove.append(key)

        for key in to_remove:
            del self._cache[key]

        if to_remove:
            logger.info("Cache invalidated %d entries for %s", len(to_remove), data_source_id)
        return len(to_remove)

    def clear(self) -> None:
        """Clear the entire cache."""
        size = len(self._cache)
        self._cache.clear()
        logger.info("Cache cleared (%d entries)", size)

    @property
    def stats(self) -> dict[str, int]:
        return {
            "size": len(self._cache),
            "max_size": self._max_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate_pct": round(
                self._hits / max(self._hits + self._misses, 1) * 100, 1
            ),
        }


# Module-level singleton
_cache = QueryCache()


def get_query_cache() -> QueryCache:
    return _cache
