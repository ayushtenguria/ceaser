"""Gene annotation service — ID mapping and metadata lookup.

Uses a local SQLite database for fast, offline gene ID mapping between
Ensembl, HGNC Symbol, and Entrez ID systems. Falls back to in-memory
heuristics when the SQLite is not available.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).parent / "data"
_DEFAULT_DB = _DATA_DIR / "gene_annotations.sqlite"

# Well-known human gene symbols for fallback detection
_HUMAN_MARKER_GENES = frozenset({
    "TP53", "BRCA1", "BRCA2", "EGFR", "MYC", "KRAS", "PIK3CA", "PTEN",
    "AKT1", "RB1", "CDH1", "VHL", "APC", "BRAF", "ERBB2", "NRAS",
    "GAPDH", "ACTB", "B2M", "HPRT1", "RPLP0", "TBP", "UBC",
    "CD4", "CD8A", "CD3E", "FOXP3", "IL2", "TNF", "IFNG",
})

_MOUSE_MARKER_GENES = frozenset({
    "Trp53", "Brca1", "Gapdh", "Actb", "B2m", "Hprt", "Cd4", "Cd8a",
})


class GeneAnnotationService:
    """Gene ID mapping and annotation lookup.

    Tries SQLite first, falls back to regex-based detection.
    """

    def __init__(self, db_path: str | Path | None = None):
        self._db_path = Path(db_path) if db_path else _DEFAULT_DB
        self._conn: sqlite3.Connection | None = None
        self._available = False

        if self._db_path.exists():
            try:
                self._conn = sqlite3.connect(str(self._db_path))
                self._conn.row_factory = sqlite3.Row
                self._available = True
                logger.info("Gene annotation DB loaded: %s", self._db_path)
            except Exception as exc:
                logger.warning("Could not load gene annotation DB: %s", exc)

    @property
    def available(self) -> bool:
        return self._available

    def map_ids(
        self,
        gene_ids: list[str],
        from_type: str,
        to_type: str,
        organism: str = "human",
    ) -> dict[str, str]:
        """Map gene IDs between systems.

        from_type / to_type: 'ensembl', 'symbol', 'entrez'
        Returns: {input_id: mapped_id}
        """
        if not self._available or not gene_ids:
            return {}

        col_map = {
            "ensembl": "ensembl_id",
            "symbol": "symbol",
            "entrez": "entrez_id",
        }

        from_col = col_map.get(from_type)
        to_col = col_map.get(to_type)
        if not from_col or not to_col:
            return {}

        result: dict[str, str] = {}
        # Query in batches of 500
        for i in range(0, len(gene_ids), 500):
            batch = gene_ids[i:i + 500]
            placeholders = ",".join("?" * len(batch))
            query = (
                f"SELECT {from_col}, {to_col} FROM genes "
                f"WHERE {from_col} IN ({placeholders}) AND organism = ?"
            )
            try:
                cursor = self._conn.execute(query, batch + [organism])  # type: ignore[union-attr]
                for row in cursor:
                    if row[1]:
                        result[str(row[0])] = str(row[1])
            except Exception as exc:
                logger.warning("Gene mapping query failed: %s", exc)

        return result

    def annotate(
        self,
        gene_ids: list[str],
        organism: str = "human",
    ) -> dict[str, dict[str, Any]]:
        """Get full annotation for gene IDs (auto-detects ID type).

        Returns: {gene_id: {symbol, description, chromosome, biotype}}
        """
        if not self._available or not gene_ids:
            return {}

        id_type = self.detect_id_type(gene_ids)
        if not id_type:
            return {}

        col_map = {"ensembl": "ensembl_id", "symbol": "symbol", "entrez": "entrez_id"}
        id_col = col_map.get(id_type, "symbol")

        result: dict[str, dict[str, Any]] = {}
        for i in range(0, len(gene_ids), 500):
            batch = gene_ids[i:i + 500]
            placeholders = ",".join("?" * len(batch))
            query = (
                f"SELECT {id_col}, symbol, description, chromosome, biotype "
                f"FROM genes WHERE {id_col} IN ({placeholders}) AND organism = ?"
            )
            try:
                cursor = self._conn.execute(query, batch + [organism])  # type: ignore[union-attr]
                for row in cursor:
                    result[str(row[0])] = {
                        "symbol": row[1],
                        "description": row[2],
                        "chromosome": row[3],
                        "biotype": row[4],
                    }
            except Exception as exc:
                logger.warning("Gene annotation query failed: %s", exc)

        return result

    @staticmethod
    def detect_id_type(gene_ids: list[str]) -> str | None:
        """Detect the ID system used by a list of gene IDs."""
        from app.agents.genomics.detector import detect_gene_id_type
        return detect_gene_id_type(gene_ids)

    @staticmethod
    def detect_organism(gene_ids: list[str]) -> str | None:
        """Detect organism from gene ID patterns."""
        from app.agents.genomics.detector import detect_organism
        return detect_organism(gene_ids)

    def get_symbols_for_ids(
        self,
        gene_ids: list[str],
        organism: str = "human",
    ) -> dict[str, str]:
        """Convenience: map any gene IDs to symbols."""
        id_type = self.detect_id_type(gene_ids)
        if id_type == "symbol":
            return {g: g for g in gene_ids}
        if id_type and id_type != "symbol":
            return self.map_ids(gene_ids, id_type, "symbol", organism)
        return {}

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


# Module-level singleton
_service: GeneAnnotationService | None = None


def get_annotation_service() -> GeneAnnotationService:
    """Return the singleton annotation service."""
    global _service
    if _service is None:
        _service = GeneAnnotationService()
    return _service
