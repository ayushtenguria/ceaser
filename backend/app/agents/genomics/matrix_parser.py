"""Expression matrix parsing for genomics files.

Handles:
- Count matrices (CSV/TSV): genes × samples, first column = gene IDs
- GCT format (Broad Institute): #1.2 or #1.3 header, description column
- Pre-computed DESeq2 results: baseMean, log2FoldChange, padj columns
- Auto-transposition detection (samples as rows → genes as rows)
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

_MAX_GENES = 100_000
_MAX_SAMPLES = 10_000


def parse_expression_matrix(
    file_path: str,
    file_format: str,
    gene_id_type: str | None = None,
) -> dict[str, Any]:
    """Parse a genomics file into a genes × samples DataFrame.

    Returns dict with:
        matrix: pd.DataFrame (genes as rows, samples as columns)
        gene_ids: list[str]
        sample_names: list[str]
        gene_count: int
        sample_count: int
        sample_metadata: dict | None
        warnings: list[str]
    """
    parsers = {
        "count_matrix": _parse_count_matrix,
        "deseq2_result": _parse_deseq2_result,
        "gct": _parse_gct,
        "generic_csv": _parse_count_matrix,
    }

    parser = parsers.get(file_format, _parse_count_matrix)
    return parser(file_path, gene_id_type)


def _parse_count_matrix(file_path: str, gene_id_type: str | None = None) -> dict[str, Any]:
    """Parse a standard count matrix (genes × samples)."""
    warnings: list[str] = []
    ext = Path(file_path).suffix.lower()
    sep = "\t" if ext in (".tsv", ".tab", ".txt") else ","

    # Detect comment lines
    skip_rows = 0
    with open(file_path, "r", errors="replace") as f:
        for line in f:
            if line.startswith("#"):
                skip_rows += 1
            else:
                break

    df = pd.read_csv(
        file_path,
        sep=sep,
        skiprows=skip_rows,
        index_col=0,
        nrows=_MAX_GENES,
    )

    # Clean index (gene IDs)
    df.index = df.index.astype(str).str.strip().str.strip('"')
    df.index.name = "gene_id"

    # Remove any non-numeric columns (descriptions, etc.)
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    non_numeric = [c for c in df.columns if c not in numeric_cols]
    if non_numeric:
        warnings.append(f"Dropped non-numeric columns: {', '.join(non_numeric[:5])}")
        df = df[numeric_cols]

    # Auto-transpose detection: if rows look like samples (< 100 rows, > 1000 cols)
    if df.shape[0] < 100 and df.shape[1] > 500:
        logger.info("Auto-transposing matrix: %d×%d → %d×%d",
                     df.shape[0], df.shape[1], df.shape[1], df.shape[0])
        df = df.T
        warnings.append("Matrix auto-transposed (detected samples as rows)")

    # Enforce limits
    if df.shape[0] > _MAX_GENES:
        df = df.iloc[:_MAX_GENES]
        warnings.append(f"Truncated to {_MAX_GENES:,} genes")
    if df.shape[1] > _MAX_SAMPLES:
        df = df.iloc[:, :_MAX_SAMPLES]
        warnings.append(f"Truncated to {_MAX_SAMPLES:,} samples")

    # Convert to numeric (handles string counts)
    df = df.apply(pd.to_numeric, errors="coerce").fillna(0)

    # Remove version suffix from Ensembl IDs (ENSG00000141510.17 → ENSG00000141510)
    if gene_id_type == "ensembl":
        df.index = df.index.str.replace(r"\.\d+$", "", regex=True)

    # Remove duplicate gene IDs (keep first)
    dup_count = df.index.duplicated().sum()
    if dup_count > 0:
        df = df[~df.index.duplicated(keep="first")]
        warnings.append(f"Removed {dup_count} duplicate gene IDs")

    gene_ids = df.index.tolist()
    sample_names = df.columns.tolist()

    logger.info("Parsed count matrix: %d genes × %d samples", len(gene_ids), len(sample_names))

    return {
        "matrix": df,
        "gene_ids": gene_ids,
        "sample_names": sample_names,
        "gene_count": len(gene_ids),
        "sample_count": len(sample_names),
        "sample_metadata": None,
        "warnings": warnings,
    }


def _parse_deseq2_result(file_path: str, gene_id_type: str | None = None) -> dict[str, Any]:
    """Parse a pre-computed DESeq2 result table."""
    warnings: list[str] = []
    ext = Path(file_path).suffix.lower()
    sep = "\t" if ext in (".tsv", ".tab", ".txt") else ","

    df = pd.read_csv(file_path, sep=sep, index_col=0, nrows=_MAX_GENES)
    df.index = df.index.astype(str).str.strip()
    df.index.name = "gene_id"

    # Normalize column names to lowercase
    df.columns = df.columns.str.strip().str.lower()

    # Ensure expected columns exist
    expected = {"basemean", "log2foldchange", "padj"}
    missing = expected - set(df.columns)
    if missing:
        warnings.append(f"Missing expected DESeq2 columns: {', '.join(missing)}")

    # Standardize column names
    rename_map = {
        "log2foldchange": "log2FoldChange",
        "basemean": "baseMean",
        "lfcse": "lfcSE",
        "padj": "padj",
        "pvalue": "pvalue",
        "stat": "stat",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    gene_ids = df.index.tolist()

    logger.info("Parsed DESeq2 result: %d genes, columns: %s",
                len(gene_ids), list(df.columns))

    return {
        "matrix": df,
        "gene_ids": gene_ids,
        "sample_names": list(df.columns),
        "gene_count": len(gene_ids),
        "sample_count": 0,  # Not a count matrix — no samples
        "sample_metadata": None,
        "warnings": warnings,
        "is_de_result": True,
    }


def _parse_gct(file_path: str, gene_id_type: str | None = None) -> dict[str, Any]:
    """Parse GCT format (Broad Institute gene expression)."""
    warnings: list[str] = []

    with open(file_path, "r", errors="replace") as f:
        version_line = f.readline().strip()
        dims_line = f.readline().strip()

    # GCT v1.2: first line is #1.2, second is nrows\tncols
    if not version_line.startswith("#1"):
        warnings.append(f"Unexpected GCT version: {version_line}")

    df = pd.read_csv(file_path, sep="\t", skiprows=2, index_col=0, nrows=_MAX_GENES)
    df.index = df.index.astype(str).str.strip()
    df.index.name = "gene_id"

    # GCT has a 'Description' column — remove it
    if "Description" in df.columns:
        df = df.drop(columns=["Description"])
    if "description" in df.columns:
        df = df.drop(columns=["description"])

    # Keep only numeric columns
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    df = df[numeric_cols]

    gene_ids = df.index.tolist()
    sample_names = df.columns.tolist()

    logger.info("Parsed GCT: %d genes × %d samples", len(gene_ids), len(sample_names))

    return {
        "matrix": df,
        "gene_ids": gene_ids,
        "sample_names": sample_names,
        "gene_count": len(gene_ids),
        "sample_count": len(sample_names),
        "sample_metadata": None,
        "warnings": warnings,
    }
