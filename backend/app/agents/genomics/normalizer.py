"""Expression matrix normalization methods.

Supports:
- CPM (Counts Per Million) — default for general use
- log2(CPM + 1) — default for visualization and clustering
- TPM (Transcripts Per Million) — requires gene lengths
- None / raw — for DESeq2 input (requires raw integer counts)
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def normalize(
    matrix: pd.DataFrame,
    method: str = "log2_cpm",
    gene_lengths: pd.Series | None = None,
) -> tuple[pd.DataFrame, str]:
    """Normalize an expression matrix.

    Args:
        matrix: genes × samples DataFrame of raw counts
        method: 'cpm', 'log2_cpm', 'tpm', 'raw'
        gene_lengths: Series of gene lengths (required for TPM)

    Returns:
        (normalized_matrix, method_applied)
    """
    if method == "raw":
        return matrix, "raw"

    if method == "tpm" and gene_lengths is not None:
        return _tpm(matrix, gene_lengths), "tpm"

    if method == "cpm":
        return _cpm(matrix), "cpm"

    # Default: log2(CPM + 1)
    return _log2_cpm(matrix), "log2_cpm"


def _cpm(matrix: pd.DataFrame) -> pd.DataFrame:
    """Counts Per Million normalization."""
    lib_sizes = matrix.sum(axis=0)
    # Avoid division by zero
    lib_sizes = lib_sizes.replace(0, 1)
    cpm = matrix.div(lib_sizes, axis=1) * 1e6
    logger.info("Applied CPM normalization")
    return cpm


def _log2_cpm(matrix: pd.DataFrame) -> pd.DataFrame:
    """Log2(CPM + 1) normalization — good default for visualization."""
    cpm = _cpm(matrix)
    result = np.log2(cpm + 1)
    logger.info("Applied log2(CPM+1) normalization")
    return result


def _tpm(matrix: pd.DataFrame, gene_lengths: pd.Series) -> pd.DataFrame:
    """Transcripts Per Million — accounts for gene length.

    TPM = (count / gene_length) / sum(count / gene_length) * 1e6
    """
    # Align gene lengths with matrix index
    common = matrix.index.intersection(gene_lengths.index)
    if len(common) < len(matrix) * 0.5:
        logger.warning("Only %d/%d genes have length info, falling back to CPM",
                       len(common), len(matrix))
        return _cpm(matrix)

    mat = matrix.loc[common]
    lengths = gene_lengths.loc[common].replace(0, 1)

    # Rate: counts per kilobase
    rate = mat.div(lengths, axis=0) * 1000

    # Normalize to per million
    rate_sum = rate.sum(axis=0).replace(0, 1)
    tpm = rate.div(rate_sum, axis=1) * 1e6

    logger.info("Applied TPM normalization (%d genes with length info)", len(common))
    return tpm


def choose_normalization(file_format: str) -> str:
    """Choose default normalization based on file format.

    - Raw count matrices → log2_cpm (good for visualization, PCA, clustering)
    - Pre-computed DESeq2 results → skip (already processed)
    - GCT files → may already be normalized → skip
    """
    if file_format == "deseq2_result":
        return "raw"  # Already processed
    if file_format == "gct":
        return "raw"  # Often already normalized
    return "log2_cpm"
