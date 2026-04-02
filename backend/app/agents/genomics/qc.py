"""Quality control for expression matrices.

Computes per-sample and per-gene QC metrics:
- Library size (total counts per sample)
- Gene detection rate (fraction of genes with count > 0)
- Outlier detection via PCA distance
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_MIN_LIBRARY_SIZE = 1000
_MIN_GENE_DETECTION = 0.10  # At least 10% of genes detected
_OUTLIER_SD_THRESHOLD = 3.0
_PCA_TOP_GENES = 500


def run_qc(matrix: pd.DataFrame) -> dict[str, Any]:
    """Run quality control on an expression matrix (genes × samples).

    Returns a QC report dict with:
        library_sizes: {sample: total_counts}
        gene_detection_rates: {sample: fraction_detected}
        low_quality_samples: [sample_names]
        outlier_samples: [sample_names]
        low_expression_genes: int (genes with 0 counts across all samples)
        qc_passed: bool
        summary: str
    """
    report: dict[str, Any] = {
        "library_sizes": {},
        "gene_detection_rates": {},
        "low_quality_samples": [],
        "outlier_samples": [],
        "low_expression_genes": 0,
        "qc_passed": True,
        "summary": "",
    }

    if matrix.empty:
        report["qc_passed"] = False
        report["summary"] = "Empty expression matrix."
        return report

    n_genes, n_samples = matrix.shape

    # Library sizes (total counts per sample)
    lib_sizes = matrix.sum(axis=0)
    report["library_sizes"] = lib_sizes.to_dict()

    # Gene detection rate (fraction of genes with count > 0 per sample)
    detection_rates = (matrix > 0).sum(axis=0) / n_genes
    report["gene_detection_rates"] = detection_rates.to_dict()

    # Flag low-quality samples
    low_lib = lib_sizes[lib_sizes < _MIN_LIBRARY_SIZE].index.tolist()
    low_det = detection_rates[detection_rates < _MIN_GENE_DETECTION].index.tolist()
    low_quality = list(set(low_lib + low_det))
    report["low_quality_samples"] = low_quality

    # Count genes with zero expression across all samples
    zero_genes = (matrix.sum(axis=1) == 0).sum()
    report["low_expression_genes"] = int(zero_genes)

    # PCA-based outlier detection (on top variable genes)
    outliers = _detect_pca_outliers(matrix)
    report["outlier_samples"] = outliers

    # Overall QC pass/fail
    total_flagged = len(set(low_quality + outliers))
    if total_flagged > n_samples * 0.5:
        report["qc_passed"] = False

    # Summary
    parts = [
        f"{n_genes:,} genes × {n_samples} samples",
        f"Library sizes: {int(lib_sizes.min()):,} – {int(lib_sizes.max()):,} (median {int(lib_sizes.median()):,})",
        f"Gene detection: {detection_rates.min():.1%} – {detection_rates.max():.1%}",
        f"Zero-expression genes: {zero_genes:,} ({zero_genes/n_genes:.1%})",
    ]
    if low_quality:
        parts.append(f"Low-quality samples: {', '.join(low_quality[:5])}")
    if outliers:
        parts.append(f"PCA outliers: {', '.join(outliers[:5])}")

    report["summary"] = ". ".join(parts)

    logger.info("QC: %d genes × %d samples, %d flagged, passed=%s",
                n_genes, n_samples, total_flagged, report["qc_passed"])

    return report


def _detect_pca_outliers(matrix: pd.DataFrame) -> list[str]:
    """Detect outlier samples via PCA distance from centroid."""
    try:
        n_genes, n_samples = matrix.shape
        if n_samples < 4:
            return []

        # Use top variable genes for PCA
        gene_vars = matrix.var(axis=1)
        top_genes = gene_vars.nlargest(min(_PCA_TOP_GENES, n_genes)).index
        subset = matrix.loc[top_genes].T  # samples × genes

        # Log transform for PCA stability
        subset = np.log2(subset + 1)

        # Simple PCA via SVD (avoid sklearn dependency at import time)
        centered = subset - subset.mean()
        U, S, Vt = np.linalg.svd(centered, full_matrices=False)
        pc_coords = U[:, :2] * S[:2]  # First 2 PCs

        # Distance from centroid
        centroid = pc_coords.mean(axis=0)
        distances = np.sqrt(((pc_coords - centroid) ** 2).sum(axis=1))

        mean_dist = distances.mean()
        std_dist = distances.std()

        if std_dist == 0:
            return []

        outlier_mask = distances > mean_dist + _OUTLIER_SD_THRESHOLD * std_dist
        outlier_samples = subset.index[outlier_mask].tolist()

        return outlier_samples

    except Exception as exc:
        logger.debug("PCA outlier detection failed: %s", exc)
        return []
