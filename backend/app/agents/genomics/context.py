"""Context builder for genomics data — generates LLM context and code preamble.

Saves expression matrices as parquet via ceaser:// protocol.
Builds genomics-specific context strings with markers that the router
detects to dispatch to the genomics pipeline.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

_CEASER_PROTOCOL = "ceaser://"


def build_genomics_context(
    matrix: pd.DataFrame,
    file_name: str,
    file_format: str,
    gene_count: int,
    sample_count: int,
    gene_id_type: str | None,
    organism: str | None,
    normalization: str | None,
    qc_summary: str,
    sample_metadata: dict[str, Any] | None,
    gene_symbols: dict[str, str] | None,
    parquet_paths: dict[str, str],
    annotation_summary: str = "",
) -> str:
    """Build the LLM context string for genomics data.

    Contains markers (GENOMICS DATA CONTEXT, GENOMICS CODE PREAMBLE) that
    the router uses to deterministically dispatch to the genomics pipeline.
    """
    lines = [
        "GENOMICS DATA CONTEXT",
        "=" * 50,
        f"File: {file_name}",
        f"Format: {file_format}",
        f"Genes: {gene_count:,}",
        f"Samples: {sample_count}",
        f"Gene ID type: {gene_id_type or 'unknown'}",
        f"Organism: {organism or 'unknown'}",
        f"Normalization: {normalization or 'raw counts'}",
        "",
    ]

    # QC summary
    if qc_summary:
        lines.append("QC Summary:")
        lines.append(f"  {qc_summary}")
        lines.append("")

    # Sample groups
    if sample_metadata:
        lines.append("Sample Groups:")
        for group, samples in sample_metadata.items():
            sample_list = ", ".join(samples[:5])
            if len(samples) > 5:
                sample_list += f", ... ({len(samples)} total)"
            lines.append(f"  {group}: {sample_list}")
        lines.append("")

    # Available columns / sample names
    sample_names = matrix.columns.tolist() if not matrix.empty else []
    if sample_names:
        shown = sample_names[:20]
        lines.append(f"Sample columns ({len(sample_names)}): {', '.join(shown)}")
        if len(sample_names) > 20:
            lines.append(f"  ... ({len(sample_names) - 20} more)")
        lines.append("")

    # Gene preview (top 20 by variance or first 20)
    if not matrix.empty and file_format != "deseq2_result":
        try:
            gene_vars = matrix.var(axis=1)
            top_genes = gene_vars.nlargest(20).index.tolist()
        except Exception:
            top_genes = matrix.index[:20].tolist()

        lines.append("Top variable genes (preview):")
        for g in top_genes:
            symbol = gene_symbols.get(g, "") if gene_symbols else ""
            label = f"{g} ({symbol})" if symbol and symbol != g else g
            lines.append(f"  {label}")
        lines.append("")

    # DESeq2 result columns
    if file_format == "deseq2_result":
        lines.append(f"Result columns: {', '.join(matrix.columns.tolist())}")
        sig_count = 0
        if "padj" in matrix.columns:
            sig_count = (matrix["padj"].dropna() < 0.05).sum()
            lines.append(f"Significant genes (padj < 0.05): {sig_count:,}")
        if "log2FoldChange" in matrix.columns:
            up = ((matrix.get("padj", pd.Series()) < 0.05) & (matrix["log2FoldChange"] > 1)).sum()
            down = ((matrix.get("padj", pd.Series()) < 0.05) & (matrix["log2FoldChange"] < -1)).sum()
            lines.append(f"  Upregulated (log2FC > 1): {up:,}")
            lines.append(f"  Downregulated (log2FC < -1): {down:,}")
        lines.append("")

    # Annotation summary
    if annotation_summary:
        lines.append(annotation_summary)
        lines.append("")

    return "\n".join(lines)


def generate_genomics_preamble(
    parquet_paths: dict[str, str],
    file_format: str,
    gene_count: int,
    sample_count: int,
    sample_names: list[str] | None = None,
    sample_metadata: dict[str, Any] | None = None,
) -> str:
    """Generate Python code preamble for genomics analysis.

    Includes:
    - Imports for genomics libraries
    - DataFrame load from parquet
    - Column metadata as comments
    - Sample group definitions
    """
    lines = [
        "import pandas as pd",
        "import numpy as np",
        "import plotly.express as px",
        "import plotly.graph_objects as go",
        "from scipy import stats",
        "",
    ]

    for var_name, remote_path in parquet_paths.items():
        safe_ref = f"{_CEASER_PROTOCOL}{remote_path}"
        lines.append(f'{var_name} = pd.read_parquet("{safe_ref}")')

        # Add metadata as comments
        lines.append(f"# {var_name}: {gene_count:,} genes × {sample_count} samples")
        if sample_names:
            shown = ", ".join(sample_names[:10])
            lines.append(f"# Sample columns: {shown}")
        lines.append("")

    # Sample group definitions (if detected)
    if sample_metadata and len(sample_metadata) >= 2:
        lines.append("# Detected sample groups:")
        for group, samples in sample_metadata.items():
            safe_group = group.replace("'", "\\'")
            sample_list = ", ".join(f"'{s}'" for s in samples)
            lines.append(f"group_{_safe_var(group)} = [{sample_list}]")
        lines.append("")

    # Format-specific hints
    if file_format == "deseq2_result":
        lines.append("# This is a pre-computed DESeq2 result table.")
        lines.append("# Columns include: baseMean, log2FoldChange, lfcSE, stat, pvalue, padj")
        lines.append("# Gene IDs are in the index. Use df.index to access them.")
        lines.append("")
    elif file_format == "count_matrix":
        lines.append("# This is a raw count matrix (genes × samples).")
        lines.append("# For DESeq2: use raw integer counts (do NOT normalize first).")
        lines.append("# For visualization/PCA: use log2(CPM+1) normalized values.")
        lines.append("")

    return "\n".join(lines)


def save_genomics_parquet(
    matrix: pd.DataFrame,
    org_id: str,
    file_name: str,
) -> dict[str, str]:
    """Save expression matrix as parquet to storage.

    Returns: {var_name: remote_path}
    """
    from app.services.storage import get_storage

    var_name = _safe_var(file_name)
    if not var_name.startswith("df_"):
        var_name = f"df_{var_name}"

    remote_path = f"parquet/{org_id}/{var_name}.parquet"
    buf = matrix.to_parquet(index=True)

    storage = get_storage()

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            future = asyncio.run_coroutine_threadsafe(
                storage.upload(buf, remote_path), loop
            )
            future.result(timeout=60)
        else:
            asyncio.run(storage.upload(buf, remote_path))
    except Exception as exc:
        logger.error("Failed to save genomics parquet: %s", exc)
        raise

    logger.info("Saved genomics parquet: %s (%d genes) -> %s",
                var_name, len(matrix), remote_path)

    return {var_name: remote_path}


def _safe_var(name: str) -> str:
    """Convert a filename to a safe Python variable name."""
    import re
    name = name.rsplit(".", 1)[0]  # Remove extension
    name = re.sub(r"[^\w]", "_", name.lower())
    name = re.sub(r"_+", "_", name).strip("_")
    if name and name[0].isdigit():
        name = f"data_{name}"
    return name[:30] or "df_genomics"
