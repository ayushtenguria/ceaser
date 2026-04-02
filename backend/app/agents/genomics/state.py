"""State definitions for the genomics pipeline.

GenomicsPipelineState — upload-time processing (mirrors ExcelPipelineState).
"""

from __future__ import annotations

from typing import Any, TypedDict


class GenomicsPipelineState(TypedDict, total=False):
    """Shared state for the genomics upload pipeline.

    Flows through 7 sequential nodes: detect → parse → QC → normalize →
    annotate → build context → generate insight.
    """

    # ── Input ──
    file_path: str
    org_id: str

    # ── Detection (node 1) ──
    file_name: str
    file_format: str
    """One of: count_matrix, deseq2_result, gct, generic_csv"""
    organism: str | None
    """'human', 'mouse', or None if unknown."""
    gene_id_type: str | None
    """'ensembl', 'symbol', 'entrez', 'probe_id', or None."""

    # ── Parsed matrix (node 2) ──
    expression_matrix: Any  # pd.DataFrame — genes × samples
    sample_names: list[str]
    gene_ids: list[str]
    sample_metadata: dict[str, Any] | None
    """Detected conditions/groups: {'treated': ['s1','s2'], 'control': ['s3','s4']}"""
    gene_count: int
    sample_count: int

    # ── QC (node 3) ──
    qc_report: dict[str, Any]
    """library_sizes, gene_detection_rates, outlier_samples, etc."""
    qc_passed: bool

    # ── Normalization (node 4) ──
    normalization_method: str | None
    """'raw', 'cpm', 'log2_cpm', 'tpm', 'pre_computed'."""

    # ── Annotation (node 5) ──
    genes_annotated: int
    gene_symbols: dict[str, str]
    """Mapping: original_id → symbol (e.g., ENSG00000141510 → TP53)."""
    annotation_summary: str

    # ── Context for LLM (node 6) ──
    genomics_context: str
    code_preamble: str
    parquet_paths: dict[str, str]

    # ── Insight (node 7) ──
    insight_summary: str
    insight_suggestions: list[str]

    # ── Pipeline metadata ──
    warnings: list[str]
    failed_steps: list[str]
    pipeline_time_seconds: float
