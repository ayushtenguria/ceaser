"""Genomics upload pipeline — processes expression data at upload time.

7-node stateful pipeline (mirrors excel/orchestrator.py):
  1. Detect format (count matrix, DESeq2 result, GCT, etc.)
  2. Parse expression matrix (genes × samples DataFrame)
  3. Quality control (library sizes, gene detection, PCA outliers)
  4. Normalize (CPM, log2-CPM, or skip for pre-computed)
  5. Annotate genes (map IDs to symbols via local SQLite)
  6. Build context (save parquet, generate LLM context + code preamble)
  7. Generate insight (LLM-powered upload summary)

Each node is fault-tolerant — failures are logged and skipped, downstream
nodes adapt. The pipeline never raises; it always returns a result dict.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from langchain_core.language_models import BaseChatModel

from app.agents.genomics.state import GenomicsPipelineState

logger = logging.getLogger(__name__)


async def process_genomics_upload(
    file_path: str,
    llm: BaseChatModel | None = None,
    org_id: str = "default",
) -> dict[str, Any]:
    """Process a genomics file at upload time.

    Returns dict with: genomics_context, code_preamble, parquet_paths,
    qc_report, gene_count, sample_count, insight, warnings, pipeline_time_seconds.
    """
    import asyncio

    start = time.monotonic()

    state: GenomicsPipelineState = {
        "file_path": file_path,
        "org_id": org_id,
        "warnings": [],
        "failed_steps": [],
    }

    # Run the pipeline nodes sequentially
    nodes = [
        ("detect", _node_detect_format),
        ("parse", _node_parse_matrix),
        ("qc", _node_qc),
        ("normalize", _node_normalize),
        ("annotate", _node_annotate),
        ("context", _node_build_context),
        ("insight", _node_generate_insight if llm else None),
    ]

    for name, node_fn in nodes:
        if node_fn is None:
            continue
        try:
            if name == "insight":
                state = await node_fn(state, llm)  # type: ignore[arg-type]
            elif name in ("context", "annotate"):
                state = await asyncio.to_thread(node_fn, state)
            else:
                state = await asyncio.to_thread(node_fn, state)
            logger.info("Genomics node %s: OK", name)
        except Exception as exc:
            logger.warning("Genomics node %s failed: %s", name, exc)
            state["warnings"] = state.get("warnings", []) + [f"{name}: {exc}"]
            state["failed_steps"] = state.get("failed_steps", []) + [name]

            # Parse failure is fatal — can't continue without a matrix
            if name == "parse":
                break

    elapsed = time.monotonic() - start
    state["pipeline_time_seconds"] = round(elapsed, 2)

    gene_count = state.get("gene_count", 0)
    sample_count = state.get("sample_count", 0)
    warnings = state.get("warnings", [])

    logger.info(
        "Genomics pipeline complete: %.1fs, %d genes, %d samples, %d warnings, %d failed",
        elapsed, gene_count, sample_count, len(warnings), len(state.get("failed_steps", [])),
    )

    return {
        "genomics_context": state.get("genomics_context", ""),
        "code_preamble": state.get("code_preamble", ""),
        "parquet_paths": state.get("parquet_paths", {}),
        "qc_report": state.get("qc_report", {}),
        "gene_count": gene_count,
        "sample_count": sample_count,
        "insight": {
            "summary": state.get("insight_summary", ""),
            "suggestions": state.get("insight_suggestions", []),
        },
        "quality_report": {
            "severity": "clean" if state.get("qc_passed", True) else "major",
            "total_issues": len(warnings),
            "items": warnings,
        },
        "warnings": warnings,
        "pipeline_time_seconds": elapsed,
    }


# ── Pipeline Nodes ──────────────────────────────────────────────────


def _node_detect_format(state: GenomicsPipelineState) -> GenomicsPipelineState:
    """Node 1: Detect file format, gene ID type, and organism."""
    from app.agents.genomics.detector import detect_format
    from pathlib import Path

    file_path = state["file_path"]
    result = detect_format(file_path)

    return {
        **state,
        "file_name": Path(file_path).name,
        "file_format": result["file_format"],
        "gene_id_type": result.get("gene_id_type"),
        "organism": result.get("organism"),
        "sample_metadata": result.get("sample_metadata"),
    }


def _node_parse_matrix(state: GenomicsPipelineState) -> GenomicsPipelineState:
    """Node 2: Parse expression matrix into genes × samples DataFrame."""
    from app.agents.genomics.matrix_parser import parse_expression_matrix

    file_path = state["file_path"]
    file_format = state.get("file_format", "generic_csv")
    gene_id_type = state.get("gene_id_type")

    result = parse_expression_matrix(file_path, file_format, gene_id_type)

    warnings = state.get("warnings", []) + result.get("warnings", [])

    # Update organism/gene_id_type if detected during parsing
    organism = state.get("organism")
    if not organism and result.get("gene_ids"):
        from app.agents.genomics.detector import detect_organism
        organism = detect_organism(result["gene_ids"])

    id_type = state.get("gene_id_type")
    if not id_type and result.get("gene_ids"):
        from app.agents.genomics.detector import detect_gene_id_type
        id_type = detect_gene_id_type(result["gene_ids"])

    # Merge sample metadata from detection and parsing
    sample_meta = state.get("sample_metadata") or result.get("sample_metadata")

    return {
        **state,
        "expression_matrix": result["matrix"],
        "gene_ids": result.get("gene_ids", []),
        "sample_names": result.get("sample_names", []),
        "gene_count": result["gene_count"],
        "sample_count": result["sample_count"],
        "sample_metadata": sample_meta,
        "organism": organism,
        "gene_id_type": id_type,
        "warnings": warnings,
    }


def _node_qc(state: GenomicsPipelineState) -> GenomicsPipelineState:
    """Node 3: Run quality control on expression matrix."""
    matrix = state.get("expression_matrix")
    file_format = state.get("file_format", "")

    # Skip QC for pre-computed results
    if file_format == "deseq2_result" or matrix is None or matrix.empty:
        return {
            **state,
            "qc_report": {"summary": "QC skipped (pre-computed result)", "qc_passed": True},
            "qc_passed": True,
        }

    from app.agents.genomics.qc import run_qc
    report = run_qc(matrix)

    return {
        **state,
        "qc_report": report,
        "qc_passed": report.get("qc_passed", True),
    }


def _node_normalize(state: GenomicsPipelineState) -> GenomicsPipelineState:
    """Node 4: Normalize expression matrix."""
    matrix = state.get("expression_matrix")
    file_format = state.get("file_format", "")

    if matrix is None or matrix.empty:
        return {**state, "normalization_method": None}

    from app.agents.genomics.normalizer import normalize, choose_normalization

    method = choose_normalization(file_format)
    normalized, applied = normalize(matrix, method)

    return {
        **state,
        "expression_matrix": normalized,
        "normalization_method": applied,
    }


def _node_annotate(state: GenomicsPipelineState) -> GenomicsPipelineState:
    """Node 5: Map gene IDs to symbols using local annotation DB."""
    gene_ids = state.get("gene_ids", [])
    organism = state.get("organism", "human")
    gene_id_type = state.get("gene_id_type")

    if not gene_ids or gene_id_type == "symbol":
        return {
            **state,
            "genes_annotated": 0,
            "gene_symbols": {},
            "annotation_summary": "Gene symbols used directly (no mapping needed)."
                if gene_id_type == "symbol" else "No genes to annotate.",
        }

    from app.agents.genomics.annotation import get_annotation_service
    service = get_annotation_service()

    if not service.available:
        return {
            **state,
            "genes_annotated": 0,
            "gene_symbols": {},
            "annotation_summary": "Gene annotation database not available. Using original IDs.",
        }

    symbols = service.get_symbols_for_ids(gene_ids[:5000], organism or "human")
    annotated = len(symbols)

    summary = (
        f"Mapped {annotated:,}/{len(gene_ids):,} gene IDs to symbols "
        f"({annotated/max(len(gene_ids),1):.0%} coverage)."
    )

    return {
        **state,
        "genes_annotated": annotated,
        "gene_symbols": symbols,
        "annotation_summary": summary,
    }


def _node_build_context(state: GenomicsPipelineState) -> GenomicsPipelineState:
    """Node 6: Save parquet + build LLM context and code preamble."""
    matrix = state.get("expression_matrix")
    if matrix is None or matrix.empty:
        return {
            **state,
            "genomics_context": "",
            "code_preamble": "",
            "parquet_paths": {},
        }

    from app.agents.genomics.context import (
        save_genomics_parquet,
        build_genomics_context,
        generate_genomics_preamble,
    )

    file_name = state.get("file_name", "genomics_data")
    org_id = state.get("org_id", "default")

    # Save to parquet
    parquet_paths = save_genomics_parquet(matrix, org_id, file_name)

    # Build context
    genomics_context = build_genomics_context(
        matrix=matrix,
        file_name=file_name,
        file_format=state.get("file_format", "unknown"),
        gene_count=state.get("gene_count", 0),
        sample_count=state.get("sample_count", 0),
        gene_id_type=state.get("gene_id_type"),
        organism=state.get("organism"),
        normalization=state.get("normalization_method"),
        qc_summary=state.get("qc_report", {}).get("summary", ""),
        sample_metadata=state.get("sample_metadata"),
        gene_symbols=state.get("gene_symbols"),
        parquet_paths=parquet_paths,
        annotation_summary=state.get("annotation_summary", ""),
    )

    # Generate preamble
    code_preamble = generate_genomics_preamble(
        parquet_paths=parquet_paths,
        file_format=state.get("file_format", "unknown"),
        gene_count=state.get("gene_count", 0),
        sample_count=state.get("sample_count", 0),
        sample_names=state.get("sample_names"),
        sample_metadata=state.get("sample_metadata"),
    )

    return {
        **state,
        "genomics_context": genomics_context,
        "code_preamble": code_preamble,
        "parquet_paths": parquet_paths,
    }


async def _node_generate_insight(
    state: GenomicsPipelineState,
    llm: BaseChatModel,
) -> GenomicsPipelineState:
    """Node 7: LLM-powered upload summary and suggested analyses."""
    from langchain_core.messages import SystemMessage, HumanMessage

    gene_count = state.get("gene_count", 0)
    sample_count = state.get("sample_count", 0)
    file_format = state.get("file_format", "unknown")
    organism = state.get("organism", "unknown")
    qc_summary = state.get("qc_report", {}).get("summary", "")
    sample_metadata = state.get("sample_metadata")
    normalization = state.get("normalization_method", "")

    prompt = f"""\
A researcher uploaded a genomics dataset. Generate a brief welcome message and
3-4 suggested next-step analyses.

Dataset summary:
- Format: {file_format}
- Genes: {gene_count:,}
- Samples: {sample_count}
- Organism: {organism}
- Normalization: {normalization}
- QC: {qc_summary}
- Sample groups: {sample_metadata or 'not detected'}

Return a JSON object:
{{"summary": "1-2 sentence welcome + data description",
  "suggestions": ["analysis suggestion 1", "suggestion 2", "suggestion 3"]}}

Suggestions should be specific to THIS data (e.g., "Run differential expression
between treated and control" not generic "analyze the data"). If sample groups
were detected, reference them. If it's a DESeq2 result, suggest volcano plot
and pathway analysis."""

    try:
        response = await llm.ainvoke([
            SystemMessage(content="You are a genomics data analyst. Return only valid JSON."),
            HumanMessage(content=prompt),
        ])

        import json
        raw = response.content.strip()  # type: ignore[union-attr]
        if raw.startswith("```"):
            lines = raw.split("\n")
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            raw = "\n".join(lines).strip()

        data = json.loads(raw)
        summary = data.get("summary", "")
        suggestions = data.get("suggestions", [])

        return {
            **state,
            "insight_summary": summary,
            "insight_suggestions": suggestions[:4],
        }
    except Exception as exc:
        logger.warning("Genomics insight generation failed: %s", exc)

        # Fallback auto-summary
        summary = (
            f"Uploaded {file_format} with {gene_count:,} genes"
            f"{f' × {sample_count} samples' if sample_count else ''}."
        )
        suggestions = ["Show PCA of all samples", "Run differential expression analysis"]
        if file_format == "deseq2_result":
            suggestions = ["Show volcano plot", "List top 20 DE genes", "Run pathway enrichment"]

        return {
            **state,
            "insight_summary": summary,
            "insight_suggestions": suggestions,
        }
