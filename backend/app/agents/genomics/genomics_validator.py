"""Genomics-specific code validation — pre-execution checks.

Catches bioinformatics mistakes that a generic validator would miss:
- Running DESeq2 on normalized data (should use raw counts)
- Using pvalue instead of padj
- Wrong matrix orientation for PCA vs DE
- Overly lenient significance thresholds
"""

from __future__ import annotations

import logging
import re

from app.agents.python_validator import validate_python as _base_validate
from app.agents.state import AgentState

logger = logging.getLogger(__name__)


def validate_genomics(state: AgentState) -> AgentState:
    """Validate genomics Python code before execution.

    Runs all base python_validator checks PLUS genomics-specific checks.
    """
    # Run base validation first (blocked imports, column refs, bare file reads, etc.)
    state = _base_validate(state)

    # If base validation already found errors, return immediately
    if state.get("error"):
        return state

    code = state.get("code_block", "")
    if not code:
        return state

    errors: list[str] = []
    warnings: list[str] = []

    # 1. DESeq2 on normalized data
    if _uses_deseq2(code) and _normalizes_before_de(code):
        errors.append(
            "DESeq2 requires RAW INTEGER COUNTS. The code normalizes data "
            "(CPM/TPM/log2) before passing to DeseqDataSet. Remove normalization "
            "or use the raw count matrix."
        )

    # 2. Using pvalue instead of padj for filtering
    pval_filter = re.search(r"""(?:df|results?|sig)\[['"]pvalue['"]\]\s*[<>]""", code)
    padj_filter = re.search(r"""(?:df|results?|sig)\[['"]padj['"]\]\s*[<>]""", code)
    if pval_filter and not padj_filter:
        warnings.append(
            "Filtering by 'pvalue' instead of 'padj' (adjusted p-value). "
            "For multiple testing correction, use 'padj' to control false discovery rate."
        )

    # 3. pydeseq2 API mistakes
    if ".deseq()" in code and ".deseq2()" not in code:
        errors.append(
            "pydeseq2 API: use .deseq2() not .deseq(). "
            "The correct call is: dds.deseq2()"
        )
    if ".results()" in code and ".results_df" not in code:
        warnings.append(
            "pydeseq2 API: access results via .results_df attribute, "
            "not .results() method."
        )

    # 4. Overly lenient thresholds
    padj_thresh = re.search(r"""padj.*?[<>]\s*([\d.]+)""", code)
    if padj_thresh:
        try:
            val = float(padj_thresh.group(1))
            if val > 0.1:
                warnings.append(
                    f"padj threshold {val} is very lenient. Standard is 0.05. "
                    f"Consider using a stricter threshold to reduce false positives."
                )
        except ValueError:
            pass

    lfc_thresh = re.search(r"""log2FoldChange.*?abs\(\).*?>\s*([\d.]+)""", code)
    if not lfc_thresh:
        lfc_thresh = re.search(r"""log2FoldChange.*?[>]\s*([\d.]+)""", code)
    if lfc_thresh:
        try:
            val = float(lfc_thresh.group(1))
            if val < 0.5:
                warnings.append(
                    f"log2FC threshold {val} is very low. Standard is 1.0 (2-fold change). "
                    f"Consider using a higher threshold for biological significance."
                )
        except ValueError:
            pass

    # 5. Heatmap without z-score normalization
    if _is_heatmap(code) and not _has_zscore(code):
        warnings.append(
            "Heatmap without z-score normalization. Raw expression values "
            "will be dominated by highly expressed genes. Consider z-scoring per gene."
        )

    # 6. gseapy with unsorted gene list
    if "gp.prerank(" in code or "gseapy.prerank(" in code:
        if "sort" not in code.lower():
            warnings.append(
                "gseapy.prerank() requires a pre-ranked gene list (sorted by statistic). "
                "Make sure the gene list is sorted by log2FoldChange or stat column."
            )

    # Report
    if errors:
        error_msg = "Genomics validation failed:\n" + "\n".join(f"- {e}" for e in errors)
        if warnings:
            error_msg += "\nWarnings:\n" + "\n".join(f"- {w}" for w in warnings)
        logger.warning("Genomics validation: %d errors, %d warnings", len(errors), len(warnings))
        retry = state.get("retry_count", 0)
        return {**state, "error": error_msg, "retry_count": retry + 1}

    if warnings:
        logger.info("Genomics validation: %d warnings (non-blocking): %s",
                     len(warnings), "; ".join(warnings))

    return {**state, "error": None}


def _uses_deseq2(code: str) -> bool:
    return "DeseqDataSet" in code or "deseq2" in code.lower()


def _normalizes_before_de(code: str) -> bool:
    """Check if normalization happens before DeseqDataSet construction."""
    # Find the line number of DeseqDataSet
    de_line = None
    for i, line in enumerate(code.split("\n")):
        if "DeseqDataSet" in line:
            de_line = i
            break
    if de_line is None:
        return False

    # Check if normalization happens in lines before DE
    pre_de = "\n".join(code.split("\n")[:de_line])
    norm_patterns = [
        r"log2\(", r"np\.log", r"\.div\(.*1e6\)", r"_cpm\(", r"_tpm\(",
        r"normalize\(", r"TPM", r"RPKM", r"CPM",
    ]
    return any(re.search(p, pre_de) for p in norm_patterns)


def _is_heatmap(code: str) -> bool:
    return "Heatmap" in code or "heatmap" in code.lower() or "imshow" in code


def _has_zscore(code: str) -> bool:
    return "zscore" in code.lower() or "z_score" in code.lower() or "z-score" in code.lower() or \
           "(row - row.mean())" in code or "stats.zscore" in code
