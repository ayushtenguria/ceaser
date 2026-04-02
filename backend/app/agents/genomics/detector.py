"""Format detection for genomics files.

Examines file extension and content to determine:
- File format (count matrix, DESeq2 result, GCT, etc.)
- Gene ID type (Ensembl, Symbol, Entrez)
- Organism (human, mouse)
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# Gene ID regex patterns
_ENSEMBL_HUMAN = re.compile(r"^ENSG\d{11}(\.\d+)?$")
_ENSEMBL_MOUSE = re.compile(r"^ENSMUSG\d{11}(\.\d+)?$")
_ENTREZ = re.compile(r"^\d{1,10}$")
_SYMBOL = re.compile(r"^[A-Z][A-Z0-9-]{1,15}$")

# DESeq2 result columns
_DESEQ2_COLUMNS = {"basemean", "log2foldchange", "lfcse", "stat", "pvalue", "padj"}
_DESEQ2_MIN_MATCH = 4


def detect_format(file_path: str) -> dict:
    """Detect genomics file format from extension and content.

    Returns dict with: file_format, gene_id_type, organism, sample_metadata.
    """
    path = Path(file_path)
    ext = path.suffix.lower()
    name = path.stem.lower()

    result = {
        "file_format": "generic_csv",
        "gene_id_type": None,
        "organism": None,
        "sample_metadata": None,
    }

    # GCT format (Broad Institute)
    if ext == ".gct":
        result["file_format"] = "gct"
        _detect_ids_from_file(file_path, result, skip_rows=2)
        return result

    # SOFT format (GEO)
    if ext in (".soft", ".soft.gz"):
        result["file_format"] = "soft"
        return result

    # TSV/CSV — need to inspect content
    if ext in (".tsv", ".csv", ".txt", ".tab"):
        _detect_from_tabular(file_path, ext, result)
        return result

    return result


def _detect_from_tabular(file_path: str, ext: str, result: dict) -> None:
    """Inspect a tabular file to determine if it's a count matrix or DESeq2 result."""
    sep = "\t" if ext in (".tsv", ".tab", ".txt") else ","

    try:
        with open(file_path, "r", errors="replace") as f:
            lines = []
            for i, line in enumerate(f):
                if i >= 25:
                    break
                lines.append(line.strip())
    except Exception as exc:
        logger.warning("Could not read file for detection: %s", exc)
        return

    if not lines:
        return

    # Find header line (skip comment lines starting with #)
    header_idx = 0
    for i, line in enumerate(lines):
        if not line.startswith("#"):
            header_idx = i
            break

    if header_idx >= len(lines):
        return

    header = lines[header_idx].split(sep)
    header_lower = {h.strip().lower() for h in header}

    # Check for DESeq2 result format
    deseq2_match = len(header_lower & _DESEQ2_COLUMNS)
    if deseq2_match >= _DESEQ2_MIN_MATCH:
        result["file_format"] = "deseq2_result"
        _detect_ids_from_file(file_path, result, skip_rows=header_idx + 1, sep=sep)
        return

    # Check if it looks like a count matrix (first col = gene IDs, rest = numeric)
    if len(header) >= 3 and header_idx + 1 < len(lines):
        data_lines = lines[header_idx + 1: header_idx + 11]
        if _looks_like_count_matrix(data_lines, sep):
            result["file_format"] = "count_matrix"
            _detect_ids_from_file(file_path, result, skip_rows=header_idx + 1, sep=sep)

            # Try to extract sample groups from column names
            sample_cols = [h.strip() for h in header[1:]]
            groups = _infer_sample_groups(sample_cols)
            if groups:
                result["sample_metadata"] = groups
            return

    # Fallback: generic CSV
    _detect_ids_from_file(file_path, result, skip_rows=header_idx + 1, sep=sep)


def _looks_like_count_matrix(data_lines: list[str], sep: str) -> bool:
    """Check if data rows look like gene_id followed by numeric values."""
    numeric_rows = 0
    for line in data_lines:
        parts = line.split(sep)
        if len(parts) < 3:
            continue
        # First column should be a gene ID (non-numeric string)
        first = parts[0].strip().strip('"')
        if not first or first.replace(".", "").replace("-", "").isdigit():
            continue
        # Remaining columns should be mostly numeric
        numeric_count = sum(
            1 for p in parts[1:] if _is_numeric(p.strip().strip('"'))
        )
        if numeric_count >= len(parts) * 0.7:
            numeric_rows += 1

    return numeric_rows >= len(data_lines) * 0.5


def _is_numeric(val: str) -> bool:
    """Check if a string is numeric (int or float)."""
    try:
        float(val)
        return True
    except (ValueError, TypeError):
        return False


def _detect_ids_from_file(
    file_path: str,
    result: dict,
    skip_rows: int = 1,
    sep: str = "\t",
) -> None:
    """Read first column values to detect gene ID type and organism."""
    try:
        with open(file_path, "r", errors="replace") as f:
            ids = []
            for i, line in enumerate(f):
                if i < skip_rows:
                    continue
                if i >= skip_rows + 200:
                    break
                parts = line.strip().split(sep)
                if parts:
                    gene_id = parts[0].strip().strip('"')
                    if gene_id:
                        ids.append(gene_id)
    except Exception:
        return

    if not ids:
        return

    result["gene_id_type"] = detect_gene_id_type(ids)
    result["organism"] = detect_organism(ids)


def detect_gene_id_type(gene_ids: list[str]) -> str | None:
    """Classify a list of gene IDs by their format."""
    if not gene_ids:
        return None

    sample = gene_ids[:100]

    ensembl_h = sum(1 for g in sample if _ENSEMBL_HUMAN.match(g))
    ensembl_m = sum(1 for g in sample if _ENSEMBL_MOUSE.match(g))
    entrez = sum(1 for g in sample if _ENTREZ.match(g))
    symbol = sum(1 for g in sample if _SYMBOL.match(g))

    total = len(sample)
    threshold = total * 0.5

    if ensembl_h > threshold or ensembl_m > threshold:
        return "ensembl"
    if symbol > threshold:
        return "symbol"
    if entrez > threshold:
        return "entrez"
    return None


def detect_organism(gene_ids: list[str]) -> str | None:
    """Detect organism from gene ID patterns."""
    if not gene_ids:
        return None

    sample = gene_ids[:100]
    human = sum(1 for g in sample if _ENSEMBL_HUMAN.match(g))
    mouse = sum(1 for g in sample if _ENSEMBL_MOUSE.match(g))

    if human > len(sample) * 0.3:
        return "human"
    if mouse > len(sample) * 0.3:
        return "mouse"

    # Check for known human gene symbols
    _HUMAN_MARKERS = {"TP53", "BRCA1", "EGFR", "MYC", "GAPDH", "ACTB", "PTEN"}
    id_set = set(g.upper() for g in sample)
    if id_set & _HUMAN_MARKERS:
        return "human"

    return None


def _infer_sample_groups(sample_names: list[str]) -> dict[str, list[str]] | None:
    """Try to infer experimental groups from sample column names.

    Looks for common patterns like: treated_1, treated_2, control_1, control_2
    or: tumor_s1, tumor_s2, normal_s1, normal_s2
    """
    if not sample_names or len(sample_names) < 2:
        return None

    # Try splitting on last underscore/number to find group prefix
    groups: dict[str, list[str]] = {}
    for name in sample_names:
        # Remove trailing numbers/replicates
        prefix = re.sub(r"[_\-.]?\d+$", "", name).strip("_- ")
        if not prefix:
            prefix = name
        groups.setdefault(prefix, []).append(name)

    # Only valid if we found 2-5 distinct groups
    if 2 <= len(groups) <= 5 and all(len(v) >= 2 for v in groups.values()):
        return groups

    return None
