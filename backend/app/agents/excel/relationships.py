"""Relationship Discovery Agent — finds links between sheets and files.

Discovers foreign-key-like relationships using:
1. Column name matching (exact + fuzzy)
2. Formula cross-references
3. Value overlap analysis
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from app.agents.excel.parser import SheetResult

logger = logging.getLogger(__name__)

_VALUE_SAMPLE_SIZE = 10_000  # Sample size for value overlap checks
_MIN_OVERLAP_SCORE = 0.3    # Minimum overlap to consider a relationship
_FUZZY_THRESHOLD = 0.8      # Minimum similarity for fuzzy name matching


@dataclass
class Relationship:
    """A discovered relationship between two sheets/columns."""
    source_sheet: str
    source_column: str
    target_sheet: str
    target_column: str
    confidence: float           # 0.0 - 1.0
    discovery_method: str       # "name_match", "formula_ref", "value_overlap"
    relationship_type: str      # "many_to_one", "one_to_one", "many_to_many"


def discover_relationships(sheets: list[SheetResult]) -> list[Relationship]:
    """Discover relationships across all sheets.

    Runs three discovery strategies in order of reliability:
    1. Formula cross-references (highest confidence)
    2. Column name matching (high confidence)
    3. Value overlap analysis (medium confidence)
    """
    if len(sheets) < 2:
        return []

    relationships: list[Relationship] = []

    # Strategy 1: Formula references
    formula_rels = _discover_from_formulas(sheets)
    relationships.extend(formula_rels)

    # Strategy 2: Column name matching
    name_rels = _discover_from_names(sheets)
    relationships.extend(name_rels)

    # Strategy 3: Value overlap (only for candidates not already found)
    existing_pairs = {(r.source_sheet, r.source_column, r.target_sheet, r.target_column) for r in relationships}
    overlap_candidates = _find_overlap_candidates(sheets, existing_pairs)
    relationships.extend(overlap_candidates)

    # Deduplicate and sort by confidence
    relationships = _deduplicate(relationships)
    relationships.sort(key=lambda r: r.confidence, reverse=True)

    logger.info("Discovered %d relationships across %d sheets", len(relationships), len(sheets))
    return relationships


def _discover_from_formulas(sheets: list[SheetResult]) -> list[Relationship]:
    """Find relationships from formula cross-references like =Sheet1!B5."""
    rels: list[Relationship] = []
    sheet_names = {s.name for s in sheets}

    for sheet in sheets:
        for cell_ref, formula in sheet.formulas.items():
            # Match patterns: Sheet1!B5, 'Sheet Name'!B5, VLOOKUP(...Sheet1!...)
            refs = re.findall(r"['\"]?(\w[\w\s]*)['\"]?\!([A-Z]+)\d+", formula)
            for ref_sheet, ref_col_letter in refs:
                ref_sheet = ref_sheet.strip("'\"")
                if ref_sheet in sheet_names and ref_sheet != sheet.name:
                    rels.append(Relationship(
                        source_sheet=sheet.name,
                        source_column="(formula)",
                        target_sheet=ref_sheet,
                        target_column=f"col_{ref_col_letter}",
                        confidence=0.9,
                        discovery_method="formula_ref",
                        relationship_type="many_to_one",
                    ))

    return rels


def _discover_from_names(sheets: list[SheetResult]) -> list[Relationship]:
    """Find relationships by matching column names across sheets."""
    rels: list[Relationship] = []

    # Build column index: column_name -> [(sheet_name, is_unique)]
    col_index: dict[str, list[tuple[str, bool, int]]] = {}
    for sheet in sheets:
        for col in sheet.df.columns:
            nunique = sheet.df[col].nunique()
            is_unique = nunique / max(len(sheet.df), 1) > 0.9
            base_name = _normalize_col_name(col)
            col_index.setdefault(base_name, []).append((sheet.name, is_unique, nunique))

    # Exact matches: same normalized name in multiple sheets
    for base_name, occurrences in col_index.items():
        if len(occurrences) < 2:
            continue

        # Find the "parent" (unique values = likely PK) and "children"
        sorted_occs = sorted(occurrences, key=lambda x: (-int(x[1]), x[2]))
        parent = sorted_occs[0]

        for child in sorted_occs[1:]:
            if parent[0] == child[0]:
                continue

            # Find original column names
            parent_col = _find_original_col(sheets, parent[0], base_name)
            child_col = _find_original_col(sheets, child[0], base_name)

            if parent_col and child_col:
                rel_type = "one_to_one" if child[1] else "many_to_one"
                rels.append(Relationship(
                    source_sheet=child[0],
                    source_column=child_col,
                    target_sheet=parent[0],
                    target_column=parent_col,
                    confidence=0.85,
                    discovery_method="name_match",
                    relationship_type=rel_type,
                ))

    # Fuzzy matches: id-like columns
    _discover_id_patterns(sheets, rels)

    return rels


def _discover_id_patterns(sheets: list[SheetResult], rels: list[Relationship]) -> None:
    """Find relationships using ID-naming patterns.

    e.g., Sheet "orders" has "customer_id" -> Sheet "customers" has "id"
    """
    sheet_map = {s.name.lower(): s for s in sheets}
    existing = {(r.source_sheet, r.source_column) for r in rels}

    for sheet in sheets:
        for col in sheet.df.columns:
            col_lower = col.lower()
            # Check if column ends with _id and matches a sheet name
            if col_lower.endswith("_id"):
                ref_name = col_lower.replace("_id", "")
                # Check plural/singular variants
                for variant in [ref_name, ref_name + "s", ref_name + "es", ref_name.rstrip("s")]:
                    if variant in sheet_map and variant != sheet.name.lower():
                        target_sheet = sheet_map[variant]
                        # Find "id" column in target
                        id_cols = [c for c in target_sheet.df.columns if c.lower() in ("id", f"{variant}_id")]
                        if id_cols and (sheet.name, col) not in existing:
                            rels.append(Relationship(
                                source_sheet=sheet.name,
                                source_column=col,
                                target_sheet=target_sheet.name,
                                target_column=id_cols[0],
                                confidence=0.8,
                                discovery_method="name_match",
                                relationship_type="many_to_one",
                            ))


def _find_overlap_candidates(
    sheets: list[SheetResult],
    existing_pairs: set,
) -> list[Relationship]:
    """Find relationships by checking value overlap between columns."""
    rels: list[Relationship] = []

    for i, sheet_a in enumerate(sheets):
        for sheet_b in sheets[i + 1:]:
            for col_a in sheet_a.df.columns:
                for col_b in sheet_b.df.columns:
                    pair = (sheet_a.name, col_a, sheet_b.name, col_b)
                    reverse = (sheet_b.name, col_b, sheet_a.name, col_a)
                    if pair in existing_pairs or reverse in existing_pairs:
                        continue

                    # Only compare compatible types
                    dtype_a = sheet_a.df[col_a].dtype
                    dtype_b = sheet_b.df[col_b].dtype
                    if not _dtypes_compatible(dtype_a, dtype_b):
                        continue

                    score = _value_overlap_score(sheet_a.df[col_a], sheet_b.df[col_b])
                    if score >= _MIN_OVERLAP_SCORE:
                        rels.append(Relationship(
                            source_sheet=sheet_a.name,
                            source_column=col_a,
                            target_sheet=sheet_b.name,
                            target_column=col_b,
                            confidence=min(score, 0.75),
                            discovery_method="value_overlap",
                            relationship_type="many_to_one",
                        ))

    return rels


def _value_overlap_score(col_a: pd.Series, col_b: pd.Series) -> float:
    """Calculate value overlap between two columns using hash sets. O(n)."""
    # Sample for large columns
    a = col_a.dropna()
    b = col_b.dropna()

    if len(a) > _VALUE_SAMPLE_SIZE:
        a = a.sample(_VALUE_SAMPLE_SIZE, random_state=42)
    if len(b) > _VALUE_SAMPLE_SIZE:
        b = b.sample(_VALUE_SAMPLE_SIZE, random_state=42)

    set_a = set(a.astype(str))
    set_b = set(b.astype(str))

    if not set_a or not set_b:
        return 0.0

    overlap = len(set_a & set_b)
    return overlap / min(len(set_a), len(set_b))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_col_name(col: str) -> str:
    """Normalize column name for matching."""
    return re.sub(r"[^a-z0-9]", "", col.lower())


def _find_original_col(sheets: list[SheetResult], sheet_name: str, normalized: str) -> str | None:
    """Find the original column name matching a normalized name."""
    for sheet in sheets:
        if sheet.name == sheet_name:
            for col in sheet.df.columns:
                if _normalize_col_name(col) == normalized:
                    return col
    return None


def _dtypes_compatible(a, b) -> bool:
    """Check if two pandas dtypes are compatible for comparison."""
    a_num = pd.api.types.is_numeric_dtype(a)
    b_num = pd.api.types.is_numeric_dtype(b)
    if a_num and b_num:
        return True
    if not a_num and not b_num:
        return True
    return False


def _deduplicate(rels: list[Relationship]) -> list[Relationship]:
    """Remove duplicate relationships, keeping highest confidence."""
    seen: dict[tuple, Relationship] = {}
    for rel in rels:
        key = tuple(sorted([(rel.source_sheet, rel.source_column), (rel.target_sheet, rel.target_column)]))
        if key not in seen or rel.confidence > seen[key].confidence:
            seen[key] = rel
    return list(seen.values())
