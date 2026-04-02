"""Relationship Mapper Agent — finds links between sheets and files.

Single job: given extracted sheets, find FK-like relationships.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import pandas as pd

from app.agents.excel.sheet_extractor import ExtractedSheet
from app.agents.excel.formula_extractor import FormulaExtractionResult

logger = logging.getLogger(__name__)

_VALUE_SAMPLE_SIZE = 10_000
_MIN_OVERLAP = 0.3


@dataclass
class Relationship:
    """A discovered relationship between two sheets."""
    source_sheet: str
    source_column: str
    target_sheet: str
    target_column: str
    confidence: float
    method: str
    rel_type: str = "many_to_one"


def map_relationships(
    sheets: list[ExtractedSheet],
    formulas: FormulaExtractionResult | None = None,
) -> list[Relationship]:
    """Discover relationships across sheets."""
    if len(sheets) < 2:
        return []

    rels: list[Relationship] = []

    if formulas:
        rels.extend(_from_formulas(sheets, formulas))

    rels.extend(_from_names(sheets))

    rels.extend(_from_id_patterns(sheets))

    existing = {(r.source_sheet, r.source_column, r.target_sheet, r.target_column) for r in rels}
    rels.extend(_from_value_overlap(sheets, existing))

    rels = _deduplicate(rels)
    logger.info("Mapped %d relationships across %d sheets", len(rels), len(sheets))
    return rels


def _from_formulas(sheets: list[ExtractedSheet], formulas: FormulaExtractionResult) -> list[Relationship]:
    """Relationships from formula cross-references."""
    rels = []
    for src, tgt in formulas.cross_sheet_references:
        rels.append(Relationship(
            source_sheet=src, source_column="(formula)",
            target_sheet=tgt, target_column="(formula)",
            confidence=0.9, method="formula_ref",
        ))
    return rels


def _from_names(sheets: list[ExtractedSheet]) -> list[Relationship]:
    """Relationships from matching column names across sheets."""
    rels = []
    col_index: dict[str, list[tuple[str, bool]]] = {}

    for sheet in sheets:
        for col in sheet.df.columns:
            norm = re.sub(r"[^a-z0-9]", "", col.lower())
            is_unique = sheet.df[col].nunique() / max(len(sheet.df), 1) > 0.9
            col_index.setdefault(norm, []).append((sheet.name, is_unique))

    for norm, occs in col_index.items():
        if len(occs) < 2:
            continue
        sorted_occs = sorted(occs, key=lambda x: -int(x[1]))
        parent = sorted_occs[0]
        for child in sorted_occs[1:]:
            if parent[0] != child[0]:
                rels.append(Relationship(
                    source_sheet=child[0], source_column=_find_col(sheets, child[0], norm),
                    target_sheet=parent[0], target_column=_find_col(sheets, parent[0], norm),
                    confidence=0.85, method="name_match",
                    rel_type="one_to_one" if child[1] else "many_to_one",
                ))
    return rels


def _from_id_patterns(sheets: list[ExtractedSheet]) -> list[Relationship]:
    """Match customer_id -> customers.id pattern."""
    rels = []
    sheet_map = {s.name.lower(): s for s in sheets}

    for sheet in sheets:
        for col in sheet.df.columns:
            if col.endswith("_id"):
                ref = col[:-3]
                for variant in [ref, ref + "s", ref + "es", ref.rstrip("s")]:
                    if variant in sheet_map and variant != sheet.name.lower():
                        target = sheet_map[variant]
                        id_cols = [c for c in target.df.columns if c in ("id", f"{variant}_id")]
                        if id_cols:
                            rels.append(Relationship(
                                source_sheet=sheet.name, source_column=col,
                                target_sheet=target.name, target_column=id_cols[0],
                                confidence=0.8, method="id_pattern",
                            ))
    return rels


def _from_value_overlap(sheets: list[ExtractedSheet], existing: set) -> list[Relationship]:
    """Relationships from value overlap analysis."""
    rels = []
    for i, sa in enumerate(sheets):
        for sb in sheets[i+1:]:
            for ca in sa.df.columns:
                for cb in sb.df.columns:
                    if (sa.name, ca, sb.name, cb) in existing:
                        continue
                    if not _dtypes_ok(sa.df[ca].dtype, sb.df[cb].dtype):
                        continue

                    score = _overlap_score(sa.df[ca], sb.df[cb])
                    if score >= _MIN_OVERLAP:
                        rels.append(Relationship(
                            source_sheet=sa.name, source_column=ca,
                            target_sheet=sb.name, target_column=cb,
                            confidence=min(score, 0.75), method="value_overlap",
                        ))
    return rels


def _overlap_score(a: pd.Series, b: pd.Series) -> float:
    """O(n) hash-set overlap score."""
    a = a.dropna()
    b = b.dropna()
    if len(a) > _VALUE_SAMPLE_SIZE:
        a = a.sample(_VALUE_SAMPLE_SIZE, random_state=42)
    if len(b) > _VALUE_SAMPLE_SIZE:
        b = b.sample(_VALUE_SAMPLE_SIZE, random_state=42)
    sa, sb = set(a.astype(str)), set(b.astype(str))
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / min(len(sa), len(sb))


def _dtypes_ok(a, b) -> bool:
    a_num = pd.api.types.is_numeric_dtype(a)
    b_num = pd.api.types.is_numeric_dtype(b)
    return (a_num == b_num)


def _find_col(sheets: list[ExtractedSheet], sheet_name: str, norm: str) -> str:
    for s in sheets:
        if s.name == sheet_name:
            for c in s.df.columns:
                if re.sub(r"[^a-z0-9]", "", c.lower()) == norm:
                    return c
    return norm


def _deduplicate(rels: list[Relationship]) -> list[Relationship]:
    seen: dict[tuple, Relationship] = {}
    for r in rels:
        key = tuple(sorted([(r.source_sheet, r.source_column), (r.target_sheet, r.target_column)]))
        if key not in seen or r.confidence > seen[key].confidence:
            seen[key] = r
    return sorted(seen.values(), key=lambda r: -r.confidence)
