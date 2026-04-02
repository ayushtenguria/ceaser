"""Sheet & Column Selector — picks the most relevant sheets and columns for a question.

Pure deterministic keyword matching — NO LLM calls.
Scores each sheet by matching the question against sheet names, column names,
and sample values. Returns top 1-3 most relevant sheets.

For large sheets (50+ columns), also filters columns by relevance so the
LLM prompt stays within token budget. Always preserves ID/key columns and
columns that match the question keywords.

Fast: ~1ms for 50 sheets. Consistent: same question → same selection.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_STOP_WORDS = frozenset({
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "shall",
    "should", "may", "might", "can", "could", "must", "need",
    "i", "me", "my", "we", "our", "you", "your", "he", "she", "it",
    "they", "them", "their", "this", "that", "these", "those",
    "what", "which", "who", "whom", "how", "when", "where", "why",
    "not", "no", "nor", "but", "or", "and", "if", "then", "else",
    "in", "on", "at", "to", "for", "of", "with", "by", "from",
    "about", "into", "through", "during", "before", "after",
    "above", "below", "between", "out", "off", "over", "under",
    "again", "further", "once", "here", "there", "all", "each",
    "every", "both", "few", "more", "most", "other", "some", "such",
    "only", "own", "same", "so", "than", "too", "very",
    "show", "me", "give", "tell", "find", "get", "list", "display",
    "please", "also", "just", "like",
    "data", "table", "sheet", "file", "column", "row", "value",
})

_INTENT_KEYWORDS = {
    "sales": ["sale", "sales", "revenue", "sold", "quantity", "order", "transaction"],
    "inventory": ["inventory", "stock", "count", "warehouse", "available", "oos", "out_of_stock"],
    "pricing": ["price", "pricing", "cost", "cp", "sp", "margin", "markup", "discount", "offer"],
    "vendor": ["vendor", "supplier", "manufacturer", "source"],
    "product": ["product", "sku", "item", "title", "catalog", "assortment"],
    "customer": ["customer", "client", "buyer", "b2b", "account"],
    "financial": ["cashflow", "cash", "flow", "payment", "invoice", "quotation", "purchase"],
    "analysis": ["analysis", "risk", "trend", "report", "summary", "region", "cohort"],
}


@dataclass
class SheetScore:
    """Relevance score for one sheet."""
    sheet_name: str
    score: float = 0.0
    matched_keywords: list[str] = field(default_factory=list)
    match_reasons: list[str] = field(default_factory=list)


@dataclass
class SheetMeta:
    """Lightweight metadata for one sheet (from cached context)."""
    name: str
    row_count: int = 0
    column_count: int = 0
    column_names: list[str] = field(default_factory=list)
    column_types: dict[str, str] = field(default_factory=dict)
    sample_values: dict[str, list] = field(default_factory=dict)
    full_context_text: str = ""


def select_relevant_sheets(
    question: str,
    sheets: list[SheetMeta],
    max_sheets: int = 3,
    min_score: float = 1.0,
) -> list[SheetMeta]:
    """Select the most relevant sheets for a question.

    Pure keyword matching — no LLM. Returns top 1-3 sheets.
    If no sheets score above min_score, returns the largest sheet (fallback).
    """
    if not sheets:
        return []

    if len(sheets) <= max_sheets:
        return sheets

    question_keywords = _extract_keywords(question)

    if not question_keywords:
        return sorted(sheets, key=lambda s: s.row_count, reverse=True)[:max_sheets]

    scores: list[SheetScore] = []

    for sheet in sheets:
        score_obj = _score_sheet(sheet, question_keywords)
        scores.append(score_obj)

    scores.sort(key=lambda s: s.score, reverse=True)

    selected_names = set()
    selected: list[SheetMeta] = []

    for sc in scores:
        if len(selected) >= max_sheets:
            break
        if sc.score >= min_score:
            sheet = next((s for s in sheets if s.name == sc.sheet_name), None)
            if sheet and sheet.name not in selected_names:
                selected.append(sheet)
                selected_names.add(sheet.name)

    if not selected:
        largest = sorted(sheets, key=lambda s: s.row_count, reverse=True)[0]
        selected = [largest]

    logger.info("Sheet selector: %d/%d sheets selected for '%s' — %s",
                len(selected), len(sheets), question[:50],
                [(s.name, next((sc.score for sc in scores if sc.sheet_name == s.name), 0)) for s in selected])

    return selected


def build_compact_summary(sheets: list[SheetMeta]) -> str:
    """Build a compact one-liner-per-sheet summary. Always sent to LLM."""
    lines = [
        "AVAILABLE DATA SHEETS:",
        f"Total: {len(sheets)} sheets, {sum(s.row_count for s in sheets):,} rows",
        "",
    ]

    for i, sheet in enumerate(sheets, 1):
        key_cols = [c for c in sheet.column_names[:6] if not c.startswith("unnamed")]
        col_hint = ", ".join(key_cols) if key_cols else "various columns"
        lines.append(f"  {i}. {sheet.name} ({sheet.row_count:,} rows, {sheet.column_count} cols) — {col_hint}")

    lines.append("")
    lines.append("The AI has access to ALL sheets above as pandas DataFrames.")
    lines.append("Use df_<filename>_<sheetname> to access any sheet.")

    return "\n".join(lines)


def build_selected_context(
    selected_sheets: list[SheetMeta],
    code_preamble: str = "",
    question: str = "",
) -> str:
    """Build full context for only the selected sheets.

    If *question* is provided and a sheet has many columns, applies column
    filtering to keep the context within token budget.
    """
    parts: list[str] = []

    parts.append("SELECTED SHEET DETAILS (most relevant to your question):")
    parts.append("=" * 50)

    for sheet in selected_sheets:
        # Apply column filtering for large sheets
        if question and sheet.column_count > _COLUMN_THRESHOLD:
            sheet = select_relevant_columns(question, sheet)

        if sheet.full_context_text:
            parts.append(sheet.full_context_text)
        else:
            parts.append(f"\nDataFrame: df_{sheet.name.lower().replace(' ', '_')} ({sheet.row_count:,} rows)")
            for col in sheet.column_names:
                col_type = sheet.column_types.get(col, "unknown")
                samples = sheet.sample_values.get(col, [])
                sample_str = f"  values: {samples[:5]}" if samples else ""
                parts.append(f"  {col}: {col_type}{sample_str}")

    if code_preamble:
        parts.append(f"\nCODE PREAMBLE (prepend to all Python code):\n{code_preamble}")

    return "\n".join(parts)


def parse_excel_context_to_sheets(excel_context: str) -> list[SheetMeta]:
    """Parse the stored excel_context string back into SheetMeta objects.

    This reconstructs sheet metadata from the text context stored in the DB.
    """
    sheets: list[SheetMeta] = []
    current_sheet: SheetMeta | None = None
    current_text_lines: list[str] = []

    for line in excel_context.split("\n"):
        sheet_match = re.match(r"^(df_\w+)\s+\(([0-9,]+)\s+rows?,\s*(\d+)\s+columns?\)", line.strip())
        if sheet_match:
            if current_sheet:
                current_sheet.full_context_text = "\n".join(current_text_lines)
                sheets.append(current_sheet)

            var_name = sheet_match.group(1)
            row_count = int(sheet_match.group(2).replace(",", ""))
            col_count = int(sheet_match.group(3))

            current_sheet = SheetMeta(
                name=var_name,
                row_count=row_count,
                column_count=col_count,
            )
            current_text_lines = [line]
            continue

        if current_sheet:
            current_text_lines.append(line)

            col_match = re.match(r"^\s{2,}(\w+):\s+(\w+)", line.strip())
            if col_match:
                col_name = col_match.group(1)
                col_type = col_match.group(2)
                current_sheet.column_names.append(col_name)
                current_sheet.column_types[col_name] = col_type

                val_match = re.search(r"values:\s*\[(.+?)\]", line)
                if val_match:
                    try:
                        vals = [v.strip().strip("'\"") for v in val_match.group(1).split(",")]
                        current_sheet.sample_values[col_name] = vals[:5]
                    except Exception:
                        pass

    if current_sheet:
        current_sheet.full_context_text = "\n".join(current_text_lines)
        sheets.append(current_sheet)

    return sheets


_COLUMN_THRESHOLD = 40
"""Sheets with more columns than this get column filtering applied."""

_MAX_SELECTED_COLUMNS = 35
"""Maximum columns to keep after filtering."""

_ALWAYS_KEEP_PATTERNS = frozenset({
    "id", "name", "email", "date", "created", "updated", "status", "type",
    "key", "code", "sku", "account", "phone", "title",
})
"""Column name fragments that are always kept (IDs, keys, dates, etc.)."""

_NUMERIC_BONUS = 3
"""Extra score for numeric columns — often needed for aggregation."""

_TEMPORAL_BONUS = 4
"""Extra score for date/time columns — often needed for trends."""


@dataclass
class ColumnScore:
    """Relevance score for one column."""
    name: str
    score: float = 0.0
    reasons: list[str] = field(default_factory=list)
    is_structural: bool = False  # ID/key/date — always kept


def select_relevant_columns(
    question: str,
    sheet: SheetMeta,
    max_columns: int = _MAX_SELECTED_COLUMNS,
    threshold: int = _COLUMN_THRESHOLD,
) -> SheetMeta:
    """Filter a sheet's columns to only the most relevant ones.

    Returns a NEW SheetMeta with filtered column_names, column_types,
    sample_values, and regenerated full_context_text.

    Skips filtering if the sheet has fewer columns than *threshold*.

    Selection strategy (no LLM):
    1. Structural columns (IDs, keys, dates, names) — always kept
    2. Keyword-matched columns — scored against question keywords
    3. Numeric columns — bonus for aggregation potential
    4. Top-N by score to fill remaining budget
    """
    if sheet.column_count <= threshold:
        return sheet

    keywords = _extract_keywords(question)

    scores: list[ColumnScore] = []
    for col in sheet.column_names:
        sc = _score_column(col, sheet.column_types.get(col, ""), sheet.sample_values.get(col, []), keywords)
        scores.append(sc)

    # Sort: structural first, then by score descending
    scores.sort(key=lambda s: (s.is_structural, s.score), reverse=True)

    # Select top columns within budget
    selected_names: list[str] = []
    for sc in scores:
        if len(selected_names) >= max_columns:
            break
        selected_names.append(sc.name)

    selected_set = set(selected_names)
    omitted = sheet.column_count - len(selected_names)

    # Build filtered SheetMeta
    filtered = SheetMeta(
        name=sheet.name,
        row_count=sheet.row_count,
        column_count=sheet.column_count,  # keep original count for awareness
        column_names=[c for c in sheet.column_names if c in selected_set],
        column_types={c: t for c, t in sheet.column_types.items() if c in selected_set},
        sample_values={c: v for c, v in sheet.sample_values.items() if c in selected_set},
    )

    # Rebuild context text with selected columns + note about omitted
    filtered.full_context_text = _rebuild_context_text(filtered, omitted)

    logger.info(
        "Column selector: %d/%d columns kept for '%s' on sheet %s",
        len(selected_names), sheet.column_count, question[:50], sheet.name,
    )

    return filtered


def _score_column(
    col_name: str,
    col_type: str,
    sample_values: list,
    keywords: list[str],
) -> ColumnScore:
    """Score a single column for relevance."""
    sc = ColumnScore(name=col_name)
    col_lower = col_name.lower()

    # 1. Structural columns (always keep)
    for pattern in _ALWAYS_KEEP_PATTERNS:
        if pattern in col_lower:
            sc.is_structural = True
            sc.score += 20
            sc.reasons.append(f"structural ({pattern})")
            break

    # 2. Keyword matching on column name
    for kw in keywords:
        if kw in col_lower:
            sc.score += 10
            sc.reasons.append(f"name matches '{kw}'")
        # Partial match (keyword is substring of column or vice versa)
        elif col_lower in kw or kw in col_lower.replace("_", ""):
            sc.score += 5
            sc.reasons.append(f"partial name match '{kw}'")

    # 3. Keyword matching on sample values
    sample_strs = [str(v).lower() for v in sample_values[:5]]
    for kw in keywords:
        if any(kw in sv for sv in sample_strs):
            sc.score += 3
            sc.reasons.append(f"sample value matches '{kw}'")
            break  # one match is enough

    # 4. Type bonuses
    type_lower = col_type.lower()
    if any(t in type_lower for t in ("int", "float", "numeric", "decimal", "money")):
        sc.score += _NUMERIC_BONUS
        sc.reasons.append("numeric type")
    if any(t in type_lower for t in ("date", "time", "timestamp")):
        sc.score += _TEMPORAL_BONUS
        sc.reasons.append("temporal type")
    # Also check column name for date hints
    if any(t in col_lower for t in ("date", "time", "year", "month", "day", "quarter", "week")):
        sc.score += _TEMPORAL_BONUS
        if not sc.is_structural:
            sc.is_structural = True

    return sc


def _rebuild_context_text(sheet: SheetMeta, omitted: int) -> str:
    """Rebuild the full_context_text for a filtered sheet."""
    lines = [
        f"\nDataFrame: {sheet.name} ({sheet.row_count:,} rows, {sheet.column_count} columns total"
        f" — showing {len(sheet.column_names)} most relevant)",
    ]

    for col in sheet.column_names:
        col_type = sheet.column_types.get(col, "unknown")
        samples = sheet.sample_values.get(col, [])
        sample_str = f"  values: {samples[:5]}" if samples else ""
        lines.append(f"  {col}: {col_type}{sample_str}")

    if omitted > 0:
        lines.append(f"  ... ({omitted} other columns available — ask about them if needed)")

    return "\n".join(lines)


def _extract_keywords(question: str) -> list[str]:
    """Extract meaningful keywords from a question."""
    words = re.findall(r"[a-zA-Z_]+", question.lower())
    keywords = [w for w in words if w not in _STOP_WORDS and len(w) > 1]

    expanded = list(keywords)
    for intent, intent_words in _INTENT_KEYWORDS.items():
        if any(kw in intent_words for kw in keywords):
            expanded.extend(intent_words)

    return list(set(expanded))


def _score_sheet(sheet: SheetMeta, keywords: list[str]) -> SheetScore:
    """Score a single sheet against the question keywords."""
    score = SheetScore(sheet_name=sheet.name)
    sheet_name_lower = sheet.name.lower()

    for kw in keywords:
        if kw in sheet_name_lower:
            score.score += 10
            score.matched_keywords.append(kw)
            score.match_reasons.append(f"sheet name contains '{kw}'")

        for col in sheet.column_names:
            if kw in col.lower():
                score.score += 5
                if kw not in score.matched_keywords:
                    score.matched_keywords.append(kw)
                score.match_reasons.append(f"column '{col}' matches '{kw}'")
                break

        for col, samples in sheet.sample_values.items():
            for sample in samples:
                if kw in str(sample).lower():
                    score.score += 2
                    if kw not in score.matched_keywords:
                        score.matched_keywords.append(kw)
                    break
            if kw in [s.matched_keywords[-1] if s.matched_keywords else "" for s in [score]]:
                break

    if sheet.row_count > 1000:
        score.score += 1
    if sheet.row_count > 10000:
        score.score += 2

    return score
