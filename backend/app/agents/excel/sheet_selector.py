"""Sheet Selector — picks the most relevant sheets for a given question.

Pure deterministic keyword matching — NO LLM calls.
Scores each sheet by matching the question against sheet names, column names,
and sample values. Returns top 1-3 most relevant sheets.

Fast: ~1ms for 50 sheets. Consistent: same question → same selection.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# Common stop words to exclude from matching
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

# Question intent keywords that boost certain sheet types
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
    # The full context text for this sheet (from excel_context)
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
        # Generic question — return the largest sheets
        return sorted(sheets, key=lambda s: s.row_count, reverse=True)[:max_sheets]

    scores: list[SheetScore] = []

    for sheet in sheets:
        score_obj = _score_sheet(sheet, question_keywords)
        scores.append(score_obj)

    # Sort by score descending
    scores.sort(key=lambda s: s.score, reverse=True)

    # Select top N above minimum score
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

    # Fallback: if nothing scored well, return the largest sheet
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
        # Generate a brief description from column names
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
) -> str:
    """Build full context for only the selected sheets."""
    parts: list[str] = []

    parts.append("SELECTED SHEET DETAILS (most relevant to your question):")
    parts.append("=" * 50)

    for sheet in selected_sheets:
        if sheet.full_context_text:
            parts.append(sheet.full_context_text)
        else:
            # Build from metadata
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
        # Detect sheet header: "df_name  (N rows, M columns)"
        sheet_match = re.match(r"^(df_\w+)\s+\(([0-9,]+)\s+rows?,\s*(\d+)\s+columns?\)", line.strip())
        if sheet_match:
            # Save previous sheet
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

            # Parse column info: "    col_name: type   values: [...]"
            col_match = re.match(r"^\s{2,}(\w+):\s+(\w+)", line.strip())
            if col_match:
                col_name = col_match.group(1)
                col_type = col_match.group(2)
                current_sheet.column_names.append(col_name)
                current_sheet.column_types[col_name] = col_type

                # Extract sample values
                val_match = re.search(r"values:\s*\[(.+?)\]", line)
                if val_match:
                    try:
                        vals = [v.strip().strip("'\"") for v in val_match.group(1).split(",")]
                        current_sheet.sample_values[col_name] = vals[:5]
                    except Exception:
                        pass

    # Save last sheet
    if current_sheet:
        current_sheet.full_context_text = "\n".join(current_text_lines)
        sheets.append(current_sheet)

    return sheets


# ---------------------------------------------------------------------------
# Internal scoring
# ---------------------------------------------------------------------------

def _extract_keywords(question: str) -> list[str]:
    """Extract meaningful keywords from a question."""
    words = re.findall(r"[a-zA-Z_]+", question.lower())
    # Remove stop words, keep meaningful terms
    keywords = [w for w in words if w not in _STOP_WORDS and len(w) > 1]

    # Add intent-expanded keywords
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
        # Match against sheet name (highest weight)
        if kw in sheet_name_lower:
            score.score += 10
            score.matched_keywords.append(kw)
            score.match_reasons.append(f"sheet name contains '{kw}'")

        # Match against column names (high weight)
        for col in sheet.column_names:
            if kw in col.lower():
                score.score += 5
                if kw not in score.matched_keywords:
                    score.matched_keywords.append(kw)
                score.match_reasons.append(f"column '{col}' matches '{kw}'")
                break  # Don't double-count same keyword

        # Match against sample values (lower weight)
        for col, samples in sheet.sample_values.items():
            for sample in samples:
                if kw in str(sample).lower():
                    score.score += 2
                    if kw not in score.matched_keywords:
                        score.matched_keywords.append(kw)
                    break
            if kw in [s.matched_keywords[-1] if s.matched_keywords else "" for s in [score]]:
                break

    # Bonus for larger sheets (more likely to be main data)
    if sheet.row_count > 1000:
        score.score += 1
    if sheet.row_count > 10000:
        score.score += 2

    return score
