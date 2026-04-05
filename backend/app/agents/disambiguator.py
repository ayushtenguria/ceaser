"""Disambiguator node — detects ambiguous terms and asks before guessing.

When a query term like "revenue" matches multiple columns across tables,
this node stops the pipeline and returns a disambiguation question.
The user selects the correct interpretation, and the query is re-submitted.

Deterministic detection (pattern matching, no LLM) for speed.
Checks memories first — if the user previously resolved this term, skips.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from app.agents.state import AgentState

logger = logging.getLogger(__name__)

# Terms that commonly cause ambiguity in business databases
_AMBIGUOUS_TERMS = {
    "revenue", "sales", "amount", "total", "cost", "price", "value",
    "profit", "margin", "discount", "rate", "count", "quantity",
    "status", "type", "category", "date", "name", "id",
    "customer", "account", "user", "order", "product",
}

# Minimum number of matches to trigger disambiguation
_MIN_MATCHES = 2


def disambiguate(state: AgentState) -> AgentState:
    """Check for ambiguous terms in the query against the schema.

    If ambiguity found: sets state["disambiguation"] and returns.
    If clear or already resolved: passes through unchanged.
    """
    query = state.get("query", "")
    schema_context = state.get("schema_context", "")
    resolution = state.get("disambiguation_resolution")

    # If user already provided a resolution, inject it and proceed
    if resolution:
        logger.info("Disambiguation resolved: %s", resolution[:100])
        return {
            **state,
            "schema_context": schema_context + f"\n\nUSER CLARIFICATION: {resolution}",
            "disambiguation": None,
        }

    # Check if memories already resolve this
    if _memories_resolve_ambiguity(schema_context, query):
        return state

    # Extract terms from query
    query_terms = _extract_query_terms(query)

    # Find ambiguous terms
    ambiguities = _find_ambiguities(query_terms, schema_context)

    if not ambiguities:
        return state

    # Build disambiguation question
    disambiguation = _build_disambiguation(ambiguities)

    logger.info("Disambiguation needed for %d terms: %s",
                len(ambiguities), [a["term"] for a in ambiguities])

    return {
        **state,
        "disambiguation": disambiguation,
        "next_action": "disambiguate",
    }


def _extract_query_terms(query: str) -> list[str]:
    """Extract meaningful terms from the user query."""
    words = re.findall(r"[a-zA-Z_]+", query.lower())
    return [w for w in words if w in _AMBIGUOUS_TERMS]


def _find_ambiguities(
    terms: list[str],
    schema_context: str,
) -> list[dict[str, Any]]:
    """Find terms that match multiple columns across tables."""
    ambiguities = []

    for term in terms:
        matches = _find_column_matches(term, schema_context)

        if len(matches) >= _MIN_MATCHES:
            # Check if the matches are in different tables (real ambiguity)
            tables = {m["table"] for m in matches}
            if len(tables) >= 2:
                ambiguities.append({
                    "term": term,
                    "matches": matches,
                })

    return ambiguities


def _find_column_matches(term: str, schema_context: str) -> list[dict[str, str]]:
    """Find all columns in the schema that match a term."""
    matches = []

    # Parse schema context for table/column info
    # Pattern: "Table: <name>" followed by "  <col_name>: <type>"
    current_table = ""

    for line in schema_context.split("\n"):
        stripped = line.strip()
        table_match = re.match(r"^Table:\s+(\w+)", stripped)
        if table_match:
            current_table = table_match.group(1)
            continue

        if current_table and stripped and not stripped.startswith(("-", "=")):
            col_match = re.match(r"^(\w+):\s+(\w+)", stripped)
            if col_match:
                col_name = col_match.group(1)
                col_type = col_match.group(2)

                # Check if term matches this column
                if term in col_name.lower():
                    # Extract sample values if present
                    samples = ""
                    val_match = re.search(r"values:\s*\[(.+?)\]", line)
                    if val_match:
                        samples = val_match.group(1)[:100]

                    # Extract alias if present
                    alias = ""
                    alias_match = re.search(r"\(≈\s*(.+?)\)", line)
                    if alias_match:
                        alias = alias_match.group(1)

                    matches.append({
                        "table": current_table,
                        "column": col_name,
                        "type": col_type,
                        "samples": samples,
                        "alias": alias,
                        "label": f"{current_table}.{col_name}" + (f" ({alias})" if alias else ""),
                    })

    return matches


def _memories_resolve_ambiguity(schema_context: str, query: str) -> bool:
    """Check if memory context already has term definitions.

    If the schema_context contains "USER CLARIFICATION" or domain_term
    memories that define ambiguous terms, skip disambiguation.
    """
    if "USER CLARIFICATION:" in schema_context:
        return True
    if "domain_term" in schema_context.lower() and "means" in schema_context.lower():
        return True
    return False


def _build_disambiguation(ambiguities: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a disambiguation question for the frontend."""
    questions = []

    for amb in ambiguities:
        term = amb["term"]
        options = []
        for i, match in enumerate(amb["matches"]):
            options.append({
                "id": f"{term}_{i}",
                "label": match["label"],
                "table": match["table"],
                "column": match["column"],
                "description": match.get("alias", ""),
                "sampleValues": match.get("samples", ""),
            })

        questions.append({
            "term": term,
            "question": f'Which "{term}" do you mean?',
            "options": options,
        })

    return {
        "type": "disambiguation",
        "questions": questions,
        "message": "I found multiple possible interpretations. Please clarify:",
    }
