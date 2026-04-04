"""Metric scanner — auto-discover potential metric definitions from schema.

Scans schema_cache columns for revenue/amount/count/price/date patterns
and proposes canonical metric definitions. Deterministic pattern matching
(no LLM dependency) for speed and reliability.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class MetricCandidate:
    """A proposed metric definition discovered from the schema."""
    name: str
    description: str
    sql_expression: str
    category: str
    source_table: str
    source_column: str
    confidence: float
    ambiguity_note: str | None = None


# Pattern rules: (column_name_regex, metric_name_template, sql_template, category, confidence)
_PATTERNS: list[tuple[re.Pattern, str, str, str, float]] = [
    # Revenue / sales
    (re.compile(r"(revenue|total_amount|gross_amount|net_amount|sales_amount|order_total|order_amount)", re.I),
     "Total {col_title}", "SUM({table}.{col})", "revenue", 0.9),

    # MRR / ARR
    (re.compile(r"\b(mrr|arr|monthly_recurring|annual_recurring)\b", re.I),
     "{col_upper}", "SUM({table}.{col})", "revenue", 0.95),

    # Count / quantity
    (re.compile(r"(quantity|qty|count|num_|number_of)", re.I),
     "Total {col_title}", "SUM({table}.{col})", "operations", 0.8),

    # Price / cost / rate
    (re.compile(r"(price|cost|rate|fee|charge|unit_price|unit_cost)", re.I),
     "Average {col_title}", "AVG({table}.{col})", "pricing", 0.75),

    # Discount
    (re.compile(r"(discount|rebate|markdown|allowance)", re.I),
     "Average {col_title}", "AVG({table}.{col})", "pricing", 0.7),

    # Margin / profit
    (re.compile(r"(margin|profit|markup|gross_profit|net_profit)", re.I),
     "Total {col_title}", "SUM({table}.{col})", "financial", 0.85),

    # Score / rating
    (re.compile(r"(score|rating|nps|csat|health_score|lead_score)", re.I),
     "Average {col_title}", "AVG({table}.{col})", "metrics", 0.8),
]

# Table-level patterns for COUNT metrics
_TABLE_COUNT_PATTERNS: list[tuple[re.Pattern, str, float]] = [
    (re.compile(r"(order|transaction|deal|sale|purchase)", re.I),
     "Total {table_title}s", 0.85),
    (re.compile(r"(customer|client|account|user|contact|lead)", re.I),
     "Total {table_title}s", 0.85),
    (re.compile(r"(ticket|issue|case|incident|support)", re.I),
     "Total {table_title}s", 0.8),
    (re.compile(r"(invoice|payment|refund)", re.I),
     "Total {table_title}s", 0.8),
]


def scan_schema_for_metrics(
    schema_cache: dict[str, Any],
    max_candidates: int = 20,
) -> list[MetricCandidate]:
    """Scan a schema cache and return proposed metric definitions.

    Pure pattern matching — no LLM calls, deterministic, < 10ms.
    """
    candidates: list[MetricCandidate] = []
    seen_names: set[str] = set()

    tables = schema_cache.get("tables", [])

    # Track columns per metric pattern for ambiguity detection
    pattern_matches: dict[str, list[tuple[str, str]]] = {}

    for table in tables:
        table_name = table.get("name", "")
        row_count = table.get("row_count", 0)
        columns = table.get("columns", [])

        # Skip empty or tiny tables
        if row_count is not None and row_count < 1:
            continue

        # Column-level patterns
        for col in columns:
            col_name = col.get("name", "")
            col_type = col.get("data_type", "").lower()

            # Only numeric columns for aggregation metrics
            is_numeric = any(t in col_type for t in (
                "int", "float", "numeric", "decimal", "double", "money", "real", "bigint",
            ))
            if not is_numeric:
                continue

            for pattern, name_tmpl, sql_tmpl, category, confidence in _PATTERNS:
                if pattern.search(col_name):
                    col_title = _humanize(col_name)
                    name = name_tmpl.format(
                        col_title=col_title,
                        col_upper=col_name.upper(),
                    )
                    sql = sql_tmpl.format(table=table_name, col=col_name)

                    # Track for ambiguity
                    key = pattern.pattern[:20]
                    pattern_matches.setdefault(key, []).append((table_name, col_name))

                    if name not in seen_names:
                        candidates.append(MetricCandidate(
                            name=name,
                            description=f"{name} from {table_name}.{col_name}",
                            sql_expression=sql,
                            category=category,
                            source_table=table_name,
                            source_column=col_name,
                            confidence=confidence,
                        ))
                        seen_names.add(name)
                    break  # One match per column

        # Table-level COUNT metrics
        for pattern, name_tmpl, confidence in _TABLE_COUNT_PATTERNS:
            if pattern.search(table_name):
                table_title = _humanize(table_name).rstrip("s")
                name = name_tmpl.format(table_title=table_title)

                if name not in seen_names:
                    candidates.append(MetricCandidate(
                        name=name,
                        description=f"Count of rows in {table_name}",
                        sql_expression=f"COUNT(*) FROM {table_name}",
                        category="operations",
                        source_table=table_name,
                        source_column="*",
                        confidence=confidence,
                    ))
                    seen_names.add(name)

    # Add ambiguity notes
    for key, matches in pattern_matches.items():
        if len(matches) > 1:
            for candidate in candidates:
                for table, col in matches:
                    if candidate.source_table == table and candidate.source_column == col:
                        others = [f"{t}.{c}" for t, c in matches if t != table or c != col]
                        candidate.ambiguity_note = (
                            f"Similar columns found: {', '.join(others)}. "
                            f"Confirm this is the canonical source."
                        )

    # Sort by confidence, limit
    candidates.sort(key=lambda c: c.confidence, reverse=True)
    return candidates[:max_candidates]


def _humanize(name: str) -> str:
    """Convert snake_case to Title Case."""
    return name.replace("_", " ").title()
