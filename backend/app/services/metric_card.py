"""Metric card detection — identify single-KPI query results.

When a query returns a single metric (1 row, 1-2 numeric values),
we emit a "metric_card" SSE event instead of a full table. The
frontend renders this as a big number with trend arrow.

Examples that trigger metric card:
  - "What's our total revenue?" → {value: 2400000, label: "total_revenue"}
  - "How many active users?" → {value: 5432, label: "active_users"}
  - "What's MRR this month vs last?" → {value: 54300, previous: 48200, label: "mrr"}
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def detect_metric_card(table_data: dict[str, Any]) -> dict[str, Any] | None:
    """Check if table_data represents a single KPI metric.

    Returns a metric_card dict if detected, None otherwise.

    Metric card format:
    {
        "value": 54300,
        "label": "Total Revenue",
        "formatted": "$54,300",
        "previous_value": 48200,       # optional — for trend
        "change_pct": 12.7,            # optional — computed from previous
        "change_direction": "up",      # "up", "down", "flat"
        "unit": "currency",            # "currency", "count", "percentage", "plain"
    }
    """
    rows = table_data.get("rows", [])
    columns = table_data.get("columns", [])
    total_rows = table_data.get("total_rows", len(rows))

    if not rows or not columns:
        return None

    # Must be 1-2 rows with 1-3 columns
    if total_rows > 2 or len(columns) > 4:
        return None

    # Single row, single value
    if total_rows == 1 and len(rows) == 1:
        row = rows[0]
        numeric_cols = _get_numeric_columns(row, columns)

        if len(numeric_cols) == 0:
            return None

        if len(numeric_cols) == 1:
            # Simple KPI: one number
            col, val = numeric_cols[0]
            return _build_card(col, val)

        if len(numeric_cols) == 2:
            # Could be value + previous, or two separate metrics
            col1, val1 = numeric_cols[0]
            col2, val2 = numeric_cols[1]

            # Check if one looks like a "previous" or "last" column
            if _is_comparison_pair(col1, col2):
                return _build_card(col1, val1, previous=val2)

            # Two separate metrics — use the first one
            return _build_card(col1, val1)

    # Two rows — might be current vs previous period
    if total_rows == 2 and len(rows) == 2:
        numeric_cols = _get_numeric_columns(rows[0], columns)
        if len(numeric_cols) == 1:
            col, current_val = numeric_cols[0]
            prev_cols = _get_numeric_columns(rows[1], columns)
            if prev_cols:
                _, prev_val = prev_cols[0]
                return _build_card(col, current_val, previous=prev_val)

    return None


def _get_numeric_columns(
    row: dict[str, Any],
    columns: list[str],
) -> list[tuple[str, float]]:
    """Extract columns with numeric values from a row."""
    result = []
    for col in columns:
        val = row.get(col)
        if val is None:
            continue
        try:
            num = float(val)
            result.append((col, num))
        except (ValueError, TypeError):
            continue
    return result


def _is_comparison_pair(col1: str, col2: str) -> bool:
    """Check if two columns look like current vs previous comparison."""
    comparison_words = ("previous", "prev", "last", "prior", "old", "before", "baseline")
    c2 = col2.lower()
    return any(w in c2 for w in comparison_words)


def _build_card(
    label: str,
    value: float,
    previous: float | None = None,
) -> dict[str, Any]:
    """Build a metric card dict."""
    unit = _detect_unit(label, value)
    formatted = _format_value(value, unit)

    card: dict[str, Any] = {
        "value": value,
        "label": _humanize_label(label),
        "formatted": formatted,
        "unit": unit,
    }

    if previous is not None and previous != 0:
        change_pct = ((value - previous) / abs(previous)) * 100
        card["previous_value"] = previous
        card["previous_formatted"] = _format_value(previous, unit)
        card["change_pct"] = round(change_pct, 1)
        card["change_direction"] = (
            "up" if change_pct > 0.5 else ("down" if change_pct < -0.5 else "flat")
        )

    return card


def _detect_unit(label: str, value: float) -> str:
    """Detect the unit type from the column label."""
    label_lower = label.lower()

    currency_words = (
        "revenue",
        "amount",
        "sales",
        "price",
        "cost",
        "mrr",
        "arr",
        "profit",
        "margin",
        "value",
        "spend",
        "budget",
        "payment",
        "income",
        "fee",
        "total_amount",
        "avg_order",
    )
    if any(w in label_lower for w in currency_words):
        return "currency"

    pct_words = (
        "rate",
        "pct",
        "percent",
        "ratio",
        "conversion",
        "churn",
        "retention",
        "ctr",
        "bounce",
    )
    if any(w in label_lower for w in pct_words):
        return "percentage"

    count_words = (
        "count",
        "total",
        "num",
        "number",
        "qty",
        "quantity",
        "users",
        "customers",
        "orders",
        "tickets",
        "items",
    )
    if any(w in label_lower for w in count_words):
        return "count"

    return "plain"


def _format_value(value: float, unit: str) -> str:
    """Format a number for display."""
    if unit == "currency":
        if abs(value) >= 1_000_000:
            return f"${value / 1_000_000:,.1f}M"
        if abs(value) >= 1_000:
            return f"${value:,.0f}"
        return f"${value:,.2f}"

    if unit == "percentage":
        return f"{value:.1f}%"

    if unit == "count":
        if abs(value) >= 1_000_000:
            return f"{value / 1_000_000:,.1f}M"
        if abs(value) >= 1_000:
            return f"{value:,.0f}"
        return f"{int(value):,}"

    # Plain
    if value == int(value):
        return f"{int(value):,}"
    return f"{value:,.2f}"


def _humanize_label(label: str) -> str:
    """Convert snake_case column name to readable label."""
    return label.replace("_", " ").title()
