"""Lightweight SQL parser for translating agent-generated SQL to ad API calls.

Extracts table name, columns, WHERE filters, GROUP BY, ORDER BY, and LIMIT
from simple SELECT statements. Not a full SQL parser — handles the patterns
that the LLM agent generates for ad data queries.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ParsedQuery:
    """Parsed components of a SQL-like query."""

    table: str = ""
    columns: list[str] = field(default_factory=list)
    filters: dict[str, Any] = field(default_factory=dict)
    group_by: list[str] = field(default_factory=list)
    order_by: list[tuple[str, str]] = field(default_factory=list)
    limit: int | None = None
    aggregations: dict[str, str] = field(default_factory=dict)
    date_start: str | None = None
    date_end: str | None = None


def parse_ads_query(sql: str) -> ParsedQuery:
    """Parse a SQL SELECT statement into structured components.

    Handles patterns like:
        SELECT campaign_name, SUM(spend), SUM(conversions)
        FROM campaigns
        WHERE date >= '2026-04-01' AND status = 'ACTIVE'
        GROUP BY campaign_name
        ORDER BY spend DESC
        LIMIT 10
    """
    result = ParsedQuery()
    sql = sql.strip().rstrip(";")

    # Normalize whitespace
    sql_clean = re.sub(r"\s+", " ", sql)

    # Extract table from FROM clause
    from_match = re.search(r"\bFROM\s+(\w+)", sql_clean, re.IGNORECASE)
    if from_match:
        result.table = from_match.group(1).lower()

    # Extract columns from SELECT ... FROM
    select_match = re.search(r"SELECT\s+(.*?)\s+FROM\b", sql_clean, re.IGNORECASE)
    if select_match:
        cols_str = select_match.group(1)
        for col in cols_str.split(","):
            col = col.strip()
            # Detect aggregations: SUM(spend), COUNT(*), AVG(cpc)
            agg_match = re.match(
                r"(SUM|COUNT|AVG|MIN|MAX)\s*\(\s*(\*|\w+)\s*\)", col, re.IGNORECASE
            )
            if agg_match:
                func = agg_match.group(1).upper()
                field_name = agg_match.group(2)
                alias = f"{func.lower()}_{field_name}"
                result.aggregations[alias] = f"{func}({field_name})"
                result.columns.append(alias)
            else:
                # Handle aliases: col AS alias
                alias_match = re.match(r"(\w+)\s+AS\s+(\w+)", col, re.IGNORECASE)
                if alias_match:
                    result.columns.append(alias_match.group(2))
                else:
                    result.columns.append(col.strip())

    # Extract WHERE filters
    where_match = re.search(
        r"\bWHERE\s+(.*?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|$)", sql_clean, re.IGNORECASE
    )
    if where_match:
        where_clause = where_match.group(1).strip()
        _parse_where(where_clause, result)

    # Extract GROUP BY
    group_match = re.search(
        r"\bGROUP\s+BY\s+(.*?)(?:\bORDER\b|\bLIMIT\b|\bHAVING\b|$)", sql_clean, re.IGNORECASE
    )
    if group_match:
        result.group_by = [c.strip() for c in group_match.group(1).split(",")]

    # Extract ORDER BY
    order_match = re.search(r"\bORDER\s+BY\s+(.*?)(?:\bLIMIT\b|$)", sql_clean, re.IGNORECASE)
    if order_match:
        for part in order_match.group(1).split(","):
            part = part.strip()
            if " DESC" in part.upper():
                result.order_by.append((part.split()[0], "DESC"))
            else:
                result.order_by.append((part.split()[0], "ASC"))

    # Extract LIMIT
    limit_match = re.search(r"\bLIMIT\s+(\d+)", sql_clean, re.IGNORECASE)
    if limit_match:
        result.limit = int(limit_match.group(1))

    return result


def _parse_where(clause: str, result: ParsedQuery) -> None:
    """Parse WHERE conditions into filters and date ranges."""
    # Split on AND (simple — doesn't handle OR or nested conditions)
    conditions = re.split(r"\bAND\b", clause, flags=re.IGNORECASE)

    for cond in conditions:
        cond = cond.strip()

        # Date filters: date >= '2026-04-01'
        date_match = re.match(
            r"(\w*date\w*)\s*(>=|<=|>|<|=)\s*'(\d{4}-\d{2}-\d{2})'",
            cond,
            re.IGNORECASE,
        )
        if date_match:
            op = date_match.group(2)
            val = date_match.group(3)
            if op in (">=", ">"):
                result.date_start = val
            elif op in ("<=", "<"):
                result.date_end = val
            elif op == "=":
                result.date_start = val
                result.date_end = val
            continue

        # Equality: status = 'ACTIVE'
        eq_match = re.match(r"(\w+)\s*=\s*'([^']*)'", cond)
        if eq_match:
            result.filters[eq_match.group(1)] = eq_match.group(2)
            continue

        # Numeric equality: campaign_id = 12345
        num_match = re.match(r"(\w+)\s*=\s*(\d+)", cond)
        if num_match:
            result.filters[num_match.group(1)] = int(num_match.group(2))
            continue

        # IN clause: status IN ('ACTIVE', 'PAUSED')
        in_match = re.match(r"(\w+)\s+IN\s*\(([^)]+)\)", cond, re.IGNORECASE)
        if in_match:
            values = [v.strip().strip("'\"") for v in in_match.group(2).split(",")]
            result.filters[in_match.group(1)] = values
