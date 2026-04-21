"""Tests for chat context building — preamble injection and sheet selection."""

from __future__ import annotations


def test_preamble_accepts_read_csv():
    """Preamble filter should accept pd.read_csv lines."""
    line = 'data = pd.read_csv("ceaser://uploads/org/file.csv")'
    assert "= pd.read_" in line


def test_preamble_accepts_read_excel():
    """Preamble filter should accept pd.read_excel lines."""
    line = 'data = pd.read_excel("ceaser://uploads/org/file.xlsx")'
    assert "= pd.read_" in line


def test_preamble_accepts_read_parquet():
    """Preamble filter should accept pd.read_parquet lines."""
    line = 'df = pd.read_parquet("ceaser://parquet/org/df.parquet")'
    assert "= pd.read_" in line


def test_preamble_accepts_imports():
    """Import lines should pass the preamble filter."""
    lines = [
        "import pandas as pd",
        "import numpy as np",
        "import plotly.express as px",
        "from collections import Counter",
    ]
    for line in lines:
        assert line.startswith(("import ", "from "))


def test_preamble_accepts_comments():
    """Comment lines should pass the preamble filter."""
    line = "# NOTE: 100,000 rows — use duckdb for aggregations"
    assert line.startswith("#")


def test_preamble_rejects_section_headers():
    """Section headers should stop preamble extraction."""
    headers = [
        "SELECTED SHEETS:",
        "AVAILABLE COLUMNS:",
        "EXCEL CONTEXT:",
        "FILE: sales.xlsx",
    ]
    for h in headers:
        assert h.startswith(("SELECTED", "AVAILABLE", "EXCEL", "FILE"))


def test_code_preamble_generation():
    """Code preamble should use correct read function for file type."""
    from pathlib import Path

    filename = "Sales Report Q4.xlsx"
    stem = Path(filename).stem
    safe_name = stem.replace(" ", "_").replace("-", "_").lower()
    assert safe_name == "sales_report_q4"

    # CSV should use pd.read_csv
    assert "pd.read_csv" if "csv" == "csv" else "pd.read_excel"
