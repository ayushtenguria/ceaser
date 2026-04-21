"""Shared test fixtures for the Ceaser backend test suite."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def mock_llm():
    """Mock LangChain BaseChatModel that returns canned responses."""
    llm = MagicMock()
    llm.ainvoke = AsyncMock(
        return_value=MagicMock(content='["What is the data about?"]')
    )
    return llm


@pytest.fixture
def sample_column_info():
    """Sample column_info dict as returned by parse_file."""
    return {
        "row_count": 100,
        "column_count": 3,
        "columns": [
            {"name": "id", "dtype": "int64", "null_count": 0, "unique_count": 100, "sample_values": [1, 2, 3]},
            {"name": "name", "dtype": "object", "null_count": 5, "unique_count": 80, "sample_values": ["Alice", "Bob"]},
            {"name": "price", "dtype": "float64", "null_count": 2, "unique_count": 50, "sample_values": [9.99, 19.99]},
        ],
    }


@pytest.fixture
def sample_callback_payload(sample_column_info):
    """Sample Fargate callback payload with all 6 fields."""
    return {
        "status": "ready",
        "column_info": sample_column_info,
        "excel_context": "SHEET: data\nColumns: id (int64), name (object), price (float64)\nRows: 100",
        "code_preamble": 'import pandas as pd\ndf = pd.read_parquet("ceaser://parquet/org/test.parquet")',
        "parquet_paths": {"df_data": "parquet/org/test.parquet"},
        "excel_metadata": {
            "insight": {"summary": "Test data with 100 rows", "suggestions": []},
            "quality_report": {"severity": "clean", "total_issues": 0, "items": []},
            "relationships": [],
        },
    }


@pytest.fixture
def file_id():
    """A stable test file ID."""
    return str(uuid.uuid4())
