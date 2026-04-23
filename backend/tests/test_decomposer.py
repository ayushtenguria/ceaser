"""Tests for the query decomposer."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.agents.decomposer import decompose_query


@pytest.mark.asyncio
async def test_single_query_passthrough(mock_llm):
    """Single queries should pass through unchanged."""
    mock_llm.ainvoke = AsyncMock(return_value=MagicMock(content='["What is the average price?"]'))
    result = await decompose_query("What is the average price?", mock_llm)
    assert result == ["What is the average price?"]


@pytest.mark.asyncio
async def test_multi_query_decomposition(mock_llm):
    """Compound queries should be decomposed into sub-queries."""
    mock_llm.ainvoke = AsyncMock(
        return_value=MagicMock(content='["total revenue", "top 5 products", "monthly trend"]')
    )
    result = await decompose_query(
        "Show total revenue, top 5 products, and monthly trend", mock_llm
    )
    assert len(result) == 3
    assert "total revenue" in result[0]


@pytest.mark.asyncio
async def test_invalid_json_fallback(mock_llm):
    """Invalid JSON should fall back to the original query."""
    mock_llm.ainvoke = AsyncMock(return_value=MagicMock(content="This is not JSON at all"))
    result = await decompose_query("What is the data about?", mock_llm)
    assert result == ["What is the data about?"]


@pytest.mark.asyncio
async def test_markdown_fenced_json(mock_llm):
    """JSON wrapped in markdown fences should still parse."""
    mock_llm.ainvoke = AsyncMock(
        return_value=MagicMock(content='```json\n["query one", "query two"]\n```')
    )
    result = await decompose_query("Show me revenue breakdown and also plot the monthly trend over time", mock_llm)
    assert len(result) == 2


@pytest.mark.asyncio
async def test_max_three_subqueries(mock_llm):
    """Should cap at 3 sub-queries."""
    mock_llm.ainvoke = AsyncMock(return_value=MagicMock(content='["q1", "q2", "q3", "q4", "q5"]'))
    result = await decompose_query("Show revenue and costs and margins and trends and predictions", mock_llm)
    assert len(result) == 3
