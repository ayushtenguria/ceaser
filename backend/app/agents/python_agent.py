"""Python agent node — generates pandas / plotly code for data analysis."""

from __future__ import annotations

import json as _json
import logging
import tempfile
from pathlib import Path

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import SystemMessage

from app.agents.state import AgentState

logger = logging.getLogger(__name__)

_DATA_DIR = Path(tempfile.gettempdir()) / "ceaser_data"
_DATA_DIR.mkdir(exist_ok=True)

_PYTHON_SYSTEM_PROMPT = """\
You are an expert data analyst who writes Python code using pandas and plotly.

{data_context}

RULES:
1. Write clean, self-contained Python code.
2. Available libraries: pandas, numpy, plotly, matplotlib, json, math, datetime, statistics, collections, itertools, re, csv, duckdb.
3. If you create a visualisation, store the Plotly figure object in a variable named exactly `fig`.
   Do NOT call `fig.show()` — the figure is captured automatically.
   Do NOT serialize the figure yourself (no fig.to_json(), no fig.to_dict(), no json.dumps(fig)).
   Just assign `fig = px.bar(...)` or `fig = px.scatter(...)` and stop. The system handles the rest.
4. Print any textual results with `print()` so they appear in stdout.
5. Return ONLY the Python code — no markdown fences, no explanations.
6. Handle potential errors gracefully (e.g., missing columns).
7. IMPORTANT: If a data file path is provided (like `df = pd.read_csv("...")`), use that
   to load the data. Do NOT hardcode data rows inline. The file contains ALL the rows.

DERIVED METRICS — if a column doesn't exist, COMPUTE it from available columns:
- Gross Margin = (selling_price - cost_price) / selling_price
- Markup = (selling_price - cost_price) / cost_price
- Profit = selling_price - cost_price
- Revenue = quantity * selling_price
- ROI = profit / cost_price
- Discount % = (compare_at_price - selling_price) / compare_at_price
When the user asks for a metric that doesn't exist as a column, identify the CLOSEST
matching columns and compute it. NEVER say "column doesn't exist" — always try to compute it first.

CHART SELECTION — pick the RIGHT chart type for the data:
- Bar chart (px.bar): comparing discrete categories (max 15 items). Use HORIZONTAL if labels are long.
- Histogram (px.histogram): distribution of a single numeric column.
- Line chart (px.line): trends over time. X-axis must be a date or ordered sequence.
- Scatter plot (px.scatter): relationship between 2 numeric variables.
- Pie chart (px.pie): proportions/shares (max 8 slices, group rest as "Other").
- Box plot (px.box): comparing distributions across groups.
- Heatmap (px.imshow): correlation matrices.

CHART RULES:
- If >15 categories → show only top 15, or group into "Other"
- If >1000 data points for scatter → sample to 500 points
- Always add a clear title
- For numbers: convert string columns to numeric with pd.to_numeric(col, errors='coerce')
- For long labels → use horizontal bar (orientation='h') or truncate
- Format numbers: use ,.0f for thousands, .1% for percentages
- ALWAYS ensure numeric columns are actually numeric types before plotting
- CRITICAL: Use ONLY the exact column names from the DataFrame. Check the columns list provided.
  If the SQL already grouped/aggregated data (e.g., columns are 'price_range', 'product_count'),
  use px.bar(df, x='price_range', y='product_count') — do NOT try to use raw columns that don't exist.
- If data is already aggregated (has count/sum/avg columns), use bar chart, NOT histogram.
  Histogram (px.histogram) is ONLY for raw, un-aggregated numeric data.

DUCKDB FOR LARGE FILES (IMPORTANT):
- When working with uploaded files (parquet via ceaser://), prefer DuckDB for aggregations.
- DuckDB reads parquet without loading the full file — 100x faster, 50x less memory.
- Use DuckDB for: GROUP BY, COUNT, SUM, AVG, filter, top-N, joins between files.
- Use pandas for: complex transforms, pivot tables, ML, custom visualizations.
- The `query_parquet()` helper is available in the runtime:
    result = query_parquet("SELECT region, SUM(revenue) FROM read_parquet('ceaser://...') GROUP BY region")
- Or use duckdb directly:
    import duckdb
    result = duckdb.sql("SELECT ... FROM read_parquet('ceaser://path') WHERE ...").fetchdf()
- The result is always a pandas DataFrame — use it with plotly normally.
- CRITICAL: For files with >100K rows, ALWAYS use duckdb.sql() for aggregations
  instead of loading the full DataFrame with pd.read_parquet().

DATA SAFETY:
- Always check if a column exists before using it: `if 'col' in df.columns`
- Convert types safely: `pd.to_numeric(df['col'], errors='coerce')` — NEVER use errors='ignore' (deprecated)
- Handle nulls: `df['col'].fillna(0)` or `df.dropna(subset=['col'])`
- For large DataFrames (>10K rows): aggregate first, then plot
- NEVER use `pd.to_numeric(col, errors='ignore')` — it is removed in pandas 3.0. Use errors='coerce' instead.

{file_context}
"""


def _save_table_data_as_csv(table_data: dict) -> str | None:
    """Save SQL result table_data to a temp CSV file. Returns the file path."""
    rows = table_data.get("rows", [])
    columns = table_data.get("columns", [])
    if not rows or not columns:
        return None

    import csv

    csv_path = _DATA_DIR / f"sql_result_{id(table_data)}.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    return str(csv_path)


async def generate_python(state: AgentState, llm: BaseChatModel) -> AgentState:
    """Use the LLM to generate Python code for analysis or visualisation."""
    data_pieces: list[str] = []
    preamble_lines: list[str] = ["import pandas as pd", "import plotly.express as px", ""]

    if state.get("table_data"):
        td = state["table_data"]
        columns = td.get("columns", [])
        rows = td.get("rows", [])
        total_rows = td.get("total_rows", len(rows))

        csv_path = _save_table_data_as_csv(td)

        if csv_path:
            preamble_lines.append(f'df = pd.read_csv("{csv_path}")')
            preamble_lines.append("# Auto-convert numeric-looking columns to numeric")
            preamble_lines.append("for _col in df.select_dtypes(include=['object']).columns:")
            preamble_lines.append("    _converted = pd.to_numeric(df[_col], errors='coerce')")
            preamble_lines.append(
                "    if _converted.notna().sum() > len(df) * 0.5:  # >50% are numeric"
            )
            preamble_lines.append("        df[_col] = _converted")
            preamble_lines.append("")

        sample_rows = rows[:5]
        sql_info = ""
        if state.get("sql_query"):
            sql_info = f"\nSQL query that produced this data:\n{state['sql_query']}\n"

        col_details = []
        for col in columns:
            sample_vals = [str(row.get(col, "")) for row in rows[:3] if row.get(col) is not None]
            col_details.append(f"  - '{col}' (sample values: {', '.join(sample_vals[:3])})")
        col_listing = "\n".join(col_details)

        data_pieces.append(
            f"SQL query returned a DataFrame `df` with {total_rows} rows.\n"
            f"EXACT COLUMNS (use ONLY these names — nothing else exists):\n{col_listing}\n"
            f"{sql_info}"
            f"The DataFrame is already loaded as `df`. Do NOT create a new DataFrame.\n"
            f"Sample (first 3 rows):\n{_json.dumps(sample_rows[:3], default=str, indent=2)}"
        )

    if state.get("execution_result"):
        data_pieces.append(f"Previous execution output:\n{state['execution_result']}")
    if state.get("schema_context"):
        data_pieces.append(state["schema_context"])

    data_context = "\n\n".join(data_pieces) if data_pieces else "No prior data context."
    file_context = ""
    if state.get("file_id"):
        file_context = (
            "A file has been uploaded. Its path and summary are included in the data context above."
        )

    messages = [
        SystemMessage(
            content=_PYTHON_SYSTEM_PROMPT.format(
                data_context=data_context,
                file_context=file_context,
            )
        ),
        *state["messages"],
    ]

    response = await llm.ainvoke(messages)
    raw_code: str = response.content.strip()  # type: ignore[union-attr]

    if raw_code.startswith("```"):
        lines = raw_code.split("\n")
        lines = [ln for ln in lines if not ln.strip().startswith("```")]
        raw_code = "\n".join(lines).strip()

    if "CODE PREAMBLE" in state.get("schema_context", ""):
        preamble_marker = "CODE PREAMBLE (prepend to all Python code):\n"
        ctx = state.get("schema_context", "")
        if preamble_marker in ctx:
            file_preamble = ctx.split(preamble_marker, 1)[1].strip()
            for line in file_preamble.split("\n"):
                stripped = line.strip()
                if not stripped:
                    continue  # skip blank lines — don't break
                if (
                    stripped.startswith(("import ", "from "))
                    or "= pd.read_" in stripped
                    or stripped.startswith("#")
                ):
                    if stripped not in preamble_lines:
                        preamble_lines.append(stripped)
                elif "→" in stripped or "CROSS" in stripped or "RELATIONSHIP" in stripped:
                    break
                elif stripped.startswith(("SELECTED", "AVAILABLE", "EXCEL", "FILE", "#")):
                    break  # hit the next section header
                else:
                    continue  # skip unrecognized lines instead of breaking

    has_parquet = any("pd.read_parquet" in p for p in preamble_lines)
    has_csv = any("pd.read_csv" in p for p in preamble_lines)
    has_file_load = has_parquet or has_csv

    code_lines = raw_code.split("\n")
    filtered = []
    for line in code_lines:
        stripped = line.strip()
        if stripped.startswith("import pandas") or stripped.startswith("import plotly"):
            continue
        # Filter out LLM-generated file reads when preamble already loads data
        if (
            has_file_load
            and (
                "pd.read_csv(" in stripped
                or "pd.read_excel(" in stripped
                or "pd.read_parquet(" in stripped
            )
            and "ceaser://" not in stripped
        ):
            continue
        if stripped.startswith("data = [") and has_file_load:
            continue
        filtered.append(line)

    final_code = "\n".join(preamble_lines) + "\n" + "\n".join(filtered)

    logger.info(
        "Generated Python code (%d chars, preamble=%d lines)", len(final_code), len(preamble_lines)
    )
    return {**state, "code_block": final_code}
