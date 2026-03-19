"""Python agent node — generates pandas / plotly code for data analysis."""

from __future__ import annotations

import logging

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import SystemMessage

from app.agents.state import AgentState

logger = logging.getLogger(__name__)

_PYTHON_SYSTEM_PROMPT = """\
You are an expert data analyst who writes Python code using pandas and plotly.

Context (data summary or prior SQL result):
{data_context}

RULES:
1. Write clean, self-contained Python code.
2. Available libraries: pandas, numpy, plotly, matplotlib, json, math, datetime, statistics, collections, itertools, re, csv.
3. If you create a visualisation, store the Plotly figure object in a variable named exactly `fig`.
   Do NOT call `fig.show()` — the figure is captured automatically.
4. Print any textual results with `print()` so they appear in stdout.
5. If provided a CSV/Excel file path, read it with `pd.read_csv(...)` or `pd.read_excel(...)`.
6. Return ONLY the Python code — no markdown fences, no explanations.
7. Handle potential errors gracefully (e.g., missing columns).
8. IMPORTANT: If SQL results (table data) are provided in the context, create a DataFrame
   directly from that data — do NOT try to connect to any database or read from files.

DERIVED METRICS — if a column doesn't exist, COMPUTE it from available columns:
- Gross Margin = (selling_price - cost_price) / selling_price
  Look for columns: sp, selling_price, sp_shopify, price AND cp, cost_price, vendor_cp, cost
- Markup = (selling_price - cost_price) / cost_price
- Profit = selling_price - cost_price
- Revenue = quantity * selling_price
- ROI = profit / cost_price
- Discount % = (compare_at_price - selling_price) / compare_at_price
When the user asks for a metric that doesn't exist as a column, identify the CLOSEST
matching columns and compute it. For example:
- "gross margin" + columns have "vendor_cp" and "sp_shopify" → compute (sp_shopify - vendor_cp) / sp_shopify
- "revenue" + columns have "quantity_sold" and "sp_shopify" → compute quantity_sold * sp_shopify
NEVER say "column doesn't exist" — always try to compute it first.

CHART SELECTION — pick the RIGHT chart type for the data:
- Bar chart (px.bar): comparing discrete categories (max 15 items). Use HORIZONTAL (px.bar with orientation='h') if labels are long.
- Histogram (px.histogram): distribution of a single numeric column. Good for "how many products have X".
- Line chart (px.line): trends over time. When x-axis is a date or time series.
- Scatter plot (px.scatter): relationship between 2 numeric variables. Good for "X vs Y".
- Pie chart (px.pie): proportions/shares (max 8 slices, group rest as "Other").
- Treemap (px.treemap): hierarchical proportions (category → subcategory).
- Box plot (px.box): comparing distributions across groups.
- Heatmap (px.imshow): correlation matrices or 2D patterns.

CHART RULES:
- If >15 categories → show only top 15, or use horizontal bar, or group into "Other"
- If >1000 data points for scatter → sample to 500 points
- Always add a clear title with fig.update_layout(title=...)
- Use color to add a dimension when possible (e.g., color by category)
- For long product names → use horizontal bar (orientation='h') or truncate labels
- For distributions → histogram, NOT bar chart
- For percentages → pie if few categories, stacked bar if comparing across groups
- Format numbers: use ,.0f for thousands, .1% for percentages

DATA SAFETY:
- Always check if a column exists before using it: `if 'col' in df.columns`
- Convert types safely: `pd.to_numeric(df['col'], errors='coerce')`
- Handle nulls: `df['col'].fillna(0)` or `df.dropna(subset=['col'])`
- For large DataFrames (>10K rows): aggregate first, then plot

{file_context}
"""


async def generate_python(state: AgentState, llm: BaseChatModel) -> AgentState:
    """Use the LLM to generate Python code for analysis or visualisation."""
    # Build data context from SQL results or schema.
    import json as _json
    data_pieces: list[str] = []
    if state.get("table_data"):
        td = state["table_data"]
        rows_json = _json.dumps(td.get("rows", []), default=str)
        data_pieces.append(
            f"SQL query returned data with columns: {td.get('columns', [])}\n"
            f"Total rows: {td.get('total_rows', 0)}\n"
            f"Data (use this directly to build your DataFrame):\n{rows_json}"
        )
    if state.get("execution_result"):
        data_pieces.append(f"Previous execution output:\n{state['execution_result']}")
    if state.get("schema_context"):
        data_pieces.append(state["schema_context"])

    data_context = "\n\n".join(data_pieces) if data_pieces else "No prior data context."
    file_context = ""
    if state.get("file_id"):
        file_context = "A file has been uploaded. Its path and summary are included in the data context above."

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

    # Strip markdown fencing.
    if raw_code.startswith("```"):
        lines = raw_code.split("\n")
        lines = [ln for ln in lines if not ln.strip().startswith("```")]
        raw_code = "\n".join(lines).strip()

    # Prepend code preamble if present (loads Excel DataFrames from parquet)
    if "CODE PREAMBLE" in state.get("schema_context", ""):
        preamble_marker = "CODE PREAMBLE (prepend to all Python code):\n"
        ctx = state.get("schema_context", "")
        if preamble_marker in ctx:
            preamble = ctx.split(preamble_marker, 1)[1].strip()
            # Only keep import + read lines
            preamble_lines = [l for l in preamble.split("\n") if l.strip().startswith(("import ", "df_", "from "))]
            if preamble_lines:
                raw_code = "\n".join(preamble_lines) + "\n\n" + raw_code

    logger.info("Generated Python code (%d chars)", len(raw_code))
    return {**state, "code_block": raw_code}
