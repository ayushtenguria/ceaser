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

Rules:
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
   Example: `df = pd.DataFrame(data)` where data is the provided rows.

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

    logger.info("Generated Python code (%d chars)", len(raw_code))
    return {**state, "code_block": raw_code}
