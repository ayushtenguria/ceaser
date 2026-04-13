"""Notebook Context Manager — tracks execution state across cells.

Maintains available DataFrames, user variables, and cell outputs so each
cell can reference results from previous cells.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class CellOutput:
    """Output from a completed cell."""

    cell_id: str
    cell_type: str
    variable_name: str
    text: str = ""
    table_data: dict | None = None
    chart_data: dict | None = None
    code: str | None = None
    dataframe_info: dict | None = None
    error: str | None = None


class NotebookContext:
    """Manages execution context across notebook cells.

    Tracks:
    - Available DataFrames (from file cells and prompt/code outputs)
    - User input variables (from input cells)
    - Previous cell outputs (text, tables, charts)
    - Code preamble (imports + DataFrame loads)
    """

    def __init__(self) -> None:
        self._user_inputs: dict[str, Any] = {}
        self._cell_outputs: list[CellOutput] = []
        self._dataframes: dict[str, dict] = {}
        self._variables: dict[str, Any] = {}
        self._code_preamble_lines: list[str] = [
            "import pandas as pd",
            "import numpy as np",
            "import plotly.express as px",
            "import plotly.graph_objects as go",
            "",
        ]
        self._connection_schema: str = ""

    def set_connection_schema(self, schema: str) -> None:
        """Set the DB schema context if a connection is configured."""
        self._connection_schema = schema

    def add_user_input(self, cell_id: str, label: str, value: Any) -> None:
        """Register a user input variable."""
        safe_label = label.lower().replace(" ", "_").replace("-", "_")
        self._user_inputs[safe_label] = value
        self._variables[safe_label] = value
        self._code_preamble_lines.append(f"{safe_label} = {json.dumps(value)}")

    def add_file(self, cell_id: str, var_name: str, parquet_path: str, info: dict) -> None:
        """Register a DataFrame loaded from a file cell."""
        self._dataframes[var_name] = {
            "path": parquet_path,
            "columns": info.get("columns", []),
            "rows": info.get("rows", 0),
        }
        self._code_preamble_lines.append(f'{var_name} = pd.read_parquet("{parquet_path}")')

    def add_cell_output(self, output: CellOutput) -> None:
        """Register the output of a completed cell."""
        self._cell_outputs.append(output)

        if output.dataframe_info and output.variable_name:
            self._dataframes[output.variable_name] = output.dataframe_info

    def resolve_template(self, text: str) -> str:
        """Replace {{variable}} placeholders with actual values."""
        import re

        def replacer(match: re.Match) -> str:
            key = match.group(1).strip()
            safe_key = key.lower().replace(" ", "_").replace("-", "_")
            if safe_key in self._variables:
                return str(self._variables[safe_key])
            return match.group(0)

        return re.sub(r"\{\{(\w[\w\s-]*)\}\}", replacer, text)

    def build_prompt_context(self) -> str:
        """Build the full context string for a prompt cell."""
        parts: list[str] = []

        if self._connection_schema:
            parts.append(self._connection_schema)

        if self._dataframes:
            parts.append("\nAVAILABLE DATAFRAMES:")
            parts.append("-" * 40)
            for var, info in self._dataframes.items():
                cols = info.get("columns", [])
                rows = info.get("rows", "?")
                parts.append(
                    f"  {var}: {rows} rows, columns: {', '.join(str(c) for c in cols[:15])}"
                )

        if self._user_inputs:
            parts.append("\nUSER INPUTS:")
            for key, val in self._user_inputs.items():
                parts.append(f"  {key} = {json.dumps(val)}")

        if self._cell_outputs:
            parts.append("\nPREVIOUS CELL RESULTS:")
            for out in self._cell_outputs[-5:]:
                parts.append(f"  [{out.cell_type}] {out.variable_name}: {out.text[:100]}")

        return "\n".join(parts)

    def build_code_preamble(self) -> str:
        """Build the Python code preamble for code/prompt cells."""
        return "\n".join(self._code_preamble_lines) + "\n\n"

    @property
    def variable_count(self) -> int:
        return len(self._variables)

    @property
    def dataframe_count(self) -> int:
        return len(self._dataframes)
