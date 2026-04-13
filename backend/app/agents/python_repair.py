"""Python Repair Agent — fixes broken Python code using the exact error traceback.

Mirrors repair_sql: takes the failed code + exact error and surgically fixes it,
rather than regenerating from scratch (which often produces the same bug).
"""

from __future__ import annotations

import logging

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from app.agents.state import AgentState

logger = logging.getLogger(__name__)

_REPAIR_PROMPT = """\
You are a Python code repair specialist for data analysis. Code failed with an error.
Your job is to FIX the code — not rewrite it from scratch.

Failed code:
```python
{code}
```

Error:
{error}

Data context:
{schema}

Common fixes:
- KeyError / "column not found" → check exact column name from the data context; use the
  EXACT names listed (case-sensitive). Look for the closest matching column.
- FileNotFoundError → use the DataFrame variable from the CODE PREAMBLE instead of
  reading the file directly. The data is already loaded via pd.read_parquet("ceaser://...").
- TypeError: "unsupported operand" → convert column to numeric with pd.to_numeric(df['col'], errors='coerce')
- ValueError: "could not convert string to float" → use pd.to_numeric(col, errors='coerce')
- AttributeError: "Series has no attribute 'X'" → check pandas API; might need different method
- IndexError → add bounds check or use .iloc safely
- pd.read_excel / pd.read_csv with bare filename → replace with the parquet variable from preamble
- fig.show() → remove it; figure is captured automatically
- errors='ignore' → replace with errors='coerce' (pandas 3.0 removed 'ignore')
- merge/join errors → check that join columns exist in both DataFrames and have matching types

Rules:
1. Return ONLY the fixed Python code — no explanations, no markdown fences
2. Keep the code as close to the original as possible
3. Only fix the specific error — don't restructure the whole script
4. Preserve all imports and the preamble (pd.read_parquet lines)
5. NEVER replace pd.read_parquet("ceaser://...") with pd.read_excel or pd.read_csv
"""


async def repair_python(state: AgentState, llm: BaseChatModel) -> AgentState:
    """Attempt to repair failed Python code using the error traceback."""
    code = state.get("code_block", "")
    error = state.get("error", "")
    schema = state.get("schema_context", "")

    if not code or not error:
        return state

    logger.info("Python repair agent: fixing error: %s", error[:150])

    messages = [
        SystemMessage(
            content=_REPAIR_PROMPT.format(
                code=code,
                error=error,
                schema=schema[:4000],
            )
        ),
        HumanMessage(content=f"Fix this Python code. The error was: {error}"),
    ]

    response = await llm.ainvoke(messages)
    fixed_code: str = response.content.strip()  # type: ignore[union-attr]

    # Strip markdown fences if present
    if fixed_code.startswith("```"):
        lines = fixed_code.split("\n")
        lines = [ln for ln in lines if not ln.strip().startswith("```")]
        fixed_code = "\n".join(lines).strip()

    # Basic sanity: must contain import pandas or read_parquet
    if not fixed_code or len(fixed_code) < 20:
        logger.warning("Python repair agent produced too-short code, keeping original")
        return state

    logger.info("Python repair agent: fixed code (%d chars → %d chars)", len(code), len(fixed_code))
    return {
        **state,
        "code_block": fixed_code,
        "error": None,
    }
