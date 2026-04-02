"""Python code validation node — pre-execution checks for generated code.

Mirrors the SQL validator: catches obvious issues before burning a sandbox
execution attempt.  Checks column references against the known DataFrame
schema embedded in the code preamble / schema_context.
"""

from __future__ import annotations

import ast
import logging
import re
from typing import Any

from app.agents.state import AgentState

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BLOCKED_CALLS = frozenset({
    "eval", "exec", "compile", "__import__", "open",
    "system", "popen", "getattr", "setattr", "delattr",
    "globals", "locals",
})

_BLOCKED_MODULES = frozenset({
    "subprocess", "shutil", "socket", "http", "urllib",
    "requests", "httpx", "aiohttp", "ftplib", "smtplib",
    "telnetlib", "xmlrpc", "ctypes", "multiprocessing",
    "webbrowser", "antigravity", "os",
})


def _extract_dataframe_columns(schema_context: str) -> dict[str, list[str]]:
    """Extract known DataFrame variable names and their columns from context.

    Parses patterns like:
        df_sales = pd.read_parquet("ceaser://...")
        DataFrame `df_sales` with columns: id, name, amount
        EXACT COLUMNS: 'id', 'name', 'amount'
        - 'id' (sample values: 1, 2, 3)
        Columns (6): item_code, description, ...
    """
    known: dict[str, list[str]] = {}

    # Pattern 1: read_parquet / read_csv assignments → extract var name
    for m in re.finditer(r"(\w+)\s*=\s*pd\.read_(?:parquet|csv)\(", schema_context):
        var = m.group(1)
        known.setdefault(var, [])

    # Pattern 2: "DataFrame `df_xxx` with ... columns:" blocks
    for m in re.finditer(
        r"DataFrame\s+`?(\w+)`?\s+with\s+\d+\s+(?:rows|columns).*?columns.*?:\s*\n((?:\s+-\s+'.+'\s*.*\n)+)",
        schema_context,
        re.IGNORECASE,
    ):
        var = m.group(1)
        cols = re.findall(r"'(\w+)'", m.group(2))
        if cols:
            known[var] = cols

    # Pattern 3: "Columns (N): col1, col2, col3" after a sheet/file header
    for m in re.finditer(
        r"(?:Columns|columns)\s*\(\d+\):\s*(.+?)(?:\n|$)", schema_context
    ):
        cols = [c.strip() for c in m.group(1).split(",") if c.strip()]
        if cols:
            # Attach to the most recently seen df variable or use "df"
            target = list(known.keys())[-1] if known else "df"
            known.setdefault(target, [])
            if not known[target]:
                known[target] = cols

    # Pattern 4: EXACT COLUMNS lines with quoted names
    for m in re.finditer(r"EXACT COLUMNS.*?:\n((?:\s+-\s+'.+?'.*\n)+)", schema_context, re.IGNORECASE):
        cols = re.findall(r"'([^']+)'", m.group(1))
        if cols:
            target = list(known.keys())[-1] if known else "df"
            known.setdefault(target, [])
            if not known[target]:
                known[target] = cols

    return known


def _check_column_refs(code: str, known_dfs: dict[str, list[str]]) -> list[str]:
    """Check if code references columns that don't exist in known DataFrames.

    Only flags issues when we have column info AND the reference is clearly wrong.
    Returns list of warning strings.
    """
    warnings: list[str] = []

    if not known_dfs:
        return warnings

    # Find df['column'] and df["column"] patterns
    col_refs = re.findall(r"(\w+)\[(?:'([^']+)'|\"([^\"]+)\")\]", code)

    for var, col_single, col_double in col_refs:
        col = col_single or col_double
        if not col:
            continue
        # Skip if var is not a known DataFrame
        if var not in known_dfs:
            continue
        known_cols = known_dfs[var]
        if not known_cols:
            continue
        if col not in known_cols:
            # Check case-insensitive match
            lower_known = {c.lower(): c for c in known_cols}
            if col.lower() in lower_known:
                warnings.append(
                    f"Column '{col}' on {var} — did you mean '{lower_known[col.lower()]}'? "
                    f"(case mismatch)"
                )
            else:
                # Find closest match
                close = _find_closest(col, known_cols)
                hint = f" Did you mean '{close}'?" if close else ""
                warnings.append(
                    f"Column '{col}' not found in {var}. "
                    f"Available: {', '.join(known_cols[:15])}.{hint}"
                )

    return warnings


def _find_closest(target: str, candidates: list[str], max_dist: int = 3) -> str | None:
    """Find closest column name by edit distance."""
    target_lower = target.lower()
    best, best_dist = None, max_dist + 1
    for c in candidates:
        d = _edit_distance(target_lower, c.lower())
        if d < best_dist:
            best, best_dist = c, d
    return best if best_dist <= max_dist else None


def _edit_distance(a: str, b: str) -> int:
    """Simple Levenshtein distance."""
    if len(a) > len(b):
        a, b = b, a
    dists: list[int] = list(range(len(a) + 1))
    for j, cb in enumerate(b):
        new_dists = [j + 1]
        for i, ca in enumerate(a):
            cost = 0 if ca == cb else 1
            new_dists.append(min(new_dists[i] + 1, dists[i + 1] + 1, dists[i] + cost))
        dists = new_dists
    return dists[-1]


def _check_blocked_imports(code: str) -> list[str]:
    """Check for imports of blocked modules."""
    issues: list[str] = []
    for m in re.finditer(r"^\s*(?:import|from)\s+(\w+)", code, re.MULTILINE):
        mod = m.group(1)
        if mod in _BLOCKED_MODULES:
            issues.append(f"Blocked module: '{mod}' is not allowed in sandbox.")
    return issues


def _check_blocked_calls(code: str) -> list[str]:
    """Check for dangerous function calls via AST."""
    issues: list[str] = []
    try:
        tree = ast.parse(code)
    except SyntaxError:
        issues.append("SyntaxError in generated code.")
        return issues

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = None
            if isinstance(func, ast.Name):
                name = func.id
            elif isinstance(func, ast.Attribute):
                name = func.attr
            if name and name in _BLOCKED_CALLS:
                issues.append(f"Blocked call: '{name}()' is not allowed.")

    return issues


def _check_fig_show(code: str) -> str | None:
    """Check if code calls fig.show() which won't work in sandbox."""
    if re.search(r"\bfig\.show\s*\(", code):
        return "Code calls fig.show() which has no effect in sandbox. Remove it."
    return None


def _check_read_excel_without_preamble(code: str) -> str | None:
    """Detect when code tries pd.read_excel/read_csv with a bare filename.

    This is the most common failure: LLM ignores the preamble and tries to
    load the original file by name, which doesn't exist in the sandbox.
    """
    # Check for read_excel("filename.xlsx") or read_csv("filename.csv")
    # but NOT ceaser:// refs or absolute paths
    pattern = r'pd\.read_(?:excel|csv)\(\s*["\']([^"\']+)["\']'
    for m in re.finditer(pattern, code):
        path = m.group(1)
        if not path.startswith(("ceaser://", "/", "http")) and "." in path:
            return (
                f"Code tries to load '{path}' directly, but the file is stored "
                f"in cloud storage. Use the DataFrame variable from the CODE "
                f"PREAMBLE (e.g., df_xxx = pd.read_parquet(\"ceaser://...\")) instead."
            )
    return None


# ---------------------------------------------------------------------------
# Main validation function
# ---------------------------------------------------------------------------

def validate_python(state: AgentState) -> AgentState:
    """Validate generated Python code before execution.

    Checks:
    1. Syntax validity (AST parse)
    2. Blocked imports and function calls
    3. Column references against known DataFrame schema
    4. Common mistakes (fig.show(), bare file reads)

    Returns state with error set if critical issues found, or warnings logged
    if non-critical.
    """
    code = state.get("code_block")
    if not code:
        return state

    schema_context = state.get("schema_context", "")
    errors: list[str] = []
    warnings: list[str] = []

    # 1. Check for bare file reads (most common failure)
    bare_read = _check_read_excel_without_preamble(code)
    if bare_read:
        errors.append(bare_read)

    # 2. Check blocked imports
    import_issues = _check_blocked_imports(code)
    errors.extend(import_issues)

    # 3. Check blocked calls and syntax
    call_issues = _check_blocked_calls(code)
    errors.extend(call_issues)

    # 4. Check column references
    known_dfs = _extract_dataframe_columns(schema_context)
    col_warnings = _check_column_refs(code, known_dfs)
    if col_warnings:
        # Column mismatches are errors — the code WILL fail
        errors.extend(col_warnings)

    # 5. Check fig.show()
    fig_issue = _check_fig_show(code)
    if fig_issue:
        warnings.append(fig_issue)

    if errors:
        error_msg = "Python validation failed:\n" + "\n".join(f"- {e}" for e in errors)
        if warnings:
            error_msg += "\nWarnings:\n" + "\n".join(f"- {w}" for w in warnings)
        logger.warning("Python validation: %d errors, %d warnings", len(errors), len(warnings))
        retry = state.get("retry_count", 0)
        return {
            **state,
            "error": error_msg,
            "retry_count": retry + 1,
        }

    if warnings:
        logger.info("Python validation: %d warnings (non-blocking)", len(warnings))

    return {**state, "error": None}
