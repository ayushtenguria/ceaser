"""Genomics Python agent — generates code for genomics analyses.

Template-first approach:
1. Match query to a known analysis template (DE, volcano, PCA, etc.)
2. Use LLM to extract parameters (groups, thresholds, gene names)
3. Fill template with parameters → reliable, tested code
4. Fall back to full LLM generation for novel queries

This is fundamentally different from the generic python_agent.py which
generates free-form pandas code. Here, pydeseq2/gseapy/scanpy have strict
API contracts that templates guarantee.
"""

from __future__ import annotations

import json
import logging
import re

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import SystemMessage, HumanMessage

from app.agents.genomics.prompts import GENOMICS_AGENT_PROMPT
from app.agents.genomics.templates import match_template, get_template
from app.agents.state import AgentState

logger = logging.getLogger(__name__)


async def generate_genomics_code(state: AgentState, llm: BaseChatModel) -> AgentState:
    """Generate Python code for genomics analysis.

    Strategy:
    1. Check if query matches a template → fill with LLM-extracted params
    2. If no template → generate from scratch with genomics-aware prompt
    """
    query = state.get("query", "")
    schema_context = state.get("schema_context", "")

    # Try template-based generation first
    template_name = match_template(query)
    if template_name:
        logger.info("Genomics agent: matched template '%s'", template_name)
        code = await _fill_template(template_name, query, schema_context, llm)
        if code:
            return {**state, "code_block": code, "analysis_type": template_name}

    # Fallback: full LLM generation with genomics prompt
    logger.info("Genomics agent: no template match, generating from scratch")
    code = await _generate_freeform(query, schema_context, state, llm)
    return {**state, "code_block": code, "analysis_type": "custom"}


async def _fill_template(
    template_name: str,
    query: str,
    schema_context: str,
    llm: BaseChatModel,
) -> str | None:
    """Fill a template by extracting parameters from the query via LLM."""
    tmpl = get_template(template_name)
    if not tmpl:
        return None

    # Extract preamble from schema context
    preamble = _extract_preamble(schema_context)
    matrix_var = _extract_matrix_var(preamble)
    sample_metadata = _extract_sample_metadata(schema_context)

    # Build parameter extraction prompt
    params_needed = [p for p in tmpl["params"] if p not in ("preamble", "matrix_var")]

    param_prompt = f"""\
Extract analysis parameters from the user's query for a {tmpl['description']}.

User query: "{query}"

Data context:
{schema_context[:2000]}

Matrix variable name: {matrix_var}
Detected sample groups: {json.dumps(sample_metadata) if sample_metadata else 'none detected'}

Return a JSON object with these parameters:
{json.dumps({p: f"<{p} value>" for p in params_needed}, indent=2)}

Parameter guidelines:
- test_group / ref_group: Use exact group names from sample metadata. If not clear, use first two groups.
- padj_threshold: Default 0.05 unless user specifies.
- lfc_threshold: Default 1.0 unless user specifies.
- gene_name: Extract from query (e.g., "TP53" from "expression of TP53").
- gene_set_db: Default "GO_Biological_Process_2023" for pathway analysis.
- organism: Default "human" unless context says otherwise.
- de_var: The DataFrame variable with DE results, usually "results" or "{matrix_var}".
- group_assignment: Python code to create a "conditions" list for DESeq2 metadata.
  Example: 'conditions = ["treated"]*3 + ["control"]*3'
- gene_selection: Python code to select genes for heatmap.
  Example: 'expr_subset = expr.nlargest(50, expr.var(axis=1).name)'
- gene_list_prep: Python code to prepare gene list for enrichment.
- group_labels: Python code to add group column to plot DataFrame.
- color_col: Column name to use for coloring in plots.

Return ONLY valid JSON — no markdown, no explanations."""

    try:
        response = await llm.ainvoke([
            SystemMessage(content="Extract analysis parameters. Return only valid JSON."),
            HumanMessage(content=param_prompt),
        ])

        raw = response.content.strip()  # type: ignore[union-attr]
        if raw.startswith("```"):
            lines = raw.split("\n")
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            raw = "\n".join(lines).strip()

        params = json.loads(raw)

    except Exception as exc:
        logger.warning("Template parameter extraction failed: %s", exc)
        # Use defaults
        params = _default_params(template_name, matrix_var, sample_metadata)

    # Fill template
    params["preamble"] = preamble
    params["matrix_var"] = matrix_var
    params.setdefault("padj_threshold", "0.05")
    params.setdefault("lfc_threshold", "1.0")
    params.setdefault("organism", "human")
    params.setdefault("gene_set_db", "GO_Biological_Process_2023")
    params.setdefault("de_var", "results")
    params.setdefault("color_col", '"group" if "group" in pc_df.columns else "sample"')

    # Fill group assignment if needed
    if "group_assignment" in tmpl["params"] and "group_assignment" not in params:
        params["group_assignment"] = _build_group_assignment(sample_metadata)

    if "group_labels" in tmpl["params"] and "group_labels" not in params:
        params["group_labels"] = _build_group_labels(sample_metadata, "pc_df")

    if "gene_selection" in tmpl["params"] and "gene_selection" not in params:
        params["gene_selection"] = "gene_var = expr.var(axis=1)\nexpr_subset = expr.loc[gene_var.nlargest(50).index]"

    if "gene_list_prep" in tmpl["params"] and "gene_list_prep" not in params:
        params["gene_list_prep"] = "gene_list = sig_genes.index.tolist()"

    try:
        code = tmpl["code"].format(**params)
        logger.info("Template '%s' filled successfully (%d chars)", template_name, len(code))
        return code
    except KeyError as exc:
        logger.warning("Template fill failed — missing param %s", exc)
        return None


async def _generate_freeform(
    query: str,
    schema_context: str,
    state: AgentState,
    llm: BaseChatModel,
) -> str:
    """Generate genomics code from scratch using the domain-aware prompt."""
    preamble = _extract_preamble(schema_context)

    data_context = schema_context
    file_context = ""
    if state.get("file_id"):
        file_context = "A genomics file is loaded. Use the DataFrame variables from the CODE PREAMBLE."

    messages = [
        SystemMessage(content=GENOMICS_AGENT_PROMPT.format(
            data_context=data_context,
            file_context=file_context,
        )),
        *state["messages"],
    ]

    response = await llm.ainvoke(messages)
    raw_code: str = response.content.strip()  # type: ignore[union-attr]

    # Strip markdown fences
    if raw_code.startswith("```"):
        lines = raw_code.split("\n")
        lines = [ln for ln in lines if not ln.strip().startswith("```")]
        raw_code = "\n".join(lines).strip()

    # Prepend preamble (dedup imports)
    preamble_lines = preamble.split("\n")
    code_lines = raw_code.split("\n")
    filtered = []
    for line in code_lines:
        stripped = line.strip()
        if stripped.startswith("import pandas") or stripped.startswith("import numpy"):
            continue
        if stripped.startswith("import plotly"):
            continue
        if "pd.read_parquet(" in stripped and "ceaser://" not in stripped:
            continue
        if "pd.read_csv(" in stripped or "pd.read_excel(" in stripped:
            continue
        filtered.append(line)

    final_code = "\n".join(preamble_lines) + "\n" + "\n".join(filtered)
    return final_code


# ── Helpers ─────────────────────────────────────────────────────────

def _extract_preamble(schema_context: str) -> str:
    """Extract CODE PREAMBLE or GENOMICS CODE PREAMBLE from context."""
    for marker in ("GENOMICS CODE PREAMBLE", "CODE PREAMBLE"):
        full_marker = f"{marker} (prepend to all Python code):\n"
        if full_marker in schema_context:
            preamble_text = schema_context.split(full_marker, 1)[1]
            # Take until next section header
            lines = []
            for line in preamble_text.split("\n"):
                stripped = line.strip()
                if stripped and stripped[0].isupper() and stripped.endswith(":") and "=" not in stripped:
                    break  # Hit next section
                if stripped.startswith(("SELECTED", "AVAILABLE", "EXCEL", "GENOMICS DATA")):
                    break
                lines.append(line)
            return "\n".join(lines).strip()
    return "import pandas as pd\nimport numpy as np\nimport plotly.express as px\nimport plotly.graph_objects as go\n"


def _extract_matrix_var(preamble: str) -> str:
    """Extract the DataFrame variable name from preamble."""
    match = re.search(r"(\w+)\s*=\s*pd\.read_parquet\(", preamble)
    if match:
        return match.group(1)
    return "df"


def _extract_sample_metadata(schema_context: str) -> dict[str, list[str]] | None:
    """Extract sample group metadata from context."""
    match = re.search(r"Sample Groups:\n((?:\s+\w+:.*\n)+)", schema_context)
    if not match:
        return None

    groups: dict[str, list[str]] = {}
    for line in match.group(1).strip().split("\n"):
        parts = line.strip().split(":", 1)
        if len(parts) == 2:
            group_name = parts[0].strip()
            samples = [s.strip() for s in parts[1].split(",") if s.strip() and "..." not in s]
            if samples:
                groups[group_name] = samples

    return groups if len(groups) >= 2 else None


def _default_params(
    template_name: str,
    matrix_var: str,
    sample_metadata: dict[str, list[str]] | None,
) -> dict:
    """Generate default parameters when LLM extraction fails."""
    params: dict = {
        "padj_threshold": "0.05",
        "lfc_threshold": "1.0",
        "organism": "human",
        "gene_set_db": "GO_Biological_Process_2023",
    }

    if sample_metadata and len(sample_metadata) >= 2:
        groups = list(sample_metadata.keys())
        params["test_group"] = groups[0]
        params["ref_group"] = groups[1]
        params["group_assignment"] = _build_group_assignment(sample_metadata)

    return params


def _build_group_assignment(sample_metadata: dict[str, list[str]] | None) -> str:
    """Build Python code that creates a conditions list for DESeq2."""
    if not sample_metadata:
        return 'conditions = ["group_A"] * (len(samples) // 2) + ["group_B"] * (len(samples) - len(samples) // 2)'

    lines = ["conditions = []", "for s in samples:"]
    for group, samples in sample_metadata.items():
        sample_list = ", ".join(f'"{s}"' for s in samples)
        lines.append(f'    if s in [{sample_list}]: conditions.append("{group}")')
    lines.append('    else: conditions.append("unknown")')
    return "\n".join(lines)


def _build_group_labels(
    sample_metadata: dict[str, list[str]] | None,
    df_var: str,
) -> str:
    """Build Python code that adds a group column to a plot DataFrame."""
    if not sample_metadata:
        return f'{df_var}["group"] = "all"'

    lines = [f'{df_var}["group"] = "unknown"']
    for group, samples in sample_metadata.items():
        sample_list = ", ".join(f'"{s}"' for s in samples)
        lines.append(
            f'{df_var}.loc[{df_var}.index.isin([{sample_list}]), "group"] = "{group}"'
        )
    return "\n".join(lines)
