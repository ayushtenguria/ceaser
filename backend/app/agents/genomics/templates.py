"""Validated code templates for common genomics analyses.

Each template is a Python code string with {placeholder} variables.
The genomics agent matches queries to templates and fills parameters
via LLM, producing reliable code that uses correct API patterns.

Templates are MORE reliable than free-form LLM generation because:
- pydeseq2/gseapy have strict API contracts that LLMs hallucinate
- Templates are tested against real datasets
- LLM only needs to extract parameters, not invent code
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


# ── Template Registry ───────────────────────────────────────────────

TEMPLATES: dict[str, dict[str, Any]] = {}


def _register(name: str, pattern: str, code: str, params: list[str], description: str) -> None:
    TEMPLATES[name] = {
        "pattern": re.compile(pattern, re.IGNORECASE),
        "code": code,
        "params": params,
        "description": description,
    }


# ── Differential Expression ─────────────────────────────────────────

_register(
    "differential_expression",
    r"(?:differential|DE|differentially)\s+(?:express|gene)|(?:find|run|compute|perform)\s+(?:DE|differential)",
    """\
from pydeseq2.dds import DeseqDataSet
from pydeseq2.ds import DeseqStat

{preamble}

# Prepare count matrix (genes × samples → DESeq2 expects samples × genes)
counts_df = {matrix_var}
if counts_df.select_dtypes(include=['float']).shape[1] > 0:
    counts_df = counts_df.round().astype(int)

# Filter lowly expressed genes (keep genes with >10 counts in at least 3 samples)
gene_filter = (counts_df > 10).sum(axis=1) >= min(3, counts_df.shape[1] // 2)
counts_df = counts_df[gene_filter]
print(f"Genes after filtering: {{len(counts_df):,}} (removed {{(~gene_filter).sum():,}} lowly expressed)")

# Sample metadata
samples = counts_df.columns.tolist()
{group_assignment}

metadata = pd.DataFrame({{"condition": conditions}}, index=samples)

# Run DESeq2
dds = DeseqDataSet(
    counts=counts_df.T,  # DESeq2 expects samples × genes
    metadata=metadata,
    design_factors="condition",
)
dds.deseq2()
stat = DeseqStat(dds, contrast=("condition", "{test_group}", "{ref_group}"))
stat.summary()
results = stat.results_df

# Filter significant genes
results = results.dropna(subset=["padj"])
sig = results[(results["padj"] < {padj_threshold}) & (results["log2FoldChange"].abs() > {lfc_threshold})]
up = sig[sig["log2FoldChange"] > 0]
down = sig[sig["log2FoldChange"] < 0]

print(f"\\nDifferential Expression Results:")
print(f"  Total DE genes: {{len(sig):,}} (padj < {padj_threshold}, |log2FC| > {lfc_threshold})")
print(f"  Upregulated: {{len(up):,}}")
print(f"  Downregulated: {{len(down):,}}")
print(f"\\nTop 20 DE genes by significance:")
print(sig.sort_values("padj").head(20)[["baseMean", "log2FoldChange", "padj"]].to_string())
""",
    ["matrix_var", "test_group", "ref_group", "padj_threshold", "lfc_threshold", "group_assignment", "preamble"],
    "Run differential expression analysis using pydeseq2",
)


# ── Volcano Plot ────────────────────────────────────────────────────

_register(
    "volcano_plot",
    r"volcano",
    """\
{preamble}

results = {de_var}
results["-log10_padj"] = -np.log10(results["padj"].clip(lower=1e-300))

# Classify significance
results["significance"] = "Not significant"
results.loc[
    (results["padj"] < {padj_threshold}) & (results["log2FoldChange"] > {lfc_threshold}),
    "significance"
] = "Upregulated"
results.loc[
    (results["padj"] < {padj_threshold}) & (results["log2FoldChange"] < -{lfc_threshold}),
    "significance"
] = "Downregulated"

sig_counts = results["significance"].value_counts()
print(f"Volcano plot: {{sig_counts.get('Upregulated', 0)}} up, {{sig_counts.get('Downregulated', 0)}} down, {{sig_counts.get('Not significant', 0)}} NS")

fig = px.scatter(
    results.reset_index(),
    x="log2FoldChange",
    y="-log10_padj",
    color="significance",
    color_discrete_map={{"Upregulated": "#e74c3c", "Downregulated": "#3498db", "Not significant": "#d5d8dc"}},
    hover_name="gene_id" if "gene_id" in results.reset_index().columns else None,
    title="Volcano Plot — Differential Expression",
    labels={{"log2FoldChange": "log\u2082 Fold Change", "-log10_padj": "-log\u2081\u2080 adjusted p-value"}},
    opacity=0.6,
)
fig.add_hline(y=-np.log10({padj_threshold}), line_dash="dash", line_color="gray", annotation_text="padj={padj_threshold}")
fig.add_vline(x={lfc_threshold}, line_dash="dash", line_color="gray")
fig.add_vline(x=-{lfc_threshold}, line_dash="dash", line_color="gray")
fig.update_layout(legend_title_text="")
""",
    ["de_var", "padj_threshold", "lfc_threshold", "preamble"],
    "Generate a volcano plot from DE results",
)


# ── PCA Plot ────────────────────────────────────────────────────────

_register(
    "pca_plot",
    r"\bpca\b|principal\s+component",
    """\
from sklearn.decomposition import PCA

{preamble}

# Prepare data: log2 transform, transpose to samples × genes
expr = {matrix_var}
expr_log = np.log2(expr + 1)

# Remove zero-variance genes
gene_var = expr_log.var(axis=1)
expr_log = expr_log[gene_var > 0]

# PCA
pca = PCA(n_components=min(3, len(expr_log.columns)))
pc_coords = pca.fit_transform(expr_log.T)  # samples × PCs

pc_df = pd.DataFrame(
    pc_coords[:, :2],
    columns=["PC1", "PC2"],
    index=expr_log.columns,
)
pc_df["sample"] = pc_df.index

# Add group labels if available
{group_labels}

var1 = pca.explained_variance_ratio_[0] * 100
var2 = pca.explained_variance_ratio_[1] * 100

print(f"PCA: PC1 explains {{var1:.1f}}% variance, PC2 explains {{var2:.1f}}%")

fig = px.scatter(
    pc_df, x="PC1", y="PC2",
    color={color_col},
    hover_name="sample",
    title="PCA — Sample Clustering",
    labels={{"PC1": f"PC1 ({{var1:.1f}}%)", "PC2": f"PC2 ({{var2:.1f}}%)"}},
)
fig.update_traces(marker=dict(size=10))
""",
    ["matrix_var", "group_labels", "color_col", "preamble"],
    "PCA visualization of sample clustering",
)


# ── Heatmap ─────────────────────────────────────────────────────────

_register(
    "heatmap",
    r"heatmap|heat\s*map",
    """\
from scipy.cluster.hierarchy import linkage, leaves_list
from scipy.spatial.distance import pdist

{preamble}

expr = {matrix_var}
{gene_selection}

# Z-score normalize per gene (across samples)
z_scored = expr_subset.apply(lambda row: (row - row.mean()) / max(row.std(), 1e-10), axis=1)

# Cluster genes and samples
if len(z_scored) > 2 and len(z_scored.columns) > 2:
    gene_linkage = linkage(pdist(z_scored.values), method="ward")
    sample_linkage = linkage(pdist(z_scored.T.values), method="ward")
    gene_order = leaves_list(gene_linkage)
    sample_order = leaves_list(sample_linkage)
    z_scored = z_scored.iloc[gene_order, sample_order]

print(f"Heatmap: {{len(z_scored)}} genes × {{len(z_scored.columns)}} samples (z-scored)")

fig = go.Figure(data=go.Heatmap(
    z=z_scored.values,
    x=z_scored.columns.tolist(),
    y=z_scored.index.tolist(),
    colorscale="RdBu_r",
    zmid=0,
    colorbar=dict(title="Z-score"),
))
fig.update_layout(
    title="Expression Heatmap (z-score normalized, clustered)",
    xaxis_title="Samples",
    yaxis_title="Genes",
    height=max(400, len(z_scored) * 15 + 100),
)
""",
    ["matrix_var", "gene_selection", "preamble"],
    "Clustered expression heatmap with z-score normalization",
)


# ── MA Plot ─────────────────────────────────────────────────────────

_register(
    "ma_plot",
    r"\bma\s*plot\b|mean.*(?:fold|change)|bland.altman",
    """\
{preamble}

results = {de_var}
results["significance"] = "Not significant"
results.loc[
    (results["padj"] < {padj_threshold}) & (results["log2FoldChange"].abs() > {lfc_threshold}),
    "significance"
] = "Significant"

fig = px.scatter(
    results.reset_index(),
    x="baseMean",
    y="log2FoldChange",
    color="significance",
    color_discrete_map={{"Significant": "#e74c3c", "Not significant": "#d5d8dc"}},
    title="MA Plot",
    labels={{"baseMean": "Mean Expression (baseMean)", "log2FoldChange": "log\u2082 Fold Change"}},
    opacity=0.5,
    log_x=True,
)
fig.add_hline(y=0, line_color="black", line_width=1)
fig.add_hline(y={lfc_threshold}, line_dash="dash", line_color="gray")
fig.add_hline(y=-{lfc_threshold}, line_dash="dash", line_color="gray")
""",
    ["de_var", "padj_threshold", "lfc_threshold", "preamble"],
    "MA plot (mean expression vs fold change)",
)


# ── UMAP ────────────────────────────────────────────────────────────

_register(
    "umap_plot",
    r"\bumap\b",
    """\
import umap

{preamble}

expr = {matrix_var}
expr_log = np.log2(expr + 1)

# Remove zero-variance genes
gene_var = expr_log.var(axis=1)
expr_log = expr_log[gene_var > 0]

# UMAP
reducer = umap.UMAP(n_neighbors=min(15, len(expr_log.columns) - 1), min_dist=0.1, random_state=42)
embedding = reducer.fit_transform(expr_log.T)  # samples × genes → 2D

umap_df = pd.DataFrame(embedding, columns=["UMAP1", "UMAP2"], index=expr_log.columns)
umap_df["sample"] = umap_df.index
{group_labels}

fig = px.scatter(
    umap_df, x="UMAP1", y="UMAP2",
    color={color_col},
    hover_name="sample",
    title="UMAP — Sample Clustering",
)
fig.update_traces(marker=dict(size=10))
""",
    ["matrix_var", "group_labels", "color_col", "preamble"],
    "UMAP dimensionality reduction plot",
)


# ── Gene Set Enrichment ────────────────────────────────────────────

_register(
    "gsea_enrichment",
    r"(?:pathway|enrichment|gsea|go\s+term|kegg|reactome|gene\s+set)",
    """\
import gseapy as gp

{preamble}

results = {de_var}

# Get significant gene list
sig_genes = results[results["padj"] < {padj_threshold}]
{gene_list_prep}

print(f"Running enrichment on {{len(gene_list)}} genes...")

# Run enrichment analysis
try:
    enr = gp.enrich(
        gene_list=gene_list,
        gene_sets="{gene_set_db}",
        organism="{organism}",
        outdir=None,
        no_plot=True,
    )
    enr_results = enr.results
    enr_results = enr_results[enr_results["Adjusted P-value"] < 0.05].sort_values("Adjusted P-value")

    if len(enr_results) == 0:
        print("No significantly enriched pathways found (padj < 0.05).")
        print("Try relaxing thresholds or using a different gene set database.")
    else:
        print(f"\\n{{len(enr_results)}} enriched pathways (padj < 0.05):")
        for _, row in enr_results.head(15).iterrows():
            print(f"  {{row['Term']}}: p={{row['Adjusted P-value']:.2e}}, genes={{row['Overlap']}}")

        # Plot top enriched pathways
        top = enr_results.head(15).copy()
        top["-log10_padj"] = -np.log10(top["Adjusted P-value"].clip(lower=1e-300))

        fig = px.bar(
            top,
            x="-log10_padj",
            y="Term",
            orientation="h",
            title="Top Enriched Pathways",
            labels={{"-log10_padj": "-log\u2081\u2080 adjusted p-value", "Term": ""}},
            color="-log10_padj",
            color_continuous_scale="Reds",
        )
        fig.update_layout(yaxis=dict(autorange="reversed"), showlegend=False, height=max(400, len(top) * 30 + 100))
except Exception as e:
    print(f"Enrichment analysis failed: {{e}}")
    print("This may be due to network issues (gseapy needs internet for gene set databases).")
    print("Alternatively, try: gp.get_library_name() to list available local gene sets.")
""",
    ["de_var", "padj_threshold", "gene_list_prep", "gene_set_db", "organism", "preamble"],
    "Gene set enrichment / pathway analysis",
)


# ── QC Library Size ─────────────────────────────────────────────────

_register(
    "qc_library_size",
    r"(?:qc|quality)\s*(?:control|check|plot)|library\s*size|sequencing\s*depth",
    """\
{preamble}

expr = {matrix_var}

# Library sizes (total counts per sample)
lib_sizes = expr.sum(axis=0).sort_values(ascending=False)

print(f"Library size summary:")
print(f"  Samples: {{len(lib_sizes)}}")
print(f"  Min: {{int(lib_sizes.min()):,}}")
print(f"  Max: {{int(lib_sizes.max()):,}}")
print(f"  Median: {{int(lib_sizes.median()):,}}")
print(f"  Mean: {{int(lib_sizes.mean()):,}}")

# Gene detection rate
detection = (expr > 0).sum(axis=0) / len(expr)
print(f"\\nGene detection rate:")
print(f"  Min: {{detection.min():.1%}}")
print(f"  Max: {{detection.max():.1%}}")
print(f"  Median: {{detection.median():.1%}}")

# Library size bar chart
fig = go.Figure()
fig.add_trace(go.Bar(
    x=lib_sizes.index.tolist(),
    y=lib_sizes.values,
    marker_color=["#e74c3c" if s < lib_sizes.median() * 0.5 else "#3498db" for s in lib_sizes.values],
))
fig.update_layout(
    title="Library Sizes (Total Counts per Sample)",
    xaxis_title="Sample",
    yaxis_title="Total Counts",
    xaxis_tickangle=-45,
)
""",
    ["matrix_var", "preamble"],
    "QC plots — library sizes and gene detection",
)


# ── Gene Expression Boxplot ─────────────────────────────────────────

_register(
    "gene_boxplot",
    r"(?:express|level|boxplot)\s+(?:of|for)\s+(\w+)|(\w+)\s+expression",
    """\
{preamble}

expr = {matrix_var}
gene = "{gene_name}"

# Find gene (case-insensitive)
gene_matches = [g for g in expr.index if g.upper() == gene.upper()]
if not gene_matches:
    gene_matches = [g for g in expr.index if gene.upper() in g.upper()]

if not gene_matches:
    print(f"Gene '{{gene}}' not found in expression matrix.")
    print(f"Available genes (first 20): {{', '.join(expr.index[:20].tolist())}}")
else:
    matched = gene_matches[0]
    gene_expr = expr.loc[matched]

    plot_df = pd.DataFrame({{"expression": gene_expr.values, "sample": gene_expr.index}})
    {group_labels_boxplot}

    fig = px.box(
        plot_df, x={box_x}, y="expression",
        points="all",
        title=f"{{matched}} Expression",
        labels={{"expression": "Expression Level"}},
    )
""",
    ["matrix_var", "gene_name", "group_labels_boxplot", "box_x", "preamble"],
    "Boxplot of gene expression across samples/groups",
)


# ── Template Matching ───────────────────────────────────────────────

def match_template(query: str) -> str | None:
    """Match a user query to a template name. Returns template name or None."""
    for name, tmpl in TEMPLATES.items():
        if tmpl["pattern"].search(query):
            return name
    return None


def get_template(name: str) -> dict[str, Any] | None:
    """Get a template by name."""
    return TEMPLATES.get(name)


def list_templates() -> list[dict[str, str]]:
    """List all available templates with their descriptions."""
    return [
        {"name": name, "description": tmpl["description"]}
        for name, tmpl in TEMPLATES.items()
    ]
