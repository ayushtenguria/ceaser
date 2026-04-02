"""Genomics analysis module — dedicated pipeline for gene expression data.

Provides:
- Upload pipeline: parse count matrices, QC, normalize, annotate, build context
- Query pipeline: genomics-aware code generation, validation, repair
- Templates: validated code patterns for DE, volcano, PCA, GSEA, etc.
- Annotation: gene ID mapping (Ensembl/Symbol/Entrez) via local SQLite
"""

from app.agents.genomics.orchestrator import process_genomics_upload

__all__ = ["process_genomics_upload"]
