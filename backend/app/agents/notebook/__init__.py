"""Notebook agents — orchestration, context management, and templates."""

from app.agents.notebook.context import NotebookContext
from app.agents.notebook.orchestrator import execute_notebook_cells
from app.agents.notebook.templates import generate_notebook_from_description

__all__ = [
    "execute_notebook_cells",
    "NotebookContext",
    "generate_notebook_from_description",
]
