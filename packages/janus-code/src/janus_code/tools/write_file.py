"""Write tool — create or overwrite files, creating parent dirs."""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, Field


class WriteFileInput(BaseModel):
    """Input schema for the Write tool."""

    path: str = Field(description="Workspace-relative or absolute path to write.")
    content: str = Field(description="Full file content to write.")


class WriteFileTool:
    """Create or overwrite a file.

    - Creates parent directories as needed.
    - Intended for new files or full rewrites only — use Edit for diffs.
    - All paths resolved relative to ``working_dir``.
    """

    name: str = "write_file"
    description: str = (
        "Write a file to the workspace. Creates parent directories. "
        "Use Edit for modifying existing files (sends only the diff)."
    )
    args_schema = WriteFileInput

    def __init__(self, working_dir: Path) -> None:
        self.working_dir = working_dir

    def _resolve(self, path: str) -> Path:
        p = Path(path)
        if p.is_absolute():
            return p
        return self.working_dir / p

    def run(self, path: str, content: str) -> str:
        """Execute the write_file tool."""
        resolved = self._resolve(path)

        try:
            resolved.parent.mkdir(parents=True, exist_ok=True)
            resolved.write_text(content)
        except OSError as e:
            return f"Error writing file: {e}"

        lines = content.count("\n") + (1 if content and not content.endswith("\n") else 0)
        return f"Successfully wrote {resolved} ({lines} lines)"
