"""Edit tool — exact string replacement with diff output."""

from __future__ import annotations

import difflib
from pathlib import Path

from pydantic import BaseModel, Field


class EditFileInput(BaseModel):
    """Input schema for the Edit tool."""

    path: str = Field(description="Workspace-relative or absolute path to edit.")
    old_str: str = Field(description="The exact string to find (must match exactly once).")
    new_str: str = Field(description="The replacement string.")


class EditFileTool:
    """Perform exact string replacement in a file.

    - ``old_str`` must match exactly ONCE in the file.
    - Returns a unified diff snippet around the change.
    - The file must already exist (read before edit).
    """

    name: str = "edit_file"
    description: str = (
        "Edit a file by replacing an exact string. The old_str must match "
        "exactly once in the file. Returns a diff snippet."
    )
    args_schema = EditFileInput

    def __init__(self, working_dir: Path) -> None:
        self.working_dir = working_dir

    def _resolve(self, path: str) -> Path:
        p = Path(path)
        if p.is_absolute():
            return p
        return self.working_dir / p

    def run(self, path: str, old_str: str, new_str: str) -> str:
        """Execute the edit_file tool."""
        resolved = self._resolve(path)

        if not resolved.exists():
            return f"Error: file does not exist: {path}. Read or create it first."

        if not resolved.is_file():
            return f"Error: not a regular file: {path}"

        try:
            content = resolved.read_text()
        except OSError as e:
            return f"Error reading file: {e}"

        # Check uniqueness
        count = content.count(old_str)
        if count == 0:
            return (
                f"Error: old_str not found in {path}. "
                "Make sure the string matches exactly (including whitespace)."
            )
        if count > 1:
            return (
                f"Error: old_str matches {count} times in {path}. "
                "Provide more context to make the match unique."
            )

        # Perform replacement
        new_content = content.replace(old_str, new_str, 1)

        try:
            resolved.write_text(new_content)
        except OSError as e:
            return f"Error writing file: {e}"

        # Generate diff snippet
        old_lines = content.splitlines(keepends=True)
        new_lines = new_content.splitlines(keepends=True)
        diff = difflib.unified_diff(
            old_lines,
            new_lines,
            fromfile=f"a/{path}",
            tofile=f"b/{path}",
            lineterm="",
        )
        diff_text = "\n".join(list(diff)[:50])  # Cap diff length

        return f"Successfully edited {path}\n\n{diff_text}"
