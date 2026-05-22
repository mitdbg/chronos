"""Memory tool — read/write/list/append auto-memory files."""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, Field

from typing import Literal


MEMORY_DIR = ".chronos-code/memory"


class MemoryInput(BaseModel):
    """Input schema for the Memory tool."""

    command: Literal["read", "write", "list", "append"] = Field(
        description=(
            "The memory operation: "
            "'read' — read a topic file, "
            "'write' — create/overwrite a topic file, "
            "'list' — list all memory files, "
            "'append' — append to an existing file."
        )
    )
    path: str | None = Field(
        default=None,
        description="Topic file name (e.g. 'build-commands.md'). Required for read/write/append.",
    )
    content: str | None = Field(
        default=None,
        description="Content to write or append.",
    )


class MemoryTool:
    """Agent auto-memory scoped to ``.chronos-code/memory/``.

    - ``read``: Load a topic file.
    - ``write``: Create or overwrite a topic file.
    - ``append``: Append to an existing topic file.
    - ``list``: Show all memory files with first-line descriptions.

    All operations go through the working directory (OverlayFS shim when
    running inside a Chronos transaction).
    """

    name: str = "memory"
    description: str = (
        "Read/write agent auto-memory files in .chronos-code/memory/. "
        "Use to persist learnings, build commands, architecture notes."
    )
    args_schema = MemoryInput

    def __init__(self, working_dir: Path) -> None:
        self.working_dir = working_dir

    @property
    def _memory_path(self) -> Path:
        return self.working_dir / MEMORY_DIR

    def run(
        self,
        command: str,
        path: str | None = None,
        content: str | None = None,
    ) -> str:
        """Execute the memory tool."""
        if command == "list":
            return self._list()
        elif command == "read":
            if not path:
                return "Error: 'path' is required for read."
            return self._read(path)
        elif command == "write":
            if not path:
                return "Error: 'path' is required for write."
            if content is None:
                return "Error: 'content' is required for write."
            return self._write(path, content)
        elif command == "append":
            if not path:
                return "Error: 'path' is required for append."
            if content is None:
                return "Error: 'content' is required for append."
            return self._append(path, content)
        else:
            return f"Error: unknown command '{command}'. Use: read, write, list, append."

    def _list(self) -> str:
        """List all memory files with first-line descriptions."""
        mem_path = self._memory_path
        if not mem_path.exists():
            return "No memory files yet. Use 'write' to create one."

        entries: list[str] = []
        for f in sorted(mem_path.iterdir()):
            if f.is_file():
                try:
                    first_line = f.read_text().split("\n", 1)[0].strip()
                except OSError:
                    first_line = "(unreadable)"
                entries.append(f"  {f.name}: {first_line}")

        if not entries:
            return "No memory files yet. Use 'write' to create one."

        return f"Memory files ({len(entries)}):\n" + "\n".join(entries)

    def _read(self, path: str) -> str:
        """Read a specific memory file."""
        file_path = self._memory_path / path
        if not file_path.exists():
            return f"Error: memory file '{path}' does not exist."
        try:
            return file_path.read_text()
        except OSError as e:
            return f"Error reading memory file: {e}"

    def _write(self, path: str, content: str) -> str:
        """Create or overwrite a memory file."""
        self._memory_path.mkdir(parents=True, exist_ok=True)
        file_path = self._memory_path / path
        try:
            file_path.write_text(content)
        except OSError as e:
            return f"Error writing memory file: {e}"
        return f"Memory file '{path}' written ({len(content)} chars)."

    def _append(self, path: str, content: str) -> str:
        """Append to an existing memory file."""
        file_path = self._memory_path / path
        if not file_path.exists():
            return f"Error: memory file '{path}' does not exist. Use 'write' to create it first."
        try:
            with open(file_path, "a") as f:
                f.write(content)
        except OSError as e:
            return f"Error appending to memory file: {e}"
        return f"Appended to memory file '{path}' ({len(content)} chars)."
