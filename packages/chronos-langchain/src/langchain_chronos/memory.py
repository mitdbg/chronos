"""Chronos Memory tool — persistent agent memory backed by overlay files.

Stores memory entries as files in a ``/memories`` subdirectory within
the overlay merged directory. This gives the agent a scratchpad that
is isolated within the transaction and can be committed or rolled back
along with all other file changes.

The tool provides view, create, str_replace, insert, delete, and list
commands — a subset of the file editor interface restricted to the
memories directory.

Modeled after Anthropic's ``memory`` tool but using the transactional
overlay filesystem for persistence and isolation.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from langchain_core.callbacks import CallbackManagerForToolRun
from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field


MEMORY_SUBDIR = "memories"

MEMORY_SYSTEM_PROMPT = """\
IMPORTANT: ALWAYS VIEW YOUR MEMORY DIRECTORY BEFORE DOING ANYTHING ELSE.

MEMORY PROTOCOL:
1. Use the `list` command of your `chronos_memory` tool to check for earlier progress.
2. Work on the task, recording status/progress/thoughts in memory as you go.
3. Before finishing, update your memory with final status and results.

ASSUME INTERRUPTION: Your context window might be reset at any moment, so
record any progress you cannot afford to lose in memory immediately."""


class ChronosMemoryInput(BaseModel):
    """Input schema for the Chronos memory tool."""

    command: Literal[
        "view", "create", "str_replace", "insert", "delete", "list"
    ] = Field(
        description=(
            "The memory operation to perform. "
            "'list': List all memory files. "
            "'view': Read a memory file. "
            "'create': Create or overwrite a memory file. "
            "'str_replace': Replace a string in a memory file. "
            "'insert': Insert text at a line in a memory file. "
            "'delete': Delete a memory file."
        )
    )
    path: str | None = Field(
        default=None,
        description=(
            "Memory file name (e.g. 'progress.md'). "
            "Required for all commands except 'list'."
        ),
    )
    file_text: str | None = Field(
        default=None,
        description="Full file content for the 'create' command.",
    )
    old_str: str | None = Field(
        default=None,
        description="The exact string to find for 'str_replace'.",
    )
    new_str: str | None = Field(
        default=None,
        description=(
            "Replacement string for 'str_replace', or text for 'insert'."
        ),
    )
    insert_line: int | None = Field(
        default=None,
        description="0-based line number to insert after (for 'insert').",
    )


class ChronosMemory(BaseTool):
    """Transactional memory tool backed by files in the overlay filesystem.

    Memory files are stored in ``<working_dir>/memories/`` and participate
    in the same transaction as all other file changes. On commit they
    persist to the real project directory; on abort they are discarded.

    The class-level ``SYSTEM_PROMPT`` constant contains a recommended
    prompt snippet that teaches the agent to always check memory first
    and persist progress incrementally.

    Example::

        mem = ChronosMemory(working_dir=Path("/tmp/tar_overlayfs/.../merged"))
        mem.invoke({"command": "create", "path": "notes.md", "file_text": "# Notes\\n"})
        mem.invoke({"command": "view", "path": "notes.md"})
        mem.invoke({"command": "list"})

        # Include in your agent's system prompt:
        system_prompt = f"...\\n\\n{ChronosMemory.SYSTEM_PROMPT}"
    """

    SYSTEM_PROMPT: str = MEMORY_SYSTEM_PROMPT
    """Recommended system prompt snippet for memory-aware agents.

    Append this to your agent's system prompt so the model learns to
    check existing memory files before starting work and to persist
    progress as it goes.  See ``MEMORY_SYSTEM_PROMPT`` at module level.
    """

    name: str = "chronos_memory"
    description: str = (
        "Persistent memory tool for recording progress, notes, and context. "
        "Files are stored in a 'memories' directory inside the transactional "
        "workspace and are isolated until commit."
    )
    args_schema: type[BaseModel] = ChronosMemoryInput

    working_dir: Path
    """The overlay merged directory."""

    def _get_memories_dir(self) -> Path:
        """Return and ensure the memories directory exists."""
        d = self.working_dir / MEMORY_SUBDIR
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _resolve_memory_path(self, path: str) -> Path:
        """Resolve a memory file name to an absolute path.

        Raises:
            ValueError: If path contains traversal sequences or escapes.
        """
        if ".." in path or "/" in path or "\\" in path:
            raise ValueError(
                f"Memory file names must be simple filenames, got: {path}"
            )
        mem_dir = self._get_memories_dir()
        full = (mem_dir / path).resolve()

        # Security check
        try:
            full.relative_to(mem_dir)
        except ValueError:
            raise ValueError(f"Path escapes memories directory: {path}")

        return full

    def _run(
        self,
        command: str,
        path: str | None = None,
        file_text: str | None = None,
        old_str: str | None = None,
        new_str: str | None = None,
        insert_line: int | None = None,
        run_manager: CallbackManagerForToolRun | None = None,
    ) -> str:
        """Execute a memory operation."""
        try:
            if command == "list":
                return self._handle_list()

            if path is None:
                return "Error: 'path' is required for all commands except 'list'."

            resolved = self._resolve_memory_path(path)

            if command == "view":
                return self._handle_view(resolved, path)
            elif command == "create":
                if file_text is None:
                    return "Error: 'file_text' is required for 'create'."
                return self._handle_create(resolved, path, file_text)
            elif command == "str_replace":
                if old_str is None:
                    return "Error: 'old_str' is required for 'str_replace'."
                return self._handle_str_replace(
                    resolved, path, old_str, new_str or ""
                )
            elif command == "insert":
                if insert_line is None:
                    return "Error: 'insert_line' is required for 'insert'."
                if new_str is None:
                    return "Error: 'new_str' is required for 'insert'."
                return self._handle_insert(resolved, path, insert_line, new_str)
            elif command == "delete":
                return self._handle_delete(resolved, path)
            else:
                return f"Error: Unknown command '{command}'."
        except Exception as e:
            return f"Error: {e}"

    # ── Handlers ─────────────────────────────────────────────────────

    def _handle_list(self) -> str:
        """List all memory files."""
        mem_dir = self._get_memories_dir()
        entries: list[str] = []
        for f in sorted(mem_dir.iterdir()):
            if f.is_file() and not f.name.startswith(".wh."):
                size = f.stat().st_size
                entries.append(f"  {f.name}  ({size} bytes)")

        if not entries:
            return "Memory directory is empty. No previous progress found."

        return "Memory files:\n" + "\n".join(entries)

    def _handle_view(self, full_path: Path, display_name: str) -> str:
        """View a memory file."""
        if not full_path.exists():
            return f"Memory file not found: {display_name}"

        try:
            content = full_path.read_text()
        except UnicodeDecodeError:
            return f"Error: Cannot read binary file: {display_name}"

        lines = content.split("\n")
        if lines and lines[-1] == "":
            lines = lines[:-1]

        formatted = [f"{i + 1:>4}|{line}" for i, line in enumerate(lines)]
        return "\n".join(formatted)

    def _handle_create(
        self, full_path: Path, display_name: str, file_text: str
    ) -> str:
        """Create or overwrite a memory file."""
        content = file_text if file_text.endswith("\n") else file_text + "\n"
        full_path.write_text(content)
        return f"Memory file created: {display_name}"

    def _handle_str_replace(
        self,
        full_path: Path,
        display_name: str,
        old_str: str,
        new_str: str,
    ) -> str:
        """Replace a string in a memory file."""
        if not full_path.exists():
            return f"Memory file not found: {display_name}"

        content = full_path.read_text()
        if old_str not in content:
            return f"Error: String not found in {display_name}"

        new_content = content.replace(old_str, new_str, 1)
        full_path.write_text(new_content)
        return f"String replaced in {display_name}"

    def _handle_insert(
        self,
        full_path: Path,
        display_name: str,
        insert_line: int,
        text: str,
    ) -> str:
        """Insert text after a line in a memory file."""
        if not full_path.exists():
            return f"Memory file not found: {display_name}"

        content = full_path.read_text()
        lines = content.split("\n")
        had_trailing_newline = content.endswith("\n")
        if had_trailing_newline and lines and lines[-1] == "":
            lines = lines[:-1]

        new_lines = text.split("\n")
        pos = max(0, min(insert_line, len(lines)))
        updated = lines[:pos] + new_lines + lines[pos:]

        new_content = "\n".join(updated)
        if had_trailing_newline:
            new_content += "\n"
        full_path.write_text(new_content)
        return f"Text inserted at line {insert_line} in {display_name}"

    def _handle_delete(self, full_path: Path, display_name: str) -> str:
        """Delete a memory file."""
        if not full_path.exists():
            return f"Memory file not found: {display_name}"

        full_path.unlink()
        return f"Memory file deleted: {display_name}"
