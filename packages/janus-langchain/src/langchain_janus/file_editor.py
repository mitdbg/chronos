"""Janus File Editor tool — transactional file operations on OverlayFS.

Provides view, create, str_replace, insert, and delete commands that
operate on the overlay's merged directory, giving agents an isolated
copy-on-write filesystem. All changes are staged in the overlay's
upperdir and only applied to the real project on commit.

Modeled after Anthropic's text_editor tool interface but backed by
a real POSIX-compatible transactional filesystem instead of in-memory
state or flat files.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any, Literal

from langchain_core.callbacks import CallbackManagerForToolRun
from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field


class JanusFileEditorInput(BaseModel):
    """Input schema for the Janus file editor tool."""

    command: Literal["view", "create", "str_replace", "insert", "delete"] = Field(
        description=(
            "The file operation to perform. "
            "'view': Read a file or list a directory. "
            "'create': Create or overwrite a file. "
            "'str_replace': Replace a string in a file. "
            "'insert': Insert text at a line number. "
            "'delete': Delete a file or directory."
        )
    )
    path: str = Field(
        description="Relative path within the project (e.g. 'src/main.py')."
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
            "The replacement string for 'str_replace', "
            "or text to insert for 'insert'."
        ),
    )
    insert_line: int | None = Field(
        default=None,
        description="0-based line number to insert text after (for 'insert').",
    )
    view_range: list[int] | None = Field(
        default=None,
        description="[start_line, end_line] 1-based range for 'view' (optional).",
    )


class JanusFileEditor(BaseTool):
    """Transactional file editor operating on an OverlayFS merged directory.

    All reads and writes go through the overlay's merged directory so
    changes are isolated from the real project until an explicit commit.

    The tool accepts a ``working_dir`` that should be the overlay's
    merged path (obtained from ``OverlayFSShim.get_working_directory()``
    or ``JanusContext.working_dir``).

    Example::

        editor = JanusFileEditor(working_dir=Path("/tmp/tar_overlayfs/.../merged"))
        result = editor.invoke({
            "command": "create",
            "path": "hello.py",
            "file_text": "print('hello world')",
        })
    """

    name: str = "janus_file_editor"
    description: str = (
        "Edit files in the transactional workspace. "
        "Supports view, create, str_replace, insert, and delete commands. "
        "All changes are isolated until the transaction is committed."
    )
    args_schema: type[BaseModel] = JanusFileEditorInput

    working_dir: Path
    """The overlay merged directory — all operations are relative to this."""

    max_file_size_bytes: int = 10 * 1024 * 1024  # 10 MB

    def _run(
        self,
        command: str,
        path: str,
        file_text: str | None = None,
        old_str: str | None = None,
        new_str: str | None = None,
        insert_line: int | None = None,
        view_range: list[int] | None = None,
        run_manager: CallbackManagerForToolRun | None = None,
    ) -> str:
        """Execute a file operation."""
        try:
            resolved = self._resolve_path(path)

            if command == "view":
                return self._handle_view(resolved, path, view_range)
            elif command == "create":
                if file_text is None:
                    return "Error: 'file_text' is required for the 'create' command."
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

    # ── Path resolution ──────────────────────────────────────────────

    def _resolve_path(self, path: str) -> Path:
        """Resolve relative path to absolute within working_dir.

        Raises:
            ValueError: If path escapes the working directory or contains
                traversal sequences.
        """
        if ".." in path:
            raise ValueError(f"Path traversal not allowed: {path}")

        # Strip leading slashes to make relative
        clean = path.lstrip("/")
        full = (self.working_dir / clean).resolve()

        # Security: ensure resolved path is within working_dir
        try:
            full.relative_to(self.working_dir)
        except ValueError:
            raise ValueError(f"Path escapes working directory: {path}")

        return full

    # ── Command handlers ─────────────────────────────────────────────

    def _handle_view(
        self,
        full_path: Path,
        display_path: str,
        view_range: list[int] | None,
    ) -> str:
        """View a file (with optional line range) or list a directory."""
        if full_path.is_dir():
            return self._list_directory(full_path, display_path)

        if not full_path.exists():
            return f"Error: File not found: {display_path}"

        if full_path.stat().st_size > self.max_file_size_bytes:
            max_mb = self.max_file_size_bytes / (1024 * 1024)
            return f"Error: File too large (>{max_mb:.0f} MB): {display_path}"

        try:
            content = full_path.read_text()
        except UnicodeDecodeError:
            return f"Error: Cannot read binary file: {display_path}"

        lines = content.split("\n")
        # Remove trailing empty line from trailing newline
        if lines and lines[-1] == "":
            lines = lines[:-1]

        if view_range:
            start = max(1, view_range[0]) - 1  # convert to 0-based
            end = min(len(lines), view_range[1]) if len(view_range) > 1 else len(lines)
            lines = lines[start:end]
            start_num = start
        else:
            start_num = 0

        formatted = [
            f"{start_num + i + 1:>4}|{line}" for i, line in enumerate(lines)
        ]
        return "\n".join(formatted)

    def _list_directory(self, full_path: Path, display_path: str) -> str:
        """List directory contents with type indicators."""
        if not full_path.exists():
            return f"Error: Directory not found: {display_path}"

        entries: list[str] = []
        try:
            for entry in sorted(full_path.iterdir()):
                name = entry.name
                # Skip OverlayFS internal files
                if name.startswith(".wh."):
                    continue
                if entry.is_dir():
                    entries.append(f"  {name}/")
                else:
                    entries.append(f"  {name}")
        except PermissionError:
            return f"Error: Permission denied: {display_path}"

        if not entries:
            return f"Directory '{display_path}' is empty."

        header = f"Directory: {display_path}\n"
        return header + "\n".join(entries)

    def _handle_create(
        self, full_path: Path, display_path: str, file_text: str
    ) -> str:
        """Create or overwrite a file."""
        full_path.parent.mkdir(parents=True, exist_ok=True)

        # Write content (ensure trailing newline for POSIX compliance)
        content = file_text if file_text.endswith("\n") else file_text + "\n"
        full_path.write_text(content)

        return f"File created: {display_path}"

    def _handle_str_replace(
        self,
        full_path: Path,
        display_path: str,
        old_str: str,
        new_str: str,
    ) -> str:
        """Replace exact string in a file (first occurrence only)."""
        if not full_path.exists():
            return f"Error: File not found: {display_path}"

        content = full_path.read_text()

        if old_str not in content:
            return f"Error: String not found in {display_path}"

        # Count occurrences for user info
        count = content.count(old_str)
        new_content = content.replace(old_str, new_str, 1)
        full_path.write_text(new_content)

        msg = f"String replaced in {display_path}"
        if count > 1:
            msg += f" (replaced 1 of {count} occurrences)"
        return msg

    def _handle_insert(
        self,
        full_path: Path,
        display_path: str,
        insert_line: int,
        text: str,
    ) -> str:
        """Insert text after a given line number (0-based)."""
        if not full_path.exists():
            return f"Error: File not found: {display_path}"

        content = full_path.read_text()
        lines = content.split("\n")
        had_trailing_newline = content.endswith("\n")
        if had_trailing_newline and lines and lines[-1] == "":
            lines = lines[:-1]

        new_lines = text.split("\n")
        insert_pos = max(0, min(insert_line, len(lines)))
        updated = lines[:insert_pos] + new_lines + lines[insert_pos:]

        new_content = "\n".join(updated)
        if had_trailing_newline:
            new_content += "\n"
        full_path.write_text(new_content)

        return f"Text inserted at line {insert_line} in {display_path}"

    def _handle_delete(self, full_path: Path, display_path: str) -> str:
        """Delete a file or directory."""
        if not full_path.exists():
            return f"Error: File not found: {display_path}"

        if full_path.is_dir():
            shutil.rmtree(full_path)
            return f"Directory deleted: {display_path}"
        else:
            full_path.unlink()
            return f"File deleted: {display_path}"
