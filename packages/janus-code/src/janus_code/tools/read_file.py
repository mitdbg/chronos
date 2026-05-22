"""ReadFile tool — read files with line ranges, detect binary, list dirs."""

from __future__ import annotations

import mimetypes
import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


MAX_LINE_LENGTH = 2000
DEFAULT_LIMIT = 200
MAX_READ_LIMIT = 400


class ReadFileInput(BaseModel):
    """Input schema for the ReadFile tool."""

    path: str = Field(
        description="Absolute or workspace-relative path to read."
    )
    offset: int | None = Field(
        default=None,
        description=(
            "1-based starting line number (default: 1). "
            "Use with limit for chunked reading."
        ),
    )
    limit: int | None = Field(
        default=None,
        description=(
            "Maximum number of lines to return "
            f"(default: {DEFAULT_LIMIT}, max: {MAX_READ_LIMIT}). "
            "Use small chunks first (for example 80-200), then continue with offset."
        ),
    )


class ReadFileTool:
    """Read file contents with optional line range.

    - Returns numbered lines from text files.
    - Detects binary files and returns a notice instead.
    - If ``path`` is a directory, lists its contents.
    - All paths are resolved relative to ``working_dir``.
    """

    name: str = "read_file"
    description: str = (
        "Read a file from the workspace. Returns numbered lines. "
        "Use chunked reads: start with a small limit, then continue via offset. "
        f"Default chunk is {DEFAULT_LIMIT} lines and max chunk is {MAX_READ_LIMIT}. "
        "If the path is a directory, lists its contents."
    )
    args_schema = ReadFileInput

    def __init__(self, working_dir: Path) -> None:
        self.working_dir = working_dir

    def _resolve(self, path: str) -> Path:
        """Resolve a path relative to the working directory."""
        p = Path(path)
        if p.is_absolute():
            return p
        return self.working_dir / p

    @staticmethod
    def _is_binary(file_path: Path) -> bool:
        """Heuristic binary detection: check for null bytes in first 8KB."""
        try:
            with open(file_path, "rb") as f:
                chunk = f.read(8192)
            return b"\x00" in chunk
        except OSError:
            return False

    def run(self, path: str, offset: int | None = None, limit: int | None = None) -> str:
        """Execute the read_file tool."""
        resolved = self._resolve(path)

        if not resolved.exists():
            return f"Error: path does not exist: {path}"

        if resolved.is_dir():
            return self._list_directory(resolved)

        if self._is_binary(resolved):
            size = resolved.stat().st_size
            mime = mimetypes.guess_type(str(resolved))[0] or "application/octet-stream"
            return f"Binary file ({mime}, {size} bytes): {path}"

        return self._read_text(resolved, offset, limit)

    def _list_directory(self, dir_path: Path) -> str:
        """List directory contents with type indicators."""
        entries: list[str] = []
        try:
            for entry in sorted(dir_path.iterdir()):
                suffix = "/" if entry.is_dir() else ""
                entries.append(f"  {entry.name}{suffix}")
        except PermissionError:
            return f"Error: permission denied: {dir_path}"

        if not entries:
            return f"Directory is empty: {dir_path}"

        header = f"Directory: {dir_path} ({len(entries)} entries)"
        return header + "\n" + "\n".join(entries)

    def _read_text(
        self, file_path: Path, offset: int | None, limit: int | None
    ) -> str:
        """Read text file with optional offset/limit, returning numbered lines."""
        effective_offset = max(1, offset or 1)
        requested_limit = limit
        if requested_limit is None:
            effective_limit = DEFAULT_LIMIT
        else:
            effective_limit = min(max(1, requested_limit), MAX_READ_LIMIT)

        try:
            with open(file_path, "r", errors="replace") as f:
                all_lines = f.readlines()
        except OSError as e:
            return f"Error reading file: {e}"

        total = len(all_lines)
        start_idx = effective_offset - 1  # 0-based
        end_idx = min(start_idx + effective_limit, total)

        if start_idx >= total:
            return (
                f"Error: offset {effective_offset} exceeds file length "
                f"({total} lines): {file_path}"
            )

        selected = all_lines[start_idx:end_idx]
        numbered: list[str] = []
        if (
            requested_limit is not None
            and requested_limit > MAX_READ_LIMIT
        ):
            numbered.append(
                f"[read_file] limit capped to {MAX_READ_LIMIT} "
                f"(requested {requested_limit})."
            )
        for i, line in enumerate(selected, start=effective_offset):
            # Truncate long lines
            content = line.rstrip("\n\r")
            if len(content) > MAX_LINE_LENGTH:
                content = content[:MAX_LINE_LENGTH] + "... (truncated)"
            numbered.append(f"{i:>6}\t{content}")

        result = "\n".join(numbered)

        # Add metadata footer
        if end_idx < total:
            result += f"\n\n... ({total - end_idx} more lines. Use offset={end_idx + 1} to continue.)"

        return result
