"""Glob tool — file pattern matching respecting .gitignore."""

from __future__ import annotations

import fnmatch
import os
from pathlib import Path

from pydantic import BaseModel, Field

DEFAULT_GLOB_MAX_RESULTS = 120
MAX_GLOB_RESULTS_CAP = 500


class GlobSearchInput(BaseModel):
    """Input schema for the Glob tool."""

    pattern: str = Field(
        description=(
            "Glob pattern to match files (e.g. '**/*.py', 'src/**/*.ts'). "
            "Prefer specific patterns; avoid broad root patterns like '*'."
        )
    )
    path: str | None = Field(
        default=None,
        description=(
            "Optional directory/file path to scope search (e.g. 'src', "
            "'tests/integration'). Use this first for progressive disclosure."
        ),
    )
    max_results: int = Field(
        default=DEFAULT_GLOB_MAX_RESULTS,
        description=(
            "Maximum number of file paths to return "
            f"(default: {DEFAULT_GLOB_MAX_RESULTS}, hard cap: {MAX_GLOB_RESULTS_CAP})."
        ),
    )


class GlobTool:
    """Search for files matching a glob pattern.

    - Respects ``.gitignore`` patterns.
    - Results sorted by modification time (most recent first).
    - Returns workspace-relative paths.
    """

    name: str = "glob_search"
    description: str = (
        "Find files by glob pattern (e.g. '**/*.py'). "
        "Respects .gitignore. Use path + specific patterns for progressive "
        "disclosure. Broad root patterns like '*' are blocked. "
        "Results sorted by modification time and truncated by "
        "max_results."
    )
    args_schema = GlobSearchInput

    def __init__(self, working_dir: Path) -> None:
        self.working_dir = working_dir
        self._ignore_patterns: list[str] = []
        self._load_gitignore()

    def _load_gitignore(self) -> None:
        """Load .gitignore patterns from workspace root."""
        gitignore = self.working_dir / ".gitignore"
        if gitignore.exists():
            try:
                for line in gitignore.read_text().splitlines():
                    line = line.strip()
                    if line and not line.startswith("#"):
                        self._ignore_patterns.append(line)
            except OSError:
                pass

    def _is_ignored(self, rel_path: str) -> bool:
        """Check if a relative path matches any gitignore pattern."""
        parts = rel_path.split(os.sep)
        for pattern in self._ignore_patterns:
            # Check against full path and each path component
            clean = pattern.rstrip("/")
            if fnmatch.fnmatch(rel_path, clean):
                return True
            if fnmatch.fnmatch(rel_path, f"**/{clean}"):
                return True
            # Directory pattern (ends with /)
            if pattern.endswith("/"):
                dir_pat = pattern.rstrip("/")
                for part in parts:
                    if fnmatch.fnmatch(part, dir_pat):
                        return True
            else:
                for part in parts:
                    if fnmatch.fnmatch(part, clean):
                        return True
        return False

    def _top_level_listing(self, max_items: int = 80) -> str:
        """Return a concise top-level listing for broad-discovery fallback."""
        entries: list[str] = []
        try:
            for p in self.working_dir.iterdir():
                name = p.name + ("/" if p.is_dir() else "")
                entries.append(name)
        except OSError:
            return "Unable to list workspace root."
        entries.sort()
        shown = entries[:max_items]
        lines = [f"Top-level entries ({len(entries)} total, showing {len(shown)}):"]
        lines.extend(f"  {name}" for name in shown)
        if len(entries) > max_items:
            lines.append(f"\n... ({len(entries) - max_items} more entries truncated.)")
        return "\n".join(lines)

    def run(
        self,
        pattern: str,
        path: str | None = None,
        max_results: int = DEFAULT_GLOB_MAX_RESULTS,
    ) -> str:
        """Execute the glob_search tool."""
        if max_results <= 0:
            max_results = 1
        max_results = min(max_results, MAX_GLOB_RESULTS_CAP)

        normalized = pattern.strip()
        if not path and normalized in {"*", "**", "**/*"}:
            return (
                "Broad root glob pattern blocked for progressive disclosure.\n"
                "Use a scoped path and specific pattern, for example:\n"
                "  glob_search(path='src', pattern='**/*.py', max_results=80)\n\n"
                + self._top_level_listing()
            )

        search_root = self.working_dir
        if path:
            requested = Path(path)
            search_root = (
                requested
                if requested.is_absolute()
                else (self.working_dir / requested)
            )
            if not search_root.exists():
                return f"Path does not exist: {path}"
            try:
                search_root.relative_to(self.working_dir)
            except ValueError:
                return "Path must be inside the workspace."

        matches: list[tuple[float, str]] = []

        for file_path in search_root.rglob("*"):
            if not file_path.is_file():
                continue
            try:
                rel = str(file_path.relative_to(self.working_dir))
            except ValueError:
                continue
            try:
                local_rel = str(file_path.relative_to(search_root))
            except ValueError:
                local_rel = rel

            if self._is_ignored(rel):
                continue

            if (
                fnmatch.fnmatch(rel, pattern)
                or fnmatch.fnmatch(local_rel, pattern)
                or fnmatch.fnmatch(str(file_path), pattern)
            ):
                try:
                    mtime = file_path.stat().st_mtime
                except OSError:
                    mtime = 0.0
                matches.append((mtime, rel))

        # Sort by mtime descending (most recent first)
        matches.sort(key=lambda x: x[0], reverse=True)

        if not matches:
            return f"No files match pattern: {pattern}"

        display_count = min(len(matches), max_results)
        lines = [
            f"Found {len(matches)} file(s) matching '{pattern}'"
            + (f" in '{path}'" if path else "")
            + f". Showing {display_count}:"
        ]
        for _, matched_path in matches[:display_count]:
            lines.append(f"  {matched_path}")

        if len(matches) > max_results:
            lines.append(
                ""
                "\n... "
                f"({len(matches) - max_results} more truncated. "
                "Narrow with path=..., a more specific pattern, or a lower max_results.)"
            )

        return "\n".join(lines)
