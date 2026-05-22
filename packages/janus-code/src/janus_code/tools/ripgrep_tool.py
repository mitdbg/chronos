"""RipGrep tool — content search wrapping the ``rg`` binary."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from pydantic import BaseModel, Field


DEFAULT_MAX_RESULTS = 30
MAX_RESULTS_CAP = 200
MAX_OUTPUT_CHARS = 12000


class RipGrepInput(BaseModel):
    """Input schema for the RipGrep tool."""

    query: str = Field(description="Search pattern (regex by default).")
    path: str | None = Field(
        default=None,
        description=(
            "Directory or file to scope the search to. "
            "Use this first for progressive disclosure."
        ),
    )
    glob: str | None = Field(
        default=None,
        description="File type filter glob (e.g. '*.py', '*.ts').",
    )
    fixed_strings: bool = Field(
        default=False,
        description="Treat query as a literal string (not regex).",
    )
    multiline: bool = Field(
        default=False,
        description="Enable multiline matching.",
    )
    max_results: int = Field(
        default=DEFAULT_MAX_RESULTS,
        description=(
            "Maximum number of matching lines to return "
            f"(default: {DEFAULT_MAX_RESULTS}, hard cap: {MAX_RESULTS_CAP})."
        ),
    )


class RipGrepTool:
    """Search file contents using ripgrep (``rg``).

    - Regex by default; ``fixed_strings=True`` for literal search.
    - Respects ``.gitignore``.
    - Output truncated at ``max_results`` to prevent context bloat.
    - Always use this tool instead of ``grep``/``rg`` via Bash.
    """

    name: str = "ripgrep"
    description: str = (
        "Search file contents using ripgrep. Supports regex, literal, "
        "and multiline search. Respects .gitignore. Use path/glob and "
        "small max_results first for progressive disclosure. ALWAYS use this "
        "instead of grep/rg in Bash."
    )
    args_schema = RipGrepInput

    def __init__(self, working_dir: Path) -> None:
        self.working_dir = working_dir
        self._rg_path = shutil.which("rg")

    def run(
        self,
        query: str,
        path: str | None = None,
        glob: str | None = None,
        fixed_strings: bool = False,
        multiline: bool = False,
        max_results: int = DEFAULT_MAX_RESULTS,
    ) -> str:
        """Execute the ripgrep tool."""
        if not self._rg_path:
            return (
                "Error: ripgrep (rg) is not installed. "
                "Install it: https://github.com/BurntSushi/ripgrep#installation"
            )

        cmd: list[str] = [self._rg_path]
        if max_results <= 0:
            max_results = 1
        max_results = min(max_results, MAX_RESULTS_CAP)

        # Core flags
        cmd.extend(["--line-number", "--color", "never", "--no-heading"])

        if fixed_strings:
            cmd.append("--fixed-strings")
        if multiline:
            cmd.extend(["--multiline", "--multiline-dotall"])
        if glob:
            cmd.extend(["--glob", glob])

        # Max results via rg's --max-count isn't per-file — use head instead
        cmd.append(query)

        # Search path
        search_path = self.working_dir
        if path:
            p = Path(path)
            if p.is_absolute():
                search_path = p
            else:
                search_path = self.working_dir / p

        cmd.append(str(search_path))

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
                cwd=str(self.working_dir),
            )
        except subprocess.TimeoutExpired:
            return "Error: ripgrep search timed out after 30 seconds."
        except FileNotFoundError:
            return "Error: ripgrep (rg) binary not found."

        if result.returncode == 1:
            return f"No matches found for: {query}"
        if result.returncode > 1:
            return f"Error running ripgrep: {result.stderr.strip()}"

        lines = result.stdout.strip().split("\n")

        # Truncate to max_results
        if len(lines) > max_results:
            truncated = lines[:max_results]
            truncated.append(
                f"\n... ({len(lines) - max_results} more matches truncated. "
                "Narrow with path=..., glob=..., or increase max_results.)"
            )
            output = "\n".join(truncated)
        else:
            output = (
                result.stdout.strip()
                if result.stdout.strip()
                else f"No matches found for: {query}"
            )

        if len(output) > MAX_OUTPUT_CHARS:
            clipped = output[:MAX_OUTPUT_CHARS]
            remainder = len(output) - MAX_OUTPUT_CHARS
            output = (
                clipped
                + f"\n\n... ({remainder} characters truncated. "
                "Narrow with path/glob or lower max_results.)"
            )

        return output
