"""MemoryLoader — load CLAUDE.md files and auto-memory at session start."""

from __future__ import annotations

import os
from pathlib import Path


CLAUDE_MD_LOCATIONS = [
    "CLAUDE.md",
    ".claude/CLAUDE.md",
    "CLAUDE.local.md",
]

MEMORY_INDEX_FILE = "MEMORY.md"
MEMORY_INDEX_MAX_LINES = 200


class MemoryLoader:
    """Load project instructions and auto-memory at session start.

    Tier 1: CLAUDE.md files (user-written, read-only).
    Tier 2: Auto-memory index (.janus-code/memory/MEMORY.md, first 200 lines).

    Also supports loading path-scoped rules from ``.claude/rules/*.md``
    and user-global CLAUDE.md from ``~/.claude/CLAUDE.md``.
    """

    def __init__(
        self,
        working_dir: Path,
        memory_dir: str = ".janus-code/memory",
    ) -> None:
        self.working_dir = working_dir
        self.memory_dir = memory_dir

    def load_claude_md(self) -> str:
        """Load all CLAUDE.md files, concatenated with headers."""
        sections: list[str] = []

        # User-global CLAUDE.md
        user_global = Path.home() / ".claude" / "CLAUDE.md"
        if user_global.exists():
            try:
                content = user_global.read_text().strip()
                if content:
                    sections.append(
                        f"## User Instructions (~/.claude/CLAUDE.md)\n\n{content}"
                    )
            except OSError:
                pass

        # Project-level CLAUDE.md files
        for loc in CLAUDE_MD_LOCATIONS:
            path = self.working_dir / loc
            if path.exists():
                try:
                    content = path.read_text().strip()
                    if content:
                        sections.append(
                            f"## Project Instructions ({loc})\n\n{content}"
                        )
                except OSError:
                    pass

        if not sections:
            return ""

        return "# Instructions\n\n" + "\n\n---\n\n".join(sections)

    def load_rules(self, file_path: str | None = None) -> str:
        """Load path-scoped rules from .claude/rules/*.md.

        If ``file_path`` is given, only loads rules whose filename
        patterns match the path.
        """
        rules_dir = self.working_dir / ".claude" / "rules"
        if not rules_dir.exists():
            return ""

        sections: list[str] = []
        for rule_file in sorted(rules_dir.glob("*.md")):
            try:
                content = rule_file.read_text().strip()
                if content:
                    sections.append(
                        f"### Rule: {rule_file.stem}\n\n{content}"
                    )
            except OSError:
                pass

        if not sections:
            return ""

        return "# Path-Scoped Rules\n\n" + "\n\n".join(sections)

    def load_auto_memory(self) -> str:
        """Load the auto-memory index (first 200 lines of MEMORY.md)."""
        index_path = self.working_dir / self.memory_dir / MEMORY_INDEX_FILE
        if not index_path.exists():
            return ""

        try:
            lines = index_path.read_text().splitlines()
        except OSError:
            return ""

        truncated = lines[:MEMORY_INDEX_MAX_LINES]
        content = "\n".join(truncated)

        if len(lines) > MEMORY_INDEX_MAX_LINES:
            content += f"\n\n... ({len(lines) - MEMORY_INDEX_MAX_LINES} more lines)"

        return f"# Auto-Memory\n\n{content}" if content.strip() else ""

    def load_all(self) -> str:
        """Load all memory tiers, concatenated."""
        parts: list[str] = []

        claude_md = self.load_claude_md()
        if claude_md:
            parts.append(claude_md)

        rules = self.load_rules()
        if rules:
            parts.append(rules)

        auto_mem = self.load_auto_memory()
        if auto_mem:
            parts.append(auto_mem)

        return "\n\n---\n\n".join(parts)
