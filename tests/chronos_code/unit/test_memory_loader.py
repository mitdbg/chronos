"""Unit tests for MemoryLoader."""

from pathlib import Path

import pytest

from chronos_code.context.memory_loader import MemoryLoader


@pytest.fixture
def loader(tmp_workspace: Path) -> MemoryLoader:
    return MemoryLoader(working_dir=tmp_workspace)


class TestMemoryLoader:
    def test_load_claude_md(self, loader: MemoryLoader, tmp_workspace: Path):
        result = loader.load_claude_md()
        assert "Project Instructions" in result
        assert "Use pytest" in result

    def test_load_claude_md_missing(self, tmp_path: Path):
        loader = MemoryLoader(working_dir=tmp_path)
        result = loader.load_claude_md()
        assert result == ""

    def test_load_claude_local_md(self, loader: MemoryLoader, tmp_workspace: Path):
        (tmp_workspace / "CLAUDE.local.md").write_text("# Local\nLocal override\n")
        result = loader.load_claude_md()
        assert "Local override" in result

    def test_load_rules(self, loader: MemoryLoader, tmp_workspace: Path):
        rules_dir = tmp_workspace / ".claude" / "rules"
        rules_dir.mkdir(parents=True)
        (rules_dir / "python.md").write_text("Always use type hints.\n")
        result = loader.load_rules()
        assert "python" in result
        assert "type hints" in result

    def test_load_rules_empty(self, loader: MemoryLoader):
        result = loader.load_rules()
        assert result == ""

    def test_load_auto_memory(self, loader: MemoryLoader, tmp_workspace: Path):
        mem_dir = tmp_workspace / ".chronos-code" / "memory"
        mem_dir.mkdir(parents=True)
        (mem_dir / "MEMORY.md").write_text("# Memory Index\n- Build: npm test\n")
        result = loader.load_auto_memory()
        assert "Memory Index" in result
        assert "npm test" in result

    def test_load_auto_memory_truncation(self, loader: MemoryLoader, tmp_workspace: Path):
        mem_dir = tmp_workspace / ".chronos-code" / "memory"
        mem_dir.mkdir(parents=True)
        lines = [f"Line {i}" for i in range(300)]
        (mem_dir / "MEMORY.md").write_text("\n".join(lines))
        result = loader.load_auto_memory()
        assert "more lines" in result

    def test_load_auto_memory_missing(self, loader: MemoryLoader):
        result = loader.load_auto_memory()
        assert result == ""

    def test_load_all(self, loader: MemoryLoader, tmp_workspace: Path):
        mem_dir = tmp_workspace / ".chronos-code" / "memory"
        mem_dir.mkdir(parents=True)
        (mem_dir / "MEMORY.md").write_text("# Memory\nnotes\n")
        result = loader.load_all()
        assert "Instructions" in result  # CLAUDE.md
        assert "Memory" in result  # auto-memory
