"""Unit tests for Glob tool."""

from pathlib import Path

import pytest

from janus_code.tools.glob_tool import GlobTool


@pytest.fixture
def tool(tmp_workspace: Path) -> GlobTool:
    return GlobTool(working_dir=tmp_workspace)


class TestGlobTool:
    def test_find_python_files(self, tool: GlobTool):
        result = tool.run("**/*.py")
        assert "main.py" in result
        assert "utils.py" in result
        assert "test_main.py" in result

    def test_find_in_subdirectory(self, tool: GlobTool):
        result = tool.run("src/*.py")
        assert "main.py" in result
        assert "test_main.py" not in result

    def test_no_matches(self, tool: GlobTool):
        result = tool.run("**/*.rs")
        assert "No files match" in result

    def test_gitignore_respected(self, tool: GlobTool, tmp_workspace: Path):
        # Create __pycache__ dir with .pyc file (should be ignored)
        cache = tmp_workspace / "__pycache__"
        cache.mkdir()
        (cache / "main.cpython-310.pyc").write_bytes(b"\x00")
        result = tool.run("**/*.pyc")
        assert "No files match" in result or "pyc" not in result

    def test_result_count(self, tool: GlobTool):
        result = tool.run("**/*.py")
        assert "Found" in result
        assert "file(s)" in result

    def test_sort_by_mtime(self, tool: GlobTool, tmp_workspace: Path):
        # Create files with different mtimes
        import time

        (tmp_workspace / "old.txt").write_text("old")
        time.sleep(0.01)
        (tmp_workspace / "new.txt").write_text("new")
        result = tool.run("*.txt")
        # new.txt should appear before old.txt (most recent first)
        lines = result.split("\n")
        file_lines = [l.strip() for l in lines if l.strip() and not l.startswith("Found")]
        if len(file_lines) >= 2:
            assert file_lines[0] == "new.txt"

    def test_max_results_truncation(self, tool: GlobTool, tmp_workspace: Path):
        many = tmp_workspace / "many"
        many.mkdir()
        for i in range(30):
            (many / f"f_{i}.txt").write_text(str(i))
        result = tool.run("**/*.txt", path="many", max_results=5)
        assert "Showing 5" in result
        assert "truncated" in result

    def test_path_scope_argument(self, tool: GlobTool, tmp_workspace: Path):
        scoped = tmp_workspace / "scoped"
        scoped.mkdir()
        (scoped / "only_here.py").write_text("pass\n")
        result = tool.run("*.py", path="scoped")
        assert "only_here.py" in result

    def test_blocks_broad_root_glob(self, tool: GlobTool):
        result = tool.run("*")
        assert "Broad root glob pattern blocked" in result
        assert "Top-level entries" in result
