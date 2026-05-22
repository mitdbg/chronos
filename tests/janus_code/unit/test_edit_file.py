"""Unit tests for Edit tool."""

from pathlib import Path

import pytest

from janus_code.tools.edit_file import EditFileTool


@pytest.fixture
def tool(tmp_workspace: Path) -> EditFileTool:
    return EditFileTool(working_dir=tmp_workspace)


class TestEditFile:
    def test_basic_replacement(self, tool: EditFileTool, tmp_workspace: Path):
        result = tool.run("src/main.py", "print('hello')", "print('world')")
        assert "Successfully edited" in result
        content = (tmp_workspace / "src" / "main.py").read_text()
        assert "print('world')" in content
        assert "print('hello')" not in content

    def test_diff_output(self, tool: EditFileTool):
        result = tool.run("src/main.py", "print('hello')", "print('world')")
        # Should contain diff markers
        assert "-" in result or "+" in result

    def test_nonexistent_file(self, tool: EditFileTool):
        result = tool.run("missing.py", "old", "new")
        assert "Error" in result
        assert "does not exist" in result

    def test_old_str_not_found(self, tool: EditFileTool):
        result = tool.run("src/main.py", "THIS_DOES_NOT_EXIST", "replacement")
        assert "Error" in result
        assert "not found" in result

    def test_old_str_multiple_matches(self, tool: EditFileTool, tmp_workspace: Path):
        # Create file with repeated content
        (tmp_workspace / "dupes.py").write_text("foo\nfoo\nbar\n")
        result = tool.run("dupes.py", "foo", "baz")
        assert "Error" in result
        assert "2 times" in result

    def test_multiline_replacement(self, tool: EditFileTool, tmp_workspace: Path):
        result = tool.run(
            "src/utils.py",
            "def add(a, b):\n    return a + b",
            "def add(a: int, b: int) -> int:\n    return a + b",
        )
        assert "Successfully edited" in result
        content = (tmp_workspace / "src" / "utils.py").read_text()
        assert "def add(a: int, b: int) -> int:" in content

    def test_empty_new_str_deletes(self, tool: EditFileTool, tmp_workspace: Path):
        (tmp_workspace / "deleteme.py").write_text("keep\nremove_this\nkeep\n")
        result = tool.run("deleteme.py", "remove_this\n", "")
        assert "Successfully edited" in result
        content = (tmp_workspace / "deleteme.py").read_text()
        assert "remove_this" not in content
