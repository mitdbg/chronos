"""Unit tests for Write tool."""

from pathlib import Path

import pytest

from janus_code.tools.write_file import WriteFileTool


@pytest.fixture
def tool(tmp_workspace: Path) -> WriteFileTool:
    return WriteFileTool(working_dir=tmp_workspace)


class TestWriteFile:
    def test_create_new_file(self, tool: WriteFileTool, tmp_workspace: Path):
        result = tool.run("new_file.py", "print('hello')\n")
        assert "Successfully wrote" in result
        assert (tmp_workspace / "new_file.py").read_text() == "print('hello')\n"

    def test_overwrite_existing(self, tool: WriteFileTool, tmp_workspace: Path):
        result = tool.run("src/main.py", "# overwritten\n")
        assert "Successfully wrote" in result
        assert (tmp_workspace / "src" / "main.py").read_text() == "# overwritten\n"

    def test_create_with_parent_dirs(self, tool: WriteFileTool, tmp_workspace: Path):
        result = tool.run("deep/nested/dir/file.py", "content\n")
        assert "Successfully wrote" in result
        assert (tmp_workspace / "deep" / "nested" / "dir" / "file.py").exists()

    def test_write_empty_file(self, tool: WriteFileTool, tmp_workspace: Path):
        result = tool.run("empty.txt", "")
        assert "Successfully wrote" in result
        assert (tmp_workspace / "empty.txt").read_text() == ""

    def test_line_count_in_output(self, tool: WriteFileTool):
        result = tool.run("lines.txt", "a\nb\nc\n")
        assert "3 lines" in result

    def test_write_absolute_path(self, tool: WriteFileTool, tmp_workspace: Path):
        abs_path = str(tmp_workspace / "abs_file.txt")
        result = tool.run(abs_path, "absolute\n")
        assert "Successfully wrote" in result
        assert Path(abs_path).read_text() == "absolute\n"
