"""Unit tests for ReadFile tool."""

from pathlib import Path

import pytest

from chronos_code.tools.read_file import ReadFileTool


@pytest.fixture
def tool(tmp_workspace: Path) -> ReadFileTool:
    return ReadFileTool(working_dir=tmp_workspace)


class TestReadFile:
    def test_read_whole_file(self, tool: ReadFileTool, tmp_workspace: Path):
        result = tool.run("src/main.py")
        assert "def main():" in result
        assert "print('hello')" in result

    def test_read_with_line_numbers(self, tool: ReadFileTool):
        result = tool.run("src/main.py")
        # Line numbers should be present
        assert "\t" in result  # tab separation

    def test_read_with_offset(self, tool: ReadFileTool, tmp_workspace: Path):
        result = tool.run("src/utils.py", offset=2)
        # Should start from line 2
        assert "return a + b" in result
        assert "def add" not in result  # line 1 skipped

    def test_read_with_limit(self, tool: ReadFileTool, tmp_workspace: Path):
        result = tool.run("src/utils.py", offset=1, limit=1)
        assert "def add" in result
        assert "return a + b" not in result

    def test_read_nonexistent_file(self, tool: ReadFileTool):
        result = tool.run("nonexistent.py")
        assert "Error" in result
        assert "does not exist" in result

    def test_read_directory(self, tool: ReadFileTool, tmp_workspace: Path):
        result = tool.run("src")
        assert "Directory:" in result
        assert "main.py" in result
        assert "utils.py" in result

    def test_read_binary_file(self, tool: ReadFileTool, tmp_workspace: Path):
        result = tool.run("image.png")
        assert "Binary file" in result
        assert "image/png" in result or "application/octet-stream" in result

    def test_read_absolute_path(self, tool: ReadFileTool, tmp_workspace: Path):
        abs_path = str(tmp_workspace / "src" / "main.py")
        result = tool.run(abs_path)
        assert "def main():" in result

    def test_read_offset_beyond_file(self, tool: ReadFileTool):
        result = tool.run("src/main.py", offset=9999)
        assert "Error" in result
        assert "exceeds" in result

    def test_read_empty_directory(self, tool: ReadFileTool, tmp_workspace: Path):
        empty = tmp_workspace / "empty_dir"
        empty.mkdir()
        result = tool.run("empty_dir")
        assert "empty" in result.lower()

    def test_truncation_notice(self, tool: ReadFileTool, tmp_workspace: Path):
        # Create a file with many lines
        big_file = tmp_workspace / "big.txt"
        big_file.write_text("\n".join(f"line {i}" for i in range(500)))
        result = tool.run("big.txt")
        assert "more lines" in result

    def test_long_line_truncation(self, tool: ReadFileTool, tmp_workspace: Path):
        long_file = tmp_workspace / "long.txt"
        long_file.write_text("x" * 5000 + "\n")
        result = tool.run("long.txt")
        assert "truncated" in result

    def test_limit_is_capped_for_chunked_reads(
        self, tool: ReadFileTool, tmp_workspace: Path
    ):
        big_file = tmp_workspace / "cap_test.txt"
        big_file.write_text("\n".join(f"line {i}" for i in range(1200)))
        result = tool.run("cap_test.txt", limit=2000)
        assert "limit capped" in result
        assert "offset=401" in result
