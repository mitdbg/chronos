"""Unit tests for Memory tool."""

from pathlib import Path

import pytest

from chronos_code.tools.memory_tool import MemoryTool


@pytest.fixture
def tool(tmp_workspace: Path) -> MemoryTool:
    return MemoryTool(working_dir=tmp_workspace)


class TestMemoryTool:
    def test_list_empty(self, tool: MemoryTool):
        result = tool.run("list")
        assert "No memory files" in result

    def test_write_and_read(self, tool: MemoryTool):
        write_result = tool.run("write", path="notes.md", content="# Notes\nTest content\n")
        assert "written" in write_result

        read_result = tool.run("read", path="notes.md")
        assert "# Notes" in read_result
        assert "Test content" in read_result

    def test_list_after_write(self, tool: MemoryTool):
        tool.run("write", path="build.md", content="# Build Commands\nnpm test\n")
        result = tool.run("list")
        assert "build.md" in result
        assert "# Build Commands" in result

    def test_append(self, tool: MemoryTool):
        tool.run("write", path="log.md", content="Line 1\n")
        tool.run("append", path="log.md", content="Line 2\n")
        result = tool.run("read", path="log.md")
        assert "Line 1" in result
        assert "Line 2" in result

    def test_append_nonexistent(self, tool: MemoryTool):
        result = tool.run("append", path="missing.md", content="data")
        assert "Error" in result
        assert "does not exist" in result

    def test_read_nonexistent(self, tool: MemoryTool):
        result = tool.run("read", path="missing.md")
        assert "Error" in result

    def test_write_overwrites(self, tool: MemoryTool):
        tool.run("write", path="data.md", content="version 1")
        tool.run("write", path="data.md", content="version 2")
        result = tool.run("read", path="data.md")
        assert "version 2" in result
        assert "version 1" not in result

    def test_missing_path(self, tool: MemoryTool):
        result = tool.run("read")
        assert "Error" in result
        assert "'path' is required" in result

    def test_missing_content(self, tool: MemoryTool):
        result = tool.run("write", path="file.md")
        assert "Error" in result
        assert "'content' is required" in result

    def test_unknown_command(self, tool: MemoryTool):
        result = tool.run("delete")
        assert "Error" in result
        assert "unknown command" in result

    def test_creates_memory_dir(self, tool: MemoryTool, tmp_workspace: Path):
        mem_dir = tmp_workspace / ".chronos-code" / "memory"
        assert not mem_dir.exists()
        tool.run("write", path="test.md", content="data")
        assert mem_dir.exists()
