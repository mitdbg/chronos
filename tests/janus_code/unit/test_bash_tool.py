"""Unit tests for Bash tool."""

from pathlib import Path

import pytest

from janus_code.tools.bash_tool import BashTool


@pytest.fixture
def tool(tmp_workspace: Path) -> BashTool:
    return BashTool(working_dir=tmp_workspace)


@pytest.fixture
def readonly_tool(tmp_workspace: Path) -> BashTool:
    return BashTool(working_dir=tmp_workspace, read_only=True)


class TestBashTool:
    def test_basic_command(self, tool: BashTool):
        result = tool.run("echo hello")
        assert "hello" in result

    def test_exit_code_reported(self, tool: BashTool):
        result = tool.run("exit 1")
        assert "exit code: 1" in result

    def test_stderr_captured(self, tool: BashTool):
        result = tool.run("echo error >&2")
        assert "error" in result

    def test_working_directory(self, tool: BashTool, tmp_workspace: Path):
        result = tool.run("ls src/")
        assert "main.py" in result

    def test_timeout(self, tool: BashTool):
        result = tool.run("sleep 30", timeout=1)
        assert "timed out" in result

    def test_piped_commands(self, tool: BashTool):
        result = tool.run("echo 'a b c' | wc -w")
        assert "3" in result

    def test_multiline_output(self, tool: BashTool):
        result = tool.run("echo 'line1'; echo 'line2'")
        assert "line1" in result
        assert "line2" in result


class TestBashReadOnly:
    def test_blocks_rm(self, readonly_tool: BashTool):
        result = readonly_tool.run("rm -rf /")
        assert "Error" in result
        assert "not allowed" in result

    def test_blocks_redirect(self, readonly_tool: BashTool):
        result = readonly_tool.run("echo x > file.txt")
        assert "Error" in result

    def test_allows_read_commands(self, readonly_tool: BashTool):
        result = readonly_tool.run("echo hello")
        assert "hello" in result

    def test_blocks_mkdir(self, readonly_tool: BashTool):
        result = readonly_tool.run("mkdir newdir")
        assert "Error" in result

    def test_allows_ls(self, readonly_tool: BashTool):
        result = readonly_tool.run("ls")
        assert "src" in result
