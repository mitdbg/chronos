"""Unit tests for RipGrep tool."""

import shutil
from pathlib import Path

import pytest

from janus_code.tools.ripgrep_tool import RipGrepTool


@pytest.fixture
def tool(tmp_workspace: Path) -> RipGrepTool:
    return RipGrepTool(working_dir=tmp_workspace)


HAS_RG = shutil.which("rg") is not None


@pytest.mark.skipif(not HAS_RG, reason="ripgrep (rg) not installed")
class TestRipGrepTool:
    def test_basic_search(self, tool: RipGrepTool):
        result = tool.run("def main")
        assert "main.py" in result
        assert "def main" in result

    def test_regex_search(self, tool: RipGrepTool):
        result = tool.run(r"def \w+\(a, b\)")
        assert "utils.py" in result

    def test_fixed_strings(self, tool: RipGrepTool):
        result = tool.run("def add(a, b):", fixed_strings=True)
        assert "utils.py" in result

    def test_no_matches(self, tool: RipGrepTool):
        result = tool.run("XYZZY_DOES_NOT_EXIST_ANYWHERE")
        assert "No matches found" in result

    def test_path_scoping(self, tool: RipGrepTool):
        result = tool.run("def", path="src")
        assert "main.py" in result or "utils.py" in result

    def test_glob_filter(self, tool: RipGrepTool):
        result = tool.run("def", glob="*.py")
        assert "def" in result

    def test_max_results(self, tool: RipGrepTool, tmp_workspace: Path):
        # Create many matches
        many = tmp_workspace / "many.py"
        many.write_text("\n".join(f"match_{i} = True" for i in range(100)))
        result = tool.run("match_", path=str(many), max_results=5)
        assert "truncated" in result or result.count("\n") <= 10


class TestRipGrepNotInstalled:
    def test_missing_rg(self, tmp_workspace: Path):
        tool = RipGrepTool(working_dir=tmp_workspace)
        tool._rg_path = None  # Simulate missing rg
        result = tool.run("test")
        assert "not installed" in result
