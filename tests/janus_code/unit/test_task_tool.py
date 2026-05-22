"""Unit tests for Task tool (stub)."""

import pytest

from janus_code.tools.task_tool import TaskTool


class TestTaskToolStub:
    def test_stub_explore(self):
        tool = TaskTool()
        result = tool.run("Find all API endpoints", type="explore")
        assert "no task launcher configured" in result.lower()
        assert "explore" in result

    def test_stub_implement(self):
        tool = TaskTool()
        result = tool.run("Add logging to auth module", type="implement")
        assert "implement" in result

    def test_stub_test(self):
        tool = TaskTool()
        result = tool.run("Run pytest on tests/", type="test")
        assert "test" in result

    def test_parallel_requires_launcher(self):
        tool = TaskTool()
        result = tool.run("Fan out work", type="implement", parallel=True)
        assert "parallel=false" in result

    def test_with_launcher(self):
        def mock_launcher(description: str, type: str) -> str:
            return f"Launched {type}: {description[:20]}"

        tool = TaskTool(launcher=mock_launcher)
        result = tool.run("Do something", type="explore")
        assert "Launched explore" in result
