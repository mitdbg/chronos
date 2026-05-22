"""Tests for LLM wrapper and tool registry."""

import pytest

from chronos_code.agent.llm import MockLLM, build_tool_schemas
from chronos_code.tools.registry import build_tools, build_tool_schemas as registry_build_schemas
from chronos_code.config import Config
from tests.chronos_code.conftest import MockChronosContext


class TestMockLLM:
    """Tests for MockLLM."""

    def test_create_empty(self):
        llm = MockLLM()
        assert llm.call_count == 0

    def test_scripted_response(self):
        llm = MockLLM([
            {"content": "Hello", "tool_calls": None},
        ])
        resp = llm([], [])
        assert resp["content"] == "Hello"
        assert resp["tool_calls"] is None
        assert llm.call_count == 1

    def test_multiple_responses(self):
        llm = MockLLM([
            {"content": "First", "tool_calls": None},
            {"content": "Second", "tool_calls": None},
        ])
        assert llm([], [])["content"] == "First"
        assert llm([], [])["content"] == "Second"
        assert llm.call_count == 2

    def test_exhausted_responses(self):
        llm = MockLLM([{"content": "Only one", "tool_calls": None}])
        llm([], [])
        resp = llm([], [])
        assert "no more" in resp["content"].lower()

    def test_add_response(self):
        llm = MockLLM()
        llm.add_response("Added")
        resp = llm([], [])
        assert resp["content"] == "Added"

    def test_tool_calls_response(self):
        tc = [{"id": "tc1", "type": "function", "function": {"name": "read_file", "arguments": "{}"}}]
        llm = MockLLM([{"content": "", "tool_calls": tc}])
        resp = llm([], [])
        assert resp["tool_calls"] is not None
        assert len(resp["tool_calls"]) == 1


class TestBuildTools:
    """Tests for tool registry build_tools."""

    def test_build_all_tools(self, tmp_path):
        config = Config()
        tools = build_tools(working_dir=tmp_path, config=config)
        expected_min = {
            "read_file", "write_file", "edit_file", "glob",
            "ripgrep", "bash", "todo_write", "memory",
            "ask_user", "task",
        }
        # Backward-compatible aliases are included as well.
        assert expected_min.issubset(set(tools.keys()))
        assert "glob_search" in tools
        assert "launch_task" in tools

    def test_tools_have_run_method(self, tmp_path):
        config = Config()
        tools = build_tools(working_dir=tmp_path, config=config)
        for name, tool in tools.items():
            assert hasattr(tool, "run"), f"Tool {name} missing run() method"

    def test_tools_have_name(self, tmp_path):
        config = Config()
        tools = build_tools(working_dir=tmp_path, config=config)
        for name, tool in tools.items():
            assert hasattr(tool, "name"), f"Tool {name} missing name attribute"

    def test_todo_listener_wired(self, tmp_path):
        config = Config()
        calls = []
        tools = build_tools(
            working_dir=tmp_path,
            config=config,
            todo_listener=lambda todos: calls.append(todos),
        )
        todo = tools["todo_write"]
        todo.run(todos=[{"id": "1", "title": "Test", "status": "pending"}])
        assert len(calls) == 1

    def test_bash_timeout_from_config(self, tmp_path):
        config = Config(bash_timeout=60)
        tools = build_tools(working_dir=tmp_path, config=config)
        assert tools["bash"].timeout == 60

    def test_sqlite_tool_present_with_chronos_context(self, tmp_path):
        config = Config()
        chronos_context = MockChronosContext(tmp_path)
        chronos_context.begin()
        tools = build_tools(working_dir=tmp_path, config=config, chronos_context=chronos_context)
        assert "sqlite" in tools
        assert hasattr(tools["sqlite"], "run")


class TestBuildToolSchemas:
    """Tests for tool schema generation."""

    def test_generates_schemas(self, tmp_path):
        config = Config()
        tools = build_tools(working_dir=tmp_path, config=config)
        schemas = registry_build_schemas(tools)
        # Aliases share tool instances and should not create duplicate schemas.
        expected_unique = len(
            {getattr(t, "name", k) for k, t in tools.items()}
        )
        assert len(schemas) == expected_unique

    def test_schema_format(self, tmp_path):
        config = Config()
        tools = build_tools(working_dir=tmp_path, config=config)
        schemas = registry_build_schemas(tools)
        for schema in schemas:
            assert "type" in schema
            assert schema["type"] == "function"
            assert "function" in schema
            func = schema["function"]
            assert "name" in func
            assert "description" in func
            assert "parameters" in func

    def test_schema_names_match_tools(self, tmp_path):
        config = Config()
        tools = build_tools(working_dir=tmp_path, config=config)
        schemas = registry_build_schemas(tools)
        schema_names = {s["function"]["name"] for s in schemas}
        tool_names = {getattr(t, "name", k) for k, t in tools.items()}
        assert schema_names == tool_names
