"""Integration tests — full agent loop end-to-end.

Tests the complete pipeline: user input → orchestrator → agent loop →
tool execution → response, using MockLLM with scripted tool calls.
"""

import pytest
from pathlib import Path

from chronos_code.agent.llm import MockLLM
from chronos_code.agent.orchestrator import Orchestrator
from chronos_code.config import Config
from chronos_code.tools.registry import build_tools, build_tool_schemas


class TestEndToEndAgentLoop:
    """Full pipeline tests with mock LLM."""

    async def test_simple_text_response(self, tmp_path):
        """LLM returns text only → returned as response."""
        config = Config()
        llm = MockLLM([
            {"content": "Hello! I can help you.", "tool_calls": None},
        ])
        tools = build_tools(working_dir=tmp_path, config=config)
        schemas = build_tool_schemas(tools)

        orchestrator = Orchestrator(
            config=config,
            working_dir=tmp_path,
            tools=tools,
            llm_fn=llm,
            tool_schemas=schemas,
        )

        response = await orchestrator.process("Hi there")
        assert "Hello" in response
        assert llm.call_count == 1

    async def test_tool_call_then_response(self, tmp_path):
        """LLM calls a tool, gets result, then responds."""
        # Create a file to read
        test_file = tmp_path / "test.py"
        test_file.write_text("print('hello')\n")

        config = Config()
        llm = MockLLM([
            # First: call read_file
            {
                "content": "",
                "tool_calls": [{
                    "id": "tc_1",
                    "type": "function",
                    "function": {
                        "name": "read_file",
                        "arguments": f'{{"path": "{test_file}"}}',
                    },
                }],
            },
            # Second: respond with content
            {
                "content": "The file contains a print statement.",
                "tool_calls": None,
            },
        ])

        tools = build_tools(working_dir=tmp_path, config=config)
        schemas = build_tool_schemas(tools)

        orchestrator = Orchestrator(
            config=config,
            working_dir=tmp_path,
            tools=tools,
            llm_fn=llm,
            tool_schemas=schemas,
        )

        response = await orchestrator.process("Read test.py")
        assert "print" in response.lower() or "file" in response.lower()
        assert llm.call_count == 2

    async def test_write_and_verify(self, tmp_path):
        """LLM writes a file, then reads it back."""
        config = Config()
        target = tmp_path / "output.txt"

        llm = MockLLM([
            # Step 1: write file
            {
                "content": "",
                "tool_calls": [{
                    "id": "tc_1",
                    "type": "function",
                    "function": {
                        "name": "write_file",
                        "arguments": f'{{"path": "{target}", "content": "Hello World"}}',
                    },
                }],
            },
            # Step 2: read it back
            {
                "content": "",
                "tool_calls": [{
                    "id": "tc_2",
                    "type": "function",
                    "function": {
                        "name": "read_file",
                        "arguments": f'{{"path": "{target}"}}',
                    },
                }],
            },
            # Step 3: respond
            {
                "content": "File written and verified.",
                "tool_calls": None,
            },
        ])

        tools = build_tools(working_dir=tmp_path, config=config)
        schemas = build_tool_schemas(tools)

        orchestrator = Orchestrator(
            config=config,
            working_dir=tmp_path,
            tools=tools,
            llm_fn=llm,
            tool_schemas=schemas,
        )

        response = await orchestrator.process("Write hello world to output.txt")
        assert target.exists()
        assert target.read_text() == "Hello World"
        assert llm.call_count == 3

    async def test_glob_tool_integration(self, tmp_path):
        """LLM uses glob to find files."""
        (tmp_path / "a.py").touch()
        (tmp_path / "b.py").touch()
        (tmp_path / "c.txt").touch()

        config = Config()
        llm = MockLLM([
            {
                "content": "",
                "tool_calls": [{
                    "id": "tc_1",
                    "type": "function",
                    "function": {
                        "name": "glob",
                        "arguments": f'{{"pattern": "*.py", "path": "{tmp_path}"}}',
                    },
                }],
            },
            {
                "content": "Found 2 Python files.",
                "tool_calls": None,
            },
        ])

        tools = build_tools(working_dir=tmp_path, config=config)
        schemas = build_tool_schemas(tools)

        orchestrator = Orchestrator(
            config=config,
            working_dir=tmp_path,
            tools=tools,
            llm_fn=llm,
            tool_schemas=schemas,
        )

        response = await orchestrator.process("Find all Python files")
        assert llm.call_count == 2

    async def test_unknown_tool_handled(self, tmp_path):
        """LLM calls a non-existent tool → error message returned."""
        config = Config()
        llm = MockLLM([
            {
                "content": "",
                "tool_calls": [{
                    "id": "tc_1",
                    "type": "function",
                    "function": {
                        "name": "nonexistent_tool",
                        "arguments": "{}",
                    },
                }],
            },
            {
                "content": "That tool doesn't exist. Let me try another way.",
                "tool_calls": None,
            },
        ])

        tools = build_tools(working_dir=tmp_path, config=config)
        schemas = build_tool_schemas(tools)

        orchestrator = Orchestrator(
            config=config,
            working_dir=tmp_path,
            tools=tools,
            llm_fn=llm,
            tool_schemas=schemas,
        )

        response = await orchestrator.process("Do something")
        # Agent should recover gracefully
        assert isinstance(response, str)

    async def test_todo_tracking(self, tmp_path):
        """LLM uses todo_write to track tasks."""
        config = Config()
        llm = MockLLM([
            {
                "content": "",
                "tool_calls": [{
                    "id": "tc_1",
                    "type": "function",
                    "function": {
                        "name": "todo_write",
                        "arguments": '{"todos": [{"id": "1", "title": "Read code", "status": "in_progress"}, {"id": "2", "title": "Write tests", "status": "pending"}]}',
                    },
                }],
            },
            {
                "content": "I've set up my task list.",
                "tool_calls": None,
            },
        ])

        tools = build_tools(working_dir=tmp_path, config=config)
        schemas = build_tool_schemas(tools)

        orchestrator = Orchestrator(
            config=config,
            working_dir=tmp_path,
            tools=tools,
            llm_fn=llm,
            tool_schemas=schemas,
        )

        response = await orchestrator.process("Plan the work")
        assert llm.call_count == 2

    async def test_bash_tool_integration(self, tmp_path):
        """LLM runs a bash command."""
        config = Config()
        llm = MockLLM([
            {
                "content": "",
                "tool_calls": [{
                    "id": "tc_1",
                    "type": "function",
                    "function": {
                        "name": "bash",
                        "arguments": '{"command": "echo hello from bash"}',
                    },
                }],
            },
            {
                "content": "The command output 'hello from bash'.",
                "tool_calls": None,
            },
        ])

        tools = build_tools(working_dir=tmp_path, config=config)
        schemas = build_tool_schemas(tools)

        orchestrator = Orchestrator(
            config=config,
            working_dir=tmp_path,
            tools=tools,
            llm_fn=llm,
            tool_schemas=schemas,
        )

        response = await orchestrator.process("Run echo command")
        assert llm.call_count == 2


class TestSubAgentEndToEnd:
    """Test sub-agent delegation end-to-end."""

    async def test_explore_implement_test_flow(self, tmp_path):
        """Orchestrator delegates to explore → implement → test sub-agents."""
        config = Config()

        call_count = 0

        def stepped_llm(messages, schemas):
            nonlocal call_count
            call_count += 1
            return {
                "content": f"Sub-agent step complete ({call_count})",
                "tool_calls": None,
            }

        tools = build_tools(working_dir=tmp_path, config=config)
        schemas = build_tool_schemas(tools)

        orchestrator = Orchestrator(
            config=config,
            working_dir=tmp_path,
            tools=tools,
            llm_fn=stepped_llm,
            tool_schemas=schemas,
        )

        response = await orchestrator.process_with_sub_agents(
            "Build a calculator",
            steps=["explore", "implement", "test"],
        )

        assert call_count == 3  # One call per sub-agent
        assert "[explore]" in response
        assert "[implement]" in response
        assert "[test]" in response
