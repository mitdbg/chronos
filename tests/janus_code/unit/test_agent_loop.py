"""Unit tests for AgentLoop with mock LLM."""

import pytest

from janus_code.agent.agent_loop import AgentLoop
from janus_code.config import Config
from janus_code.context.context_manager import ContextManager
from janus_code.context.conversation import Conversation, Message, ToolCall
from janus_code.context.memory_loader import MemoryLoader
from pathlib import Path


class MockTool:
    """Simple mock tool for testing."""

    def __init__(self, name: str, response: str = "ok"):
        self.name = name
        self._response = response
        self.calls: list[dict] = []

    def run(self, **kwargs) -> str:
        self.calls.append(kwargs)
        return self._response


def make_llm_fn(responses: list[dict]):
    """Create a mock LLM function that returns pre-scripted responses."""
    idx = [0]

    def llm_fn(messages, tool_schemas):
        if idx[0] >= len(responses):
            return {"content": "[done]", "tool_calls": None}
        resp = responses[idx[0]]
        idx[0] += 1
        return resp

    return llm_fn


@pytest.fixture
def tmp_ws(tmp_path: Path) -> Path:
    (tmp_path / "CLAUDE.md").write_text("# Test\n")
    return tmp_path


class TestAgentLoop:
    @pytest.mark.asyncio
    async def test_simple_response(self, tmp_ws: Path):
        """LLM returns a simple text response (no tool calls)."""
        conv = Conversation()
        conv.append_system("sys")
        conv.append_user("hello")

        llm_fn = make_llm_fn([{"content": "Hi there!", "tool_calls": None}])
        config = Config()
        loader = MemoryLoader(tmp_ws)
        ctx_mgr = ContextManager(config, loader, conv)

        loop = AgentLoop(conv, ctx_mgr, {}, llm_fn)
        result = await loop.run()

        assert result.content == "Hi there!"
        assert result.role == "assistant"

    @pytest.mark.asyncio
    async def test_tool_call_and_response(self, tmp_ws: Path):
        """LLM calls a tool, then responds."""
        conv = Conversation()
        conv.append_system("sys")
        conv.append_user("list files")

        read_tool = MockTool("read_file", "file1.py\nfile2.py")

        responses = [
            # First: tool call
            {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_1",
                        "function": {
                            "name": "read_file",
                            "arguments": '{"path": "."}',
                        },
                    }
                ],
            },
            # Second: final response
            {"content": "Found 2 files.", "tool_calls": None},
        ]

        llm_fn = make_llm_fn(responses)
        config = Config()
        loader = MemoryLoader(tmp_ws)
        ctx_mgr = ContextManager(config, loader, conv)

        loop = AgentLoop(conv, ctx_mgr, {"read_file": read_tool}, llm_fn)
        result = await loop.run()

        assert result.content == "Found 2 files."
        assert len(read_tool.calls) == 1
        assert read_tool.calls[0]["path"] == "."

    @pytest.mark.asyncio
    async def test_multiple_tool_calls(self, tmp_ws: Path):
        """LLM makes multiple tool calls in sequence."""
        conv = Conversation()
        conv.append_system("sys")
        conv.append_user("search and read")

        grep_tool = MockTool("ripgrep", "main.py:1: def main()")
        read_tool = MockTool("read_file", "def main():\n    pass")

        responses = [
            {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_1",
                        "function": {
                            "name": "ripgrep",
                            "arguments": '{"query": "def main"}',
                        },
                    }
                ],
            },
            {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_2",
                        "function": {
                            "name": "read_file",
                            "arguments": '{"path": "main.py"}',
                        },
                    }
                ],
            },
            {"content": "Found the main function.", "tool_calls": None},
        ]

        llm_fn = make_llm_fn(responses)
        config = Config()
        loader = MemoryLoader(tmp_ws)
        ctx_mgr = ContextManager(config, loader, conv)

        tools = {"ripgrep": grep_tool, "read_file": read_tool}
        loop = AgentLoop(conv, ctx_mgr, tools, llm_fn)
        result = await loop.run()

        assert result.content == "Found the main function."
        assert len(grep_tool.calls) == 1
        assert len(read_tool.calls) == 1

    @pytest.mark.asyncio
    async def test_unknown_tool(self, tmp_ws: Path):
        """LLM calls an unknown tool — returns error."""
        conv = Conversation()
        conv.append_system("sys")
        conv.append_user("test")

        responses = [
            {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_1",
                        "function": {
                            "name": "nonexistent_tool",
                            "arguments": "{}",
                        },
                    }
                ],
            },
            {"content": "done", "tool_calls": None},
        ]

        llm_fn = make_llm_fn(responses)
        config = Config()
        loader = MemoryLoader(tmp_ws)
        ctx_mgr = ContextManager(config, loader, conv)

        loop = AgentLoop(conv, ctx_mgr, {}, llm_fn)
        result = await loop.run()

        # Should have an error tool result in conversation
        tool_msgs = [m for m in conv.messages if m.role == "tool"]
        assert len(tool_msgs) == 1
        assert "unknown tool" in tool_msgs[0].content

    @pytest.mark.asyncio
    async def test_max_iterations(self, tmp_ws: Path):
        """Loop respects max_iterations."""
        conv = Conversation()
        conv.append_system("sys")
        conv.append_user("loop forever")

        # Always returns a tool call — should be capped
        def infinite_llm(msgs, schemas):
            return {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_x",
                        "function": {"name": "bash", "arguments": '{"command": "echo hi"}'},
                    }
                ],
            }

        config = Config()
        loader = MemoryLoader(tmp_ws)
        ctx_mgr = ContextManager(config, loader, conv)
        bash_tool = MockTool("bash", "hi")

        loop = AgentLoop(
            conv, ctx_mgr, {"bash": bash_tool}, infinite_llm, max_iterations=3
        )
        result = await loop.run()
        assert "Max iterations" in result.content
        assert len(bash_tool.calls) == 3

    @pytest.mark.asyncio
    async def test_callbacks(self, tmp_ws: Path):
        """Test on_assistant_message, on_tool_call, and on_tool_result callbacks."""
        conv = Conversation()
        conv.append_system("sys")
        conv.append_user("test")

        assistant_msgs: list = []
        tool_calls: list = []
        tool_results: list = []

        responses = [
            {
                "content": "",
                "tool_calls": [
                    {
                        "id": "c1",
                        "function": {"name": "bash", "arguments": '{"command": "echo"}'},
                    }
                ],
            },
            {"content": "done", "tool_calls": None},
        ]

        llm_fn = make_llm_fn(responses)
        config = Config()
        loader = MemoryLoader(tmp_ws)
        ctx_mgr = ContextManager(config, loader, conv)

        loop = AgentLoop(
            conv,
            ctx_mgr,
            {"bash": MockTool("bash", "output")},
            llm_fn,
            on_assistant_message=lambda m: assistant_msgs.append(m),
            on_tool_call=lambda tc: tool_calls.append(tc),
            on_tool_result=lambda tc: tool_results.append(tc),
        )
        await loop.run()

        assert len(assistant_msgs) == 2  # tool call msg + final msg
        assert len(tool_calls) == 1
        assert len(tool_results) == 1
        assert tool_results[0].result == "output"
