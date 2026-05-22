"""Unit tests for Conversation."""

import json

import pytest

from janus_code.context.conversation import Conversation, Message, ToolCall


class TestToolCall:
    def test_create_with_auto_id(self):
        tc = ToolCall(name="read_file", arguments={"path": "test.py"})
        assert tc.id.startswith("call_")
        assert tc.name == "read_file"

    def test_roundtrip(self):
        tc = ToolCall(
            id="call_abc",
            name="bash",
            arguments={"command": "ls"},
            result="file.py",
            duration_ms=42,
        )
        d = tc.to_dict()
        tc2 = ToolCall.from_dict(d)
        assert tc2.id == tc.id
        assert tc2.name == tc.name
        assert tc2.arguments == tc.arguments
        assert tc2.result == tc.result
        assert tc2.duration_ms == tc.duration_ms


class TestMessage:
    def test_basic_message(self):
        msg = Message(role="user", content="hello")
        assert msg.role == "user"
        assert msg.content == "hello"

    def test_assistant_with_tool_calls(self):
        tc = ToolCall(name="read_file", arguments={"path": "f.py"})
        msg = Message(role="assistant", content="", tool_calls=[tc])
        assert msg.tool_calls is not None
        assert len(msg.tool_calls) == 1

    def test_tool_result_message(self):
        msg = Message(role="tool", content="file contents", tool_call_id="call_abc")
        assert msg.tool_call_id == "call_abc"

    def test_serialization_roundtrip(self):
        tc = ToolCall(name="bash", arguments={"command": "ls"}, result="ok")
        msg = Message(
            role="assistant", content="running", tool_calls=[tc], turn=3
        )
        d = msg.to_dict()
        msg2 = Message.from_dict(d)
        assert msg2.role == msg.role
        assert msg2.content == msg.content
        assert msg2.turn == msg.turn
        assert len(msg2.tool_calls) == 1
        assert msg2.tool_calls[0].name == "bash"

    def test_to_llm_format(self):
        tc = ToolCall(id="c1", name="read_file", arguments={"path": "x"})
        msg = Message(role="assistant", content="", tool_calls=[tc])
        llm = msg.to_llm_format()
        assert llm["role"] == "assistant"
        assert "tool_calls" in llm
        assert llm["tool_calls"][0]["function"]["name"] == "read_file"


class TestConversation:
    def test_append_and_length(self):
        conv = Conversation()
        conv.append_user("hi")
        conv.append_assistant("hello")
        assert len(conv) == 2

    def test_turn_tracking(self):
        conv = Conversation()
        assert conv.turn == 0
        conv.advance_turn()
        assert conv.turn == 1
        conv.append_user("msg")
        assert conv.messages[-1].turn == 1

    def test_json_roundtrip(self):
        conv = Conversation()
        conv.advance_turn()
        conv.append_user("test")
        conv.append_assistant("response")

        json_str = conv.to_json()
        conv2 = Conversation.from_json(json_str)
        assert len(conv2) == 2
        assert conv2.turn == 1
        assert conv2.messages[0].content == "test"

    def test_get_last_n_turns(self):
        conv = Conversation()
        conv.advance_turn()
        conv.append_user("turn 1")
        conv.advance_turn()
        conv.append_user("turn 2")
        conv.advance_turn()
        conv.append_user("turn 3")

        last_2 = conv.get_last_n_turns(2)
        assert len(last_2) == 2
        assert last_2[0].content == "turn 2"

    def test_truncate_to_turns(self):
        conv = Conversation()
        conv.append_system("system msg")
        conv.advance_turn()
        conv.append_user("old")
        conv.advance_turn()
        conv.append_user("new")

        removed = conv.truncate_to_turns(1)
        assert len(removed) == 1  # "old" was removed
        # System message is kept
        assert any(m.role == "system" for m in conv.messages)

    def test_to_llm_messages(self):
        conv = Conversation()
        conv.append_system("sys")
        conv.append_user("hi")
        llm_msgs = conv.to_llm_messages()
        assert len(llm_msgs) == 2
        assert llm_msgs[0]["role"] == "system"

    def test_clear(self):
        conv = Conversation()
        conv.advance_turn()
        conv.append_user("test")
        conv.clear()
        assert len(conv) == 0
        assert conv.turn == 1  # turn counter preserved
