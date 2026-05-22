"""Unit tests for MessageStore."""

import pytest

from chronos_code.context.conversation import Message, ToolCall
from chronos_code.context.message_store import MessageStore
from tests.chronos_code.conftest import MockChronosContext


@pytest.fixture
def store(tmp_path) -> MessageStore:
    ctx = MockChronosContext(tmp_path)
    txn = ctx.begin()
    s = MessageStore(":memory:", shim=ctx._sqlite_shim, txn=txn)
    s.create_session("test-session", "/tmp/project")
    return s


class TestMessageStore:
    def test_create_and_get_session(self, store: MessageStore):
        session = store.get_session("test-session")
        assert session is not None
        assert session["project_path"] == "/tmp/project"
        assert session["status"] == "active"

    def test_list_sessions(self, store: MessageStore):
        store.create_session("session-2", "/tmp/other")
        sessions = store.list_sessions()
        assert len(sessions) == 2

    def test_store_and_load_message(self, store: MessageStore):
        msg = Message(role="user", content="hello", turn=1)
        store.store_message("test-session", msg)

        loaded = store.load_messages("test-session")
        assert len(loaded) == 1
        assert loaded[0].role == "user"
        assert loaded[0].content == "hello"
        assert loaded[0].turn == 1

    def test_store_message_with_tool_calls(self, store: MessageStore):
        tc = ToolCall(id="c1", name="read_file", arguments={"path": "x.py"})
        msg = Message(role="assistant", content="", tool_calls=[tc], turn=1)
        store.store_message("test-session", msg)

        loaded = store.load_messages("test-session")
        assert len(loaded) == 1
        assert loaded[0].tool_calls is not None
        assert loaded[0].tool_calls[0].name == "read_file"

    def test_store_multiple_messages_ordering(self, store: MessageStore):
        store.store_message(
            "test-session", Message(role="user", content="q1", turn=1)
        )
        store.store_message(
            "test-session", Message(role="assistant", content="a1", turn=1)
        )
        store.store_message(
            "test-session", Message(role="user", content="q2", turn=2)
        )

        loaded = store.load_messages("test-session")
        assert len(loaded) == 3
        assert loaded[0].content == "q1"
        assert loaded[1].content == "a1"
        assert loaded[2].content == "q2"

    def test_get_last_turn(self, store: MessageStore):
        store.store_message(
            "test-session", Message(role="user", content="t1", turn=1)
        )
        store.store_message(
            "test-session", Message(role="user", content="t3", turn=3)
        )
        assert store.get_last_turn("test-session") == 3

    def test_store_tool_call(self, store: MessageStore):
        tc = ToolCall(
            id="c1", name="bash", arguments={"command": "ls"},
            result="file.py", duration_ms=50
        )
        store.store_tool_call("test-session", 1, tc)

        loaded = store.load_tool_calls("test-session")
        assert len(loaded) == 1
        assert loaded[0].name == "bash"
        assert loaded[0].result == "file.py"

    def test_load_tool_calls_by_turn(self, store: MessageStore):
        store.store_tool_call(
            "test-session", 1,
            ToolCall(id="c1", name="bash", arguments={})
        )
        store.store_tool_call(
            "test-session", 2,
            ToolCall(id="c2", name="read_file", arguments={})
        )

        turn1 = store.load_tool_calls("test-session", turn=1)
        assert len(turn1) == 1
        assert turn1[0].name == "bash"

    def test_store_and_load_todos(self, store: MessageStore):
        todos = [
            {"id": "1", "title": "Fix bug", "status": "completed"},
            {"id": "2", "title": "Write tests", "status": "pending"},
        ]
        store.store_todos("test-session", todos)

        loaded = store.load_todos("test-session")
        assert len(loaded) == 2
        assert loaded[0]["title"] == "Fix bug"
        assert loaded[0]["status"] == "completed"

    def test_store_todos_replaces(self, store: MessageStore):
        store.store_todos("test-session", [{"id": "1", "title": "Old"}])
        store.store_todos("test-session", [{"id": "2", "title": "New"}])

        loaded = store.load_todos("test-session")
        assert len(loaded) == 1
        assert loaded[0]["title"] == "New"

    def test_session_isolation(self, store: MessageStore):
        store.create_session("other", "/tmp/other")
        store.store_message(
            "test-session", Message(role="user", content="s1", turn=1)
        )
        store.store_message(
            "other", Message(role="user", content="s2", turn=1)
        )

        s1_msgs = store.load_messages("test-session")
        s2_msgs = store.load_messages("other")
        assert len(s1_msgs) == 1
        assert len(s2_msgs) == 1
        assert s1_msgs[0].content == "s1"
        assert s2_msgs[0].content == "s2"
