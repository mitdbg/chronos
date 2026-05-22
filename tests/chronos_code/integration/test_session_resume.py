"""Integration tests — session resume and crash recovery.

Tests that after committing transactions, the session can be
resumed and all messages and state are intact.
"""

import pytest
from pathlib import Path

from chronos_code.config import Config
from chronos_code.context.conversation import Conversation, Message
from chronos_code.chronos_integration.session_manager import SessionManager
from tests.chronos_code.conftest import MockChronosContext


class TestSessionResume:
    """Test session resume from committed state."""

    def test_resume_loads_messages(self, tmp_path):
        """Commit messages, resume → messages loaded."""
        config = Config()
        shared_chronos = MockChronosContext(tmp_path)
        session = SessionManager(
            project_path=tmp_path,
            config=config,
            chronos_context=shared_chronos,
        )
        session_id = session.start_session()

        # Simulate two turns with committed messages
        msgs = [
            Message(role="user", content="Hello", turn=1),
            Message(role="assistant", content="Hi there", turn=1),
            Message(role="user", content="Write code", turn=2),
            Message(role="assistant", content="Done!", turn=2),
        ]
        session.persist_messages(msgs)
        session.turn_number = 2

        # Resume in a new session manager instance
        session2 = SessionManager(
            project_path=tmp_path,
            config=config,
            chronos_context=shared_chronos,
        )
        loaded = session2.resume_session(session_id)

        assert len(loaded) == 4
        assert loaded[0].content == "Hello"
        assert loaded[-1].content == "Done!"
        assert session2.turn_number == 3  # ready for next turn

    def test_resume_with_no_messages(self, tmp_path):
        """Resume empty session → turn 0."""
        config = Config()
        shared_chronos = MockChronosContext(tmp_path)
        session = SessionManager(
            project_path=tmp_path,
            config=config,
            chronos_context=shared_chronos,
        )
        session_id = session.start_session()

        session2 = SessionManager(
            project_path=tmp_path,
            config=config,
            chronos_context=shared_chronos,
        )
        loaded = session2.resume_session(session_id)

        assert len(loaded) == 0
        assert session2.turn_number == 0

    def test_uncommitted_txn_not_persisted(self, tmp_path):
        """Messages in an aborted txn are not persisted."""
        config = Config()
        shared_chronos = MockChronosContext(tmp_path)
        session = SessionManager(
            project_path=tmp_path,
            config=config,
            chronos_context=shared_chronos,
        )
        session_id = session.start_session()

        # Committed turn
        session.persist_messages([
            Message(role="user", content="Committed msg", turn=1),
        ])

        # Start a txn but don't persist messages (simulating abort)
        txn = session.begin_txn()
        # Agent does work but txn aborts
        txn.abort()
        # No persist_messages call for the aborted turn

        # Resume → only committed messages
        session2 = SessionManager(
            project_path=tmp_path,
            config=config,
            chronos_context=shared_chronos,
        )
        loaded = session2.resume_session(session_id)

        assert len(loaded) == 1
        assert loaded[0].content == "Committed msg"

    def test_multiple_sessions(self, tmp_path):
        """Multiple sessions have isolated message stores."""
        config = Config()
        shared_chronos = MockChronosContext(tmp_path)

        s1 = SessionManager(
            project_path=tmp_path,
            config=config,
            chronos_context=shared_chronos,
        )
        id1 = s1.start_session()
        s1.persist_messages([Message(role="user", content="Session 1", turn=1)])

        s2 = SessionManager(
            project_path=tmp_path,
            config=config,
            chronos_context=shared_chronos,
        )
        id2 = s2.start_session()
        s2.persist_messages([Message(role="user", content="Session 2", turn=1)])

        # Each session loads its own messages
        tx = s1.begin_txn()
        loaded1 = s1.message_store.load_messages(id1)
        loaded2 = s1.message_store.load_messages(id2)
        s1.abort_txn(tx)

        assert len(loaded1) == 1
        assert loaded1[0].content == "Session 1"
        assert len(loaded2) == 1
        assert loaded2[0].content == "Session 2"


class TestMemoryPersistence:
    """Test transactional memory persistence."""

    def test_memory_write_and_read(self, tmp_path):
        """Write memory in committed txn → memory persists."""
        from chronos_code.tools.memory_tool import MemoryTool

        config = Config()
        tool = MemoryTool(working_dir=tmp_path)

        # Write memory
        result = tool.run(command="write", path="learnings", content="Tests pass!")
        assert "written" in result.lower() or "success" in result.lower() or "wrote" in result.lower()

        # Read it back
        result = tool.run(command="read", path="learnings")
        assert "Tests pass!" in result

    def test_memory_list(self, tmp_path):
        """List memory files."""
        from chronos_code.tools.memory_tool import MemoryTool

        tool = MemoryTool(working_dir=tmp_path)
        tool.run(command="write", path="topic1", content="content1")
        tool.run(command="write", path="topic2", content="content2")

        result = tool.run(command="list")
        assert "topic1" in result
        assert "topic2" in result

    def test_memory_append(self, tmp_path):
        """Append to existing memory."""
        from chronos_code.tools.memory_tool import MemoryTool

        tool = MemoryTool(working_dir=tmp_path)
        tool.run(command="write", path="notes", content="Line 1")
        tool.run(command="append", path="notes", content="Line 2")

        result = tool.run(command="read", path="notes")
        assert "Line 1" in result
        assert "Line 2" in result
