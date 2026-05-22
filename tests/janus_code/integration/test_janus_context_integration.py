"""Integration tests for Janus-Code's JanusContext integration.

These tests verify that the shim-based MVCC protocol is actually wired in:
- File writes via JanusContext tools are isolated (not visible in base_path until commit)
- Abort discards all changes (base_path unchanged)
- Savepoint/rollback works (changes after savepoint are discarded)
- SessionManager.begin_txn() and commit_txn() delegate to JanusContext
- build_tools() roots standard tools in overlay when JanusContext is active
- JanusToolAdapter correctly bridges LangChain tool protocol
- MessageStore uses SQLiteShim — messages are rolled back on abort

All tests skip automatically if fuse-overlayfs/root is unavailable.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from janus_code.config import Config
from janus_code.janus_integration.session_manager import SessionManager, TxnContext
from janus_code.tools.registry import JanusToolAdapter, build_tools, build_tool_schemas


# ── Helpers ──────────────────────────────────────────────────────────

def _require_tar(janus_context: Any) -> None:
    """Skip test if janus_context fixture was skipped."""
    if janus_context is None:
        pytest.skip("JanusContext not available")


# ── JanusContext basic lifecycle ────────────────────────────────────────

class TestJanusContextIsolation:
    """Verify that writes are isolated and only visible after commit."""

    def test_write_isolated_before_commit(self, janus_context: Any, tmp_workspace: Path) -> None:
        """File written via overlay is NOT visible in base_path until commit."""
        _require_tar(janus_context)
        janus_context.begin()
        workdir = janus_context.working_dir
        assert workdir is not None

        new_file = workdir / "new_feature.py"
        new_file.write_text("# new feature\n")

        # Should exist in overlay...
        assert new_file.exists()
        # ...but NOT in the real base path yet
        assert not (tmp_workspace / "new_feature.py").exists()

        janus_context.commit()

        # After commit, must appear in base_path
        assert (tmp_workspace / "new_feature.py").exists()
        assert (tmp_workspace / "new_feature.py").read_text() == "# new feature\n"

    def test_abort_discards_all_changes(self, janus_context: Any, tmp_workspace: Path) -> None:
        """Aborting a transaction leaves base_path unchanged."""
        _require_tar(janus_context)
        janus_context.begin()
        workdir = janus_context.working_dir

        # Write a new file and modify an existing one
        (workdir / "should_not_exist.py").write_text("# transient\n")
        existing = workdir / "src" / "main.py"
        existing.write_text("# replaced\n")

        janus_context.abort()

        assert not (tmp_workspace / "should_not_exist.py").exists()
        assert (tmp_workspace / "src" / "main.py").read_text() == "def main():\n    print('hello')\n"

    def test_edit_existing_file_isolated(self, janus_context: Any, tmp_workspace: Path) -> None:
        """Editing an existing file is isolated to the overlay."""
        _require_tar(janus_context)
        original = "def main():\n    print('hello')\n"
        janus_context.begin()
        workdir = janus_context.working_dir

        target = workdir / "src" / "main.py"
        target.write_text("def main():\n    print('world')\n")

        # Real file unchanged
        assert (tmp_workspace / "src" / "main.py").read_text() == original

        janus_context.commit()
        assert (tmp_workspace / "src" / "main.py").read_text() == "def main():\n    print('world')\n"


# ── Savepoint / rollback ──────────────────────────────────────────────

class TestSavepointRollback:
    """Verify savepoint/rollback preserves changes before the savepoint."""

    def test_rollback_discards_post_savepoint_changes(
        self, janus_context: Any, tmp_workspace: Path
    ) -> None:
        """Changes after savepoint are discarded on rollback; pre-savepoint survives."""
        _require_tar(janus_context)
        janus_context.begin()
        parent_workdir = janus_context.working_dir

        # Write file A before savepoint
        (parent_workdir / "before.py").write_text("# before\n")

        janus_context.savepoint("checkpoint")

        # After savepoint, working_dir returns the child overlay
        child_workdir = janus_context.working_dir
        assert child_workdir != parent_workdir, "savepoint should switch to child overlay"

        # Write file B into child overlay (post-savepoint)
        (child_workdir / "after.py").write_text("# after\n")

        # Rollback discards file B (child overlay)
        janus_context.rollback("checkpoint")

        # After rollback, working_dir is back to parent
        workdir = janus_context.working_dir
        assert (workdir / "before.py").exists(), "pre-savepoint file should survive"
        assert not (workdir / "after.py").exists(), "post-savepoint file should be gone"

        janus_context.commit()

        assert (tmp_workspace / "before.py").exists()
        assert not (tmp_workspace / "after.py").exists()

    def test_commit_after_savepoint_includes_all(
        self, janus_context: Any, tmp_workspace: Path
    ) -> None:
        """If savepoint is not rolled back, commit includes everything."""
        _require_tar(janus_context)
        janus_context.begin()
        workdir = janus_context.working_dir

        (workdir / "file_a.py").write_text("a\n")
        janus_context.savepoint("sp")
        (workdir / "file_b.py").write_text("b\n")

        janus_context.commit()

        assert (tmp_workspace / "file_a.py").exists()
        assert (tmp_workspace / "file_b.py").exists()


# ── SessionManager + JanusContext wiring ───────────────────────────────

class TestSessionManagerJanusIntegration:
    """Verify SessionManager properly delegates to JanusContext."""

    def test_begin_txn_starts_janus_transaction(
        self, janus_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """begin_txn() calls janus_context.begin() and returns active TxnContext."""
        _require_tar(janus_context)
        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            janus_context=janus_context,
        )
        session.start_session()

        assert not janus_context.is_active

        txn = session.begin_txn()

        assert isinstance(txn, TxnContext)
        assert txn.is_active
        assert janus_context.is_active

        session.abort_txn(txn)
        assert not janus_context.is_active

    def test_commit_txn_merges_to_real_project(
        self, janus_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """commit_txn() via TxnContext.commit() calls janus_context.commit()."""
        _require_tar(janus_context)
        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            janus_context=janus_context,
        )
        session.start_session()
        txn = session.begin_txn()

        # Write via overlay working dir
        workdir = janus_context.working_dir
        (workdir / "committed.py").write_text("# committed\n")

        assert not (tmp_workspace / "committed.py").exists()

        session.commit_txn(txn)

        assert (tmp_workspace / "committed.py").exists()
        assert not janus_context.is_active

    def test_abort_txn_discards_changes(
        self, janus_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """abort_txn() via TxnContext.abort() calls janus_context.abort()."""
        _require_tar(janus_context)
        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            janus_context=janus_context,
        )
        session.start_session()
        txn = session.begin_txn()

        workdir = janus_context.working_dir
        (workdir / "discarded.py").write_text("# gone\n")

        session.abort_txn(txn)

        assert not (tmp_workspace / "discarded.py").exists()
        assert not janus_context.is_active

    def test_subtxn_savepoint_and_rollback(
        self, janus_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """begin_subtxn / commit / abort delegate to JanusContext savepoints."""
        _require_tar(janus_context)
        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            janus_context=janus_context,
        )
        session.start_session()
        parent_txn = session.begin_txn()
        workdir = janus_context.working_dir

        (workdir / "parent_write.py").write_text("# parent\n")

        # Begin subtransaction (creates savepoint)
        child_txn = parent_txn.begin_subtxn("explore")
        assert janus_context._child_txn is not None

        # Write in child
        child_workdir = janus_context.working_dir  # tools switched to child
        (child_workdir / "child_write.py").write_text("# child\n")

        # Abort child — child write discarded, parent write survives
        child_txn.abort()
        assert janus_context._child_txn is None

        # Parent commit — only parent_write.py should land
        parent_txn.commit()

        assert (tmp_workspace / "parent_write.py").exists()
        assert not (tmp_workspace / "child_write.py").exists()


# ── build_tools() Janus mode ───────────────────────────────────────────

class TestBuildToolsJanusMode:
    """Verify build_tools roots standard tools in Janus overlay when active."""

    def test_janus_tools_present_when_active(
        self, janus_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """Standard tools stay available and operate on the overlay."""
        _require_tar(janus_context)
        janus_context.begin()

        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            janus_context=janus_context,
        )

        # Standard tool names should be present in Janus mode.
        assert "write_file" in tools
        assert "edit_file" in tools
        assert "bash" in tools
        assert "memory" in tools
        assert "sqlite" in tools

        # janus_txn control tool is internal-only by default
        assert "janus_txn" not in tools

        # Read/search tools still present
        assert "read_file" in tools
        assert "glob" in tools
        assert "ripgrep" in tools

        janus_context.abort()

    def test_tools_bind_project_root_when_janus_context_inactive(
        self, janus_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """Before begin(), tools bind to the project root."""
        _require_tar(janus_context)
        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            janus_context=janus_context,
        )

        assert "write_file" in tools
        assert "edit_file" in tools
        assert "bash" in tools
        assert "memory" in tools
        assert "sqlite" in tools
        assert "janus_txn" not in tools

    def test_janus_tool_adapter_bridges_kwargs(
        self, janus_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """JanusToolAdapter.run(**kwargs) passes dict to LangChain tool."""
        _require_tar(janus_context)
        janus_context.begin()

        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            janus_context=janus_context,
            expose_txn_control=True,
        )

        # janus_txn should be a JanusToolAdapter
        txn_tool = tools.get("janus_txn")
        assert txn_tool is not None
        assert isinstance(txn_tool, JanusToolAdapter)

        # Calling run(**kwargs) should work (status action)
        result = txn_tool.run(action="status")
        assert isinstance(result, str)
        assert "active" in result.lower() or "Transaction" in result

        janus_context.abort()

    def test_tool_schemas_include_janus_tools(
        self, janus_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """build_tool_schemas handles JanusToolAdapter args_schema correctly."""
        _require_tar(janus_context)
        janus_context.begin()

        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            janus_context=janus_context,
            expose_txn_control=True,
        )
        schemas = build_tool_schemas(tools)

        assert len(schemas) > 0
        names = [s["function"]["name"] for s in schemas]
        assert "sqlite" in names
        assert "janus_txn" in names

        janus_context.abort()


# ── Write through standard write_file tool in Janus mode ──────────────

class TestWriteFileToolInJanusMode:
    """Verify write_file goes through overlay when Janus is active."""

    def test_create_file_via_write_file(
        self, janus_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """write_file writes to overlay, not base, until commit."""
        _require_tar(janus_context)
        janus_context.begin()

        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            janus_context=janus_context,
        )

        writer = tools["write_file"]
        result = writer.run(path="overlay_test.py", content="x = 1\n")
        assert "error" not in result.lower(), f"Unexpected error: {result}"

        # File exists in overlay working dir
        workdir = janus_context.working_dir
        assert (workdir / "overlay_test.py").exists()

        # Not yet in base path
        assert not (tmp_workspace / "overlay_test.py").exists()

        janus_context.commit()

        # Now in base path
        assert (tmp_workspace / "overlay_test.py").exists()
        assert (tmp_workspace / "overlay_test.py").read_text() == "x = 1\n"


# ── Transactional message persistence ────────────────────────────────

class TestTransactionalMessagePersistence:
    """Verify that MessageStore participates in the 2PC via SQLiteShim.

    Key invariants:
    - Messages written during a committed turn are visible after commit.
    - Messages written during an aborted turn are NOT visible afterward.
    - Cross-turn isolation: turn N's messages don't bleed into turn N+1.
    - Session metadata (session row) is committed during start_session().
    """

    def test_messages_visible_after_commit(
        self, janus_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """Messages persisted in a committed turn are loadable afterward."""
        _require_tar(janus_context)
        from janus_code.context.conversation import Message

        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            janus_context=janus_context,
        )
        session_id = session.start_session()

        txn = session.begin_txn()
        msg = Message(role="user", content="hello", turn=1)
        session.persist_message(msg)
        session.commit_txn(txn)

        # Open a new transaction to read committed messages
        txn2 = session.begin_txn()
        loaded = session.load_messages()
        session.abort_txn(txn2)

        assert len(loaded) == 1
        assert loaded[0].content == "hello"

    def test_messages_rolled_back_on_abort(
        self, janus_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """Messages written in an aborted turn are not visible afterward."""
        _require_tar(janus_context)
        from janus_code.context.conversation import Message

        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            janus_context=janus_context,
        )
        session.start_session()

        # Turn 1: write and abort
        txn1 = session.begin_txn()
        session.persist_message(Message(role="user", content="should vanish", turn=1))
        session.abort_txn(txn1)

        # Turn 2: read — aborted messages must not appear
        txn2 = session.begin_txn()
        loaded = session.load_messages()
        session.abort_txn(txn2)

        assert len(loaded) == 0, (
            f"Aborted messages must be invisible; got {[m.content for m in loaded]}"
        )

    def test_committed_then_aborted_turn_isolation(
        self, janus_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """Committed messages survive; subsequent aborted turn adds nothing."""
        _require_tar(janus_context)
        from janus_code.context.conversation import Message

        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            janus_context=janus_context,
        )
        session.start_session()

        # Turn 1: commit "good"
        txn1 = session.begin_txn()
        session.persist_message(Message(role="user", content="good", turn=1))
        session.commit_txn(txn1)

        # Turn 2: abort "bad" (simulates agent crash)
        txn2 = session.begin_txn()
        session.persist_message(Message(role="user", content="bad", turn=2))
        session.abort_txn(txn2)

        # Turn 3: only "good" should be visible
        txn3 = session.begin_txn()
        loaded = session.load_messages()
        session.abort_txn(txn3)

        contents = [m.content for m in loaded]
        assert "good" in contents
        assert "bad" not in contents, f"Aborted message leaked: {contents}"

    def test_session_metadata_durable_after_start(
        self, janus_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """start_session() durably commits the session row via bootstrap txn."""
        _require_tar(janus_context)

        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            janus_context=janus_context,
        )
        session_id = session.start_session()

        # Open a new transaction and read the session metadata
        txn = session.begin_txn()
        meta = session.message_store.get_session(session_id)
        session.abort_txn(txn)

        assert meta is not None
        assert meta["session_id"] == session_id
        assert meta["project_path"] == str(tmp_workspace)

    def test_filesystem_and_messages_atomic(
        self, janus_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """Both file writes and messages commit or abort together (atomicity)."""
        _require_tar(janus_context)
        from janus_code.context.conversation import Message

        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            janus_context=janus_context,
        )
        session.start_session()

        # Turn 1: write a file and a message, then abort both
        txn1 = session.begin_txn()
        workdir = janus_context.working_dir
        (workdir / "atomic_test.py").write_text("x = 1\n")
        session.persist_message(Message(role="user", content="atomic msg", turn=1))
        session.abort_txn(txn1)

        # Neither the file nor the message should have landed
        assert not (tmp_workspace / "atomic_test.py").exists(), "file must not survive abort"
        txn2 = session.begin_txn()
        loaded = session.load_messages()
        session.abort_txn(txn2)
        assert len(loaded) == 0, f"messages must not survive abort; got {[m.content for m in loaded]}"

        # Turn 2: write both and commit — both should land
        txn3 = session.begin_txn()
        workdir = janus_context.working_dir
        (workdir / "atomic_test.py").write_text("x = 2\n")
        session.persist_message(Message(role="user", content="committed msg", turn=3))
        session.commit_txn(txn3)

        assert (tmp_workspace / "atomic_test.py").exists(), "file must survive commit"
        txn4 = session.begin_txn()
        loaded = session.load_messages()
        session.abort_txn(txn4)
        contents = [m.content for m in loaded]
        assert "committed msg" in contents, f"committed message missing; got {contents}"


class TestFilesystemAndSQLiteToolAtomicity:
    """Verify atomicity across filesystem + exposed sqlite tool."""

    def test_filesystem_and_sqlite_tool_abort_together(
        self, janus_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """Aborting a txn discards both file writes and sqlite row writes."""
        _require_tar(janus_context)

        # Bootstrap: register table once.
        janus_context.begin()
        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            janus_context=janus_context,
        )
        sqlite_tool = tools["sqlite"]
        sqlite_tool.run(
            command="register_table",
            table="kv_abort",
            columns=["id TEXT", "value TEXT"],
            pk_column="id",
        )
        janus_context.commit()

        # Transaction under test: write FS + SQLite then abort.
        janus_context.begin()
        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            janus_context=janus_context,
        )
        sqlite_tool = tools["sqlite"]
        workdir = janus_context.working_dir
        (workdir / "cross_abort.txt").write_text("transient\n")
        sqlite_tool.run(
            command="put",
            table="kv_abort",
            row={"id": "k1", "value": "v1"},
        )
        janus_context.abort()

        # FS effect discarded.
        assert not (tmp_workspace / "cross_abort.txt").exists()

        # SQLite effect discarded.
        janus_context.begin()
        row = janus_context.sqlite.run({
            "command": "get",
            "table": "kv_abort",
            "pk_value": "k1",
        })
        janus_context.abort()
        assert "No row found" in row

    def test_filesystem_and_sqlite_tool_commit_together(
        self, janus_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """Committing a txn persists both file writes and sqlite row writes."""
        _require_tar(janus_context)

        # Bootstrap: register table once.
        janus_context.begin()
        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            janus_context=janus_context,
        )
        sqlite_tool = tools["sqlite"]
        sqlite_tool.run(
            command="register_table",
            table="kv_commit",
            columns=["id TEXT", "value TEXT"],
            pk_column="id",
        )
        janus_context.commit()

        # Transaction under test: write FS + SQLite then commit.
        janus_context.begin()
        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            janus_context=janus_context,
        )
        sqlite_tool = tools["sqlite"]
        workdir = janus_context.working_dir
        (workdir / "cross_commit.txt").write_text("durable\n")
        sqlite_tool.run(
            command="put",
            table="kv_commit",
            row={"id": "k2", "value": "v2"},
        )
        janus_context.commit()

        # FS effect persisted.
        assert (tmp_workspace / "cross_commit.txt").exists()
        assert (tmp_workspace / "cross_commit.txt").read_text() == "durable\n"

        # SQLite effect persisted.
        janus_context.begin()
        row = janus_context.sqlite.run({
            "command": "get",
            "table": "kv_commit",
            "pk_value": "k2",
        })
        janus_context.abort()
        assert '"id": "k2"' in row
        assert '"value": "v2"' in row
