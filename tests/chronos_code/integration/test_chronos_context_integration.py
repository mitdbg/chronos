"""Integration tests for Chronos-Code's ChronosContext integration.

These tests verify that the shim-based MVCC protocol is actually wired in:
- File writes via ChronosContext tools are isolated (not visible in base_path until commit)
- Abort discards all changes (base_path unchanged)
- Savepoint/rollback works (changes after savepoint are discarded)
- SessionManager.begin_txn() and commit_txn() delegate to ChronosContext
- build_tools() roots standard tools in overlay when ChronosContext is active
- ChronosToolAdapter correctly bridges LangChain tool protocol
- MessageStore uses SQLiteShim — messages are rolled back on abort

All tests skip automatically if fuse-overlayfs/root is unavailable.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from chronos_code.config import Config
from chronos_code.chronos_integration.session_manager import SessionManager, TxnContext
from chronos_code.tools.registry import ChronosToolAdapter, build_tools, build_tool_schemas


# ── Helpers ──────────────────────────────────────────────────────────

def _require_tar(chronos_context: Any) -> None:
    """Skip test if chronos_context fixture was skipped."""
    if chronos_context is None:
        pytest.skip("ChronosContext not available")


# ── ChronosContext basic lifecycle ────────────────────────────────────────

class TestChronosContextIsolation:
    """Verify that writes are isolated and only visible after commit."""

    def test_write_isolated_before_commit(self, chronos_context: Any, tmp_workspace: Path) -> None:
        """File written via overlay is NOT visible in base_path until commit."""
        _require_tar(chronos_context)
        chronos_context.begin()
        workdir = chronos_context.working_dir
        assert workdir is not None

        new_file = workdir / "new_feature.py"
        new_file.write_text("# new feature\n")

        # Should exist in overlay...
        assert new_file.exists()
        # ...but NOT in the real base path yet
        assert not (tmp_workspace / "new_feature.py").exists()

        chronos_context.commit()

        # After commit, must appear in base_path
        assert (tmp_workspace / "new_feature.py").exists()
        assert (tmp_workspace / "new_feature.py").read_text() == "# new feature\n"

    def test_abort_discards_all_changes(self, chronos_context: Any, tmp_workspace: Path) -> None:
        """Aborting a transaction leaves base_path unchanged."""
        _require_tar(chronos_context)
        chronos_context.begin()
        workdir = chronos_context.working_dir

        # Write a new file and modify an existing one
        (workdir / "should_not_exist.py").write_text("# transient\n")
        existing = workdir / "src" / "main.py"
        existing.write_text("# replaced\n")

        chronos_context.abort()

        assert not (tmp_workspace / "should_not_exist.py").exists()
        assert (tmp_workspace / "src" / "main.py").read_text() == "def main():\n    print('hello')\n"

    def test_edit_existing_file_isolated(self, chronos_context: Any, tmp_workspace: Path) -> None:
        """Editing an existing file is isolated to the overlay."""
        _require_tar(chronos_context)
        original = "def main():\n    print('hello')\n"
        chronos_context.begin()
        workdir = chronos_context.working_dir

        target = workdir / "src" / "main.py"
        target.write_text("def main():\n    print('world')\n")

        # Real file unchanged
        assert (tmp_workspace / "src" / "main.py").read_text() == original

        chronos_context.commit()
        assert (tmp_workspace / "src" / "main.py").read_text() == "def main():\n    print('world')\n"


# ── Savepoint / rollback ──────────────────────────────────────────────

class TestSavepointRollback:
    """Verify savepoint/rollback preserves changes before the savepoint."""

    def test_rollback_discards_post_savepoint_changes(
        self, chronos_context: Any, tmp_workspace: Path
    ) -> None:
        """Changes after savepoint are discarded on rollback; pre-savepoint survives."""
        _require_tar(chronos_context)
        chronos_context.begin()
        parent_workdir = chronos_context.working_dir

        # Write file A before savepoint
        (parent_workdir / "before.py").write_text("# before\n")

        chronos_context.savepoint("checkpoint")

        # After savepoint, working_dir returns the child overlay
        child_workdir = chronos_context.working_dir
        assert child_workdir != parent_workdir, "savepoint should switch to child overlay"

        # Write file B into child overlay (post-savepoint)
        (child_workdir / "after.py").write_text("# after\n")

        # Rollback discards file B (child overlay)
        chronos_context.rollback("checkpoint")

        # After rollback, working_dir is back to parent
        workdir = chronos_context.working_dir
        assert (workdir / "before.py").exists(), "pre-savepoint file should survive"
        assert not (workdir / "after.py").exists(), "post-savepoint file should be gone"

        chronos_context.commit()

        assert (tmp_workspace / "before.py").exists()
        assert not (tmp_workspace / "after.py").exists()

    def test_commit_after_savepoint_includes_all(
        self, chronos_context: Any, tmp_workspace: Path
    ) -> None:
        """If savepoint is not rolled back, commit includes everything."""
        _require_tar(chronos_context)
        chronos_context.begin()
        workdir = chronos_context.working_dir

        (workdir / "file_a.py").write_text("a\n")
        chronos_context.savepoint("sp")
        (workdir / "file_b.py").write_text("b\n")

        chronos_context.commit()

        assert (tmp_workspace / "file_a.py").exists()
        assert (tmp_workspace / "file_b.py").exists()


# ── SessionManager + ChronosContext wiring ───────────────────────────────

class TestSessionManagerChronosIntegration:
    """Verify SessionManager properly delegates to ChronosContext."""

    def test_begin_txn_starts_chronos_transaction(
        self, chronos_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """begin_txn() calls chronos_context.begin() and returns active TxnContext."""
        _require_tar(chronos_context)
        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
        )
        session.start_session()

        assert not chronos_context.is_active

        txn = session.begin_txn()

        assert isinstance(txn, TxnContext)
        assert txn.is_active
        assert chronos_context.is_active

        session.abort_txn(txn)
        assert not chronos_context.is_active

    def test_commit_txn_merges_to_real_project(
        self, chronos_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """commit_txn() via TxnContext.commit() calls chronos_context.commit()."""
        _require_tar(chronos_context)
        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
        )
        session.start_session()
        txn = session.begin_txn()

        # Write via overlay working dir
        workdir = chronos_context.working_dir
        (workdir / "committed.py").write_text("# committed\n")

        assert not (tmp_workspace / "committed.py").exists()

        session.commit_txn(txn)

        assert (tmp_workspace / "committed.py").exists()
        assert not chronos_context.is_active

    def test_abort_txn_discards_changes(
        self, chronos_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """abort_txn() via TxnContext.abort() calls chronos_context.abort()."""
        _require_tar(chronos_context)
        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
        )
        session.start_session()
        txn = session.begin_txn()

        workdir = chronos_context.working_dir
        (workdir / "discarded.py").write_text("# gone\n")

        session.abort_txn(txn)

        assert not (tmp_workspace / "discarded.py").exists()
        assert not chronos_context.is_active

    def test_subtxn_savepoint_and_rollback(
        self, chronos_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """begin_subtxn / commit / abort delegate to ChronosContext savepoints."""
        _require_tar(chronos_context)
        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
        )
        session.start_session()
        parent_txn = session.begin_txn()
        workdir = chronos_context.working_dir

        (workdir / "parent_write.py").write_text("# parent\n")

        # Begin subtransaction (creates savepoint)
        child_txn = parent_txn.begin_subtxn("explore")
        assert chronos_context._child_txn is not None

        # Write in child
        child_workdir = chronos_context.working_dir  # tools switched to child
        (child_workdir / "child_write.py").write_text("# child\n")

        # Abort child — child write discarded, parent write survives
        child_txn.abort()
        assert chronos_context._child_txn is None

        # Parent commit — only parent_write.py should land
        parent_txn.commit()

        assert (tmp_workspace / "parent_write.py").exists()
        assert not (tmp_workspace / "child_write.py").exists()


# ── build_tools() Chronos mode ───────────────────────────────────────────

class TestBuildToolsChronosMode:
    """Verify build_tools roots standard tools in Chronos overlay when active."""

    def test_chronos_tools_present_when_active(
        self, chronos_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """Standard tools stay available and operate on the overlay."""
        _require_tar(chronos_context)
        chronos_context.begin()

        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
        )

        # Standard tool names should be present in Chronos mode.
        assert "write_file" in tools
        assert "edit_file" in tools
        assert "bash" in tools
        assert "memory" in tools
        assert "sqlite" in tools

        # chronos_txn control tool is internal-only by default
        assert "chronos_txn" not in tools

        # Read/search tools still present
        assert "read_file" in tools
        assert "glob" in tools
        assert "ripgrep" in tools

        chronos_context.abort()

    def test_tools_bind_project_root_when_chronos_context_inactive(
        self, chronos_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """Before begin(), tools bind to the project root."""
        _require_tar(chronos_context)
        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
        )

        assert "write_file" in tools
        assert "edit_file" in tools
        assert "bash" in tools
        assert "memory" in tools
        assert "sqlite" in tools
        assert "chronos_txn" not in tools

    def test_chronos_tool_adapter_bridges_kwargs(
        self, chronos_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """ChronosToolAdapter.run(**kwargs) passes dict to LangChain tool."""
        _require_tar(chronos_context)
        chronos_context.begin()

        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
            expose_txn_control=True,
        )

        # chronos_txn should be a ChronosToolAdapter
        txn_tool = tools.get("chronos_txn")
        assert txn_tool is not None
        assert isinstance(txn_tool, ChronosToolAdapter)

        # Calling run(**kwargs) should work (status action)
        result = txn_tool.run(action="status")
        assert isinstance(result, str)
        assert "active" in result.lower() or "Transaction" in result

        chronos_context.abort()

    def test_tool_schemas_include_chronos_tools(
        self, chronos_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """build_tool_schemas handles ChronosToolAdapter args_schema correctly."""
        _require_tar(chronos_context)
        chronos_context.begin()

        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
            expose_txn_control=True,
        )
        schemas = build_tool_schemas(tools)

        assert len(schemas) > 0
        names = [s["function"]["name"] for s in schemas]
        assert "sqlite" in names
        assert "chronos_txn" in names

        chronos_context.abort()


# ── Write through standard write_file tool in Chronos mode ──────────────

class TestWriteFileToolInChronosMode:
    """Verify write_file goes through overlay when Chronos is active."""

    def test_create_file_via_write_file(
        self, chronos_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """write_file writes to overlay, not base, until commit."""
        _require_tar(chronos_context)
        chronos_context.begin()

        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
        )

        writer = tools["write_file"]
        result = writer.run(path="overlay_test.py", content="x = 1\n")
        assert "error" not in result.lower(), f"Unexpected error: {result}"

        # File exists in overlay working dir
        workdir = chronos_context.working_dir
        assert (workdir / "overlay_test.py").exists()

        # Not yet in base path
        assert not (tmp_workspace / "overlay_test.py").exists()

        chronos_context.commit()

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
        self, chronos_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """Messages persisted in a committed turn are loadable afterward."""
        _require_tar(chronos_context)
        from chronos_code.context.conversation import Message

        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
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
        self, chronos_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """Messages written in an aborted turn are not visible afterward."""
        _require_tar(chronos_context)
        from chronos_code.context.conversation import Message

        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
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
        self, chronos_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """Committed messages survive; subsequent aborted turn adds nothing."""
        _require_tar(chronos_context)
        from chronos_code.context.conversation import Message

        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
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
        self, chronos_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """start_session() durably commits the session row via bootstrap txn."""
        _require_tar(chronos_context)

        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
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
        self, chronos_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """Both file writes and messages commit or abort together (atomicity)."""
        _require_tar(chronos_context)
        from chronos_code.context.conversation import Message

        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
        )
        session.start_session()

        # Turn 1: write a file and a message, then abort both
        txn1 = session.begin_txn()
        workdir = chronos_context.working_dir
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
        workdir = chronos_context.working_dir
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
        self, chronos_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """Aborting a txn discards both file writes and sqlite row writes."""
        _require_tar(chronos_context)

        # Bootstrap: register table once.
        chronos_context.begin()
        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
        )
        sqlite_tool = tools["sqlite"]
        sqlite_tool.run(
            command="register_table",
            table="kv_abort",
            columns=["id TEXT", "value TEXT"],
            pk_column="id",
        )
        chronos_context.commit()

        # Transaction under test: write FS + SQLite then abort.
        chronos_context.begin()
        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
        )
        sqlite_tool = tools["sqlite"]
        workdir = chronos_context.working_dir
        (workdir / "cross_abort.txt").write_text("transient\n")
        sqlite_tool.run(
            command="put",
            table="kv_abort",
            row={"id": "k1", "value": "v1"},
        )
        chronos_context.abort()

        # FS effect discarded.
        assert not (tmp_workspace / "cross_abort.txt").exists()

        # SQLite effect discarded.
        chronos_context.begin()
        row = chronos_context.sqlite.run({
            "command": "get",
            "table": "kv_abort",
            "pk_value": "k1",
        })
        chronos_context.abort()
        assert "No row found" in row

    def test_filesystem_and_sqlite_tool_commit_together(
        self, chronos_context: Any, tmp_workspace: Path, config: Config
    ) -> None:
        """Committing a txn persists both file writes and sqlite row writes."""
        _require_tar(chronos_context)

        # Bootstrap: register table once.
        chronos_context.begin()
        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
        )
        sqlite_tool = tools["sqlite"]
        sqlite_tool.run(
            command="register_table",
            table="kv_commit",
            columns=["id TEXT", "value TEXT"],
            pk_column="id",
        )
        chronos_context.commit()

        # Transaction under test: write FS + SQLite then commit.
        chronos_context.begin()
        tools = build_tools(
            working_dir=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
        )
        sqlite_tool = tools["sqlite"]
        workdir = chronos_context.working_dir
        (workdir / "cross_commit.txt").write_text("durable\n")
        sqlite_tool.run(
            command="put",
            table="kv_commit",
            row={"id": "k2", "value": "v2"},
        )
        chronos_context.commit()

        # FS effect persisted.
        assert (tmp_workspace / "cross_commit.txt").exists()
        assert (tmp_workspace / "cross_commit.txt").read_text() == "durable\n"

        # SQLite effect persisted.
        chronos_context.begin()
        row = chronos_context.sqlite.run({
            "command": "get",
            "table": "kv_commit",
            "pk_value": "k2",
        })
        chronos_context.abort()
        assert '"id": "k2"' in row
        assert '"value": "v2"' in row
