"""Tests for JanusContext and JanusTransactionControl.

Tests the full transactional lifecycle: begin, commit, abort,
savepoints, and the transaction control tool.

Run with: sudo python -m pytest tests/test_context.py -v
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Generator

import pytest

from janus_core.transaction.coordinator import TransactionCoordinator
from janus_core.transaction.shim_fs import OverlayFSShim

from langchain_janus.bash import JanusBash
from langchain_janus.context import (
    JanusContext,
    JanusTransactionControl,
    create_janus_tools,
    create_janus_tools_with_active_txn,
)
from langchain_janus.file_editor import JanusFileEditor
from langchain_janus.memory import JanusMemory


def _require_root() -> None:
    if os.geteuid() != 0:
        pytest.skip("Requires root for OverlayFS")


@pytest.fixture
def fresh_base_dir() -> Generator[Path, None, None]:
    """Separate base dir for context-level tests (not sharing conftest)."""
    _require_root()
    d = Path(tempfile.mkdtemp(prefix="janus_ctx_test_"))
    (d / "original.txt").write_text("original content\n")
    (d / "src").mkdir()
    (d / "src" / "app.py").write_text("app = True\n")
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ── JanusContext lifecycle tests ───────────────────────────────────────


class TestJanusContextLifecycle:
    """Tests for begin / commit / abort lifecycle."""

    def test_begin_creates_overlay(self, fresh_base_dir: Path) -> None:
        """begin() mounts the overlay and creates tools."""
        ctx = JanusContext(fresh_base_dir)
        txn = ctx.begin()

        assert ctx.is_active
        assert ctx.working_dir is not None
        assert ctx.working_dir.is_dir()
        assert txn.is_active

        # Tools are available
        assert isinstance(ctx.file_editor, JanusFileEditor)
        assert isinstance(ctx.memory, JanusMemory)
        assert isinstance(ctx.bash, JanusBash)

        ctx.abort()

    def test_commit_merges_to_base(self, fresh_base_dir: Path) -> None:
        """commit() merges overlay changes to the real project."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()

        # Make a change in the overlay
        ctx.file_editor.invoke(
            {"command": "create", "path": "committed.txt", "file_text": "committed!"}
        )
        # Not in base yet
        assert not (fresh_base_dir / "committed.txt").exists()

        ctx.commit()

        # Now in base
        assert (fresh_base_dir / "committed.txt").exists()
        assert "committed!" in (fresh_base_dir / "committed.txt").read_text()

    def test_abort_discards_changes(self, fresh_base_dir: Path) -> None:
        """abort() discards all overlay changes."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()

        ctx.file_editor.invoke(
            {"command": "create", "path": "aborted.txt", "file_text": "should not persist"}
        )
        ctx.abort()

        assert not (fresh_base_dir / "aborted.txt").exists()

    def test_double_begin_raises(self, fresh_base_dir: Path) -> None:
        """Calling begin() twice without commit/abort raises."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()
        with pytest.raises(RuntimeError, match="already active"):
            ctx.begin()
        ctx.abort()

    def test_commit_without_begin_raises(self, fresh_base_dir: Path) -> None:
        """Commit without begin raises."""
        ctx = JanusContext(fresh_base_dir)
        with pytest.raises(RuntimeError, match="No active"):
            ctx.commit()

    def test_abort_without_begin_raises(self, fresh_base_dir: Path) -> None:
        """Abort without begin raises."""
        ctx = JanusContext(fresh_base_dir)
        with pytest.raises(RuntimeError, match="No active"):
            ctx.abort()

    def test_tools_unavailable_before_begin(self, fresh_base_dir: Path) -> None:
        """Accessing tools before begin() raises."""
        ctx = JanusContext(fresh_base_dir)
        with pytest.raises(RuntimeError, match="not begun"):
            _ = ctx.file_editor
        with pytest.raises(RuntimeError, match="not begun"):
            _ = ctx.memory
        with pytest.raises(RuntimeError, match="not begun"):
            _ = ctx.bash

    def test_tools_unavailable_after_commit(self, fresh_base_dir: Path) -> None:
        """Tools are cleaned up after commit."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()
        ctx.commit()
        with pytest.raises(RuntimeError, match="not begun"):
            _ = ctx.file_editor

    def test_sequential_transactions(self, fresh_base_dir: Path) -> None:
        """Can begin a new transaction after commit."""
        ctx = JanusContext(fresh_base_dir)

        ctx.begin()
        ctx.file_editor.invoke(
            {"command": "create", "path": "txn1.txt", "file_text": "first"}
        )
        ctx.commit()
        assert (fresh_base_dir / "txn1.txt").exists()

        ctx.begin()
        ctx.file_editor.invoke(
            {"command": "create", "path": "txn2.txt", "file_text": "second"}
        )
        ctx.commit()
        assert (fresh_base_dir / "txn2.txt").exists()


# ── Context manager tests ────────────────────────────────────────────


class TestContextManager:
    """Tests for the 'with' statement support."""

    def test_context_manager_auto_aborts(self, fresh_base_dir: Path) -> None:
        """Context manager aborts on normal exit without commit."""
        with JanusContext(fresh_base_dir) as ctx:
            ctx.file_editor.invoke(
                {"command": "create", "path": "ctx_test.txt", "file_text": "hello"}
            )
        # Should be aborted
        assert not (fresh_base_dir / "ctx_test.txt").exists()

    def test_context_manager_commit(self, fresh_base_dir: Path) -> None:
        """Explicit commit within context manager persists changes."""
        with JanusContext(fresh_base_dir) as ctx:
            ctx.file_editor.invoke(
                {"command": "create", "path": "ctx_commit.txt", "file_text": "persisted"}
            )
            ctx.commit()
        assert (fresh_base_dir / "ctx_commit.txt").exists()

    def test_context_manager_on_exception(self, fresh_base_dir: Path) -> None:
        """Context manager aborts on exception."""
        with pytest.raises(ValueError):
            with JanusContext(fresh_base_dir) as ctx:
                ctx.file_editor.invoke(
                    {"command": "create", "path": "exc_test.txt", "file_text": "oops"}
                )
                raise ValueError("test error")
        assert not (fresh_base_dir / "exc_test.txt").exists()


# ── Commit + base verification tests ────────────────────────────────


class TestCommitVerification:
    """Tests verifying commit correctly merges to base."""

    def test_commit_new_file(self, fresh_base_dir: Path) -> None:
        """New files appear in base after commit."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()
        ctx.file_editor.invoke(
            {"command": "create", "path": "new.py", "file_text": "x = 1\n"}
        )
        ctx.commit()
        assert (fresh_base_dir / "new.py").read_text() == "x = 1\n"

    def test_commit_modified_file(self, fresh_base_dir: Path) -> None:
        """Modified files are updated in base after commit."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()
        ctx.file_editor.invoke(
            {
                "command": "str_replace",
                "path": "original.txt",
                "old_str": "original content",
                "new_str": "modified content",
            }
        )
        ctx.commit()
        assert "modified content" in (fresh_base_dir / "original.txt").read_text()

    def test_commit_new_directory(self, fresh_base_dir: Path) -> None:
        """New directories appear in base after commit."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()
        ctx.file_editor.invoke(
            {"command": "create", "path": "pkg/mod.py", "file_text": "y = 2\n"}
        )
        ctx.commit()
        assert (fresh_base_dir / "pkg" / "mod.py").exists()

    def test_commit_memory_files(self, fresh_base_dir: Path) -> None:
        """Memory files persist in base after commit."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()
        ctx.memory.invoke(
            {"command": "create", "path": "notes.md", "file_text": "# Notes\n"}
        )
        ctx.commit()
        assert (fresh_base_dir / "memories" / "notes.md").exists()

    def test_commit_bash_changes(self, fresh_base_dir: Path) -> None:
        """Files created by bash persist after commit."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()
        ctx.bash.invoke({"command": "echo 'bash created' > bash_file.txt"})
        ctx.commit()
        assert (fresh_base_dir / "bash_file.txt").exists()
        assert "bash created" in (fresh_base_dir / "bash_file.txt").read_text()


# ── Savepoint tests ─────────────────────────────────────────────────


class TestSavepoints:
    """Tests for savepoint / rollback via child subtransactions."""

    def test_savepoint_and_rollback(self, fresh_base_dir: Path) -> None:
        """Create savepoint, make changes, rollback discards them."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()

        # Make initial change (parent overlay)
        ctx.file_editor.invoke(
            {"command": "create", "path": "before_sp.txt", "file_text": "before"}
        )

        # Savepoint — tools switch to child overlay
        ctx.savepoint("sp1")

        # Make more changes (child overlay)
        ctx.file_editor.invoke(
            {"command": "create", "path": "after_sp.txt", "file_text": "after"}
        )

        # Rollback — discards child overlay, tools back to parent
        ctx.rollback("sp1")

        # Commit — only before_sp.txt should persist
        ctx.commit()
        assert (fresh_base_dir / "before_sp.txt").exists()
        assert not (fresh_base_dir / "after_sp.txt").exists()

    def test_rollback_without_savepoint_raises(self, fresh_base_dir: Path) -> None:
        """Rollback without an active savepoint raises."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()
        with pytest.raises(RuntimeError, match="No active savepoint"):
            ctx.rollback()
        ctx.abort()


# ── get_changes tests ────────────────────────────────────────────────


class TestGetChanges:
    """Tests for get_changes()."""

    def test_no_changes_initially(self, fresh_base_dir: Path) -> None:
        """No changes right after begin."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()
        changes = ctx.get_changes()
        assert len(changes) == 0
        ctx.abort()

    def test_changes_after_create(self, fresh_base_dir: Path) -> None:
        """Creating a file shows up in changes."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()
        ctx.file_editor.invoke(
            {"command": "create", "path": "tracked.txt", "file_text": "content"}
        )
        changes = ctx.get_changes()
        assert len(changes) >= 1
        paths = [c.resource_id for c in changes]
        assert any("tracked.txt" in p for p in paths)
        ctx.abort()


# ── get_tools tests ──────────────────────────────────────────────────


class TestGetTools:
    """Tests for get_tools()."""

    def test_get_tools_returns_five(self, fresh_base_dir: Path) -> None:
        """get_tools returns file_editor, memory, bash, sqlite, vectorstore."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()
        tools = ctx.get_tools()
        assert len(tools) == 5
        names = {t.name for t in tools}
        assert "janus_file_editor" in names
        assert "janus_memory" in names
        assert "janus_bash" in names
        assert "janus_sqlite" in names
        assert "janus_vectorstore" in names
        ctx.abort()

    def test_get_tools_before_begin_raises(self, fresh_base_dir: Path) -> None:
        """get_tools before begin raises."""
        ctx = JanusContext(fresh_base_dir)
        with pytest.raises(RuntimeError, match="not begun"):
            ctx.get_tools()


# ── JanusTransactionControl tests ─────────────────────────────────────


class TestJanusTransactionControl:
    """Tests for the transaction control tool."""

    def test_begin_action(self, fresh_base_dir: Path) -> None:
        """'begin' action starts a transaction."""
        ctx = JanusContext(fresh_base_dir)
        ctl = JanusTransactionControl(janus_context=ctx)
        result = ctl.invoke({"action": "begin"})
        assert "started" in result.lower() or "begun" in result.lower()
        assert ctx.is_active
        ctx.abort()

    def test_commit_action(self, fresh_base_dir: Path) -> None:
        """'commit' action commits the transaction."""
        ctx = JanusContext(fresh_base_dir)
        ctl = JanusTransactionControl(janus_context=ctx)
        ctl.invoke({"action": "begin"})

        # Make a change through the file editor
        ctx.file_editor.invoke(
            {"command": "create", "path": "ctl_commit.txt", "file_text": "via control"}
        )
        result = ctl.invoke({"action": "commit"})
        assert "committed" in result.lower()
        assert (fresh_base_dir / "ctl_commit.txt").exists()

    def test_abort_action(self, fresh_base_dir: Path) -> None:
        """'abort' action aborts the transaction."""
        ctx = JanusContext(fresh_base_dir)
        ctl = JanusTransactionControl(janus_context=ctx)
        ctl.invoke({"action": "begin"})
        ctx.file_editor.invoke(
            {"command": "create", "path": "ctl_abort.txt", "file_text": "nope"}
        )
        result = ctl.invoke({"action": "abort"})
        assert "aborted" in result.lower()
        assert not (fresh_base_dir / "ctl_abort.txt").exists()

    def test_status_action(self, fresh_base_dir: Path) -> None:
        """'status' action returns transaction info."""
        ctx = JanusContext(fresh_base_dir)
        ctl = JanusTransactionControl(janus_context=ctx)
        ctl.invoke({"action": "begin"})
        result = ctl.invoke({"action": "status"})
        assert "active" in result.lower()
        ctx.abort()

    def test_status_no_txn(self, fresh_base_dir: Path) -> None:
        """'status' with no active transaction."""
        ctx = JanusContext(fresh_base_dir)
        ctl = JanusTransactionControl(janus_context=ctx)
        result = ctl.invoke({"action": "status"})
        assert "no active" in result.lower()

    def test_changes_action(self, fresh_base_dir: Path) -> None:
        """'changes' action lists current changes."""
        ctx = JanusContext(fresh_base_dir)
        ctl = JanusTransactionControl(janus_context=ctx)
        ctl.invoke({"action": "begin"})

        result = ctl.invoke({"action": "changes"})
        assert "no changes" in result.lower()

        ctx.file_editor.invoke(
            {"command": "create", "path": "change_test.txt", "file_text": "x"}
        )
        result = ctl.invoke({"action": "changes"})
        assert "change_test.txt" in result
        ctx.abort()

    def test_savepoint_action(self, fresh_base_dir: Path) -> None:
        """'savepoint' action creates a savepoint."""
        ctx = JanusContext(fresh_base_dir)
        ctl = JanusTransactionControl(janus_context=ctx)
        ctl.invoke({"action": "begin"})
        result = ctl.invoke({"action": "savepoint", "name": "sp1"})
        assert "savepoint" in result.lower()
        ctx.abort()

    def test_savepoint_missing_name(self, fresh_base_dir: Path) -> None:
        """'savepoint' without name returns error."""
        ctx = JanusContext(fresh_base_dir)
        ctl = JanusTransactionControl(janus_context=ctx)
        ctl.invoke({"action": "begin"})
        result = ctl.invoke({"action": "savepoint"})
        assert "error" in result.lower()
        ctx.abort()

    def test_rollback_action(self, fresh_base_dir: Path) -> None:
        """'rollback' action rolls back to savepoint."""
        ctx = JanusContext(fresh_base_dir)
        ctl = JanusTransactionControl(janus_context=ctx)
        ctl.invoke({"action": "begin"})
        ctl.invoke({"action": "savepoint", "name": "sp1"})
        result = ctl.invoke({"action": "rollback"})
        assert "rolled back" in result.lower()
        ctx.abort()

    def test_unknown_action(self, fresh_base_dir: Path) -> None:
        """Unknown action returns error."""
        ctx = JanusContext(fresh_base_dir)
        ctl = JanusTransactionControl(janus_context=ctx)
        result = ctl.invoke({"action": "unknown_xyz"})
        assert "error" in result.lower()


# ── create_janus_tools convenience function tests ─────────────────────


class TestCreateJanusTools:
    """Tests for create_janus_tools and create_janus_tools_with_active_txn."""

    def test_create_janus_tools(self, fresh_base_dir: Path) -> None:
        """create_janus_tools returns context and control tool."""
        ctx, tools = create_janus_tools(fresh_base_dir)
        assert not ctx.is_active  # Not begun yet
        assert len(tools) == 1  # Only txn_control
        assert tools[0].name == "janus_txn"

    def test_create_janus_tools_with_active_txn(self, fresh_base_dir: Path) -> None:
        """create_janus_tools_with_active_txn returns everything ready."""
        ctx, tools = create_janus_tools_with_active_txn(fresh_base_dir)
        try:
            assert ctx.is_active
            assert len(tools) == 6  # editor, memory, bash, sqlite, vectorstore, txn
            names = {t.name for t in tools}
            assert "janus_file_editor" in names
            assert "janus_memory" in names
            assert "janus_bash" in names
            assert "janus_sqlite" in names
            assert "janus_vectorstore" in names
            assert "janus_txn" in names
        finally:
            ctx.abort()


# ── Integration: cross-tool tests ────────────────────────────────────


class TestCrossToolIntegration:
    """Integration tests using multiple tools together."""

    def test_editor_then_bash(self, fresh_base_dir: Path) -> None:
        """File created by editor is runnable by bash."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()

        ctx.file_editor.invoke(
            {
                "command": "create",
                "path": "run_me.py",
                "file_text": "print('from editor')",
            }
        )
        result = ctx.bash.invoke({"command": "python3 run_me.py"})
        assert "from editor" in result
        ctx.abort()

    def test_bash_then_editor(self, fresh_base_dir: Path) -> None:
        """File created by bash is viewable by editor."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()

        ctx.bash.invoke({"command": "echo 'bash made this' > bash_file.txt"})
        result = ctx.file_editor.invoke(
            {"command": "view", "path": "bash_file.txt"}
        )
        assert "bash made this" in result
        ctx.abort()

    def test_memory_survives_bash(self, fresh_base_dir: Path) -> None:
        """Memory files are not affected by bash commands in other dirs."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()

        ctx.memory.invoke(
            {"command": "create", "path": "memo.md", "file_text": "important note"}
        )
        ctx.bash.invoke({"command": "rm -rf src/"})

        # Memory should still be accessible
        result = ctx.memory.invoke({"command": "view", "path": "memo.md"})
        assert "important note" in result
        ctx.abort()

    def test_full_workflow_commit(self, fresh_base_dir: Path) -> None:
        """Full workflow: begin → edit → test → commit."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()

        # Create a module file
        ctx.file_editor.invoke(
            {
                "command": "create",
                "path": "lib.py",
                "file_text": "def greet(name):\n    return f'Hello, {name}!'\n",
            }
        )

        # Create a test script (no pytest dependency — use plain assert)
        ctx.file_editor.invoke(
            {
                "command": "create",
                "path": "test_lib.py",
                "file_text": (
                    "from lib import greet\n\n"
                    "assert greet('World') == 'Hello, World!'\n"
                    "print('ALL TESTS PASSED')\n"
                ),
            }
        )

        # Run the test
        result = ctx.bash.invoke({"command": "python3 test_lib.py 2>&1"})
        assert "ALL TESTS PASSED" in result

        # Record progress
        ctx.memory.invoke(
            {
                "command": "create",
                "path": "progress.md",
                "file_text": "# Progress\n- [x] Created lib.py\n- [x] Tests pass\n",
            }
        )

        # Commit
        ctx.commit()

        # Verify everything persisted
        assert (fresh_base_dir / "lib.py").exists()
        assert (fresh_base_dir / "test_lib.py").exists()
        assert (fresh_base_dir / "memories" / "progress.md").exists()

    def test_full_workflow_abort(self, fresh_base_dir: Path) -> None:
        """Full workflow: begin → edit → abort → nothing persists."""
        ctx = JanusContext(fresh_base_dir)
        ctx.begin()

        ctx.file_editor.invoke(
            {"command": "create", "path": "doomed.py", "file_text": "x = 1\n"}
        )
        ctx.memory.invoke(
            {"command": "create", "path": "doomed.md", "file_text": "nope\n"}
        )
        ctx.bash.invoke({"command": "echo 'doomed' > doomed.txt"})

        ctx.abort()

        assert not (fresh_base_dir / "doomed.py").exists()
        assert not (fresh_base_dir / "memories" / "doomed.md").exists()
        assert not (fresh_base_dir / "doomed.txt").exists()
