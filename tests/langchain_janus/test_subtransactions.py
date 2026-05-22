"""Subtransaction and branching tests for Janus.

Verifies that savepoints (implemented as child subtransactions) provide
correct isolation across all data systems:

  - FS isolation: child overlay files discarded on rollback.
  - DB isolation: child MVCC rows cleaned up on rollback.
  - Combined isolation: both systems rolled back consistently.
  - Sequential savepoints: second savepoint auto-commits the first.
  - Commit/abort with active child: auto-aborts child first.
  - Memory isolation: memory files scoped to child overlay.
  - Bash in child overlay: commands run in child working directory.
  - Complex exploratory patterns: try-fail-rollback-retry workflows.

Run with: sudo pytest tests/test_subtransactions.py -v
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Generator

import pytest

from langchain_janus.context import (
    JanusContext,
    JanusTransactionControl,
)


def _require_root() -> None:
    if os.geteuid() != 0:
        pytest.skip("Requires root for OverlayFS")


@pytest.fixture
def fresh_dir() -> Generator[Path, None, None]:
    _require_root()
    d = Path(tempfile.mkdtemp(prefix="tar_subtxn_"))
    (d / "README.md").write_text("# Subtxn Test\n")
    (d / "src").mkdir()
    (d / "src" / "app.py").write_text("APP = True\n")
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ── Basic savepoint mechanics ────────────────────────────────────────


class TestSavepointBasics:
    """Basic savepoint create / rollback lifecycle."""

    def test_savepoint_creates_child(self, fresh_dir: Path) -> None:
        """savepoint() records a child subtransaction."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.savepoint("sp1")
        assert ctx._child_txn is not None
        assert ctx._child_txn.is_active
        ctx.abort()

    def test_rollback_clears_child(self, fresh_dir: Path) -> None:
        """rollback() clears the child reference."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.savepoint("sp1")
        ctx.rollback("sp1")
        assert ctx._child_txn is None
        ctx.abort()

    def test_savepoint_without_txn_raises(self, fresh_dir: Path) -> None:
        """savepoint before begin raises."""
        ctx = JanusContext(fresh_dir)
        with pytest.raises(RuntimeError, match="No active transaction"):
            ctx.savepoint("sp")

    def test_rollback_without_savepoint_raises(
        self, fresh_dir: Path
    ) -> None:
        """rollback without prior savepoint raises."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        with pytest.raises(RuntimeError, match="No active savepoint"):
            ctx.rollback()
        ctx.abort()

    def test_multiple_rollback_calls_fail(self, fresh_dir: Path) -> None:
        """Rolling back twice without a new savepoint raises."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.savepoint("sp1")
        ctx.rollback("sp1")
        with pytest.raises(RuntimeError, match="No active savepoint"):
            ctx.rollback("sp1")
        ctx.abort()


# ── FS isolation with savepoints ─────────────────────────────────────


class TestSavepointFSIsolation:
    """File operations scoped to the child overlay."""

    def test_rollback_discards_fs_creates(self, fresh_dir: Path) -> None:
        """Files created after savepoint are gone after rollback."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()

        ctx.file_editor.invoke({
            "command": "create",
            "path": "kept.txt",
            "file_text": "kept",
        })
        ctx.savepoint("sp")
        ctx.file_editor.invoke({
            "command": "create",
            "path": "discarded.txt",
            "file_text": "discarded",
        })

        # Visible during child
        v = ctx.file_editor.invoke({
            "command": "view",
            "path": "discarded.txt",
        })
        assert "discarded" in v

        ctx.rollback("sp")

        # Gone after rollback — not in parent overlay
        v2 = ctx.file_editor.invoke({
            "command": "view",
            "path": "discarded.txt",
        })
        assert "not found" in v2.lower() or "Error" in v2

        ctx.commit()
        assert (fresh_dir / "kept.txt").exists()
        assert not (fresh_dir / "discarded.txt").exists()

    def test_rollback_discards_fs_modifications(
        self, fresh_dir: Path
    ) -> None:
        """str_replace during child overlay is reverted on rollback."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()

        # Modify a pre-existing file in parent overlay
        ctx.file_editor.invoke({
            "command": "str_replace",
            "path": "src/app.py",
            "old_str": "APP = True",
            "new_str": "APP = False",
        })

        ctx.savepoint("sp")

        # Further modify in child overlay
        ctx.file_editor.invoke({
            "command": "str_replace",
            "path": "src/app.py",
            "old_str": "APP = False",
            "new_str": "APP = None",
        })

        ctx.rollback("sp")

        # Back to parent overlay state (APP = False)
        v = ctx.file_editor.invoke({
            "command": "view",
            "path": "src/app.py",
        })
        assert "APP = False" in v

        ctx.commit()
        content = (fresh_dir / "src" / "app.py").read_text()
        assert "APP = False" in content

    def test_rollback_preserves_pre_savepoint_fs(
        self, fresh_dir: Path
    ) -> None:
        """Files created before savepoint survive rollback."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()

        ctx.file_editor.invoke({
            "command": "create",
            "path": "before.txt",
            "file_text": "before savepoint",
        })

        ctx.savepoint("sp")
        ctx.file_editor.invoke({
            "command": "create",
            "path": "after.txt",
            "file_text": "after savepoint",
        })
        ctx.rollback("sp")

        # Parent overlay file still accessible
        v = ctx.file_editor.invoke({
            "command": "view",
            "path": "before.txt",
        })
        assert "before savepoint" in v

        ctx.commit()
        assert (fresh_dir / "before.txt").exists()

    def test_rollback_discards_fs_deletes(self, fresh_dir: Path) -> None:
        """File deletion during child is undone on rollback."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()

        ctx.savepoint("sp")
        ctx.file_editor.invoke({
            "command": "delete",
            "path": "README.md",
        })

        # In child overlay, file is deleted
        v = ctx.file_editor.invoke({
            "command": "view",
            "path": "README.md",
        })
        assert "not found" in v.lower() or "Error" in v

        ctx.rollback("sp")

        # After rollback, file is back (parent overlay has the original)
        v2 = ctx.file_editor.invoke({
            "command": "view",
            "path": "README.md",
        })
        assert "Subtxn Test" in v2
        ctx.abort()


# ── DB isolation with savepoints ─────────────────────────────────────


class TestSavepointDBIsolation:
    """SQLite MVCC writes scoped to the child transaction."""

    def test_rollback_discards_db_inserts(self, fresh_dir: Path) -> None:
        """Rows inserted during child txn are gone after rollback."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "items",
            "columns": ["id TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "items",
            "row": {"id": "1", "val": "parent"},
        })

        ctx.savepoint("sp")

        ctx.sqlite.invoke({
            "command": "put",
            "table": "items",
            "row": {"id": "2", "val": "child"},
        })

        # Visible during child
        r = ctx.sqlite.invoke({
            "command": "get",
            "table": "items",
            "pk_value": "2",
        })
        assert "child" in r

        ctx.rollback("sp")

        # Gone after rollback
        r2 = ctx.sqlite.invoke({
            "command": "get",
            "table": "items",
            "pk_value": "2",
        })
        assert "No row found" in r2

        # Parent row still there
        r3 = ctx.sqlite.invoke({
            "command": "get",
            "table": "items",
            "pk_value": "1",
        })
        assert "parent" in r3
        ctx.abort()

    def test_rollback_discards_db_updates(self, fresh_dir: Path) -> None:
        """Row updated in child reverts to parent version on rollback."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "data",
            "columns": ["id TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "data",
            "row": {"id": "1", "val": "original"},
        })

        ctx.savepoint("sp")

        ctx.sqlite.invoke({
            "command": "put",
            "table": "data",
            "row": {"id": "1", "val": "modified"},
        })

        # During child, see modified
        r = ctx.sqlite.invoke({
            "command": "get",
            "table": "data",
            "pk_value": "1",
        })
        assert json.loads(r)["val"] == "modified"

        ctx.rollback("sp")

        # After rollback, back to original
        r2 = ctx.sqlite.invoke({
            "command": "get",
            "table": "data",
            "pk_value": "1",
        })
        assert json.loads(r2)["val"] == "original"
        ctx.abort()

    def test_rollback_discards_db_deletes(self, fresh_dir: Path) -> None:
        """Row deleted in child is restored on rollback."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "items",
            "columns": ["id TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "items",
            "row": {"id": "1", "val": "alive"},
        })

        ctx.savepoint("sp")

        ctx.sqlite.invoke({
            "command": "delete",
            "table": "items",
            "pk_value": "1",
        })

        # During child, deleted
        r = ctx.sqlite.invoke({
            "command": "get",
            "table": "items",
            "pk_value": "1",
        })
        assert "No row found" in r

        ctx.rollback("sp")

        # After rollback, restored
        r2 = ctx.sqlite.invoke({
            "command": "get",
            "table": "items",
            "pk_value": "1",
        })
        assert "alive" in r2
        ctx.abort()

    def test_commit_after_rollback_persists_pre_sp(
        self, fresh_dir: Path
    ) -> None:
        """Pre-savepoint DB data persists after rollback + commit."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "data",
            "columns": ["id TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "data",
            "row": {"id": "1", "val": "safe"},
        })

        ctx.savepoint("sp")
        ctx.sqlite.invoke({
            "command": "put",
            "table": "data",
            "row": {"id": "2", "val": "doomed"},
        })
        ctx.rollback("sp")

        ctx.commit()

        # Pre-sp row persists
        ctx.begin()
        r1 = ctx.sqlite.invoke({
            "command": "get",
            "table": "data",
            "pk_value": "1",
        })
        assert "safe" in r1

        r2 = ctx.sqlite.invoke({
            "command": "get",
            "table": "data",
            "pk_value": "2",
        })
        assert "No row found" in r2
        ctx.abort()


# ── Combined FS + DB savepoint ───────────────────────────────────────


class TestSavepointBothSystems:
    """Both FS and DB are rolled back together by savepoint rollback."""

    def test_rollback_discards_fs_and_db(self, fresh_dir: Path) -> None:
        """Both file and DB row created after savepoint disappear."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        # Pre-savepoint work
        ctx.file_editor.invoke({
            "command": "create",
            "path": "kept.txt",
            "file_text": "kept",
        })
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "t",
            "columns": ["id TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "t",
            "row": {"id": "1", "val": "safe"},
        })

        ctx.savepoint("sp")

        # Post-savepoint work
        ctx.file_editor.invoke({
            "command": "create",
            "path": "gone.txt",
            "file_text": "gone",
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "t",
            "row": {"id": "2", "val": "gone"},
        })

        ctx.rollback("sp")

        ctx.commit()

        assert (fresh_dir / "kept.txt").exists()
        assert not (fresh_dir / "gone.txt").exists()

        ctx.begin()
        r1 = ctx.sqlite.invoke({
            "command": "get",
            "table": "t",
            "pk_value": "1",
        })
        assert "safe" in r1
        r2 = ctx.sqlite.invoke({
            "command": "get",
            "table": "t",
            "pk_value": "2",
        })
        assert "No row found" in r2
        ctx.abort()

    def test_commit_after_sp_rollback_persists_both(
        self, fresh_dir: Path
    ) -> None:
        """Pre-savepoint changes in both systems persist after rollback+commit."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        ctx.file_editor.invoke({
            "command": "create",
            "path": "config.py",
            "file_text": "DEBUG = True\n",
        })
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "meta",
            "columns": ["key TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "meta",
            "row": {"key": "version", "val": "1"},
        })

        ctx.savepoint("sp")
        # Speculative work
        ctx.file_editor.invoke({
            "command": "create",
            "path": "extra.py",
            "file_text": "nope\n",
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "meta",
            "row": {"key": "extra", "val": "nope"},
        })
        ctx.rollback("sp")

        # Add more work after rollback (on parent overlay again)
        ctx.file_editor.invoke({
            "command": "create",
            "path": "final.py",
            "file_text": "final\n",
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "meta",
            "row": {"key": "final", "val": "yes"},
        })

        ctx.commit()

        assert (fresh_dir / "config.py").exists()
        assert (fresh_dir / "final.py").exists()
        assert not (fresh_dir / "extra.py").exists()

        ctx.begin()
        r = ctx.sqlite.invoke({
            "command": "query",
            "table": "meta",
        })
        rows = json.loads(r)
        keys = {row["key"] for row in rows}
        assert "version" in keys
        assert "final" in keys
        assert "extra" not in keys
        ctx.abort()


# ── Sequential savepoints ───────────────────────────────────────────


class TestSequentialSavepoints:
    """Creating a second savepoint auto-commits the first child."""

    def test_sequential_sp_commits_previous(
        self, fresh_dir: Path
    ) -> None:
        """sp2 auto-commits sp1's child; rollback(sp2) keeps sp1's work."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "steps",
            "columns": ["id TEXT", "val TEXT"],
        })

        # sp1: write data
        ctx.savepoint("sp1")
        ctx.file_editor.invoke({
            "command": "create",
            "path": "sp1.txt",
            "file_text": "from sp1",
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "steps",
            "row": {"id": "1", "val": "sp1"},
        })

        # sp2: auto-commits sp1's child, creates new child
        ctx.savepoint("sp2")
        ctx.file_editor.invoke({
            "command": "create",
            "path": "sp2.txt",
            "file_text": "from sp2",
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "steps",
            "row": {"id": "2", "val": "sp2"},
        })

        # Rollback sp2 — discards sp2's work, keeps sp1's (committed)
        ctx.rollback("sp2")

        ctx.commit()

        # sp1's work persists (was committed into parent)
        assert (fresh_dir / "sp1.txt").exists()
        # sp2's work rolled back
        assert not (fresh_dir / "sp2.txt").exists()

        ctx.begin()
        r1 = ctx.sqlite.invoke({
            "command": "get",
            "table": "steps",
            "pk_value": "1",
        })
        r2 = ctx.sqlite.invoke({
            "command": "get",
            "table": "steps",
            "pk_value": "2",
        })
        assert "sp1" in r1
        assert "No row found" in r2
        ctx.abort()

    def test_sequential_sp_both_committed(self, fresh_dir: Path) -> None:
        """Two savepoints followed by commit preserves all work."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "x",
            "columns": ["id TEXT"],
        })

        ctx.savepoint("sp1")
        ctx.file_editor.invoke({
            "command": "create",
            "path": "a.txt",
            "file_text": "a",
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "x",
            "row": {"id": "1"},
        })

        ctx.savepoint("sp2")  # auto-commits sp1
        ctx.file_editor.invoke({
            "command": "create",
            "path": "b.txt",
            "file_text": "b",
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "x",
            "row": {"id": "2"},
        })

        # Commit without rollback — both sets of work persist
        ctx.commit()

        assert (fresh_dir / "a.txt").exists()
        assert (fresh_dir / "b.txt").exists()

        ctx.begin()
        r1 = ctx.sqlite.invoke({
            "command": "get",
            "table": "x",
            "pk_value": "1",
        })
        r2 = ctx.sqlite.invoke({
            "command": "get",
            "table": "x",
            "pk_value": "2",
        })
        assert "No row found" not in r1
        assert "No row found" not in r2
        ctx.abort()


# ── Memory isolation with savepoints ─────────────────────────────────


class TestSavepointMemoryIsolation:
    """Memory files scoped to child overlay."""

    def test_memory_rollback_discards(self, fresh_dir: Path) -> None:
        """Memory file created in child is discarded on rollback."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()

        ctx.memory.invoke({
            "command": "create",
            "path": "kept.md",
            "file_text": "# Kept\n",
        })

        ctx.savepoint("sp")
        ctx.memory.invoke({
            "command": "create",
            "path": "discarded.md",
            "file_text": "# Discarded\n",
        })
        ctx.rollback("sp")

        mem_list = ctx.memory.invoke({"command": "list"})
        assert "kept.md" in mem_list
        assert "discarded.md" not in mem_list
        ctx.abort()

    def test_memory_survives_rollback_then_commit(
        self, fresh_dir: Path
    ) -> None:
        """Pre-savepoint memory persists after rollback + commit."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()

        ctx.memory.invoke({
            "command": "create",
            "path": "progress.md",
            "file_text": "# Step 1\n",
        })

        ctx.savepoint("sp")
        ctx.memory.invoke({
            "command": "create",
            "path": "temp.md",
            "file_text": "# Temp\n",
        })
        ctx.rollback("sp")

        ctx.commit()
        assert (fresh_dir / "memories" / "progress.md").exists()
        assert not (fresh_dir / "memories" / "temp.md").exists()


# ── Bash in child overlay ────────────────────────────────────────────


class TestSavepointBash:
    """Bash commands run in the correct overlay."""

    def test_bash_sees_child_files(self, fresh_dir: Path) -> None:
        """Bash can see files created in child overlay."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()

        ctx.savepoint("sp")
        ctx.file_editor.invoke({
            "command": "create",
            "path": "child.txt",
            "file_text": "child_content\n",
        })
        output = ctx.bash.invoke({"command": "cat child.txt"})
        assert "child_content" in output
        ctx.rollback("sp")

        # After rollback, file is gone
        output2 = ctx.bash.invoke({"command": "cat child.txt 2>&1"})
        assert "No such file" in output2 or "Error" in output2
        ctx.abort()

    def test_bash_sees_parent_files_in_child(
        self, fresh_dir: Path
    ) -> None:
        """Bash in child overlay can see parent's files."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()

        ctx.file_editor.invoke({
            "command": "create",
            "path": "parent.txt",
            "file_text": "from parent\n",
        })

        ctx.savepoint("sp")
        output = ctx.bash.invoke({"command": "cat parent.txt"})
        assert "from parent" in output
        ctx.rollback("sp")
        ctx.abort()


# ── Commit/abort with active child ──────────────────────────────────


class TestCommitAbortWithActiveChild:
    """commit/abort auto-aborts active child subtransaction."""

    def test_commit_includes_active_child_fs(
        self, fresh_dir: Path
    ) -> None:
        """Commit with active savepoint auto-commits child overlay's work."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        ctx.file_editor.invoke({
            "command": "create",
            "path": "parent.txt",
            "file_text": "parent",
        })
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "t",
            "columns": ["id TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "t",
            "row": {"id": "1"},
        })

        ctx.savepoint("sp")
        ctx.file_editor.invoke({
            "command": "create",
            "path": "child.txt",
            "file_text": "child",
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "t",
            "row": {"id": "2"},
        })

        # Commit without rollback — auto-commits child first
        ctx.commit()

        # Both parent and child work persist
        assert (fresh_dir / "parent.txt").exists()
        assert (fresh_dir / "child.txt").exists()

        ctx.begin()
        r1 = ctx.sqlite.invoke({
            "command": "get",
            "table": "t",
            "pk_value": "1",
        })
        r2 = ctx.sqlite.invoke({
            "command": "get",
            "table": "t",
            "pk_value": "2",
        })
        assert "No row found" not in r1
        assert "No row found" not in r2
        ctx.abort()

    def test_abort_with_active_child(self, fresh_dir: Path) -> None:
        """Abort with active savepoint discards everything."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "t",
            "columns": ["id TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "t",
            "row": {"id": "1"},
        })
        ctx.file_editor.invoke({
            "command": "create",
            "path": "parent.txt",
            "file_text": "p",
        })

        ctx.savepoint("sp")
        ctx.file_editor.invoke({
            "command": "create",
            "path": "child.txt",
            "file_text": "c",
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "t",
            "row": {"id": "2"},
        })

        # Abort everything
        ctx.abort()

        assert not (fresh_dir / "parent.txt").exists()
        assert not (fresh_dir / "child.txt").exists()

        ctx.begin()
        r = ctx.sqlite.invoke({
            "command": "get",
            "table": "t",
            "pk_value": "1",
        })
        assert "No row found" in r
        ctx.abort()


# ── JanusTransactionControl savepoints ─────────────────────────────────


class TestTxnCtlSavepoints:
    """Savepoint/rollback via the JanusTransactionControl tool."""

    def test_ctl_savepoint_and_rollback(self, fresh_dir: Path) -> None:
        """JanusTransactionControl savepoint/rollback works end-to-end."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctl = JanusTransactionControl(janus_context=ctx)

        ctl.invoke({"action": "begin"})

        ctx.file_editor.invoke({
            "command": "create",
            "path": "before.txt",
            "file_text": "b",
        })
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "t",
            "columns": ["id TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "t",
            "row": {"id": "1"},
        })

        ctl.invoke({"action": "savepoint", "name": "sp"})

        ctx.file_editor.invoke({
            "command": "create",
            "path": "after.txt",
            "file_text": "a",
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "t",
            "row": {"id": "2"},
        })

        ctl.invoke({"action": "rollback", "name": "sp"})
        ctl.invoke({"action": "commit"})

        assert (fresh_dir / "before.txt").exists()
        assert not (fresh_dir / "after.txt").exists()

        ctx.begin()
        r1 = ctx.sqlite.invoke({
            "command": "get",
            "table": "t",
            "pk_value": "1",
        })
        r2 = ctx.sqlite.invoke({
            "command": "get",
            "table": "t",
            "pk_value": "2",
        })
        assert "No row found" not in r1
        assert "No row found" in r2
        ctx.abort()

    def test_ctl_savepoint_name_required(self, fresh_dir: Path) -> None:
        """Savepoint without name returns error."""
        ctx = JanusContext(fresh_dir)
        ctl = JanusTransactionControl(janus_context=ctx)
        ctl.invoke({"action": "begin"})
        result = ctl.invoke({"action": "savepoint"})
        assert "Error" in result
        ctl.invoke({"action": "abort"})


# ── Complex exploratory patterns ─────────────────────────────────────


class TestExploratoryPatterns:
    """End-to-end patterns an agent might use with savepoints."""

    def test_try_fail_rollback_succeed(self, fresh_dir: Path) -> None:
        """Agent tries approach A (fails), rollback, tries B (works)."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "approaches",
            "columns": ["id TEXT", "status TEXT"],
        })

        # Attempt A
        ctx.savepoint("attempt_a")
        ctx.file_editor.invoke({
            "command": "create",
            "path": "solution.py",
            "file_text": "# Bad solution\nraise NotImplementedError\n",
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "approaches",
            "row": {"id": "A", "status": "failed"},
        })

        # Simulate test failure → rollback
        ctx.rollback("attempt_a")

        # Attempt B
        ctx.file_editor.invoke({
            "command": "create",
            "path": "solution.py",
            "file_text": "def solve():\n    return 42\n",
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "approaches",
            "row": {"id": "B", "status": "success"},
        })

        # Test passes → commit
        output = ctx.bash.invoke({
            "command": "python3 -c 'from solution import solve; assert solve() == 42; print(\"OK\")'",
        })
        assert "OK" in output
        ctx.commit()

        # Verify
        content = (fresh_dir / "solution.py").read_text()
        assert "return 42" in content

        ctx.begin()
        ra = ctx.sqlite.invoke({
            "command": "get",
            "table": "approaches",
            "pk_value": "A",
        })
        rb = ctx.sqlite.invoke({
            "command": "get",
            "table": "approaches",
            "pk_value": "B",
        })
        assert "No row found" in ra  # Rolled-back approach
        assert "success" in rb
        ctx.abort()

    def test_multiple_rollback_cycles(self, fresh_dir: Path) -> None:
        """Multiple savepoint-rollback cycles don't corrupt state."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "log",
            "columns": ["id TEXT", "msg TEXT"],
        })

        # Base work
        ctx.sqlite.invoke({
            "command": "put",
            "table": "log",
            "row": {"id": "base", "msg": "base"},
        })

        # Cycle 1: create, rollback
        ctx.savepoint("c1")
        ctx.sqlite.invoke({
            "command": "put",
            "table": "log",
            "row": {"id": "c1", "msg": "cycle1"},
        })
        ctx.file_editor.invoke({
            "command": "create",
            "path": "c1.txt",
            "file_text": "c1",
        })
        ctx.rollback("c1")

        # Cycle 2: create, rollback
        ctx.savepoint("c2")
        ctx.sqlite.invoke({
            "command": "put",
            "table": "log",
            "row": {"id": "c2", "msg": "cycle2"},
        })
        ctx.file_editor.invoke({
            "command": "create",
            "path": "c2.txt",
            "file_text": "c2",
        })
        ctx.rollback("c2")

        # Cycle 3: create, keep
        ctx.file_editor.invoke({
            "command": "create",
            "path": "final.txt",
            "file_text": "final",
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "log",
            "row": {"id": "final", "msg": "final"},
        })

        ctx.commit()

        # Only base + final survived
        assert not (fresh_dir / "c1.txt").exists()
        assert not (fresh_dir / "c2.txt").exists()
        assert (fresh_dir / "final.txt").exists()

        ctx.begin()
        for key, expected in [
            ("base", True),
            ("c1", False),
            ("c2", False),
            ("final", True),
        ]:
            r = ctx.sqlite.invoke({
                "command": "get",
                "table": "log",
                "pk_value": key,
            })
            if expected:
                assert "No row found" not in r, f"{key} should exist"
            else:
                assert "No row found" in r, f"{key} should NOT exist"
        ctx.abort()

    def test_savepoint_rollback_then_work_then_commit(
        self, fresh_dir: Path
    ) -> None:
        """New work after rollback (on parent overlay) persists on commit."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "t",
            "columns": ["id TEXT", "val TEXT"],
        })

        ctx.savepoint("sp")
        ctx.sqlite.invoke({
            "command": "put",
            "table": "t",
            "row": {"id": "1", "val": "speculative"},
        })
        ctx.file_editor.invoke({
            "command": "create",
            "path": "spec.txt",
            "file_text": "speculative",
        })
        ctx.rollback("sp")

        # New work on parent overlay (no savepoint)
        ctx.sqlite.invoke({
            "command": "put",
            "table": "t",
            "row": {"id": "2", "val": "definitive"},
        })
        ctx.file_editor.invoke({
            "command": "create",
            "path": "definitive.txt",
            "file_text": "definitive",
        })
        ctx.commit()

        assert not (fresh_dir / "spec.txt").exists()
        assert (fresh_dir / "definitive.txt").exists()

        ctx.begin()
        r1 = ctx.sqlite.invoke({
            "command": "get",
            "table": "t",
            "pk_value": "1",
        })
        r2 = ctx.sqlite.invoke({
            "command": "get",
            "table": "t",
            "pk_value": "2",
        })
        assert "No row found" in r1
        assert "definitive" in r2
        ctx.abort()
