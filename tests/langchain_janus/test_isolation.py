"""Isolation correctness tests for Janus.

Verifies that transactional isolation holds across all data systems:
  - Read-your-own-writes within a single transaction.
  - Uncommitted changes invisible after abort.
  - Committed changes persist and are visible in subsequent transactions.
  - Snapshot isolation for DB (MVCC visibility predicate).
  - Context manager semantics (auto-abort, exception safety).
  - Error conditions (double begin, tools before begin, etc.).

Run with: sudo pytest tests/test_isolation.py -v
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Generator

import pytest

from langchain_janus.context import JanusContext


def _require_root() -> None:
    if os.geteuid() != 0:
        pytest.skip("Requires root for OverlayFS")


@pytest.fixture
def fresh_dir() -> Generator[Path, None, None]:
    _require_root()
    d = Path(tempfile.mkdtemp(prefix="tar_iso_"))
    (d / "README.md").write_text("# Isolation Test\n")
    (d / "src").mkdir()
    (d / "src" / "main.py").write_text('print("hello")\n')
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ── Read-your-own-writes ─────────────────────────────────────────────


class TestReadYourOwnWrites:
    """Within a single transaction, all writes are immediately readable."""

    def test_fs_read_own_write(self, fresh_dir: Path) -> None:
        """File created in txn is viewable in same txn."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.file_editor.invoke({
            "command": "create",
            "path": "new_file.py",
            "file_text": "x = 42\n",
        })
        result = ctx.file_editor.invoke({
            "command": "view",
            "path": "new_file.py",
        })
        assert "x = 42" in result
        ctx.abort()

    def test_db_read_own_write(self, fresh_dir: Path) -> None:
        """Row inserted in txn is readable in same txn."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "items",
            "columns": ["id TEXT", "name TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "items",
            "row": {"id": "1", "name": "Widget"},
        })
        result = ctx.sqlite.invoke({
            "command": "get",
            "table": "items",
            "pk_value": "1",
        })
        assert "Widget" in result
        ctx.abort()

    def test_db_update_own_write(self, fresh_dir: Path) -> None:
        """Updated row shows latest value within same txn."""
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
            "row": {"id": "1", "val": "v1"},
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "items",
            "row": {"id": "1", "val": "v2"},
        })
        result = ctx.sqlite.invoke({
            "command": "get",
            "table": "items",
            "pk_value": "1",
        })
        data = json.loads(result)
        assert data["val"] == "v2"
        ctx.abort()

    def test_memory_read_own_write(self, fresh_dir: Path) -> None:
        """Memory file created in txn is viewable in same txn."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.memory.invoke({
            "command": "create",
            "path": "notes.md",
            "file_text": "# My Notes\n",
        })
        result = ctx.memory.invoke({
            "command": "view",
            "path": "notes.md",
        })
        assert "My Notes" in result
        ctx.abort()

    def test_fs_str_replace_own_write(self, fresh_dir: Path) -> None:
        """str_replace on a file already created in the same txn."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.file_editor.invoke({
            "command": "create",
            "path": "config.py",
            "file_text": "DEBUG = True\n",
        })
        ctx.file_editor.invoke({
            "command": "str_replace",
            "path": "config.py",
            "old_str": "DEBUG = True",
            "new_str": "DEBUG = False",
        })
        result = ctx.file_editor.invoke({
            "command": "view",
            "path": "config.py",
        })
        assert "DEBUG = False" in result
        ctx.abort()

    def test_fs_view_preexisting_file(self, fresh_dir: Path) -> None:
        """Pre-existing base files are visible within a transaction."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        result = ctx.file_editor.invoke({
            "command": "view",
            "path": "README.md",
        })
        assert "Isolation Test" in result
        ctx.abort()

    def test_bash_sees_own_writes(self, fresh_dir: Path) -> None:
        """Bash commands see files created in the same txn."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.file_editor.invoke({
            "command": "create",
            "path": "data.txt",
            "file_text": "line1\nline2\nline3\n",
        })
        result = ctx.bash.invoke({"command": "wc -l data.txt"})
        assert "3" in result
        ctx.abort()


# ── Abort discards all changes ───────────────────────────────────────


class TestAbortDiscards:
    """Abort must discard all uncommitted changes from every data system."""

    def test_abort_discards_fs_changes(self, fresh_dir: Path) -> None:
        """Created files do NOT appear in base after abort."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.file_editor.invoke({
            "command": "create",
            "path": "ephemeral.txt",
            "file_text": "gone",
        })
        ctx.abort()
        assert not (fresh_dir / "ephemeral.txt").exists()

    def test_abort_discards_db_writes(self, fresh_dir: Path) -> None:
        """Rows inserted are not visible in the next transaction."""
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
            "row": {"id": "1", "val": "aborted"},
        })
        ctx.abort()

        ctx.begin()
        result = ctx.sqlite.invoke({
            "command": "get",
            "table": "data",
            "pk_value": "1",
        })
        assert "No row found" in result
        ctx.abort()

    def test_abort_discards_memory(self, fresh_dir: Path) -> None:
        """Memory files are discarded after abort."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.memory.invoke({
            "command": "create",
            "path": "temp.md",
            "file_text": "temporary",
        })
        ctx.abort()
        assert not (fresh_dir / "memories" / "temp.md").exists()

    def test_abort_multiple_files(self, fresh_dir: Path) -> None:
        """All files in a multi-file transaction are discarded on abort."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        for i in range(5):
            ctx.file_editor.invoke({
                "command": "create",
                "path": f"file_{i}.txt",
                "file_text": f"content {i}",
            })
        ctx.abort()
        for i in range(5):
            assert not (fresh_dir / f"file_{i}.txt").exists()

    def test_abort_multiple_rows(self, fresh_dir: Path) -> None:
        """All DB rows in a multi-row transaction are discarded on abort."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "batch",
            "columns": ["id TEXT", "val TEXT"],
        })
        for i in range(5):
            ctx.sqlite.invoke({
                "command": "put",
                "table": "batch",
                "row": {"id": str(i), "val": f"val_{i}"},
            })
        ctx.abort()

        ctx.begin()
        result = ctx.sqlite.invoke({
            "command": "query",
            "table": "batch",
        })
        assert "No rows found" in result
        ctx.abort()

    def test_abort_str_replace_discards(self, fresh_dir: Path) -> None:
        """Modifications to existing base files are discarded on abort."""
        original = (fresh_dir / "src" / "main.py").read_text()
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.file_editor.invoke({
            "command": "str_replace",
            "path": "src/main.py",
            "old_str": 'print("hello")',
            "new_str": 'print("goodbye")',
        })
        ctx.abort()
        assert (fresh_dir / "src" / "main.py").read_text() == original

    def test_abort_delete_discards(self, fresh_dir: Path) -> None:
        """File deletion in aborted transaction does not affect base."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.file_editor.invoke({
            "command": "delete",
            "path": "README.md",
        })
        ctx.abort()
        assert (fresh_dir / "README.md").exists()


# ── Commit persists all changes ──────────────────────────────────────


class TestCommitPersists:
    """Commit must persist all changes to the base directory and DB."""

    def test_commit_persists_fs(self, fresh_dir: Path) -> None:
        """Created files appear in base after commit."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.file_editor.invoke({
            "command": "create",
            "path": "committed.txt",
            "file_text": "I am committed\n",
        })
        ctx.commit()
        assert (fresh_dir / "committed.txt").read_text() == "I am committed\n"

    def test_commit_persists_db_across_txns(self, fresh_dir: Path) -> None:
        """Committed DB rows are visible in the next transaction."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "users",
            "columns": ["id TEXT", "name TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "users",
            "row": {"id": "1", "name": "Alice"},
        })
        ctx.commit()

        ctx.begin()
        result = ctx.sqlite.invoke({
            "command": "get",
            "table": "users",
            "pk_value": "1",
        })
        assert "Alice" in result
        ctx.abort()

    def test_commit_str_replace_persists(self, fresh_dir: Path) -> None:
        """Modifications to base files persist after commit."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.file_editor.invoke({
            "command": "str_replace",
            "path": "src/main.py",
            "old_str": 'print("hello")',
            "new_str": 'print("goodbye")',
        })
        ctx.commit()
        content = (fresh_dir / "src" / "main.py").read_text()
        assert 'print("goodbye")' in content

    def test_commit_delete_persists(self, fresh_dir: Path) -> None:
        """File deletion committed to base actually removes the file."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.file_editor.invoke({
            "command": "delete",
            "path": "README.md",
        })
        ctx.commit()
        assert not (fresh_dir / "README.md").exists()

    def test_commit_memory_persists(self, fresh_dir: Path) -> None:
        """Memory files persist in base after commit."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.memory.invoke({
            "command": "create",
            "path": "progress.md",
            "file_text": "# Progress\n- step 1 done\n",
        })
        ctx.commit()
        assert (fresh_dir / "memories" / "progress.md").exists()


# ── Snapshot isolation (DB) ──────────────────────────────────────────


class TestSnapshotIsolation:
    """Verify MVCC snapshot isolation for the SQLite shim."""

    def test_committed_visible_to_next_txn(self, fresh_dir: Path) -> None:
        """Data committed in txn1 is visible in txn2."""
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
            "row": {"id": "A", "val": "from_txn1"},
        })
        ctx.commit()

        ctx.begin()
        r = ctx.sqlite.invoke({
            "command": "get",
            "table": "data",
            "pk_value": "A",
        })
        data = json.loads(r)
        assert data["val"] == "from_txn1"
        ctx.abort()

    def test_seed_data_visible(self, fresh_dir: Path) -> None:
        """Seed data is visible to all transactions."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "config",
            "columns": ["key TEXT", "value TEXT"],
            "seed_data": [
                {"key": "env", "value": "test"},
                {"key": "version", "value": "1.0"},
            ],
        })
        r = ctx.sqlite.invoke({
            "command": "get",
            "table": "config",
            "pk_value": "env",
        })
        assert "test" in r
        ctx.abort()

        # Also visible in a new txn
        ctx.begin()
        r2 = ctx.sqlite.invoke({
            "command": "get",
            "table": "config",
            "pk_value": "env",
        })
        assert "test" in r2
        ctx.abort()

    def test_db_delete_abort_restores(self, fresh_dir: Path) -> None:
        """Deleting a committed row then aborting restores the row."""
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
        ctx.commit()

        # Delete and abort
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "delete",
            "table": "items",
            "pk_value": "1",
        })
        ctx.abort()

        # Should still be visible
        ctx.begin()
        r = ctx.sqlite.invoke({
            "command": "get",
            "table": "items",
            "pk_value": "1",
        })
        assert "alive" in r
        ctx.abort()

    def test_db_delete_commit_removes(self, fresh_dir: Path) -> None:
        """Deleting a committed row then committing makes it invisible."""
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
            "row": {"id": "1", "val": "doomed"},
        })
        ctx.commit()

        ctx.begin()
        ctx.sqlite.invoke({
            "command": "delete",
            "table": "items",
            "pk_value": "1",
        })
        ctx.commit()

        ctx.begin()
        r = ctx.sqlite.invoke({
            "command": "get",
            "table": "items",
            "pk_value": "1",
        })
        assert "No row found" in r
        ctx.abort()

    def test_query_filters_work_with_mvcc(self, fresh_dir: Path) -> None:
        """Query with filters respects MVCC visibility."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "tasks",
            "columns": ["id TEXT", "status TEXT"],
        })
        for i, status in enumerate(["active", "active", "done"]):
            ctx.sqlite.invoke({
                "command": "put",
                "table": "tasks",
                "row": {"id": str(i), "status": status},
            })
        r = ctx.sqlite.invoke({
            "command": "query",
            "table": "tasks",
            "filters": {"status": "active"},
        })
        rows = json.loads(r)
        assert len(rows) == 2
        ctx.abort()


# ── Context manager semantics ────────────────────────────────────────


class TestContextManager:
    """Verify __enter__ / __exit__ semantics."""

    def test_context_manager_auto_abort(self, fresh_dir: Path) -> None:
        """Exiting with-block without commit auto-aborts."""
        with JanusContext(fresh_dir) as ctx:
            ctx.file_editor.invoke({
                "command": "create",
                "path": "auto_abort.txt",
                "file_text": "should vanish",
            })
        # No commit → auto-abort
        assert not (fresh_dir / "auto_abort.txt").exists()

    def test_context_manager_commit_then_exit(self, fresh_dir: Path) -> None:
        """Commit inside with-block; exit does not double-abort."""
        with JanusContext(fresh_dir) as ctx:
            ctx.file_editor.invoke({
                "command": "create",
                "path": "committed.txt",
                "file_text": "safe",
            })
            ctx.commit()
        assert (fresh_dir / "committed.txt").exists()

    def test_context_manager_exception_aborts(self, fresh_dir: Path) -> None:
        """Exception inside with-block auto-aborts."""
        with pytest.raises(ValueError):
            with JanusContext(fresh_dir) as ctx:
                ctx.file_editor.invoke({
                    "command": "create",
                    "path": "exception.txt",
                    "file_text": "will not persist",
                })
                raise ValueError("boom")
        assert not (fresh_dir / "exception.txt").exists()


# ── Error conditions ─────────────────────────────────────────────────


class TestErrorConditions:
    """Verify proper errors for invalid operations."""

    def test_double_begin_raises(self, fresh_dir: Path) -> None:
        """Calling begin twice raises RuntimeError."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        with pytest.raises(RuntimeError, match="already active"):
            ctx.begin()
        ctx.abort()

    def test_commit_without_begin_raises(self, fresh_dir: Path) -> None:
        """Commit without begin raises."""
        ctx = JanusContext(fresh_dir)
        with pytest.raises(RuntimeError, match="No active transaction"):
            ctx.commit()

    def test_abort_without_begin_raises(self, fresh_dir: Path) -> None:
        """Abort without begin raises."""
        ctx = JanusContext(fresh_dir)
        with pytest.raises(RuntimeError, match="No active transaction"):
            ctx.abort()

    def test_tools_before_begin_raises(self, fresh_dir: Path) -> None:
        """Accessing tools before begin raises RuntimeError."""
        ctx = JanusContext(fresh_dir)
        with pytest.raises(RuntimeError, match="not begun"):
            _ = ctx.file_editor
        with pytest.raises(RuntimeError, match="not begun"):
            _ = ctx.memory
        with pytest.raises(RuntimeError, match="not begun"):
            _ = ctx.bash

    def test_tools_after_commit_raises(self, fresh_dir: Path) -> None:
        """Accessing tools after commit raises."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.commit()
        with pytest.raises(RuntimeError, match="not begun"):
            _ = ctx.file_editor

    def test_tools_after_abort_raises(self, fresh_dir: Path) -> None:
        """Accessing tools after abort raises."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        ctx.abort()
        with pytest.raises(RuntimeError, match="not begun"):
            _ = ctx.memory

    def test_sqlite_disabled_raises(self, fresh_dir: Path) -> None:
        """Accessing sqlite when disabled raises RuntimeError."""
        ctx = JanusContext(fresh_dir, enable_sqlite=False)
        ctx.begin()
        with pytest.raises(RuntimeError, match="not available"):
            _ = ctx.sqlite
        ctx.abort()

    def test_get_tools_before_begin_raises(self, fresh_dir: Path) -> None:
        """get_tools before begin raises."""
        ctx = JanusContext(fresh_dir)
        with pytest.raises(RuntimeError, match="not begun"):
            ctx.get_tools()

    def test_savepoint_before_begin_raises(self, fresh_dir: Path) -> None:
        """savepoint before begin raises."""
        ctx = JanusContext(fresh_dir)
        with pytest.raises(RuntimeError, match="No active transaction"):
            ctx.savepoint("sp1")

    def test_rollback_without_savepoint_raises(self, fresh_dir: Path) -> None:
        """rollback without prior savepoint raises."""
        ctx = JanusContext(fresh_dir)
        ctx.begin()
        with pytest.raises(RuntimeError, match="No active savepoint"):
            ctx.rollback()
        ctx.abort()
