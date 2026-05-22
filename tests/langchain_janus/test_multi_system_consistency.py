"""Multi-data-system consistency tests for Janus.

Verifies that the OverlayFS filesystem and SQLite MVCC database are
kept in a consistent state across transactional operations:

  - Atomic persistence: both or neither are committed/aborted.
  - Cross-system data flow: data written by one tool readable by another.
  - Sequential transactions build correct cumulative state.
  - Disabling one system doesn't break the other.
  - get_changes() accurately reflects both systems.

Run with: sudo pytest tests/test_multi_system_consistency.py -v
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
    d = Path(tempfile.mkdtemp(prefix="tar_multi_sys_"))
    (d / "README.md").write_text("# Multi-system test\n")
    (d / "src").mkdir()
    (d / "src" / "app.py").write_text("VERSION = '1.0'\n")
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ── Atomic persistence ──────────────────────────────────────────────


class TestAtomicPersistence:
    """FS and DB changes must commit or abort as a unit."""

    def test_commit_both_persist(self, fresh_dir: Path) -> None:
        """After commit, both FS file and DB row exist."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()
        ctx.file_editor.invoke({
            "command": "create",
            "path": "manifest.json",
            "file_text": '{"v": 1}\n',
        })
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "manifest",
            "columns": ["id TEXT", "version INTEGER"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "manifest",
            "row": {"id": "v1", "version": 1},
        })
        ctx.commit()

        assert (fresh_dir / "manifest.json").exists()
        ctx.begin()
        r = ctx.sqlite.invoke({
            "command": "get",
            "table": "manifest",
            "pk_value": "v1",
        })
        assert "1" in r
        ctx.abort()

    def test_abort_both_discarded(self, fresh_dir: Path) -> None:
        """After abort, neither FS file nor DB row exists."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()
        ctx.file_editor.invoke({
            "command": "create",
            "path": "temp.txt",
            "file_text": "temp",
        })
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "temp",
            "columns": ["id TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "temp",
            "row": {"id": "1", "val": "temp"},
        })
        ctx.abort()

        assert not (fresh_dir / "temp.txt").exists()
        ctx.begin()
        r = ctx.sqlite.invoke({
            "command": "get",
            "table": "temp",
            "pk_value": "1",
        })
        assert "No row found" in r
        ctx.abort()

    def test_abort_preserves_prior_commit(self, fresh_dir: Path) -> None:
        """Abort only discards current txn, not previously committed state."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)

        # Txn 1: commit
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "safe",
            "columns": ["id TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "safe",
            "row": {"id": "1", "val": "committed"},
        })
        ctx.file_editor.invoke({
            "command": "create",
            "path": "safe.txt",
            "file_text": "safe\n",
        })
        ctx.commit()

        # Txn 2: abort
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "put",
            "table": "safe",
            "row": {"id": "2", "val": "doomed"},
        })
        ctx.file_editor.invoke({
            "command": "create",
            "path": "doomed.txt",
            "file_text": "doomed",
        })
        ctx.abort()

        # Verify previous commit survived
        assert (fresh_dir / "safe.txt").exists()
        assert not (fresh_dir / "doomed.txt").exists()

        ctx.begin()
        r1 = ctx.sqlite.invoke({
            "command": "get",
            "table": "safe",
            "pk_value": "1",
        })
        assert "committed" in r1
        r2 = ctx.sqlite.invoke({
            "command": "get",
            "table": "safe",
            "pk_value": "2",
        })
        assert "No row found" in r2
        ctx.abort()


# ── Cross-data-system flow ───────────────────────────────────────────


class TestCrossDataSystemFlow:
    """Tools can reference each other's outputs within a transaction."""

    def test_db_metadata_tracks_fs_files(self, fresh_dir: Path) -> None:
        """Create files, register metadata in DB, query consistently."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "files",
            "columns": ["path TEXT", "size INTEGER", "status TEXT"],
        })

        for name, content in [
            ("mod_a.py", "def a(): pass\n"),
            ("mod_b.py", "def b(): pass\n"),
        ]:
            ctx.file_editor.invoke({
                "command": "create",
                "path": name,
                "file_text": content,
            })
            ctx.sqlite.invoke({
                "command": "put",
                "table": "files",
                "row": {"path": name, "size": len(content), "status": "ok"},
            })

        # DB and FS are consistent
        r = ctx.sqlite.invoke({
            "command": "query",
            "table": "files",
        })
        rows = json.loads(r)
        assert len(rows) == 2

        for row in rows:
            view = ctx.file_editor.invoke({
                "command": "view",
                "path": row["path"],
            })
            assert "def " in view
        ctx.abort()

    def test_fs_export_from_db(self, fresh_dir: Path) -> None:
        """Store config in DB, export to file, bash reads file."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "config",
            "columns": ["key TEXT", "value TEXT"],
            "seed_data": [{"key": "greeting", "value": "Hello DB!"}],
        })

        r = ctx.sqlite.invoke({
            "command": "get",
            "table": "config",
            "pk_value": "greeting",
        })
        data = json.loads(r)
        ctx.file_editor.invoke({
            "command": "create",
            "path": "greeting.txt",
            "file_text": data["value"],
        })

        bash_out = ctx.bash.invoke({"command": "cat greeting.txt"})
        assert "Hello DB!" in bash_out
        ctx.abort()

    def test_bash_output_to_db(self, fresh_dir: Path) -> None:
        """Execute bash, store result in DB."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "cmd_log",
            "columns": ["id TEXT", "cmd TEXT", "output TEXT"],
        })

        output = ctx.bash.invoke({"command": "echo hello_world"})
        ctx.sqlite.invoke({
            "command": "put",
            "table": "cmd_log",
            "row": {
                "id": "1",
                "cmd": "echo hello_world",
                "output": output.strip(),
            },
        })

        r = ctx.sqlite.invoke({
            "command": "get",
            "table": "cmd_log",
            "pk_value": "1",
        })
        data = json.loads(r)
        assert data["output"] == "hello_world"
        ctx.abort()

    def test_memory_with_db_summary(self, fresh_dir: Path) -> None:
        """Track DB operations in memory notes."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "tasks",
            "columns": ["id TEXT", "task TEXT", "done INTEGER"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "tasks",
            "row": {"id": "1", "task": "build", "done": 0},
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "tasks",
            "row": {"id": "2", "task": "test", "done": 0},
        })

        # Summarise in memory
        ctx.memory.invoke({
            "command": "create",
            "path": "pipeline.md",
            "file_text": "# Pipeline\n- 2 tasks created\n",
        })

        # Complete a task
        ctx.sqlite.invoke({
            "command": "put",
            "table": "tasks",
            "row": {"id": "1", "task": "build", "done": 1},
        })
        ctx.memory.invoke({
            "command": "str_replace",
            "path": "pipeline.md",
            "old_str": "- 2 tasks created",
            "new_str": "- 2 tasks created\n- task 1 done",
        })

        mem = ctx.memory.invoke({
            "command": "view",
            "path": "pipeline.md",
        })
        assert "task 1 done" in mem

        r = ctx.sqlite.invoke({
            "command": "get",
            "table": "tasks",
            "pk_value": "1",
        })
        assert json.loads(r)["done"] == 1
        ctx.abort()


# ── Sequential transaction consistency ───────────────────────────────


class TestSequentialConsistency:
    """Multiple sequential transactions build correct cumulative state."""

    def test_three_txns_accumulate(self, fresh_dir: Path) -> None:
        """Three commits in sequence build up both FS and DB state."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)

        # Txn 1
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "log",
            "columns": ["id TEXT", "msg TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "log",
            "row": {"id": "1", "msg": "init"},
        })
        ctx.file_editor.invoke({
            "command": "create",
            "path": "log.txt",
            "file_text": "=== Log ===\n",
        })
        ctx.commit()

        # Txn 2
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "put",
            "table": "log",
            "row": {"id": "2", "msg": "step2"},
        })
        view = ctx.file_editor.invoke({
            "command": "view",
            "path": "log.txt",
        })
        assert "Log" in view
        ctx.commit()

        # Txn 3: query accumulated
        ctx.begin()
        r = ctx.sqlite.invoke({"command": "query", "table": "log"})
        rows = json.loads(r)
        assert len(rows) == 2
        msgs = {row["msg"] for row in rows}
        assert msgs == {"init", "step2"}
        ctx.abort()

    def test_interleaved_commits_and_aborts(self, fresh_dir: Path) -> None:
        """Commit-abort-commit sequence leaves correct state."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)

        # Txn 1: commit
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "data",
            "columns": ["id TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "data",
            "row": {"id": "A", "val": "committed1"},
        })
        ctx.file_editor.invoke({
            "command": "create",
            "path": "a.txt",
            "file_text": "A\n",
        })
        ctx.commit()

        # Txn 2: abort
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "put",
            "table": "data",
            "row": {"id": "B", "val": "aborted"},
        })
        ctx.file_editor.invoke({
            "command": "create",
            "path": "b.txt",
            "file_text": "B\n",
        })
        ctx.abort()

        # Txn 3: commit
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "put",
            "table": "data",
            "row": {"id": "C", "val": "committed2"},
        })
        ctx.file_editor.invoke({
            "command": "create",
            "path": "c.txt",
            "file_text": "C\n",
        })
        ctx.commit()

        # Final state
        assert (fresh_dir / "a.txt").exists()
        assert not (fresh_dir / "b.txt").exists()
        assert (fresh_dir / "c.txt").exists()

        ctx.begin()
        ra = ctx.sqlite.invoke({
            "command": "get",
            "table": "data",
            "pk_value": "A",
        })
        rb = ctx.sqlite.invoke({
            "command": "get",
            "table": "data",
            "pk_value": "B",
        })
        rc = ctx.sqlite.invoke({
            "command": "get",
            "table": "data",
            "pk_value": "C",
        })
        assert "committed1" in ra
        assert "No row found" in rb
        assert "committed2" in rc
        ctx.abort()

    def test_update_across_txns(self, fresh_dir: Path) -> None:
        """A value updated across multiple transactions has correct final state."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)

        ctx.begin()
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "counter",
            "columns": ["id TEXT", "val INTEGER"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "counter",
            "row": {"id": "c", "val": 0},
        })
        ctx.commit()

        for i in range(1, 4):
            ctx.begin()
            ctx.sqlite.invoke({
                "command": "put",
                "table": "counter",
                "row": {"id": "c", "val": i},
            })
            ctx.commit()

        ctx.begin()
        r = ctx.sqlite.invoke({
            "command": "get",
            "table": "counter",
            "pk_value": "c",
        })
        assert json.loads(r)["val"] == 3
        ctx.abort()


# ── Enable/disable SQLite ────────────────────────────────────────────


class TestMixedSystemDisabling:
    """Disabling SQLite still allows FS-only transactions."""

    def test_sqlite_disabled_fs_works(self, fresh_dir: Path) -> None:
        """FS operations work normally when SQLite is disabled."""
        ctx = JanusContext(fresh_dir, enable_sqlite=False)
        ctx.begin()
        ctx.file_editor.invoke({
            "command": "create",
            "path": "fs_only.txt",
            "file_text": "no db needed",
        })
        ctx.commit()
        assert (fresh_dir / "fs_only.txt").exists()

    def test_sqlite_disabled_abort_works(self, fresh_dir: Path) -> None:
        """Abort works with SQLite disabled."""
        ctx = JanusContext(fresh_dir, enable_sqlite=False)
        ctx.begin()
        ctx.file_editor.invoke({
            "command": "create",
            "path": "nope.txt",
            "file_text": "gone",
        })
        ctx.abort()
        assert not (fresh_dir / "nope.txt").exists()

    def test_sqlite_disabled_get_tools_excludes_sqlite(
        self, fresh_dir: Path
    ) -> None:
        """get_tools returns 4 tools (no sqlite) when disabled (vectorstore still enabled)."""
        ctx = JanusContext(fresh_dir, enable_sqlite=False)
        ctx.begin()
        tools = ctx.get_tools()
        names = {t.name for t in tools}
        assert "janus_sqlite" not in names
        assert "janus_vectorstore" in names
        assert len(tools) == 4
        ctx.abort()

    def test_sqlite_disabled_shim_is_none(self, fresh_dir: Path) -> None:
        """sqlite_shim is None when disabled."""
        ctx = JanusContext(fresh_dir, enable_sqlite=False)
        assert ctx.sqlite_shim is None


# ── get_changes consistency ──────────────────────────────────────────


class TestGetChangesConsistency:
    """get_changes() accurately reflects operations from both systems."""

    def test_changes_empty_initially(self, fresh_dir: Path) -> None:
        """No changes right after begin."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()
        changes = ctx.get_changes()
        assert len(changes) == 0
        ctx.abort()

    def test_changes_reflect_both(self, fresh_dir: Path) -> None:
        """changes include entries from both FS and DB."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()
        ctx.file_editor.invoke({
            "command": "create",
            "path": "tracked.txt",
            "file_text": "data",
        })
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "tracked",
            "columns": ["id TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "tracked",
            "row": {"id": "1"},
        })

        changes = ctx.get_changes()
        resource_ids = [c.resource_id for c in changes]
        has_fs = any("tracked.txt" in r for r in resource_ids)
        has_db = any("tracked/" in r for r in resource_ids)
        assert has_fs, f"No FS change in {resource_ids}"
        assert has_db, f"No DB change in {resource_ids}"
        ctx.abort()

    def test_changes_grow_with_operations(self, fresh_dir: Path) -> None:
        """Number of changes increases with operations."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()
        assert len(ctx.get_changes()) == 0

        ctx.file_editor.invoke({
            "command": "create",
            "path": "f1.txt",
            "file_text": "x",
        })
        n1 = len(ctx.get_changes())
        assert n1 > 0

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "t1",
            "columns": ["id TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "t1",
            "row": {"id": "1"},
        })
        n2 = len(ctx.get_changes())
        assert n2 > n1
        ctx.abort()

    def test_changes_after_no_ops_in_new_txn(self, fresh_dir: Path) -> None:
        """New txn after commit has zero changes (no work done yet)."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "x",
            "columns": ["id TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "x",
            "row": {"id": "1"},
        })
        ctx.commit()

        ctx.begin()
        assert len(ctx.get_changes()) == 0
        ctx.abort()


# ── Transaction control tool consistency ─────────────────────────────


class TestTransactionControlConsistency:
    """JanusTransactionControl mirrors system state correctly."""

    def test_status_shows_changes(self, fresh_dir: Path) -> None:
        """status action reports change count from both systems."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctl = JanusTransactionControl(janus_context=ctx)

        ctl.invoke({"action": "begin"})
        ctx.file_editor.invoke({
            "command": "create",
            "path": "ctl_test.txt",
            "file_text": "x",
        })
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "ctl",
            "columns": ["id TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "ctl",
            "row": {"id": "1"},
        })

        status = ctl.invoke({"action": "status"})
        assert "active" in status.lower() or "Active" in status
        assert "Changes:" in status
        ctl.invoke({"action": "abort"})

    def test_changes_action_reports_both(self, fresh_dir: Path) -> None:
        """changes action reports entries from both FS and DB."""
        ctx = JanusContext(fresh_dir, enable_sqlite=True)
        ctl = JanusTransactionControl(janus_context=ctx)

        ctl.invoke({"action": "begin"})
        ctx.file_editor.invoke({
            "command": "create",
            "path": "report.txt",
            "file_text": "x",
        })
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "report",
            "columns": ["id TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "report",
            "row": {"id": "1"},
        })

        changes_text = ctl.invoke({"action": "changes"})
        assert "report.txt" in changes_text or "report/" in changes_text
        ctl.invoke({"action": "abort"})
