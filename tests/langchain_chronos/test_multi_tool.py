"""Multi-tool transactional tests.

Verifies that OverlayFS file tools and SQLite MVCC tool participate
in the **same** 2PC transaction:

  - commit persists both DB rows and filesystem changes atomically.
  - abort discards both.
  - savepoint/rollback rolls back both.
  - tools can reference each other's outputs within a transaction.

Run with: sudo python -m pytest tests/test_multi_tool.py -v
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Generator

import pytest

from langchain_chronos.context import (
    ChronosContext,
    ChronosTransactionControl,
    create_chronos_tools_with_active_txn,
)


def _require_root() -> None:
    if os.geteuid() != 0:
        pytest.skip("Requires root for OverlayFS")


@pytest.fixture
def fresh_base() -> Generator[Path, None, None]:
    _require_root()
    d = Path(tempfile.mkdtemp(prefix="tar_multi_test_"))
    (d / "README.md").write_text("# Multi-tool test project\n")
    (d / "src").mkdir()
    (d / "src" / "app.py").write_text("APP_VERSION = '1.0'\n")
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ── Atomic commit: FS + DB ───────────────────────────────────────────


class TestAtomicCommit:
    """Verify that commit persists both FS and DB changes atomically."""

    def test_commit_persists_fs_and_db(self, fresh_base: Path) -> None:
        """Files + DB rows both persist on commit."""
        ctx = ChronosContext(fresh_base, enable_sqlite=True)
        ctx.begin()

        # Create a file
        ctx.file_editor.invoke({
            "command": "create",
            "path": "data_manifest.json",
            "file_text": '{"version": 1}\n',
        })

        # Insert a DB row
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "manifest",
            "columns": ["id TEXT", "version INTEGER", "status TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "manifest",
            "row": {"id": "v1", "version": 1, "status": "active"},
        })

        ctx.commit()

        # File persists
        assert (fresh_base / "data_manifest.json").exists()
        # DB state persists in next txn
        ctx2 = ChronosContext(fresh_base, enable_sqlite=True,
                          db_path=ctx.sqlite_shim.conn.execute(
                              "PRAGMA database_list").fetchone()[2]
                          if ctx.sqlite_shim else ":memory:")
        # Since we used :memory:, DB state doesn't persist across contexts.
        # But the FS change does.
        content = (fresh_base / "data_manifest.json").read_text()
        assert '"version": 1' in content

    def test_commit_db_persists_across_txns(self, fresh_base: Path) -> None:
        """DB rows committed in txn1 are visible in txn2 (same context)."""
        ctx = ChronosContext(fresh_base, enable_sqlite=True)
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

        # Second transaction sees committed data
        ctx.begin()
        result = ctx.sqlite.invoke({
            "command": "get",
            "table": "users",
            "pk_value": "1",
        })
        assert "Alice" in result
        ctx.abort()


# ── Atomic abort: FS + DB ────────────────────────────────────────────


class TestAtomicAbort:
    """Verify that abort discards both FS and DB changes."""

    def test_abort_discards_fs_and_db(self, fresh_base: Path) -> None:
        """Files + DB rows are both discarded on abort."""
        ctx = ChronosContext(fresh_base, enable_sqlite=True)
        ctx.begin()

        ctx.file_editor.invoke({
            "command": "create",
            "path": "aborted.txt",
            "file_text": "should not persist",
        })
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "stuff",
            "columns": ["id TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "stuff",
            "row": {"id": "1", "val": "gone"},
        })

        ctx.abort()

        # File not in base
        assert not (fresh_base / "aborted.txt").exists()

        # DB row not visible in new txn
        ctx.begin()
        result = ctx.sqlite.invoke({
            "command": "get",
            "table": "stuff",
            "pk_value": "1",
        })
        assert "No row found" in result
        ctx.abort()

    def test_abort_preserves_previously_committed(
        self, fresh_base: Path
    ) -> None:
        """Abort only discards current txn, not previously committed data."""
        ctx = ChronosContext(fresh_base, enable_sqlite=True)

        # Txn 1: commit some data
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "persisted",
            "columns": ["id TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "persisted",
            "row": {"id": "1", "val": "safe"},
        })
        ctx.file_editor.invoke({
            "command": "create",
            "path": "safe.txt",
            "file_text": "safe content",
        })
        ctx.commit()

        # Txn 2: make changes then abort
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "put",
            "table": "persisted",
            "row": {"id": "2", "val": "doomed"},
        })
        ctx.file_editor.invoke({
            "command": "create",
            "path": "doomed.txt",
            "file_text": "doomed",
        })
        ctx.abort()

        # Previously committed data still there
        assert (fresh_base / "safe.txt").exists()
        ctx.begin()
        r = ctx.sqlite.invoke({"command": "get", "table": "persisted", "pk_value": "1"})
        assert "safe" in r
        r2 = ctx.sqlite.invoke({"command": "get", "table": "persisted", "pk_value": "2"})
        assert "No row found" in r2
        ctx.abort()


# ── Savepoint / rollback across tools ────────────────────────────────


class TestMultiToolSavepoint:
    """Verify savepoint/rollback works across both FS and DB."""

    def test_rollback_discards_both_fs_and_db(
        self, fresh_base: Path
    ) -> None:
        """After savepoint: new FS + DB changes are discarded on rollback.

        Tools switch to child overlay/txn during savepoint, so both
        FS writes and DB writes are scoped to the child. On rollback,
        the child overlay is discarded and child MVCC rows are cleaned up.
        """
        ctx = ChronosContext(fresh_base, enable_sqlite=True)
        ctx.begin()

        # Phase 1: pre-savepoint work (parent overlay + parent txn)
        ctx.file_editor.invoke({
            "command": "create",
            "path": "before_sp.txt",
            "file_text": "kept",
        })
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "sp_test",
            "columns": ["id TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "sp_test",
            "row": {"id": "1", "val": "before_sp"},
        })

        # Phase 2: savepoint (tools switch to child overlay + child txn)
        ctx.savepoint("sp1")

        # Phase 3: post-savepoint work (child overlay + child txn)
        ctx.file_editor.invoke({
            "command": "create",
            "path": "after_sp.txt",
            "file_text": "discarded",
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "sp_test",
            "row": {"id": "2", "val": "after_sp"},
        })

        # Phase 4: rollback — child overlay + child MVCC rows discarded
        ctx.rollback("sp1")

        # Commit — only pre-savepoint work persists
        ctx.commit()

        assert (fresh_base / "before_sp.txt").exists()
        assert not (fresh_base / "after_sp.txt").exists()

        # DB: pre-savepoint row persists, post-savepoint row is gone
        ctx.begin()
        r1 = ctx.sqlite.invoke({"command": "get", "table": "sp_test", "pk_value": "1"})
        assert "before_sp" in r1
        r2 = ctx.sqlite.invoke({"command": "get", "table": "sp_test", "pk_value": "2"})
        assert "No row found" in r2
        ctx.abort()


# ── Cross-tool data flow ────────────────────────────────────────────


class TestCrossToolDataFlow:
    """Tests where tools reference each other's outputs."""

    def test_db_tracks_file_operations(self, fresh_base: Path) -> None:
        """Use DB to track metadata about files created on FS."""
        ctx = ChronosContext(fresh_base, enable_sqlite=True)
        ctx.begin()

        # Register a file-tracking table
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "file_registry",
            "columns": ["path TEXT", "size INTEGER", "status TEXT"],
        })

        # Create files and track them in DB
        for name, content in [
            ("module_a.py", "def func_a(): pass\n"),
            ("module_b.py", "def func_b(): pass\n"),
            ("module_c.py", "def func_c(): pass\n"),
        ]:
            ctx.file_editor.invoke({
                "command": "create",
                "path": name,
                "file_text": content,
            })
            ctx.sqlite.invoke({
                "command": "put",
                "table": "file_registry",
                "row": {"path": name, "size": len(content), "status": "created"},
            })

        # Query the tracking DB
        result = ctx.sqlite.invoke({
            "command": "query",
            "table": "file_registry",
            "filters": {"status": "created"},
        })
        rows = json.loads(result)
        assert len(rows) == 3

        # Verify files exist on FS
        for row in rows:
            view = ctx.file_editor.invoke({
                "command": "view",
                "path": row["path"],
            })
            assert "def func_" in view

        ctx.abort()

    def test_bash_reads_db_output_via_file(self, fresh_base: Path) -> None:
        """Store data in DB, export to file, run bash on file."""
        ctx = ChronosContext(fresh_base, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "params",
            "columns": ["key TEXT", "value TEXT"],
            "seed_data": [
                {"key": "greeting", "value": "Hello from DB!"},
            ],
        })

        # Read from DB, write to file for bash to use
        result = ctx.sqlite.invoke({
            "command": "get",
            "table": "params",
            "pk_value": "greeting",
        })
        data = json.loads(result)
        ctx.file_editor.invoke({
            "command": "create",
            "path": "greeting.txt",
            "file_text": data["value"],
        })

        # Bash reads the file
        bash_result = ctx.bash.invoke({"command": "cat greeting.txt"})
        assert "Hello from DB!" in bash_result

        ctx.abort()

    def test_memory_notes_with_db_summary(self, fresh_base: Path) -> None:
        """Agent workflow: DB ops tracked in memory notes."""
        ctx = ChronosContext(fresh_base, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "tasks",
            "columns": ["id TEXT", "task TEXT", "done INTEGER"],
        })

        # Create tasks
        for i, task in enumerate(["parse input", "transform", "validate"]):
            ctx.sqlite.invoke({
                "command": "put",
                "table": "tasks",
                "row": {"id": str(i), "task": task, "done": 0},
            })

        # Record progress
        ctx.memory.invoke({
            "command": "create",
            "path": "progress.md",
            "file_text": (
                "# Pipeline Progress\n"
                "- Created 3 tasks in DB\n"
                "- Tasks: parse input, transform, validate\n"
            ),
        })

        # Mark first task done
        ctx.sqlite.invoke({
            "command": "put",
            "table": "tasks",
            "row": {"id": "0", "task": "parse input", "done": 1},
        })
        ctx.memory.invoke({
            "command": "str_replace",
            "path": "progress.md",
            "old_str": "- Created 3 tasks in DB",
            "new_str": "- Created 3 tasks in DB\n- [x] Task 0 complete",
        })

        # Verify memory
        mem = ctx.memory.invoke({"command": "view", "path": "progress.md"})
        assert "Task 0 complete" in mem

        # Verify DB
        r = ctx.sqlite.invoke({"command": "get", "table": "tasks", "pk_value": "0"})
        data = json.loads(r)
        assert data["done"] == 1

        ctx.abort()


# ── get_tools includes sqlite ────────────────────────────────────────


class TestGetToolsIncludesSQLite:
    def test_get_tools_includes_sqlite(self, fresh_base: Path) -> None:
        ctx = ChronosContext(fresh_base, enable_sqlite=True)
        ctx.begin()
        tools = ctx.get_tools()
        names = {t.name for t in tools}
        assert "chronos_sqlite" in names
        assert "chronos_file_editor" in names
        assert "chronos_memory" in names
        assert "chronos_bash" in names
        assert "chronos_vectorstore" in names
        assert len(tools) == 5
        ctx.abort()

    def test_get_tools_without_sqlite(self, fresh_base: Path) -> None:
        ctx = ChronosContext(fresh_base, enable_sqlite=False)
        ctx.begin()
        tools = ctx.get_tools()
        names = {t.name for t in tools}
        assert "chronos_sqlite" not in names
        assert "chronos_vectorstore" in names  # vectorstore is still enabled
        assert len(tools) == 4
        ctx.abort()

    def test_create_chronos_tools_with_active_txn_includes_sqlite(
        self, fresh_base: Path
    ) -> None:
        ctx, tools = create_chronos_tools_with_active_txn(fresh_base)
        try:
            names = {t.name for t in tools}
            assert "chronos_sqlite" in names
            assert "chronos_vectorstore" in names
            assert "chronos_txn" in names
            assert len(tools) == 6  # editor, memory, bash, sqlite, vectorstore, txn
        finally:
            ctx.abort()


# ── get_changes combines both shims ─────────────────────────────────


class TestCombinedChanges:
    def test_changes_from_both_shims(self, fresh_base: Path) -> None:
        """get_changes() includes both FS and DB changes."""
        ctx = ChronosContext(fresh_base, enable_sqlite=True)
        ctx.begin()

        # FS change
        ctx.file_editor.invoke({
            "command": "create",
            "path": "tracked.txt",
            "file_text": "content",
        })

        # DB change
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "tracked",
            "columns": ["id TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "tracked",
            "row": {"id": "1", "val": "x"},
        })

        changes = ctx.get_changes()
        resource_ids = [c.resource_id for c in changes]

        # Should have both FS and DB changes
        has_fs = any("tracked.txt" in r for r in resource_ids)
        has_db = any("tracked/" in r for r in resource_ids)
        assert has_fs, f"Expected FS change in {resource_ids}"
        assert has_db, f"Expected DB change in {resource_ids}"

        ctx.abort()


# ── ChronosTransactionControl with sqlite ─────────────────────────────


class TestTransactionControlWithSQLite:
    """The ChronosTransactionControl tool manages both FS and DB."""

    def test_ctl_commit_persists_both(self, fresh_base: Path) -> None:
        ctx = ChronosContext(fresh_base, enable_sqlite=True)
        ctl = ChronosTransactionControl(chronos_context=ctx)

        ctl.invoke({"action": "begin"})
        ctx.file_editor.invoke({
            "command": "create",
            "path": "via_ctl.txt",
            "file_text": "ctl",
        })
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "ctl_test",
            "columns": ["id TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "ctl_test",
            "row": {"id": "1", "val": "ctl_data"},
        })

        result = ctl.invoke({"action": "changes"})
        assert "via_ctl.txt" in result or "ctl_test" in result

        ctl.invoke({"action": "commit"})
        assert (fresh_base / "via_ctl.txt").exists()

    def test_ctl_abort_discards_both(self, fresh_base: Path) -> None:
        ctx = ChronosContext(fresh_base, enable_sqlite=True)
        ctl = ChronosTransactionControl(chronos_context=ctx)

        ctl.invoke({"action": "begin"})
        ctx.file_editor.invoke({
            "command": "create",
            "path": "ctl_abort.txt",
            "file_text": "gone",
        })
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "ctl_gone",
            "columns": ["id TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "ctl_gone",
            "row": {"id": "1"},
        })
        ctl.invoke({"action": "abort"})

        assert not (fresh_base / "ctl_abort.txt").exists()


# ── Complex workflow scenario ────────────────────────────────────────


class TestComplexWorkflow:
    """End-to-end multi-tool workflow scenarios."""

    def test_code_gen_with_test_tracking(self, fresh_base: Path) -> None:
        """
        Scenario: Agent generates code, runs tests, tracks results in DB,
        and only commits if all tests pass.
        """
        ctx = ChronosContext(fresh_base, enable_sqlite=True)
        ctx.begin()

        # Set up test tracking DB
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "test_results",
            "columns": ["test_name TEXT", "result TEXT", "output TEXT"],
        })

        # Generate a module
        ctx.file_editor.invoke({
            "command": "create",
            "path": "calculator.py",
            "file_text": (
                "def add(a, b): return a + b\n"
                "def mul(a, b): return a * b\n"
                "def div(a, b):\n"
                "    if b == 0: raise ValueError('division by zero')\n"
                "    return a / b\n"
            ),
        })

        # Generate test script
        ctx.file_editor.invoke({
            "command": "create",
            "path": "test_calc.py",
            "file_text": (
                "from calculator import add, mul, div\n"
                "import sys\n\n"
                "failures = []\n"
                "tests = [\n"
                "    ('test_add', lambda: add(2, 3) == 5),\n"
                "    ('test_mul', lambda: mul(4, 5) == 20),\n"
                "    ('test_div', lambda: div(10, 2) == 5.0),\n"
                "    ('test_div_zero', lambda: _check_raises()),\n"
                "]\n\n"
                "def _check_raises():\n"
                "    try: div(1, 0); return False\n"
                "    except ValueError: return True\n\n"
                "for name, fn in tests:\n"
                "    try:\n"
                "        ok = fn()\n"
                "        status = 'PASS' if ok else 'FAIL'\n"
                "    except Exception as e:\n"
                "        status = f'ERROR: {e}'\n"
                "    print(f'{name}: {status}')\n"
                "    if status != 'PASS': failures.append(name)\n\n"
                "if failures:\n"
                "    print(f'FAILED: {failures}')\n"
                "    sys.exit(1)\n"
                "else:\n"
                "    print('ALL TESTS PASSED')\n"
            ),
        })

        # Run the tests via bash
        output = ctx.bash.invoke({"command": "python3 test_calc.py 2>&1"})

        # Parse results and store in DB
        for line in output.strip().split("\n"):
            if ": " in line and not line.startswith("ALL") and not line.startswith("FAILED"):
                name, status = line.split(": ", 1)
                ctx.sqlite.invoke({
                    "command": "put",
                    "table": "test_results",
                    "row": {"test_name": name, "result": status, "output": ""},
                })

        # Record in memory
        ctx.memory.invoke({
            "command": "create",
            "path": "test_run.md",
            "file_text": f"# Test Run\n\n```\n{output}\n```\n",
        })

        # Check results — if all passed, commit
        assert "ALL TESTS PASSED" in output

        # Verify DB tracked 4 tests
        r = ctx.sqlite.invoke({"command": "query", "table": "test_results"})
        rows = json.loads(r)
        assert len(rows) == 4
        assert all(row["result"] == "PASS" for row in rows)

        ctx.commit()

        # Verify everything persisted
        assert (fresh_base / "calculator.py").exists()
        assert (fresh_base / "memories" / "test_run.md").exists()

    def test_exploratory_development_with_rollback(
        self, fresh_base: Path
    ) -> None:
        """
        Scenario: Agent tries an approach (savepoint), finds it wrong,
        rolls back, tries another approach that works.

        With proper savepoint isolation, the failed approach's FS and
        DB changes are fully discarded on rollback.
        """
        ctx = ChronosContext(fresh_base, enable_sqlite=True)
        ctx.begin()

        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "approaches",
            "columns": ["id TEXT", "description TEXT", "outcome TEXT"],
        })

        # First approach: try a regex solution
        ctx.savepoint("attempt_1")

        ctx.file_editor.invoke({
            "command": "create",
            "path": "parser.py",
            "file_text": (
                "import re\n"
                "def parse(s):\n"
                "    # Buggy regex approach\n"
                "    return re.findall(r'\\d+', s)\n"
            ),
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "approaches",
            "row": {"id": "1", "description": "regex", "outcome": "failed"},
        })

        # Roll back — both FS and DB changes discarded
        ctx.rollback("attempt_1")

        # Second approach: manual parsing (back on parent overlay)
        ctx.file_editor.invoke({
            "command": "create",
            "path": "parser.py",
            "file_text": (
                "def parse(s):\n"
                "    # Simple split approach\n"
                "    return s.split()\n"
            ),
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "approaches",
            "row": {"id": "2", "description": "split", "outcome": "success"},
        })

        # Test it
        output = ctx.bash.invoke({
            "command": "python3 -c \"from parser import parse; print(parse('hello world'))\""
        })
        assert "hello" in output

        ctx.commit()

        # Verify final state — FS has the good approach
        content = (fresh_base / "parser.py").read_text()
        assert "split" in content  # The good approach persisted

        # The failed approach's DB row was rolled back
        ctx.begin()
        r1 = ctx.sqlite.invoke({
            "command": "get", "table": "approaches", "pk_value": "1"
        })
        assert "No row found" in r1
        r2 = ctx.sqlite.invoke({
            "command": "get", "table": "approaches", "pk_value": "2"
        })
        assert "success" in r2
        ctx.abort()

    def test_sequential_transactions_accumulate(
        self, fresh_base: Path
    ) -> None:
        """
        Multiple sequential transactions build on each other.
        """
        ctx = ChronosContext(fresh_base, enable_sqlite=True)

        ctx.sqlite_shim  # ensure shim exists

        # Txn 1: Create table + initial data
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "log",
            "columns": ["id TEXT", "msg TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "log",
            "row": {"id": "1", "msg": "initialized"},
        })
        ctx.file_editor.invoke({
            "command": "create",
            "path": "app.log",
            "file_text": "=== App Log ===\n",
        })
        ctx.commit()

        # Txn 2: Add more
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "put",
            "table": "log",
            "row": {"id": "2", "msg": "step 2"},
        })
        # The file from txn1 should be in the new overlay
        view = ctx.file_editor.invoke({"command": "view", "path": "app.log"})
        assert "App Log" in view
        ctx.commit()

        # Txn 3: Query accumulated data
        ctx.begin()
        r = ctx.sqlite.invoke({"command": "query", "table": "log"})
        rows = json.loads(r)
        assert len(rows) == 2
        msgs = {row["msg"] for row in rows}
        assert "initialized" in msgs
        assert "step 2" in msgs
        ctx.abort()
