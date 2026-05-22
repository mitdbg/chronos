"""Shared test fixtures for Janus-Code tests."""

from __future__ import annotations

from pathlib import Path
import re
from types import SimpleNamespace

import pytest

from janus_code.config import Config


class MockJanusContext:
    """Minimal JanusContext-compatible shim for tests without real OverlayFS."""

    def __init__(self, base_path: Path) -> None:
        self.base_path = Path(base_path)
        self.working_dir = self.base_path
        self.is_active = False
        self._sqlite_shim = MockSQLiteShim()
        self._sqlite = None
        self._txn = None
        self._child_txn = None
        self._next_txn_id = 0

    def begin(self):
        self._next_txn_id += 1
        self.is_active = True
        self.working_dir = self.base_path
        self._child_txn = None
        self._txn = SimpleNamespace(id=self._next_txn_id, parent_id=None)
        self._sqlite = MockJanusSQLite(shim=self._sqlite_shim, txn=self._txn)
        return self._txn

    def savepoint(self, _name: str = "") -> None:
        if not self.is_active:
            raise RuntimeError("No active transaction for savepoint")
        self._next_txn_id += 1
        self._child_txn = SimpleNamespace(
            id=self._next_txn_id,
            parent_id=self._next_txn_id - 1,
        )
        if self._sqlite is not None:
            self._sqlite.txn = self._child_txn

    def _commit_active_savepoint(self) -> None:
        if self._child_txn is not None and self._txn is not None:
            self._sqlite_shim.merge_txn(self._child_txn, self._txn)
        self._child_txn = None
        if self._sqlite is not None and self._txn is not None:
            self._sqlite.txn = self._txn

    def rollback(self, _name: str | None = None) -> None:
        if self._child_txn is not None:
            self._sqlite_shim.abort_txn(self._child_txn)
        self._child_txn = None
        if self._sqlite is not None and self._txn is not None:
            self._sqlite.txn = self._txn

    def commit(self) -> None:
        if self._child_txn is not None and self._txn is not None:
            self._sqlite_shim.merge_txn(self._child_txn, self._txn)
            self._child_txn = None
        if self._txn is not None:
            self._sqlite_shim.commit_txn(self._txn)
        self.is_active = False
        self._txn = None
        self._child_txn = None
        self._sqlite = None

    def abort(self) -> None:
        if self._child_txn is not None:
            self._sqlite_shim.abort_txn(self._child_txn)
        if self._txn is not None:
            self._sqlite_shim.abort_txn(self._txn)
        self.is_active = False
        self._txn = None
        self._child_txn = None
        self._sqlite = None

    @property
    def sqlite(self):
        if self._sqlite is None:
            raise RuntimeError("SQLite not available. Transaction not begun.")
        return self._sqlite


class MockSQLiteShim:
    """Minimal in-memory SQLiteShim-like store for Janus-code unit tests."""

    def __init__(self) -> None:
        self._tables: dict[str, SimpleNamespace] = {}
        self._committed: dict[str, dict[str, dict]] = {}
        self._pending: dict[str, dict[str, dict[str, dict | None]]] = {}

    @staticmethod
    def _txn_id(txn) -> str:
        return str(getattr(txn, "id", txn))

    def register_table(self, table: str, columns: list[str], pk_column: str | None = None):
        if not columns:
            raise ValueError("columns are required")
        col_names = [c.split()[0] for c in columns]
        pk = pk_column or col_names[0]
        if table not in self._tables:
            self._tables[table] = SimpleNamespace(pk=pk, columns=col_names)
            self._committed[table] = {}

    def seed_data(self, table: str, rows: list[dict]):
        meta = self._tables[table]
        for row in rows:
            self._committed[table][str(row[meta.pk])] = dict(row)

    def _view(self, txn) -> dict[str, dict[str, dict]]:
        tid = self._txn_id(txn)
        view = {t: {k: dict(v) for k, v in rows.items()} for t, rows in self._committed.items()}
        changes = self._pending.get(tid, {})
        for table, rows in changes.items():
            bucket = view.setdefault(table, {})
            for pk, row in rows.items():
                if row is None:
                    bucket.pop(pk, None)
                else:
                    bucket[pk] = dict(row)
        return view

    def put(self, txn, table: str, row: dict):
        if table not in self._tables:
            raise ValueError(f"unknown table: {table}")
        meta = self._tables[table]
        if meta.pk not in row:
            raise ValueError(f"row missing pk column '{meta.pk}'")
        tid = self._txn_id(txn)
        table_changes = self._pending.setdefault(tid, {}).setdefault(table, {})
        table_changes[str(row[meta.pk])] = dict(row)

    def get(self, txn, table: str, pk_value: str):
        rows = self._view(txn).get(table, {})
        row = rows.get(str(pk_value))
        return None if row is None else dict(row)

    def query(self, txn, table: str, filters: dict | None = None, order_by: str | None = None, limit: int | None = None):
        rows = list(self._view(txn).get(table, {}).values())
        if filters:
            def keep(row):
                return all(row.get(k) == v for k, v in filters.items())
            rows = [r for r in rows if keep(r)]
        if order_by:
            rows.sort(key=lambda r: r.get(order_by))
        if limit is not None:
            rows = rows[: max(0, int(limit))]
        return [dict(r) for r in rows]

    def delete(self, txn, table: str, pk_value: str) -> bool:
        existing = self.get(txn, table, pk_value)
        tid = self._txn_id(txn)
        table_changes = self._pending.setdefault(tid, {}).setdefault(table, {})
        table_changes[str(pk_value)] = None
        return existing is not None

    def execute_sql(self, txn, sql: str, params: list | None = None):
        sql_s = sql.strip()
        m = re.match(r"(?is)^select\s+\*\s+from\s+([a-zA-Z0-9_]+)(?:\s+where\s+([a-zA-Z0-9_]+)\s*=\s*\?)?(?:\s+limit\s+(\d+))?\s*;?$", sql_s)
        if not m:
            raise ValueError("MockSQLiteShim only supports simple SELECT * queries in tests.")
        table, where_col, limit_s = m.groups()
        filters = None
        if where_col:
            value = params[0] if params else None
            filters = {where_col: value}
        limit = int(limit_s) if limit_s else None
        return self.query(txn, table, filters=filters, limit=limit)

    def commit_txn(self, txn):
        tid = self._txn_id(txn)
        changes = self._pending.pop(tid, {})
        for table, rows in changes.items():
            bucket = self._committed.setdefault(table, {})
            for pk, row in rows.items():
                if row is None:
                    bucket.pop(pk, None)
                else:
                    bucket[pk] = dict(row)

    def abort_txn(self, txn):
        tid = self._txn_id(txn)
        self._pending.pop(tid, None)

    def merge_txn(self, child_txn, parent_txn):
        child_id = self._txn_id(child_txn)
        parent_id = self._txn_id(parent_txn)
        child_changes = self._pending.pop(child_id, {})
        if not child_changes:
            return
        parent_changes = self._pending.setdefault(parent_id, {})
        for table, rows in child_changes.items():
            table_changes = parent_changes.setdefault(table, {})
            table_changes.update(rows)


class MockJanusSQLite:
    """Minimal sqlite tool facade over MockSQLiteShim for tests."""

    def __init__(self, shim: MockSQLiteShim, txn) -> None:
        self.shim = shim
        self.txn = txn

    def run(self, payload: dict) -> str:
        command = payload.get("command")
        table = payload.get("table")
        row = payload.get("row")
        pk_value = payload.get("pk_value")
        columns = payload.get("columns")
        pk_column = payload.get("pk_column")
        filters = payload.get("filters")
        order_by = payload.get("order_by")
        limit = payload.get("limit")
        sql = payload.get("sql")
        params = payload.get("params")
        seed_data = payload.get("seed_data")

        try:
            if command == "register_table":
                if not table:
                    return "Error: 'table' is required for register_table."
                if not columns:
                    return "Error: 'columns' is required for register_table."
                self.shim.register_table(table, columns, pk_column)
                if seed_data:
                    self.shim.seed_data(table, seed_data)
                return f"Table '{table}' registered."
            if command == "put":
                if not table:
                    return "Error: 'table' is required for put."
                if not row:
                    return "Error: 'row' is required for put."
                self.shim.put(self.txn, table, row)
                return f"Row inserted/updated in '{table}'."
            if command == "get":
                if not table:
                    return "Error: 'table' is required for get."
                if pk_value is None:
                    return "Error: 'pk_value' is required for get."
                out = self.shim.get(self.txn, table, pk_value)
                return "null" if out is None else str(out)
            if command == "query":
                if not table:
                    return "Error: 'table' is required for query."
                out = self.shim.query(
                    self.txn,
                    table,
                    filters=filters,
                    order_by=order_by,
                    limit=limit,
                )
                return str(out)
            if command == "delete":
                if not table:
                    return "Error: 'table' is required for delete."
                if pk_value is None:
                    return "Error: 'pk_value' is required for delete."
                deleted = self.shim.delete(self.txn, table, pk_value)
                if deleted:
                    return f"Row deleted from '{table}' (pk='{pk_value}')."
                return f"No row found to delete in '{table}' (pk='{pk_value}')."
            if command == "list_tables":
                tables = sorted(self.shim._tables.keys())
                return "\n".join(tables) if tables else "No tables registered."
            if command == "sql":
                if not sql:
                    return "Error: 'sql' is required for sql command."
                out = self.shim.execute_sql(self.txn, sql, params)
                return str(out)
            return f"Error: Unknown command '{command}'."
        except Exception as e:
            return f"Error: {e}"


@pytest.fixture
def tmp_workspace(tmp_path: Path) -> Path:
    """Create a temporary workspace directory with some sample files."""
    # Create directory structure
    src = tmp_path / "src"
    src.mkdir()
    (src / "main.py").write_text("def main():\n    print('hello')\n")
    (src / "utils.py").write_text(
        "def add(a, b):\n    return a + b\n\n"
        "def subtract(a, b):\n    return a - b\n"
    )
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_main.py").write_text(
        "from src.main import main\n\ndef test_main():\n    main()\n"
    )

    # Binary file
    (tmp_path / "image.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)

    # .gitignore
    (tmp_path / ".gitignore").write_text("__pycache__/\n*.pyc\nnode_modules/\n")

    # CLAUDE.md
    (tmp_path / "CLAUDE.md").write_text(
        "# Project Instructions\n\n"
        "- Use pytest for testing\n"
        "- Follow PEP 8\n"
    )

    return tmp_path


@pytest.fixture
def config() -> Config:
    """Default configuration for tests."""
    return Config()


@pytest.fixture
def memory_dir(tmp_workspace: Path) -> Path:
    """Create and return the .janus-code/memory directory."""
    mem = tmp_workspace / ".janus-code" / "memory"
    mem.mkdir(parents=True)
    return mem


@pytest.fixture
def janus_context(tmp_workspace: Path):
    """JanusContext for Janus integration tests.

    Skips automatically if langchain_janus is unavailable or if the system
    cannot mount an overlay (no fuse-overlayfs and no root).
    """
    langchain_janus = pytest.importorskip("langchain_janus")
    JanusContext = langchain_janus.JanusContext
    ctx = None
    try:
        ctx = JanusContext(str(tmp_workspace), enable_sqlite=True, enable_vectorstore=False)
    except Exception as e:
        pytest.skip(f"JanusContext unavailable: {e}")
    yield ctx
    # Cleanup: abort any active transaction
    try:
        if ctx is not None and ctx.is_active:
            ctx.abort()
    except Exception:
        pass


@pytest.fixture(autouse=True)
def patch_session_manager_default_tar(monkeypatch: pytest.MonkeyPatch):
    """Inject a mock JanusContext when tests construct SessionManager without one."""
    from janus_code.janus_integration import session_manager as sm_mod

    original_init = sm_mod.SessionManager.__init__

    def patched_init(self, project_path, config, janus_context=None, db_path=":memory:"):
        effective_janus = janus_context or MockJanusContext(Path(project_path))
        return original_init(
            self,
            project_path=project_path,
            config=config,
            janus_context=effective_janus,
            db_path=db_path,
        )

    monkeypatch.setattr(sm_mod.SessionManager, "__init__", patched_init)
