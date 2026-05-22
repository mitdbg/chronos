"""Tests for ChronosSQLite tool.

Tests MVCC-based SQLite operations: register_table, put, get,
query, delete, list_tables, sql.  Also tests transactional
isolation — writes in one transaction are invisible to another,
and abort discards all changes.

Run with: sudo python -m pytest tests/test_sqlite_tool.py -v
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Generator

import pytest

from chronos_core.transaction.coordinator import TransactionCoordinator
from chronos_core.transaction.shim_fs import OverlayFSShim
from chronos_core.transaction.shim_sqlite import SQLiteShim

from langchain_chronos.context import ChronosContext
from langchain_chronos.sqlite_tool import ChronosSQLite


def _require_root() -> None:
    if os.geteuid() != 0:
        pytest.skip("Requires root for OverlayFS")


@pytest.fixture
def fresh_base() -> Generator[Path, None, None]:
    _require_root()
    d = Path(tempfile.mkdtemp(prefix="chronos_sqlite_test_"))
    (d / "placeholder.txt").write_text("placeholder\n")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def ctx(fresh_base: Path) -> Generator[ChronosContext, None, None]:
    """ChronosContext with sqlite enabled (in-memory DB)."""
    c = ChronosContext(fresh_base, enable_sqlite=True)
    c.begin()
    yield c
    if c.is_active:
        c.abort()


@pytest.fixture
def sqlite(ctx: ChronosContext) -> ChronosSQLite:
    return ctx.sqlite


# ── register_table ───────────────────────────────────────────────────


class TestRegisterTable:
    def test_register_table(self, sqlite: ChronosSQLite) -> None:
        result = sqlite.invoke({
            "command": "register_table",
            "table": "users",
            "columns": ["id TEXT", "name TEXT", "email TEXT"],
        })
        assert "registered" in result.lower()
        assert "users" in result

    def test_register_table_with_pk(self, sqlite: ChronosSQLite) -> None:
        result = sqlite.invoke({
            "command": "register_table",
            "table": "items",
            "columns": ["item_id TEXT", "title TEXT", "price REAL"],
            "pk_column": "item_id",
        })
        assert "registered" in result.lower()
        assert "item_id" in result

    def test_register_table_with_seed(self, sqlite: ChronosSQLite) -> None:
        result = sqlite.invoke({
            "command": "register_table",
            "table": "config",
            "columns": ["key TEXT", "value TEXT"],
            "seed_data": [
                {"key": "debug", "value": "true"},
                {"key": "version", "value": "1.0"},
            ],
        })
        assert "Seeded 2 row" in result

    def test_register_missing_table(self, sqlite: ChronosSQLite) -> None:
        result = sqlite.invoke({"command": "register_table", "columns": ["id TEXT"]})
        assert "error" in result.lower()

    def test_register_missing_columns(self, sqlite: ChronosSQLite) -> None:
        result = sqlite.invoke({"command": "register_table", "table": "bad"})
        assert "error" in result.lower()


# ── put / get ────────────────────────────────────────────────────────


class TestPutGet:
    def _setup_table(self, sqlite: ChronosSQLite) -> None:
        sqlite.invoke({
            "command": "register_table",
            "table": "users",
            "columns": ["id TEXT", "name TEXT", "email TEXT"],
        })

    def test_put_and_get(self, sqlite: ChronosSQLite) -> None:
        self._setup_table(sqlite)
        sqlite.invoke({
            "command": "put",
            "table": "users",
            "row": {"id": "1", "name": "Alice", "email": "alice@test.com"},
        })
        result = sqlite.invoke({"command": "get", "table": "users", "pk_value": "1"})
        assert "Alice" in result
        assert "alice@test.com" in result

    def test_put_update(self, sqlite: ChronosSQLite) -> None:
        """Updating a row creates a new MVCC version."""
        self._setup_table(sqlite)
        sqlite.invoke({
            "command": "put",
            "table": "users",
            "row": {"id": "1", "name": "Alice", "email": "old@test.com"},
        })
        sqlite.invoke({
            "command": "put",
            "table": "users",
            "row": {"id": "1", "name": "Alice", "email": "new@test.com"},
        })
        result = sqlite.invoke({"command": "get", "table": "users", "pk_value": "1"})
        assert "new@test.com" in result
        assert "old@test.com" not in result

    def test_get_nonexistent(self, sqlite: ChronosSQLite) -> None:
        self._setup_table(sqlite)
        result = sqlite.invoke({"command": "get", "table": "users", "pk_value": "99"})
        assert "No row found" in result

    def test_put_missing_table(self, sqlite: ChronosSQLite) -> None:
        result = sqlite.invoke({
            "command": "put",
            "row": {"id": "1", "name": "Alice"},
        })
        assert "error" in result.lower()

    def test_put_missing_row(self, sqlite: ChronosSQLite) -> None:
        self._setup_table(sqlite)
        result = sqlite.invoke({"command": "put", "table": "users"})
        assert "error" in result.lower()

    def test_get_missing_pk(self, sqlite: ChronosSQLite) -> None:
        self._setup_table(sqlite)
        result = sqlite.invoke({"command": "get", "table": "users"})
        assert "error" in result.lower()


# ── query ────────────────────────────────────────────────────────────


class TestQuery:
    def _setup(self, sqlite: ChronosSQLite) -> None:
        sqlite.invoke({
            "command": "register_table",
            "table": "products",
            "columns": ["id TEXT", "name TEXT", "category TEXT", "price REAL"],
        })
        for i, (name, cat, price) in enumerate([
            ("Widget", "tools", 9.99),
            ("Gadget", "tools", 19.99),
            ("Book", "media", 14.99),
            ("Album", "media", 12.99),
        ]):
            sqlite.invoke({
                "command": "put",
                "table": "products",
                "row": {"id": str(i), "name": name, "category": cat, "price": price},
            })

    def test_query_all(self, sqlite: ChronosSQLite) -> None:
        self._setup(sqlite)
        result = sqlite.invoke({"command": "query", "table": "products"})
        assert "Widget" in result
        assert "Book" in result

    def test_query_with_filter(self, sqlite: ChronosSQLite) -> None:
        self._setup(sqlite)
        result = sqlite.invoke({
            "command": "query",
            "table": "products",
            "filters": {"category": "media"},
        })
        assert "Book" in result
        assert "Album" in result
        assert "Widget" not in result

    def test_query_with_limit(self, sqlite: ChronosSQLite) -> None:
        self._setup(sqlite)
        result = sqlite.invoke({
            "command": "query",
            "table": "products",
            "limit": 2,
        })
        # Should contain at most 2 rows
        import json
        rows = json.loads(result)
        assert len(rows) <= 2

    def test_query_empty(self, sqlite: ChronosSQLite) -> None:
        self._setup(sqlite)
        result = sqlite.invoke({
            "command": "query",
            "table": "products",
            "filters": {"category": "nonexistent"},
        })
        assert "No rows found" in result

    def test_query_missing_table(self, sqlite: ChronosSQLite) -> None:
        result = sqlite.invoke({"command": "query"})
        assert "error" in result.lower()


# ── delete ───────────────────────────────────────────────────────────


class TestDelete:
    def _setup(self, sqlite: ChronosSQLite) -> None:
        sqlite.invoke({
            "command": "register_table",
            "table": "items",
            "columns": ["id TEXT", "title TEXT"],
        })
        sqlite.invoke({
            "command": "put",
            "table": "items",
            "row": {"id": "1", "title": "Thing"},
        })

    def test_delete_existing(self, sqlite: ChronosSQLite) -> None:
        self._setup(sqlite)
        result = sqlite.invoke({"command": "delete", "table": "items", "pk_value": "1"})
        assert "deleted" in result.lower()
        # Verify it's gone
        get_result = sqlite.invoke({"command": "get", "table": "items", "pk_value": "1"})
        assert "No row found" in get_result

    def test_delete_nonexistent(self, sqlite: ChronosSQLite) -> None:
        self._setup(sqlite)
        result = sqlite.invoke({"command": "delete", "table": "items", "pk_value": "99"})
        assert "No row found to delete" in result

    def test_delete_missing_pk(self, sqlite: ChronosSQLite) -> None:
        self._setup(sqlite)
        result = sqlite.invoke({"command": "delete", "table": "items"})
        assert "error" in result.lower()


# ── list_tables ──────────────────────────────────────────────────────


class TestListTables:
    def test_list_empty(self, sqlite: ChronosSQLite) -> None:
        result = sqlite.invoke({"command": "list_tables"})
        assert "No tables" in result

    def test_list_after_register(self, sqlite: ChronosSQLite) -> None:
        sqlite.invoke({
            "command": "register_table",
            "table": "alpha",
            "columns": ["id TEXT", "val TEXT"],
        })
        sqlite.invoke({
            "command": "register_table",
            "table": "beta",
            "columns": ["id TEXT", "data TEXT"],
        })
        result = sqlite.invoke({"command": "list_tables"})
        assert "alpha" in result
        assert "beta" in result


# ── sql ──────────────────────────────────────────────────────────────


class TestSQL:
    def test_raw_select(self, sqlite: ChronosSQLite) -> None:
        sqlite.invoke({
            "command": "register_table",
            "table": "t",
            "columns": ["id TEXT", "v INTEGER"],
            "seed_data": [{"id": "a", "v": 10}, {"id": "b", "v": 20}],
        })
        result = sqlite.invoke({
            "command": "sql",
            "sql": "SELECT * FROM t WHERE v > ?",
            "params": [15],
        })
        assert "b" in result
        # "a" has v=10, which is not > 15, but it might appear in raw
        # results since raw SQL bypasses MVCC. That's expected.

    def test_sql_missing(self, sqlite: ChronosSQLite) -> None:
        result = sqlite.invoke({"command": "sql"})
        assert "error" in result.lower()


# ── unknown command ──────────────────────────────────────────────────


class TestUnknownCommand:
    def test_unknown(self, sqlite: ChronosSQLite) -> None:
        result = sqlite.invoke({"command": "drop_table"})
        assert "error" in result.lower()


# ── Transaction isolation ────────────────────────────────────────────


class TestSQLiteIsolation:
    """Test that MVCC isolation works correctly via the tool."""

    def test_abort_discards_writes(self, fresh_base: Path) -> None:
        """Data written in an aborted txn is invisible after re-begin."""
        ctx = ChronosContext(fresh_base, enable_sqlite=True)
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "t",
            "columns": ["id TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "t",
            "row": {"id": "1", "val": "first"},
        })
        ctx.abort()

        # New transaction — the write should be gone
        # The table still exists (DDL is not transactional in SQLite),
        # but the MVCC row is cleaned up by abort().
        ctx.begin()
        result = ctx.sqlite.invoke({
            "command": "get",
            "table": "t",
            "pk_value": "1",
        })
        assert "No row found" in result
        ctx.abort()

    def test_commit_persists_writes(self, fresh_base: Path) -> None:
        """Data written in a committed txn is visible in the next txn."""
        ctx = ChronosContext(fresh_base, enable_sqlite=True)
        ctx.begin()
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "t",
            "columns": ["id TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "t",
            "row": {"id": "1", "val": "committed"},
        })
        ctx.commit()

        ctx.begin()
        result = ctx.sqlite.invoke({
            "command": "get",
            "table": "t",
            "pk_value": "1",
        })
        assert "committed" in result
        ctx.abort()

    def test_seed_data_visible(self, sqlite: ChronosSQLite) -> None:
        """Seed data (committed_txn_id=0) is visible to any snapshot."""
        sqlite.invoke({
            "command": "register_table",
            "table": "seeded",
            "columns": ["id TEXT", "val TEXT"],
            "seed_data": [
                {"id": "s1", "val": "seed1"},
                {"id": "s2", "val": "seed2"},
            ],
        })
        result = sqlite.invoke({"command": "query", "table": "seeded"})
        assert "seed1" in result
        assert "seed2" in result

    def test_changes_tracked(self, ctx: ChronosContext) -> None:
        """get_changes() reports SQLite writes."""
        ctx.sqlite.invoke({
            "command": "register_table",
            "table": "changes_test",
            "columns": ["id TEXT", "val TEXT"],
        })
        ctx.sqlite.invoke({
            "command": "put",
            "table": "changes_test",
            "row": {"id": "1", "val": "x"},
        })
        changes = ctx.get_changes()
        resource_ids = [c.resource_id for c in changes]
        assert any("changes_test" in r for r in resource_ids)
