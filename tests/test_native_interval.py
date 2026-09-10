import sqlite3
import pytest

import chronos_core._native_interval as native_interval
from chronos_core.branching import ChronosBranchContext, MergeResolution


def _connect(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _create_blocks(conn):
    conn.execute(
        """
        CREATE TABLE blocks (
            inode_id INTEGER NOT NULL,
            block_index INTEGER NOT NULL,
            data BLOB,
            valid_length INTEGER,
            live_lo INTEGER NOT NULL,
            live_hi INTEGER NOT NULL,
            writer_segment_id INTEGER NOT NULL,
            deleted INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        "CREATE INDEX blocks_visible_idx ON blocks (inode_id, block_index, live_lo, live_hi)"
    )


def _rows(conn):
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT inode_id, block_index, data, valid_length, live_lo, live_hi, writer_segment_id, deleted
            FROM blocks
            ORDER BY inode_id, block_index, live_lo, live_hi
            """
        )
    ]


def test_native_connectors_smoke(tmp_path):
    db_path = tmp_path / "connector.sqlite"

    connector = native_interval.SQLiteConnector(str(db_path))
    connector.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
    connector.execute("INSERT INTO t VALUES (1, 'ok')")

    assert connector.query("SELECT id, value FROM t") == [{"id": 1, "value": "ok"}]
    assert native_interval.sqlite3_libversion()
    assert native_interval.libpq_version() > 0
    assert native_interval.recommended_sql_parser()["name"] == "libpg_query"
    assert native_interval.supports_connection_dialect("sqlite") is True
    assert native_interval.supports_connection_dialect("postgres") is True


def test_native_sqlite_connection_uses_chronos_wal_checkpoint_default(tmp_path, monkeypatch):
    monkeypatch.delenv("CHRONOS_NATIVE_SQLITE_WAL_AUTOCHECKPOINT_PAGES", raising=False)
    db_path = tmp_path / "wal_default.sqlite"
    sqlite3.connect(db_path).close()

    conn = native_interval.NativeSqlConnection(f"sqlite:///{db_path}")

    assert conn.query_sql("PRAGMA wal_autocheckpoint") == [[16384]]


def test_sqlite_adapter_uses_chronos_wal_checkpoint_default(tmp_path):
    db_path = tmp_path / "adapter_wal_default.sqlite"
    ctx = ChronosBranchContext.connect(f"sqlite:///{db_path}", backend="interval")
    try:
        assert ctx.db.execute("PRAGMA wal_autocheckpoint").fetchone()[0] == 16384
    finally:
        ctx.close()


def test_sqlite_interval_bulk_upsert_splits_overlapping_rows(tmp_path):
    db_path = tmp_path / "blocks.sqlite"
    with _connect(db_path) as conn:
        _create_blocks(conn)
        conn.execute(
            """
            INSERT INTO blocks
            VALUES (1, 0, ?, 3, 0, 100, 7, 0)
            """,
            (b"old",),
        )

    with _connect(db_path) as conn:
        stats = native_interval.interval_bulk_upsert_connection(
            "sqlite",
            conn,
            "blocks",
            ["inode_id", "block_index", "data", "valid_length"],
            ["inode_id", "block_index"],
            [{"inode_id": 1, "block_index": 0, "data": b"new", "valid_length": 3}],
            40,
            60,
            8,
            True,
        )

    with _connect(db_path) as conn:
        rows = _rows(conn)

    assert stats["strategy"] == "native_interval_bulk_upsert"
    assert stats["input_rows"] == 1
    assert stats["selected_rows"] == 1
    assert stats["deleted_rows"] == 1
    assert stats["inserted_rows"] == 3
    assert rows == [
        {
            "inode_id": 1,
            "block_index": 0,
            "data": b"old",
            "valid_length": 3,
            "live_lo": 0,
            "live_hi": 40,
            "writer_segment_id": 7,
            "deleted": 0,
        },
        {
            "inode_id": 1,
            "block_index": 0,
            "data": b"new",
            "valid_length": 3,
            "live_lo": 40,
            "live_hi": 60,
            "writer_segment_id": 8,
            "deleted": 0,
        },
        {
            "inode_id": 1,
            "block_index": 0,
            "data": b"old",
            "valid_length": 3,
            "live_lo": 60,
            "live_hi": 100,
            "writer_segment_id": 7,
            "deleted": 0,
        },
    ]


def test_sqlite_interval_bulk_upsert_batches_direct_and_overlapping_rows(tmp_path):
    db_path = tmp_path / "batch.sqlite"
    with _connect(db_path) as conn:
        _create_blocks(conn)
        conn.executemany(
            """
            INSERT INTO blocks
            VALUES (?, ?, ?, ?, ?, ?, ?, 0)
            """,
            [
                (1, 0, b"parent-0", 8, 0, 100, 1),
                (1, 1, b"parent-1", 8, 0, 100, 1),
            ],
        )

    input_rows = [
        {"inode_id": 1, "block_index": 0, "data": b"child-0", "valid_length": 7},
        {"inode_id": 1, "block_index": 1, "data": b"child-1", "valid_length": 7},
    ]
    input_rows.extend(
        {
            "inode_id": 2,
            "block_index": idx,
            "data": f"new-{idx}".encode(),
            "valid_length": len(f"new-{idx}"),
        }
        for idx in range(64)
    )

    with _connect(db_path) as conn:
        stats = native_interval.interval_bulk_upsert_connection(
            "sqlite",
            conn,
            "blocks",
            ["inode_id", "block_index", "data", "valid_length"],
            ["inode_id", "block_index"],
            input_rows,
            25,
            75,
            2,
            True,
        )

    with _connect(db_path) as conn:
        row_count = conn.execute("SELECT COUNT(*) FROM blocks").fetchone()[0]
        visible_children = [
            dict(row)
            for row in conn.execute(
                """
                SELECT inode_id, block_index, data, live_lo, live_hi, writer_segment_id
                FROM blocks
                WHERE writer_segment_id = 2
                ORDER BY inode_id, block_index
                """
            )
        ]

    assert stats["input_rows"] == 66
    assert stats["selected_rows"] == 2
    assert stats["deleted_rows"] == 2
    assert stats["strategy"] == "native_interval_bulk_upsert"
    assert stats["inserted_rows"] == 70
    assert row_count == 70
    assert visible_children[:2] == [
        {
            "inode_id": 1,
            "block_index": 0,
            "data": b"child-0",
            "live_lo": 25,
            "live_hi": 75,
            "writer_segment_id": 2,
        },
        {
            "inode_id": 1,
            "block_index": 1,
            "data": b"child-1",
            "live_lo": 25,
            "live_hi": 75,
            "writer_segment_id": 2,
        },
    ]
    assert len(visible_children) == 66


def test_native_branch_session_executes_branch_aware_sql(tmp_path):
    db_path = tmp_path / "branch_sql.sqlite"
    ctx = ChronosBranchContext.connect(f"sqlite:///{db_path}", backend="interval")
    try:
        ctx.conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)")
        ctx.conn.execute(
            "INSERT INTO items VALUES (1, 'base', 10), (2, 'other', 20)"
        )
        ctx.conn.commit()
        ctx.register_table("items", ["id"])
        ctx.create_branch("child")

        native_store = native_interval.NativeBranchStore("sqlite", ctx.db.raw_connection)
        native_session = native_store.checkout("child")

        assert native_session.query(
            "SELECT id, name FROM items WHERE qty >= :qty ORDER BY id",
            {"qty": 10},
        ) == [{"id": 1, "name": "base"}, {"id": 2, "name": "other"}]

        assert native_session.execute(
            "INSERT INTO items (id, name, qty) VALUES (:id, :name, :qty)",
            {"id": 3, "name": "child", "qty": 30},
        ) == 1
        assert native_session.execute(
            "UPDATE items SET qty = :qty WHERE id = :id",
            {"qty": 11, "id": 1},
        ) == 1
        assert native_session.execute(
            "DELETE FROM items WHERE id = :id",
            {"id": 2},
        ) == 1

        assert native_session.query(
            "SELECT id, name, qty FROM items ORDER BY id",
        ) == [
            {"id": 1, "name": "base", "qty": 11},
            {"id": 3, "name": "child", "qty": 30},
        ]

        main = native_store.checkout("main")
        assert main.query("SELECT id, name, qty FROM items ORDER BY id") == [
            {"id": 1, "name": "base", "qty": 10},
            {"id": 2, "name": "other", "qty": 20},
        ]
    finally:
        ctx.close()


def test_native_branch_write_guard_is_transaction_scoped(tmp_path):
    db_path = tmp_path / "branch_guard_scope.sqlite"
    ctx = ChronosBranchContext.connect(f"sqlite:///{db_path}", backend="interval")
    try:
        ctx.conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)"
        )
        ctx.conn.execute("INSERT INTO items VALUES (1, 10)")
        ctx.conn.commit()
        ctx.register_table("items", ["id"])
        ctx.create_branch("child")

        native_store = native_interval.NativeBranchStore(
            "sqlite", ctx.db.raw_connection
        )
        child = native_store.checkout("child")
        trace: list[str] = []
        ctx.db.raw_connection.set_trace_callback(trace.append)
        try:
            child.begin()
            child.execute(
                "INSERT INTO items (id, value) VALUES (:id, :value)",
                {"id": 2, "value": 20},
            )
            child.execute(
                "UPDATE items SET value = :value WHERE id = :id",
                {"id": 1, "value": 11},
            )
            child.execute("DELETE FROM items WHERE id = :id", {"id": 2})
            child.commit()

            # Autocommit remains a one-statement scope and therefore takes one
            # fresh guard after the explicit transaction has released its lock.
            child.execute(
                "UPDATE items SET value = :value WHERE id = :id",
                {"id": 1, "value": 12},
            )
        finally:
            ctx.db.raw_connection.set_trace_callback(None)

        guard_selects = [
            sql
            for sql in trace
            if "SELECT branch.current_segment_id, barrier.barrier_id" in sql
        ]
        assert len(guard_selects) == 2
        assert child.query("SELECT value FROM items WHERE id = 1") == [{"value": 12}]
    finally:
        ctx.close()


def test_native_branch_session_supports_null_predicates(tmp_path):
    db_path = tmp_path / "branch_null_predicate.sqlite"
    ctx = ChronosBranchContext.connect(f"sqlite:///{db_path}", backend="interval")
    try:
        ctx.conn.execute(
            "CREATE TABLE customer (c_id INTEGER PRIMARY KEY, c_balance INTEGER)"
        )
        ctx.conn.executemany(
            "INSERT INTO customer VALUES (?, ?)",
            [(1, None), (2, 5), (3, None), (4, 7)],
        )
        ctx.conn.commit()
        ctx.register_table("customer", ["c_id"])
        ctx.create_branch("child")

        native_store = native_interval.NativeBranchStore("sqlite", ctx.db.raw_connection)
        child = native_store.checkout("child")

        assert child.execute(
            "UPDATE customer SET c_balance = 0 WHERE c_balance IS NULL"
        ) == 2
        assert child.query("SELECT c_id, c_balance FROM customer ORDER BY c_id") == [
            {"c_id": 1, "c_balance": 0},
            {"c_id": 2, "c_balance": 5},
            {"c_id": 3, "c_balance": 0},
            {"c_id": 4, "c_balance": 7},
        ]
        assert child.execute(
            "DELETE FROM customer WHERE c_balance IS NOT NULL AND c_balance >= :minimum",
            {"minimum": 7},
        ) == 1
        assert child.query("SELECT c_id, c_balance FROM customer ORDER BY c_id") == [
            {"c_id": 1, "c_balance": 0},
            {"c_id": 2, "c_balance": 5},
            {"c_id": 3, "c_balance": 0},
        ]

        main = native_store.checkout("main")
        assert main.query("SELECT c_id, c_balance FROM customer ORDER BY c_id") == [
            {"c_id": 1, "c_balance": None},
            {"c_id": 2, "c_balance": 5},
            {"c_id": 3, "c_balance": None},
            {"c_id": 4, "c_balance": 7},
        ]
    finally:
        ctx.close()


def test_native_branch_session_updates_with_primary_key_prefix_predicate(tmp_path):
    db_path = tmp_path / "branch_prefix_update.sqlite"
    ctx = ChronosBranchContext.connect(f"sqlite:///{db_path}", backend="interval")
    try:
        ctx.conn.execute(
            """
            CREATE TABLE order_line (
                w_id INTEGER NOT NULL,
                d_id INTEGER NOT NULL,
                o_id INTEGER NOT NULL,
                line_number INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                delivered INTEGER NOT NULL,
                PRIMARY KEY (w_id, d_id, o_id, line_number)
            )
            """
        )
        ctx.conn.executemany(
            "INSERT INTO order_line VALUES (?, ?, ?, ?, ?, 0)",
            [
                (1, 1, 10, 1, 10),
                (1, 1, 10, 2, 20),
                (1, 1, 10, 3, 30),
                (1, 1, 11, 1, 40),
                (1, 2, 10, 1, 50),
                (2, 1, 10, 1, 60),
            ],
        )
        ctx.conn.commit()
        ctx.register_table("order_line", ["w_id", "d_id", "o_id", "line_number"])
        ctx.create_branch("child")

        native_store = native_interval.NativeBranchStore("sqlite", ctx.db.raw_connection)
        child = native_store.checkout("child")
        assert child.execute(
            """
            UPDATE order_line
            SET delivered = :delivered
            WHERE o_id = :o_id AND d_id = :d_id AND w_id = :w_id
            """,
            {"delivered": 1, "o_id": 10, "d_id": 1, "w_id": 1},
        ) == 3

        assert child.query(
            """
            SELECT w_id, d_id, o_id, line_number, delivered
            FROM order_line
            ORDER BY w_id, d_id, o_id, line_number
            """
        ) == [
            {"w_id": 1, "d_id": 1, "o_id": 10, "line_number": 1, "delivered": 1},
            {"w_id": 1, "d_id": 1, "o_id": 10, "line_number": 2, "delivered": 1},
            {"w_id": 1, "d_id": 1, "o_id": 10, "line_number": 3, "delivered": 1},
            {"w_id": 1, "d_id": 1, "o_id": 11, "line_number": 1, "delivered": 0},
            {"w_id": 1, "d_id": 2, "o_id": 10, "line_number": 1, "delivered": 0},
            {"w_id": 2, "d_id": 1, "o_id": 10, "line_number": 1, "delivered": 0},
        ]
        assert native_store.checkout("main").query(
            "SELECT COUNT(*) AS changed FROM order_line WHERE delivered = 1"
        ) == [{"changed": 0}]
    finally:
        ctx.close()


def test_native_branch_store_creates_logical_index(tmp_path):
    db_path = tmp_path / "branch_index.sqlite"
    ctx = ChronosBranchContext.connect(f"sqlite:///{db_path}", backend="interval")
    try:
        ctx.conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, sku TEXT, qty INTEGER)"
        )
        ctx.conn.execute("INSERT INTO items VALUES (1, 'a', 10), (2, 'b', 20)")
        ctx.conn.commit()
        ctx.register_table("items", ["id"])

        native_store = native_interval.NativeBranchStore("sqlite", ctx.db.raw_connection)
        assert native_store.create_index("items", ["qty", "sku"], "items_qty_sku") == "items_qty_sku"

        registry = ctx.db.execute(
            """
            SELECT table_name, columns
            FROM _chronos_branch_indexes
            WHERE backend = 'interval' AND index_name = 'items_qty_sku'
            """
        ).fetchone()
        assert dict(registry) == {
            "table_name": "items",
            "columns": '["qty", "sku"]',
        }

        physical_indexes = {
            row["name"]
            for row in ctx.db.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        }
        assert "_chronos_idx_interval_items_qty_sku" in physical_indexes
    finally:
        ctx.close()


def test_native_branch_store_diffs_branch_visible_rows(tmp_path):
    db_path = tmp_path / "branch_diff.sqlite"
    ctx = ChronosBranchContext.connect(f"sqlite:///{db_path}", backend="interval")
    try:
        ctx.conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, sku TEXT, qty INTEGER)"
        )
        ctx.conn.execute("INSERT INTO items VALUES (1, 'a', 10), (2, 'b', 20)")
        ctx.conn.commit()
        ctx.register_table("items", ["id"])
        ctx.create_branch("work")
        work = ctx.checkout("work")
        work.execute("UPDATE items SET qty = :qty WHERE id = :id", {"id": 1, "qty": 11})
        work.execute("DELETE FROM items WHERE id = :id", {"id": 2})
        work.execute(
            "INSERT INTO items (id, sku, qty) VALUES (:id, :sku, :qty)",
            {"id": 3, "sku": "c", "qty": 30},
        )

        native_store = native_interval.NativeBranchStore("sqlite", ctx.db.raw_connection)
        assert native_store.diff_rows("main", "work", "items") == [
            {
                "table": "items",
                "key": {"id": 1},
                "change": "modified",
                "before": {"id": 1, "sku": "a", "qty": 10},
                "after": {"id": 1, "sku": "a", "qty": 11},
            },
            {
                "table": "items",
                "key": {"id": 2},
                "change": "deleted",
                "before": {"id": 2, "sku": "b", "qty": 20},
                "after": None,
            },
            {
                "table": "items",
                "key": {"id": 3},
                "change": "added",
                "before": None,
                "after": {"id": 3, "sku": "c", "qty": 30},
            },
        ]
    finally:
        ctx.close()


def test_native_branch_store_previews_merge_changes_and_conflicts(tmp_path):
    db_path = tmp_path / "branch_preview.sqlite"
    ctx = ChronosBranchContext.connect(f"sqlite:///{db_path}", backend="interval")
    try:
        ctx.conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, sku TEXT, qty INTEGER)"
        )
        ctx.conn.execute("INSERT INTO items VALUES (1, 'a', 10), (2, 'b', 20)")
        ctx.conn.commit()
        ctx.register_table("items", ["id"])

        ctx.create_branch("agent")
        agent = ctx.checkout("agent")
        agent.execute("UPDATE items SET qty = :qty WHERE id = :id", {"id": 1, "qty": 11})
        agent.execute(
            "INSERT INTO items (id, sku, qty) VALUES (:id, :sku, :qty)",
            {"id": 3, "sku": "c", "qty": 30},
        )
        main = ctx.checkout("main")
        main.execute("UPDATE items SET qty = :qty WHERE id = :id", {"id": 2, "qty": 22})

        native_store = native_interval.NativeBranchStore("sqlite", ctx.db.raw_connection)
        assert native_store.merge_preview("agent", "main") == {
            "changes": [
                {
                    "table": "items",
                    "key": {"id": 1},
                    "change": "modified",
                    "before": {"id": 1, "sku": "a", "qty": 10},
                    "after": {"id": 1, "sku": "a", "qty": 11},
                },
                {
                    "table": "items",
                    "key": {"id": 3},
                    "change": "added",
                    "before": None,
                    "after": {"id": 3, "sku": "c", "qty": 30},
                },
            ],
            "conflicts": [],
        }

        ctx.create_branch("conflict")
        ctx.checkout("conflict").execute(
            "UPDATE items SET qty = :qty WHERE id = :id",
            {"id": 1, "qty": 12},
        )
        agent.execute("UPDATE items SET qty = :qty WHERE id = :id", {"id": 1, "qty": 13})

        preview = native_store.merge_preview("agent", "conflict")
        assert preview["changes"] == [
            {
                "table": "items",
                "key": {"id": 3},
                "change": "added",
                "before": None,
                "after": {"id": 3, "sku": "c", "qty": 30},
            }
        ]
        assert preview["conflicts"] == [
            {
                "table": "items",
                "key": {"id": 1},
                "change": "modified",
                "before": {"id": 1, "sku": "a", "qty": 12},
                "after": {"id": 1, "sku": "a", "qty": 13},
            }
        ]
    finally:
        ctx.close()


def test_interval_backend_applies_explicit_resolution_via_native_store(tmp_path):
    db_path = tmp_path / "resolved_merge.sqlite"
    ctx = ChronosBranchContext.connect(f"sqlite:///{db_path}", backend="interval")
    try:
        ctx.conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, sku TEXT, qty INTEGER)"
        )
        ctx.conn.execute("INSERT INTO items VALUES (1, 'a', 10)")
        ctx.conn.commit()
        ctx.register_table("items", ["id"])

        ctx.create_branch("agent")
        ctx.checkout("agent").execute(
            "UPDATE items SET qty = :qty WHERE id = :id",
            {"id": 1, "qty": 11},
        )
        ctx.checkout("main").execute(
            "UPDATE items SET qty = :qty WHERE id = :id",
            {"id": 1, "qty": 12},
        )

        preview = ctx.merge_preview("agent", "main", policy="manual_review")
        conflict_id = preview.conflicts[0].conflict_id
        assert conflict_id is not None

        result = ctx._backend.merge_apply(
            "agent",
            "main",
            resolution=MergeResolution({conflict_id: "source"}),
        )

        assert result is not None
        assert result.applied == 1
        assert ctx.checkout("main").query(
            "SELECT qty FROM items WHERE id = :id",
            {"id": 1},
        ) == [{"qty": 11}]
    finally:
        ctx.close()


@pytest.mark.skipif(not native_interval.has_duckdb, reason="DuckDB driver disabled in this build")
def test_native_duckdb_sql_connection_executes_data_plane_sql(tmp_path):
    db_path = tmp_path / "branch_sql.duckdb"
    conn = native_interval.NativeSqlConnection(f"duckdb:///{db_path}")

    conn.execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, qty BIGINT)")
    conn.execute_sql("INSERT INTO items VALUES (?, ?)", (1, 10))
    conn.commit()

    assert conn.dialect() == "duckdb"
    assert conn.query_sql_dict("SELECT sum(qty) AS total FROM items") == [{"total": 10}]
    assert conn.query_sql("SELECT id, qty FROM items") == [[1, 10]]
