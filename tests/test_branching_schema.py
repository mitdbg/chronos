from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import json
from pathlib import Path

import pytest

from chronos_core.branching import (
    BranchAlreadyExistsError,
    ChronosBranchContext,
    TableNotRegisteredError,
    UnsupportedSQLError,
)
from chronos_core.workspace import ChronosFilesystemStore, ChronosWorkspaceContext

from test_branching import _postgres_dsn, _reset_postgres_schema
from test_workspace_filesystem import _require_fuse_overlayfs


SQL_BACKENDS = ("sqlite", "postgres")


def _ctx(
    sql_backend: str = "sqlite",
    *,
    enable_schema_branching: bool,
    branch_backend: str = "interval",
) -> ChronosBranchContext:
    if sql_backend == "postgres":
        _reset_postgres_schema()
        dsn = _postgres_dsn()
    else:
        dsn = "sqlite:///:memory:"
    ctx = ChronosBranchContext.connect(
        dsn,
        backend=branch_backend,
        enable_schema_branching=enable_schema_branching,
    )
    ctx.db.execute("CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)")
    ctx.db.executemany(
        "INSERT INTO products VALUES (?, ?, ?)",
        [("abc", "Alpha", 10), ("def", "Delta", 20)],
    )
    ctx.db.commit()
    ctx.register_table("products", ["sku"])
    ctx._test_sql_backend = sql_backend  # type: ignore[attr-defined]
    return ctx


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
@pytest.mark.parametrize("branch_backend", ["interval", "copy"])
def test_schema_changes_rejected_when_feature_switch_off(
    sql_backend: str, branch_backend: str
) -> None:
    ctx = _ctx(
        sql_backend,
        enable_schema_branching=False,
        branch_backend=branch_backend,
    )
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        with pytest.raises(UnsupportedSQLError):
            exp.execute("ALTER TABLE products ADD COLUMN score INTEGER")
        with pytest.raises(UnsupportedSQLError):
            exp.execute("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)")
        with pytest.raises(UnsupportedSQLError):
            exp.execute("DROP TABLE products")
    finally:
        ctx.close()


def test_schema_changes_rejected_by_default() -> None:
    ctx = ChronosBranchContext.connect("sqlite:///:memory:", backend="interval")
    try:
        ctx.db.execute("CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT)")
        ctx.db.execute("INSERT INTO products VALUES (?, ?)", ("abc", "Alpha"))
        ctx.db.commit()
        ctx.register_table("products", ["sku"])
        ctx.create_branch("exp", from_branch="main")

        exp = ctx.checkout("exp")
        with pytest.raises(UnsupportedSQLError):
            exp.execute("ALTER TABLE products ADD COLUMN score INTEGER")
    finally:
        ctx.close()


def test_schema_branching_switch_rejected_for_unsupported_backend() -> None:
    with pytest.raises(ValueError):
        ChronosBranchContext.connect(
            "sqlite:///:memory:",
            backend="log",
            enable_schema_branching=True,
        )

@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_alter_add_column_is_branch_local_and_interval_branchable(sql_backend: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN score INTEGER")

        assert exp.query("SELECT sku, price, score FROM products ORDER BY sku") == [
            {"sku": "abc", "price": 10, "score": None},
            {"sku": "def", "price": 20, "score": None},
        ]
        with pytest.raises(Exception):
            ctx.checkout("main").query("SELECT score FROM products")

        exp.execute(
            "UPDATE products SET score = :score WHERE sku = :sku",
            {"sku": "abc", "score": 99},
        )
        ctx.create_branch("child", from_branch="exp")
        child = ctx.checkout("child")

        assert child.query("SELECT sku, score FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"sku": "abc", "score": 99}
        ]
        child.execute(
            "UPDATE products SET score = :score WHERE sku = :sku",
            {"sku": "abc", "score": 77},
        )

        assert exp.query("SELECT sku, score FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"sku": "abc", "score": 99}
        ]
        assert child.query("SELECT sku, score FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"sku": "abc", "score": 77}
        ]
        assert ctx.checkout("main").query("SELECT sku, price FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"sku": "abc", "price": 10}
        ]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_private_schema_full_table_update_remains_branchable(sql_backend: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN tier TEXT DEFAULT 'base'")
        assert exp.execute(
            """
            UPDATE products SET
              tier = CASE WHEN price > 15 THEN 'high' ELSE 'low' END
            """
        ).rowcount == 2

        ctx.create_branch("child", from_branch="exp")
        child = ctx.checkout("child")
        exp.execute("UPDATE products SET tier = 'parent' WHERE sku = 'abc'")

        assert child.query("SELECT sku, tier FROM products ORDER BY sku") == [
            {"sku": "abc", "tier": "low"},
            {"sku": "def", "tier": "high"},
        ]
        assert exp.query("SELECT sku, tier FROM products ORDER BY sku") == [
            {"sku": "abc", "tier": "parent"},
            {"sku": "def", "tier": "high"},
        ]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_interval_schema_copy_preserves_writer_provenance(sql_backend: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN score INTEGER DEFAULT 5")

        exp_segment_id = int(ctx.get_branch("exp").current_ref)
        exp_segment = ctx.db.execute(
            """
            SELECT branch_point
            FROM _chronos_branch_interval_segments
            WHERE segment_id = ?
            """,
            (exp_segment_id,),
        ).fetchone()
        binding = ctx.db.execute(
            """
            SELECT v.physical_table
            FROM _chronos_branch_table_bindings b
            JOIN _chronos_branch_table_schema_versions v
              ON v.backend = b.backend
             AND v.schema_version_id = b.schema_version_id
            WHERE b.backend = 'interval'
              AND b.table_name = 'products'
              AND b.tombstone = 0
              AND b.live_lo <= ?
              AND ? < b.live_hi
            """,
            (exp_segment["branch_point"], exp_segment["branch_point"]),
        ).fetchone()
        assert binding is not None
        physical = '"' + binding["physical_table"].replace('"', '""') + '"'

        copied = ctx.db.execute(
            f"""
            SELECT sku, score, writer_segment_id
            FROM {physical}
            ORDER BY sku
            """
        ).fetchall()
        assert [(row["sku"], row["score"]) for row in copied] == [
            ("abc", 5),
            ("def", 5),
        ]
        assert {int(row["writer_segment_id"]) for row in copied} == {1}

        exp.execute("UPDATE products SET score = 9 WHERE sku = 'abc'")
        rows = ctx.db.execute(
            f"""
            SELECT sku, score, writer_segment_id
            FROM {physical}
            WHERE deleted = FALSE
            ORDER BY sku
            """
        ).fetchall()
        assert [(row["sku"], row["score"], int(row["writer_segment_id"])) for row in rows] == [
            ("abc", 9, exp_segment_id),
            ("def", 5, 1),
        ]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_full_table_update_batch_splice_preserves_child_snapshot(sql_backend: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        assert exp.execute("UPDATE products SET price = price + 100").rowcount == 2

        ctx.create_branch("child", from_branch="exp")
        child = ctx.checkout("child")
        assert exp.execute("UPDATE products SET price = price + 10").rowcount == 2

        assert child.query("SELECT sku, price FROM products ORDER BY sku") == [
            {"sku": "abc", "price": 110},
            {"sku": "def", "price": 120},
        ]
        assert exp.query("SELECT sku, price FROM products ORDER BY sku") == [
            {"sku": "abc", "price": 120},
            {"sku": "def", "price": 130},
        ]
        assert ctx.checkout("main").query("SELECT sku, price FROM products ORDER BY sku") == [
            {"sku": "abc", "price": 10},
            {"sku": "def", "price": 20},
        ]
    finally:
        ctx.close()


def test_postgres_full_table_update_batch_splice_handles_overlap_and_params() -> None:
    _reset_postgres_schema()
    dsn = _postgres_dsn()
    ctx = ChronosBranchContext.connect(
        dsn,
        backend="interval",
        enable_schema_branching=True,
    )
    try:
        ctx.db.execute(
            """
            CREATE TABLE accounts (
              id INTEGER PRIMARY KEY,
              balance INTEGER,
              label TEXT
            )
            """
        )
        ctx.db.executemany(
            "INSERT INTO accounts VALUES (?, ?, ?)",
            [(1, 100, "base"), (2, 200, "base"), (3, 300, "base")],
        )
        ctx.db.commit()
        ctx.register_table("accounts", ["id"])

        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        assert exp.execute(
            "UPDATE accounts SET balance = balance + :delta, label = 'first'",
            {"delta": 10},
        ).rowcount == 3

        ctx.create_branch("child", from_branch="exp")
        child = ctx.checkout("child")
        child.execute("UPDATE accounts SET balance = balance + 1 WHERE id = 1")

        assert exp.execute(
            """
            UPDATE accounts SET
              balance = balance * 2,
              label = CASE WHEN balance >= 200 THEN 'large' ELSE 'small' END
            """
        ).rowcount == 3

        assert child.query("SELECT id, balance, label FROM accounts ORDER BY id") == [
            {"id": 1, "balance": 111, "label": "first"},
            {"id": 2, "balance": 210, "label": "first"},
            {"id": 3, "balance": 310, "label": "first"},
        ]
        assert exp.query("SELECT id, balance, label FROM accounts ORDER BY id") == [
            {"id": 1, "balance": 220, "label": "small"},
            {"id": 2, "balance": 420, "label": "large"},
            {"id": 3, "balance": 620, "label": "large"},
        ]
        assert ctx.checkout("main").query(
            "SELECT id, balance, label FROM accounts ORDER BY id"
        ) == [
            {"id": 1, "balance": 100, "label": "base"},
            {"id": 2, "balance": 200, "label": "base"},
            {"id": 3, "balance": 300, "label": "base"},
        ]
    finally:
        ctx.close()


@pytest.mark.parametrize("branch_backend", ["interval", "copy"])
def test_postgres_decimal_arithmetic_update_preserves_numeric_type(
    branch_backend: str,
) -> None:
    _reset_postgres_schema()
    dsn = _postgres_dsn()
    ctx = ChronosBranchContext.connect(
        dsn,
        backend=branch_backend,
        enable_schema_branching=True,
    )
    try:
        ctx.db.execute(
            """
            CREATE TABLE customer (
              id INTEGER PRIMARY KEY,
              balance DECIMAL(10,2)
            )
            """
        )
        ctx.db.execute("INSERT INTO customer VALUES (1, 100.00)")
        ctx.db.commit()
        ctx.register_table("customer", ["id"])

        ctx.create_branch("repro", from_branch="main")
        repro = ctx.checkout("repro")
        assert repro.execute(
            "UPDATE customer SET balance = balance - 72.50 WHERE id = 1"
        ).rowcount == 1
        assert repro.execute(
            "UPDATE customer SET balance = balance + :delta WHERE id = 1",
            {"delta": 2.25},
        ).rowcount == 1

        assert repro.query("SELECT balance FROM customer WHERE id = 1") == [
            {"balance": Decimal("29.75")}
        ]
        assert ctx.checkout("main").query("SELECT balance FROM customer WHERE id = 1") == [
            {"balance": Decimal("100.00")}
        ]
    finally:
        ctx.close()


def test_postgres_concurrent_branch_local_schema_changes_are_serialized() -> None:
    _reset_postgres_schema()
    dsn = _postgres_dsn()
    ctx = ChronosBranchContext.connect(
        dsn,
        backend="interval",
        enable_schema_branching=True,
    )
    try:
        ctx.db.execute("CREATE TABLE products (sku TEXT PRIMARY KEY, price INTEGER)")
        ctx.db.executemany(
            "INSERT INTO products VALUES (?, ?)",
            [(f"sku{i}", i) for i in range(20)],
        )
        ctx.db.commit()
        ctx.register_table("products", ["sku"])
        for branch in ("b0", "b1", "b2", "b3", "b4"):
            ctx.create_branch(branch, from_branch="main")
    finally:
        ctx.close()

    def alter_branch(branch: str) -> list[dict[str, object]]:
        local = ChronosBranchContext.connect(
            dsn,
            backend="interval",
            enable_schema_branching=True,
        )
        try:
            session = local.checkout(branch)
            column = f"score_{branch}"
            session.execute(
                f"ALTER TABLE products ADD COLUMN {column} INTEGER DEFAULT 0"
            )
            session.execute(f"UPDATE products SET {column} = price")
            return session.query(
                f"SELECT COUNT(*) AS count, SUM({column}) AS total FROM products"
            )
        finally:
            local.close()

    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(alter_branch, ("b0", "b1", "b2", "b3", "b4")))

    assert results == [[{"count": 20, "total": 190}]] * 5


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_interval_update_case_expression_for_macrobench_backfill(sql_backend: str) -> None:
    if sql_backend == "postgres":
        _reset_postgres_schema()
        dsn = _postgres_dsn()
    else:
        dsn = "sqlite:///:memory:"
    ctx = ChronosBranchContext.connect(
        dsn,
        backend="interval",
        enable_schema_branching=True,
    )
    try:
        ctx.db.execute(
            """
            CREATE TABLE customer (
              c_id INTEGER PRIMARY KEY,
              c_ytd_payment INTEGER,
              c_credit_lim INTEGER
            )
            """
        )
        ctx.db.executemany(
            "INSERT INTO customer VALUES (?, ?, ?)",
            [(1, 9500, 1000), (2, 7000, 2000), (3, 100, 3000)],
        )
        ctx.db.commit()
        ctx.register_table("customer", ["c_id"])

        ctx.create_branch("dev", from_branch="main")
        dev = ctx.checkout("dev")
        dev.execute("ALTER TABLE customer ADD COLUMN loyalty_tier_t0_s1 VARCHAR(8)")
        dev.execute("ALTER TABLE customer ADD COLUMN credit_lim_t0_s1 INTEGER")
        result = dev.execute(
            """
            UPDATE customer SET
              loyalty_tier_t0_s1 = CASE
                WHEN c_ytd_payment > 9000 THEN 'Gold'
                WHEN c_ytd_payment > 5000 THEN 'Silver'
                ELSE 'Bronze'
              END,
              credit_lim_t0_s1 = c_credit_lim
            """
        )

        assert result.rowcount == 3
        assert dev.query(
            """
            SELECT c_id, loyalty_tier_t0_s1, credit_lim_t0_s1
            FROM customer
            ORDER BY c_id
            """
        ) == [
            {"c_id": 1, "loyalty_tier_t0_s1": "Gold", "credit_lim_t0_s1": 1000},
            {"c_id": 2, "loyalty_tier_t0_s1": "Silver", "credit_lim_t0_s1": 2000},
            {"c_id": 3, "loyalty_tier_t0_s1": "Bronze", "credit_lim_t0_s1": 3000},
        ]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_interval_insert_current_timestamp_on_conflict_do_nothing(sql_backend: str) -> None:
    if sql_backend == "postgres":
        _reset_postgres_schema()
        dsn = _postgres_dsn()
    else:
        dsn = "sqlite:///:memory:"
    ctx = ChronosBranchContext.connect(
        dsn,
        backend="interval",
        enable_schema_branching=True,
    )
    try:
        ctx.db.execute(
            """
            CREATE TABLE orders (
              o_id INTEGER PRIMARY KEY,
              o_entry_d TIMESTAMP,
              status TEXT
            )
            """
        )
        ctx.db.commit()
        ctx.register_table("orders", ["o_id"])

        ctx.create_branch("replay", from_branch="main")
        replay = ctx.checkout("replay")
        first = replay.execute(
            """
            INSERT INTO orders (o_id, o_entry_d, status)
            VALUES (1, CURRENT_TIMESTAMP, 'first')
            ON CONFLICT DO NOTHING
            """
        )
        duplicate = replay.execute(
            """
            INSERT INTO orders (o_id, o_entry_d, status)
            VALUES (1, CURRENT_TIMESTAMP, 'duplicate')
            ON CONFLICT DO NOTHING
            """
        )
        mixed = replay.execute(
            """
            INSERT INTO orders (o_id, o_entry_d, status)
            VALUES
              (2, CURRENT_TIMESTAMP, 'second'),
              (2, CURRENT_TIMESTAMP, 'second-duplicate')
            ON CONFLICT DO NOTHING
            """
        )

        assert first.rowcount == 1
        assert duplicate.rowcount == 0
        assert mixed.rowcount == 1
        rows = replay.query("SELECT o_id, o_entry_d, status FROM orders ORDER BY o_id")
        assert [(row["o_id"], row["status"]) for row in rows] == [
            (1, "first"),
            (2, "second"),
        ]
        assert rows[0]["o_entry_d"] is not None
        assert rows[1]["o_entry_d"] is not None
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_original_branch_dml_continues_after_sibling_schema_diverges(sql_backend: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        ctx.checkout("exp").execute("ALTER TABLE products ADD COLUMN score INTEGER")

        main = ctx.checkout("main")
        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        assert main.query("SELECT sku, price FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"sku": "abc", "price": 11}
        ]
        assert ctx.checkout("exp").query(
            "SELECT sku, price, score FROM products WHERE sku = :sku",
            {"sku": "abc"},
        ) == [{"sku": "abc", "price": 10, "score": None}]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_sibling_branch_does_not_see_later_parent_schema_change(sql_backend: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("sibling", from_branch="main")
        ctx.checkout("main").execute("ALTER TABLE products ADD COLUMN score INTEGER")

        main = ctx.checkout("main")
        sibling = ctx.checkout("sibling")

        assert main.query("SELECT score FROM products ORDER BY sku") == [
            {"score": None},
            {"score": None},
        ]
        with pytest.raises(Exception):
            sibling.query("SELECT score FROM products")
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_create_table_is_branch_local_and_forkable(sql_backend: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)")
        exp.execute(
            "INSERT INTO notes (id, body) VALUES (:id, :body)",
            {"id": "n1", "body": "branch note"},
        )

        with pytest.raises(TableNotRegisteredError):
            ctx.checkout("main").query("SELECT * FROM notes")

        ctx.create_branch("child", from_branch="exp")
        child = ctx.checkout("child")
        assert child.query("SELECT * FROM notes") == [{"id": "n1", "body": "branch note"}]
        child.execute(
            "INSERT INTO notes (id, body) VALUES (:id, :body)",
            {"id": "n2", "body": "child note"},
        )
        assert exp.query("SELECT * FROM notes ORDER BY id") == [
            {"id": "n1", "body": "branch note"}
        ]
        assert child.query("SELECT * FROM notes ORDER BY id") == [
            {"id": "n1", "body": "branch note"},
            {"id": "n2", "body": "child note"},
        ]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_diff_includes_branch_created_tables_and_schema_columns(sql_backend: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN score INTEGER")
        exp.execute(
            "UPDATE products SET score = :score WHERE sku = :sku",
            {"sku": "abc", "score": 3},
        )
        exp.execute("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)")
        exp.execute(
            "INSERT INTO notes (id, body) VALUES (:id, :body)",
            {"id": "n1", "body": "new"},
        )

        diff = ctx.diff("main", "exp")
        changes = {(change.table, tuple(change.key.items()), change.change) for change in diff.changes}
        assert ("products", (("sku", "abc"),), "modified") in changes
        assert ("notes", (("id", "n1"),), "added") in changes
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_complex_where_on_branch_created_table_uses_branch_schema(sql_backend: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT, score INTEGER)")
        exp.execute(
            "INSERT INTO notes (id, body, score) VALUES (:id, :body, :score)",
            {"id": "n1", "body": "one", "score": 1},
        )
        exp.execute(
            "INSERT INTO notes (id, body, score) VALUES (:id, :body, :score)",
            {"id": "n2", "body": "two", "score": 2},
        )
        exp.execute(
            "UPDATE notes SET body = :body WHERE score = :score OR id = :id",
            {"body": "updated", "score": 2, "id": "missing"},
        )
        assert exp.query("SELECT id, body FROM notes ORDER BY id") == [
            {"id": "n1", "body": "one"},
            {"id": "n2", "body": "updated"},
        ]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_drop_table_is_branch_local_and_inherited_by_descendants(sql_backend: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("dropper", from_branch="main")
        dropper = ctx.checkout("dropper")
        dropper.execute("DROP TABLE products")

        with pytest.raises(TableNotRegisteredError):
            dropper.query("SELECT * FROM products")
        assert ctx.checkout("main").query("SELECT sku FROM products ORDER BY sku") == [
            {"sku": "abc"},
            {"sku": "def"},
        ]

        ctx.create_branch("child", from_branch="dropper")
        with pytest.raises(TableNotRegisteredError):
            ctx.checkout("child").query("SELECT * FROM products")
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_drop_then_recreate_table_on_branch(sql_backend: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("DROP TABLE products")
        exp.execute("CREATE TABLE products (sku TEXT PRIMARY KEY, description TEXT)")
        exp.execute(
            "INSERT INTO products (sku, description) VALUES (:sku, :description)",
            {"sku": "new", "description": "replacement schema"},
        )

        assert exp.query("SELECT * FROM products") == [
            {"sku": "new", "description": "replacement schema"}
        ]
        assert ctx.checkout("main").query("SELECT sku, name, price FROM products ORDER BY sku") == [
            {"sku": "abc", "name": "Alpha", "price": 10},
            {"sku": "def", "name": "Delta", "price": 20},
        ]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
@pytest.mark.parametrize(
    "ddl",
    [
        "CREATE INDEX idx_products_price ON products (price)",
        "ALTER TABLE products RENAME COLUMN price TO amount",
        "ALTER TABLE products RENAME TO renamed_products",
    ],
)
def test_unsupported_ddl_forms_are_rejected(sql_backend: str, ddl: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        with pytest.raises(UnsupportedSQLError):
            ctx.checkout("exp").execute(ddl)
    finally:
        ctx.close()


def test_postgres_supported_ddl_matrix_is_branch_local_and_forkable() -> None:
    ctx = _ctx("postgres", enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")

        exp.execute("CREATE TABLE runs (id TEXT PRIMARY KEY, status TEXT)")
        exp.execute(
            "INSERT INTO runs (id, status) VALUES (:id, :status)",
            {"id": "r1", "status": "open"},
        )
        exp.execute("ALTER TABLE runs ADD COLUMN score INTEGER")
        exp.execute(
            "UPDATE runs SET score = :score WHERE id = :id",
            {"id": "r1", "score": 8},
        )

        ctx.create_branch("child", from_branch="exp")
        child = ctx.checkout("child")
        assert child.query("SELECT id, status, score FROM runs") == [
            {"id": "r1", "status": "open", "score": 8}
        ]

        child.execute("DROP TABLE runs")
        with pytest.raises(TableNotRegisteredError):
            child.query("SELECT * FROM runs")
        assert exp.query("SELECT id, score FROM runs") == [{"id": "r1", "score": 8}]
        with pytest.raises(TableNotRegisteredError):
            ctx.checkout("main").query("SELECT * FROM runs")
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
@pytest.mark.parametrize("branch_backend", ["interval", "copy"])
def test_alter_drop_column_is_branch_local_and_forkable(
    sql_backend: str, branch_backend: str
) -> None:
    ctx = _ctx(
        sql_backend,
        enable_schema_branching=True,
        branch_backend=branch_backend,
    )
    try:
        ctx.create_index("products", ["price", "sku"], name="products_price_sku")
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products DROP COLUMN price")

        assert exp.query("SELECT sku, name FROM products ORDER BY sku") == [
            {"sku": "abc", "name": "Alpha"},
            {"sku": "def", "name": "Delta"},
        ]
        with pytest.raises(Exception):
            exp.query("SELECT price FROM products")
        assert ctx.checkout("main").query(
            "SELECT sku, name, price FROM products WHERE sku = :sku",
            {"sku": "abc"},
        ) == [{"sku": "abc", "name": "Alpha", "price": 10}]

        exp.execute(
            "INSERT INTO products (sku, name) VALUES (:sku, :name)",
            {"sku": "ghi", "name": "Gamma"},
        )
        ctx.create_branch("child", from_branch="exp")
        child = ctx.checkout("child")
        assert child.query("SELECT sku, name FROM products ORDER BY sku") == [
            {"sku": "abc", "name": "Alpha"},
            {"sku": "def", "name": "Delta"},
            {"sku": "ghi", "name": "Gamma"},
        ]
        child.execute(
            "UPDATE products SET name = :name WHERE sku = :sku",
            {"sku": "abc", "name": "Child"},
        )
        assert exp.query("SELECT name FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"name": "Alpha"}
        ]
        assert child.query("SELECT name FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"name": "Child"}
        ]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
@pytest.mark.parametrize("branch_backend", ["interval", "copy"])
def test_alter_drop_column_after_add_column_matches_failure_repro_pattern(
    sql_backend: str, branch_backend: str
) -> None:
    ctx = _ctx(
        sql_backend,
        enable_schema_branching=True,
        branch_backend=branch_backend,
    )
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN discount DECIMAL(5,2)")
        exp.execute(
            "UPDATE products SET discount = :discount WHERE sku = :sku",
            {"sku": "abc", "discount": 1.25},
        )
        exp.execute("ALTER TABLE products DROP COLUMN discount")
        exp.execute("ALTER TABLE products ADD COLUMN flagged BOOLEAN DEFAULT false")

        assert exp.query("SELECT sku, flagged FROM products ORDER BY sku") == [
            {"sku": "abc", "flagged": False if sql_backend == "postgres" else 0},
            {"sku": "def", "flagged": False if sql_backend == "postgres" else 0},
        ]
        with pytest.raises(Exception):
            exp.query("SELECT discount FROM products")

        ctx.create_branch("child", from_branch="exp")
        child = ctx.checkout("child")
        assert child.query("SELECT sku, flagged FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"sku": "abc", "flagged": False if sql_backend == "postgres" else 0}
        ]
        with pytest.raises(Exception):
            child.query("SELECT discount FROM products")
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
@pytest.mark.parametrize("branch_backend", ["interval", "copy"])
def test_alter_drop_primary_key_column_is_rejected(
    sql_backend: str, branch_backend: str
) -> None:
    ctx = _ctx(
        sql_backend,
        enable_schema_branching=True,
        branch_backend=branch_backend,
    )
    try:
        ctx.create_branch("exp", from_branch="main")
        with pytest.raises(UnsupportedSQLError):
            ctx.checkout("exp").execute("ALTER TABLE products DROP COLUMN sku")
        assert ctx.checkout("exp").query("SELECT sku FROM products ORDER BY sku") == [
            {"sku": "abc"},
            {"sku": "def"},
        ]
    finally:
        ctx.close()


def _postgres_physical_table_for_latest_schema_change(
    ctx: ChronosBranchContext, *, ddl_op: str, commit: bool = True
) -> str:
    row = ctx.db.execute(
        """
        SELECT physical_table
        FROM _chronos_branch_table_schema_versions
        WHERE backend = 'interval'
          AND table_name = 'products'
          AND ddl_op = :ddl_op
        ORDER BY created_at DESC, schema_version_id DESC
        LIMIT 1
        """,
        {"ddl_op": ddl_op},
    ).fetchone()
    assert row is not None
    if commit and ctx.db.in_transaction:
        ctx.db.commit()
    return row["physical_table"]


def _interval_schema_versions_for_table(
    ctx: ChronosBranchContext, table: str = "products", commit: bool = True
) -> list[dict[str, object]]:
    rows = ctx.db.execute(
        """
        SELECT schema_version_id, parent_schema_version_id, physical_table,
               columns, column_defs, ddl_op
        FROM _chronos_branch_table_schema_versions
        WHERE backend = 'interval'
          AND table_name = :table
        ORDER BY created_at, schema_version_id
        """,
        {"table": table},
    ).fetchall()
    if commit and ctx.db.in_transaction:
        ctx.db.commit()
    return [dict(row) for row in rows]


def _interval_active_schema_version_for_branch(
    ctx: ChronosBranchContext,
    branch: str,
    table: str = "products",
    commit: bool = True,
) -> dict[str, object]:
    row = ctx.db.execute(
        """
        SELECT tb.schema_version_id, v.physical_table, v.columns, v.column_defs, v.ddl_op
        FROM _chronos_branch_interval_branches b
        JOIN _chronos_branch_interval_segments s
          ON s.segment_id = b.current_segment_id
        JOIN _chronos_branch_table_bindings tb
          ON tb.live_lo <= s.branch_point
         AND s.branch_point < tb.live_hi
        JOIN _chronos_branch_table_schema_versions v
          ON v.backend = tb.backend
         AND v.schema_version_id = tb.schema_version_id
        WHERE b.branch_id = :branch
          AND tb.backend = 'interval'
          AND tb.table_name = :table
          AND tb.tombstone = 0
        """,
        {"branch": branch, "table": table},
    ).fetchone()
    assert row is not None
    if commit and ctx.db.in_transaction:
        ctx.db.commit()
    return dict(row)


def _interval_physical_rows(
    ctx: ChronosBranchContext,
    physical_table: str,
    columns: str = "*",
    commit: bool = True,
) -> list[dict[str, object]]:
    rows = ctx.db.execute(
        f"""
        SELECT {columns}
        FROM "{physical_table}"
        ORDER BY 1, live_lo
        """
    ).fetchall()
    if commit and ctx.db.in_transaction:
        ctx.db.commit()
    return [dict(row) for row in rows]


def _orpheus_physical_for_branch(
    ctx: ChronosBranchContext,
    branch: str,
    table: str = "products",
    commit: bool = True,
) -> str:
    row = ctx.db.execute(
        """
        SELECT physical_table
        FROM _chronos_branch_orpheus_table_bindings
        WHERE owner_kind = 'branch'
          AND owner_id = ?
          AND table_name = ?
          AND tombstone = 0
        """,
        (branch, table),
    ).fetchone()
    assert row is not None
    if commit and ctx.db.in_transaction:
        ctx.db.commit()
    return str(row["physical_table"])


def _postgres_index_defs_for_table(
    ctx: ChronosBranchContext, physical_table: str, commit: bool = True
) -> list[str]:
    rows = [
        row["indexdef"]
        for row in ctx.db.execute(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = :physical_table
            ORDER BY indexname
            """,
            {"physical_table": physical_table},
        ).fetchall()
    ]
    if commit and ctx.db.in_transaction:
        ctx.db.commit()
    return rows


def test_postgres_interval_schema_copy_preserves_logical_indexes() -> None:
    ctx = _ctx("postgres", enable_schema_branching=True)
    try:
        ctx.create_index("products", ["price", "sku"], name="products_price_sku")
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")

        with exp.transaction():
            exp.execute("ALTER TABLE products ADD COLUMN score INTEGER")
            score_physical = _postgres_physical_table_for_latest_schema_change(
                ctx, ddl_op="alter_table_add_column", commit=False
            )
            score_indexes_before_commit = _postgres_index_defs_for_table(
                ctx, score_physical, commit=False
            )
            assert not any("products_price_sku" in indexdef for indexdef in score_indexes_before_commit)
            assert not any("_pk_hi" in indexdef for indexdef in score_indexes_before_commit)
        ctx.wait_for_background_work()
        score_indexes = _postgres_index_defs_for_table(ctx, score_physical)
        assert any("_pk_hi" in indexdef for indexdef in score_indexes)
        assert any(
            "price" in indexdef
            and "sku" in indexdef
            and "live_lo" in indexdef
            and "live_hi" in indexdef
            and "deleted" in indexdef
            for indexdef in score_indexes
        )

        ctx.create_branch("child", from_branch="exp")
        exp.execute("ALTER TABLE products ADD COLUMN note TEXT")
        note_physical = _postgres_physical_table_for_latest_schema_change(
            ctx, ddl_op="alter_table_add_column"
        )
        ctx.wait_for_background_work()
        note_indexes = _postgres_index_defs_for_table(ctx, note_physical)
        assert any("_pk_hi" in indexdef for indexdef in note_indexes)
        assert any(
            "price" in indexdef
            and "sku" in indexdef
            and "live_lo" in indexdef
            and "live_hi" in indexdef
            and "deleted" in indexdef
            for indexdef in note_indexes
        )
        assert score_physical != note_physical
        assert exp.query(
            "SELECT sku, score, note FROM products WHERE price = :price",
            {"price": 10},
        ) == [{"sku": "abc", "score": None, "note": None}]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_copy_backend_alter_add_column_is_branch_local_and_forkable(sql_backend: str) -> None:
    ctx = _ctx(
        sql_backend,
        enable_schema_branching=True,
        branch_backend="copy",
    )
    try:
        ctx.create_index("products", ["price", "sku"], name="products_price_sku")
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN score INTEGER")
        exp.execute(
            "UPDATE products SET score = :score WHERE sku = :sku",
            {"sku": "abc", "score": 12},
        )

        with pytest.raises(Exception):
            ctx.checkout("main").query("SELECT score FROM products")

        ctx.create_branch("child", from_branch="exp")
        child = ctx.checkout("child")
        assert child.query("SELECT sku, score FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"sku": "abc", "score": 12}
        ]
        child.execute(
            "UPDATE products SET score = :score WHERE sku = :sku",
            {"sku": "abc", "score": 13},
        )
        assert exp.query("SELECT score FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"score": 12}
        ]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_copy_backend_create_drop_and_checkpoint_schema(sql_backend: str) -> None:
    ctx = _ctx(
        sql_backend,
        enable_schema_branching=True,
        branch_backend="copy",
    )
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)")
        exp.execute(
            "INSERT INTO notes (id, body) VALUES (:id, :body)",
            {"id": "n1", "body": "copy branch"},
        )
        exp.execute("DROP TABLE products")

        ctx.create_checkpoint("snap", branch="exp")
        ctx.create_branch_from_checkpoint("restored", "snap")
        restored = ctx.checkout("restored")

        assert restored.query("SELECT * FROM notes") == [
            {"id": "n1", "body": "copy branch"}
        ]
        with pytest.raises(TableNotRegisteredError):
            restored.query("SELECT * FROM products")
        with pytest.raises(TableNotRegisteredError):
            ctx.checkout("main").query("SELECT * FROM notes")
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
@pytest.mark.parametrize("branch_backend", ["interval", "copy"])
def test_add_column_constant_default_is_branch_local_and_used_for_existing_and_new_rows(
    sql_backend: str, branch_backend: str
) -> None:
    ctx = _ctx(
        sql_backend,
        enable_schema_branching=True,
        branch_backend=branch_backend,
    )
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN score INTEGER DEFAULT 7")

        assert exp.query("SELECT sku, score FROM products ORDER BY sku") == [
            {"sku": "abc", "score": 7},
            {"sku": "def", "score": 7},
        ]
        exp.execute(
            "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
            {"sku": "ghi", "name": "Gamma", "price": 30},
        )
        assert exp.query("SELECT sku, score FROM products WHERE sku = :sku", {"sku": "ghi"}) == [
            {"sku": "ghi", "score": 7}
        ]
        with pytest.raises(Exception):
            ctx.checkout("main").query("SELECT score FROM products")

        ctx.create_branch("child", from_branch="exp")
        assert ctx.checkout("child").query(
            "SELECT sku, score FROM products WHERE sku = :sku",
            {"sku": "abc"},
        ) == [{"sku": "abc", "score": 7}]
    finally:
        ctx.close()


@pytest.mark.parametrize("branch_backend", ["interval", "copy"])
def test_postgres_alter_column_type_is_branch_local_and_forkable(
    branch_backend: str,
) -> None:
    ctx = _ctx(
        "postgres",
        enable_schema_branching=True,
        branch_backend=branch_backend,
    )
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ALTER COLUMN price TYPE TEXT")

        assert exp.query("SELECT sku, price FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"sku": "abc", "price": "10"}
        ]
        exp.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": "eleven"},
        )
        assert ctx.checkout("main").query(
            "SELECT sku, price FROM products WHERE sku = :sku",
            {"sku": "abc"},
        ) == [{"sku": "abc", "price": 10}]

        ctx.create_branch("child", from_branch="exp")
        child = ctx.checkout("child")
        assert child.query("SELECT sku, price FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"sku": "abc", "price": "eleven"}
        ]
    finally:
        ctx.close()


def test_postgres_interval_alter_column_type_using_expression() -> None:
    ctx = _ctx("postgres", enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ALTER COLUMN price TYPE TEXT USING (price + 1)::text")

        assert exp.query("SELECT sku, price FROM products ORDER BY sku") == [
            {"sku": "abc", "price": "11"},
            {"sku": "def", "price": "21"},
        ]
        assert ctx.checkout("main").query("SELECT sku, price FROM products ORDER BY sku") == [
            {"sku": "abc", "price": 10},
            {"sku": "def", "price": 20},
        ]
    finally:
        ctx.close()


@pytest.mark.parametrize(
    "ddl",
    [
        "CREATE VIEW product_names AS SELECT sku FROM products",
        "CREATE SEQUENCE product_seq",
        "CREATE TABLE table_pk (id TEXT, body TEXT, PRIMARY KEY (id))",
        "CREATE TABLE constrained (id TEXT PRIMARY KEY, body TEXT NOT NULL)",
        "CREATE TABLE no_pk (id TEXT, body TEXT)",
        "ALTER TABLE products ADD COLUMN score INTEGER DEFAULT abs(1)",
        "ALTER TABLE products ADD COLUMN score INTEGER NOT NULL",
        "ALTER TABLE products ADD COLUMN score INTEGER UNIQUE",
        "ALTER TABLE products ADD CONSTRAINT products_price_positive CHECK (price > 0)",
        "ALTER TABLE products ALTER COLUMN price SET DEFAULT 0",
        "ALTER TABLE products RENAME COLUMN price TO amount",
        "ALTER TABLE products RENAME TO renamed_products",
        "DROP VIEW product_names",
        "DROP INDEX idx_products_price",
        "DROP SEQUENCE product_seq",
        "TRUNCATE TABLE products",
    ],
)
def test_postgres_unsupported_ddl_matrix_is_rejected(ddl: str) -> None:
    ctx = _ctx("postgres", enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        with pytest.raises(UnsupportedSQLError):
            ctx.checkout("exp").execute(ddl)
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_repeated_schema_changes_create_forkable_schema_versions(sql_backend: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN score INTEGER")
        exp.execute("ALTER TABLE products ADD COLUMN note TEXT")
        exp.execute(
            "UPDATE products SET score = :score, note = :note WHERE sku = :sku",
            {"sku": "abc", "score": 5, "note": "ready"},
        )

        ctx.create_branch("child", from_branch="exp")
        child = ctx.checkout("child")
        assert child.query(
            "SELECT sku, score, note FROM products WHERE sku = :sku",
            {"sku": "abc"},
        ) == [{"sku": "abc", "score": 5, "note": "ready"}]
        child.execute(
            "UPDATE products SET note = :note WHERE sku = :sku",
            {"sku": "abc", "note": "child"},
        )
        assert exp.query("SELECT note FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"note": "ready"}
        ]
        assert child.query("SELECT note FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"note": "child"}
        ]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_interval_repeated_schema_changes_reuse_private_schema_version(
    sql_backend: str,
) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")

        exp.execute("ALTER TABLE products ADD COLUMN score INTEGER DEFAULT 7")
        first_versions = _interval_schema_versions_for_table(ctx)
        first_active = _interval_active_schema_version_for_branch(ctx, "exp")

        exp.execute("ALTER TABLE products ADD COLUMN note TEXT")
        second_versions = _interval_schema_versions_for_table(ctx)
        second_active = _interval_active_schema_version_for_branch(ctx, "exp")

        assert len(second_versions) == len(first_versions)
        assert second_active["schema_version_id"] == first_active["schema_version_id"]
        assert second_active["physical_table"] == first_active["physical_table"]
        assert json.loads(second_active["columns"]) == ["sku", "name", "price", "score", "note"]
        assert exp.query("SELECT sku, score, note FROM products ORDER BY sku") == [
            {"sku": "abc", "score": 7, "note": None},
            {"sku": "def", "score": 7, "note": None},
        ]
        with pytest.raises(Exception):
            ctx.checkout("main").query("SELECT score FROM products")
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_interval_schema_change_copies_after_child_branch(sql_backend: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN score INTEGER")
        exp.execute("ALTER TABLE products ADD COLUMN note TEXT")
        before_child = _interval_active_schema_version_for_branch(ctx, "exp")

        ctx.create_branch("child", from_branch="exp")
        exp.execute("ALTER TABLE products ADD COLUMN flag INTEGER DEFAULT 1")

        exp_active = _interval_active_schema_version_for_branch(ctx, "exp")
        child_active = _interval_active_schema_version_for_branch(ctx, "child")
        assert exp_active["schema_version_id"] != before_child["schema_version_id"]
        assert exp_active["physical_table"] != before_child["physical_table"]
        assert child_active["schema_version_id"] == before_child["schema_version_id"]
        assert json.loads(exp_active["columns"]) == [
            "sku",
            "name",
            "price",
            "score",
            "note",
            "flag",
        ]
        assert exp.query("SELECT sku, flag FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"sku": "abc", "flag": 1}
        ]
        with pytest.raises(Exception):
            ctx.checkout("child").query("SELECT flag FROM products")
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_interval_full_table_update_on_schema_version_preserves_child(
    sql_backend: str,
) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN score INTEGER DEFAULT 0")
        assert exp.execute("UPDATE products SET score = price").rowcount == 2

        ctx.create_branch("child", from_branch="exp")
        child = ctx.checkout("child")
        assert exp.execute("UPDATE products SET score = price + 100").rowcount == 2

        assert child.query("SELECT sku, score FROM products ORDER BY sku") == [
            {"sku": "abc", "score": 10},
            {"sku": "def", "score": 20},
        ]
        assert exp.query("SELECT sku, score FROM products ORDER BY sku") == [
            {"sku": "abc", "score": 110},
            {"sku": "def", "score": 120},
        ]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_interval_private_schema_dml_preserves_canonical_physical_table(
    sql_backend: str,
) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN score INTEGER DEFAULT 0")
        active = _interval_active_schema_version_for_branch(ctx, "exp")
        physical = str(active["physical_table"])

        assert exp.execute(
            "UPDATE products SET score = price + 1 WHERE sku = :sku",
            {"sku": "abc"},
        ).rowcount == 1
        rows = _interval_physical_rows(
            ctx, physical, "sku, price, score, live_lo, live_hi, deleted"
        )
        assert len(rows) == 2
        assert {row["sku"]: row["score"] for row in rows} == {"abc": 11, "def": 0}
        assert {row["deleted"] for row in rows} == {0}
        assert len({(row["live_lo"], row["live_hi"]) for row in rows}) == 1

        assert exp.execute("DELETE FROM products WHERE sku = :sku", {"sku": "def"}).rowcount == 1
        rows = _interval_physical_rows(
            ctx, physical, "sku, price, score, live_lo, live_hi, deleted"
        )
        assert rows == [
            {
                "sku": "abc",
                "price": 10,
                "score": 11,
                "live_lo": rows[0]["live_lo"],
                "live_hi": rows[0]["live_hi"],
                "deleted": 0,
            }
        ]

        ctx.create_branch("child", from_branch="exp")
        child = ctx.checkout("child")
        exp.execute("UPDATE products SET score = 99 WHERE sku = :sku", {"sku": "abc"})
        assert child.query("SELECT sku, score FROM products ORDER BY sku") == [
            {"sku": "abc", "score": 11}
        ]
        assert exp.query("SELECT sku, score FROM products ORDER BY sku") == [
            {"sku": "abc", "score": 99}
        ]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_interval_root_registered_table_uses_private_canonical_dml(
    sql_backend: str,
) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        active = _interval_active_schema_version_for_branch(ctx, "main")
        physical = str(active["physical_table"])

        main = ctx.checkout("main")
        assert main.execute("UPDATE products SET price = price + 5").rowcount == 2
        rows = _interval_physical_rows(ctx, physical, "sku, price, live_lo, live_hi, deleted")
        assert {row["sku"]: row["price"] for row in rows} == {"abc": 15, "def": 25}
        assert {row["deleted"] for row in rows} == {0}
        assert len({(row["live_lo"], row["live_hi"]) for row in rows}) == 1

        assert main.execute("DELETE FROM products WHERE sku = :sku", {"sku": "def"}).rowcount == 1
        rows = _interval_physical_rows(ctx, physical, "sku, price, live_lo, live_hi, deleted")
        assert len(rows) == 1
        assert rows[0]["sku"] == "abc"
        assert rows[0]["deleted"] == 0
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_interval_private_schema_version_drop_column_in_place(sql_backend: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN discount INTEGER")
        before_drop_versions = _interval_schema_versions_for_table(ctx)
        before_drop = _interval_active_schema_version_for_branch(ctx, "exp")

        exp.execute("ALTER TABLE products DROP COLUMN discount")

        after_drop_versions = _interval_schema_versions_for_table(ctx)
        after_drop = _interval_active_schema_version_for_branch(ctx, "exp")
        assert len(after_drop_versions) == len(before_drop_versions)
        assert after_drop["schema_version_id"] == before_drop["schema_version_id"]
        assert after_drop["physical_table"] == before_drop["physical_table"]
        assert json.loads(after_drop["columns"]) == ["sku", "name", "price"]
        assert exp.query("SELECT sku, price FROM products ORDER BY sku") == [
            {"sku": "abc", "price": 10},
            {"sku": "def", "price": 20},
        ]
        with pytest.raises(Exception):
            exp.query("SELECT discount FROM products")
    finally:
        ctx.close()


def test_postgres_interval_private_schema_version_alter_type_in_place() -> None:
    ctx = _ctx("postgres", enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN score INTEGER")
        before_type_versions = _interval_schema_versions_for_table(ctx)
        before_type = _interval_active_schema_version_for_branch(ctx, "exp")

        exp.execute("ALTER TABLE products ALTER COLUMN price TYPE TEXT")

        after_type_versions = _interval_schema_versions_for_table(ctx)
        after_type = _interval_active_schema_version_for_branch(ctx, "exp")
        assert len(after_type_versions) == len(before_type_versions)
        assert after_type["schema_version_id"] == before_type["schema_version_id"]
        assert after_type["physical_table"] == before_type["physical_table"]
        assert json.loads(after_type["columns"]) == ["sku", "name", "price", "score"]
        assert any(
            definition == '"price" TEXT'
            for definition in json.loads(after_type["column_defs"])
        )
        assert exp.query("SELECT sku, price FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"sku": "abc", "price": "10"}
        ]
        assert ctx.checkout("main").query(
            "SELECT sku, price FROM products WHERE sku = :sku",
            {"sku": "abc"},
        ) == [{"sku": "abc", "price": 10}]
    finally:
        ctx.close()


def test_postgres_orpheus_schema_add_default_is_branch_local_and_forkable() -> None:
    ctx = _ctx("postgres", enable_schema_branching=True, branch_backend="orpheus")
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN score INTEGER DEFAULT 7")
        assert exp.query("SELECT sku, score FROM products ORDER BY sku") == [
            {"sku": "abc", "score": 7},
            {"sku": "def", "score": 7},
        ]
        with pytest.raises(Exception):
            ctx.checkout("main").query("SELECT score FROM products")

        exp.execute(
            "UPDATE products SET score = :score WHERE sku = :sku",
            {"sku": "abc", "score": 11},
        )
        ctx.create_branch("child", from_branch="exp")
        child = ctx.checkout("child")
        child.execute(
            "UPDATE products SET score = :score WHERE sku = :sku",
            {"sku": "abc", "score": 13},
        )
        assert exp.query("SELECT sku, score FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"sku": "abc", "score": 11}
        ]
        assert child.query("SELECT sku, score FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"sku": "abc", "score": 13}
        ]
    finally:
        ctx.close()


def test_postgres_orpheus_repeated_schema_changes_reuse_private_physical_table() -> None:
    ctx = _ctx("postgres", enable_schema_branching=True, branch_backend="orpheus")
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN score INTEGER DEFAULT 7")
        first_physical = _orpheus_physical_for_branch(ctx, "exp")

        exp.execute("ALTER TABLE products ADD COLUMN note TEXT")

        assert _orpheus_physical_for_branch(ctx, "exp") == first_physical
        assert exp.query("SELECT sku, score, note FROM products ORDER BY sku") == [
            {"sku": "abc", "score": 7, "note": None},
            {"sku": "def", "score": 7, "note": None},
        ]
    finally:
        ctx.close()


def test_postgres_orpheus_schema_change_copies_after_child_branch() -> None:
    ctx = _ctx("postgres", enable_schema_branching=True, branch_backend="orpheus")
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN score INTEGER DEFAULT 7")
        ctx.create_branch("child", from_branch="exp")
        child_physical = _orpheus_physical_for_branch(ctx, "child")

        exp.execute("ALTER TABLE products DROP COLUMN score")

        assert _orpheus_physical_for_branch(ctx, "exp") != child_physical
        assert ctx.checkout("child").query("SELECT sku, score FROM products ORDER BY sku") == [
            {"sku": "abc", "score": 7},
            {"sku": "def", "score": 7},
        ]
        assert exp.query("SELECT sku, price FROM products ORDER BY sku") == [
            {"sku": "abc", "price": 10},
            {"sku": "def", "price": 20},
        ]
        with pytest.raises(Exception):
            exp.query("SELECT score FROM products")
    finally:
        ctx.close()


def test_postgres_orpheus_alter_column_type_is_branch_local() -> None:
    ctx = _ctx("postgres", enable_schema_branching=True, branch_backend="orpheus")
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ALTER COLUMN price TYPE TEXT")

        assert exp.query("SELECT sku, price FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"sku": "abc", "price": "10"}
        ]
        assert ctx.checkout("main").query(
            "SELECT sku, price FROM products WHERE sku = :sku",
            {"sku": "abc"},
        ) == [{"sku": "abc", "price": 10}]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_checkpoint_restore_preserves_schema_version(sql_backend: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("ALTER TABLE products ADD COLUMN score INTEGER")
        exp.execute(
            "UPDATE products SET score = :score WHERE sku = :sku",
            {"sku": "abc", "score": 1},
        )
        ctx.create_checkpoint("snap", branch="exp")
        exp.execute("ALTER TABLE products ADD COLUMN note TEXT")
        exp.execute(
            "UPDATE products SET score = :score, note = :note WHERE sku = :sku",
            {"sku": "abc", "score": 2, "note": "later"},
        )

        ctx.create_branch_from_checkpoint("restored", "snap")
        restored = ctx.checkout("restored")
        assert restored.query("SELECT sku, score FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"sku": "abc", "score": 1}
        ]
        with pytest.raises(Exception):
            restored.query("SELECT note FROM products")
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_duplicate_create_table_rejected_on_same_branch(sql_backend: str) -> None:
    ctx = _ctx(sql_backend, enable_schema_branching=True)
    try:
        exp = ctx.checkout("main")
        exp.execute("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)")
        with pytest.raises(BranchAlreadyExistsError):
            exp.execute("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)")
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_multi_store_branch_can_change_schema_and_filesystem(tmp_path: Path, sql_backend: str) -> None:
    _require_fuse_overlayfs()
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "README.md").write_text("# main\n")
    sql = _ctx(sql_backend, enable_schema_branching=True)
    fs = ChronosFilesystemStore(repo, state_dir=tmp_path / "state")
    store_name = "postgresql" if sql_backend == "postgres" else "sqlite"
    workspace = ChronosWorkspaceContext(filesystem=fs, **{store_name: sql})
    try:
        workspace.create_branch("agent", from_branch="main")
        agent = workspace.checkout("agent")
        sql_session = getattr(agent, store_name)
        assert sql_session is not None
        assert agent.fs is not None

        sql_session.execute("ALTER TABLE products ADD COLUMN score INTEGER")
        sql_session.execute(
            "UPDATE products SET score = :score WHERE sku = :sku",
            {"sku": "abc", "score": 42},
        )
        (agent.fs.path / "report.md").write_text("score: 42\n")

        assert sql_session.query("SELECT score FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"score": 42}
        ]
        assert (agent.fs.path / "report.md").read_text() == "score: 42\n"
        with pytest.raises(Exception):
            getattr(workspace.checkout("main"), store_name).query("SELECT score FROM products")
        assert not (repo / "report.md").exists()
    finally:
        workspace.close()
