from __future__ import annotations

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
    ctx: ChronosBranchContext, *, ddl_op: str
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
    return row["physical_table"]


def _postgres_index_defs_for_table(
    ctx: ChronosBranchContext, physical_table: str
) -> list[str]:
    return [
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


def test_postgres_interval_schema_copy_preserves_logical_indexes() -> None:
    ctx = _ctx("postgres", enable_schema_branching=True)
    try:
        ctx.create_index("products", ["price", "sku"], name="products_price_sku")
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")

        exp.execute("ALTER TABLE products ADD COLUMN score INTEGER")
        score_physical = _postgres_physical_table_for_latest_schema_change(
            ctx, ddl_op="alter_table_add_column"
        )
        score_indexes = _postgres_index_defs_for_table(ctx, score_physical)
        assert any(
            "price" in indexdef
            and "sku" in indexdef
            and "live_lo" in indexdef
            and "live_hi" in indexdef
            and "deleted" in indexdef
            for indexdef in score_indexes
        )

        exp.execute("ALTER TABLE products ADD COLUMN note TEXT")
        note_physical = _postgres_physical_table_for_latest_schema_change(
            ctx, ddl_op="alter_table_add_column"
        )
        note_indexes = _postgres_index_defs_for_table(ctx, note_physical)
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
    workspace = ChronosWorkspaceContext(relational=sql, filesystem=fs)
    try:
        workspace.create_branch("agent", from_branch="main")
        agent = workspace.checkout("agent")
        assert agent.sql is not None
        assert agent.fs is not None

        agent.sql.execute("ALTER TABLE products ADD COLUMN score INTEGER")
        agent.sql.execute(
            "UPDATE products SET score = :score WHERE sku = :sku",
            {"sku": "abc", "score": 42},
        )
        (agent.fs.path / "report.md").write_text("score: 42\n")

        assert agent.sql.query("SELECT score FROM products WHERE sku = :sku", {"sku": "abc"}) == [
            {"score": 42}
        ]
        assert (agent.fs.path / "report.md").read_text() == "score: 42\n"
        with pytest.raises(Exception):
            workspace.checkout("main").sql.query("SELECT score FROM products")  # type: ignore[union-attr]
        assert not (repo / "report.md").exists()
    finally:
        workspace.close()
