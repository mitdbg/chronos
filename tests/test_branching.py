from __future__ import annotations

import atexit
import os
import subprocess
import time

import pytest

from chronos_core.branching import (
    BranchAlreadyExistsError,
    BranchingError,
    DuplicateKeyError,
    TableNotRegisteredError,
    ChronosBranchContext,
    UnsupportedSQLError,
)


BRANCH_BACKENDS = ("copy", "interval", "log")
SQL_BACKENDS = ("sqlite", "postgres")
TEST_POSTGRES_CONTAINER = os.environ.get(
    "CHRONOS_BRANCH_TEST_POSTGRES_NAME", "chronos-branch-postgres-tests"
)
TEST_POSTGRES_PORT = os.environ.get("CHRONOS_BRANCH_TEST_POSTGRES_PORT", "55434")
TEST_POSTGRES_DB = os.environ.get("CHRONOS_BRANCH_TEST_POSTGRES_DB", "chronos_branch_test")
TEST_POSTGRES_PASSWORD = os.environ.get("CHRONOS_BRANCH_TEST_POSTGRES_PASSWORD", "postgres")
TEST_POSTGRES_IMAGE = os.environ.get("CHRONOS_BRANCH_TEST_POSTGRES_IMAGE", "postgres:16-alpine")
_POSTGRES_DSN: str | None = os.environ.get("CHRONOS_BRANCH_POSTGRES_DSN") or os.environ.get(
    "CHRONOS_POSTGRES_DSN"
)
_POSTGRES_STARTED_BY_TESTS = False


def _database_url(sql_backend: str) -> str:
    if sql_backend == "sqlite":
        return "sqlite:///:memory:"
    return _postgres_dsn()


def _run_docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args],
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _docker_container_names(all_containers: bool) -> set[str]:
    args = ["ps", "--format", "{{.Names}}"]
    if all_containers:
        args.insert(1, "-a")
    result = _run_docker(*args)
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def _wait_for_postgres() -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        result = _run_docker(
            "exec",
            TEST_POSTGRES_CONTAINER,
            "pg_isready",
            "-U",
            "postgres",
            "-d",
            TEST_POSTGRES_DB,
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError(f"PostgreSQL test container did not become ready: {TEST_POSTGRES_CONTAINER}")


def _stop_managed_postgres() -> None:
    if _POSTGRES_STARTED_BY_TESTS:
        _run_docker("stop", TEST_POSTGRES_CONTAINER, check=False)


def _postgres_dsn() -> str:
    global _POSTGRES_DSN, _POSTGRES_STARTED_BY_TESTS
    if _POSTGRES_DSN is not None:
        return _POSTGRES_DSN

    running = _docker_container_names(all_containers=False)
    if TEST_POSTGRES_CONTAINER in running:
        _wait_for_postgres()
    else:
        all_names = _docker_container_names(all_containers=True)
        if TEST_POSTGRES_CONTAINER in all_names:
            _run_docker("rm", TEST_POSTGRES_CONTAINER)
        _run_docker(
            "run",
            "--rm",
            "-d",
            "--name",
            TEST_POSTGRES_CONTAINER,
            "-e",
            f"POSTGRES_PASSWORD={TEST_POSTGRES_PASSWORD}",
            "-e",
            f"POSTGRES_DB={TEST_POSTGRES_DB}",
            "-p",
            f"{TEST_POSTGRES_PORT}:5432",
            TEST_POSTGRES_IMAGE,
        )
        _POSTGRES_STARTED_BY_TESTS = True
        atexit.register(_stop_managed_postgres)
        _wait_for_postgres()

    _POSTGRES_DSN = (
        f"postgresql://postgres:{TEST_POSTGRES_PASSWORD}"
        f"@localhost:{TEST_POSTGRES_PORT}/{TEST_POSTGRES_DB}"
    )
    return _POSTGRES_DSN


def _reset_postgres_schema() -> None:
    context = ChronosBranchContext.connect(_postgres_dsn(), backend="interval")
    try:
        rows = context.db.execute(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
            """
        ).fetchall()
        for row in rows:
            context.db.drop_table(row["tablename"])
        context.db.commit()
    finally:
        context.close()


def _make_context(sql_backend: str, branch_backend: str) -> ChronosBranchContext:
    if sql_backend == "postgres":
        _reset_postgres_schema()
    context = ChronosBranchContext.connect(
        _database_url(sql_backend), backend=branch_backend
    )
    context._test_sql_backend = sql_backend  # type: ignore[attr-defined]
    db = context.db
    db.execute(
        "CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)"
    )
    db.execute(
        "CREATE TABLE orders (order_id TEXT PRIMARY KEY, sku TEXT, quantity INTEGER)"
    )
    db.execute(
        "CREATE TABLE nodes (node_id TEXT PRIMARY KEY, label TEXT, score INTEGER)"
    )
    db.execute(
        "CREATE TABLE edges (edge_id TEXT PRIMARY KEY, src_id TEXT, dst_id TEXT)"
    )
    db.executemany(
        "INSERT INTO products VALUES (?, ?, ?)",
        [("abc", "Alpha", 10), ("def", "Delta", 20)],
    )
    db.executemany(
        "INSERT INTO orders VALUES (?, ?, ?)",
        [("o1", "abc", 2), ("o2", "def", 1)],
    )
    db.executemany(
        "INSERT INTO nodes VALUES (?, ?, ?)",
        [("n1", "one", 1), ("n2", "two", 2)],
    )
    db.execute("INSERT INTO edges VALUES (?, ?, ?)", ("e1", "n1", "n2"))
    db.commit()
    context.register_table("products", ["sku"])
    context.register_table("orders", ["order_id"])
    context.register_table("nodes", ["node_id"])
    context.register_table("edges", ["edge_id"])
    return context


@pytest.fixture(params=SQL_BACKENDS)
def sql_backend(request) -> str:
    return request.param


@pytest.fixture(params=BRANCH_BACKENDS)
def branch_backend(request) -> str:
    return request.param


@pytest.fixture
def ctx(sql_backend: str, branch_backend: str) -> ChronosBranchContext:
    context = _make_context(sql_backend, branch_backend)
    yield context
    context.close()


def _product(session, sku: str) -> dict:
    rows = session.query(
        "SELECT sku, name, price FROM products WHERE sku = :sku", {"sku": sku}
    )
    assert len(rows) == 1
    return rows[0]


def _physical_change_count(ctx: ChronosBranchContext, table: str) -> int:
    if ctx.backend_name == "interval":
        row = ctx.db.execute(
            f"SELECT COUNT(*) AS count FROM _chronos_b_interval_{table}"
        ).fetchone()
        return int(row["count"])
    if ctx.backend_name == "log":
        row = ctx.db.execute(
            f"SELECT COUNT(*) AS count FROM _chronos_b_log_{table}"
        ).fetchone()
        return int(row["count"])
    backend = ctx._backend  # type: ignore[attr-defined]
    return sum(
        int(
            ctx.db.execute(
                "SELECT COUNT(*) AS count "
                f"FROM {backend._branch_table(branch.branch_id, table)}"
            ).fetchone()["count"]
        )
        for branch in ctx.list_branches()
    )


def _sql_backend(ctx: ChronosBranchContext) -> str:
    return ctx._test_sql_backend  # type: ignore[attr-defined]


def _index_exists(
    ctx: ChronosBranchContext, *, name: str | None = None, pattern: str | None = None
) -> bool:
    if _sql_backend(ctx) == "postgres":
        if name is not None:
            row = ctx.db.execute(
                """
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname = :name
                """,
                {"name": name},
            ).fetchone()
        else:
            row = ctx.db.execute(
                """
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname LIKE :pattern
                """,
                {"pattern": pattern},
            ).fetchone()
        return row is not None
    if name is not None:
        row = ctx.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?",
            (name,),
        ).fetchone()
    else:
        row = ctx.db.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'index'
              AND name LIKE ?
            """,
            (pattern,),
        ).fetchone()
    return row is not None


def _table_exists(ctx: ChronosBranchContext, table: str) -> bool:
    if _sql_backend(ctx) == "postgres":
        row = ctx.db.execute(
            """
            SELECT 1
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename = :table
            """,
            {"table": table},
        ).fetchone()
    else:
        row = ctx.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
    return row is not None


def _make_products_only_context(
    sql_backend: str, branch_backend: str
) -> ChronosBranchContext:
    if sql_backend == "postgres":
        _reset_postgres_schema()
    ctx = ChronosBranchContext.connect(_database_url(sql_backend), backend=branch_backend)
    ctx._test_sql_backend = sql_backend  # type: ignore[attr-defined]
    ctx.db.execute("CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)")
    ctx.db.executemany(
        "INSERT INTO products VALUES (?, ?, ?)",
        [("abc", "Alpha", 10), ("def", "Delta", 20)],
    )
    ctx.db.commit()
    ctx.register_table("products", ["sku"])
    return ctx


def test_create_branch_storage_behavior_for_user_records(ctx: ChronosBranchContext) -> None:
    before = _physical_change_count(ctx, "products")
    ctx.create_branch("exp", from_branch="main")
    after = _physical_change_count(ctx, "products")

    if ctx.backend_name == "copy":
        assert after > before
    else:
        assert before == after
    assert {branch.branch_id for branch in ctx.list_branches()} == {"main", "exp"}


def test_users_can_add_logical_indexes_to_registered_tables(ctx: ChronosBranchContext) -> None:
    index = ctx.create_index("products", ["price", "sku"], name="products_price_sku")

    assert index.name == "products_price_sku"
    assert index.table == "products"
    assert index.columns == ("price", "sku")
    assert ctx.list_indexes("products") == [index]

    if ctx.backend_name == "copy":
        assert _index_exists(ctx, pattern="_chronos_idx_copy_%products_price_sku_%")
    else:
        index_name = (
            "_chronos_idx_interval_products_price_sku"
            if ctx.backend_name == "interval"
            else "_chronos_idx_log_products_price_sku"
        )
        assert _index_exists(ctx, name=index_name)
    assert ctx.checkout("main").query(
        "SELECT sku FROM products WHERE price >= :price ORDER BY sku",
        {"price": 10},
    ) == [{"sku": "abc"}, {"sku": "def"}]


def test_indexes_validate_registered_tables_and_columns(ctx: ChronosBranchContext) -> None:
    with pytest.raises(TableNotRegisteredError):
        ctx.create_index("missing", ["id"])
    with pytest.raises(TableNotRegisteredError):
        ctx.create_index("products", ["missing"])
    with pytest.raises(ValueError):
        ctx.create_index("products", [])


def test_branch_update_isolated_from_source_and_source_remains_mutable(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("exp", from_branch="main")
    exp = ctx.checkout("exp")
    main = ctx.checkout("main")

    exp.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 15, "sku": "abc"},
    )
    main.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 12, "sku": "abc"},
    )

    assert _product(exp, "abc")["price"] == 15
    assert _product(main, "abc")["price"] == 12


def test_many_transactions_can_mutate_same_branch(ctx: ChronosBranchContext) -> None:
    ctx.create_branch("work", from_branch="main")
    session = ctx.checkout("work")

    with session.transaction():
        session.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 30, "sku": "abc"},
        )
        session.execute(
            """
            INSERT INTO products (sku, name, price)
            VALUES (:sku, :name, :price)
            """,
            {"sku": "ghi", "name": "Gamma", "price": 40},
        )

    session.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 41, "sku": "ghi"},
    )

    assert _product(session, "abc")["price"] == 30
    assert _product(session, "ghi")["price"] == 41
    assert ctx.checkout("main").query("SELECT * FROM products WHERE sku = 'ghi'") == []


def test_rollback_discards_branch_writes(ctx: ChronosBranchContext) -> None:
    ctx.create_branch("work", from_branch="main")
    session = ctx.checkout("work")

    with pytest.raises(RuntimeError):
        with session.transaction():
            session.execute(
                "UPDATE products SET price = :price WHERE sku = :sku",
                {"price": 99, "sku": "abc"},
            )
            raise RuntimeError("abort")

    assert _product(session, "abc")["price"] == 10


def test_delete_is_logical_and_hides_inherited_rows(ctx: ChronosBranchContext) -> None:
    ctx.create_branch("exp", from_branch="main")
    exp = ctx.checkout("exp")

    exp.execute("DELETE FROM products WHERE sku = :sku", {"sku": "abc"})

    assert exp.query("SELECT * FROM products WHERE sku = 'abc'") == []
    assert _product(ctx.checkout("main"), "abc")["price"] == 10


def test_delete_then_branch_out_inherits_tombstone_and_parent_can_resurrect(
    ctx: ChronosBranchContext,
) -> None:
    main = ctx.checkout("main")
    main.execute("DELETE FROM products WHERE sku = :sku", {"sku": "abc"})
    ctx.create_branch("after_delete", from_branch="main")
    child = ctx.checkout("after_delete")

    assert main.query("SELECT * FROM products WHERE sku = 'abc'") == []
    assert child.query("SELECT * FROM products WHERE sku = 'abc'") == []

    main.execute(
        "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
        {"sku": "abc", "name": "Alpha v2", "price": 17},
    )

    assert _product(main, "abc") == {"sku": "abc", "name": "Alpha v2", "price": 17}
    assert child.query("SELECT * FROM products WHERE sku = 'abc'") == []


def test_child_can_resurrect_deleted_inherited_row_without_affecting_parent(
    ctx: ChronosBranchContext,
) -> None:
    main = ctx.checkout("main")
    main.execute("DELETE FROM products WHERE sku = :sku", {"sku": "abc"})
    ctx.create_branch("child", from_branch="main")
    child = ctx.checkout("child")

    child.execute(
        "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
        {"sku": "abc", "name": "Child Alpha", "price": 18},
    )

    assert _product(child, "abc") == {
        "sku": "abc",
        "name": "Child Alpha",
        "price": 18,
    }
    assert main.query("SELECT * FROM products WHERE sku = 'abc'") == []


def test_graph_branch_mutations_are_isolated_from_main(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("graph_exp", from_branch="main")
    graph = ctx.checkout("graph_exp")

    with graph.transaction():
        graph.execute(
            "UPDATE nodes SET score = :score WHERE node_id = :node_id",
            {"score": 20, "node_id": "n2"},
        )
        graph.execute("DELETE FROM edges WHERE edge_id = :edge_id", {"edge_id": "e1"})
        graph.execute(
            """
            INSERT INTO nodes (node_id, label, score)
            VALUES (:node_id, :label, :score)
            """,
            {"node_id": "n3", "label": "three", "score": 3},
        )
        graph.execute(
            """
            INSERT INTO edges (edge_id, src_id, dst_id)
            VALUES (:edge_id, :src_id, :dst_id)
            """,
            {"edge_id": "e2", "src_id": "n2", "dst_id": "n3"},
        )

    assert graph.query(
        """
        SELECT e.edge_id, e.src_id, e.dst_id, n.label AS dst_label
        FROM edges AS e
        JOIN nodes AS n ON n.node_id = e.dst_id
        ORDER BY e.edge_id
        """
    ) == [{"edge_id": "e2", "src_id": "n2", "dst_id": "n3", "dst_label": "three"}]
    assert graph.query("SELECT node_id, score FROM nodes ORDER BY node_id") == [
        {"node_id": "n1", "score": 1},
        {"node_id": "n2", "score": 20},
        {"node_id": "n3", "score": 3},
    ]

    main = ctx.checkout("main")
    assert main.query("SELECT edge_id, src_id, dst_id FROM edges ORDER BY edge_id") == [
        {"edge_id": "e1", "src_id": "n1", "dst_id": "n2"}
    ]
    assert main.query("SELECT node_id, score FROM nodes ORDER BY node_id") == [
        {"node_id": "n1", "score": 1},
        {"node_id": "n2", "score": 2},
    ]


def test_graph_child_branch_inherits_fork_point_not_later_parent_changes(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("graph_parent", from_branch="main")
    parent = ctx.checkout("graph_parent")
    parent.execute(
        """
        INSERT INTO nodes (node_id, label, score)
        VALUES (:node_id, :label, :score)
        """,
        {"node_id": "n3", "label": "three", "score": 3},
    )
    parent.execute(
        """
        INSERT INTO edges (edge_id, src_id, dst_id)
        VALUES (:edge_id, :src_id, :dst_id)
        """,
        {"edge_id": "e2", "src_id": "n2", "dst_id": "n3"},
    )

    ctx.create_branch("graph_child", from_branch="graph_parent")
    child = ctx.checkout("graph_child")

    parent.execute(
        "UPDATE nodes SET label = :label, score = :score WHERE node_id = :node_id",
        {"label": "three parent", "score": 30, "node_id": "n3"},
    )
    parent.execute(
        """
        INSERT INTO edges (edge_id, src_id, dst_id)
        VALUES (:edge_id, :src_id, :dst_id)
        """,
        {"edge_id": "e3", "src_id": "n3", "dst_id": "n1"},
    )
    child.execute("DELETE FROM edges WHERE edge_id = :edge_id", {"edge_id": "e2"})
    child.execute(
        "UPDATE nodes SET label = :label, score = :score WHERE node_id = :node_id",
        {"label": "three child", "score": 13, "node_id": "n3"},
    )

    assert parent.query(
        "SELECT node_id, label, score FROM nodes WHERE node_id = 'n3'"
    ) == [{"node_id": "n3", "label": "three parent", "score": 30}]
    assert parent.query("SELECT edge_id FROM edges ORDER BY edge_id") == [
        {"edge_id": "e1"},
        {"edge_id": "e2"},
        {"edge_id": "e3"},
    ]
    assert child.query(
        "SELECT node_id, label, score FROM nodes WHERE node_id = 'n3'"
    ) == [{"node_id": "n3", "label": "three child", "score": 13}]
    assert child.query("SELECT edge_id FROM edges ORDER BY edge_id") == [
        {"edge_id": "e1"}
    ]


def test_graph_recursive_reachability_uses_branch_visible_edges(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("graph_walk", from_branch="main")
    graph = ctx.checkout("graph_walk")
    with graph.transaction():
        graph.execute(
            "INSERT INTO nodes (node_id, label, score) VALUES (:node_id, :label, :score)",
            {"node_id": "n3", "label": "three", "score": 3},
        )
        graph.execute(
            "INSERT INTO nodes (node_id, label, score) VALUES (:node_id, :label, :score)",
            {"node_id": "n4", "label": "four", "score": 4},
        )
        graph.execute(
            "INSERT INTO edges (edge_id, src_id, dst_id) VALUES (:edge_id, :src_id, :dst_id)",
            {"edge_id": "e2", "src_id": "n2", "dst_id": "n3"},
        )
        graph.execute(
            "INSERT INTO edges (edge_id, src_id, dst_id) VALUES (:edge_id, :src_id, :dst_id)",
            {"edge_id": "e3", "src_id": "n3", "dst_id": "n4"},
        )

    assert graph.query(
        """
        WITH RECURSIVE reach(node_id, depth) AS (
          SELECT dst_id, 1
          FROM edges
          WHERE src_id = :start
          UNION ALL
          SELECT e.dst_id, r.depth + 1
          FROM edges AS e
          JOIN reach AS r ON e.src_id = r.node_id
          WHERE r.depth < 4
        )
        SELECT node_id, MIN(depth) AS depth
        FROM reach
        GROUP BY node_id
        ORDER BY depth, node_id
        """,
        {"start": "n1"},
    ) == [
        {"node_id": "n2", "depth": 1},
        {"node_id": "n3", "depth": 2},
        {"node_id": "n4", "depth": 3},
    ]
    assert ctx.checkout("main").query(
        """
        WITH RECURSIVE reach(node_id, depth) AS (
          SELECT dst_id, 1
          FROM edges
          WHERE src_id = :start
          UNION ALL
          SELECT e.dst_id, r.depth + 1
          FROM edges AS e
          JOIN reach AS r ON e.src_id = r.node_id
          WHERE r.depth < 4
        )
        SELECT node_id, MIN(depth) AS depth
        FROM reach
        GROUP BY node_id
        ORDER BY depth, node_id
        """,
        {"start": "n1"},
    ) == [{"node_id": "n2", "depth": 1}]


def test_graph_diff_and_merge_apply_nodes_and_edges(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("graph_merge", from_branch="main")
    graph = ctx.checkout("graph_merge")
    with graph.transaction():
        graph.execute(
            "UPDATE nodes SET score = :score WHERE node_id = :node_id",
            {"score": 11, "node_id": "n1"},
        )
        graph.execute("DELETE FROM edges WHERE edge_id = :edge_id", {"edge_id": "e1"})
        graph.execute(
            "INSERT INTO nodes (node_id, label, score) VALUES (:node_id, :label, :score)",
            {"node_id": "n3", "label": "three", "score": 3},
        )
        graph.execute(
            "INSERT INTO edges (edge_id, src_id, dst_id) VALUES (:edge_id, :src_id, :dst_id)",
            {"edge_id": "e2", "src_id": "n1", "dst_id": "n3"},
        )

    node_changes = {(change.key["node_id"], change.change) for change in ctx.diff_rows("main", "graph_merge", "nodes")}
    edge_changes = {(change.key["edge_id"], change.change) for change in ctx.diff_rows("main", "graph_merge", "edges")}
    assert node_changes == {("n1", "modified"), ("n3", "added")}
    assert edge_changes == {("e1", "deleted"), ("e2", "added")}

    assert ctx.merge_apply(source="graph_merge", target="main").applied == 4
    main = ctx.checkout("main")
    assert main.query("SELECT node_id, score FROM nodes ORDER BY node_id") == [
        {"node_id": "n1", "score": 11},
        {"node_id": "n2", "score": 2},
        {"node_id": "n3", "score": 3},
    ]
    assert main.query("SELECT edge_id, src_id, dst_id FROM edges ORDER BY edge_id") == [
        {"edge_id": "e2", "src_id": "n1", "dst_id": "n3"}
    ]


def test_join_subquery_ordering_and_aggregation_use_logical_tables(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("exp", from_branch="main")
    exp = ctx.checkout("exp")
    exp.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 15, "sku": "abc"},
    )

    joined = exp.query(
        """
        SELECT p.sku, p.price, o.quantity
        FROM products AS p
        JOIN orders AS o ON p.sku = o.sku
        WHERE p.price >= :min_price
        ORDER BY p.sku
        """,
        {"min_price": 10},
    )
    aggregate = exp.query(
        """
        SELECT COUNT(*) AS count, SUM(price) AS total
        FROM products
        WHERE sku IN (SELECT sku FROM orders WHERE quantity >= :quantity)
        """,
        {"quantity": 1},
    )

    assert joined == [
        {"sku": "abc", "price": 15, "quantity": 2},
        {"sku": "def", "price": 20, "quantity": 1},
    ]
    assert aggregate == [{"count": 2, "total": 35}]


def test_complex_join_cte_group_by_and_anti_join_after_branch_writes(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("analytics", from_branch="main")
    session = ctx.checkout("analytics")
    with session.transaction():
        session.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 15, "sku": "abc"},
        )
        session.execute("DELETE FROM orders WHERE order_id = :order_id", {"order_id": "o2"})
        session.execute(
            "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
            {"sku": "ghi", "name": "Gamma", "price": 7},
        )
        session.execute(
            """
            INSERT INTO orders (order_id, sku, quantity)
            VALUES (:order_id, :sku, :quantity)
            """,
            {"order_id": "o3", "sku": "ghi", "quantity": 5},
        )

    revenue = session.query(
        """
        WITH revenue AS (
          SELECT p.sku, p.name, SUM(p.price * o.quantity) AS total
          FROM products AS p
          JOIN orders AS o ON o.sku = p.sku
          GROUP BY p.sku, p.name
        )
        SELECT sku, name, total
        FROM revenue
        WHERE total >= :min_total
        ORDER BY total DESC, sku
        """,
        {"min_total": 10},
    )
    missing_orders = session.query(
        """
        SELECT p.sku
        FROM products AS p
        WHERE NOT EXISTS (
          SELECT 1 FROM orders AS o WHERE o.sku = p.sku
        )
        ORDER BY p.sku
        """
    )
    main_revenue = ctx.checkout("main").query(
        """
        SELECT p.sku, SUM(p.price * o.quantity) AS total
        FROM products p
        JOIN orders o ON p.sku = o.sku
        GROUP BY p.sku
        ORDER BY p.sku
        """
    )

    assert revenue == [
        {"sku": "ghi", "name": "Gamma", "total": 35},
        {"sku": "abc", "name": "Alpha", "total": 30},
    ]
    assert missing_orders == [{"sku": "def"}]
    assert main_revenue == [
        {"sku": "abc", "total": 20},
        {"sku": "def", "total": 20},
    ]


def test_left_join_aggregation_sees_branch_local_deletes_and_inserts(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("report", from_branch="main")
    session = ctx.checkout("report")
    session.execute("DELETE FROM orders WHERE order_id = :order_id", {"order_id": "o1"})
    session.execute(
        "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
        {"sku": "zzz", "name": "No Orders", "price": 100},
    )

    rows = session.query(
        """
        SELECT p.sku, COUNT(o.order_id) AS order_count, COALESCE(SUM(o.quantity), 0) AS units
        FROM products AS p
        LEFT JOIN orders AS o ON o.sku = p.sku
        GROUP BY p.sku
        ORDER BY p.sku
        """
    )

    assert rows == [
        {"sku": "abc", "order_count": 0, "units": 0},
        {"sku": "def", "order_count": 1, "units": 1},
        {"sku": "zzz", "order_count": 0, "units": 0},
    ]


def test_subquery_driven_update_and_delete_match_branch_view(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("mutate", from_branch="main")
    session = ctx.checkout("mutate")
    session.execute("DELETE FROM orders WHERE order_id = :order_id", {"order_id": "o2"})
    session.execute(
        """
        UPDATE products
        SET price = :price
        WHERE sku IN (
          SELECT sku FROM orders WHERE quantity >= :min_quantity
        )
        """,
        {"price": 99, "min_quantity": 2},
    )
    deleted = session.execute(
        """
        DELETE FROM products
        WHERE sku NOT IN (SELECT sku FROM orders)
        """
    )

    assert deleted.rowcount == 1
    assert session.query("SELECT sku, price FROM products ORDER BY sku") == [
        {"sku": "abc", "price": 99}
    ]
    assert ctx.checkout("main").query("SELECT sku, price FROM products ORDER BY sku") == [
        {"sku": "abc", "price": 10},
        {"sku": "def", "price": 20},
    ]


def test_noop_update_and_delete_return_zero_and_do_not_fragment_state(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("noop", from_branch="main")
    session = ctx.checkout("noop")

    updated = session.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 1000, "sku": "missing"},
    )
    deleted = session.execute("DELETE FROM orders WHERE order_id = :order_id", {"order_id": "missing"})

    assert updated.rowcount == 0
    assert deleted.rowcount == 0
    assert session.query("SELECT sku, price FROM products ORDER BY sku") == [
        {"sku": "abc", "price": 10},
        {"sku": "def", "price": 20},
    ]
    assert session.query("SELECT order_id, sku FROM orders ORDER BY order_id") == [
        {"order_id": "o1", "sku": "abc"},
        {"order_id": "o2", "sku": "def"},
    ]


def test_multi_table_transaction_rollback_restores_all_tables(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("rollback_all", from_branch="main")
    session = ctx.checkout("rollback_all")

    with pytest.raises(RuntimeError):
        with session.transaction():
            session.execute(
                "UPDATE products SET price = :price WHERE sku = :sku",
                {"price": 55, "sku": "abc"},
            )
            session.execute("DELETE FROM orders WHERE order_id = :order_id", {"order_id": "o1"})
            session.execute(
                "INSERT INTO nodes (node_id, label, score) VALUES (:node_id, :label, :score)",
                {"node_id": "n3", "label": "three", "score": 3},
            )
            raise RuntimeError("abort")

    assert _product(session, "abc")["price"] == 10
    assert session.query("SELECT order_id FROM orders ORDER BY order_id") == [
        {"order_id": "o1"},
        {"order_id": "o2"},
    ]
    assert session.query("SELECT node_id FROM nodes ORDER BY node_id") == [
        {"node_id": "n1"},
        {"node_id": "n2"},
    ]


def test_repeated_delete_and_reinsert_same_key_across_forks(ctx: ChronosBranchContext) -> None:
    ctx.create_branch("left", from_branch="main")
    left = ctx.checkout("left")
    left.execute("DELETE FROM products WHERE sku = :sku", {"sku": "abc"})
    ctx.create_branch("right", from_branch="left")
    right = ctx.checkout("right")

    left.execute(
        "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
        {"sku": "abc", "name": "Left Alpha", "price": 31},
    )
    right.execute(
        "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
        {"sku": "abc", "name": "Right Alpha", "price": 32},
    )
    left.execute("DELETE FROM products WHERE sku = :sku", {"sku": "abc"})

    assert left.query("SELECT * FROM products WHERE sku = 'abc'") == []
    assert _product(right, "abc") == {
        "sku": "abc",
        "name": "Right Alpha",
        "price": 32,
    }
    assert _product(ctx.checkout("main"), "abc")["price"] == 10


def test_deduplicating_backends_match_copy_reference_on_complex_scenario(
    sql_backend: str,
) -> None:
    def run_scenario(backend: str) -> dict[str, list[dict]]:
        ctx = _make_context(sql_backend, backend)
        try:
            ctx.create_index("products", ["price", "sku"], name="price_sku")
            ctx.create_branch("exp", from_branch="main")
            exp = ctx.checkout("exp")
            with exp.transaction():
                exp.execute(
                    "UPDATE products SET price = :price WHERE sku = :sku",
                    {"price": 15, "sku": "abc"},
                )
                exp.execute("DELETE FROM orders WHERE order_id = :order_id", {"order_id": "o2"})
                exp.execute(
                    "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
                    {"sku": "ghi", "name": "Gamma", "price": 7},
                )
                exp.execute(
                    "INSERT INTO orders (order_id, sku, quantity) VALUES (:order_id, :sku, :quantity)",
                    {"order_id": "o3", "sku": "ghi", "quantity": 5},
                )
            ctx.create_branch("child", from_branch="exp")
            child = ctx.checkout("child")
            child.execute("DELETE FROM products WHERE sku = :sku", {"sku": "abc"})
            child.execute(
                "UPDATE products SET price = :price WHERE sku IN (SELECT sku FROM orders)",
                {"price": 33},
            )
            return {
                "main_products": ctx.checkout("main").query(
                    "SELECT sku, price FROM products ORDER BY sku"
                ),
                "exp_revenue": exp.query(
                    """
                    SELECT p.sku, SUM(p.price * o.quantity) AS revenue
                    FROM products p
                    JOIN orders o ON p.sku = o.sku
                    GROUP BY p.sku
                    ORDER BY p.sku
                    """
                ),
                "child_left_join": child.query(
                    """
                    SELECT p.sku, COUNT(o.order_id) AS orders, COALESCE(SUM(o.quantity), 0) AS units
                    FROM products p
                    LEFT JOIN orders o ON p.sku = o.sku
                    GROUP BY p.sku
                    ORDER BY p.sku
                    """
                ),
            }
        finally:
            ctx.close()

    reference = run_scenario("copy")
    assert run_scenario("interval") == reference
    assert run_scenario("log") == reference


def test_log_backend_reads_do_not_use_temp_tables(sql_backend: str) -> None:
    ctx = _make_context(sql_backend, "log")
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 15, "sku": "abc"},
        )
        exp.execute("DELETE FROM orders WHERE order_id = :order_id", {"order_id": "o2"})
        exp.execute(
            "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
            {"sku": "ghi", "name": "Gamma", "price": 7},
        )
        exp.execute(
            "INSERT INTO orders (order_id, sku, quantity) VALUES (:order_id, :sku, :quantity)",
            {"order_id": "o3", "sku": "ghi", "quantity": 5},
        )

        def fail_temp(*args, **kwargs):
            raise AssertionError("log read path should not use temp tables")

        ctx.db.create_temp_table = fail_temp  # type: ignore[method-assign]
        ctx.db.drop_table = fail_temp  # type: ignore[method-assign]

        assert exp.query(
            """
            SELECT p.sku, SUM(p.price * o.quantity) AS revenue
            FROM products p
            JOIN orders o ON p.sku = o.sku
            GROUP BY p.sku
            ORDER BY p.sku
            """
        ) == [
            {"sku": "abc", "revenue": 30},
            {"sku": "ghi", "revenue": 35},
        ]
    finally:
        ctx.close()


def test_log_backend_does_not_create_txn_allocator_table(sql_backend: str) -> None:
    ctx = _make_context(sql_backend, "log")
    try:
        exp = ctx.checkout("main")
        exp.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 13, "sku": "abc"},
        )

        assert not _table_exists(ctx, "_chronos_branch_log_txns")
        assert _product(exp, "abc")["price"] == 13
    finally:
        ctx.close()


def test_checkpoint_is_stable_after_later_branch_writes(ctx: ChronosBranchContext) -> None:
    ctx.create_branch("exp", from_branch="main")
    exp = ctx.checkout("exp")
    exp.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 15, "sku": "abc"},
    )
    ctx.create_checkpoint("before_more_work", branch="exp")
    exp.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 25, "sku": "abc"},
    )

    checkpoint = ctx.checkout_checkpoint("before_more_work")
    assert _product(checkpoint, "abc")["price"] == 15
    assert _product(exp, "abc")["price"] == 25
    with pytest.raises(BranchingError):
        checkpoint.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 1, "sku": "abc"},
        )


def test_create_branch_from_checkpoint(ctx: ChronosBranchContext) -> None:
    ctx.create_branch("exp", from_branch="main")
    exp = ctx.checkout("exp")
    exp.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 15, "sku": "abc"},
    )
    ctx.create_checkpoint("snap", branch="exp")
    exp.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 25, "sku": "abc"},
    )
    ctx.create_branch_from_checkpoint("restored", "snap")

    restored = ctx.checkout("restored")
    restored.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 16, "sku": "abc"},
    )

    assert _product(restored, "abc")["price"] == 16
    assert _product(exp, "abc")["price"] == 25


def test_diff_rows_and_merge_apply(ctx: ChronosBranchContext) -> None:
    ctx.create_branch("exp", from_branch="main")
    exp = ctx.checkout("exp")
    exp.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 15, "sku": "abc"},
    )
    exp.execute("DELETE FROM products WHERE sku = :sku", {"sku": "def"})
    exp.execute(
        "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
        {"sku": "ghi", "name": "Gamma", "price": 40},
    )

    changes = ctx.diff_rows("main", "exp", "products")
    assert [(c.key, c.change) for c in changes] == [
        ({"sku": "abc"}, "modified"),
        ({"sku": "def"}, "deleted"),
        ({"sku": "ghi"}, "added"),
    ]
    preview = ctx.merge_preview(source="exp", target="main")
    assert not preview.conflicts
    assert ctx.merge_apply(source="exp", target="main").applied == 3

    main = ctx.checkout("main")
    assert _product(main, "abc")["price"] == 15
    assert main.query("SELECT * FROM products WHERE sku = 'def'") == []
    assert _product(main, "ghi")["price"] == 40


def test_duplicate_insert_and_duplicate_branch_errors(ctx: ChronosBranchContext) -> None:
    session = ctx.checkout("main")
    with pytest.raises(DuplicateKeyError):
        session.execute(
            "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
            {"sku": "abc", "name": "Other", "price": 1},
        )

    ctx.create_branch("exp", from_branch="main")
    with pytest.raises(BranchAlreadyExistsError):
        ctx.create_branch("exp", from_branch="main")


def test_unsupported_write_expression_is_rejected(ctx: ChronosBranchContext) -> None:
    session = ctx.checkout("main")
    with pytest.raises(UnsupportedSQLError):
        session.execute("UPDATE products SET price = price + 1 WHERE sku = 'abc'")


def test_deep_branch_chain_has_isolated_leaf_state(ctx: ChronosBranchContext) -> None:
    parent = "main"
    for index in range(12):
        child = f"b{index}"
        ctx.create_branch(child, from_branch=parent)
        ctx.checkout(child).execute(
            "UPDATE nodes SET score = :score WHERE node_id = :node_id",
            {"score": index + 10, "node_id": "n1"},
        )
        parent = child

    assert ctx.checkout("b11").query(
        "SELECT score FROM nodes WHERE node_id = 'n1'"
    ) == [{"score": 21}]
    assert ctx.checkout("b5").query(
        "SELECT score FROM nodes WHERE node_id = 'n1'"
    ) == [{"score": 15}]
    assert ctx.checkout("main").query(
        "SELECT score FROM nodes WHERE node_id = 'n1'"
    ) == [{"score": 1}]


def test_parent_writes_after_child_fork_do_not_leak_to_child(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("child", from_branch="main")
    child = ctx.checkout("child")
    main = ctx.checkout("main")

    main.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 99, "sku": "abc"},
    )

    assert _product(child, "abc")["price"] == 10
    assert _product(main, "abc")["price"] == 99


def test_interval_backend_splices_fragments_without_copying_whole_table(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    assert _physical_change_count(ctx, "products") == 2
    ctx.create_branch("exp", from_branch="main")
    ctx.create_branch("child", from_branch="exp")
    assert _physical_change_count(ctx, "products") == 2
    ctx.checkout("exp").execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 15, "sku": "abc"},
    )

    fragments = ctx.db.execute(
        """
        SELECT sku, price, live_lo, live_hi, deleted
        FROM _chronos_b_interval_products
        WHERE sku = 'abc'
        ORDER BY live_lo
        """
    ).fetchall()
    assert len(fragments) == 3
    assert [row["price"] for row in fragments] == [10, 15, 10]
    assert all(not row["deleted"] for row in fragments)
    assert _physical_change_count(ctx, "products") == 4
    ctx.close()


def test_interval_session_reuses_prepared_segment_metadata(sql_backend: str) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    ctx.create_branch("exp", from_branch="main")

    session = ctx.checkout("exp")
    metadata_reads = 0
    original_execute = ctx.db.execute

    def counting_execute(sql, params=()):
        nonlocal metadata_reads
        if (
            "_chronos_branch_interval_branches" in sql
            or "_chronos_branch_interval_segments" in sql
        ) and sql.lstrip().upper().startswith("SELECT"):
            metadata_reads += 1
        return original_execute(sql, params)

    ctx.db.execute = counting_execute  # type: ignore[method-assign]
    for _ in range(5):
        assert session.query("SELECT price FROM products WHERE sku = 'abc'") == [
            {"price": 10}
        ]
    session.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 11, "sku": "abc"},
    )
    assert session.query("SELECT price FROM products WHERE sku = 'abc'") == [
        {"price": 11}
    ]

    assert metadata_reads == 0
    ctx.close()


def test_log_backend_branching_shares_log_prefix_and_appends_only_writes(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "log")
    assert _physical_change_count(ctx, "products") == 2
    ctx.create_branch("exp", from_branch="main")
    assert _physical_change_count(ctx, "products") == 2
    ctx.checkout("main").execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 99, "sku": "abc"},
    )
    ctx.checkout("exp").execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 15, "sku": "abc"},
    )

    assert _physical_change_count(ctx, "products") == 4
    assert _product(ctx.checkout("main"), "abc")["price"] == 99
    assert _product(ctx.checkout("exp"), "abc")["price"] == 15
    ctx.close()


def test_log_session_reuses_lineage_for_reads_and_refreshes_after_writes(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "log")
    ctx.create_branch("exp", from_branch="main")

    session = ctx.checkout("exp")
    branch_reads = 0
    original_execute = ctx.db.execute

    def counting_execute(sql, params=()):
        nonlocal branch_reads
        if "_chronos_branch_log_branches" in sql and sql.lstrip().upper().startswith("SELECT"):
            branch_reads += 1
        return original_execute(sql, params)

    ctx.db.execute = counting_execute  # type: ignore[method-assign]
    for _ in range(5):
        assert session.query("SELECT price FROM products WHERE sku = 'abc'") == [
            {"price": 10}
        ]
    assert branch_reads == 0

    session.execute(
        """
        INSERT INTO products (sku, name, price)
        VALUES (:sku, :name, :price)
        """,
        {"sku": "xyz", "name": "Xray", "price": 30},
    )
    assert session.query("SELECT price FROM products WHERE sku = 'xyz'") == [
        {"price": 30}
    ]
    assert branch_reads > 0
    ctx.close()
