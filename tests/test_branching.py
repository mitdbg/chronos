from __future__ import annotations

import pytest

from janus_core.branching import (
    BranchAlreadyExistsError,
    BranchingError,
    DuplicateKeyError,
    TableNotRegisteredError,
    JanusBranchContext,
    UnsupportedSQLError,
)


BACKENDS = ("copy", "interval", "log")


def _make_context(backend: str) -> JanusBranchContext:
    context = JanusBranchContext.connect("sqlite:///:memory:", backend=backend)
    conn = context.conn
    conn.execute(
        "CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)"
    )
    conn.execute(
        "CREATE TABLE orders (order_id TEXT PRIMARY KEY, sku TEXT, quantity INTEGER)"
    )
    conn.execute(
        "CREATE TABLE nodes (node_id TEXT PRIMARY KEY, label TEXT, score INTEGER)"
    )
    conn.execute(
        "CREATE TABLE edges (edge_id TEXT PRIMARY KEY, src_id TEXT, dst_id TEXT)"
    )
    conn.executemany(
        "INSERT INTO products VALUES (?, ?, ?)",
        [("abc", "Alpha", 10), ("def", "Delta", 20)],
    )
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?)",
        [("o1", "abc", 2), ("o2", "def", 1)],
    )
    conn.executemany(
        "INSERT INTO nodes VALUES (?, ?, ?)",
        [("n1", "one", 1), ("n2", "two", 2)],
    )
    conn.execute("INSERT INTO edges VALUES (?, ?, ?)", ("e1", "n1", "n2"))
    conn.commit()
    context.register_table("products", ["sku"])
    context.register_table("orders", ["order_id"])
    context.register_table("nodes", ["node_id"])
    context.register_table("edges", ["edge_id"])
    return context


@pytest.fixture(params=BACKENDS)
def ctx(request) -> JanusBranchContext:
    context = _make_context(request.param)
    yield context
    context.close()


def _product(session, sku: str) -> dict:
    rows = session.query(
        "SELECT sku, name, price FROM products WHERE sku = :sku", {"sku": sku}
    )
    assert len(rows) == 1
    return rows[0]


def _physical_change_count(ctx: JanusBranchContext, table: str) -> int:
    if ctx.backend_name == "interval":
        return ctx.conn.execute(
            f"SELECT COUNT(*) FROM _janus_b_interval_{table}"
        ).fetchone()[0]
    if ctx.backend_name == "log":
        return ctx.conn.execute(f"SELECT COUNT(*) FROM _janus_b_log_{table}").fetchone()[0]
    backend = ctx._backend  # type: ignore[attr-defined]
    return sum(
        ctx.conn.execute(
            f"SELECT COUNT(*) FROM {backend._branch_table(branch.branch_id, table)}"
        ).fetchone()[0]
        for branch in ctx.list_branches()
    )


def test_create_branch_storage_behavior_for_user_records(ctx: JanusBranchContext) -> None:
    before = _physical_change_count(ctx, "products")
    ctx.create_branch("exp", from_branch="main")
    after = _physical_change_count(ctx, "products")

    if ctx.backend_name == "copy":
        assert after > before
    else:
        assert before == after
    assert {branch.branch_id for branch in ctx.list_branches()} == {"main", "exp"}


def test_users_can_add_logical_indexes_to_registered_tables(ctx: JanusBranchContext) -> None:
    index = ctx.create_index("products", ["price", "sku"], name="products_price_sku")

    assert index.name == "products_price_sku"
    assert index.table == "products"
    assert index.columns == ("price", "sku")
    assert ctx.list_indexes("products") == [index]

    if ctx.backend_name == "copy":
        assert ctx.conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'index'
              AND name LIKE '_janus_idx_copy_%products_price_sku_%'
            """
        ).fetchone() is not None
    else:
        sqlite_index_name = (
            "_janus_idx_interval_products_price_sku"
            if ctx.backend_name == "interval"
            else "_janus_idx_log_products_price_sku"
        )
        assert ctx.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?",
            (sqlite_index_name,),
        ).fetchone() is not None
    assert ctx.checkout("main").query(
        "SELECT sku FROM products WHERE price >= :price ORDER BY sku",
        {"price": 10},
    ) == [{"sku": "abc"}, {"sku": "def"}]


def test_indexes_validate_registered_tables_and_columns(ctx: JanusBranchContext) -> None:
    with pytest.raises(TableNotRegisteredError):
        ctx.create_index("missing", ["id"])
    with pytest.raises(TableNotRegisteredError):
        ctx.create_index("products", ["missing"])
    with pytest.raises(ValueError):
        ctx.create_index("products", [])


def test_branch_update_isolated_from_source_and_source_remains_mutable(
    ctx: JanusBranchContext,
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


def test_many_transactions_can_mutate_same_branch(ctx: JanusBranchContext) -> None:
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


def test_rollback_discards_branch_writes(ctx: JanusBranchContext) -> None:
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


def test_delete_is_logical_and_hides_inherited_rows(ctx: JanusBranchContext) -> None:
    ctx.create_branch("exp", from_branch="main")
    exp = ctx.checkout("exp")

    exp.execute("DELETE FROM products WHERE sku = :sku", {"sku": "abc"})

    assert exp.query("SELECT * FROM products WHERE sku = 'abc'") == []
    assert _product(ctx.checkout("main"), "abc")["price"] == 10


def test_delete_then_branch_out_inherits_tombstone_and_parent_can_resurrect(
    ctx: JanusBranchContext,
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
    ctx: JanusBranchContext,
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


def test_join_subquery_ordering_and_aggregation_use_logical_tables(
    ctx: JanusBranchContext,
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
    ctx: JanusBranchContext,
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
    ctx: JanusBranchContext,
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
    ctx: JanusBranchContext,
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
    ctx: JanusBranchContext,
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
    ctx: JanusBranchContext,
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


def test_repeated_delete_and_reinsert_same_key_across_forks(ctx: JanusBranchContext) -> None:
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


def test_deduplicating_backends_match_copy_reference_on_complex_scenario() -> None:
    def run_scenario(backend: str) -> dict[str, list[dict]]:
        ctx = _make_context(backend)
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


def test_log_backend_reads_do_not_use_temp_tables() -> None:
    ctx = _make_context("log")
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


def test_log_backend_does_not_create_txn_allocator_table() -> None:
    ctx = _make_context("log")
    try:
        exp = ctx.checkout("main")
        exp.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 13, "sku": "abc"},
        )

        assert ctx.conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table'
              AND name = '_janus_branch_log_txns'
            """
        ).fetchone() is None
        assert _product(exp, "abc")["price"] == 13
    finally:
        ctx.close()


def test_checkpoint_is_stable_after_later_branch_writes(ctx: JanusBranchContext) -> None:
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


def test_create_branch_from_checkpoint(ctx: JanusBranchContext) -> None:
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


def test_diff_rows_and_merge_apply(ctx: JanusBranchContext) -> None:
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


def test_duplicate_insert_and_duplicate_branch_errors(ctx: JanusBranchContext) -> None:
    session = ctx.checkout("main")
    with pytest.raises(DuplicateKeyError):
        session.execute(
            "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
            {"sku": "abc", "name": "Other", "price": 1},
        )

    ctx.create_branch("exp", from_branch="main")
    with pytest.raises(BranchAlreadyExistsError):
        ctx.create_branch("exp", from_branch="main")


def test_unsupported_write_expression_is_rejected(ctx: JanusBranchContext) -> None:
    session = ctx.checkout("main")
    with pytest.raises(UnsupportedSQLError):
        session.execute("UPDATE products SET price = price + 1 WHERE sku = 'abc'")


def test_deep_branch_chain_has_isolated_leaf_state(ctx: JanusBranchContext) -> None:
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
    ctx: JanusBranchContext,
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


def test_interval_backend_splices_fragments_without_copying_whole_table() -> None:
    ctx = JanusBranchContext.connect("sqlite:///:memory:", backend="interval")
    conn = ctx.conn
    conn.execute(
        "CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)"
    )
    conn.executemany(
        "INSERT INTO products VALUES (?, ?, ?)",
        [("abc", "Alpha", 10), ("def", "Delta", 20)],
    )
    conn.commit()
    ctx.register_table("products", ["sku"])

    assert _physical_change_count(ctx, "products") == 2
    ctx.create_branch("exp", from_branch="main")
    ctx.create_branch("child", from_branch="exp")
    assert _physical_change_count(ctx, "products") == 2
    ctx.checkout("exp").execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 15, "sku": "abc"},
    )

    fragments = conn.execute(
        """
        SELECT sku, price, live_lo, live_hi, deleted
        FROM _janus_b_interval_products
        WHERE sku = 'abc'
        ORDER BY live_lo
        """
    ).fetchall()
    assert len(fragments) == 3
    assert [row["price"] for row in fragments] == [10, 15, 10]
    assert all(row["deleted"] == 0 for row in fragments)
    assert _physical_change_count(ctx, "products") == 4
    ctx.close()


def test_interval_session_reuses_prepared_segment_metadata() -> None:
    ctx = JanusBranchContext.connect("sqlite:///:memory:", backend="interval")
    conn = ctx.conn
    conn.execute(
        "CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)"
    )
    conn.executemany(
        "INSERT INTO products VALUES (?, ?, ?)",
        [("abc", "Alpha", 10), ("def", "Delta", 20)],
    )
    conn.commit()
    ctx.register_table("products", ["sku"])
    ctx.create_branch("exp", from_branch="main")

    session = ctx.checkout("exp")
    metadata_reads = 0
    original_execute = ctx.db.execute

    def counting_execute(sql, params=()):
        nonlocal metadata_reads
        if (
            "_janus_branch_interval_branches" in sql
            or "_janus_branch_interval_segments" in sql
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


def test_log_backend_branching_shares_log_prefix_and_appends_only_writes() -> None:
    ctx = JanusBranchContext.connect("sqlite:///:memory:", backend="log")
    conn = ctx.conn
    conn.execute(
        "CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)"
    )
    conn.executemany(
        "INSERT INTO products VALUES (?, ?, ?)",
        [("abc", "Alpha", 10), ("def", "Delta", 20)],
    )
    conn.commit()
    ctx.register_table("products", ["sku"])

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


def test_log_session_reuses_lineage_for_reads_and_refreshes_after_writes() -> None:
    ctx = JanusBranchContext.connect("sqlite:///:memory:", backend="log")
    conn = ctx.conn
    conn.execute(
        "CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)"
    )
    conn.executemany(
        "INSERT INTO products VALUES (?, ?, ?)",
        [("abc", "Alpha", 10), ("def", "Delta", 20)],
    )
    conn.commit()
    ctx.register_table("products", ["sku"])
    ctx.create_branch("exp", from_branch="main")

    session = ctx.checkout("exp")
    branch_reads = 0
    original_execute = ctx.db.execute

    def counting_execute(sql, params=()):
        nonlocal branch_reads
        if "_janus_branch_log_branches" in sql and sql.lstrip().upper().startswith("SELECT"):
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
