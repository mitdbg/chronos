from __future__ import annotations

import os

import pytest

from janus_core.branching import JanusBranchContext


POSTGRES_DSN = os.environ.get("JANUS_BRANCH_POSTGRES_DSN") or os.environ.get(
    "JANUS_POSTGRES_DSN"
)


pytestmark = pytest.mark.skipif(
    not POSTGRES_DSN,
    reason="set JANUS_BRANCH_POSTGRES_DSN or JANUS_POSTGRES_DSN to run PostgreSQL branching tests",
)


def _reset_public_schema() -> None:
    assert POSTGRES_DSN is not None
    ctx = JanusBranchContext.connect(POSTGRES_DSN, backend="interval")
    try:
        rows = ctx.db.execute(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
            """
        ).fetchall()
        for row in rows:
            ctx.db.drop_table(row["tablename"])
        ctx.db.commit()
    finally:
        ctx.close()


def _make_context(backend: str) -> JanusBranchContext:
    assert POSTGRES_DSN is not None
    _reset_public_schema()
    ctx = JanusBranchContext.connect(POSTGRES_DSN, backend=backend)
    db = ctx.db
    db.execute("CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)")
    db.execute(
        "CREATE TABLE orders (order_id TEXT PRIMARY KEY, sku TEXT, quantity INTEGER)"
    )
    db.executemany(
        "INSERT INTO products VALUES (?, ?, ?)",
        [("abc", "Alpha", 10), ("def", "Delta", 20)],
    )
    db.executemany(
        "INSERT INTO orders VALUES (?, ?, ?)",
        [("o1", "abc", 2), ("o2", "def", 1)],
    )
    db.commit()
    ctx.register_table("products", ["sku"])
    ctx.register_table("orders", ["order_id"])
    return ctx


@pytest.mark.parametrize("backend", ["interval", "log", "copy"])
def test_postgres_branching_backend_isolates_writes_and_supports_checkpoints(
    backend: str,
) -> None:
    ctx = _make_context(backend)
    try:
        ctx.create_index("products", ["price", "sku"], name=f"{backend}_price_sku")
        ctx.create_branch("exp", from_branch="main")

        exp = ctx.checkout("exp")
        with exp.transaction():
            exp.execute(
                "UPDATE products SET price = :price WHERE sku = :sku",
                {"price": 15, "sku": "abc"},
            )
            exp.execute(
                """
                INSERT INTO products (sku, name, price)
                VALUES (:sku, :name, :price)
                """,
                {"sku": "ghi", "name": "Gamma", "price": 30},
            )
            exp.execute(
                "DELETE FROM orders WHERE order_id = :order_id",
                {"order_id": "o1"},
            )

        assert exp.query("SELECT sku, price FROM products ORDER BY sku") == [
            {"sku": "abc", "price": 15},
            {"sku": "def", "price": 20},
            {"sku": "ghi", "price": 30},
        ]
        assert ctx.checkout("main").query(
            "SELECT sku, price FROM products ORDER BY sku"
        ) == [
            {"sku": "abc", "price": 10},
            {"sku": "def", "price": 20},
        ]
        assert exp.query("SELECT * FROM orders WHERE order_id = 'o1'") == []

        ctx.create_checkpoint("cp", branch="exp")
        exp.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 18, "sku": "abc"},
        )

        assert ctx.checkout_checkpoint("cp").query(
            "SELECT price FROM products WHERE sku = 'abc'"
        ) == [{"price": 15}]
        assert exp.query("SELECT price FROM products WHERE sku = 'abc'") == [
            {"price": 18}
        ]

        changes = {
            (change.table, tuple(change.key.items()), change.change)
            for change in ctx.diff("main", "exp").changes
        }
        assert ("products", (("sku", "abc"),), "modified") in changes
        assert ("products", (("sku", "ghi"),), "added") in changes
        assert ("orders", (("order_id", "o1"),), "deleted") in changes
    finally:
        ctx.close()
