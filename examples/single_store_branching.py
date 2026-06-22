from __future__ import annotations

from chronos_core.branching import ChronosBranchContext


def main() -> None:
    chronos = ChronosBranchContext.connect("sqlite:///:memory:", backend="interval")
    try:
        chronos.db.execute(
            """
            CREATE TABLE products (
              sku TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              price INTEGER NOT NULL,
              stock INTEGER NOT NULL
            )
            """
        )
        chronos.db.execute(
            "INSERT INTO products VALUES (?, ?, ?, ?)",
            ("abc", "Keyboard", 100, 5),
        )
        chronos.db.commit()
        chronos.register_table("products", primary_key=["sku"])

        chronos.create_branch("agent", from_branch="main")
        agent = chronos.checkout("agent")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 90, "sku": "abc"},
        )

        main = chronos.checkout("main")
        assert main.query("SELECT sku, price FROM products") == [
            {"sku": "abc", "price": 100},
        ]
        assert agent.query("SELECT sku, price FROM products") == [
            {"sku": "abc", "price": 90},
        ]

        preview = chronos.merge_preview("agent", "main")
        assert len(preview.changes) == 1
        assert preview.conflicts == []

        result = chronos.merge_apply(source="agent", target="main")
        assert result.applied == 1
        assert chronos.checkout("main").query("SELECT sku, price FROM products") == [
            {"sku": "abc", "price": 90},
        ]
    finally:
        chronos.close()

    print("single-store branching example passed")


if __name__ == "__main__":
    main()
