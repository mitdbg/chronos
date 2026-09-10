from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from chronos_core.branching import ChronosBranchContext
from chronos_core.workspace import ChronosFSStore, ChronosWorkspaceContext


def main() -> None:
    with TemporaryDirectory(prefix="chronos-example-") as temp_dir:
        root = Path(temp_dir)
        sqlite = ChronosBranchContext.connect(
            f"sqlite:///{root / 'orders.sqlite'}",
            backend="interval",
        )
        filesystem = ChronosFSStore.connect(
            sqlite.db.database_url,
            backend="interval",
        )
        filesystem.ensure()
        chronos = ChronosWorkspaceContext(
            sqlite=sqlite, filesystem=filesystem,
            shared_metadata_url=sqlite.db.database_url,
        )

        try:
            sqlite.db.execute(
                """
                CREATE TABLE orders (
                  id INTEGER PRIMARY KEY,
                  status TEXT NOT NULL,
                  stock INTEGER NOT NULL
                )
                """
            )
            sqlite.db.execute("INSERT INTO orders VALUES (?, ?, ?)", (7, "new", 3))
            sqlite.db.commit()
            sqlite.register_table("orders", ["id"])
            sqlite.set_merge_table_scope(["orders"])
            filesystem.write_file("main", "/reports/order-7.md", "new\n", parents=True)

            chronos.create_branch("txn_42", from_branch="main")
            txn = chronos.checkout("txn_42")
            txn.sqlite.execute(
                """
                UPDATE orders
                SET status = :status, stock = stock - 1
                WHERE id = :id
                """,
                {"id": 7, "status": "reviewed"},
            )
            txn.fs.write_file("/reports/order-7.md", "reviewed\n", parents=True)

            main = chronos.checkout("main")
            assert main.sqlite.query("SELECT status, stock FROM orders WHERE id = :id", {"id": 7}) == [
                {"status": "new", "stock": 3}
            ]
            assert main.fs.read_text("/reports/order-7.md") == "new\n"

            preview = chronos.merge_atomic_preview("txn_42", "main", policy="manual_review")
            assert preview.stores["sqlite"].changes
            assert preview.stores["filesystem"].changes

            chronos.merge_atomic("txn_42", "main", policy="snapshot_isolation", operation_id="example-merge")
            committed = chronos.checkout("main")
            assert committed.sqlite.query(
                "SELECT status, stock FROM orders WHERE id = :id",
                {"id": 7},
            ) == [{"status": "reviewed", "stock": 2}]
            assert committed.fs.read_text("/reports/order-7.md") == "reviewed\n"
        finally:
            chronos.close()

    print("branch transaction example passed")


if __name__ == "__main__":
    main()
