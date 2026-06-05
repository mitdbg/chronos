# chronos-core

Core branch management, merge, checkpoint, and framework-independent state
isolation primitives.

## Relational branching

`ChronosBranchContext` provides named SQL branches over SQLite or PostgreSQL.
Applications manage branches through Python APIs and keep using SQL against
logical table names inside a checked-out branch:

```python
from chronos_core.branching import ChronosBranchContext

ctx = ChronosBranchContext.connect(
    "postgresql://postgres:postgres@localhost:5432/chronos",
    backend="interval",
)

ctx.register_table("products", ["sku"])
ctx.create_branch("agent_experiment", from_branch="main")

session = ctx.checkout("agent_experiment")
with session.transaction():
    session.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 90, "sku": "abc"},
    )
```

Supported branch backends are `interval`, `log`, and `copy`.

The interval backend is the default. It supports metadata-only branch creation,
branch-local writes, checkpoints, diff, merge preview, merge apply, branch
deletion, and opt-in branch-local schema changes. PostgreSQL has the most
complete DDL coverage.

## Lower-Level PostgreSQL Shim

`PostgresShim` provides the same MVCC-style transactional table interface as
`SQLiteShim`, backed by PostgreSQL:

```python
from chronos_core.transaction import PostgresShim, TransactionCoordinator

shim = PostgresShim("postgresql://postgres:postgres@localhost:5432/chronos")
shim.register_table("users", ["id TEXT", "name TEXT", "credits INTEGER"], pk_column="id")
shim.seed_data("users", [{"id": "u1", "name": "Alice", "credits": 100}])

coordinator = TransactionCoordinator()
coordinator.register_shim(shim)

txn = coordinator.begin()
shim.put(txn, "users", {"id": "u1", "name": "Alice", "credits": 90})
coordinator.commit(txn.id)
```

Run the PostgreSQL tests with a real database by setting:

```bash
CHRONOS_POSTGRES_DSN=postgresql://postgres:postgres@localhost:55432/chronos_test pytest -q tests/test_postgres_shim.py
```
