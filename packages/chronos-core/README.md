# chronos-core

Core Transactional Agent Runtime primitives and framework-independent shims.

## Relational branching

`ChronosBranchContext` provides named SQL branches over SQLite or PostgreSQL:

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

## PostgreSQL shim

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
