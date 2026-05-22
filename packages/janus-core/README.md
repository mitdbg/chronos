# janus-core

Core Transactional Agent Runtime primitives and framework-independent shims.

## PostgreSQL shim

`PostgresShim` provides the same MVCC-style transactional table interface as
`SQLiteShim`, backed by PostgreSQL:

```python
from janus_core.transaction import PostgresShim, TransactionCoordinator

shim = PostgresShim("postgresql://postgres:postgres@localhost:5432/janus")
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
JANUS_POSTGRES_DSN=postgresql://postgres:postgres@localhost:55432/janus_test pytest -q tests/test_postgres_shim.py
```
