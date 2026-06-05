# Chronos

Chronos is a bolt-on branching layer over data stores for stateful applications and agents. It
lets an application fork, mutate, diff, merge, checkpoint, or discard data store state while
each underlying store keeps its native access model: SQL for relational data,
POSIX-style access for filesystems, and store-specific APIs for memory, vectors,
or other application state.

Chronos is built for workflows where "try it and see what happens" is useful but
unsafe without a branch: code editing, debugging, data repair, structured memory
updates, RAG indexing, and multi-step tool plans.

## What Chronos Provides

- **Named branches:** applications can create, check out, query, mutate, diff,
  merge, and delete branches.
- **Store-native access:** branch sessions expose SQL for relational stores and
  filesystem paths for code and artifacts.
- **Long-lived SQL branches:** relational data can keep branch state across many
  operations, including opt-in schema changes on branch-local physical tables.
- **Branch diffs and merges:** Chronos can compare branches and apply approved
  row-level changes back to a target branch.
- **Zero-copy filesystem branches:** file operations use OverlayFS or
  `fuse-overlayfs` copy-on-write layers when available.
- **Branch transactions:** an agent can run tool calls inside a branch, inspect
  the final diff, and merge only the approved result.
- **Framework separation:** the branch layer is framework-independent. Adapter
  packages can be built on top without changing the core API.

## Current Scope

Chronos has two related APIs. The main API is branch management:

```text
create branch -> checkout branch session -> query/write with store-native APIs
diff/merge -> compare or apply branch changes
delete branch -> discard speculative state
```

For relational data, branch management is Python API driven. User data is still
stored and queried with SQL through a branch-bound session object.

The lower-level transaction runtime provides short-lived, transaction-scoped
virtual branches over stateful tools:

```text
begin transaction -> isolated branch
  tool calls
  savepoint / rollback to savepoint
commit -> merge into real state
abort  -> discard branch
```

For long agent runs, a branch transaction is usually the better abstraction:

```text
branch = fork(application_state)
agent performs tool calls inside branch
system computes DB/file diff
policy checker reviews final state
merge approved changes or discard branch
```

This avoids holding a database transaction open across LLM calls and avoids
exposing saga-style intermediate state to other users or agents.

## Quickstart: Branch SQL State

Use `ChronosBranchContext` when you want named, mutable branches over SQL tables.
The application manages branches with Python APIs, while agents and application
code continue to issue SQL against logical table names.

```python
from chronos_core.branching import ChronosBranchContext

ctx = ChronosBranchContext.connect("sqlite:///:memory:", backend="interval")
conn = ctx.conn

conn.execute(
    """
    CREATE TABLE products (
      sku TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      price INTEGER NOT NULL,
      stock INTEGER NOT NULL
    )
    """
)
conn.execute("INSERT INTO products VALUES ('abc', 'Keyboard', 100, 5)")
conn.commit()

ctx.register_table("products", primary_key=["sku"])

ctx.create_branch("agent_experiment", from_branch="main")
agent = ctx.checkout("agent_experiment")

with agent.transaction():
    agent.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 90, "sku": "abc"},
    )
    agent.execute(
        """
        INSERT INTO products (sku, name, price, stock)
        VALUES (:sku, :name, :price, :stock)
        """,
        {"sku": "def", "name": "Mouse", "price": 25, "stock": 10},
    )

assert agent.query("SELECT sku, price FROM products ORDER BY sku") == [
    {"sku": "abc", "price": 90},
    {"sku": "def", "price": 25},
]
assert ctx.checkout("main").query("SELECT sku, price FROM products") == [
    {"sku": "abc", "price": 100},
]

diff = ctx.diff("main", "agent_experiment")
for change in diff.changes:
    print(change.table, change.key, change.change)

ctx.merge_apply(source="agent_experiment", target="main")
```

`checkout()` returns a reusable `BranchSession`. Reusing the session is
important for performance because it caches branch metadata such as interval
segment bounds or log lineage.

## Packages

```text
packages/
  chronos-core        Core branching context, coordinator, and backend shims
```

### `chronos-core`

Framework-independent runtime components:

- `TransactionCoordinator`
- `ToolShim` interface
- `ChronosBranchContext` and `BranchSession`
- `OverlayFSShim`
- `SQLiteShim`
- `PostgresShim`
- `SqliteVecShim`
- relational branch backends: `interval`, `log`, and `copy`
- transaction handles, snapshots, savepoints, branch diffs, change records, and
  vote types

Adapter packages for agent frameworks live in this repository, but the branch
API and the examples below use `chronos-core` directly.

## Installation

From this directory:

```bash
uv sync --all-extras
```

For editable installs without `uv`:

```bash
python -m pip install -e packages/chronos-core
```

If you do not install the packages, run commands with:

```bash
PYTHONPATH=packages/chronos-core/src
```

## System Requirements

- Python 3.10+
- Linux for filesystem isolation
- `fuse-overlayfs` for unprivileged filesystem transactions, or root privileges
  for kernel OverlayFS mounts
- PostgreSQL when using PostgreSQL-backed shims or relational branches

Install `fuse-overlayfs` on Ubuntu/Debian:

```bash
sudo apt install fuse-overlayfs
```

## Lower-Level Runtime: SQLite Transactions

```python
from chronos_core.transaction import SQLiteShim, TransactionCoordinator

shim = SQLiteShim(":memory:")
shim.register_table(
    "users",
    ["id TEXT", "name TEXT", "credits INTEGER"],
    pk_column="id",
)
shim.seed_data("users", [{"id": "u1", "name": "Alice", "credits": 100}])

coordinator = TransactionCoordinator()
coordinator.register_shim(shim)

txn = coordinator.begin()
shim.put(txn, "users", {"id": "u1", "name": "Alice", "credits": 90})

assert shim.get(txn, "users", "u1")["credits"] == 90
coordinator.commit(txn.id)
```

SQLite rows are versioned with Chronos metadata columns:

- `_begin_txn`: transaction numeric ID that created the version
- `_end_txn`: transaction numeric ID that superseded/deleted the version
- `_row_version`: reserved row-version metadata

Reads apply the transaction snapshot visibility predicate. Updates close the
old version and insert a new version.

## Relational Branching Details

After a table is registered, SQL continues to use logical table names. Chronos
rewrites the query for the selected branch and sends it to the underlying
database. Use `session.transaction()` to group several writes into one
underlying database transaction.

### Branching Multiple Tables

A branch represents the logical state of the registered database tables
together. Register all application tables that should branch as one coherent
state:

```python
ctx.register_table("nodes", primary_key=["node_id"])
ctx.register_table("edges", primary_key=["edge_id"])
ctx.register_table("documents", primary_key=["doc_id"])
```

After registration, SQL uses the logical names:

```python
graph = ctx.checkout("agent_graph_search")

neighbors = graph.query(
    """
    SELECT e.dst_id, n.label
    FROM edges AS e
    JOIN nodes AS n ON n.node_id = e.dst_id
    WHERE e.src_id = :node_id
    """,
    {"node_id": "n42"},
)
```

The branch layer rewrites logical table references to backend-specific physical
tables or subqueries before sending SQL to the database.

### Checkpoints, Diff, And Merge

```python
ctx.create_checkpoint("before_agent_run", branch="agent_experiment")

checkpoint = ctx.checkout_checkpoint("before_agent_run")
snapshot_rows = checkpoint.query("SELECT * FROM products")

diff = ctx.diff("main", "agent_experiment")
for change in diff.changes:
    print(change.table, change.key, change.change)

preview = ctx.merge_preview(source="agent_experiment", target="main")
ctx.merge_apply(
    source="agent_experiment",
    target="main",
    resolution=preview.resolution,
)
```

Checkpoint sessions are read-only. `create_branch_from_checkpoint()` can promote
a checkpoint into a new mutable branch.

```python
ctx.create_branch_from_checkpoint(
    "retry_from_before_agent_run",
    checkpoint="before_agent_run",
)
```

### Branch Backend Choices

`ChronosBranchContext.connect(..., backend=...)` supports SQLite and PostgreSQL
database URLs with three physical branch implementations:

| Backend | How it stores branch state | Read behavior | Write behavior | Good for |
| --- | --- | --- | --- | --- |
| `interval` | User rows plus `live_lo`, `live_hi`, and logical delete metadata | Constant-shape visibility predicate using a prepared branch point | Splits overlapping physical rows for the current branch interval | Default branch backend and SQL-heavy reads |
| `log` | Append-only per-table operation log with branch lineage metadata | Reconstructs latest visible row per key from shared log prefixes | Appends update/insert/delete records | Cheap branch creation and studying log-based designs |
| `copy` | Full physical table copy per branch/checkpoint | Direct SQL against private branch tables | Direct writes to private branch tables | Correctness baseline and small datasets |

The default recommendation is `backend="interval"`. The `copy` backend is
simple and useful for validating behavior. The `log` backend avoids copying at
branch creation but has heavier arbitrary SQL reads because it resolves the
latest visible operation per key.

## Quickstart: PostgreSQL Shim

```python
from chronos_core.transaction import PostgresShim, TransactionCoordinator

shim = PostgresShim("postgresql://postgres:postgres@localhost:5432/chronos")
shim.register_table(
    "users",
    ["id TEXT", "name TEXT", "credits INTEGER"],
    pk_column="id",
)

coordinator = TransactionCoordinator()
coordinator.register_shim(shim)

txn = coordinator.begin()
shim.put(txn, "users", {"id": "u1", "name": "Alice", "credits": 100})
coordinator.commit(txn.id)
```

The PostgreSQL backend follows the same MVCC protocol as SQLite and uses
PostgreSQL transactions for durable SQL execution.

## Backend Semantics

Transaction shims participate in `TransactionCoordinator`:

| Backend | Isolation mechanism | Commit behavior |
| --- | --- | --- |
| Filesystem | OverlayFS or `fuse-overlayfs` copy-on-write layer | Copy changed files into the base project |
| SQLite | Chronos MVCC columns and visibility predicates | Mark transaction IDs committed |
| PostgreSQL | Chronos MVCC columns and visibility predicates | PostgreSQL-backed row versioning |
| Vector store | sqlite-vec style transactional records | Commit visible vector records |

All registered shims participate in the same coordinator commit. If one
participant votes abort during prepare, the coordinator aborts the transaction.

Relational branch backends are used through `ChronosBranchContext`:

| Branch backend | Branch creation | Query path | Storage cost |
| --- | --- | --- | --- |
| `interval` | Metadata-only segment split | SQL rewrite plus interval visibility predicate | Stores extra physical rows only when writes overlap branch intervals |
| `log` | Metadata-only lineage fork | SQL rewrite to log replay subqueries | Stores append-only operation records |
| `copy` | Copies every registered table | SQL rewrite to branch-private physical tables | Duplicates registered tables per branch/checkpoint |

These are independent of the transaction shims. A branch session can still use
the database's normal transaction mechanism through `session.transaction()`.

## Running Tests

Run core branching and shim tests:

```bash
PYTHONPATH=packages/chronos-core/src \
pytest -q \
  tests/test_adapter_imports.py \
  tests/test_branching.py \
  tests/test_branching_schema.py \
  tests/test_workspace_filesystem.py \
  tests/test_postgres_shim.py
```

Many of these tests require filesystem transaction support. Some filesystem
test fixtures require root privileges for OverlayFS mounts; the runtime can
also use `fuse-overlayfs` on systems where it is installed.

Run PostgreSQL shim tests:

```bash
docker run --rm -d --name chronos-postgres-test \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=chronos_test \
  -p 55432:5432 \
  postgres:18-alpine

CHRONOS_POSTGRES_DSN=postgresql://postgres:postgres@localhost:55432/chronos_test \
PYTHONPATH=packages/chronos-core/src \
pytest -q tests/test_postgres_shim.py

docker stop chronos-postgres-test
```

## Limitations

- The coordinator is currently an in-process coordinator, not a distributed
  transaction manager.
- Durable transaction metadata is not yet externalized for multi-process use.
- Branch-local schema changes are opt-in with `enable_schema_branching=True`.
  The strongest coverage is on PostgreSQL. The current DDL support focuses on
  table-local schema evolution such as adding columns, dropping columns, type
  changes, and index preservation/copying for branch-local physical tables.
- `ChronosBranchContext.connect()` currently has SQLite and PostgreSQL database
  adapters. Additional SQL databases need an adapter implementation.
- Branch write SQL supports a focused DML subset: `INSERT ... VALUES`, simple
  `UPDATE` assignments and expressions, and `DELETE`. Branch reads can use
  richer `SELECT` statements because Chronos rewrites table references before
  execution.
- The interval backend uses fixed-width integer interval allocation. Very deep
  single-child chains can exhaust interval space without future relabeling or a
  wider numeric representation.
- The log backend can be much slower for arbitrary reads because it resolves
  branch-visible rows from log lineage at read time.
- Filesystem isolation requires Linux OverlayFS support or `fuse-overlayfs`.
- Non-transactional external APIs require compensating actions; generic saga
  support is not production-complete in this package.

## Design Notes

The design documents in `docs/` describe the broader research direction:

- `Chronos-design.md`: architecture and transaction model
- `Chronos-implementation-plan.md`: implementation roadmap
- `Chronos-summary.md`: technical overview
- `bolt-on-branching.md`: relational branch/versioning layer

## Development Guidelines

- Keep `chronos-core` independent of application frameworks.
- Keep transaction semantics in shims explicit: begin, prepare, commit, abort,
  savepoint, rollback, and change inspection.
