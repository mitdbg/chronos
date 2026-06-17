# Chronos

Chronos is a bolt-on branching layer for application state. It lets an
application fork, mutate, inspect, merge, checkpoint, or discard state while
the underlying stores keep their native access model: SQL for relational data
and POSIX-style paths for filesystems.

Chronos is built for agentic and exploratory workflows where speculative work
must be isolated until it is approved: data repair, feature engineering,
workspace execution, RAG index updates, debugging, and multi-step tool plans.

## What Chronos Provides

- **Named branches:** create, check out, query, mutate, diff, merge, checkpoint,
  and delete application-state branches.
- **Store-native access:** branch sessions expose normal SQL for relational
  stores and filesystem operations for workspace state.
- **Shared interval branching:** PostgreSQL, SQLite, and DuckDB-backed SQL data
  can use the same interval backend for branch visibility, writes, diffs, and
  merges.
- **Postgres metadata plane:** branch metadata, segment allocation, checkpoints,
  and registries stay in PostgreSQL when using split stores such as
  DuckDB-data plus Postgres-metadata.
- **Multi-store workspaces:** coordinate the same branch names across named
  stores such as `postgresql`, `duckdb`, and `filesystem`.
- **Branch transactions:** run work in a branch, inspect the resulting diff, and
  publish approved changes with `merge_apply`.

Chronos does not require applications to hold a database transaction open
across LLM calls or long-running tool execution. A branch is durable application
state, not a temporary connection-local transaction.

## Core Model

```text
create_branch("attempt", from_branch="main")
  checkout("attempt")
  use store-native APIs: SQL, filesystem paths, etc.
  diff("main", "attempt")
merge_apply("attempt", "main") or delete_branch("attempt")
```

For relational stores, applications register logical tables once. After that,
SQL continues to use the logical table names through a branch-bound
`BranchSession`. Chronos rewrites reads and writes to the physical branch
representation.

## Installation

From this directory:

```bash
uv sync --all-extras
```

Editable install without `uv`:

```bash
python -m pip install -e packages/chronos-core
```

DuckDB support is optional:

```bash
python -m pip install -e 'packages/chronos-core[duckdb]'
```

If the package is not installed, run commands with:

```bash
PYTHONPATH=packages/chronos-core/src
```

## Quickstart: Relational Branching

Use `ChronosBranchContext` when one SQL store owns the relational state.

```python
from chronos_core.branching import ChronosBranchContext

ctx = ChronosBranchContext.connect("sqlite:///:memory:", backend="interval")

ctx.db.execute(
    """
    CREATE TABLE products (
      sku TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      price INTEGER NOT NULL,
      stock INTEGER NOT NULL
    )
    """
)
ctx.db.execute("INSERT INTO products VALUES ('abc', 'Keyboard', 100, 5)")
ctx.db.commit()

ctx.register_table("products", primary_key=["sku"])

ctx.create_branch("agent", from_branch="main")
agent = ctx.checkout("agent")

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

assert ctx.checkout("main").query("SELECT sku, price FROM products") == [
    {"sku": "abc", "price": 100},
]
assert agent.query("SELECT sku, price FROM products ORDER BY sku") == [
    {"sku": "abc", "price": 90},
    {"sku": "def", "price": 25},
]

diff = ctx.diff("main", "agent")
for change in diff.changes:
    print(change.table, change.key, change.change)

ctx.merge_apply(source="agent", target="main")
```

`checkout()` returns a reusable `BranchSession`. Reusing the session is usually
faster than repeatedly checking out the same branch because backend metadata can
be cached.

Use `session.transaction()` only when you want to group several physical writes
inside the underlying database transaction. The branch itself is still the
isolation boundary.

```python
with agent.transaction():
    agent.execute("UPDATE products SET stock = stock - 1 WHERE sku = :sku", {"sku": "abc"})
    agent.execute("UPDATE products SET stock = stock + 1 WHERE sku = :sku", {"sku": "def"})
```

## PostgreSQL Store

Use `ChronosPostgresStore` when PostgreSQL stores the application data. This is
the OLTP-oriented row-store path.

```python
from chronos_core.workspace import ChronosPostgresStore

postgresql = ChronosPostgresStore(
    "postgresql://postgres:postgres@localhost:5432/app",
)

postgresql.db.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
postgresql.db.execute("INSERT INTO docs VALUES ('d1', 'main')")
postgresql.db.commit()
postgresql.register_table("docs", ["id"])

postgresql.create_branch("agent", from_branch="main")
agent = postgresql.checkout("agent")
agent.execute("UPDATE docs SET body = :body WHERE id = :id", {"id": "d1", "body": "draft"})
```

PostgreSQL can also be used as both data and metadata through
`ChronosBranchContext.connect(...)`; this remains compatible with existing
call sites.

## DuckDB Store With Postgres Metadata

Use `ChronosDuckDBStore` for OLAP-oriented relational data while keeping
branching metadata in PostgreSQL.

```python
from chronos_core.workspace import ChronosDuckDBStore

duckdb = ChronosDuckDBStore(
    data_url="duckdb:///analytics.duckdb",
    metadata_url="postgresql://postgres:postgres@localhost:5432/chronos_meta",
)

duckdb.db.execute("CREATE TABLE metrics (id VARCHAR PRIMARY KEY, region VARCHAR, value DOUBLE)")
duckdb.db.execute("INSERT INTO metrics VALUES ('m1', 'west', 10.0)")
duckdb.db.commit()
duckdb.register_table("metrics", ["id"])

duckdb.create_branch("experiment", from_branch="main")
experiment = duckdb.checkout("experiment")
experiment.execute(
    "UPDATE metrics SET value = :value WHERE id = :id",
    {"id": "m1", "value": 42.0},
)

assert duckdb.checkout("main").query("SELECT sum(value) AS total FROM metrics") == [
    {"total": 10.0},
]
assert experiment.query("SELECT region, sum(value) AS total FROM metrics GROUP BY region") == [
    {"region": "west", "total": 42.0},
]
```

DuckDB v1 uses the shared interval backend for fixed-schema data operations.
Branch-local schema branching is intentionally disabled for DuckDB; use
PostgreSQL interval branches for schema-branching workloads.

## Multi-Store Workspace

Use `ChronosWorkspaceContext` when one branch name should span multiple stores.
Stores are addressed by their system name, so sessions expose
`session.postgresql`, `session.duckdb`, and `session.fs`.

```python
from chronos_core.workspace import (
    ChronosDuckDBStore,
    ChronosPostgresStore,
    ChronosWorkspaceContext,
)

postgresql = ChronosPostgresStore(
    "postgresql://postgres:postgres@localhost:5432/app",
)
duckdb = ChronosDuckDBStore(
    data_url="duckdb:///analytics.duckdb",
    metadata_url="postgresql://postgres:postgres@localhost:5432/chronos_meta",
)

workspace = ChronosWorkspaceContext(
    postgresql=postgresql,
    duckdb=duckdb,
)

workspace.create_branch("agent", from_branch="main")
agent = workspace.checkout("agent")

agent.postgresql.execute(
    "UPDATE docs SET body = :body WHERE id = :id",
    {"id": "d1", "body": "candidate"},
)
agent.duckdb.execute(
    "UPDATE metrics SET value = value + 1 WHERE id = :id",
    {"id": "m1"},
)

diffs = workspace.diff("main", "agent")
print(diffs["postgresql"].changes)
print(diffs["duckdb"].changes)

workspace.merge_apply("agent", "main")
workspace.close()
```

Cross-store branch operations are coordinated best-effort in v1. Chronos does
not add a global two-phase commit protocol across independent stores. The
branching design instead keeps speculative writes private to branch-visible
state and publishes branch changes through each store's `merge_apply`.

## Filesystem Branching

Chronos includes two filesystem options:

- `ChronosFilesystemStore`: workspace branching over filesystem directories.
- `ChronosFSStore`: filesystem contents stored in Chronos interval tables, with
  optional FUSE mounting for POSIX execution inside a branch.

`ChronosFSStore` is useful when code, generated files, model artifacts, and
relational state should share the same branch abstraction.

```python
from chronos_core.workspace.chronosfs import ChronosFSStore, mount_chronosfs

fs = ChronosFSStore.connect("postgresql://postgres:postgres@localhost:5432/app")
fs.ensure()
fs.create_branch("agent", from_branch="main")
fs.write_file("agent", "/src/solution.py", "print('hello')\n", parents=True)

diff = fs.diff("main", "agent")
fs.merge_apply("agent", "main")
```

FUSE mounting is available through `mount_chronosfs(...)` when the process needs
normal POSIX reads and writes against a branch. Parallel branch execution should
use separate mount points per active branch.

## Branch APIs

The core branch lifecycle is shared across relational stores and workspace
stores:

```python
ctx.create_branch("child", from_branch="main", metadata={"owner": "agent"})
session = ctx.checkout("child")
checkpoint = ctx.create_checkpoint("before_merge", branch="child")
readonly = ctx.checkout_checkpoint("before_merge")
diff = ctx.diff("main", "child")
ctx.merge_apply("child", "main")
ctx.delete_branch("child")
```

Checkpoint sessions are read-only. `create_branch_from_checkpoint()` can promote
a checkpoint into a new mutable branch.

```python
ctx.create_branch_from_checkpoint("retry", checkpoint="before_merge")
```

## Branch Backend Choices

`ChronosBranchContext.connect(..., backend=...)` supports these relational
branch backends:

| Backend | How it stores branch state | Read behavior | Write behavior | Good for |
| --- | --- | --- | --- | --- |
| `interval` | Physical rows plus interval visibility and delete metadata | Rewrites SQL through a prepared branch point | Splits overlapping row intervals and writes into the active segment | Default; SQL-heavy reads; Postgres, SQLite, DuckDB data planes |
| `log` | Append-only per-table operation log with branch lineage metadata | Reconstructs latest visible row per key from log prefixes | Appends insert/update/delete records | Studying log-based designs |
| `copy` | Full physical table copy per branch/checkpoint | Direct SQL against branch-private tables | Direct writes to private branch tables | Correctness baseline and small datasets |

The default recommendation is `backend="interval"`.

## Compatibility Surface

Existing relational API entry points remain supported:

```python
ChronosBranchContext.connect(database_url, backend="interval")
ChronosBranchContext.from_database_adapter(adapter, backend="interval")
ctx.db
ctx.conn
ctx.backend_name
ctx.checkout("branch")
session.query(...)
session.execute(...)
session.upsert_rows(...)
session.delete_keys(...)
ctx._backend.refresh_registries()
```

The interval backend keeps the existing metadata table names:

```text
_chronos_branch_tables
_chronos_branch_indexes
_chronos_branch_interval_*
_chronos_b_interval_*
```

PostgreSQL schema branching remains opt-in with
`enable_schema_branching=True`.

## Running Tests

Run the core test suite:

```bash
PYTHONPATH=packages/chronos-core/src pytest -q
```

Run only branching and workspace-focused tests:

```bash
PYTHONPATH=packages/chronos-core/src \
pytest -q \
  tests/test_branching.py \
  tests/test_branching_schema.py \
  tests/test_interval_stores.py \
  tests/test_workspace_filesystem.py
```

Some tests require PostgreSQL. A local test database can be started with:

```bash
docker run --rm -d --name chronos-postgres-test \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=chronos_test \
  -p 55432:5432 \
  postgres:18-alpine

CHRONOS_POSTGRES_DSN=postgresql://postgres:postgres@localhost:55432/chronos_test \
PYTHONPATH=packages/chronos-core/src \
pytest -q tests/test_branching.py tests/test_branching_schema.py tests/test_interval_stores.py

docker stop chronos-postgres-test
```

## Limitations

- Cross-store workspace operations are best-effort in v1; there is no global
  distributed commit protocol across independent stores.
- DuckDB supports fixed-schema interval data operations in v1. Branch-local
  schema branching is disabled for DuckDB.
- Branch-local schema changes are opt-in with `enable_schema_branching=True`.
  PostgreSQL has the strongest schema-branching coverage.
- Branch write SQL supports a focused DML subset: `INSERT ... VALUES`, simple
  `UPDATE` assignments and expressions, `DELETE`, `upsert_rows`, and
  `delete_keys`. Branch reads can use richer `SELECT` statements because
  Chronos rewrites table references before execution.
- The interval backend uses fixed-width integer interval allocation. Very deep
  single-child chains can exhaust interval space without future relabeling or a
  wider numeric representation.
- The log backend can be much slower for arbitrary reads because it resolves
  branch-visible rows from log lineage at read time.
- FUSE-based ChronosFS mounting requires Linux FUSE support and the optional
  `chronos-core[fuse]` dependencies.

## Design Notes

The design documents in `docs/` describe the broader branch model:

- `docs/branching-introduction.md`
- `docs/bolt-on-branching.md`
- `docs/branching-transaction.md`
- `docs/multi-store-branching.md`
- `docs/filesystem-on-chronos.md`
- `docs/related-work.md`

## Development Guidelines

- Keep `chronos-core` independent of application frameworks.
- Preserve existing `ChronosBranchContext` and `BranchSession` call sites.
- Prefer additive store APIs over replacing stable branch APIs.
- Keep branch metadata names stable for OKG, db-fork, and existing Chronos
  deployments.
