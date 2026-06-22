# Chronos

Chronos is a bolt-on branching layer for application state. It lets an
application fork, mutate, inspect, merge, checkpoint, or discard state while
the underlying stores keep their native access model: SQL for relational data
and POSIX for applications that rely on Unix tools, Python scripts, compilers,
and other filesystem-facing runtimes.

Chronos is built for agentic and exploratory workflows where speculative work
must be isolated until it is approved: data repair, feature engineering,
sandboxed execution, RAG index updates, debugging, and multi-step tool plans.

## What Chronos Provides

- **Named branches:** create, check out, query, mutate, diff, merge, checkpoint,
  and delete application-state branches.
- **Store-native access:** branch sessions expose normal SQL for relational
  stores and POSIX execution for tools and runtimes that expect a filesystem.
- **Efficient versioning:** branch creation does not copy data, and storage
  cost grows with the amount of data changed between branches rather than the
  size of the whole database or filesystem.
- **Multi-store branching:** coordinate the same branch names across named
  stores such as `postgresql`, `duckdb`, and `filesystem`.
- **Branch transactions:** run work in a branch, inspect the resulting diff, and
  merge approved changes with `merge_apply` atomically.

Chronos does not require applications to hold a database transaction open
across LLM calls or long-running tool execution. A branch is durable application
state, not a temporary connection-local transaction.

## Core Model

```text
create_branch("attempt", from_branch="main")
  checkout("attempt")
  use store-native APIs: SQL, POSIX tools, Python scripts, etc.
  diff("main", "attempt")
merge_apply("attempt", "main") or delete_branch("attempt")
```

For relational stores, applications register logical tables once. After that,
SQL continues to use the logical table names through a branch-bound
`BranchSession`. Chronos keeps branches isolated while preserving normal SQL
access.

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

Run every README example:

```bash
PYTHONPATH=packages/chronos-core/src python3 examples/run_all.py
```

Each script in [examples/](examples/) creates temporary state, checks the
expected branch behavior with assertions, and prints a success line. The
snippets below are excerpts from those runnable files.

## Multi-Store Branching

Chronos is designed for branches that span multiple systems:
relational data, analytical data, and POSIX-dependent execution state.

```bash
PYTHONPATH=packages/chronos-core/src python3 examples/multi_store_branching.py
```

Core excerpt:

```python
chronos = ChronosWorkspaceContext(sqlite=sqlite, filesystem=filesystem)

chronos.create_branch("agent", from_branch="main")
agent = chronos.checkout("agent")
agent.sqlite.execute(
    "UPDATE docs SET body = :body WHERE id = :id",
    {"id": "d1", "body": "candidate"},
)
agent.fs.write_file("/reports/summary.md", "candidate\n", parents=True)

preview = chronos.merge_preview("agent", "main", policy="manual_review")
chronos.merge_apply("agent", "main", policy="snapshot_isolation")
```

The example creates one branch across a SQLite relational store and ChronosFS,
mutates both stores privately, previews the cross-store diff, and commits it.
Cross-store branch commits keep speculative changes private until commit, then
publish the approved branch state atomically from the user's point of view. The
internal protocol and its polystore assumptions are covered in
[docs/multi-store-branching.md](docs/multi-store-branching.md).

## Branch APIs

The branch lifecycle is the main Chronos API. The same operations apply whether
a branch contains one store or spans multiple stores. The examples below use
`chronos` for the object that manages branches.

```bash
PYTHONPATH=packages/chronos-core/src python3 examples/branch_apis.py
```

Core excerpt:

```python
chronos.create_branch("agent", from_branch="main")
branch = chronos.checkout("agent")
branch.sqlite.execute(
    "UPDATE docs SET body = :body WHERE id = :id",
    {"id": "d1", "body": "agent"},
)
branch.fs.write_file("/reports/summary.md", "agent\n", parents=True)

chronos.create_checkpoint("before_merge", branch="agent")
readonly = chronos.checkout_checkpoint("before_merge")
chronos.create_branch_from_checkpoint("retry", checkpoint="before_merge")
chronos.merge_apply("retry", "main", policy="snapshot_isolation")
```

The example exercises branch creation, checkout, checkpointing, branch creation
from a checkpoint, merge preview, merge commit, and branch deletion.
`checkout()` returns a reusable branch session. Reusing the session is usually
faster than repeatedly checking out the same branch because branch state can be
reused.

## POSIX Workloads With ChronosFS

ChronosFS stores files in Chronos versioned storage and exposes them through a
FUSE mount for apps that rely on POSIX. Use it when Unix tools, Python
programs, compilers, generated artifacts, and relational state should share the
same branch abstraction.

```bash
PYTHONPATH=packages/chronos-core/src python3 examples/chronosfs_direct.py
```

Core excerpt:

```python
fs.write_file("main", "/src/solution.py", "print('main')\n", parents=True)
fs.create_branch("agent", from_branch="main")
fs.write_file("agent", "/src/solution.py", "print('agent')\n", parents=True)
fs.write_file("agent", "/reports/result.txt", "candidate\n", parents=True)

preview = fs.merge_preview("agent", "main", policy="manual_review")
fs.merge_apply("agent", "main", policy="snapshot_isolation")
```

FUSE mounting is available when a process needs to run against a branch through
POSIX. Parallel branch execution should use separate mount points per active
branch. Inside a mount, `.chronos/` exposes a small POSIX control plane for
branch checkout, preview, and merge commit:

```bash
PYTHONPATH=packages/chronos-core/src python3 examples/chronosfs_fuse_control.py
```

Inside a mounted branch, the control-plane flow looks like this:

```bash
mkdir .chronos/branches/agent
printf 'agent\n' > .chronos/current

cat .chronos/merge-preview/agent..main.json
cat > .chronos/merge-apply/agent..main <<'JSON'
{"policy":"weak_snapshot_isolation"}
JSON
```

`merge-preview` reports ChronosFS conflicts at path and byte-range granularity
with text diffs for textual blocks. It does not expose internal inode ids or
block indexes.

## Single-Store Branching

Single-store branching uses the same lifecycle without multi-store setup.
Use `ChronosBranchContext` when one SQL store owns the application state.

```bash
PYTHONPATH=packages/chronos-core/src python3 examples/single_store_branching.py
```

Core excerpt:

```python
chronos = ChronosBranchContext.connect("sqlite:///:memory:", backend="interval")
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
chronos.merge_apply(source="agent", target="main")
```

PostgreSQL is the OLTP row-store path. DuckDB support is available for
fixed-schema OLAP tables. PostgreSQL has the strongest schema-branching
coverage. Detailed store setup is covered in
[docs/multi-store-branching.md](docs/multi-store-branching.md).

## Branch Transactions

A Chronos branch transaction is a durable private branch plus a merge-time
commit decision. Long-running tools and LLM calls happen outside physical
database transactions; only the final branch transaction commit is short.

```bash
PYTHONPATH=packages/chronos-core/src python3 examples/branch_transactions.py
```

Core excerpt:

```python
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

preview = chronos.merge_preview("txn_42", "main", policy="manual_review")
chronos.merge_apply("txn_42", "main", policy="snapshot_isolation")
```

The example commits one private branch across SQL rows and ChronosFS files.
In the commit path, readers see either the old target branch state or the fully
committed branch state, not a partial merge. The internal protocol is described
in [docs/branching-transaction.md](docs/branching-transaction.md).

## Versioning Model

Chronos uses a record-oriented versioning technique for relational rows and
ChronosFS file blocks. Forking a branch does not copy the database or
filesystem. Chronos pays for changed records and changed file blocks, so small
branch edits stay small even when the parent state is large.

The implementation details live in the design docs:

- [docs/bolt-on-branching.md](docs/bolt-on-branching.md)
- [docs/branching-transaction.md](docs/branching-transaction.md)
- [docs/multi-store-branching.md](docs/multi-store-branching.md)
- [docs/filesystem-on-chronos.md](docs/filesystem-on-chronos.md)

## Running Tests

Run the core test suite:

```bash
PYTHONPATH=packages/chronos-core/src pytest -q
```

Run only branching and filesystem-focused tests:

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

- Cross-store branch operations are best-effort in v1; there is no global
  distributed commit protocol across independent stores.
- DuckDB supports fixed-schema data operations in v1. Branch-local schema
  branching is disabled for DuckDB.
- Branch-local schema changes are opt-in with `enable_schema_branching=True`.
  PostgreSQL has the strongest schema-branching coverage.
- Branch write SQL supports a focused DML subset: `INSERT ... VALUES`, simple
  `UPDATE` assignments and expressions, `DELETE`, `upsert_rows`, and
  `delete_keys`. Branch reads can use richer `SELECT` statements.
- FUSE-based ChronosFS mounting requires Linux FUSE 3 runtime support and
  `/dev/fuse`.

## Design Notes

The design documents in `docs/` describe the broader branch model:

- `docs/branching-introduction.md`
- `docs/bolt-on-branching.md`
- `docs/branching-transaction.md`
- `docs/multi-store-branching.md`
- `docs/filesystem-on-chronos.md`
- `docs/related-work.md`
