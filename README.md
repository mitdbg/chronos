# Transactional Agent Runtime

Janus, the Transactional Agent Runtime, gives state-modifying AI agents a
transaction boundary. Agents can edit files, run commands, update relational
state, write memory, and use vector storage inside an isolated virtual branch.
The application can then commit the whole attempt or abort it without leaving
partial side effects behind.

Janus is built for agent workflows where "try it and see what happens" is useful
but unsafe without isolation: code editing, debugging, data repair, structured
memory updates, RAG indexing, and multi-step tool plans.

## What Janus Provides

- **Isolated execution:** tool calls run against transaction-local state.
- **Atomic commit/abort:** all enrolled backends commit or roll back together.
- **Savepoints:** agents can checkpoint before risky work and roll back only
  the failed part.
- **Snapshot-style reads:** relational and vector shims use MVCC metadata so a
  transaction sees a stable view plus its own writes.
- **Zero-copy filesystem branching:** file operations use OverlayFS or
  `fuse-overlayfs` copy-on-write layers.
- **Agent-facing tools:** LangChain tools expose file editing, bash, memory,
  SQLite, vector store, and transaction control.
- **Framework separation:** the transaction layer is framework-independent;
  LangChain and LangGraph integrations are adapters on top.

## Current Scope

The current implementation supports transaction-scoped virtual branches:

```text
begin transaction -> isolated branch
  tool calls
  savepoint / rollback to savepoint
commit -> merge into real state
abort  -> discard branch
```

Named, long-lived database branches and branch diff/merge are design targets
described in `docs/bolt-on-branching.md`. They are not exposed as a
stable API in this package yet.

## Packages

```text
packages/
  janus-core        Core coordinator, transaction types, and backend shims
  langchain-janus  LangChain tools backed by Janus transactions
  janus-langgraph  LangGraph helpers using public LangGraph APIs
  janus-code       Transactional coding-agent CLI and MCP server
```

### `janus-core`

Framework-independent runtime components:

- `TransactionCoordinator`
- `ToolShim` interface
- `OverlayFSShim`
- `SQLiteShim`
- `PostgresShim`
- `SqliteVecShim`
- transaction handles, snapshots, savepoints, change records, and vote types

### `langchain-janus`

LangChain tools for agent use:

- `JanusContext`
- `JanusFileEditor`
- `JanusBash`
- `JanusMemory`
- `JanusSQLite`
- `JanusVectorStore`
- `JanusTransactionControl`

### `janus-langgraph`

LangGraph integration helpers:

- transaction-control tools
- `TransactionalToolWrapper`
- `create_transactional_agent`
- branch-aware LangGraph store shim

This package uses public LangGraph package APIs such as `langgraph.prebuilt`,
`langgraph.types`, and `langgraph.store`.

### `janus-code`

A coding-agent application built on Janus:

- `janus-code` CLI
- session and transaction manager
- coding tools for read/write/edit/bash/glob/ripgrep/todos/memory
- sub-agent and parallel-agent orchestration
- MCP server and bootstrap helpers

## Installation

From this directory:

```bash
uv sync --all-extras
```

For editable installs without `uv`:

```bash
python -m pip install -e packages/janus-core
python -m pip install -e packages/janus-langchain
python -m pip install -e packages/janus-langgraph
python -m pip install -e packages/janus-code
```

If you do not install the packages, run commands with:

```bash
PYTHONPATH=packages/janus-core/src:packages/janus-langchain/src:packages/janus-langgraph/src:packages/janus-code/src
```

## System Requirements

- Python 3.10+
- Linux for filesystem isolation
- `fuse-overlayfs` for unprivileged filesystem transactions, or root privileges
  for kernel OverlayFS mounts
- PostgreSQL only when using `PostgresShim`
- `rg` for ripgrep-backed Janus-code tests/tools
- `mcp[cli]` for MCP server functionality
- model provider credentials for live Janus-code agent runs

Install `fuse-overlayfs` on Ubuntu/Debian:

```bash
sudo apt install fuse-overlayfs
```

## Quickstart: App-Controlled Commit

Use this pattern when the application decides whether an agent's work should be
kept.

```python
from pathlib import Path

from langchain_janus import JanusContext

project = Path("./demo_project").resolve()
project.mkdir(exist_ok=True)

ctx = JanusContext(project, enable_sqlite=True, enable_vectorstore=False)
ctx.begin()

ctx.file_editor.invoke({
    "command": "create",
    "path": "README.md",
    "file_text": "# Demo\n\nCreated inside a Janus transaction.\n",
})

ctx.bash.invoke({"command": "ls"})

checks_passed = True
if checks_passed:
    ctx.commit()
else:
    ctx.abort()
```

Inside the transaction, tools see the edited project. Outside the transaction,
the real project is unchanged until `commit()`.

## Quickstart: Savepoint And Rollback

```python
from langchain_janus import JanusContext

ctx = JanusContext("./demo_project", enable_sqlite=True, enable_vectorstore=False)
ctx.begin()

ctx.file_editor.invoke({
    "command": "create",
    "path": "stable.txt",
    "file_text": "keep this\n",
})

ctx.savepoint("before_experiment")

ctx.file_editor.invoke({
    "command": "create",
    "path": "experiment.txt",
    "file_text": "discard this if checks fail\n",
})

checks_passed = False
if not checks_passed:
    ctx.rollback("before_experiment")

ctx.commit()
```

The final commit keeps `stable.txt` and discards `experiment.txt`.

## Quickstart: SQLite Transactions

```python
from janus_core.transaction import SQLiteShim, TransactionCoordinator

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

SQLite rows are versioned with Janus metadata columns:

- `_begin_txn`: transaction numeric ID that created the version
- `_end_txn`: transaction numeric ID that superseded/deleted the version
- `_row_version`: reserved row-version metadata

Reads apply the transaction snapshot visibility predicate. Updates close the
old version and insert a new version.

## Quickstart: PostgreSQL Shim

```python
from janus_core.transaction import PostgresShim, TransactionCoordinator

shim = PostgresShim("postgresql://postgres:postgres@localhost:5432/janus")
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

## LangGraph Integration

```python
from langgraph.prebuilt import create_react_agent
from langchain_janus import JanusContext
from langchain_janus.context import JanusTransactionControl

ctx = JanusContext("./demo_project", enable_sqlite=True, enable_vectorstore=False)
ctx.begin()

tools = ctx.get_tools() + [JanusTransactionControl(janus_context=ctx)]

agent = create_react_agent(
    model,
    tools=tools,
    prompt=(
        "A Janus transaction is active. Use savepoints before risky changes. "
        "Commit only after checks pass."
    ),
)
```

`janus-langgraph` also provides helpers for constructing transaction-management
tools and wrapping tool calls with automatic savepoints.

## Janus-Code CLI

Run a transactional coding-agent session:

```bash
janus-code --project /path/to/project
```

The CLI creates a Janus-backed session so edits, commands, memory, and SQLite
state can be committed or aborted together. It also includes MCP server support
for exposing Janus tools to external agents.

## Backend Semantics

| Backend | Isolation mechanism | Commit behavior |
| --- | --- | --- |
| Filesystem | OverlayFS or `fuse-overlayfs` copy-on-write layer | Copy changed files into the base project |
| SQLite | Janus MVCC columns and visibility predicates | Mark transaction IDs committed |
| PostgreSQL | Janus MVCC columns and visibility predicates | PostgreSQL-backed row versioning |
| Vector store | sqlite-vec style transactional records | Commit visible vector records |
| LangGraph store | Branch-prefixed namespaces | Flush branch entries into main namespace |

All registered shims participate in the same coordinator commit. If one
participant votes abort during prepare, the coordinator aborts the transaction.

## Running Tests

Run fast unit and integration tests that do not require live model calls:

```bash
PYTHONPATH=packages/janus-core/src:packages/janus-langchain/src:packages/janus-langgraph/src:packages/janus-code/src \
pytest -q \
  tests/test_adapter_imports.py \
  tests/test_postgres_shim.py \
  tests/janus_code/unit \
  tests/janus_code/integration \
  --ignore=tests/janus_code/integration/test_cli_live_e2e.py
```

Run LangChain Janus tests:

```bash
PYTHONPATH=packages/janus-core/src:packages/janus-langchain/src:packages/janus-langgraph/src:packages/janus-code/src \
pytest -q -rs tests/langchain_janus
```

Many of these tests require filesystem transaction support. Some filesystem
test fixtures require root privileges for OverlayFS mounts; the runtime can
also use `fuse-overlayfs` on systems where it is installed.

Run PostgreSQL shim tests:

```bash
docker run --rm -d --name janus-postgres-test \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=janus_test \
  -p 55432:5432 \
  postgres:16-alpine

JANUS_POSTGRES_DSN=postgresql://postgres:postgres@localhost:55432/janus_test \
PYTHONPATH=packages/janus-core/src:packages/janus-langchain/src:packages/janus-langgraph/src:packages/janus-code/src \
pytest -q tests/test_postgres_shim.py

docker stop janus-postgres-test
```

Run live CLI tests only when model credentials are available:

```bash
OPENROUTER_API_KEY=... \
PYTHONPATH=packages/janus-core/src:packages/janus-langchain/src:packages/janus-langgraph/src:packages/janus-code/src \
pytest -q tests/janus_code/integration/test_cli_live_e2e.py
```

## Limitations

- The coordinator is currently an in-process coordinator, not a distributed
  transaction manager.
- Durable transaction metadata is not yet externalized for multi-process use.
- The relational shims support row-versioned data, not branch-local schema
  changes.
- Named long-lived branches, branch diff, and branch merge are design targets,
  not stable runtime APIs yet.
- Filesystem isolation requires Linux OverlayFS support or `fuse-overlayfs`.
- Non-transactional external APIs require compensating actions; generic saga
  support is not production-complete in this package.

## Design Notes

The design documents in `docs/` describe the broader research direction:

- `Janus-design.md`: architecture and transaction model
- `Janus-implementation-plan.md`: implementation roadmap
- `Janus-summary.md`: technical overview
- `bolt-on-branching.md`: future relational branch/versioning layer

## Development Guidelines

- Keep `janus-core` independent of LangChain and LangGraph.
- Use public LangChain and LangGraph package APIs in adapter packages.
- Do not depend on internal LangChain or LangGraph module paths.
- Keep transaction semantics in shims explicit: begin, prepare, commit, abort,
  savepoint, rollback, and change inspection.
