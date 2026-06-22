# Chronos Quickstart: Branch State, Review Changes, Merge

Chronos gives applications a branch API for state-changing work. A branch can be
forked from `main`, mutated through the store's normal API, compared against its
parent, merged if approved, or deleted if rejected.

## What You Get

- Named branches for speculative work.
- SQL branches for relational data, including branch-local schema changes when
  schema branching is enabled.
- Filesystem branches for code edits, generated artifacts, and command output.
- Checkpoints, diffs, merge preview, merge apply, and branch deletion.
- A lower-level transaction runtime for short-lived isolated operations.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e packages/chronos-core
```

If the package is not installed, run examples with:

```bash
PYTHONPATH=packages/chronos-core/src
```

## Branch Transaction

Use this pattern for long-running agent workflows. The agent works inside a
branch. The application validates the final state and merges only approved
changes.

```python
from chronos_core.branching import ChronosBranchContext

ctx = ChronosBranchContext.connect("sqlite:///:memory:", backend="interval")
conn = ctx.conn

conn.execute(
    """
    CREATE TABLE tickets (
      id TEXT PRIMARY KEY,
      status TEXT NOT NULL,
      note TEXT NOT NULL
    )
    """
)
conn.execute("INSERT INTO tickets VALUES ('t1', 'open', 'initial report')")
conn.commit()

ctx.register_table("tickets", primary_key=["id"])
ctx.create_branch("agent_fix", from_branch="main")

agent = ctx.checkout("agent_fix")
with agent.transaction():
    agent.execute(
        "UPDATE tickets SET status = :status, note = :note WHERE id = :id",
        {"status": "resolved", "note": "validated fix", "id": "t1"},
    )

diff = ctx.diff("main", "agent_fix")

# App-level guardrails go here: tests, policy checks, reviewers, etc.
approved = bool(diff.changes)

if approved:
    ctx.merge_apply(source="agent_fix", target="main")
else:
    ctx.delete_branch("agent_fix")
```

The branch gives snapshot-style isolation without holding a database transaction
open across model calls or long-running tools.

## Schema Changes On A Branch

Relational branches can diverge in schema when schema branching is enabled. When
a branch changes schema, Chronos uses a branch-local physical schema version
while preserving branch semantics for future forks.

```python
# Create the context with schema branching enabled, then create/register the
# table as in the previous example before branching.
ctx = ChronosBranchContext.connect(
    "sqlite:///:memory:",
    backend="interval",
    enable_schema_branching=True,
)

ctx.create_branch("schema_exp", from_branch="main")
session = ctx.checkout("schema_exp")

with session.transaction():
    session.execute("ALTER TABLE tickets ADD COLUMN severity INTEGER DEFAULT 0")
    session.execute(
        "UPDATE tickets SET severity = :severity WHERE id = :id",
        {"severity": 3, "id": "t1"},
    )

assert "severity" not in ctx.checkout("main").query("SELECT * FROM tickets")[0]
assert session.query("SELECT severity FROM tickets WHERE id = :id", {"id": "t1"}) == [
    {"severity": 3}
]
```

## Filesystem Branches

Filesystem branches expose normal paths. Code and command-line tools read and
write the branch directory directly; they do not need a Chronos-specific file
API. The current design stores filesystem state in Chronos interval-managed SQL
tables, with fixed-size file blocks versioned by the interval backend.

See `filesystem-on-chronos.md` for the SQL-backed filesystem design,
`multi-store-branching.md` for the broader multi-store design, and
`bolt-on-branching.md` for the relational interval backend.
See `related-work.md` for a research log of papers and systems related to
Chronos branching.

When mounted through ChronosFS, agents can use POSIX paths and the `.chronos`
control plane:

```bash
mkdir .chronos/branches/agent
printf 'agent\n' > .chronos/current
cat .chronos/merge-preview/agent..main.json
cat > .chronos/merge-apply/agent..main <<'JSON'
{"policy":"weak_snapshot_isolation"}
JSON
```

Mermaid diagrams live in `docs/diagrams/`, including:

- `interval-architecture-overview.mmd`
- `interval-dml-processing.mmd`
- `interval-merge-publish.mmd`
- `polystore-session-processing.mmd`
- `polystore-workspace-processing.mmd`
- `chronosfs-posix-control-plane.mmd`

## Branch Transactions Versus Database Transactions

A normal database transaction is fast, but holding it open while an LLM thinks
or a tool runs can reduce concurrency and still does not isolate filesystem
effects. A saga avoids a long transaction, but it exposes intermediate state and
needs compensation logic. A Chronos branch transaction keeps the attempt private
until merge:

```text
branch = fork(application_state)
agent mutates files and relational data inside the branch
system computes the diff
policy checks the final state
merge approved changes or delete the branch
```
