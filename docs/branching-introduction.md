# Chronos Branching: High-Level Introduction

**Status:** Draft

## Summary

Chronos branching is a state management layer for agentic applications. Its
vision is to provide one unified branching abstraction across many state stores:
relational databases, filesystems, sandboxes, vector stores, object stores, and
other application-managed state. Agents should be able to fork, mutate,
evaluate, compare, discard, and merge speculative execution paths without
reasoning separately about each backend's native snapshot mechanism.

Chronos is designed to layer on top of TAR, the Transactional Agent Runtime. TAR
provides the execution substrate: tool coordination, transaction boundaries,
commit/abort, savepoints, and recovery across stateful tools. Chronos adds a
branching abstraction above that substrate: instead of treating an agent run as
one linear transaction, Chronos exposes a logical history tree of states.

The key design choice is to decouple branching from database transactions.
Databases still provide the serial, durable, isolated execution primitive.
Chronos uses those transactions to update branch metadata and branch-local data
atomically, while presenting agents with Git-like branching semantics.

For the relational backend, Chronos is bolt-on: applications keep using SQL over
logical tables, and Chronos rewrites queries to expose the state of the checked
out branch.
## Why Agentic Systems Need Branching

Agentic applications are increasingly stateful. They update short-term memory,
long-term memory, databases, files, vector indexes, generated artifacts, and
external environments. A reliable agent runtime therefore needs more than a
single commit/rollback boundary. It needs structured state exploration.

Several properties make agentic execution different from traditional
transactional workloads:

- Agents often discover errors late. When a trajectory fails, the useful action
  is not always "rollback the last step"; it may be "return to the state before
  step 17 and try a different plan."
- Tool calls and sandboxed programs are stateful. A trajectory may mutate files,
  databases, caches, generated code, indexes, and environment state. Restoring
  the sandbox exactly matters, but full sandbox checkpointing can be expensive.
- Exploration is non-linear. Agents may run multiple branches in parallel,
  compare their outcomes, keep the best branch, and discard the rest.
- The state is heterogeneous. Agent state can include internal state such as
  memory and plans, and external state such as relational data, multimodal data,
  files, and environment state.

The BranchBench paper, "BranchBench: Aligning Database Branching with Agentic
Demands" (arXiv:2604.17180), argues that agentic workloads create a
branch-mutate-evaluate loop over database state. It identifies representative
workloads such as agentic software engineering, failure reproduction, data
curation, Monte Carlo Tree Search, and simulation. These workloads need fast
branch creation and deletion, scalable deep and wide branch trees, efficient
reads and writes on branch state, cross-branch comparison, and isolation between
speculative worlds.

Chronos adopts this view, but broadens the scope from database branching alone
to agent state management across heterogeneous stores. The goal is a system that
supports CRUD over branchable application state with consistency and reliability
guarantees, whether that state lives in relational tables, files, sandboxed
program state, vector indexes, or other backend systems.

## Why Transactions Alone Are Not Enough

A database transaction models a serial unit of work:

```text
begin -> read/write -> commit
              |
              +-> rollback staged work
```

This is useful, but it does not directly model an exploration tree.
Transactions and savepoints let an application time travel inside one linear
execution order. Agent exploration often wants a durable tree:

```text
main
  |
  +-- attempt_a
  |     |
  |     +-- attempt_a1
  |
  +-- attempt_b
        |
        +-- attempt_b1
```

Nested transactions can simulate part of this model. For example, best-of-N
exploration can create subtransactions, commit the winner, and roll back the
losers. But that is still a poor fit for general agentic branching:

- Branches are not first-class named states.
- Parallel branches are awkward to keep alive concurrently.
- Rollback is tied to a stack-like execution order.
- Long-running transactions are problematic because LLM calls and tool
  execution may take seconds or minutes.
- MVCC garbage collection and locks can be harmed by long-lived speculative
  transactions.
- Heterogeneous state such as files, vector stores, and sandboxes does not share
  one native transaction manager.

Chronos therefore treats transactions as an implementation primitive, not as the
user-facing branching abstraction.

## Layering With TAR

TAR provides reliable execution over tools and state systems. Chronos branching
layers on top of TAR to give agents a persistent logical state tree.

```text
Agentic application
  - planning
  - evaluation
  - branch selection
  - policy checks

Chronos branching layer
  - branch creation and checkout
  - branch-local CRUD
  - checkpoints
  - diff, compare, merge
  - branch garbage collection

Predicate rewrite and backend adapters
  - SQL query rewrite
  - filesystem/sandbox overlays
  - vector or memory overlays
  - backend-specific visibility rules

TAR transaction layer
  - begin / commit / abort
  - savepoints
  - tool enrollment
  - recovery and coordination

Underlying state systems
  - PostgreSQL / SQLite
  - filesystems and sandboxes
  - vector stores
  - external services
```

The branching layer gives agents one logical model across many physical storage
systems. The TAR layer ensures each mutation to that model is applied safely.

## System Model

Chronos separates two concerns:

```text
Database/TAR transaction layer
  - serial execution
  - atomic commit and rollback
  - isolation and durability
  - locks, indexes, constraints, recovery

Chronos branching layer
  - named branches
  - arbitrary branch tree topology
  - branch-local reads and writes
  - checkpoints
  - branch comparison and merge
```

A normal database transaction opened through a checked-out Chronos branch
mutates only that branch. If the database transaction commits, the branch state
advances. If it rolls back, the branch state is unchanged.

This lets Chronos preserve ordinary database correctness while exposing a richer
state model to the agent.

## Relational Branching In One Picture

The relational backend has three conceptual layers:

```text
User SQL
  SELECT * FROM products WHERE sku = 'abc'

Predicate rewrite layer
  SELECT *
  FROM physical_products
  WHERE sku = 'abc'
    AND live_lo <= :branch_point
    AND :branch_point < live_hi
    AND deleted = false

Interval backend
  - branches own points in integer space
  - row versions own visibility intervals
  - writes split intervals to preserve branch isolation
```

Applications continue to use logical table names. The branch session determines
which branch state those table names refer to.

## Interval Backend: Core Idea

The interval backend represents branch visibility with a number-line model:

- Every branch owns a unique `branch_point` in an integer space.
- Every physical row version owns a visibility interval `[live_lo, live_hi)`.
- One physical row version can be shared by many branches.
- A row version is visible in branch `b` when:

```text
row.live_lo <= b.branch_point < row.live_hi
AND row.deleted = false
```

Chronos adds this predicate to every user query over a registered logical table.
Reads do not walk the branch lineage. They evaluate one branch point against row
intervals.

## Branching

Each branch owns a segment of the integer space. When Chronos forks a child
branch, it splits the parent branch's current segment:

```text
Before:

  parent segment
  [------------------------------------)

After fork:

  parent continuation
  [------------------)

  child segment
                    [-----------------)
```

The parent continues in one subsegment. The child receives the other subsegment.
Each segment receives an interior branch point. This partitioning recurses as
branches fork from branches.

Branch creation is metadata-only. No user rows are copied when a branch is
created.

## Writes

Writes are the maintenance step that preserves logical branch isolation.

For a given logical primary key and branch, Chronos maintains this invariant:

```text
At most one live physical row is visible for that logical key in that branch.
```

When a branch updates or deletes a logical row, Chronos finds row versions whose
intervals overlap the branch segment and splits them as needed.

Example:

```text
old row:
  value = A
  visible over [0, 100)

branch writes value B over [20, 40)

after write:
  A over [0, 20)
  B over [20, 40)
  A over [40, 100)
```

For delete, the middle interval becomes a tombstone:

```text
A over [0, 20)
deleted tombstone over [20, 40)
A over [40, 100)
```

This is why Chronos can avoid copying full tables at branch creation time while
still giving each branch an isolated logical table view.

## Guarantees and Design Goals

Chronos aims to provide:

- Branch isolation: writes to one branch are not visible in sibling branches.
- Cheap forks: branch creation updates metadata rather than copying user rows.
- Transactional branch mutation: each branch-local SQL transaction commits or
  rolls back atomically.
- Unified branching: agents use one branch/checkpoint/diff/merge abstraction
  across relational data, files, sandboxes, vector stores, and other state
  backends.
- Constant-shape read visibility: reads use a fixed predicate independent of
  branch depth.
- Shared physical storage: unchanged rows are shared across branches.
- Explicit state management: branches, checkpoints, diffs, and merges are
  application-visible concepts.

These guarantees assume that all relevant application tables are registered with
Chronos and accessed through branch-bound sessions.

## Trade-Offs

Chronos intentionally chooses a bolt-on design. That makes it deployable over
existing databases and compatible with ordinary SQL, but it creates trade-offs:

- Query performance depends on the SQL optimizer and indexes over visibility
  metadata.
- Write-heavy branching can fragment physical rows because updates split
  intervals.
- Fixed-width interval spaces require careful allocation, relabeling, or depth
  limits.
- Branch-local schema changes are outside the first relational backend design;
  the initial model branches row contents under a shared schema.
- Cross-backend consistency depends on TAR coordination and backend adapters.

These are different trade-offs from systems that implement branching in the
storage layer, WAL layer, or a custom content-addressed storage engine. Chronos
prioritizes a portable logical branching layer for agent state management.
