# Transactional Agent Runtime (Chronos) — Technical Summary

**Version:** 0.1.0 · February 2026

---

## 1  Overview

Chronos brings database-grade transactional guarantees to LLM-powered agent
workflows.  Agents operate in isolated **virtual branches**—ephemeral,
copy-on-write "universes" where changes are buffered until an explicit
commit.  Failed or exploratory branches are discarded without side effects,
giving agents safe *speculative execution* over heterogeneous backends.

---

## 2  Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    LLM  Agent  (ReAct loop)                     │
│  tools: chronos_file_editor · chronos_bash · chronos_sqlite · chronos_memory   │
│         chronos_vectorstore · chronos_txn                               │
├──────────────────────┬──────────────────────────────────────────┤
│  ChronosContext          │  TransactionCoordinator                  │
│  (orchestrator)      │  ┌──────────┐ ┌───────────────────────┐ │
│                      │  │ Branch   │ │ Checkpoint / Savepoint│ │
│                      │  │ Manager  │ │ Manager               │ │
│                      │  └──────────┘ └───────────────────────┘ │
│                      │  ┌───────────────────────┐              │
│                      │  │ Saga Coordinator      │              │
│                      │  │ (compensating actions) │              │
│                      │  └───────────────────────┘              │
├──────────────────────┴──────────────────────────────────────────┤
│                        TMCP  Shim  Layer                        │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌─────────────┐ │
│  │ OverlayFS  │ │ SQLite     │ │ Vector     │ │ Mock API    │ │
│  │ Shim       │ │ MVCC Shim  │ │ (sqlite-   │ │ Shim (saga  │ │
│  │ (files)    │ │            │ │  vec) Shim │ │  + compen.) │ │
│  └────────────┘ └────────────┘ └────────────┘ └─────────────┘ │
├─────────────────────────────────────────────────────────────────┤
│  Real  Backends                                                 │
│  Local FS · SQLite DBs · Vector collections · External APIs     │
└─────────────────────────────────────────────────────────────────┘
```

### Key components

| Component | Role |
|-----------|------|
| **ChronosContext** | Orchestrator that mounts overlays, creates tool instances, and wires them to a shared `TransactionCoordinator`. Exposes `begin()`, `commit()`, `abort()`, `savepoint()`, `rollback()`. |
| **TransactionCoordinator** | Manages the full transaction lifecycle, shim enrollment, and the 2PC commit protocol.  Houses the BranchManager, CheckpointManager, and SagaCoordinator. |
| **BranchManager** | Creates and tracks virtual branches (one per transaction, with parent→child hierarchy for nested subtransactions). |
| **CheckpointManager** | Handles savepoint creation, partial rollback, and optional auto-checkpoint strategies (by action count, time interval, or dangerous-operation trigger). |
| **SagaCoordinator** | Runs saga-style compensating actions for non-transactional side-effects (API calls, emails, payments). |
| **TMCP Shim** | Abstract interface that every backend adapter must implement (see §3). |

---

## 3  TMCP Shim Interface

Every supported backend is wrapped by a **TMCP Shim** (Transactional Model
Context Protocol Shim) — an abstract class `TMCPShim` that enforces a
uniform contract:

```
TMCPShim
 ├── shim_id: str          # unique instance identifier
 ├── shim_type: str         # "sqlite" | "filesystem" | "vector" | "mock_api"
 │
 ├── begin(txn)             # create branch / snapshot
 ├── prepare(txn) → Vote    # 2PC phase-1: COMMIT or ABORT
 ├── commit(txn)            # 2PC phase-2: merge branch → main
 ├── abort(txn)             # discard branch
 │
 ├── savepoint(txn, sp)              # checkpoint within txn
 ├── rollback_to_savepoint(txn, sp)  # partial rollback
 ├── release_savepoint(txn, sp)      # optional cleanup
 │
 ├── get_changes(txn) → [ChangeRecord]
 └── health_check() → bool
```

A convenience base class `BaseTMCPShim` provides change-tracking
bookkeeping and default savepoint logic (truncate the change list to the
recorded marker).

---

## 4  Versioning / Concurrency Protocol

Chronos uses an **Epoxy-inspired MVCC** scheme combined with a lightweight
**two-phase commit (2PC)** protocol.

### 4.1  Transaction lifecycle

```
  begin ──► ACTIVE ──► prepare ──► PREPARING ──► commit ──► COMMITTED
                │                      │
                │                      └──► abort ──► ABORTED
                └──────────────────────────► abort ──► ABORTED
```

### 4.2  Snapshot isolation

Each transaction records a `snapshot_timestamp` at `begin()`.  Reads see
only data visible at that timestamp; writes go into a per-branch shadow
store.

| Backend | Branch isolation mechanism |
|---------|---------------------------|
| SQLite  | In-memory shadow store keyed by `(table, key)`. Reads check shadow first, fall through to the main DB. |
| Filesystem | Linux **OverlayFS** kernel-level copy-on-write.  Writes land in `upperdir`; reads merge `upperdir` + `lowerdir` transparently. |
| Vector store | Per-branch overlay of `VectorOverlayEntry` records.  Queries combine overlay + main collection. |
| External APIs | Not isolated — compensated via **Saga** (see §4.4). |

### 4.3  Two-phase commit

When `commit()` is called on the coordinator:

1. **Prepare phase** — Each enrolled shim is asked `prepare(txn)`.
   It validates pending changes and returns `Vote.COMMIT` or `Vote.ABORT`.
2. **Decision** — If *all* shims vote COMMIT, proceed; otherwise abort all.
3. **Commit / Abort phase** — Each shim applies (or discards) its branch
   changes atomically.

This guarantees all-or-nothing semantics across heterogeneous backends.

### 4.4  Savepoints and partial rollback

Within a transaction, agents can create named **savepoints**.  Each shim
records enough state (change-list index, overlay snapshot, OverlayFS
sub-layer) to truncate back to a savepoint without aborting the entire
transaction.

Savepoints enable the core "speculative try → observe → keep or undo"
loop that makes agents reliable.

### 4.5  Saga compensation

For truly non-reversible side-effects (payment charges, sent emails), Chronos
registers `CompensatingAction` objects.  If the saga's forward path fails,
compensations are executed in reverse order.  Each compensation must be
**idempotent** (safe to retry).

---

## 5  Supported Data Systems

| # | Data system | Shim class | Isolation mechanism | Notes |
|---|-------------|------------|---------------------|-------|
| 1 | **Local filesystem** | `OverlayFSShim` / `FileSystemShim` | Linux OverlayFS (kernel CoW) or in-memory overlay | File reads, writes, dirs, deletes — all isolated |
| 2 | **SQLite** | `SQLiteShim` | MVCC shadow store with version counters | KV + relational interface; async via `aiosqlite` |
| 3 | **Vector store** (sqlite-vec / brute-force) | `SqliteVecShim` / `ChromaDBShim` | Per-branch overlay entries; queries merge overlay + main | Supports `add_texts`, `similarity_search`, `delete` |
| 4 | **External APIs** (mock) | `MockAPIShim` | Saga-based compensation (refunds, unsend, etc.) | Payment, email, generic API calls |

All four participate in the same 2PC protocol, so a single `commit()` or
`abort()` is atomic across files + database + vectors + API effects.

---

## 6  Agent-Facing Tools (LangChain)

Chronos ships as a LangChain partner package (`langchain-chronos`) that exposes
six tools to LLM agents:

| Tool | Description |
|------|-------------|
| `chronos_file_editor` | View, create, edit, delete files inside the transactional overlay. |
| `chronos_bash` | Run shell commands with `cwd` set to the overlay's merged directory. |
| `chronos_sqlite` | MVCC key-value / SQL operations on a transactional SQLite store. |
| `chronos_memory` | Persistent agent scratchpad (files under `memories/`), committed or rolled back with the transaction. |
| `chronos_vectorstore` | Transactional vector storage — add embeddings, similarity search, delete — all branch-isolated. |
| `chronos_txn` | Transaction control: `begin`, `commit`, `abort`, `savepoint <name>`, `rollback`, `status`, `changes`. |

`ChronosContext` is the entry point that mounts the overlay, creates all tools
bound to a shared coordinator, and exposes convenience functions:

```python
from langchain_chronos import ChronosContext, ChronosTransactionControl

ctx = ChronosContext("/path/to/project")
ctx.begin()                          # mount overlay, create tools
tools = ctx.get_tools()              # [file_editor, memory, bash, sqlite?, vec?]
txn_ctl = ChronosTransactionControl(ctx) # explicit txn control tool

# ... agent loop ...

ctx.commit()  # 2PC across all backends
```

---

## 7  Design Principles

1. **Bolt-on, not baked-in.**  Chronos wraps unmodified backends via shims;
   no backend code changes required.
2. **Same tool semantics.**  `chronos_file_editor` behaves identically to a
   plain `file_editor` — agents don't need new skills, only new
   affordances (savepoint/rollback).
3. **All-or-nothing across backends.**  2PC ensures a single commit is
   atomic over files, databases, vectors, and API effects.
4. **Safe speculation.**  Savepoints + rollback cost O(1) — OverlayFS
   discards the upperdir; SQLite truncates the shadow list.
5. **Minimal LLM overhead.**  Transaction metadata stays outside the
   context window; only tool results are visible to the model.
