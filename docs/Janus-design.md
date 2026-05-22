# Transactional Agent Runtime (Janus)

## Design Document

**Version:** 0.1.0  
**Status:** Draft  
**Authors:** Xinjing Zhou
**Date:** February 2026

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Problem Statement](#2-problem-statement)
3. [Proposed Solution](#3-proposed-solution)
4. [System Architecture](#4-system-architecture)
5. [Interface Layer: TMCP Protocol](#5-interface-layer-tmcp-protocol)
6. [Storage Layer: Shim-Based Virtual Branching](#6-storage-layer-shim-based-virtual-branching)
7. [Concurrency Control](#7-concurrency-control)
8. [Checkpoints and Subtransactions](#8-checkpoints-and-subtransactions)
9. [Context Window Optimization](#9-context-window-optimization)
10. [Core Research Questions](#10-core-research-questions)
11. [Demo Applications](#11-demo-applications)
12. [Implementation Roadmap](#12-implementation-roadmap)

---

## 1. Executive Summary

Transactional Agent Runtime (Janus) is a novel runtime infrastructure that brings database-grade transactional guarantees to autonomous AI agent workflows. As LLM-powered agents evolve from stateless chatbots into long-running, state-modifying systems, they require robust mechanisms to safely explore actions, recover from failures, and maintain data integrity across heterogeneous backends.

Janus introduces **Virtual Branching**—a Git-like branching model for agent execution—where agents operate in isolated, ephemeral "universes." Successful workflows commit their changes to the main state, while failed or hallucinated workflows are cleanly discarded without side effects.

**Key Contributions:**
- **TMCP (Transactional Model Context Protocol):** An extension to MCP that adds transaction primitives (`Begin`, `Commit`, `Rollback`, `Savepoint`) for unified state management.
- **Bolt-On Shim Architecture:** A per-tool shim layer that enables virtual branching on systems that don't natively support it (e.g., MySQL, standard filesystems via Linux OverlayFS, SaaS APIs).
- **Epoxy-Inspired Zero-Copy MVCC:** A multi-version concurrency control scheme—drawing from the Epoxy polystore transaction protocol—that achieves snapshot isolation across heterogeneous data systems *without copying data*. Each record is tagged with `beginTxn`/`endTxn` metadata; reads apply predicate filters to see exactly the right snapshot. This works transparently across relational stores (filter columns), vector stores (metadata-filtered search), object stores (object tags), and filesystems (OverlayFS kernel-level CoW).
- **Checkpoint/Subtransaction Abstractions:** Higher-level primitives that abstract away rollback complexity from the LLM, improving context window efficiency.

---

## 2. Problem Statement

### 2.1 The Evolution of AI Agents

AI agents are rapidly evolving from simple question-answering systems into autonomous workflows that:

| Generation | Characteristics | State Management |
|------------|-----------------|------------------|
| **Gen 1: Chatbots** | Single-turn Q&A, stateless | None |
| **Gen 2: Assistants** | Multi-turn conversations, memory | Session context |
| **Gen 3: Agents** | Tool use, ReAct loops, external actions | **Persistent state modification** |
| **Gen 4: Autonomous Workflows** | Long-running, multi-system, self-directed | **Distributed transactions** |

### 2.2 The Reliability Crisis

Current agent infrastructure relies on:
- **Application-level retry logic:** Simple retry-on-failure patterns that don't account for partial state corruption.
- **Prompt engineering:** Instructions like "be careful" or "verify before acting" that have no enforcement mechanism.
- **Hope-based error handling:** Assuming the LLM "won't make mistakes" for critical operations.

**This is inadequate because:**

1. **LLMs are non-deterministic:** The same prompt can produce different (and sometimes wrong) outputs.
2. **Hallucinations corrupt state:** An agent might confidently execute `DROP TABLE users` believing it's cleaning up test data.
3. **Partial failures leave inconsistency:** If an agent updates Database A but fails mid-way through updating Database B, the system is left in an inconsistent state.
4. **No isolation between attempts:** Retry logic re-executes on already-corrupted state.

### 2.3 The Need for "Playground" Semantics

To safely leverage LLM capabilities, agents need the ability to:

```
┌─────────────────────────────────────────────────────────────────┐
│  SPECULATIVE EXECUTION: Try actions without permanent effects   │
├─────────────────────────────────────────────────────────────────┤
│  • Execute "what-if" scenarios                                  │
│  • Observe consequences of actions before committing            │
│  • Safely explore multiple solution paths                       │
│  • Recover cleanly from mistakes                                │
└─────────────────────────────────────────────────────────────────┘
```

**Analogy:** Just as Git allows developers to code on a branch without breaking `main`, Janus allows agents to operate in ephemeral, isolated "universes."

---

## 3. Proposed Solution

### 3.1 Transactional Agent Runtime Overview

Janus wraps the ReAct (Reasoning-Act) loop within a distributed transaction:

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        TRANSACTIONAL AGENT RUNTIME                       │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│    ┌─────────────┐     ┌─────────────┐     ┌─────────────┐              │
│    │   OBSERVE   │────▶│   REASON    │────▶│    ACT      │              │
│    │  (Read)     │     │  (LLM)      │     │  (Write)    │              │
│    └─────────────┘     └─────────────┘     └─────────────┘              │
│           │                   │                   │                      │
│           ▼                   ▼                   ▼                      │
│    ┌─────────────────────────────────────────────────────┐              │
│    │              TRANSACTION COORDINATOR                 │              │
│    │  • Begin / Savepoint / Rollback / Commit            │              │
│    │  • Branch Management                                 │              │
│    │  • Conflict Detection                                │              │
│    └─────────────────────────────────────────────────────┘              │
│                              │                                           │
│           ┌──────────────────┼──────────────────┐                       │
│           ▼                  ▼                  ▼                        │
│    ┌───────────┐      ┌───────────┐      ┌───────────┐                  │
│    │  SQL Shim │      │OverlayFS  │      │Vector Shim│                  │
│    └───────────┘      │  Shim     │      └───────────┘                  │
│           │           └───────────┘            │                        │
│           │                  │                  │                        │
│           ▼                  ▼                  ▼                        │
│    ┌───────────┐      ┌───────────┐      ┌───────────┐                  │
│    │  MySQL    │      │ Linux     │      │ Pinecone  │                  │
│    └───────────┘      │ OverlayFS │      └───────────┘                  │
│                       └───────────┘                                     │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Core Abstractions

| Abstraction | Description | Analogy |
|-------------|-------------|---------|
| **Branch** | An isolated execution context with its own view of state | Git branch |
| **Savepoint** | A checkpoint within a branch that can be rolled back to | Git commit (local) |
| **Commit** | Merge branch changes into the main state | Git merge to main |
| **Rollback** | Discard branch changes entirely | Git branch -D |


### 3.3 Transaction Primitives Exposed to LLM

The runtime exposes these primitives directly to the agent:

```python
# Available tool calls for the LLM agent
tools = [
    "tar_begin()",           # Start a new transaction branch
    "tar_savepoint(name)",   # Create a named checkpoint
    "tar_rollback(target)",  # Rollback to savepoint or abort entirely
    "tar_commit()",          # Commit all changes
    "tar_status()",          # Get current transaction state
]
```

**Key Design Decision:** Rather than hiding transactions from the LLM, we expose them as first-class tools. This enables:
- Explicit agent control over speculative execution
- Clear semantics for exploration vs. commitment
- Debuggability and auditability

---

## 4. System Architecture

### 4.1 Layered Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           AGENT LAYER                                   │
│  • LLM-powered ReAct loop                                               │
│  • Task orchestration                                                   │
│  • High-level tool invocation                                           │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        COORDINATION LAYER                               │
│  • Transaction Coordinator (2PC-like)                                   │
│  • Branch Manager                                                       │
│  • Checkpoint Manager                                                   │
│  • Conflict Detector                                                    │

└───────────────────────────────────┬─────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         INTERFACE LAYER                                 │
│  • TMCP Protocol (Transactional MCP)                                    │
│  • Tool Registry                                                        │
│  • Schema Validation                                                    │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          SHIM LAYER                                     │
│  • Per-tool TMCP Shims                                                  │
│  • Virtual Branching Implementation                                     │
│  • MVCC Management (SQL/Vector) & OverlayFS Mounts (Filesystem)         │

└───────────────────────────────────┬─────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         BACKEND LAYER                                   │
│  • Databases (MySQL, PostgreSQL, MongoDB)                               │
│  • File Systems (Linux OverlayFS for branching, local FS as base)       │
│  • Vector Stores (Pinecone, Weaviate, Qdrant)                           │
│  • External APIs (SaaS services)                                        │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Component Interactions

```
┌─────────┐                    ┌─────────────────┐
│  Agent  │─── tar_begin() ──▶│  Coordinator    │
└─────────┘                    └────────┬────────┘
                                        │
     ┌──────────────────────────────────┼──────────────────────────────┐
     │                                  │                              │
     ▼                                  ▼                              ▼
┌─────────┐                      ┌─────────┐                    ┌──────────┐
│SQL Shim │◀── TMCP_BEGIN ────── │Coord.   │─── TMCP_BEGIN ────▶│OverlayFS │
│         │                      │         │                    │  Shim    │
│ Creates │                      │ Assigns │                    │ Mounts   │
│ Branch  │                      │ TxnID   │                    │ overlay  │
└─────────┘                      └─────────┘                    └──────────┘

     │                                                               │
     └───────────────────────┬───────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Agent executes │
                    │  tool calls in  │
                    │  branch context │
                    │  (uses merged/) │
                    └─────────────────┘
```

---

## 5. Interface Layer: TMCP Protocol

### 5.1 Transactional Model Context Protocol (TMCP)

TMCP extends the standard Model Context Protocol (MCP) with transaction semantics:

```typescript
interface TMCPProtocol extends MCPProtocol {
  // Transaction Management
  tmcp_begin(options?: BeginOptions): Promise<TransactionHandle>;
  tmcp_commit(txn: TransactionHandle): Promise<CommitResult>;
  tmcp_rollback(txn: TransactionHandle, target?: SavepointId): Promise<void>;
  
  // Checkpoint Management
  tmcp_savepoint(txn: TransactionHandle, name: string): Promise<SavepointId>;
  tmcp_release_savepoint(txn: TransactionHandle, id: SavepointId): Promise<void>;
  
  // Status & Inspection
  tmcp_status(txn: TransactionHandle): Promise<TransactionStatus>;
  tmcp_list_changes(txn: TransactionHandle): Promise<ChangeSet>;
  
  // Standard MCP operations (now transaction-aware)
  read(resource: ResourceId, txn?: TransactionHandle): Promise<Data>;
  write(resource: ResourceId, data: Data, txn?: TransactionHandle): Promise<void>;
}
```

### 5.2 Transaction Handle

```typescript
interface TransactionHandle {
  id: string;                    // Unique transaction identifier
  branchId: string;              // Virtual branch identifier
  parentBranch: string | null;   // For nested transactions
  startTimestamp: number;        // Snapshot timestamp
  state: 'ACTIVE' | 'PREPARING' | 'COMMITTED' | 'ABORTED';
  participants: ParticipantId[]; // Enrolled TMCP shims
}
```

### 5.3 Begin Options

```typescript
interface BeginOptions {
  isolationLevel?: 'READ_COMMITTED' | 'SNAPSHOT' | 'SERIALIZABLE';
  timeout?: number;              // Auto-abort after timeout (ms)
  readOnly?: boolean;            // Optimize for read-only transactions
  parent?: TransactionHandle;    // For subtransactions
  metadata?: Record<string, any>; // Agent-provided context
}
```

### 5.4 2PC-Like Protocol Flow

```
                    ┌──────────────────────┐
                    │    COORDINATOR       │
                    └──────────┬───────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
          ▼                    ▼                    ▼
    ┌──────────┐         ┌──────────┐         ┌──────────┐
    │ Shim A   │         │ Shim B   │         │ Shim C   │
    │ (MySQL)  │         │(OvlayFS) │         │ (Vector) │
    └──────────┘         └──────────┘         └──────────┘

PHASE 1: PREPARE
    ├──── PREPARE ────▶├──── PREPARE ────▶├──── PREPARE ────▶
    │                  │                  │
    ◀── VOTE_COMMIT ──◀── VOTE_COMMIT ──◀── VOTE_COMMIT ──

PHASE 2: COMMIT (if all voted COMMIT)
    ├──── COMMIT ─────▶├──── COMMIT ─────▶├──── COMMIT ─────▶
    │                  │                  │
    ◀──── ACK ────────◀──── ACK ────────◀──── ACK ────────

PHASE 2: ABORT (if any voted ABORT)
    ├──── ABORT ──────▶├──── ABORT ──────▶├──── ABORT ──────▶
    │                  │                  │
    (Rollback actions triggered)
```

### 5.5 The "Bolt-On" Shim Design

**Challenge:** Most stateful systems don't support branching natively.

**Solution:** Per-tool shims that implement TMCP by intercepting operations and managing virtual state.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         SHIM ARCHITECTURE                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌───────────────┐                                                  │
│  │ Incoming      │                                                  │
│  │ Operation     │                                                  │
│  │ (e.g., UPDATE)│                                                  │
│  └───────┬───────┘                                                  │
│          │                                                          │
│          ▼                                                          │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    INTERCEPTOR                                 │  │
│  │  • Extract operation metadata                                  │  │
│  │  • Identify affected keys                                      │  │
│  │  • Check transaction context                                   │  │
│  └───────────────────────────────────────────────────────────────┘  │
│          │                                                          │
│          ▼                                                          │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │            EPOXY-STYLE METADATA INTERPOSITION                  │  │
│  │                                                                │  │
│  │  For WRITE (zero-copy versioning):                             │  │
│  │    • Tag new record with beginTxn = current_txn_id             │  │
│  │    • Set endTxn = current_txn_id on superseded old record      │  │
│  │    • No data is copied — only metadata is added                │  │
│  │    • FS: kernel-level CoW via OverlayFS upperdir               │  │
│  │                                                                │  │
│  │  For READ (snapshot isolation via predicate filtering):        │  │
│  │    • Append filter: visible(record, txn_snapshot)              │  │
│  │    • Predicate: beginTxn committed before snapshot             │  │
│  │                  AND endTxn not committed before snapshot      │  │
│  │    • FS: kernel handles automatically via merged dir           │  │
│  │                                                                │  │
│  │  For DELETE:                                                    │  │
│  │    • Set endTxn = current_txn_id on target record              │  │
│  │    • No physical deletion — GC reclaims later                  │  │
│  │    • FS: kernel creates whiteout (.wh.*) in upperdir           │  │
│  └───────────────────────────────────────────────────────────────┘  │
│          │                                                          │
│          ▼                                                          │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │              UNDERLYING DATA SYSTEM                            │  │
│  │  • MySQL / PostgreSQL / Pinecone / S3 / etc.                   │  │
│  │  • Sees only physical operations (inserts + metadata updates)  │  │
│  │  • No native transaction support required                      │  │
│  │  • Only needs: durable writes + metadata filtering             │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Key Insight (from Epoxy):** The shim does *not* require 2PC participant protocol support from the underlying data system. The only requirement is **durable writes** — the coordinator decides commit/abort, and the shim merely tags records with versioning metadata on writes and applies predicate filters on reads. This makes it possible to bolt transaction semantics onto virtually any data system that supports metadata storage and filtering.

### 5.6 Minimum Backend Requirements (Epoxy Model)

For a shim to implement zero-copy virtual branching via MVCC metadata, the underlying system needs only:

| Requirement | Description | Example |
|-------------|-------------|---------|
| **Durable Writes** | Writes persist once acknowledged | Any database, object store, or filesystem |
| **Metadata Storage** | Ability to attach `beginTxn`/`endTxn` fields to records | SQL columns, document fields, vector metadata, object tags |
| **Metadata Filtering** | Query/filter records by metadata predicates | `WHERE beginTxn < ? AND endTxn >= ?`, vector metadata filter, S3 ListObjects with tag filter |

**Key non-requirements (following Epoxy):**
- **No native MVCC support needed** — the shim implements MVCC externally via metadata
- **No native transaction support needed** — beyond single-record atomic writes
- **No 2PC participant protocol needed** — coordinator makes commit/abort decisions unilaterally; participants only need durable writes
- **No built-in branching needed** — branching emerges from metadata filtering
- **No data copying needed** — records are tagged in-place; snapshot isolation is achieved via read-time predicate filters

**Data System Capability Spectrum:**

```
┌─────────────────────────────────────────────────────────────────────┐
│           DATA SYSTEM CAPABILITY FOR ZERO-COPY BRANCHING            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  TIER 1: Native Kernel-Level CoW (zero shim overhead)               │
│  ├── Linux OverlayFS    → kernel handles CoW, whiteouts, merging   │
│  └── ZFS/Btrfs snapshots → filesystem-level branching               │
│                                                                     │
│  TIER 2: Inline Metadata Filtering (predicate push-down)            │
│  ├── SQL databases      → beginTxn/endTxn columns + WHERE clause   │
│  ├── Vector stores      → metadata filter on similarity search     │
│  ├── Document stores    → field-level filter on queries            │
│  └── Key-value stores   → composite key or metadata column         │
│                                                                     │
│  TIER 3: External Metadata (coordinator-side tracking)              │
│  ├── Object stores (S3) → object tags or coordinator metadata      │
│  ├── SaaS APIs          → coordinator tracks resource IDs + state  │
│  └── External services  → coordinator-side tracking                │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Special case: OverlayFS Shim**

The filesystem shim bypasses metadata-based MVCC entirely by delegating branching to the Linux kernel's OverlayFS. No metadata columns, no predicate filters—just `mount -t overlay`. This means:
- Zero application-level bookkeeping for CoW, tombstones, or visibility
- Full POSIX compatibility (Unix tools work unmodified on the merged directory)
- Kernel-enforced isolation between concurrent transaction branches
- `overlayfs-tools` handles the merge (commit) operation

---

## 6. Storage Layer: Shim-Based Virtual Branching

### 6.1 Epoxy-Style Zero-Copy MVCC

Drawing from the Epoxy polystore transaction protocol, Janus implements **zero-copy branching** via multi-version concurrency control (MVCC) using two metadata fields on every record:

- **`beginTxn`**: The transaction ID that *created* this record version.
- **`endTxn`**: The transaction ID that *superseded* (updated or deleted) this record. Initially set to `∞` (a sentinel value indicating the record is still live).

No data is ever copied to create a branch. Instead, the shim:
1. **On write**: inserts a *new* record version tagged with `beginTxn = current_txn_id`, and sets `endTxn = current_txn_id` on the previous version.
2. **On read**: applies a **visibility predicate** that filters based on `beginTxn`/`endTxn` relative to the reading transaction's snapshot.

This achieves snapshot isolation across heterogeneous data systems without any data copying.

```
PHYSICAL RECORD TABLE (single shared table — no per-branch copies)
┌──────────────────────────────────────────────────────────────────────┐
│  Key  │  Value       │  beginTxn  │  endTxn  │  Notes               │
├───────┼──────────────┼────────────┼──────────┼──────────────────────┤
│  K1   │  "original"  │  T_init    │  T_42    │  Superseded by T_42  │
│  K1   │  "modified"  │  T_42      │  ∞       │  Current (via T_42)  │
│  K2   │  "data"      │  T_init    │  ∞       │  Never modified      │
│  K3   │  "more"      │  T_init    │  T_99    │  Deleted by T_99     │
│  K4   │  "new key"   │  T_42      │  ∞       │  Created by T_42     │
└──────────────────────────────────────────────────────────────────────┘

Transaction T_42 sees:                  Transaction T_50 (snapshot before T_42 committed) sees:
  K1 → "modified"  (beginTxn=T_42)       K1 → "original"  (beginTxn=T_init, endTxn=T_42 not yet committed)
  K2 → "data"      (beginTxn=T_init)     K2 → "data"      (beginTxn=T_init)
  K3 → "more"      (beginTxn=T_init)     K3 → "more"      (beginTxn=T_init)
  K4 → "new key"   (beginTxn=T_42)       K4 → invisible   (beginTxn=T_42 not committed before snapshot)
```

#### 6.1.1 Visibility Predicate (Snapshot Isolation)

A record `r` is **visible** to transaction `x` with snapshot `S` if and only if:

$$
\text{visible}(r, x, S) = 
\big(\text{committed}(r.\text{beginTxn}, S) \lor r.\text{beginTxn} = x\big)
\;\land\;
\big(\lnot\,\text{committed}(r.\text{endTxn}, S) \land r.\text{endTxn} \neq x\big)
$$

Where $\text{committed}(t, S)$ means transaction $t$ was committed before snapshot $S$ was taken.

In practice (following Epoxy's formulation), for a transaction $x$ that started when the minimum active transaction was $x_{\min}$ and the set of recently committed transactions since $x$ began is $\text{rc}$:

$$
\text{visible}(r, x) = 
\big(r.\text{beginTxn} < x_{\min} \;\lor\; r.\text{beginTxn} \in \text{rc} \;\lor\; r.\text{beginTxn} = x\big)
\;\land\;
\big(r.\text{endTxn} \geq x_{\min} \;\land\; r.\text{endTxn} \notin \text{rc} \;\land\; r.\text{endTxn} \neq x\big)
$$

This predicate can be pushed down into the underlying data system as a **filter**:
- **SQL**: `WHERE (beginTxn < ? OR beginTxn IN (?) OR beginTxn = ?) AND (endTxn >= ? AND endTxn NOT IN (?) AND endTxn != ?)`
- **Vector Store**: metadata filter `{"$and": [{"beginTxn": {"$lt": xmin}}, ...]}`
- **Document Store**: query filter on `beginTxn`/`endTxn` fields
- **Object Store**: coordinator-side visibility check against a metadata index

#### 6.1.2 Why Zero-Copy Matters

| Aspect | Copy-Based Branching | Zero-Copy MVCC (Epoxy) |
|--------|---------------------|------------------------|
| **Branch creation** | Copy entire dataset | Insert 0 records (instant) |
| **Storage overhead** | O(dataset) per branch | O(changes) per branch |
| **Write cost** | Copy-on-write to shadow | Tag record + insert new version |
| **Read cost** | Walk version chain | Single query with filter predicate |
| **Commit cost** | Merge shadow → main | Update `endTxn` on old versions |
| **Abort cost** | Delete shadow copies | Mark `beginTxn`'s txn as aborted (GC later) |
| **Cross-system consistency** | Per-system snapshot | Unified visibility predicate across all stores |

#### 6.1.3 Subtransaction Extension (1-Level Nesting)

The flat Epoxy MVCC model uses a single transaction ID $x$ in the visibility predicate. To support **1-level nested subtransactions** (parent → child, no deeper), we generalize the single "self" ID to a **self-set** $\mathcal{S}(x)$:

$$
\mathcal{S}(x) = \{x\} \cup \{c : c \text{ is a child committed into } x\}
$$

The extended visibility predicate replaces every equality check on $x$ with a **set membership** check on $\mathcal{S}(x)$:

$$
\text{visible}(r, x) = 
\big(r.\text{beginTxn} < x_{\min} \;\lor\; r.\text{beginTxn} \in \text{rc} \;\lor\; r.\text{beginTxn} \in \mathcal{S}(x)\big)
\;\land\;
\big(r.\text{endTxn} \geq x_{\min} \;\land\; r.\text{endTxn} \notin \text{rc} \;\land\; r.\text{endTxn} \notin \mathcal{S}(x)\big)
$$

For a **child** $c$ with parent $p$, the child's self-set is:

$$
\mathcal{S}(c) = \mathcal{S}(p) \cup \{c\}
$$

This means the child sees: base snapshot + parent's writes + committed siblings' writes + its own writes. Crucially, **aborted children are excluded** from $\mathcal{S}$ — their records become automatically invisible.

##### Why This Works: Walkthrough

```
Setup:
  Parent p, snapshot S = (xmin=10, rc={}).
  p writes K1="B" → record: (K1, "B", beginTxn=p, endTxn=∞)
  Child c starts under p.
  c writes K1="C":
    → set endTxn=c on (K1, "B", beginTxn=p)   [superseded by child]
    → insert  (K1, "C", beginTxn=c, endTxn=∞)  [child's version]

During child c's execution:
  S(c) = {p, c}
  • (K1, "B", beginTxn=p, endTxn=c):  endTxn=c ∈ S(c) → NOT visible  ✓
  • (K1, "C", beginTxn=c, endTxn=∞):  beginTxn=c ∈ S(c) → VISIBLE    ✓
  Child sees K1="C" (its own write). Correct.

Parent p's view (child still active):
  S(p) = {p}           [c not committed yet, not in S(p)]
  • (K1, "B", beginTxn=p, endTxn=c):  endTxn=c, c ∉ S(p) → VISIBLE   ✓
  • (K1, "C", beginTxn=c, endTxn=∞):  beginTxn=c, c ∉ S(p) → NOT visible ✓
  Parent sees K1="B" (its own write, unaware of child). Correct.

After child c COMMITS into parent:
  S(p) = {p, c}        [c added to committed-children set]
  • (K1, "B", beginTxn=p, endTxn=c):  endTxn=c ∈ S(p) → NOT visible  ✓
  • (K1, "C", beginTxn=c, endTxn=∞):  beginTxn=c ∈ S(p) → VISIBLE    ✓
  Parent now sees K1="C" (child's write merged). Correct.

After child c ABORTS (alternative path):
  S(p) = {p}           [c NOT added — it was aborted]
  • (K1, "B", beginTxn=p, endTxn=c):  endTxn=c ∉ S(p) → VISIBLE     ✓
  • (K1, "C", beginTxn=c, endTxn=∞):  beginTxn=c ∉ S(p) → NOT visible ✓
  Parent sees K1="B" (child's write discarded, parent restored). Correct.
```

**Critical correctness property:** Both child commit and child abort require **zero per-record work** in any data store. The coordinator simply updates the self-set, and the visibility predicate handles everything at read time.

##### Savepoints as Sequential Children

Savepoints map naturally to sequential child transactions. Each savepoint creates a new child ID; rolling back to a savepoint aborts that child (and all children created after it):

```
Parent p:
  writes w1, w2              (beginTxn = p)
  SAVEPOINT sp1              → child c1 created, current_segment = c1
    writes w3, w4            (beginTxn = c1)
  SAVEPOINT sp2              → child c2 created, current_segment = c2
    writes w5, w6            (beginTxn = c2)
  ROLLBACK TO sp1            → abort c2 AND c1
                               S(p) = {p}  (c1, c2 excluded)
                               w3..w6 invisible, w1..w2 intact
  writes w7                 (beginTxn = p, or new child c3)
  COMMIT                     → commit p globally

Predicate cost during transaction:
  S(p) has at most (1 + num_committed_children) IDs.
  Typical agent: 2-5 savepoints → IN clause with 3-6 IDs.
  Negligible for SQL indexes, vector metadata filters.
```

##### Self-Set Size and Performance

| Scenario | \|S(x)\| | Predicate overhead | Notes |
|----------|----------|-------------------|-------|
| Flat transaction (no children) | 1 | None (degenerates to `= x`) | Default case |
| 1 active child | 2 | `IN (p, c)` vs `= p` | Minimal |
| 3 sequential savepoints (all committed) | 4 | `IN (p, c1, c2, c3)` | Typical agent workflow |
| 5 savepoints, 2 rolled back | 4 | `IN (p, c1, c4, c5)` | Aborted children excluded |
| Max practical | ~10 | `IN (p, c1..c9)` | Still negligible |

SQL `IN` clauses with <20 elements have no measurable overhead on indexed columns. Vector store metadata `$in` filters are equally cheap. The self-set approach introduces **zero overhead for the common case** (flat transactions) and **bounded, negligible overhead** for subtransactions.

##### Constraint: 1-Level Nesting Only

We intentionally restrict to 1-level nesting (parent → child) for several reasons:

1. **Predicate simplicity:** Self-set is flat (no recursive traversal). Checking $\text{beginTxn} \in \mathcal{S}$ is a single `IN` query.
2. **Coordinator simplicity:** `child_to_parent` map is a flat lookup, not a tree traversal.
3. **Commit correctness:** `globally\_committed(t)` requires at most 2 hops: $t \to \text{parent}(t) \to \text{commit\_log}$.
4. **Agent use cases:** Savepoints and speculative exploration rarely need deeper than 1 level. If deeper isolation is truly needed, the OverlayFS shim supports arbitrary nesting independently (via stacked overlays).

### 6.2 Snapshot Isolation via Predicate Filtering

Instead of traversing a version chain, all reads inject a **visibility predicate** that the underlying data system evaluates natively. This pushes snapshot isolation into the storage engine, avoiding application-level version chain walks.

```python
class TxnSnapshot:
    """
    Snapshot context for a transaction, following the Epoxy model
    extended with self-set support for 1-level subtransactions.
    
    The coordinator maintains a global commit log. When a transaction
    begins, the snapshot captures:
      - xmin: the smallest active transaction ID at snapshot time
      - rc_txns: set of transaction IDs that committed after xmin
                 but before the current transaction started
      - self_set: the set {txn_id} ∪ {committed children of txn_id}
                  (see Section 6.1.3 for derivation)
    """
    
    def __init__(self, txn_id: str, xmin: int, rc_txns: Set[int],
                 self_set: Optional[Set[int]] = None):
        self.txn_id = txn_id
        self.xmin = xmin         # lowest active txn at snapshot time
        self.rc_txns = rc_txns   # recently committed txns since xmin
        # self_set defaults to {txn_id} for flat transactions.
        # For subtransactions, it's {parent} ∪ {committed children} ∪ {self}.
        self.self_set = self_set if self_set is not None else {hash(txn_id)}
    
    def with_child(self, child_id: int) -> 'TxnSnapshot':
        """
        Create a derived snapshot for a child subtransaction.
        The child's self_set = parent's self_set ∪ {child_id}.
        The base snapshot (xmin, rc_txns) is inherited unchanged.
        """
        return TxnSnapshot(
            txn_id=self.txn_id,
            xmin=self.xmin,
            rc_txns=self.rc_txns,
            self_set=self.self_set | {child_id},
        )
    
    def commit_child(self, child_id: int) -> None:
        """
        Commit a child into the parent: add child_id to self_set.
        This is a coordinator-only operation — O(1), no per-record work.
        After this call, records tagged with beginTxn=child_id become
        visible to the parent via the expanded self_set.
        """
        self.self_set.add(child_id)
    
    def abort_child(self, child_id: int) -> None:
        """
        Abort a child: ensure child_id is NOT in self_set.
        This is a coordinator-only operation — O(1), no per-record work.
        Records tagged with beginTxn=child_id become invisible;
        records tagged with endTxn=child_id become visible again.
        Both effects happen automatically via the visibility predicate.
        """
        self.self_set.discard(child_id)
    
    def visibility_filter(self) -> dict:
        """
        Generate a filter predicate for the underlying data system.
        
        Returns a dict that each shim translates into the native 
        query language of its backend (SQL WHERE, vector metadata 
        filter, document query, etc.).
        
        The self_set replaces the single txn_id in the predicate:
          beginTxn IN self_set  (instead of beginTxn = x)
          endTxn NOT IN self_set  (instead of endTxn ≠ x)
        For flat transactions (|self_set| = 1), this degenerates to
        the standard equality check with no overhead.
        """
        return {
            "self_set": list(self.self_set),
            "xmin": self.xmin,
            "rc_txns": list(self.rc_txns),
        }
    
    def is_visible(self, begin_txn: int, end_txn: int) -> bool:
        """
        Check if a record with given beginTxn/endTxn is visible 
        to this snapshot.
        
        Extended Epoxy visibility predicate with self_set:
          visible = (beginTxn < xmin ∨ beginTxn ∈ rc ∨ beginTxn ∈ S(x))
                  ∧ (endTxn ≥ xmin ∧ endTxn ∉ rc ∧ endTxn ∉ S(x))
        """
        begin_ok = (
            begin_txn < self.xmin or
            begin_txn in self.rc_txns or
            begin_txn in self.self_set
        )
        end_ok = (
            end_txn >= self.xmin and
            end_txn not in self.rc_txns and
            end_txn not in self.self_set
        )
        return begin_ok and end_ok


def read_with_snapshot(key: str, snapshot: TxnSnapshot, backend) -> Optional[Value]:
    """
    Read the correct version of a key using predicate push-down.
    
    Instead of traversing a version chain, we issue a single query
    with the visibility predicate. The data system evaluates the 
    filter natively (via index scan, metadata filter, etc.).
    """
    # Single query with visibility predicate — no version chain walk
    results = backend.query(
        key=key,
        filter=snapshot.visibility_filter()
    )
    
    # At most one record should be visible per key per snapshot
    if results:
        return results[0].value
    return None
```

#### 6.2.1 Predicate Push-Down by Data System Type

| Data System | How Visibility Predicate is Evaluated | Index Strategy |
|-------------|--------------------------------------|----------------|
| **SQL (PostgreSQL, MySQL)** | `WHERE (beginTxn < ? OR beginTxn IN (...) OR beginTxn = ?) AND (endTxn >= ? AND endTxn NOT IN (...) AND endTxn != ?)` | Composite index on `(key, beginTxn, endTxn)` |
| **Vector Store (Pinecone, Qdrant)** | Metadata filter on similarity search: `filter={"$and": [{"beginTxn": {"$lte": xmin}}, {"endTxn": {"$gte": xmin}}]}` | Native metadata index |
| **Document Store (MongoDB)** | `db.collection.find({key: k, beginTxn: {$lt: xmin}, endTxn: {$gte: xmin}})` | Compound index on `{key, beginTxn, endTxn}` |
| **Key-Value Store (Redis, DynamoDB)** | Coordinator-side filtering or composite keys `key#beginTxn` | Sort key range scan |
| **Object Store (S3)** | Coordinator maintains a metadata index mapping object keys to `beginTxn`/`endTxn`; objects themselves are immutable | Metadata table lookup |
| **Filesystem (OverlayFS)** | Kernel handles natively — merged dir provides correct visibility | No metadata needed |

### 6.3 Write Handling (Zero-Copy Versioning)

Writes never modify existing records. Instead, they create new record versions and mark old ones as superseded:

```python
def write(key: str, value: Value, txn: Transaction, backend) -> None:
    """
    Write a key using Epoxy-style zero-copy versioning.
    
    1. Find the currently visible record for this key (if any)
    2. Set endTxn = txn.id on the old record (marks it superseded)
    3. Insert a new record with beginTxn = txn.id, endTxn = ∞
    
    No data is copied. The old record remains in place for other
    transactions that still need to see it (snapshot isolation).
    Garbage collection reclaims it once no active tx can see it.
    
    For subtransactions: the child uses its OWN txn ID (not the
    parent's). The visibility predicate's self_set handles the
    rest — the child sees parent's writes via S(c) = S(p) ∪ {c}.
    """
    # 1. Find current visible record (respects self_set)
    old_record = read_with_snapshot(key, txn.snapshot, backend)
    
    if old_record is not None:
        # 2. Mark old record as superseded by this transaction
        backend.update_metadata(
            key=key,
            filter=txn.snapshot.visibility_filter(),
            set_fields={"endTxn": txn.numeric_id}
        )
    
    # 3. Insert new version (beginTxn = this txn, endTxn = ∞)
    backend.insert(
        key=key,
        value=value,
        metadata={
            "beginTxn": txn.numeric_id,
            "endTxn": INFINITY,  # sentinel: record is live
        }
    )
    
    # Track in write set for conflict detection at commit
    txn.write_set.add(key)


def delete(key: str, txn: Transaction, backend) -> None:
    """
    Delete a key using Epoxy-style tombstone.
    
    Simply sets endTxn on the current version. No physical delete.
    The record becomes invisible to this and future transactions.
    GC reclaims the storage later.
    """
    backend.update_metadata(
        key=key,
        filter=txn.snapshot.visibility_filter(),
        set_fields={"endTxn": txn.numeric_id}
    )
    
    txn.write_set.add(key)
```

#### 6.3.1 Commit and Abort Semantics

```python
def commit(txn: Transaction, coordinator) -> None:
    """
    Top-level commit: mark txn as committed in the coordinator's commit log.
    
    Key insight from Epoxy: no per-record work is needed at commit time.
    The coordinator simply records that this txn_id is committed.
    All records tagged with beginTxn ∈ S(txn) are now visible to
    future snapshots automatically (because the visibility predicate
    checks the commit log, and globally_committed() traverses the
    child→parent mapping for subtransaction IDs).
    """
    # Validate: check for write-write conflicts (optimistic CC)
    for key in txn.write_set:
        # If another committed txn also wrote this key since our snapshot,
        # that's a write-write conflict → abort
        if coordinator.has_conflicting_write(key, txn):
            raise WriteWriteConflictError(key, txn)
    
    # Record commit in coordinator's commit log
    coordinator.commit_log.add(txn.numeric_id)
    txn.state = "COMMITTED"
    # All committed children's records become globally visible via:
    #   globally_committed(child_id) = child_committed_into_parent(child_id)
    #                                  AND globally_committed(parent_id)


def abort(txn: Transaction, coordinator) -> None:
    """
    Top-level abort: mark txn as aborted. All records with
    beginTxn ∈ S(txn) become permanently invisible. GC reclaims them.
    """
    coordinator.abort_log.add(txn.numeric_id)
    txn.state = "ABORTED"
    
    # Optionally: eagerly reset endTxn on records this txn superseded
    # (optimization to restore visibility without waiting for GC)
    for key in txn.write_set:
        backend.update_metadata(
            key=key,
            filter={"endTxn": txn.numeric_id},
            set_fields={"endTxn": INFINITY}
        )
```

#### 6.3.2 Subtransaction Commit and Abort (1-Level Nesting)

Unlike top-level commit/abort, subtransaction operations are **O(1) coordinator-only** — no per-record work in any data store. The visibility predicate's self-set handles everything.

```python
def commit_child(child: Transaction, parent: Transaction, coordinator) -> None:
    """
    Commit a child subtransaction into its parent.
    
    This is a COORDINATOR-ONLY operation: add child.id to the
    parent's self_set. No per-record updates in any data store.
    
    After this call:
      - Records with beginTxn = child.id become visible to parent
        (because child.id is now in S(parent))
      - Records with endTxn = child.id become invisible to parent
        (because child.id is now in S(parent))
    
    Cost: O(1) — a single set insertion in coordinator memory.
    """
    parent.snapshot.commit_child(child.numeric_id)
    child.state = "COMMITTED_INTO_PARENT"
    
    # Merge write sets so parent tracks all keys for top-level commit
    parent.write_set |= child.write_set


def abort_child(child: Transaction, parent: Transaction, coordinator) -> None:
    """
    Abort a child subtransaction.
    
    This is a COORDINATOR-ONLY operation: ensure child.id is NOT
    in the parent's self_set. No per-record updates in any data store.
    
    After this call:
      - Records with beginTxn = child.id become invisible to parent
        (because child.id is not in S(parent))
      - Records with endTxn = child.id become visible again to parent
        (because child.id is not in S(parent))
      → Parent's state is automatically restored to pre-child state.
    
    Cost: O(1) — a single set removal (or no-op if never added).
    """
    parent.snapshot.abort_child(child.numeric_id)
    child.state = "ABORTED"
    
    # Do NOT merge write sets — child's writes are discarded.
    # GC will eventually reclaim records with beginTxn = child.id.


def rollback_to_savepoint(savepoint_name: str, parent: Transaction,
                          coordinator) -> None:
    """
    Rollback to a named savepoint by aborting all children created
    at or after that savepoint.
    
    Each savepoint records the list of active children at creation.
    Rolling back aborts all children created since (O(1) per child).
    """
    sp = parent.savepoints[savepoint_name]
    
    # Abort all children created after this savepoint
    for child_id in sp.children_to_abort:
        parent.snapshot.abort_child(child_id)
    
    # Restore write set to savepoint's snapshot
    parent.write_set = sp.write_set_snapshot.copy()
    parent.current_segment = None  # back to parent-level writes
```

**Key correctness property:** Subtransaction abort requires NO eager `endTxn` restoration in data stores. In the flat (top-level) abort case, we optionally reset `endTxn` for faster visibility restoration. But for children, the self-set exclusion achieves the same effect at read time with zero write I/O. This is safe because:

1. The parent's predicate checks `endTxn ∉ S(parent)`.
2. An aborted child's ID is never in `S(parent)`.
3. Therefore, `endTxn = aborted_child_id` does NOT cause record invisibility.
4. The parent sees the record as if the child never existed.

The only cost is that aborted children's *created* records (with `beginTxn = child_id`) remain in storage until GC. Since these are invisible to all transactions, they consume space but not correctness or read performance.

### 6.4 Per-Tool Shim Implementations

#### 6.4.1 SQL Database Shim (Relational Stores)

```python
class SQLDatabaseShim(TMCPShim):
    """
    Shim for relational databases (MySQL, PostgreSQL, SQLite).
    
    Implements Epoxy-style zero-copy MVCC by adding beginTxn/endTxn
    columns to tracked tables. All queries are rewritten to include
    the visibility predicate as a WHERE clause filter.
    
    Key property: NO data is copied to create a branch. Branching
    is free — it's just a new transaction ID in the coordinator.
    """
    
    def __init__(self, connection_string: str):
        self.conn = connect(connection_string)
        self._ensure_metadata_columns()
    
    def _ensure_metadata_columns(self):
        """Add Epoxy-style MVCC metadata columns to tracked tables."""
        for table in self.tracked_tables:
            self.conn.execute(f"""
                ALTER TABLE {table} ADD COLUMN IF NOT EXISTS
                    _janus_begin_txn BIGINT NOT NULL DEFAULT 0,
                    _janus_end_txn BIGINT NOT NULL DEFAULT {INFINITY}
            """)
            # Composite index for efficient visibility predicate evaluation
            self.conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_janus_mvcc_{table} 
                ON {table} (_janus_begin_txn, _janus_end_txn)
            """)
    
    def _visibility_where(self, snapshot: TxnSnapshot) -> str:
        """
        Generate SQL WHERE clause for Epoxy visibility predicate.
        
        visible(r) = (beginTxn < xmin ∨ beginTxn ∈ rc ∨ beginTxn = x)
                    ∧ (endTxn ≥ xmin ∧ endTxn ∉ rc ∧ endTxn ≠ x)
        """
        x = snapshot.txn_id
        xmin = snapshot.xmin
        rc = ','.join(str(t) for t in snapshot.rc_txns) if snapshot.rc_txns else '-1'
        
        return (
            f"((_janus_begin_txn < {xmin}) OR (_janus_begin_txn IN ({rc})) OR (_janus_begin_txn = {x})) "
            f"AND ((_janus_end_txn >= {xmin}) AND (_janus_end_txn NOT IN ({rc})) AND (_janus_end_txn != {x}))"
        )
    
    def intercept_query(self, sql: str, txn: Transaction) -> str:
        """
        Rewrite SQL to inject visibility predicate.
        
        SELECT queries get the visibility WHERE clause appended.
        UPDATE/DELETE are converted to metadata-only operations
        (set endTxn on old record, insert new record with beginTxn).
        INSERT gets beginTxn/endTxn metadata tags.
        """
        vis = self._visibility_where(txn.snapshot)
        
        if sql.upper().startswith("SELECT"):
            return self._inject_visibility_filter(sql, vis)
        elif sql.upper().startswith("UPDATE"):
            # UPDATE → set endTxn on old + INSERT new version with beginTxn
            return self._convert_to_new_version(sql, txn)
        elif sql.upper().startswith("DELETE"):
            # DELETE → just set endTxn on matching records
            return self._convert_to_end_txn_update(sql, txn)
        elif sql.upper().startswith("INSERT"):
            # INSERT → tag with beginTxn, endTxn=∞
            return self._tag_with_begin_txn(sql, txn)
        return sql
```

#### 6.4.2 File System Shim (Linux OverlayFS)

The File System shim uses **Linux OverlayFS**—a kernel-level copy-on-write union filesystem—to provide true POSIX-compatible branching. Unlike an in-memory shadow filesystem, OverlayFS mounts are real directories visible to all Unix tools (`grep`, `sed`, `gcc`, `git`, test runners, etc.) with zero application changes.

**Key insight:** OverlayFS already implements copy-on-write semantics natively in the kernel. Writes go to the `upperdir`; reads fall through to the `lowerdir`. Deletes create whiteout files. This is *exactly* the branching model Janus needs for filesystems.

##### 6.4.2.1 OverlayFS Anatomy

```
┌─────────────────────────────────────────────────────────────────────┐
│                    OVERLAYFS MOUNT LAYOUT                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Per-Branch Mount (branch_id encoded in path):                      │
│                                                                     │
│  /tmp/janus/<txn_id>/                                                 │
│  ├── upper/            ← branch-specific writes (CoW)               │
│  ├── work/             ← OverlayFS internal bookkeeping             │
│  └── merged/           ← unified view (reads + writes)              │
│                                                                     │
│  Base directory (shared lowerdir):                                  │
│  /path/to/project/     ← original files, NEVER modified directly    │
│                                                                     │
│  Mount command:                                                     │
│  mount -t overlay overlay \                                         │
│    -o lowerdir=/path/to/project,                                    │
│       upperdir=/tmp/janus/<txn_id>/upper,                             │
│       workdir=/tmp/janus/<txn_id>/work \                              │
│    /tmp/janus/<txn_id>/merged                                         │
│                                                                     │
│  Behavior:                                                          │
│  • READ /merged/foo.py  → returns upper/foo.py if exists,           │
│                            else lowerdir/foo.py                     │
│  • WRITE /merged/foo.py → copy-up to upper/foo.py (kernel handles)  │
│  • DELETE /merged/foo.py → whiteout in upper/ (.wh.foo.py)          │
│  • `gcc`, `grep`, `pytest`, etc. work unmodified on /merged/        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

##### 6.4.2.2 Branch Isolation via Independent Overlays

Multiple transactions share the same `lowerdir` but get independent `upperdir`s, providing full isolation:

```
Shared lowerdir: /path/to/project/

┌──────────────────────────┐     ┌──────────────────────────┐
│   Transaction A          │     │   Transaction B          │
│   branch: txn-abc-123    │     │   branch: txn-def-456    │
├──────────────────────────┤     ├──────────────────────────┤
│                          │     │                          │
│  upper: /tmp/janus/        │     │  upper: /tmp/janus/        │
│    txn-abc-123/upper/    │     │    txn-def-456/upper/    │
│                          │     │                          │
│  merged: /tmp/janus/       │     │  merged: /tmp/janus/       │
│    txn-abc-123/merged/   │     │    txn-def-456/merged/   │
│                          │     │                          │
│  Agent A writes here     │     │  Agent B writes here     │
│  → only A sees changes   │     │  → only B sees changes   │
│  → base is untouched     │     │  → base is untouched     │
└──────────────────────────┘     └──────────────────────────┘
```

##### 6.4.2.3 Branch ID Encoding in Paths

The branch ID is encoded directly into the filesystem path, providing a natural namespace for transaction isolation:

```python
# Path structure:
#   /tmp/janus/<branch_id>/upper/   ← CoW writes
#   /tmp/janus/<branch_id>/work/    ← OverlayFS workdir
#   /tmp/janus/<branch_id>/merged/  ← unified POSIX view
#
# For subtransactions, branch_id includes parent lineage:
#   /tmp/janus/<parent_txn_id>:<sub_txn_id>/upper/
#   /tmp/janus/<parent_txn_id>:<sub_txn_id>/merged/
#
# This makes it trivial to:
#   - List all active branches:  ls /tmp/janus/
#   - Find subtransactions:      ls /tmp/janus/ | grep "^parent_id:"
#   - Clean up after abort:      umount + rm -rf /tmp/janus/<branch_id>/
```

##### 6.4.2.4 Shim Implementation

```python
import subprocess
import shutil
from pathlib import Path
from typing import Optional


class OverlayFSShim(TMCPShim):
    """
    File system shim using Linux OverlayFS for true POSIX-compatible branching.
    
    Each transaction gets its own OverlayFS mount:
      - lowerdir = base project directory (shared, read-only)
      - upperdir = /tmp/janus/<branch_id>/upper (branch-specific writes)
      - merged   = /tmp/janus/<branch_id>/merged (unified view)
    
    Unix tools (grep, gcc, pytest, git diff, etc.) work unmodified
    on the merged directory.
    """
    
    JANUS_ROOT = Path("/tmp/janus")
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path).resolve()
        self.active_mounts: dict[str, Path] = {}  # branch_id → merged path
    
    def _branch_dir(self, branch_id: str) -> Path:
        """Get the root directory for a branch, encoding branch_id in path."""
        return self.JANUS_ROOT / branch_id
    
    def _upper_dir(self, branch_id: str) -> Path:
        return self._branch_dir(branch_id) / "upper"
    
    def _work_dir(self, branch_id: str) -> Path:
        return self._branch_dir(branch_id) / "work"
    
    def _merged_dir(self, branch_id: str) -> Path:
        return self._branch_dir(branch_id) / "merged"
    
    async def tmcp_begin(self, txn_id: str, options: BeginOptions) -> None:
        """
        Mount an OverlayFS for this transaction's branch.
        
        For subtransactions, the parent's merged dir becomes the lowerdir,
        creating a layered overlay stack.
        """
        branch_id = options.get('branch_id', txn_id)
        parent_branch = options.get('parent_branch', None)
        
        # Create directory structure
        for d in [self._upper_dir(branch_id),
                  self._work_dir(branch_id),
                  self._merged_dir(branch_id)]:
            d.mkdir(parents=True, exist_ok=True)
        
        # Determine lowerdir: base path, or parent's merged dir for subtxns
        if parent_branch and parent_branch in self.active_mounts:
            lowerdir = str(self._merged_dir(parent_branch))
        else:
            lowerdir = str(self.base_path)
        
        # Mount OverlayFS
        subprocess.run([
            "mount", "-t", "overlay", "overlay",
            "-o", f"lowerdir={lowerdir},"
                   f"upperdir={self._upper_dir(branch_id)},"
                   f"workdir={self._work_dir(branch_id)}",
            str(self._merged_dir(branch_id))
        ], check=True)
        
        self.active_mounts[branch_id] = self._merged_dir(branch_id)
    
    def get_working_directory(self, branch_id: str) -> Path:
        """
        Return the merged directory for this branch.
        
        This is the path that should be passed to Unix tools, test runners,
        build systems, etc. All reads/writes through this path are
        automatically isolated by the kernel.
        """
        return self._merged_dir(branch_id)
    
    async def tmcp_prepare(self, txn_id: str) -> Vote:
        """
        Prepare phase: validate that the upperdir changes are conflict-free.
        
        Conflict detection: check if any file in the upperdir was also
        modified in the base (lowerdir) since the transaction started.
        """
        branch_id = txn_id  # or resolve from txn metadata
        upper = self._upper_dir(branch_id)
        
        # Walk upperdir to find all modified files (excluding whiteouts)
        conflicts = []
        for changed_file in upper.rglob("*"):
            if changed_file.is_file():
                rel_path = changed_file.relative_to(upper)
                base_file = self.base_path / rel_path
                if base_file.exists():
                    # Compare base mtime against txn start time
                    if base_file.stat().st_mtime > self._txn_start_time(txn_id):
                        conflicts.append(str(rel_path))
        
        if conflicts:
            return Vote.ABORT  # Conflict detected
        return Vote.COMMIT
    
    async def tmcp_commit(self, txn_id: str) -> None:
        """
        Commit: merge upperdir changes back to the base directory
        using overlayfs-tools, then unmount and clean up.
        """
        branch_id = txn_id
        
        # Use overlay-tools "merge" to apply upper → lower
        # This handles whiteouts, opaque dirs, and xattrs correctly
        subprocess.run([
            "overlay", "merge",
            "-l", str(self.base_path),
            "-u", str(self._upper_dir(branch_id)),
        ], check=True)
        
        # Unmount and clean up
        await self._cleanup_branch(branch_id)
    
    async def tmcp_abort(self, txn_id: str) -> None:
        """
        Abort: unmount and discard the upperdir. Zero cost—no changes
        were ever applied to the base directory.
        """
        branch_id = txn_id
        await self._cleanup_branch(branch_id)
    
    async def _cleanup_branch(self, branch_id: str) -> None:
        """Unmount overlay and remove branch directory tree."""
        merged = self._merged_dir(branch_id)
        subprocess.run(["umount", str(merged)], check=True)
        shutil.rmtree(self._branch_dir(branch_id))
        self.active_mounts.pop(branch_id, None)
```

##### 6.4.2.5 Why OverlayFS, Not In-Memory Shadow FS

| Aspect | In-Memory Shadow FS ❌ | OverlayFS ✅ |
|--------|------------------------|---------------|
| **Unix tool compat** | None—tools can't see shadow state | Full POSIX—`gcc`, `grep`, `pytest` work unmodified |
| **Isolation** | Python-level dict overlay | Kernel-enforced per-mount isolation |
| **CoW semantics** | Manual bookkeeping, error-prone | Native kernel CoW, battle-tested |
| **Deletes** | Manual tombstone tracking | Kernel whiteout files (`.wh.*`) |
| **Performance** | Python overhead on every I/O | Zero overhead (kernel-level) |
| **Subtransactions** | Must re-implement visibility chains | Stack overlays: parent merged → child lower |
| **Commit** | Walk dict, copy files manually | `overlay merge` (overlayfs-tools) |
| **Abort** | Walk dict, discard | `umount` + `rm -rf` (instant) |
| **Large files** | Copies everything into RAM | Kernel lazy copy-up, only changed blocks |
| **Platform** | Cross-platform | Linux only (use MaterializedFS fallback elsewhere) |

#### 6.4.3 Vector Store Shim (Metadata-Filtered Search)

```python
class VectorStoreShim(TMCPShim):
    """
    Shim for vector databases (Pinecone, Weaviate, Qdrant, Chroma).
    
    Implements Epoxy-style zero-copy MVCC using vector metadata fields.
    Modern vector stores support metadata filtering on similarity search 
    — this is the key capability that enables zero-copy branching.
    
    Instead of creating per-branch namespaces/collections (which would
    require copying all vectors), we tag each vector with beginTxn/endTxn
    metadata and filter during search. This means:
    
    - Branch creation is FREE (no vector copying)
    - Similarity search naturally respects snapshot isolation
    - Writes insert new vector versions with updated metadata
    - Deletes just set endTxn on existing vectors
    """
    
    def __init__(self, client: VectorDBClient, collection: str):
        self.client = client
        self.collection = collection
    
    def upsert(self, vectors: List[Vector], txn: Transaction) -> None:
        """
        Upsert vectors with Epoxy-style MVCC metadata.
        
        If updating an existing vector, set endTxn on the old version
        and insert a new version with beginTxn = current txn.
        """
        for v in vectors:
            # Check if this vector ID already exists and is visible
            existing = self.client.fetch(
                ids=[v.id],
                filter=self._visibility_filter(txn.snapshot)
            )
            
            if existing:
                # Mark old version as superseded
                self.client.update_metadata(
                    id=v.id,
                    filter=self._visibility_filter(txn.snapshot),
                    metadata={"_janus_end_txn": txn.numeric_id}
                )
            
            # Insert new version with MVCC metadata
            self.client.upsert(vectors=[
                Vector(
                    id=f"{v.id}_v{txn.numeric_id}",  # versioned ID
                    values=v.values,
                    metadata={
                        **v.metadata,
                        "_janus_original_id": v.id,
                        "_janus_begin_txn": txn.numeric_id,
                        "_janus_end_txn": INFINITY,
                    }
                )
            ])
        
        txn.write_set.update(v.id for v in vectors)
    
    def query(self, vector: List[float], top_k: int, 
              txn: Transaction, **kwargs) -> List[Match]:
        """
        Similarity search with snapshot isolation via metadata filtering.
        
        The visibility predicate is pushed down into the vector store's
        native metadata filter. Only vectors visible to this transaction's
        snapshot are considered as candidates.
        
        Key insight: this is a SINGLE query — no need to query multiple
        namespaces and merge results. The metadata filter handles
        everything in one pass through the index.
        """
        results = self.client.query(
            vector=vector,
            top_k=top_k,
            filter=self._visibility_filter(txn.snapshot),
            **kwargs
        )
        
        # Deduplicate by original_id (in case of version overlap)
        seen = set()
        deduped = []
        for match in results:
            orig_id = match.metadata.get("_janus_original_id", match.id)
            if orig_id not in seen:
                seen.add(orig_id)
                deduped.append(match)
        
        return deduped[:top_k]
    
    def _visibility_filter(self, snapshot: TxnSnapshot) -> dict:
        """
        Generate vector store metadata filter for Epoxy visibility.
        
        Pinecone/Qdrant/Chroma all support metadata filtering with
        comparison operators. The visibility predicate translates to:
        
        For the common case (no recently-committed txns to track):
          filter = {beginTxn <= xmin, endTxn > xmin}
        
        Full predicate with rc_txns:
          filter = (beginTxn < xmin OR beginTxn IN rc OR beginTxn = x)
                 AND (endTxn >= xmin AND endTxn NOT IN rc AND endTxn != x)
        """
        x = snapshot.txn_id
        xmin = snapshot.xmin
        
        # Simplified filter for stores with limited predicate support
        # Full Epoxy predicate for stores with rich filtering (Qdrant, Weaviate)
        return {
            "$and": [
                {"$or": [
                    {"_janus_begin_txn": {"$lt": xmin}},
                    {"_janus_begin_txn": {"$in": list(snapshot.rc_txns)}},
                    {"_janus_begin_txn": {"$eq": x}},
                ]},
                {"_janus_end_txn": {"$gte": xmin}},
                {"_janus_end_txn": {"$nin": list(snapshot.rc_txns)}},
                {"_janus_end_txn": {"$ne": x}},
            ]
        }
```

**Why metadata-filtered search beats branch namespaces:**

| Aspect | Per-Branch Namespace ❌ | Metadata-Filtered MVCC ✅ |
|--------|------------------------|---------------------------|
| **Branch creation** | Copy all vectors to new namespace | Free (just a new txn ID) |
| **Storage** | O(vectors × branches) | O(vectors × versions) |
| **Search** | Query 2 namespaces + merge | Single query + metadata filter |
| **Consistency** | Manual merge logic | Automatic via visibility predicate |
| **Cross-store snapshot** | Ad-hoc | Unified predicate across all stores |

#### 6.4.4 Object Store Shim (S3, GCS, Azure Blob)

Object stores present a unique challenge: objects are typically immutable blobs without inline metadata filtering on `GET` or `LIST` operations. Janus handles this with **coordinator-side metadata tracking**.

```python
class ObjectStoreShim(TMCPShim):
    """
    Shim for object stores (S3, GCS, Azure Blob Storage).
    
    Objects are immutable — we can't add beginTxn/endTxn fields 
    inline. Instead, the coordinator maintains a metadata index
    mapping object keys to their MVCC version metadata.
    
    Two strategies:
    
    1. OBJECT TAGS (if supported): Use S3 object tags or GCS 
       custom metadata to store beginTxn/endTxn. ListObjects 
       can filter by tags (S3 Storage Lens / S3 Inventory).
       
    2. COORDINATOR METADATA TABLE: Maintain a side table (in 
       the coordinator's durable store) mapping:
         (bucket, key, version_id) → (beginTxn, endTxn)
       The shim queries this table for visibility, then fetches
       the object by version_id from the object store.
    """
    
    def __init__(self, client: ObjectStoreClient, 
                 metadata_store: MetadataStore):
        self.client = client
        self.metadata = metadata_store  # coordinator-side index
    
    def put_object(self, bucket: str, key: str, body: bytes,
                   txn: Transaction) -> None:
        """
        Put an object with MVCC versioning.
        
        1. Upload new object version (immutable)
        2. Record beginTxn in coordinator metadata
        3. Set endTxn on previous version's metadata entry
        """
        # Upload (objects are immutable; new version gets new version_id)
        version_id = self.client.put_object(
            Bucket=bucket, Key=key, Body=body
        )["VersionId"]
        
        # Record in coordinator metadata
        old_entry = self.metadata.get_visible(bucket, key, txn.snapshot)
        if old_entry:
            self.metadata.update(
                old_entry.id, endTxn=txn.numeric_id
            )
        
        self.metadata.insert(
            bucket=bucket, key=key, version_id=version_id,
            beginTxn=txn.numeric_id, endTxn=INFINITY
        )
        
        txn.write_set.add((bucket, key))
    
    def get_object(self, bucket: str, key: str,
                   txn: Transaction) -> Optional[bytes]:
        """
        Get the version of an object visible to this snapshot.
        
        Look up the coordinator metadata to find the version_id
        matching the visibility predicate, then fetch that version.
        """
        entry = self.metadata.get_visible(bucket, key, txn.snapshot)
        if entry is None:
            return None
        
        return self.client.get_object(
            Bucket=bucket, Key=key, VersionId=entry.version_id
        )["Body"]
    
    def list_objects(self, bucket: str, prefix: str,
                    txn: Transaction) -> List[ObjectInfo]:
        """
        List objects with snapshot isolation.
        
        Query coordinator metadata for all keys matching prefix
        that are visible to this snapshot, then return their info.
        """
        return self.metadata.list_visible(
            bucket=bucket, prefix=prefix, snapshot=txn.snapshot
        )
```

**Object Store MVCC Architecture:**

```
┌──────────────────────────────────────────────────────────────────┐
│                  OBJECT STORE SHIM (S3 / GCS)                    │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────────────────────────┐                 │
│  │        COORDINATOR METADATA INDEX           │                 │
│  │  ┌────────┬────────┬──────────┬──────────┐  │                 │
│  │  │ Key    │VerID   │beginTxn  │ endTxn   │  │                 │
│  │  ├────────┼────────┼──────────┼──────────┤  │                 │
│  │  │ doc.pdf│ v1     │ T_init   │ T_42     │  │                 │
│  │  │ doc.pdf│ v2     │ T_42     │ ∞        │  │                 │
│  │  │ img.png│ v1     │ T_init   │ ∞        │  │                 │
│  │  └────────┴────────┴──────────┴──────────┘  │                 │
│  │  └── Visibility predicate applied here       │                 │
│  └─────────────────────────────────────────────┘                 │
│          │ version_id                                             │
│          ▼                                                        │
│  ┌─────────────────────────────────────────────┐                 │
│  │            OBJECT STORE (S3/GCS)            │                 │
│  │  • Objects are immutable blobs              │                 │
│  │  • Versioning enabled on bucket             │                 │
│  │  • Shim fetches by specific version_id      │                 │
│  └─────────────────────────────────────────────┘                 │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 6.5 Commit Process (Epoxy Model)

The Epoxy-inspired commit model is fundamentally simpler than traditional 2PC because data-system shims require only **durable writes** — the coordinator decides commit/abort unilaterally.

```python
async def commit(txn: Transaction, coordinator: Coordinator) -> CommitResult:
    """
    Top-level commit using the Epoxy protocol.
    
    Key difference from traditional 2PC: no PREPARE phase is needed
    for metadata-based shims (SQL, Vector, Object Store). Records
    are already tagged with beginTxn/endTxn — committing simply
    records the txn as committed in the coordinator's commit log.
    
    Subtransaction handling: at top-level commit, all children with
    status COMMITTED_INTO_PARENT become globally visible via:
        globally_committed(child_id) =
            child_status(child_id) == COMMITTED_INTO_PARENT
            AND globally_committed(parent_id)
    This traversal is at most 2 hops for 1-level nesting.
    
    For OverlayFS: still needs PREPARE (conflict detection via mtime)
    and COMMIT (overlay merge to apply upper → base).
    
    For metadata-based shims: commit is a coordinator-only operation.
    No per-record work — the Epoxy visibility predicate automatically
    makes records with beginTxn ∈ S(txn) visible to future snapshots
    once the parent txn is in the commit log.
    """
    # Phase 0: Ensure no active (uncommitted) children remain
    for child_id in txn.snapshot.self_set - {txn.numeric_id}:
        child = coordinator.get_transaction(child_id)
        if child.state not in ("COMMITTED_INTO_PARENT", "ABORTED"):
            raise ActiveChildError(
                f"Cannot commit parent while child {child_id} is active"
            )
    
    # Phase 1: Validation (optimistic concurrency control)
    #   Check write-write conflicts for ALL keys in the merged write_set
    #   (includes parent's own writes + all committed children's writes).
    for key in txn.write_set:
        if coordinator.has_conflicting_write(key, txn):
            await abort(txn, coordinator)
            raise WriteWriteConflictError(key, txn)
    
    # Phase 1b: OverlayFS-specific conflict check (if enrolled)
    for shim in txn.participants:
        if isinstance(shim, OverlayFSShim):
            vote = await shim.prepare(txn)
            if vote == Vote.ABORT:
                await abort(txn, coordinator)
                raise TransactionAbortedException("OverlayFS conflict")
    
    # Phase 2: Commit
    try:
        # For metadata-based shims: just mark txn as committed.
        # Records with beginTxn ∈ S(txn) become auto-visible to
        # future snapshots via the globally_committed() traversal.
        coordinator.commit_log.add(txn.numeric_id)
        
        # For OverlayFS shim: apply overlay merge (upper → base)  
        for shim in txn.participants:
            if isinstance(shim, OverlayFSShim):
                await shim.commit(txn)
        
        txn.state = "COMMITTED"
        return CommitResult(success=True, changes=txn.write_set)
    
    except Exception as e:
        # Rollback: mark txn as aborted, cleanup OverlayFS mounts
        await abort(txn, coordinator)
        raise TransactionCommitFailedException(str(e))


async def abort(txn: Transaction, coordinator: Coordinator) -> None:
    """
    Top-level abort.
    
    For metadata-based shims: mark txn as aborted in coordinator.
    Records with beginTxn ∈ S(txn) become permanently invisible
    (no transaction will ever include these IDs in its self_set).
    Records with endTxn ∈ S(txn) become visible again (endTxn ∉ S(x)
    for any future txn x, so the endTxn check passes).
    GC will eventually reclaim the invisible records.
    
    Note: unlike flat abort, we do NOT need eager endTxn restoration
    for committed children's writes — the predicate handles it.
    We only eagerly restore endTxn for the parent's OWN writes as
    an optimization (same as flat case).
    
    For OverlayFS: umount + rm -rf (instant cleanup).
    """
    coordinator.abort_log.add(txn.numeric_id)
    
    for shim in txn.participants:
        if isinstance(shim, OverlayFSShim):
            await shim.abort(txn)  # umount + rm -rf
        else:
            # Optional: eagerly reset endTxn for parent's own writes
            await shim.restore_superseded_records(txn)
    
    txn.state = "ABORTED"
```

**Subtransaction Lifecycle in the Commit Process:**

```
  Parent p begins (self_set = {p})
       │
       ├── SAVEPOINT sp1 → child c1 (self_set = {p, c1})
       │       writes use beginTxn = c1
       │
       ├── RELEASE sp1 → commit_child(c1, p)  [O(1)]
       │       self_set becomes {p, c1}
       │       c1's writes now visible to p
       │
       ├── SAVEPOINT sp2 → child c2 (self_set = {p, c1, c2})
       │       writes use beginTxn = c2
       │
       ├── ROLLBACK TO sp2 → abort_child(c2, p)  [O(1)]
       │       self_set becomes {p, c1}
       │       c2's writes invisible; superseded records restored
       │
       └── COMMIT p → coordinator.commit_log.add(p)
               globally_committed(p) = true
               globally_committed(c1) = COMMITTED_INTO_PARENT ∧ gc(p) = true
               globally_committed(c2) = ABORTED → false
```

**Epoxy vs Traditional 2PC Commit (Extended for Subtransactions):**

| Aspect | Traditional 2PC | Epoxy Model | Epoxy + Subtransactions |
|--------|----------------|-------------|------------------------|
| **Prepare phase** | All participants vote | Only OverlayFS | Only OverlayFS |
| **Commit work** | Each participant applies | Coordinator log entry¹ | Coordinator log entry¹ |
| **Child commit** | Re-parent records in all stores | N/A | Coordinator-only O(1) |
| **Child abort** | Restore records in all stores | N/A | Coordinator-only O(1) |
| **Abort cost** | Each participant rolls back | Coordinator + GC | Coordinator + GC |
| **Coordinator failure** | Replay WAL to all participants | Re-read commit log | Re-read commit + child map |

¹ For metadata-based shims. OverlayFS still needs `overlay merge`.

### 6.6 Merge Strategies

When committing, the merge strategy depends on the shim type:

| Strategy | Description | Use Case |
|----------|-------------|----------|
| **Epoxy Commit Log** | Txn added to commit log; visibility automatic | Metadata-based shims (SQL, Vector, Object) |
| **OverlayFS Merge** | `overlay merge` upper → base via overlayfs-tools | Filesystem shim |
| **Conflict Detection** | Write-write conflict check at validation | All shims (optimistic CC) |
| **Custom Merge** | Application-defined merge function | Complex domain logic |

#### 6.6.1 OverlayFS Merge via overlayfs-tools

For the OverlayFS shim, the merge process uses **overlayfs-tools** (`overlay merge`), a purpose-built utility that correctly handles all OverlayFS semantics:

```
┌─────────────────────────────────────────────────────────────────────┐
│                OVERLAYFS COMMIT (MERGE) FLOW                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. PREPARE: Walk upperdir, detect conflicts with base              │
│     ┌─────────────────────────────────────────────────────────┐     │
│     │  For each file in /tmp/janus/<branch_id>/upper/:          │     │
│     │    - If base file mtime > txn.start_time → CONFLICT     │     │
│     │    - If whiteout (.wh.*) and base deleted → OK          │     │
│     │    - Otherwise → CLEAN                                  │     │
│     └─────────────────────────────────────────────────────────┘     │
│                                                                     │
│  2. MERGE: Apply upper → lower via overlayfs-tools                  │
│     ┌─────────────────────────────────────────────────────────┐     │
│     │  $ overlay merge -l /path/to/project -u upper/          │     │
│     │                                                         │     │
│     │  What it does:                                          │     │
│     │    - Copies modified files from upper → base            │     │
│     │    - Deletes base files where whiteouts exist           │     │
│     │    - Handles opaque directories (replace entire dir)    │     │
│     │    - Preserves xattrs and permissions                   │     │
│     └─────────────────────────────────────────────────────────┘     │
│                                                                     │
│  3. CLEANUP: Unmount overlay, remove branch directory               │
│     ┌─────────────────────────────────────────────────────────┐     │
│     │  $ umount /tmp/janus/<branch_id>/merged                   │     │
│     │  $ rm -rf /tmp/janus/<branch_id>/                         │     │
│     └─────────────────────────────────────────────────────────┘     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

#### 6.6.2 Metadata-Based Commit (SQL, Vector, Object Store Shims)

For metadata-based shims, "committing" is a **coordinator-only operation** — no merge needed:

```python
async def metadata_commit(txn: Transaction, coordinator: Coordinator) -> None:
    """
    Commit for metadata-based shims (SQL, Vector, Object Store).
    
    Unlike OverlayFS merge, this requires NO per-record work.
    The coordinator simply adds txn.id to its commit log, and the
    Epoxy visibility predicate automatically makes all records
    tagged with beginTxn = txn.id visible to future snapshots.
    
    This is the key insight from Epoxy: commit is O(1) regardless
    of how many records the transaction touched.
    """
    # 1. Validate: no write-write conflicts
    coordinator.validate_no_conflicts(txn)
    
    # 2. Commit: single log entry (O(1))
    coordinator.commit_log.add(txn.numeric_id)
    
    # That's it. No per-record updates needed.
    # The visibility predicate handles everything:
    #   - Records with beginTxn = txn.id are now visible
    #   - Records with endTxn = txn.id are now invisible
    #   - Both conditions are evaluated at read time via metadata filters
```

---

## 7. Concurrency Control

### 7.1 The Latency Problem

Traditional database concurrency control assumes:
- Transactions complete in milliseconds
- Lock contention is transient
- Deadlock detection is feasible

**Agent transactions break these assumptions:**

| Metric | Traditional DB | Agent Workflow |
|--------|----------------|----------------|
| Transaction duration | 10-100ms | 30s - hours |
| Wait sources | Disk I/O, Network | LLM inference, Human approval |
| Abort cost | Low (retry cheap) | High (wasted LLM tokens, lost context) |

### 7.2 Snapshot Isolation for Agents (Epoxy Model)

**Why Snapshot Isolation (SI)?**
- Reads never block writes
- Writers never block readers
- Natural fit with MVCC-based storage
- Predictable behavior for long-running transactions
- **Unified across heterogeneous data systems** via the Epoxy visibility predicate

#### 7.2.1 How Snapshot Isolation Works with beginTxn/endTxn

When a transaction begins, the coordinator captures a **snapshot** consisting of:
- **`xmin`**: The smallest active transaction ID at snapshot time
- **`rc_txns`**: The set of transactions that committed between `xmin` and the current transaction's start

Every read operation across *every* data system applies the same visibility predicate:

$$
\text{visible}(r) = 
\big(r.\text{beginTxn} < x_{\min} \;\lor\; r.\text{beginTxn} \in \text{rc} \;\lor\; r.\text{beginTxn} = x\big)
\;\land\;
\big(r.\text{endTxn} \geq x_{\min} \;\land\; r.\text{endTxn} \notin \text{rc} \;\land\; r.\text{endTxn} \neq x\big)
$$

This guarantees that each agent sees a **consistent point-in-time** across all data systems — SQL databases, vector stores, object stores — using the *same* predicate translated into each system's native filter language.

```
Timeline (with beginTxn/endTxn MVCC):

    T0        T1        T2        T3        T4
    │         │         │         │         │
    ▼         ▼         ▼         ▼         ▼
    
Record K1:
    v1: [value="A", beginTxn=T_init, endTxn=T2]
    v2: [value="B", beginTxn=T2, endTxn=∞]      ← written by concurrent agent

Agent-123:
    BEGIN (snapshot: xmin=T0, rc_txns={})
    │
    READ K1 → "A"
      visible(v1) = (T_init < T0 ✓) ∧ (endTxn=T2 ≥ T0 ✓, T2 ∉ rc ✓) → VISIBLE
      visible(v2) = (T2 < T0 ✗, T2 ∉ rc ✗, T2 ≠ x ✓) → INVISIBLE
    │
    WRITE K1 = "C" → insert v3: [value="C", beginTxn=T_x, endTxn=∞]
                      update v1: endTxn = T_x
    │
    COMMIT @ T4 → Write-write conflict! (T2 also wrote K1)
                  Coordinator detects: another committed txn wrote K1
                  since our snapshot → ABORT
```

#### 7.2.2 Cross-System Snapshot Consistency (Polystore)

The Epoxy model achieves polystore snapshot consistency *without* distributed locking:

```
┌─────────────────────────────────────────────────────────────────────┐
│          POLYSTORE SNAPSHOT CONSISTENCY (EPOXY MODEL)               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Coordinator assigns snapshot (xmin, rc_txns) at BEGIN.             │
│  SAME snapshot context sent to ALL shims.                           │
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │  SQL Shim    │  │ Vector Shim  │  │ Object Shim  │              │
│  │              │  │              │  │              │              │
│  │  WHERE       │  │  filter={    │  │  metadata    │              │
│  │  beginTxn<10 │  │  beginTxn<10 │  │  .get_visible│              │
│  │  AND         │  │  AND         │  │  (snapshot)  │              │
│  │  endTxn>=10  │  │  endTxn>=10  │  │              │              │
│  │  ...         │  │  ...}        │  │              │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
│        │                  │                  │                      │
│        ▼                  ▼                  ▼                      │
│  ┌───────────────────────────────────────────────────┐              │
│  │   CONSISTENT POINT-IN-TIME VIEW ACROSS ALL STORES │              │
│  │   (no distributed locking, no 2PC for reads)      │              │
│  └───────────────────────────────────────────────────┘              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

#### 7.2.3 Write-Write Conflict Detection (Optimistic CC)

Janus uses **optimistic concurrency control** — transactions execute without acquiring locks and conflicts are detected at commit time:

```python
def validate_no_conflicts(self, txn: Transaction) -> None:
    """
    Optimistic validation: check that no other committed transaction
    wrote to any key in txn's write set since txn's snapshot.
    
    This is the "first-committer-wins" rule from Snapshot Isolation.
    """
    for key in txn.write_set:
        # Check commit log for any txn that:
        #   1. Committed after our snapshot was taken
        #   2. Also wrote to this key
        conflicting_txns = self.commit_log.find_writers(
            key=key,
            committed_after=txn.snapshot.xmin,
            excluding=txn.numeric_id
        )
        if conflicting_txns:
            raise WriteWriteConflictError(
                key=key,
                txn=txn,
                conflicting=conflicting_txns[0],
                message=f"Key {key} was modified by txn {conflicting_txns[0]} "
                        f"since snapshot {txn.snapshot.xmin}"
            )
```

### 7.3 Optimistic Commit with Conflict Resolution

```python
async def optimistic_commit(txn: Transaction, coordinator: Coordinator) -> CommitResult:
    """
    Optimistic commit with conflict detection and resolution.
    """
    # Check for conflicts
    conflicts = await coordinator.detect_conflicts(txn)
    
    if not conflicts:
        # Happy path: no conflicts, commit directly
        return await coordinator.commit(txn)
    
    # Conflict resolution strategies
    resolution = await coordinator.resolve_conflicts(
        txn=txn,
        conflicts=conflicts,
        strategy=txn.conflict_resolution_strategy
    )
    
    if resolution.strategy == 'RETRY':
        # Re-read conflicting data and let agent decide
        raise ConflictRequiresRetryException(conflicts)
    
    elif resolution.strategy == 'MERGE':
        # Automatic merge (e.g., CRDT-style)
        merged = resolution.merged_values
        await coordinator.apply_merged_commit(txn, merged)
        return CommitResult(success=True, merged=True)
    
    elif resolution.strategy == 'ABORT':
        await coordinator.abort(txn)
        raise TransactionAbortedException("Unresolvable conflict")
```

---

## 8. Checkpoints and Subtransactions

**Advisor's Insight:** "Think about checkpoints and subtransactions—can those mechanisms further help?"

### 8.1 Motivation

Agents performing complex tasks often need to:
1. **Experiment with multiple approaches** within a single logical task
2. **Recover from partial failures** without losing all progress
3. **Isolate risky operations** from already-validated work

**Without checkpoints:** A failure at step 10 of 20 requires restarting from step 1.

**With checkpoints:** A failure at step 10 can rollback to step 5's checkpoint, preserving validated work.

### 8.2 Savepoint Abstraction

```
Transaction Timeline with Savepoints:

BEGIN ──┬── Action 1 ──┬── Action 2 ──┬── Action 3 ──┬── ... ──┬── COMMIT
        │              │              │              │         │
        │              ▼              │              ▼         │
        │         SAVEPOINT_1         │         SAVEPOINT_2    │
        │              │              │              │         │
        │              │              │              │         │
        │              └──────────────│──────────────┘         │
        │                 (Can ROLLBACK TO SAVEPOINT_1)        │
        │                                                      │
        └──────────────────────────────────────────────────────┘
                    (Or ROLLBACK entirely)
```

### 8.3 Subtransaction Design

Subtransactions are **nested transactions** that can be independently committed or rolled back. For the OverlayFS shim, subtransactions are implemented as **layered overlays**: the parent transaction's merged directory becomes the child's `lowerdir`, creating a natural visibility chain.

#### 8.3.1 Layered OverlayFS for Subtransactions

```
┌─────────────────────────────────────────────────────────────────────┐
│              LAYERED OVERLAYFS SUBTRANSACTION MODEL                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Base Directory (original project):                                 │
│  /path/to/project/                                                  │
│       │                                                             │
│       ▼  lowerdir                                                   │
│  ┌─────────────────────────────────────────────────────────┐        │
│  │  Parent Transaction: txn-abc-123                        │        │
│  │  upper: /tmp/janus/txn-abc-123/upper/                     │        │
│  │  merged: /tmp/janus/txn-abc-123/merged/                   │        │
│  │         ↑ sees base + parent's writes                   │        │
│  └─────────────────────────┬───────────────────────────────┘        │
│                            │                                        │
│                            ▼  lowerdir (parent's merged)            │
│  ┌─────────────────────────────────────────────────────────┐        │
│  │  Child Subtxn: txn-abc-123:sub-001                      │        │
│  │  upper: /tmp/janus/txn-abc-123:sub-001/upper/             │        │
│  │  merged: /tmp/janus/txn-abc-123:sub-001/merged/           │        │
│  │         ↑ sees base + parent's writes + child's writes  │        │
│  └─────────────────────────┬───────────────────────────────┘        │
│                            │                                        │
│                            ▼  lowerdir (child's merged)             │
│  ┌─────────────────────────────────────────────────────────┐        │
│  │  Grandchild Subtxn: txn-abc-123:sub-001:sub-002         │        │
│  │  upper: /tmp/janus/txn-abc-123:sub-001:sub-002/upper/     │        │
│  │  merged: /tmp/janus/txn-abc-123:sub-001:sub-002/merged/   │        │
│  │         ↑ sees all ancestor writes + own writes         │        │
│  └─────────────────────────────────────────────────────────┘        │
│                                                                     │
│  Key Properties:                                                    │
│  • Visibility flows downward: children see parents, not vice versa  │
│  • Commit = overlay merge child's upper → parent's merged           │
│  • Abort  = umount child + rm -rf (parent unaffected)               │
│  • Branch ID encodes full lineage: "parent:child:grandchild"        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

#### 8.3.2 Parallel Exploration with Sibling Overlays

Multiple subtransactions of the same parent share the same lowerdir (parent's merged) but get independent upperdirs—exactly as verified in the OverlayFS isolation experiment:

```
Parent Transaction: txn-abc-123
  merged: /tmp/janus/txn-abc-123/merged/
       │
       ├──── lowerdir ────┐                ┌──── lowerdir ────┐
       ▼                  │                │                  ▼
  ┌──────────────┐        │                │        ┌──────────────┐
  │ Subtxn A     │        │                │        │ Subtxn B     │
  │ Try approach │        │                │        │ Try approach │
  │ 1 (JWT)      │        │                │        │ 2 (OAuth)    │
  │              │        │                │        │              │
  │ branch_id:   │   Same parent merged    │        │ branch_id:   │
  │ txn-abc:A    │   is lowerdir for both  │        │ txn-abc:B    │
  └──────────────┘        │                │        └──────────────┘
       │                  │                │                  │
       │ FAILED           │                │           SUCCESS│
       ▼                  │                │                  ▼
  umount + rm -rf    ◄────┘                └────►   overlay merge
  (zero cost)                                       into parent upper
```

#### 8.3.3 Subtransaction Implementation

```python
class Subtransaction:
    """
    A nested transaction within a parent transaction.
    
    For the OverlayFS shim, this is implemented as a layered overlay:
    the parent's merged directory becomes this subtransaction's lowerdir.
    Branch ID encodes the full parent:child lineage in the path.
    """
    
    def __init__(self, parent: Transaction, name: str):
        self.id = generate_id()
        self.parent = parent
        self.name = name
        # Branch ID encodes lineage: "parent_branch:subtxn_id"
        self.branch_id = f"{parent.branch_id}:{self.id}"
        self.write_set: Set[Tuple] = set()
        self.status = 'ACTIVE'
        self.savepoints: List[Savepoint] = []
    
    async def begin(self, fs_shim: OverlayFSShim) -> None:
        """
        Mount a layered overlay for this subtransaction.
        Parent's merged dir becomes our lowerdir.
        """
        await fs_shim.tmcp_begin(self.id, {
            'branch_id': self.branch_id,
            'parent_branch': self.parent.branch_id,
        })
        # Now /tmp/janus/<branch_id>/merged/ has:
        #   lowerdir = parent's merged (which itself overlays the base)
        #   upperdir = our own writes
        # Agent tools should use fs_shim.get_working_directory(self.branch_id)
    
    async def commit(self, fs_shim: OverlayFSShim) -> None:
        """
        Merge subtransaction changes to parent branch.
        Uses overlayfs-tools to merge our upper → parent's merged.
        """
        if self.status != 'ACTIVE':
            raise InvalidStateError(f"Cannot commit {self.status} subtransaction")
        
        # Merge our upperdir into parent's overlay
        # This applies our writes to the parent's view
        subprocess.run([
            "overlay", "merge",
            "-l", str(fs_shim._merged_dir(self.parent.branch_id)),
            "-u", str(fs_shim._upper_dir(self.branch_id)),
        ], check=True)
        
        # Unmount and clean up our overlay
        await fs_shim._cleanup_branch(self.branch_id)
        
        self.status = 'COMMITTED'
        self.parent.subtransaction_log.append(self)
    
    async def rollback(self, fs_shim: OverlayFSShim) -> None:
        """
        Discard subtransaction changes.
        Just unmount + rm -rf. Parent's state is completely unaffected.
        """
        if self.status != 'ACTIVE':
            raise InvalidStateError(f"Cannot rollback {self.status} subtransaction")
        
        await fs_shim._cleanup_branch(self.branch_id)
        self.status = 'ABORTED'
```

### 8.4 Unified Subtransaction Model

The OverlayFS layered overlays (§8.3.1–8.3.3) and the MVCC self-set extension (§6.1.3) provide **complementary subtransaction mechanisms** for different shim types. The CheckpointManager unifies them behind a single API.

#### 8.4.1 Shim-Specific Subtransaction Dispatch

| Operation | OverlayFS Shim | Metadata-Based Shims (SQL, Vector, Object) |
|-----------|----------------|---------------------------------------------|
| **Create child** | Mount layered overlay (parent's merged → child's lowerdir) | Coordinator assigns child txn ID; `parent.snapshot.with_child(child_id)` |
| **Child writes** | Writes go to child's upperdir | Records tagged with `beginTxn = child_id` |
| **Child commit** | `overlay merge` child upper → parent merged + umount | Coordinator-only: `parent.snapshot.commit_child(child_id)` — O(1) |
| **Child abort** | `umount + rm -rf` child overlay | Coordinator-only: `parent.snapshot.abort_child(child_id)` — O(1) |
| **Visibility** | File system layer stacking (natural) | Predicate: `beginTxn ∈ S(parent)`, `endTxn ∉ S(parent)` |

**Key design property:** Both mechanisms achieve the same semantic guarantees (child isolation, atomic commit/abort, parent transparency) but via fundamentally different implementation strategies — structural (OverlayFS layers) vs. predicate-based (MVCC self-set).

#### 8.4.2 Nesting Depth Constraints

| Shim Type | Max Nesting | Reason |
|-----------|------------|--------|
| **OverlayFS** | Kernel-limited (typically 2–3 layers) | Linux overlay stacking depth limit |
| **Metadata-based** | 1 level (design constraint) | Keeps predicate flat, O(1) coordinator lookups, max 2-hop `globally_committed()` (see §6.1.3) |

For the current design, we enforce **1-level nesting across all shims**. OverlayFS could technically support deeper nesting (as shown in the grandchild example in §8.3.1), but constraining to 1 level keeps the unified model simple and avoids divergent semantics between shim types.

### 8.5 Checkpoint Manager

```python
class CheckpointManager:
    """
    Manages savepoints and subtransactions for an agent's transaction.
    
    Uses child transaction IDs (§6.1.3) for metadata-based shims and
    layered overlays for the OverlayFS shim. Both mechanisms are
    dispatched behind the same create/rollback API.
    """
    
    def __init__(self, txn: Transaction, coordinator: Coordinator):
        self.txn = txn
        self.coordinator = coordinator
        self.checkpoints: Dict[str, Checkpoint] = {}
        self.checkpoint_stack: List[str] = []
    
    async def create_checkpoint(self, name: str) -> Checkpoint:
        """
        Create a named checkpoint (savepoint).
        
        Implementation:
          1. Allocate a child transaction ID from the coordinator.
          2. For OverlayFS shims: mount a child overlay with parent's
             merged as lowerdir.
          3. For metadata-based shims: future writes use beginTxn =
             child_id (the snapshot's self_set handles visibility).
          4. Record the savepoint for potential rollback.
        """
        child_id = self.coordinator.allocate_child_id(self.txn)
        
        checkpoint = Checkpoint(
            name=name,
            txn_id=self.txn.id,
            child_id=child_id,
            prior_children=list(self.txn.snapshot.self_set),
            timestamp=time.time(),
            agent_state=await self._capture_agent_state()
        )
        
        # Set up child context for each shim type
        for shim in self.txn.participants:
            if isinstance(shim, OverlayFSShim):
                # Mount child overlay: parent merged → child lowerdir
                await shim.begin_child(child_id, self.txn.branch_id)
            else:
                # Metadata shims: switch write segment to child_id
                # (reads use parent.snapshot.with_child(child_id))
                pass
        
        # Set current write segment to child
        self.txn.current_write_segment = child_id
        
        self.checkpoints[name] = checkpoint
        self.checkpoint_stack.append(name)
        await self._persist_checkpoint(checkpoint)
        
        return checkpoint
    
    async def rollback_to_checkpoint(self, name: str) -> None:
        """
        Rollback to a named checkpoint, discarding subsequent changes.
        
        Implementation:
          1. Abort all child IDs created at or after this savepoint.
          2. For OverlayFS: umount + rm -rf each child overlay.
          3. For metadata-based shims: coordinator-only — remove
             child IDs from parent's self_set. Records with
             beginTxn = aborted_child become invisible; records with
             endTxn = aborted_child become visible again. ZERO
             per-record work.
          4. Restore write set to savepoint's snapshot.
        """
        if name not in self.checkpoints:
            raise CheckpointNotFoundError(name)
        
        checkpoint = self.checkpoints[name]
        
        # Collect all children to abort (this savepoint + subsequent)
        children_to_abort = []
        found = False
        for sp_name in list(self.checkpoint_stack):
            if sp_name == name:
                found = True
            if found:
                children_to_abort.append(
                    self.checkpoints[sp_name].child_id
                )
        
        # Abort each child — O(1) per child for metadata shims
        for child_id in children_to_abort:
            # Metadata shims: coordinator-only, no per-record work
            self.txn.snapshot.abort_child(child_id)
            
            # OverlayFS: umount + rm -rf
            for shim in self.txn.participants:
                if isinstance(shim, OverlayFSShim):
                    await shim.abort_child(child_id)
        
        # Restore write set to checkpoint's snapshot
        self.txn.write_set = checkpoint.write_set_snapshot.copy()
        self.txn.current_write_segment = None  # back to parent-level
        
        # Pop checkpoints from this savepoint onwards
        while self.checkpoint_stack and self.checkpoint_stack[-1] != name:
            popped = self.checkpoint_stack.pop()
            del self.checkpoints[popped]
        # Also pop the savepoint itself
        self.checkpoint_stack.pop()
        del self.checkpoints[name]
    
    async def _capture_agent_state(self) -> Dict:
        """Capture relevant agent state for checkpoint."""
        return {
            'participant_states': {
                p.id: await p.get_state() for p in self.txn.participants
            },
            'write_set_snapshot': self.txn.write_set.copy(),
        }
```

**Checkpoint Rollback Cost Comparison:**

| Aspect | Traditional (write-set diff) | MVCC Self-Set Model |
|--------|------------------------------|---------------------|
| **Rollback cost** | O(writes since savepoint) — must undo each record | O(1) coordinator-only — self_set exclusion |
| **Data store I/O** | Must update endTxn on every superseded record | Zero — predicate handles visibility |
| **OverlayFS** | N/A | `umount + rm -rf` (instant, same as before) |
| **Multiple savepoints** | Must undo in reverse order | Abort all children in any order — independent |
| **Recovery** | Must replay undo log | Rebuild self_set from coordinator state |

### 8.6 Automatic Checkpoint Strategies

The runtime can automatically create checkpoints based on configurable policies:

```python
class AutoCheckpointPolicy:
    """Policies for automatic checkpoint creation."""
    
    # Checkpoint after every N tool calls
    EVERY_N_ACTIONS = 'every_n_actions'
    
    # Checkpoint before "dangerous" operations
    BEFORE_DANGEROUS = 'before_dangerous'
    
    # Checkpoint at natural task boundaries
    TASK_BOUNDARIES = 'task_boundaries'
    
    # Checkpoint based on elapsed time
    TIME_BASED = 'time_based'

class AutoCheckpointManager:
    """Automatically manages checkpoints based on policy."""
    
    def __init__(self, policy: AutoCheckpointPolicy, config: Dict):
        self.policy = policy
        self.config = config
        self.action_count = 0
        self.last_checkpoint_time = time.time()
    
    def should_checkpoint(self, action: ToolCall) -> bool:
        """Determine if a checkpoint should be created."""
        if self.policy == AutoCheckpointPolicy.EVERY_N_ACTIONS:
            self.action_count += 1
            return self.action_count >= self.config.get('n', 5)
        
        elif self.policy == AutoCheckpointPolicy.BEFORE_DANGEROUS:
            dangerous_patterns = self.config.get('dangerous_patterns', [
                'DELETE', 'DROP', 'TRUNCATE', 'rm -rf', 'remove'
            ])
            return any(p in str(action) for p in dangerous_patterns)
        
        elif self.policy == AutoCheckpointPolicy.TIME_BASED:
            interval = self.config.get('interval_seconds', 60)
            return time.time() - self.last_checkpoint_time > interval
        
        return False
```

### 8.7 Subtransaction Use Cases

```
┌─────────────────────────────────────────────────────────────────────┐
│                 SUBTRANSACTION USE CASES                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. PARALLEL EXPLORATION (Sibling OverlayFS Mounts)                 │
│     ┌─────────────────────────────────────────────────────────┐     │
│     │  Main Txn (OverlayFS mount on base dir)                 │     │
│     │    ├── Subtxn A: Try approach 1 ──▶ FAILED (umount+rm)  │     │
│     │    ├── Subtxn B: Try approach 2 ──▶ SUCCESS (merge)     │     │
│     │    └── Continue with approach 2's results               │     │
│     │  Each subtxn is a separate overlay sharing parent's     │     │
│     │  merged dir as lowerdir                                 │     │
│     └─────────────────────────────────────────────────────────┘     │
│                                                                     │
│  2. RISKY OPERATION ISOLATION (Layered OverlayFS)                   │
│     ┌─────────────────────────────────────────────────────────┐     │
│     │  Main Txn: Database migration (parent overlay)          │     │
│     │    ├── Step 1: Backup (committed to parent's upper)     │     │
│     │    ├── Subtxn: Apply migration (child overlay)          │     │
│     │    │     ├── Run schema changes (in child's merged/)    │     │
│     │    │     ├── Run data migration                         │     │
│     │    │     └── Validate ──▶ FAILED? umount child only     │     │
│     │    └── Retry with different approach (new child overlay)│     │
│     └─────────────────────────────────────────────────────────┘     │
│                                                                     │
│  3. MULTI-STEP VALIDATION                                           │
│     ┌─────────────────────────────────────────────────────────┐     │
│     │  Main Txn: Code refactoring                             │     │
│     │    ├── Checkpoint: "pre-refactor"                       │     │
│     │    ├── Apply refactoring changes                        │     │
│     │    ├── Run tests                                        │     │
│     │    │     └── FAILED? Rollback to "pre-refactor"         │     │
│     │    ├── Checkpoint: "post-refactor"                      │     │
│     │    └── Continue...                                      │     │
│     └─────────────────────────────────────────────────────────┘     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 9. Context Window Optimization

**Advisor's Insight:** "By having those fault-tolerance mechanisms in place, can you improve the agent capability by making the context window usage more efficiently?"

### 9.1 The Context Window Problem

LLM agents face a fundamental constraint: **limited context window**. Current agents waste significant context on:

| Waste Category | Description | Example |
|----------------|-------------|---------|
| **Error Recovery** | Retrying failed operations with debugging info | "The previous INSERT failed because... let me try again..." |
| **State Tracking** | Manually tracking what changed and what can be rolled back | "I modified file X, Y, Z. If this fails, I need to revert..." |
| **Defensive Prompting** | Verbose safety checks and confirmations | "Before I delete, let me verify... Are you sure?" |
| **Undo Planning** | Figuring out how to reverse actions | "To undo the migration, I would need to..." |

### 9.2 Janus's Context Efficiency Benefits

```
┌─────────────────────────────────────────────────────────────────────┐
│              CONTEXT WINDOW USAGE: BEFORE vs AFTER Janus              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  WITHOUT Janus (Traditional Agent):                                   │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │████████████████████████████████████████████████████████████│    │
│  │ Error │ State  │ Defensive │ Undo   │ Actual │ Actual      │    │
│  │ Msgs  │ Track  │ Prompts   │ Plan   │ Work   │ Reasoning   │    │
│  │ (20%) │ (15%)  │ (15%)     │ (10%)  │ (25%)  │ (15%)       │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
│  WITH Janus (Transactional Agent):                                    │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │████████████████████████████████████████████████████████████│    │
│  │ Janus   │        Actual Work         │   Actual Reasoning    │    │
│  │ Calls │          (45%)             │       (50%)           │    │
│  │ (5%)  │                            │                       │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
│  Net Gain: ~40% more context for productive work                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 9.3 Abstraction Design for LLM Efficiency

#### 9.3.1 High-Level Transaction Tools

Instead of exposing low-level primitives, provide semantic abstractions:

```python
# LOW-LEVEL (context-heavy):
tools = [
    "tar_begin()",
    "tar_savepoint('before_migration')",
    "execute_sql('ALTER TABLE...')",
    "tar_savepoint('after_schema')",
    "execute_sql('INSERT INTO...')",
    "tar_rollback('before_migration')",  # Agent must decide
    "tar_commit()"
]

# HIGH-LEVEL (context-efficient):
tools = [
    "tar_try_operation(operation, on_failure='rollback')",
    "tar_atomic_batch(operations)",
    "tar_explore_alternatives([option1, option2, option3])",
    "tar_safe_commit(validation_checks)"
]
```

#### 9.3.2 Automatic Error Handling

```python
class JanusContextOptimizer:
    """
    Provides context-efficient abstractions that hide error handling from LLM.
    """
    
    async def try_operation(
        self,
        operation: ToolCall,
        on_failure: str = 'rollback',  # 'rollback' | 'retry' | 'ask'
        max_retries: int = 3
    ) -> OperationResult:
        """
        Execute operation with automatic error handling.
        The LLM doesn't see internal retries or rollbacks.
        """
        checkpoint = await self.checkpoint_manager.create_checkpoint('pre_op')
        
        for attempt in range(max_retries):
            try:
                result = await self.execute(operation)
                return OperationResult(success=True, result=result)
            
            except RecoverableError as e:
                if on_failure == 'rollback':
                    await self.checkpoint_manager.rollback_to_checkpoint('pre_op')
                    return OperationResult(
                        success=False,
                        error="Operation failed and was rolled back",
                        suggestion=self._suggest_alternative(operation, e)
                    )
                elif on_failure == 'retry':
                    await self.checkpoint_manager.rollback_to_checkpoint('pre_op')
                    continue  # Retry silently
        
        return OperationResult(success=False, error="Max retries exceeded")
```

#### 9.3.3 Batch Operations

```python
async def atomic_batch(self, operations: List[ToolCall]) -> BatchResult:
    """
    Execute multiple operations atomically.
    Either all succeed or all are rolled back.
    """
    subtxn = await self.txn.begin_subtransaction('batch')
    
    try:
        results = []
        for op in operations:
            result = await self.execute_in_subtxn(op, subtxn)
            results.append(result)
        
        await subtxn.commit()
        return BatchResult(success=True, results=results)
    
    except Exception as e:
        await subtxn.rollback()
        return BatchResult(
            success=False,
            error=f"Batch failed at operation {len(results)}, all changes rolled back"
        )
```

#### 9.3.4 Parallel Exploration

```python
async def explore_alternatives(
    self,
    alternatives: List[ToolCall],
    selection_criteria: str = 'first_success'  # | 'best_result' | 'user_choice'
) -> ExplorationResult:
    """
    Try multiple approaches in parallel branches.
    Returns the best result without polluting context with failed attempts.
    """
    # Create subtransactions for each alternative
    subtxns = [
        await self.txn.begin_subtransaction(f'alt_{i}')
        for i in range(len(alternatives))
    ]
    
    # Execute in parallel
    results = await asyncio.gather(*[
        self._try_alternative(alt, subtxn)
        for alt, subtxn in zip(alternatives, subtxns)
    ], return_exceptions=True)
    
    # Select best result
    successful = [
        (alt, result, subtxn)
        for alt, result, subtxn in zip(alternatives, results, subtxns)
        if not isinstance(result, Exception) and result.success
    ]
    
    if not successful:
        # All failed - rollback all
        for subtxn in subtxns:
            await subtxn.rollback()
        return ExplorationResult(success=False, error="All alternatives failed")
    
    # Commit winning alternative, rollback others
    if selection_criteria == 'first_success':
        winner_alt, winner_result, winner_subtxn = successful[0]
    elif selection_criteria == 'best_result':
        winner_alt, winner_result, winner_subtxn = max(
            successful, key=lambda x: x[1].quality_score
        )
    
    await winner_subtxn.commit()
    for alt, result, subtxn in successful:
        if subtxn != winner_subtxn:
            await subtxn.rollback()
    
    return ExplorationResult(
        success=True,
        chosen_alternative=winner_alt,
        result=winner_result
    )
```

### 9.4 Simplified Tool Interface for LLM

```python
# The LLM sees these clean, high-level tools:

Janus_TOOLS = [
    {
        "name": "tar_safe_execute",
        "description": "Execute an operation safely. If it fails, changes are automatically rolled back.",
        "parameters": {
            "operation": "The operation to execute",
            "critical": "If true, failure aborts the entire task"
        }
    },
    {
        "name": "tar_explore",
        "description": "Try multiple approaches and automatically select the best one. Failed approaches are discarded.",
        "parameters": {
            "approaches": "List of operations to try",
            "criteria": "How to select the winner: 'first_success' or 'best_result'"
        }
    },
    {
        "name": "tar_checkpoint",
        "description": "Save current progress. You can return here if later steps fail.",
        "parameters": {
            "name": "Name for this checkpoint"
        }
    },
    {
        "name": "tar_status",
        "description": "Get current transaction status including checkpoints and pending changes.",
        "parameters": {}
    }
]
```

### 9.5 Context Efficiency Metrics

| Metric | Without Janus | With Janus | Improvement |
|--------|-------------|----------|-------------|
| Tokens for error recovery | ~500/failure | ~50/failure | 90% reduction |
| Tokens for state tracking | ~200/action | 0 (automatic) | 100% reduction |
| Retry visibility | Full (all attempts) | None (hidden) | Cleaner context |
| Undo planning | Manual | Automatic | No context cost |
| **Net productive tokens** | ~60% | ~95% | **+58% relative** |

---

## 10. Core Research Questions

### 10.1 Shim Efficiency & Correctness

**Q1: What are the minimum capabilities a data system must have to support efficient bolt-on branching?**

**Hypothesis:**
- **Necessary:** Keyed access, atomic single-record writes, metadata storage
- **Sufficient:** Above + efficient secondary index on metadata

**Research Approach:**
1. Characterize data system capabilities taxonomically
2. Implement shims for systems at different capability levels
3. Measure overhead (latency, storage) vs. native branching (e.g., Git)
4. Derive minimum viable capability set

**Q2: How can we prove shim correctness with respect to isolation guarantees?**

**Approach:**
- Formal specification of Snapshot Isolation invariants
- Model checking of shim implementations
- Jepsen-style testing for concurrency bugs

### 10.2 Non-TMCP Tool Handling

**Q3: How do we handle stateful tool calls that don't implement TMCP?**

**Proposed Solutions:**

| Approach | Description | Trade-off |
|----------|-------------|-----------|
| **Log + Rollback** | No branching, just log + rollback | Simple, but no isolation |
| **Proxy Wrapping** | Intercept at API level | Works if API is interceptable |
| **State Mirroring** | Shadow state in Janus-controlled store | Overhead, consistency risk |
| **Quarantine Mode** | Execute non-TMCP calls only at commit | Limits interleaving |

### 10.3 Polystore Snapshot Consistency

**Q4: How do we enforce Snapshot Isolation across a polystore transaction?**

The challenge: Ensuring an agent sees a consistent point-in-time across Vector DB and SQL DB without global locking.

**Answer (Epoxy Model):** The Epoxy-style `beginTxn`/`endTxn` MVCC provides a **unified solution** to this problem. When the coordinator assigns a snapshot `(xmin, rc_txns)` at transaction begin, the *same* visibility predicate is applied across all data systems. No per-system snapshot coordination is needed — the predicate is stateless and can be evaluated independently by each shim.

```
┌─────────────────────────────────────────────────────────────────────┐
│       POLYSTORE SNAPSHOT PROTOCOL (EPOXY-STYLE)                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. BEGIN: Coordinator computes snapshot (xmin, rc_txns)            │
│     - xmin = min(active_txn_ids) at BEGIN time                     │
│     - rc_txns = {txns committed since xmin}                        │
│     - Snapshot is a pair of integers + a set — O(active_txns)      │
│                                                                     │
│  2. READ: Each shim translates snapshot into native filter          │
│     - SQL Shim: WHERE clause with beginTxn/endTxn predicates       │
│     - Vector Shim: metadata filter on similarity search            │
│     - Object Shim: coordinator metadata index lookup               │
│     - OverlayFS Shim: kernel handles via mount isolation           │
│     ALL shims use the SAME (xmin, rc_txns) → consistent view      │
│                                                                     │
│  3. COMMIT: Optimistic validation (write-write conflict check)      │
│     - No 2PC needed for metadata-based shims                       │
│     - Coordinator adds txn to commit log (O(1))                    │
│     - OverlayFS: overlay merge upper → base                        │
│                                                                     │
│  Key insight: consistency comes from the PREDICATE, not from        │
│  per-system coordination. Each shim independently evaluates the     │
│  same predicate on its own data → global consistency.               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 10.4 Garbage Collection of Record Versions

**Q6: How do we efficiently reclaim storage from old record versions?**

In the Epoxy MVCC model, records are never physically deleted during normal operation — old versions accumulate with `endTxn` set. Garbage collection (GC) must periodically reclaim these records.

**Challenges:**
- Old record versions are scattered across multiple data systems
- Live transactions may still need to see old versions (snapshot isolation)
- Some versions might be needed for auditing
- GC must not impact read/write performance

**GC Safety Rule:** A record version `r` can be garbage collected if and only if **no active or future transaction can ever see it**. Formally:

$$
\text{gc\_safe}(r) = 
\big(r.\text{endTxn} < \min(\text{active\_snapshots})\big)
\;\lor\;
\big(r.\text{beginTxn} \in \text{aborted\_txns}\big)
$$

- If `endTxn` is less than the oldest active snapshot, no transaction can ever see this version again.
- If `beginTxn` is an aborted transaction, the record was never logically created.

**Proposed GC Design:**

```python
class MVCCGarbageCollector:
    """
    Asynchronous garbage collection of old record versions 
    following the Epoxy model.
    
    Two categories of reclaimable records:
    1. SUPERSEDED: records with endTxn < oldest_active_snapshot
       (no active txn can see them — they've been replaced)
    2. ABORTED: records with beginTxn ∈ aborted_txns
       (they were created by an aborted txn — never logically existed)
    """
    
    async def gc_cycle(self) -> GCStats:
        # 1. Compute the GC watermark
        oldest_snapshot = self.coordinator.get_oldest_active_snapshot()
        aborted_txns = self.coordinator.get_aborted_txn_ids()
        
        stats = GCStats()
        
        # 2. For each shim, reclaim old versions
        for shim in self.shims:
            if isinstance(shim, OverlayFSShim):
                # OverlayFS: already cleaned up at abort/commit time
                # No ongoing GC needed (umount + rm -rf is immediate)
                continue
            
            # Delete superseded records: endTxn < oldest_snapshot
            superseded = await shim.delete_records(
                filter={"endTxn": {"$lt": oldest_snapshot}}
            )
            stats.superseded_records += superseded
            
            # Delete aborted records: beginTxn ∈ aborted_txns
            aborted = await shim.delete_records(
                filter={"beginTxn": {"$in": aborted_txns}}
            )
            stats.aborted_records += aborted
        
        # 3. Prune commit/abort logs
        self.coordinator.prune_logs(older_than=oldest_snapshot)
        
        return stats
    
    def gc_per_shim_type(self) -> dict:
        """
        Per-data-system GC operations:
        """
        return {
            "SQL": "DELETE FROM table WHERE _janus_end_txn < ? "
                   "OR _janus_begin_txn IN (?)",
            "Vector": "client.delete(filter={'$or': ["
                      "{'_janus_end_txn': {'$lt': watermark}}, "
                      "{'_janus_begin_txn': {'$in': aborted}}]})",
            "Object Store": "coordinator.metadata.delete_entries(...) "
                           "+ client.delete_object(VersionId=...)",
            "OverlayFS": "N/A (cleaned up at commit/abort time)",
        }
```

**GC Scheduling Strategies:**

| Strategy | Description | Trade-off |
|----------|-------------|-----------|
| **Periodic** | Run GC every N minutes | Simple, predictable load |
| **Threshold-Based** | GC when version count exceeds limit | Prevents unbounded growth |
| **Piggyback** | GC old versions during normal reads | Zero extra I/O, adds latency |
| **Low-Traffic** | Schedule GC during idle periods | Minimal impact on agents |

### 10.6 Side-Channel Access Detection

**Q7: How can the runtime detect or mitigate out-of-band writes?**

**Problem:** If a sysadmin connects directly to Postgres and modifies data, the shim can't intercept it.

**Proposed Mitigations:**

| Mitigation | Description | Trade-off |
|------------|-------------|-----------|
| **CDC Monitoring** | Use database CDC (Change Data Capture) to detect external changes | Requires DB support, latency |
| **Trigger-Based** | Install DB triggers to flag non-Janus modifications | Invasive, performance impact |
| **Periodic Reconciliation** | Compare expected state vs. actual state periodically | Detects but doesn't prevent |
| **Access Control** | Restrict direct DB access, all queries through Janus | Operationally challenging |
| **Optimistic Detection** | At commit time, verify no external changes since snapshot | Only detects at commit |

**Recommended Approach:** Layered defense
1. Access control where possible
2. CDC monitoring for audit
3. Optimistic detection at commit for safety

---

## 11. Demo Applications

### 11.1 Demo 1: Hotel Reservation System (Multi-Database)

**Complexity Level:** Cool → Great  
**Systems Involved:** 
- PostgreSQL (reservations, inventory)
- Redis (session cache, rate limiting)
- External Payment API

**Scenario:**
```
User: "Book a room at Marriott Boston for Feb 15-17, and charge my card ending 4242."

Agent Workflow:
1. BEGIN transaction
2. Check room availability (PostgreSQL)
3. Create provisional reservation (PostgreSQL)
4. Update inventory count (PostgreSQL)
5. Cache reservation in session (Redis)
6. Charge payment (External API - Stripe)
7. COMMIT if all succeed, ROLLBACK if any fail

Failure Scenarios to Demo:
- Payment declined → Rollback reservation + inventory
- Room became unavailable mid-transaction → Clean abort
- Network failure during commit → Coordinator-driven rollback
```

**Code Structure:**
```
demos/hotel-reservation/
├── src/
│   ├── agent/
│   │   └── booking_agent.py      # LLM agent logic
│   ├── shims/
│   │   ├── postgres_shim.py      # TMCP shim for PostgreSQL
│   │   ├── redis_shim.py         # TMCP shim for Redis
│   │   └── stripe_shim.py        # TMCP shim for Stripe
│   ├── models/
│   │   └── reservation.py        # Domain models
│   └── coordinator/
│       └── booking_coordinator.py # Transaction coordination
├── tests/
│   ├── test_happy_path.py
│   ├── test_payment_failure.py
│   ├── test_concurrent_booking.py
│   └── test_network_failure.py
└── docker-compose.yaml           # PostgreSQL, Redis setup
```

### 11.2 Demo 2: Coding Agent with Memory (File System + Vector Store)

**Complexity Level:** Great  
**Systems Involved:**
- Local File System (code files)
- Vector Store (Qdrant or Pinecone - agent memory/RAG)
- Git (optional - for real version control comparison)

**Scenario:**
```
User: "Refactor the authentication module to use JWT instead of sessions. 
       Remember this change for future context."

Agent Workflow:
1. BEGIN transaction
2. CHECKPOINT "pre-refactor"
3. Read current auth implementation (FS via OverlayFS merged dir)
4. Query past refactoring patterns from memory (Vector)
5. Generate refactored code
6. Write new auth files (FS - writes go to OverlayFS upperdir)
7. Run tests in branch environment (pytest runs on merged/ dir)
8. If tests fail:
   a. ROLLBACK TO "pre-refactor"
   b. Try alternative approach (new sibling subtxn overlay)
9. If tests pass:
   a. Store refactoring pattern in memory (Vector - to branch)
   b. CHECKPOINT "post-refactor"
   c. COMMIT (overlay merge upper → base via overlayfs-tools)

Key Features to Demo:
- OverlayFS overlay (agent sees modified files via merged/, OS base is untouched)
- Unix tools work unmodified (gcc, pytest, grep operate on merged/ directory)
- Vector memory branching (learning is isolated until commit)
- Checkpoint-based retry (failed refactor attempts don't pollute context)
- Context efficiency (LLM doesn't see failed attempts)
```

**Code Structure:**
```
demos/coding-agent/
├── src/
│   ├── agent/
│   │   ├── coding_agent.py       # Main agent logic
│   │   └── prompts/              # Agent prompts
│   ├── shims/
│   │   ├── overlayfs_shim.py     # OverlayFS TMCP shim (mount/merge/unmount)
│   │   └── vector_shim.py        # Qdrant TMCP shim
│   ├── memory/
│   │   ├── embedding.py          # Code embedding
│   │   └── retrieval.py          # Memory retrieval
│   └── testing/
│       └── sandbox.py            # Isolated test execution
├── tests/
│   ├── test_overlayfs_isolation.py
│   ├── test_memory_branching.py
│   ├── test_checkpoint_recovery.py
│   └── test_concurrent_agents.py
├── sample_project/               # Sample codebase for demo
│   └── auth/
│       └── session_auth.py
└── docker-compose.yaml           # Qdrant setup
```

### 11.3 Demo Comparison Matrix

| Feature | Hotel Reservation | Coding Agent |
|---------|-------------------|--------------|
| **Database TMCP** | PostgreSQL | - |
| **Cache TMCP** | Redis | - |
| **File System TMCP** | - | OverlayFS (kernel CoW) |
| **Vector Store TMCP** | - | Qdrant |
| **External API** | Stripe | - |
| **Checkpoints** | Basic | Advanced (multi-level) |
| **Conflict Handling** | Optimistic (inventory) | Merge (file conflicts) |
| **Context Optimization** | Error hiding | Exploration hiding |

---

## 12. Implementation Roadmap

### Phase 1: Foundation (Months 1-2)

**Goals:**
- Core transaction coordinator
- Basic TMCP protocol implementation
- Single shim (PostgreSQL)

**Deliverables:**
- [ ] Transaction coordinator with BEGIN/COMMIT/ROLLBACK
- [ ] PostgreSQL TMCP shim with MVCC-based branching
- [ ] Basic Snapshot Isolation implementation
- [ ] Unit test suite for coordinator and shim
- [ ] Design doc for TMCP protocol specification

### Phase 2: Multi-Shim Support (Months 3-4)

**Goals:**
- File system shim
- 2PC protocol implementation
- Checkpoint/savepoint support

**Deliverables:**
- [ ] File system overlay shim
- [ ] 2PC prepare/commit/abort protocol
- [ ] Savepoint creation and rollback
- [ ] Integration tests for multi-shim transactions
- [ ] Hotel reservation demo (v1)

### Phase 3: Vector Store & Advanced Features (Months 5-6)

**Goals:**
- Vector store shim
- Context optimization abstractions

**Deliverables:**
- [ ] Qdrant/Pinecone TMCP shim
- [ ] High-level `tar_safe_execute`, `tar_explore` APIs
- [ ] Coding agent demo (v1)
- [ ] Performance benchmarks

### Phase 4: Production Hardening (Months 7-8)

**Goals:**
- Garbage collection
- Monitoring and observability
- Recovery mechanisms

**Deliverables:**
- [ ] Branch garbage collector
- [ ] Prometheus metrics for transaction lifecycle
- [ ] Transaction recovery after coordinator crash
- [ ] Documentation and API reference
- [ ] Both demos at production quality

### Phase 5: Research Explorations (Ongoing)

**Goals:**
- Address core research questions
- Publish findings
- Community building

**Deliverables:**
- [ ] Paper on shim efficiency bounds
- [ ] Paper on polystore snapshot consistency
- [ ] Open-source release
- [ ] Conference presentations

---

---

*Document generated by Sea Labs AI Research Team*  
*Last updated: February 2026*
