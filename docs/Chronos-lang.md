# Chronos Implementation in LangChain / LangGraph

**Version:** 0.1.0 · February 2026

This document describes the implementation of Transactional Agent Runtime (Chronos) 
within the LangChain and LangGraph packages, covering the MVCC protocol, snapshot 
management, visibility rules, commit/validation protocol, tools, and implemented shims.

---

## 1. Overview

Chronos is implemented as two complementary packages:

| Package | Location | Purpose |
|---------|----------|---------|
| **langgraph.transaction** | `langgraph/libs/langgraph/langgraph/transaction/` | Core transactional runtime: coordinator, shims, MVCC types |
| **langchain-chronos** | `langchain/libs/partners/Chronos/langchain_chronos/` | LangChain tools that expose Chronos to LLM agents |

```
┌─────────────────────────────────────────────────────────────────┐
│                    LLM  Agent  (ReAct loop)                     │
│  tools: ChronosFileEditor · ChronosBash · ChronosSQLite · ChronosMemory        │
│         ChronosVectorStore                                          │
├──────────────────────┬──────────────────────────────────────────┤
│  ChronosContext          │  TransactionCoordinator                  │
│  (langchain_chronos)     │  (langgraph.transaction)                │
├──────────────────────┴──────────────────────────────────────────┤
│                        ToolShim  Interface                      │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐                  │
│  │ OverlayFS  │ │ SQLite     │ │ SqliteVec  │                  │
│  │ Shim       │ │ MVCC Shim  │ │ Shim       │                  │
│  └────────────┘ └────────────┘ └────────────┘                  │
├─────────────────────────────────────────────────────────────────┤
│  Real  Backends                                                 │
│  Linux OverlayFS  ·  SQLite  ·  sqlite-vec                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. MVCC Protocol

Chronos implements an **Epoxy-inspired MVCC** (Multi-Version Concurrency Control) protocol 
that provides snapshot isolation without data copying.

### 2.1 Record Versioning Schema

Every record is tagged with two transaction IDs:

| Column | Type | Description |
|--------|------|-------------|
| `_begin_txn` | INTEGER | The numeric txn ID that created this version |
| `_end_txn` | INTEGER | The numeric txn ID that superseded/deleted this version (0 = live) |

**Key operations:**
- **INSERT**: Set `_begin_txn = txn.numeric_id`, `_end_txn = 0`
- **UPDATE**: Set `_end_txn = txn.numeric_id` on old version, INSERT new version
- **DELETE**: Set `_end_txn = txn.numeric_id` on the visible version
- **READ**: Apply visibility predicate to filter visible records

### 2.2 Zero-Copy Branching

The MVCC protocol achieves **zero-copy branching**:
- No data is physically copied when a transaction begins
- Each transaction operates on a "virtual branch" defined by its snapshot
- Writes create new record versions tagged with the transaction's `numeric_id`
- Reads apply predicate filters to see exactly the right snapshot

### 2.3 Global Transaction Counter

```python
_txn_counter = itertools.count(1)

def _next_numeric_id() -> int:
    return next(_txn_counter)
```

Each transaction receives a globally unique monotonic `numeric_id` used for:
- MVCC version tagging
- Snapshot visibility computation
- Transaction ordering

---

## 3. Snapshot Management

### 3.1 TxnSnapshot Structure

Located in [langgraph/transaction/types.py](../langgraph/libs/langgraph/langgraph/transaction/types.py):

```python
@dataclass
class TxnSnapshot:
    xmin: int                        # Low-water mark: txns < xmin are committed
    committed_set: frozenset[int]    # Txns committed since xmin
    self_set: set[int]               # {own_id} ∪ {committed children's ids}
```

### 3.2 Snapshot Creation

When a transaction begins:

```python
txn.snapshot = TxnSnapshot(
    xmin=self._min_active_numeric_id or txn.numeric_id,
    committed_set=frozenset(self._committed_txn_ids),
    self_set={txn.numeric_id},
)
```

### 3.3 Subtransaction Snapshots (1-Level Nesting)

Child transactions derive their snapshot from the parent:

```python
def with_child(self, child_numeric_id: int) -> TxnSnapshot:
    child_self = self.self_set.copy()
    child_self.add(child_numeric_id)
    return TxnSnapshot(
        xmin=self.xmin,
        committed_set=self.committed_set,
        self_set=child_self,
    )
```

**Child lifecycle operations** (O(1)):
- `commit_child(child_id)`: Add child's `numeric_id` to parent's `self_set`
- `abort_child(child_id)`: Discard child's `numeric_id` from parent's `self_set`

---

## 4. Visibility Rules

### 4.1 Visibility Predicate

A record version with `(begin_txn, end_txn)` is visible to a transaction if:

```
visible(record, txn) = 
    (begin_txn ∈ committed_set ∨ begin_txn ∈ self_set ∨ begin_txn < xmin)
    ∧ (end_txn = 0 ∨ end_txn NOT ∈ committed_set ∧ end_txn NOT ∈ self_set ∧ end_txn ≥ xmin)
```

### 4.2 Python Implementation

```python
def is_visible(self, begin_txn: int, end_txn: int | None) -> bool:
    # begin_txn must be visible (committed before snapshot or self)
    begin_visible = (
        begin_txn < self.xmin
        or begin_txn in self.committed_set
        or begin_txn in self.self_set
    )
    if not begin_visible:
        return False

    # end_txn must NOT be visible (record not yet superseded)
    if end_txn is None or end_txn == 0:
        return True  # record is live
    end_visible = (
        end_txn < self.xmin
        or end_txn in self.committed_set
        or end_txn in self.self_set
    )
    return not end_visible
```

### 4.3 SQL WHERE Clause Generation

For efficient predicate push-down on SQLite:

```python
def visibility_sql(self) -> tuple[str, list[int]]:
    visible_ids = list(self.committed_set | self.self_set)
    placeholders = ",".join("?" * len(visible_ids))
    clause = (
        f"(_begin_txn < ? OR _begin_txn IN ({placeholders})) "
        f"AND (_end_txn IS NULL OR _end_txn = 0 OR "
        f"(_end_txn >= ? AND _end_txn NOT IN ({placeholders})))"
    )
    params = [self.xmin] + visible_ids + [self.xmin] + visible_ids
    return clause, params
```

---

## 5. Commit / Validation Protocol

### 5.1 Transaction States

```python
class TransactionState(enum.Enum):
    ACTIVE = "active"
    PREPARING = "preparing"
    COMMITTED = "committed"
    ABORTED = "aborted"
    COMMITTED_INTO_PARENT = "committed_into_parent"  # For subtransactions
```

### 5.2 Two-Phase Commit (2PC) Protocol

The `TransactionCoordinator` implements 2PC across all enrolled shims:

```
  ┌─────────────┐        ┌─────────────┐       ┌─────────────┐
  │ OverlayFS   │        │   SQLite    │       │ SqliteVec   │
  │    Shim     │        │    Shim     │       │    Shim     │
  └──────┬──────┘        └──────┬──────┘       └──────┬──────┘
         │                      │                     │
         │          PHASE 1: PREPARE                  │
         │◄───── prepare(txn) ──────────────────────►│
         │          Vote.COMMIT / Vote.ABORT          │
         │                      │                     │
         │          PHASE 2: COMMIT / ABORT           │
         │◄───── commit(txn) ───────────────────────►│
         │        or abort(txn)                       │
         │                      │                     │
```

### 5.3 Commit Implementation

From [coordinator.py](../langgraph/libs/langgraph/langgraph/transaction/coordinator.py):

```python
def commit(self, txn_id: str | None = None) -> list[ChangeRecord]:
    txn = self._resolve_txn(txn_id)
    
    # Check for active children (must be committed/aborted first)
    for child_id in txn.children:
        child = self._transactions.get(child_id)
        if child and child.is_active:
            raise ActiveChildError(...)

    # Phase 1: Prepare
    txn.state = TransactionState.PREPARING
    votes: dict[str, Vote] = {}
    abort_reasons: list[str] = []

    for shim_id in txn.participants:
        shim = self._shims[shim_id]
        try:
            vote = shim.prepare(txn)
            votes[shim_id] = vote
            if vote == Vote.ABORT:
                abort_reasons.append(f"{shim_id}: voted ABORT")
        except Exception as e:
            votes[shim_id] = Vote.ABORT
            abort_reasons.append(f"{shim_id}: {e}")

    # Phase 2: Commit or Abort
    if abort_reasons:
        for shim_id in txn.participants:
            self._shims[shim_id].abort(txn)
        txn.state = TransactionState.ABORTED
        raise CommitConflictError(...)

    # All voted COMMIT — apply
    all_changes: list[ChangeRecord] = []
    for shim_id in txn.participants:
        shim = self._shims[shim_id]
        all_changes.extend(shim.get_changes(txn))
        shim.commit(txn)

    txn.state = TransactionState.COMMITTED
    self._committed_txn_ids.add(txn.numeric_id)
    # Also mark all committed children as globally committed
    for nid in txn.snapshot.self_set:
        if nid != txn.numeric_id:
            self._committed_txn_ids.add(nid)
    
    return all_changes
```

### 5.4 Subtransaction Commit (O(1) for MVCC)

Child commit into parent is O(1) for MVCC shims:

```python
def commit_child(self, child_txn_id: str) -> None:
    child = self._resolve_txn(child_txn_id)
    parent = self._resolve_txn(child.parent_id)

    # Update parent's snapshot self_set (O(1))
    parent.snapshot.commit_child(child.numeric_id)

    # Merge write sets
    parent.write_set |= child.write_set

    # Notify shims
    for shim_id in child.participants:
        shim = self._shims[shim_id]
        if hasattr(shim, "commit_child"):
            shim.commit_child(child, parent)

    child.state = TransactionState.COMMITTED_INTO_PARENT
```

---

## 6. Implemented Shims

### 6.1 ToolShim Interface

All shims implement the abstract `ToolShim` interface:

```python
class ToolShim(ABC):
    @property
    @abstractmethod
    def shim_id(self) -> str: ...
    
    @abstractmethod
    def begin(self, txn: TransactionHandle) -> None: ...
    
    @abstractmethod
    def prepare(self, txn: TransactionHandle) -> Vote: ...
    
    @abstractmethod
    def commit(self, txn: TransactionHandle) -> None: ...
    
    @abstractmethod
    def abort(self, txn: TransactionHandle) -> None: ...
    
    @abstractmethod
    def savepoint(self, txn: TransactionHandle, sp: Savepoint) -> Any: ...
    
    @abstractmethod
    def rollback_to_savepoint(self, txn: TransactionHandle, sp: Savepoint) -> None: ...
    
    @abstractmethod
    def get_changes(self, txn: TransactionHandle) -> list[ChangeRecord]: ...
```

### 6.2 OverlayFSShim (Filesystem)

**Location:** [shim_fs.py](../langgraph/libs/langgraph/langgraph/transaction/shim_fs.py)

**Isolation Mechanism:** Linux kernel OverlayFS (copy-on-write)

| Operation | Implementation |
|-----------|----------------|
| **begin** | Mount overlay: `lowerdir=base`, `upperdir=branch`, `merged=working_dir` |
| **read** | Kernel transparently serves from `merged` (upper wins over lower) |
| **write** | Kernel automatically CoW's to `upperdir` |
| **delete** | Kernel creates whiteout file (`.wh.*`) in `upperdir` |
| **prepare** | Check for conflicts (files modified on base since txn start) |
| **commit** | Unmount, merge `upperdir` → base, process whiteouts |
| **abort** | Unmount, `rm -rf` the branch directory |
| **savepoint** | `tar cf` archive of `upperdir` |
| **rollback** | Unmount, restore `upperdir` from archive, remount |

**Subtransaction support:**
- Child mounts a layered overlay with parent's `merged` as `lowerdir`
- Child commit = merge child upper → parent upper + remount parent
- Child abort = umount child + rm -rf (parent unaffected)

### 6.3 SQLiteShim (Database)

**Location:** [shim_sqlite.py](../langgraph/libs/langgraph/langgraph/transaction/shim_sqlite.py)

**Isolation Mechanism:** MVCC with `_begin_txn` / `_end_txn` columns

| Operation | Implementation |
|-----------|----------------|
| **begin** | Initialize write set for transaction |
| **put (insert/update)** | Set `_end_txn` on old version, INSERT new with `_begin_txn=numeric_id` |
| **get** | SELECT with visibility predicate SQL |
| **query** | SELECT with visibility predicate + user filters, deduplicate by PK |
| **delete** | Set `_end_txn=numeric_id` on visible version |
| **prepare** | Vote COMMIT (optimistic; conflict detection at coordinator) |
| **commit** | No per-record work needed (coordinator records as committed) |
| **abort** | Eagerly delete created records, restore `_end_txn=0` on superseded |
| **savepoint** | Snapshot the write set |
| **rollback** | Restore write set, undo post-savepoint writes |

**Zero-copy commit:** Since visibility is predicate-based, commit is O(1) — 
the coordinator simply adds the `numeric_id` to `committed_set`, and future 
snapshots will see this transaction's writes.

**Subtransaction support (O(1)):**
- `begin_child`: Initialize child's write set
- `commit_child`: Merge child write set into parent (coordinator handles self_set)
- `abort_child`: Eagerly cleanup child's records

### 6.4 SqliteVecShim (Vector Store)

**Location:** [shim_vec.py](../langgraph/libs/langgraph/langgraph/transaction/shim_vec.py)

**Isolation Mechanism:** MVCC metadata on `_meta` table + sqlite-vec for ANN search

**Architecture:**
```
Collection "docs":
┌─────────────────────────────┐   ┌─────────────────────────────────────┐
│  docs_vec (vec0 virtual)    │   │  docs_meta (MVCC metadata + payload)│
│  - id TEXT PRIMARY KEY      │   │  - id TEXT                          │
│  - embedding float[N]       │   │  - payload TEXT (JSON)              │
│                             │   │  - embedding BLOB                   │
│                             │   │  - _begin_txn INTEGER               │
│                             │   │  - _end_txn INTEGER                 │
└─────────────────────────────┘   └─────────────────────────────────────┘
```

| Operation | Implementation |
|-----------|----------------|
| **upsert** | Insert into both `_vec` and `_meta` with MVCC tagging |
| **search** | ANN search on `_vec`, filter results via visibility on `_meta` |
| **get** | Query `_meta` with visibility predicate |
| **delete** | Set `_end_txn` on `_meta` (vec cleanup on GC) |
| **commit** | O(1) — coordinator handles via `committed_set` |
| **abort** | Eagerly cleanup dead records in `_meta` |

**Fallback:** If `sqlite-vec` extension is unavailable, the shim falls back to 
brute-force cosine similarity on the stored embeddings.

---

## 7. LangChain Tools

### 7.1 ChronosContext (Orchestrator)

**Location:** [langchain_chronos/context.py](../langchain/libs/partners/Chronos/langchain_chronos/context.py)

`ChronosContext` is the main entry point that orchestrates the transactional lifecycle:

```python
class ChronosContext:
    def __init__(
        self,
        base_path: str | Path,
        coordinator: TransactionCoordinator | None = None,
        db_path: str = ":memory:",
        enable_sqlite: bool = True,
        enable_vectorstore: bool = True,
        vector_dimensions: int = 384,
    ): ...
    
    def begin(self) -> TransactionHandle: ...
    def commit(self) -> None: ...
    def abort(self) -> None: ...
    def savepoint(self, name: str) -> None: ...
    def rollback(self, name: str | None = None) -> None: ...
    def get_tools(self) -> list[BaseTool]: ...
    def get_changes(self) -> list[ChangeRecord]: ...
```

**Usage:**
```python
from langchain_chronos import ChronosContext

ctx = ChronosContext("/path/to/project")
ctx.begin()
tools = ctx.get_tools()  # [ChronosFileEditor, ChronosMemory, ChronosBash, ChronosSQLite?, ChronosVectorStore?]

# ... agent uses tools ...

ctx.commit()  # 2PC across all backends
```

### 7.2 ChronosFileEditor

**Location:** [langchain_chronos/file_editor.py](../langchain/libs/partners/Chronos/langchain_chronos/file_editor.py)

Transactional file operations on the OverlayFS merged directory.

| Command | Description |
|---------|-------------|
| `view` | Read a file (with optional line range) or list a directory |
| `create` | Create or overwrite a file with given content |
| `str_replace` | Replace a string occurrence in a file |
| `insert` | Insert text after a specified line number |
| `delete` | Delete a file or directory |

All changes are isolated in the overlay's `upperdir` until the transaction commits.

### 7.3 ChronosBash

**Location:** [langchain_chronos/bash.py](../langchain/libs/partners/Chronos/langchain_chronos/bash.py)

Execute shell commands inside the transactional overlay filesystem.

```python
bash = ChronosBash(working_dir=overlay_merged_path)
result = bash.invoke({"command": "python hello.py"})
result = bash.invoke({"command": "gcc -o prog main.c && ./prog"})
result = bash.invoke({"command": "pytest tests/"})
```

**Features:**
- Commands run with `cwd` set to overlay's merged directory
- All file-system side effects captured in the overlay
- Unix tools (gcc, python, pytest, git, etc.) work unmodified
- Output truncated at 100KB, timeout configurable

### 7.4 ChronosMemory

**Location:** [langchain_chronos/memory.py](../langchain/libs/partners/Chronos/langchain_chronos/memory.py)

Persistent agent scratchpad backed by files in `memories/` directory.

| Command | Description |
|---------|-------------|
| `save` | Save text to a named memory file |
| `load` | Load content from a memory file |
| `list` | List all saved memories |
| `delete` | Delete a memory file |

Memory files live in the overlay and are committed/rolled back with the transaction.

### 7.5 ChronosSQLite

**Location:** [langchain_chronos/sqlite_tool.py](../langchain/libs/partners/Chronos/langchain_chronos/sqlite_tool.py)

Transactional MVCC SQLite operations.

| Command | Description |
|---------|-------------|
| `create_table` | Define a new table with columns |
| `put` | Insert or update a row by primary key |
| `get` | Retrieve a row by primary key |
| `query` | Query rows with filters |
| `delete` | Delete a row by primary key |
| `seed` | Seed initial data (committed immediately) |

All reads apply the visibility predicate; writes are isolated per-transaction.

### 7.6 ChronosVectorStore

**Location:** [langchain_chronos/vectorstore_tool.py](../langchain/libs/partners/Chronos/langchain_chronos/vectorstore_tool.py)

Transactional MVCC vector storage for RAG and long-term memory.

| Command | Description |
|---------|-------------|
| `register_collection` | Define a new vector collection |
| `add_texts` | Add text documents with embeddings |
| `add_documents` | Add Document objects with embeddings |
| `similarity_search` | Find similar documents by query embedding |
| `get` | Retrieve a document by ID |
| `delete` | Delete a document by ID |
| `list_collections` | List all registered collections |

---

## 8. Data Flow Example

### 8.1 Agent Session with Savepoint and Rollback

```
Agent                    ChronosContext        Coordinator         OverlayFS     SQLite
  |                          |                  |                  |            |
  |-- begin() ------------->|                  |                  |            |
  |                          |-- begin() ----->|                  |            |
  |                          |                  |-- begin() ----->| mount      |
  |                          |                  |-- begin() ----->|----->|     |
  |                          |                  |                  |      |     |
  |-- write file ---------->|                  |                  |      |     |
  |                          |      (writes to merged → CoW to upper)     |     |
  |                          |                  |                  |      |     |
  |-- put DB row ---------->|                  |                  |      |     |
  |                          |       (INSERT with _begin_txn=N)    |      |--->|
  |                          |                  |                  |      |     |
  |-- savepoint("cp1") ---->|                  |                  |      |     |
  |                          |-- begin_child ->| child.numeric_id |      |     |
  |                          |    (tools switch to child overlay)  |      |     |
  |                          |                  |                  |      |     |
  |-- speculative writes ---|---------------------------------------->|--->|
  |                          |                  |                  |      |     |
  |-- rollback("cp1") ----->|                  |                  |      |     |
  |                          |-- abort_child ->| (child discarded) |      |     |
  |                          |    (tools switch back to parent)    |      |     |
  |                          |                  |                  |      |     |
  |-- commit() ------------>|                  |                  |      |     |
  |                          |-- commit() ---->|                  |      |     |
  |                          |                  |-- prepare() --->| Vote |     |
  |                          |                  |-- prepare() --->|----->| Vote|
  |                          |                  |-- commit() ---->| merge|     |
  |                          |                  |-- commit() ---->|----->| (1) |
  |                          |                  |                  |      |     |
  
(1) For MVCC shims, commit is O(1) — numeric_id added to committed_set
```

---

## 9. Key Design Principles

1. **Bolt-on, not baked-in.** Chronos wraps unmodified backends via shims; 
   no backend code changes required.

2. **Zero-copy branching.** MVCC predicate filtering achieves snapshot isolation 
   without copying data. OverlayFS provides kernel-level CoW.

3. **Same tool semantics.** `ChronosFileEditor` behaves identically to a plain 
   file editor — agents don't need new skills, only new affordances.

4. **All-or-nothing across backends.** 2PC ensures a single commit is atomic 
   over files, databases, and vectors.

5. **O(1) subtransactions.** Child commit/abort operates only on snapshot 
   metadata, not on the underlying records.

6. **Safe speculation.** Savepoints enable "try → observe → keep or undo" 
   loops that make agents reliable.

---

## 10. File Reference

| Path | Description |
|------|-------------|
| [langgraph/transaction/types.py](../langgraph/libs/langgraph/langgraph/transaction/types.py) | Core types: `TransactionHandle`, `TxnSnapshot`, `Savepoint`, etc. |
| [langgraph/transaction/coordinator.py](../langgraph/libs/langgraph/langgraph/transaction/coordinator.py) | `TransactionCoordinator` implementing 2PC |
| [langgraph/transaction/shim.py](../langgraph/libs/langgraph/langgraph/transaction/shim.py) | Abstract `ToolShim` interface |
| [langgraph/transaction/shim_fs.py](../langgraph/libs/langgraph/langgraph/transaction/shim_fs.py) | `OverlayFSShim` for filesystem isolation |
| [langgraph/transaction/shim_sqlite.py](../langgraph/libs/langgraph/langgraph/transaction/shim_sqlite.py) | `SQLiteShim` with MVCC |
| [langgraph/transaction/shim_vec.py](../langgraph/libs/langgraph/langgraph/transaction/shim_vec.py) | `SqliteVecShim` for vector store |
| [langchain_chronos/context.py](../langchain/libs/partners/Chronos/langchain_chronos/context.py) | `ChronosContext` orchestrator |
| [langchain_chronos/file_editor.py](../langchain/libs/partners/Chronos/langchain_chronos/file_editor.py) | `ChronosFileEditor` tool |
| [langchain_chronos/bash.py](../langchain/libs/partners/Chronos/langchain_chronos/bash.py) | `ChronosBash` tool |
| [langchain_chronos/memory.py](../langchain/libs/partners/Chronos/langchain_chronos/memory.py) | `ChronosMemory` tool |
| [langchain_chronos/sqlite_tool.py](../langchain/libs/partners/Chronos/langchain_chronos/sqlite_tool.py) | `ChronosSQLite` tool |
| [langchain_chronos/vectorstore_tool.py](../langchain/libs/partners/Chronos/langchain_chronos/vectorstore_tool.py) | `ChronosVectorStore` tool |

---

*Document created for Chronos LangChain/LangGraph implementation reference*
