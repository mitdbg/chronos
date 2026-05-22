# Chronos Academic Prototype Implementation Plan

## Overview

This document outlines a pragmatic implementation plan for building a **Transactional Agent Runtime (Chronos)** prototype suitable for academic research and publication. The goal is to demonstrate the key research contributions while maintaining a manageable scope.

---

## 1. Implementation Philosophy for Academic Prototypes

### 1.1 Guiding Principles

| Principle | Description |
|-----------|-------------|
| **Depth over Breadth** | Implement 1-2 shims deeply rather than many shims shallowly |
| **Research-First** | Prioritize components that answer research questions |
| **Measurable** | Every component should produce metrics for evaluation |
| **Reproducible** | Use containerization, fixed seeds, recorded traces |
| **Minimal Dependencies** | Avoid complex infrastructure where possible |

### 1.2 Build vs. Mock vs. Skip Decision Matrix

| Component | Decision | Rationale |
|-----------|----------|-----------|
| **Transaction Coordinator** | BUILD (full) | Core contribution |
| **SQLite/DuckDB Shim** | BUILD (full) | Simplest SQL, good for demos |
| **PostgreSQL Shim** | BUILD (basic) | More realistic, production-like |
| **File System Shim** | BUILD (full) | Critical for coding agent demo |
| **Vector Store Shim** | BUILD (basic) | RAG is hot topic, minimal effort with Qdrant |
| **Redis Shim** | SKIP | Not essential for research contribution |
| **External API Shim** | MOCK | Use compensation stubs, not real Stripe |
| **LLM Agent Integration** | BUILD (adapter) | Wrap existing agent framework |
| **Distributed Coordinator** | SKIP | Single-node is sufficient for prototype |
| **Production GC** | MOCK | Simple eager cleanup is fine |
| **Security/Auth** | SKIP | Not a research contribution |

---

## 2. Technology Stack

### 2.1 Core Stack

```
┌─────────────────────────────────────────────────────────────┐
│                     TECHNOLOGY CHOICES                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Language:        Python 3.11+                              │
│                   (Fast iteration, LLM ecosystem)           │
│                                                             │
│  Async Runtime:   asyncio + uvloop                          │
│                   (Agent workflows are I/O bound)           │
│                                                             │
│  SQL Database:    SQLite (dev) / DuckDB (analytics)         │
│                   PostgreSQL (production-like demo)         │
│                                                             │
│  Vector Store:    Qdrant (local Docker, simple API)         │
│                   or ChromaDB (in-process, simpler)         │
│                                                             │
│  File Overlay:    Custom (in-memory dict + temp dirs)       │
│                   or OverlayFS (Linux only, more realistic) │
│                                                             │
│  LLM Framework:   LangChain or custom ReAct loop            │
│                   (Adapter pattern for portability)         │
│                                                             │
│  Serialization:   Pydantic models + JSON                    │
│                                                             │
│  Testing:         pytest + pytest-asyncio + hypothesis      │
│                                                             │
│  Containers:      Docker Compose (Postgres, Qdrant)         │
│                                                             │
│  Metrics:         Prometheus client + custom CSV export     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Why These Choices?

| Choice | Rationale |
|--------|-----------|
| **Python** | Fastest iteration, best LLM library ecosystem, async support |
| **SQLite/DuckDB** | Zero infrastructure, in-process, perfect for unit tests |
| **Qdrant** | Simple REST API, Docker image, good for research |
| **Custom FS Overlay** | Avoid Linux-only OverlayFS, simpler to debug |
| **Pydantic** | Type-safe serialization, good for protocol definitions |

---

## 3. Project Structure

```
chronos/
├── README.md
├── pyproject.toml                 # Poetry/uv for dependency management
├── docker-compose.yaml            # Postgres, Qdrant for integration tests
│
├── src/
│   └── chronos/
│       ├── __init__.py
│       │
│       ├── core/                  # Core abstractions (no dependencies)
│       │   ├── __init__.py
│       │   ├── types.py           # TransactionId, BranchId, Savepoint, etc.
│       │   ├── interfaces.py      # TMCPShim protocol, Coordinator protocol
│       │   ├── transaction.py     # Transaction, Subtransaction classes
│       │   └── exceptions.py      # Chronos-specific exceptions
│       │
│       ├── coordinator/           # Transaction coordination
│       │   ├── __init__.py
│       │   ├── coordinator.py     # TransactionCoordinator implementation
│       │   ├── branch_manager.py  # Branch creation, merging
│       │   ├── checkpoint.py      # CheckpointManager
│       │   ├── saga.py            # SagaCoordinator for compensation
│       │   └── conflict.py        # Conflict detection and resolution
│       │
│       ├── shims/                 # Per-backend TMCP implementations
│       │   ├── __init__.py
│       │   ├── base.py            # BaseTMCPShim abstract class
│       │   ├── sqlite_shim.py     # SQLite with MVCC overlay
│       │   ├── postgres_shim.py   # PostgreSQL shim
│       │   ├── filesystem_shim.py # File system overlay
│       │   ├── vector_shim.py     # Qdrant/Chroma shim
│       │   └── mock_api_shim.py   # Mock external API with compensation
│       │
│       ├── agent/                 # Agent integration layer
│       │   ├── __init__.py
│       │   ├── tools.py           # Chronos tools exposed to LLM
│       │   ├── context_optimizer.py # High-level abstractions
│       │   └── adapters/
│       │       ├── langchain.py   # LangChain adapter
│       │       └── react.py       # Simple ReAct loop
│       │
│       ├── protocol/              # TMCP protocol definitions
│       │   ├── __init__.py
│       │   ├── messages.py        # Protocol message types
│       │   └── serialization.py   # Wire format (JSON)
│       │
│       └── metrics/               # Observability
│           ├── __init__.py
│           ├── collector.py       # Metrics collection
│           └── export.py          # CSV/Prometheus export
│
├── tests/
│   ├── unit/
│   │   ├── test_transaction.py
│   │   ├── test_coordinator.py
│   │   ├── test_checkpoint.py
│   │   └── test_shims/
│   │       ├── test_sqlite_shim.py
│   │       ├── test_filesystem_shim.py
│   │       └── test_vector_shim.py
│   │
│   ├── integration/
│   │   ├── test_multi_shim.py     # Cross-shim transactions
│   │   ├── test_conflict.py       # Conflict scenarios
│   │   └── test_recovery.py       # Rollback scenarios
│   │
│   ├── benchmarks/
│   │   ├── bench_overhead.py      # Measure Chronos overhead vs baseline
│   │   ├── bench_branching.py     # Branch creation/merge performance
│   │   └── bench_context.py       # Context window usage measurement
│   │
│   └── demos/
│       ├── hotel_reservation/     # Demo 1: Multi-database booking
│       │   ├── agent.py
│       │   ├── setup.py
│       │   └── scenarios.py
│       │
│       └── coding_agent/          # Demo 2: Code refactoring
│           ├── agent.py
│           ├── sample_project/
│           └── scenarios.py
│
├── experiments/                   # Research experiments
│   ├── exp1_overhead/             # RQ1: What's the runtime overhead?
│   ├── exp2_context/              # RQ2: Context window efficiency
│   ├── exp3_isolation/            # RQ3: Isolation correctness
│   └── exp4_recovery/             # RQ4: Recovery success rate
│
├── scripts/
│   ├── setup_dev.sh               # Development environment setup
│   ├── run_experiments.py         # Run all experiments
│   └── generate_plots.py          # Generate paper figures
│
└── docs/
    ├── Chronos-design.md              # Design document
    ├── Chronos-implementation-plan.md # This document
    └── api/                       # Generated API docs
```

---

## 4. Core Components Implementation

### 4.1 Transaction and Branch Model

```python
# src/chronos/core/types.py

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Set, Dict, Any
import uuid
import time

class TransactionState(Enum):
    ACTIVE = "active"
    PREPARING = "preparing"
    COMMITTED = "committed"
    ABORTED = "aborted"

@dataclass
class BranchId:
    """Unique identifier for a virtual branch."""
    value: str
    parent: Optional["BranchId"] = None
    
    @classmethod
    def main(cls) -> "BranchId":
        return cls(value="main", parent=None)
    
    @classmethod
    def create(cls, parent: "BranchId") -> "BranchId":
        return cls(value=f"branch-{uuid.uuid4().hex[:8]}", parent=parent)
    
    def is_main(self) -> bool:
        return self.value == "main"

@dataclass
class Savepoint:
    """A checkpoint within a transaction."""
    name: str
    branch_id: BranchId
    timestamp: float
    write_set_snapshot: Set[tuple]
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Transaction:
    """A Chronos transaction with virtual branching."""
    id: str
    branch_id: BranchId
    snapshot_ts: float  # Snapshot timestamp for SI
    state: TransactionState = TransactionState.ACTIVE
    write_set: Set[tuple] = field(default_factory=set)
    read_set: Set[tuple] = field(default_factory=set)
    savepoints: Dict[str, Savepoint] = field(default_factory=dict)
    participants: Set[str] = field(default_factory=set)  # Shim IDs
    parent: Optional["Transaction"] = None  # For subtransactions
    
    @classmethod
    def begin(cls, parent_branch: Optional[BranchId] = None) -> "Transaction":
        branch = BranchId.create(parent_branch or BranchId.main())
        return cls(
            id=f"txn-{uuid.uuid4().hex[:12]}",
            branch_id=branch,
            snapshot_ts=time.time()
        )
    
    def create_savepoint(self, name: str) -> Savepoint:
        sp = Savepoint(
            name=name,
            branch_id=self.branch_id,
            timestamp=time.time(),
            write_set_snapshot=self.write_set.copy()
        )
        self.savepoints[name] = sp
        return sp
    
    def begin_subtransaction(self, name: str) -> "Transaction":
        return Transaction(
            id=f"{self.id}:sub-{uuid.uuid4().hex[:8]}",
            branch_id=BranchId.create(self.branch_id),
            snapshot_ts=time.time(),
            parent=self
        )
```

### 4.2 TMCP Shim Interface

```python
# src/chronos/core/interfaces.py

from abc import ABC, abstractmethod
from typing import Any, Optional, List
from enum import Enum

from .types import Transaction, Savepoint, BranchId

class Vote(Enum):
    COMMIT = "commit"
    ABORT = "abort"

class TMCPShim(ABC):
    """
    Abstract interface for a TMCP-compliant shim.
    Each backend (SQL, FS, Vector) implements this.
    """
    
    @property
    @abstractmethod
    def shim_id(self) -> str:
        """Unique identifier for this shim instance."""
        pass
    
    # === Transaction Lifecycle ===
    
    @abstractmethod
    async def begin(self, txn: Transaction) -> None:
        """Initialize shim state for a new transaction."""
        pass
    
    @abstractmethod
    async def prepare(self, txn: Transaction) -> Vote:
        """
        Phase 1 of 2PC: Validate and prepare to commit.
        Returns COMMIT if ready, ABORT if cannot commit.
        """
        pass
    
    @abstractmethod
    async def commit(self, txn: Transaction) -> None:
        """Phase 2 of 2PC: Commit changes to main branch."""
        pass
    
    @abstractmethod
    async def abort(self, txn: Transaction) -> None:
        """Abort transaction and discard branch changes."""
        pass
    
    # === Savepoints ===
    
    @abstractmethod
    async def savepoint(self, txn: Transaction, sp: Savepoint) -> None:
        """Record a savepoint for this shim's state."""
        pass
    
    @abstractmethod
    async def rollback_to_savepoint(self, txn: Transaction, sp: Savepoint) -> None:
        """Rollback shim state to a savepoint."""
        pass
    
    # === Data Operations (shim-specific) ===
    # These are defined per-shim type, not in the abstract interface


class CoordinatorProtocol(ABC):
    """Protocol for the transaction coordinator."""
    
    @abstractmethod
    async def begin_transaction(self) -> Transaction:
        pass
    
    @abstractmethod
    async def commit(self, txn: Transaction) -> bool:
        pass
    
    @abstractmethod
    async def rollback(self, txn: Transaction, to_savepoint: Optional[str] = None) -> None:
        pass
    
    @abstractmethod
    async def savepoint(self, txn: Transaction, name: str) -> Savepoint:
        pass
    
    @abstractmethod
    def register_shim(self, shim: TMCPShim) -> None:
        pass
```

### 4.3 Transaction Coordinator

```python
# src/chronos/coordinator/coordinator.py

import asyncio
from typing import Dict, Optional, List
from dataclasses import dataclass
import logging

from chronos.core.types import Transaction, TransactionState, Savepoint, BranchId
from chronos.core.interfaces import TMCPShim, Vote, CoordinatorProtocol
from chronos.core.exceptions import (
    TransactionAbortedException,
    CommitFailedException,
    SavepointNotFoundException
)

logger = logging.getLogger(__name__)

@dataclass
class TransactionRecord:
    """Coordinator's record of a transaction."""
    txn: Transaction
    enrolled_shims: List[str]  # Shim IDs that have been enrolled

class TransactionCoordinator(CoordinatorProtocol):
    """
    Central coordinator for Chronos transactions.
    Implements 2PC-like protocol for multi-shim commits.
    """
    
    def __init__(self):
        self._shims: Dict[str, TMCPShim] = {}
        self._transactions: Dict[str, TransactionRecord] = {}
    
    def register_shim(self, shim: TMCPShim) -> None:
        """Register a shim with the coordinator."""
        self._shims[shim.shim_id] = shim
        logger.info(f"Registered shim: {shim.shim_id}")
    
    async def begin_transaction(self, parent: Optional[Transaction] = None) -> Transaction:
        """Start a new transaction."""
        parent_branch = parent.branch_id if parent else None
        txn = Transaction.begin(parent_branch)
        txn.parent = parent
        
        self._transactions[txn.id] = TransactionRecord(
            txn=txn,
            enrolled_shims=[]
        )
        
        logger.info(f"Started transaction {txn.id} on branch {txn.branch_id.value}")
        return txn
    
    async def enroll_shim(self, txn: Transaction, shim_id: str) -> None:
        """Enroll a shim in a transaction (lazy enrollment)."""
        if shim_id not in self._shims:
            raise ValueError(f"Unknown shim: {shim_id}")
        
        record = self._transactions.get(txn.id)
        if not record:
            raise ValueError(f"Unknown transaction: {txn.id}")
        
        if shim_id not in record.enrolled_shims:
            shim = self._shims[shim_id]
            await shim.begin(txn)
            record.enrolled_shims.append(shim_id)
            txn.participants.add(shim_id)
            logger.debug(f"Enrolled {shim_id} in {txn.id}")
    
    async def savepoint(self, txn: Transaction, name: str) -> Savepoint:
        """Create a savepoint across all enrolled shims."""
        sp = txn.create_savepoint(name)
        
        record = self._transactions.get(txn.id)
        if record:
            for shim_id in record.enrolled_shims:
                shim = self._shims[shim_id]
                await shim.savepoint(txn, sp)
        
        logger.info(f"Created savepoint '{name}' in {txn.id}")
        return sp
    
    async def rollback(
        self, 
        txn: Transaction, 
        to_savepoint: Optional[str] = None
    ) -> None:
        """
        Rollback a transaction.
        If to_savepoint is provided, rollback to that savepoint.
        Otherwise, abort the entire transaction.
        """
        record = self._transactions.get(txn.id)
        if not record:
            raise ValueError(f"Unknown transaction: {txn.id}")
        
        if to_savepoint:
            # Rollback to savepoint
            if to_savepoint not in txn.savepoints:
                raise SavepointNotFoundException(to_savepoint)
            
            sp = txn.savepoints[to_savepoint]
            
            for shim_id in record.enrolled_shims:
                shim = self._shims[shim_id]
                await shim.rollback_to_savepoint(txn, sp)
            
            # Restore write set
            txn.write_set = sp.write_set_snapshot.copy()
            
            # Remove savepoints after this one
            to_remove = [
                name for name, s in txn.savepoints.items()
                if s.timestamp > sp.timestamp
            ]
            for name in to_remove:
                del txn.savepoints[name]
            
            logger.info(f"Rolled back {txn.id} to savepoint '{to_savepoint}'")
        else:
            # Full abort
            txn.state = TransactionState.ABORTED
            
            for shim_id in record.enrolled_shims:
                shim = self._shims[shim_id]
                try:
                    await shim.abort(txn)
                except Exception as e:
                    logger.error(f"Error aborting {shim_id}: {e}")
            
            del self._transactions[txn.id]
            logger.info(f"Aborted transaction {txn.id}")
    
    async def commit(self, txn: Transaction) -> bool:
        """
        Commit a transaction using 2PC protocol.
        Returns True if successful, raises exception otherwise.
        """
        record = self._transactions.get(txn.id)
        if not record:
            raise ValueError(f"Unknown transaction: {txn.id}")
        
        if txn.state != TransactionState.ACTIVE:
            raise ValueError(f"Transaction {txn.id} is not active: {txn.state}")
        
        txn.state = TransactionState.PREPARING
        
        # Phase 1: Prepare
        logger.info(f"Phase 1 (PREPARE) for {txn.id}")
        votes: Dict[str, Vote] = {}
        
        async def prepare_shim(shim_id: str) -> tuple:
            shim = self._shims[shim_id]
            try:
                vote = await shim.prepare(txn)
                return (shim_id, vote)
            except Exception as e:
                logger.error(f"Prepare failed for {shim_id}: {e}")
                return (shim_id, Vote.ABORT)
        
        results = await asyncio.gather(*[
            prepare_shim(sid) for sid in record.enrolled_shims
        ])
        
        votes = dict(results)
        
        # Check if all voted COMMIT
        all_commit = all(v == Vote.COMMIT for v in votes.values())
        
        if not all_commit:
            # Phase 2: Abort
            logger.info(f"Phase 2 (ABORT) for {txn.id} - votes: {votes}")
            txn.state = TransactionState.ABORTED
            
            for shim_id in record.enrolled_shims:
                shim = self._shims[shim_id]
                try:
                    await shim.abort(txn)
                except Exception as e:
                    logger.error(f"Error aborting {shim_id}: {e}")
            
            del self._transactions[txn.id]
            raise TransactionAbortedException(
                f"Transaction {txn.id} aborted. Votes: {votes}"
            )
        
        # Phase 2: Commit
        logger.info(f"Phase 2 (COMMIT) for {txn.id}")
        
        commit_errors = []
        for shim_id in record.enrolled_shims:
            shim = self._shims[shim_id]
            try:
                await shim.commit(txn)
            except Exception as e:
                logger.error(f"Commit failed for {shim_id}: {e}")
                commit_errors.append((shim_id, e))
        
        if commit_errors:
            # This is a critical failure - some shims committed, others didn't
            # In a real system, we'd need recovery. For prototype, log and raise.
            logger.critical(f"Partial commit failure for {txn.id}: {commit_errors}")
            raise CommitFailedException(
                f"Partial commit failure: {commit_errors}"
            )
        
        txn.state = TransactionState.COMMITTED
        del self._transactions[txn.id]
        logger.info(f"Committed transaction {txn.id}")
        
        return True
```

### 4.4 SQLite Shim (Simplest Implementation)

```python
# src/chronos/shims/sqlite_shim.py

import sqlite3
import json
from typing import Any, Optional, Dict, List
from dataclasses import dataclass
import time
import asyncio

from chronos.core.interfaces import TMCPShim, Vote
from chronos.core.types import Transaction, Savepoint, BranchId

@dataclass
class VersionedRow:
    """A row with MVCC metadata."""
    key: str
    value: Any
    version: int
    branch_id: str
    timestamp: float
    txn_id: str
    is_tombstone: bool = False

class SQLiteShim(TMCPShim):
    """
    TMCP shim for SQLite with MVCC-based virtual branching.
    Uses a single table with branch/version columns for simplicity.
    """
    
    def __init__(self, db_path: str = ":memory:"):
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._setup_schema()
        self._branch_state: Dict[str, Dict] = {}  # Per-transaction state
    
    @property
    def shim_id(self) -> str:
        return f"sqlite:{self._db_path}"
    
    def _setup_schema(self):
        """Create the versioned data table."""
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS tar_data (
                key TEXT NOT NULL,
                value TEXT,
                version INTEGER NOT NULL,
                branch_id TEXT NOT NULL,
                timestamp REAL NOT NULL,
                txn_id TEXT NOT NULL,
                is_tombstone INTEGER DEFAULT 0,
                PRIMARY KEY (key, branch_id, version)
            );
            
            CREATE INDEX IF NOT EXISTS idx_chronos_branch 
            ON tar_data (branch_id, timestamp);
            
            CREATE INDEX IF NOT EXISTS idx_chronos_key 
            ON tar_data (key, branch_id);
        """)
        self._conn.commit()
    
    # === Transaction Lifecycle ===
    
    async def begin(self, txn: Transaction) -> None:
        """Initialize state for a new transaction."""
        self._branch_state[txn.id] = {
            "branch_id": txn.branch_id.value,
            "snapshot_ts": txn.snapshot_ts,
            "write_set": set()
        }
    
    async def prepare(self, txn: Transaction) -> Vote:
        """
        Validate that we can commit.
        Check for write-write conflicts with main branch.
        """
        state = self._branch_state.get(txn.id)
        if not state:
            return Vote.ABORT
        
        # Check for conflicts: did any key we wrote change on main since our snapshot?
        for key in state["write_set"]:
            cursor = self._conn.execute("""
                SELECT MAX(timestamp) as max_ts
                FROM tar_data
                WHERE key = ? AND branch_id = 'main' AND timestamp > ?
            """, (key, state["snapshot_ts"]))
            
            row = cursor.fetchone()
            if row and row["max_ts"]:
                # Conflict detected
                return Vote.ABORT
        
        return Vote.COMMIT
    
    async def commit(self, txn: Transaction) -> None:
        """Merge branch changes to main."""
        state = self._branch_state.get(txn.id)
        if not state:
            return
        
        branch_id = state["branch_id"]
        commit_ts = time.time()
        
        # For each key in write set, copy latest branch version to main
        for key in state["write_set"]:
            # Get latest version from branch
            cursor = self._conn.execute("""
                SELECT value, is_tombstone
                FROM tar_data
                WHERE key = ? AND branch_id = ?
                ORDER BY version DESC
                LIMIT 1
            """, (key, branch_id))
            
            row = cursor.fetchone()
            if row:
                # Get next version for main
                cursor = self._conn.execute("""
                    SELECT COALESCE(MAX(version), 0) + 1 as next_ver
                    FROM tar_data
                    WHERE key = ? AND branch_id = 'main'
                """, (key,))
                next_ver = cursor.fetchone()["next_ver"]
                
                # Insert into main
                self._conn.execute("""
                    INSERT INTO tar_data 
                    (key, value, version, branch_id, timestamp, txn_id, is_tombstone)
                    VALUES (?, ?, ?, 'main', ?, ?, ?)
                """, (key, row["value"], next_ver, commit_ts, txn.id, row["is_tombstone"]))
        
        self._conn.commit()
        
        # Cleanup branch data
        self._conn.execute("""
            DELETE FROM tar_data WHERE branch_id = ?
        """, (branch_id,))
        self._conn.commit()
        
        del self._branch_state[txn.id]
    
    async def abort(self, txn: Transaction) -> None:
        """Discard branch changes."""
        state = self._branch_state.get(txn.id)
        if not state:
            return
        
        branch_id = state["branch_id"]
        
        # Delete all branch data
        self._conn.execute("""
            DELETE FROM tar_data WHERE branch_id = ?
        """, (branch_id,))
        self._conn.commit()
        
        del self._branch_state[txn.id]
    
    # === Savepoints ===
    
    async def savepoint(self, txn: Transaction, sp: Savepoint) -> None:
        """Record savepoint state."""
        state = self._branch_state.get(txn.id)
        if state:
            sp.metadata["sqlite_write_set"] = state["write_set"].copy()
    
    async def rollback_to_savepoint(self, txn: Transaction, sp: Savepoint) -> None:
        """Rollback to savepoint."""
        state = self._branch_state.get(txn.id)
        if not state:
            return
        
        branch_id = state["branch_id"]
        saved_write_set = sp.metadata.get("sqlite_write_set", set())
        
        # Delete rows written after savepoint
        keys_to_revert = state["write_set"] - saved_write_set
        for key in keys_to_revert:
            self._conn.execute("""
                DELETE FROM tar_data 
                WHERE key = ? AND branch_id = ? AND timestamp > ?
            """, (key, branch_id, sp.timestamp))
        
        self._conn.commit()
        state["write_set"] = saved_write_set.copy()
    
    # === Data Operations ===
    
    async def read(self, txn: Transaction, key: str) -> Optional[Any]:
        """
        Read a key with branch visibility.
        1. Check branch for latest version <= snapshot_ts
        2. Fall back to main
        """
        state = self._branch_state.get(txn.id)
        if not state:
            raise ValueError(f"Transaction {txn.id} not found")
        
        branch_id = state["branch_id"]
        snapshot_ts = state["snapshot_ts"]
        
        # Check branch first
        cursor = self._conn.execute("""
            SELECT value, is_tombstone
            FROM tar_data
            WHERE key = ? AND branch_id = ? AND timestamp <= ?
            ORDER BY version DESC
            LIMIT 1
        """, (key, branch_id, snapshot_ts + 1000))  # +1000 to see our own writes
        
        row = cursor.fetchone()
        if row:
            if row["is_tombstone"]:
                return None
            return json.loads(row["value"]) if row["value"] else None
        
        # Fall back to main
        cursor = self._conn.execute("""
            SELECT value, is_tombstone
            FROM tar_data
            WHERE key = ? AND branch_id = 'main' AND timestamp <= ?
            ORDER BY version DESC
            LIMIT 1
        """, (key, snapshot_ts))
        
        row = cursor.fetchone()
        if row:
            if row["is_tombstone"]:
                return None
            return json.loads(row["value"]) if row["value"] else None
        
        return None
    
    async def write(self, txn: Transaction, key: str, value: Any) -> None:
        """Write a key to the branch."""
        state = self._branch_state.get(txn.id)
        if not state:
            raise ValueError(f"Transaction {txn.id} not found")
        
        branch_id = state["branch_id"]
        
        # Get next version for this key in this branch
        cursor = self._conn.execute("""
            SELECT COALESCE(MAX(version), 0) + 1 as next_ver
            FROM tar_data
            WHERE key = ? AND branch_id = ?
        """, (key, branch_id))
        next_ver = cursor.fetchone()["next_ver"]
        
        # Insert new version
        self._conn.execute("""
            INSERT INTO tar_data 
            (key, value, version, branch_id, timestamp, txn_id, is_tombstone)
            VALUES (?, ?, ?, ?, ?, ?, 0)
        """, (key, json.dumps(value), next_ver, branch_id, time.time(), txn.id))
        
        self._conn.commit()
        state["write_set"].add(key)
        txn.write_set.add(("sqlite", key))
    
    async def delete(self, txn: Transaction, key: str) -> None:
        """Delete a key (write tombstone)."""
        state = self._branch_state.get(txn.id)
        if not state:
            raise ValueError(f"Transaction {txn.id} not found")
        
        branch_id = state["branch_id"]
        
        # Get next version
        cursor = self._conn.execute("""
            SELECT COALESCE(MAX(version), 0) + 1 as next_ver
            FROM tar_data
            WHERE key = ? AND branch_id = ?
        """, (key, branch_id))
        next_ver = cursor.fetchone()["next_ver"]
        
        # Insert tombstone
        self._conn.execute("""
            INSERT INTO tar_data 
            (key, value, version, branch_id, timestamp, txn_id, is_tombstone)
            VALUES (?, NULL, ?, ?, ?, ?, 1)
        """, (key, next_ver, branch_id, time.time(), txn.id))
        
        self._conn.commit()
        state["write_set"].add(key)
        txn.write_set.add(("sqlite", key))
```

### 4.5 File System Shim

```python
# src/chronos/shims/filesystem_shim.py

import os
import shutil
import json
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass

from chronos.core.interfaces import TMCPShim, Vote
from chronos.core.types import Transaction, Savepoint

@dataclass
class FileState:
    """State of a file in a branch."""
    content: Optional[bytes]
    is_deleted: bool
    timestamp: float

class FileSystemShim(TMCPShim):
    """
    TMCP shim for file system operations.
    Uses an in-memory overlay for simplicity (could use temp dirs for larger files).
    """
    
    def __init__(self, base_path: str):
        self._base_path = Path(base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)
        
        # Per-transaction overlay: {txn_id: {path: FileState}}
        self._overlays: Dict[str, Dict[str, FileState]] = {}
    
    @property
    def shim_id(self) -> str:
        return f"fs:{self._base_path}"
    
    # === Transaction Lifecycle ===
    
    async def begin(self, txn: Transaction) -> None:
        self._overlays[txn.id] = {}
    
    async def prepare(self, txn: Transaction) -> Vote:
        """Check for conflicts with base filesystem."""
        overlay = self._overlays.get(txn.id, {})
        
        for path, state in overlay.items():
            base_file = self._base_path / path
            
            # Check if file was modified since transaction started
            if base_file.exists():
                mtime = base_file.stat().st_mtime
                if mtime > txn.snapshot_ts:
                    return Vote.ABORT
        
        return Vote.COMMIT
    
    async def commit(self, txn: Transaction) -> None:
        """Apply overlay changes to base filesystem."""
        overlay = self._overlays.get(txn.id, {})
        
        for path, state in overlay.items():
            base_file = self._base_path / path
            
            if state.is_deleted:
                if base_file.exists():
                    base_file.unlink()
            elif state.content is not None:
                base_file.parent.mkdir(parents=True, exist_ok=True)
                base_file.write_bytes(state.content)
        
        del self._overlays[txn.id]
    
    async def abort(self, txn: Transaction) -> None:
        """Discard overlay."""
        if txn.id in self._overlays:
            del self._overlays[txn.id]
    
    # === Savepoints ===
    
    async def savepoint(self, txn: Transaction, sp: Savepoint) -> None:
        overlay = self._overlays.get(txn.id, {})
        sp.metadata["fs_overlay"] = {
            path: FileState(
                content=state.content,
                is_deleted=state.is_deleted,
                timestamp=state.timestamp
            )
            for path, state in overlay.items()
        }
    
    async def rollback_to_savepoint(self, txn: Transaction, sp: Savepoint) -> None:
        saved = sp.metadata.get("fs_overlay", {})
        self._overlays[txn.id] = saved.copy()
    
    # === Data Operations ===
    
    async def read_file(self, txn: Transaction, path: str) -> Optional[bytes]:
        """Read file with overlay visibility."""
        overlay = self._overlays.get(txn.id, {})
        
        # Check overlay first
        if path in overlay:
            state = overlay[path]
            if state.is_deleted:
                raise FileNotFoundError(f"File deleted in branch: {path}")
            return state.content
        
        # Fall back to base
        base_file = self._base_path / path
        if base_file.exists():
            return base_file.read_bytes()
        
        raise FileNotFoundError(path)
    
    async def write_file(self, txn: Transaction, path: str, content: bytes) -> None:
        """Write file to overlay."""
        overlay = self._overlays.get(txn.id, {})
        
        import time
        overlay[path] = FileState(
            content=content,
            is_deleted=False,
            timestamp=time.time()
        )
        
        txn.write_set.add(("fs", path))
    
    async def delete_file(self, txn: Transaction, path: str) -> None:
        """Mark file as deleted in overlay."""
        overlay = self._overlays.get(txn.id, {})
        
        import time
        overlay[path] = FileState(
            content=None,
            is_deleted=True,
            timestamp=time.time()
        )
        
        txn.write_set.add(("fs", path))
    
    async def list_files(self, txn: Transaction, directory: str = "") -> list:
        """List files with overlay visibility."""
        overlay = self._overlays.get(txn.id, {})
        
        # Get base files
        base_dir = self._base_path / directory
        if base_dir.exists():
            base_files = set(
                str(f.relative_to(self._base_path))
                for f in base_dir.rglob("*")
                if f.is_file()
            )
        else:
            base_files = set()
        
        # Apply overlay
        result_files = base_files.copy()
        
        for path, state in overlay.items():
            if path.startswith(directory):
                if state.is_deleted:
                    result_files.discard(path)
                else:
                    result_files.add(path)
        
        return sorted(result_files)
```

---

## 5. Agent Integration

### 5.1 Chronos Tools for LLM

```python
# src/chronos/agent/tools.py

from typing import Any, Dict, List, Optional
from dataclasses import dataclass

from chronos.coordinator.coordinator import TransactionCoordinator
from chronos.core.types import Transaction

@dataclass
class ChronosToolResult:
    success: bool
    data: Any = None
    error: Optional[str] = None

class ChronosTools:
    """
    High-level Chronos tools exposed to the LLM agent.
    These are designed for context efficiency.
    """
    
    def __init__(self, coordinator: TransactionCoordinator):
        self._coordinator = coordinator
        self._current_txn: Optional[Transaction] = None
    
    async def tar_begin(self) -> ChronosToolResult:
        """Start a new transaction. Returns transaction ID."""
        try:
            self._current_txn = await self._coordinator.begin_transaction()
            return ChronosToolResult(
                success=True,
                data={"txn_id": self._current_txn.id}
            )
        except Exception as e:
            return ChronosToolResult(success=False, error=str(e))
    
    async def tar_checkpoint(self, name: str) -> ChronosToolResult:
        """Create a named checkpoint you can rollback to."""
        if not self._current_txn:
            return ChronosToolResult(success=False, error="No active transaction")
        
        try:
            sp = await self._coordinator.savepoint(self._current_txn, name)
            return ChronosToolResult(
                success=True,
                data={"checkpoint": name, "timestamp": sp.timestamp}
            )
        except Exception as e:
            return ChronosToolResult(success=False, error=str(e))
    
    async def tar_rollback(self, to_checkpoint: Optional[str] = None) -> ChronosToolResult:
        """
        Rollback changes.
        If checkpoint name provided, rollback to that point.
        Otherwise, abort entire transaction.
        """
        if not self._current_txn:
            return ChronosToolResult(success=False, error="No active transaction")
        
        try:
            await self._coordinator.rollback(self._current_txn, to_checkpoint)
            if to_checkpoint is None:
                self._current_txn = None
            return ChronosToolResult(success=True)
        except Exception as e:
            return ChronosToolResult(success=False, error=str(e))
    
    async def tar_commit(self) -> ChronosToolResult:
        """Commit all changes. Returns success/failure."""
        if not self._current_txn:
            return ChronosToolResult(success=False, error="No active transaction")
        
        try:
            await self._coordinator.commit(self._current_txn)
            self._current_txn = None
            return ChronosToolResult(success=True)
        except Exception as e:
            return ChronosToolResult(success=False, error=str(e))
    
    async def tar_status(self) -> ChronosToolResult:
        """Get current transaction status."""
        if not self._current_txn:
            return ChronosToolResult(
                success=True,
                data={"active": False}
            )
        
        return ChronosToolResult(
            success=True,
            data={
                "active": True,
                "txn_id": self._current_txn.id,
                "branch": self._current_txn.branch_id.value,
                "checkpoints": list(self._current_txn.savepoints.keys()),
                "write_count": len(self._current_txn.write_set)
            }
        )

# === Context-Optimized Tools ===

class ChronosContextOptimizedTools(ChronosTools):
    """
    Higher-level tools that hide error handling from the LLM.
    These reduce context window usage.
    """
    
    async def tar_safe_execute(
        self,
        operation: str,
        args: Dict,
        on_failure: str = "rollback"  # "rollback" | "retry" | "continue"
    ) -> ChronosToolResult:
        """
        Execute an operation with automatic error handling.
        Creates a checkpoint before, rolls back on failure.
        """
        checkpoint_name = f"pre_{operation}_{id(args)}"
        
        # Create checkpoint
        await self.tar_checkpoint(checkpoint_name)
        
        try:
            # Execute the operation (dispatch based on operation name)
            result = await self._dispatch_operation(operation, args)
            return ChronosToolResult(success=True, data=result)
        
        except Exception as e:
            if on_failure == "rollback":
                await self.tar_rollback(checkpoint_name)
                return ChronosToolResult(
                    success=False,
                    error=f"Operation failed, rolled back to checkpoint: {e}"
                )
            elif on_failure == "continue":
                return ChronosToolResult(success=False, error=str(e))
            else:
                raise
    
    async def _dispatch_operation(self, operation: str, args: Dict) -> Any:
        """Dispatch to actual shim operations."""
        # This would be implemented based on registered shims
        raise NotImplementedError()
```

### 5.2 LangChain Adapter

```python
# src/chronos/agent/adapters/langchain.py

from langchain.tools import BaseTool
from langchain.pydantic_v1 import BaseModel, Field
from typing import Optional, Type

from chronos.agent.tools import ChronosTools

class ChronosBeginInput(BaseModel):
    """Input for tar_begin - no parameters needed."""
    pass

class ChronosBeginTool(BaseTool):
    name = "tar_begin"
    description = """Start a new transaction. Call this before making any changes.
    Returns a transaction ID."""
    args_schema: Type[BaseModel] = ChronosBeginInput
    
    tar_tools: ChronosTools = None
    
    def _run(self, **kwargs):
        import asyncio
        return asyncio.run(self.tar_tools.tar_begin())
    
    async def _arun(self, **kwargs):
        return await self.tar_tools.tar_begin()


class ChronosCheckpointInput(BaseModel):
    name: str = Field(description="Name for this checkpoint")

class ChronosCheckpointTool(BaseTool):
    name = "tar_checkpoint"
    description = """Create a named checkpoint. You can rollback to this point later.
    Use descriptive names like 'before_migration' or 'after_tests'."""
    args_schema: Type[BaseModel] = ChronosCheckpointInput
    
    tar_tools: ChronosTools = None
    
    async def _arun(self, name: str):
        return await self.tar_tools.tar_checkpoint(name)


class ChronosRollbackInput(BaseModel):
    to_checkpoint: Optional[str] = Field(
        default=None,
        description="Checkpoint name to rollback to. If not provided, aborts entire transaction."
    )

class ChronosRollbackTool(BaseTool):
    name = "tar_rollback"
    description = """Rollback changes. Provide checkpoint name to rollback to that point,
    or leave empty to abort the entire transaction."""
    args_schema: Type[BaseModel] = ChronosRollbackInput
    
    tar_tools: ChronosTools = None
    
    async def _arun(self, to_checkpoint: Optional[str] = None):
        return await self.tar_tools.tar_rollback(to_checkpoint)


class ChronosCommitInput(BaseModel):
    pass

class ChronosCommitTool(BaseTool):
    name = "tar_commit"
    description = """Commit all changes made in the current transaction.
    Call this when you're satisfied with the results."""
    args_schema: Type[BaseModel] = ChronosCommitInput
    
    tar_tools: ChronosTools = None
    
    async def _arun(self, **kwargs):
        return await self.tar_tools.tar_commit()


def create_chronos_langchain_tools(tar_tools: ChronosTools) -> list:
    """Create LangChain tools from Chronos tools."""
    return [
        ChronosBeginTool(tar_tools=tar_tools),
        ChronosCheckpointTool(tar_tools=tar_tools),
        ChronosRollbackTool(tar_tools=tar_tools),
        ChronosCommitTool(tar_tools=tar_tools),
    ]
```

---

## 6. Experiments and Evaluation

### 6.1 Research Questions to Answer

| RQ | Question | Metric | Experiment |
|----|----------|--------|------------|
| RQ1 | What is the runtime overhead of Chronos? | Latency, throughput | exp1_overhead |
| RQ2 | Does Chronos improve context efficiency? | Token count, task success | exp2_context |
| RQ3 | Does Chronos provide correct isolation? | Anomaly count | exp3_isolation |
| RQ4 | How well does Chronos recover from failures? | Recovery success rate | exp4_recovery |

### 6.2 Experiment Templates

```python
# experiments/exp1_overhead/run.py

"""
Experiment 1: Runtime Overhead
Compare Chronos-wrapped operations vs direct operations.
"""

import asyncio
import time
import statistics
from dataclasses import dataclass
from typing import List

from chronos.coordinator.coordinator import TransactionCoordinator
from chronos.shims.sqlite_shim import SQLiteShim

@dataclass
class BenchmarkResult:
    operation: str
    baseline_latency_ms: float
    tar_latency_ms: float
    overhead_percent: float
    samples: int

async def benchmark_write_overhead(num_ops: int = 1000) -> BenchmarkResult:
    """Benchmark write operation overhead."""
    
    # Baseline: Direct SQLite
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE data (key TEXT PRIMARY KEY, value TEXT)")
    
    baseline_times = []
    for i in range(num_ops):
        start = time.perf_counter()
        conn.execute("INSERT OR REPLACE INTO data VALUES (?, ?)", (f"key_{i}", f"value_{i}"))
        conn.commit()
        baseline_times.append((time.perf_counter() - start) * 1000)
    
    # Chronos: Through shim
    coordinator = TransactionCoordinator()
    shim = SQLiteShim(":memory:")
    coordinator.register_shim(shim)
    
    tar_times = []
    for i in range(num_ops):
        txn = await coordinator.begin_transaction()
        await coordinator.enroll_shim(txn, shim.shim_id)
        
        start = time.perf_counter()
        await shim.write(txn, f"key_{i}", f"value_{i}")
        tar_times.append((time.perf_counter() - start) * 1000)
        
        await coordinator.commit(txn)
    
    baseline_avg = statistics.mean(baseline_times)
    tar_avg = statistics.mean(tar_times)
    
    return BenchmarkResult(
        operation="write",
        baseline_latency_ms=baseline_avg,
        tar_latency_ms=tar_avg,
        overhead_percent=((tar_avg - baseline_avg) / baseline_avg) * 100,
        samples=num_ops
    )

async def main():
    results = []
    
    print("Running overhead benchmarks...")
    results.append(await benchmark_write_overhead())
    # Add more benchmarks...
    
    # Output results
    for r in results:
        print(f"\n{r.operation}:")
        print(f"  Baseline: {r.baseline_latency_ms:.3f} ms")
        print(f"  Chronos: {r.tar_latency_ms:.3f} ms")
        print(f"  Overhead: {r.overhead_percent:.1f}%")

if __name__ == "__main__":
    asyncio.run(main())
```

### 6.3 Context Efficiency Experiment

```python
# experiments/exp2_context/run.py

"""
Experiment 2: Context Window Efficiency
Compare token usage with and without Chronos for the same tasks.
"""

import json
from dataclasses import dataclass
from typing import List

@dataclass
class ContextMetrics:
    task_name: str
    without_chronos_tokens: int
    with_chronos_tokens: int
    without_chronos_success: bool
    with_chronos_success: bool
    
    @property
    def token_reduction_percent(self) -> float:
        return ((self.without_chronos_tokens - self.with_chronos_tokens) 
                / self.without_chronos_tokens * 100)

def count_tokens(messages: List[dict]) -> int:
    """Simple token counting (use tiktoken for accuracy)."""
    import tiktoken
    enc = tiktoken.encoding_for_model("gpt-4")
    
    total = 0
    for msg in messages:
        total += len(enc.encode(msg.get("content", "")))
    return total

# Tasks to evaluate
TASKS = [
    {
        "name": "file_refactor_with_error",
        "description": "Refactor a file, encounter error, recover",
        "expected_outcome": "Successful refactor after retry"
    },
    {
        "name": "database_migration",
        "description": "Run a multi-step migration with rollback on failure",
        "expected_outcome": "Clean state after failed migration"
    },
    {
        "name": "multi_approach_exploration",
        "description": "Try 3 approaches, pick the best",
        "expected_outcome": "Best approach committed, others discarded"
    }
]

# Run experiments and collect traces
# Compare token counts between Chronos and non-Chronos versions
```

---

## 7. Demo Applications

### 7.1 Hotel Reservation Demo (Simplified)

```python
# tests/demos/hotel_reservation/agent.py

"""
Hotel Reservation Demo
Demonstrates: Multi-shim transaction, compensation on failure
"""

import asyncio
from chronos.coordinator.coordinator import TransactionCoordinator
from chronos.shims.sqlite_shim import SQLiteShim

async def demo_booking():
    # Setup
    coordinator = TransactionCoordinator()
    
    # Database shim for reservations
    db_shim = SQLiteShim("hotel.db")
    coordinator.register_shim(db_shim)
    
    # Seed data
    await seed_hotel_data(db_shim)
    
    # Start transaction
    txn = await coordinator.begin_transaction()
    await coordinator.enroll_shim(txn, db_shim.shim_id)
    
    try:
        # Step 1: Check availability
        room = await db_shim.read(txn, "room:101")
        if room.get("available") != True:
            raise Exception("Room not available")
        
        # Step 2: Create reservation
        await db_shim.write(txn, "reservation:12345", {
            "room": "101",
            "guest": "Alice",
            "dates": "2024-02-15 to 2024-02-17"
        })
        
        # Step 3: Update room availability
        room["available"] = False
        await db_shim.write(txn, "room:101", room)
        
        # Step 4: Simulate payment (could fail)
        payment_success = await mock_payment("4242", 299.99)
        if not payment_success:
            raise Exception("Payment failed")
        
        # Commit
        await coordinator.commit(txn)
        print("✅ Booking successful!")
        
    except Exception as e:
        print(f"❌ Booking failed: {e}")
        await coordinator.rollback(txn)
        print("↩️ Transaction rolled back")

async def mock_payment(card_suffix: str, amount: float) -> bool:
    """Mock payment - fails 30% of the time for demo."""
    import random
    return random.random() > 0.3

async def seed_hotel_data(shim: SQLiteShim):
    """Seed initial hotel data."""
    # This would be done outside a transaction in a real system
    pass

if __name__ == "__main__":
    asyncio.run(demo_booking())
```

### 7.2 Coding Agent Demo

```python
# tests/demos/coding_agent/agent.py

"""
Coding Agent Demo
Demonstrates: File system overlay, checkpoints for retry
"""

import asyncio
from chronos.coordinator.coordinator import TransactionCoordinator
from chronos.shims.filesystem_shim import FileSystemShim

async def demo_refactor():
    # Setup
    coordinator = TransactionCoordinator()
    
    fs_shim = FileSystemShim("./sample_project")
    coordinator.register_shim(fs_shim)
    
    # Start transaction
    txn = await coordinator.begin_transaction()
    await coordinator.enroll_shim(txn, fs_shim.shim_id)
    
    try:
        # Create checkpoint before changes
        await coordinator.savepoint(txn, "pre-refactor")
        
        # Read current file
        old_code = await fs_shim.read_file(txn, "auth/session_auth.py")
        print(f"📖 Read {len(old_code)} bytes from auth/session_auth.py")
        
        # Generate new code (mock LLM call)
        new_code = generate_jwt_auth(old_code.decode())
        
        # Write new file
        await fs_shim.write_file(txn, "auth/jwt_auth.py", new_code.encode())
        print("📝 Wrote auth/jwt_auth.py")
        
        # Delete old file
        await fs_shim.delete_file(txn, "auth/session_auth.py")
        print("🗑️ Deleted auth/session_auth.py")
        
        # Run tests (mock - could fail)
        tests_pass = await run_tests()
        
        if not tests_pass:
            print("❌ Tests failed, rolling back to checkpoint")
            await coordinator.rollback(txn, "pre-refactor")
            
            # Try alternative approach...
            # (In a real agent, this would be an LLM decision)
            
        else:
            print("✅ Tests passed, committing")
            await coordinator.commit(txn)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        await coordinator.rollback(txn)

def generate_jwt_auth(old_code: str) -> str:
    """Mock code generation."""
    return old_code.replace("session", "jwt")

async def run_tests() -> bool:
    """Mock test runner."""
    import random
    return random.random() > 0.3

if __name__ == "__main__":
    asyncio.run(demo_refactor())
```

---

## 8. Implementation Phases

### Phase 1: Core (2-3 weeks)
- [ ] Transaction and branch types
- [ ] TMCP shim interface
- [ ] Transaction coordinator (single-shim)
- [ ] SQLite shim with MVCC
- [ ] Basic unit tests

### Phase 2: Multi-Shim (2-3 weeks)
- [ ] 2PC protocol in coordinator
- [ ] File system shim
- [ ] Savepoints and checkpoints
- [ ] Integration tests

### Phase 3: Agent Integration (2 weeks)
- [ ] Chronos tools for LLM
- [ ] LangChain adapter
- [ ] Simple ReAct demo
- [ ] Context optimization tools

### Phase 4: Demos & Experiments (2-3 weeks)
- [ ] Hotel reservation demo
- [ ] Coding agent demo
- [ ] Overhead benchmarks
- [ ] Context efficiency experiments

### Phase 5: Paper Writing (2-4 weeks)
- [ ] Collect results
- [ ] Generate figures
- [ ] Write paper

---

## 9. Simplifications for Prototype

### What We're NOT Building (Yet)

| Component | Production Need | Prototype Approach |
|-----------|-----------------|-------------------|
| Coordinator persistence | Yes | In-memory only |
| Distributed coordinator | Yes | Single-node |
| Recovery from crashes | Yes | Transactions lost on crash |
| Security/multi-tenant | Yes | Single user assumed |
| Connection pooling | Yes | Single connection |
| Background GC | Yes | Eager cleanup on commit/abort |
| SQL query parsing | Yes | Key-value interface only |
| Real payment APIs | Yes | Mock with compensation stubs |

### Acceptable Limitations for Research

1. **Memory-only transactions**: Fine for demos, just don't run > 1000s of concurrent branches
2. **No coordinator recovery**: Document as future work
3. **SQLite only for SQL**: PostgreSQL can be added later, SQLite proves the concept
4. **Simple conflict detection**: Write-write only, no predicate locks

---

## 10. Getting Started

```bash
# Clone and setup
git clone <repo>
cd chronos

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/unit -v

# Run demo
python tests/demos/hotel_reservation/agent.py
```

---

*Document created for Chronos academic prototype*
*Sea Labs AI Research Team - February 2026*
