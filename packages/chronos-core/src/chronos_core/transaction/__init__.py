"""Core transaction support for agent runtimes.

Provides virtual branching, savepoints, 1-level subtransactions, and atomic
commit/rollback for agent tool executions across heterogeneous backends
(filesystem, SQLite, sqlite_vec, etc.).

Key components:

- :class:`TransactionCoordinator`: Manages transaction lifecycle with
  two-phase commit and 1-level subtransactions across registered shims.
- :class:`OverlayFSShim`: Real Linux OverlayFS-backed filesystem
  branching (requires root).
- :class:`SQLiteShim`: MVCC-based SQLite relational database shim.
- :class:`PostgresShim`: MVCC-based PostgreSQL relational database shim.
- :class:`SqliteVecShim`: MVCC-based sqlite_vec vector store shim.
- :class:`ToolShim`: Abstract base class for plugging new transactional
  backends.
"""

from chronos_core.transaction.coordinator import (
    ActiveChildError,
    CommitConflictError,
    NestingDepthError,
    SavepointNotFoundError,
    TransactionCoordinator,
    TransactionError,
    TransactionNotActiveError,
)
from chronos_core.transaction.shim import ToolShim
from chronos_core.transaction.shim_fs import OverlayFSShim
from chronos_core.transaction.shim_postgres import PostgresShim
from chronos_core.transaction.shim_sqlite import SQLiteShim
from chronos_core.transaction.shim_vec import SqliteVecShim
from chronos_core.transaction.types import (
    BranchId,
    ChangeRecord,
    ChangeType,
    Savepoint,
    TransactionHandle,
    TransactionPolicy,
    TransactionState,
    TxnSnapshot,
    Vote,
)

__all__ = [
    # Coordinator
    "TransactionCoordinator",
    "TransactionError",
    "TransactionNotActiveError",
    "SavepointNotFoundError",
    "CommitConflictError",
    "ActiveChildError",
    "NestingDepthError",
    # Shims
    "ToolShim",
    "OverlayFSShim",
    "SQLiteShim",
    "PostgresShim",
    "SqliteVecShim",
    # Types
    "BranchId",
    "ChangeRecord",
    "ChangeType",
    "Savepoint",
    "TransactionHandle",
    "TransactionPolicy",
    "TransactionState",
    "TxnSnapshot",
    "Vote",
]
