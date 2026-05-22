"""Shim interface for transactional tool wrapping.

A ToolShim intercepts operations to a stateful backend and implements
virtual branching — isolated, copy-on-write execution contexts that
can be committed or discarded atomically.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from chronos_core.transaction.types import (
    ChangeRecord,
    Savepoint,
    TransactionHandle,
    Vote,
)


class ToolShim(ABC):
    """Abstract base for transactional tool shims.

    Each shim wraps a stateful backend (filesystem, database, vector store)
    and participates in the coordinator's 2PC protocol. Shims implement
    virtual branching so that writes within a transaction are isolated
    until commit.
    """

    @property
    @abstractmethod
    def shim_id(self) -> str:
        """Unique identifier for this shim instance."""
        ...

    @abstractmethod
    def begin(self, txn: TransactionHandle) -> None:
        """Initialize branch-local state for a new transaction."""
        ...

    @abstractmethod
    def prepare(self, txn: TransactionHandle) -> Vote:
        """2PC prepare: validate and vote COMMIT or ABORT."""
        ...

    @abstractmethod
    def commit(self, txn: TransactionHandle) -> None:
        """Merge branch changes into the main state."""
        ...

    @abstractmethod
    def abort(self, txn: TransactionHandle) -> None:
        """Discard all branch changes."""
        ...

    @abstractmethod
    def savepoint(self, txn: TransactionHandle, sp: Savepoint) -> Any:
        """Create a snapshot of the current branch state.

        Returns opaque snapshot data to be stored in sp.shim_snapshots.
        """
        ...

    @abstractmethod
    def rollback_to_savepoint(
        self, txn: TransactionHandle, sp: Savepoint
    ) -> None:
        """Restore branch state from a savepoint snapshot."""
        ...

    @abstractmethod
    def get_changes(self, txn: TransactionHandle) -> list[ChangeRecord]:
        """Return all changes made in this transaction's branch."""
        ...
