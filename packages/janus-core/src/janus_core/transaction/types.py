"""Core types for LangGraph transactional execution.

Provides virtual branching, savepoints, and atomic commit/rollback
for agent tool executions across heterogeneous backends.

Supports 1-level nested subtransactions via the self_set extension
to the MVCC visibility predicate (Epoxy model).
"""

from __future__ import annotations

import enum
import itertools
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

# Global monotonically increasing transaction ID counter
_txn_counter = itertools.count(1)


def _next_numeric_id() -> int:
    """Get the next globally unique numeric transaction ID."""
    return next(_txn_counter)


class TransactionState(enum.Enum):
    """State of a transaction."""

    ACTIVE = "active"
    PREPARING = "preparing"
    COMMITTED = "committed"
    ABORTED = "aborted"
    COMMITTED_INTO_PARENT = "committed_into_parent"


class Vote(enum.Enum):
    """2PC vote from a shim participant."""

    COMMIT = "commit"
    ABORT = "abort"


class ChangeType(enum.Enum):
    """Type of state change recorded by a shim."""

    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


@dataclass
class BranchId:
    """Unique branch identifier with optional parent lineage."""

    id: str
    parent_id: str | None = None

    @staticmethod
    def create(parent: BranchId | None = None) -> BranchId:
        bid = uuid.uuid4().hex[:12]
        return BranchId(id=bid, parent_id=parent.id if parent else None)

    def __str__(self) -> str:
        if self.parent_id:
            return f"{self.parent_id}:{self.id}"
        return self.id


@dataclass
class TxnSnapshot:
    """MVCC snapshot for visibility predicate evaluation.

    The self_set S(x) controls which records are visible to transaction x:
      visible(r, x) =
        (r.begin_txn in committed_set OR r.begin_txn in self_set)
        AND (r.end_txn IS NULL OR (r.end_txn NOT in committed_set AND r.end_txn NOT in self_set))

    For flat transactions: self_set = {numeric_id} (single element).
    For subtransactions: self_set grows as children commit into parent.
    """

    xmin: int  # snapshot "low-water mark" — txns < xmin are committed
    committed_set: frozenset[int]  # txns committed since xmin
    self_set: set[int]  # {own_id} ∪ {committed children's ids}

    def is_visible(self, begin_txn: int, end_txn: int | None) -> bool:
        """Check if a record version is visible under this snapshot.

        Args:
            begin_txn: The numeric txn ID that created this record version.
            end_txn: The numeric txn ID that deleted/superseded this version,
                     or None/0 if the version is live.
        """
        # begin_txn must be visible (committed before snapshot or self)
        begin_visible = (
            begin_txn < self.xmin
            or begin_txn in self.committed_set
            or begin_txn in self.self_set
        )
        if not begin_visible:
            return False

        # end_txn must NOT be visible (record not yet superseded from our POV)
        if end_txn is None or end_txn == 0:
            return True  # record is live
        end_visible = (
            end_txn < self.xmin
            or end_txn in self.committed_set
            or end_txn in self.self_set
        )
        return not end_visible

    def with_child(self, child_numeric_id: int) -> TxnSnapshot:
        """Create a derived snapshot for a child subtransaction.

        The child sees everything the parent sees, plus its own writes.
        """
        child_self = self.self_set.copy()
        child_self.add(child_numeric_id)
        return TxnSnapshot(
            xmin=self.xmin,
            committed_set=self.committed_set,
            self_set=child_self,
        )

    def commit_child(self, child_numeric_id: int) -> None:
        """Commit a child into this (parent) snapshot. O(1)."""
        self.self_set.add(child_numeric_id)

    def abort_child(self, child_numeric_id: int) -> None:
        """Abort a child — ensure it's not in self_set. O(1)."""
        self.self_set.discard(child_numeric_id)

    def visibility_sql(self) -> tuple[str, list[int]]:
        """Generate SQL WHERE clause fragment for this snapshot.

        Returns (clause_str, params) where clause_str uses ? placeholders.
        """
        # Build combined set: committed_set ∪ self_set
        visible_ids = list(self.committed_set | self.self_set)
        if not visible_ids:
            # Only xmin-based visibility
            return (
                "(_begin_txn < ? AND (_end_txn IS NULL OR _end_txn = 0 OR _end_txn >= ?))",
                [self.xmin, self.xmin],
            )

        placeholders = ",".join("?" * len(visible_ids))
        clause = (
            f"(_begin_txn < ? OR _begin_txn IN ({placeholders})) "
            f"AND (_end_txn IS NULL OR _end_txn = 0 OR "
            f"(_end_txn >= ? AND _end_txn NOT IN ({placeholders})))"
        )
        params: list[int] = (
            [self.xmin] + visible_ids + [self.xmin] + visible_ids
        )
        return clause, params


@dataclass
class ChangeRecord:
    """A single state change recorded by a shim."""

    shim_id: str
    resource_id: str
    change_type: ChangeType
    old_value: Any = None
    new_value: Any = None
    timestamp: float = field(default_factory=time.time)

    def summary(self) -> str:
        return f"[{self.shim_id}] {self.change_type.value} {self.resource_id}"


@dataclass
class Savepoint:
    """A named checkpoint within a transaction."""

    id: str
    name: str
    transaction_id: str
    timestamp: float = field(default_factory=time.time)
    # Per-shim opaque snapshot data, keyed by shim_id
    shim_snapshots: dict[str, Any] = field(default_factory=dict)
    # Child numeric ID associated with this savepoint (for MVCC subtxns)
    child_numeric_id: int | None = None

    @staticmethod
    def create(
        name: str, transaction_id: str, child_numeric_id: int | None = None
    ) -> Savepoint:
        return Savepoint(
            id=uuid.uuid4().hex[:12],
            name=name,
            transaction_id=transaction_id,
            child_numeric_id=child_numeric_id,
        )


@dataclass
class TransactionHandle:
    """Full transaction context spanning multiple shims."""

    id: str
    branch_id: BranchId
    numeric_id: int = field(default_factory=_next_numeric_id)
    state: TransactionState = TransactionState.ACTIVE
    snapshot_ts: float = field(default_factory=time.time)
    snapshot: TxnSnapshot | None = None
    read_set: set[tuple[str, str]] = field(default_factory=set)
    write_set: set[tuple[str, str]] = field(default_factory=set)
    savepoints: list[Savepoint] = field(default_factory=list)
    participants: list[str] = field(default_factory=list)
    parent_id: str | None = None
    children: list[str] = field(default_factory=list)  # child txn IDs

    @staticmethod
    def create(parent: TransactionHandle | None = None) -> TransactionHandle:
        txn_id = uuid.uuid4().hex[:12]
        parent_branch = parent.branch_id if parent else None
        numeric = _next_numeric_id()
        txn = TransactionHandle(
            id=txn_id,
            branch_id=BranchId.create(parent_branch),
            numeric_id=numeric,
            parent_id=parent.id if parent else None,
        )
        return txn

    @property
    def is_active(self) -> bool:
        return self.state == TransactionState.ACTIVE


@dataclass
class TransactionPolicy:
    """Configuration for automatic transaction management."""

    auto_begin: bool = False
    auto_commit_on_end: bool = False
    auto_savepoint_before_tools: bool = True
    dangerous_patterns: list[str] = field(
        default_factory=lambda: [
            r"(?i)\b(delete|drop|truncate|remove|rm)\b",
        ]
    )
    isolation_level: str = "snapshot"
    conflict_resolution: str = "fail"

    def is_dangerous(self, tool_name: str, tool_input: str = "") -> bool:
        """Check if a tool call matches a dangerous pattern."""
        text = f"{tool_name} {tool_input}"
        return any(re.search(p, text) for p in self.dangerous_patterns)
