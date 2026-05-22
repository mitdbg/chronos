"""Transaction coordinator for LangGraph agents.

Manages the lifecycle of transactions across multiple ToolShim participants
using a two-phase commit (2PC) protocol. Provides begin, commit, rollback,
savepoint, and 1-level nested subtransaction operations.

Subtransaction model:
  - begin_child(parent_txn_id) creates a child with its own numeric_id.
  - commit_child(child_txn_id) merges child into parent (O(1) for MVCC shims).
  - abort_child(child_txn_id) discards child (O(1) for MVCC shims).
  - Savepoints map to sequential children internally.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from janus_core.transaction.shim import ToolShim
from janus_core.transaction.types import (
    ChangeRecord,
    Savepoint,
    TransactionHandle,
    TransactionPolicy,
    TransactionState,
    TxnSnapshot,
    Vote,
    _next_numeric_id,
)

logger = logging.getLogger(__name__)


class TransactionError(Exception):
    """Base exception for transaction errors."""


class TransactionNotActiveError(TransactionError):
    """Raised when an operation requires an active transaction."""


class SavepointNotFoundError(TransactionError):
    """Raised when a savepoint name is not found."""


class CommitConflictError(TransactionError):
    """Raised when a prepare-phase vote is ABORT due to conflicts."""

    def __init__(self, message: str, conflicts: list[str] | None = None):
        super().__init__(message)
        self.conflicts = conflicts or []


class ActiveChildError(TransactionError):
    """Raised when committing a parent that has an active (uncommitted) child."""


class NestingDepthError(TransactionError):
    """Raised when attempting to nest deeper than 1 level."""


class TransactionCoordinator:
    """Coordinates transactions across multiple ToolShim participants.

    Manages the full transaction lifecycle: begin, savepoint, rollback,
    two-phase commit, and 1-level subtransactions across all enrolled shims.

    Example::

        coordinator = TransactionCoordinator()
        coordinator.register_shim(fs_shim)
        coordinator.register_shim(sqlite_shim)
        coordinator.register_shim(vec_shim)

        txn = coordinator.begin()
        # ... agent performs tool calls ...
        child = coordinator.begin_child(txn.id)
        # ... speculative exploration ...
        coordinator.commit_child(child.id)  # merge into parent
        coordinator.commit(txn.id)  # merge to main
    """

    def __init__(
        self,
        *,
        policy: TransactionPolicy | None = None,
        max_concurrent: int = 10,
    ):
        self._shims: dict[str, ToolShim] = {}
        self._transactions: dict[str, TransactionHandle] = {}
        self._active_txn_id: str | None = None
        self._lock = threading.Lock()
        self._policy = policy or TransactionPolicy()
        self._max_concurrent = max_concurrent
        # Global counter for xmin computation
        self._committed_txn_ids: set[int] = set()
        self._min_active_numeric_id: int = 0

    @property
    def policy(self) -> TransactionPolicy:
        return self._policy

    # ── Shim management ──────────────────────────────────────────────

    def register_shim(self, shim: ToolShim) -> None:
        """Register a ToolShim to participate in transactions."""
        with self._lock:
            if shim.shim_id in self._shims:
                raise TransactionError(
                    f"Shim '{shim.shim_id}' is already registered"
                )
            self._shims[shim.shim_id] = shim
            logger.info("Registered shim: %s", shim.shim_id)

    def get_shim(self, shim_id: str) -> ToolShim:
        return self._shims[shim_id]

    # ── Transaction lifecycle ────────────────────────────────────────

    def begin(
        self, parent: TransactionHandle | None = None
    ) -> TransactionHandle:
        """Start a new top-level transaction, enrolling all registered shims."""
        with self._lock:
            if (
                len(
                    [
                        t
                        for t in self._transactions.values()
                        if t.parent_id is None and t.is_active
                    ]
                )
                >= self._max_concurrent
            ):
                raise TransactionError(
                    f"Max concurrent transactions ({self._max_concurrent}) reached"
                )

            txn = TransactionHandle.create(parent)

            # Create MVCC snapshot
            xmin = self._min_active_numeric_id or txn.numeric_id
            txn.snapshot = TxnSnapshot(
                xmin=xmin,
                committed_set=frozenset(self._committed_txn_ids),
                self_set={txn.numeric_id},
            )

            self._transactions[txn.id] = txn

            # Track minimum active numeric_id for future xmin computations.
            # Only set when currently 0 (no active transactions); subsequent
            # transactions have higher numeric_ids so they cannot lower the min.
            if self._min_active_numeric_id == 0:
                self._min_active_numeric_id = txn.numeric_id

            # Enroll all shims
            for shim_id, shim in self._shims.items():
                shim.begin(txn)
                txn.participants.append(shim_id)

            if parent is None:
                self._active_txn_id = txn.id

            logger.info(
                "Transaction %s begun (branch=%s, numeric=%d, participants=%s)",
                txn.id,
                txn.branch_id,
                txn.numeric_id,
                txn.participants,
            )
            return txn

    # ── Subtransaction lifecycle ─────────────────────────────────────

    def begin_child(self, parent_txn_id: str) -> TransactionHandle:
        """Start a child subtransaction (1-level nesting only).

        The child gets its own numeric_id. Its snapshot is derived
        from the parent's snapshot with the child_id added to self_set.
        """
        parent = self._resolve_txn(parent_txn_id)
        if not parent.is_active:
            raise TransactionNotActiveError(
                f"Parent {parent.id} is not active"
            )
        # Enforce 1-level nesting
        if parent.parent_id is not None:
            raise NestingDepthError(
                "Cannot create child of a child (max 1-level nesting)"
            )

        child = TransactionHandle.create(parent)

        # Derive child snapshot from parent
        assert parent.snapshot is not None
        child.snapshot = parent.snapshot.with_child(child.numeric_id)

        self._transactions[child.id] = child
        parent.children.append(child.id)

        # Enroll child in all shims
        for shim_id, shim in self._shims.items():
            child.participants.append(shim_id)
            # Use shim-specific child begin if available
            if hasattr(shim, "begin_child"):
                shim.begin_child(child, parent)  # type: ignore[arg-type]
            else:
                shim.begin(child)

        logger.info(
            "Child transaction %s begun (parent=%s, numeric=%d)",
            child.id,
            parent.id,
            child.numeric_id,
        )
        return child

    def commit_child(self, child_txn_id: str) -> None:
        """Commit a child into its parent.

        For MVCC shims: O(1) — add child.numeric_id to parent's self_set.
        For OverlayFS: merge child upper → parent upper + remount.
        """
        child = self._resolve_txn(child_txn_id)
        if not child.is_active:
            raise TransactionNotActiveError(
                f"Child {child.id} is not active"
            )
        if child.parent_id is None:
            raise TransactionError(
                f"Transaction {child.id} is not a child"
            )

        parent = self._resolve_txn(child.parent_id)

        # Update parent's snapshot self_set
        assert parent.snapshot is not None
        parent.snapshot.commit_child(child.numeric_id)

        # Merge write sets
        parent.write_set |= child.write_set

        # Notify shims
        for shim_id in child.participants:
            shim = self._shims[shim_id]
            if hasattr(shim, "commit_child"):
                shim.commit_child(child, parent)  # type: ignore[arg-type]

        child.state = TransactionState.COMMITTED_INTO_PARENT
        logger.info(
            "Child %s committed into parent %s", child.id, parent.id
        )

    def abort_child(self, child_txn_id: str) -> None:
        """Abort a child subtransaction.

        For MVCC shims: O(1) — child_id not in parent's self_set.
        For OverlayFS: umount + rm -rf child overlay.
        """
        child = self._resolve_txn(child_txn_id)
        if child.state in (
            TransactionState.COMMITTED,
            TransactionState.COMMITTED_INTO_PARENT,
            TransactionState.ABORTED,
        ):
            return  # already finalized

        if child.parent_id is None:
            raise TransactionError(
                f"Transaction {child.id} is not a child"
            )

        parent = self._resolve_txn(child.parent_id)

        # Ensure child_id is NOT in parent's self_set
        assert parent.snapshot is not None
        parent.snapshot.abort_child(child.numeric_id)

        # Notify shims
        for shim_id in child.participants:
            shim = self._shims[shim_id]
            if hasattr(shim, "abort_child"):
                shim.abort_child(child, parent)  # type: ignore[arg-type]

        child.state = TransactionState.ABORTED
        logger.info(
            "Child %s aborted (parent %s)", child.id, parent.id
        )

    # ── Top-level commit/rollback ────────────────────────────────────

    def commit(self, txn_id: str | None = None) -> list[ChangeRecord]:
        """Two-phase commit: prepare all shims, then commit or abort.

        Before committing, ensures no active (uncommitted) children remain.
        Returns the list of all changes that were committed.
        """
        txn = self._resolve_txn(txn_id)
        if not txn.is_active:
            raise TransactionNotActiveError(
                f"Transaction {txn.id} is {txn.state.value}, not active"
            )

        # Check for active children
        for child_id in txn.children:
            child = self._transactions.get(child_id)
            if child and child.is_active:
                raise ActiveChildError(
                    f"Cannot commit parent while child {child.id} is active"
                )

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
            # Abort all
            for shim_id in txn.participants:
                try:
                    self._shims[shim_id].abort(txn)
                except Exception as e:
                    logger.error("Error aborting shim %s: %s", shim_id, e)
            txn.state = TransactionState.ABORTED
            with self._lock:
                if self._active_txn_id == txn.id:
                    self._active_txn_id = None
            raise CommitConflictError(
                f"Commit aborted: {'; '.join(abort_reasons)}",
                conflicts=abort_reasons,
            )

        # All voted COMMIT — apply. If any participant fails during commit,
        # run best-effort compensating abort on all participants to avoid
        # leaving partial durable state.
        all_changes: list[ChangeRecord] = []
        try:
            for shim_id in txn.participants:
                shim = self._shims[shim_id]
                all_changes.extend(shim.get_changes(txn))
                shim.commit(txn)
        except Exception as e:
            abort_reasons = [f"commit failed on {shim_id}: {e}"]
            for abort_shim_id in txn.participants:
                try:
                    self._shims[abort_shim_id].abort(txn)
                except Exception as abort_err:
                    logger.error(
                        "Error aborting shim %s after commit failure: %s",
                        abort_shim_id,
                        abort_err,
                    )
                    abort_reasons.append(
                        f"{abort_shim_id}: abort after commit failure raised {abort_err}"
                    )
            txn.state = TransactionState.ABORTED
            with self._lock:
                if self._active_txn_id == txn.id:
                    self._active_txn_id = None
                self._recompute_min_active()
            raise CommitConflictError(
                f"Commit aborted during phase 2: {'; '.join(abort_reasons)}",
                conflicts=abort_reasons,
            ) from e

        txn.state = TransactionState.COMMITTED
        with self._lock:
            self._committed_txn_ids.add(txn.numeric_id)
            # Also mark all committed children as globally committed
            if txn.snapshot:
                for nid in txn.snapshot.self_set:
                    if nid != txn.numeric_id:
                        self._committed_txn_ids.add(nid)
            if self._active_txn_id == txn.id:
                self._active_txn_id = None
            self._recompute_min_active()

        logger.info(
            "Transaction %s committed (%d changes)",
            txn.id,
            len(all_changes),
        )
        return all_changes

    def rollback(self, txn_id: str | None = None) -> None:
        """Abort the transaction and discard all branch changes."""
        txn = self._resolve_txn(txn_id)
        if txn.state in (
            TransactionState.COMMITTED,
            TransactionState.ABORTED,
        ):
            raise TransactionNotActiveError(
                f"Transaction {txn.id} already {txn.state.value}"
            )

        # First abort any active children
        for child_id in txn.children:
            child = self._transactions.get(child_id)
            if child and child.is_active:
                self.abort_child(child_id)

        for shim_id in txn.participants:
            try:
                self._shims[shim_id].abort(txn)
            except Exception as e:
                logger.error("Error aborting shim %s: %s", shim_id, e)

        txn.state = TransactionState.ABORTED
        with self._lock:
            if self._active_txn_id == txn.id:
                self._active_txn_id = None
            self._recompute_min_active()

        logger.info("Transaction %s rolled back", txn.id)

    # ── Savepoints ───────────────────────────────────────────────────

    def savepoint(
        self, name: str, txn_id: str | None = None
    ) -> Savepoint:
        """Create a named savepoint across all enrolled shims."""
        txn = self._resolve_txn(txn_id)
        if not txn.is_active:
            raise TransactionNotActiveError(
                f"Transaction {txn.id} is not active"
            )

        # Check for duplicate name
        for sp in txn.savepoints:
            if sp.name == name:
                raise TransactionError(
                    f"Savepoint '{name}' already exists in transaction {txn.id}"
                )

        sp = Savepoint.create(name, txn.id)

        # Notify each shim to create a snapshot
        for shim_id in txn.participants:
            shim = self._shims[shim_id]
            snapshot_data = shim.savepoint(txn, sp)
            sp.shim_snapshots[shim_id] = snapshot_data

        txn.savepoints.append(sp)
        logger.info(
            "Savepoint '%s' created in transaction %s", name, txn.id
        )
        return sp

    def rollback_to_savepoint(
        self, name: str, txn_id: str | None = None
    ) -> None:
        """Rollback to a named savepoint, discarding changes after it."""
        txn = self._resolve_txn(txn_id)
        if not txn.is_active:
            raise TransactionNotActiveError(
                f"Transaction {txn.id} is not active"
            )

        # Find the savepoint
        sp_idx = None
        for i, sp in enumerate(txn.savepoints):
            if sp.name == name:
                sp_idx = i
                break

        if sp_idx is None:
            raise SavepointNotFoundError(
                f"Savepoint '{name}' not found in transaction {txn.id}"
            )

        target_sp = txn.savepoints[sp_idx]

        # Rollback each shim to this savepoint
        for shim_id in txn.participants:
            shim = self._shims[shim_id]
            shim.rollback_to_savepoint(txn, target_sp)

        # Discard savepoints after the target
        txn.savepoints = txn.savepoints[: sp_idx + 1]

        logger.info(
            "Rolled back to savepoint '%s' in transaction %s",
            name,
            txn.id,
        )

    # ── Inspection ───────────────────────────────────────────────────

    def get_changes(
        self, txn_id: str | None = None
    ) -> list[ChangeRecord]:
        """Get all changes made in a transaction across all shims."""
        txn = self._resolve_txn(txn_id)
        changes: list[ChangeRecord] = []
        for shim_id in txn.participants:
            changes.extend(self._shims[shim_id].get_changes(txn))
        return changes

    def get_active_transaction(self) -> TransactionHandle | None:
        """Get the currently active transaction, if any."""
        if self._active_txn_id is None:
            return None
        return self._transactions.get(self._active_txn_id)

    def status(self) -> dict[str, Any]:
        """Return a summary of the coordinator's state."""
        active = self.get_active_transaction()
        return {
            "active_transaction": active.id if active else None,
            "active_branch": str(active.branch_id) if active else None,
            "transaction_state": active.state.value if active else None,
            "participants": list(self._shims.keys()),
            "savepoints": (
                [sp.name for sp in active.savepoints] if active else []
            ),
            "total_transactions": len(self._transactions),
            "change_count": (
                len(self.get_changes()) if active else 0
            ),
        }

    # ── Helpers ───────────────────────────────────────────────────────

    def _recompute_min_active(self) -> None:
        """Recompute _min_active_numeric_id from current active top-level txns.

        Called (under self._lock) after every commit or rollback so that the
        next begin() uses an accurate xmin for its MVCC snapshot.
        """
        active_ids = [
            t.numeric_id
            for t in self._transactions.values()
            if t.parent_id is None and t.is_active
        ]
        self._min_active_numeric_id = min(active_ids) if active_ids else 0

    def _resolve_txn(
        self, txn_id: str | None = None
    ) -> TransactionHandle:
        """Resolve a transaction by ID or return the active one."""
        if txn_id is not None:
            txn = self._transactions.get(txn_id)
            if txn is None:
                raise TransactionNotActiveError(
                    f"Transaction {txn_id} not found"
                )
            return txn

        if self._active_txn_id is None:
            raise TransactionNotActiveError("No active transaction")
        txn = self._transactions.get(self._active_txn_id)
        if txn is None:
            raise TransactionNotActiveError("Active transaction not found")
        return txn
