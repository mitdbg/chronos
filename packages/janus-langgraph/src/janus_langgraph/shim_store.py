"""Branch-aware store shim for transactional agent memory.

Wraps any LangGraph ``BaseStore`` to provide branch-isolated reads and
writes. Branch data is persisted in the *underlying* store using
namespace-prefixed keys, so nothing is purely in-memory.

The branching model:
  - Writes in a transaction go to a branch-specific namespace
    ``("__txn__", branch_id, *original_namespace)``
  - Reads check the branch namespace first, falling back to the main
    namespace (read-through)
  - Tombstones (deletes) are recorded in the branch namespace so that
    deleted items are masked from reads
  - On commit: branch entries are flushed to the main namespace
  - On abort: branch entries are deleted

Vector search is branch-aware: results from both the branch and main
namespaces are merged, with the branch taking priority.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from langgraph.store.base import (
    BaseStore,
    GetOp,
    Item,
    ListNamespacesOp,
    Op,
    PutOp,
    Result,
    SearchItem,
    SearchOp,
)
from janus_core.transaction.shim import ToolShim
from janus_core.transaction.types import (
    ChangeRecord,
    ChangeType,
    Savepoint,
    TransactionHandle,
    Vote,
)

logger = logging.getLogger(__name__)

_TOMBSTONE_VALUE = {"__tombstone__": True}
_BRANCH_PREFIX = "__txn__"


def _is_tombstone(item: Item | None) -> bool:
    """Check if an item is a tombstone marker."""
    if item is None:
        return False
    return isinstance(item.value, dict) and item.value.get("__tombstone__") is True


def _branch_ns(
    branch_id: str, namespace: tuple[str, ...]
) -> tuple[str, ...]:
    """Prefix a namespace with the branch identifier."""
    return (_BRANCH_PREFIX, branch_id) + namespace


def _strip_branch_ns(
    branch_id: str, namespace: tuple[str, ...]
) -> tuple[str, ...]:
    """Remove branch prefix from a namespace."""
    prefix = (_BRANCH_PREFIX, branch_id)
    if namespace[: len(prefix)] == prefix:
        return namespace[len(prefix) :]
    return namespace


@dataclass
class _JournalEntry:
    """Records a single branch operation for savepoint rollback."""

    action: str  # "put" or "delete"
    namespace: tuple[str, ...]
    key: str
    previous_value: dict[str, Any] | None  # value before this op (in branch ns)
    previous_existed: bool  # whether a branch entry existed before


@dataclass
class _BranchState:
    """Per-branch tracking state."""

    branch_id: str
    # Which (original_ns, key) pairs are modified in the branch
    modified_keys: set[tuple[tuple[str, ...], str]] = field(
        default_factory=set
    )
    # Which (original_ns, key) pairs are tombstoned (deleted)
    tombstones: set[tuple[tuple[str, ...], str]] = field(
        default_factory=set
    )
    # Journal for savepoint rollback
    journal: list[_JournalEntry] = field(default_factory=list)
    # Savepoint name → journal position
    savepoint_positions: dict[str, int] = field(default_factory=dict)


class BranchAwareStore(BaseStore, ToolShim):
    """Branch-aware store that wraps a BaseStore with transaction isolation.

    All writes within a transaction are stored in a namespace-prefixed
    area of the underlying store. Reads check the branch first, then
    fall back to the main namespace. On commit, branch data is flushed
    to main. On abort, branch data is deleted.

    Example::

        from langgraph.store.memory import InMemoryStore
        from janus_core.transaction import TransactionCoordinator

        underlying = InMemoryStore()
        store = BranchAwareStore(underlying)
        coordinator = TransactionCoordinator()
        coordinator.register_shim(store)

        txn = coordinator.begin()
        store.put(("users",), "u1", {"name": "Alice"})  # goes to branch
        item = store.get(("users",), "u1")  # reads from branch
        coordinator.commit()  # flushed to main
        item = underlying.get(("users",), "u1")  # now visible in main
    """

    __slots__ = ("_underlying", "_branches", "_lock", "_active_branch_id")

    def __init__(self, underlying: BaseStore):
        self._underlying = underlying
        self._branches: dict[str, _BranchState] = {}
        self._lock = threading.Lock()
        self._active_branch_id: str | None = None

    @property
    def shim_id(self) -> str:
        return "branch_store"

    @property
    def underlying(self) -> BaseStore:
        return self._underlying

    # ── ToolShim interface ───────────────────────────────────────────

    def begin(self, txn: TransactionHandle) -> None:
        bid = str(txn.branch_id)
        with self._lock:
            self._branches[bid] = _BranchState(branch_id=bid)
            self._active_branch_id = bid

    def prepare(self, txn: TransactionHandle) -> Vote:
        """Optimistic conflict detection: check if any key in the write
        set was modified in the main store since the snapshot timestamp."""
        bid = str(txn.branch_id)
        bs = self._branches.get(bid)
        if bs is None:
            return Vote.ABORT
        # For now: always vote COMMIT (optimistic).
        # Full conflict detection can compare item.updated_at vs txn.snapshot_ts.
        return Vote.COMMIT

    def commit(self, txn: TransactionHandle) -> None:
        bid = str(txn.branch_id)
        bs = self._branches.get(bid)
        if bs is None:
            return

        # Flush modified keys to main namespace
        ops: list[Op] = []
        for ns, key in bs.modified_keys:
            if (ns, key) in bs.tombstones:
                continue
            branch_item = self._underlying.batch(
                [GetOp(namespace=_branch_ns(bid, ns), key=key)]
            )[0]
            if branch_item is not None and not _is_tombstone(branch_item):
                ops.append(PutOp(namespace=ns, key=key, value=branch_item.value))

        # Apply deletes (tombstones) to main
        for ns, key in bs.tombstones:
            ops.append(PutOp(namespace=ns, key=key, value=None))

        if ops:
            self._underlying.batch(ops)

        # Clean up branch namespace entries
        self._delete_branch_entries(bid, bs)
        with self._lock:
            self._branches.pop(bid, None)
            if self._active_branch_id == bid:
                self._active_branch_id = None

        logger.info(
            "BranchStore committed for branch %s (%d modified, %d deleted)",
            bid,
            len(bs.modified_keys),
            len(bs.tombstones),
        )

    def abort(self, txn: TransactionHandle) -> None:
        bid = str(txn.branch_id)
        bs = self._branches.get(bid)
        if bs is None:
            return

        self._delete_branch_entries(bid, bs)
        with self._lock:
            self._branches.pop(bid, None)
            if self._active_branch_id == bid:
                self._active_branch_id = None

    def savepoint(self, txn: TransactionHandle, sp: Savepoint) -> Any:
        bid = str(txn.branch_id)
        bs = self._branches.get(bid)
        if bs is None:
            return None

        pos = len(bs.journal)
        bs.savepoint_positions[sp.name] = pos
        return pos

    def rollback_to_savepoint(
        self, txn: TransactionHandle, sp: Savepoint
    ) -> None:
        bid = str(txn.branch_id)
        bs = self._branches.get(bid)
        if bs is None:
            return

        snapshot_data = sp.shim_snapshots.get(self.shim_id)
        if snapshot_data is None:
            journal_pos = bs.savepoint_positions.get(sp.name, 0)
        else:
            journal_pos = snapshot_data

        # Undo journal entries in reverse from current position to savepoint
        entries_to_undo = bs.journal[journal_pos:]
        for entry in reversed(entries_to_undo):
            branch_key_ns = _branch_ns(bid, entry.namespace)
            if entry.previous_existed:
                # Restore previous value in branch namespace
                self._underlying.batch(
                    [
                        PutOp(
                            namespace=branch_key_ns,
                            key=entry.key,
                            value=entry.previous_value,
                        )
                    ]
                )
            else:
                # Key didn't exist before → delete from branch namespace
                self._underlying.batch(
                    [
                        PutOp(
                            namespace=branch_key_ns,
                            key=entry.key,
                            value=None,
                        )
                    ]
                )
            # Update tracking sets
            nk = (entry.namespace, entry.key)
            if entry.action == "put":
                if not entry.previous_existed:
                    bs.modified_keys.discard(nk)
                bs.tombstones.discard(nk)
            elif entry.action == "delete":
                bs.tombstones.discard(nk)
                if not entry.previous_existed:
                    bs.modified_keys.discard(nk)

        # Truncate journal
        bs.journal = bs.journal[:journal_pos]

        # Remove savepoints after this one
        to_remove = [
            name
            for name, pos in bs.savepoint_positions.items()
            if pos > journal_pos
        ]
        for name in to_remove:
            del bs.savepoint_positions[name]

    def get_changes(self, txn: TransactionHandle) -> list[ChangeRecord]:
        bid = str(txn.branch_id)
        bs = self._branches.get(bid)
        if bs is None:
            return []

        changes: list[ChangeRecord] = []
        for ns, key in bs.modified_keys:
            if (ns, key) in bs.tombstones:
                changes.append(
                    ChangeRecord(
                        shim_id=self.shim_id,
                        resource_id=f"{'.'.join(ns)}/{key}",
                        change_type=ChangeType.DELETE,
                    )
                )
            else:
                # Check if key existed in main to determine create vs update
                main_item = self._underlying.batch(
                    [GetOp(namespace=ns, key=key)]
                )[0]
                ct = (
                    ChangeType.UPDATE
                    if main_item is not None
                    else ChangeType.CREATE
                )
                changes.append(
                    ChangeRecord(
                        shim_id=self.shim_id,
                        resource_id=f"{'.'.join(ns)}/{key}",
                        change_type=ct,
                    )
                )
        for ns, key in bs.tombstones:
            if (ns, key) not in bs.modified_keys:
                changes.append(
                    ChangeRecord(
                        shim_id=self.shim_id,
                        resource_id=f"{'.'.join(ns)}/{key}",
                        change_type=ChangeType.DELETE,
                    )
                )
        return changes

    # ── BaseStore interface ──────────────────────────────────────────

    def batch(self, ops: Iterable[Op]) -> list[Result]:
        bid = self._active_branch_id
        if bid is None:
            # No active branch — pass through
            return self._underlying.batch(ops)

        bs = self._branches.get(bid)
        if bs is None:
            return self._underlying.batch(ops)

        results: list[Result] = []
        for op in ops:
            if isinstance(op, GetOp):
                results.append(self._branch_get(bid, bs, op))
            elif isinstance(op, PutOp):
                self._branch_put(bid, bs, op)
                results.append(None)
            elif isinstance(op, SearchOp):
                results.append(self._branch_search(bid, bs, op))
            elif isinstance(op, ListNamespacesOp):
                results.append(self._branch_list_namespaces(bid, bs, op))
            else:
                # Unknown op — pass through
                results.append(self._underlying.batch([op])[0])
        return results

    async def abatch(self, ops: Iterable[Op]) -> list[Result]:
        # Sync delegation — sufficient for store shim
        return self.batch(ops)

    # ── Branch operation implementations ─────────────────────────────

    def _branch_get(
        self, bid: str, bs: _BranchState, op: GetOp
    ) -> Item | None:
        nk = (op.namespace, op.key)

        # Check tombstone
        if nk in bs.tombstones:
            return None

        # Check branch namespace first
        if nk in bs.modified_keys:
            branch_item = self._underlying.batch(
                [GetOp(namespace=_branch_ns(bid, op.namespace), key=op.key)]
            )[0]
            if branch_item is not None and not _is_tombstone(branch_item):
                # Return item with corrected namespace
                return Item(
                    namespace=op.namespace,
                    key=branch_item.key,
                    value=branch_item.value,
                    created_at=branch_item.created_at,
                    updated_at=branch_item.updated_at,
                )
            return None

        # Fall back to main
        return self._underlying.batch([op])[0]

    def _branch_put(
        self, bid: str, bs: _BranchState, op: PutOp
    ) -> None:
        nk = (op.namespace, op.key)

        # Record journal entry for savepoint rollback
        previous_value: dict | None = None
        previous_existed = nk in bs.modified_keys or nk in bs.tombstones
        if previous_existed:
            prev_item = self._underlying.batch(
                [GetOp(namespace=_branch_ns(bid, op.namespace), key=op.key)]
            )[0]
            previous_value = prev_item.value if prev_item else None

        if op.value is None:
            # Delete operation — record tombstone
            self._underlying.batch(
                [
                    PutOp(
                        namespace=_branch_ns(bid, op.namespace),
                        key=op.key,
                        value=_TOMBSTONE_VALUE,
                    )
                ]
            )
            bs.tombstones.add(nk)
            bs.modified_keys.add(nk)
            bs.journal.append(
                _JournalEntry(
                    action="delete",
                    namespace=op.namespace,
                    key=op.key,
                    previous_value=previous_value,
                    previous_existed=previous_existed,
                )
            )
        else:
            # Write to branch namespace
            self._underlying.batch(
                [
                    PutOp(
                        namespace=_branch_ns(bid, op.namespace),
                        key=op.key,
                        value=op.value,
                        index=op.index if hasattr(op, "index") else None,
                    )
                ]
            )
            bs.modified_keys.add(nk)
            bs.tombstones.discard(nk)
            bs.journal.append(
                _JournalEntry(
                    action="put",
                    namespace=op.namespace,
                    key=op.key,
                    previous_value=previous_value,
                    previous_existed=previous_existed,
                )
            )

    def _branch_search(
        self, bid: str, bs: _BranchState, op: SearchOp
    ) -> list[SearchItem]:
        # Search main namespace
        main_results: list[SearchItem] = self._underlying.batch([op])[0]  # type: ignore

        # Search branch namespace
        branch_prefix = _branch_ns(bid, op.namespace_prefix)
        branch_op = SearchOp(
            namespace_prefix=branch_prefix,
            filter=op.filter,
            limit=op.limit,
            offset=0,  # We'll handle offset in merge
            query=op.query,
            refresh_ttl=op.refresh_ttl,
        )
        branch_results: list[SearchItem] = self._underlying.batch(
            [branch_op]
        )[0]  # type: ignore

        # Build a map of branch items with corrected namespaces
        branch_map: dict[tuple[tuple[str, ...], str], SearchItem] = {}
        for item in branch_results:
            corrected_ns = _strip_branch_ns(bid, item.namespace)
            if not _is_tombstone_search(item):
                branch_map[(corrected_ns, item.key)] = SearchItem(
                    namespace=corrected_ns,
                    key=item.key,
                    value=item.value,
                    created_at=item.created_at,
                    updated_at=item.updated_at,
                    score=item.score,
                )

        # Merge: branch overrides main, tombstoned keys excluded
        merged_map: dict[tuple[tuple[str, ...], str], SearchItem] = {}

        for item in main_results:
            nk = (item.namespace, item.key)
            if nk in bs.tombstones:
                continue  # Deleted in branch
            if nk in branch_map:
                merged_map[nk] = branch_map[nk]  # Branch overrides
            else:
                merged_map[nk] = item

        # Add branch-only items (created in branch)
        for nk, item in branch_map.items():
            if nk not in merged_map:
                merged_map[nk] = item

        # Sort by score (desc) if scores exist, else by updated_at
        merged_list = list(merged_map.values())
        merged_list.sort(
            key=lambda x: (x.score if x.score is not None else 0.0),
            reverse=True,
        )

        # Apply offset and limit
        start = op.offset
        end = start + op.limit
        return merged_list[start:end]

    def _branch_list_namespaces(
        self,
        bid: str,
        bs: _BranchState,
        op: ListNamespacesOp,
    ) -> list[tuple[str, ...]]:
        # Get main namespaces
        main_ns: list[tuple[str, ...]] = self._underlying.batch([op])[0]  # type: ignore

        # Get branch namespaces and strip prefix
        branch_ns_set: set[tuple[str, ...]] = set()
        for ns, _key in bs.modified_keys:
            branch_ns_set.add(ns)

        # Merge
        all_ns = set(main_ns) | branch_ns_set
        return sorted(all_ns)[op.offset : op.offset + op.limit]

    # ── Helpers ──────────────────────────────────────────────────────

    def _delete_branch_entries(self, bid: str, bs: _BranchState) -> None:
        """Delete all entries in the branch namespace."""
        ops: list[Op] = []
        all_keys = bs.modified_keys | bs.tombstones
        for ns, key in all_keys:
            ops.append(
                PutOp(
                    namespace=_branch_ns(bid, ns),
                    key=key,
                    value=None,
                )
            )
        if ops:
            self._underlying.batch(ops)


def _is_tombstone_search(item: SearchItem) -> bool:
    """Check if a search item is a tombstone."""
    return isinstance(item.value, dict) and item.value.get("__tombstone__") is True
