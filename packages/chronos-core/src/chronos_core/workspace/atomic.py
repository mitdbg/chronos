"""Public types for selective multi-store interval transactions.

The transaction itself is owned by Chronos's interval metadata plane.  A
workspace adds no branch head, generation, manifest, writer lease, or result
catalog above the native branch and segment tables.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Literal

from chronos_core.branching import BranchingError, MergePreview, RowDiff


class AtomicMergeError(BranchingError):
    """Base error for a multi-store branch transaction failure."""


class StaleAtomicMergePreviewError(AtomicMergeError):
    """Raised when a reviewed preview no longer names current branch heads."""


@dataclass(frozen=True)
class WorkspaceBranchToken:
    """The native Chronos interval head captured for a branch."""

    branch_id: str
    current_segment_id: int


@dataclass(frozen=True)
class MergeSelection:
    """An allow-list of globally unique store change IDs."""

    change_ids: frozenset[str] = field(default_factory=frozenset)

    @classmethod
    def from_ids(cls, values: Sequence[str]) -> MergeSelection:
        return cls(frozenset(str(value) for value in values))


@dataclass(frozen=True)
class AtomicMergePreview:
    source: str
    target: str
    preview_token: str
    source_token: WorkspaceBranchToken
    target_token: WorkspaceBranchToken
    stores: dict[str, MergePreview]

    @property
    def change_ids(self) -> frozenset[str]:
        return frozenset(
            change.change_id
            for preview in self.stores.values()
            for change in (*preview.changes, *preview.conflicts)
            if change.change_id is not None
        )


@dataclass(frozen=True)
class AtomicMergeResult:
    operation_id: str
    source: str
    target: str
    status: Literal["committed", "noop", "aborted"]
    source_token: WorkspaceBranchToken
    old_target_token: WorkspaceBranchToken
    new_target_token: WorkspaceBranchToken
    selected: int
    skipped: int
    stores: dict[str, int] = field(default_factory=dict)


def atomic_change_id(store: str, change: RowDiff) -> str:
    payload = json.dumps(
        {
            "store": store,
            "table": change.table,
            "key": change.key,
            "change": change.change,
            "before": change.before,
            "after": change.after,
            "conflict_id": change.conflict_id,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return f"{store}:{hashlib.sha256(payload).hexdigest()[:24]}"
