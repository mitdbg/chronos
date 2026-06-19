from __future__ import annotations

from typing import Any, Iterable, Protocol, Sequence

from chronos_core.branching._common import _IntervalSegment, _TableMeta


class IntervalDataPlane(Protocol):
    """Capabilities required by the shared interval branching algorithm.

    SQL row stores, column stores, search indexes, and vector stores can all use
    interval branching when they can filter by a branch point, maintain physical
    row versions, and build useful secondary indexes over visibility metadata.
    """

    dialect: str

    def table_defs(self, table: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Return logical columns and store-native column definitions."""

    def create_interval_table(
        self,
        physical: str,
        column_defs: Sequence[str],
        pk_columns: Sequence[str],
    ) -> None:
        """Create a physical interval table/collection/index."""

    def create_secondary_index(
        self,
        physical: str,
        columns: Sequence[str],
        name: str | None = None,
    ) -> None:
        """Create a store-native index for branch-visible filtering."""

    def visible_rows(
        self,
        meta: _TableMeta,
        segment: _IntervalSegment,
        columns: Sequence[str] | None = None,
        predicate: Any | None = None,
    ) -> Iterable[dict[str, Any]]:
        """Return rows visible at ``segment.branch_point``."""

    def insert_interval_rows(
        self,
        meta: _TableMeta,
        rows: Iterable[dict[str, Any]],
        live_lo: int,
        live_hi: int,
        deleted: bool,
        writer_segment_id: int,
    ) -> None:
        """Insert physical row versions for an interval."""

    def delete_interval_rows(
        self,
        meta: _TableMeta,
        keys: Iterable[dict[str, Any]],
        live_lo: int | None = None,
    ) -> None:
        """Delete physical row versions by logical key and optional interval."""


class IntervalMetadataPlane(Protocol):
    """Branch metadata operations shared by all interval data planes."""

    dialect: str

    def ensure(self) -> None:
        """Create branch, segment, checkpoint, and registry metadata."""

    def allocate_segment_ids(self, count: int) -> list[int]:
        """Allocate globally unique interval segment IDs."""

    def lock_branch(self, branch_id: str) -> None:
        """Serialize metadata mutations for one mutable branch when supported."""

    def commit(self) -> None:
        """Commit metadata changes."""

    def rollback(self) -> None:
        """Roll back metadata changes."""

