from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from chronos_core.branching import BranchSession, ChronosBranchContext


@dataclass
class ChronosPostgresStore:
    """Workspace store for Postgres-backed interval data.

    With no separate metadata URL this is the existing single-Postgres interval
    backend. Supplying ``metadata_url`` uses Postgres for both data and metadata
    through the split-store path.
    """

    data_url: str
    metadata_url: str | None = None
    autocommit: bool = True
    ensure_metadata: bool = True
    interval_continuation_percent: int = 5
    interval_child_width: int | None = None
    interval_allocation_strategy: str = "adaptive"

    def __post_init__(self) -> None:
        if self.metadata_url is None:
            self.context = ChronosBranchContext.connect(
                self.data_url,
                backend="interval",
                autocommit=self.autocommit,
                ensure_metadata=self.ensure_metadata,
                interval_continuation_percent=self.interval_continuation_percent,
                interval_child_width=self.interval_child_width,
                interval_allocation_strategy=self.interval_allocation_strategy,  # type: ignore[arg-type]
            )
        else:
            self.context = ChronosBranchContext.connect_split(
                self.data_url,
                self.metadata_url,
                backend="interval",
                autocommit=self.autocommit,
                ensure_metadata=self.ensure_metadata,
                interval_continuation_percent=self.interval_continuation_percent,
                interval_child_width=self.interval_child_width,
                interval_allocation_strategy=self.interval_allocation_strategy,  # type: ignore[arg-type]
            )

    @property
    def db(self) -> Any:
        return self.context.db

    @property
    def metadata_db(self) -> Any:
        return self.context.metadata_db

    def register_table(self, table: str, primary_key: list[str]) -> None:
        self.context.register_table(table, primary_key)

    def checkout(self, branch_id: str = "main") -> BranchSession:
        return self.context.checkout(branch_id)

    def checkout_checkpoint(self, checkpoint: str) -> BranchSession:
        return self.context.checkout_checkpoint(checkpoint)

    def create_branch(
        self,
        branch_id: str,
        from_branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.context.create_branch(branch_id, from_branch=from_branch, metadata=metadata)

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
        self.context.create_branch_from_checkpoint(branch_id, checkpoint)

    def delete_branch(self, branch_id: str) -> None:
        self.context.delete_branch(branch_id)

    def create_checkpoint(
        self,
        checkpoint: str,
        branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        return self.context.create_checkpoint(checkpoint, branch=branch, metadata=metadata)

    def diff(self, left: str, right: str) -> Any:
        return self.context.diff(left, right)

    def merge_apply(self, source: str, target: str) -> Any:
        return self.context.merge_apply(source, target)

    def close(self) -> None:
        self.context.close()


@dataclass
class ChronosDuckDBStore:
    """Workspace store for DuckDB interval data with row-store metadata."""

    data_url: str
    metadata_url: str
    autocommit: bool = True
    ensure_metadata: bool = True
    interval_continuation_percent: int = 5
    interval_child_width: int | None = None
    interval_allocation_strategy: str = "adaptive"

    def __post_init__(self) -> None:
        self.context = ChronosBranchContext.connect_split(
            self.data_url,
            self.metadata_url,
            backend="interval",
            autocommit=self.autocommit,
            ensure_metadata=self.ensure_metadata,
            interval_continuation_percent=self.interval_continuation_percent,
            interval_child_width=self.interval_child_width,
            interval_allocation_strategy=self.interval_allocation_strategy,  # type: ignore[arg-type]
            enable_schema_branching=False,
        )

    @property
    def db(self) -> Any:
        return self.context.db

    @property
    def metadata_db(self) -> Any:
        return self.context.metadata_db

    def register_table(self, table: str, primary_key: list[str]) -> None:
        self.context.register_table(table, primary_key)

    def checkout(self, branch_id: str = "main") -> BranchSession:
        return self.context.checkout(branch_id)

    def checkout_checkpoint(self, checkpoint: str) -> BranchSession:
        return self.context.checkout_checkpoint(checkpoint)

    def create_branch(
        self,
        branch_id: str,
        from_branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.context.create_branch(branch_id, from_branch=from_branch, metadata=metadata)

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
        self.context.create_branch_from_checkpoint(branch_id, checkpoint)

    def delete_branch(self, branch_id: str) -> None:
        self.context.delete_branch(branch_id)

    def create_checkpoint(
        self,
        checkpoint: str,
        branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        return self.context.create_checkpoint(checkpoint, branch=branch, metadata=metadata)

    def diff(self, left: str, right: str) -> Any:
        return self.context.diff(left, right)

    def merge_apply(self, source: str, target: str) -> Any:
        return self.context.merge_apply(source, target)

    def close(self) -> None:
        self.context.close()
