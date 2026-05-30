from __future__ import annotations

import contextlib
from dataclasses import dataclass
from typing import Any, Iterator

from chronos_core.branching import BranchSession, ChronosBranchContext
from chronos_core.workspace.filesystem import (
    ChronosFilesystemStore,
    FilesystemBranchSession,
    FilesystemDiff,
)


@dataclass(frozen=True)
class WorkspaceBranchSession:
    branch_id: str
    sql: BranchSession | None = None
    fs: FilesystemBranchSession | None = None

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        if self.sql is None:
            yield
            return
        with self.sql.transaction():
            yield


class ChronosWorkspaceContext:
    """Additive multi-store branch facade.

    Existing relational callers should continue to use ChronosBranchContext
    directly. This class coordinates the same branch names across relational
    and filesystem stores for sandboxed workspace execution.
    """

    def __init__(
        self,
        relational: ChronosBranchContext | None = None,
        filesystem: ChronosFilesystemStore | None = None,
    ):
        if relational is None and filesystem is None:
            raise ValueError("at least one workspace store is required")
        self.relational = relational
        self.filesystem = filesystem

    def create_branch(
        self,
        branch_id: str,
        from_branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        created_relational = False
        created_filesystem = False
        try:
            if self.relational is not None:
                self.relational.create_branch(
                    branch_id,
                    from_branch=from_branch,
                    metadata=metadata,
                )
                created_relational = True
            if self.filesystem is not None:
                self.filesystem.create_branch(
                    branch_id,
                    from_branch=from_branch,
                    metadata=metadata,
                )
                created_filesystem = True
        except Exception:
            if created_filesystem and self.filesystem is not None:
                with contextlib.suppress(Exception):
                    self.filesystem.delete_branch(branch_id)
            if created_relational and self.relational is not None:
                with contextlib.suppress(Exception):
                    self.relational.delete_branch(branch_id)
            raise

    def create_branch_from_checkpoint(
        self,
        branch_id: str,
        checkpoint: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if self.relational is not None:
            self.relational.create_branch_from_checkpoint(branch_id, checkpoint)
        if self.filesystem is not None:
            self.filesystem.create_branch_from_checkpoint(
                branch_id,
                checkpoint,
                metadata=metadata,
            )

    def delete_branch(self, branch_id: str) -> None:
        errors: list[Exception] = []
        if self.filesystem is not None:
            try:
                self.filesystem.delete_branch(branch_id)
            except Exception as exc:
                errors.append(exc)
        if self.relational is not None:
            try:
                self.relational.delete_branch(branch_id)
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise errors[0]

    def checkout(self, branch_id: str = "main") -> WorkspaceBranchSession:
        sql = self.relational.checkout(branch_id) if self.relational is not None else None
        fs = self.filesystem.checkout(branch_id) if self.filesystem is not None else None
        return WorkspaceBranchSession(branch_id=branch_id, sql=sql, fs=fs)

    def checkout_checkpoint(self, checkpoint: str) -> WorkspaceBranchSession:
        sql = (
            self.relational.checkout_checkpoint(checkpoint)
            if self.relational is not None
            else None
        )
        fs = (
            self.filesystem.checkout_checkpoint(checkpoint)
            if self.filesystem is not None
            else None
        )
        return WorkspaceBranchSession(branch_id=checkpoint, sql=sql, fs=fs)

    def create_checkpoint(
        self,
        checkpoint: str,
        branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.relational is not None:
            result["relational"] = self.relational.create_checkpoint(
                checkpoint,
                branch=branch,
                metadata=metadata,
            )
        if self.filesystem is not None:
            result["filesystem"] = self.filesystem.create_checkpoint(
                checkpoint,
                branch=branch,
                metadata=metadata,
            )
        return result

    def diff(self, left: str, right: str) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.relational is not None:
            result["relational"] = self.relational.diff(left, right)
        if self.filesystem is not None:
            result["filesystem"] = self.filesystem.diff(left, right)
        return result

    def merge_apply(self, source: str, target: str) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.relational is not None:
            result["relational"] = self.relational.merge_apply(source, target)
        if self.filesystem is not None:
            result["filesystem"] = self.filesystem.merge_apply(source, target)
        return result

    def close(self) -> None:
        if self.filesystem is not None:
            self.filesystem.close()
        if self.relational is not None:
            self.relational.close()


__all__ = [
    "ChronosWorkspaceContext",
    "FilesystemDiff",
    "WorkspaceBranchSession",
]
