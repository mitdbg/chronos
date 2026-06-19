from __future__ import annotations

import contextlib
from dataclasses import dataclass
from typing import Any, Iterator, Protocol

from chronos_core.branching import BranchSession
from chronos_core.workspace.filesystem import (
    ChronosFilesystemStore,
    FilesystemBranchSession,
    FilesystemDiff,
)


class BranchStore(Protocol):
    def create_branch(
        self,
        branch_id: str,
        from_branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> None: ...

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None: ...

    def delete_branch(self, branch_id: str) -> None: ...

    def checkout(self, branch_id: str = "main") -> BranchSession: ...

    def checkout_checkpoint(self, checkpoint: str) -> BranchSession: ...

    def create_checkpoint(
        self,
        checkpoint: str,
        branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> Any: ...

    def diff(self, left: str, right: str) -> Any: ...

    def merge_apply(self, source: str, target: str) -> Any: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class WorkspaceBranchSession:
    branch_id: str
    fs: FilesystemBranchSession | None = None
    stores: dict[str, BranchSession] | None = None

    def __getattr__(self, name: str) -> BranchSession:
        stores = self.stores or {}
        if name in stores:
            return stores[name]
        raise AttributeError(name)

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        with contextlib.ExitStack() as stack:
            for session in (self.stores or {}).values():
                stack.enter_context(session.transaction())
            yield


class ChronosWorkspaceContext:
    """Additive multi-store branch facade.

    SQL stores are passed by system name, e.g. ``postgresql=...`` or
    ``duckdb=...``. This class coordinates the same branch names across named
    data stores and the optional filesystem store for sandboxed workspace
    execution.
    """

    def __init__(
        self,
        filesystem: ChronosFilesystemStore | None = None,
        stores: dict[str, BranchStore] | None = None,
        **named_stores: BranchStore,
    ):
        if stores and named_stores:
            duplicate_names = sorted(set(stores) & set(named_stores))
            if duplicate_names:
                joined = ", ".join(duplicate_names)
                raise ValueError(f"duplicate workspace store names: {joined}")
        all_stores = dict(stores or {})
        all_stores.update(named_stores)
        if filesystem is None and not all_stores:
            raise ValueError("at least one workspace store is required")
        reserved_session_attrs = {
            "branch_id",
            "fs",
            "stores",
            "transaction",
        }
        reserved_session_attrs.update(dir(WorkspaceBranchSession))
        conflicts = sorted(set(all_stores) & reserved_session_attrs)
        if conflicts:
            joined = ", ".join(conflicts)
            raise ValueError(f"workspace store names conflict with session attributes: {joined}")
        self.filesystem = filesystem
        self.stores = all_stores

    def create_branch(
        self,
        branch_id: str,
        from_branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        created_filesystem = False
        created_stores: list[str] = []
        try:
            if self.filesystem is not None:
                self.filesystem.create_branch(
                    branch_id,
                    from_branch=from_branch,
                    metadata=metadata,
                )
                created_filesystem = True
            for name, store in self.stores.items():
                store.create_branch(
                    branch_id,
                    from_branch=from_branch,
                    metadata=metadata,
                )
                created_stores.append(name)
        except Exception:
            for name in reversed(created_stores):
                with contextlib.suppress(Exception):
                    self.stores[name].delete_branch(branch_id)
            if created_filesystem and self.filesystem is not None:
                with contextlib.suppress(Exception):
                    self.filesystem.delete_branch(branch_id)
            raise

    def create_branch_from_checkpoint(
        self,
        branch_id: str,
        checkpoint: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if self.filesystem is not None:
            self.filesystem.create_branch_from_checkpoint(
                branch_id,
                checkpoint,
                metadata=metadata,
            )
        for store in self.stores.values():
            store.create_branch_from_checkpoint(branch_id, checkpoint)

    def delete_branch(self, branch_id: str) -> None:
        errors: list[Exception] = []
        if self.filesystem is not None:
            try:
                self.filesystem.delete_branch(branch_id)
            except Exception as exc:
                errors.append(exc)
        for store in self.stores.values():
            try:
                store.delete_branch(branch_id)
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise errors[0]

    def checkout(self, branch_id: str = "main") -> WorkspaceBranchSession:
        fs = self.filesystem.checkout(branch_id) if self.filesystem is not None else None
        stores = {
            name: store.checkout(branch_id)
            for name, store in self.stores.items()
        }
        return WorkspaceBranchSession(branch_id=branch_id, fs=fs, stores=stores)

    def checkout_checkpoint(self, checkpoint: str) -> WorkspaceBranchSession:
        fs = (
            self.filesystem.checkout_checkpoint(checkpoint)
            if self.filesystem is not None
            else None
        )
        stores = {
            name: store.checkout_checkpoint(checkpoint)
            for name, store in self.stores.items()
        }
        return WorkspaceBranchSession(branch_id=checkpoint, fs=fs, stores=stores)

    def create_checkpoint(
        self,
        checkpoint: str,
        branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.filesystem is not None:
            result["filesystem"] = self.filesystem.create_checkpoint(
                checkpoint,
                branch=branch,
                metadata=metadata,
            )
        for name, store in self.stores.items():
            result[name] = store.create_checkpoint(
                checkpoint,
                branch=branch,
                metadata=metadata,
            )
        return result

    def diff(self, left: str, right: str) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.filesystem is not None:
            result["filesystem"] = self.filesystem.diff(left, right)
        for name, store in self.stores.items():
            result[name] = store.diff(left, right)
        return result

    def merge_apply(self, source: str, target: str) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.filesystem is not None:
            result["filesystem"] = self.filesystem.merge_apply(source, target)
        for name, store in self.stores.items():
            result[name] = store.merge_apply(source, target)
        return result

    def close(self) -> None:
        if self.filesystem is not None:
            self.filesystem.close()
        for store in self.stores.values():
            store.close()


__all__ = [
    "ChronosWorkspaceContext",
    "BranchStore",
    "FilesystemDiff",
    "WorkspaceBranchSession",
]
