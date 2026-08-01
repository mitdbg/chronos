from __future__ import annotations

import contextlib
import json
import os
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterator, Protocol

from chronos_core.branching import BranchSession, BranchingError, MergeResolution
from chronos_core.branching._common import (
    MergePolicyInput,
    _normalize_merge_policy,
    _resolve_merge_changes,
)
from chronos_core.workspace.filesystem import (
    ChronosFilesystemStore,
    FilesystemBranchSession,
    FilesystemDiff,
)

_GLOBAL_MERGE_LOCKS_GUARD = threading.Lock()
_GLOBAL_MERGE_LOCKS: dict[tuple[tuple[tuple[str, str], ...], str], threading.Lock] = {}
_GLOBAL_STORE_LOCKS_GUARD = threading.Lock()
_GLOBAL_STORE_LOCKS: dict[tuple[str, str], threading.RLock] = {}


@contextlib.contextmanager
def _profile_merge_stage(
    stage: str,
    source: str,
    target: str,
) -> Iterator[None]:
    if os.environ.get("CHRONOS_WORKSPACE_PROFILE") != "1":
        yield
        return
    started = time.perf_counter_ns()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
        print(
            json.dumps(
                {
                    "chronos_workspace_profile": stage,
                    "source": source,
                    "target": target,
                    "elapsed_ms": elapsed_ms,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )


class BranchStore(Protocol):
    def create_branch(
        self,
        branch_id: str,
        from_branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> None: ...

    def create_branch_from_checkpoint(
        self,
        branch_id: str,
        checkpoint: str,
        metadata: dict[str, Any] | None = None,
    ) -> None: ...

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

    def merge_preview(
        self,
        source: str,
        target: str,
        *,
        policy: MergePolicyInput = None,
    ) -> Any: ...

    def merge_apply(
        self,
        source: str,
        target: str,
        resolution: MergeResolution | None = None,
        *,
        policy: MergePolicyInput = None,
    ) -> Any: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class WorkspaceBranchSession:
    branch_id: str
    fs: Any | None = None
    stores: dict[str, Any] | None = None

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


class _SynchronizedSession:
    """Serialize access to a checked-out session that shares one store handle."""

    def __init__(self, session: Any, lock: threading.RLock):
        self._session = session
        self._lock = lock

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._session, name)
        if not callable(attr):
            return attr

        def synchronized(*args: Any, **kwargs: Any) -> Any:
            with self._lock:
                return attr(*args, **kwargs)

        return synchronized

    @property
    def branch_id(self) -> str:
        with self._lock:
            return self._session.branch_id

    @property
    def current_ref(self) -> str:
        with self._lock:
            return self._session.current_ref

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        with self._lock:
            with self._session.transaction():
                yield


def _workspace_store_identity(store: Any) -> str:
    filesystem_url = getattr(store, "_database_url", None)
    if filesystem_url:
        return str(filesystem_url)
    context = getattr(store, "context", store)
    db = getattr(context, "db", None)
    metadata_db = getattr(context, "metadata_db", None)
    db_key = _database_identity(db)
    metadata_key = _database_identity(metadata_db)
    if db_key or metadata_key:
        return "|".join(part for part in (db_key, metadata_key) if part)
    root = getattr(store, "root", None)
    state_dir = getattr(store, "state_dir", None)
    if root or state_dir:
        return f"{root}|{state_dir}"
    return f"{type(store).__module__}.{type(store).__qualname__}:{id(store)}"


def _database_identity(db: Any) -> str:
    if db is None:
        return ""
    data_db = getattr(db, "data_db", None)
    metadata_db = getattr(db, "metadata_db", None)
    if data_db is not None or metadata_db is not None:
        return "|".join(
            part
            for part in (
                _database_identity(data_db),
                _database_identity(metadata_db),
            )
            if part
        )
    database_url = getattr(db, "database_url", None)
    if database_url:
        return str(database_url)
    database_path = getattr(db, "database_path", None)
    if database_path:
        return str(database_path)
    return ""


def _refresh_workspace_store(store: Any) -> None:
    context = getattr(store, "context", store)
    db = getattr(context, "db", None)
    refresh = getattr(db, "refresh_connection", None)
    if callable(refresh):
        refresh()
    backend = getattr(context, "_backend", None)
    refresh_native = getattr(backend, "refresh_native_connections", None)
    if callable(refresh_native):
        refresh_native()
    else:
        invalidate = getattr(backend, "_invalidate_native_branch_sessions", None)
        if callable(invalidate):
            invalidate()


def _global_store_lock(key: tuple[str, str]) -> threading.RLock:
    with _GLOBAL_STORE_LOCKS_GUARD:
        lock = _GLOBAL_STORE_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _GLOBAL_STORE_LOCKS[key] = lock
        return lock


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
        self._filesystem_lock = (
            _global_store_lock(("filesystem", _workspace_store_identity(filesystem)))
            if filesystem is not None
            else threading.RLock()
        )
        self._store_locks = {
            name: _global_store_lock((name, _workspace_store_identity(store)))
            for name, store in all_stores.items()
        }
        self._merge_lock_namespace = self._merge_namespace()

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
        fs = (
            _SynchronizedSession(
                self.filesystem.checkout(branch_id),
                self._filesystem_lock,
            )
            if self.filesystem is not None else None
        )
        stores = {
            name: _SynchronizedSession(store.checkout(branch_id), self._store_locks[name])
            for name, store in self.stores.items()
        }
        return WorkspaceBranchSession(branch_id=branch_id, fs=fs, stores=stores)

    def checkout_checkpoint(self, checkpoint: str) -> WorkspaceBranchSession:
        fs = (
            _SynchronizedSession(
                self.filesystem.checkout_checkpoint(checkpoint),
                self._filesystem_lock,
            )
            if self.filesystem is not None
            else None
        )
        stores = {
            name: _SynchronizedSession(
                store.checkout_checkpoint(checkpoint),
                self._store_locks[name],
            )
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

    def merge_preview(
        self,
        source: str,
        target: str,
        *,
        policy: MergePolicyInput = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.filesystem is not None:
            preview = getattr(self.filesystem, "merge_preview", None)
            if callable(preview):
                with self._filesystem_lock:
                    result["filesystem"] = preview(source, target, policy=policy)
            elif policy is not None:
                raise BranchingError(
                    "policy-aware workspace merge preview is not supported for filesystem stores"
                )
            else:
                result["filesystem"] = self.filesystem.diff(target, source)
        for name, store in self.stores.items():
            preview = getattr(store, "merge_preview", None)
            if preview is None:
                raise BranchingError(
                    f"workspace store does not support merge preview: {name}"
                )
            with self._store_locks[name]:
                _refresh_workspace_store(store)
                result[name] = preview(source, target, policy=policy)
        return result

    def merge_apply(
        self,
        source: str,
        target: str,
        resolution: MergeResolution | dict[str, MergeResolution] | None = None,
        *,
        policy: MergePolicyInput = None,
    ) -> dict[str, Any]:
        with self._merge_lock(target):
            return self._merge_apply_locked(
                source,
                target,
                resolution,
                policy=policy,
            )

    @contextlib.contextmanager
    def _merge_lock(self, target: str) -> Iterator[None]:
        key = (self._merge_lock_namespace, target)
        with _GLOBAL_MERGE_LOCKS_GUARD:
            lock = _GLOBAL_MERGE_LOCKS.get(key)
            if lock is None:
                lock = threading.Lock()
                _GLOBAL_MERGE_LOCKS[key] = lock
        with lock:
            yield

    def _merge_namespace(self) -> tuple[tuple[str, str], ...]:
        members: list[tuple[str, str]] = []
        if self.filesystem is not None:
            members.append(("filesystem", _workspace_store_identity(self.filesystem)))
        for name, store in self.stores.items():
            members.append((name, _workspace_store_identity(store)))
        return tuple(sorted(members))

    def _merge_apply_locked(
        self,
        source: str,
        target: str,
        resolution: MergeResolution | dict[str, MergeResolution] | None = None,
        *,
        policy: MergePolicyInput = None,
    ) -> dict[str, Any]:
        previews = self._preview_stores_for_merge(
            source,
            target,
            policy=policy,
            resolution=resolution,
        )
        self._prevalidate_store_previews(previews, resolution, policy=policy)

        result: dict[str, Any] = {}
        if self.filesystem is not None:
            filesystem_resolution = self._resolution_for_store(
                "filesystem",
                resolution,
                previews.get("filesystem"),
            )
            with self._filesystem_lock:
                with _profile_merge_stage("apply:filesystem", source, target):
                    if filesystem_resolution is None and policy is None:
                        result["filesystem"] = self.filesystem.merge_apply(source, target)
                    else:
                        try:
                            result["filesystem"] = self.filesystem.merge_apply(
                                source,
                                target,
                                filesystem_resolution,
                                policy=policy,
                            )
                        except TypeError as exc:
                            raise BranchingError(
                                "filesystem store does not support policy-aware merge"
                            ) from exc
        for name, store in self.stores.items():
            store_resolution = self._resolution_for_store(
                name,
                resolution,
                previews.get(name),
            )
            with self._store_locks[name]:
                _refresh_workspace_store(store)
                with _profile_merge_stage(f"apply:{name}", source, target):
                    if store_resolution is None and policy is None:
                        result[name] = store.merge_apply(source, target)
                    else:
                        try:
                            result[name] = store.merge_apply(
                                source,
                                target,
                                store_resolution,
                                policy=policy,
                            )
                        except TypeError as exc:
                            raise BranchingError(
                                f"workspace store does not support policy-aware merge: {name}"
                            ) from exc
        return result

    def _preview_stores_for_merge(
        self,
        source: str,
        target: str,
        *,
        policy: MergePolicyInput,
        resolution: MergeResolution | dict[str, MergeResolution] | None,
    ) -> dict[str, Any]:
        previews: dict[str, Any] = {}
        if self.filesystem is not None:
            native_conflict_check = bool(
                getattr(
                    self.filesystem,
                    "native_conflict_checked_merge",
                    False,
                )
            )
            if policy is not None or resolution is not None or not native_conflict_check:
                preview = getattr(self.filesystem, "merge_preview", None)
                if callable(preview):
                    with self._filesystem_lock:
                        with _profile_merge_stage(
                            "preview:filesystem", source, target
                        ):
                            previews["filesystem"] = preview(
                                source,
                                target,
                                policy=policy,
                            )
                elif policy is not None:
                    raise BranchingError(
                        "filesystem store does not support policy-aware merge"
                    )
        for name, store in self.stores.items():
            preview = getattr(store, "merge_preview", None)
            if preview is None:
                if policy is not None:
                    raise BranchingError(
                        f"workspace store does not support policy-aware merge: {name}"
                    )
                continue
            with self._store_locks[name]:
                _refresh_workspace_store(store)
                with _profile_merge_stage(f"preview:{name}", source, target):
                    previews[name] = preview(source, target, policy=policy)
        return previews

    def _prevalidate_store_previews(
        self,
        previews: dict[str, Any],
        resolution: MergeResolution | dict[str, MergeResolution] | None,
        *,
        policy: MergePolicyInput,
    ) -> None:
        normalized = _normalize_merge_policy(policy)
        for name, preview in previews.items():
            store_resolution = self._resolution_for_store(name, resolution, preview)
            try:
                _resolve_merge_changes(
                    preview,
                    normalized,
                    store_resolution,
                    backend=f"workspace.{name}",
                )
            except BranchingError as exc:
                raise BranchingError(
                    f"workspace merge validation failed for store {name}: {exc}"
                ) from exc

    @staticmethod
    def _resolution_for_store(
        name: str,
        resolution: MergeResolution | dict[str, MergeResolution] | None,
        preview: Any | None,
    ) -> MergeResolution | None:
        if resolution is None:
            return None
        if isinstance(resolution, dict):
            return resolution.get(name)
        if preview is None:
            return resolution
        conflict_ids = {
            conflict.conflict_id
            for conflict in getattr(preview, "conflicts", [])
            if conflict.conflict_id is not None
        }
        if not conflict_ids:
            return MergeResolution()
        return MergeResolution(
            {
                conflict_id: choice
                for conflict_id, choice in resolution.conflict_choices.items()
                if conflict_id in conflict_ids
            }
        )

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
