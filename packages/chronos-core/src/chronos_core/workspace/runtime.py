from __future__ import annotations

import contextlib
import dataclasses
import hashlib
import json
import os
import sys
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Protocol

from chronos_core.branching import (
    BranchingError,
    BranchSession,
    MergePreview,
    MergeResolution,
    RowDiff,
)
from chronos_core.branching._common import (
    MergePolicyInput,
    _normalize_merge_policy,
    _resolve_merge_changes,
)
from chronos_core.workspace.atomic import (
    AtomicMergeError,
    AtomicMergePreview,
    AtomicMergeReservation,
    AtomicMergeResult,
    MergeSelection,
    StaleAtomicMergePreviewError,
    WorkspaceBranchRecord,
    WorkspaceBranchToken,
    WorkspaceMergeCoordinator,
    atomic_change_id,
)
from chronos_core.workspace.filesystem import (
    ChronosFilesystemStore,
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
        atomic_metadata_url: str | None = None,
        atomic_workspace_id: str | None = None,
        atomic_write_wait_timeout: float = 30.0,
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
        self._atomic: WorkspaceMergeCoordinator | None = None
        if atomic_metadata_url is not None:
            workspace_id = atomic_workspace_id or hashlib.sha256(
                repr(self._merge_lock_namespace).encode("utf-8")
            ).hexdigest()[:24]
            self._atomic = WorkspaceMergeCoordinator(
                atomic_metadata_url,
                workspace_id=workspace_id,
                write_wait_timeout=atomic_write_wait_timeout,
            )
            self._atomic.ensure_branch("main", self._identity_manifest("main"))
            self._recover_atomic_merges()

    @property
    def atomic_merge_enabled(self) -> bool:
        return self._atomic is not None

    def _store_map(self) -> dict[str, Any]:
        result = dict(self.stores)
        if self.filesystem is not None:
            result["filesystem"] = self.filesystem
        return result

    @contextlib.contextmanager
    def branch_write(self, branch_id: str) -> Iterator[None]:
        """Coordinate an ordinary branch mutation with atomic publication.

        Applications that opt into ``merge_atomic`` should wrap logical write
        operations in this context. Writers that arrive during staging wait
        for the merge to publish or abort; a reserved merge waits for writers
        that started first.
        """

        if self._atomic is None:
            yield
            return
        writer_id = self._atomic.acquire_writer(branch_id)
        changed = True
        try:
            yield
        finally:
            self._atomic.release_writer(writer_id, branch_id, changed=changed)

    def merge_atomic_preview(
        self,
        source: str,
        target: str,
        *,
        policy: MergePolicyInput = None,
    ) -> AtomicMergePreview:
        if self._atomic is None:
            raise AtomicMergeError(
                "merge_atomic requires atomic_metadata_url on the workspace"
            )
        source_record = self._atomic.branch(source)
        target_record = self._atomic.branch(target)
        previews: dict[str, MergePreview] = {}
        for name, store in self._store_map().items():
            source_branch = source_record.manifest[name]
            target_branch = target_record.manifest[name]
            preview_fn = getattr(store, "merge_preview", None)
            if not callable(preview_fn):
                raise AtomicMergeError(
                    f"workspace store does not support atomic merge preview: {name}"
                )
            lock = self._filesystem_lock if name == "filesystem" else self._store_locks[name]
            with lock:
                _refresh_workspace_store(store)
                raw = preview_fn(source_branch, target_branch, policy=policy)
            changes = [self._atomic_change(name, change) for change in raw.changes]
            conflicts = [self._atomic_change(name, change) for change in raw.conflicts]
            previews[name] = MergePreview(
                source=source_branch,
                target=target_branch,
                changes=changes,
                conflicts=conflicts,
                resolution=raw.resolution,
            )
        token_payload = {
            "source": dataclasses.asdict(source_record.token),
            "target": dataclasses.asdict(target_record.token),
            "stores": {
                name: [
                    change.change_id
                    for change in (*preview.changes, *preview.conflicts)
                ]
                for name, preview in sorted(previews.items())
            },
        }
        preview_token = hashlib.sha256(
            json.dumps(token_payload, sort_keys=True).encode("utf-8")
        ).hexdigest()
        return AtomicMergePreview(
            source,
            target,
            preview_token,
            source_record.token,
            target_record.token,
            previews,
        )

    @staticmethod
    def _atomic_change(store: str, change: RowDiff) -> RowDiff:
        return dataclasses.replace(
            change,
            change_id=atomic_change_id(store, change),
        )

    def merge_atomic(
        self,
        source: str,
        target: str,
        *,
        selection: MergeSelection | None = None,
        preview_token: str | None = None,
        resolution: MergeResolution | dict[str, MergeResolution] | None = None,
        policy: MergePolicyInput = None,
        operation_id: str,
    ) -> AtomicMergeResult:
        if self._atomic is None:
            raise AtomicMergeError(
                "merge_atomic requires atomic_metadata_url on the workspace"
            )
        if not operation_id.strip():
            raise ValueError("operation_id must not be empty")
        completed = self._completed_atomic_result(operation_id, source, target)
        if completed is not None:
            return completed

        with self._merge_lock(target):
            completed = self._completed_atomic_result(operation_id, source, target)
            if completed is not None:
                return completed
            preview = self.merge_atomic_preview(source, target, policy=policy)
            if preview_token is not None and preview_token != preview.preview_token:
                raise StaleAtomicMergePreviewError(
                    "source or target changed after atomic merge preview"
                )
            selected_ids = (
                set(preview.change_ids)
                if selection is None
                else set(selection.change_ids)
            )
            unknown = selected_ids - set(preview.change_ids)
            if unknown:
                raise AtomicMergeError(
                    "atomic merge selection contains unknown or stale changes: "
                    + ", ".join(sorted(unknown))
                )
            self._validate_filesystem_selection(preview, selected_ids)
            resolved: dict[str, list[RowDiff]] = {}
            for name, store_preview in preview.stores.items():
                filtered = MergePreview(
                    source=store_preview.source,
                    target=store_preview.target,
                    changes=[
                        change
                        for change in store_preview.changes
                        if change.change_id in selected_ids
                    ],
                    conflicts=[
                        change
                        for change in store_preview.conflicts
                        if change.change_id in selected_ids
                    ],
                    resolution=store_preview.resolution,
                )
                store_resolution = self._resolution_for_store(
                    name,
                    resolution,
                    filtered,
                )
                resolved[name] = _resolve_merge_changes(
                    filtered,
                    policy,
                    store_resolution,
                    backend=f"workspace.{name}",
                )

            skipped = len(preview.change_ids) - len(selected_ids)
            if not selected_ids:
                result = AtomicMergeResult(
                    operation_id,
                    source,
                    target,
                    "noop",
                    preview.source_token,
                    preview.target_token,
                    preview.target_token,
                    0,
                    skipped,
                    {},
                )
                self._atomic.record_result(
                    operation_id,
                    "noop",
                    self._atomic_result_dict(result),
                    branch_id=target,
                )
                return result

            suffix = hashlib.sha256(
                f"{self._atomic.workspace_id}:{operation_id}".encode()
            ).hexdigest()[:16]
            staging_manifest = dict(self._atomic.branch(target).manifest)
            for name, changes in resolved.items():
                if changes:
                    staging_manifest[name] = f"__chronos_atomic_{suffix}_{name}"
            reservation = self._atomic.reserve(
                operation_id,
                self._atomic.branch(source),
                self._atomic.branch(target),
                preview_token=preview.preview_token,
                staging_manifest=staging_manifest,
            )
            staged: list[str] = []
            applied: dict[str, int] = {}
            publication_attempted = False
            try:
                for name, changes in resolved.items():
                    if not changes:
                        continue
                    store = self._store_map()[name]
                    stage_branch = staging_manifest[name]
                    target_branch = reservation.target.manifest[name]
                    lock = (
                        self._filesystem_lock
                        if name == "filesystem"
                        else self._store_locks[name]
                    )
                    with lock:
                        _refresh_workspace_store(store)
                        staged.append(name)
                        store.create_branch(stage_branch, from_branch=target_branch)
                        applied[name] = self._stage_atomic_changes(
                            name,
                            store,
                            reservation.source.manifest[name],
                            stage_branch,
                            changes,
                        )
                pending_result = AtomicMergeResult(
                    operation_id,
                    source,
                    target,
                    "committed",
                    reservation.source.token,
                    reservation.target.token,
                    WorkspaceBranchToken(
                        target,
                        reservation.target.generation + 1,
                        hashlib.sha256(
                            json.dumps(staging_manifest, sort_keys=True).encode("utf-8")
                        ).hexdigest(),
                    ),
                    len(selected_ids),
                    skipped,
                    applied,
                )
                result_dict = self._atomic_result_dict(pending_result)
                publication_attempted = True
                published = self._atomic.publish(reservation, result_dict)
                return dataclasses.replace(
                    pending_result,
                    new_target_token=published.token,
                )
            except Exception as exc:
                if publication_attempted:
                    try:
                        completed_after_publish = self._atomic.completed_result(
                            operation_id
                        )
                        target_after_publish = self._atomic.branch(target)
                    except Exception:  # noqa: BLE001 - commit outcome is uncertain
                        # The commit outcome is uncertain. Leave successors and
                        # the reservation intact for idempotent recovery.
                        raise exc
                    if (
                        completed_after_publish is not None
                        and completed_after_publish.get("status") == "committed"
                    ):
                        return self._atomic_result_from_dict(
                            completed_after_publish
                        )
                    if (
                        target_after_publish.generation
                        == reservation.target.generation + 1
                        and target_after_publish.manifest
                        == reservation.staging_manifest
                    ):
                        return dataclasses.replace(
                            pending_result,
                            new_target_token=target_after_publish.token,
                        )
                self._cleanup_atomic_staging(reservation, staged)
                self._atomic.abort(
                    reservation,
                    {
                        "operation_id": operation_id,
                        "source": source,
                        "target": target,
                        "status": "aborted",
                        "error": str(exc),
                    },
                )
                raise

    def _completed_atomic_result(
        self,
        operation_id: str,
        source: str,
        target: str,
    ) -> AtomicMergeResult | None:
        assert self._atomic is not None
        completed = self._atomic.completed_result(operation_id)
        if completed is None:
            return None
        if completed.get("source") != source or completed.get("target") != target:
            raise AtomicMergeError(
                "atomic merge operation_id was already used for a different merge"
            )
        if completed.get("status") == "aborted":
            raise AtomicMergeError(
                str(completed.get("error") or "atomic merge operation was aborted")
            )
        return self._atomic_result_from_dict(completed)

    def _stage_atomic_changes(
        self,
        name: str,
        store: Any,
        source_branch: str,
        stage_branch: str,
        changes: list[RowDiff],
    ) -> int:
        if name == "filesystem":
            return self._stage_filesystem_changes(
                store,
                source_branch,
                stage_branch,
                changes,
            )
        target_session = store.checkout(stage_branch)
        if hasattr(target_session, "upsert_many") and hasattr(target_session, "delete_many"):
            by_collection: dict[str, list[RowDiff]] = {}
            for change in changes:
                by_collection.setdefault(change.table, []).append(change)
            applied = 0
            with target_session.transaction():
                for collection, collection_changes in by_collection.items():
                    upserts = []
                    deletes = []
                    for change in collection_changes:
                        if change.after is None:
                            deletes.append(str(change.key["id"]))
                        else:
                            from chronos_core.workspace.qdrant import QdrantUpsert

                            upserts.append(
                                QdrantUpsert(
                                    id=str(change.key["id"]),
                                    vector=change.after["vector"],
                                    payload=change.after["payload"],
                                )
                            )
                    if upserts:
                        target_session.upsert_many(collection, upserts)
                    if deletes:
                        target_session.delete_many(collection, deletes)
                    applied += len(collection_changes)
            return applied

        pending_upserts: dict[str, list[dict[str, Any]]] = {}
        pending_deletes: dict[str, list[dict[str, Any]]] = {}
        for change in changes:
            if change.after is None:
                pending_deletes.setdefault(change.table, []).append(dict(change.key))
            else:
                pending_upserts.setdefault(change.table, []).append(dict(change.after))
        with target_session.transaction():
            for table, keys in pending_deletes.items():
                target_session.delete_keys(table, keys)
            for table, rows in pending_upserts.items():
                target_session.upsert_rows(table, rows)
        return len(changes)

    def _stage_filesystem_changes(
        self,
        store: Any,
        source_branch: str,
        stage_branch: str,
        changes: list[RowDiff],
    ) -> int:
        source = store.checkout(source_branch)
        target = store.checkout(stage_branch)
        paths = sorted(
            {
                str(change.key.get("path"))
                for change in changes
                if change.key.get("path") not in {None, "<unknown>", "/"}
            }
        )
        deleted_paths = [path for path in paths if not source.exists(path)]
        for path in sorted(
            deleted_paths,
            key=lambda value: (value.count("/"), value),
            reverse=True,
        ):
            if target.exists(path):
                stat = target.stat(path)
                if stat.kind == "directory":
                    target.rmdir(path)
                else:
                    target.unlink(path)
        for path in sorted(
            set(paths) - set(deleted_paths),
            key=lambda value: (value.count("/"), value),
        ):
            stat = source.stat(path)
            if stat.kind == "directory":
                if not target.exists(path):
                    target.mkdir(path, mode=stat.mode, parents=True)
                else:
                    target.chmod(path, stat.mode)
            elif stat.kind == "symlink":
                if target.exists(path):
                    existing = target.stat(path)
                    if existing.kind == "directory":
                        target.rmdir(path)
                    else:
                        target.unlink(path)
                target.symlink(source.readlink(path), path, parents=True)
            else:
                target.write_file(
                    path,
                    source.read_file(path),
                    mode=stat.mode,
                    parents=True,
                )
        return len(changes)

    @staticmethod
    def _validate_filesystem_selection(
        preview: AtomicMergePreview,
        selected: set[str],
    ) -> None:
        """Require whole-path selection because staging copies complete paths."""

        filesystem = preview.stores.get("filesystem")
        if filesystem is None:
            return
        by_path: dict[str, set[str]] = {}
        for change in (*filesystem.changes, *filesystem.conflicts):
            path = str(change.key.get("path", ""))
            if path and change.change_id is not None:
                by_path.setdefault(path, set()).add(change.change_id)
        missing: set[str] = set()
        for bundle in by_path.values():
            if bundle & selected and not bundle <= selected:
                missing.update(bundle - selected)
        if missing:
            raise AtomicMergeError(
                "atomic filesystem selection must include every change for a path; "
                "missing: " + ", ".join(sorted(missing))
            )

    def _cleanup_atomic_staging(
        self,
        reservation: AtomicMergeReservation,
        stores: list[str],
    ) -> None:
        for name in reversed(stores):
            store = self._store_map()[name]
            lock = (
                self._filesystem_lock
                if name == "filesystem"
                else self._store_locks[name]
            )
            with lock, contextlib.suppress(Exception):
                _refresh_workspace_store(store)
                store.delete_branch(reservation.staging_manifest[name])

    def _recover_atomic_merges(self) -> None:
        if self._atomic is None:
            return
        for reservation in self._atomic.abandoned():
            staged = [
                name
                for name, branch in reservation.staging_manifest.items()
                if branch != reservation.target.manifest.get(name)
            ]
            self._cleanup_atomic_staging(reservation, staged)
            self._atomic.abort(
                reservation,
                {
                    "operation_id": reservation.operation_id,
                    "source": reservation.source.branch_id,
                    "target": reservation.target.branch_id,
                    "status": "aborted",
                    "error": "recovery aborted unpublished atomic merge",
                },
            )

    @staticmethod
    def _atomic_result_dict(result: AtomicMergeResult) -> dict[str, Any]:
        return dataclasses.asdict(result)

    @staticmethod
    def _atomic_result_from_dict(value: dict[str, Any]) -> AtomicMergeResult:
        def token(name: str) -> WorkspaceBranchToken:
            raw = value[name]
            return WorkspaceBranchToken(
                str(raw["branch_id"]),
                int(raw["generation"]),
                str(raw["manifest_digest"]),
            )

        return AtomicMergeResult(
            str(value["operation_id"]),
            str(value["source"]),
            str(value["target"]),
            str(value["status"]),  # type: ignore[arg-type]
            token("source_token"),
            token("old_target_token"),
            token("new_target_token"),
            int(value["selected"]),
            int(value["skipped"]),
            {str(k): int(v) for k, v in dict(value.get("stores") or {}).items()},
        )

    def _identity_manifest(self, branch_id: str) -> dict[str, str]:
        return {name: branch_id for name in self._store_map()}

    def _branch_record(self, branch_id: str) -> WorkspaceBranchRecord:
        if self._atomic is None:
            return WorkspaceBranchRecord(branch_id, 0, self._identity_manifest(branch_id))
        return self._atomic.branch(branch_id)

    def resolve_branch(self, store: str, branch_id: str) -> str:
        record = self._branch_record(branch_id)
        try:
            return record.manifest[store]
        except KeyError as exc:
            raise AtomicMergeError(
                f"workspace branch {branch_id!r} has no participant {store!r}"
            ) from exc

    def list_branches(self) -> list[str]:
        if self._atomic is not None:
            return self._atomic.list_branches()
        first = next(iter(self._store_map().values()), None)
        if first is None:
            return []
        branches = getattr(first, "branches", None)
        if branches is not None and not callable(branches):
            return sorted(str(value) for value in branches)
        context = getattr(first, "context", first)
        return [info.branch_id for info in context.list_branches()]

    def create_branch(
        self,
        branch_id: str,
        from_branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if branch_id.startswith("__chronos_atomic_"):
            raise ValueError("branch name uses the reserved atomic-merge prefix")
        parent_record = self._branch_record(from_branch)
        created_filesystem = False
        created_stores: list[str] = []
        try:
            if self.filesystem is not None:
                self.filesystem.create_branch(
                    branch_id,
                    from_branch=parent_record.manifest["filesystem"],
                    metadata=metadata,
                )
                created_filesystem = True
            for name, store in self.stores.items():
                store.create_branch(
                    branch_id,
                    from_branch=parent_record.manifest[name],
                    metadata=metadata,
                )
                created_stores.append(name)
            if self._atomic is not None:
                self._atomic.create_branch(branch_id, self._identity_manifest(branch_id))
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
        if branch_id.startswith("__chronos_atomic_"):
            raise ValueError("branch name uses the reserved atomic-merge prefix")
        created_filesystem = False
        created_stores: list[str] = []
        try:
            if self.filesystem is not None:
                self.filesystem.create_branch_from_checkpoint(
                    branch_id,
                    checkpoint,
                    metadata=metadata,
                )
                created_filesystem = True
            for name, store in self.stores.items():
                store.create_branch_from_checkpoint(
                    branch_id,
                    checkpoint,
                )
                created_stores.append(name)
            if self._atomic is not None:
                self._atomic.create_branch(branch_id, self._identity_manifest(branch_id))
        except Exception:
            for name in reversed(created_stores):
                with contextlib.suppress(Exception):
                    self.stores[name].delete_branch(branch_id)
            if created_filesystem and self.filesystem is not None:
                with contextlib.suppress(Exception):
                    self.filesystem.delete_branch(branch_id)
            raise

    def delete_branch(self, branch_id: str) -> None:
        with self.branch_write(branch_id):
            record = self._branch_record(branch_id)
            errors: list[Exception] = []
            if self.filesystem is not None:
                try:
                    self.filesystem.delete_branch(record.manifest["filesystem"])
                except Exception as exc:
                    errors.append(exc)
            for name, store in self.stores.items():
                try:
                    store.delete_branch(record.manifest[name])
                except Exception as exc:
                    errors.append(exc)
            if not errors and self._atomic is not None:
                self._atomic.delete_branch(branch_id)
            if errors:
                raise errors[0]

    def checkout(self, branch_id: str = "main") -> WorkspaceBranchSession:
        record = self._branch_record(branch_id)
        fs = (
            _SynchronizedSession(
                self.filesystem.checkout(record.manifest["filesystem"]),
                self._filesystem_lock,
            )
            if self.filesystem is not None else None
        )
        stores = {
            name: _SynchronizedSession(
                store.checkout(record.manifest[name]),
                self._store_locks[name],
            )
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
        record = self._branch_record(branch)
        result: dict[str, Any] = {}
        if self.filesystem is not None:
            result["filesystem"] = self.filesystem.create_checkpoint(
                checkpoint,
                branch=record.manifest["filesystem"],
                metadata=metadata,
            )
        for name, store in self.stores.items():
            result[name] = store.create_checkpoint(
                checkpoint,
                branch=record.manifest[name],
                metadata=metadata,
            )
        return result

    def diff(self, left: str, right: str) -> dict[str, Any]:
        left_record = self._branch_record(left)
        right_record = self._branch_record(right)
        result: dict[str, Any] = {}
        if self.filesystem is not None:
            result["filesystem"] = self.filesystem.diff(
                left_record.manifest["filesystem"],
                right_record.manifest["filesystem"],
            )
        for name, store in self.stores.items():
            result[name] = store.diff(
                left_record.manifest[name],
                right_record.manifest[name],
            )
        return result

    def merge_preview(
        self,
        source: str,
        target: str,
        *,
        policy: MergePolicyInput = None,
    ) -> dict[str, Any]:
        if self._atomic is not None:
            return self.merge_atomic_preview(source, target, policy=policy).stores
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
        if self._atomic is not None:
            raise AtomicMergeError(
                "merge_apply is unsafe for an atomic workspace; use merge_atomic"
            )
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
        if self._atomic is not None:
            self._atomic.close()


__all__ = [
    "AtomicMergePreview",
    "AtomicMergeResult",
    "BranchStore",
    "ChronosWorkspaceContext",
    "FilesystemDiff",
    "MergeSelection",
    "WorkspaceBranchSession",
]
