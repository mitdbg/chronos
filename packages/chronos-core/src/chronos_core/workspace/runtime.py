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
from pathlib import Path
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
    AtomicMergeResult,
    MergeSelection,
    StaleAtomicMergePreviewError,
    WorkspaceBranchToken,
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
        payload = json.dumps(
            {
                "chronos_workspace_profile": stage,
                "source": source,
                "target": target,
                "elapsed_ms": elapsed_ms,
                "pid": os.getpid(),
            },
            sort_keys=True,
        )
        profile_path = os.environ.get("CHRONOS_WORKSPACE_PROFILE_FILE")
        if profile_path:
            # O_APPEND makes each short JSON record an independent append when
            # many replay processes profile the same workspace concurrently.
            fd = os.open(
                profile_path,
                os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                0o644,
            )
            try:
                os.write(fd, (payload + "\n").encode("utf-8"))
            finally:
                os.close(fd)
        else:
            print(payload, file=sys.stderr, flush=True)


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
            sessions = sorted(
                (self.stores or {}).values(),
                key=lambda session: getattr(
                    session,
                    "_workspace_transaction_priority",
                    0,
                ),
            )
            for session in sessions:
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


def _database_url_identity(value: str) -> str:
    raw = str(value)
    prefix = "sqlite:///"
    if raw.startswith(prefix):
        return prefix + str(Path(raw[len(prefix) :]).expanduser().resolve())
    return raw


def _refresh_workspace_store(store: Any) -> None:
    context = getattr(store, "context", store)
    if hasattr(context, "_metadata_epoch"):
        context._metadata_epoch += 1
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


def _refresh_workspace_branch(store: Any, branch_id: str) -> None:
    _refresh_workspace_store(store)
    refresh_branch = getattr(store, "refresh_branch", None)
    if callable(refresh_branch):
        refresh_branch(branch_id)


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
        shared_metadata_url: str | None = None,
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
            raise ValueError(
                f"workspace store names conflict with session attributes: {joined}"
            )
        self.filesystem = filesystem
        self.stores = all_stores
        filesystem_context = getattr(filesystem, "context", filesystem)
        shared_context_lock: threading.RLock | None = None
        if filesystem is not None:
            for store in all_stores.values():
                if getattr(store, "context", store) is filesystem_context:
                    # ChronosFS and relational interval rows can share one
                    # native branch store.  Use one process-local lock for
                    # both facades so their calls cannot concurrently use the
                    # same native database connection.
                    shared_context_lock = _global_store_lock(
                        ("shared-context", _workspace_store_identity(filesystem))
                    )
                    break
        self._filesystem_lock = (
            shared_context_lock
            or (
                _global_store_lock(
                    ("filesystem", _workspace_store_identity(filesystem))
                )
                if filesystem is not None
                else threading.RLock()
            )
        )
        self._store_locks = {}
        for name, store in all_stores.items():
            if (
                shared_context_lock is not None
                and getattr(store, "context", store) is filesystem_context
            ):
                self._store_locks[name] = shared_context_lock
            else:
                self._store_locks[name] = _global_store_lock(
                    ("store", _workspace_store_identity(store))
                )
        self._merge_lock_namespace = self._merge_namespace()
        self._atomic_control: Any | None = None
        if shared_metadata_url is not None:
            expected = _database_url_identity(shared_metadata_url)
            participant_contexts: list[Any] = []
            for name, store in self._store_map().items():
                context = getattr(store, "context", store)
                metadata_db = getattr(context, "metadata_db", None)
                identity = _database_identity(metadata_db)
                if _database_url_identity(identity) != expected:
                    raise ValueError(
                        f"atomic workspace participant {name!r} does not use "
                        "the shared interval metadata plane"
                    )
                participant_contexts.append(context)
            if not participant_contexts:
                raise ValueError("atomic workspace requires an interval participant")
            self._atomic_control = participant_contexts[0]

    @property
    def atomic_merge_enabled(self) -> bool:
        return self._atomic_control is not None

    def _store_map(self) -> dict[str, Any]:
        result = dict(self.stores)
        if self.filesystem is not None:
            result["filesystem"] = self.filesystem
        return result

    @contextlib.contextmanager
    def branch_write(self, branch_id: str) -> Iterator[None]:
        """Compatibility context; Chronos interval writes enforce exclusion."""

        del branch_id
        yield

    def _branch_token(self, branch_id: str) -> WorkspaceBranchToken:
        if self._atomic_control is None:
            raise AtomicMergeError("atomic merge is not enabled")
        info = self._atomic_control.get_branch(branch_id)
        return WorkspaceBranchToken(branch_id, int(info.current_ref))

    def merge_atomic_preview(
        self,
        source: str,
        target: str,
        *,
        policy: MergePolicyInput = None,
    ) -> AtomicMergePreview:
        if self._atomic_control is None:
            raise AtomicMergeError(
                "merge_atomic requires shared_metadata_url on the workspace"
            )
        source_token = self._branch_token(source)
        target_token = self._branch_token(target)
        previews: dict[str, MergePreview] = {}
        for name, store in self._store_map().items():
            preview_fn = getattr(store, "merge_preview", None)
            if not callable(preview_fn):
                raise AtomicMergeError(
                    f"workspace store does not support atomic merge preview: {name}"
                )
            lock = (
                self._filesystem_lock
                if name == "filesystem"
                else self._store_locks[name]
            )
            with lock:
                with _profile_merge_stage(f"preview.refresh:{name}", source, target):
                    _refresh_workspace_store(store)
                with _profile_merge_stage(f"preview.store:{name}", source, target):
                    raw = preview_fn(source, target, policy=policy)
            changes = [self._atomic_change(name, change) for change in raw.changes]
            conflicts = [self._atomic_change(name, change) for change in raw.conflicts]
            previews[name] = MergePreview(
                source=source,
                target=target,
                changes=changes,
                conflicts=conflicts,
                resolution=raw.resolution,
            )
        token_payload = {
            "source": dataclasses.asdict(source_token),
            "target": dataclasses.asdict(target_token),
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
            source_token,
            target_token,
            previews,
        )

    @staticmethod
    def _atomic_change(store: str, change: RowDiff) -> RowDiff:
        return dataclasses.replace(
            change,
            change_id=atomic_change_id(store, change),
        )

    def _resolve_atomic_selection(
        self,
        preview: AtomicMergePreview,
        selection: MergeSelection | None,
        resolution: MergeResolution | dict[str, MergeResolution] | None,
        policy: MergePolicyInput,
    ) -> tuple[set[str], dict[str, list[RowDiff]], int]:
        selected_ids = (
            set(preview.change_ids) if selection is None else set(selection.change_ids)
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
        return selected_ids, resolved, len(preview.change_ids) - len(selected_ids)

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
        _prepared_preview: AtomicMergePreview | None = None,
        _allow_stable_selection_rebase: bool = False,
    ) -> AtomicMergeResult:
        if self._atomic_control is None:
            raise AtomicMergeError(
                "merge_atomic requires shared_metadata_url on the workspace"
            )
        if not operation_id.strip():
            raise ValueError("operation_id must not be empty")
        with _profile_merge_stage("atomic.preview", source, target):
            preview = _prepared_preview or self.merge_atomic_preview(
                source,
                target,
                policy=policy,
            )
        if preview.source != source or preview.target != target:
            raise AtomicMergeError(
                "prepared atomic preview does not match the merge branches"
            )
        requested_ids = (
            set(preview.change_ids) if selection is None else set(selection.change_ids)
        )
        if preview_token is not None and preview_token != preview.preview_token:
            stable_selection = (
                _allow_stable_selection_rebase
                and selection is not None
                and requested_ids <= preview.change_ids
            )
            if not stable_selection:
                raise StaleAtomicMergePreviewError(
                    "source or target changed after atomic merge preview"
                )
        with _profile_merge_stage("atomic.resolve", source, target):
            selected_ids, resolved, skipped = self._resolve_atomic_selection(
                preview,
                selection,
                resolution,
                policy,
            )
        if not selected_ids:
            return AtomicMergeResult(
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

        participants = [name for name, changes in resolved.items() if changes]
        with self._merge_lock(target, source=source):
            with _profile_merge_stage("atomic.reserve", source, target):
                transaction = self._atomic_control.reserve_branch_transaction(
                    source,
                    target,
                    participants,
                    metadata={
                        "operation_id": operation_id,
                        "preview_token": preview.preview_token,
                    },
                )
            applied: dict[str, int] = {}
            publication_attempted = False
            try:
                # Reservation blocks new native writers. Recompute once under
                # that guard so a writer that committed after the reviewed
                # preview but before reservation cannot slip through merely
                # because ordinary DML kept the same mutable segment head.
                with _profile_merge_stage("atomic.revalidate", source, target):
                    reserved_preview = self.merge_atomic_preview(
                        source,
                        target,
                        policy=policy,
                    )
                if reserved_preview.preview_token != preview.preview_token:
                    stable_selection = (
                        _allow_stable_selection_rebase
                        and selection is not None
                        and selected_ids <= reserved_preview.change_ids
                        and preview.change_ids == reserved_preview.change_ids
                    )
                    if not stable_selection:
                        raise StaleAtomicMergePreviewError(
                            "source or target changed while reserving the merge"
                        )
                    with _profile_merge_stage("atomic.resolve_rebased", source, target):
                        selected_ids, resolved, skipped = (
                            self._resolve_atomic_selection(
                                reserved_preview,
                                selection,
                                resolution,
                                policy,
                            )
                        )
                    preview = reserved_preview
                for name, changes in resolved.items():
                    if not changes:
                        continue
                    store = self._store_map()[name]
                    lock = (
                        self._filesystem_lock
                        if name == "filesystem"
                        else self._store_locks[name]
                    )
                    with lock:
                        with _profile_merge_stage(
                            f"stage.refresh:{name}", source, target
                        ):
                            _refresh_workspace_store(store)
                        with _profile_merge_stage(
                            f"stage.store:{name}", source, target
                        ):
                            stage = getattr(
                                store,
                                "stage_branch_transaction_changes",
                                None,
                            )
                            context = getattr(store, "context", store)
                            if store is not context and callable(stage):
                                applied[name] = int(
                                    stage(transaction, source, target, changes)
                                )
                            else:
                                applied[name] = int(
                                    context.stage_branch_transaction_changes(
                                        transaction, changes
                                    )
                                )
                pending_result = AtomicMergeResult(
                    operation_id,
                    source,
                    target,
                    "committed",
                    preview.source_token,
                    preview.target_token,
                    WorkspaceBranchToken(target, transaction.continuation_segment_id),
                    len(selected_ids),
                    skipped,
                    applied,
                )
                publication_attempted = True
                with _profile_merge_stage("atomic.publish", source, target):
                    self._atomic_control.publish_branch_transaction(transaction)
                for name, store in self._store_map().items():
                    with _profile_merge_stage(
                        f"publish.refresh:{name}", source, target
                    ):
                        _refresh_workspace_store(store)
                        refresh_branch = getattr(store, "refresh_branch", None)
                        if callable(refresh_branch):
                            refresh_branch(target)
                return pending_result
            except Exception as exc:
                if publication_attempted:
                    try:
                        target_after_publish = self._branch_token(target)
                    except Exception:  # noqa: BLE001 - commit outcome is uncertain
                        raise exc
                    if target_after_publish.current_segment_id == (
                        transaction.continuation_segment_id
                    ):
                        return pending_result
                self._atomic_control.abort_branch_transaction(transaction)
                raise

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

    def resolve_branch(self, store: str, branch_id: str) -> str:
        if store not in self._store_map():
            raise AtomicMergeError(
                f"workspace branch {branch_id!r} has no participant {store!r}"
            )
        return branch_id

    def list_branches(self) -> list[str]:
        if self._atomic_control is not None:
            return [info.branch_id for info in self._atomic_control.list_branches()]
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
        if self._atomic_control is not None:
            self._atomic_control.create_branch(
                branch_id,
                from_branch=from_branch,
                metadata=metadata,
            )
            for store in self._store_map().values():
                _refresh_workspace_branch(store, branch_id)
            return
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
        if branch_id.startswith("__chronos_atomic_"):
            raise ValueError("branch name uses the reserved atomic-merge prefix")
        if self._atomic_control is not None:
            self._atomic_control.create_branch_from_checkpoint(branch_id, checkpoint)
            for store in self._store_map().values():
                _refresh_workspace_store(store)
            return
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
        except Exception:
            for name in reversed(created_stores):
                with contextlib.suppress(Exception):
                    self.stores[name].delete_branch(branch_id)
            if created_filesystem and self.filesystem is not None:
                with contextlib.suppress(Exception):
                    self.filesystem.delete_branch(branch_id)
            raise

    def delete_branch(self, branch_id: str) -> None:
        if self._atomic_control is not None:
            self._atomic_control.delete_branch(branch_id)
            for store in self._store_map().values():
                _refresh_workspace_branch(store, branch_id)
            return
        with self.branch_write(branch_id):
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
        if self._atomic_control is not None:
            # Capture the live branch interval once. Every participant then
            # prepares its data-plane session from that same reference instead
            # of independently rereading the shared branch head. The lock only
            # protects local checkout construction from local publication; it
            # is not held while the returned writable session is used.
            with self._merge_lock(branch_id):
                control_session = self._atomic_control.checkout(branch_id)
                current_ref = control_session.current_ref
                fs = self._checkout_shared_filesystem(
                    branch_id,
                    current_ref,
                    control_session,
                )
                stores = {
                    name: _SynchronizedSession(
                        self._checkout_shared_store(
                            store,
                            branch_id,
                            current_ref,
                            control_session,
                        ),
                        self._store_locks[name],
                    )
                    for name, store in self.stores.items()
                }
                return WorkspaceBranchSession(
                    branch_id=branch_id,
                    fs=fs,
                    stores=stores,
                )
        fs = (
            _SynchronizedSession(
                self.filesystem.checkout(branch_id),
                self._filesystem_lock,
            )
            if self.filesystem is not None
            else None
        )
        stores = {
            name: _SynchronizedSession(
                store.checkout(branch_id),
                self._store_locks[name],
            )
            for name, store in self.stores.items()
        }
        return WorkspaceBranchSession(branch_id=branch_id, fs=fs, stores=stores)

    def _checkout_shared_filesystem(
        self,
        branch_id: str,
        current_ref: str,
        control_session: BranchSession,
    ) -> _SynchronizedSession | None:
        if self.filesystem is None:
            return None
        checkout_ref = getattr(self.filesystem, "checkout_ref", None)
        if not callable(checkout_ref):
            raise AtomicMergeError(
                "atomic workspace filesystem does not support shared checkout"
            )
        return _SynchronizedSession(
            checkout_ref(
                branch_id,
                current_ref,
                control_session=control_session,
            ),
            self._filesystem_lock,
        )

    def _checkout_shared_store(
        self,
        store: Any,
        branch_id: str,
        current_ref: str,
        control_session: BranchSession,
    ) -> Any:
        if store is self._atomic_control:
            return control_session
        if getattr(store, "context", None) is self._atomic_control:
            checkout_control = getattr(store, "checkout_control", None)
            if callable(checkout_control):
                return checkout_control(control_session)
        checkout_ref = getattr(store, "checkout_ref", None)
        if callable(checkout_ref):
            return checkout_ref(branch_id, current_ref)
        raise AtomicMergeError(
            "atomic workspace store does not support shared checkout: "
            f"{type(store).__module__}.{type(store).__qualname__}"
        )

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
        if self._atomic_control is not None:
            info = self._atomic_control.create_checkpoint(
                checkpoint,
                branch=branch,
                metadata=metadata,
            )
            return {name: info for name in self._store_map()}
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
        if self._atomic_control is not None:
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
        if self._atomic_control is not None:
            raise AtomicMergeError(
                "merge_apply is unsafe for an atomic workspace; use merge_atomic"
            )
        with self._merge_lock(target, source=source):
            return self._merge_apply_locked(
                source,
                target,
                resolution,
                policy=policy,
            )

    @contextlib.contextmanager
    def _merge_lock(
        self,
        target: str,
        *,
        source: str | None = None,
    ) -> Iterator[None]:
        key = (self._merge_lock_namespace, target)
        with _GLOBAL_MERGE_LOCKS_GUARD:
            lock = _GLOBAL_MERGE_LOCKS.get(key)
            if lock is None:
                lock = threading.Lock()
                _GLOBAL_MERGE_LOCKS[key] = lock
        profile_source = target if source is None else source
        with _profile_merge_stage("merge.lock_wait", profile_source, target):
            lock.acquire()
        try:
            with _profile_merge_stage("merge.lock_held", profile_source, target):
                yield
        finally:
            lock.release()

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
                        result["filesystem"] = self.filesystem.merge_apply(
                            source, target
                        )
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
            if (
                policy is not None
                or resolution is not None
                or not native_conflict_check
            ):
                preview = getattr(self.filesystem, "merge_preview", None)
                if callable(preview):
                    with self._filesystem_lock:
                        with _profile_merge_stage("preview:filesystem", source, target):
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
    "AtomicMergePreview",
    "AtomicMergeResult",
    "BranchStore",
    "ChronosWorkspaceContext",
    "FilesystemDiff",
    "MergeSelection",
    "WorkspaceBranchSession",
]
