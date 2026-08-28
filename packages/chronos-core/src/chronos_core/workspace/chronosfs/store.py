"""ChronosFS public API backed by native C++ interval storage."""

from __future__ import annotations

import contextlib
import difflib
import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterator, Literal

from chronos_core import _native_interval
from chronos_core.branching import (
    ChronosBranchContext,
    MergePreview,
    MergeResolution,
    MergeResult,
    RowDiff,
)
from chronos_core.branching._common import (
    _BranchRef,
    MergePolicyInput,
    _preview_with_merge_policy,
    _resolve_merge_changes,
)
from chronos_core.branching._runtime import BranchSession


CHRONOSFS_BLOCK_SIZE = 4096
_ROOT_INODE = 1
_MERGE_PREVIEW_STABILITY_RETRIES = 8


class ChronosFSError(Exception):
    """Raised when ChronosFS operations fail."""


@dataclass(frozen=True)
class ChronosFSStat:
    inode_id: int
    kind: Literal["file", "directory", "symlink"]
    mode: int
    uid: int
    gid: int
    size: int
    nlink: int
    symlink_target: str | None = None


@dataclass(frozen=True)
class ChronosFSPathChange:
    path: str
    change: Literal["added", "deleted", "modified"]
    before_kind: str | None = None
    after_kind: str | None = None
    before_hash: str | None = None
    after_hash: str | None = None


@dataclass(frozen=True)
class ChronosFSDiff:
    left: str
    right: str
    changes: list[ChronosFSPathChange]


def _chronosfs_epoch_operation(method: Any) -> Any:
    def wrapped(self: "ChronosFSBranchSession", *args: Any, **kwargs: Any) -> Any:
        if self._control_session is None:
            return method(self, *args, **kwargs)
        with self._control_session._operation_epoch():
            return method(self, *args, **kwargs)

    return wrapped


class ChronosFSBranchSession:
    """Branch-bound convenience facade for workspace checkouts.

    ChronosFS itself stores data in interval tables and normally takes an
    explicit branch id on every operation.  Workspace sessions already carry a
    branch id, so this wrapper binds that id and presents file operations as
    methods on ``session.fs`` without requiring a FUSE mount.
    """

    def __init__(
        self,
        store: "ChronosFSStore",
        branch_id: str,
        control_session: BranchSession | None = None,
    ):
        self._store = store
        self.branch_id = branch_id
        self._control_session = control_session

    @_chronosfs_epoch_operation
    def stat(self, path: str) -> ChronosFSStat:
        return self._store.stat(self.branch_id, path)

    @_chronosfs_epoch_operation
    def exists(self, path: str) -> bool:
        return self._store.exists(self.branch_id, path)

    @_chronosfs_epoch_operation
    def listdir(self, path: str = "/") -> list[str]:
        return self._store.listdir(self.branch_id, path)

    @_chronosfs_epoch_operation
    def mkdir(self, path: str, *, mode: int = 0o755, parents: bool = False) -> int:
        return self._store.mkdir(self.branch_id, path, mode=mode, parents=parents)

    @_chronosfs_epoch_operation
    def write_file(
        self,
        path: str,
        data: bytes | str,
        *,
        mode: int = 0o644,
        parents: bool = False,
    ) -> int:
        return self._store.write_file(
            self.branch_id,
            path,
            data,
            mode=mode,
            parents=parents,
        )

    @_chronosfs_epoch_operation
    def write_files(
        self,
        files: Sequence[tuple[str, bytes | str]],
        *,
        mode: int = 0o644,
        parents: bool = False,
    ) -> None:
        self._store.write_files(
            self.branch_id,
            files,
            mode=mode,
            parents=parents,
        )

    @_chronosfs_epoch_operation
    def import_tree(self, source: str | Path) -> None:
        self._store.import_tree(self.branch_id, source)

    @_chronosfs_epoch_operation
    def write_at(self, path: str, offset: int, data: bytes | str) -> None:
        self._store.write_at(self.branch_id, path, offset, data)

    @_chronosfs_epoch_operation
    def read_file(self, path: str) -> bytes:
        return self._store.read_file(self.branch_id, path)

    @_chronosfs_epoch_operation
    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self._store.read_text(self.branch_id, path, encoding=encoding)

    @_chronosfs_epoch_operation
    def truncate(self, path: str, size: int) -> None:
        self._store.truncate(self.branch_id, path, size)

    @_chronosfs_epoch_operation
    def unlink(self, path: str) -> None:
        self._store.unlink(self.branch_id, path)

    @_chronosfs_epoch_operation
    def rmdir(self, path: str) -> None:
        self._store.rmdir(self.branch_id, path)

    @_chronosfs_epoch_operation
    def rename(self, old_path: str, new_path: str) -> None:
        self._store.rename(self.branch_id, old_path, new_path)

    @_chronosfs_epoch_operation
    def symlink(self, target: str, link_path: str, *, parents: bool = False) -> int:
        return self._store.symlink(self.branch_id, target, link_path, parents=parents)

    @_chronosfs_epoch_operation
    def readlink(self, path: str) -> str:
        return self._store.readlink(self.branch_id, path)

    @_chronosfs_epoch_operation
    def chmod(self, path: str, mode: int) -> None:
        self._store.chmod(self.branch_id, path, mode)


class ChronosFSStore:
    """Filesystem state stored through the native C++ ChronosFS backend."""

    # A plain native merge performs its own conflict check before applying any
    # filesystem rows.  Workspace coordination can therefore prevalidate the
    # other stores without materializing a second filesystem merge preview.
    native_conflict_checked_merge = True

    def __init__(
        self,
        context: ChronosBranchContext,
        *,
        block_size: int = CHRONOSFS_BLOCK_SIZE,
    ):
        if block_size <= 0:
            raise ValueError("block_size must be positive")
        self.context = context
        self.block_size = int(block_size)
        self._workspace_refs: dict[str, str] = {}
        self._database_url = _database_url_for_context(context)
        metadata_url = _database_url_for_adapter(context.metadata_db)
        # A single-database interval context already owns the native branch
        # store used for relational rows.  Reuse it for ChronosFS as well so a
        # worker does not keep a duplicate native PostgreSQL connection.  A
        # genuinely split data/metadata deployment retains the existing
        # independent native store because its data driver may be DuckDB.
        shared_native = None
        if metadata_url == self._database_url:
            backend = getattr(context, "_backend", None)
            shared_native = getattr(backend, "_native_branch_store", None)
        if shared_native is not None:
            self._native = _native_interval.NativeChronosFSStore(
                shared_native,
                self._database_url,
                self.block_size,
            )
            self._borrows_context_native = True
            register_borrower = getattr(
                getattr(context, "_backend", None),
                "register_native_store_borrower",
                None,
            )
            if callable(register_borrower):
                register_borrower()
        else:
            self._native = (
                _native_interval.NativeChronosFSStore(
                    self._database_url,
                    metadata_url,
                    self.block_size,
                )
                if metadata_url != self._database_url
                else _native_interval.NativeChronosFSStore(
                    self._database_url,
                    self.block_size,
                )
            )
            self._borrows_context_native = False

    @classmethod
    def connect(
        cls,
        database_url: str,
        *,
        metadata_url: str | None = None,
        block_size: int = CHRONOSFS_BLOCK_SIZE,
        backend: str = "interval",
        enable_session_epochs: bool = True,
    ) -> "ChronosFSStore":
        ctx = (
            ChronosBranchContext.connect_split(
                database_url,
                metadata_url,
                backend=backend,  # type: ignore[arg-type]
                enable_session_epochs=enable_session_epochs,
            )
            if metadata_url is not None
            else ChronosBranchContext.connect(
                database_url,
                backend=backend,  # type: ignore[arg-type]
                enable_session_epochs=enable_session_epochs,
            )
        )
        return cls(ctx, block_size=block_size)

    def ensure(self) -> None:
        self._native.ensure()
        refresh = getattr(self.context._backend, "refresh_registries", None)
        if callable(refresh):
            refresh()
        index_name = "chronosfs_dirents_by_inode"
        if index_name not in {
            index.name for index in self.context.list_indexes("chronosfs_dirents")
        }:
            self.context.create_index(
                "chronosfs_dirents",
                ["inode_id"],
                index_name,
            )

    def create_root_if_missing(self, branch: str = "main") -> None:
        self.ensure()

    @property
    def branches(self) -> tuple[str, ...]:
        return tuple(self._native.branches())

    def create_branch(
        self,
        branch_id: str,
        from_branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self._flush_mount_writes()
        if metadata:
            self.context.create_branch(
                branch_id, from_branch=from_branch, metadata=metadata
            )
        else:
            self._native.create_branch(branch_id, from_branch)
            self._refresh_context_backend()
        self._clear_cache(branch_id)

    def create_branch_from_checkpoint(
        self,
        branch_id: str,
        checkpoint: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self._flush_mount_writes()
        self.context.create_branch_from_checkpoint(branch_id, checkpoint)
        if metadata:
            self.context.update_branch_metadata(branch_id, metadata)
        self._clear_cache(branch_id)

    def delete_branch(self, branch_id: str) -> None:
        self._flush_mount_writes()
        self._native.delete_branch(branch_id)
        self._refresh_context_backend()
        self._clear_cache(branch_id)

    def create_checkpoint(
        self,
        checkpoint: str,
        branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        self._flush_mount_writes()
        result = self.context.create_checkpoint(
            checkpoint, branch=branch, metadata=metadata
        )
        self._clear_cache(branch)
        return result

    def checkout(self, branch_id: str = "main") -> ChronosFSBranchSession:
        control_session = self.context.checkout(branch_id)
        with control_session._operation_epoch():
            self.stat(branch_id, "/")
        return ChronosFSBranchSession(self, branch_id, control_session)

    def checkout_ref(
        self,
        branch_id: str,
        current_ref: str | int,
        *,
        control_session: Any | None = None,
    ) -> ChronosFSBranchSession:
        """Bind the live filesystem view to a workspace-selected interval."""

        ref = str(current_ref)
        session = control_session or self.context.checkout_ref(branch_id, ref)
        if self._workspace_refs.get(branch_id) != ref:
            # Interval metadata is shared by every workspace participant, so
            # the relational control session already contains the complete
            # filesystem visibility interval. Only fall back to this store's
            # context when checkout_ref is used outside a workspace.
            if session.branch_id != branch_id or session.current_ref != ref:
                raise ChronosFSError(
                    f"invalid shared checkout for {branch_id}:{ref}"
                )
            segment = session._ref.metadata.get("segment")
            checkout_segment = getattr(self._native, "checkout_segment", None)
            if segment is None or not callable(checkout_segment):
                raise ChronosFSError(
                    "native ChronosFS does not support shared interval checkout"
                )
            checkout_segment(
                branch_id,
                int(segment.segment_id),
                str(segment.live_lo),
                str(segment.live_hi),
                str(segment.branch_point),
            )
            self._workspace_refs[branch_id] = ref
        return ChronosFSBranchSession(self, branch_id, session)

    def checkout_checkpoint(self, checkpoint: str) -> ChronosFSBranchSession:
        branch_id = f"checkpoint-{checkpoint}"
        if branch_id not in self.branches:
            self.create_branch_from_checkpoint(branch_id, checkpoint)
        return self.checkout(branch_id)

    def refresh_branch(self, branch_id: str) -> None:
        """Discard cached paths after writes made through a FUSE checkout."""
        self._flush_mount_writes()
        self._workspace_refs.pop(branch_id, None)
        refresh = getattr(self._native, "refresh_branch", None)
        if callable(refresh):
            refresh(branch_id)
        else:
            self._clear_cache(branch_id)

    def _flush_mount_writes(self) -> None:
        # Import lazily to avoid the store/fuse module cycle.
        from chronos_core.workspace.chronosfs.fuse import flush_chronosfs_daemon

        flush_chronosfs_daemon(self)

    def close(self) -> None:
        native = self._native
        if native is None:
            self.context.close()
            return
        native.wait_for_gc()
        # The borrowed NativeBranchStore is owned by the interval context.
        # Destroy the filesystem facade before closing that context so its
        # keep-alive edge never observes a dangling native store.
        if self._borrows_context_native:
            unregister_borrower = getattr(
                getattr(self.context, "_backend", None),
                "unregister_native_store_borrower",
                None,
            )
            if callable(unregister_borrower):
                unregister_borrower()
            self._native = None
            self._borrows_context_native = False
        self.context.close()

    def wait_for_gc(self) -> None:
        """Wait until asynchronous interval and object reclamation settles."""
        native = self._native
        if native is not None:
            native.wait_for_gc()

    def _refresh_context_backend(self) -> None:
        invalidate = getattr(
            self.context._backend, "_invalidate_native_branch_sessions", None
        )
        if callable(invalidate):
            invalidate()

    def stat(self, branch_id: str, path: str) -> ChronosFSStat:
        return _stat_from_native(self._native.stat(branch_id, _normalize_path(path)))

    def stat_inode(self, branch_id: str, inode_id: int) -> ChronosFSStat:
        return _stat_from_native(self._native.stat_inode(branch_id, int(inode_id)))

    def lookup_child(
        self, branch_id: str, parent_inode_id: int, name: str
    ) -> ChronosFSStat:
        return _stat_from_native(
            self._native.lookup_child(
                branch_id, int(parent_inode_id), _check_name(name)
            )
        )

    def exists(self, branch_id: str, path: str) -> bool:
        try:
            return bool(self._native.exists(branch_id, _normalize_path(path)))
        except Exception:
            return False

    def listdir(self, branch_id: str, path: str = "/") -> list[str]:
        return list(self._native.listdir(branch_id, _normalize_path(path)))

    def listdir_inode(self, branch_id: str, inode_id: int) -> list[str]:
        return list(self._native.listdir_inode(branch_id, int(inode_id)))

    def read_inode(self, branch_id: str, inode_id: int) -> bytes:
        stat = self.stat_inode(branch_id, inode_id)
        if stat.kind == "symlink":
            raise ChronosFSError(f"cannot read symlink inode as file: {inode_id}")
        if stat.kind != "file":
            raise ChronosFSError(f"not a file inode: {inode_id}")
        return self.read_inode_range(branch_id, inode_id, 0, stat.size)

    def read_inode_range(
        self,
        branch_id: str,
        inode_id: int,
        offset: int,
        size: int,
    ) -> bytes:
        if offset < 0:
            raise ValueError("offset must be non-negative")
        if size < 0:
            raise ValueError("size must be non-negative")
        if size == 0:
            return b""
        return bytes(
            self._native.read_inode_range(
                branch_id, int(inode_id), int(offset), int(size)
            )
        )

    def write_inode_at(
        self, branch_id: str, inode_id: int, offset: int, data: bytes | str
    ) -> None:
        if offset < 0:
            raise ValueError("offset must be non-negative")
        payload = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        if payload:
            self._native.write_inode_at(branch_id, int(inode_id), int(offset), payload)

    def truncate_inode(self, branch_id: str, inode_id: int, size: int) -> None:
        if size < 0:
            raise ValueError("size must be non-negative")
        self._native.truncate_inode(branch_id, int(inode_id), int(size))

    def chmod_inode(self, branch_id: str, inode_id: int, mode: int) -> None:
        self._native.chmod_inode(branch_id, int(inode_id), int(mode))

    def create_file_at(
        self,
        branch_id: str,
        parent_inode_id: int,
        name: str,
        *,
        mode: int = 0o644,
    ) -> int:
        return int(
            self._native.create_file_at(
                branch_id,
                int(parent_inode_id),
                _check_name(name),
                int(mode),
            )
        )

    def mkdir_at(
        self,
        branch_id: str,
        parent_inode_id: int,
        name: str,
        *,
        mode: int = 0o755,
    ) -> int:
        return int(
            self._native.mkdir_at(
                branch_id,
                int(parent_inode_id),
                _check_name(name),
                int(mode),
            )
        )

    def symlink_at(
        self, branch_id: str, parent_inode_id: int, name: str, target: str
    ) -> int:
        return int(
            self._native.symlink_at(
                branch_id,
                int(parent_inode_id),
                _check_name(name),
                str(target),
            )
        )

    def unlink_at(self, branch_id: str, parent_inode_id: int, name: str) -> None:
        self._native.unlink_at(branch_id, int(parent_inode_id), _check_name(name))

    def rmdir_at(self, branch_id: str, parent_inode_id: int, name: str) -> None:
        self._native.rmdir_at(branch_id, int(parent_inode_id), _check_name(name))

    def rename_at(
        self,
        branch_id: str,
        old_parent_inode_id: int,
        old_name: str,
        new_parent_inode_id: int,
        new_name: str,
    ) -> None:
        self._native.rename_at(
            branch_id,
            int(old_parent_inode_id),
            _check_name(old_name),
            int(new_parent_inode_id),
            _check_name(new_name),
        )

    def mkdir(
        self,
        branch_id: str,
        path: str,
        *,
        mode: int = 0o755,
        parents: bool = False,
    ) -> int:
        return int(
            self._native.mkdir(
                branch_id,
                _normalize_path(path),
                int(mode),
                bool(parents),
            )
        )

    def write_file(
        self,
        branch_id: str,
        path: str,
        data: bytes | str,
        *,
        mode: int = 0o644,
        parents: bool = False,
    ) -> int:
        payload = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        return int(
            self._native.write_file(
                branch_id,
                _normalize_path(path),
                payload,
                int(mode),
                bool(parents),
            )
        )

    def write_files(
        self,
        branch_id: str,
        files: Sequence[tuple[str, bytes | str]],
        *,
        mode: int = 0o644,
        parents: bool = False,
    ) -> None:
        paths: list[str] = []
        payloads: list[bytes] = []
        for path, data in files:
            paths.append(_normalize_path(path))
            payloads.append(
                data.encode("utf-8") if isinstance(data, str) else bytes(data)
            )
        if paths:
            self._native.write_files(
                branch_id,
                paths,
                payloads,
                int(mode),
                bool(parents),
            )

    def import_tree(self, branch_id: str, source: str | Path) -> None:
        source_path = Path(source).expanduser().resolve()
        if not source_path.is_dir():
            raise ChronosFSError(f"import source is not a directory: {source_path}")
        self._native.import_tree(branch_id, str(source_path))

    def write_at(
        self, branch_id: str, path: str, offset: int, data: bytes | str
    ) -> None:
        if offset < 0:
            raise ValueError("offset must be non-negative")
        payload = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        if payload:
            self._native.write_at(
                branch_id, _normalize_path(path), int(offset), payload
            )

    def read_file(self, branch_id: str, path: str) -> bytes:
        return bytes(self._native.read_file(branch_id, _normalize_path(path)))

    def read_text(self, branch_id: str, path: str, encoding: str = "utf-8") -> str:
        return self.read_file(branch_id, path).decode(encoding)

    def truncate(self, branch_id: str, path: str, size: int) -> None:
        if size < 0:
            raise ValueError("size must be non-negative")
        self._native.truncate(branch_id, _normalize_path(path), int(size))

    def unlink(self, branch_id: str, path: str) -> None:
        self._native.unlink(branch_id, _normalize_path(path))

    def rmdir(self, branch_id: str, path: str) -> None:
        self._native.rmdir(branch_id, _normalize_path(path))

    def rename(self, branch_id: str, old_path: str, new_path: str) -> None:
        self._native.rename(
            branch_id, _normalize_path(old_path), _normalize_path(new_path)
        )

    def symlink(
        self,
        branch_id: str,
        target: str,
        link_path: str,
        *,
        parents: bool = False,
    ) -> int:
        return int(
            self._native.symlink(
                branch_id,
                str(target),
                _normalize_path(link_path),
                bool(parents),
            )
        )

    def readlink(self, branch_id: str, path: str) -> str:
        return str(self._native.readlink(branch_id, _normalize_path(path)))

    def chmod(self, branch_id: str, path: str, mode: int) -> None:
        self._native.chmod(branch_id, _normalize_path(path), int(mode))

    def diff(self, left: str, right: str) -> ChronosFSDiff:
        self._flush_mount_writes()
        left_manifest, right_manifest = self._stable_manifests(left, right)
        changes: list[ChronosFSPathChange] = []
        for path in sorted(set(left_manifest) | set(right_manifest)):
            before = left_manifest.get(path)
            after = right_manifest.get(path)
            if before is None and after is not None:
                changes.append(
                    ChronosFSPathChange(
                        path=path,
                        change="added",
                        after_kind=after["kind"],
                        after_hash=after.get("hash"),
                    )
                )
            elif before is not None and after is None:
                changes.append(
                    ChronosFSPathChange(
                        path=path,
                        change="deleted",
                        before_kind=before["kind"],
                        before_hash=before.get("hash"),
                    )
                )
            elif before != after:
                assert before is not None and after is not None
                changes.append(
                    ChronosFSPathChange(
                        path=path,
                        change="modified",
                        before_kind=before["kind"],
                        after_kind=after["kind"],
                        before_hash=before.get("hash"),
                        after_hash=after.get("hash"),
                    )
                )
        return ChronosFSDiff(left=left, right=right, changes=changes)

    def _stable_manifests(
        self,
        left: str,
        right: str,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        """Read two complete trees from branch heads that remain unchanged.

        ``manifest`` uses the direct ChronosFS API for POSIX-like traversal.
        Refreshing both branch sessions before the traversal and validating the
        heads afterwards turns that sequence into a snapshot read.  If a head
        moves while the trees are being walked, discard both trees and retry;
        never return a mixed pair.
        """

        with self._branch_state_lock(left, right):
            for _attempt in range(_MERGE_PREVIEW_STABILITY_RETRIES):
                left_ref = str(self.context.get_branch(left).current_ref)
                right_ref = str(self.context.get_branch(right).current_ref)
                self._refresh_cache_only(left)
                if right != left:
                    self._refresh_cache_only(right)
                left_manifest = self.manifest(left)
                right_manifest = (
                    left_manifest if right == left else self.manifest(right)
                )
                current_left_ref = str(self.context.get_branch(left).current_ref)
                current_right_ref = str(self.context.get_branch(right).current_ref)
                if (left_ref, right_ref) == (current_left_ref, current_right_ref):
                    return left_manifest, right_manifest
        raise ChronosFSError(
            "could not obtain stable filesystem manifests: "
            f"branches changed during {_MERGE_PREVIEW_STABILITY_RETRIES} attempts"
        )

    @contextlib.contextmanager
    def _branch_state_lock(self, left: str, right: str) -> Iterator[None]:
        """Exclude ChronosFS writers while a multi-query read is assembled.

        Ordinary writes keep the branch's mutable segment id, so checking that
        id before and after a read cannot detect an intervening write.  The
        metadata-plane row lock is the authoritative Chronos coordination
        mechanism: PostgreSQL writers acquire a share lock on these rows and
        SQLite uses ``BEGIN IMMEDIATE``.  A preview therefore either sees the
        complete pre-write state or waits and sees the complete post-write
        state; it never combines rows from both.
        """

        db = self.context.metadata_db
        if db.in_transaction:
            raise ChronosFSError(
                "cannot acquire a filesystem snapshot lock inside an active "
                "metadata transaction"
            )
        branches = sorted({left, right})
        try:
            if db.dialect == "sqlite":
                db.execute("BEGIN IMMEDIATE")
            elif db.dialect == "postgres":
                db.begin()
            else:
                raise ChronosFSError(
                    f"filesystem snapshot locking is unsupported for {db.dialect}"
                )
            placeholders = ", ".join("?" for _ in branches)
            suffix = " FOR UPDATE" if db.dialect == "postgres" else ""
            rows = db.execute(
                "SELECT branch_id FROM _chronos_branch_interval_branches "
                f"WHERE branch_id IN ({placeholders}) ORDER BY branch_id{suffix}",
                tuple(branches),
            ).fetchall()
            found = {str(row["branch_id"] if isinstance(row, Mapping) else row[0]) for row in rows}
            missing = [branch for branch in branches if branch not in found]
            if missing:
                raise ChronosFSError(
                    "filesystem snapshot branch not found: " + ", ".join(missing)
                )
            yield
        except Exception:
            if db.in_transaction:
                db.rollback()
            raise
        else:
            db.commit()

    def _refresh_cache_only(self, branch_id: str) -> None:
        self._workspace_refs.pop(branch_id, None)
        refresh = getattr(self._native, "refresh_branch", None)
        if callable(refresh):
            refresh(branch_id)
        else:
            self._clear_cache(branch_id)

    def merge_preview(
        self,
        source: str,
        target: str,
        *,
        policy: MergePolicyInput = None,
    ) -> MergePreview:
        self._flush_mount_writes()
        # ChronosFS stores file data in the normal interval backend tables:
        # chronosfs_inodes, chronosfs_dirents, and chronosfs_file_blocks.  The
        # native preview is therefore already conflict-checked at record
        # granularity.  This method intentionally converts those internal rows
        # into agent-facing file/range conflicts: users should reason about
        # paths, byte ranges, and text diffs rather than inode ids or block
        # indexes.  We preserve the internal conflict ids so merge_apply can
        # still route choices back to the native interval backend.
        with self._branch_state_lock(source, target):
            internal, sessions = self._stable_merge_preview_tables(source, target)
            source_manifest, target_manifest = self._changed_inode_manifests(
                source,
                target,
                (*internal.changes, *internal.conflicts),
                sessions=sessions,
            )
            preview = MergePreview(
                source=source,
                target=target,
                changes=self._public_merge_diffs(
                    internal.changes,
                    source_manifest,
                    target_manifest,
                ),
                conflicts=self._public_merge_diffs(
                    internal.conflicts,
                    source_manifest,
                    target_manifest,
                ),
            )
        return _preview_with_merge_policy(preview, policy, backend="chronosfs")

    def stage_branch_transaction_changes(
        self,
        transaction: Any,
        source: str,
        target: str,
        changes: list[RowDiff],
    ) -> int:
        """Write selected filesystem rows into Chronos's shared merge segment."""

        selected_paths = {
            str(change.key.get("path"))
            for change in changes
            if change.key.get("path") is not None
        }
        with self._branch_state_lock(source, target):
            internal, sessions = self._stable_merge_preview_tables(source, target)
            source_manifest, target_manifest = self._changed_inode_manifests(
                source,
                target,
                (*internal.changes, *internal.conflicts),
                sessions=sessions,
            )
            # Branch-local filesystem writes allocate globally unique inode ids,
            # but two branches can still create the same directory path with
            # different inode ids.  Staging the raw rows for both paths would
            # make the later merge replace the target's directory edge and hide
            # files that were already published there.  Rebase the selected rows
            # onto target directory inodes before writing the shared merge
            # segment.  This keeps the filesystem merge path-based while the
            # underlying store remains block/version oriented.
            source_inode_by_path = {
                "/" + path.lstrip("/"): int(state["inode_id"])
                for path, state in source_manifest.items()
                if state.get("inode_id") is not None
            }
            target_inode_by_path = {
                "/" + path.lstrip("/"): int(state["inode_id"])
                for path, state in target_manifest.items()
                if state.get("inode_id") is not None
            }
            inode_remap = {
                source_inode: target_inode_by_path[path]
                for path, source_inode in source_inode_by_path.items()
                if path in target_inode_by_path
                and source_inode != target_inode_by_path[path]
            }
            target_kinds = {
                "/" + path.lstrip("/"): str(state.get("kind", ""))
                for path, state in target_manifest.items()
            }
            source_kinds = {
                "/" + path.lstrip("/"): str(state.get("kind", ""))
                for path, state in source_manifest.items()
            }
            selected_internal: list[RowDiff] = []
            for change in (*internal.changes, *internal.conflicts):
                path = _public_path_for_internal_diff(
                    change,
                    source_manifest,
                    target_manifest,
                )
                if path == "<unknown>":
                    raise ChronosFSError(
                        "could not resolve a path for a ChronosFS merge row: "
                        f"{change.table} {change.key!r}"
                    )
                # A branch may create and then remove an inode before merge. Its
                # interval rows remain useful storage history, but neither branch
                # head can reach the inode, so it is not a logical filesystem
                # change and must not be staged into the target.
                if path == "<unlinked>":
                    continue
                if path in selected_paths or any(
                    selected.startswith(path.rstrip("/") + "/")
                    for selected in selected_paths
                    if path not in {"", "/", "<unknown>", "<unlinked>"}
                ):
                    source_kind = source_kinds.get(path)
                    target_kind = target_kinds.get(path)
                    # If the target already has this directory, retain its
                    # directory inode and edge.  Child file rows are still
                    # staged below, with the source parent inode remapped to
                    # the target directory.  The same rule applies to an
                    # existing file/symlink edge: content rows update the
                    # target inode, while the source dirent must not replace
                    # that edge.
                    if target_kind and change.table == "chronosfs_inodes":
                        if source_kind == "directory":
                            continue
                    if target_kind and change.table == "chronosfs_dirents":
                        if change.change != "deleted":
                            continue

                    selected_internal.append(
                        _remap_chronosfs_row_diff(change, inode_remap)
                    )
        # The metadata-plane lock protects the preview/path snapshot.  The
        # native staging call must run after releasing it: staging acquires
        # the transaction's own metadata locks, and holding the branch-row
        # lock here would make two connections in this process wait on one
        # another.  The merge reservation remains the write-side guard while
        # the selected rows are copied into the merge transaction.
        applied = self.context.stage_branch_transaction_changes(
            transaction,
            selected_internal,
        )
        self._clear_cache(source)
        self._clear_cache(target)
        return applied

    def _public_merge_diffs(
        self,
        diffs: Sequence[RowDiff],
        source_manifest: dict[str, dict[str, Any]],
        target_manifest: dict[str, dict[str, Any]],
    ) -> list[RowDiff]:
        result: list[RowDiff] = []
        for diff in diffs:
            # The interval backend reports physical rows changed during the
            # branch lifetime. Public filesystem semantics are instead the
            # final reachable trees. Drop rows for inodes absent from both
            # heads before decoding blocks or constructing rich text diffs.
            path = _public_path_for_internal_diff(
                diff,
                source_manifest,
                target_manifest,
            )
            if path == "<unknown>":
                raise ChronosFSError(
                    "could not resolve a path for a ChronosFS merge row: "
                    f"{diff.table} {diff.key!r}"
                )
            if path == "<unlinked>":
                continue
            result.append(
                self._public_row_diff(
                    diff,
                    source_manifest,
                    target_manifest,
                )
            )
        return result

    def _changed_inode_manifests(
        self,
        source: str,
        target: str,
        diffs: Sequence[RowDiff],
        *,
        sessions: Mapping[str, Any] | None = None,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        if sessions is None:
            raise ChronosFSError(
                "filesystem diff requires sessions pinned to the merge preview"
            )
        source_session = sessions[source]
        target_session = sessions[target]
        inode_ids: set[int] = set()
        # All path and type information must come from the same branch
        # references used by merge_preview_tables.  A second live lookup can
        # observe a later publication (or a different FUSE/cache session) and
        # turn a valid row difference into a spurious path-not-found result.
        source_kinds: dict[int, str] = {}
        target_kinds: dict[int, str] = {}
        for diff in diffs:
            for row in (diff.key, diff.before, diff.after):
                if row is None:
                    continue
                for field in ("inode_id", "parent_inode_id"):
                    inode_id = _optional_int(row.get(field))
                    if inode_id is not None:
                        inode_ids.add(inode_id)
            if diff.table == "chronosfs_inodes":
                target_row = diff.before
                source_row = diff.after
                target_inode = (
                    _optional_int(target_row.get("inode_id"))
                    if target_row is not None
                    else None
                )
                source_inode = (
                    _optional_int(source_row.get("inode_id"))
                    if source_row is not None
                    else None
                )
                if target_inode is not None and target_row is not None:
                    kind = target_row.get("kind")
                    if kind is not None:
                        target_kinds[target_inode] = str(kind)
                if source_inode is not None and source_row is not None:
                    kind = source_row.get("kind")
                    if kind is not None:
                        source_kinds[source_inode] = str(kind)

        def kind_for_inode(session: Any, inode_id: int, known_kind: str | None) -> str | None:
            if known_kind is not None:
                return known_kind
            rows = self._query_pinned(
                session,
                """
                SELECT kind
                FROM chronosfs_inodes
                WHERE inode_id = :inode_id
                LIMIT 1
                """,
                {"inode_id": inode_id},
            )
            return str(rows[0]["kind"]) if rows else None

        def manifest_state(
            branch_id: str,
            path: str,
            inode_id: int,
            known_kind: str | None,
        ) -> dict[str, Any]:
            state: dict[str, Any] = {"inode_id": inode_id}
            del path
            session = source_session if branch_id == source else target_session
            kind = kind_for_inode(session, inode_id, known_kind)
            if kind is not None:
                state["kind"] = kind
            return state

        source_manifest: dict[str, dict[str, Any]] = {}
        target_manifest: dict[str, dict[str, Any]] = {}
        for inode_id in inode_ids:
            source_path = self._path_for_inode_in_branch(
                source,
                inode_id,
                session=source_session,
            )
            if source_path is not None:
                source_manifest[source_path.lstrip("/")] = manifest_state(
                    source,
                    source_path,
                    inode_id,
                    source_kinds.get(inode_id),
                )
                # A sibling branch may have created the same logical path
                # with a different inode id.  Look up that path explicitly so
                # staging can rebind the source row to the target inode.
                target_entry = self._inode_for_path_in_session(
                    target_session,
                    source_path,
                )
                if target_entry is not None:
                    target_manifest[source_path.lstrip("/")] = {
                        "inode_id": target_entry["inode_id"],
                        "kind": target_entry["kind"],
                    }
            target_path = self._path_for_inode_in_branch(
                target,
                inode_id,
                session=target_session,
            )
            if target_path is not None:
                target_manifest[target_path.lstrip("/")] = manifest_state(
                    target,
                    target_path,
                    inode_id,
                    target_kinds.get(inode_id),
                )
                source_entry = self._inode_for_path_in_session(
                    source_session,
                    target_path,
                )
                if source_entry is not None:
                    source_manifest[target_path.lstrip("/")] = {
                        "inode_id": source_entry["inode_id"],
                        "kind": source_entry["kind"],
                    }
        return source_manifest, target_manifest

    def _stable_merge_preview_tables(
        self,
        source: str,
        target: str,
    ) -> tuple[Any, dict[str, Any]]:
        """Return a preview and sessions pinned to the same branch heads.

        The native preview and filesystem path reconstruction must describe one
        logical pair of branch states.  We capture both heads, run the native
        row preview, reconstruct paths through sessions pinned to those heads,
        and accept the result only if neither head moved meanwhile.  Under
        continuous writes we fail explicitly instead of returning a mixed view.
        """

        tables = [
            "chronosfs_inodes",
            "chronosfs_dirents",
            "chronosfs_file_blocks",
        ]
        for _attempt in range(_MERGE_PREVIEW_STABILITY_RETRIES):
            source_ref = str(self.context.get_branch(source).current_ref)
            target_ref = str(self.context.get_branch(target).current_ref)
            sessions = {
                source: self._checkout_ref_without_commit(source, source_ref),
                target: self._checkout_ref_without_commit(target, target_ref),
            }
            internal = self.context.merge_preview_tables(source, target, tables)
            current_source_ref = str(self.context.get_branch(source).current_ref)
            current_target_ref = str(self.context.get_branch(target).current_ref)
            if (source_ref, target_ref) == (current_source_ref, current_target_ref):
                return internal, sessions
        raise ChronosFSError(
            "could not obtain a stable filesystem merge preview: "
            f"branches changed during {_MERGE_PREVIEW_STABILITY_RETRIES} attempts"
        )

    def _checkout_ref_without_commit(self, branch_id: str, ref: str) -> Any:
        """Prepare a pinned session without committing the snapshot lock."""

        prepared = self.context._prepare_ref(_BranchRef(branch_id, str(ref)))
        return BranchSession(self.context, prepared)

    def _path_for_inode_in_branch(
        self,
        branch_id: str,
        inode_id: int,
        *,
        session: Any | None = None,
    ) -> str | None:
        if inode_id == 1:
            return "/"
        if session is None:
            raise ChronosFSError(
                "filesystem path resolution requires a pinned branch session"
            )
        parts: list[str] = []
        current = inode_id
        seen: set[int] = set()
        while current != 1:
            if current in seen:
                raise ChronosFSError(
                    f"cycle in ChronosFS directory entries at inode {current}"
                )
            seen.add(current)
            rows = self._query_pinned(
                session,
                """
                SELECT parent_inode_id, name
                FROM chronosfs_dirents
                WHERE inode_id = :inode_id
                ORDER BY parent_inode_id, name
                LIMIT 1
                """,
                {"inode_id": current},
            )
            if not rows:
                return None
            row = rows[0]
            parts.append(str(row["name"]))
            current = int(row["parent_inode_id"])
        return "/" + "/".join(reversed(parts))

    @staticmethod
    def _inode_for_path_in_session(session: Any, path: str) -> dict[str, Any] | None:
        current = _ROOT_INODE
        if path not in {"", "/"}:
            start = 1
            while start < len(path):
                slash = path.find("/", start)
                name = path[start:] if slash == -1 else path[start:slash]
                rows = ChronosFSStore._query_pinned(
                    session,
                    """
                    SELECT inode_id
                    FROM chronosfs_dirents
                    WHERE parent_inode_id = :parent_inode_id
                      AND name = :name
                    LIMIT 1
                    """,
                    {"parent_inode_id": current, "name": name},
                )
                if not rows:
                    return None
                current = int(rows[0]["inode_id"])
                if slash == -1:
                    break
                start = slash + 1
        rows = ChronosFSStore._query_pinned(
            session,
            """
            SELECT inode_id, kind
            FROM chronosfs_inodes
            WHERE inode_id = :inode_id
            LIMIT 1
            """,
            {"inode_id": current},
        )
        if not rows:
            return None
        return {"inode_id": int(rows[0]["inode_id"]), "kind": str(rows[0]["kind"])}

    @staticmethod
    def _query_pinned(
        session: Any,
        sql: str,
        params: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        """Query a prepared branch reference without refreshing or committing it."""

        context = session._context
        return context._backend.query(session._ref, sql, dict(params))

    def merge_apply(
        self,
        source: str,
        target: str,
        resolution: MergeResolution | None = None,
        *,
        policy: MergePolicyInput = None,
    ) -> Any:
        self._flush_mount_writes()
        if resolution is not None or policy is not None:
            preview = self.merge_preview(source, target, policy=policy)
            active_resolution = resolution
            if active_resolution is None and preview.resolution.conflict_choices:
                active_resolution = preview.resolution
            _resolve_merge_changes(
                preview,
                policy or "manual_review",
                active_resolution,
                backend="chronosfs",
            )
            result = self.context.merge_apply(
                source,
                target,
                active_resolution or MergeResolution(),
                policy="manual_review",
            )
            self._clear_cache(source)
            self._clear_cache(target)
            return result
        applied = int(self._native.merge_apply(source, target))
        self._clear_cache(source)
        self._clear_cache(target)
        return MergeResult(source=source, target=target, applied=applied)

    def _public_row_diff(
        self,
        diff: RowDiff,
        source_manifest: dict[str, dict[str, Any]],
        target_manifest: dict[str, dict[str, Any]],
    ) -> RowDiff:
        if diff.table == "chronosfs_file_blocks":
            return self._public_file_block_diff(diff, source_manifest, target_manifest)
        if diff.table == "chronosfs_inodes":
            return self._public_inode_diff(diff, source_manifest, target_manifest)
        if diff.table == "chronosfs_dirents":
            return self._public_dirent_diff(diff, source_manifest, target_manifest)
        return RowDiff(
            table="chronosfs",
            key=_sanitize_internal_row(diff.key),
            change=diff.change,
            before=_sanitize_internal_row(diff.before),
            after=_sanitize_internal_row(diff.after),
            conflict_id=diff.conflict_id,
        )

    def _public_file_block_diff(
        self,
        diff: RowDiff,
        source_manifest: dict[str, dict[str, Any]],
        target_manifest: dict[str, dict[str, Any]],
    ) -> RowDiff:
        inode_id = int(diff.key["inode_id"])
        path = _path_for_inode(inode_id, source_manifest, target_manifest)
        before_bytes = _block_data(diff.before)
        after_bytes = _block_data(diff.after)
        start = min(
            _range_start(diff.before, diff.key),
            _range_start(diff.after, diff.key),
        )
        end = max(
            _range_end(diff.before, diff.key),
            _range_end(diff.after, diff.key),
        )
        byte_range = {"start": start, "end": end}
        before = _content_payload(before_bytes, path, byte_range)
        after = _content_payload(after_bytes, path, byte_range)
        if after is not None:
            after["unified_diff"] = _unified_content_diff(
                path,
                byte_range,
                before_bytes,
                after_bytes,
            )
        return RowDiff(
            table="chronosfs_file_range",
            key={"path": path, "byte_range": byte_range},
            change=diff.change,
            before=before,
            after=after,
            conflict_id=diff.conflict_id,
        )

    def _public_inode_diff(
        self,
        diff: RowDiff,
        source_manifest: dict[str, dict[str, Any]],
        target_manifest: dict[str, dict[str, Any]],
    ) -> RowDiff:
        inode_id = _optional_int(diff.key.get("inode_id"))
        path = (
            _path_for_inode(inode_id, source_manifest, target_manifest)
            if inode_id
            else "<unknown>"
        )
        return RowDiff(
            table="chronosfs_inode",
            key={"path": path},
            change=diff.change,
            before=_inode_payload(diff.before, path),
            after=_inode_payload(diff.after, path),
            conflict_id=diff.conflict_id,
        )

    def _public_dirent_diff(
        self,
        diff: RowDiff,
        source_manifest: dict[str, dict[str, Any]],
        target_manifest: dict[str, dict[str, Any]],
    ) -> RowDiff:
        parent_inode_id = _optional_int(diff.key.get("parent_inode_id"))
        name = str(diff.key.get("name", ""))
        path = _child_path_for_parent(
            parent_inode_id, name, source_manifest, target_manifest
        )
        return RowDiff(
            table="chronosfs_dirent",
            key={"path": path},
            change=diff.change,
            before=_dirent_payload(diff.before, path),
            after=_dirent_payload(diff.after, path),
            conflict_id=diff.conflict_id,
        )

    def manifest(self, branch_id: str) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}

        def visit(path: str) -> None:
            stat = self.stat(branch_id, "/" + path if path else "/")
            if path:
                entry: dict[str, Any] = {
                    "inode_id": stat.inode_id,
                    "kind": stat.kind,
                    "mode": stat.mode,
                    "size": stat.size,
                    "target": stat.symlink_target,
                }
                if stat.kind == "file":
                    entry["hash"] = hashlib.sha256(
                        self.read_file(branch_id, "/" + path)
                    ).hexdigest()
                result[path] = entry
            if stat.kind != "directory":
                return
            base = "/" + path if path else "/"
            for name in self.listdir(branch_id, base):
                child = f"{path}/{name}" if path else name
                visit(child)

        visit("")
        return result

    def _clear_cache(self, branch_id: str) -> None:
        try:
            self._native.clear_cache(branch_id)
        except Exception:
            pass


def _database_url_for_context(context: ChronosBranchContext) -> str:
    return _database_url_for_adapter(context.db)


def _database_url_for_adapter(adapter: Any) -> str:
    database_url = getattr(adapter, "database_url", None)
    if database_url:
        return str(database_url)
    database_path = getattr(adapter, "database_path", None)
    if database_path and str(database_path) != ":memory:":
        return f"sqlite:///{database_path}"
    raise ChronosFSError(
        "native ChronosFS requires a file-backed SQLite or PostgreSQL database"
    )


def _stat_from_native(row: dict[str, Any]) -> ChronosFSStat:
    return ChronosFSStat(
        inode_id=int(row["inode_id"]),
        kind=str(row["kind"]),  # type: ignore[arg-type]
        mode=int(row["mode"]),
        uid=int(row["uid"]),
        gid=int(row["gid"]),
        size=int(row["size"]),
        nlink=int(row["nlink"]),
        symlink_target=row.get("symlink_target"),
    )


def _normalize_path(path: str) -> str:
    raw = str(path)
    if not raw.startswith("/"):
        raw = "/" + raw
    parts: list[str] = []
    for part in PurePosixPath(raw).parts:
        if part in {"", "/", "."}:
            continue
        if part == "..":
            raise ChronosFSError(f"invalid path: {path}")
        parts.append(part)
    if parts and parts[0] == ".chronos":
        raise ChronosFSError(f"reserved path: {path}")
    return "/" + "/".join(parts) if parts else "/"


def _check_name(name: str) -> str:
    if not name or "/" in name or name in {".", ".."}:
        raise ChronosFSError(f"invalid directory entry name: {name}")
    return str(name)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _public_path(path: str) -> str:
    return "/" + path.lstrip("/") if path else "/"


def _inode_path_map(manifest: dict[str, dict[str, Any]]) -> dict[int, list[str]]:
    result: dict[int, list[str]] = {}
    for path, state in manifest.items():
        inode_id = _optional_int(state.get("inode_id"))
        if inode_id is not None:
            result.setdefault(inode_id, []).append(_public_path(path))
    for paths in result.values():
        paths.sort()
    return result


def _path_for_inode(
    inode_id: int,
    source_manifest: dict[str, dict[str, Any]],
    target_manifest: dict[str, dict[str, Any]],
) -> str:
    source_paths = _inode_path_map(source_manifest).get(inode_id)
    if source_paths:
        return source_paths[0]
    target_paths = _inode_path_map(target_manifest).get(inode_id)
    if target_paths:
        return target_paths[0]
    return "<unlinked>"


def _public_path_for_internal_diff(
    diff: RowDiff,
    source_manifest: dict[str, dict[str, Any]],
    target_manifest: dict[str, dict[str, Any]],
) -> str:
    if diff.table in {"chronosfs_file_blocks", "chronosfs_inodes"}:
        inode_id = _optional_int(diff.key.get("inode_id"))
        if inode_id is None:
            return "<unknown>"
        return _path_for_inode(inode_id, source_manifest, target_manifest)
    if diff.table == "chronosfs_dirents":
        return _child_path_for_parent(
            _optional_int(diff.key.get("parent_inode_id")),
            str(diff.key.get("name", "")),
            source_manifest,
            target_manifest,
        )
    return "<unknown>"


def _remap_chronosfs_row_diff(
    diff: RowDiff,
    inode_remap: dict[int, int],
) -> RowDiff:
    """Rebind source inode references to existing target path inodes.

    ChronosFS allocates inode ids globally, so newly created source objects
    can retain their ids.  Only objects whose path already exists in the
    target need rebinding.  The native interval writer consumes the key and
    ``after`` row independently, hence both must be rewritten for directory
    edges, inode rows, and block rows.
    """

    if not inode_remap:
        return diff

    def remap_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        result = dict(row)
        for field in ("inode_id", "parent_inode_id"):
            value = result.get(field)
            try:
                inode_id = int(value)
            except (TypeError, ValueError):
                continue
            replacement = inode_remap.get(inode_id)
            if replacement is not None:
                result[field] = replacement
        return result

    return RowDiff(
        table=diff.table,
        key=remap_row(diff.key) or {},
        change=diff.change,
        before=remap_row(diff.before),
        after=remap_row(diff.after),
        conflict_id=diff.conflict_id,
        change_id=diff.change_id,
    )


def _child_path_for_parent(
    parent_inode_id: int | None,
    name: str,
    source_manifest: dict[str, dict[str, Any]],
    target_manifest: dict[str, dict[str, Any]],
) -> str:
    if parent_inode_id is None:
        return _public_path(name)
    parent = _path_for_inode(parent_inode_id, source_manifest, target_manifest)
    if parent == "/":
        return _public_path(name)
    if parent == "<unlinked>":
        return parent
    return parent.rstrip("/") + "/" + name


def _block_data(row: dict[str, Any] | None) -> bytes | None:
    if row is None:
        return None
    value = row.get("data")
    if value is None:
        return None
    return bytes(value)


def _range_start(row: dict[str, Any] | None, key: dict[str, Any]) -> int:
    if row is not None and row.get("byte_start") is not None:
        return int(row["byte_start"])
    if key.get("byte_start") is not None:
        return int(key["byte_start"])
    if key.get("block_index") is not None:
        return int(key["block_index"]) * 4096
    return 0


def _range_end(row: dict[str, Any] | None, key: dict[str, Any]) -> int:
    if row is not None and row.get("byte_end") is not None:
        return int(row["byte_end"])
    data = _block_data(row)
    return _range_start(row, key) + (len(data) if data is not None else 0)


def _content_payload(
    data: bytes | None,
    path: str,
    byte_range: dict[str, int],
) -> dict[str, Any] | None:
    if data is None:
        return None
    payload: dict[str, Any] = {
        "path": path,
        "byte_range": dict(byte_range),
        "byte_length": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    text = _decode_text(data)
    if text is None:
        payload["content_encoding"] = "hex"
        payload["content_hex"] = data.hex()
    else:
        payload["content_encoding"] = "utf-8"
        payload["content"] = text
    return payload


def _decode_text(data: bytes | None) -> str | None:
    if data is None:
        return None
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return None
    if any(ord(ch) < 32 and ch not in "\n\r\t" for ch in text):
        return None
    return text


def _unified_content_diff(
    path: str,
    byte_range: dict[str, int],
    before: bytes | None,
    after: bytes | None,
) -> str:
    start = byte_range["start"]
    end = byte_range["end"]
    before_text = _decode_text(before)
    after_text = _decode_text(after)
    if before_text is None or after_text is None:
        before_text = "" if before is None else before.hex()
        after_text = "" if after is None else after.hex()
    before_lines = before_text.splitlines()
    after_lines = after_text.splitlines()
    lines = list(
        difflib.unified_diff(
            before_lines,
            after_lines,
            fromfile=f"target:{path}@bytes:{start}-{end}",
            tofile=f"source:{path}@bytes:{start}-{end}",
            lineterm="",
        )
    )
    return "\n".join(lines) + ("\n" if lines else "")


def _inode_payload(row: dict[str, Any] | None, path: str) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "path": path,
        "kind": row.get("kind"),
        "mode": row.get("mode"),
        "size": row.get("size"),
        "nlink": row.get("nlink"),
        "symlink_target": row.get("symlink_target"),
    }


def _dirent_payload(row: dict[str, Any] | None, path: str) -> dict[str, Any] | None:
    if row is None:
        return None
    return {"path": path, "present": True}


def _sanitize_internal_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    hidden = {"inode_id", "parent_inode_id", "block_index", "byte_start", "byte_end"}
    return {key: value for key, value in row.items() if key not in hidden}
