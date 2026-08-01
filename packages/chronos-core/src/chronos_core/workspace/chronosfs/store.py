"""ChronosFS public API backed by native C++ interval storage."""

from __future__ import annotations

import difflib
import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from chronos_core import _native_interval
from chronos_core.branching import (
    ChronosBranchContext,
    MergePreview,
    MergeResolution,
    MergeResult,
    RowDiff,
)
from chronos_core.branching._common import (
    MergePolicyInput,
    _preview_with_merge_policy,
    _resolve_merge_changes,
)


CHRONOSFS_BLOCK_SIZE = 4096
_ROOT_INODE = 1


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


class ChronosFSBranchSession:
    """Branch-bound convenience facade for workspace checkouts.

    ChronosFS itself stores data in interval tables and normally takes an
    explicit branch id on every operation.  Workspace sessions already carry a
    branch id, so this wrapper binds that id and presents file operations as
    methods on ``session.fs`` without requiring a FUSE mount.
    """

    def __init__(self, store: "ChronosFSStore", branch_id: str):
        self._store = store
        self.branch_id = branch_id

    def stat(self, path: str) -> ChronosFSStat:
        return self._store.stat(self.branch_id, path)

    def exists(self, path: str) -> bool:
        return self._store.exists(self.branch_id, path)

    def listdir(self, path: str = "/") -> list[str]:
        return self._store.listdir(self.branch_id, path)

    def mkdir(self, path: str, *, mode: int = 0o755, parents: bool = False) -> int:
        return self._store.mkdir(self.branch_id, path, mode=mode, parents=parents)

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

    def import_tree(self, source: str | Path) -> None:
        self._store.import_tree(self.branch_id, source)

    def write_at(self, path: str, offset: int, data: bytes | str) -> None:
        self._store.write_at(self.branch_id, path, offset, data)

    def read_file(self, path: str) -> bytes:
        return self._store.read_file(self.branch_id, path)

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self._store.read_text(self.branch_id, path, encoding=encoding)

    def truncate(self, path: str, size: int) -> None:
        self._store.truncate(self.branch_id, path, size)

    def unlink(self, path: str) -> None:
        self._store.unlink(self.branch_id, path)

    def rmdir(self, path: str) -> None:
        self._store.rmdir(self.branch_id, path)

    def rename(self, old_path: str, new_path: str) -> None:
        self._store.rename(self.branch_id, old_path, new_path)

    def symlink(self, target: str, link_path: str, *, parents: bool = False) -> int:
        return self._store.symlink(self.branch_id, target, link_path, parents=parents)

    def readlink(self, path: str) -> str:
        return self._store.readlink(self.branch_id, path)

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
        self._database_url = _database_url_for_context(context)
        metadata_url = _database_url_for_adapter(context.metadata_db)
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

    @classmethod
    def connect(
        cls,
        database_url: str,
        *,
        metadata_url: str | None = None,
        block_size: int = CHRONOSFS_BLOCK_SIZE,
        backend: str = "interval",
    ) -> "ChronosFSStore":
        ctx = (
            ChronosBranchContext.connect_split(
                database_url,
                metadata_url,
                backend=backend,  # type: ignore[arg-type]
            )
            if metadata_url is not None
            else ChronosBranchContext.connect(
                database_url,
                backend=backend,  # type: ignore[arg-type]
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
        result = self.context.create_checkpoint(
            checkpoint, branch=branch, metadata=metadata
        )
        self._clear_cache(branch)
        return result

    def checkout(self, branch_id: str = "main") -> ChronosFSBranchSession:
        self.stat(branch_id, "/")
        return ChronosFSBranchSession(self, branch_id)

    def checkout_checkpoint(self, checkpoint: str) -> ChronosFSBranchSession:
        branch_id = f"checkpoint-{checkpoint}"
        if branch_id not in self.branches:
            self.create_branch_from_checkpoint(branch_id, checkpoint)
        return self.checkout(branch_id)

    def refresh_branch(self, branch_id: str) -> None:
        """Discard cached paths after writes made through a FUSE checkout."""
        self._flush_mount_writes()
        self._clear_cache(branch_id)

    def _flush_mount_writes(self) -> None:
        # Import lazily to avoid the store/fuse module cycle.
        from chronos_core.workspace.chronosfs.fuse import flush_chronosfs_daemon

        flush_chronosfs_daemon(self)

    def close(self) -> None:
        self._native.wait_for_gc()
        self.context.close()

    def wait_for_gc(self) -> None:
        """Wait until asynchronous interval and object reclamation settles."""
        self._native.wait_for_gc()

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
        left_manifest = self.manifest(left)
        right_manifest = self.manifest(right)
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
        internal = self.context.merge_preview_tables(
            source,
            target,
            [
                "chronosfs_inodes",
                "chronosfs_dirents",
                "chronosfs_file_blocks",
            ],
        )
        source_manifest, target_manifest = self._changed_inode_manifests(
            source,
            target,
            (*internal.changes, *internal.conflicts),
        )
        preview = MergePreview(
            source=source,
            target=target,
            changes=[
                self._public_row_diff(diff, source_manifest, target_manifest)
                for diff in internal.changes
            ],
            conflicts=[
                self._public_row_diff(diff, source_manifest, target_manifest)
                for diff in internal.conflicts
            ],
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
        internal = self.context.merge_preview_tables(
            source,
            target,
            [
                "chronosfs_inodes",
                "chronosfs_dirents",
                "chronosfs_file_blocks",
            ],
        )
        source_manifest, target_manifest = self._changed_inode_manifests(
            source,
            target,
            (*internal.changes, *internal.conflicts),
        )
        selected_internal: list[RowDiff] = []
        for change in (*internal.changes, *internal.conflicts):
            public = self._public_row_diff(
                change,
                source_manifest,
                target_manifest,
            )
            path = str(public.key.get("path"))
            if path in selected_paths or any(
                selected.startswith(path.rstrip("/") + "/")
                for selected in selected_paths
                if path not in {"", "/", "<unknown>", "<unlinked>"}
            ):
                selected_internal.append(change)
        applied = self.context.stage_branch_transaction_changes(
            transaction,
            selected_internal,
        )
        self._clear_cache(source)
        self._clear_cache(target)
        return applied

    def _changed_inode_manifests(
        self,
        source: str,
        target: str,
        diffs: Sequence[RowDiff],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        inode_ids: set[int] = set()
        for diff in diffs:
            for row in (diff.key, diff.before, diff.after):
                if row is None:
                    continue
                for field in ("inode_id", "parent_inode_id"):
                    inode_id = _optional_int(row.get(field))
                    if inode_id is not None:
                        inode_ids.add(inode_id)

        source_manifest: dict[str, dict[str, Any]] = {}
        target_manifest: dict[str, dict[str, Any]] = {}
        for inode_id in inode_ids:
            source_path = self._path_for_inode_in_branch(source, inode_id)
            if source_path is not None:
                source_manifest[source_path.lstrip("/")] = {"inode_id": inode_id}
            target_path = self._path_for_inode_in_branch(target, inode_id)
            if target_path is not None:
                target_manifest[target_path.lstrip("/")] = {"inode_id": inode_id}
        return source_manifest, target_manifest

    def _path_for_inode_in_branch(
        self,
        branch_id: str,
        inode_id: int,
    ) -> str | None:
        if inode_id == 1:
            return "/"
        session = self.context.checkout(branch_id)
        parts: list[str] = []
        current = inode_id
        seen: set[int] = set()
        while current != 1:
            if current in seen:
                raise ChronosFSError(
                    f"cycle in ChronosFS directory entries at inode {current}"
                )
            seen.add(current)
            rows = session.query(
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
