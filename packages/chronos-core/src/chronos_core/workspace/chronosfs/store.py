"""ChronosFS public API backed by native C++ interval storage."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Literal

from chronos_core import _native_interval
from chronos_core.branching import ChronosBranchContext


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


class ChronosFSStore:
    """Filesystem state stored through the native C++ ChronosFS backend."""

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
        self._native = _native_interval.NativeChronosFSStore(
            self._database_url,
            self.block_size,
        )

    @classmethod
    def connect(
        cls,
        database_url: str,
        *,
        block_size: int = CHRONOSFS_BLOCK_SIZE,
        backend: str = "interval",
    ) -> "ChronosFSStore":
        ctx = ChronosBranchContext.connect(database_url, backend=backend)  # type: ignore[arg-type]
        return cls(ctx, block_size=block_size)

    def ensure(self) -> None:
        self._native.ensure()
        refresh = getattr(self.context._backend, "refresh_registries", None)
        if callable(refresh):
            refresh()

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
        if metadata:
            self.context.create_branch(branch_id, from_branch=from_branch, metadata=metadata)
        else:
            self._native.create_branch(branch_id, from_branch)
            self._refresh_context_backend()
        self._clear_cache(branch_id)

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
        self.context.create_branch_from_checkpoint(branch_id, checkpoint)
        self._clear_cache(branch_id)

    def delete_branch(self, branch_id: str) -> None:
        self._native.delete_branch(branch_id)
        self._refresh_context_backend()
        self._clear_cache(branch_id)

    def create_checkpoint(
        self,
        checkpoint: str,
        branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        result = self.context.create_checkpoint(checkpoint, branch=branch, metadata=metadata)
        self._clear_cache(branch)
        return result

    def close(self) -> None:
        self.context.close()

    def _refresh_context_backend(self) -> None:
        invalidate = getattr(self.context._backend, "_invalidate_native_branch_sessions", None)
        if callable(invalidate):
            invalidate()

    def stat(self, branch_id: str, path: str) -> ChronosFSStat:
        return _stat_from_native(self._native.stat(branch_id, _normalize_path(path)))

    def stat_inode(self, branch_id: str, inode_id: int) -> ChronosFSStat:
        return _stat_from_native(self._native.stat_inode(branch_id, int(inode_id)))

    def lookup_child(self, branch_id: str, parent_inode_id: int, name: str) -> ChronosFSStat:
        return _stat_from_native(
            self._native.lookup_child(branch_id, int(parent_inode_id), _check_name(name))
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
        return bytes(self._native.read_inode_range(branch_id, int(inode_id), int(offset), int(size)))

    def write_inode_at(self, branch_id: str, inode_id: int, offset: int, data: bytes | str) -> None:
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

    def symlink_at(self, branch_id: str, parent_inode_id: int, name: str, target: str) -> int:
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

    def write_at(self, branch_id: str, path: str, offset: int, data: bytes | str) -> None:
        if offset < 0:
            raise ValueError("offset must be non-negative")
        payload = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        if payload:
            self._native.write_at(branch_id, _normalize_path(path), int(offset), payload)

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
        self._native.rename(branch_id, _normalize_path(old_path), _normalize_path(new_path))

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

    def merge_apply(self, source: str, target: str) -> Any:
        from chronos_core.branching import MergeResult

        applied = int(self._native.merge_apply(source, target))
        self._clear_cache(source)
        self._clear_cache(target)
        return MergeResult(source=source, target=target, applied=applied)

    def manifest(self, branch_id: str) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}

        def visit(path: str) -> None:
            stat = self.stat(branch_id, "/" + path if path else "/")
            if path:
                entry: dict[str, Any] = {
                    "kind": stat.kind,
                    "mode": stat.mode,
                    "size": stat.size,
                    "target": stat.symlink_target,
                }
                if stat.kind == "file":
                    entry["hash"] = hashlib.sha256(self.read_file(branch_id, "/" + path)).hexdigest()
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
    database_url = getattr(context.db, "database_url", None)
    if database_url:
        return str(database_url)
    database_path = getattr(context.db, "database_path", None)
    if database_path and str(database_path) != ":memory:":
        return f"sqlite:///{database_path}"
    raise ChronosFSError("native ChronosFS requires a file-backed SQLite or PostgreSQL database")


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
