from __future__ import annotations

import errno
import hashlib
import os
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import pyfuse3
    import trio
except ImportError as exc:  # pragma: no cover - exercised in envs without FUSE deps.
    pyfuse3 = None  # type: ignore[assignment]
    trio = None  # type: ignore[assignment]
    _PYFUSE3_IMPORT_ERROR = exc
else:
    _PYFUSE3_IMPORT_ERROR = None

from chronos_core.workspace.chronosfs.store import (
    CHRONOSFS_BLOCK_SIZE,
    ChronosFSError,
    ChronosFSStat,
    ChronosFSStore,
    _DIRENTS,
    _INODES,
    _utc_now,
)


_CONTROL_INODE_BASE = 9_000_000_000_000_000_000
_CONTROL_ROOT = _CONTROL_INODE_BASE + 1
_CONTROL_CURRENT = _CONTROL_INODE_BASE + 2
_CONTROL_STATUS = _CONTROL_INODE_BASE + 3
_CONTROL_CTL = _CONTROL_INODE_BASE + 4
_CONTROL_BRANCHES = _CONTROL_INODE_BASE + 5
_CONTROL_CHECKPOINTS = _CONTROL_INODE_BASE + 6
_CONTROL_DIFF = _CONTROL_INODE_BASE + 7
_CONTROL_MERGE_PREVIEW = _CONTROL_INODE_BASE + 8


class ChronosFSMountError(Exception):
    """Raised when a ChronosFS FUSE mount cannot start."""


@dataclass
class _Handle:
    inode: int
    flags: int = 0
    control_path: str | None = None
    buffer: bytearray | None = None
    node: ChronosFSStat | None = None


class ChronosFuseOperations(pyfuse3.Operations if pyfuse3 is not None else object):  # type: ignore[misc]
    """pyfuse3 operations for direct ChronosFS access.

    These operations call `ChronosFSStore` methods directly. There is no
    exported checkout directory and no secondary storage layer: normal Unix
    syscalls become Chronos interval-table reads and writes.
    """

    enable_writeback_cache = False
    supports_dot_lookup = True

    def __init__(self, store: ChronosFSStore, branch_id: str = "main"):
        if pyfuse3 is None:
            raise ChronosFSMountError(
                "pyfuse3 is required for ChronosFS FUSE mounts"
            ) from _PYFUSE3_IMPORT_ERROR
        super().__init__()
        self.store = store
        self.branch_id = branch_id
        self._next_fh = 1
        self._handles: dict[int, _Handle] = {}
        self._cached_session_branch: str | None = None
        self._cached_session: Any | None = None
        self._stat_cache: dict[int, ChronosFSStat] = {}
        self._dirent_cache: dict[tuple[int, str], ChronosFSStat] = {}

    async def lookup(self, parent_inode: int, name: bytes, ctx: Any) -> Any:
        text = _decode_name(name)
        try:
            if parent_inode == 1 and text == ".chronos":
                return self._control_attr(_CONTROL_ROOT, "directory")
            if self._is_control_inode(parent_inode):
                return self._lookup_control(parent_inode, text)
            if text == ".":
                return await self.getattr(parent_inode, ctx)
            if text == "..":
                return await self.getattr(1, ctx)
            session = self._branch_session()
            return self._stat_to_attr(self._lookup_child(session, parent_inode, text))
        except ChronosFSError as exc:
            raise _fuse_error(exc)

    async def getattr(self, inode: int, ctx: Any) -> Any:
        try:
            if self._is_control_inode(inode):
                return self._control_attr_by_inode(inode)
            return self._stat_to_attr(self._stat_inode(self._branch_session(), inode))
        except ChronosFSError as exc:
            raise _fuse_error(exc)

    async def setattr(
        self,
        inode: int,
        attr: Any,
        fields: Any,
        fh: int | None,
        ctx: Any,
    ) -> Any:
        try:
            if self._is_control_inode(inode):
                raise pyfuse3.FUSEError(errno.EPERM)
            session = self._branch_session()
            with session.transaction():
                if fields.update_size:
                    node = self.store._require_inode(session, inode)
                    if node["kind"] != "file":
                        raise ChronosFSError(f"not a file inode: {inode}")
                    self.store._truncate_inode(session, inode, node, int(attr.st_size))
                if fields.update_mode:
                    node = self.store._require_inode(session, inode)
                    updated = dict(node)
                    updated.update({"mode": stat.S_IMODE(attr.st_mode), "ctime": _utc_now()})
                    session.upsert_rows(_INODES, [updated])
                self._invalidate_metadata_cache()
                node = self._stat_inode(session, inode)
                return self._stat_to_attr(node)
        except ChronosFSError as exc:
            raise _fuse_error(exc)

    async def opendir(self, inode: int, ctx: Any) -> int:
        return self._new_handle(inode)

    async def readdir(self, fh: int, start_id: int, token: Any) -> None:
        handle = self._handles[fh]
        inode = handle.inode
        try:
            entries = self._control_readdir(inode) if self._is_control_inode(inode) else self._regular_readdir(inode)
            for index, (name, attr) in enumerate(entries, start=1):
                if index <= start_id:
                    continue
                if not pyfuse3.readdir_reply(token, name.encode("utf-8"), attr, index):
                    break
        except ChronosFSError as exc:
            raise _fuse_error(exc)

    async def releasedir(self, fh: int) -> None:
        self._handles.pop(fh, None)

    async def open(self, inode: int, flags: int, ctx: Any) -> Any:
        try:
            if self._is_control_inode(inode):
                return self._file_info(self._new_handle(inode, flags=flags, control_path=self._control_path(inode)))
            session = self._branch_session()
            if flags & os.O_TRUNC:
                with session.transaction():
                    node = self.store._require_inode(session, inode)
                    if node["kind"] != "file":
                        raise ChronosFSError(f"not a file inode: {inode}")
                    self.store._truncate_inode(session, inode, node, 0)
                    self._invalidate_metadata_cache()
            # Validate type before returning a handle.
            node = self._stat_inode(session, inode)
            if node.kind != "file":
                raise pyfuse3.FUSEError(errno.EISDIR)
            return self._file_info(self._new_handle(inode, flags=flags, node=node))
        except ChronosFSError as exc:
            raise _fuse_error(exc)

    async def create(
        self,
        parent_inode: int,
        name: bytes,
        mode: int,
        flags: int,
        ctx: Any,
    ) -> tuple[Any, Any]:
        text = _decode_name(name)
        try:
            if self._is_control_inode(parent_inode):
                raise pyfuse3.FUSEError(errno.EPERM)
            session = self._branch_session()
            inode = self._create_file_at(session, parent_inode, text, stat.S_IMODE(mode))
            self._invalidate_metadata_cache()
            node = self._stat_inode(session, inode)
            self._cache_lookup(parent_inode, text, node)
            fh = self._new_handle(inode, flags=flags, node=node)
            return self._file_info(fh), self._stat_to_attr(node)
        except ChronosFSError as exc:
            raise _fuse_error(exc)

    async def mknod(
        self,
        parent_inode: int,
        name: bytes,
        mode: int,
        rdev: int,
        ctx: Any,
    ) -> Any:
        if stat.S_IFMT(mode) not in {0, stat.S_IFREG}:
            raise pyfuse3.FUSEError(errno.EPERM)
        try:
            if self._is_control_inode(parent_inode):
                raise pyfuse3.FUSEError(errno.EPERM)
            session = self._branch_session()
            text = _decode_name(name)
            inode = self._create_file_at(session, parent_inode, text, stat.S_IMODE(mode))
            self._invalidate_metadata_cache()
            node = self._stat_inode(session, inode)
            self._cache_lookup(parent_inode, text, node)
            return self._stat_to_attr(node)
        except ChronosFSError as exc:
            raise _fuse_error(exc)

    async def read(self, fh: int, off: int, size: int) -> bytes:
        handle = self._handles[fh]
        if handle.control_path is not None:
            data = self._read_control(handle.control_path)
            return data[off:off + size]
        try:
            session = self._branch_session()
            node = handle.node
            if node is None:
                node = self._stat_inode(session, handle.inode)
                handle.node = node
            if node.kind == "symlink":
                raise ChronosFSError(f"cannot read symlink inode as file: {handle.inode}")
            if node.kind != "file":
                raise ChronosFSError(f"not a file inode: {handle.inode}")
            file_size = int(node.size)
            if off >= file_size:
                return b""
            return self.store._read_file_range_by_inode(
                session,
                handle.inode,
                off,
                min(file_size, off + size),
            )
        except ChronosFSError as exc:
            raise _fuse_error(exc)

    async def write(self, fh: int, off: int, buf: bytes) -> int:
        handle = self._handles[fh]
        if handle.control_path is not None:
            if handle.buffer is None:
                handle.buffer = bytearray()
            end = off + len(buf)
            if len(handle.buffer) < end:
                handle.buffer.extend(b"\x00" * (end - len(handle.buffer)))
            handle.buffer[off:end] = buf
            return len(buf)
        try:
            session = self._branch_session()
            with session.transaction():
                inode = self.store._require_inode(session, handle.inode)
                if inode["kind"] != "file":
                    raise ChronosFSError(f"not a file inode: {handle.inode}")
                self.store._write_inode_at(session, handle.inode, inode, off, bytes(buf))
                self._invalidate_metadata_cache()
                handle.node = self._stat_inode(session, handle.inode)
                self._cache_stat(handle.node)
            return len(buf)
        except ChronosFSError as exc:
            raise _fuse_error(exc)

    async def flush(self, fh: int) -> None:
        await self._flush_handle(fh)

    async def fsync(self, fh: int, datasync: bool) -> None:
        await self._flush_handle(fh)

    async def release(self, fh: int) -> None:
        try:
            await self._flush_handle(fh)
        finally:
            self._handles.pop(fh, None)

    async def mkdir(self, parent_inode: int, name: bytes, mode: int, ctx: Any) -> Any:
        text = _decode_name(name)
        try:
            if parent_inode == _CONTROL_BRANCHES:
                if text not in self.store.branches:
                    self.store.create_branch(text, from_branch=self.branch_id)
                    self._invalidate_branch_session()
                return self._control_attr(self._branch_inode(text), "directory")
            if self._is_control_inode(parent_inode):
                raise pyfuse3.FUSEError(errno.EPERM)
            session = self._branch_session()
            inode = self._mkdir_at(session, parent_inode, text, stat.S_IMODE(mode))
            self._invalidate_metadata_cache()
            node = self._stat_inode(session, inode)
            self._cache_lookup(parent_inode, text, node)
            return self._stat_to_attr(node)
        except ChronosFSError as exc:
            raise _fuse_error(exc)

    async def unlink(self, parent_inode: int, name: bytes, ctx: Any) -> None:
        text = _decode_name(name)
        try:
            if self._is_control_inode(parent_inode):
                raise pyfuse3.FUSEError(errno.EPERM)
            self.store.unlink_at(self.branch_id, parent_inode, text)
            self._invalidate_metadata_cache()
        except ChronosFSError as exc:
            raise _fuse_error(exc)

    async def rmdir(self, parent_inode: int, name: bytes, ctx: Any) -> None:
        text = _decode_name(name)
        try:
            if self._is_control_inode(parent_inode):
                raise pyfuse3.FUSEError(errno.EPERM)
            self.store.rmdir_at(self.branch_id, parent_inode, text)
            self._invalidate_metadata_cache()
        except ChronosFSError as exc:
            raise _fuse_error(exc)

    async def rename(
        self,
        parent_inode_old: int,
        name_old: bytes,
        parent_inode_new: int,
        name_new: bytes,
        flags: int,
        ctx: Any,
    ) -> None:
        if flags:
            raise pyfuse3.FUSEError(errno.EINVAL)
        try:
            if self._is_control_inode(parent_inode_old) or self._is_control_inode(parent_inode_new):
                raise pyfuse3.FUSEError(errno.EPERM)
            self.store.rename_at(
                self.branch_id,
                parent_inode_old,
                _decode_name(name_old),
                parent_inode_new,
                _decode_name(name_new),
            )
            self._invalidate_metadata_cache()
        except ChronosFSError as exc:
            raise _fuse_error(exc)

    async def access(self, inode: int, mode: int, ctx: Any) -> bool:
        try:
            if self._is_control_inode(inode):
                self._control_attr_by_inode(inode)
                return True
            self._stat_inode(self._branch_session(), inode)
            return True
        except ChronosFSError as exc:
            raise _fuse_error(exc)

    async def statfs(self, ctx: Any) -> Any:
        statvfs = pyfuse3.StatvfsData()
        statvfs.f_bsize = self.store.block_size
        statvfs.f_frsize = self.store.block_size
        statvfs.f_blocks = 1024 * 1024
        statvfs.f_bfree = 1024 * 1024
        statvfs.f_bavail = 1024 * 1024
        statvfs.f_files = 1024 * 1024
        statvfs.f_ffree = 1024 * 1024
        statvfs.f_favail = 1024 * 1024
        statvfs.f_namemax = 255
        return statvfs

    async def symlink(self, parent_inode: int, name: bytes, target: bytes, ctx: Any) -> Any:
        try:
            if self._is_control_inode(parent_inode):
                raise pyfuse3.FUSEError(errno.EPERM)
            session = self._branch_session()
            text = _decode_name(name)
            inode = self._symlink_at(session, parent_inode, text, _decode_name(target))
            self._invalidate_metadata_cache()
            node = self._stat_inode(session, inode)
            self._cache_lookup(parent_inode, text, node)
            return self._stat_to_attr(node)
        except ChronosFSError as exc:
            raise _fuse_error(exc)

    async def readlink(self, inode: int, ctx: Any) -> bytes:
        try:
            node = self._stat_inode(self._branch_session(), inode)
            if node.kind != "symlink":
                raise pyfuse3.FUSEError(errno.EINVAL)
            return (node.symlink_target or "").encode("utf-8")
        except ChronosFSError as exc:
            raise _fuse_error(exc)

    def _branch_session(self) -> Any:
        if self._cached_session is None or self._cached_session_branch != self.branch_id:
            self._cached_session = self.store.context.checkout(self.branch_id)
            self._cached_session_branch = self.branch_id
        return self._cached_session

    def _invalidate_branch_session(self) -> None:
        self._cached_session = None
        self._cached_session_branch = None
        self._invalidate_metadata_cache()

    def _invalidate_metadata_cache(self) -> None:
        self._stat_cache.clear()
        self._dirent_cache.clear()

    def _cache_stat(self, node: ChronosFSStat) -> ChronosFSStat:
        self._stat_cache[node.inode_id] = node
        return node

    def _cache_lookup(self, parent_inode_id: int, name: str, node: ChronosFSStat) -> ChronosFSStat:
        self._cache_stat(node)
        self._dirent_cache[(parent_inode_id, name)] = node
        return node

    def _stat_inode(self, session: Any, inode_id: int) -> ChronosFSStat:
        cached = self._stat_cache.get(inode_id)
        if cached is not None:
            return cached
        return self._cache_stat(self.store._stat_from_row(self.store._require_inode(session, inode_id)))

    def _lookup_child(self, session: Any, parent_inode_id: int, name: str) -> ChronosFSStat:
        cached = self._dirent_cache.get((parent_inode_id, name))
        if cached is not None:
            return cached
        return self._cache_lookup(parent_inode_id, name, self.store._lookup_child_stat(session, parent_inode_id, name))

    def _listdir_inode(self, session: Any, inode_id: int) -> list[str]:
        inode = self.store._require_inode(session, inode_id)
        if inode["kind"] != "directory":
            raise ChronosFSError(f"not a directory inode: {inode_id}")
        rows = session.query(
            f"""
            SELECT name
            FROM {_DIRENTS}
            WHERE parent_inode_id = :parent
            ORDER BY name
            """,
            {"parent": inode_id},
        )
        return [str(row["name"]) for row in rows]

    def _create_file_at(self, session: Any, parent_inode_id: int, name: str, mode: int) -> int:
        with session.transaction():
            parent = self.store._require_inode(session, parent_inode_id)
            if parent["kind"] != "directory":
                raise ChronosFSError(f"parent is not a directory inode: {parent_inode_id}")
            if self.store._dirent_row(session, parent_inode_id, name) is not None:
                raise ChronosFSError(f"path already exists: {name}")
            inode_id = self.store._allocate_inode_id()
            now = _utc_now()
            session.upsert_rows(
                _INODES,
                [self.store._inode_payload(inode_id, "file", mode, 0, None, now)],
            )
            session.upsert_rows(
                _DIRENTS,
                [self.store._dirent_payload(parent_inode_id, name, inode_id, now)],
            )
            self.store._touch_inode(session, parent_inode_id, now)
            return inode_id

    def _mkdir_at(self, session: Any, parent_inode_id: int, name: str, mode: int) -> int:
        with session.transaction():
            parent = self.store._require_inode(session, parent_inode_id)
            if parent["kind"] != "directory":
                raise ChronosFSError(f"parent is not a directory inode: {parent_inode_id}")
            if self.store._dirent_row(session, parent_inode_id, name) is not None:
                raise ChronosFSError(f"path already exists: {name}")
            inode_id = self.store._allocate_inode_id()
            now = _utc_now()
            session.upsert_rows(
                _INODES,
                [self.store._inode_payload(inode_id, "directory", mode, 0, None, now)],
            )
            session.upsert_rows(
                _DIRENTS,
                [self.store._dirent_payload(parent_inode_id, name, inode_id, now)],
            )
            self.store._touch_inode(session, parent_inode_id, now)
            return inode_id

    def _symlink_at(self, session: Any, parent_inode_id: int, name: str, target: str) -> int:
        with session.transaction():
            parent = self.store._require_inode(session, parent_inode_id)
            if parent["kind"] != "directory":
                raise ChronosFSError(f"parent is not a directory inode: {parent_inode_id}")
            if self.store._dirent_row(session, parent_inode_id, name) is not None:
                raise ChronosFSError(f"path already exists: {name}")
            inode_id = self.store._allocate_inode_id()
            now = _utc_now()
            session.upsert_rows(
                _INODES,
                [self.store._inode_payload(inode_id, "symlink", 0o777, len(target), target, now)],
            )
            session.upsert_rows(
                _DIRENTS,
                [self.store._dirent_payload(parent_inode_id, name, inode_id, now)],
            )
            self.store._touch_inode(session, parent_inode_id, now)
            return inode_id

    def _regular_readdir(self, inode: int) -> list[tuple[str, Any]]:
        session = self._branch_session()
        entries = [(".", self._stat_to_attr(self._stat_inode(session, inode)))]
        entries.append(("..", self._stat_to_attr(self._stat_inode(session, 1))))
        if inode == 1:
            entries.append((".chronos", self._control_attr(_CONTROL_ROOT, "directory")))
        for name, node in self.store._listdir_inode_stats(session, inode):
            self._cache_lookup(inode, name, node)
            entries.append((name, self._stat_to_attr(node)))
        return entries

    def _control_readdir(self, inode: int) -> list[tuple[str, Any]]:
        if inode == _CONTROL_ROOT:
            session = self._branch_session()
            return [
                (".", self._control_attr(_CONTROL_ROOT, "directory")),
                ("..", self._stat_to_attr(self._stat_inode(session, 1))),
                ("current", self._control_attr(_CONTROL_CURRENT, "file", self.branch_id + "\n")),
                ("status", self._control_attr(_CONTROL_STATUS, "file", self._status_text())),
                ("ctl", self._control_attr(_CONTROL_CTL, "file", "")),
                ("branches", self._control_attr(_CONTROL_BRANCHES, "directory")),
                ("checkpoints", self._control_attr(_CONTROL_CHECKPOINTS, "directory")),
                ("diff", self._control_attr(_CONTROL_DIFF, "directory")),
                ("merge-preview", self._control_attr(_CONTROL_MERGE_PREVIEW, "directory")),
            ]
        if inode == _CONTROL_BRANCHES:
            entries = [
                (".", self._control_attr(_CONTROL_BRANCHES, "directory")),
                ("..", self._control_attr(_CONTROL_ROOT, "directory")),
            ]
            for branch in self.store.branches:
                entries.append((branch, self._control_attr(self._branch_inode(branch), "directory")))
            return entries
        branch = self._branch_name_from_inode(inode)
        if branch is not None:
            return [
                (".", self._control_attr(inode, "directory")),
                ("..", self._control_attr(_CONTROL_BRANCHES, "directory")),
                ("info", self._control_attr(self._branch_info_inode(branch), "file", f"branch {branch}\n")),
            ]
        return [(".", self._control_attr(inode, "directory")), ("..", self._control_attr(_CONTROL_ROOT, "directory"))]

    def _lookup_control(self, parent_inode: int, name: str) -> Any:
        if name == ".":
            return self._control_attr_by_inode(parent_inode)
        if parent_inode == _CONTROL_ROOT:
            session = self._branch_session()
            mapping = {
                "..": self._stat_to_attr(self._stat_inode(session, 1)),
                "current": self._control_attr(_CONTROL_CURRENT, "file", self.branch_id + "\n"),
                "status": self._control_attr(_CONTROL_STATUS, "file", self._status_text()),
                "ctl": self._control_attr(_CONTROL_CTL, "file", ""),
                "branches": self._control_attr(_CONTROL_BRANCHES, "directory"),
                "checkpoints": self._control_attr(_CONTROL_CHECKPOINTS, "directory"),
                "diff": self._control_attr(_CONTROL_DIFF, "directory"),
                "merge-preview": self._control_attr(_CONTROL_MERGE_PREVIEW, "directory"),
            }
            if name in mapping:
                return mapping[name]
        if parent_inode == _CONTROL_BRANCHES:
            if name == "..":
                return self._control_attr(_CONTROL_ROOT, "directory")
            if name in self.store.branches:
                return self._control_attr(self._branch_inode(name), "directory")
        branch = self._branch_name_from_inode(parent_inode)
        if branch is not None and name == "info":
            return self._control_attr(self._branch_info_inode(branch), "file", f"branch {branch}\n")
        raise pyfuse3.FUSEError(errno.ENOENT)

    def _read_control(self, control_path: str) -> bytes:
        if control_path == "current":
            return f"{self.branch_id}\n".encode("utf-8")
        if control_path == "status":
            return self._status_text().encode("utf-8")
        if control_path.startswith("branch-info:"):
            branch = control_path.split(":", 1)[1]
            return f"branch {branch}\n".encode("utf-8")
        return b""

    async def _flush_handle(self, fh: int) -> None:
        handle = self._handles.get(fh)
        if handle is None or handle.control_path is None or handle.buffer is None:
            return
        data = bytes(handle.buffer).rstrip(b"\x00")
        text = data.decode("utf-8").strip()
        handle.buffer = bytearray()
        if not text:
            return
        if handle.control_path == "current":
            if text not in self.store.branches:
                raise pyfuse3.FUSEError(errno.ENOENT)
            self.branch_id = text
            self._invalidate_branch_session()
            return
        if handle.control_path == "ctl":
            self._apply_ctl(text)
            return
        raise pyfuse3.FUSEError(errno.EPERM)

    def _apply_ctl(self, text: str) -> None:
        for command in text.splitlines():
            parts = command.strip().split()
            if not parts:
                continue
            if parts[:1] == ["create-branch"] and len(parts) in {2, 4}:
                source = self.branch_id
                if len(parts) == 4:
                    if parts[2] != "from":
                        raise pyfuse3.FUSEError(errno.EINVAL)
                    source = parts[3]
                if parts[1] not in self.store.branches:
                    self.store.create_branch(parts[1], from_branch=source)
                    self._invalidate_branch_session()
                continue
            if parts[:1] == ["create-checkpoint"] and len(parts) == 2:
                self.store.create_checkpoint(parts[1], branch=self.branch_id)
                continue
            if parts[:1] == ["delete-branch"] and len(parts) == 2:
                self.store.delete_branch(parts[1])
                self._invalidate_branch_session()
                continue
            if parts[:1] == ["merge"] and len(parts) == 4 and parts[2] == "into":
                self.store.merge_apply(parts[1], parts[3])
                self._invalidate_branch_session()
                continue
            raise pyfuse3.FUSEError(errno.EINVAL)

    def _status_text(self) -> str:
        return (
            f"branch {self.branch_id}\n"
            "readonly false\n"
            "dirty_handles 0\n"
            f"open_handles {len(self._handles)}\n"
            f"block_size {self.store.block_size}\n"
        )

    def _new_handle(
        self,
        inode: int,
        flags: int = 0,
        control_path: str | None = None,
        node: ChronosFSStat | None = None,
    ) -> int:
        fh = self._next_fh
        self._next_fh += 1
        self._handles[fh] = _Handle(
            inode=inode,
            flags=flags,
            control_path=control_path,
            node=node,
        )
        return fh

    def _file_info(self, fh: int) -> Any:
        info = pyfuse3.FileInfo()
        info.fh = fh
        info.keep_cache = False
        return info

    def _stat_to_attr(self, node: ChronosFSStat) -> Any:
        if node.kind == "directory":
            mode_type = stat.S_IFDIR
        elif node.kind == "symlink":
            mode_type = stat.S_IFLNK
        else:
            mode_type = stat.S_IFREG
        return self._attr(
            node.inode_id,
            mode_type | (node.mode & 0o7777),
            node.size,
            nlink=max(1, node.nlink),
            uid=node.uid,
            gid=node.gid,
        )

    def _control_attr_by_inode(self, inode: int) -> Any:
        if inode in {
            _CONTROL_ROOT,
            _CONTROL_BRANCHES,
            _CONTROL_CHECKPOINTS,
            _CONTROL_DIFF,
            _CONTROL_MERGE_PREVIEW,
        } or self._branch_name_from_inode(inode) is not None:
            return self._control_attr(inode, "directory")
        if inode == _CONTROL_CURRENT:
            return self._control_attr(inode, "file", self.branch_id + "\n")
        if inode == _CONTROL_STATUS:
            return self._control_attr(inode, "file", self._status_text())
        if inode == _CONTROL_CTL:
            return self._control_attr(inode, "file", "")
        branch = self._branch_info_name_from_inode(inode)
        if branch is not None:
            return self._control_attr(inode, "file", f"branch {branch}\n")
        raise pyfuse3.FUSEError(errno.ENOENT)

    def _control_attr(self, inode: int, kind: str, content: str = "") -> Any:
        mode_type = stat.S_IFDIR if kind == "directory" else stat.S_IFREG
        mode = 0o755 if kind == "directory" else 0o666
        return self._attr(inode, mode_type | mode, len(content.encode("utf-8")), nlink=1)

    def _attr(
        self,
        inode: int,
        mode: int,
        size: int,
        *,
        nlink: int = 1,
        uid: int | None = None,
        gid: int | None = None,
    ) -> Any:
        attr = pyfuse3.EntryAttributes()
        now = time.time_ns()
        attr.st_ino = inode
        attr.st_mode = mode
        attr.st_nlink = nlink
        attr.st_uid = os.getuid() if uid is None else uid
        attr.st_gid = os.getgid() if gid is None else gid
        attr.st_rdev = 0
        attr.st_size = size
        attr.st_blksize = max(CHRONOSFS_BLOCK_SIZE, self.store.block_size)
        attr.st_blocks = (size + 511) // 512
        attr.st_atime_ns = now
        attr.st_mtime_ns = now
        attr.st_ctime_ns = now
        attr.entry_timeout = 0
        attr.attr_timeout = 0
        return attr

    def _is_control_inode(self, inode: int) -> bool:
        return inode >= _CONTROL_INODE_BASE

    def _control_path(self, inode: int) -> str:
        if inode == _CONTROL_CURRENT:
            return "current"
        if inode == _CONTROL_STATUS:
            return "status"
        if inode == _CONTROL_CTL:
            return "ctl"
        branch = self._branch_info_name_from_inode(inode)
        if branch is not None:
            return f"branch-info:{branch}"
        return ""

    def _branch_inode(self, branch: str) -> int:
        return _CONTROL_INODE_BASE + 100_000 + _stable_u32(branch)

    def _branch_info_inode(self, branch: str) -> int:
        return _CONTROL_INODE_BASE + 4_000_000_000 + _stable_u32(branch)

    def _branch_name_from_inode(self, inode: int) -> str | None:
        for branch in self.store.branches:
            if self._branch_inode(branch) == inode:
                return branch
        return None

    def _branch_info_name_from_inode(self, inode: int) -> str | None:
        for branch in self.store.branches:
            if self._branch_info_inode(branch) == inode:
                return branch
        return None


def mount_chronosfs(
    store: ChronosFSStore,
    mountpoint: str | Path,
    *,
    branch_id: str = "main",
    foreground: bool = True,
    options: set[str] | None = None,
) -> None:
    """Mount ChronosFS with pyfuse3 and block until unmounted."""
    if pyfuse3 is None or trio is None:
        raise ChronosFSMountError("pyfuse3 and trio are required") from _PYFUSE3_IMPORT_ERROR
    ops = ChronosFuseOperations(store, branch_id=branch_id)
    mount_path = os.fsdecode(mountpoint)
    fuse_options = set(options or ())
    fuse_options.update({"fsname=chronosfs"})
    fuse_options.discard("default_permissions")
    pyfuse3.init(ops, mount_path, fuse_options)
    try:
        trio.run(pyfuse3.main)
    finally:
        pyfuse3.close(unmount=True)


def _decode_name(name: bytes) -> str:
    return os.fsdecode(name)


def _fuse_error(exc: Exception) -> Exception:
    message = str(exc)
    if "not found" in message or "not exist" in message:
        return pyfuse3.FUSEError(errno.ENOENT)
    if "not a directory" in message:
        return pyfuse3.FUSEError(errno.ENOTDIR)
    if "directory not empty" in message:
        return pyfuse3.FUSEError(errno.ENOTEMPTY)
    if "already exists" in message:
        return pyfuse3.FUSEError(errno.EEXIST)
    if "is a directory" in message:
        return pyfuse3.FUSEError(errno.EISDIR)
    return pyfuse3.FUSEError(errno.EIO)


def _stable_u32(value: str) -> int:
    return int.from_bytes(hashlib.sha1(value.encode("utf-8")).digest()[:4], "big")
