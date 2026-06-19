"""Chronos interval-backed filesystem store.

The storage model is deliberately small:

* inodes hold stable numeric file identities and metadata;
* dirents map directory/name pairs to inode ids;
* file_blocks hold fixed-size byte ranges keyed by (inode_id, block_index).

The hard invariant is that ChronosFS does not implement its own copy-on-write
layer. Every durable mutation is an upsert/delete against one of these logical
tables through a Chronos branch session, and the interval backend does the row
versioning. Unix tooling support is provided by the FUSE adapter, which calls
these same SQL-backed methods directly.
"""

from __future__ import annotations

import hashlib
import os
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Literal

from chronos_core.branching import ChronosBranchContext


CHRONOSFS_BLOCK_SIZE = 4096
_INODES = "chronosfs_inodes"
_DIRENTS = "chronosfs_dirents"
_BLOCKS = "chronosfs_file_blocks"
_ALLOCATOR = "_chronosfs_inode_allocator"
_METADATA = "_chronosfs_metadata"
_ROOT_INODE = 1
_CONTROL_DIR = ".chronos"


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
    """Filesystem state stored as Chronos interval rows.

    This class implements the durable SQL-backed filesystem model. The FUSE
    adapter calls the inode-level methods for lookup/read/write/truncate rather
    than maintaining another persistence path.
    """

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
        """Create logical ChronosFS tables and register them with Chronos.

        The three filesystem tables are branchable. The allocator and metadata
        tables are not: inode numbers only need global uniqueness, not branch
        visibility. Allocator gaps are acceptable after rollbacks or failed
        operations, matching normal database sequence behavior.
        """
        db = self.context.db
        blob_type = "BYTEA" if db.dialect == "postgres" else "BLOB"
        db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_INODES} (
              inode_id BIGINT PRIMARY KEY,
              kind TEXT NOT NULL,
              mode INTEGER NOT NULL,
              uid INTEGER NOT NULL,
              gid INTEGER NOT NULL,
              size BIGINT NOT NULL,
              nlink INTEGER NOT NULL,
              symlink_target TEXT,
              atime TEXT NOT NULL,
              mtime TEXT NOT NULL,
              ctime TEXT NOT NULL
            )
            """
        )
        db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_DIRENTS} (
              parent_inode_id BIGINT NOT NULL,
              name TEXT NOT NULL,
              inode_id BIGINT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY (parent_inode_id, name)
            )
            """
        )
        db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_BLOCKS} (
              inode_id BIGINT NOT NULL,
              block_index BIGINT NOT NULL,
              data {blob_type} NOT NULL,
              valid_length INTEGER NOT NULL,
              PRIMARY KEY (inode_id, block_index)
            )
            """
        )
        db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_ALLOCATOR} (
              id INTEGER PRIMARY KEY,
              next_inode_id BIGINT NOT NULL
            )
            """
        )
        db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_METADATA} (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            )
            """
        )
        db.execute(
            f"INSERT OR IGNORE INTO {_ALLOCATOR} (id, next_inode_id) VALUES (1, 2)"
            if db.dialect == "sqlite"
            else f"""
            INSERT INTO {_ALLOCATOR} (id, next_inode_id)
            VALUES (1, 2)
            ON CONFLICT (id) DO NOTHING
            """
        )
        db.execute(
            f"INSERT OR IGNORE INTO {_METADATA} (key, value) VALUES ('block_size', ?)"
            if db.dialect == "sqlite"
            else f"""
            INSERT INTO {_METADATA} (key, value)
            VALUES ('block_size', ?)
            ON CONFLICT (key) DO NOTHING
            """,
            (str(self.block_size),),
        )
        db.commit()
        self.context.register_table(_INODES, ["inode_id"])
        self.context.register_table(_DIRENTS, ["parent_inode_id", "name"])
        self.context.register_table(_BLOCKS, ["inode_id", "block_index"])
        self.create_root_if_missing()

    def create_root_if_missing(self, branch: str = "main") -> None:
        session = self.context.checkout(branch)
        if self._inode_row(session, _ROOT_INODE) is not None:
            return
        now = _utc_now()
        session.upsert_rows(
            _INODES,
            [
                {
                    "inode_id": _ROOT_INODE,
                    "kind": "directory",
                    "mode": 0o755,
                    "uid": os.getuid() if hasattr(os, "getuid") else 0,
                    "gid": os.getgid() if hasattr(os, "getgid") else 0,
                    "size": 0,
                    "nlink": 1,
                    "symlink_target": None,
                    "atime": now,
                    "mtime": now,
                    "ctime": now,
                }
            ],
        )

    @property
    def branches(self) -> tuple[str, ...]:
        return tuple(info.branch_id for info in self.context.list_branches())

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

    def close(self) -> None:
        self.context.close()

    def stat(self, branch_id: str, path: str) -> ChronosFSStat:
        session = self._read_session(branch_id)
        inode_id = self._resolve_path(session, path)
        row = self._require_inode(session, inode_id)
        return self._stat_from_row(row)

    def exists(self, branch_id: str, path: str) -> bool:
        try:
            self.stat(branch_id, path)
            return True
        except ChronosFSError:
            return False

    def listdir(self, branch_id: str, path: str = "/") -> list[str]:
        session = self._read_session(branch_id)
        inode_id = self._resolve_path(session, path)
        return self.listdir_inode(branch_id, inode_id)

    def listdir_inode(self, branch_id: str, inode_id: int) -> list[str]:
        session = self._read_session(branch_id)
        inode = self._require_inode(session, inode_id)
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

    def stat_inode(self, branch_id: str, inode_id: int) -> ChronosFSStat:
        session = self._read_session(branch_id)
        return self._stat_from_row(self._require_inode(session, inode_id))

    def lookup_child(self, branch_id: str, parent_inode_id: int, name: str) -> ChronosFSStat:
        session = self._read_session(branch_id)
        return self._lookup_child_stat(session, parent_inode_id, name)

    def read_inode(self, branch_id: str, inode_id: int) -> bytes:
        session = self._read_session(branch_id)
        inode = self._require_inode(session, inode_id)
        if inode["kind"] == "symlink":
            raise ChronosFSError(f"cannot read symlink inode as file: {inode_id}")
        if inode["kind"] != "file":
            raise ChronosFSError(f"not a file inode: {inode_id}")
        return self._read_file_by_inode(session, branch_id, inode_id, inode)

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
        session = self._read_session(branch_id)
        inode = self._require_inode(session, inode_id)
        if inode["kind"] == "symlink":
            raise ChronosFSError(f"cannot read symlink inode as file: {inode_id}")
        if inode["kind"] != "file":
            raise ChronosFSError(f"not a file inode: {inode_id}")
        file_size = int(inode["size"])
        if offset >= file_size:
            return b""
        end = min(file_size, offset + size)
        return self._read_file_range_by_inode(session, inode_id, offset, end)

    def write_inode_at(self, branch_id: str, inode_id: int, offset: int, data: bytes | str) -> None:
        if offset < 0:
            raise ValueError("offset must be non-negative")
        payload = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        if not payload:
            return
        session = self.context.checkout(branch_id)
        with session.transaction():
            inode = self._require_inode(session, inode_id)
            if inode["kind"] != "file":
                raise ChronosFSError(f"not a file inode: {inode_id}")
            self._write_inode_at(session, inode_id, inode, offset, payload)

    def truncate_inode(self, branch_id: str, inode_id: int, size: int) -> None:
        if size < 0:
            raise ValueError("size must be non-negative")
        session = self.context.checkout(branch_id)
        with session.transaction():
            inode = self._require_inode(session, inode_id)
            if inode["kind"] != "file":
                raise ChronosFSError(f"not a file inode: {inode_id}")
            self._truncate_inode(session, inode_id, inode, size)

    def chmod_inode(self, branch_id: str, inode_id: int, mode: int) -> None:
        session = self.context.checkout(branch_id)
        with session.transaction():
            inode = self._require_inode(session, inode_id)
            now = _utc_now()
            updated = dict(inode)
            updated.update({"mode": mode & 0o7777, "ctime": now})
            session.upsert_rows(_INODES, [updated])

    def create_file_at(
        self,
        branch_id: str,
        parent_inode_id: int,
        name: str,
        *,
        mode: int = 0o644,
    ) -> int:
        session = self.context.checkout(branch_id)
        with session.transaction():
            parent = self._require_inode(session, parent_inode_id)
            if parent["kind"] != "directory":
                raise ChronosFSError(f"parent is not a directory inode: {parent_inode_id}")
            if self._dirent_row(session, parent_inode_id, name) is not None:
                raise ChronosFSError(f"path already exists: {name}")
            inode_id = self._allocate_inode_id()
            now = _utc_now()
            session.upsert_rows(
                _INODES,
                [self._inode_payload(inode_id, "file", mode, 0, None, now)],
            )
            session.upsert_rows(
                _DIRENTS,
                [self._dirent_payload(parent_inode_id, name, inode_id, now)],
            )
            self._touch_inode(session, parent_inode_id, now)
        return inode_id

    def mkdir_at(
        self,
        branch_id: str,
        parent_inode_id: int,
        name: str,
        *,
        mode: int = 0o755,
    ) -> int:
        session = self.context.checkout(branch_id)
        with session.transaction():
            parent = self._require_inode(session, parent_inode_id)
            if parent["kind"] != "directory":
                raise ChronosFSError(f"parent is not a directory inode: {parent_inode_id}")
            if self._dirent_row(session, parent_inode_id, name) is not None:
                raise ChronosFSError(f"path already exists: {name}")
            inode_id = self._allocate_inode_id()
            now = _utc_now()
            session.upsert_rows(
                _INODES,
                [self._inode_payload(inode_id, "directory", mode, 0, None, now)],
            )
            session.upsert_rows(
                _DIRENTS,
                [self._dirent_payload(parent_inode_id, name, inode_id, now)],
            )
            self._touch_inode(session, parent_inode_id, now)
        return inode_id

    def symlink_at(self, branch_id: str, parent_inode_id: int, name: str, target: str) -> int:
        session = self.context.checkout(branch_id)
        with session.transaction():
            parent = self._require_inode(session, parent_inode_id)
            if parent["kind"] != "directory":
                raise ChronosFSError(f"parent is not a directory inode: {parent_inode_id}")
            if self._dirent_row(session, parent_inode_id, name) is not None:
                raise ChronosFSError(f"path already exists: {name}")
            inode_id = self._allocate_inode_id()
            now = _utc_now()
            session.upsert_rows(
                _INODES,
                [self._inode_payload(inode_id, "symlink", 0o777, len(target), target, now)],
            )
            session.upsert_rows(
                _DIRENTS,
                [self._dirent_payload(parent_inode_id, name, inode_id, now)],
            )
            self._touch_inode(session, parent_inode_id, now)
        return inode_id

    def unlink_at(self, branch_id: str, parent_inode_id: int, name: str) -> None:
        self._remove_at(branch_id, parent_inode_id, name, allow_dir=False)

    def rmdir_at(self, branch_id: str, parent_inode_id: int, name: str) -> None:
        self._remove_at(branch_id, parent_inode_id, name, allow_dir=True)

    def rename_at(
        self,
        branch_id: str,
        old_parent_inode_id: int,
        old_name: str,
        new_parent_inode_id: int,
        new_name: str,
    ) -> None:
        session = self.context.checkout(branch_id)
        old_dirent = self._dirent_row(session, old_parent_inode_id, old_name)
        if old_dirent is None:
            raise ChronosFSError(f"entry not found: {old_name}")
        replacement = self._dirent_row(session, new_parent_inode_id, new_name)
        now = _utc_now()
        with session.transaction():
            if replacement is not None:
                self._remove_inode_tree(session, int(replacement["inode_id"]))
                session.delete_keys(
                    _DIRENTS,
                    [{"parent_inode_id": new_parent_inode_id, "name": new_name}],
                )
            session.delete_keys(
                _DIRENTS,
                [{"parent_inode_id": old_parent_inode_id, "name": old_name}],
            )
            session.upsert_rows(
                _DIRENTS,
                [self._dirent_payload(
                    new_parent_inode_id,
                    new_name,
                    int(old_dirent["inode_id"]),
                    now,
                )],
            )
            self._touch_inode(session, old_parent_inode_id, now)
            if old_parent_inode_id != new_parent_inode_id:
                self._touch_inode(session, new_parent_inode_id, now)

    def mkdir(
        self,
        branch_id: str,
        path: str,
        *,
        mode: int = 0o755,
        parents: bool = False,
    ) -> int:
        norm = _normalize_path(path)
        if norm == "/":
            return _ROOT_INODE
        session = self.context.checkout(branch_id)
        with session.transaction():
            if parents:
                return self._ensure_directory_path(session, norm, mode=mode)
            return self._create_directory_path(session, norm, mode=mode)

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
        norm = _normalize_path(path)
        parent_path, name = _split_parent(norm)
        session = self.context.checkout(branch_id)
        with session.transaction():
            parent_id = (
                self._ensure_directory_path(session, parent_path, mode=0o755)
                if parents
                else self._resolve_path(session, parent_path)
            )
            existing = self._dirent_row(session, parent_id, name)
            now = _utc_now()
            if existing is None:
                inode_id = self._allocate_inode_id()
                session.upsert_rows(
                    _INODES,
                    [self._inode_payload(inode_id, "file", mode, len(payload), None, now)],
                )
                session.upsert_rows(
                    _DIRENTS,
                    [self._dirent_payload(parent_id, name, inode_id, now)],
                )
                self._replace_blocks(session, inode_id, payload)
                self._touch_inode(session, parent_id, now)
                return inode_id
            inode_id = int(existing["inode_id"])
            inode = self._require_inode(session, inode_id)
            if inode["kind"] != "file":
                raise ChronosFSError(f"not a file: {path}")
            updated = dict(inode)
            updated.update({"mode": mode, "size": len(payload), "mtime": now, "ctime": now})
            session.upsert_rows(_INODES, [updated])
            self._replace_blocks(session, inode_id, payload)
            return inode_id

    def write_at(self, branch_id: str, path: str, offset: int, data: bytes | str) -> None:
        """Patch file bytes starting at ``offset``.

        Partial-block writes are the subtle case: the currently visible block is
        read for this branch, patched in memory, and written back as one logical
        block row. Chronos then interval-splices that row. Unchanged block rows
        remain shared by visibility interval; there is no separate block
        sharing/refcount layer.
        """
        payload = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        session = self.context.checkout(branch_id)
        with session.transaction():
            inode_id = self._resolve_path(session, path)
            inode = self._require_inode(session, inode_id)
            if inode["kind"] != "file":
                raise ChronosFSError(f"not a file: {path}")
            self._write_inode_at(session, inode_id, inode, offset, payload)

    def read_file(self, branch_id: str, path: str) -> bytes:
        session = self._read_session(branch_id)
        inode_id = self._resolve_path(session, path)
        inode = self._require_inode(session, inode_id)
        if inode["kind"] == "symlink":
            target = str(inode["symlink_target"] or "")
            return self.read_file(branch_id, _join_symlink_target(path, target))
        if inode["kind"] != "file":
            raise ChronosFSError(f"not a file: {path}")
        return self._read_file_by_inode(session, branch_id, inode_id, inode)

    def read_text(self, branch_id: str, path: str, encoding: str = "utf-8") -> str:
        return self.read_file(branch_id, path).decode(encoding)

    def truncate(self, branch_id: str, path: str, size: int) -> None:
        """Resize a file, using missing block rows as sparse zero regions."""
        session = self.context.checkout(branch_id)
        with session.transaction():
            inode_id = self._resolve_path(session, path)
            inode = self._require_inode(session, inode_id)
            if inode["kind"] != "file":
                raise ChronosFSError(f"not a file: {path}")
            self._truncate_inode(session, inode_id, inode, size)

    def unlink(self, branch_id: str, path: str) -> None:
        self._remove_path(branch_id, path, allow_dir=False)

    def rmdir(self, branch_id: str, path: str) -> None:
        self._remove_path(branch_id, path, allow_dir=True)

    def rename(self, branch_id: str, old_path: str, new_path: str) -> None:
        old_norm = _normalize_path(old_path)
        new_norm = _normalize_path(new_path)
        if old_norm == "/" or new_norm == "/":
            raise ChronosFSError("cannot rename root")
        session = self.context.checkout(branch_id)
        old_parent_path, old_name = _split_parent(old_norm)
        new_parent_path, new_name = _split_parent(new_norm)
        old_parent = self._resolve_path(session, old_parent_path)
        new_parent = self._resolve_path(session, new_parent_path)
        old_dirent = self._dirent_row(session, old_parent, old_name)
        if old_dirent is None:
            raise ChronosFSError(f"path not found: {old_path}")
        replacement = self._dirent_row(session, new_parent, new_name)
        now = _utc_now()
        with session.transaction():
            if replacement is not None:
                self._remove_inode_tree(session, int(replacement["inode_id"]))
                session.delete_keys(
                    _DIRENTS,
                    [{"parent_inode_id": new_parent, "name": new_name}],
                )
            session.delete_keys(
                _DIRENTS,
                [{"parent_inode_id": old_parent, "name": old_name}],
            )
            session.upsert_rows(
                _DIRENTS,
                [self._dirent_payload(new_parent, new_name, int(old_dirent["inode_id"]), now)],
            )
            self._touch_inode(session, old_parent, now)
            if old_parent != new_parent:
                self._touch_inode(session, new_parent, now)

    def symlink(
        self,
        branch_id: str,
        target: str,
        link_path: str,
        *,
        parents: bool = False,
    ) -> int:
        norm = _normalize_path(link_path)
        parent_path, name = _split_parent(norm)
        session = self.context.checkout(branch_id)
        with session.transaction():
            parent_id = (
                self._ensure_directory_path(session, parent_path, mode=0o755)
                if parents
                else self._resolve_path(session, parent_path)
            )
            existing = self._dirent_row(session, parent_id, name)
            if existing is not None:
                raise ChronosFSError(f"path already exists: {link_path}")
            inode_id = self._allocate_inode_id()
            now = _utc_now()
            session.upsert_rows(
                _INODES,
                [self._inode_payload(inode_id, "symlink", 0o777, len(target), target, now)],
            )
            session.upsert_rows(
                _DIRENTS,
                [self._dirent_payload(parent_id, name, inode_id, now)],
            )
            self._touch_inode(session, parent_id, now)
        return inode_id

    def readlink(self, branch_id: str, path: str) -> str:
        session = self._read_session(branch_id)
        inode_id = self._resolve_path(session, path)
        inode = self._require_inode(session, inode_id)
        if inode["kind"] != "symlink":
            raise ChronosFSError(f"not a symlink: {path}")
        return str(inode["symlink_target"] or "")

    def chmod(self, branch_id: str, path: str, mode: int) -> None:
        session = self.context.checkout(branch_id)
        with session.transaction():
            inode_id = self._resolve_path(session, path)
            inode = self._require_inode(session, inode_id)
            now = _utc_now()
            updated = dict(inode)
            updated.update({"mode": mode & 0o7777, "ctime": now})
            session.upsert_rows(_INODES, [updated])

    def diff(self, left: str, right: str) -> ChronosFSDiff:
        """Return a path-oriented diff derived from branch-visible manifests."""
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
        """Apply source-visible paths into target.

        This first implementation is intentionally path-oriented and conservative
        enough for non-conflicting tests. It is not yet a full three-way merge:
        conflict detection belongs in a later merge_preview layer.
        """
        diff = self.diff(target, source)
        for change in diff.changes:
            path = "/" + change.path
            if change.change == "deleted":
                if self.exists(target, path):
                    node = self.stat(target, path)
                    if node.kind == "directory":
                        self.rmdir(target, path)
                    else:
                        self.unlink(target, path)
                continue
            source_node = self.stat(source, path)
            if source_node.kind == "directory":
                if not self.exists(target, path):
                    self.mkdir(target, path, mode=source_node.mode, parents=True)
                else:
                    self.chmod(target, path, source_node.mode)
            elif source_node.kind == "symlink":
                if self.exists(target, path):
                    node = self.stat(target, path)
                    if node.kind == "directory":
                        self.rmdir(target, path)
                    else:
                        self.unlink(target, path)
                self.symlink(target, self.readlink(source, path), path, parents=True)
            else:
                self.write_file(
                    target,
                    path,
                    self.read_file(source, path),
                    mode=source_node.mode,
                    parents=True,
                )
        from chronos_core.branching import MergeResult

        return MergeResult(source=source, target=target, applied=len(diff.changes))

    def manifest(self, branch_id: str) -> dict[str, dict[str, Any]]:
        """Build a stable path manifest for diff/import/merge.

        Directory entries are identified by path for user-facing operations.
        File content is identified by a hash of visible bytes, not by physical
        block-row identity, because two branches may have equivalent bytes in
        different interval rows after independent writes.
        """
        result: dict[str, dict[str, Any]] = {}

        def visit(path: str, inode_id: int) -> None:
            session = self._read_session(branch_id)
            inode = self._require_inode(session, inode_id)
            if path:
                result[path] = self._manifest_entry(branch_id, path, inode)
            if inode["kind"] != "directory":
                return
            for name in self.listdir(branch_id, "/" + path if path else "/"):
                child_path = f"{path}/{name}" if path else name
                child_id = self._resolve_path(session, "/" + child_path)
                visit(child_path, child_id)

        visit("", _ROOT_INODE)
        return result

    def _ensure_directory_path(self, session: Any, path: str, *, mode: int) -> int:
        norm = _normalize_path(path)
        if norm == "/":
            return _ROOT_INODE
        inode_id = _ROOT_INODE
        for part in _parts(norm):
            row = self._dirent_row(session, inode_id, part)
            if row is None:
                inode_id = self._create_directory_child(session, inode_id, part, mode=mode)
                continue
            inode_id = int(row["inode_id"])
            inode = self._require_inode(session, inode_id)
            if inode["kind"] != "directory":
                raise ChronosFSError(f"not a directory: {path}")
        return inode_id

    def _create_directory_path(self, session: Any, path: str, *, mode: int) -> int:
        parent_path, name = _split_parent(_normalize_path(path))
        parent_id = self._resolve_path(session, parent_path)
        if self._dirent_row(session, parent_id, name) is not None:
            raise ChronosFSError(f"path already exists: {path}")
        return self._create_directory_child(session, parent_id, name, mode=mode)

    def _create_directory_child(
        self,
        session: Any,
        parent_inode_id: int,
        name: str,
        *,
        mode: int,
    ) -> int:
        inode_id = self._allocate_inode_id()
        now = _utc_now()
        session.upsert_rows(
            _INODES,
            [self._inode_payload(inode_id, "directory", mode, 0, None, now)],
        )
        session.upsert_rows(
            _DIRENTS,
            [self._dirent_payload(parent_inode_id, name, inode_id, now)],
        )
        self._touch_inode(session, parent_inode_id, now)
        return inode_id

    def _read_session(self, branch_id: str) -> Any:
        return self.context.checkout(branch_id)

    def _allocate_inode_id(self) -> int:
        # This is intentionally outside Chronos interval tables. Branch-local
        # inode allocation would risk the same numeric inode being created in
        # sibling branches and later colliding at merge. Global allocation makes
        # inode ids stable object identities across the whole filesystem.
        db = self.context.db
        row = db.execute(
            f"SELECT next_inode_id FROM {_ALLOCATOR} WHERE id = 1"
        ).fetchone()
        if row is None:
            db.execute(f"INSERT INTO {_ALLOCATOR} (id, next_inode_id) VALUES (1, 2)")
            next_id = 2
        else:
            next_id = int(row["next_inode_id"])
        db.execute(
            f"UPDATE {_ALLOCATOR} SET next_inode_id = ? WHERE id = 1",
            (next_id + 1,),
        )
        return next_id

    def _read_file_by_inode(
        self,
        session: Any,
        branch_id: str,
        inode_id: int,
        inode: dict[str, Any],
    ) -> bytes:
        size = int(inode["size"])
        if size == 0:
            return b""
        return self._read_file_range_by_inode(session, inode_id, 0, size)

    def _read_file_range_by_inode(
        self,
        session: Any,
        inode_id: int,
        start: int,
        end: int,
    ) -> bytes:
        if start >= end:
            return b""
        output = bytearray()
        first_block = start // self.block_size
        last_block = (end - 1) // self.block_size
        rows = session.query(
            f"""
            SELECT block_index, data, valid_length
            FROM {_BLOCKS}
            WHERE inode_id = :inode
              AND block_index >= :first
              AND block_index <= :last
            ORDER BY block_index
            """,
            {"inode": inode_id, "first": first_block, "last": last_block},
        )
        by_index = {int(row["block_index"]): row for row in rows}
        cursor = start
        while cursor < end:
            index = cursor // self.block_size
            block_offset = cursor % self.block_size
            want = min(end - cursor, self.block_size - block_offset)
            row = by_index.get(index)
            if row is None:
                output.extend(b"\x00" * want)
            else:
                data = bytes(row["data"])
                valid = int(row["valid_length"])
                if block_offset >= valid:
                    output.extend(b"\x00" * want)
                else:
                    available = min(valid - block_offset, want)
                    segment = data[block_offset:block_offset + available]
                    output.extend(segment)
                    if len(segment) < available:
                        output.extend(b"\x00" * (available - len(segment)))
                    if available < want:
                        output.extend(b"\x00" * (want - available))
            cursor += want
        return bytes(output)

    def _write_inode_at(
        self,
        session: Any,
        inode_id: int,
        inode: dict[str, Any],
        offset: int,
        payload: bytes,
    ) -> None:
        if offset < 0:
            raise ValueError("offset must be non-negative")
        if not payload:
            return
        now = _utc_now()
        rows: list[dict[str, Any]] = []
        first_block = offset // self.block_size
        last_block = (offset + len(payload) - 1) // self.block_size
        existing_blocks = self._read_blocks(session, inode_id, first_block, last_block)
        cursor = 0
        while cursor < len(payload):
            absolute = offset + cursor
            block_index = absolute // self.block_size
            block_offset = absolute % self.block_size
            take = min(len(payload) - cursor, self.block_size - block_offset)
            existing_data, existing_valid_length = existing_blocks.get(block_index, (b"", 0))
            current = bytearray(existing_data)
            if len(current) < self.block_size:
                current.extend(b"\x00" * (self.block_size - len(current)))
            current[block_offset:block_offset + take] = payload[cursor:cursor + take]
            valid_length = max(existing_valid_length, block_offset + take)
            rows.append(
                {
                    "inode_id": inode_id,
                    "block_index": block_index,
                    "data": bytes(current[:valid_length]),
                    "valid_length": valid_length,
                }
            )
            cursor += take
        new_size = max(int(inode["size"]), offset + len(payload))
        with session.transaction():
            session.upsert_rows(_BLOCKS, rows)
            updated = dict(inode)
            updated.update({"size": new_size, "mtime": now, "ctime": now})
            session.upsert_rows(_INODES, [updated])

    def _truncate_inode(
        self,
        session: Any,
        inode_id: int,
        inode: dict[str, Any],
        size: int,
    ) -> None:
        if size < 0:
            raise ValueError("size must be non-negative")
        old_size = int(inode["size"])
        now = _utc_now()
        delete_keys: list[dict[str, Any]] = []
        upserts: list[dict[str, Any]] = []
        if size == 0:
            delete_keys = self._block_keys(session, inode_id)
        else:
            last_index = (size - 1) // self.block_size
            delete_keys = [
                key for key in self._block_keys(session, inode_id)
                if int(key["block_index"]) > last_index
            ]
            final_len = size - (last_index * self.block_size)
            existing = self._read_block(session, inode_id, last_index)
            if existing or size > old_size:
                if len(existing) < final_len:
                    existing = existing + (b"\x00" * (final_len - len(existing)))
                upserts.append(
                    {
                        "inode_id": inode_id,
                        "block_index": last_index,
                        "data": existing[:final_len],
                        "valid_length": final_len,
                    }
                )
        with session.transaction():
            if delete_keys:
                session.delete_keys(_BLOCKS, delete_keys)
            if upserts:
                session.upsert_rows(_BLOCKS, upserts)
            updated = dict(inode)
            updated.update({"size": size, "mtime": now, "ctime": now})
            session.upsert_rows(_INODES, [updated])

    def _resolve_path(self, session: Any, path: str) -> int:
        norm = _normalize_path(path)
        inode_id = _ROOT_INODE
        if norm == "/":
            return inode_id
        for part in _parts(norm):
            row = self._dirent_row(session, inode_id, part)
            if row is None:
                raise ChronosFSError(f"path not found: {path}")
            inode_id = int(row["inode_id"])
        return inode_id

    def _dirent_row(self, session: Any, parent_inode_id: int, name: str) -> dict[str, Any] | None:
        rows = session.query(
            f"""
            SELECT parent_inode_id, name, inode_id, created_at
            FROM {_DIRENTS}
            WHERE parent_inode_id = :parent AND name = :name
            """,
            {"parent": parent_inode_id, "name": name},
        )
        return rows[0] if rows else None

    def _inode_row(self, session: Any, inode_id: int) -> dict[str, Any] | None:
        rows = session.query(
            f"""
            SELECT inode_id, kind, mode, uid, gid, size, nlink, symlink_target,
                   atime, mtime, ctime
            FROM {_INODES}
            WHERE inode_id = :inode
            """,
            {"inode": inode_id},
        )
        return rows[0] if rows else None

    def _require_inode(self, session: Any, inode_id: int) -> dict[str, Any]:
        row = self._inode_row(session, inode_id)
        if row is None:
            raise ChronosFSError(f"inode not found: {inode_id}")
        return row

    def _listdir_inode_stats(self, session: Any, inode_id: int) -> list[tuple[str, ChronosFSStat]]:
        inode = self._require_inode(session, inode_id)
        if inode["kind"] != "directory":
            raise ChronosFSError(f"not a directory inode: {inode_id}")
        rows = session.query(
            f"""
            SELECT d.name,
                   i.inode_id, i.kind, i.mode, i.uid, i.gid, i.size, i.nlink,
                   i.symlink_target, i.atime, i.mtime, i.ctime
            FROM {_DIRENTS} AS d
            JOIN {_INODES} AS i ON i.inode_id = d.inode_id
            WHERE d.parent_inode_id = :parent
            ORDER BY d.name
            """,
            {"parent": inode_id},
        )
        return [(str(row["name"]), self._stat_from_row(row)) for row in rows]

    def _lookup_child_stat(
        self,
        session: Any,
        parent_inode_id: int,
        name: str,
    ) -> ChronosFSStat:
        rows = session.query(
            f"""
            SELECT i.inode_id, i.kind, i.mode, i.uid, i.gid, i.size, i.nlink,
                   i.symlink_target, i.atime, i.mtime, i.ctime
            FROM {_DIRENTS} AS d
            JOIN {_INODES} AS i ON i.inode_id = d.inode_id
            WHERE d.parent_inode_id = :parent AND d.name = :name
            """,
            {"parent": parent_inode_id, "name": name},
        )
        if not rows:
            raise ChronosFSError(f"entry not found: {name}")
        return self._stat_from_row(rows[0])

    def _stat_from_row(self, row: dict[str, Any]) -> ChronosFSStat:
        return ChronosFSStat(
            inode_id=int(row["inode_id"]),
            kind=row["kind"],  # type: ignore[arg-type]
            mode=int(row["mode"]),
            uid=int(row["uid"]),
            gid=int(row["gid"]),
            size=int(row["size"]),
            nlink=int(row["nlink"]),
            symlink_target=row.get("symlink_target"),
        )

    def _inode_payload(
        self,
        inode_id: int,
        kind: str,
        mode: int,
        size: int,
        symlink_target: str | None,
        now: str,
    ) -> dict[str, Any]:
        return {
            "inode_id": inode_id,
            "kind": kind,
            "mode": mode & 0o7777,
            "uid": os.getuid() if hasattr(os, "getuid") else 0,
            "gid": os.getgid() if hasattr(os, "getgid") else 0,
            "size": size,
            "nlink": 1,
            "symlink_target": symlink_target,
            "atime": now,
            "mtime": now,
            "ctime": now,
        }

    def _dirent_payload(
        self, parent_inode_id: int, name: str, inode_id: int, now: str
    ) -> dict[str, Any]:
        return {
            "parent_inode_id": parent_inode_id,
            "name": name,
            "inode_id": inode_id,
            "created_at": now,
        }

    def _touch_inode(self, session: Any, inode_id: int, now: str) -> None:
        inode = self._require_inode(session, inode_id)
        updated = dict(inode)
        updated.update({"mtime": now, "ctime": now})
        session.upsert_rows(_INODES, [updated])

    def _replace_blocks(self, session: Any, inode_id: int, data: bytes) -> None:
        # Whole-file replacement is expressed as block upserts plus tombstones
        # for blocks beyond EOF. Chronos interval versioning keeps old blocks
        # visible to branches/checkpoints whose branch point still selects them.
        old_keys = self._block_keys(session, inode_id)
        rows = []
        for index, start in enumerate(range(0, len(data), self.block_size)):
            block = data[start:start + self.block_size]
            rows.append(
                {
                    "inode_id": inode_id,
                    "block_index": index,
                    "data": block,
                    "valid_length": len(block),
                }
            )
        new_indexes = {row["block_index"] for row in rows}
        delete_keys = [
            key for key in old_keys if int(key["block_index"]) not in new_indexes
        ]
        if delete_keys:
            session.delete_keys(_BLOCKS, delete_keys)
        if rows:
            session.upsert_rows(_BLOCKS, rows)

    def _block_keys(self, session: Any, inode_id: int) -> list[dict[str, Any]]:
        rows = session.query(
            f"""
            SELECT block_index
            FROM {_BLOCKS}
            WHERE inode_id = :inode
            ORDER BY block_index
            """,
            {"inode": inode_id},
        )
        return [
            {"inode_id": inode_id, "block_index": int(row["block_index"])}
            for row in rows
        ]

    def _read_block(self, session: Any, inode_id: int, block_index: int) -> bytes:
        rows = session.query(
            f"""
            SELECT data, valid_length
            FROM {_BLOCKS}
            WHERE inode_id = :inode AND block_index = :block
            """,
            {"inode": inode_id, "block": block_index},
        )
        if not rows:
            return b""
        row = rows[0]
        return bytes(row["data"])[: int(row["valid_length"])]

    def _read_blocks(
        self,
        session: Any,
        inode_id: int,
        first_block: int,
        last_block: int,
    ) -> dict[int, tuple[bytes, int]]:
        if first_block > last_block:
            return {}
        rows = session.query(
            f"""
            SELECT block_index, data, valid_length
            FROM {_BLOCKS}
            WHERE inode_id = :inode
              AND block_index >= :first
              AND block_index <= :last
            ORDER BY block_index
            """,
            {"inode": inode_id, "first": first_block, "last": last_block},
        )
        return {
            int(row["block_index"]): (
                bytes(row["data"])[: int(row["valid_length"])],
                int(row["valid_length"]),
            )
            for row in rows
        }

    def _visible_block_length(self, session: Any, inode_id: int, block_index: int) -> int:
        rows = session.query(
            f"""
            SELECT valid_length
            FROM {_BLOCKS}
            WHERE inode_id = :inode AND block_index = :block
            """,
            {"inode": inode_id, "block": block_index},
        )
        return int(rows[0]["valid_length"]) if rows else 0

    def _remove_path(self, branch_id: str, path: str, *, allow_dir: bool) -> None:
        norm = _normalize_path(path)
        if norm == "/":
            raise ChronosFSError("cannot remove root")
        session = self.context.checkout(branch_id)
        parent_path, name = _split_parent(norm)
        parent_id = self._resolve_path(session, parent_path)
        dirent = self._dirent_row(session, parent_id, name)
        if dirent is None:
            raise ChronosFSError(f"path not found: {path}")
        inode_id = int(dirent["inode_id"])
        inode = self._require_inode(session, inode_id)
        if inode["kind"] == "directory" and not allow_dir:
            raise ChronosFSError(f"is a directory: {path}")
        if inode["kind"] != "directory" and allow_dir:
            raise ChronosFSError(f"not a directory: {path}")
        if inode["kind"] == "directory" and self.listdir(branch_id, norm):
            raise ChronosFSError(f"directory not empty: {path}")
        now = _utc_now()
        with session.transaction():
            session.delete_keys(_DIRENTS, [{"parent_inode_id": parent_id, "name": name}])
            self._remove_inode_tree(session, inode_id)
            self._touch_inode(session, parent_id, now)

    def _remove_at(
        self,
        branch_id: str,
        parent_inode_id: int,
        name: str,
        *,
        allow_dir: bool,
    ) -> None:
        session = self.context.checkout(branch_id)
        dirent = self._dirent_row(session, parent_inode_id, name)
        if dirent is None:
            raise ChronosFSError(f"entry not found: {name}")
        inode_id = int(dirent["inode_id"])
        inode = self._require_inode(session, inode_id)
        if inode["kind"] == "directory" and not allow_dir:
            raise ChronosFSError(f"is a directory: {name}")
        if inode["kind"] != "directory" and allow_dir:
            raise ChronosFSError(f"not a directory: {name}")
        if inode["kind"] == "directory" and self.listdir_inode(branch_id, inode_id):
            raise ChronosFSError(f"directory not empty: {name}")
        now = _utc_now()
        with session.transaction():
            session.delete_keys(_DIRENTS, [{"parent_inode_id": parent_inode_id, "name": name}])
            self._remove_inode_tree(session, inode_id)
            self._touch_inode(session, parent_inode_id, now)

    def _remove_inode_tree(self, session: Any, inode_id: int) -> None:
        # Recursive removal deletes dirents before the inode row they point to,
        # and file block rows before the file inode. These are logical Chronos
        # deletes, so inherited rows remain visible in branches outside the
        # writer's interval segment.
        inode = self._require_inode(session, inode_id)
        if inode["kind"] == "directory":
            children = session.query(
                f"""
                SELECT name, inode_id
                FROM {_DIRENTS}
                WHERE parent_inode_id = :parent
                """,
                {"parent": inode_id},
            )
            for child in children:
                self._remove_inode_tree(session, int(child["inode_id"]))
                session.delete_keys(
                    _DIRENTS,
                    [{"parent_inode_id": inode_id, "name": child["name"]}],
                )
        elif inode["kind"] == "file":
            keys = self._block_keys(session, inode_id)
            if keys:
                session.delete_keys(_BLOCKS, keys)
        session.delete_keys(_INODES, [{"inode_id": inode_id}])

    def _manifest_entry(
        self, branch_id: str, path: str, inode: dict[str, Any]
    ) -> dict[str, Any]:
        kind = str(inode["kind"])
        entry: dict[str, Any] = {
            "kind": kind,
            "mode": int(inode["mode"]),
            "size": int(inode["size"]),
        }
        if kind == "file":
            entry["hash"] = hashlib.sha256(self.read_file(branch_id, "/" + path)).hexdigest()
        elif kind == "symlink":
            entry["target"] = str(inode["symlink_target"] or "")
            entry["hash"] = hashlib.sha256(entry["target"].encode("utf-8")).hexdigest()
        return entry

def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_path(path: str) -> str:
    if not path:
        raise ChronosFSError("empty path")
    pure = PurePosixPath(path)
    if not pure.is_absolute():
        pure = PurePosixPath("/") / pure
    parts = []
    for part in pure.parts:
        if part in {"", "/"}:
            continue
        if part == ".":
            continue
        if part == "..":
            raise ChronosFSError(f"path escapes root: {path}")
        parts.append(part)
    if parts and parts[0] == _CONTROL_DIR:
        raise ChronosFSError(f"reserved control path: {path}")
    return "/" + "/".join(parts) if parts else "/"


def _parts(path: str) -> list[str]:
    norm = _normalize_path(path)
    return [] if norm == "/" else norm.strip("/").split("/")


def _split_parent(path: str) -> tuple[str, str]:
    norm = _normalize_path(path)
    if norm == "/":
        raise ChronosFSError("root has no parent")
    parent, name = norm.rsplit("/", 1)
    return parent or "/", name


def _join_symlink_target(source_path: str, target: str) -> str:
    if target.startswith("/"):
        return target
    parent, _name = _split_parent(_normalize_path(source_path))
    return str(PurePosixPath(parent) / target)


def _path_depth(path: str) -> int:
    return len([part for part in path.split("/") if part])
