"""OverlayFS-based filesystem shim for transactional branching.

Uses the Linux kernel's OverlayFS (or fuse-overlayfs for unprivileged)
to provide true POSIX-compatible copy-on-write branching. Each transaction
gets its own overlay mount:
  - lowerdir = base project directory (shared, read-only within overlay)
  - upperdir = per-branch writes (kernel-level CoW)
  - merged   = unified view (reads + writes, fully POSIX-compatible)

Subtransaction support (1-level nesting):
  - Child mounts a layered overlay with parent's merged as lowerdir.
  - Child commit = merge child upper → parent upper + remount parent.
  - Child abort  = umount child + rm -rf (parent unaffected).

Unix tools (gcc, pytest, grep, git, etc.) work unmodified on the merged
directory.

Mount strategy (auto-detected at init time, in priority order):
  1. fuse-overlayfs — user-space FUSE implementation; no root required.
     Install: sudo apt install fuse-overlayfs
  2. unshare -rm   — kernel overlayfs inside a user namespace; no root.
     Requires: unprivileged_userns_clone=1 (Ubuntu/Debian default).
  3. root mount    — standard ``mount -t overlay``; requires root / CAP_SYS_ADMIN.
"""

from __future__ import annotations

import logging
import os
import shutil
import stat
import subprocess
from pathlib import Path
from typing import Any

from janus_core.transaction.shim import ToolShim
from janus_core.transaction.types import (
    ChangeRecord,
    ChangeType,
    Savepoint,
    TransactionHandle,
    Vote,
)

logger = logging.getLogger(__name__)

# Mount strategy constants
_STRATEGY_FUSE = "fuse-overlayfs"
_STRATEGY_UNSHARE = "unshare"
_STRATEGY_ROOT = "root"


class OverlayFSError(Exception):
    """Raised on OverlayFS operation failures."""


def _detect_mount_strategy() -> str:
    """Detect the best available mount strategy for this environment.

    Tries (in order):
    1. fuse-overlayfs binary
    2. unshare + user namespaces (unprivileged_userns_clone=1)
    3. Direct mount (root / CAP_SYS_ADMIN)

    Raises OverlayFSError if none are available.
    """
    # 1. fuse-overlayfs — works without any privileges
    if shutil.which("fuse-overlayfs"):
        logger.debug("OverlayFS strategy: fuse-overlayfs")
        return _STRATEGY_FUSE

    # 2. unshare with user namespaces
    try:
        userns_val = Path("/proc/sys/kernel/unprivileged_userns_clone").read_text().strip()
        if userns_val == "1" and shutil.which("unshare"):
            logger.debug("OverlayFS strategy: unshare (user namespaces)")
            return _STRATEGY_UNSHARE
    except OSError:
        pass

    # 3. Direct mount (needs root)
    if os.geteuid() == 0:
        logger.debug("OverlayFS strategy: root mount")
        return _STRATEGY_ROOT

    raise OverlayFSError(
        "Cannot mount overlayfs: no suitable mount strategy found.\n"
        "  Option 1 (recommended): sudo apt install fuse-overlayfs\n"
        "  Option 2: run as root\n"
        "  Option 3: enable user namespaces: "
        "sudo sysctl kernel.unprivileged_userns_clone=1"
    )


class OverlayFSShim(ToolShim):
    """Filesystem shim using Linux OverlayFS for branch isolation.

    Each transaction gets a kernel-level overlay mount so that all file
    modifications are captured in a per-branch ``upperdir`` and the base
    project directory is never touched until an explicit commit.

    Savepoints are implemented by tar-archiving the ``upperdir``.
    Rollback to a savepoint restores the ``upperdir`` from the archive
    and remounts the overlay.

    Commit walks the ``upperdir``, copies modified files to the base
    directory, and processes whiteout entries (deletes).

    Works without root when fuse-overlayfs is installed or when
    kernel.unprivileged_userns_clone=1 (Ubuntu/Debian default).

    Example::

        shim = OverlayFSShim("/path/to/project")
        txn = TransactionHandle.create()
        shim.begin(txn)

        workdir = shim.get_working_directory(txn)
        # All reads/writes through workdir are isolated
        (workdir / "hello.py").write_text("print('hello')")

        shim.commit(txn)  # hello.py now exists in /path/to/project
    """

    def __init__(
        self,
        base_path: str | Path,
        mount_strategy: str | None = None,
        janus_root: Path | None = None,
        enable_conflict_detection: bool = True,
    ):
        self._base_path = Path(base_path).resolve()
        if not self._base_path.is_dir():
            raise OverlayFSError(
                f"Base path does not exist or is not a directory: {self._base_path}"
            )
        # branch_id -> mount metadata
        self._mounts: dict[str, _MountInfo] = {}

        # Per-user writable root for overlay dirs.  Avoids collisions with
        # /tmp/janus_overlayfs which may be root-owned from older runs.
        if janus_root is not None:
            self._janus_root = Path(janus_root)
        elif os.environ.get("JANUS_ROOT"):
            self._janus_root = Path(os.environ["JANUS_ROOT"])
        else:
            uid = os.getuid()
            self._janus_root = Path(f"/tmp/janus_overlayfs_u{uid}")
        self._janus_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        # Backward-compatible attribute used by existing test fixtures.
        self.JANUS_ROOT = self._janus_root

        # Detect and cache the mount strategy once
        if mount_strategy is not None:
            self._mount_strategy = mount_strategy
        else:
            self._mount_strategy = _detect_mount_strategy()
        self._enable_conflict_detection = bool(enable_conflict_detection)

    @property
    def shim_id(self) -> str:
        return f"overlayfs:{self._base_path}"

    @property
    def base_path(self) -> Path:
        return self._base_path

    @property
    def mount_strategy(self) -> str:
        """The active mount strategy ('fuse-overlayfs', 'unshare', or 'root')."""
        return self._mount_strategy

    # ── Internal helpers ─────────────────────────────────────────────

    @staticmethod
    def _safe_bid(branch_id: Any) -> str:
        """Return a filesystem-safe version of a branch ID.

        Branch IDs for child transactions contain a colon (``parent:child``).
        Colons are special in overlayfs ``lowerdir`` option values (used as
        separators for stacked lowerdirs), so we replace them with underscores.
        """
        return str(branch_id).replace(":", "_")

    # ── ToolShim interface ───────────────────────────────────────────

    def begin(self, txn: TransactionHandle) -> None:
        bid = self._safe_bid(txn.branch_id)
        branch_root = self._janus_root / bid
        upper = branch_root / "upper"
        work = branch_root / "work"
        merged = branch_root / "merged"
        savepoints_dir = branch_root / "savepoints"

        for d in (upper, work, merged, savepoints_dir):
            d.mkdir(parents=True, exist_ok=True)

        # Determine lowerdir: parent's merged (for subtransactions)
        # or the real base directory
        raw_parent_bid = txn.branch_id.parent_id
        if raw_parent_bid:
            parent_bid_safe = self._safe_bid(raw_parent_bid)
            if parent_bid_safe in self._mounts:
                lowerdir = str(self._mounts[parent_bid_safe].merged)
            else:
                lowerdir = str(self._base_path)
        else:
            lowerdir = str(self._base_path)

        self._mount_overlay(lowerdir, str(upper), str(work), str(merged))

        self._mounts[bid] = _MountInfo(
            branch_id=bid,
            lowerdir=Path(lowerdir),
            upper=upper,
            work=work,
            merged=merged,
            savepoints_dir=savepoints_dir,
            snapshot_ts=txn.snapshot_ts,
        )
        logger.info(
            "OverlayFS mounted for branch %s (merged=%s, strategy=%s)",
            bid, merged, self._mount_strategy,
        )

    def prepare(self, txn: TransactionHandle) -> Vote:
        """Check for conflicts: files modified on base since txn start."""
        if not self._enable_conflict_detection:
            return Vote.COMMIT
        bid = self._safe_bid(txn.branch_id)
        info = self._mounts.get(bid)
        if info is None:
            return Vote.ABORT

        conflicts = self._detect_conflicts(info)
        if conflicts:
            logger.warning(
                "OverlayFS conflicts detected for branch %s: %s",
                bid,
                conflicts,
            )
            return Vote.ABORT
        return Vote.COMMIT

    def commit(self, txn: TransactionHandle) -> None:
        bid = self._safe_bid(txn.branch_id)
        info = self._mounts.get(bid)
        if info is None:
            return

        # Unmount first so we can walk the raw upperdir
        self._unmount(info.merged)

        # Merge upper → base
        self._merge_upper_to_base(info.upper, self._base_path)

        # Cleanup
        self._cleanup(bid)
        logger.info("OverlayFS committed for branch %s", bid)

    def abort(self, txn: TransactionHandle) -> None:
        bid = self._safe_bid(txn.branch_id)
        info = self._mounts.get(bid)
        if info is None:
            return

        self._unmount(info.merged)
        self._cleanup(bid)
        logger.info("OverlayFS aborted for branch %s", bid)

    def savepoint(self, txn: TransactionHandle, sp: Savepoint) -> Any:
        bid = self._safe_bid(txn.branch_id)
        info = self._mounts.get(bid)
        if info is None:
            raise OverlayFSError(f"No mount for branch {bid}")

        archive = info.savepoints_dir / f"{sp.name}.tar"

        # Sync filesystem caches before snapshot
        subprocess.run(["sync"], check=True)

        # Archive the upper dir contents
        subprocess.run(
            ["tar", "cf", str(archive), "-C", str(info.upper), "."],
            check=True,
        )
        logger.info(
            "OverlayFS savepoint '%s' archived for branch %s",
            sp.name,
            bid,
        )
        return str(archive)

    def rollback_to_savepoint(
        self, txn: TransactionHandle, sp: Savepoint
    ) -> None:
        bid = self._safe_bid(txn.branch_id)
        info = self._mounts.get(bid)
        if info is None:
            raise OverlayFSError(f"No mount for branch {bid}")

        archive_path = sp.shim_snapshots.get(self.shim_id)
        if not archive_path or not Path(archive_path).exists():
            raise OverlayFSError(
                f"Savepoint archive not found: {archive_path}"
            )

        # Must unmount before modifying upper
        self._unmount(info.merged)

        # Clear upper and restore from archive
        shutil.rmtree(info.upper)
        info.upper.mkdir(parents=True)
        subprocess.run(
            ["tar", "xf", archive_path, "-C", str(info.upper)],
            check=True,
        )

        # Clear workdir (OverlayFS requires empty workdir on mount)
        shutil.rmtree(info.work)
        info.work.mkdir(parents=True)

        # Remount
        self._mount_overlay(
            str(info.lowerdir),
            str(info.upper),
            str(info.work),
            str(info.merged),
        )

        # Delete savepoint archives that came after this one
        for f in info.savepoints_dir.iterdir():
            if f.name != Path(archive_path).name and f.suffix == ".tar":
                # Only delete if created after target savepoint
                if f.stat().st_mtime > sp.timestamp:
                    f.unlink()

        logger.info(
            "OverlayFS rolled back to savepoint '%s' for branch %s",
            sp.name,
            bid,
        )

    def get_changes(self, txn: TransactionHandle) -> list[ChangeRecord]:
        bid = self._safe_bid(txn.branch_id)
        info = self._mounts.get(bid)
        if info is None:
            return []

        changes: list[ChangeRecord] = []
        upper = info.upper

        for root, dirs, files in os.walk(upper):
            root_path = Path(root)
            rel_root = root_path.relative_to(upper)

            for fname in files:
                fpath = root_path / fname
                rel = rel_root / fname

                if self._is_whiteout(fpath):
                    # Whiteout = delete
                    original_name = fname
                    if fname.startswith(".wh."):
                        original_name = fname[4:]
                    changes.append(
                        ChangeRecord(
                            shim_id=self.shim_id,
                            resource_id=str(rel_root / original_name),
                            change_type=ChangeType.DELETE,
                        )
                    )
                else:
                    # Modified or created file
                    base_file = self._base_path / rel
                    ct = (
                        ChangeType.UPDATE
                        if base_file.exists()
                        else ChangeType.CREATE
                    )
                    changes.append(
                        ChangeRecord(
                            shim_id=self.shim_id,
                            resource_id=str(rel),
                            change_type=ct,
                        )
                    )

            # Check for opaque directory markers
            for dname in dirs:
                dpath = root_path / dname
                if self._is_opaque_dir(dpath):
                    changes.append(
                        ChangeRecord(
                            shim_id=self.shim_id,
                            resource_id=str(rel_root / dname),
                            change_type=ChangeType.UPDATE,
                        )
                    )

        return changes

    # ── Public helpers ───────────────────────────────────────────────

    def get_working_directory(self, txn: TransactionHandle) -> Path:
        """Return the merged directory — the POSIX-visible working dir.

        All tool operations should use this path. Unix tools work
        unmodified on it.
        """
        bid = self._safe_bid(txn.branch_id)
        info = self._mounts.get(bid)
        if info is None:
            raise OverlayFSError(f"No mount for branch {bid}")
        return info.merged

    def is_mounted(self, txn: TransactionHandle) -> bool:
        bid = self._safe_bid(txn.branch_id)
        return bid in self._mounts

    # ── Subtransaction support ───────────────────────────────────────

    def begin_child(
        self, child_txn: TransactionHandle, parent_txn: TransactionHandle
    ) -> None:
        """Mount a layered overlay for a child subtransaction.

        The parent's merged directory becomes the child's lowerdir,
        creating a visibility chain: child sees parent + own writes.
        """
        parent_bid = self._safe_bid(parent_txn.branch_id)
        child_bid = self._safe_bid(child_txn.branch_id)
        parent_info = self._mounts.get(parent_bid)
        if parent_info is None:
            raise OverlayFSError(
                f"Parent mount not found for branch {parent_bid}"
            )

        branch_root = self._janus_root / child_bid
        upper = branch_root / "upper"
        work = branch_root / "work"
        merged = branch_root / "merged"
        savepoints_dir = branch_root / "savepoints"

        for d in (upper, work, merged, savepoints_dir):
            d.mkdir(parents=True, exist_ok=True)

        # Child's lowerdir = parent's merged
        lowerdir = str(parent_info.merged)
        self._mount_overlay(lowerdir, str(upper), str(work), str(merged))

        self._mounts[child_bid] = _MountInfo(
            branch_id=child_bid,
            lowerdir=Path(lowerdir),
            upper=upper,
            work=work,
            merged=merged,
            savepoints_dir=savepoints_dir,
            snapshot_ts=child_txn.snapshot_ts,
        )
        logger.info(
            "OverlayFS child mounted: %s (parent=%s)", child_bid, parent_bid
        )

    def commit_child(
        self, child_txn: TransactionHandle, parent_txn: TransactionHandle
    ) -> None:
        """Merge child overlay into parent: child upper → parent upper.

        After merge, the parent is remounted to see the new files.
        """
        child_bid = self._safe_bid(child_txn.branch_id)
        parent_bid = self._safe_bid(parent_txn.branch_id)
        child_info = self._mounts.get(child_bid)
        parent_info = self._mounts.get(parent_bid)

        if child_info is None:
            return
        if parent_info is None:
            raise OverlayFSError(f"Parent mount not found: {parent_bid}")

        # Unmount child first
        self._unmount(child_info.merged)

        # Merge child's upper into parent's upper
        self._merge_upper_to_base(child_info.upper, parent_info.upper)

        # Clean up child
        self._cleanup(child_bid)

        # Remount parent to see the merged files
        self._unmount(parent_info.merged)
        # Clear parent workdir (required for remount)
        shutil.rmtree(parent_info.work)
        parent_info.work.mkdir(parents=True)
        self._mount_overlay(
            str(parent_info.lowerdir),
            str(parent_info.upper),
            str(parent_info.work),
            str(parent_info.merged),
        )

        logger.info(
            "OverlayFS child committed: %s → parent %s",
            child_bid,
            parent_bid,
        )

    def abort_child(
        self, child_txn: TransactionHandle, parent_txn: TransactionHandle
    ) -> None:
        """Discard child overlay — umount + rm -rf. Parent unaffected."""
        child_bid = self._safe_bid(child_txn.branch_id)
        child_info = self._mounts.get(child_bid)
        if child_info is None:
            return

        self._unmount(child_info.merged)
        self._cleanup(child_bid)
        logger.info(
            "OverlayFS child aborted: %s (parent=%s)",
            child_bid,
            str(parent_txn.branch_id),
        )

    # ── Private helpers ──────────────────────────────────────────────

    def _mount_overlay(
        self,
        lowerdir: str,
        upperdir: str,
        workdir: str,
        merged: str,
    ) -> None:
        """Mount an overlay using the detected strategy."""
        opts = f"lowerdir={lowerdir},upperdir={upperdir},workdir={workdir}"

        if self._mount_strategy == _STRATEGY_FUSE:
            result = subprocess.run(
                ["fuse-overlayfs", "-o", opts, merged],
                capture_output=True,
                text=True,
            )
        elif self._mount_strategy == _STRATEGY_UNSHARE:
            result = subprocess.run(
                [
                    "unshare", "-rm", "--",
                    "mount", "-t", "overlay", "overlay",
                    "-o", opts,
                    merged,
                ],
                capture_output=True,
                text=True,
            )
        else:  # _STRATEGY_ROOT
            result = subprocess.run(
                [
                    "mount", "-t", "overlay", "overlay",
                    "-o", opts,
                    merged,
                ],
                capture_output=True,
                text=True,
            )

        if result.returncode != 0:
            raise OverlayFSError(
                f"Failed to mount OverlayFS ({self._mount_strategy}): "
                f"{result.stderr.strip()}"
            )

    def _unmount(self, merged: Path) -> None:
        """Unmount an overlay mount point."""
        if self._mount_strategy == _STRATEGY_FUSE:
            # Try fusermount3 first, then fusermount (older systems)
            for cmd in [["fusermount3", "-u"], ["fusermount", "-u"]]:
                if shutil.which(cmd[0]):
                    result = subprocess.run(
                        cmd + [str(merged)],
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode == 0:
                        return
                    logger.warning(
                        "%s failed for %s: %s", cmd[0], merged, result.stderr
                    )

        # Fallback for root/unshare modes (and fuse fallback)
        result = subprocess.run(
            ["umount", "-l", str(merged)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            logger.warning(
                "Lazy unmount failed for %s: %s", merged, result.stderr
            )
            subprocess.run(
                ["umount", "-f", str(merged)],
                capture_output=True,
                text=True,
            )

    def _cleanup(self, bid: str) -> None:
        info = self._mounts.pop(bid, None)
        if info is None:
            return
        branch_root = self._janus_root / bid
        if branch_root.exists():
            shutil.rmtree(branch_root, ignore_errors=True)

    def cleanup_all(self) -> None:
        """Unmount and remove all active branches."""
        for bid in list(self._mounts.keys()):
            self._unmount(bid)
            self._cleanup(bid)

    def _merge_upper_to_base(self, upper: Path, base: Path) -> None:
        """Walk the upperdir and apply changes to the base directory.

        Handles:
        - Regular files: copy to base
        - Whiteout char devices (0,0): delete corresponding base file
        - Whiteout .wh.* files: delete corresponding base file
        - Opaque directories: replace base directory entirely
        """
        for root, dirs, files in os.walk(upper, topdown=True):
            root_path = Path(root)
            rel_root = root_path.relative_to(upper)
            base_root = base / rel_root

            # Ensure target directory exists
            base_root.mkdir(parents=True, exist_ok=True)

            # Handle opaque directory markers — replace entire dir
            if self._is_opaque_dir(root_path) and rel_root != Path("."):
                if base_root.exists():
                    shutil.rmtree(base_root)
                # Opaque upper dirs can contain overlay metadata entries
                # (e.g. .wh..opq) and special files. Skip those while copying
                # user-visible content into base.
                shutil.copytree(
                    root_path,
                    base_root,
                    ignore=self._overlay_copy_ignore,
                )
                dirs.clear()  # Don't descend further
                continue

            for fname in files:
                src = root_path / fname
                if self._is_overlay_metadata_name(fname):
                    logger.debug("Skipping overlay metadata entry: %s", src)
                    continue
                if self._is_whiteout(src):
                    # Delete corresponding file in base
                    original = fname[4:] if fname.startswith(".wh.") else fname
                    target = base_root / original
                    if target.exists():
                        if target.is_dir():
                            shutil.rmtree(target)
                        else:
                            target.unlink()
                    logger.debug("Whiteout applied: %s", target)
                else:
                    try:
                        mode = src.lstat().st_mode
                        if (
                            stat.S_ISCHR(mode)
                            or stat.S_ISBLK(mode)
                            or stat.S_ISSOCK(mode)
                            or stat.S_ISFIFO(mode)
                        ):
                            logger.debug(
                                "Skipping special non-regular file during merge: %s",
                                src,
                            )
                            continue
                    except OSError:
                        logger.debug(
                            "Skipping unreadable source entry during merge: %s",
                            src,
                        )
                        continue
                    # Copy file to base
                    dst = base_root / fname
                    shutil.copy2(src, dst)
                    logger.debug("Merged file: %s", dst)

    @staticmethod
    def _is_overlay_metadata_name(name: str) -> bool:
        """Return True for overlay-internal metadata entries."""
        return (
            name == ".wh..wh..opq"
            or name == ".wh..opq"
            or name.startswith(".wh..wh.")
        )

    def _overlay_copy_ignore(self, dirpath: str, names: list[str]) -> list[str]:
        """Ignore overlay metadata and special entries in opaque copytree."""
        ignored: list[str] = []
        root = Path(dirpath)
        for name in names:
            if self._is_overlay_metadata_name(name):
                ignored.append(name)
                continue
            path = root / name
            try:
                mode = path.lstat().st_mode
            except OSError:
                ignored.append(name)
                continue
            if (
                stat.S_ISCHR(mode)
                or stat.S_ISBLK(mode)
                or stat.S_ISSOCK(mode)
                or stat.S_ISFIFO(mode)
            ):
                ignored.append(name)
        return ignored

    def _is_whiteout(self, path: Path) -> bool:
        """Check if a path is an OverlayFS whiteout entry."""
        try:
            st = path.lstat()
            # Character device with major/minor 0,0
            if stat.S_ISCHR(st.st_mode) and os.major(st.st_rdev) == 0 and os.minor(st.st_rdev) == 0:
                return True
        except OSError:
            pass
        # Fallback: check for .wh. prefix (used by some overlay implementations)
        return path.name.startswith(".wh.") and not self._is_overlay_metadata_name(
            path.name
        )

    def _is_opaque_dir(self, path: Path) -> bool:
        """Check if a directory is an opaque overlay directory."""
        try:
            val = os.getxattr(str(path), b"trusted.overlay.opaque")
            return val == b"y"
        except (OSError, AttributeError):
            pass
        # Fallback markers observed across overlay implementations.
        return (
            (path / ".wh..wh..opq").exists()
            or (path / ".wh..opq").exists()
        )

    def _detect_conflicts(self, info: _MountInfo) -> list[str]:
        """Detect base files modified since the transaction start."""
        conflicts: list[str] = []
        upper = info.upper

        for root, _dirs, files in os.walk(upper):
            root_path = Path(root)
            rel_root = root_path.relative_to(upper)

            for fname in files:
                if self._is_whiteout(root_path / fname):
                    original = fname[4:] if fname.startswith(".wh.") else fname
                    rel = rel_root / original
                else:
                    rel = rel_root / fname

                base_file = self._base_path / rel
                if base_file.exists():
                    try:
                        mtime = base_file.stat().st_mtime
                        if mtime > info.snapshot_ts:
                            conflicts.append(str(rel))
                    except OSError:
                        pass
        return conflicts


class _MountInfo:
    """Internal state for a mounted overlay branch."""

    __slots__ = (
        "branch_id",
        "lowerdir",
        "upper",
        "work",
        "merged",
        "savepoints_dir",
        "snapshot_ts",
    )

    def __init__(
        self,
        branch_id: str,
        lowerdir: Path,
        upper: Path,
        work: Path,
        merged: Path,
        savepoints_dir: Path,
        snapshot_ts: float,
    ):
        self.branch_id = branch_id
        self.lowerdir = lowerdir
        self.upper = upper
        self.work = work
        self.merged = merged
        self.savepoints_dir = savepoints_dir
        self.snapshot_ts = snapshot_ts
