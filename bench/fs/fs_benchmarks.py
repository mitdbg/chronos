from __future__ import annotations

import argparse
import csv
import errno
import json
import mmap
import os
import random
import shutil
import sqlite3
import subprocess
import tempfile
import time
from array import array
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Iterator

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from chronos_core.branching import ChronosBranchContext
from chronos_core.workspace.chronosfs import ChronosFSStore


BACKENDS = ("chronosfs", "overlayfs", "xfs", "btrfs", "turso")
DEFAULT_BACKENDS = "chronosfs,overlayfs,btrfs,turso"
BACKEND_COLORS = {
    "chronosfs": "#2563eb",
    "overlayfs": "#16a34a",
    "xfs": "#f97316",
    "btrfs": "#a855f7",
    "turso": "#111827",
}
BACKEND_LIGHT_COLORS = {
    "chronosfs": "#60a5fa",
    "overlayfs": "#86efac",
    "xfs": "#fdba74",
    "btrfs": "#d8b4fe",
    "turso": "#9ca3af",
}
SQLITE_SYNCHRONOUS_MODES = {"OFF", "NORMAL", "FULL", "EXTRA"}
FILE_WRITE_ALIGNMENT = 4096
DIRECT_IO_ALIGNMENT = 4096
DIRECT_IO_CACHE_SIZE_KIB = 64 * 1024
PLOT_LOG_SCALE_RATIO_THRESHOLD = 50.0
MICRO_PHASES = (
    "branch_create",
    "branch_delete",
    "file_read",
    "file_write",
    "file_read_directio",
    "file_write_directio",
)
PHASE_LABELS = {
    "branch_create": "branch create",
    "branch_delete": "branch delete",
    "file_read": "file read",
    "file_write": "file write",
    "file_read_directio": "file read (directio)",
    "file_write_directio": "file write (directio)",
}
CSV_FIELDS = [
    "workload",
    "backend",
    "phase",
    "parameter",
    "repeat",
    "status",
    "elapsed_ms",
    "ops",
    "bytes",
    "storage_delta_bytes",
    "ops_per_sec",
    "mb_per_sec",
    "op_latency_p10_ms",
    "op_latency_p50_ms",
    "op_latency_p99_ms",
    "error",
    "details",
]
SUMMARY_FIELDS = [
    "workload",
    "backend",
    "phase",
    "parameter",
    "status",
    "repetitions",
    "median_elapsed_ms",
    "p10_elapsed_ms",
    "p90_elapsed_ms",
    "avg_elapsed_ms",
    "median_ms_per_op",
    "p10_ms_per_op",
    "p90_ms_per_op",
    "p99_ms_per_op",
    "median_ops_per_sec",
    "median_mb_per_sec",
    "p10_mb_per_sec",
    "p90_mb_per_sec",
    "median_storage_delta_bytes",
    "p10_storage_delta_bytes",
    "p90_storage_delta_bytes",
    "error",
]


@dataclass(frozen=True)
class BenchConfig:
    backends: tuple[str, ...]
    repeats: int
    branch_iterations: int
    file_count: int
    io_duration_seconds: float
    io_size: int
    file_size: int
    directio_cache_size: int
    directio_sqlite_synchronous: str
    cow_file_size: int
    cow_write_sizes: tuple[int, ...]
    run_micro: bool
    run_cow: bool
    compile_repeats: int
    compile_jobs: int
    run_compile: bool
    redis_repo_url: str
    redis_ref: str
    redis_source: Path | None
    keep_workdir: bool
    output_dir: Path
    chronosfs_sqlite_synchronous: str
    chronosfs_sqlite_wal_autocheckpoint_pages: int | None
    overlayfs_root: Path | None
    xfs_root: Path | None
    btrfs_root: Path | None
    turso_root: Path | None
    turso_agentfs_bin: str
    turso_sqlite_synchronous: str


class UnsupportedBackend(RuntimeError):
    pass


def normalize_sqlite_synchronous(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in SQLITE_SYNCHRONOUS_MODES:
        raise argparse.ArgumentTypeError(
            "expected one of: " + ", ".join(sorted(SQLITE_SYNCHRONOUS_MODES))
        )
    return normalized


def sqlite_cache_size_kib(cache_size_bytes: int | None) -> int | None:
    if cache_size_bytes is None:
        return None
    return max(1, cache_size_bytes // 1024)


def configure_benchmark_sqlite(
    db: Any,
    synchronous: str,
    *,
    cache_size_bytes: int | None = None,
    wal_autocheckpoint_pages: int | None = None,
) -> None:
    if getattr(db, "dialect", None) != "sqlite":
        return
    db.execute("PRAGMA journal_mode=WAL")
    db.execute(f"PRAGMA synchronous={normalize_sqlite_synchronous(synchronous)}")
    if wal_autocheckpoint_pages is not None:
        db.execute(f"PRAGMA wal_autocheckpoint={max(0, wal_autocheckpoint_pages)}")
    if (cache_size := sqlite_cache_size_kib(cache_size_bytes)) is not None:
        db.execute(f"PRAGMA cache_size=-{cache_size}")
    db.execute("PRAGMA fullfsync=OFF")
    db.execute("PRAGMA checkpoint_fullfsync=OFF")


def configure_agentfs_sqlite_file(
    db_path: Path,
    synchronous: str,
    *,
    cache_size_bytes: int | None = None,
) -> dict[str, Any]:
    normalized = normalize_sqlite_synchronous(synchronous)
    with sqlite3.connect(db_path) as conn:
        # AgentFS opens the database with Turso/libSQL.  Leaving a CPython
        # SQLite WAL shared-memory file behind makes that opener reject the
        # database as locked, so keep the initialized file in rollback mode.
        journal_mode = conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
        conn.execute(f"PRAGMA synchronous={normalized}")
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        requested_cache_pages = None
        if (cache_size := sqlite_cache_size_kib(cache_size_bytes)) is not None:
            requested_cache_pages = max(
                1,
                (cache_size_bytes + page_size - 1) // page_size,
            )
            # cache_size is connection-local.  default_cache_size persists
            # the equivalent page count for AgentFS's later connections.
            conn.execute(f"PRAGMA default_cache_size={requested_cache_pages}")
        effective_synchronous = conn.execute("PRAGMA synchronous").fetchone()[0]
        effective_cache_size = conn.execute("PRAGMA cache_size").fetchone()[0]
    return {
        "journal_mode": str(journal_mode),
        "requested_synchronous": normalized,
        "effective_synchronous": effective_synchronous,
        "requested_cache_size_kib": sqlite_cache_size_kib(cache_size_bytes),
        "requested_cache_pages": requested_cache_pages,
        "effective_cache_size": effective_cache_size,
    }


class FsBackend:
    name: str

    def setup(self, workdir: Path, *, file_count: int, file_size: int) -> None:
        raise NotImplementedError

    def cleanup(self) -> None:
        pass

    def create_branch(self, branch_id: str, from_branch: str = "main") -> None:
        raise NotImplementedError

    def delete_branch(self, branch_id: str) -> None:
        raise NotImplementedError

    @contextmanager
    def checkout_path(self, branch_id: str) -> Iterator[Path]:
        raise NotImplementedError

    def import_tree(self, source: Path) -> None:
        raise NotImplementedError

    def source_parent(self, workdir: Path) -> Path:
        path = workdir / "source"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def storage_bytes(self) -> int:
        raise NotImplementedError


class ChronosFSBenchBackend(FsBackend):
    name = "chronosfs"

    def __init__(
        self,
        block_size: int = 4096,
        sqlite_synchronous: str = "OFF",
        sqlite_cache_size_bytes: int | None = None,
        sqlite_wal_autocheckpoint_pages: int | None = None,
    ):
        self.block_size = block_size
        self.sqlite_synchronous = sqlite_synchronous
        self.sqlite_cache_size_bytes = sqlite_cache_size_bytes
        self.sqlite_wal_autocheckpoint_pages = sqlite_wal_autocheckpoint_pages
        self.context: ChronosBranchContext | None = None
        self.store: ChronosFSStore | None = None
        self.db_path: Path | None = None
        self._previous_native_sync: str | None = None
        self._previous_native_cache_size: str | None = None
        self._previous_native_wal_autocheckpoint: str | None = None

    def setup(self, workdir: Path, *, file_count: int, file_size: int) -> None:
        db_path = workdir / "chronosfs.sqlite"
        self.db_path = db_path
        self._apply_native_sqlite_env()
        try:
            self.context = ChronosBranchContext.connect(
                f"sqlite:///{db_path}",
                backend="interval",
            )
            configure_benchmark_sqlite(
                self.context.db,
                self.sqlite_synchronous,
                cache_size_bytes=self.sqlite_cache_size_bytes,
                wal_autocheckpoint_pages=self.sqlite_wal_autocheckpoint_pages,
            )
            self.store = ChronosFSStore(self.context, block_size=self.block_size)
            self.store.ensure()
            seed_chronosfs(self.store, "main", file_count=file_count, file_size=file_size)
        except Exception:
            self._restore_native_sqlite_env()
            raise

    def cleanup(self) -> None:
        if self.context is not None:
            self.context.close()
        self._restore_native_sqlite_env()

    def create_branch(self, branch_id: str, from_branch: str = "main") -> None:
        self._store().create_branch(branch_id, from_branch=from_branch)

    def delete_branch(self, branch_id: str) -> None:
        self._store().delete_branch(branch_id)

    @contextmanager
    def checkout_path(self, branch_id: str) -> Iterator[Path]:
        if self.db_path is None:
            raise RuntimeError("backend is not set up")
        with mounted_chronosfs(
            self._store(),
            branch_id,
            database_url=f"sqlite:///{self.db_path}",
            sqlite_synchronous=self.sqlite_synchronous,
            sqlite_cache_size_bytes=self.sqlite_cache_size_bytes,
            sqlite_wal_autocheckpoint_pages=self.sqlite_wal_autocheckpoint_pages,
        ) as mountpoint:
            yield mountpoint

    def import_tree(self, source: Path) -> None:
        self._store().import_tree("main", source)

    def storage_bytes(self) -> int:
        if self.db_path is None:
            return 0
        total = 0
        for path in (
            self.db_path,
            self.db_path.with_name(self.db_path.name + "-wal"),
            self.db_path.with_name(self.db_path.name + "-shm"),
        ):
            if path.exists():
                total += path.stat().st_size
        return total

    def _store(self) -> ChronosFSStore:
        if self.store is None:
            raise RuntimeError("backend is not set up")
        return self.store

    def _apply_native_sqlite_env(self) -> None:
        self._previous_native_sync = os.environ.get("CHRONOS_NATIVE_SQLITE_SYNCHRONOUS")
        self._previous_native_cache_size = os.environ.get(
            "CHRONOS_NATIVE_SQLITE_CACHE_SIZE_KIB"
        )
        self._previous_native_wal_autocheckpoint = os.environ.get(
            "CHRONOS_NATIVE_SQLITE_WAL_AUTOCHECKPOINT_PAGES"
        )
        os.environ["CHRONOS_NATIVE_SQLITE_SYNCHRONOUS"] = normalize_sqlite_synchronous(
            self.sqlite_synchronous
        )
        if (cache_size := sqlite_cache_size_kib(self.sqlite_cache_size_bytes)) is not None:
            os.environ["CHRONOS_NATIVE_SQLITE_CACHE_SIZE_KIB"] = str(cache_size)
        if self.sqlite_wal_autocheckpoint_pages is not None:
            os.environ["CHRONOS_NATIVE_SQLITE_WAL_AUTOCHECKPOINT_PAGES"] = str(
                self.sqlite_wal_autocheckpoint_pages
            )

    def _restore_native_sqlite_env(self) -> None:
        restore_env("CHRONOS_NATIVE_SQLITE_SYNCHRONOUS", self._previous_native_sync)
        restore_env(
            "CHRONOS_NATIVE_SQLITE_CACHE_SIZE_KIB",
            self._previous_native_cache_size,
        )
        restore_env(
            "CHRONOS_NATIVE_SQLITE_WAL_AUTOCHECKPOINT_PAGES",
            self._previous_native_wal_autocheckpoint,
        )


class OverlayFSBenchBackend(FsBackend):
    name = "overlayfs"

    def __init__(self, root: Path | None = None):
        self.root_base = root
        self.base_dir: Path | None = None
        self.root: Path | None = None
        self.state_dir: Path | None = None
        self.branches_dir: Path | None = None
        self._mounted_branches: set[str] = set()
        self._owns_base_dir = False

    def setup(self, workdir: Path, *, file_count: int, file_size: int) -> None:
        if shutil.which("mount") is None or shutil.which("umount") is None:
            raise UnsupportedBackend("kernel overlayfs backend requires mount and umount")
        if os.geteuid() != 0 and shutil.which("sudo") is None:
            raise UnsupportedBackend("kernel overlayfs backend requires sudo")
        base_dir = self._select_base_dir(workdir)
        try:
            base_dir.mkdir(parents=True, exist_ok=True)
            self.base_dir = base_dir
            root = base_dir / "overlay-root"
            root.mkdir(parents=True, exist_ok=True)
            seed_posix_tree(root, file_count=file_count, file_size=file_size)
            self.root = root
            self.state_dir = base_dir / "overlay-state"
            self.branches_dir = self.state_dir / "branches"
            self.branches_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            if self._owns_base_dir:
                remove_tree(base_dir)
            raise

    def cleanup(self) -> None:
        for branch_id in list(self._mounted_branches):
            with suppress_errors():
                self._unmount_branch(branch_id)
        if self.base_dir is not None and self._owns_base_dir:
            remove_tree(self.base_dir)

    def create_branch(self, branch_id: str, from_branch: str = "main") -> None:
        if from_branch != "main":
            raise UnsupportedBackend(
                "kernel overlayfs baseline currently branches from main only"
            )
        branch_dir = self._branch_dir(branch_id)
        if branch_dir.exists():
            raise FileExistsError(f"branch already exists: {branch_id}")
        (branch_dir / "upper").mkdir(parents=True)
        (branch_dir / "work").mkdir()
        (branch_dir / "merged").mkdir()

    def delete_branch(self, branch_id: str) -> None:
        if branch_id == "main":
            raise ValueError("cannot delete main branch")
        with suppress_errors():
            self._unmount_branch(branch_id)
        remove_tree(self._branch_dir(branch_id))

    @contextmanager
    def checkout_path(self, branch_id: str) -> Iterator[Path]:
        self._mount_branch(branch_id)
        try:
            yield self._branch_dir(branch_id) / "merged"
        finally:
            self._unmount_branch(branch_id)

    def import_tree(self, source: Path) -> None:
        # Overlayfs can import an existing tree by using it directly as the
        # lowerdir. This measures the filesystem's native attach/COW behavior
        # instead of pre-copying bytes into a synthetic lower tree.
        self.root = source.resolve()

    def storage_bytes(self) -> int:
        if self.state_dir is None:
            return 0
        return directory_size(self.state_dir)

    def _root(self) -> Path:
        if self.root is None:
            raise RuntimeError("backend is not set up")
        return self.root

    def _select_base_dir(self, workdir: Path) -> Path:
        if self.root_base is None:
            self._owns_base_dir = False
            return workdir
        root = self.root_base
        root.mkdir(parents=True, exist_ok=True)
        self._owns_base_dir = True
        return Path(tempfile.mkdtemp(prefix="chronos-overlayfs-bench-", dir=root))

    def _branch_dir(self, branch_id: str) -> Path:
        if self.branches_dir is None:
            raise RuntimeError("backend is not set up")
        return self.branches_dir / branch_id

    def _mount_branch(self, branch_id: str) -> None:
        branch_dir = self._branch_dir(branch_id)
        if not branch_dir.exists():
            raise FileNotFoundError(f"branch does not exist: {branch_id}")
        merged = branch_dir / "merged"
        if os.path.ismount(merged):
            self._mounted_branches.add(branch_id)
            return
        lowerdir = str(self._root())
        upperdir = str(branch_dir / "upper")
        workdir = str(branch_dir / "work")
        options = f"lowerdir={lowerdir},upperdir={upperdir},workdir={workdir}"
        run_privileged_checked(
            ["mount", "-t", "overlay", "overlay", "-o", options, str(merged)],
            cwd=branch_dir,
            timeout=30,
        )
        self._mounted_branches.add(branch_id)

    def _unmount_branch(self, branch_id: str) -> None:
        merged = self._branch_dir(branch_id) / "merged"
        if os.path.ismount(merged):
            try:
                run_privileged_checked(
                    ["umount", str(merged)],
                    cwd=merged.parent,
                    timeout=120,
                )
            except subprocess.TimeoutExpired:
                if os.path.ismount(merged):
                    run_privileged_checked(
                        ["umount", "--lazy", str(merged)],
                        cwd=merged.parent,
                        timeout=30,
                    )
        self._mounted_branches.discard(branch_id)


class XFSReflinkBenchBackend(FsBackend):
    name = "xfs"

    def __init__(self, root: Path | None = None):
        self.root_base = root
        self.base_dir: Path | None = None
        self.branches_dir: Path | None = None
        self._owns_base_dir = False

    def setup(self, workdir: Path, *, file_count: int, file_size: int) -> None:
        if shutil.which("cp") is None:
            raise UnsupportedBackend("cp is required for XFS reflink branching")
        base_dir = self._select_base_dir(workdir)
        try:
            base_dir.mkdir(parents=True, exist_ok=True)
            fstype = filesystem_type(base_dir)
            if fstype != "xfs":
                raise UnsupportedBackend(
                    f"xfs backend requires an XFS workdir, got {fstype or 'unknown'} at {base_dir}"
                )
            ensure_loop_direct_io_for_path(base_dir)
            self._verify_reflink(base_dir)
            self.base_dir = base_dir
            self.branches_dir = base_dir / "branches"
            self.branches_dir.mkdir(parents=True, exist_ok=True)
            seed_posix_tree(
                self._branch_path("main"),
                file_count=file_count,
                file_size=file_size,
            )
        except Exception:
            if self._owns_base_dir:
                shutil.rmtree(base_dir, ignore_errors=True)
            raise

    def cleanup(self) -> None:
        if self.base_dir is not None and self._owns_base_dir:
            shutil.rmtree(self.base_dir, ignore_errors=True)

    def create_branch(self, branch_id: str, from_branch: str = "main") -> None:
        src = self._branch_path(from_branch)
        dst = self._branch_path(branch_id)
        if dst.exists():
            raise FileExistsError(f"branch already exists: {branch_id}")
        dst.mkdir(parents=True)
        try:
            run_checked(
                ["cp", "-a", "--reflink=always", f"{src}/.", str(dst)],
                cwd=src.parent,
                timeout=300,
            )
        except Exception:
            shutil.rmtree(dst, ignore_errors=True)
            raise

    def delete_branch(self, branch_id: str) -> None:
        if branch_id == "main":
            raise ValueError("cannot delete main branch")
        shutil.rmtree(self._branch_path(branch_id))

    @contextmanager
    def checkout_path(self, branch_id: str) -> Iterator[Path]:
        yield self._branch_path(branch_id)

    def import_tree(self, source: Path) -> None:
        main = self._branch_path("main")
        if main.exists():
            shutil.rmtree(main)
        copytree_reflink(source, main)

    def storage_bytes(self) -> int:
        if self.base_dir is None:
            return 0
        return filesystem_used_bytes(self.base_dir)

    def source_parent(self, workdir: Path) -> Path:
        if self.base_dir is None:
            return super().source_parent(workdir)
        path = self.base_dir / "import-source"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _select_base_dir(self, workdir: Path) -> Path:
        if self.root_base is None:
            self._owns_base_dir = False
            return workdir
        root = self.root_base
        root.mkdir(parents=True, exist_ok=True)
        self._owns_base_dir = True
        return Path(tempfile.mkdtemp(prefix="chronos-xfs-bench-", dir=root))

    def _verify_reflink(self, directory: Path) -> None:
        probe_dir = Path(tempfile.mkdtemp(prefix="reflink-probe-", dir=directory))
        try:
            src = probe_dir / "src"
            dst = probe_dir / "dst"
            src.write_bytes(b"chronos-xfs-reflink-probe")
            run_checked(
                ["cp", "--reflink=always", str(src), str(dst)],
                cwd=probe_dir,
                timeout=30,
            )
        except Exception as exc:
            raise UnsupportedBackend(
                f"XFS reflink clone is not available at {directory}: {exc}"
            ) from exc
        finally:
            shutil.rmtree(probe_dir, ignore_errors=True)

    def _branch_path(self, branch_id: str) -> Path:
        if self.branches_dir is None:
            raise RuntimeError("backend is not set up")
        return self.branches_dir / branch_id


class BtrfsSubvolumeBenchBackend(FsBackend):
    name = "btrfs"

    def __init__(self, root: Path | None = None):
        self.root_base = root
        self.base_dir: Path | None = None
        self.branches_dir: Path | None = None
        self._owns_base_dir = False

    def setup(self, workdir: Path, *, file_count: int, file_size: int) -> None:
        if shutil.which("btrfs") is None:
            raise UnsupportedBackend("btrfs CLI is not installed")
        base_dir = self._select_base_dir(workdir)
        try:
            base_dir.mkdir(parents=True, exist_ok=True)
            fstype = filesystem_type(base_dir)
            if fstype != "btrfs":
                raise UnsupportedBackend(
                    f"btrfs backend requires a Btrfs workdir, got {fstype or 'unknown'} at {base_dir}"
                )
            ensure_loop_direct_io_for_path(base_dir)
            self.base_dir = base_dir
            self.branches_dir = base_dir / "branches"
            self.branches_dir.mkdir(parents=True, exist_ok=True)
            self._create_subvolume(self._branch_path("main"))
            seed_posix_tree(
                self._branch_path("main"),
                file_count=file_count,
                file_size=file_size,
            )
        except Exception:
            if self._owns_base_dir:
                self._cleanup_owned_base_dir(base_dir)
            raise

    def cleanup(self) -> None:
        if self.base_dir is not None and self._owns_base_dir:
            self._cleanup_owned_base_dir(self.base_dir)

    def create_branch(self, branch_id: str, from_branch: str = "main") -> None:
        src = self._branch_path(from_branch)
        dst = self._branch_path(branch_id)
        if dst.exists():
            raise FileExistsError(f"branch already exists: {branch_id}")
        try:
            run_checked(
                ["btrfs", "subvolume", "snapshot", str(src), str(dst)],
                cwd=src.parent,
                timeout=300,
            )
        except Exception:
            self._remove_path_or_subvolume(dst)
            raise

    def delete_branch(self, branch_id: str) -> None:
        if branch_id == "main":
            raise ValueError("cannot delete main branch")
        self._delete_subvolume(self._branch_path(branch_id))

    @contextmanager
    def checkout_path(self, branch_id: str) -> Iterator[Path]:
        yield self._branch_path(branch_id)

    def import_tree(self, source: Path) -> None:
        main = self._branch_path("main")
        if main.exists():
            self._delete_subvolume(main)
        self._create_subvolume(main)
        copytree_reflink(source, main)

    def storage_bytes(self) -> int:
        if self.base_dir is None:
            return 0
        return filesystem_used_bytes(self.base_dir)

    def source_parent(self, workdir: Path) -> Path:
        if self.base_dir is None:
            return super().source_parent(workdir)
        path = self.base_dir / "import-source"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _select_base_dir(self, workdir: Path) -> Path:
        if self.root_base is None:
            self._owns_base_dir = False
            return workdir
        root = self.root_base
        root.mkdir(parents=True, exist_ok=True)
        self._owns_base_dir = True
        return Path(tempfile.mkdtemp(prefix="chronos-btrfs-bench-", dir=root))

    def _create_subvolume(self, path: Path) -> None:
        if path.exists():
            raise FileExistsError(f"subvolume path already exists: {path}")
        path.parent.mkdir(parents=True, exist_ok=True)
        run_checked(
            ["btrfs", "subvolume", "create", str(path)],
            cwd=path.parent,
            timeout=120,
        )

    def _delete_subvolume(self, path: Path) -> None:
        if not path.exists():
            return
        try:
            run_checked(
                ["btrfs", "subvolume", "delete", str(path)],
                cwd=path.parent,
                timeout=120,
            )
        except Exception:
            self._remove_path_or_subvolume(path)

    def _cleanup_owned_base_dir(self, base_dir: Path) -> None:
        branches = base_dir / "branches"
        if branches.exists():
            for branch in sorted(branches.iterdir(), reverse=True):
                self._delete_subvolume(branch)
        shutil.rmtree(base_dir, ignore_errors=True)

    def _remove_path_or_subvolume(self, path: Path) -> None:
        if not path.exists():
            return
        result = subprocess.run(
            ["btrfs", "subvolume", "delete", str(path)],
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode != 0:
            shutil.rmtree(path, ignore_errors=True)

    def _branch_path(self, branch_id: str) -> Path:
        if self.branches_dir is None:
            raise RuntimeError("backend is not set up")
        return self.branches_dir / branch_id


class TursoAgentFSBenchBackend(FsBackend):
    name = "turso"

    def __init__(
        self,
        root: Path | None = None,
        agentfs_bin: str = "agentfs",
        sqlite_synchronous: str = "OFF",
        sqlite_cache_size_bytes: int | None = None,
    ):
        self.root_base = root
        self.agentfs_bin = agentfs_bin
        self.sqlite_synchronous = sqlite_synchronous
        self.sqlite_cache_size_bytes = sqlite_cache_size_bytes
        self.base_dir: Path | None = None
        self.state_dir: Path | None = None
        self._owns_state_dir = False
        self.sqlite_config: dict[str, Any] = {}

    def setup(self, workdir: Path, *, file_count: int, file_size: int) -> None:
        if shutil.which(self.agentfs_bin) is None:
            raise UnsupportedBackend(
                f"agentfs CLI is not installed: {self.agentfs_bin}"
            )
        state_dir = self._select_state_dir(workdir)
        try:
            state_dir.mkdir(parents=True, exist_ok=True)
            self.state_dir = state_dir
            self.base_dir = state_dir / "base"
            self.base_dir.mkdir(parents=True, exist_ok=True)
            seed_posix_tree(
                self.base_dir,
                file_count=file_count,
                file_size=file_size,
            )
            self._create_agent_db("main")
        except Exception:
            if self._owns_state_dir:
                shutil.rmtree(state_dir, ignore_errors=True)
            self.state_dir = None
            self.base_dir = None
            raise

    def cleanup(self) -> None:
        if self.state_dir is not None and self._owns_state_dir:
            shutil.rmtree(self.state_dir, ignore_errors=True)

    def create_branch(self, branch_id: str, from_branch: str = "main") -> None:
        if from_branch != "main":
            raise UnsupportedBackend(
                "turso AgentFS baseline currently branches from main only"
            )
        self._create_agent_db(branch_id)

    def delete_branch(self, branch_id: str) -> None:
        if branch_id == "main":
            raise ValueError("cannot delete main branch")
        self._remove_agent_db(branch_id)

    @contextmanager
    def checkout_path(self, branch_id: str) -> Iterator[Path]:
        db_path = self._agent_db_path(branch_id)
        if not db_path.exists():
            if branch_id == "main":
                self._create_agent_db("main")
            else:
                raise FileNotFoundError(f"branch does not exist: {branch_id}")
        mountpoint = Path(tempfile.mkdtemp(prefix=f"agentfs-{branch_id}-"))
        proc = subprocess.Popen(
            [
                self.agentfs_bin,
                "mount",
                "--foreground",
                str(self._agent_db_path(branch_id)),
                str(mountpoint),
            ],
            cwd=self._state_dir(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            wait_for_mount(proc, mountpoint, backend_name="Turso AgentFS")
            yield mountpoint
        finally:
            unmount(mountpoint)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.terminate()
                with suppress_errors():
                    proc.wait(timeout=2)
                if proc.poll() is None:
                    proc.kill()
                    with suppress_errors():
                        proc.wait(timeout=2)
            shutil.rmtree(mountpoint, ignore_errors=True)

    def import_tree(self, source: Path) -> None:
        # AgentFS accepts an existing base directory. Point it at the source
        # tree directly so import measures AgentFS initialization over that
        # tree, not a preparatory Python copy.
        self.base_dir = source.resolve()
        agent_dir = self._state_dir() / ".agentfs"
        if agent_dir.exists():
            shutil.rmtree(agent_dir)
        self._create_agent_db("main")

    def storage_bytes(self) -> int:
        if self.state_dir is None:
            return 0
        return directory_size(self.state_dir / ".agentfs")

    def _select_state_dir(self, workdir: Path) -> Path:
        if self.root_base is None:
            self._owns_state_dir = False
            return workdir / "turso-agentfs"
        root = self.root_base
        root.mkdir(parents=True, exist_ok=True)
        self._owns_state_dir = True
        return Path(tempfile.mkdtemp(prefix="chronos-turso-bench-", dir=root))

    def _create_agent_db(self, branch_id: str) -> None:
        state_dir = self._state_dir()
        base_dir = self._base_dir()
        self._remove_agent_db(branch_id)
        run_checked(
            [
                self.agentfs_bin,
                "init",
                "--force",
                "--base",
                str(base_dir),
                self._agent_id(branch_id),
            ],
            cwd=state_dir,
            timeout=120,
        )
        self.sqlite_config = configure_agentfs_sqlite_file(
            self._agent_db_path(branch_id),
            self.sqlite_synchronous,
            cache_size_bytes=self.sqlite_cache_size_bytes,
        )

    def _remove_agent_db(self, branch_id: str) -> None:
        state_dir = self._state_dir()
        agent_dir = state_dir / ".agentfs"
        agent_id = self._agent_id(branch_id)
        for path in agent_dir.glob(f"{agent_id}.db*"):
            with suppress_errors():
                path.unlink()

    def _agent_db_path(self, branch_id: str) -> Path:
        return self._state_dir() / ".agentfs" / f"{self._agent_id(branch_id)}.db"

    def _agent_id(self, branch_id: str) -> str:
        safe = "".join(
            char if char.isalnum() or char in {"-", "_"} else "_"
            for char in branch_id
        )
        return f"chronos-bench-{safe}"

    def _state_dir(self) -> Path:
        if self.state_dir is None:
            raise RuntimeError("backend is not set up")
        return self.state_dir

    def _base_dir(self) -> Path:
        if self.base_dir is None:
            raise RuntimeError("backend is not set up")
        return self.base_dir


@contextmanager
def suppress_errors() -> Iterator[None]:
    try:
        yield
    except Exception:
        pass


def restore_env(name: str, previous: str | None) -> None:
    if previous is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = previous


def seed_bytes(size: int, seed: int) -> bytes:
    pattern = f"chronosfs-bench-{seed:08d}-".encode()
    return (pattern * ((size // len(pattern)) + 1))[:size]


def display_size(size: int) -> str:
    units = ("B", "KiB", "MiB", "GiB")
    value = float(size)
    unit = units[0]
    for unit in units:
        if value < 1024 or unit == units[-1]:
            break
        value /= 1024
    if unit == "B":
        return f"{size}B"
    return f"{value:.1f}{unit}"


def copytree_reflink(source: Path, destination: Path) -> None:
    """Import a tree with filesystem COW file clones instead of byte copies."""
    if shutil.which("cp") is None:
        raise UnsupportedBackend("cp is required for reflink import")
    if destination.exists():
        if any(destination.iterdir()):
            shutil.rmtree(destination)
            destination.mkdir(parents=True, exist_ok=True)
    else:
        destination.mkdir(parents=True, exist_ok=True)
    try:
        run_checked(
            ["cp", "-a", "--reflink=always", str(source) + "/.", str(destination)],
            cwd=source.parent,
            timeout=300,
        )
    except Exception:
        shutil.rmtree(destination, ignore_errors=True)
        raise


def progress(message: str) -> None:
    print(message, flush=True)


def seed_posix_tree(root: Path, *, file_count: int, file_size: int) -> None:
    for index in range(file_count):
        directory = root / "data" / f"{index // 256:04d}"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"file_{index:06d}.bin").write_bytes(seed_bytes(file_size, index))


def seed_chronosfs(
    store: ChronosFSStore,
    branch_id: str,
    *,
    file_count: int,
    file_size: int,
) -> None:
    for index in range(file_count):
        directory = f"/data/{index // 256:04d}"
        store.mkdir(branch_id, directory, parents=True)
        store.write_file(
            branch_id,
            f"{directory}/file_{index:06d}.bin",
            seed_bytes(file_size, index),
            parents=True,
        )


def directory_size(path: Path) -> int:
    total = 0
    if not path.exists():
        return total
    for item in path.rglob("*"):
        if item.is_file() and not item.is_symlink():
            try:
                total += item.stat().st_size
            except OSError:
                pass
    return total


def filesystem_type(path: Path) -> str:
    path.mkdir(parents=True, exist_ok=True)
    if shutil.which("findmnt"):
        result = subprocess.run(
            ["findmnt", "-n", "-T", str(path), "-o", "FSTYPE"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode == 0:
            lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
            if lines:
                return lines[0]
    result = subprocess.run(
        ["stat", "-f", "-c", "%T", str(path)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    if result.returncode == 0:
        return result.stdout.strip()
    return ""


def filesystem_used_bytes(path: Path) -> int:
    stat = os.statvfs(path)
    return (stat.f_blocks - stat.f_bavail) * stat.f_frsize


def ensure_loop_direct_io_for_path(path: Path) -> None:
    """Enable O_DIRECT on loop-backed benchmark roots.

    Btrfs/XFS baselines may be mounted from sparse loopback images on the host
    filesystem. In that setup, `O_DIRECT` on files inside the mounted filesystem
    can still go through the host page cache unless the loop device opens its
    backing file with direct I/O. The benchmark enforces `losetup DIO=1` for
    loop-backed roots so the direct-IO phases are not accidentally cached by the
    backing ext4 file.
    """
    source = mount_source_for_path(path)
    if not source.startswith("/dev/loop"):
        return
    if loop_direct_io_enabled(source):
        return
    enable_loop_direct_io(source)
    if not loop_direct_io_enabled(source):
        raise UnsupportedBackend(
            f"{source} backs {path} but losetup still reports DIO=0 after enabling direct I/O"
        )


def mount_source_for_path(path: Path) -> str:
    if shutil.which("findmnt") is None:
        return ""
    result = subprocess.run(
        ["findmnt", "-n", "-T", str(path), "-o", "SOURCE", "--raw"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        return ""
    return result.stdout.strip().splitlines()[0] if result.stdout.strip() else ""


def loop_direct_io_enabled(loop_device: str) -> bool:
    if shutil.which("losetup") is None:
        raise UnsupportedBackend("losetup is required to inspect loop direct I/O")
    result = subprocess.run(
        ["losetup", "-l", "-n", "-O", "DIO", loop_device],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise UnsupportedBackend(
            f"could not inspect loop direct I/O for {loop_device}: {result.stderr.strip()}"
        )
    return result.stdout.strip() == "1"


def enable_loop_direct_io(loop_device: str) -> None:
    if shutil.which("losetup") is None:
        raise UnsupportedBackend("losetup is required to enable loop direct I/O")
    command = ["losetup", "--direct-io=on", loop_device]
    result = subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode == 0:
        return
    if shutil.which("sudo") is not None:
        sudo_result = subprocess.run(
            ["sudo", "-n", *command],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if sudo_result.returncode == 0:
            return
        stderr = sudo_result.stderr.strip() or result.stderr.strip()
    else:
        stderr = result.stderr.strip()
    raise UnsupportedBackend(
        f"could not enable direct I/O for {loop_device}; run "
        f"`sudo losetup --direct-io=on {loop_device}` before benchmarking. {stderr}"
    )


def reset_benchmark_workdir(path: Path) -> None:
    if path.exists():
        remove_tree(path)
    path.mkdir(parents=True, exist_ok=False)


def run_microbench(backend_name: str, cfg: BenchConfig, workdir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    backend = backend_factory(backend_name, cfg)
    normal_workdir = workdir / "normal"
    directio_workdir = workdir / "directio"
    try:
        reset_benchmark_workdir(normal_workdir)
        progress(
            f"micro backend={backend.name} phase=setup "
            f"files={cfg.file_count} file_size={display_size(cfg.file_size)}"
        )
        backend.setup(normal_workdir, file_count=cfg.file_count, file_size=cfg.file_size)
    except Exception as exc:
        return [
            failed_row(
                workload="micro",
                backend=backend.name,
                phase=phase,
                repeat=0,
                error=exc,
            )
            for phase in MICRO_PHASES
        ]
    try:
        progress(f"micro backend={backend.name} phase=branch_lifecycle")
        rows.extend(bench_branch_create_delete(backend, cfg))
        progress(f"micro backend={backend.name} phase=posix_io")
        rows.extend(bench_posix_io(backend, cfg))
    finally:
        backend.cleanup()

    directio_backend = backend_factory(backend_name, cfg, directio_profile=True)
    try:
        reset_benchmark_workdir(directio_workdir)
        progress(
            f"micro backend={directio_backend.name} phase=directio_setup "
            f"files={cfg.file_count} file_size={display_size(cfg.file_size)}"
        )
        directio_backend.setup(
            directio_workdir,
            file_count=cfg.file_count,
            file_size=cfg.file_size,
        )
        progress(f"micro backend={directio_backend.name} phase=directio")
        rows.extend(bench_posix_directio(directio_backend, cfg))
    except Exception as exc:
        for phase in ("file_read_directio", "file_write_directio"):
            rows.append(
                failed_row(
                    workload="micro",
                    backend=directio_backend.name,
                    phase=phase,
                    repeat=0,
                    error=exc,
                    details=directio_details(cfg),
                )
            )
    finally:
        directio_backend.cleanup()
    return rows


def run_cowbench(backend: FsBackend, cfg: BenchConfig, workdir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        backend.setup(workdir / "backend", file_count=0, file_size=cfg.file_size)
        source = backend.source_parent(workdir) / "large-file-source"
        if source.exists():
            remove_tree(source)
        source.mkdir(parents=True, exist_ok=True)
        large_file = source / "large.bin"
        progress(f"cow backend={backend.name} phase=create_source size={display_size(cfg.cow_file_size)}")
        write_large_file(large_file, cfg.cow_file_size)
        progress(f"cow backend={backend.name} phase=import")
        import_start = time.perf_counter_ns()
        before_import_storage = backend.storage_bytes()
        backend.import_tree(source)
        after_import_storage = backend.storage_bytes()
        file_count, source_bytes = tree_file_stats(source)
        rows.append(
            ok_row(
                workload="import",
                backend=backend.name,
                phase="tree_import",
                repeat=0,
                elapsed_ms=elapsed_since_ms(import_start),
                ops=file_count,
                byte_count=source_bytes,
                storage_delta_bytes=max(0, after_import_storage - before_import_storage),
                details={
                    "file_count": file_count,
                    "source_bytes": source_bytes,
                    "cow_file_size": cfg.cow_file_size,
                },
            )
        )
        for write_size in cfg.cow_write_sizes:
            for repeat in range(cfg.repeats):
                progress(
                    f"cow backend={backend.name} phase=overwrite "
                    f"size={display_size(write_size)} repeat={repeat + 1}/{cfg.repeats}"
                )
                rows.append(run_cow_case(backend, cfg, write_size, repeat))
    except Exception as exc:
        for write_size in cfg.cow_write_sizes:
            rows.append(
                failed_row(
                    workload="cow",
                    backend=backend.name,
                    phase="large_file_overwrite",
                    repeat=0,
                    error=exc,
                    parameter=str(write_size),
                )
            )
    finally:
        backend.cleanup()
    return rows


def tree_file_stats(root: Path) -> tuple[int, int]:
    file_count = 0
    total_bytes = 0
    for path in root.rglob("*"):
        if should_skip_path(path.relative_to(root)):
            continue
        if path.is_file() and not path.is_symlink():
            file_count += 1
            total_bytes += path.stat().st_size
    return file_count, total_bytes


def write_large_file(path: Path, size: int) -> None:
    chunk_size = 1024 * 1024
    remaining = size
    seed = 0
    with path.open("wb") as handle:
        while remaining:
            amount = min(chunk_size, remaining)
            handle.write(seed_bytes(amount, seed))
            remaining -= amount
            seed += 1


def run_cow_case(
    backend: FsBackend,
    cfg: BenchConfig,
    write_size: int,
    repeat: int,
) -> dict[str, Any]:
    branch_id = f"cow_{write_size}_{repeat}"
    try:
        backend.create_branch(branch_id, from_branch="main")
        before_storage = backend.storage_bytes()
        offset = directio_aligned_offset(
            max(0, cfg.cow_file_size // 2 - write_size // 2)
        )
        with backend.checkout_path(branch_id) as root:
            start = time.perf_counter_ns()
            directio_cow_write(root / "large.bin", offset, write_size, write_size + repeat)
            elapsed_ms = elapsed_since_ms(start)
        after_storage = backend.storage_bytes()
        return ok_row(
            workload="cow",
            backend=backend.name,
            phase="large_file_overwrite",
            repeat=repeat,
            elapsed_ms=elapsed_ms,
            ops=1,
            byte_count=write_size,
            storage_delta_bytes=max(0, after_storage - before_storage),
            parameter=str(write_size),
            details={
                "cow_file_size": cfg.cow_file_size,
                "write_size": write_size,
                "offset": offset,
                "timed_scope": "open_direct_pwrite_close",
                "checkout_included": False,
                "direct_io": True,
                "direct_io_alignment": DIRECT_IO_ALIGNMENT,
                "direct_io_bytes": directio_aligned_size(write_size),
                "storage_before_bytes": before_storage,
                "storage_after_bytes": after_storage,
            },
        )
    except Exception as exc:
        return failed_row(
            workload="cow",
            backend=backend.name,
            phase="large_file_overwrite",
            repeat=repeat,
            error=exc,
            parameter=str(write_size),
        )
    finally:
        with suppress_errors():
            backend.delete_branch(branch_id)


def directio_cow_write(path: Path, offset: int, logical_size: int, seed: int) -> None:
    aligned_size = directio_aligned_size(logical_size)
    payload = mmap.mmap(-1, aligned_size)
    try:
        payload[:logical_size] = seed_bytes(logical_size, seed)
        if aligned_size > logical_size:
            payload[logical_size:] = b"\0" * (aligned_size - logical_size)
        fd = open_direct(path, os.O_RDWR)
        try:
            write_direct(fd, payload, offset, aligned_size)
        finally:
            os.close(fd)
    finally:
        payload.close()


def directio_aligned_size(size: int) -> int:
    return max(
        DIRECT_IO_ALIGNMENT,
        ((size + DIRECT_IO_ALIGNMENT - 1) // DIRECT_IO_ALIGNMENT) * DIRECT_IO_ALIGNMENT,
    )


def directio_aligned_offset(offset: int) -> int:
    return (offset // DIRECT_IO_ALIGNMENT) * DIRECT_IO_ALIGNMENT


def bench_branch_create_delete(backend: FsBackend, cfg: BenchConfig) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for repeat in range(cfg.repeats):
        created: list[str] = []
        try:
            start = time.perf_counter_ns()
            for index in range(cfg.branch_iterations):
                branch_id = f"bench_create_{repeat}_{index}"
                backend.create_branch(branch_id, from_branch="main")
                created.append(branch_id)
            elapsed_ms = elapsed_since_ms(start)
            rows.append(
                ok_row(
                    workload="micro",
                    backend=backend.name,
                    phase="branch_create",
                    repeat=repeat,
                    elapsed_ms=elapsed_ms,
                    ops=cfg.branch_iterations,
                    byte_count=0,
                )
            )
            start = time.perf_counter_ns()
            for branch_id in reversed(created):
                backend.delete_branch(branch_id)
            elapsed_ms = elapsed_since_ms(start)
            rows.append(
                ok_row(
                    workload="micro",
                    backend=backend.name,
                    phase="branch_delete",
                    repeat=repeat,
                    elapsed_ms=elapsed_ms,
                    ops=cfg.branch_iterations,
                    byte_count=0,
                )
            )
        except Exception as exc:
            rows.append(
                failed_row(
                    workload="micro",
                    backend=backend.name,
                    phase="branch_create_delete",
                    repeat=repeat,
                    error=exc,
                )
            )
        finally:
            for branch_id in reversed(created):
                with suppress_errors():
                    backend.delete_branch(branch_id)
    return rows


def bench_posix_io(backend: FsBackend, cfg: BenchConfig) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for repeat in range(cfg.repeats):
        branch_id = f"bench_io_{repeat}"
        try:
            progress(
                f"micro backend={backend.name} phase=posix_io "
                f"repeat={repeat + 1}/{cfg.repeats}"
            )
            backend.create_branch(branch_id, from_branch="main")
            with backend.checkout_path(branch_id) as path:
                rows.append(run_file_read(path, backend.name, repeat, cfg))
                rows.append(run_file_write(path, backend.name, repeat, cfg))
        except Exception as exc:
            for phase in (
                "file_read",
                "file_write",
            ):
                rows.append(
                    failed_row(
                        workload="micro",
                        backend=backend.name,
                        phase=phase,
                        repeat=repeat,
                        error=exc,
                    )
                )
        finally:
            with suppress_errors():
                backend.delete_branch(branch_id)
    return rows


def bench_posix_directio(backend: FsBackend, cfg: BenchConfig) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for repeat in range(cfg.repeats):
        branch_id = f"bench_directio_{repeat}"
        try:
            progress(
                f"micro backend={backend.name} phase=directio "
                f"repeat={repeat + 1}/{cfg.repeats}"
            )
            backend.create_branch(branch_id, from_branch="main")
            with backend.checkout_path(branch_id) as path:
                for phase, runner in (
                    ("file_read_directio", run_file_read_directio),
                    ("file_write_directio", run_file_write_directio),
                ):
                    try:
                        rows.append(runner(path, backend.name, repeat, cfg))
                    except Exception as exc:
                        rows.append(
                            failed_row(
                                workload="micro",
                                backend=backend.name,
                                phase=phase,
                                repeat=repeat,
                                error=directio_exception(exc),
                                details=directio_details(cfg),
                            )
                        )
        except Exception as exc:
            for phase in ("file_read_directio", "file_write_directio"):
                rows.append(
                    failed_row(
                        workload="micro",
                        backend=backend.name,
                        phase=phase,
                        repeat=repeat,
                        error=exc,
                        details=directio_details(cfg),
                    )
                )
        finally:
            with suppress_errors():
                backend.delete_branch(branch_id)
    return rows


def run_file_read(
    root: Path,
    backend: str,
    repeat: int,
    cfg: BenchConfig,
) -> dict[str, Any]:
    total = 0
    ops = 0
    io_size = min(cfg.io_size, cfg.file_size)
    offsets = random_io_offsets(cfg.file_size, io_size)
    latency_ns = array("Q")
    rng = random.Random(789_123 + repeat)
    with ExitStack() as stack:
        handles = [
            stack.enter_context(path.open("rb", buffering=0))
            for path in data_file_paths(root, cfg.file_count)
        ]
        for handle in handles:
            handle.seek(0)
            handle.read()
        start = time.perf_counter_ns()
        deadline = start + int(cfg.io_duration_seconds * 1_000_000_000)
        while True:
            op_start = time.perf_counter_ns()
            if op_start >= deadline:
                break
            handle = handles[rng.randrange(len(handles))]
            offset = offsets[rng.randrange(len(offsets))]
            handle.seek(offset)
            total += len(handle.read(io_size))
            latency_ns.append(time.perf_counter_ns() - op_start)
            ops += 1
    elapsed_ms = elapsed_since_ms(start)
    latency = latency_percentiles_ms(latency_ns)
    return ok_row(
        workload="micro",
        backend=backend,
        phase="file_read",
        repeat=repeat,
        elapsed_ms=elapsed_ms,
        ops=ops,
        byte_count=total,
        op_latency_p10_ms=latency["p10"],
        op_latency_p50_ms=latency["p50"],
        op_latency_p99_ms=latency["p99"],
        details=working_set_details(cfg)
        | {
            "io_mode": "random_read",
            "latency_sample_count": len(latency_ns),
            "timed_scope": "duration",
            "warmup_read_files": len(handles),
            "offset_count": len(offsets),
            "file_selection": "random",
        },
    )


def run_file_write(
    root: Path,
    backend: str,
    repeat: int,
    cfg: BenchConfig,
) -> dict[str, Any]:
    io_size = min(cfg.io_size, cfg.file_size)
    payload = seed_bytes(io_size, 123_456 + repeat)
    ops = 0
    total = 0
    offsets = random_io_offsets(cfg.file_size, io_size)
    latency_ns = array("Q")
    rng = random.Random(123_456 + repeat)
    committed_write = committed_write_required(backend)
    fsync_elapsed_ms = 0.0
    fsync_count = 0
    with ExitStack() as stack:
        handles = [
            stack.enter_context(path.open("r+b", buffering=0))
            for path in data_file_paths(root, cfg.file_count)
        ]
        start = time.perf_counter_ns()
        deadline = start + int(cfg.io_duration_seconds * 1_000_000_000)
        while True:
            op_start = time.perf_counter_ns()
            if op_start >= deadline:
                break
            handle = handles[rng.randrange(len(handles))]
            offset = offsets[rng.randrange(len(offsets))]
            handle.seek(offset)
            written = handle.write(payload)
            total += written
            if committed_write:
                fsync_start = time.perf_counter_ns()
                handle.flush()
                os.fsync(handle.fileno())
                fsync_elapsed_ms += elapsed_since_ms(fsync_start)
                fsync_count += 1
            latency_ns.append(time.perf_counter_ns() - op_start)
            ops += 1
    elapsed_ms = elapsed_since_ms(start)
    latency = latency_percentiles_ms(latency_ns)
    return ok_row(
        workload="micro",
        backend=backend,
        phase="file_write",
        repeat=repeat,
        elapsed_ms=elapsed_ms,
        ops=ops,
        byte_count=total,
        op_latency_p10_ms=latency["p10"],
        op_latency_p50_ms=latency["p50"],
        op_latency_p99_ms=latency["p99"],
        details=working_set_details(cfg)
        | {
            "io_mode": "random_write",
            "latency_sample_count": len(latency_ns),
            "timed_scope": "duration",
            "offset_alignment": FILE_WRITE_ALIGNMENT,
            "offset_count": len(offsets),
            "file_selection": "random",
            "committed_write": committed_write,
            "fsync_per_write": committed_write,
            "fsync_count": fsync_count,
            "fsync_elapsed_ms": fsync_elapsed_ms,
        },
    )


def run_file_read_directio(
    root: Path,
    backend: str,
    repeat: int,
    cfg: BenchConfig,
) -> dict[str, Any]:
    validate_directio_config(cfg)
    total = 0
    ops = 0
    io_size = min(cfg.io_size, cfg.file_size)
    offsets = random_io_offsets(cfg.file_size, io_size)
    latency_ns = array("Q")
    rng = random.Random(789_123 + repeat)
    read_buffer = mmap.mmap(-1, io_size)
    fds: list[int] = []
    start = time.perf_counter_ns()
    try:
        fds = [
            open_direct(path, os.O_RDONLY)
            for path in data_file_paths(root, cfg.file_count)
        ]
        for fd in fds:
            for offset in range(0, cfg.file_size - io_size + 1, io_size):
                read_direct(fd, read_buffer, offset, io_size)
        start = time.perf_counter_ns()
        deadline = start + int(cfg.io_duration_seconds * 1_000_000_000)
        while True:
            op_start = time.perf_counter_ns()
            if op_start >= deadline:
                break
            fd = fds[rng.randrange(len(fds))]
            offset = offsets[rng.randrange(len(offsets))]
            total += read_direct(fd, read_buffer, offset, io_size)
            latency_ns.append(time.perf_counter_ns() - op_start)
            ops += 1
    finally:
        close_fds(fds)
        read_buffer.close()
    elapsed_ms = elapsed_since_ms(start)
    latency = latency_percentiles_ms(latency_ns)
    return ok_row(
        workload="micro",
        backend=backend,
        phase="file_read_directio",
        repeat=repeat,
        elapsed_ms=elapsed_ms,
        ops=ops,
        byte_count=total,
        op_latency_p10_ms=latency["p10"],
        op_latency_p50_ms=latency["p50"],
        op_latency_p99_ms=latency["p99"],
        details=working_set_details(cfg)
        | directio_details(cfg)
        | {
            "io_mode": "random_read_directio",
            "latency_sample_count": len(latency_ns),
            "timed_scope": "duration",
            "warmup_read_files": len(fds),
            "offset_count": len(offsets),
            "file_selection": "random",
        },
    )


def run_file_write_directio(
    root: Path,
    backend: str,
    repeat: int,
    cfg: BenchConfig,
) -> dict[str, Any]:
    validate_directio_config(cfg)
    io_size = min(cfg.io_size, cfg.file_size)
    payload = mmap.mmap(-1, io_size)
    payload[:] = seed_bytes(io_size, 223_456 + repeat)
    ops = 0
    total = 0
    offsets = random_io_offsets(cfg.file_size, io_size)
    latency_ns = array("Q")
    rng = random.Random(223_456 + repeat)
    fds: list[int] = []
    committed_write = committed_write_required(backend)
    fsync_elapsed_ms = 0.0
    fsync_count = 0
    start = time.perf_counter_ns()
    try:
        fds = [
            open_direct(path, os.O_RDWR)
            for path in data_file_paths(root, cfg.file_count)
        ]
        start = time.perf_counter_ns()
        deadline = start + int(cfg.io_duration_seconds * 1_000_000_000)
        while True:
            op_start = time.perf_counter_ns()
            if op_start >= deadline:
                break
            fd = fds[rng.randrange(len(fds))]
            offset = offsets[rng.randrange(len(offsets))]
            total += write_direct(fd, payload, offset, io_size)
            if committed_write:
                fsync_start = time.perf_counter_ns()
                os.fsync(fd)
                fsync_elapsed_ms += elapsed_since_ms(fsync_start)
                fsync_count += 1
            latency_ns.append(time.perf_counter_ns() - op_start)
            ops += 1
    finally:
        close_fds(fds)
        payload.close()
    elapsed_ms = elapsed_since_ms(start)
    latency = latency_percentiles_ms(latency_ns)
    return ok_row(
        workload="micro",
        backend=backend,
        phase="file_write_directio",
        repeat=repeat,
        elapsed_ms=elapsed_ms,
        ops=ops,
        byte_count=total,
        op_latency_p10_ms=latency["p10"],
        op_latency_p50_ms=latency["p50"],
        op_latency_p99_ms=latency["p99"],
        details=working_set_details(cfg)
        | directio_details(cfg)
        | {
            "io_mode": "random_write_directio",
            "latency_sample_count": len(latency_ns),
            "timed_scope": "duration",
            "offset_alignment": DIRECT_IO_ALIGNMENT,
            "offset_count": len(offsets),
            "file_selection": "random",
            "committed_write": committed_write,
            "fsync_per_write": committed_write,
            "fsync_count": fsync_count,
            "fsync_elapsed_ms": fsync_elapsed_ms,
        },
    )


def validate_directio_config(cfg: BenchConfig) -> None:
    if not hasattr(os, "O_DIRECT"):
        raise UnsupportedBackend("O_DIRECT is not available on this platform")
    if not hasattr(os, "preadv") or not hasattr(os, "pwritev"):
        raise UnsupportedBackend("preadv/pwritev are required for aligned direct I/O")
    io_size = min(cfg.io_size, cfg.file_size)
    if io_size <= 0:
        raise UnsupportedBackend("direct I/O size must be positive")
    if io_size % DIRECT_IO_ALIGNMENT != 0:
        raise UnsupportedBackend(
            f"direct I/O size must be {DIRECT_IO_ALIGNMENT}-byte aligned; got {io_size}"
        )
    if cfg.file_size < io_size:
        raise UnsupportedBackend("direct I/O file size must be at least the I/O size")


def committed_write_required(backend: str) -> bool:
    return backend == "turso"


def open_direct(path: Path, flags: int) -> int:
    try:
        return os.open(path, flags | os.O_DIRECT)
    except OSError as exc:
        raise directio_exception(exc) from exc


def read_direct(fd: int, buffer: mmap.mmap, offset: int, size: int) -> int:
    try:
        return os.preadv(fd, [buffer], offset)
    except OSError as exc:
        raise directio_exception(exc) from exc


def write_direct(fd: int, buffer: mmap.mmap, offset: int, size: int) -> int:
    try:
        written = os.pwritev(fd, [buffer], offset)
    except OSError as exc:
        raise directio_exception(exc) from exc
    if written != size:
        raise OSError(f"short direct I/O write: wrote {written} of {size} bytes")
    return written


def directio_exception(exc: Exception) -> Exception:
    if isinstance(exc, UnsupportedBackend):
        return exc
    if isinstance(exc, OSError) and exc.errno in {
        errno.EINVAL,
        errno.EOPNOTSUPP,
        errno.ENOTTY,
    }:
        return UnsupportedBackend(f"direct I/O is not supported here: {exc}")
    return exc


def close_fds(fds: list[int]) -> None:
    for fd in fds:
        with suppress_errors():
            os.close(fd)


def directio_details(cfg: BenchConfig) -> dict[str, Any]:
    return {
        "direct_io": True,
        "direct_io_alignment": DIRECT_IO_ALIGNMENT,
        "directio_sqlite_synchronous": cfg.directio_sqlite_synchronous,
        "directio_cache_size": cfg.directio_cache_size,
        "directio_cache_size_kib": sqlite_cache_size_kib(cfg.directio_cache_size),
    }


def random_io_offsets(file_size: int, io_size: int) -> list[int]:
    max_offset = max(0, file_size - io_size)
    return list(range(0, max_offset + 1, FILE_WRITE_ALIGNMENT)) or [0]


def data_file_paths(root: Path, file_count: int) -> list[Path]:
    return [
        root / "data" / f"{index // 256:04d}" / f"file_{index:06d}.bin"
        for index in range(file_count)
    ]


def working_set_details(cfg: BenchConfig) -> dict[str, Any]:
    return {
        "open_file_count": cfg.file_count,
        "file_size": cfg.file_size,
        "io_duration_seconds": cfg.io_duration_seconds,
        "io_size": cfg.io_size,
    }


def run_compilebench(backend: FsBackend, cfg: BenchConfig, workdir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        backend.setup(workdir / "backend", file_count=0, file_size=cfg.file_size)
        source_parent = backend.source_parent(workdir)
        source = prepare_redis_source(cfg, source_parent)
        before_import_storage = backend.storage_bytes()
        import_start = time.perf_counter_ns()
        backend.import_tree(source)
        after_import_storage = backend.storage_bytes()
        file_count, source_bytes = tree_file_stats(source)
        rows.append(
            ok_row(
                workload="import",
                backend=backend.name,
                phase="tree_import_compile",
                repeat=0,
                elapsed_ms=elapsed_since_ms(import_start),
                ops=file_count,
                byte_count=source_bytes,
                storage_delta_bytes=max(0, after_import_storage - before_import_storage),
                details={
                    "file_count": file_count,
                    "source_bytes": source_bytes,
                    "source": "redis",
                },
            )
        )
        for repeat in range(cfg.compile_repeats):
            branch_id = f"compile_{repeat}"
            before_branch_storage = backend.storage_bytes()
            branch_start = time.perf_counter_ns()
            try:
                backend.create_branch(branch_id, from_branch="main")
            except Exception as exc:
                rows.append(
                    failed_row(
                        workload="compile_branch",
                        backend=backend.name,
                        phase="source_tree_branch_create",
                        repeat=repeat,
                        error=exc,
                        details={
                            "file_count": file_count,
                            "source_bytes": source_bytes,
                            "source": "redis",
                        },
                    )
                )
                continue
            after_branch_storage = backend.storage_bytes()
            rows.append(
                ok_row(
                    workload="compile_branch",
                    backend=backend.name,
                    phase="source_tree_branch_create",
                    repeat=repeat,
                    elapsed_ms=elapsed_since_ms(branch_start),
                    ops=1,
                    byte_count=source_bytes,
                    storage_delta_bytes=max(0, after_branch_storage - before_branch_storage),
                    details={
                        "branch_id": branch_id,
                        "file_count": file_count,
                        "source_bytes": source_bytes,
                        "source": "redis",
                    },
                )
            )
            try:
                with backend.checkout_path(branch_id) as path:
                    rows.append(run_redis_make(path, backend.name, repeat, cfg))
            finally:
                before_delete_storage = backend.storage_bytes()
                delete_start = time.perf_counter_ns()
                try:
                    backend.delete_branch(branch_id)
                except Exception as exc:
                    rows.append(
                        failed_row(
                            workload="compile_branch",
                            backend=backend.name,
                            phase="source_tree_branch_delete",
                            repeat=repeat,
                            error=exc,
                            details={
                                "branch_id": branch_id,
                                "file_count": file_count,
                                "source_bytes": source_bytes,
                                "source": "redis",
                            },
                        )
                    )
                else:
                    after_delete_storage = backend.storage_bytes()
                    rows.append(
                        ok_row(
                            workload="compile_branch",
                            backend=backend.name,
                            phase="source_tree_branch_delete",
                            repeat=repeat,
                            elapsed_ms=elapsed_since_ms(delete_start),
                            ops=1,
                            byte_count=source_bytes,
                            storage_delta_bytes=max(0, after_delete_storage - before_delete_storage),
                            details={
                                "branch_id": branch_id,
                                "file_count": file_count,
                                "source_bytes": source_bytes,
                                "source": "redis",
                            },
                        )
                    )
    except Exception as exc:
        rows.append(
            failed_row(
                workload="compile",
                backend=backend.name,
                phase="redis_make",
                repeat=0,
                error=exc,
            )
        )
    finally:
        backend.cleanup()
    return rows


def prepare_redis_source(cfg: BenchConfig, target_parent: Path) -> Path:
    if cfg.redis_source is not None:
        source = cfg.redis_source.resolve()
        if not source.is_dir():
            raise FileNotFoundError(f"Redis source is not a directory: {source}")
        return source
    target = target_parent / "redis"
    if target.exists():
        return target
    if shutil.which("git") is None:
        raise UnsupportedBackend("git is required to fetch Redis source")
    run_checked(
        [
            "git",
            "clone",
            "--depth",
            "1",
            "--branch",
            cfg.redis_ref,
            cfg.redis_repo_url,
            str(target),
        ],
        cwd=target_parent,
        timeout=300,
    )
    return target


def run_redis_make(
    root: Path,
    backend: str,
    repeat: int,
    cfg: BenchConfig,
) -> dict[str, Any]:
    if shutil.which("make") is None:
        return failed_row(
            workload="compile",
            backend=backend,
            phase="redis_make",
            repeat=repeat,
            error=UnsupportedBackend("make is not installed"),
        )
    start = time.perf_counter_ns()
    result = subprocess.run(
        ["make", f"-j{cfg.compile_jobs}", "BUILD_TLS=no", "MALLOC=libc"],
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=1800,
    )
    elapsed_ms = elapsed_since_ms(start)
    details = {
        "returncode": result.returncode,
        "stdout_tail": result.stdout[-2000:],
        "stderr_tail": result.stderr[-2000:],
    }
    if result.returncode != 0:
        return failed_row(
            workload="compile",
            backend=backend,
            phase="redis_make",
            repeat=repeat,
            error=RuntimeError(f"make failed with exit {result.returncode}"),
            details=details,
        )
    binary = root / "src" / "redis-server"
    if not binary.exists():
        return failed_row(
            workload="compile",
            backend=backend,
            phase="redis_make",
            repeat=repeat,
            error=RuntimeError("src/redis-server was not created"),
            details=details,
        )
    return ok_row(
        workload="compile",
        backend=backend,
        phase="redis_make",
        repeat=repeat,
        elapsed_ms=elapsed_ms,
        ops=1,
        byte_count=0,
        details=details,
    )


def import_tree_to_chronosfs(store: ChronosFSStore, branch_id: str, source: Path) -> None:
    store.import_tree(branch_id, source)


def ignore_benchmark_paths(directory: str, names: list[str]) -> set[str]:
    return {name for name in names if should_skip_path(Path(name))}


def should_skip_path(path: Path) -> bool:
    parts = path.parts
    if not parts:
        return False
    if parts[0] in {".git", ".github", "__pycache__"}:
        return True
    if path.name.endswith((".o", ".a", ".so", ".pyc")):
        return True
    return False


@contextmanager
def mounted_chronosfs(
    store: ChronosFSStore,
    branch_id: str,
    *,
    database_url: str,
    sqlite_synchronous: str = "OFF",
    sqlite_cache_size_bytes: int | None = None,
    sqlite_wal_autocheckpoint_pages: int | None = None,
) -> Iterator[Path]:
    from chronos_core.workspace.chronosfs.fuse import (
        _database_urls_for_mount,
        _shutdown_shared_chronosfs_daemon,
        _start_shared_chronosfs_mount,
        _with_default_cache_options,
    )

    mountpoint = Path(tempfile.mkdtemp(prefix=f"chronosfs-{branch_id}-"))
    mount_database_url, metadata_url = _database_urls_for_mount(store)
    cache_size_kib = sqlite_cache_size_kib(sqlite_cache_size_bytes)
    previous_sync = os.environ.get("CHRONOS_NATIVE_SQLITE_SYNCHRONOUS")
    previous_cache_size = os.environ.get("CHRONOS_NATIVE_SQLITE_CACHE_SIZE_KIB")
    previous_wal_autocheckpoint = os.environ.get(
        "CHRONOS_NATIVE_SQLITE_WAL_AUTOCHECKPOINT_PAGES"
    )
    try:
        os.environ["CHRONOS_NATIVE_SQLITE_SYNCHRONOUS"] = normalize_sqlite_synchronous(
            sqlite_synchronous
        )
        if cache_size_kib is not None:
            os.environ["CHRONOS_NATIVE_SQLITE_CACHE_SIZE_KIB"] = str(cache_size_kib)
        if sqlite_wal_autocheckpoint_pages is not None:
            os.environ["CHRONOS_NATIVE_SQLITE_WAL_AUTOCHECKPOINT_PAGES"] = str(
                sqlite_wal_autocheckpoint_pages
            )
        _start_shared_chronosfs_mount(
            mount_database_url,
            metadata_url,
            mountpoint,
            branch_id=branch_id,
            block_size=store.block_size,
            options=_with_default_cache_options(set()),
        )
        yield mountpoint
    finally:
        unmount(mountpoint)
        _shutdown_shared_chronosfs_daemon(
            mount_database_url,
            metadata_url,
            store.block_size,
        )
        restore_env("CHRONOS_NATIVE_SQLITE_SYNCHRONOUS", previous_sync)
        restore_env("CHRONOS_NATIVE_SQLITE_CACHE_SIZE_KIB", previous_cache_size)
        restore_env(
            "CHRONOS_NATIVE_SQLITE_WAL_AUTOCHECKPOINT_PAGES",
            previous_wal_autocheckpoint,
        )
        shutil.rmtree(mountpoint, ignore_errors=True)


def wait_for_mount(
    proc: subprocess.Popen[str],
    mountpoint: Path,
    *,
    backend_name: str = "ChronosFS",
) -> None:
    deadline = time.time() + 15
    while time.time() < deadline:
        if proc.poll() is not None:
            stdout, stderr = proc.communicate(timeout=1)
            raise UnsupportedBackend(
                f"{backend_name} mount exited early: "
                f"stdout={stdout[-1000:]!r} stderr={stderr[-2000:]!r}"
            )
        if os.path.ismount(mountpoint):
            return
        time.sleep(0.05)
    raise UnsupportedBackend(f"{backend_name} mount did not become ready: {mountpoint}")


def unmount(mountpoint: Path) -> None:
    commands = []
    if shutil.which("fusermount3"):
        commands.extend((["fusermount3", "-u", str(mountpoint)], ["fusermount3", "-uz", str(mountpoint)]))
    if shutil.which("fusermount"):
        commands.extend((["fusermount", "-u", str(mountpoint)], ["fusermount", "-uz", str(mountpoint)]))
    commands.append(["umount", str(mountpoint)])
    for command in commands:
        result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if result.returncode == 0:
            return


def run_checked(
    argv: list[str],
    *,
    cwd: Path,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        argv,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(argv)}\n"
            f"stdout={result.stdout[-2000:]}\nstderr={result.stderr[-2000:]}"
        )
    return result


def run_privileged_checked(
    argv: list[str],
    *,
    cwd: Path,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    if os.geteuid() == 0:
        return run_checked(argv, cwd=cwd, timeout=timeout)
    if shutil.which("sudo") is None:
        raise UnsupportedBackend(
            f"sudo is required to run privileged command: {' '.join(argv)}"
        )
    return run_checked(["sudo", "-n", *argv], cwd=cwd, timeout=timeout)


def remove_tree(path: Path) -> None:
    if not path.exists():
        return
    try:
        shutil.rmtree(path)
    except OSError:
        run_privileged_checked(
            ["rm", "-rf", "--", str(path)],
            cwd=Path("/"),
            timeout=300,
        )


def elapsed_since_ms(start_ns: int) -> float:
    return (time.perf_counter_ns() - start_ns) / 1_000_000


def ok_row(
    *,
    workload: str,
    backend: str,
    phase: str,
    repeat: int,
    elapsed_ms: float,
    ops: int,
    byte_count: int,
    parameter: str = "",
    storage_delta_bytes: int = 0,
    op_latency_p10_ms: float = 0.0,
    op_latency_p50_ms: float = 0.0,
    op_latency_p99_ms: float = 0.0,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    seconds = elapsed_ms / 1000 if elapsed_ms > 0 else 0
    return {
        "workload": workload,
        "backend": backend,
        "phase": phase,
        "parameter": parameter,
        "repeat": repeat,
        "status": "ok",
        "elapsed_ms": elapsed_ms,
        "ops": ops,
        "bytes": byte_count,
        "storage_delta_bytes": storage_delta_bytes,
        "ops_per_sec": (ops / seconds) if seconds else 0,
        "mb_per_sec": ((byte_count / (1024 * 1024)) / seconds) if seconds else 0,
        "op_latency_p10_ms": op_latency_p10_ms,
        "op_latency_p50_ms": op_latency_p50_ms,
        "op_latency_p99_ms": op_latency_p99_ms,
        "error": "",
        "details": json.dumps(details or {}, sort_keys=True),
    }


def failed_row(
    *,
    workload: str,
    backend: str,
    phase: str,
    repeat: int,
    error: Exception,
    parameter: str = "",
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    merged_details = dict(details or {})
    merged_details["error_type"] = error.__class__.__name__
    return {
        "workload": workload,
        "backend": backend,
        "phase": phase,
        "parameter": parameter,
        "repeat": repeat,
        "status": "failed" if not isinstance(error, UnsupportedBackend) else "unsupported",
        "elapsed_ms": 0.0,
        "ops": 0,
        "bytes": 0,
        "storage_delta_bytes": 0,
        "ops_per_sec": 0.0,
        "mb_per_sec": 0.0,
        "op_latency_p10_ms": 0.0,
        "op_latency_p50_ms": 0.0,
        "op_latency_p99_ms": 0.0,
        "error": str(error),
        "details": json.dumps(merged_details, sort_keys=True),
    }


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    keys = sorted({(row["workload"], row["backend"], row["phase"], row["parameter"]) for row in rows})
    for workload, backend, phase, parameter in keys:
        group = [
            row
            for row in rows
            if (
                row["workload"] == workload
                and row["backend"] == backend
                and row["phase"] == phase
                and row["parameter"] == parameter
            )
        ]
        ok = [row for row in group if row["status"] == "ok"]
        if not ok:
            first = group[0]
            result.append(
                {
                    "workload": workload,
                    "backend": backend,
                    "phase": phase,
                    "parameter": parameter,
                    "status": first["status"],
                    "repetitions": 0,
                    "median_elapsed_ms": 0.0,
                    "p10_elapsed_ms": 0.0,
                    "p90_elapsed_ms": 0.0,
                    "avg_elapsed_ms": 0.0,
                    "median_ms_per_op": 0.0,
                    "p10_ms_per_op": 0.0,
                    "p90_ms_per_op": 0.0,
                    "p99_ms_per_op": 0.0,
                    "median_ops_per_sec": 0.0,
                    "median_mb_per_sec": 0.0,
                    "p10_mb_per_sec": 0.0,
                    "p90_mb_per_sec": 0.0,
                    "median_storage_delta_bytes": 0,
                    "p10_storage_delta_bytes": 0,
                    "p90_storage_delta_bytes": 0,
                    "error": first["error"],
                }
            )
            continue
        elapsed = [float(row["elapsed_ms"]) for row in ok]
        per_op = [
            float(row["elapsed_ms"]) / max(1, int(row["ops"]))
            for row in ok
        ]
        measured_p10 = [float(row.get("op_latency_p10_ms") or 0.0) for row in ok]
        measured_p50 = [float(row.get("op_latency_p50_ms") or 0.0) for row in ok]
        measured_p99 = [float(row.get("op_latency_p99_ms") or 0.0) for row in ok]
        has_measured_latency = any(value > 0 for value in measured_p50)
        if has_measured_latency:
            median_ms_per_op = median(value for value in measured_p50 if value > 0)
            p10_ms_per_op = median(value for value in measured_p10 if value > 0)
            p90_ms_per_op = percentile(per_op, 90)
            p99_ms_per_op = median(value for value in measured_p99 if value > 0)
        else:
            median_ms_per_op = median(per_op)
            p10_ms_per_op = percentile(per_op, 10)
            p90_ms_per_op = percentile(per_op, 90)
            p99_ms_per_op = percentile(per_op, 99)
        throughput = [float(row["mb_per_sec"]) for row in ok]
        storage_delta = [float(row["storage_delta_bytes"]) for row in ok]
        result.append(
            {
                "workload": workload,
                "backend": backend,
                "phase": phase,
                "parameter": parameter,
                "status": "ok",
                "repetitions": len(ok),
                "median_elapsed_ms": median(elapsed),
                "p10_elapsed_ms": percentile(elapsed, 10),
                "p90_elapsed_ms": percentile(elapsed, 90),
                "avg_elapsed_ms": sum(elapsed) / len(elapsed),
                "median_ms_per_op": median_ms_per_op,
                "p10_ms_per_op": p10_ms_per_op,
                "p90_ms_per_op": p90_ms_per_op,
                "p99_ms_per_op": p99_ms_per_op,
                "median_ops_per_sec": median(float(row["ops_per_sec"]) for row in ok),
                "median_mb_per_sec": median(throughput),
                "p10_mb_per_sec": percentile(throughput, 10),
                "p90_mb_per_sec": percentile(throughput, 90),
                "median_storage_delta_bytes": median(storage_delta),
                "p10_storage_delta_bytes": percentile(storage_delta, 10),
                "p90_storage_delta_bytes": percentile(storage_delta, 90),
                "error": "",
            }
        )
    return result


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * (pct / 100.0)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    fraction = rank - low
    return ordered[low] * (1 - fraction) + ordered[high] * fraction


def latency_percentiles_ms(latency_ns: array) -> dict[str, float]:
    if not latency_ns:
        return {"p10": 0.0, "p50": 0.0, "p99": 0.0}
    ordered = sorted(latency_ns)
    return {
        "p10": percentile_sorted_ns_to_ms(ordered, 10),
        "p50": percentile_sorted_ns_to_ms(ordered, 50),
        "p99": percentile_sorted_ns_to_ms(ordered, 99),
    }


def percentile_sorted_ns_to_ms(ordered: list[int], pct: float) -> float:
    if len(ordered) == 1:
        return ordered[0] / 1_000_000
    rank = (len(ordered) - 1) * (pct / 100.0)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    fraction = rank - low
    value = ordered[low] * (1 - fraction) + ordered[high] * fraction
    return value / 1_000_000


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, rows: list[dict[str, Any]], summary: list[dict[str, Any]], config: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(
            {"config": config, "results": rows, "summary": summary},
            indent=2,
            sort_keys=True,
        )
    )


def plot_results(output_dir: Path, summary_rows: list[dict[str, Any]]) -> list[Path]:
    paths: list[Path] = []
    micro = [
        row for row in summary_rows
        if row["workload"] == "micro" and row["status"] == "ok"
    ]
    if micro:
        paths.append(plot_micro_latency(output_dir, micro))
        throughput_rows = [
            row for row in micro
            if row["phase"] in {
                "file_read",
                "file_write",
                "file_read_directio",
                "file_write_directio",
            }
        ]
        if throughput_rows:
            paths.append(plot_micro_throughput(output_dir, throughput_rows))
    compile_rows = [
        row for row in summary_rows
        if row["workload"] == "compile" and row["status"] == "ok"
    ]
    if compile_rows:
        paths.append(plot_compile(output_dir, compile_rows))
    compile_branch_rows = [
        row for row in summary_rows
        if row["workload"] == "compile_branch" and row["status"] == "ok"
    ]
    if compile_branch_rows:
        paths.append(plot_compile_branch(output_dir, compile_branch_rows))
    import_rows = [
        row for row in summary_rows
        if row["workload"] == "import" and row["status"] == "ok"
    ]
    if import_rows:
        paths.append(plot_import(output_dir, import_rows))
    cow_rows = [
        row for row in summary_rows
        if row["workload"] == "cow" and row["status"] == "ok"
    ]
    if cow_rows:
        paths.append(plot_cow_latency(output_dir, cow_rows))
        paths.append(plot_cow_storage(output_dir, cow_rows))
    return paths


def maybe_use_log_yaxis(
    ax: Any,
    plotted_values: list[float],
    *,
    ylabel: str,
) -> None:
    positive = [value for value in plotted_values if value > 0]
    if len(positive) < 2:
        ax.set_ylabel(ylabel)
        return
    if max(positive) / min(positive) < PLOT_LOG_SCALE_RATIO_THRESHOLD:
        ax.set_ylabel(ylabel)
        return
    if all(value >= 0 for value in plotted_values):
        if any(value == 0 for value in plotted_values):
            ax.set_yscale("symlog", linthresh=max(min(positive), 1e-9))
            ax.set_ylabel(f"{ylabel} (symmetric log scale)")
        else:
            ax.set_yscale("log")
            ax.set_ylabel(f"{ylabel} (log scale)")
    else:
        ax.set_ylabel(ylabel)


def percentile_yerr(
    medians: list[float],
    lows: list[float],
    highs: list[float],
) -> list[list[float]]:
    lower = [max(0.0, median_value - low) for median_value, low in zip(medians, lows)]
    upper = [max(0.0, high - median_value) for median_value, high in zip(medians, highs)]
    return [lower, upper]


def bar_error_kwargs() -> dict[str, Any]:
    return {
        "ecolor": "#262626",
        "capsize": 0,
        "error_kw": {"elinewidth": 0.9},
    }


LABEL_BBOX = {
    "boxstyle": "round,pad=0.12",
    "facecolor": "white",
    "edgecolor": "none",
    "alpha": 0.78,
}


def format_bar_label(value: float) -> str:
    absolute = abs(value)
    if absolute >= 100:
        return f"{value:.0f}"
    if absolute >= 10:
        return f"{value:.1f}"
    if absolute >= 1:
        return f"{value:.2f}"
    if absolute > 0:
        return f"{value:.2g}"
    return "0"


def add_bar_labels(ax: Any, bars: Any, heights: list[float]) -> None:
    labels = [format_bar_label(height) for height in heights]
    if hasattr(ax, "bar_label"):
        ax.bar_label(
            bars,
            labels=labels,
            padding=2,
            fontsize=7,
            rotation=0,
            bbox=LABEL_BBOX,
        )
        return
    for bar, label in zip(bars, labels):
        ax.annotate(
            label,
            xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
            xytext=(0, 2),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=7,
            rotation=0,
            bbox=LABEL_BBOX,
            clip_on=False,
        )


def plot_micro_latency(output_dir: Path, rows: list[dict[str, Any]]) -> Path:
    phases = [phase for phase in MICRO_PHASES if any(row["phase"] == phase for row in rows)]
    backends = [backend for backend in BACKENDS if any(row["backend"] == backend for row in rows)]
    values = {
        (row["backend"], row["phase"]): float(row["median_ms_per_op"])
        for row in rows
    }
    fig, ax = plt.subplots(figsize=(max(12, len(phases) * 1.8), 5.8), constrained_layout=True)
    group_width = 0.8
    bar_width = group_width / max(1, len(backends))
    x_positions = list(range(len(phases)))
    plotted_values: list[float] = []
    for backend_index, backend in enumerate(backends):
        offsets = [
            x - group_width / 2 + bar_width * (backend_index + 0.5)
            for x in x_positions
        ]
        heights = [values.get((backend, phase), 0.0) for phase in phases]
        plotted_values.extend(heights)
        bars = ax.bar(
            offsets,
            heights,
            width=bar_width,
            label=backend,
            color=BACKEND_COLORS.get(backend, "#525252"),
        )
        add_bar_labels(ax, bars, heights)
    ax.set_title("Filesystem Microbenchmark Latency")
    maybe_use_log_yaxis(
        ax,
        plotted_values,
        ylabel="milliseconds per operation",
    )
    ax.set_xticks(x_positions)
    ax.set_xticklabels([phase_label(phase) for phase in phases], rotation=25, ha="right")
    ax.legend()
    path = output_dir / "fs_micro_latency.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return path


def plot_micro_throughput(output_dir: Path, rows: list[dict[str, Any]]) -> Path:
    phases = [phase for phase in MICRO_PHASES if any(row["phase"] == phase for row in rows)]
    backends = [backend for backend in BACKENDS if any(row["backend"] == backend for row in rows)]
    values = {
        (row["backend"], row["phase"]): float(row["median_mb_per_sec"])
        for row in rows
    }
    p10 = {
        (row["backend"], row["phase"]): float(row["p10_mb_per_sec"])
        for row in rows
    }
    p90 = {
        (row["backend"], row["phase"]): float(row["p90_mb_per_sec"])
        for row in rows
    }
    fig, ax = plt.subplots(figsize=(max(8, len(phases) * 1.5), 5.5), constrained_layout=True)
    group_width = 0.8
    bar_width = group_width / max(1, len(backends))
    x_positions = list(range(len(phases)))
    plotted_values: list[float] = []
    for backend_index, backend in enumerate(backends):
        offsets = [
            x - group_width / 2 + bar_width * (backend_index + 0.5)
            for x in x_positions
        ]
        heights = [values.get((backend, phase), 0.0) for phase in phases]
        lows = [p10.get((backend, phase), 0.0) for phase in phases]
        highs = [p90.get((backend, phase), 0.0) for phase in phases]
        plotted_values.extend(heights)
        bars = ax.bar(
            offsets,
            heights,
            width=bar_width,
            label=backend,
            color=BACKEND_LIGHT_COLORS.get(backend, "#525252"),
            yerr=percentile_yerr(heights, lows, highs),
            **bar_error_kwargs(),
        )
        add_bar_labels(ax, bars, heights)
    ax.set_title("Filesystem Microbenchmark Throughput")
    maybe_use_log_yaxis(ax, plotted_values, ylabel="median MiB/s")
    ax.set_xticks(x_positions)
    ax.set_xticklabels([phase_label(phase) for phase in phases], rotation=25, ha="right")
    ax.legend()
    path = output_dir / "fs_micro_throughput.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return path


def plot_compile(output_dir: Path, rows: list[dict[str, Any]]) -> Path:
    backends = [backend for backend in BACKENDS if any(row["backend"] == backend for row in rows)]
    values = {
        row["backend"]: float(row["median_elapsed_ms"]) / 1000
        for row in rows
    }
    p10 = {
        row["backend"]: float(row["p10_elapsed_ms"]) / 1000
        for row in rows
    }
    p90 = {
        row["backend"]: float(row["p90_elapsed_ms"]) / 1000
        for row in rows
    }
    plotted_values = [values.get(backend, 0.0) for backend in backends]
    lows = [p10.get(backend, 0.0) for backend in backends]
    highs = [p90.get(backend, 0.0) for backend in backends]
    fig, ax = plt.subplots(figsize=(7, 5), constrained_layout=True)
    bars = ax.bar(
        list(range(len(backends))),
        plotted_values,
        color=[BACKEND_COLORS.get(backend, "#525252") for backend in backends],
        yerr=percentile_yerr(plotted_values, lows, highs),
        **bar_error_kwargs(),
    )
    add_bar_labels(ax, bars, plotted_values)
    ax.set_title("Redis Compile After Branch")
    maybe_use_log_yaxis(ax, plotted_values, ylabel="median seconds")
    ax.set_xticks(list(range(len(backends))))
    ax.set_xticklabels(backends)
    path = output_dir / "fs_compile_redis.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return path


def plot_compile_branch(output_dir: Path, rows: list[dict[str, Any]]) -> Path:
    phases = [
        phase
        for phase in ("source_tree_branch_create", "source_tree_branch_delete")
        if any(row["phase"] == phase for row in rows)
    ]
    backends = [backend for backend in BACKENDS if any(row["backend"] == backend for row in rows)]
    values = {
        (row["backend"], row["phase"]): float(row["median_elapsed_ms"])
        for row in rows
    }
    p10 = {
        (row["backend"], row["phase"]): float(row["p10_elapsed_ms"])
        for row in rows
    }
    p90 = {
        (row["backend"], row["phase"]): float(row["p90_elapsed_ms"])
        for row in rows
    }
    fig, ax = plt.subplots(figsize=(max(7, len(phases) * 1.8), 5), constrained_layout=True)
    group_width = 0.8
    bar_width = group_width / max(1, len(backends))
    x_positions = list(range(len(phases)))
    plotted_values: list[float] = []
    for backend_index, backend in enumerate(backends):
        offsets = [
            x - group_width / 2 + bar_width * (backend_index + 0.5)
            for x in x_positions
        ]
        heights = [values.get((backend, phase), 0.0) for phase in phases]
        lows = [p10.get((backend, phase), 0.0) for phase in phases]
        highs = [p90.get((backend, phase), 0.0) for phase in phases]
        plotted_values.extend(heights)
        bars = ax.bar(
            offsets,
            heights,
            width=bar_width,
            label=backend,
            color=BACKEND_COLORS.get(backend, "#525252"),
            yerr=percentile_yerr(heights, lows, highs),
            **bar_error_kwargs(),
        )
        add_bar_labels(ax, bars, heights)
    ax.set_title("Redis Source Tree Branch Latency")
    maybe_use_log_yaxis(ax, plotted_values, ylabel="median milliseconds")
    ax.set_xticks(x_positions)
    ax.set_xticklabels(
        [
            "branch create" if phase.endswith("create") else "branch delete"
            for phase in phases
        ],
        rotation=20,
        ha="right",
    )
    ax.legend()
    path = output_dir / "fs_compile_branch_latency.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return path


def plot_import(output_dir: Path, rows: list[dict[str, Any]]) -> Path:
    phases = sorted({row["phase"] for row in rows})
    backends = [backend for backend in BACKENDS if any(row["backend"] == backend for row in rows)]
    values = {
        (row["backend"], row["phase"]): float(row["median_elapsed_ms"])
        for row in rows
    }
    fig, ax = plt.subplots(figsize=(max(7, len(phases) * 1.6), 5), constrained_layout=True)
    group_width = 0.8
    bar_width = group_width / max(1, len(backends))
    x_positions = list(range(len(phases)))
    plotted_values: list[float] = []
    for backend_index, backend in enumerate(backends):
        offsets = [
            x - group_width / 2 + bar_width * (backend_index + 0.5)
            for x in x_positions
        ]
        heights = [values.get((backend, phase), 0.0) for phase in phases]
        plotted_values.extend(heights)
        bars = ax.bar(
            offsets,
            heights,
            width=bar_width,
            label=backend,
            color=BACKEND_COLORS.get(backend, "#525252"),
        )
        add_bar_labels(ax, bars, heights)
    ax.set_title("Filesystem Import Latency")
    maybe_use_log_yaxis(ax, plotted_values, ylabel="median milliseconds")
    ax.set_xticks(x_positions)
    ax.set_xticklabels(
        ["large file import" if phase == "tree_import" else "source tree import" for phase in phases],
        rotation=20,
        ha="right",
    )
    ax.legend()
    path = output_dir / "fs_import_latency.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return path


def plot_cow_latency(output_dir: Path, rows: list[dict[str, Any]]) -> Path:
    return plot_cow_metric(
        output_dir,
        rows,
        metric="median_elapsed_ms",
        ylabel="median milliseconds",
        title="Large File Branch Overwrite Latency",
        output_name="fs_cow_large_file_latency.png",
    )


def plot_cow_storage(output_dir: Path, rows: list[dict[str, Any]]) -> Path:
    return plot_cow_metric(
        output_dir,
        rows,
        metric="median_storage_delta_bytes",
        ylabel="median storage delta (MiB)",
        title="Large File Branch Overwrite Storage Growth",
        output_name="fs_cow_large_file_storage.png",
        value_scale=1 / (1024 * 1024),
    )


def plot_cow_metric(
    output_dir: Path,
    rows: list[dict[str, Any]],
    *,
    metric: str,
    ylabel: str,
    title: str,
    output_name: str,
    value_scale: float = 1.0,
) -> Path:
    write_sizes = sorted({int(row["parameter"]) for row in rows})
    backends = [backend for backend in BACKENDS if any(row["backend"] == backend for row in rows)]
    values = {
        (row["backend"], int(row["parameter"])): float(row[metric]) * value_scale
        for row in rows
    }
    low_metric, high_metric = percentile_fields_for_metric(metric)
    p10 = {
        (row["backend"], int(row["parameter"])): float(row[low_metric]) * value_scale
        for row in rows
    }
    p90 = {
        (row["backend"], int(row["parameter"])): float(row[high_metric]) * value_scale
        for row in rows
    }
    fig, ax = plt.subplots(figsize=(max(8, len(write_sizes) * 1.25), 5.5), constrained_layout=True)
    group_width = 0.8
    bar_width = group_width / max(1, len(backends))
    x_positions = list(range(len(write_sizes)))
    plotted_values: list[float] = []
    for backend_index, backend in enumerate(backends):
        offsets = [
            x - group_width / 2 + bar_width * (backend_index + 0.5)
            for x in x_positions
        ]
        heights = [values.get((backend, size), 0.0) for size in write_sizes]
        lows = [p10.get((backend, size), 0.0) for size in write_sizes]
        highs = [p90.get((backend, size), 0.0) for size in write_sizes]
        plotted_values.extend(heights)
        bars = ax.bar(
            offsets,
            heights,
            width=bar_width,
            label=backend,
            color=BACKEND_COLORS.get(backend, "#525252"),
            yerr=percentile_yerr(heights, lows, highs),
            **bar_error_kwargs(),
        )
        add_bar_labels(ax, bars, heights)
    ax.set_title(title)
    maybe_use_log_yaxis(ax, plotted_values, ylabel=ylabel)
    ax.set_xlabel("overwrite size")
    ax.set_xticks(x_positions)
    ax.set_xticklabels([format_bytes(size) for size in write_sizes], rotation=25, ha="right")
    ax.legend()
    path = output_dir / output_name
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return path


def percentile_fields_for_metric(metric: str) -> tuple[str, str]:
    if metric == "median_elapsed_ms":
        return "p10_elapsed_ms", "p90_elapsed_ms"
    if metric == "median_storage_delta_bytes":
        return "p10_storage_delta_bytes", "p90_storage_delta_bytes"
    raise ValueError(f"no percentile fields for metric: {metric}")


def write_markdown_summary(
    path: Path,
    summary_rows: list[dict[str, Any]],
    config: dict[str, Any],
    plot_paths: list[Path],
) -> None:
    unsupported = [
        row for row in summary_rows
        if row["status"] != "ok"
    ]
    unsupported_lines = "\n".join(
        f"- `{row['workload']}` `{row['backend']}` `{row['phase']}`"
        f"{' ' + str(row['parameter']) if row['parameter'] else ''}: "
        f"{row['status']} {row['error']}"
        for row in unsupported
    )
    image_lines = "\n".join(f"![{plot.stem}]({plot.name})" for plot in plot_paths)
    path.write_text(
        "# ChronosFS Benchmark\n\n"
        f"Generated at `{config['generated_at']}`.\n\n"
        "This benchmark compares SQLite-backed ChronosFS against basic "
        "overlay filesystem, XFS reflink, Btrfs subvolume, and Turso AgentFS "
        "branch baselines. "
        "Microbenchmarks measure branch "
        "creation/deletion, POSIX read/write operations, and direct-IO read/write "
        "operations. Direct I/O is requested on the mounted file descriptor; "
        "FUSE daemons and SQLite-backed systems may still use backend caches. "
        "The COW workload "
        "branches from a parent containing one large file and overwrites varied "
        "byte ranges, recording both latency and backend storage growth to show "
        "block-level versus file-level copy-on-write behavior. The optional compile "
        "workload creates a branch and builds Redis inside that branch.\n\n"
        "## Config\n\n"
        f"```json\n{json.dumps(config, indent=2, sort_keys=True)}\n```\n\n"
        "## Unsupported Or Failed Cases\n\n"
        f"{unsupported_lines or '- none'}\n\n"
        "## Plots\n\n"
        f"{image_lines or '- none'}\n\n"
        "Raw results are in `results.csv`, `summary.csv`, and `results.json`.\n"
    )


def backend_factory(
    name: str,
    cfg: BenchConfig,
    *,
    directio_profile: bool = False,
) -> FsBackend:
    if name == "chronosfs":
        return ChronosFSBenchBackend(
            sqlite_synchronous=(
                cfg.directio_sqlite_synchronous
                if directio_profile
                else cfg.chronosfs_sqlite_synchronous
            ),
            sqlite_cache_size_bytes=cfg.directio_cache_size if directio_profile else None,
            sqlite_wal_autocheckpoint_pages=(
                cfg.chronosfs_sqlite_wal_autocheckpoint_pages
            ),
        )
    if name == "overlayfs":
        return OverlayFSBenchBackend(root=cfg.overlayfs_root)
    if name == "xfs":
        return XFSReflinkBenchBackend(root=cfg.xfs_root)
    if name == "btrfs":
        return BtrfsSubvolumeBenchBackend(root=cfg.btrfs_root)
    if name == "turso":
        return TursoAgentFSBenchBackend(
            root=cfg.turso_root,
            agentfs_bin=cfg.turso_agentfs_bin,
            sqlite_synchronous=(
                cfg.directio_sqlite_synchronous
                if directio_profile
                else cfg.turso_sqlite_synchronous
            ),
            sqlite_cache_size_bytes=cfg.directio_cache_size if directio_profile else None,
        )
    raise ValueError(f"unknown backend: {name}")


def parse_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def parse_size(value: str) -> int:
    text = value.strip().lower()
    multipliers = {
        "k": 1024,
        "kb": 1024,
        "kib": 1024,
        "m": 1024 * 1024,
        "mb": 1024 * 1024,
        "mib": 1024 * 1024,
        "g": 1024 * 1024 * 1024,
        "gb": 1024 * 1024 * 1024,
        "gib": 1024 * 1024 * 1024,
    }
    for suffix, multiplier in sorted(multipliers.items(), key=lambda item: -len(item[0])):
        if text.endswith(suffix):
            return int(float(text[: -len(suffix)]) * multiplier)
    return int(text)


def parse_size_list(value: str) -> tuple[int, ...]:
    sizes = tuple(parse_size(item) for item in value.split(",") if item.strip())
    if not sizes:
        raise argparse.ArgumentTypeError("expected at least one size")
    if any(size <= 0 for size in sizes):
        raise argparse.ArgumentTypeError("sizes must be positive")
    return sizes


def format_bytes(value: int) -> str:
    units = [
        (1024 * 1024 * 1024, "GiB"),
        (1024 * 1024, "MiB"),
        (1024, "KiB"),
    ]
    for factor, suffix in units:
        if value >= factor and value % factor == 0:
            return f"{value // factor}{suffix}"
        if value >= factor:
            return f"{value / factor:.1f}{suffix}"
    return f"{value}B"


def optional_path_from_env(name: str) -> Path | None:
    value = os.environ.get(name)
    if not value:
        return None
    return Path(value)


def default_xfs_root() -> Path | None:
    configured = optional_path_from_env("CHRONOS_FS_BENCH_XFS_ROOT")
    if configured is not None:
        return configured
    candidates = (
        Path("/mnt/dbfork-nvme-xfs/chronos-fs-xfs-baseline"),
        Path("/mnt/dbfork-nvme-xfs"),
    )
    for candidate in candidates:
        if candidate.exists() and filesystem_type(candidate) == "xfs":
            if candidate.name == "chronos-fs-xfs-baseline":
                return candidate
            return candidate / "chronos-fs-xfs-baseline"
    return None


def default_overlayfs_root() -> Path | None:
    return optional_path_from_env("CHRONOS_FS_BENCH_OVERLAYFS_ROOT")


def default_btrfs_root() -> Path | None:
    configured = optional_path_from_env("CHRONOS_FS_BENCH_BTRFS_ROOT")
    if configured is not None:
        return configured
    candidates = (
        Path("/mnt/dbfork-btrfs-loop/chronos-fs-btrfs-baseline"),
        Path("/mnt/dbfork-btrfs-loop"),
    )
    for candidate in candidates:
        if candidate.exists() and filesystem_type(candidate) == "btrfs":
            if candidate.name == "chronos-fs-btrfs-baseline":
                return candidate
            return candidate / "chronos-fs-btrfs-baseline"
    return None


def phase_label(phase: str) -> str:
    return PHASE_LABELS.get(phase, phase.replace("_", " "))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark ChronosFS read/write and branch lifecycle performance."
    )
    parser.add_argument("--backends", default=DEFAULT_BACKENDS)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--branch-iterations", type=int, default=50)
    parser.add_argument("--file-count", type=int, default=64)
    parser.add_argument(
        "--file-size",
        type=parse_size,
        default=parse_size("16M"),
        help="Bytes per seeded file for the microbenchmark working set. Defaults to 16 MiB.",
    )
    parser.add_argument(
        "--io-duration-seconds",
        type=float,
        default=5.0,
        help="Timed duration for each POSIX random read/write phase. Defaults to 5 seconds.",
    )
    parser.add_argument(
        "--io-size",
        type=parse_size,
        default=parse_size("4K"),
        help="Bytes per random read/write operation. Defaults to 4 KiB.",
    )
    parser.add_argument(
        "--directio-cache-size",
        type=parse_size,
        default=parse_size("64M"),
        help=(
            "SQLite cache size requested for ChronosFS/Turso direct-IO phases. "
            "Defaults to 64 MiB."
        ),
    )
    parser.add_argument(
        "--directio-sqlite-synchronous",
        type=normalize_sqlite_synchronous,
        default="NORMAL",
        help=(
            "SQLite PRAGMA synchronous requested for ChronosFS/Turso direct-IO "
            "phases. Defaults to NORMAL."
        ),
    )
    parser.add_argument("--cow-file-size", type=parse_size, default=parse_size("512M"))
    parser.add_argument(
        "--cow-write-sizes",
        type=parse_size_list,
        default=parse_size_list("512,4K,64K,1M,4M"),
        help="Comma-separated overwrite sizes for the large-file COW workload.",
    )
    parser.add_argument(
        "--run-micro",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include microbenchmarks. Enabled by default.",
    )
    parser.add_argument(
        "--run-cow",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include the large-file COW benchmark. Enabled by default.",
    )
    parser.add_argument(
        "--run-compile",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include the Redis compile benchmark. Enabled by default.",
    )
    parser.add_argument("--compile-repeats", type=int, default=1)
    parser.add_argument("--compile-jobs", type=int, default=max(1, os.cpu_count() or 1))
    parser.add_argument("--redis-repo-url", default="https://github.com/redis/redis.git")
    parser.add_argument("--redis-ref", default="7.2.5")
    parser.add_argument("--redis-source", type=Path)
    parser.add_argument("--keep-workdir", action="store_true")
    parser.add_argument(
        "--chronosfs-sqlite-synchronous",
        type=normalize_sqlite_synchronous,
        default="OFF",
        help="SQLite PRAGMA synchronous for ChronosFS benchmark connections. Defaults to OFF.",
    )
    parser.add_argument(
        "--chronosfs-sqlite-wal-autocheckpoint-pages",
        type=int,
        default=int(
            os.environ.get(
                "CHRONOS_FS_BENCH_CHRONOSFS_WAL_AUTOCHECKPOINT_PAGES",
                "16384",
            )
        ),
        help=(
            "SQLite WAL autocheckpoint threshold, in pages, for ChronosFS benchmark "
            "connections. Use 0 to disable automatic checkpoints. Defaults to 16384."
        ),
    )
    parser.add_argument(
        "--xfs-root",
        type=Path,
        default=default_xfs_root(),
        help=(
            "Directory on an XFS filesystem used for the xfs reflink baseline. "
            "Defaults to CHRONOS_FS_BENCH_XFS_ROOT or the local /mnt/dbfork-nvme-xfs "
            "workdir when present. If omitted, the xfs backend requires the "
            "benchmark workdir itself to be on XFS."
        ),
    )
    parser.add_argument(
        "--overlayfs-root",
        type=Path,
        default=default_overlayfs_root(),
        help=(
            "Directory used for the overlayfs baseline. "
            "Defaults to CHRONOS_FS_BENCH_OVERLAYFS_ROOT. If omitted, overlayfs "
            "uses the benchmark workdir, which is typically on the host ext4 root."
        ),
    )
    parser.add_argument(
        "--btrfs-root",
        type=Path,
        default=default_btrfs_root(),
        help=(
            "Directory on a Btrfs filesystem used for the btrfs subvolume "
            "snapshot baseline. Defaults to CHRONOS_FS_BENCH_BTRFS_ROOT or "
            "the local /mnt/dbfork-btrfs-loop workdir when present. If omitted, "
            "the btrfs backend requires the benchmark workdir itself to be on Btrfs."
        ),
    )
    parser.add_argument(
        "--turso-root",
        type=Path,
        default=optional_path_from_env("CHRONOS_FS_BENCH_TURSO_ROOT"),
        help=(
            "Directory used for Turso AgentFS benchmark state. "
            "Defaults to CHRONOS_FS_BENCH_TURSO_ROOT. If omitted, state is "
            "written under the benchmark workdir."
        ),
    )
    parser.add_argument(
        "--turso-agentfs-bin",
        default="agentfs",
        help="AgentFS CLI binary used by the turso baseline.",
    )
    parser.add_argument(
        "--turso-sqlite-synchronous",
        type=normalize_sqlite_synchronous,
        default=normalize_sqlite_synchronous(
            os.environ.get("CHRONOS_FS_BENCH_TURSO_SQLITE_SYNCHRONOUS", "OFF")
        ),
        help=(
            "SQLite PRAGMA synchronous requested for Turso AgentFS benchmark DB "
            "post-init configuration. Defaults to OFF."
        ),
    )
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--output-dir", type=Path)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    backends = parse_csv(args.backends)
    unknown = sorted(set(backends) - set(BACKENDS))
    if unknown:
        raise SystemExit(f"unknown backend(s): {', '.join(unknown)}")
    # Quick mode reduces dataset sizes and operation counts, but it should not
    # collapse the repeat count.  The performance-acceptance gate compares
    # medians, and single-sample COW writes are too noisy to distinguish real
    # regressions from filesystem scheduling variance.
    repeats = args.repeats
    branch_iterations = min(args.branch_iterations, 3) if args.quick else args.branch_iterations
    file_count = max(1, min(args.file_count, 8) if args.quick else args.file_count)
    io_size = max(1, args.io_size)
    directio_cache_size = max(1024, args.directio_cache_size)
    io_duration_seconds = max(0.001, float(args.io_duration_seconds))
    if args.quick:
        io_duration_seconds = min(io_duration_seconds, 0.1)
    file_size = min(args.file_size, max(io_size, 4 * 1024)) if args.quick else args.file_size
    file_size = max(file_size, io_size)
    cow_file_size = min(args.cow_file_size, 1024 * 1024) if args.quick else args.cow_file_size
    cow_write_sizes = (
        tuple(size for size in args.cow_write_sizes if size <= cow_file_size)
        or (min(args.cow_write_sizes),)
    )
    if args.quick:
        cow_write_sizes = tuple(size for size in cow_write_sizes if size <= 64 * 1024) or (512,)
    compile_repeats = min(args.compile_repeats, 1) if args.quick else args.compile_repeats
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = args.output_dir or Path(".benchmarks") / f"fs-{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    cfg = BenchConfig(
        backends=backends,
        repeats=repeats,
        branch_iterations=branch_iterations,
        file_count=file_count,
        io_duration_seconds=io_duration_seconds,
        io_size=io_size,
        file_size=file_size,
        directio_cache_size=directio_cache_size,
        directio_sqlite_synchronous=args.directio_sqlite_synchronous,
        cow_file_size=cow_file_size,
        cow_write_sizes=cow_write_sizes,
        run_micro=bool(args.run_micro),
        run_cow=bool(args.run_cow),
        compile_repeats=compile_repeats,
        compile_jobs=args.compile_jobs,
        run_compile=bool(args.run_compile),
        redis_repo_url=args.redis_repo_url,
        redis_ref=args.redis_ref,
        redis_source=args.redis_source,
        keep_workdir=bool(args.keep_workdir),
        output_dir=output_dir,
        chronosfs_sqlite_synchronous=args.chronosfs_sqlite_synchronous,
        chronosfs_sqlite_wal_autocheckpoint_pages=(
            max(0, args.chronosfs_sqlite_wal_autocheckpoint_pages)
            if args.chronosfs_sqlite_wal_autocheckpoint_pages is not None
            else None
        ),
        overlayfs_root=args.overlayfs_root,
        xfs_root=args.xfs_root,
        btrfs_root=args.btrfs_root,
        turso_root=args.turso_root,
        turso_agentfs_bin=args.turso_agentfs_bin,
        turso_sqlite_synchronous=args.turso_sqlite_synchronous,
    )
    config = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "backends": list(cfg.backends),
        "repeats": cfg.repeats,
        "branch_iterations": cfg.branch_iterations,
        "file_count": cfg.file_count,
        "io_duration_seconds": cfg.io_duration_seconds,
        "io_size": cfg.io_size,
        "read_warmup": "sequential full-file pass over every open file before the timed random-read phase",
        "file_size": cfg.file_size,
        "directio_cache_size": cfg.directio_cache_size,
        "directio_cache_size_kib": sqlite_cache_size_kib(cfg.directio_cache_size),
        "directio_sqlite_synchronous": cfg.directio_sqlite_synchronous,
        "cow_file_size": cfg.cow_file_size,
        "cow_write_sizes": list(cfg.cow_write_sizes),
        "run_micro": cfg.run_micro,
        "run_cow": cfg.run_cow,
        "run_compile": cfg.run_compile,
        "compile_repeats": cfg.compile_repeats,
        "compile_jobs": cfg.compile_jobs,
        "redis_repo_url": cfg.redis_repo_url,
        "redis_ref": cfg.redis_ref,
        "redis_source": str(cfg.redis_source) if cfg.redis_source else None,
        "chronosfs_sqlite_synchronous": cfg.chronosfs_sqlite_synchronous,
        "chronosfs_sqlite_wal_autocheckpoint_pages": (
            cfg.chronosfs_sqlite_wal_autocheckpoint_pages
        ),
        "overlayfs_root": str(cfg.overlayfs_root) if cfg.overlayfs_root else None,
        "xfs_root": str(cfg.xfs_root) if cfg.xfs_root else None,
        "btrfs_root": str(cfg.btrfs_root) if cfg.btrfs_root else None,
        "turso_root": str(cfg.turso_root) if cfg.turso_root else None,
        "turso_agentfs_bin": cfg.turso_agentfs_bin,
        "turso_sqlite_synchronous": cfg.turso_sqlite_synchronous,
    }
    rows: list[dict[str, Any]] = []
    tmp_context = tempfile.TemporaryDirectory(prefix="chronos-fs-bench-")
    work_root = Path(tmp_context.name)
    if cfg.keep_workdir:
        tmp_context.cleanup()
        work_root = output_dir / "workdir"
        work_root.mkdir(parents=True, exist_ok=True)
    try:
        if cfg.run_micro:
            for backend_name in cfg.backends:
                backend_workdir = work_root / backend_name / "micro"
                backend_workdir.mkdir(parents=True, exist_ok=True)
                print(f"micro backend={backend_name}", flush=True)
                rows.extend(run_microbench(backend_name, cfg, backend_workdir))
        if cfg.run_cow:
            for backend_name in cfg.backends:
                backend_workdir = work_root / backend_name / "cow"
                backend_workdir.mkdir(parents=True, exist_ok=True)
                print(f"cow backend={backend_name}", flush=True)
                rows.extend(
                    run_cowbench(
                        backend_factory(backend_name, cfg, directio_profile=True),
                        cfg,
                        backend_workdir,
                    )
                )
        if cfg.run_compile:
            for backend_name in cfg.backends:
                backend_workdir = work_root / backend_name / "compile"
                backend_workdir.mkdir(parents=True, exist_ok=True)
                print(f"compile backend={backend_name}", flush=True)
                rows.extend(run_compilebench(backend_factory(backend_name, cfg), cfg, backend_workdir))
    finally:
        if not cfg.keep_workdir:
            tmp_context.cleanup()

    summary_rows = summarize(rows)
    write_csv(output_dir / "results.csv", rows, CSV_FIELDS)
    write_csv(output_dir / "summary.csv", summary_rows, SUMMARY_FIELDS)
    write_json(output_dir / "results.json", rows, summary_rows, config)
    plot_paths = plot_results(output_dir, summary_rows)
    write_markdown_summary(output_dir / "README.md", summary_rows, config, plot_paths)
    print(f"\nWrote filesystem benchmark results to {output_dir}")
    if plot_paths:
        print(f"Wrote matplotlib plots: {', '.join(path.name for path in plot_paths)}")


if __name__ == "__main__":
    main()
