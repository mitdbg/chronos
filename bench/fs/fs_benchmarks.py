from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
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
from chronos_core.workspace.filesystem import ChronosFilesystemStore
from chronos_core.workspace.chronosfs import ChronosFSStore


BACKENDS = ("chronosfs", "overlayfs")
SQLITE_SYNCHRONOUS_MODES = {"OFF", "NORMAL", "FULL", "EXTRA"}
MICRO_PHASES = (
    "branch_create",
    "branch_delete",
    "file_create_write",
    "file_read",
    "partial_overwrite",
    "readdir_stat",
)
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
    "avg_elapsed_ms",
    "median_ms_per_op",
    "median_ops_per_sec",
    "median_mb_per_sec",
    "median_storage_delta_bytes",
    "error",
]


@dataclass(frozen=True)
class BenchConfig:
    backends: tuple[str, ...]
    repeats: int
    branch_iterations: int
    file_count: int
    file_ops_per_file: int
    file_size: int
    overwrite_size: int
    cow_file_size: int
    cow_write_sizes: tuple[int, ...]
    compile_repeats: int
    compile_jobs: int
    run_compile: bool
    redis_repo_url: str
    redis_ref: str
    redis_source: Path | None
    keep_workdir: bool
    output_dir: Path
    chronosfs_sqlite_synchronous: str


class UnsupportedBackend(RuntimeError):
    pass


def normalize_sqlite_synchronous(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in SQLITE_SYNCHRONOUS_MODES:
        raise argparse.ArgumentTypeError(
            "expected one of: " + ", ".join(sorted(SQLITE_SYNCHRONOUS_MODES))
        )
    return normalized


def configure_benchmark_sqlite(db: Any, synchronous: str) -> None:
    if getattr(db, "dialect", None) != "sqlite":
        return
    db.execute("PRAGMA journal_mode=WAL")
    db.execute(f"PRAGMA synchronous={normalize_sqlite_synchronous(synchronous)}")
    db.execute("PRAGMA fullfsync=OFF")
    db.execute("PRAGMA checkpoint_fullfsync=OFF")


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

    def storage_bytes(self) -> int:
        raise NotImplementedError


class ChronosFSBenchBackend(FsBackend):
    name = "chronosfs"

    def __init__(self, block_size: int = 4096, sqlite_synchronous: str = "NORMAL"):
        self.block_size = block_size
        self.sqlite_synchronous = sqlite_synchronous
        self.context: ChronosBranchContext | None = None
        self.store: ChronosFSStore | None = None
        self.db_path: Path | None = None

    def setup(self, workdir: Path, *, file_count: int, file_size: int) -> None:
        db_path = workdir / "chronosfs.sqlite"
        self.db_path = db_path
        self.context = ChronosBranchContext.connect(
            f"sqlite:///{db_path}",
            backend="interval",
        )
        configure_benchmark_sqlite(self.context.db, self.sqlite_synchronous)
        self.store = ChronosFSStore(self.context, block_size=self.block_size)
        self.store.ensure()
        seed_chronosfs(self.store, "main", file_count=file_count, file_size=file_size)

    def cleanup(self) -> None:
        if self.context is not None:
            self.context.close()

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
        ) as mountpoint:
            yield mountpoint

    def import_tree(self, source: Path) -> None:
        import_tree_to_chronosfs(self._store(), "main", source)

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


class OverlayFSBenchBackend(FsBackend):
    name = "overlayfs"

    def __init__(self):
        self.store: ChronosFilesystemStore | None = None
        self.state_dir: Path | None = None

    def setup(self, workdir: Path, *, file_count: int, file_size: int) -> None:
        if shutil.which("fuse-overlayfs") is None:
            raise UnsupportedBackend("fuse-overlayfs is not installed")
        if shutil.which("fusermount3") is None and shutil.which("fusermount") is None:
            raise UnsupportedBackend("fusermount3 or fusermount is required")
        root = workdir / "overlay-root"
        root.mkdir(parents=True, exist_ok=True)
        seed_posix_tree(root, file_count=file_count, file_size=file_size)
        self.state_dir = workdir / "overlay-state"
        self.store = ChronosFilesystemStore(
            root=root,
            state_dir=self.state_dir,
        )

    def cleanup(self) -> None:
        if self.store is None:
            return
        for branch_id in list(self.store._mounts):  # noqa: SLF001 - benchmark cleanup.
            with suppress_errors():
                self.store.unmount(branch_id)

    def create_branch(self, branch_id: str, from_branch: str = "main") -> None:
        self._store().create_branch(branch_id, from_branch=from_branch)

    def delete_branch(self, branch_id: str) -> None:
        self._store().delete_branch(branch_id)

    @contextmanager
    def checkout_path(self, branch_id: str) -> Iterator[Path]:
        session = self._store().checkout(branch_id)
        try:
            yield session.path
        finally:
            self._store().unmount(branch_id)

    def import_tree(self, source: Path) -> None:
        store = self._store()
        if store.root.exists():
            shutil.rmtree(store.root)
        shutil.copytree(source, store.root, symlinks=True, ignore=ignore_benchmark_paths)

    def storage_bytes(self) -> int:
        if self.state_dir is None:
            return 0
        return directory_size(self.state_dir)

    def _store(self) -> ChronosFilesystemStore:
        if self.store is None:
            raise RuntimeError("backend is not set up")
        return self.store


@contextmanager
def suppress_errors() -> Iterator[None]:
    try:
        yield
    except Exception:
        pass


def seed_bytes(size: int, seed: int) -> bytes:
    pattern = f"chronosfs-bench-{seed:08d}-".encode()
    return (pattern * ((size // len(pattern)) + 1))[:size]


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


def run_microbench(backend: FsBackend, cfg: BenchConfig, workdir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        backend.setup(workdir, file_count=cfg.file_count, file_size=cfg.file_size)
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
        rows.extend(bench_branch_create_delete(backend, cfg))
        rows.extend(bench_posix_io(backend, cfg))
    finally:
        backend.cleanup()
    return rows


def run_cowbench(backend: FsBackend, cfg: BenchConfig, workdir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source = workdir / "source"
    source.mkdir(parents=True, exist_ok=True)
    large_file = source / "large.bin"
    write_large_file(large_file, cfg.cow_file_size)
    try:
        backend.setup(workdir / "backend", file_count=0, file_size=cfg.file_size)
        backend.import_tree(source)
        for write_size in cfg.cow_write_sizes:
            for repeat in range(cfg.repeats):
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
        payload = seed_bytes(write_size, write_size + repeat)
        offset = max(0, cfg.cow_file_size // 2 - write_size // 2)
        with backend.checkout_path(branch_id) as root:
            start = time.perf_counter_ns()
            with (root / "large.bin").open("r+b") as handle:
                handle.seek(offset)
                handle.write(payload)
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
                "timed_scope": "open_seek_write_close",
                "checkout_included": False,
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
            backend.create_branch(branch_id, from_branch="main")
            with backend.checkout_path(branch_id) as path:
                rows.append(run_file_create_write(path, backend.name, repeat, cfg))
                rows.append(run_file_read(path, backend.name, repeat, cfg))
                rows.append(run_partial_overwrite(path, backend.name, repeat, cfg))
                rows.append(run_readdir_stat(path, backend.name, repeat, cfg))
        except Exception as exc:
            for phase in (
                "file_create_write",
                "file_read",
                "partial_overwrite",
                "readdir_stat",
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


def run_file_create_write(
    root: Path,
    backend: str,
    repeat: int,
    cfg: BenchConfig,
) -> dict[str, Any]:
    payload = seed_bytes(cfg.file_size, 99_999 + repeat)
    ops = cfg.file_count * cfg.file_ops_per_file
    start = time.perf_counter_ns()
    paths = created_file_paths(root, cfg.file_count)
    with ExitStack() as stack:
        handles = []
        for path in paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            handles.append(stack.enter_context(path.open("w+b")))
        for op_index in range(ops):
            handle = handles[op_index % len(handles)]
            handle.seek(0)
            handle.truncate(0)
            handle.write(payload)
        for handle in handles:
            handle.flush()
    elapsed_ms = elapsed_since_ms(start)
    return ok_row(
        workload="micro",
        backend=backend,
        phase="file_create_write",
        repeat=repeat,
        elapsed_ms=elapsed_ms,
        ops=ops,
        byte_count=ops * cfg.file_size,
        details=working_set_details(cfg),
    )


def run_file_read(
    root: Path,
    backend: str,
    repeat: int,
    cfg: BenchConfig,
) -> dict[str, Any]:
    total = 0
    ops = cfg.file_count * cfg.file_ops_per_file
    start = time.perf_counter_ns()
    with ExitStack() as stack:
        handles = [stack.enter_context(path.open("rb")) for path in data_file_paths(root, cfg.file_count)]
        for op_index in range(ops):
            handle = handles[op_index % len(handles)]
            handle.seek(0)
            total += len(handle.read())
    elapsed_ms = elapsed_since_ms(start)
    return ok_row(
        workload="micro",
        backend=backend,
        phase="file_read",
        repeat=repeat,
        elapsed_ms=elapsed_ms,
        ops=ops,
        byte_count=total,
        details=working_set_details(cfg),
    )


def run_partial_overwrite(
    root: Path,
    backend: str,
    repeat: int,
    cfg: BenchConfig,
) -> dict[str, Any]:
    payload = seed_bytes(cfg.overwrite_size, 123_456 + repeat)
    ops = cfg.file_count * cfg.file_ops_per_file
    offset = max(0, cfg.file_size // 2 - cfg.overwrite_size // 2)
    start = time.perf_counter_ns()
    with ExitStack() as stack:
        handles = [stack.enter_context(path.open("r+b")) for path in data_file_paths(root, cfg.file_count)]
        for op_index in range(ops):
            handle = handles[op_index % len(handles)]
            handle.seek(offset)
            handle.write(payload)
        for handle in handles:
            handle.flush()
    elapsed_ms = elapsed_since_ms(start)
    return ok_row(
        workload="micro",
        backend=backend,
        phase="partial_overwrite",
        repeat=repeat,
        elapsed_ms=elapsed_ms,
        ops=ops,
        byte_count=ops * cfg.overwrite_size,
        details=working_set_details(cfg) | {"offset": offset},
    )


def data_file_paths(root: Path, file_count: int) -> list[Path]:
    return [
        root / "data" / f"{index // 256:04d}" / f"file_{index:06d}.bin"
        for index in range(file_count)
    ]


def created_file_paths(root: Path, file_count: int) -> list[Path]:
    return [
        root / "created" / f"{index // 256:04d}" / f"new_{index:06d}.bin"
        for index in range(file_count)
    ]


def working_set_details(cfg: BenchConfig) -> dict[str, int]:
    return {
        "open_file_count": cfg.file_count,
        "ops_per_file": cfg.file_ops_per_file,
        "total_ops": cfg.file_count * cfg.file_ops_per_file,
    }


def run_readdir_stat(
    root: Path,
    backend: str,
    repeat: int,
    cfg: BenchConfig,
) -> dict[str, Any]:
    count = 0
    start = time.perf_counter_ns()
    for path in (root / "data").rglob("*"):
        path.stat()
        count += 1
    elapsed_ms = elapsed_since_ms(start)
    return ok_row(
        workload="micro",
        backend=backend,
        phase="readdir_stat",
        repeat=repeat,
        elapsed_ms=elapsed_ms,
        ops=count,
        byte_count=0,
    )


def run_compilebench(backend: FsBackend, cfg: BenchConfig, workdir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source_parent = workdir / "source"
    source_parent.mkdir(parents=True, exist_ok=True)
    try:
        source = prepare_redis_source(cfg, source_parent)
        backend.setup(workdir / "backend", file_count=0, file_size=cfg.file_size)
        backend.import_tree(source)
        for repeat in range(cfg.compile_repeats):
            branch_id = f"compile_{repeat}"
            backend.create_branch(branch_id, from_branch="main")
            try:
                with backend.checkout_path(branch_id) as path:
                    rows.append(run_redis_make(path, backend.name, repeat, cfg))
            finally:
                with suppress_errors():
                    backend.delete_branch(branch_id)
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
        ["make", f"-j{cfg.compile_jobs}", "BUILD_TLS=no"],
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
    for path in sorted(source.rglob("*")):
        rel = path.relative_to(source)
        if should_skip_path(rel):
            continue
        target = "/" + rel.as_posix()
        if path.is_symlink():
            store.symlink(branch_id, os.readlink(path), target, parents=True)
        elif path.is_dir():
            store.mkdir(branch_id, target, parents=True)
        elif path.is_file():
            store.write_file(branch_id, target, path.read_bytes(), parents=True)


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
    sqlite_synchronous: str = "NORMAL",
) -> Iterator[Path]:
    import chronos_core.workspace.chronosfs.fuse as fuse_module

    if fuse_module.pyfuse3 is None:
        raise UnsupportedBackend("pyfuse3/trio are required for ChronosFS FUSE")
    mountpoint = Path(tempfile.mkdtemp(prefix=f"chronosfs-{branch_id}-"))
    code = (
        "from chronos_core.workspace.chronosfs import ChronosFSStore, mount_chronosfs\n"
        f"store = ChronosFSStore.connect({database_url!r}, backend='interval', block_size={store.block_size!r})\n"
        "if store.context.db.dialect == 'sqlite':\n"
        "    store.context.db.execute('PRAGMA journal_mode=WAL')\n"
        f"    store.context.db.execute('PRAGMA synchronous={normalize_sqlite_synchronous(sqlite_synchronous)}')\n"
        "    store.context.db.execute('PRAGMA fullfsync=OFF')\n"
        "    store.context.db.execute('PRAGMA checkpoint_fullfsync=OFF')\n"
        "store.ensure()\n"
        f"mount_chronosfs(store, {str(mountpoint)!r}, branch_id={branch_id!r})\n"
    )
    env = os.environ.copy()
    src = str(Path(__file__).resolve().parents[2] / "packages" / "chronos-core" / "src")
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = src + (os.pathsep + existing if existing else "")
    proc = subprocess.Popen(
        [sys.executable, "-c", code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        wait_for_mount(proc, mountpoint)
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


def wait_for_mount(proc: subprocess.Popen[str], mountpoint: Path) -> None:
    deadline = time.time() + 15
    while time.time() < deadline:
        if proc.poll() is not None:
            stdout, stderr = proc.communicate(timeout=1)
            raise UnsupportedBackend(
                "ChronosFS mount exited early: "
                f"stdout={stdout[-1000:]!r} stderr={stderr[-2000:]!r}"
            )
        if os.path.ismount(mountpoint):
            return
        time.sleep(0.05)
    raise UnsupportedBackend(f"ChronosFS mount did not become ready: {mountpoint}")


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
                    "avg_elapsed_ms": 0.0,
                    "median_ms_per_op": 0.0,
                    "median_ops_per_sec": 0.0,
                    "median_mb_per_sec": 0.0,
                    "median_storage_delta_bytes": 0,
                    "error": first["error"],
                }
            )
            continue
        elapsed = [float(row["elapsed_ms"]) for row in ok]
        per_op = [
            float(row["elapsed_ms"]) / max(1, int(row["ops"]))
            for row in ok
        ]
        result.append(
            {
                "workload": workload,
                "backend": backend,
                "phase": phase,
                "parameter": parameter,
                "status": "ok",
                "repetitions": len(ok),
                "median_elapsed_ms": median(elapsed),
                "avg_elapsed_ms": sum(elapsed) / len(elapsed),
                "median_ms_per_op": median(per_op),
                "median_ops_per_sec": median(float(row["ops_per_sec"]) for row in ok),
                "median_mb_per_sec": median(float(row["mb_per_sec"]) for row in ok),
                "median_storage_delta_bytes": median(
                    float(row["storage_delta_bytes"]) for row in ok
                ),
                "error": "",
            }
        )
    return result


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
            if row["phase"] in {"file_create_write", "file_read", "partial_overwrite"}
        ]
        if throughput_rows:
            paths.append(plot_micro_throughput(output_dir, throughput_rows))
    compile_rows = [
        row for row in summary_rows
        if row["workload"] == "compile" and row["status"] == "ok"
    ]
    if compile_rows:
        paths.append(plot_compile(output_dir, compile_rows))
    cow_rows = [
        row for row in summary_rows
        if row["workload"] == "cow" and row["status"] == "ok"
    ]
    if cow_rows:
        paths.append(plot_cow_latency(output_dir, cow_rows))
        paths.append(plot_cow_storage(output_dir, cow_rows))
    return paths


def plot_micro_latency(output_dir: Path, rows: list[dict[str, Any]]) -> Path:
    phases = [phase for phase in MICRO_PHASES if any(row["phase"] == phase for row in rows)]
    backends = [backend for backend in BACKENDS if any(row["backend"] == backend for row in rows)]
    values = {
        (row["backend"], row["phase"]): float(row["median_ms_per_op"])
        for row in rows
    }
    fig, ax = plt.subplots(figsize=(max(10, len(phases) * 1.4), 5.5), constrained_layout=True)
    group_width = 0.8
    bar_width = group_width / max(1, len(backends))
    x_positions = list(range(len(phases)))
    colors = {"chronosfs": "#2563eb", "overlayfs": "#16a34a"}
    for backend_index, backend in enumerate(backends):
        offsets = [
            x - group_width / 2 + bar_width * (backend_index + 0.5)
            for x in x_positions
        ]
        ax.bar(
            offsets,
            [values.get((backend, phase), 0.0) for phase in phases],
            width=bar_width,
            label=backend,
            color=colors.get(backend, "#525252"),
        )
    ax.set_title("Filesystem Microbenchmark Latency")
    ax.set_ylabel("median milliseconds per operation")
    ax.set_xticks(x_positions)
    ax.set_xticklabels([phase.replace("_", " ") for phase in phases], rotation=25, ha="right")
    ax.grid(axis="y", alpha=0.25)
    ax.legend()
    path = output_dir / "fs_micro_latency.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return path


def plot_micro_throughput(output_dir: Path, rows: list[dict[str, Any]]) -> Path:
    phases = sorted({row["phase"] for row in rows})
    backends = [backend for backend in BACKENDS if any(row["backend"] == backend for row in rows)]
    values = {
        (row["backend"], row["phase"]): float(row["median_mb_per_sec"])
        for row in rows
    }
    fig, ax = plt.subplots(figsize=(max(8, len(phases) * 1.5), 5.5), constrained_layout=True)
    group_width = 0.8
    bar_width = group_width / max(1, len(backends))
    x_positions = list(range(len(phases)))
    colors = {"chronosfs": "#60a5fa", "overlayfs": "#86efac"}
    for backend_index, backend in enumerate(backends):
        offsets = [
            x - group_width / 2 + bar_width * (backend_index + 0.5)
            for x in x_positions
        ]
        ax.bar(
            offsets,
            [values.get((backend, phase), 0.0) for phase in phases],
            width=bar_width,
            label=backend,
            color=colors.get(backend, "#525252"),
        )
    ax.set_title("Filesystem Microbenchmark Throughput")
    ax.set_ylabel("median MiB/s")
    ax.set_xticks(x_positions)
    ax.set_xticklabels([phase.replace("_", " ") for phase in phases], rotation=25, ha="right")
    ax.grid(axis="y", alpha=0.25)
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
    fig, ax = plt.subplots(figsize=(7, 5), constrained_layout=True)
    ax.bar(
        list(range(len(backends))),
        [values.get(backend, 0.0) for backend in backends],
        color=["#2563eb" if backend == "chronosfs" else "#16a34a" for backend in backends],
    )
    ax.set_title("Redis Compile After Branch")
    ax.set_ylabel("median seconds")
    ax.set_xticks(list(range(len(backends))))
    ax.set_xticklabels(backends)
    ax.grid(axis="y", alpha=0.25)
    path = output_dir / "fs_compile_redis.png"
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
    fig, ax = plt.subplots(figsize=(max(8, len(write_sizes) * 1.25), 5.5), constrained_layout=True)
    group_width = 0.8
    bar_width = group_width / max(1, len(backends))
    x_positions = list(range(len(write_sizes)))
    colors = {"chronosfs": "#2563eb", "overlayfs": "#16a34a"}
    for backend_index, backend in enumerate(backends):
        offsets = [
            x - group_width / 2 + bar_width * (backend_index + 0.5)
            for x in x_positions
        ]
        ax.bar(
            offsets,
            [values.get((backend, size), 0.0) for size in write_sizes],
            width=bar_width,
            label=backend,
            color=colors.get(backend, "#525252"),
        )
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xlabel("overwrite size")
    ax.set_xticks(x_positions)
    ax.set_xticklabels([format_bytes(size) for size in write_sizes], rotation=25, ha="right")
    ax.grid(axis="y", alpha=0.25)
    ax.legend()
    path = output_dir / output_name
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return path


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
        "This benchmark compares SQLite-backed ChronosFS against a basic "
        "overlay filesystem branch baseline. Microbenchmarks measure branch "
        "creation/deletion and POSIX read/write operations. The COW workload "
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


def backend_factory(name: str, cfg: BenchConfig) -> FsBackend:
    if name == "chronosfs":
        return ChronosFSBenchBackend(sqlite_synchronous=cfg.chronosfs_sqlite_synchronous)
    if name == "overlayfs":
        return OverlayFSBenchBackend()
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark ChronosFS read/write and branch lifecycle performance."
    )
    parser.add_argument("--backends", default="chronosfs,overlayfs")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--branch-iterations", type=int, default=50)
    parser.add_argument("--file-count", type=int, default=200)
    parser.add_argument(
        "--file-ops-per-file",
        type=int,
        default=8,
        help="Number of read/write operations to issue per open file in the POSIX IO working set.",
    )
    parser.add_argument("--file-size", type=int, default=4096)
    parser.add_argument("--overwrite-size", type=int, default=512)
    parser.add_argument("--cow-file-size", type=parse_size, default=parse_size("64M"))
    parser.add_argument(
        "--cow-write-sizes",
        type=parse_size_list,
        default=parse_size_list("512,4K,64K,1M,4M"),
        help="Comma-separated overwrite sizes for the large-file COW workload.",
    )
    parser.add_argument("--run-compile", action="store_true")
    parser.add_argument("--compile-repeats", type=int, default=1)
    parser.add_argument("--compile-jobs", type=int, default=max(1, os.cpu_count() or 1))
    parser.add_argument("--redis-repo-url", default="https://github.com/redis/redis.git")
    parser.add_argument("--redis-ref", default="7.2.5")
    parser.add_argument("--redis-source", type=Path)
    parser.add_argument("--keep-workdir", action="store_true")
    parser.add_argument(
        "--chronosfs-sqlite-synchronous",
        type=normalize_sqlite_synchronous,
        default="NORMAL",
        help="SQLite PRAGMA synchronous for ChronosFS benchmark connections. Defaults to NORMAL.",
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
    repeats = min(args.repeats, 1) if args.quick else args.repeats
    branch_iterations = min(args.branch_iterations, 3) if args.quick else args.branch_iterations
    file_count = max(1, min(args.file_count, 8) if args.quick else args.file_count)
    file_ops_per_file = min(args.file_ops_per_file, 2) if args.quick else args.file_ops_per_file
    file_size = min(args.file_size, 1024) if args.quick else args.file_size
    overwrite_size = min(args.overwrite_size, 128) if args.quick else args.overwrite_size
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
        file_ops_per_file=max(1, file_ops_per_file),
        file_size=file_size,
        overwrite_size=overwrite_size,
        cow_file_size=cow_file_size,
        cow_write_sizes=cow_write_sizes,
        compile_repeats=compile_repeats,
        compile_jobs=args.compile_jobs,
        run_compile=bool(args.run_compile),
        redis_repo_url=args.redis_repo_url,
        redis_ref=args.redis_ref,
        redis_source=args.redis_source,
        keep_workdir=bool(args.keep_workdir),
        output_dir=output_dir,
        chronosfs_sqlite_synchronous=args.chronosfs_sqlite_synchronous,
    )
    config = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "backends": list(cfg.backends),
        "repeats": cfg.repeats,
        "branch_iterations": cfg.branch_iterations,
        "file_count": cfg.file_count,
        "file_ops_per_file": cfg.file_ops_per_file,
        "file_size": cfg.file_size,
        "overwrite_size": cfg.overwrite_size,
        "cow_file_size": cfg.cow_file_size,
        "cow_write_sizes": list(cfg.cow_write_sizes),
        "run_compile": cfg.run_compile,
        "compile_repeats": cfg.compile_repeats,
        "compile_jobs": cfg.compile_jobs,
        "redis_repo_url": cfg.redis_repo_url,
        "redis_ref": cfg.redis_ref,
        "redis_source": str(cfg.redis_source) if cfg.redis_source else None,
        "chronosfs_sqlite_synchronous": cfg.chronosfs_sqlite_synchronous,
    }
    rows: list[dict[str, Any]] = []
    tmp_context = tempfile.TemporaryDirectory(prefix="chronos-fs-bench-")
    work_root = Path(tmp_context.name)
    if cfg.keep_workdir:
        tmp_context.cleanup()
        work_root = output_dir / "workdir"
        work_root.mkdir(parents=True, exist_ok=True)
    try:
        for backend_name in cfg.backends:
            backend_workdir = work_root / backend_name / "micro"
            backend_workdir.mkdir(parents=True, exist_ok=True)
            print(f"micro backend={backend_name}", flush=True)
            rows.extend(run_microbench(backend_factory(backend_name, cfg), cfg, backend_workdir))
        for backend_name in cfg.backends:
            backend_workdir = work_root / backend_name / "cow"
            backend_workdir.mkdir(parents=True, exist_ok=True)
            print(f"cow backend={backend_name}", flush=True)
            rows.extend(run_cowbench(backend_factory(backend_name, cfg), cfg, backend_workdir))
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
