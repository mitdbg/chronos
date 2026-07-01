"""Native ChronosFS FUSE mount API."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path

from chronos_core import _native_interval
from chronos_core.workspace.chronosfs.store import ChronosFSStore


class ChronosFSMountError(Exception):
    """Raised when a ChronosFS FUSE mount cannot start."""


class ChronosFuseOperations:
    """Small compatibility shim for tests of store/FUSE dispatch policy.

    This is not a FUSE implementation.  Real mounts go through native libfuse
    via `mount_chronosfs()`.  The shim keeps direct unit tests focused on the
    store-level dispatch behavior without reintroducing pyfuse3.
    """

    def __init__(self, store: ChronosFSStore, branch_id: str = "main") -> None:
        self.store = store
        self.branch_id = branch_id
        self._next_fh = 1
        self._handles: dict[int, int] = {}

    def _new_handle(self, inode_id: int) -> int:
        fh = self._next_fh
        self._next_fh += 1
        self._handles[fh] = inode_id
        return fh

    async def read(self, fh: int, off: int, size: int) -> bytes:
        return self.store.read_inode_range(self.branch_id, self._handles[fh], off, size)

    def _lookup_child(self, *_args: object) -> object:
        raise ChronosFSMountError("_lookup_child is not used by the native FUSE shim")

    def _regular_readdir(self, inode_id: int) -> list[tuple[str, object]]:
        entries = [(".", self.store.stat_inode(self.branch_id, inode_id))]
        entries.append(("..", self.store.stat_inode(self.branch_id, 1)))
        for name in self.store.listdir_inode(self.branch_id, inode_id):
            entries.append((name, self.store.lookup_child(self.branch_id, inode_id, name)))
        return entries


def mount_chronosfs(
    store: ChronosFSStore,
    mountpoint: str | Path,
    *,
    branch_id: str = "main",
    foreground: bool = True,
    options: set[str] | None = None,
    shared_daemon: bool = True,
    shutdown_daemon_on_unmount: bool = False,
) -> None:
    """Mount ChronosFS with native libfuse and block until unmounted."""
    if not foreground:
        raise ChronosFSMountError("native ChronosFS mounts currently run in foreground mode")
    database_url = _database_url_for_mount(store)
    mount_path = Path(mountpoint)
    mount_options = _with_default_cache_options(options or set())
    if shared_daemon:
        _mount_via_shared_daemon(
            database_url,
            mount_path,
            branch_id=branch_id,
            block_size=store.block_size,
            options=mount_options,
            shutdown_daemon_on_unmount=shutdown_daemon_on_unmount,
        )
        return
    try:
        rc = _native_interval.mount_chronosfs_native(
            database_url,
            str(mount_path),
            branch_id,
            store.block_size,
            mount_options,
        )
    except Exception as exc:  # pragma: no cover - host FUSE setup dependent.
        raise ChronosFSMountError(str(exc)) from exc
    if rc != 0:
        raise ChronosFSMountError(f"native ChronosFS mount exited with status {rc}")


def start_chronosfs_mount(
    store: ChronosFSStore,
    mountpoint: str | Path,
    *,
    branch_id: str = "main",
    options: set[str] | None = None,
) -> Path:
    """Start a shared-daemon ChronosFS mount and return once it is ready.

    This is the non-blocking companion to :func:`mount_chronosfs`.  It is
    intended for control-plane integrations such as MCP servers that need to
    hand a mounted POSIX path back to another process.  The mount is hosted by
    the same shared local daemon used by ``mount_chronosfs(shared_daemon=True)``,
    so multiple mount points for one backing store share one native backend.
    """
    database_url = _database_url_for_mount(store)
    mount_path = Path(mountpoint)
    _start_shared_chronosfs_mount(
        database_url,
        mount_path,
        branch_id=branch_id,
        block_size=store.block_size,
        options=_with_default_cache_options(options or set()),
    )
    return mount_path


def _database_url_for_mount(store: ChronosFSStore) -> str:
    database_url = getattr(store.context.db, "database_url", None)
    if not database_url:
        database_path = getattr(store.context.db, "database_path", None)
        if database_path:
            database_url = f"sqlite:///{database_path}"
    if not database_url:
        raise ChronosFSMountError("native ChronosFS FUSE requires a file-backed database URL")
    return str(database_url)


def _runtime_dir() -> Path:
    base = os.environ.get("XDG_RUNTIME_DIR")
    if base:
        root = Path(base)
    else:
        root = Path("/tmp") / f"chronosfs-{os.getuid() if hasattr(os, 'getuid') else os.getpid()}"
    path = root / "mount-daemons"
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def _with_default_cache_options(options: set[str]) -> list[str]:
    merged = set(options)
    defaults = {
        "entry_timeout": "1",
        "attr_timeout": "1",
        "negative_timeout": "0",
    }
    for name, value in defaults.items():
        if not any(option == name or option.startswith(name + "=") for option in merged):
            merged.add(f"{name}={value}")
    return sorted(merged)


def _daemon_key(database_url: str, block_size: int) -> str:
    raw = json.dumps(
        {
            "database_url": database_url,
            "block_size": block_size,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _socket_request(socket_path: Path, request: dict[str, object], *, timeout: float = 2.0) -> dict[str, object]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        client.connect(str(socket_path))
        client.sendall(json.dumps(request).encode("utf-8"))
        raw = client.recv(65536)
    response = json.loads(raw.decode("utf-8"))
    if not isinstance(response, dict):
        raise ChronosFSMountError("shared ChronosFS daemon returned an invalid response")
    return response


def _wait_for_daemon(socket_path: Path, *, deadline_s: float = 10.0) -> None:
    deadline = time.monotonic() + deadline_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            _socket_request(socket_path, {"ping": True}, timeout=0.2)
            return
        except Exception as exc:  # pragma: no cover - host scheduling dependent.
            last_error = exc
            time.sleep(0.05)
    raise ChronosFSMountError(f"timed out waiting for shared ChronosFS daemon: {last_error}")


def _is_mountpoint(path: Path) -> bool:
    return path.exists() and os.path.ismount(path)


def _wait_for_mount(mountpoint: Path, socket_path: Path, *, deadline_s: float = 10.0) -> None:
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        if _is_mountpoint(mountpoint):
            return
        if not socket_path.exists():
            raise ChronosFSMountError("shared ChronosFS daemon exited before mounting")
        time.sleep(0.05)
    raise ChronosFSMountError(f"timed out waiting for ChronosFS mount at {mountpoint}")


def _block_until_unmounted(mountpoint: Path, socket_path: Path) -> None:
    while _is_mountpoint(mountpoint):
        if not socket_path.exists():
            break
        time.sleep(0.1)


def _ensure_shared_daemon(
    *,
    socket_path: Path,
    lock_path: Path,
    log_path: Path,
    database_url: str,
    block_size: int,
    options: list[str],
) -> None:
    with lock_path.open("w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            _socket_request(socket_path, {"ping": True}, timeout=0.2)
            return
        except Exception:
            with suppress(FileNotFoundError):
                socket_path.unlink()
        with log_path.open("ab") as log:
            subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "chronos_core.workspace.chronosfs.daemon",
                    "--socket",
                    str(socket_path),
                    "--database-url",
                    database_url,
                    "--block-size",
                    str(block_size),
                    "--options-json",
                    json.dumps(options),
                ],
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                close_fds=True,
                start_new_session=True,
            )
        _wait_for_daemon(socket_path)


def _start_shared_chronosfs_mount(
    database_url: str,
    mountpoint: Path,
    *,
    branch_id: str,
    block_size: int,
    options: list[str],
) -> None:
    mountpoint.mkdir(parents=True, exist_ok=True)
    key = _daemon_key(database_url, block_size)
    runtime = _runtime_dir()
    socket_path = runtime / f"{key}.sock"
    lock_path = runtime / f"{key}.lock"
    log_path = runtime / f"{key}.log"
    _ensure_shared_daemon(
        socket_path=socket_path,
        lock_path=lock_path,
        log_path=log_path,
        database_url=database_url,
        block_size=block_size,
        options=options,
    )
    try:
        response = _socket_request(
            socket_path,
            {
                "mountpoint": str(mountpoint),
                "branch_id": branch_id,
                "options": options,
            },
        )
    except FileNotFoundError:
        _ensure_shared_daemon(
            socket_path=socket_path,
            lock_path=lock_path,
            log_path=log_path,
            database_url=database_url,
            block_size=block_size,
            options=options,
        )
        response = _socket_request(
            socket_path,
            {
                "mountpoint": str(mountpoint),
                "branch_id": branch_id,
                "options": options,
            },
        )
    if response.get("status") != "ok":
        raise ChronosFSMountError(str(response.get("error", "shared daemon mount failed")))
    _wait_for_mount(mountpoint, socket_path)
    return None


def _shutdown_shared_chronosfs_daemon(database_url: str, block_size: int) -> None:
    key = _daemon_key(database_url, block_size)
    socket_path = _runtime_dir() / f"{key}.sock"
    if not socket_path.exists():
        return
    with suppress(Exception):
        _socket_request(socket_path, {"shutdown": True}, timeout=0.2)


def _mount_via_shared_daemon(
    database_url: str,
    mountpoint: Path,
    *,
    branch_id: str,
    block_size: int,
    options: list[str],
    shutdown_daemon_on_unmount: bool = False,
) -> None:
    key = _daemon_key(database_url, block_size)
    socket_path = _runtime_dir() / f"{key}.sock"
    _start_shared_chronosfs_mount(
        database_url,
        mountpoint,
        branch_id=branch_id,
        block_size=block_size,
        options=options,
    )
    _block_until_unmounted(mountpoint, socket_path)
    if shutdown_daemon_on_unmount:
        _shutdown_shared_chronosfs_daemon(database_url, block_size)
