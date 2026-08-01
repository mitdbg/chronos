"""Local shared ChronosFS mount daemon.

The daemon hosts multiple FUSE sessions for the same ChronosFS backing store in
one process. The native layer then shares one backend/cache object across those
sessions, avoiding stale same-machine metadata caches when users create multiple
mount points for the same database and branch.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from chronos_core import _native_interval

MOUNT_READY_TIMEOUT_S = 30.0
MOUNT_START_ATTEMPTS = 3
MOUNT_READY_POLL_INTERVAL_S = 0.001
SQLITE_CHECKPOINT_INTERVAL_S = 1.0


def _load_options(raw: str) -> list[str]:
    value = json.loads(raw)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("options JSON must be a list of strings")
    return value


def _serve_mount(
    database_url: str,
    metadata_url: str,
    branch_id: str,
    block_size: int,
    options: list[str],
    mountpoint: str,
    state: "_DaemonState",
) -> None:
    with state.active_guard:
        state.active_mounts += 1
        state.active_mountpoints.add(mountpoint)
        state.unmounted_since = None
    try:
        rc = _native_interval.mount_chronosfs_native(
            database_url,
            mountpoint,
            branch_id,
            block_size,
            options,
            metadata_url,
        )
        if rc != 0:
            with state.active_guard:
                state.last_mount_error = (
                    f"native ChronosFS mount exited with status {rc}"
                )
    except BaseException as exc:
        with state.active_guard:
            state.last_mount_error = repr(exc)
        raise
    finally:
        with state.active_guard:
            state.active_mounts -= 1
            state.active_mountpoints.discard(mountpoint)
            state.unmounted_since = None
            state.last_idle_at = time.monotonic()


class _DaemonState:
    def __init__(self) -> None:
        self.active_mounts = 0
        self.active_mountpoints: set[str] = set()
        self.last_idle_at = time.monotonic()
        self.shutdown_requested = False
        self.keep_alive = False
        self.unmounted_since: float | None = None
        self.last_mount_error: str | None = None
        self.active_guard = threading.Lock()
        self.stop_maintenance = threading.Event()

    def should_exit(
        self, idle_timeout_s: float, stale_unmounted_timeout_s: float
    ) -> bool:
        with self.active_guard:
            now = time.monotonic()
            if self.shutdown_requested:
                return True
            if self.active_mounts == 0:
                self.unmounted_since = None
                if self.keep_alive:
                    return False
                return now - self.last_idle_at >= idle_timeout_s
            any_mounted = any(os.path.ismount(path) for path in self.active_mountpoints)
            if any_mounted:
                self.unmounted_since = None
                return False
            if self.unmounted_since is None:
                self.unmounted_since = now
                return False
            return now - self.unmounted_since >= stale_unmounted_timeout_s

    def request_shutdown(self) -> None:
        with self.active_guard:
            self.shutdown_requested = True
        self.stop_maintenance.set()

    def retain(self) -> None:
        with self.active_guard:
            self.keep_alive = True


def _sqlite_database_path(database_url: str) -> str | None:
    prefix = "sqlite:///"
    if not database_url.startswith(prefix):
        return None
    return "/" + database_url[len(prefix) :]


def _checkpoint_sqlite(database_url: str, state: _DaemonState) -> None:
    database_path = _sqlite_database_path(database_url)
    if database_path is None:
        return
    wal_path = Path(database_path + "-wal")
    connection = sqlite3.connect(database_path, timeout=0.05)
    try:
        connection.execute("PRAGMA busy_timeout=50")
        connection.execute("PRAGMA synchronous=NORMAL")
        previous_wal_state: tuple[int, int] | None = None
        while not state.stop_maintenance.wait(SQLITE_CHECKPOINT_INTERVAL_S):
            try:
                wal_stat = wal_path.stat()
            except FileNotFoundError:
                previous_wal_state = None
                continue
            wal_state = (wal_stat.st_mtime_ns, wal_stat.st_size)
            if wal_stat.st_size == 0 or wal_state != previous_wal_state:
                previous_wal_state = wal_state
                continue
            try:
                # Checkpoint only after the WAL has been unchanged for a full
                # interval. This keeps page copying and fsync off sustained
                # write bursts, then releases the temporary WAL space once the
                # filesystem becomes idle.
                # PASSIVE yields to a new writer instead of holding the write
                # path behind a long TRUNCATE checkpoint. The WAL file can be
                # reused after checkpointing; truncation is not required for
                # foreground correctness.
                connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            except sqlite3.OperationalError:
                # A concurrent schema/branch transaction may briefly hold the
                # database lock. The next periodic pass will make progress.
                continue
            previous_wal_state = None
    finally:
        connection.close()


def _handle_client(
    conn: socket.socket,
    *,
    database_url: str,
    metadata_url: str,
    default_branch_id: str,
    block_size: int,
    default_options: list[str],
    state: _DaemonState,
) -> None:
    with conn:
        try:
            raw = conn.recv(65536)
            request = json.loads(raw.decode("utf-8"))
            if request.get("ping") is True:
                conn.sendall(b'{"status":"ok"}\n')
                return
            if request.get("shutdown") is True:
                state.request_shutdown()
                conn.sendall(b'{"status":"ok"}\n')
                return
            if request.get("keep_alive") is True:
                state.retain()
                conn.sendall(b'{"status":"ok"}\n')
                return
            if request.get("flush") is True:
                _native_interval.flush_chronosfs_native(
                    database_url,
                    block_size,
                    metadata_url,
                )
                conn.sendall(b'{"status":"ok"}\n')
                return
            mountpoint = request["mountpoint"]
            if not isinstance(mountpoint, str):
                raise ValueError("mountpoint must be a string")
            branch_id = request.get("branch_id", default_branch_id)
            if not isinstance(branch_id, str):
                raise ValueError("branch_id must be a string")
            options = request.get("options", default_options)
            if not isinstance(options, list) or not all(
                isinstance(item, str) for item in options
            ):
                raise ValueError("options must be a list of strings")
            last_error = ""
            for attempt in range(1, MOUNT_START_ATTEMPTS + 1):
                with state.active_guard:
                    state.last_mount_error = None
                thread = threading.Thread(
                    target=_serve_mount,
                    args=(
                        database_url,
                        metadata_url,
                        branch_id,
                        block_size,
                        options,
                        mountpoint,
                        state,
                    ),
                    daemon=True,
                )
                thread.start()
                deadline = time.monotonic() + MOUNT_READY_TIMEOUT_S
                while time.monotonic() < deadline:
                    if os.path.ismount(mountpoint):
                        break
                    if not thread.is_alive():
                        with state.active_guard:
                            last_error = state.last_mount_error or ""
                        if not last_error:
                            last_error = f"ChronosFS mount thread exited before mounting {mountpoint}"
                        break
                    time.sleep(MOUNT_READY_POLL_INTERVAL_S)
                else:
                    last_error = (
                        f"timed out waiting for ChronosFS mount at {mountpoint}"
                    )
                if os.path.ismount(mountpoint):
                    break
                if attempt < MOUNT_START_ATTEMPTS:
                    time.sleep(0.2)
            else:
                raise RuntimeError(
                    last_error or f"ChronosFS mount failed for {mountpoint}"
                )
            response: dict[str, Any] = {"status": "ok"}
        except Exception as exc:
            response = {"status": "error", "error": str(exc)}
        conn.sendall((json.dumps(response) + "\n").encode("utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a shared local ChronosFS mount daemon."
    )
    parser.add_argument("--socket", required=True)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--metadata-url", default="")
    parser.add_argument("--branch-id", default="main")
    parser.add_argument("--block-size", type=int, required=True)
    parser.add_argument("--options-json", default="[]")
    parser.add_argument("--idle-timeout", type=float, default=30.0)
    parser.add_argument("--stale-unmounted-timeout", type=float, default=5.0)
    args = parser.parse_args()

    # FUSE writeback and close already define the filesystem's visibility
    # boundary.  Requiring SQLite to fsync the WAL after every small close is
    # stronger than the POSIX contract and turns package extraction into
    # thousands of serial disk flushes.  NORMAL retains transactional crash
    # consistency; explicit FUSE fsync requests force a full checkpoint in the
    # native filesystem implementation.
    os.environ.setdefault("CHRONOS_NATIVE_SQLITE_SYNCHRONOUS", "NORMAL")
    os.environ.setdefault("CHRONOS_NATIVE_SQLITE_WAL_AUTOCHECKPOINT_PAGES", "0")

    socket_path = Path(args.socket)
    socket_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        socket_path.unlink()
    except FileNotFoundError:
        pass

    options = _load_options(args.options_json)
    state = _DaemonState()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(socket_path))
    os.chmod(socket_path, 0o600)
    server.listen()
    server.settimeout(1.0)

    try:
        while True:
            if state.should_exit(args.idle_timeout, args.stale_unmounted_timeout):
                break
            try:
                conn, _ = server.accept()
            except socket.timeout:
                continue
            threading.Thread(
                target=_handle_client,
                kwargs={
                    "conn": conn,
                    "database_url": args.database_url,
                    "metadata_url": args.metadata_url or args.database_url,
                    "default_branch_id": args.branch_id,
                    "block_size": args.block_size,
                    "default_options": options,
                    "state": state,
                },
                daemon=True,
            ).start()
    finally:
        state.stop_maintenance.set()
        server.close()
        try:
            socket_path.unlink()
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    main()
