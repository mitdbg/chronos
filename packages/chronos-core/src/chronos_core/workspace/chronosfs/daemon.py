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
import threading
import time
from pathlib import Path
from typing import Any

from chronos_core import _native_interval


def _load_options(raw: str) -> list[str]:
    value = json.loads(raw)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("options JSON must be a list of strings")
    return value


def _serve_mount(
    database_url: str,
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
        _native_interval.mount_chronosfs_native(
            database_url,
            mountpoint,
            branch_id,
            block_size,
            options,
        )
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
        self.unmounted_since: float | None = None
        self.active_guard = threading.Lock()

    def should_exit(self, idle_timeout_s: float, stale_unmounted_timeout_s: float) -> bool:
        with self.active_guard:
            now = time.monotonic()
            if self.shutdown_requested:
                return True
            if self.active_mounts == 0:
                self.unmounted_since = None
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


def _handle_client(
    conn: socket.socket,
    *,
    database_url: str,
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
            mountpoint = request["mountpoint"]
            if not isinstance(mountpoint, str):
                raise ValueError("mountpoint must be a string")
            branch_id = request.get("branch_id", default_branch_id)
            if not isinstance(branch_id, str):
                raise ValueError("branch_id must be a string")
            options = request.get("options", default_options)
            if (
                not isinstance(options, list)
                or not all(isinstance(item, str) for item in options)
            ):
                raise ValueError("options must be a list of strings")
            thread = threading.Thread(
                target=_serve_mount,
                args=(database_url, branch_id, block_size, options, mountpoint, state),
                daemon=True,
            )
            thread.start()
            response: dict[str, Any] = {"status": "ok"}
        except Exception as exc:
            response = {"status": "error", "error": str(exc)}
        conn.sendall((json.dumps(response) + "\n").encode("utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a shared local ChronosFS mount daemon.")
    parser.add_argument("--socket", required=True)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--branch-id", default="main")
    parser.add_argument("--block-size", type=int, required=True)
    parser.add_argument("--options-json", default="[]")
    parser.add_argument("--idle-timeout", type=float, default=30.0)
    parser.add_argument("--stale-unmounted-timeout", type=float, default=5.0)
    args = parser.parse_args()

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
                    "default_branch_id": args.branch_id,
                    "block_size": args.block_size,
                    "default_options": options,
                    "state": state,
                },
                daemon=True,
            ).start()
    finally:
        server.close()
        try:
            socket_path.unlink()
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    main()
