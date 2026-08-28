from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
import threading
import time
import uuid
import weakref
from collections.abc import Callable, Iterator
from typing import Any

from chronos_core.branching._common import BranchNotFoundError
from chronos_core.branching.sql_adapters import (
    SQLDatabaseAdapter,
    connect_sql_database,
)


_HEARTBEAT_SECONDS = 0.25
_LEASE_SECONDS = 2.0
_SQLITE_WAIT_SECONDS = 0.01


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _row_value(row: Any, name: str, index: int = 0) -> Any:
    if hasattr(row, "keys") and name in row.keys():
        return row[name]
    return row[index]


@dataclass
class _BranchEpochState:
    handles: set[SessionEpochHandle] = field(default_factory=set)
    paused: bool = False
    local_barriers: int = 0
    barrier_id: str | None = None
    active_operations: int = 0
    advisory_lock_held: bool = False


class SessionEpochHandle:
    """Process-local admission state for one checked-out branch session."""

    def __init__(
        self,
        coordinator: SessionEpochCoordinator,
        session_id: str,
        branch_id: str,
    ):
        self._coordinator_ref = weakref.ref(coordinator)
        self.session_id = session_id
        self.branch_id = branch_id
        self._local = threading.local()
        self._needs_refresh = False
        self._closed = False

    @contextlib.contextmanager
    def operation(self, refresh: Callable[[], None]) -> Iterator[None]:
        coordinator = self._coordinator_ref()
        if coordinator is None or self._closed:
            yield
            return
        depth = int(getattr(self._local, "depth", 0))
        if depth:
            self._local.depth = depth + 1
            try:
                yield
            finally:
                self._local.depth -= 1
            return

        needs_refresh = coordinator._enter(self)
        self._local.depth = 1
        try:
            if needs_refresh:
                refresh()
                self._needs_refresh = False
            yield
        finally:
            self._local.depth = 0
            coordinator._exit(self)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        coordinator = self._coordinator_ref()
        if coordinator is not None:
            coordinator.unregister(self)


class SessionEpochCoordinator:
    """Coordinates session-level epochs through the Chronos metadata store."""

    @classmethod
    def for_context(
        cls,
        backend_name: str,
        metadata_db: SQLDatabaseAdapter,
    ) -> SessionEpochCoordinator | None:
        if backend_name != "interval" or metadata_db.dialect != "postgres":
            return None
        database_url = str(getattr(metadata_db, "database_url", "") or "")
        if not database_url or database_url in {":memory:", "sqlite:///:memory:"}:
            return None
        # Keep the coordinator's control connection lazy.  A context can be
        # opened for schema/bootstrap work, or fail during worker startup,
        # without ever checking out a writable branch session.  In those
        # cases opening a dedicated PostgreSQL session is unnecessary; the
        # connection is created by ``register`` on the first real operation.
        return cls(database_url, dialect=metadata_db.dialect)

    def __init__(self, database_url: str, *, dialect: str = "postgres"):
        self._database_url = database_url
        self._db: SQLDatabaseAdapter | None = None
        self._dialect = dialect
        self._db_init_lock = threading.Lock()
        self._condition = threading.Condition()
        self._db_lock = threading.Lock()
        self._states: dict[str, _BranchEpochState] = {}
        self._pending_unregisters: list[tuple[str, str, bool]] = []
        self._stopping = threading.Event()
        self._worker_wakeup = threading.Event()
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None
        self._closed = False

    def _ensure_connection(self) -> SQLDatabaseAdapter:
        db = self._db
        if db is not None:
            return db
        with self._db_init_lock:
            if self._db is None:
                if self._closed:
                    raise RuntimeError("Chronos session epoch coordinator is closed")
                self._db = connect_sql_database(self._database_url)
            return self._db

    def register(self, branch_id: str) -> SessionEpochHandle:
        self._ensure_connection()
        self._raise_background_error()
        session_id = uuid.uuid4().hex
        handle = SessionEpochHandle(self, session_id, branch_id)
        while True:
            with self._condition:
                if self._closed:
                    raise RuntimeError("Chronos session epoch coordinator is closed")
                state = self._states.setdefault(branch_id, _BranchEpochState())
                first_local_session = not state.handles
                try:
                    # Advisory locks are connection-local and therefore
                    # reference-counted by PostgreSQL.  A checkout can close
                    # its last handle and immediately open another one before
                    # the coordinator thread gets to its pending unregister.
                    # Flush that handoff while the branch is still empty so a
                    # new first handle cannot acquire a second shared fence
                    # that the deferred cleanup would only unlock once.
                    if first_local_session:
                        self._flush_pending_unregisters()
                    self._register_metadata_session(
                        session_id,
                        branch_id,
                        acquire_advisory=first_local_session,
                    )
                except _BarrierInProgress:
                    pass
                else:
                    state.handles.add(handle)
                    if first_local_session and self._dialect == "postgres":
                        state.advisory_lock_held = True
                    self._ensure_worker_locked()
                    self._condition.notify_all()
                    break
            if not handle._closed and handle not in state.handles:
                time.sleep(_SQLITE_WAIT_SECONDS)
        return handle

    def unregister(self, handle: SessionEpochHandle) -> None:
        if self._closed:
            return
        release_advisory = False
        with self._condition:
            state = self._states.get(handle.branch_id)
            if state is not None:
                state.handles.discard(handle)
                # ``close`` can run from ``BranchSession.__del__`` while a
                # different handle on the same branch is in an operation.
                # Waiting for the branch-wide operation count here can then
                # deadlock the operation that triggered Python's refcount
                # cleanup (for example while a Qdrant response is decoded).
                # Removing a non-last handle is safe: the coordinator's
                # shared advisory fence remains held by the remaining
                # handles, and the metadata row is retired asynchronously.
                if not state.handles:
                    while state.active_operations:
                        self._condition.wait(timeout=0.05)
                    release_advisory = state.advisory_lock_held
                    self._states.pop(handle.branch_id, None)
            self._condition.notify_all()
            self._pending_unregisters.append(
                (handle.session_id, handle.branch_id, release_advisory)
            )
        self._worker_wakeup.set()

    @contextlib.contextmanager
    def local_branch_operation(self, branches: list[str]) -> Iterator[None]:
        """Drain sessions owned by this context before a branch operation."""

        local_branches: list[str] = []
        unlock_branches: list[str] = []
        with self._condition:
            for branch in dict.fromkeys(branches):
                state = self._states.get(branch)
                if state is None:
                    continue
                state.local_barriers += 1
                state.paused = True
                local_branches.append(branch)
            while any(
                self._states.get(branch) is not None
                and self._states[branch].active_operations
                for branch in local_branches
            ):
                self._condition.wait(timeout=0.05)
                self._raise_background_error()
            for branch in local_branches:
                state = self._states.get(branch)
                if state is not None and state.advisory_lock_held:
                    state.advisory_lock_held = False
                    unlock_branches.append(branch)
        try:
            if unlock_branches:
                try:
                    with self._db_lock:
                        for branch in unlock_branches:
                            self._release_shared_advisory_fence(branch)
                        self._db.commit()
                except Exception:
                    self._rollback_control_connection()
                    raise
            yield
        finally:
            with self._condition:
                for branch in local_branches:
                    state = self._states.get(branch)
                    if state is not None:
                        state.local_barriers = max(0, state.local_barriers - 1)
                self._condition.notify_all()
            self._worker_wakeup.set()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stopping.set()
        self._worker_wakeup.set()
        with self._condition:
            self._condition.notify_all()
        if self._thread is not None:
            self._thread.join(timeout=max(2.0, _LEASE_SECONDS * 2))
        with self._condition:
            sessions = [
                handle.session_id
                for state in self._states.values()
                for handle in list(state.handles)
            ]
            locked_branches = [
                branch
                for branch, state in self._states.items()
                if state.advisory_lock_held
            ]
            pending_unregisters = list(self._pending_unregisters)
            self._pending_unregisters.clear()
            self._states.clear()
        db = self._db
        if db is None:
            return
        try:
            with self._db_lock:
                for session_id in sessions:
                    self._db.execute(
                        "DELETE FROM _chronos_branch_sessions WHERE session_id = ?",
                        (session_id,),
                    )
                for session_id, branch, release_advisory in pending_unregisters:
                    self._db.execute(
                        "DELETE FROM _chronos_branch_sessions WHERE session_id = ?",
                        (session_id,),
                    )
                    if release_advisory and self._dialect == "postgres":
                        self._release_shared_advisory_fence(branch)
                for branch in locked_branches:
                    if self._dialect == "postgres":
                        self._release_shared_advisory_fence(branch)
                self._db.commit()
        finally:
            db.close()

    def _register_metadata_session(
        self,
        session_id: str,
        branch_id: str,
        *,
        acquire_advisory: bool,
    ) -> None:
        with self._db_lock:
            try:
                self._begin_control_transaction()
                lock_sql = (
                    "SELECT branch_id FROM _chronos_branch_interval_branches "
                    "WHERE branch_id = ?"
                )
                if self._dialect == "postgres":
                    lock_sql += " FOR SHARE"
                branch = self._db.execute(lock_sql, (branch_id,)).fetchone()
                if branch is None:
                    raise BranchNotFoundError(branch_id)
                barrier = self._db.execute(
                    "SELECT barrier_id FROM _chronos_branch_session_barriers "
                    "WHERE branch_id = ?",
                    (branch_id,),
                ).fetchone()
                if barrier is not None:
                    raise _BarrierInProgress
                if acquire_advisory and self._dialect == "postgres":
                    self._db.execute(
                        "SELECT pg_advisory_lock_shared("
                        "hashtext('_chronos_session_epoch'), hashtext(?))",
                        (branch_id,),
                    )
                now_ms = _now_ms()
                self._db.execute(
                    "INSERT INTO _chronos_branch_sessions "
                    "(session_id, branch_id, session_epoch, required_epoch, "
                    " barrier_id, status, lease_expires_ms) "
                    "VALUES (?, ?, 0, 0, NULL, 'active', ?)",
                    (session_id, branch_id, now_ms + int(_LEASE_SECONDS * 1000)),
                )
                self._db.commit()
            except Exception:
                self._rollback_control_connection()
                raise

    def _enter(self, handle: SessionEpochHandle) -> bool:
        self._raise_background_error()
        with self._condition:
            while True:
                state = self._states.get(handle.branch_id)
                if state is None:
                    return False
                if not state.paused:
                    state.active_operations += 1
                    return handle._needs_refresh
                self._condition.wait(timeout=0.05)
                self._raise_background_error()

    def _exit(self, handle: SessionEpochHandle) -> None:
        with self._condition:
            state = self._states.get(handle.branch_id)
            if state is not None:
                state.active_operations -= 1
                if state.active_operations < 0:
                    state.active_operations = 0
                self._condition.notify_all()

    def _run(self) -> None:
        next_heartbeat = 0.0
        try:
            while not self._stopping.is_set():
                self._flush_pending_unregisters()
                now = time.monotonic()
                if now >= next_heartbeat:
                    self._heartbeat_and_observe_barriers()
                    next_heartbeat = now + _HEARTBEAT_SECONDS
                else:
                    self._observe_barriers()
                timeout = max(0.001, min(next_heartbeat - time.monotonic(), 0.1))
                self._worker_wakeup.wait(timeout)
                self._worker_wakeup.clear()
            self._flush_pending_unregisters()
        except BaseException as exc:
            self._error = exc
            self._release_advisory_locks_after_failure()
            with self._condition:
                self._condition.notify_all()

    def _flush_pending_unregisters(self) -> None:
        with self._condition:
            pending = list(self._pending_unregisters)
            self._pending_unregisters.clear()
        if not pending:
            return
        try:
            with self._db_lock:
                for session_id, branch, release_advisory in pending:
                    self._db.execute(
                        "DELETE FROM _chronos_branch_sessions WHERE session_id = ?",
                        (session_id,),
                    )
                    if release_advisory and self._dialect == "postgres":
                        self._release_shared_advisory_fence(branch)
                self._db.commit()
        except Exception:
            self._rollback_control_connection()
            with self._condition:
                self._pending_unregisters[0:0] = pending
            raise

    def _heartbeat_and_observe_barriers(self) -> None:
        session_ids = self._session_ids()
        if not session_ids:
            return
        expires_ms = _now_ms() + int(_LEASE_SECONDS * 1000)
        with self._db_lock:
            for session_id in session_ids:
                self._db.execute(
                    "UPDATE _chronos_branch_sessions SET lease_expires_ms = ? "
                    "WHERE session_id = ?",
                    (expires_ms, session_id),
                )
            self._db.commit()
        self._observe_barriers()

    def _observe_barriers(self) -> None:
        branches = self._branch_ids()
        if not branches:
            return
        placeholders = ", ".join("?" for _ in branches)
        with self._db_lock:
            rows = self._db.execute(
                "SELECT branch_id, barrier_id "
                "FROM _chronos_branch_session_barriers "
                f"WHERE branch_id IN ({placeholders})",
                tuple(branches),
            ).fetchall()
            if self._db.in_transaction:
                self._db.commit()
        barriers = {
            str(_row_value(row, "branch_id")): str(_row_value(row, "barrier_id", 1))
            for row in rows
        }
        for branch, barrier_id in barriers.items():
            self._drain_branch(branch, barrier_id)
        for branch in branches:
            if branch not in barriers:
                self._resume_branch(branch)

    def _drain_branch(self, branch: str, barrier_id: str) -> None:
        with self._condition:
            state = self._states.get(branch)
            if state is None:
                return
            state.paused = True
            state.barrier_id = barrier_id
            while state.active_operations and not self._stopping.is_set():
                self._condition.wait(timeout=0.05)
            session_ids = [handle.session_id for handle in list(state.handles)]
            state.advisory_lock_held = False
        with self._db_lock:
            try:
                # The metadata row is the durable record of the session,
                # while the advisory fence is connection-local.  A prior
                # local barrier can clear the in-memory flag before a
                # remotely-installed barrier is observed, leaving the
                # connection-local fence behind.  Unlock once on every
                # PostgreSQL drain; PostgreSQL simply returns false when this
                # coordinator does not own a fence for the branch.
                if self._dialect == "postgres":
                    self._release_shared_advisory_fence(branch)
                expires_ms = _now_ms() + int(_LEASE_SECONDS * 1000)
                for session_id in session_ids:
                    self._db.execute(
                        "UPDATE _chronos_branch_sessions "
                        "SET session_epoch = required_epoch, status = 'quiescent', "
                        "lease_expires_ms = ? "
                        "WHERE session_id = ? AND barrier_id = ?",
                        (expires_ms, session_id, barrier_id),
                    )
                self._db.commit()
            except Exception:
                self._rollback_control_connection()
                raise

    def _resume_branch(self, branch: str) -> None:
        with self._condition:
            state = self._states.get(branch)
            if state is None or not state.paused or state.local_barriers:
                return
            handles = list(state.handles)
        if not handles:
            return
        branch_deleted = False
        acquired_advisory = False
        if self._dialect == "postgres":
            with self._db_lock:
                try:
                    self._begin_control_transaction()
                    branch_row = self._db.execute(
                        "SELECT branch_id FROM _chronos_branch_interval_branches "
                        "WHERE branch_id = ? FOR SHARE",
                        (branch,),
                    ).fetchone()
                    if branch_row is None:
                        for handle in handles:
                            self._db.execute(
                                "DELETE FROM _chronos_branch_sessions "
                                "WHERE session_id = ?",
                                (handle.session_id,),
                            )
                        self._db.commit()
                        branch_deleted = True
                    else:
                        barrier = self._db.execute(
                            "SELECT barrier_id FROM _chronos_branch_session_barriers "
                            "WHERE branch_id = ?",
                            (branch,),
                        ).fetchone()
                        if barrier is not None:
                            self._db.rollback()
                            return
                        self._db.execute(
                            "SELECT pg_advisory_lock_shared("
                            "hashtext('_chronos_session_epoch'), hashtext(?))",
                            (branch,),
                        )
                        acquired_advisory = True
                        expires_ms = _now_ms() + int(_LEASE_SECONDS * 1000)
                        for handle in handles:
                            self._db.execute(
                                "UPDATE _chronos_branch_sessions "
                                "SET barrier_id = NULL, status = 'active', "
                                "required_epoch = session_epoch, lease_expires_ms = ? "
                                "WHERE session_id = ?",
                                (expires_ms, handle.session_id),
                            )
                        self._db.commit()
                except Exception:
                    self._rollback_control_connection()
                    raise
        release_raced_advisory = False
        with self._condition:
            state = self._states.get(branch)
            if state is None:
                release_raced_advisory = acquired_advisory
            elif state.local_barriers:
                release_raced_advisory = acquired_advisory
            else:
                state.advisory_lock_held = (
                    self._dialect == "postgres" and not branch_deleted
                )
                state.paused = False
                state.barrier_id = None
                for handle in list(state.handles):
                    handle._needs_refresh = True
                self._condition.notify_all()
        if release_raced_advisory:
            with self._db_lock:
                self._release_shared_advisory_fence(branch)
                self._db.commit()

    def _session_ids(self) -> list[str]:
        with self._condition:
            return [
                handle.session_id
                for state in self._states.values()
                for handle in list(state.handles)
            ]

    def _branch_ids(self) -> list[str]:
        with self._condition:
            return list(self._states)

    def _ensure_worker_locked(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name="chronos-session-epoch",
            daemon=True,
        )
        self._thread.start()

    def _begin_control_transaction(self) -> None:
        if self._dialect == "sqlite":
            self._db.execute("BEGIN IMMEDIATE")
        else:
            self._db.begin()

    def _rollback_control_connection(self) -> None:
        try:
            if self._db.in_transaction:
                self._db.rollback()
        except Exception:
            pass

    def _release_shared_advisory_fence(self, branch: str) -> None:
        """Release every local acquisition of a branch's shared fence.

        PostgreSQL advisory locks are connection-local and reference-counted.
        The coordinator normally holds one acquisition per branch, but a
        close/register handoff can leave more than one acquisition on the
        dedicated control connection.  A single unlock would then leave a
        stale fence that can block a merge forever.  Releasing until
        PostgreSQL reports ``false`` makes every drain and cleanup path
        idempotent without affecting locks owned by another connection.
        """

        if self._dialect != "postgres":
            return
        while True:
            row = self._db.execute(
                "SELECT pg_advisory_unlock_shared("
                "hashtext('_chronos_session_epoch'), hashtext(?))",
                (branch,),
            ).fetchone()
            if row is None or not bool(_row_value(row, "pg_advisory_unlock_shared")):
                return

    def _release_advisory_locks_after_failure(self) -> None:
        with self._condition:
            branches = [
                branch
                for branch, state in self._states.items()
                if state.advisory_lock_held
            ]
            for branch in branches:
                self._states[branch].advisory_lock_held = False
        if self._dialect != "postgres" or not branches:
            return
        with self._db_lock:
            try:
                self._rollback_control_connection()
                for branch in branches:
                    self._release_shared_advisory_fence(branch)
                self._db.commit()
            except Exception:
                self._rollback_control_connection()

    def _raise_background_error(self) -> None:
        if self._error is not None:
            raise RuntimeError("Chronos session epoch coordinator failed") from self._error


class _BarrierInProgress(RuntimeError):
    pass
