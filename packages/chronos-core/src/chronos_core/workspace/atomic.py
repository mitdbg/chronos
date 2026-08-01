"""Atomic publication metadata and public types for Chronos workspaces.

The coordinator deliberately owns only the workspace branch reference. Store
branches remain ordinary Chronos branches and can therefore reuse the existing
copy-on-write, diff, conflict, and merge machinery. An atomic merge prepares
private successor branches and publishes their manifest with one SQL row CAS.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import socket
import time
import uuid
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from chronos_core.branching import BranchingError, MergePreview, RowDiff
from chronos_core.branching.sql_adapters import (
    connect_sql_database,
)


class AtomicMergeError(BranchingError):
    """Base error for workspace atomic merge failures."""


class StaleAtomicMergePreviewError(AtomicMergeError):
    """Raised when an agent applies a selection to a different branch state."""


class AtomicMergeWriteTimeoutError(AtomicMergeError):
    """Raised when a coordinated writer cannot outwait an active merge."""


@dataclass(frozen=True)
class WorkspaceBranchToken:
    branch_id: str
    generation: int
    manifest_digest: str


@dataclass(frozen=True)
class MergeSelection:
    """An allow-list of globally unique logical change ids.

    ``None`` at the API boundary means all changes. An explicit empty
    selection is a valid no-op.
    """

    change_ids: frozenset[str] = field(default_factory=frozenset)

    @classmethod
    def from_ids(cls, values: Sequence[str]) -> MergeSelection:
        return cls(frozenset(str(value) for value in values))


@dataclass(frozen=True)
class AtomicMergePreview:
    source: str
    target: str
    preview_token: str
    source_token: WorkspaceBranchToken
    target_token: WorkspaceBranchToken
    stores: dict[str, MergePreview]

    @property
    def change_ids(self) -> frozenset[str]:
        return frozenset(
            change.change_id
            for preview in self.stores.values()
            for change in (*preview.changes, *preview.conflicts)
            if change.change_id is not None
        )


@dataclass(frozen=True)
class AtomicMergeResult:
    operation_id: str
    source: str
    target: str
    status: Literal["committed", "noop", "aborted"]
    source_token: WorkspaceBranchToken
    old_target_token: WorkspaceBranchToken
    new_target_token: WorkspaceBranchToken
    selected: int
    skipped: int
    stores: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class WorkspaceBranchRecord:
    branch_id: str
    generation: int
    manifest: dict[str, str]

    @property
    def token(self) -> WorkspaceBranchToken:
        return WorkspaceBranchToken(
            self.branch_id,
            self.generation,
            _digest_json(self.manifest),
        )


@dataclass(frozen=True)
class AtomicMergeReservation:
    operation_id: str
    source: WorkspaceBranchRecord
    target: WorkspaceBranchRecord
    staging_manifest: dict[str, str]
    preview_token: str


def _digest_json(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def atomic_change_id(store: str, change: RowDiff) -> str:
    digest = _digest_json(
        {
            "store": store,
            "table": change.table,
            "key": change.key,
            "change": change.change,
            "before": change.before,
            "after": change.after,
            "conflict_id": change.conflict_id,
        }
    )
    return f"{store}:{digest[:24]}"


class WorkspaceMergeCoordinator:
    """SQLite/Postgres metadata coordinator for atomic workspace publication."""

    def __init__(
        self,
        metadata_url: str,
        *,
        workspace_id: str,
        write_wait_timeout: float = 30.0,
    ):
        if not workspace_id.strip():
            raise ValueError("workspace_id must not be empty")
        if write_wait_timeout <= 0:
            raise ValueError("write_wait_timeout must be positive")
        self.metadata_url = metadata_url
        self.workspace_id = workspace_id
        self.write_wait_timeout = float(write_wait_timeout)
        self.owner_host = socket.gethostname()
        self.owner_pid = os.getpid()
        self.db = connect_sql_database(metadata_url)
        if self.db.dialect not in {"sqlite", "postgres"}:
            raise ValueError("atomic workspace metadata must use SQLite or Postgres")
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        statements = (
            """
            CREATE TABLE IF NOT EXISTS _chronos_workspace_branches (
                workspace_id TEXT NOT NULL,
                branch_id TEXT NOT NULL,
                generation BIGINT NOT NULL,
                manifest_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (workspace_id, branch_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS _chronos_workspace_atomic_merges (
                workspace_id TEXT NOT NULL,
                operation_id TEXT NOT NULL,
                source_branch_id TEXT NOT NULL,
                target_branch_id TEXT NOT NULL,
                source_generation BIGINT NOT NULL,
                target_generation BIGINT NOT NULL,
                preview_token TEXT NOT NULL,
                staging_manifest_json TEXT NOT NULL,
                owner_host TEXT NOT NULL,
                owner_pid BIGINT NOT NULL,
                heartbeat_at BIGINT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (workspace_id, operation_id),
                UNIQUE (workspace_id, target_branch_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS _chronos_workspace_atomic_results (
                workspace_id TEXT NOT NULL,
                operation_id TEXT NOT NULL,
                status TEXT NOT NULL,
                result_json TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                PRIMARY KEY (workspace_id, operation_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS _chronos_workspace_writers (
                workspace_id TEXT NOT NULL,
                writer_id TEXT NOT NULL,
                branch_id TEXT NOT NULL,
                owner_host TEXT NOT NULL,
                owner_pid BIGINT NOT NULL,
                heartbeat_at BIGINT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (workspace_id, writer_id)
            )
            """,
        )
        with self._transaction(write=True):
            for statement in statements:
                self.db.execute(statement)
            owner_columns = {
                "owner_host": "TEXT NOT NULL DEFAULT ''",
                "owner_pid": "BIGINT NOT NULL DEFAULT -1",
                "heartbeat_at": "BIGINT NOT NULL DEFAULT 0",
            }
            for table in (
                "_chronos_workspace_atomic_merges",
                "_chronos_workspace_writers",
            ):
                columns, _ = self.db.table_defs(table)
                for name, definition in owner_columns.items():
                    if name not in columns:
                        self.db.execute(
                            f"ALTER TABLE {table} ADD COLUMN {name} {definition}"
                        )

    @contextlib.contextmanager
    def _transaction(self, *, write: bool) -> Iterator[None]:
        if self.db.in_transaction:
            yield
            return
        if write and self.db.dialect == "sqlite":
            self.db.execute("BEGIN IMMEDIATE")
        else:
            self.db.begin()
        try:
            yield
        except Exception:
            self.db.rollback()
            raise
        else:
            self.db.commit()

    def _branch_row(self, branch_id: str, *, lock: bool = False) -> Any:
        suffix = " FOR UPDATE" if lock and self.db.dialect == "postgres" else ""
        return self.db.execute(
            """
            SELECT branch_id, generation, manifest_json
            FROM _chronos_workspace_branches
            WHERE workspace_id = ? AND branch_id = ?
            """
            + suffix,
            (self.workspace_id, branch_id),
        ).fetchone()

    @staticmethod
    def _record(row: Mapping[str, Any]) -> WorkspaceBranchRecord:
        return WorkspaceBranchRecord(
            str(row["branch_id"]),
            int(row["generation"]),
            {
                str(name): str(branch)
                for name, branch in json.loads(str(row["manifest_json"])).items()
            },
        )

    def ensure_branch(self, branch_id: str, manifest: Mapping[str, str]) -> None:
        with self._transaction(write=True):
            self.db.execute(
                """
                INSERT INTO _chronos_workspace_branches
                    (workspace_id, branch_id, generation, manifest_json, updated_at)
                VALUES (?, ?, 0, ?, ?)
                ON CONFLICT (workspace_id, branch_id) DO NOTHING
                """,
                (
                    self.workspace_id,
                    branch_id,
                    json.dumps(dict(manifest), sort_keys=True),
                    self._now(),
                ),
            )

    def create_branch(self, branch_id: str, manifest: Mapping[str, str]) -> None:
        with self._transaction(write=True):
            if self._branch_row(branch_id, lock=True) is not None:
                raise AtomicMergeError(f"workspace branch already exists: {branch_id}")
            self.db.execute(
                """
                INSERT INTO _chronos_workspace_branches
                    (workspace_id, branch_id, generation, manifest_json, updated_at)
                VALUES (?, ?, 0, ?, ?)
                """,
                (
                    self.workspace_id,
                    branch_id,
                    json.dumps(dict(manifest), sort_keys=True),
                    self._now(),
                ),
            )

    def delete_branch(self, branch_id: str) -> None:
        with self._transaction(write=True):
            active = self.db.execute(
                """
                SELECT 1 FROM _chronos_workspace_atomic_merges
                WHERE workspace_id = ?
                  AND (source_branch_id = ? OR target_branch_id = ?)
                """,
                (self.workspace_id, branch_id, branch_id),
            ).fetchone()
            if active is not None:
                raise AtomicMergeError(
                    f"atomic merge is active for branch: {branch_id}"
                )
            cursor = self.db.execute(
                """
                DELETE FROM _chronos_workspace_branches
                WHERE workspace_id = ? AND branch_id = ?
                """,
                (self.workspace_id, branch_id),
            )
            if cursor.rowcount != 1:
                raise AtomicMergeError(f"workspace branch does not exist: {branch_id}")

    def branch(self, branch_id: str) -> WorkspaceBranchRecord:
        with self._transaction(write=False):
            row = self._branch_row(branch_id)
            if row is None:
                raise AtomicMergeError(f"workspace branch does not exist: {branch_id}")
            return self._record(row)

    def list_branches(self) -> list[str]:
        with self._transaction(write=False):
            rows = self.db.execute(
                """
                SELECT branch_id FROM _chronos_workspace_branches
                WHERE workspace_id = ? ORDER BY branch_id
                """,
                (self.workspace_id,),
            ).fetchall()
            return [str(row["branch_id"]) for row in rows]

    def completed_result(self, operation_id: str) -> dict[str, Any] | None:
        with self._transaction(write=False):
            row = self.db.execute(
                """
                SELECT result_json FROM _chronos_workspace_atomic_results
                WHERE workspace_id = ? AND operation_id = ?
                """,
                (self.workspace_id, operation_id),
            ).fetchone()
            return None if row is None else json.loads(str(row["result_json"]))

    def record_result(
        self,
        operation_id: str,
        status: Literal["committed", "noop", "aborted"],
        result: Mapping[str, Any],
    ) -> None:
        with self._transaction(write=True):
            existing = self.db.execute(
                """
                SELECT 1 FROM _chronos_workspace_atomic_results
                WHERE workspace_id = ? AND operation_id = ?
                """,
                (self.workspace_id, operation_id),
            ).fetchone()
            if existing is not None:
                return
            self.db.execute(
                """
                INSERT INTO _chronos_workspace_atomic_results
                    (workspace_id, operation_id, status, result_json, completed_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    self.workspace_id,
                    operation_id,
                    status,
                    json.dumps(dict(result), sort_keys=True),
                    self._now(),
                ),
            )

    def reserve(
        self,
        operation_id: str,
        source: WorkspaceBranchRecord,
        target: WorkspaceBranchRecord,
        *,
        preview_token: str,
        staging_manifest: Mapping[str, str],
    ) -> AtomicMergeReservation:
        deadline = time.monotonic() + self.write_wait_timeout
        while True:
            with self._transaction(write=True):
                self._delete_abandoned_writers()
                current_source = self._branch_row(source.branch_id, lock=True)
                current_target = self._branch_row(target.branch_id, lock=True)
                if current_source is None or current_target is None:
                    raise StaleAtomicMergePreviewError(
                        "source or target branch was deleted"
                    )
                source_now = self._record(current_source)
                target_now = self._record(current_target)
                if source_now.token != source.token or target_now.token != target.token:
                    raise StaleAtomicMergePreviewError(
                        "source or target changed after atomic merge preview"
                    )
                writers = self.db.execute(
                    """
                    SELECT 1 FROM _chronos_workspace_writers
                    WHERE workspace_id = ? AND branch_id IN (?, ?) LIMIT 1
                    """,
                    (self.workspace_id, source.branch_id, target.branch_id),
                ).fetchone()
                if writers is None:
                    try:
                        self.db.execute(
                            """
                            INSERT INTO _chronos_workspace_atomic_merges
                                (workspace_id, operation_id, source_branch_id,
                                 target_branch_id, source_generation,
                                 target_generation, preview_token,
                                 staging_manifest_json, owner_host, owner_pid,
                                 heartbeat_at, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                self.workspace_id,
                                operation_id,
                                source.branch_id,
                                target.branch_id,
                                source.generation,
                                target.generation,
                                preview_token,
                                json.dumps(dict(staging_manifest), sort_keys=True),
                                self.owner_host,
                                self.owner_pid,
                                int(time.time()),
                                self._now(),
                            ),
                        )
                    except Exception as exc:
                        raise AtomicMergeError(
                            f"another atomic merge is active for {target.branch_id}"
                        ) from exc
                    return AtomicMergeReservation(
                        operation_id,
                        source,
                        target,
                        dict(staging_manifest),
                        preview_token,
                    )
            if time.monotonic() >= deadline:
                raise AtomicMergeWriteTimeoutError(
                    f"timed out waiting for writers on {target.branch_id}"
                )
            time.sleep(0.025)

    def publish(
        self,
        reservation: AtomicMergeReservation,
        result: Mapping[str, Any],
    ) -> WorkspaceBranchRecord:
        with self._transaction(write=True):
            target_row = self._branch_row(reservation.target.branch_id, lock=True)
            if target_row is None:
                raise StaleAtomicMergePreviewError("target branch was deleted")
            target_now = self._record(target_row)
            if target_now.token != reservation.target.token:
                raise StaleAtomicMergePreviewError(
                    "target changed before atomic merge publication"
                )
            active = self.db.execute(
                """
                SELECT preview_token FROM _chronos_workspace_atomic_merges
                WHERE workspace_id = ? AND operation_id = ?
                  AND target_branch_id = ?
                """,
                (
                    self.workspace_id,
                    reservation.operation_id,
                    reservation.target.branch_id,
                ),
            ).fetchone()
            if (
                active is None
                or str(active["preview_token"]) != reservation.preview_token
            ):
                raise AtomicMergeError("atomic merge reservation is missing or stale")
            next_generation = reservation.target.generation + 1
            published_record = WorkspaceBranchRecord(
                reservation.target.branch_id,
                next_generation,
                dict(reservation.staging_manifest),
            )
            cursor = self.db.execute(
                """
                UPDATE _chronos_workspace_branches
                SET generation = ?, manifest_json = ?, updated_at = ?
                WHERE workspace_id = ? AND branch_id = ? AND generation = ?
                """,
                (
                    next_generation,
                    json.dumps(reservation.staging_manifest, sort_keys=True),
                    self._now(),
                    self.workspace_id,
                    reservation.target.branch_id,
                    reservation.target.generation,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleAtomicMergePreviewError(
                    "target changed before atomic merge publication"
                )
            stored_result = dict(result)
            stored_result["new_target_token"] = {
                "branch_id": published_record.token.branch_id,
                "generation": published_record.token.generation,
                "manifest_digest": published_record.token.manifest_digest,
            }
            self.db.execute(
                """
                INSERT INTO _chronos_workspace_atomic_results
                    (workspace_id, operation_id, status, result_json, completed_at)
                VALUES (?, ?, 'committed', ?, ?)
                """,
                (
                    self.workspace_id,
                    reservation.operation_id,
                    json.dumps(stored_result, sort_keys=True),
                    self._now(),
                ),
            )
            self.db.execute(
                """
                DELETE FROM _chronos_workspace_atomic_merges
                WHERE workspace_id = ? AND operation_id = ?
                """,
                (self.workspace_id, reservation.operation_id),
            )
            return published_record

    def abort(
        self,
        reservation: AtomicMergeReservation,
        result: Mapping[str, Any],
    ) -> None:
        with self._transaction(write=True):
            self.db.execute(
                """
                DELETE FROM _chronos_workspace_atomic_merges
                WHERE workspace_id = ? AND operation_id = ?
                """,
                (self.workspace_id, reservation.operation_id),
            )
            existing = self.db.execute(
                """
                SELECT 1 FROM _chronos_workspace_atomic_results
                WHERE workspace_id = ? AND operation_id = ?
                """,
                (self.workspace_id, reservation.operation_id),
            ).fetchone()
            if existing is None:
                self.db.execute(
                    """
                    INSERT INTO _chronos_workspace_atomic_results
                        (workspace_id, operation_id, status, result_json, completed_at)
                    VALUES (?, ?, 'aborted', ?, ?)
                    """,
                    (
                        self.workspace_id,
                        reservation.operation_id,
                        json.dumps(dict(result), sort_keys=True),
                        self._now(),
                    ),
                )

    def abandoned(self) -> list[AtomicMergeReservation]:
        with self._transaction(write=False):
            rows = self.db.execute(
                """
                SELECT operation_id, source_branch_id, target_branch_id,
                       source_generation, target_generation, preview_token,
                       staging_manifest_json, owner_host, owner_pid, heartbeat_at
                FROM _chronos_workspace_atomic_merges
                WHERE workspace_id = ? ORDER BY created_at
                """,
                (self.workspace_id,),
            ).fetchall()
        reservations: list[AtomicMergeReservation] = []
        for row in rows:
            if not self._owner_is_abandoned(
                str(row["owner_host"]),
                int(row["owner_pid"]),
                int(row["heartbeat_at"]),
            ):
                continue
            source = self.branch(str(row["source_branch_id"]))
            target = self.branch(str(row["target_branch_id"]))
            reservations.append(
                AtomicMergeReservation(
                    str(row["operation_id"]),
                    WorkspaceBranchRecord(
                        source.branch_id,
                        int(row["source_generation"]),
                        source.manifest,
                    ),
                    WorkspaceBranchRecord(
                        target.branch_id,
                        int(row["target_generation"]),
                        target.manifest,
                    ),
                    {
                        str(name): str(branch)
                        for name, branch in json.loads(
                            str(row["staging_manifest_json"])
                        ).items()
                    },
                    str(row["preview_token"]),
                )
            )
        return reservations

    def acquire_writer(self, branch_id: str) -> str:
        writer_id = f"{os.getpid()}:{uuid.uuid4().hex}"
        deadline = time.monotonic() + self.write_wait_timeout
        while True:
            with self._transaction(write=True):
                self._delete_abandoned_writers()
                if self._branch_row(branch_id, lock=True) is None:
                    raise AtomicMergeError(
                        f"workspace branch does not exist: {branch_id}"
                    )
                active = self.db.execute(
                    """
                    SELECT 1 FROM _chronos_workspace_atomic_merges
                    WHERE workspace_id = ?
                      AND (source_branch_id = ? OR target_branch_id = ?)
                    LIMIT 1
                    """,
                    (self.workspace_id, branch_id, branch_id),
                ).fetchone()
                if active is None:
                    self.db.execute(
                        """
                        INSERT INTO _chronos_workspace_writers
                            (workspace_id, writer_id, branch_id, owner_host,
                             owner_pid, heartbeat_at, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            self.workspace_id,
                            writer_id,
                            branch_id,
                            self.owner_host,
                            self.owner_pid,
                            int(time.time()),
                            self._now(),
                        ),
                    )
                    return writer_id
            if time.monotonic() >= deadline:
                raise AtomicMergeWriteTimeoutError(
                    f"timed out waiting for atomic merge on {branch_id}"
                )
            time.sleep(0.025)

    def release_writer(self, writer_id: str, branch_id: str, *, changed: bool) -> None:
        with self._transaction(write=True):
            cursor = self.db.execute(
                """
                DELETE FROM _chronos_workspace_writers
                WHERE workspace_id = ? AND writer_id = ? AND branch_id = ?
                """,
                (self.workspace_id, writer_id, branch_id),
            )
            if cursor.rowcount != 1:
                raise AtomicMergeError("workspace writer lease is missing")
            if changed:
                self.db.execute(
                    """
                    UPDATE _chronos_workspace_branches
                    SET generation = generation + 1, updated_at = ?
                    WHERE workspace_id = ? AND branch_id = ?
                    """,
                    (self._now(), self.workspace_id, branch_id),
                )

    def close(self) -> None:
        self.db.close()

    def _delete_abandoned_writers(self) -> None:
        rows = self.db.execute(
            """
            SELECT writer_id, owner_host, owner_pid, heartbeat_at
            FROM _chronos_workspace_writers
            WHERE workspace_id = ?
            """,
            (self.workspace_id,),
        ).fetchall()
        abandoned = [
            str(row["writer_id"])
            for row in rows
            if self._owner_is_abandoned(
                str(row["owner_host"]),
                int(row["owner_pid"]),
                int(row["heartbeat_at"]),
            )
        ]
        for writer_id in abandoned:
            self.db.execute(
                """
                DELETE FROM _chronos_workspace_writers
                WHERE workspace_id = ? AND writer_id = ?
                """,
                (self.workspace_id, writer_id),
            )

    def _owner_is_abandoned(
        self,
        owner_host: str,
        owner_pid: int,
        heartbeat_at: int,
    ) -> bool:
        if owner_host == self.owner_host:
            if owner_pid <= 0:
                return True
            try:
                os.kill(owner_pid, 0)
            except (OSError, ValueError):
                return True
            return False
        # A different machine cannot be probed. Favor blocking over an unsafe
        # takeover, but eventually permit recovery after a long-dead host.
        return heartbeat_at < int(time.time()) - 3600

    @staticmethod
    def _now() -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


__all__ = [
    "AtomicMergeError",
    "AtomicMergePreview",
    "AtomicMergeReservation",
    "AtomicMergeResult",
    "AtomicMergeWriteTimeoutError",
    "MergeSelection",
    "StaleAtomicMergePreviewError",
    "WorkspaceBranchRecord",
    "WorkspaceBranchToken",
    "WorkspaceMergeCoordinator",
    "atomic_change_id",
]
