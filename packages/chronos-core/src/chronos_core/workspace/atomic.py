"""Atomic publication over the existing interval branch metadata.

Store branches remain ordinary Chronos branches.  The multi-store manifest and
short-lived merge state live in the ``metadata`` column of the relational
store's existing ``_chronos_branch_interval_branches`` row.  No parallel
workspace branch, merge, result, or writer tables are created.
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
    """Coordinate publication through existing interval branch rows only."""

    _BRANCHES_TABLE = "_chronos_branch_interval_branches"
    _METADATA_KEY = "_chronos_workspace"
    _RESULT_LIMIT = 128

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
        columns, _ = self.db.table_defs(self._BRANCHES_TABLE)
        if not {"branch_id", "metadata"} <= set(columns):
            raise ValueError(
                "atomic workspace metadata must point at an existing interval "
                "branch database"
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
            f"""
            SELECT branch_id, current_segment_id, metadata
            FROM {self._BRANCHES_TABLE}
            WHERE branch_id = ?
            """
            + suffix,
            (branch_id,),
        ).fetchone()

    def _metadata(self, row: Mapping[str, Any]) -> dict[str, Any]:
        raw = row["metadata"]
        metadata = json.loads(str(raw)) if raw else {}
        if not isinstance(metadata, dict):
            raise AtomicMergeError("interval branch metadata must be a JSON object")
        return metadata

    def _state(self, metadata: Mapping[str, Any]) -> dict[str, Any] | None:
        workspaces = metadata.get(self._METADATA_KEY)
        if not isinstance(workspaces, Mapping):
            return None
        value = workspaces.get(self.workspace_id)
        return dict(value) if isinstance(value, Mapping) else None

    def _set_state(
        self,
        metadata: Mapping[str, Any],
        state: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        updated = dict(metadata)
        workspaces = updated.get(self._METADATA_KEY)
        workspace_values = dict(workspaces) if isinstance(workspaces, Mapping) else {}
        if state is None:
            workspace_values.pop(self.workspace_id, None)
        else:
            workspace_values[self.workspace_id] = dict(state)
        if workspace_values:
            updated[self._METADATA_KEY] = workspace_values
        else:
            updated.pop(self._METADATA_KEY, None)
        return updated

    def _write_metadata(self, branch_id: str, metadata: Mapping[str, Any]) -> None:
        cursor = self.db.execute(
            f"UPDATE {self._BRANCHES_TABLE} SET metadata = ? WHERE branch_id = ?",
            (json.dumps(dict(metadata), sort_keys=True), branch_id),
        )
        if cursor.rowcount != 1:
            raise AtomicMergeError(f"workspace branch does not exist: {branch_id}")

    def _record(self, row: Mapping[str, Any]) -> WorkspaceBranchRecord:
        state = self._state(self._metadata(row))
        if state is None:
            raise AtomicMergeError(
                f"workspace branch does not exist: {row['branch_id']}"
            )
        return WorkspaceBranchRecord(
            str(row["branch_id"]),
            int(state.get("generation", 0)),
            {
                str(name): str(branch)
                for name, branch in dict(state.get("manifest") or {}).items()
            },
        )

    @staticmethod
    def _initial_state(manifest: Mapping[str, str]) -> dict[str, Any]:
        return {
            "generation": 0,
            "manifest": dict(manifest),
            "writers": {},
            "results": {},
        }

    def ensure_branch(self, branch_id: str, manifest: Mapping[str, str]) -> None:
        with self._transaction(write=True):
            row = self._branch_row(branch_id, lock=True)
            if row is None:
                raise AtomicMergeError(
                    f"relational branch metadata does not contain {branch_id!r}"
                )
            metadata = self._metadata(row)
            if self._state(metadata) is None:
                self._write_metadata(
                    branch_id,
                    self._set_state(metadata, self._initial_state(manifest)),
                )

    def create_branch(self, branch_id: str, manifest: Mapping[str, str]) -> None:
        with self._transaction(write=True):
            row = self._branch_row(branch_id, lock=True)
            if row is None:
                raise AtomicMergeError(
                    f"relational branch metadata does not contain {branch_id!r}"
                )
            metadata = self._metadata(row)
            if self._state(metadata) is not None:
                raise AtomicMergeError(f"workspace branch already exists: {branch_id}")
            self._write_metadata(
                branch_id,
                self._set_state(metadata, self._initial_state(manifest)),
            )

    def delete_branch(self, branch_id: str) -> None:
        with self._transaction(write=True):
            row = self._branch_row(branch_id, lock=True)
            if row is None:
                return
            metadata = self._metadata(row)
            state = self._state(metadata)
            if state is None:
                return
            if state.get("active_merge"):
                raise AtomicMergeError(
                    f"atomic merge is active for branch: {branch_id}"
                )
            self._write_metadata(branch_id, self._set_state(metadata, None))

    def branch(self, branch_id: str) -> WorkspaceBranchRecord:
        with self._transaction(write=False):
            row = self._branch_row(branch_id)
            if row is None:
                raise AtomicMergeError(f"workspace branch does not exist: {branch_id}")
            return self._record(row)

    def list_branches(self) -> list[str]:
        with self._transaction(write=False):
            rows = self.db.execute(
                f"SELECT branch_id, metadata FROM {self._BRANCHES_TABLE} ORDER BY branch_id"
            ).fetchall()
            return [
                str(row["branch_id"])
                for row in rows
                if self._state(self._metadata(row)) is not None
            ]

    def completed_result(self, operation_id: str) -> dict[str, Any] | None:
        with self._transaction(write=False):
            rows = self.db.execute(
                f"SELECT metadata FROM {self._BRANCHES_TABLE}"
            ).fetchall()
            for row in rows:
                state = self._state(self._metadata(row))
                results = state.get("results") if state else None
                if isinstance(results, Mapping) and operation_id in results:
                    value = results[operation_id]
                    return dict(value) if isinstance(value, Mapping) else None
            return None

    def record_result(
        self,
        operation_id: str,
        status: Literal["committed", "noop", "aborted"],
        result: Mapping[str, Any],
        *,
        branch_id: str,
    ) -> None:
        with self._transaction(write=True):
            row = self._branch_row(branch_id, lock=True)
            if row is None:
                raise AtomicMergeError(f"workspace branch does not exist: {branch_id}")
            metadata = self._metadata(row)
            state = self._state(metadata)
            if state is None:
                raise AtomicMergeError(f"workspace branch does not exist: {branch_id}")
            results = dict(state.get("results") or {})
            if operation_id in results:
                return
            stored = dict(result)
            stored["status"] = status
            stored["completed_at"] = self._now()
            results[operation_id] = stored
            state["results"] = self._trim_results(results)
            self._write_metadata(branch_id, self._set_state(metadata, state))

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
                source_metadata = self._metadata(current_source)
                target_metadata = self._metadata(current_target)
                source_state = self._state(source_metadata)
                target_state = self._state(target_metadata)
                assert source_state is not None and target_state is not None
                self._delete_abandoned_writers(source_state)
                self._delete_abandoned_writers(target_state)
                active = source_state.get("active_merge") or target_state.get(
                    "active_merge"
                )
                writers = dict(source_state.get("writers") or {}) or dict(
                    target_state.get("writers") or {}
                )
                if active:
                    raise AtomicMergeError(
                        f"another atomic merge is active for {target.branch_id}"
                    )
                if not writers:
                    active_merge = {
                        "operation_id": operation_id,
                        "source_branch_id": source.branch_id,
                        "target_branch_id": target.branch_id,
                        "source_generation": source.generation,
                        "target_generation": target.generation,
                        "source_manifest": source.manifest,
                        "target_manifest": target.manifest,
                        "preview_token": preview_token,
                        "staging_manifest": dict(staging_manifest),
                        "owner_host": self.owner_host,
                        "owner_pid": self.owner_pid,
                        "heartbeat_at": int(time.time()),
                    }
                    source_state["active_merge"] = active_merge
                    target_state["active_merge"] = active_merge
                    self._write_metadata(
                        source.branch_id,
                        self._set_state(source_metadata, source_state),
                    )
                    if target.branch_id != source.branch_id:
                        self._write_metadata(
                            target.branch_id,
                            self._set_state(target_metadata, target_state),
                        )
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
            target_metadata = self._metadata(target_row)
            target_state = self._state(target_metadata)
            assert target_state is not None
            active = target_state.get("active_merge")
            if not isinstance(active, Mapping) or (
                str(active.get("operation_id")) != reservation.operation_id
                or str(active.get("preview_token")) != reservation.preview_token
            ):
                raise AtomicMergeError("atomic merge reservation is missing or stale")
            next_generation = reservation.target.generation + 1
            published_record = WorkspaceBranchRecord(
                reservation.target.branch_id,
                next_generation,
                dict(reservation.staging_manifest),
            )
            stored_result = dict(result)
            stored_result["new_target_token"] = {
                "branch_id": published_record.token.branch_id,
                "generation": published_record.token.generation,
                "manifest_digest": published_record.token.manifest_digest,
            }
            stored_result["completed_at"] = self._now()
            results = dict(target_state.get("results") or {})
            results[reservation.operation_id] = stored_result
            target_state.update(
                {
                    "generation": next_generation,
                    "manifest": dict(reservation.staging_manifest),
                    "results": self._trim_results(results),
                }
            )
            target_state.pop("active_merge", None)
            self._write_metadata(
                reservation.target.branch_id,
                self._set_state(target_metadata, target_state),
            )
            if reservation.source.branch_id != reservation.target.branch_id:
                source_row = self._branch_row(reservation.source.branch_id, lock=True)
                if source_row is not None:
                    source_metadata = self._metadata(source_row)
                    source_state = self._state(source_metadata)
                    if source_state is not None:
                        source_state.pop("active_merge", None)
                        self._write_metadata(
                            reservation.source.branch_id,
                            self._set_state(source_metadata, source_state),
                        )
            return published_record

    def abort(
        self,
        reservation: AtomicMergeReservation,
        result: Mapping[str, Any],
    ) -> None:
        with self._transaction(write=True):
            for branch_id in {
                reservation.source.branch_id,
                reservation.target.branch_id,
            }:
                row = self._branch_row(branch_id, lock=True)
                if row is None:
                    continue
                metadata = self._metadata(row)
                state = self._state(metadata)
                if state is None:
                    continue
                active = state.get("active_merge")
                if (
                    isinstance(active, Mapping)
                    and str(active.get("operation_id")) == reservation.operation_id
                ):
                    state.pop("active_merge", None)
                if branch_id == reservation.target.branch_id:
                    results = dict(state.get("results") or {})
                    if reservation.operation_id not in results:
                        stored = dict(result)
                        stored["completed_at"] = self._now()
                        results[reservation.operation_id] = stored
                        state["results"] = self._trim_results(results)
                self._write_metadata(branch_id, self._set_state(metadata, state))

    def abandoned(self) -> list[AtomicMergeReservation]:
        with self._transaction(write=False):
            rows = self.db.execute(
                f"SELECT branch_id, metadata FROM {self._BRANCHES_TABLE} ORDER BY branch_id"
            ).fetchall()
        reservations: list[AtomicMergeReservation] = []
        seen: set[str] = set()
        for row in rows:
            state = self._state(self._metadata(row))
            active = state.get("active_merge") if state else None
            if not isinstance(active, Mapping):
                continue
            operation_id = str(active.get("operation_id", ""))
            if not operation_id or operation_id in seen:
                continue
            seen.add(operation_id)
            if not self._owner_is_abandoned(
                str(active.get("owner_host", "")),
                int(active.get("owner_pid", -1)),
                int(active.get("heartbeat_at", 0)),
            ):
                continue
            reservations.append(
                AtomicMergeReservation(
                    operation_id,
                    WorkspaceBranchRecord(
                        str(active["source_branch_id"]),
                        int(active["source_generation"]),
                        {
                            str(k): str(v)
                            for k, v in dict(active["source_manifest"]).items()
                        },
                    ),
                    WorkspaceBranchRecord(
                        str(active["target_branch_id"]),
                        int(active["target_generation"]),
                        {
                            str(k): str(v)
                            for k, v in dict(active["target_manifest"]).items()
                        },
                    ),
                    {
                        str(name): str(branch)
                        for name, branch in dict(active["staging_manifest"]).items()
                    },
                    str(active["preview_token"]),
                )
            )
        return reservations

    def acquire_writer(self, branch_id: str) -> str:
        writer_id = f"{os.getpid()}:{uuid.uuid4().hex}"
        deadline = time.monotonic() + self.write_wait_timeout
        while True:
            with self._transaction(write=True):
                row = self._branch_row(branch_id, lock=True)
                if row is None:
                    raise AtomicMergeError(
                        f"workspace branch does not exist: {branch_id}"
                    )
                metadata = self._metadata(row)
                state = self._state(metadata)
                if state is None:
                    raise AtomicMergeError(
                        f"workspace branch does not exist: {branch_id}"
                    )
                self._delete_abandoned_writers(state)
                if not state.get("active_merge"):
                    writers = dict(state.get("writers") or {})
                    writers[writer_id] = {
                        "owner_host": self.owner_host,
                        "owner_pid": self.owner_pid,
                        "heartbeat_at": int(time.time()),
                    }
                    state["writers"] = writers
                    self._write_metadata(branch_id, self._set_state(metadata, state))
                    return writer_id
            if time.monotonic() >= deadline:
                raise AtomicMergeWriteTimeoutError(
                    f"timed out waiting for atomic merge on {branch_id}"
                )
            time.sleep(0.025)

    def release_writer(self, writer_id: str, branch_id: str, *, changed: bool) -> None:
        with self._transaction(write=True):
            row = self._branch_row(branch_id, lock=True)
            if row is None:
                # Branch deletion removes the relational anchor before its
                # surrounding writer context exits; there is no lease left to
                # release in that case.
                return
            metadata = self._metadata(row)
            state = self._state(metadata)
            if state is None:
                return
            writers = dict(state.get("writers") or {})
            if writers.pop(writer_id, None) is None:
                raise AtomicMergeError("workspace writer lease is missing")
            state["writers"] = writers
            if changed:
                state["generation"] = int(state.get("generation", 0)) + 1
            self._write_metadata(branch_id, self._set_state(metadata, state))

    def close(self) -> None:
        self.db.close()

    def _delete_abandoned_writers(self, state: dict[str, Any]) -> None:
        writers = dict(state.get("writers") or {})
        state["writers"] = {
            writer_id: owner
            for writer_id, owner in writers.items()
            if not isinstance(owner, Mapping)
            or not self._owner_is_abandoned(
                str(owner.get("owner_host", "")),
                int(owner.get("owner_pid", -1)),
                int(owner.get("heartbeat_at", 0)),
            )
        }

    def _trim_results(self, results: Mapping[str, Any]) -> dict[str, Any]:
        ordered = sorted(
            results.items(),
            key=lambda item: str(
                item[1].get("completed_at", "") if isinstance(item[1], Mapping) else ""
            ),
        )
        return dict(ordered[-self._RESULT_LIMIT :])

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
