#!/usr/bin/env python3
"""Replay an incident-response swarm with real process-level concurrency.

Every worker opens its own backend connection and replays one normalized Codex
trace against the same prepared state.  Private task branches are namespaced
per worker, while all reviewed packages publish to the shared SRE branch.
The replay can reproduce the recorded model-response delays.  The default
native run leaves workers unlocked; ``--native-big-lock`` adds a separate
coarse-grained native-branching baseline in which one process-shared exclusive
lock covers the complete agent replay, including model delays and all state
accesses.  ``--workers``
controls the number of operating-system processes and is bounded at 128 so the
stress level is explicit and reproducible.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import fcntl
import json
import multiprocessing as mp
import os
import re
import shutil
import subprocess
import threading
import time
import traceback
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from queue import Empty
from typing import Any
from urllib.parse import urlparse

from chronos_core.workspace.chronosfs import shutdown_chronosfs_daemon
from chronos_enterprise_knowledge.backends import (
    create_knowledge_backend,
    default_chronos_postgres_dsn,
)
from chronos_enterprise_knowledge.embedding import ZeroEmbedder
from chronos_enterprise_knowledge.rollout_trace import (
    WorkloadReplayer,
    WorkloadTrace,
)
from chronos_enterprise_knowledge.service import KnowledgeService
from chronos_enterprise_knowledge.timing import instrument_backend

BACKENDS = ("chronos", "doltgres-qdrant-btrfs")
MAX_WORKERS = 128
MAX_VERIFIER_SAMPLES = 256
_SAFE = re.compile(r"[^A-Za-z0-9_.-]+")
_CONNECTION_LIMIT_ENV = {
    # Keep hidden numerical-library fan-out from multiplying process-level
    # workers in direct runs as well as in the sweep driver.
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "NUMEXPR_NUM_THREADS": "1",
    "ORT_NUM_THREADS": "1",
    "TOKENIZERS_PARALLELISM": "false",
    # FastEmbed does not necessarily honor the BLAS variables above; bound
    # its BM25 tokenizer explicitly as well.
    "CHRONOS_BM25_THREADS": "1",
    # One bounded transport pool per Qdrant client.  This setting is shared by
    # Chronos and the native-branching backend through the common helper.
    "CHRONOS_QDRANT_POOL_SIZE": "1",
    # Avoid one extra PostgreSQL GC connection per worker after branch delete.
    "CHRONOS_INTERVAL_GC_SYNCHRONOUS": "1",
}


def _parse_merge_retries(value: str) -> int:
    """Parse a finite retry count or the explicit unlimited sentinel."""

    normalized = str(value).strip().casefold()
    if normalized in {"unlimited", "infinite", "inf", "-1"}:
        return -1
    try:
        retries = int(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "merge retries must be a non-negative integer or 'unlimited'"
        ) from exc
    if retries < 0:
        raise argparse.ArgumentTypeError(
            "merge retries must be a non-negative integer or 'unlimited'"
        )
    return retries


class ProcessSharedLock:
    """A single advisory exclusive lock shared by spawned processes."""

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        self._stack = 0
        self.wait_seconds = 0.0
        self.acquires = 0
        self.hold_seconds = 0.0
        self._held_since: float | None = None

    def acquire(self) -> None:
        if self._stack:
            self._stack += 1
            return
        started = time.perf_counter()
        fcntl.flock(self._fd, fcntl.LOCK_EX)
        self.wait_seconds += time.perf_counter() - started
        self._stack = 1
        self.acquires += 1
        self._held_since = time.perf_counter()

    def release(self) -> None:
        if not self._stack:
            raise RuntimeError("exclusive lock is not held")
        self._stack -= 1
        if not self._stack:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            if self._held_since is not None:
                self.hold_seconds += time.perf_counter() - self._held_since
                self._held_since = None

    @contextlib.contextmanager
    def locked(self):
        self.acquire()
        try:
            yield
        finally:
            self.release()

    def release_all(self) -> None:
        while self._stack:
            self.release()

    def close(self) -> None:
        self.release_all()
        if self._fd >= 0:
            os.close(self._fd)
            self._fd = -1


class NativeBigLockController:
    """Serialize one complete native agent replay with one shared lock."""

    def __init__(self, path: str | Path):
        self.lock = ProcessSharedLock(path)
        self._replay_scope = False

    def before_replay(self, trace_id: str) -> Any:
        del trace_id
        if self._replay_scope:
            raise RuntimeError("native big lock replay scope is already active")
        self.lock.acquire()
        self._replay_scope = True
        return True

    def after_replay(
        self,
        token: Any,
        trace_id: str,
        *,
        succeeded: bool,
    ) -> None:
        del token, trace_id, succeeded
        if not self._replay_scope:
            return
        try:
            self.lock.release()
        finally:
            self._replay_scope = False

    def before_operation(self, name: str, arguments: Mapping[str, Any]) -> Any:
        del name, arguments
        if self._replay_scope:
            # The outer replay scope already protects this event.
            return None
        self.lock.acquire()
        # ``None`` means "no hook" to WorkloadReplayer; return a sentinel so
        # the matching after-operation hook always releases this acquisition.
        return True

    def after_operation(
        self,
        token: Any,
        name: str,
        arguments: Mapping[str, Any],
        *,
        succeeded: bool,
    ) -> None:
        del name, arguments
        if token is None:
            return
        del succeeded
        self.lock.release()

    def release_all(self) -> None:
        self.lock.release_all()
        self._replay_scope = False

    def metrics(self) -> dict[str, float | int]:
        return {
            "lock_wait_seconds": self.lock.wait_seconds,
            "lock_acquires": self.lock.acquires,
            "lock_hold_seconds": self.lock.hold_seconds,
        }

    def close(self) -> None:
        self.release_all()
        self.lock.close()


@dataclass(frozen=True)
class IncidentInput:
    incident_id: str
    incident_title: str
    incident_query: str
    source_hints: str


@dataclass(frozen=True)
class BundleSpec:
    input_id: str
    document_id: str
    artifact_path: str
    memory_id: str
    query: str
    query_embedding: tuple[float, ...]
    task_branch: str
    target_branch: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "input_id": self.input_id,
            "document_id": self.document_id,
            "artifact_path": self.artifact_path,
            "memory_id": self.memory_id,
            "query": self.query,
            "query_embedding": list(self.query_embedding),
            "task_branch": self.task_branch,
            "target_branch": self.target_branch,
        }


@dataclass(frozen=True)
class WorkerPlan:
    worker_id: int
    input_id: str
    trace_path: str
    trace_id: str
    bundle: BundleSpec

    def as_dict(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "input_id": self.input_id,
            "trace_path": self.trace_path,
            "trace_id": self.trace_id,
            "bundle": self.bundle.as_dict(),
        }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--traces-dir", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--state-dir", action="append", required=True, metavar="BACKEND=PATH")
    parser.add_argument(
        "--backend",
        action="append",
        choices=BACKENDS,
        dest="selected_backends",
        help=(
            "Run only the selected backend(s). By default the comparison runs "
            "both backends."
        ),
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--dimensions", type=int, required=True)
    parser.add_argument("--embedding-model", default="sentence-transformers/all-MiniLM-L6-v2")
    parser.add_argument("--repo-dir", type=Path, required=True)
    parser.add_argument("--qdrant-url")
    parser.add_argument("--qdrant-api-key")
    parser.add_argument("--qdrant-storage-dir", type=Path)
    parser.add_argument("--doltgres-dsn")
    parser.add_argument("--btrfs-root", type=Path)
    parser.add_argument("--doltgres-data-dir", type=Path)
    parser.add_argument(
        "--chronos-postgres-dsn",
        default=default_chronos_postgres_dsn(),
        help=(
            "PostgreSQL DSN for Chronos' relational control/data plane. "
            "The default is the benchmark PostgreSQL service; SQLite is "
            "not permitted for this concurrent experiment."
        ),
    )
    parser.add_argument("--chronos-postgres-data-dir", type=Path)
    parser.add_argument("--allow-shell", action="store_true")
    parser.add_argument(
        "--no-replay-llm-latency",
        action="store_true",
        help="Do not sleep for the model-response delays recorded in each trace.",
    )
    parser.add_argument(
        "--llm-latency-scale",
        type=float,
        default=1.0,
        help="Scale recorded model-response delays before sleeping (default: 1).",
    )
    parser.add_argument("--allow-trace-reuse", action="store_true")
    parser.add_argument(
        "--merge-retries",
        type=_parse_merge_retries,
        default=11,
        help=(
            "Retry transient merge races with jittered exponential backoff; "
            "use -1 or 'unlimited' to retry until success."
        ),
    )
    parser.add_argument(
        "--no-session-epochs",
        action="store_true",
        help=(
            "Disable the per-worker PostgreSQL session-epoch coordinator. "
            "Chronos then uses its native branch guard, avoiding one control "
            "connection and heartbeat thread per worker; this is useful for "
            "high-fan-out connection-pressure tests."
        ),
    )
    parser.add_argument("--startup-timeout-seconds", type=float, default=300.0)
    parser.add_argument(
        "--startup-batch-size",
        type=int,
        default=4,
        help=(
            "Number of worker processes admitted per backend-initialization "
            "batch (startup only; replay concurrency is unchanged)."
        ),
    )
    parser.add_argument(
        "--run-timeout-seconds",
        type=float,
        default=0.0,
        help=(
            "Maximum replay wait in seconds; zero (the default) waits for every "
            "worker without an artificial run timeout."
        ),
    )
    parser.add_argument(
        "--close-timeout-seconds",
        type=float,
        default=300.0,
        help="Grace period for workers to unmount ChronosFS checkouts before cleanup.",
    )
    parser.add_argument("--verifier-startup-timeout-seconds", type=float, default=120.0)
    parser.add_argument("--verifier-search-every", type=int, default=16)
    parser.add_argument("--verifier-search-limit", type=int, default=128)
    parser.add_argument("--no-verifier", action="store_true")
    parser.add_argument(
        "--native-big-lock",
        dest="native_big_lock",
        action="store_true",
        help=(
            "Run the native Doltgres+Qdrant+Btrfs backend with one shared "
            "process-level exclusive lock around each complete agent replay, "
            "including recorded model delays and all state accesses. The lock "
            "is applied only to the native baseline."
        ),
    )
    parser.add_argument(
        "--target-branch",
        default="team/site-reliability",
        help="Shared branch to which every worker publishes.",
    )
    return parser.parse_args(argv)


def _safe(value: str) -> str:
    return _SAFE.sub("-", value).strip("-") or "worker"


def _parse_backend_paths(values: Sequence[str]) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for value in values:
        backend, separator, raw_path = value.partition("=")
        if not separator or backend not in BACKENDS or not raw_path:
            raise ValueError(
                f"state directory must be BACKEND=PATH for {BACKENDS}: {value!r}"
            )
        if backend in result:
            raise ValueError(f"duplicate state directory for {backend}")
        result[backend] = Path(raw_path).expanduser().resolve()
    return result


def _load_inputs(path: Path) -> list[IncidentInput]:
    rows: list[IncidentInput] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, Mapping):
            raise ValueError(f"input line {line_number} is not an object")
        rows.append(
            IncidentInput(
                incident_id=str(value["incident_id"]),
                incident_title=str(value["incident_title"]),
                incident_query=str(value["incident_query"]),
                source_hints=str(value["source_hints"]),
            )
        )
    if not rows:
        raise ValueError(f"no incident inputs found in {path}")
    ids = [row.incident_id for row in rows]
    if len(set(ids)) != len(ids):
        raise ValueError("incident_id values must be unique")
    return rows


def _replace_value(value: Any, replacements: Mapping[str, str]) -> Any:
    if isinstance(value, str):
        result = value
        for source, target in sorted(replacements.items(), key=lambda item: -len(item[0])):
            result = result.replace(source, target)
        return result
    if isinstance(value, Mapping):
        return {str(key): _replace_value(item, replacements) for key, item in value.items()}
    if isinstance(value, list):
        return [_replace_value(item, replacements) for item in value]
    if isinstance(value, tuple):
        return tuple(_replace_value(item, replacements) for item in value)
    return value


def _trace_values(trace: WorkloadTrace, row: IncidentInput) -> dict[str, str]:
    values = trace.metadata.get("template_values")
    if isinstance(values, Mapping):
        result = {str(key): str(value) for key, value in values.items()}
    else:
        result = {
            "incident_id": row.incident_id,
            "task_branch": f"task/swarm-{row.incident_id}",
            "artifact_path": f"/artifacts/incidents/swarm-{row.incident_id}.md",
            "document_id": f"incident-report/swarm-{row.incident_id}",
            "memory_id": f"episodic/swarm-{row.incident_id}",
        }
    required = ("task_branch", "artifact_path", "document_id", "memory_id")
    missing = [key for key in required if key not in result]
    if missing:
        raise ValueError(f"trace {trace.trace_id} lacks template values {missing}")
    return result


def _query_embedding(trace: WorkloadTrace, dimensions: int) -> tuple[float, ...]:
    for event in trace.events:
        if event.kind == "mcp" and event.name == "knowledge_search":
            values = event.arguments.get("query_embedding")
            if isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
                return tuple(float(item) for item in values)
    return tuple(0.0 for _ in range(dimensions))


def _expand_trace(
    trace: WorkloadTrace,
    row: IncidentInput,
    *,
    worker_id: int,
    run_id: str,
    dimensions: int,
    target_branch: str,
) -> tuple[WorkloadTrace, BundleSpec]:
    values = _trace_values(trace, row)
    namespace = f"swarm/{run_id}/worker-{worker_id:03d}"
    suffix = f"{_safe(row.incident_id)}-{run_id}-w{worker_id:03d}"
    task_branch = f"{namespace}/task"
    artifact_path = f"/artifacts/incidents/{suffix}.md"
    document_id = f"incident-report/{suffix}"
    memory_id = f"episodic/{suffix}"
    # ``--allow-trace-reuse`` deliberately replays one captured incident
    # many times.  Keep the verifier/final-state key unique per worker;
    # otherwise the last duplicate input silently overwrites earlier bundles
    # in the state maps and makes successful publications look incomplete.
    bundle_id = f"{row.incident_id}--{run_id}-w{worker_id:03d}"
    replacements = {
        str(values["task_branch"]): task_branch,
        str(values["artifact_path"]): artifact_path,
        str(values["document_id"]): document_id,
        str(values["memory_id"]): memory_id,
        "team/site-reliability": target_branch,
    }
    events = tuple(
        dataclasses.replace(
            event,
            arguments=_replace_value(event.arguments, replacements),
            expected=_replace_value(event.expected, replacements),
        )
        for event in trace.events
    )
    metadata = dict(trace.metadata)
    metadata.update(
        {
            "worker_id": worker_id,
            "worker_namespace": namespace,
            "input_id": row.incident_id,
            "shared_target_branch": target_branch,
            "template_values": {
                **values,
                "task_branch": task_branch,
                "artifact_path": artifact_path,
                "document_id": document_id,
                "memory_id": memory_id,
            },
        }
    )
    expanded = WorkloadTrace(
        f"{trace.trace_id}--{run_id}-w{worker_id:03d}",
        events,
        metadata,
    )
    bundle = BundleSpec(
        input_id=bundle_id,
        document_id=document_id,
        artifact_path=artifact_path,
        memory_id=memory_id,
        query=row.incident_query,
        query_embedding=_query_embedding(trace, dimensions),
        task_branch=task_branch,
        target_branch=target_branch,
    )
    return expanded, bundle


def _backend_options(args: argparse.Namespace, backend_name: str) -> dict[str, Any]:
    options: dict[str, Any] = {
        "qdrant_url": args.qdrant_url,
        "qdrant_api_key": args.qdrant_api_key,
        "qdrant_storage_dir": str(args.qdrant_storage_dir.resolve())
        if args.qdrant_storage_dir is not None
        else None,
    }
    if backend_name == "doltgres-qdrant-btrfs":
        options.update(
            {
                "doltgres_dsn": args.doltgres_dsn,
                "btrfs_root": str(args.btrfs_root.resolve())
                if args.btrfs_root is not None
                else None,
                "doltgres_data_dir": str(args.doltgres_data_dir.resolve())
                if args.doltgres_data_dir is not None
                else None,
            }
        )
    else:
        options.update(
            {
                "chronos_postgres_dsn": args.chronos_postgres_dsn,
                "chronos_postgres_data_dir": str(args.chronos_postgres_data_dir.resolve())
                if args.chronos_postgres_data_dir is not None
                else None,
            }
        )
    if backend_name == "chronos":
        options["chronos_enable_session_epochs"] = not bool(
            getattr(args, "no_session_epochs", False)
        )
    return {key: value for key, value in options.items() if value is not None}


def _open_backend(
    backend_name: str,
    state_dir: str,
    dimensions: int,
    options: Mapping[str, Any],
) -> Any:
    return create_knowledge_backend(
        backend_name,
        state_dir=state_dir,
        vector_dimensions=dimensions,
        **dict(options),
    )


def _close_worker_backend(backend: Any) -> None:
    """Release a worker's database sessions as soon as replay is complete.

    Chronos workers share one local ChronosFS daemon.  Closing a worker with
    ``shutdown_daemon=False`` releases its relational/native connections
    without taking that daemon away from the other workers.  Comparison
    backends do not accept the keyword, so retain their ordinary close path.
    Keeping completed workers connected until the slowest worker exits makes
    high-fan-out runs consume one full connection set per process for no
    useful work.
    """

    close = getattr(backend, "close", None)
    if not callable(close):
        return
    try:
        close(shutdown_daemon=False)
    except TypeError:
        close()


def _document_state(backend: Any, branch: str, *, document_id: str | None = None, path: str | None = None) -> dict[str, Any]:
    errors: list[str] = []
    identifier = document_id
    if path is not None:
        try:
            path_id = backend.find_document_id_by_path(branch, path)
        except BaseException as exc:
            path_id = None
            errors.append(f"path lookup: {type(exc).__name__}: {exc}")
        if path_id is not None:
            if identifier is not None and str(path_id) != identifier:
                errors.append(f"path maps to {path_id!r}, expected {identifier!r}")
            identifier = str(path_id)
    indexed = None
    if identifier is not None:
        try:
            indexed = backend.get_document(branch, identifier)
        except BaseException as exc:
            errors.append(f"document lookup: {type(exc).__name__}: {exc}")
    if indexed is None:
        return {"present": False, "identifier": identifier, "errors": errors}
    document = indexed.document
    if path is not None and document.path != path:
        errors.append(f"document path is {document.path!r}, expected {path!r}")
    try:
        content = backend.read_file(branch, document.path)
    except BaseException as exc:
        errors.append(f"file read: {type(exc).__name__}: {exc}")
    else:
        if content != document.content.encode("utf-8"):
            errors.append("filesystem bytes differ from relational content")
    return {
        "present": True,
        "identifier": str(document.id),
        "path": str(document.path),
        "title": str(document.title),
        "query_text": str(document.title),
        "errors": errors,
    }


def _search_contains(
    backend: Any,
    branch: str,
    query: str,
    embedding: Sequence[float],
    identifiers: set[str],
    limit: int,
) -> tuple[bool, list[str], str | None]:
    try:
        hits = backend.search(branch, query, embedding, limit=limit)
    except BaseException as exc:
        return False, [], f"{type(exc).__name__}: {exc}"
    hit_ids = [str(hit.document_id) for hit in hits]
    return bool(identifiers & set(hit_ids)), hit_ids, None


def _branch_version(backend: Any, branch: str) -> str | None:
    """Return the metadata version used to validate a verifier read.

    A bundle check reads several stores.  If publication advances the branch
    between those reads, the individual calls can legitimately observe two
    different committed views even though publication itself was atomic.  The
    verifier therefore compares the branch version before and after the
    bundle check and ignores observations that crossed a publication.
    """

    relational = getattr(backend, "relational", None)
    if relational is not None:
        try:
            return str(relational.get_branch(branch).current_ref)
        except BaseException:
            return None
    registry_rows = getattr(backend, "_registry_rows", None)
    if callable(registry_rows):
        try:
            row = registry_rows().get(branch)
            if row is not None:
                return str(row.get("current_seq"))
        except BaseException:
            return None
    return None


def _check_bundle(
    backend: Any,
    bundle: Mapping[str, Any],
    *,
    search: bool,
    search_limit: int,
) -> dict[str, Any]:
    target = str(bundle["target_branch"])
    version_start = _branch_version(backend, target)
    artifact = _document_state(
        backend,
        target,
        document_id=str(bundle["document_id"]),
        path=str(bundle["artifact_path"]),
    )
    memory = _document_state(
        backend,
        target,
        document_id=str(bundle["memory_id"]),
    )
    component_presence = {
        "artifact": bool(artifact["present"]),
        "memory": bool(memory["present"]),
    }
    errors = list(artifact["errors"]) + list(memory["errors"])
    vector_hits: list[str] = []
    vector_error: str | None = None
    vector_observed = False
    if search:
        vector_observed, vector_hits, vector_error = _search_contains(
            backend,
            target,
            str(bundle["query"]),
            tuple(float(value) for value in bundle.get("query_embedding") or ()),
            {str(bundle["document_id"]), str(bundle["memory_id"])},
            search_limit,
        )
    complete = all(component_presence.values())
    any_component = any(component_presence.values()) or vector_observed
    version_end = _branch_version(backend, target)
    return {
        "complete": complete,
        "any_component": any_component,
        "component_presence": component_presence,
        "artifact": artifact,
        "memory": memory,
        "vector_observed": vector_observed,
        "vector_hits": vector_hits,
        "vector_error": vector_error,
        "errors": errors,
        "branch_version_start": version_start,
        "branch_version_end": version_end,
        "branch_version_stable": (
            version_start is not None
            and version_end is not None
            and version_start == version_end
        ),
    }


def _compact_verifier_state(state: Mapping[str, Any]) -> dict[str, Any]:
    """Keep verifier messages bounded without dropping the anomaly signal.

    A verifier result is sent through a multiprocessing queue after the busy
    loop stops.  Retaining every search hit and full document payload makes
    that message needlessly large, and a process killed while flushing a
    large queue message can leave the reader blocked on a partial frame.  The
    invariant-relevant fields are small and sufficient for diagnosis.
    """

    compact: dict[str, Any] = {
        key: state.get(key)
        for key in (
            "complete",
            "any_component",
            "component_presence",
            "vector_observed",
            "vector_error",
            "errors",
            "branch_version_start",
            "branch_version_end",
            "branch_version_stable",
        )
    }
    for component in ("artifact", "memory"):
        value = state.get(component)
        if isinstance(value, Mapping):
            compact[component] = {
                key: value.get(key)
                for key in ("present", "identifier", "path", "errors")
            }
    return compact


def _disable_observer_session_epochs(backend: Any) -> None:
    """Keep the read-only verifier out of the writer admission protocol.

    A verifier repeatedly checks live branches while another process publishes
    merges.  Normal writable checkouts register a session-epoch lease and hold
    a PostgreSQL shared advisory lock for the lifetime of the checkout.  That
    is appropriate for an agent session, but it would make an observer block
    every publication fence.  The verifier has no writes to protect, so close
    the coordinators created during backend construction and disable epoch
    management on the contexts it can use.  Its reads remain live and can
    still observe a publication in progress; they simply do not participate
    in writer quiescence.
    """

    candidates: list[Any] = [backend]
    workspace = getattr(backend, "workspace", None)
    if workspace is not None:
        candidates.extend(
            [
                getattr(workspace, "_atomic_control", None),
                getattr(workspace, "filesystem", None),
                *getattr(workspace, "stores", {}).values(),
            ]
        )
    candidates.extend(
        [
            getattr(backend, "relational", None),
            getattr(backend, "filesystem", None),
            getattr(backend, "qdrant", None),
        ]
    )
    seen: set[int] = set()
    for candidate in candidates:
        if candidate is None:
            continue
        context = getattr(candidate, "context", candidate)
        identity = id(context)
        if identity in seen:
            continue
        seen.add(identity)
        coordinator = getattr(context, "_session_epochs", None)
        if coordinator is not None:
            coordinator.close()
            context._session_epochs = None
        backend_impl = getattr(context, "_backend", None)
        set_managed = getattr(backend_impl, "set_session_epoch_managed", None)
        if callable(set_managed):
            set_managed(False)


def _verifier_main(
    backend_name: str,
    state_dir: str,
    dimensions: int,
    options: Mapping[str, Any],
    bundles: Sequence[Mapping[str, Any]],
    stop_event: Any,
    ready_event: Any,
    result_queue: Any,
    search_every: int,
    search_limit: int,
    lock_path: str | None,
) -> None:
    backend: Any | None = None
    shared_lock: ProcessSharedLock | None = None
    checks = 0
    violations: list[dict[str, Any]] = []
    publication_violation_count = 0
    cross_store_violation_count = 0
    process_error_count = 0
    observed_complete: set[str] = set()
    last_signatures: dict[str, str] = {}

    def record_violation(
        kind: str,
        input_id: str,
        target_branch: str,
        state: Mapping[str, Any],
    ) -> None:
        nonlocal publication_violation_count, cross_store_violation_count
        if kind == "partial-publication":
            publication_violation_count += 1
        elif kind == "cross-store-inconsistency":
            cross_store_violation_count += 1
        if len(violations) >= MAX_VERIFIER_SAMPLES:
            return
        violations.append(
            {
                "kind": kind,
                "input_id": input_id,
                "target_branch": target_branch,
                "state": _compact_verifier_state(state),
                "checks": checks,
                "time": time.time(),
            }
        )

    try:
        # Keep the swarm's per-store timing measurements honest.  This runner
        # opens backends directly (rather than through PreparedBenchmark), so
        # it must install the same instrumentation explicitly before replay.
        backend = instrument_backend(
            _open_backend(backend_name, state_dir, dimensions, options)
        )
        _disable_observer_session_epochs(backend)
        if lock_path is not None:
            shared_lock = ProcessSharedLock(lock_path)
        ready_event.set()
        while not stop_event.is_set():
            checks += 1
            for bundle in bundles:
                key = str(bundle["input_id"])
                do_search = search_every > 0 and checks % search_every == 0
                if shared_lock is None:
                    state = _check_bundle(
                        backend,
                        bundle,
                        search=do_search,
                        search_limit=search_limit,
                    )
                else:
                    with shared_lock.locked():
                        state = _check_bundle(
                            backend,
                            bundle,
                            search=do_search,
                            search_limit=search_limit,
                        )
                signature = json.dumps(
                    {
                        "complete": state["complete"],
                        "any_component": state["any_component"],
                        "presence": state["component_presence"],
                        "errors": state["errors"],
                    },
                    sort_keys=True,
                )
                if (
                    state["branch_version_stable"]
                    and state["any_component"]
                    and not state["complete"]
                ):
                    record_violation(
                        "partial-publication",
                        key,
                        str(bundle["target_branch"]),
                        state,
                    )
                if (
                    state["branch_version_stable"]
                    and state["errors"]
                    and state["any_component"]
                ):
                    record_violation(
                        "cross-store-inconsistency",
                        key,
                        str(bundle["target_branch"]),
                        state,
                    )
                if state["complete"]:
                    observed_complete.add(key)
                if signature != last_signatures.get(key):
                    last_signatures[key] = signature
    except BaseException as exc:
        ready_event.set()
        process_error_count += 1
        result_queue.put(
            {
                "checks": checks,
                "violation_count": (
                    publication_violation_count
                    + cross_store_violation_count
                    + process_error_count
                ),
                "publication_violation_count": publication_violation_count,
                "cross_store_violation_count": cross_store_violation_count,
                "process_error": f"{type(exc).__name__}: {exc}",
                "violations": violations,
                "lock": (
                    {
                        "lock_wait_seconds": shared_lock.wait_seconds,
                        "lock_acquires": shared_lock.acquires,
                        "lock_hold_seconds": shared_lock.hold_seconds,
                    }
                    if shared_lock is not None
                    else None
                ),
            }
        )
        return
    finally:
        if backend is not None:
            try:
                backend.close()
            except BaseException:
                pass
        if shared_lock is not None:
            try:
                shared_lock.close()
            except BaseException:
                pass
    result_queue.put(
        {
            "execution_mode": "process-busy-loop",
            "checks": checks,
            "observed_complete_bundles": sorted(observed_complete),
            "violation_count": (
                publication_violation_count
                + cross_store_violation_count
                + process_error_count
            ),
            "publication_violation_count": publication_violation_count,
            "cross_store_violation_count": cross_store_violation_count,
            "violations": violations,
            "lock": (
                {
                    "lock_wait_seconds": shared_lock.wait_seconds,
                    "lock_acquires": shared_lock.acquires,
                    "lock_hold_seconds": shared_lock.hold_seconds,
                }
                if shared_lock is not None
                else None
            ),
        }
    )


def _worker_main(
    worker_id: int,
    backend_name: str,
    state_dir: str,
    dimensions: int,
    embedding_model: str,
    options: Mapping[str, Any],
    trace_path: str,
    repo_dir: str,
    allow_shell: bool,
    replay_llm_latency: bool,
    llm_latency_scale: float,
    merge_retries: int,
    ready_queue: Any,
    result_queue: Any,
    start_event: Any,
    close_event: Any,
    lock_path: str | None,
) -> None:
    backend: Any | None = None
    operation_lock: NativeBigLockController | None = None
    try:
        # This runner opens backends directly (rather than through
        # PreparedBenchmark), so install the same per-store timing wrappers
        # before replaying the trace.
        backend = instrument_backend(
            _open_backend(backend_name, state_dir, dimensions, options)
        )
        if lock_path is not None:
            operation_lock = NativeBigLockController(lock_path)
        embedder = ZeroEmbedder(dimensions, model=embedding_model)
        service = KnowledgeService(backend, embedder)
        # ``run_backend`` starts the one shared ChronosFS daemon before it
        # fans out the worker processes.  Starting it again from every
        # process races on the daemon socket at high fan-out; the workers only
        # need their independent backend sessions here.
        ready_queue.put({"worker_id": worker_id, "status": "ready", "time": time.time()})
        start_event.wait()
        replay_started = time.time()
        trace = WorkloadTrace.load(trace_path)
        replayer = WorkloadReplayer(
            service,
            repo_dir=repo_dir,
            allow_shell=allow_shell,
            replay_llm_latency=replay_llm_latency,
            llm_latency_scale=llm_latency_scale,
            continue_on_error=False,
            merge_retries=merge_retries,
            operation_lock=operation_lock,
        )
        report = replayer.replay(trace)
        release = getattr(backend, "release_session_checkouts", None)
        if callable(release):
            release()
        replay_finished = time.time()
        replay_payload = report.as_dict(include_events=False)
        # Keep enough information to diagnose a failed replay without copying
        # full event arguments (which can contain multi-megabyte artifacts).
        # The previous benchmark discarded ``EventReplayResult.error`` and
        # therefore could report only the operation at which a worker stopped.
        replay_payload["failed_events"] = [
            {
                "sequence": event.sequence,
                "kind": event.kind,
                "name": event.name,
                "status": event.status,
                "expected_status": event.expected_status,
                "matched": event.matched,
                "error": event.error,
                "shell_exit_code": event.shell_exit_code,
                "normalized_digest": event.normalized_digest,
                "expected_digest": event.expected_digest,
                "elapsed_ms": event.elapsed_ns / 1_000_000,
                "timing_ms": dict(event.timing_ms),
                "model_inference_ms": event.model_inference_ms,
            }
            for event in report.events
            if event.error
            or event.matched is False
            or (
                event.expected_status not in {"any", event.status}
                and event.status != "ok"
            )
        ]
        result_queue.put(
            {
                "worker_id": worker_id,
                "status": "ok" if report.succeeded else "replay-failed",
                "replay_started_at": replay_started,
                "replay_finished_at": replay_finished,
                "finished_at": replay_finished,
                "replay": replay_payload,
                "trace": str(trace_path),
                "lock": operation_lock.metrics() if operation_lock is not None else None,
            }
        )
        # The result is now durable in the parent queue.  Release this
        # worker's SQL/native sessions immediately instead of keeping them
        # open while faster workers wait at the shared close barrier.
        try:
            _close_worker_backend(backend)
        except BaseException:
            # Replay outcome is already reported; a best-effort close must
            # not turn a successful worker into a second error result.
            pass
        backend = None
    except BaseException as exc:
        ready_queue.put(
            {
                "worker_id": worker_id,
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc(),
            }
        )
        result_queue.put(
            {
                "worker_id": worker_id,
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc(),
                "finished_at": time.time(),
                "lock": operation_lock.metrics() if operation_lock is not None else None,
            }
        )
        if backend is not None:
            try:
                _close_worker_backend(backend)
            except BaseException:
                pass
            backend = None
    finally:
        # ChronosFS uses a shared daemon.  A successful worker has already
        # released its backend without shutting that daemon down; an error
        # path still waits for the parent before performing the same cleanup.
        close_event.wait()
        if backend is not None:
            try:
                _close_worker_backend(backend)
            except BaseException:
                pass
        if operation_lock is not None:
            try:
                operation_lock.close()
            except BaseException:
                pass


def _wait_for_verifier(
    process: Any,
    ready_event: Any,
    timeout: float,
) -> None:
    if ready_event.wait(timeout=max(1.0, timeout)):
        return
    if process.is_alive():
        process.terminate()
        process.join(timeout=5.0)
    raise RuntimeError("verifier did not become ready")


def _check_final_state(
    backend_name: str,
    state_dir: Path,
    dimensions: int,
    options: Mapping[str, Any],
    bundles: Sequence[BundleSpec],
) -> dict[str, Any]:
    backend = _open_backend(backend_name, str(state_dir), dimensions, options)
    try:
        states = {
            bundle.input_id: _check_bundle(
                backend,
                bundle.as_dict(),
                search=True,
                search_limit=128,
            )
            for bundle in bundles
        }
    finally:
        backend.close()
    return {
        "complete_bundles": sorted(key for key, state in states.items() if state["complete"]),
        "incomplete_bundles": sorted(key for key, state in states.items() if not state["complete"]),
        "states": states,
    }


def _cleanup_worker_branches(
    backend_name: str,
    state_dir: Path,
    dimensions: int,
    options: Mapping[str, Any],
    prefix: str,
) -> list[str]:
    backend = _open_backend(backend_name, str(state_dir), dimensions, options)
    deleted: list[str] = []
    try:
        while True:
            branches = [branch for branch in backend.list_branches() if branch.startswith(prefix)]
            if not branches:
                return deleted
            branch = max(branches, key=lambda value: (value.count("/"), len(value)))
            try:
                backend.delete_branch(branch)
            except BaseException as exc:
                # Workers delete their task branch after a successful
                # publication.  A concurrent cleanup pass can therefore see
                # a branch in a stale listing after it has already gone;
                # cleanup is intentionally idempotent in that case.
                message = str(exc).casefold()
                if "branch not found" not in message and "unknown branch" not in message:
                    raise
            deleted.append(branch)
    finally:
        backend.close()


def _build_plans(
    traces_dir: Path,
    inputs: Sequence[IncidentInput],
    workers: int,
    *,
    allow_trace_reuse: bool,
    run_id: str,
    dimensions: int,
    target_branch: str,
    expanded_dir: Path,
) -> list[WorkerPlan]:
    if workers < 1 or workers > MAX_WORKERS:
        raise ValueError(f"workers must be between 1 and {MAX_WORKERS}")
    if workers > len(inputs) and not allow_trace_reuse:
        raise ValueError(
            f"{workers} workers requested but only {len(inputs)} distinct incident traces exist; "
            "pass --allow-trace-reuse for a namespaced stress run"
        )
    expanded_dir.mkdir(parents=True, exist_ok=True)
    plans: list[WorkerPlan] = []
    for worker_id in range(workers):
        row = inputs[worker_id % len(inputs)]
        if worker_id >= len(inputs) and not allow_trace_reuse:
            raise AssertionError("trace reuse validation failed")
        source = traces_dir / f"incident-response-swarm-{row.incident_id}.jsonl"
        if not source.is_file():
            raise FileNotFoundError(f"missing incident trace: {source}")
        trace = WorkloadTrace.load(source)
        expanded, bundle = _expand_trace(
            trace,
            row,
            worker_id=worker_id,
            run_id=run_id,
            dimensions=dimensions,
            target_branch=target_branch,
        )
        expanded_path = expanded_dir / f"worker-{worker_id:03d}.jsonl"
        expanded.write(expanded_path)
        plans.append(
            WorkerPlan(
                worker_id=worker_id,
                input_id=row.incident_id,
                trace_path=str(expanded_path),
                trace_id=expanded.trace_id,
                bundle=bundle,
            )
        )
    return plans


def _start_verifier(
    context: Any,
    *,
    backend_name: str,
    state_dir: Path,
    dimensions: int,
    options: Mapping[str, Any],
    bundles: Sequence[BundleSpec],
    search_every: int,
    search_limit: int,
    timeout: float,
    lock_path: str | None,
) -> tuple[Any, Any, Any, Any]:
    stop_event = context.Event()
    ready_event = context.Event()
    result_queue = context.Queue()
    process = context.Process(
        target=_verifier_main,
        args=(
            backend_name,
            str(state_dir),
            dimensions,
            dict(options),
            [bundle.as_dict() for bundle in bundles],
            stop_event,
            ready_event,
            result_queue,
            max(1, int(search_every)),
            max(1, int(search_limit)),
            lock_path,
        ),
        name=f"incident-swarm-verifier-{backend_name}",
        daemon=True,
    )
    process.start()
    _wait_for_verifier(process, ready_event, timeout)
    return process, stop_event, result_queue, ready_event


def _stop_verifier(process: Any, stop_event: Any, result_queue: Any) -> dict[str, Any]:
    """Stop the verifier without allowing a damaged queue frame to deadlock.

    ``multiprocessing.Queue.get(timeout=...)`` only bounds its initial poll.
    If a child is terminated while its feeder thread is writing a message,
    ``_recv_bytes`` can block forever on the incomplete frame.  Read the
    summary on a daemon helper thread with an explicit wall-clock bound so
    worker cleanup and final-state checking always proceed.
    """

    stop_event.set()
    process.join(timeout=10.0)
    forced = False
    if process.is_alive():
        forced = True
        process.terminate()
        process.join(timeout=5.0)

    result_holder: dict[str, Any] = {}

    def receive() -> None:
        try:
            result_holder["result"] = dict(result_queue.get())
        except BaseException as exc:  # queue EOF/corruption is diagnostic data
            result_holder["error"] = f"{type(exc).__name__}: {exc}"

    reader = threading.Thread(
        target=receive,
        name="incident-verifier-result-reader",
        daemon=True,
    )
    reader.start()
    reader.join(timeout=1.0 if forced else 5.0)
    result_value = result_holder.get("result")
    if isinstance(result_value, Mapping):
        result = dict(result_value)
    else:
        result = {
            "process_error": (
                "verifier was terminated before its summary was readable"
                if forced
                else result_holder.get(
                    "error", "verifier exited without a readable summary"
                )
            ),
            "checks": 0,
            "violation_count": 1,
            "publication_violation_count": 0,
            "cross_store_violation_count": 0,
            "violations": [],
        }
    result.update({"pid": process.pid, "exitcode": process.exitcode})
    return result


def _max_interval_overlap(results: Sequence[Mapping[str, Any]]) -> int:
    points: list[tuple[float, int]] = []
    for result in results:
        start = result.get("replay_started_at")
        finish = result.get("replay_finished_at")
        if not isinstance(start, (int, float)) or not isinstance(finish, (int, float)):
            continue
        if finish < start:
            continue
        points.extend(((float(start), 1), (float(finish), -1)))
    # End points sort before starts at the same timestamp so an operation that
    # finished exactly as another started is not counted as overlapping.
    points.sort(key=lambda item: (item[0], item[1]))
    active = 0
    maximum = 0
    for _, delta in points:
        active += delta
        maximum = max(maximum, active)
    return maximum


def _mounted_chronosfs_paths(root: Path) -> list[Path]:
    """Return ChronosFS mounts below one run's state directory.

    ``os.walk`` cannot discover a disconnected FUSE endpoint reliably.  The
    kernel mount table remains authoritative even after the owning worker has
    been terminated, so cleanup reads ``/proc/self/mountinfo`` directly.
    """

    resolved_root = root.expanduser().resolve()
    paths: list[Path] = []
    try:
        mountinfo = Path("/proc/self/mountinfo").read_text(encoding="utf-8")
    except OSError:
        return paths
    for line in mountinfo.splitlines():
        before, separator, after = line.partition(" - ")
        after_fields = after.split()
        if not separator or len(after_fields) < 2 or after_fields[1] != "chronosfs":
            continue
        fields = before.split()
        if len(fields) < 5:
            continue
        raw_path = fields[4].replace("\\040", " ").replace("\\011", "\t")
        raw_path = raw_path.replace("\\134", "\\")
        path = Path(raw_path)
        try:
            is_in_tree = path.is_relative_to(resolved_root)
        except AttributeError:  # pragma: no cover - Python 3.8 fallback.
            is_in_tree = str(path).startswith(str(resolved_root) + os.sep)
        if is_in_tree:
            paths.append(path)
    return sorted(set(paths), key=lambda item: (len(item.parts), str(item)), reverse=True)


def _force_unmount_chronosfs(root: Path) -> list[str]:
    """Detach all remaining run-scoped FUSE endpoints, deepest first."""

    failures: list[str] = []
    for path in _mounted_chronosfs_paths(root):
        detached = False
        for command in (
            ("fusermount3", "-u", str(path)),
            ("fusermount", "-u", str(path)),
            ("umount", str(path)),
            ("fusermount3", "-uz", str(path)),
            ("fusermount", "-uz", str(path)),
            ("umount", "-l", str(path)),
        ):
            if shutil.which(command[0]) is None:
                continue
            try:
                subprocess.run(
                    command,
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=10.0,
                )
            except (OSError, subprocess.TimeoutExpired):
                continue
            if not os.path.ismount(path):
                detached = True
                break
        if not detached and os.path.ismount(path):
            failures.append(str(path))
    return failures


def _force_shutdown_chronosfs(backend: Any) -> None:
    filesystem = getattr(backend, "filesystem", None)
    if filesystem is not None:
        shutdown_chronosfs_daemon(filesystem, force=True)


def run_backend(
    args: argparse.Namespace,
    *,
    backend_name: str,
    state_dir: Path,
    plans: Sequence[WorkerPlan],
    run_id: str,
) -> dict[str, Any]:
    context = mp.get_context("spawn")
    options = _backend_options(args, backend_name)
    lock_path = None
    if args.native_big_lock and backend_name == "doltgres-qdrant-btrfs":
        lock_path = str(
            (args.output / "native-branching-big.lock")
            .expanduser()
            .resolve()
        )
    result_backend_name = (
        "native-branching-lock"
        if lock_path is not None
        else (
            "native-branching"
            if backend_name == "doltgres-qdrant-btrfs"
            else backend_name
        )
    )
    # Start ChronosFS once in the parent before fanning out 128 independent
    # workers.  The daemon has one lock-protected startup path, but asking a
    # large process swarm to discover and start it simultaneously can still
    # exhaust the short readiness window.  The daemon's explicit keep-alive
    # lease keeps it running, so the parent's bootstrap backend can be closed
    # immediately; retaining its full relational/native connection set would
    # otherwise consume several unnecessary database sessions for the run.
    control_backend = _open_backend(backend_name, str(state_dir), args.dimensions, options)
    control_backend_released = False
    try:
        start_backend = getattr(control_backend, "start", None)
        if callable(start_backend):
            start_backend()
            close_without_daemon = getattr(control_backend, "close", None)
            if callable(close_without_daemon):
                try:
                    close_without_daemon(shutdown_daemon=False)
                except TypeError:
                    # Non-Chronos comparison backends do not own the shared
                    # daemon and retain their ordinary close signature.
                    close_without_daemon()
            control_backend_released = True
        else:
            close_backend = getattr(control_backend, "close", None)
            if callable(close_backend):
                close_backend()
            control_backend_released = True
    except BaseException:
        with contextlib.suppress(BaseException):
            control_backend.close()
        raise
    verifier_process = verifier_stop = verifier_queue = None
    if not args.no_verifier:
        verifier_process, verifier_stop, verifier_queue, _ = _start_verifier(
            context,
            backend_name=backend_name,
            state_dir=state_dir,
            dimensions=args.dimensions,
            options=options,
            bundles=[plan.bundle for plan in plans],
            search_every=args.verifier_search_every,
            search_limit=args.verifier_search_limit,
            timeout=args.verifier_startup_timeout_seconds,
            lock_path=lock_path,
        )
    start_event = context.Event()
    close_event = context.Event()
    ready_queue = context.Queue()
    result_queue = context.Queue()
    processes: list[Any] = []
    started = time.perf_counter()
    ready: dict[int, dict[str, Any]] = {}
    startup_deadline = time.monotonic() + args.startup_timeout_seconds
    startup_error: str | None = None
    # Native interval stores perform one-time schema/index initialization when
    # a process opens its connection.  Starting 128 initializers in the same
    # instant can make them race on that setup even though the replay itself is
    # intentionally fully concurrent.  Admit a small batch, wait for that
    # batch to finish opening, and only then admit the next batch.  All workers
    # still wait on ``start_event`` and begin the measured agent replay
    # together, so this changes startup robustness—not workload concurrency.
    startup_batch_size = max(1, int(args.startup_batch_size))
    for offset in range(0, len(plans), startup_batch_size):
        batch = plans[offset : offset + startup_batch_size]
        batch_ids = {plan.worker_id for plan in batch}
        for plan in batch:
            process = context.Process(
                target=_worker_main,
                args=(
                    plan.worker_id,
                    backend_name,
                    str(state_dir),
                    args.dimensions,
                    args.embedding_model,
                    options,
                    plan.trace_path,
                    str(args.repo_dir),
                    bool(args.allow_shell),
                    not bool(args.no_replay_llm_latency),
                    float(args.llm_latency_scale),
                    int(args.merge_retries),
                    ready_queue,
                    result_queue,
                    start_event,
                    close_event,
                    lock_path,
                ),
                name=f"incident-swarm-{result_backend_name}-worker-{plan.worker_id:03d}",
                daemon=True,
            )
            process.start()
            processes.append(process)
        while not batch_ids <= ready.keys() and time.monotonic() < startup_deadline:
            try:
                event = ready_queue.get(timeout=0.25)
            except Empty:
                continue
            worker_id = int(event.get("worker_id", -1))
            if event.get("status") == "ready":
                ready[worker_id] = event
            else:
                startup_error = (
                    f"worker {worker_id} failed during startup: "
                    f"{event.get('error') or event}"
                    + (
                        f"\n{event['traceback']}"
                        if event.get("traceback")
                        else ""
                    )
                )
                break
        if startup_error is not None:
            break
    if startup_error is not None or len(ready) != len(processes):
        close_event.set()
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=5.0)
        _force_unmount_chronosfs(state_dir)
        if verifier_process is not None:
            _stop_verifier(verifier_process, verifier_stop, verifier_queue)
        with contextlib.suppress(BaseException):
            _force_shutdown_chronosfs(control_backend)
        with contextlib.suppress(BaseException):
            if not control_backend_released:
                control_backend.close()
        raise RuntimeError(
            startup_error
            or f"only {len(ready)}/{len(processes)} workers became ready"
        )

    start_event.set()
    results: dict[int, dict[str, Any]] = {}
    run_timeout = float(args.run_timeout_seconds)
    if run_timeout < 0:
        raise SystemExit("--run-timeout-seconds must be non-negative")
    deadline = (
        None if run_timeout == 0 else time.monotonic() + run_timeout
    )
    while len(results) < len(processes) and (
        deadline is None or time.monotonic() < deadline
    ):
        try:
            result = dict(result_queue.get(timeout=0.5))
        except Empty:
            continue
        results[int(result.get("worker_id", -1))] = result

    if len(results) != len(processes):
        close_event.set()
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=5.0)
        _force_unmount_chronosfs(state_dir)
        timed_out = sorted(set(range(len(processes))) - set(results))
        for worker_id in timed_out:
            results[worker_id] = {
                "worker_id": worker_id,
                "status": "timeout",
                "error": "worker exceeded --run-timeout-seconds",
            }

    verifier = {"execution_mode": "disabled", "checks": 0, "violations": []}
    try:
        if verifier_process is not None:
            verifier = _stop_verifier(
                verifier_process,
                verifier_stop,
                verifier_queue,
            )
    finally:
        # Verifier shutdown must never keep workers parked at close_event.wait.
        close_event.set()
    close_deadline = time.monotonic() + max(
        1.0, float(args.close_timeout_seconds)
    )
    for process in processes:
        remaining = max(0.0, close_deadline - time.monotonic())
        process.join(timeout=remaining)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5.0)
    mount_cleanup_failures = _force_unmount_chronosfs(state_dir)
    with contextlib.suppress(BaseException):
        _force_shutdown_chronosfs(control_backend)
    final = _check_final_state(
        backend_name,
        state_dir,
        args.dimensions,
        options,
        [plan.bundle for plan in plans],
    )
    prefix = f"swarm/{run_id}/"
    deleted = _cleanup_worker_branches(
        backend_name,
        state_dir,
        args.dimensions,
        options,
        prefix,
    )
    with contextlib.suppress(BaseException):
        if not control_backend_released:
            control_backend.close()
    worker_values = [results[index] for index in sorted(results)]
    successful = sum(item.get("status") == "ok" for item in worker_values)
    worker_locks = [
        item.get("lock")
        for item in worker_values
        if isinstance(item.get("lock"), Mapping)
    ]
    lock_summary = (
        {
            "scope": "complete-agent-replay",
            "lock_wait_seconds": sum(
                float(item.get("lock_wait_seconds", 0.0))
                for item in worker_locks
            ),
            "lock_acquires": sum(
                int(item.get("lock_acquires", 0)) for item in worker_locks
            ),
            "lock_hold_seconds": sum(
                float(item.get("lock_hold_seconds", 0.0))
                for item in worker_locks
            ),
        }
        if worker_locks
        else None
    )
    elapsed = time.perf_counter() - started
    return {
        "backend": result_backend_name,
        "underlying_backend": backend_name,
        "chronosfs_metadata_cache": "disabled",
        "coordination": "whole-agent-exclusive-lock"
        if lock_path is not None
        else "none",
        "lock_path": lock_path,
        "lock": lock_summary,
        "workers": len(plans),
        "successful_workers": successful,
        "elapsed_seconds": elapsed,
        "throughput_bundles_per_second": successful / elapsed if elapsed > 0 else 0.0,
        "maximum_replay_overlap": _max_interval_overlap(worker_values),
        "worker_results": worker_values,
        "verifier": verifier,
        "final_state": final,
        "deleted_private_branches": deleted,
        "chronosfs_mount_cleanup_failures": mount_cleanup_failures,
        "status": (
            "ok"
            if successful == len(plans)
            and not final["incomplete_bundles"]
            and int(verifier.get("violation_count", 0)) == 0
            else "anomaly"
        ),
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    # Apply conservative connection/thread defaults symmetrically to either
    # backend.  An explicit caller setting remains authoritative.
    for name, value in _CONNECTION_LIMIT_ENV.items():
        os.environ.setdefault(name, value)
    # The swarm deliberately allows independent processes to observe state
    # while other workers publish.  Disable both native ChronosFS metadata
    # caches and FUSE metadata TTLs so every lookup goes back to the current
    # interval-visible state.
    os.environ["CHRONOSFS_DISABLE_METADATA_CACHE"] = "1"
    if args.workers < 1 or args.workers > MAX_WORKERS:
        raise SystemExit(f"--workers must be between 1 and {MAX_WORKERS}")
    if args.merge_retries < -1:
        raise SystemExit(
            "--merge-retries must be a non-negative integer or 'unlimited'"
        )
    if args.close_timeout_seconds <= 0:
        raise SystemExit("--close-timeout-seconds must be positive")
    if args.startup_batch_size < 1:
        raise SystemExit("--startup-batch-size must be positive")
    if not args.qdrant_url:
        raise SystemExit(
            "incident-response swarm runs require the shared Docker Qdrant "
            "service for every worker count; pass --qdrant-url"
        )
    dsn = str(args.chronos_postgres_dsn or "").strip()
    if urlparse(dsn).scheme not in {"postgres", "postgresql"}:
        raise SystemExit(
            "incident-response Chronos runs require PostgreSQL; pass "
            "--chronos-postgres-dsn with a postgres:// or postgresql:// URL"
        )
    state_dirs = _parse_backend_paths(args.state_dir)
    selected_backends = tuple(args.selected_backends or BACKENDS)
    if args.native_big_lock and "doltgres-qdrant-btrfs" not in selected_backends:
        raise SystemExit(
            "--native-big-lock applies only to the doltgres-qdrant-btrfs "
            "native-branching backend"
        )
    missing_backends = sorted(set(selected_backends) - set(state_dirs))
    if missing_backends:
        raise SystemExit(
            "the requested comparison is missing prepared state for "
            f"missing {', '.join(missing_backends)}"
        )
    state_dirs = {
        backend: state_dirs[backend]
        for backend in selected_backends
    }
    args.traces_dir = args.traces_dir.expanduser().resolve()
    args.inputs = args.inputs.expanduser().resolve()
    args.output = args.output.expanduser().resolve()
    args.repo_dir = args.repo_dir.expanduser().resolve()
    inputs = _load_inputs(args.inputs)
    run_id = f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{os.getpid()}"
    expanded_dir = args.output / "expanded-traces"
    plans = _build_plans(
        args.traces_dir,
        inputs,
        args.workers,
        allow_trace_reuse=args.allow_trace_reuse,
        run_id=run_id,
        dimensions=args.dimensions,
        target_branch=args.target_branch,
        expanded_dir=expanded_dir,
    )
    args.output.mkdir(parents=True, exist_ok=True)
    config = {
        "schema_version": 1,
        "experiment": "incident-response-swarm-v1",
        "run_id": run_id,
        "workers": args.workers,
        "max_workers": MAX_WORKERS,
        "backends": sorted(state_dirs),
        "native_big_lock": bool(args.native_big_lock),
        "native_big_lock_scope": (
            "complete-agent-replay" if args.native_big_lock else None
        ),
        "merge_retries": (
            "unlimited" if args.merge_retries == -1 else int(args.merge_retries)
        ),
        "replay_llm_latency": not bool(args.no_replay_llm_latency),
        "llm_latency_scale": float(args.llm_latency_scale),
        "run_timeout_seconds": float(args.run_timeout_seconds),
        "close_timeout_seconds": float(args.close_timeout_seconds),
        "startup_batch_size": int(args.startup_batch_size),
        "connection_limits": {
            "qdrant_pool_size": int(
                _CONNECTION_LIMIT_ENV["CHRONOS_QDRANT_POOL_SIZE"]
            ),
            "chronos_interval_gc": "synchronous",
            "worker_thread_limits": {
                name: value
                for name, value in _CONNECTION_LIMIT_ENV.items()
                if name not in {
                    "CHRONOS_QDRANT_POOL_SIZE",
                    "CHRONOS_INTERVAL_GC_SYNCHRONOUS",
                }
            },
        },
        "target_branch": args.target_branch,
        "allow_trace_reuse": bool(args.allow_trace_reuse),
        "verifier": not args.no_verifier,
        "verifier_search_every": args.verifier_search_every,
        "plans": [plan.as_dict() for plan in plans],
    }
    (args.output / "config.json").write_text(
        json.dumps(config, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    results: list[dict[str, Any]] = []
    for backend_name in sorted(state_dirs):
        result = run_backend(
            args,
            backend_name=backend_name,
            state_dir=state_dirs[backend_name],
            plans=plans,
            run_id=run_id,
        )
        results.append(result)
        print(
            json.dumps({"backend": result["backend"], "status": result["status"]}),
            flush=True,
        )
    payload = {**config, "results": results}
    (args.output / "results.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return 0 if all(result["status"] == "ok" for result in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
