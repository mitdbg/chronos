#!/usr/bin/env python3
"""Concurrent, verifier-driven experiments for the enterprise polystore.

The four experiments in this file use the same backend-neutral interface as the
normal enterprise workflow replays.  They deliberately keep the data set small
so that the interesting variable is concurrent state management rather than
LLM or ingestion cost:

``disjoint``
    Independent engineers promote different documents at the same time.
``competing``
    Two revisions of one document race; only the selected revision and its
    complete index bundle may be promoted.
``recursive``
    A parent fans out to children and the children fan back in to two targets
    while another parent update contends for the same targets.
``crash``
    A child process is terminated at a publication fail point, then the parent
    reopens the state and checks recovery.

The verifier is a separate operating-system process.  It repeatedly reads the
relational record, file, and vector-search result for every tracked document
while the workers are writing and merging.  A mismatch is recorded with its
scope (publication or branch-local write), phase, and timestamp.  Running the
verifier in a process, rather than a Python thread, ensures that its checks can
execute concurrently with Python-side worker activity despite the GIL.

Examples (run from the repository root):

    uv run python apps/enterprise-knowledge-mcp/scripts/run_concurrent_polystore_experiments.py \
      --backend chronos --scenario all --output .enterprise-knowledge/concurrency

For a fair three-backend run, point every backend at the same Docker Qdrant and
the configured Doltgres/Btrfs service:

    uv run python ... --backend chronos --backend app-managed \
      --backend doltgres-qdrant-btrfs --qdrant-url http://127.0.0.1:6333 \
      --doltgres-dsn postgresql://root@127.0.0.1:5433/knowledge

Crash runs use a separate process and should use a remote Docker Qdrant service;
the local embedded Qdrant server is intentionally not shared across processes.
The fail-point hooks are confined to this experiment driver and are never part
of Chronos or an application backend.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import random
from queue import Empty
import sys
import tempfile
import threading
import time
import traceback
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from chronos_enterprise_knowledge.backends.factory import (
    create_knowledge_backend,
)
from chronos_enterprise_knowledge.models import (
    DocumentChunk,
    IndexedDocument,
    KnowledgeDocument,
    SearchHit,
    content_hash,
)


BACKEND_NAMES = (
    "chronos",
    "app-managed",
    "physical-clone",
    "doltgres-qdrant-btrfs",
)
SCENARIOS = ("disjoint", "competing", "recursive", "crash")
_RETRY_RANDOM = random.SystemRandom()
_RETRY_BACKOFF_CAP_SECONDS = 5.0


def _retry_backoff_seconds(attempt: int) -> float:
    """Return full-jitter exponential backoff for a retry attempt.

    Attempt zero is the initial merge and does not sleep.  Subsequent retries
    use a 1 ms base and cap the randomized delay at five seconds, matching the
    normal trace replay.  The policy is backend-neutral: it only spaces a
    fresh preview/apply attempt after a transient publication race.
    """

    if attempt <= 0:
        return 0.0
    cap = min(
        0.001 * (2 ** min(attempt - 1, 10)),
        _RETRY_BACKOFF_CAP_SECONDS,
    )
    return _RETRY_RANDOM.uniform(0.0, cap)


def _now() -> float:
    return time.time()


def _json(value: Any) -> Any:
    """Convert backend payloads to stable JSON without losing error context."""

    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(k): _json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json(v) for v in value]
    return str(value)


def _has_errors(value: Any) -> bool:
    """Whether a nested final-check payload contains a non-empty error."""

    if value is None or value is False:
        return False
    if isinstance(value, str):
        return bool(value)
    if isinstance(value, Mapping):
        return any(_has_errors(item) for item in value.values())
    if isinstance(value, (list, tuple, set, frozenset)):
        return any(_has_errors(item) for item in value)
    return bool(value)


def _has_worker_failures(value: Any) -> bool:
    """Detect failed worker operations while allowing an expected loser."""

    if not isinstance(value, (list, tuple)):
        return False
    for worker in value:
        if not isinstance(worker, Mapping):
            continue
        if worker.get("status") == "error":
            return True
        if worker.get("status") == "rejected":
            continue
        for key, item in worker.items():
            if key in {"branch", "status"}:
                continue
            if isinstance(item, str) and (
                "Error(" in item or "Error_(" in item or "Exception(" in item
            ):
                return True
    return False


def _safe_not_found(exc: BaseException) -> bool:
    if isinstance(exc, FileNotFoundError):
        return True
    message = str(exc).lower()
    return any(
        token in message
        for token in ("file not found", "path not found", "no such file")
    )


@dataclass(frozen=True)
class DocumentSpec:
    document_id: str
    path: str
    query_text: str
    query_embedding: tuple[float, ...]


@dataclass
class RunState:
    phase: str = "initializing"
    lock: threading.Lock = field(default_factory=threading.Lock)
    shared_phase: Any | None = field(default=None, repr=False)

    def set(self, phase: str) -> None:
        with self.lock:
            self.phase = phase
            if self.shared_phase is not None:
                self.shared_phase["phase"] = phase

    def get(self) -> str:
        if self.shared_phase is not None:
            try:
                return str(self.shared_phase.get("phase", self.phase))
            except BaseException:
                # A verifier should still be able to report a useful phase if
                # the manager is shutting down while it records an anomaly.
                pass
        with self.lock:
            return self.phase


class BranchRegistry:
    """Thread/process-safe view of branches that the verifier should inspect.

    The parent worker pool updates the registry from Python threads.  The
    verifier process reads the same manager-backed dictionaries, so it sees
    branch creation and active-write windows without sharing a backend object
    or relying on a GIL-contending verifier thread.
    """

    def __init__(self, *, shared: Mapping[str, Any] | None = None) -> None:
        self._branches: set[str] = {"main"}
        self._targets: set[str] = {"main"}
        self._active_writes: set[str] = set()
        self._lock = threading.Lock()
        self._shared = shared
        if self._shared is not None:
            self._shared["branches"]["main"] = True
            self._shared["targets"]["main"] = True

    def add(self, branch: str, *, target: bool = False) -> None:
        with self._lock:
            self._branches.add(branch)
            if self._shared is not None:
                self._shared["branches"][branch] = True
            if target:
                self._targets.add(branch)
                if self._shared is not None:
                    self._shared["targets"][branch] = True

    def remove(self, branch: str) -> None:
        with self._lock:
            self._branches.discard(branch)
            self._targets.discard(branch)
            self._active_writes.discard(branch)
            if self._shared is not None:
                self._shared["branches"].pop(branch, None)
                self._shared["targets"].pop(branch, None)
                self._shared["active_writes"].pop(branch, None)

    def begin_write(self, branch: str) -> None:
        with self._lock:
            self._active_writes.add(branch)
            if self._shared is not None:
                self._shared["active_writes"][branch] = True

    def end_write(self, branch: str) -> None:
        with self._lock:
            self._active_writes.discard(branch)
            if self._shared is not None:
                self._shared["active_writes"].pop(branch, None)

    def snapshot(self) -> tuple[set[str], set[str], set[str]]:
        with self._lock:
            if self._shared is not None:
                return (
                    set(self._shared["branches"].keys()),
                    set(self._shared["targets"].keys()),
                    set(self._shared["active_writes"].keys()),
                )
            return (
                set(self._branches),
                set(self._targets),
                set(self._active_writes),
            )


@dataclass
class Recorder:
    events: list[dict[str, Any]] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, kind: str, **fields: Any) -> None:
        event = {"time": _now(), "kind": kind, **_json(fields)}
        with self.lock:
            self.events.append(event)


class InvariantVerifier:
    """Continuously validate relational, filesystem, and vector-store state.

    This class is deliberately a single-process checker.  The experiment
    driver runs :meth:`run_until` in a dedicated ``multiprocessing`` child;
    keeping the checker itself free of a background thread makes it impossible
    to accidentally regress to a GIL-contending verifier.
    """

    def __init__(
        self,
        backend: Any,
        registry: BranchRegistry,
        documents: Callable[[], tuple[DocumentSpec, ...]],
        state: RunState,
        recorder: Recorder,
        *,
        interval: float = 0.01,
        search_every: int = 5,
        max_pairs_per_check: int = 8,
    ) -> None:
        self.backend = backend
        self.registry = registry
        self.documents = documents
        self.state = state
        self.recorder = recorder
        self.interval = max(0.001, interval)
        self.search_every = max(1, search_every)
        self.max_pairs_per_check = max(1, max_pairs_per_check)
        self.checks = 0
        self.violations: list[dict[str, Any]] = []
        self._pair_cursor = 0

    def run_until(self, stop_event: Any) -> None:
        """Run checks continuously until a process-safe stop event is set.

        This is intentionally a busy loop.  The verifier is an independent
        process, so it does not contend with worker Python threads for the
        GIL, and a sleep-based polling interval would create an avoidable
        sampling gap during short publication windows.
        """

        while not stop_event.is_set():
            try:
                self.check_once()
            except BaseException as exc:  # verifier failures are experiment data
                self._record_violation(
                    "verifier", "*", "*", "verifier-error", repr(exc)
                )

    def _record_violation(
        self,
        scope: str,
        branch: str,
        document_id: str,
        kind: str,
        error: str,
    ) -> None:
        violation = {
            "time": _now(),
            "scope": scope,
            "branch": branch,
            "document_id": document_id,
            "kind": kind,
            "phase": self.state.get(),
            "error": error,
        }
        self.violations.append(violation)

    def check_once(self) -> None:
        branches, targets, active_writes = self.registry.snapshot()
        specs = self.documents()
        self.checks += 1
        do_search = self.checks % self.search_every == 0
        pairs = [(branch, spec) for branch in sorted(branches) for spec in specs]
        if not pairs:
            return
        if len(pairs) > self.max_pairs_per_check:
            start = self._pair_cursor % len(pairs)
            self._pair_cursor += self.max_pairs_per_check
            pairs = [
                pairs[(start + offset) % len(pairs)]
                for offset in range(self.max_pairs_per_check)
            ]
        for branch, spec in pairs:
            scope = "publication" if branch in targets else "branch-local"
            if branch in active_writes:
                scope = "branch-local-write"
            self._check_document(branch, spec, scope, do_search)

    def _check_document(
        self,
        branch: str,
        spec: DocumentSpec,
        scope: str,
        do_search: bool,
    ) -> None:
        try:
            indexed = self.backend.get_document(branch, spec.document_id)
        except BaseException as exc:
            self._record_violation(
                scope, branch, spec.document_id, "get-document", repr(exc)
            )
            return

        try:
            path_id = self.backend.find_document_id_by_path(branch, spec.path)
        except BaseException as exc:
            self._record_violation(
                scope, branch, spec.document_id, "path-lookup", repr(exc)
            )
            path_id = None

        if indexed is None:
            if path_id is not None:
                self._record_violation(
                    scope,
                    branch,
                    spec.document_id,
                    "orphan-file-index",
                    f"path lookup returned {path_id!r} without a document row",
                )
            try:
                self.backend.read_file(branch, spec.path)
            except BaseException as exc:
                if not _safe_not_found(exc):
                    self._record_violation(
                        scope, branch, spec.document_id, "orphan-file", repr(exc)
                    )
            else:
                self._record_violation(
                    scope,
                    branch,
                    spec.document_id,
                    "orphan-file",
                    "file is visible without a relational document",
                )
            try:
                hits = self.backend.search(
                    branch,
                    spec.query_text,
                    spec.query_embedding,
                    limit=8,
                )
            except BaseException as exc:
                self._record_violation(
                    scope, branch, spec.document_id, "orphan-vector-check", repr(exc)
                )
            else:
                if any(hit.document_id == spec.document_id for hit in hits):
                    self._record_violation(
                        scope,
                        branch,
                        spec.document_id,
                        "orphan-vector",
                        "Qdrant returned a point without a relational document",
                    )
            return

        document = indexed.document
        if document.id != spec.document_id:
            self._record_violation(
                scope,
                branch,
                spec.document_id,
                "document-id-mismatch",
                f"returned {document.id!r}",
            )
        if document.path != spec.path:
            self._record_violation(
                scope,
                branch,
                spec.document_id,
                "document-path-mismatch",
                f"returned {document.path!r}",
            )
        if path_id != spec.document_id:
            self._record_violation(
                scope,
                branch,
                spec.document_id,
                "path-index-mismatch",
                f"path lookup returned {path_id!r}",
            )
        try:
            file_content = self.backend.read_file(branch, spec.path)
        except BaseException as exc:
            self._record_violation(
                scope, branch, spec.document_id, "missing-file", repr(exc)
            )
            file_content = None
        if file_content is not None and file_content != document.content.encode(
            "utf-8"
        ):
            self._record_violation(
                scope,
                branch,
                spec.document_id,
                "file-record-mismatch",
                "filesystem bytes differ from the relational document",
            )
        expected_texts = {chunk.text for chunk in indexed.chunks}
        for chunk in indexed.chunks:
            if chunk.document_id != document.id:
                self._record_violation(
                    scope,
                    branch,
                    spec.document_id,
                    "chunk-document-mismatch",
                    f"chunk {chunk.id!r} points to {chunk.document_id!r}",
                )
            if chunk.sha256 != content_hash(chunk.text):
                self._record_violation(
                    scope,
                    branch,
                    spec.document_id,
                    "chunk-hash-mismatch",
                    f"chunk {chunk.id!r} has an invalid text hash",
                )
        if not do_search:
            return
        try:
            hits = self.backend.search(
                branch,
                spec.query_text,
                spec.query_embedding,
                limit=8,
            )
        except BaseException as exc:
            self._record_violation(scope, branch, spec.document_id, "search", repr(exc))
            return
        for hit in hits:
            if not isinstance(hit, SearchHit):
                continue
            if hit.document_id == document.id:
                if hit.path != document.path or hit.text not in expected_texts:
                    self._record_violation(
                        scope,
                        branch,
                        spec.document_id,
                        "vector-record-mismatch",
                        "search returned a stale path or chunk payload",
                    )

    def summary(self) -> dict[str, Any]:
        violations = list(self.violations)
        return {
            "checks": self.checks,
            "violation_count": len(violations),
            "publication_violation_count": sum(
                item["scope"] == "publication" for item in violations
            ),
            "branch_local_violation_count": sum(
                item["scope"] != "publication" for item in violations
            ),
            "violations": violations[:200],
        }


def _shared_document_specs(shared_documents: Any) -> tuple[DocumentSpec, ...]:
    """Materialize document specifications from a manager-backed mapping."""

    values = list(shared_documents.values())
    values.sort(key=lambda value: str(value["document_id"]))
    return tuple(
        DocumentSpec(
            str(value["document_id"]),
            str(value["path"]),
            str(value["query_text"]),
            tuple(float(item) for item in value["query_embedding"]),
        )
        for value in values
    )


def _verifier_process_main(
    backend_name: str,
    state_dir: str,
    dimensions: int,
    factory_kwargs: Mapping[str, Any],
    shared_registry: Mapping[str, Any],
    shared_documents: Any,
    shared_phase: Any,
    stop_event: Any,
    ready_event: Any,
    result_queue: Any,
    interval: float,
    max_pairs_per_check: int,
) -> None:
    """Run the verifier in an independent process with its own backend.

    The process receives only serializable backend configuration and
    manager-backed coordination state.  It never inherits or shares the
    worker backend connection, which avoids both connection-level races and
    Python-thread scheduling/GIL effects.
    """

    started = _now()
    backend: Any | None = None
    result: dict[str, Any] = {
        "execution_mode": "process",
        "pid": os.getpid(),
        "started_at": started,
    }
    try:
        backend = create_knowledge_backend(
            backend_name,
            state_dir=state_dir,
            vector_dimensions=dimensions,
            **dict(factory_kwargs),
        )
        registry = BranchRegistry(shared=shared_registry)
        state = RunState(shared_phase=shared_phase)
        verifier = InvariantVerifier(
            backend,
            registry,
            lambda: _shared_document_specs(shared_documents),
            state,
            Recorder(),
            interval=interval,
            max_pairs_per_check=max_pairs_per_check,
        )
        ready_event.set()
        verifier.run_until(stop_event)
        result.update(verifier.summary())
    except BaseException as exc:  # verifier failures are experiment data
        ready_event.set()
        result.update(
            {
                "checks": 0,
                "violation_count": 1,
                "publication_violation_count": 0,
                "branch_local_violation_count": 1,
                "process_error": repr(exc),
                "violations": [
                    {
                        "time": _now(),
                        "scope": "verifier",
                        "branch": "*",
                        "document_id": "*",
                        "kind": "verifier-process-error",
                        "phase": str(shared_phase.get("phase", "unknown")),
                        "error": repr(exc),
                    }
                ],
            }
        )
    finally:
        if backend is not None:
            try:
                backend.close()
            except BaseException:
                pass
        result["stopped_at"] = _now()
        try:
            result_queue.put(result)
        except BaseException:
            # The parent may have terminated while the verifier was winding
            # down.  There is no safe way to report the summary in that case.
            pass


@dataclass
class VerifierProcess:
    """Parent-side lifecycle wrapper for the independent verifier process."""

    process: Any
    stop_event: Any
    ready_event: Any
    result_queue: Any
    interval: float
    started_at: float
    ready_at: float | None = None
    worker_started_at: float | None = None

    def stop(self) -> dict[str, Any]:
        self.stop_event.set()
        timeout = max(5.0, self.interval * 100.0)
        self.process.join(timeout=timeout)
        if self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout=5.0)
        try:
            summary = dict(self.result_queue.get(timeout=2.0))
        except Empty:
            summary = {
                "execution_mode": "process",
                "checks": 0,
                "violation_count": 1,
                "publication_violation_count": 0,
                "branch_local_violation_count": 1,
                "process_error": "verifier exited without a summary",
                "violations": [],
            }
        summary.setdefault("execution_mode", "process")
        summary["process_pid"] = self.process.pid
        summary["process_exitcode"] = self.process.exitcode
        summary["parent_started_at"] = self.started_at
        summary["verifier_ready_at"] = self.ready_at
        summary["parent_stopped_at"] = _now()
        summary["worker_started_at"] = self.worker_started_at
        summary["concurrent_with_workers"] = bool(
            self.worker_started_at is not None
            and self.ready_at is not None
            and self.worker_started_at >= self.ready_at
            and summary["parent_stopped_at"] > self.worker_started_at
        )
        return summary


def _vector(index: int, dimensions: int) -> tuple[float, ...]:
    if dimensions < 1:
        raise ValueError("vector dimensions must be positive")
    values = [0.0] * dimensions
    values[index % dimensions] = 1.0
    return tuple(values)


def make_document(
    document_id: str,
    path: str,
    content: str,
    *,
    dimensions: int,
    vector_index: int,
) -> IndexedDocument:
    document = KnowledgeDocument(
        id=document_id,
        path=path,
        title=document_id.replace("/", " ").replace("-", " ").title(),
        source="concurrent-polystore-experiment",
        content=content,
        kind="curated",
        metadata={"experiment": "concurrent-polystore"},
    )
    chunk = DocumentChunk(
        id=f"{document_id}:chunk-0",
        document_id=document_id,
        ordinal=0,
        text=content,
        embedding=_vector(vector_index, dimensions),
        metadata={"experiment": "concurrent-polystore"},
    )
    return IndexedDocument(document=document, chunks=(chunk,))


@dataclass
class ExperimentContext:
    backend: Any
    backend_name: str
    state_dir: str
    factory_kwargs: Mapping[str, Any]
    registry: BranchRegistry
    state: RunState
    recorder: Recorder
    dimensions: int
    verify_interval: float = 0.01
    verify_pairs: int = 8
    documents: dict[str, DocumentSpec] = field(default_factory=dict)
    shared_documents: Any | None = None
    verifier_enabled: bool = True
    verifier: VerifierProcess | None = None
    verifier_started_at: float | None = None
    worker_started_at: float | None = None

    def track(self, indexed: IndexedDocument) -> None:
        spec = DocumentSpec(
            indexed.document.id,
            indexed.document.path,
            indexed.document.title,
            indexed.chunks[0].embedding
            if indexed.chunks
            else _vector(0, self.dimensions),
        )
        self.documents[indexed.document.id] = spec
        if self.shared_documents is not None:
            self.shared_documents[indexed.document.id] = {
                "document_id": spec.document_id,
                "path": spec.path,
                "query_text": spec.query_text,
                "query_embedding": tuple(spec.query_embedding),
            }

    def specs(self) -> tuple[DocumentSpec, ...]:
        return tuple(self.documents.values())

    def mark_workers_started(self) -> None:
        self.worker_started_at = _now()
        if self.verifier is not None:
            self.verifier.worker_started_at = self.worker_started_at

    def worker_backend(self) -> tuple[Any, bool]:
        """Open an agent/session connection when the backend supports it."""

        if not self.factory_kwargs.get("qdrant_url"):
            # Embedded Qdrant deliberately permits only one client per path.
            # The local fallback is for smoke tests, not performance claims.
            return self.backend, False
        return (
            create_knowledge_backend(
                self.backend_name,
                state_dir=self.state_dir,
                vector_dimensions=self.dimensions,
                **dict(self.factory_kwargs),
            ),
            True,
        )

    def worker_backends(self, count: int) -> tuple[list[Any], bool]:
        """Create agent connections serially to avoid collection-init races."""

        if not self.factory_kwargs.get("qdrant_url"):
            return [self.backend] * count, False
        opened: list[Any] = []
        try:
            for _ in range(count):
                backend, owns = self.worker_backend()
                assert owns
                opened.append(backend)
        except BaseException:
            for backend in opened:
                backend.close()
            raise
        return opened, True

    def create_branch(self, branch: str, parent: str, *, target: bool = False) -> None:
        self.backend.create_branch(branch, parent)
        self.registry.add(branch, target=target)
        self.recorder.add("branch-create", branch=branch, parent=parent)

    def put(
        self,
        branch: str,
        indexed: IndexedDocument,
        *,
        backend: Any | None = None,
    ) -> None:
        self.track(indexed)
        active_backend = self.backend if backend is None else backend
        self.registry.begin_write(branch)
        try:
            active_backend.put_document(
                branch,
                indexed,
                operation_id=f"put-{branch}-{indexed.document.id}-{time.time_ns()}",
            )
            self.recorder.add(
                "document-put", branch=branch, document_id=indexed.document.id
            )
        finally:
            self.registry.end_write(branch)

    def write_file(
        self,
        branch: str,
        path: str,
        content: bytes,
        *,
        backend: Any | None = None,
    ) -> None:
        active_backend = self.backend if backend is None else backend
        self.registry.begin_write(branch)
        try:
            active_backend.write_file(
                branch,
                path,
                content,
                operation_id=f"file-{branch}-{time.time_ns()}",
            )
            self.recorder.add("file-write", branch=branch, path=path)
        finally:
            self.registry.end_write(branch)

    def preview(
        self,
        source: str,
        target: str,
        *,
        backend: Any | None = None,
    ) -> dict[str, Any]:
        active_backend = self.backend if backend is None else backend
        return active_backend.merge_preview(source, target)

    def merge(
        self,
        source: str,
        target: str,
        *,
        selected: Sequence[str] | None = None,
        policy: str | None = None,
        retries: int = 0,
        backend: Any | None = None,
    ) -> dict[str, Any]:
        active_backend = self.backend if backend is None else backend
        last_error: BaseException | None = None
        for attempt in range(retries + 1):
            backoff_seconds = _retry_backoff_seconds(attempt)
            if backoff_seconds:
                time.sleep(backoff_seconds)
            preview = self.preview(source, target, backend=active_backend)
            change_ids = (
                _all_change_ids(preview) if selected is None else list(selected)
            )
            kwargs: dict[str, Any] = {
                "operation_id": f"merge-{source}-{target}-{time.time_ns()}",
                "selected_change_ids": change_ids,
                "preview_token": preview.get("preview_token"),
            }
            if policy is not None:
                kwargs["policy"] = policy
            started = _now()
            self.state.set(f"merge:{source}->{target}")
            try:
                result = active_backend.merge(source, target, **kwargs)
            except BaseException as exc:
                last_error = exc
                self.recorder.add(
                    "merge-error",
                    source=source,
                    target=target,
                    attempt=attempt,
                    backoff_ms=backoff_seconds * 1000.0,
                    error=repr(exc),
                )
                text = str(exc).lower()
                if attempt < retries and any(
                    token in text
                    for token in (
                        "stale",
                        "advanced",
                        "changed after",
                        "preview",
                        "transaction",
                        "commit",
                        "database is locked",
                        "busy",
                        "in progress",
                        "invalid literal",
                    )
                ):
                    continue
                raise
            else:
                self.recorder.add(
                    "merge",
                    source=source,
                    target=target,
                    duration_ms=(_now() - started) * 1000.0,
                    attempt=attempt,
                    backoff_ms=backoff_seconds * 1000.0,
                    selected_change_ids=change_ids,
                    result=result,
                )
                return result
        assert last_error is not None
        raise last_error


def _bundle_ids(preview: Mapping[str, Any], document_id: str) -> list[str]:
    groups = preview.get("selection_groups") or {}
    indexed = groups.get("indexed_documents") or {}
    if document_id in indexed:
        return [str(value) for value in indexed[document_id]]
    # A native backend may expose a flat change description.  Keep the
    # fallback conservative: select only entries that name this document.
    selected: list[str] = []
    for key in ("changes", "conflicts"):
        for item in preview.get(key, ()) or ():
            if not isinstance(item, Mapping):
                continue
            raw = json.dumps(item, sort_keys=True)
            if document_id in raw:
                value = item.get("change_id") or item.get("id")
                if value is not None:
                    selected.append(str(value))
    return sorted(set(selected))


def _all_change_ids(preview: Mapping[str, Any]) -> list[str]:
    """Return all selectable IDs across store-specific preview payloads."""

    values = {str(value) for value in (preview.get("change_ids") or ())}
    groups = preview.get("selection_groups") or {}
    for group in groups.values():
        if not isinstance(group, Mapping):
            continue
        for ids in group.values():
            values.update(str(value) for value in (ids or ()))
    stores = preview.get("stores") or {}
    for store in stores.values():
        if not isinstance(store, Mapping):
            continue
        for key in ("changes", "conflicts"):
            for item in store.get(key, ()) or ():
                if isinstance(item, Mapping):
                    value = item.get("change_id") or item.get("id")
                    if value is not None:
                        values.add(str(value))
    return sorted(values)


def _check_document_now(backend: Any, branch: str, spec: DocumentSpec) -> list[str]:
    errors: list[str] = []
    try:
        indexed = backend.get_document(branch, spec.document_id)
    except BaseException as exc:
        return [f"{branch}/{spec.document_id}: get_document: {exc!r}"]
    try:
        path_id = backend.find_document_id_by_path(branch, spec.path)
    except BaseException as exc:
        errors.append(f"{branch}/{spec.document_id}: path lookup: {exc!r}")
        path_id = None
    if indexed is None:
        if path_id is not None:
            errors.append(
                f"{branch}/{spec.document_id}: path has orphan id {path_id!r}"
            )
        try:
            backend.read_file(branch, spec.path)
        except BaseException as exc:
            if not _safe_not_found(exc):
                errors.append(
                    f"{branch}/{spec.document_id}: orphan file check: {exc!r}"
                )
        else:
            errors.append(f"{branch}/{spec.document_id}: file exists without document")
        try:
            hits = backend.search(
                branch,
                spec.query_text,
                spec.query_embedding,
                limit=8,
            )
        except BaseException as exc:
            errors.append(f"{branch}/{spec.document_id}: orphan vector check: {exc!r}")
        else:
            if any(hit.document_id == spec.document_id for hit in hits):
                errors.append(
                    f"{branch}/{spec.document_id}: vector exists without document"
                )
        return errors
    if indexed.document.path != spec.path:
        errors.append(f"{branch}/{spec.document_id}: path {indexed.document.path!r}")
    if path_id != spec.document_id:
        errors.append(f"{branch}/{spec.document_id}: path index {path_id!r}")
    try:
        content = backend.read_file(branch, spec.path)
        if content != indexed.document.content.encode("utf-8"):
            errors.append(f"{branch}/{spec.document_id}: file/record mismatch")
    except BaseException as exc:
        errors.append(f"{branch}/{spec.document_id}: file read: {exc!r}")
    texts = {chunk.text for chunk in indexed.chunks}
    for chunk in indexed.chunks:
        if chunk.document_id != indexed.document.id or chunk.sha256 != content_hash(
            chunk.text
        ):
            errors.append(f"{branch}/{spec.document_id}: invalid chunk {chunk.id}")
    try:
        hits = backend.search(branch, spec.query_text, spec.query_embedding, limit=8)
    except BaseException as exc:
        errors.append(f"{branch}/{spec.document_id}: search: {exc!r}")
    else:
        for hit in hits:
            if hit.document_id == indexed.document.id and (
                hit.path != indexed.document.path or hit.text not in texts
            ):
                errors.append(f"{branch}/{spec.document_id}: vector payload mismatch")
    return errors


def _check_required_document(
    backend: Any,
    branch: str,
    spec: DocumentSpec,
) -> list[str]:
    errors = _check_document_now(backend, branch, spec)
    try:
        indexed = backend.get_document(branch, spec.document_id)
    except BaseException:
        return errors
    if indexed is None:
        errors.append(f"{branch}/{spec.document_id}: required document is missing")
    return errors


def _seed(ctx: ExperimentContext) -> IndexedDocument:
    base = make_document(
        "shared-runbook",
        "/knowledge/shared/runbook.md",
        "Shared incident response runbook: verify the database, filesystem, and index together.",
        dimensions=ctx.dimensions,
        vector_index=0,
    )
    ctx.put("main", base)
    ctx.state.set("seeded")
    return base


def _start_verifier(ctx: ExperimentContext, *, interval: float) -> None:
    if not ctx.verifier_enabled:
        return
    if ctx.shared_documents is None or ctx.registry._shared is None:
        raise RuntimeError(
            "the process verifier requires manager-backed registry and document state"
        )
    process_context = mp.get_context("spawn")
    stop_event = process_context.Event()
    ready_event = process_context.Event()
    result_queue = process_context.Queue()
    started_at = _now()
    process = process_context.Process(
        target=_verifier_process_main,
        args=(
            ctx.backend_name,
            ctx.state_dir,
            ctx.dimensions,
            dict(ctx.factory_kwargs),
            ctx.registry._shared,
            ctx.shared_documents,
            ctx.state.shared_phase,
            stop_event,
            ready_event,
            result_queue,
            interval,
            ctx.verify_pairs,
        ),
        name="polystore-invariant-verifier",
        daemon=True,
    )
    process.start()
    if not ready_event.wait(timeout=max(30.0, interval * 500.0)):
        if process.is_alive():
            process.terminate()
            process.join(timeout=5.0)
        raise RuntimeError("verifier process did not become ready")
    ctx.verifier_started_at = started_at
    ctx.verifier = VerifierProcess(
        process=process,
        stop_event=stop_event,
        ready_event=ready_event,
        result_queue=result_queue,
        interval=interval,
        started_at=started_at,
        ready_at=_now(),
    )


def _stop_verifier(ctx: ExperimentContext) -> dict[str, Any]:
    if ctx.verifier is None:
        return {"checks": 0, "violation_count": 0, "violations": []}
    summary = ctx.verifier.stop()
    ctx.verifier = None
    return summary


def run_disjoint(ctx: ExperimentContext, agents: int) -> dict[str, Any]:
    ctx.state.set("branching")
    branches = [f"disjoint/agent-{index:02d}" for index in range(agents)]
    docs = [
        make_document(
            f"disjoint-{index:02d}",
            f"/knowledge/disjoint-{index:02d}.md",
            f"Reviewed disjoint promotion {index}: the approved mitigation is isolated.",
            dimensions=ctx.dimensions,
            vector_index=index + 1,
        )
        for index in range(agents)
    ]
    # Seed each path before forking.  The branches then update disjoint
    # existing records, so the experiment does not accidentally measure a
    # shared-directory inode conflict instead of independent promotions.
    initial_docs = [
        make_document(
            f"disjoint-{index:02d}",
            f"/knowledge/disjoint-{index:02d}.md",
            f"Baseline disjoint record {index}.",
            dimensions=ctx.dimensions,
            vector_index=index + 1,
        )
        for index in range(agents)
    ]
    for initial in initial_docs:
        ctx.put("main", initial)
    for branch in branches:
        ctx.create_branch(branch, "main")
    for doc in docs:
        ctx.track(doc)
    _start_verifier(ctx, interval=ctx.verify_interval)
    barrier = threading.Barrier(agents, timeout=60.0)
    worker_backends, owns_backends = ctx.worker_backends(agents)

    def worker(index: int) -> dict[str, Any]:
        branch, doc = branches[index], docs[index]
        agent_backend = worker_backends[index]
        try:
            ctx.put(branch, doc, backend=agent_backend)
            barrier.wait()
        except BaseException as exc:
            barrier.abort()
            return {"branch": branch, "status": "error", "error": repr(exc)}
        try:
            ctx.merge(branch, "main", retries=3, backend=agent_backend)
            return {"branch": branch, "status": "merged"}
        except BaseException as exc:
            return {"branch": branch, "status": "error", "error": repr(exc)}

    ctx.mark_workers_started()
    with ThreadPoolExecutor(max_workers=agents) as pool:
        results = [
            future.result()
            for future in as_completed([pool.submit(worker, i) for i in range(agents)])
        ]
    if owns_backends:
        for agent_backend in worker_backends:
            agent_backend.close()
    ctx.state.set("final-check")
    errors: list[str] = []
    for doc in docs:
        errors.extend(
            _check_required_document(
                ctx.backend,
                "main",
                DocumentSpec(
                    doc.document.id,
                    doc.document.path,
                    doc.document.title,
                    doc.chunks[0].embedding,
                ),
            )
        )
    return {"agents": agents, "workers": results, "final_errors": errors}


def run_competing(ctx: ExperimentContext, agents: int) -> dict[str, Any]:
    count = max(2, min(agents, 2))
    branches = [f"competing/candidate-{index}" for index in range(count)]
    docs = [
        make_document(
            "competing-runbook",
            "/knowledge/competing/runbook.md",
            f"Candidate {index} changes the rollback rule after reviewing a different incident.",
            dimensions=ctx.dimensions,
            vector_index=index + 1,
        )
        for index in range(count)
    ]
    for branch in branches:
        ctx.create_branch(branch, "main")
    for doc in docs:
        ctx.track(doc)
    _start_verifier(ctx, interval=ctx.verify_interval)
    write_barrier = threading.Barrier(count, timeout=60.0)
    preview_barrier = threading.Barrier(count, timeout=60.0)
    previews: dict[str, dict[str, Any]] = {}
    preview_lock = threading.Lock()
    worker_backends, owns_backends = ctx.worker_backends(count)

    def worker(index: int) -> dict[str, Any]:
        branch, doc = branches[index], docs[index]
        agent_backend = worker_backends[index]
        try:
            ctx.put(branch, doc, backend=agent_backend)
            ctx.write_file(
                branch,
                f"/tmp/competing-{index}.scratch",
                b"not for promotion",
                backend=agent_backend,
            )
            write_barrier.wait()
            preview = ctx.preview(branch, "main", backend=agent_backend)
        except BaseException as exc:
            write_barrier.abort()
            preview_barrier.abort()
            return {"branch": branch, "status": "error", "error": repr(exc)}
        with preview_lock:
            previews[branch] = preview
        try:
            preview_barrier.wait()
        except BaseException as exc:
            preview_barrier.abort()
            return {"branch": branch, "status": "error", "error": repr(exc)}
        selected = _bundle_ids(preview, doc.document.id)
        try:
            result = agent_backend.merge(
                branch,
                "main",
                operation_id=f"competing-{index}-{time.time_ns()}",
                selected_change_ids=selected,
                preview_token=preview.get("preview_token"),
                policy="source_wins",
            )
            ctx.recorder.add(
                "merge",
                source=branch,
                target="main",
                selected_change_ids=selected,
                result=result,
            )
            return {"branch": branch, "status": "merged", "selected": selected}
        except BaseException as exc:
            ctx.recorder.add(
                "merge-error", source=branch, target="main", error=repr(exc)
            )
            return {
                "branch": branch,
                "status": "rejected",
                "selected": selected,
                "error": repr(exc),
            }

    ctx.mark_workers_started()
    with ThreadPoolExecutor(max_workers=count) as pool:
        results = [
            future.result()
            for future in as_completed([pool.submit(worker, i) for i in range(count)])
        ]
    if owns_backends:
        for agent_backend in worker_backends:
            agent_backend.close()
    ctx.state.set("final-check")
    indexed = ctx.backend.get_document("main", "competing-runbook")
    final_errors: list[str] = []
    if indexed is None:
        final_errors.append("main/competing-runbook is missing")
    else:
        accepted = {doc.document.content for doc in docs}
        if indexed.document.content not in accepted:
            final_errors.append(
                "main contains a hybrid or unrecognized competing revision"
            )
        final_errors.extend(
            _check_document_now(ctx.backend, "main", ctx.documents["competing-runbook"])
        )
    for index in range(count):
        try:
            ctx.backend.read_file("main", f"/tmp/competing-{index}.scratch")
        except BaseException as exc:
            if not _safe_not_found(exc):
                final_errors.append(f"main temp artifact check failed: {exc!r}")
        else:
            final_errors.append(
                f"unselected temp artifact /tmp/competing-{index}.scratch was promoted"
            )
    return {
        "candidates": count,
        "workers": results,
        "previews": previews,
        "final_errors": final_errors,
    }


def run_recursive(ctx: ExperimentContext, agents: int) -> dict[str, Any]:
    count = max(2, agents)
    parent = "recursive/team"
    consolidation = "recursive/consolidation"
    ctx.create_branch(parent, "main", target=True)
    parent_doc = make_document(
        "recursive-parent",
        "/knowledge/recursive-team.md",
        "Team-level guidance before fan-out.",
        dimensions=ctx.dimensions,
        vector_index=1,
    )
    ctx.put(parent, parent_doc)
    ctx.create_branch(consolidation, parent, target=True)
    children = [f"recursive/team/agent-{index:02d}" for index in range(count)]
    child_docs = [
        make_document(
            f"recursive-child-{index:02d}",
            f"/knowledge/recursive-agent-{index:02d}.md",
            f"Child {index} investigated a separate slice of the incident.",
            dimensions=ctx.dimensions,
            vector_index=index + 2,
        )
        for index in range(count)
    ]
    initial_child_docs = [
        make_document(
            f"recursive-child-{index:02d}",
            f"/knowledge/recursive-agent-{index:02d}.md",
            f"Baseline child record {index}.",
            dimensions=ctx.dimensions,
            vector_index=index + 2,
        )
        for index in range(count)
    ]
    initial_parent_update = make_document(
        "recursive-parent-update",
        "/knowledge/recursive-update.md",
        "Baseline parent update record.",
        dimensions=ctx.dimensions,
        vector_index=count + 2,
    )
    # Establish all paths before fan-out.  Each child and the parent writer
    # then update an existing record, avoiding unrelated shared-directory
    # inode conflicts in the filesystem participant.
    for initial in (*initial_child_docs, initial_parent_update):
        ctx.put(consolidation, initial)
    # Children fan out from the consolidation scope.  Consequently both
    # fan-in targets (the parent and the consolidation branch) are ancestors
    # of every child, which is also the restriction imposed by native branch
    # implementations.
    for branch in children:
        ctx.create_branch(branch, consolidation)
    parent_writer = "recursive/parent-writer"
    ctx.create_branch(parent_writer, consolidation)
    parent_update = make_document(
        "recursive-parent-update",
        "/knowledge/recursive-update.md",
        "A concurrent parent update during fan-in.",
        dimensions=ctx.dimensions,
        vector_index=count + 2,
    )
    ctx.track(parent_doc)
    ctx.track(parent_update)
    for doc in child_docs:
        ctx.track(doc)
    _start_verifier(ctx, interval=ctx.verify_interval)
    # The parent writer joins the same contention window as all children.
    child_barrier = threading.Barrier(count + 1, timeout=60.0)
    worker_backends, owns_backends = ctx.worker_backends(count + 1)

    def child_worker(index: int) -> dict[str, Any]:
        branch, doc = children[index], child_docs[index]
        agent_backend = worker_backends[index]
        try:
            ctx.put(branch, doc, backend=agent_backend)
            child_barrier.wait()
        except BaseException as exc:
            child_barrier.abort()
            return {"branch": branch, "status": "error", "error": repr(exc)}
        outcome: dict[str, Any] = {"branch": branch}
        for target in (parent, consolidation):
            try:
                ctx.merge(branch, target, retries=5, backend=agent_backend)
                outcome[target] = "merged"
            except BaseException as exc:
                outcome[target] = repr(exc)
        return outcome

    def parent_worker() -> dict[str, Any]:
        agent_backend = worker_backends[count]
        try:
            ctx.put(parent_writer, parent_update, backend=agent_backend)
            child_barrier.wait()
        except BaseException as exc:
            child_barrier.abort()
            return {"branch": parent_writer, "status": "error", "error": repr(exc)}
        outcome: dict[str, Any] = {"branch": parent_writer}
        for target in (parent, consolidation):
            try:
                ctx.merge(parent_writer, target, retries=5, backend=agent_backend)
                outcome[target] = "merged"
            except BaseException as exc:
                outcome[target] = repr(exc)
        return outcome

    ctx.mark_workers_started()
    with ThreadPoolExecutor(max_workers=count + 1) as pool:
        futures = [pool.submit(child_worker, i) for i in range(count)]
        futures.append(pool.submit(parent_worker))
        results = [future.result() for future in as_completed(futures)]
    if owns_backends:
        for agent_backend in worker_backends:
            agent_backend.close()
    ctx.state.set("final-check")
    expected = [parent_doc, parent_update, *child_docs]
    final_errors: dict[str, list[str]] = {}
    for target in (parent, consolidation):
        errors: list[str] = []
        for doc in expected:
            errors.extend(
                _check_required_document(
                    ctx.backend,
                    target,
                    DocumentSpec(
                        doc.document.id,
                        doc.document.path,
                        doc.document.title,
                        doc.chunks[0].embedding,
                    ),
                )
            )
        final_errors[target] = errors
    return {
        "children": count,
        "workers": results,
        "targets": [parent, consolidation],
        "final_errors": final_errors,
    }


def _install_crash_hook(backend: Any, phase: str) -> str:
    """Install an experiment-only hard-exit hook and return its description."""

    def kill_after(original: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        value = original(*args, **kwargs)
        os._exit(137)  # noqa: PLR1722 - intentional crash experiment
        return value

    if backend.backend_name == "chronos":
        control = getattr(getattr(backend, "workspace", None), "_atomic_control", None)
        method_name = (
            "publish_branch_transaction"
            if phase == "after-publish"
            else "stage_branch_transaction_changes"
        )
        original = getattr(control, method_name, None)
        if original is None:
            raise RuntimeError(f"Chronos crash hook {method_name} is unavailable")
        setattr(control, method_name, lambda *a, **kw: kill_after(original, *a, **kw))
        return f"chronos.{method_name}"
    if backend.backend_name == "doltgres-qdrant-btrfs":
        files = getattr(backend, "_files", None)
        # Selective merges use BtrfsWorkspaceStore.write directly; the
        # unfiltered native path uses apply_paths_delta. Hook the primitive so
        # both publication paths are covered.
        original = getattr(files, "write", None)
        if original is None:
            raise RuntimeError("native Btrfs crash hook is unavailable")
        setattr(files, "write", lambda *a, **kw: kill_after(original, *a, **kw))
        return "native.write"
    qdrant = getattr(backend, "_qdrant", None)
    original = getattr(qdrant, "upsert", None)
    if original is None:
        raise RuntimeError("application-managed Qdrant crash hook is unavailable")
    setattr(qdrant, "upsert", lambda *a, **kw: kill_after(original, *a, **kw))
    return "app-managed.qdrant.upsert"


def _crash_child(
    backend_name: str,
    state_dir: str,
    dimensions: int,
    qdrant_url: str | None,
    qdrant_api_key: str | None,
    doltgres_dsn: str | None,
    btrfs_root: str | None,
    doltgres_data_dir: str | None,
    qdrant_storage_dir: str | None,
    phase: str,
) -> None:
    backend = create_knowledge_backend(
        backend_name,
        state_dir=state_dir,
        vector_dimensions=dimensions,
        qdrant_url=qdrant_url,
        qdrant_api_key=qdrant_api_key,
        doltgres_dsn=doltgres_dsn,
        btrfs_root=btrfs_root,
        doltgres_data_dir=doltgres_data_dir,
        qdrant_storage_dir=qdrant_storage_dir,
    )
    try:
        hook = _install_crash_hook(backend, phase)
        indexed = backend.get_document("crash/source", "crash-runbook")
        if indexed is None:
            raise RuntimeError("crash source document was not seeded")
        preview = backend.merge_preview("crash/source", "main")
        backend.merge(
            "crash/source",
            "main",
            operation_id=f"crash-child-{time.time_ns()}",
            selected_change_ids=_all_change_ids(preview),
            preview_token=preview.get("preview_token"),
        )
        raise RuntimeError(f"crash hook {hook} did not terminate the process")
    finally:
        backend.close()


def run_crash(
    ctx: ExperimentContext,
    dimensions: int,
    factory_kwargs: Mapping[str, Any],
    state_dir: str,
    phase: str,
    interval: float,
) -> dict[str, Any]:
    source = "crash/source"
    ctx.create_branch(source, "main")
    crash_doc = make_document(
        "crash-runbook",
        "/knowledge/crash/runbook.md",
        "A publication that may be interrupted.",
        dimensions=dimensions,
        vector_index=1,
    )
    ctx.put(source, crash_doc)
    ctx.track(crash_doc)
    _start_verifier(ctx, interval=interval)
    # The crash child and the independent verifier process run alongside the
    # parent.  The verifier continues reading the target while the publication
    # child is killed, so transient torn state is observable.
    ctx.state.set("crash-publication")
    ctx.mark_workers_started()
    process = mp.get_context("spawn").Process(
        target=_crash_child,
        args=(
            ctx.backend.backend_name,
            state_dir,
            dimensions,
            factory_kwargs.get("qdrant_url"),
            factory_kwargs.get("qdrant_api_key"),
            factory_kwargs.get("doltgres_dsn"),
            factory_kwargs.get("btrfs_root"),
            factory_kwargs.get("doltgres_data_dir"),
            factory_kwargs.get("qdrant_storage_dir"),
            phase,
        ),
    )
    process.start()
    while process.is_alive():
        time.sleep(max(0.002, interval))
    process.join()
    # Discard the parent's pre-crash connections before checking recovery.  A
    # real restart would reopen every store, and doing the same here prevents
    # cached SQLite/Dolt state from masking a torn publication.
    ctx.backend.close()
    ctx.backend = create_knowledge_backend(
        ctx.backend_name,
        state_dir=ctx.state_dir,
        vector_dimensions=ctx.dimensions,
        **dict(ctx.factory_kwargs),
    )
    ctx.state.set("post-crash-recovery")
    post_crash_errors = (
        _check_required_document(ctx.backend, "main", ctx.documents["crash-runbook"])
        if phase == "after-publish"
        else _check_document_now(ctx.backend, "main", ctx.documents["crash-runbook"])
    )
    return {
        "phase": phase,
        "child_exit_code": process.exitcode,
        "expected_crash": process.exitcode == 137,
        "post_crash_errors": post_crash_errors,
    }


def _factory_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "qdrant_url": args.qdrant_url,
        "qdrant_api_key": args.qdrant_api_key,
        "doltgres_dsn": args.doltgres_dsn,
        "btrfs_root": args.btrfs_root,
        "doltgres_data_dir": args.doltgres_data_dir,
        "qdrant_storage_dir": args.qdrant_storage_dir,
    }


def run_one(
    backend_name: str,
    scenario: str,
    state_dir: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    state_dir.mkdir(parents=True, exist_ok=True)
    kwargs = _factory_kwargs(args)
    started = _now()
    recorder = Recorder()
    process_context = mp.get_context("spawn")
    manager = process_context.Manager()
    shared_registry = {
        "branches": manager.dict({"main": True}),
        "targets": manager.dict({"main": True}),
        "active_writes": manager.dict(),
    }
    shared_documents = manager.dict()
    shared_phase = manager.dict({"phase": "initializing"})
    state = RunState(shared_phase=shared_phase)
    backend: Any | None = None
    try:
        backend = create_knowledge_backend(
            backend_name,
            state_dir=str(state_dir),
            vector_dimensions=args.dimensions,
            **kwargs,
        )
        ctx = ExperimentContext(
            backend=backend,
            backend_name=backend_name,
            state_dir=str(state_dir),
            factory_kwargs=kwargs,
            registry=BranchRegistry(shared=shared_registry),
            state=state,
            recorder=recorder,
            dimensions=args.dimensions,
            verify_interval=args.verify_interval,
            verify_pairs=args.verify_pairs,
            shared_documents=shared_documents,
        )
        try:
            _seed(ctx)
            if scenario == "disjoint":
                payload = run_disjoint(ctx, args.agents)
            elif scenario == "competing":
                payload = run_competing(ctx, args.agents)
            elif scenario == "recursive":
                payload = run_recursive(ctx, args.agents)
            elif scenario == "crash":
                payload = run_crash(
                    ctx,
                    args.dimensions,
                    kwargs,
                    str(state_dir),
                    args.crash_phase,
                    args.verify_interval,
                )
            else:
                raise ValueError(f"unknown scenario {scenario!r}")
            verifier = _stop_verifier(ctx)
            status = (
                "ok"
                if payload.get("expected_crash", True)
                and not _has_worker_failures(payload.get("workers"))
                and not _has_errors(payload.get("final_errors"))
                and not _has_errors(payload.get("post_crash_errors"))
                and not verifier.get("process_error")
                and verifier.get("publication_violation_count", 0) == 0
                else "anomaly"
            )
            return {
                "backend": backend_name,
                "scenario": scenario,
                "chronosfs_metadata_cache": "disabled",
                "state_dir": str(state_dir),
                "status": status,
                "duration_s": _now() - started,
                "payload": _json(payload),
                "verifier": verifier,
                "events": recorder.events,
            }
        except BaseException as exc:
            verifier = _stop_verifier(ctx)
            return {
                "backend": backend_name,
                "scenario": scenario,
                "chronosfs_metadata_cache": "disabled",
                "state_dir": str(state_dir),
                "status": "error",
                "duration_s": _now() - started,
                "error": repr(exc),
                "traceback": traceback.format_exc(),
                "verifier": verifier,
                "events": recorder.events,
            }
    finally:
        if backend is not None:
            backend.close()
        manager.shutdown()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", action="append", choices=BACKEND_NAMES)
    parser.add_argument("--scenario", choices=("all", *SCENARIOS), default="all")
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument(
        "--output", type=Path, default=Path("concurrent-polystore-results")
    )
    parser.add_argument("--agents", type=int, default=4)
    parser.add_argument("--dimensions", type=int, default=3)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--verify-interval", type=float, default=0.01)
    parser.add_argument("--verify-pairs", type=int, default=8)
    parser.add_argument(
        "--crash-phase", choices=("after-stage", "after-publish"), default="after-stage"
    )
    parser.add_argument("--qdrant-url")
    parser.add_argument("--qdrant-api-key")
    parser.add_argument("--doltgres-dsn")
    parser.add_argument("--btrfs-root")
    parser.add_argument("--doltgres-data-dir")
    parser.add_argument("--qdrant-storage-dir")
    parser.add_argument("--allow-local-qdrant", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    # These experiments intentionally race independent processes and a
    # separate verifier.  Disable native and FUSE metadata caches so reads do
    # not reuse a path/inode result across a publication boundary.
    os.environ["CHRONOSFS_DISABLE_METADATA_CACHE"] = "1"
    backends = args.backend or ["chronos", "app-managed"]
    scenarios = list(SCENARIOS if args.scenario == "all" else [args.scenario])
    if len(backends) > 1 and not args.qdrant_url and not args.allow_local_qdrant:
        raise SystemExit(
            "multi-backend runs require --qdrant-url for the same Docker Qdrant "
            "service (or explicitly pass --allow-local-qdrant for a local test)"
        )
    if "crash" in scenarios and not args.qdrant_url:
        raise SystemExit(
            "the crash experiment requires --qdrant-url so the parent and crash "
            "child can open the same Qdrant service"
        )
    if args.agents < 2:
        raise SystemExit("--agents must be at least 2")
    args.output.mkdir(parents=True, exist_ok=True)
    all_results: list[dict[str, Any]] = []
    for repetition in range(args.repetitions):
        for backend_name in backends:
            for scenario in scenarios:
                if args.state_dir is None:
                    state_dir = Path(
                        tempfile.mkdtemp(
                            prefix=f"concurrent-{backend_name}-{scenario}-"
                        )
                    )
                else:
                    state_dir = (
                        args.state_dir
                        / backend_name
                        / scenario
                        / f"rep-{repetition:02d}"
                    )
                result = run_one(backend_name, scenario, state_dir, args)
                result["repetition"] = repetition
                all_results.append(result)
                path = (
                    args.output / f"{backend_name}-{scenario}-rep-{repetition:02d}.json"
                )
                path.write_text(
                    json.dumps(result, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                print(f"{backend_name:24s} {scenario:10s} {result['status']}")
    summary = {
        "backends": backends,
        "scenarios": scenarios,
        "results": all_results,
        "generated_at": _now(),
    }
    (args.output / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0 if all(item["status"] != "error" for item in all_results) else 1


if __name__ == "__main__":
    sys.exit(main())
