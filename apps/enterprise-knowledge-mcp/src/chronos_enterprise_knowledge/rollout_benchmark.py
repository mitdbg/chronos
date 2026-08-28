"""Cross-backend benchmark orchestration for normalized Codex rollouts."""

from __future__ import annotations

import hashlib
import json
import shutil
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from chronos_enterprise_knowledge.backends import create_knowledge_backend
from chronos_enterprise_knowledge.embedding import (
    CacheOnlyEmbedder,
    EmbeddingCache,
    ZeroEmbedder,
)
from chronos_enterprise_knowledge.models import canonical_json
from chronos_enterprise_knowledge.rollout_trace import (
    WorkloadReplayer,
    WorkloadTrace,
)
from chronos_enterprise_knowledge.service import KnowledgeService
from chronos_enterprise_knowledge.snapshot import (
    EmbeddingSnapshot,
    ingest_snapshot,
)


@dataclass(frozen=True)
class BenchmarkRun:
    trace_id: str
    backend: str
    repetition: int
    state_dir: str
    seed: dict[str, Any]
    baseline_digests: dict[str, str]
    replay: dict[str, Any]
    final_digests: dict[str, str]
    storage: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "backend": self.backend,
            "repetition": self.repetition,
            "state_dir": self.state_dir,
            "seed": self.seed,
            "baseline_digests": self.baseline_digests,
            "replay": self.replay,
            "final_digests": self.final_digests,
            "storage": self.storage,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> BenchmarkRun:
        return cls(
            trace_id=str(value["trace_id"]),
            backend=str(value["backend"]),
            repetition=int(value["repetition"]),
            state_dir=str(value["state_dir"]),
            seed=dict(value["seed"]),
            baseline_digests=dict(value["baseline_digests"]),
            replay=dict(value["replay"]),
            final_digests=dict(value["final_digests"]),
            storage=dict(value["storage"]),
        )


@dataclass(frozen=True)
class BenchmarkReport:
    snapshot: str
    embedding_cache: str
    backends: tuple[str, ...]
    traces: tuple[str, ...]
    require_result_match: bool
    runs: tuple[BenchmarkRun, ...]
    comparisons: tuple[dict[str, Any], ...]

    @property
    def matched(self) -> bool:
        return (
            all(run.replay.get("succeeded") for run in self.runs)
            and all(comparison["matched"] for comparison in self.comparisons)
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "snapshot": self.snapshot,
            "embedding_cache": self.embedding_cache,
            "backends": list(self.backends),
            "traces": list(self.traces),
            "require_result_match": self.require_result_match,
            "matched": self.matched,
            "runs": [run.as_dict() for run in self.runs],
            "comparisons": list(self.comparisons),
        }

    def write(self, path: str | Path) -> Path:
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            json.dumps(self.as_dict(), indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        return destination


class RolloutBenchmark:
    """Seed identical logical states and replay traces across backends."""

    def __init__(
        self,
        *,
        snapshot_dir: str | Path,
        embedding_cache: str | Path,
        output_dir: str | Path,
        repo_dir: str | Path,
        backends: tuple[str, ...] = (
            "chronos",
            "app-managed",
            "physical-clone",
        ),
        max_documents: int | None = None,
        repetitions: int = 1,
        allow_shell: bool = False,
        keep_states: bool = True,
        require_result_match: bool = False,
        resume: bool = False,
        qdrant_url: str | None = None,
        qdrant_api_key: str | None = None,
        doltgres_dsn: str | None = None,
        btrfs_root: str | Path | None = None,
        doltgres_data_dir: str | Path | None = None,
        qdrant_storage_dir: str | Path | None = None,
        chronos_postgres_dsn: str | None = None,
        chronos_postgres_data_dir: str | Path | None = None,
        force_zero_embeddings: bool = False,
        progress: Callable[[dict[str, Any]], None] | None = None,
    ):
        self.snapshot_dir = Path(snapshot_dir).expanduser().resolve()
        self.embedding_cache_path = Path(embedding_cache).expanduser().resolve()
        self.output_dir = Path(output_dir).expanduser().resolve()
        self.repo_dir = Path(repo_dir).expanduser().resolve()
        self.backends = tuple(backends)
        self.max_documents = max_documents
        self.repetitions = int(repetitions)
        self.allow_shell = allow_shell
        self.keep_states = keep_states
        self.require_result_match = require_result_match
        self.resume = resume
        self.qdrant_url = qdrant_url
        self.qdrant_api_key = qdrant_api_key
        self.doltgres_dsn = doltgres_dsn
        self.btrfs_root = (
            Path(btrfs_root).expanduser().resolve()
            if btrfs_root is not None
            else None
        )
        self.doltgres_data_dir = (
            Path(doltgres_data_dir).expanduser().resolve()
            if doltgres_data_dir is not None
            else None
        )
        self.qdrant_storage_dir = (
            Path(qdrant_storage_dir).expanduser().resolve()
            if qdrant_storage_dir is not None
            else None
        )
        self.chronos_postgres_dsn = chronos_postgres_dsn
        self.chronos_postgres_data_dir = (
            Path(chronos_postgres_data_dir).expanduser().resolve()
            if chronos_postgres_data_dir is not None
            else None
        )
        self.force_zero_embeddings = force_zero_embeddings
        self.progress = progress
        if self.repetitions <= 0:
            raise ValueError("repetitions must be positive")
        if not self.embedding_cache_path.is_file():
            raise FileNotFoundError(self.embedding_cache_path)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def run(self, traces: list[WorkloadTrace]) -> BenchmarkReport:
        snapshot = EmbeddingSnapshot(self.snapshot_dir)
        try:
            dimensions = snapshot.dimensions
            model = str(snapshot.metadata()["spec"]["embedding_model"])
            runs: list[BenchmarkRun] = []
            for trace in traces:
                for repetition in range(self.repetitions):
                    for backend_name in self.backends:
                        fingerprint = self._run_fingerprint(
                            trace,
                            backend_name=backend_name,
                            repetition=repetition,
                            snapshot=snapshot,
                            dimensions=dimensions,
                            embedding_model=model,
                        )
                        checkpoint = self._checkpoint_path(
                            trace.trace_id,
                            backend_name,
                            repetition,
                        )
                        if self.resume and checkpoint.is_file():
                            run = self._load_checkpoint(checkpoint, fingerprint)
                            runs.append(run)
                            self._emit(
                                phase="run",
                                status="resumed",
                                trace_id=trace.trace_id,
                                backend=backend_name,
                                repetition=repetition,
                            )
                            continue
                        run_root = self._run_root(
                            trace.trace_id,
                            backend_name,
                            repetition,
                        )
                        if self.resume and run_root.exists():
                            # A missing checkpoint marks an interrupted run.
                            # Explicit resume mode authorizes rebuilding only
                            # that benchmark-owned partial state.
                            shutil.rmtree(run_root)
                        self._emit(
                            phase="run",
                            status="started",
                            trace_id=trace.trace_id,
                            backend=backend_name,
                            repetition=repetition,
                        )
                        run = self._run_one(
                            trace,
                            backend_name=backend_name,
                            repetition=repetition,
                            snapshot=snapshot,
                            dimensions=dimensions,
                            embedding_model=model,
                        )
                        self._write_checkpoint(checkpoint, fingerprint, run)
                        runs.append(run)
                        self._emit(
                            phase="run",
                            status="completed",
                            trace_id=trace.trace_id,
                            backend=backend_name,
                            repetition=repetition,
                        )
        finally:
            snapshot.close()
        comparisons = self._compare(traces, runs)
        return BenchmarkReport(
            str(self.snapshot_dir),
            str(self.embedding_cache_path),
            self.backends,
            tuple(trace.trace_id for trace in traces),
            self.require_result_match,
            tuple(runs),
            tuple(comparisons),
        )

    def _run_one(
        self,
        trace: WorkloadTrace,
        *,
        backend_name: str,
        repetition: int,
        snapshot: EmbeddingSnapshot,
        dimensions: int,
        embedding_model: str,
    ) -> BenchmarkRun:
        run_root = self._run_root(
            trace.trace_id,
            backend_name,
            repetition,
        )
        if run_root.exists():
            raise FileExistsError(
                f"benchmark state already exists; choose a new output directory: "
                f"{run_root}"
            )
        run_root.mkdir(parents=True)
        run_doltgres_dsn = self.doltgres_dsn
        database_name: str | None = None
        if backend_name == "doltgres-qdrant-btrfs":
            if not self.doltgres_dsn:
                raise ValueError(
                    "the native branching baseline requires doltgres_dsn"
                )
            database_name = _benchmark_database_name(run_root)
            run_doltgres_dsn = _create_benchmark_database(
                self.doltgres_dsn,
                database_name,
                replace=self.resume,
            )
        backend = create_knowledge_backend(
            backend_name,  # type: ignore[arg-type]
            run_root,
            vector_dimensions=dimensions,
            qdrant_url=self.qdrant_url,
            qdrant_api_key=self.qdrant_api_key,
            doltgres_dsn=run_doltgres_dsn,
            btrfs_root=self.btrfs_root,
            doltgres_data_dir=self.doltgres_data_dir,
            qdrant_storage_dir=self.qdrant_storage_dir,
            chronos_postgres_dsn=self.chronos_postgres_dsn,
            chronos_postgres_data_dir=self.chronos_postgres_data_dir,
        )
        cache = EmbeddingCache(self.embedding_cache_path)
        try:
            seed_started = time.perf_counter_ns()
            seed_spec = dict(trace.metadata.get("seed") or {})
            required_documents = [
                str(identifier)
                for identifier in seed_spec.get("document_ids") or ()
            ]
            selected = snapshot.selected_document_ids(
                required=required_documents,
                max_documents=self.max_documents,
            )
            selected_documents = (
                len(selected)
                if selected is not None
                else int(snapshot.metadata().get("selected_documents", 0))
            )
            last_seed_progress = 0

            def report_seed(stats: Any) -> None:
                nonlocal last_seed_progress
                if stats.documents - last_seed_progress < 1_000:
                    return
                last_seed_progress = stats.documents
                self._emit(
                    phase="seed",
                    status="running",
                    trace_id=trace.trace_id,
                    backend=backend_name,
                    repetition=repetition,
                    documents=stats.documents,
                    total_documents=selected_documents,
                    chunks=stats.chunks,
                    bytes=stats.bytes,
                )

            stats = ingest_snapshot(
                snapshot,
                backend,
                "main",
                batch_size=32,
                document_ids=selected,
                force_zero_embeddings=self.force_zero_embeddings,
                progress=report_seed,
            )
            self._emit(
                phase="seed",
                status="completed",
                trace_id=trace.trace_id,
                backend=backend_name,
                repetition=repetition,
                documents=stats.documents,
                total_documents=selected_documents,
                chunks=stats.chunks,
                bytes=stats.bytes,
            )
            branches = list(seed_spec.get("branches") or ())
            for branch in branches:
                backend.create_branch(
                    str(branch["branch_id"]),
                    str(branch["parent_branch"]),
                    dict(branch.get("metadata") or {}),
                )
            seed_elapsed = time.perf_counter_ns() - seed_started
            baseline_branches = [
                str(branch)
                for branch in (
                    trace.metadata.get("baseline_branches")
                    or trace.metadata.get("verify_branches")
                    or ["main"]
                )
                if str(branch) in backend.list_branches()
            ]
            baseline = {
                branch: backend.state_digest(branch)
                for branch in baseline_branches
            }
            storage_stats = getattr(backend, "storage_stats", None)
            baseline_storage = (
                storage_stats() if callable(storage_stats) else {}
            )
            embedder = (
                ZeroEmbedder(
                    dimensions,
                    model=embedding_model,
                )
                if self.force_zero_embeddings
                else CacheOnlyEmbedder(
                    cache,
                    model=embedding_model,
                    dimensions=dimensions,
                )
            )
            service = KnowledgeService(backend, embedder)
            replay = WorkloadReplayer(
                service,
                repo_dir=self.repo_dir,
                allow_shell=self.allow_shell,
            ).replay(trace)
            verify_branches = [
                str(branch)
                for branch in trace.metadata.get("verify_branches") or ["main"]
            ]
            current_branches = set(backend.list_branches())
            final = {
                branch: (
                    backend.state_digest(branch)
                    if branch in current_branches
                    else "<missing>"
                )
                for branch in verify_branches
            }
            final_storage = (
                storage_stats() if callable(storage_stats) else {}
            )
            storage = _storage_comparison(
                baseline_storage,
                final_storage,
            )
            run = BenchmarkRun(
                trace.trace_id,
                backend_name,
                repetition,
                str(run_root),
                {
                    "documents": stats.documents,
                    "chunks": stats.chunks,
                    "bytes": stats.bytes,
                    "branches": len(branches) + 1,
                    "elapsed_ms": seed_elapsed / 1_000_000,
                    "selection_digest": _selection_digest(
                        selected,
                        snapshot.metadata(),
                    ),
                    "required_document_ids": required_documents,
                    "embedding_mode": (
                        "forced-zero"
                        if self.force_zero_embeddings
                        else "snapshot"
                    ),
                },
                baseline,
                replay.as_dict(),
                final,
                storage,
            )
        finally:
            cache.close()
            try:
                if not self.keep_states:
                    destroy = getattr(backend, "destroy", None)
                    if callable(destroy):
                        destroy()
            finally:
                try:
                    backend.close()
                finally:
                    if database_name is not None and not self.keep_states:
                        _drop_benchmark_database(
                            self.doltgres_dsn or "",
                            database_name,
                        )
        if not self.keep_states:
            shutil.rmtree(run_root)
        return run

    def _run_root(
        self,
        trace_id: str,
        backend_name: str,
        repetition: int,
    ) -> Path:
        return (
            self.output_dir
            / "states"
            / _safe_name(trace_id)
            / f"repeat-{repetition:03d}"
            / backend_name
        )

    def _checkpoint_path(
        self,
        trace_id: str,
        backend_name: str,
        repetition: int,
    ) -> Path:
        return (
            self.output_dir
            / "checkpoints"
            / _safe_name(trace_id)
            / f"repeat-{repetition:03d}"
            / f"{backend_name}.json"
        )

    def _run_fingerprint(
        self,
        trace: WorkloadTrace,
        *,
        backend_name: str,
        repetition: int,
        snapshot: EmbeddingSnapshot,
        dimensions: int,
        embedding_model: str,
    ) -> str:
        value = {
            "trace_id": trace.trace_id,
            "trace_metadata": trace.metadata,
            "events": [event.as_dict() for event in trace.events],
            "backend": backend_name,
            "repetition": repetition,
            "snapshot": str(self.snapshot_dir),
            "snapshot_metadata": snapshot.metadata(),
            "dimensions": dimensions,
            "embedding_model": embedding_model,
            "embedding_cache": str(self.embedding_cache_path),
            "max_documents": self.max_documents,
            "allow_shell": self.allow_shell,
            "keep_states": self.keep_states,
            "require_result_match": self.require_result_match,
            "qdrant_url": self.qdrant_url,
            "doltgres_dsn": self.doltgres_dsn,
            "chronos_postgres_dsn": self.chronos_postgres_dsn,
            "btrfs_root": (
                str(self.btrfs_root) if self.btrfs_root is not None else None
            ),
            "doltgres_data_dir": (
                str(self.doltgres_data_dir)
                if self.doltgres_data_dir is not None
                else None
            ),
            "qdrant_storage_dir": (
                str(self.qdrant_storage_dir)
                if self.qdrant_storage_dir is not None
                else None
            ),
            "chronos_postgres_data_dir": (
                str(self.chronos_postgres_data_dir)
                if self.chronos_postgres_data_dir is not None
                else None
            ),
            "force_zero_embeddings": self.force_zero_embeddings,
        }
        return hashlib.sha256(canonical_json(value).encode()).hexdigest()

    @staticmethod
    def _load_checkpoint(path: Path, fingerprint: str) -> BenchmarkRun:
        value = json.loads(path.read_text(encoding="utf-8"))
        if (
            value.get("schema_version") != 1
            or value.get("fingerprint") != fingerprint
            or not isinstance(value.get("run"), Mapping)
        ):
            raise ValueError(
                f"benchmark checkpoint does not match the requested run: {path}"
            )
        return BenchmarkRun.from_dict(value["run"])

    @staticmethod
    def _write_checkpoint(
        path: Path,
        fingerprint: str,
        run: BenchmarkRun,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "fingerprint": fingerprint,
                    "run": run.as_dict(),
                },
                indent=2,
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)

    def _emit(self, **event: Any) -> None:
        if self.progress is not None:
            self.progress(event)

    def _compare(
        self,
        traces: list[WorkloadTrace],
        runs: list[BenchmarkRun],
    ) -> list[dict[str, Any]]:
        comparisons: list[dict[str, Any]] = []
        by_key = {
            (run.trace_id, run.repetition, run.backend): run for run in runs
        }
        reference_backend = self.backends[0]
        for trace in traces:
            ignored_sequences = {
                event.sequence
                for event in trace.events
                if event.expected.get("compare") is False
            }
            for repetition in range(self.repetitions):
                reference = by_key[(trace.trace_id, repetition, reference_backend)]
                reference_events = {
                    int(event["sequence"]): event
                    for event in reference.replay.get("events", [])
                }
                for backend_name in self.backends[1:]:
                    candidate = by_key[(trace.trace_id, repetition, backend_name)]
                    candidate_events = {
                        int(event["sequence"]): event
                        for event in candidate.replay.get("events", [])
                    }
                    event_mismatches = []
                    for sequence in sorted(
                        reference_events.keys() | candidate_events.keys()
                    ):
                        if sequence in ignored_sequences:
                            continue
                        reference_event = reference_events.get(sequence)
                        candidate_event = candidate_events.get(sequence)
                        if (
                            reference_event is None
                            or candidate_event is None
                            or reference_event.get("normalized_digest")
                            != candidate_event.get("normalized_digest")
                        ):
                            event_mismatches.append(sequence)
                    baseline_match = (
                        reference.baseline_digests == candidate.baseline_digests
                    )
                    final_match = reference.final_digests == candidate.final_digests
                    replay_succeeded = bool(
                        reference.replay.get("succeeded")
                        and candidate.replay.get("succeeded")
                    )
                    results_matched = not event_mismatches
                    comparisons.append(
                        {
                            "trace_id": trace.trace_id,
                            "repetition": repetition,
                            "reference_backend": reference_backend,
                            "candidate_backend": backend_name,
                            "baseline_state_matched": baseline_match,
                            "final_state_matched": final_match,
                            "replay_succeeded": replay_succeeded,
                            "results_matched": results_matched,
                            "event_mismatches": event_mismatches,
                            "matched": (
                                baseline_match
                                and final_match
                                and replay_succeeded
                                and (
                                    results_matched
                                    or not self.require_result_match
                                )
                            ),
                        }
                    )
        return comparisons


def _safe_name(value: str) -> str:
    return "".join(
        character if character.isalnum() or character in "._-" else "-"
        for character in value
    ).strip("-")


def _benchmark_database_name(run_root: Path) -> str:
    digest = hashlib.sha256(str(run_root).encode()).hexdigest()[:24]
    return f"enterprise_knowledge_{digest}"


def _create_benchmark_database(
    admin_dsn: str,
    database_name: str,
    *,
    replace: bool,
) -> str:
    with psycopg.connect(admin_dsn, autocommit=True) as connection:
        exists = connection.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (database_name,),
        ).fetchone()
        if exists is not None:
            if not replace:
                raise FileExistsError(
                    f"benchmark database already exists: {database_name}"
                )
            connection.execute(
                sql.SQL("DROP DATABASE {}").format(
                    sql.Identifier(database_name)
                )
            )
        connection.execute(
            sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(database_name)
            )
        )
    parameters = conninfo_to_dict(admin_dsn)
    parameters["dbname"] = database_name
    return make_conninfo(**parameters)


def _drop_benchmark_database(admin_dsn: str, database_name: str) -> None:
    with psycopg.connect(admin_dsn, autocommit=True) as connection:
        connection.execute(
            sql.SQL("DROP DATABASE IF EXISTS {}").format(
                sql.Identifier(database_name)
            )
        )


def _selection_digest(
    selected: tuple[str, ...] | None,
    snapshot_metadata: dict[str, Any],
) -> str:
    value: Any = (
        list(selected)
        if selected is not None
        else {
            "all_documents": True,
            "snapshot_selection_digest": snapshot_metadata.get(
                "selection_digest"
            ),
        }
    )
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def _storage_comparison(
    baseline: dict[str, Any],
    final: dict[str, Any],
) -> dict[str, Any]:
    delta = {
        key: final[key] - baseline[key]
        for key in sorted(baseline.keys() & final.keys())
        if isinstance(baseline[key], (int, float))
        and isinstance(final[key], (int, float))
    }
    baseline_bytes = int(baseline.get("total_state_bytes", 0))
    final_bytes = int(final.get("total_state_bytes", 0))
    added_branches = int(final.get("branches", 0)) - int(
        baseline.get("branches", 0)
    )
    return {
        "baseline": baseline,
        "final": final,
        "delta": delta,
        "state_size_ratio": (
            final_bytes / baseline_bytes if baseline_bytes else None
        ),
        "bytes_per_added_branch": (
            (final_bytes - baseline_bytes) / added_branches
            if added_branches > 0
            else None
        ),
    }


__all__ = [
    "BenchmarkReport",
    "BenchmarkRun",
    "RolloutBenchmark",
]
