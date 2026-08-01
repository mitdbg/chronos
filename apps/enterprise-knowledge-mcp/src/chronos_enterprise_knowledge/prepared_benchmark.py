"""Independent full-root benchmarks for captured Codex workflows."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from chronos_enterprise_knowledge.backends import create_knowledge_backend
from chronos_enterprise_knowledge.embedding import ZeroEmbedder
from chronos_enterprise_knowledge.models import (
    canonical_json,
    content_hash,
    indexed_document_digest,
)
from chronos_enterprise_knowledge.rollout_benchmark import (
    BenchmarkReport,
    BenchmarkRun,
)
from chronos_enterprise_knowledge.rollout_trace import (
    WorkloadReplayer,
    WorkloadTrace,
)
from chronos_enterprise_knowledge.service import KnowledgeService


class PreparedRootBenchmark:
    """Replay each workflow from a reflinked, fully ingested backend root.

    The prepared root is copied with filesystem CoW before each repetition.
    Reset time is recorded but excluded from workload replay latency and
    storage deltas. Setup traces run before the baseline storage measurement,
    allowing dependent workflows to remain separate experiments.
    """

    def __init__(
        self,
        *,
        snapshot_manifest: (
            str | Path | Sequence[str | Path]
        ),
        base_states: Mapping[str, str | Path],
        work_root: str | Path,
        output_root: str | Path,
        repo_dir: str | Path,
        dimensions: int,
        embedding_model: str,
        repetitions: int = 3,
        allow_shell: bool = True,
        max_shell_interrupt_seconds: float | None = None,
        require_result_match: bool = False,
        backend_options: Mapping[str, Mapping[str, Any]] | None = None,
        in_place_backends: Sequence[str] = (),
    ):
        manifest_values = (
            (snapshot_manifest,)
            if isinstance(snapshot_manifest, (str, Path))
            else tuple(snapshot_manifest)
        )
        if not manifest_values:
            raise ValueError("at least one snapshot manifest is required")
        self.snapshot_manifests = tuple(
            Path(value).resolve() for value in manifest_values
        )
        manifests = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in self.snapshot_manifests
        ]
        self.manifest = _combined_manifest(
            self.snapshot_manifests,
            manifests,
        )
        self.snapshot_description = ";".join(
            str(path.parent) for path in self.snapshot_manifests
        )
        self.base_states = {
            str(backend): Path(path).resolve()
            for backend, path in base_states.items()
        }
        self.work_root = Path(work_root).resolve()
        self.output_root = Path(output_root).resolve()
        self.repo_dir = Path(repo_dir).resolve()
        self.dimensions = int(dimensions)
        self.embedding_model = str(embedding_model)
        self.repetitions = int(repetitions)
        self.run_namespace = f"run-{os.getpid()}-{time.time_ns()}"
        self.allow_shell = bool(allow_shell)
        self.max_shell_interrupt_seconds = (
            None
            if max_shell_interrupt_seconds is None
            else float(max_shell_interrupt_seconds)
        )
        if (
            self.max_shell_interrupt_seconds is not None
            and self.max_shell_interrupt_seconds <= 0
        ):
            raise ValueError(
                "max_shell_interrupt_seconds must be positive"
            )
        self.require_result_match = bool(require_result_match)
        self.backend_options = {
            str(backend): dict(options)
            for backend, options in (backend_options or {}).items()
        }
        self.in_place_backends = frozenset(
            str(backend) for backend in in_place_backends
        )
        if self.repetitions <= 0:
            raise ValueError("repetitions must be positive")
        unknown_options = set(self.backend_options) - set(self.base_states)
        if unknown_options:
            raise ValueError(
                "backend options provided for unknown backends: "
                f"{sorted(unknown_options)}"
            )
        unknown_in_place = self.in_place_backends - set(self.base_states)
        if unknown_in_place:
            raise ValueError(
                "in-place mode requested for unknown backends: "
                f"{sorted(unknown_in_place)}"
            )
        for manifest in manifests:
            if int(manifest["documents"]) != int(
                manifest["selected_documents"]
            ):
                raise ValueError(
                    "prepared-root benchmark requires complete snapshots"
                )
        for backend, base in self.base_states.items():
            if not base.is_dir():
                raise FileNotFoundError(
                    f"prepared state for {backend} does not exist: {base}"
                )
        self.work_root.mkdir(parents=True, exist_ok=True)
        self.output_root.mkdir(parents=True, exist_ok=True)

    def run_workflow(
        self,
        trace: WorkloadTrace,
        *,
        setup_traces: Sequence[WorkloadTrace] = (),
    ) -> BenchmarkReport:
        if self.in_place_backends:
            raise ValueError(
                "in-place backends require shared-root sequence mode"
            )
        output = self.output_root / _safe_name(trace.trace_id)
        output.mkdir(parents=True, exist_ok=True)
        runs: list[BenchmarkRun] = []
        backends = tuple(self.base_states)
        for repetition in range(self.repetitions):
            for backend_name, base_state in self.base_states.items():
                run_state = (
                    self.work_root
                    / self.run_namespace
                    / _safe_name(trace.trace_id)
                    / f"repeat-{repetition:03d}"
                    / backend_name
                )
                checkpoint = (
                    output
                    / "checkpoints"
                    / f"repeat-{repetition:03d}-{backend_name}.json"
                )
                if checkpoint.is_file():
                    runs.append(
                        BenchmarkRun.from_dict(
                            json.loads(
                                checkpoint.read_text(encoding="utf-8")
                            )
                        )
                    )
                    continue
                if run_state.exists():
                    shutil.rmtree(run_state)
                reset_started = time.perf_counter_ns()
                _reflink_tree(base_state, run_state)
                reset_elapsed = time.perf_counter_ns() - reset_started
                try:
                    run = self._run_one(
                        trace,
                        setup_traces=setup_traces,
                        backend_name=backend_name,
                        repetition=repetition,
                        run_state=run_state,
                        reset_elapsed_ns=reset_elapsed,
                    )
                finally:
                    shutil.rmtree(run_state, ignore_errors=True)
                checkpoint.parent.mkdir(parents=True, exist_ok=True)
                checkpoint.write_text(
                    json.dumps(run.as_dict(), indent=2) + "\n",
                    encoding="utf-8",
                )
                runs.append(run)
        comparisons = _compare_runs(
            trace,
            runs,
            backends=backends,
            repetitions=self.repetitions,
            require_result_match=self.require_result_match,
        )
        report = BenchmarkReport(
            snapshot=self.snapshot_description,
            embedding_cache="<forced-zero>",
            backends=backends,
            traces=(trace.trace_id,),
            require_result_match=self.require_result_match,
            runs=tuple(runs),
            comparisons=tuple(comparisons),
        )
        report.write(output / "results.json")
        return report

    def run_sequence(
        self,
        traces: Sequence[WorkloadTrace],
    ) -> list[BenchmarkReport]:
        """Replay workflows in order from one prepared root per backend.

        Each backend/repetition starts from a single CoW copy of the fully
        ingested root. Workflows are then replayed sequentially over that same
        state, with timing and storage deltas measured separately for each
        workflow. This is useful for full-corpus experiments where repeatedly
        resetting the root dominates the benchmark wall-clock time.
        """

        if not traces:
            return []
        runs_by_trace = {
            trace.trace_id: [] for trace in traces
        }
        backends = tuple(self.base_states)
        for repetition in range(self.repetitions):
            for backend_name, base_state in self.base_states.items():
                in_place = backend_name in self.in_place_backends
                run_state = (
                    base_state
                    if in_place
                    else (
                        self.work_root
                        / self.run_namespace
                        / "shared-sequence"
                        / f"repeat-{repetition:03d}"
                        / backend_name
                    )
                )
                checkpoint_dir = (
                    self.output_root
                    / "sequence-checkpoints"
                    / f"repeat-{repetition:03d}-{backend_name}"
                )
                resume_slot = (
                    self.work_root
                    / "sequence-resume"
                    / f"repeat-{repetition:03d}-{backend_name}"
                )
                complete_marker = checkpoint_dir / ".complete"
                if complete_marker.is_file():
                    shutil.rmtree(resume_slot, ignore_errors=True)
                    for trace in traces:
                        checkpoint = checkpoint_dir / (
                            f"{_safe_name(trace.trace_id)}.json"
                        )
                        runs_by_trace[trace.trace_id].append(
                            BenchmarkRun.from_dict(
                                json.loads(
                                    checkpoint.read_text(encoding="utf-8")
                                )
                            )
                        )
                    continue
                if in_place:
                    resume_position = _in_place_resume_position(
                        checkpoint_dir,
                        traces=traces,
                    )
                    start_position = (
                        0 if resume_position is None else resume_position + 1
                    )
                    reset_elapsed = 0
                else:
                    resume_position = _load_sequence_resume_position(
                        resume_slot,
                        traces=traces,
                        backend_name=backend_name,
                        repetition=repetition,
                        selection_digest=str(
                            self.manifest["selection_digest"]
                        ),
                    )
                    if run_state.exists():
                        shutil.rmtree(run_state)
                    reset_started = time.perf_counter_ns()
                    if resume_position is None:
                        _reflink_tree(base_state, run_state)
                        start_position = 0
                    else:
                        _reflink_tree(resume_slot / "state", run_state)
                        start_position = resume_position + 1
                    reset_elapsed = time.perf_counter_ns() - reset_started
                for trace in traces[:start_position]:
                    checkpoint = checkpoint_dir / (
                        f"{_safe_name(trace.trace_id)}.json"
                    )
                    if not checkpoint.is_file():
                        raise RuntimeError(
                            "sequence resume state is missing checkpoint "
                            f"{checkpoint}"
                        )
                    runs_by_trace[trace.trace_id].append(
                        BenchmarkRun.from_dict(
                            json.loads(
                                checkpoint.read_text(encoding="utf-8")
                            )
                        )
                    )
                checkpoint_dir.mkdir(parents=True, exist_ok=True)
                try:
                    backend = self._create_backend(backend_name, run_state)
                    embedder = ZeroEmbedder(
                        self.dimensions,
                        model=self.embedding_model,
                    )
                    service = KnowledgeService(backend, embedder)
                    service.start()
                    try:
                        for position, trace in enumerate(traces):
                            if position < start_position:
                                continue
                            run = self._run_sequence_trace(
                                trace,
                                backend=backend,
                                service=service,
                                backend_name=backend_name,
                                repetition=repetition,
                                run_state=run_state,
                                reset_elapsed_ns=(
                                    reset_elapsed if position == 0 else 0
                                ),
                                position=position,
                            )
                            checkpoint = checkpoint_dir / (
                                f"{_safe_name(trace.trace_id)}.json"
                            )
                            checkpoint.write_text(
                                json.dumps(run.as_dict(), indent=2) + "\n",
                                encoding="utf-8",
                            )
                            runs_by_trace[trace.trace_id].append(run)
                            if not in_place:
                                _save_sequence_resume(
                                    run_state,
                                    resume_slot,
                                    traces=traces,
                                    backend_name=backend_name,
                                    repetition=repetition,
                                    selection_digest=str(
                                        self.manifest["selection_digest"]
                                    ),
                                    completed_position=position,
                                )
                    finally:
                        backend.close()
                finally:
                    if not in_place:
                        shutil.rmtree(run_state, ignore_errors=True)
                complete_marker.write_text("complete\n", encoding="utf-8")
                if not in_place:
                    shutil.rmtree(resume_slot, ignore_errors=True)

        reports: list[BenchmarkReport] = []
        for trace in traces:
            output = self.output_root / _safe_name(trace.trace_id)
            output.mkdir(parents=True, exist_ok=True)
            runs = runs_by_trace[trace.trace_id]
            comparisons = _compare_runs(
                trace,
                runs,
                backends=backends,
                repetitions=self.repetitions,
                require_result_match=self.require_result_match,
            )
            report = BenchmarkReport(
                snapshot=self.snapshot_description,
                embedding_cache="<forced-zero>",
                backends=backends,
                traces=(trace.trace_id,),
                require_result_match=self.require_result_match,
                runs=tuple(runs),
                comparisons=tuple(comparisons),
            )
            report.write(output / "results.json")
            reports.append(report)
        return reports

    def run_isolated_workflows(
        self,
        traces: Sequence[WorkloadTrace],
        *,
        setup_traces: Mapping[str, Sequence[WorkloadTrace]] | None = None,
    ) -> list[BenchmarkReport]:
        """Replay each workflow against a fresh logical clone of its inputs.

        The fully ingested backend remains online and immutable. Before each
        workflow, every pre-existing branch referenced by that workflow (or
        its setup traces) is forked into a private namespace. The replayer
        maps all branch arguments into that namespace, and the namespace is
        deleted after the measurement. This resets external services such as
        Qdrant and Doltgres without physically copying their full state.
        """

        if not traces:
            return []
        setup_by_trace = {
            str(trace_id): tuple(values)
            for trace_id, values in (setup_traces or {}).items()
        }
        runs_by_trace = {trace.trace_id: [] for trace in traces}
        backends = tuple(self.base_states)
        for repetition in range(self.repetitions):
            for backend_name, base_state in self.base_states.items():
                backend = self._create_backend(backend_name, base_state)
                embedder = ZeroEmbedder(
                    self.dimensions,
                    model=self.embedding_model,
                )
                service = KnowledgeService(backend, embedder)
                service.start()
                pristine_branches = set(backend.list_branches())
                try:
                    for trace in traces:
                        output = self.output_root / _safe_name(trace.trace_id)
                        checkpoint = (
                            output
                            / "checkpoints"
                            / f"repeat-{repetition:03d}-{backend_name}.json"
                        )
                        if checkpoint.is_file():
                            runs_by_trace[trace.trace_id].append(
                                BenchmarkRun.from_dict(
                                    json.loads(
                                        checkpoint.read_text(encoding="utf-8")
                                    )
                                )
                            )
                            continue

                        dependencies = setup_by_trace.get(trace.trace_id, ())
                        namespace = _isolation_namespace(
                            repetition,
                            trace.trace_id,
                        )
                        _delete_isolation_namespace(backend, namespace)
                        branch_map = _prepare_isolated_branches(
                            backend,
                            (*dependencies, trace),
                            namespace=namespace,
                            pristine_branches=pristine_branches,
                        )
                        try:
                            run = self._run_isolated_trace(
                                trace,
                                setup_traces=dependencies,
                                backend=backend,
                                service=service,
                                backend_name=backend_name,
                                repetition=repetition,
                                run_state=base_state,
                                branch_map=branch_map,
                            )
                        finally:
                            release = getattr(
                                backend,
                                "release_session_checkouts",
                                None,
                            )
                            if callable(release):
                                release()
                            _delete_isolation_namespace(backend, namespace)
                        current_branches = set(backend.list_branches())
                        if current_branches != pristine_branches:
                            raise RuntimeError(
                                "workflow isolation did not restore the "
                                f"starting branch set for {trace.trace_id}: "
                                f"added={sorted(current_branches - pristine_branches)}, "
                                f"missing={sorted(pristine_branches - current_branches)}"
                            )
                        checkpoint.parent.mkdir(parents=True, exist_ok=True)
                        checkpoint.write_text(
                            json.dumps(run.as_dict(), indent=2) + "\n",
                            encoding="utf-8",
                        )
                        runs_by_trace[trace.trace_id].append(run)
                finally:
                    backend.close()

        reports: list[BenchmarkReport] = []
        for trace in traces:
            output = self.output_root / _safe_name(trace.trace_id)
            output.mkdir(parents=True, exist_ok=True)
            runs = runs_by_trace[trace.trace_id]
            comparisons = _compare_runs(
                trace,
                runs,
                backends=backends,
                repetitions=self.repetitions,
                require_result_match=self.require_result_match,
            )
            report = BenchmarkReport(
                snapshot=self.snapshot_description,
                embedding_cache="<forced-zero>",
                backends=backends,
                traces=(trace.trace_id,),
                require_result_match=self.require_result_match,
                runs=tuple(runs),
                comparisons=tuple(comparisons),
            )
            report.write(output / "results.json")
            reports.append(report)
        return reports

    def _run_one(
        self,
        trace: WorkloadTrace,
        *,
        setup_traces: Sequence[WorkloadTrace],
        backend_name: str,
        repetition: int,
        run_state: Path,
        reset_elapsed_ns: int,
    ) -> BenchmarkRun:
        backend = self._create_backend(backend_name, run_state)
        embedder = ZeroEmbedder(
            self.dimensions,
            model=self.embedding_model,
        )
        service = KnowledgeService(backend, embedder)
        service.start()
        setup_elapsed_ns = 0
        try:
            for setup in setup_traces:
                setup_report = WorkloadReplayer(
                    service,
                    repo_dir=self.repo_dir,
                    allow_shell=self.allow_shell,
                    max_interrupt_seconds=self.max_shell_interrupt_seconds,
                ).replay(setup)
                if not setup_report.succeeded:
                    raise RuntimeError(
                        f"setup trace {setup.trace_id} failed on "
                        f"{backend_name}: {setup_report.as_dict()}"
                    )
                setup_elapsed_ns += setup_report.wall_time_ns
            storage_stats = getattr(backend, "storage_stats")
            baseline_storage = storage_stats()
            baseline_digests = {
                "selection_digest": str(self.manifest["selection_digest"]),
                "setup_traces": hashlib.sha256(
                    canonical_json(
                        [setup.trace_id for setup in setup_traces]
                    ).encode()
                ).hexdigest(),
            }
            replay = WorkloadReplayer(
                service,
                repo_dir=self.repo_dir,
                allow_shell=self.allow_shell,
                max_interrupt_seconds=self.max_shell_interrupt_seconds,
            ).replay(trace)
            if not replay.succeeded:
                raise RuntimeError(
                    f"trace {trace.trace_id} failed on {backend_name}: "
                    f"{replay.as_dict()}"
                )
            release_checkouts = getattr(
                backend,
                "release_session_checkouts",
                None,
            )
            if callable(release_checkouts):
                release_checkouts()
            final_digests = _touched_state_digests(
                backend,
                trace,
                replay.as_dict(),
            )
            final_storage = storage_stats()
            storage = _storage_comparison(
                baseline_storage,
                final_storage,
            )
        finally:
            backend.close()
        return BenchmarkRun(
            trace_id=trace.trace_id,
            backend=backend_name,
            repetition=repetition,
            state_dir=str(run_state),
            seed={
                "documents": int(self.manifest["documents"]),
                "chunks": int(self.manifest["chunks"]),
                "bytes": int(self.manifest["bytes"]),
                "branches": int(baseline_storage.get("branches", 0)),
                "elapsed_ms": setup_elapsed_ns / 1_000_000,
                "reset_elapsed_ms": reset_elapsed_ns / 1_000_000,
                "selection_digest": self.manifest["selection_digest"],
                "embedding_mode": "forced-zero",
                "prepared_root_reused": True,
                "max_shell_interrupt_seconds": (
                    self.max_shell_interrupt_seconds
                ),
                "setup_traces": [
                    setup.trace_id for setup in setup_traces
                ],
            },
            baseline_digests=baseline_digests,
            replay=replay.as_dict(),
            final_digests=final_digests,
            storage=storage,
        )

    def _create_backend(self, backend_name: str, state_dir: Path) -> Any:
        return create_knowledge_backend(
            backend_name,  # type: ignore[arg-type]
            state_dir,
            vector_dimensions=self.dimensions,
            **self.backend_options.get(backend_name, {}),
        )

    def _run_sequence_trace(
        self,
        trace: WorkloadTrace,
        *,
        backend: Any,
        service: KnowledgeService,
        backend_name: str,
        repetition: int,
        run_state: Path,
        reset_elapsed_ns: int,
        position: int,
    ) -> BenchmarkRun:
        storage_stats = getattr(backend, "storage_stats")
        baseline_storage = storage_stats()
        baseline_digests = {
            "selection_digest": str(self.manifest["selection_digest"]),
            "sequence_position": str(position),
        }
        replay = WorkloadReplayer(
            service,
            repo_dir=self.repo_dir,
            allow_shell=self.allow_shell,
            max_interrupt_seconds=self.max_shell_interrupt_seconds,
        ).replay(trace)
        if not replay.succeeded:
            raise RuntimeError(
                f"trace {trace.trace_id} failed on {backend_name}: "
                f"{replay.as_dict()}"
            )
        release_checkouts = getattr(
            backend,
            "release_session_checkouts",
            None,
        )
        if callable(release_checkouts):
            release_checkouts()
        final_digests = _touched_state_digests(
            backend,
            trace,
            replay.as_dict(),
        )
        final_storage = storage_stats()
        storage = _storage_comparison(
            baseline_storage,
            final_storage,
        )
        return BenchmarkRun(
            trace_id=trace.trace_id,
            backend=backend_name,
            repetition=repetition,
            state_dir=str(run_state),
            seed={
                "documents": int(self.manifest["documents"]),
                "chunks": int(self.manifest["chunks"]),
                "bytes": int(self.manifest["bytes"]),
                "branches": int(baseline_storage.get("branches", 0)),
                "elapsed_ms": 0,
                "reset_elapsed_ms": reset_elapsed_ns / 1_000_000,
                "selection_digest": self.manifest["selection_digest"],
                "embedding_mode": "forced-zero",
                "prepared_root_reused": True,
                "max_shell_interrupt_seconds": (
                    self.max_shell_interrupt_seconds
                ),
                "shared_root_sequence": True,
                "sequence_position": position,
            },
            baseline_digests=baseline_digests,
            replay=replay.as_dict(),
            final_digests=final_digests,
            storage=storage,
        )

    def _run_isolated_trace(
        self,
        trace: WorkloadTrace,
        *,
        setup_traces: Sequence[WorkloadTrace],
        backend: Any,
        service: KnowledgeService,
        backend_name: str,
        repetition: int,
        run_state: Path,
        branch_map: Mapping[str, str],
    ) -> BenchmarkRun:
        setup_elapsed_ns = 0
        for setup in setup_traces:
            setup_report = WorkloadReplayer(
                service,
                repo_dir=self.repo_dir,
                allow_shell=self.allow_shell,
                max_interrupt_seconds=self.max_shell_interrupt_seconds,
                branch_map=branch_map,
            ).replay(setup)
            if not setup_report.succeeded:
                raise RuntimeError(
                    f"setup trace {setup.trace_id} failed on "
                    f"{backend_name}: {setup_report.as_dict()}"
                )
            setup_elapsed_ns += setup_report.wall_time_ns

        storage_stats = getattr(backend, "storage_stats")
        baseline_storage = storage_stats()
        baseline_digests = {
            "selection_digest": str(self.manifest["selection_digest"]),
            "isolated_workflow": trace.trace_id,
            "setup_traces": hashlib.sha256(
                canonical_json(
                    [setup.trace_id for setup in setup_traces]
                ).encode()
            ).hexdigest(),
        }
        replay = WorkloadReplayer(
            service,
            repo_dir=self.repo_dir,
            allow_shell=self.allow_shell,
            max_interrupt_seconds=self.max_shell_interrupt_seconds,
            branch_map=branch_map,
        ).replay(trace)
        if not replay.succeeded:
            raise RuntimeError(
                f"trace {trace.trace_id} failed on {backend_name}: "
                f"{replay.as_dict()}"
            )
        release = getattr(backend, "release_session_checkouts", None)
        if callable(release):
            release()
        final_digests = _touched_state_digests(
            backend,
            trace,
            replay.as_dict(),
            branch_map=branch_map,
        )
        final_storage = storage_stats()
        return BenchmarkRun(
            trace_id=trace.trace_id,
            backend=backend_name,
            repetition=repetition,
            state_dir=str(run_state),
            seed={
                "documents": int(self.manifest["documents"]),
                "chunks": int(self.manifest["chunks"]),
                "bytes": int(self.manifest["bytes"]),
                "branches": int(baseline_storage.get("branches", 0)),
                "elapsed_ms": setup_elapsed_ns / 1_000_000,
                "reset_elapsed_ms": 0,
                "selection_digest": self.manifest["selection_digest"],
                "embedding_mode": "forced-zero",
                "prepared_root_reused": True,
                "isolated_workflow": True,
                "setup_traces": [
                    setup.trace_id for setup in setup_traces
                ],
            },
            baseline_digests=baseline_digests,
            replay=replay.as_dict(),
            final_digests=final_digests,
            storage=_storage_comparison(baseline_storage, final_storage),
        )


def _reflink_tree(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.mkdir()
    completed = subprocess.run(
        [
            "cp",
            "-a",
            "--reflink=always",
            f"{source}/.",
            str(destination),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode:
        shutil.rmtree(destination, ignore_errors=True)
        raise RuntimeError(
            "prepared-root reset must use a CoW reflink; refusing a physical "
            f"copy: {completed.stderr.strip()}"
        )


def _load_sequence_resume_position(
    resume_slot: Path,
    *,
    traces: Sequence[WorkloadTrace],
    backend_name: str,
    repetition: int,
    selection_digest: str,
) -> int | None:
    metadata_path = resume_slot / "resume.json"
    state = resume_slot / "state"
    if not metadata_path.is_file() or not state.is_dir():
        return None
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    expected_trace_ids = [trace.trace_id for trace in traces]
    if (
        metadata.get("schema_version") != 1
        or metadata.get("backend") != backend_name
        or int(metadata.get("repetition", -1)) != repetition
        or metadata.get("selection_digest") != selection_digest
        or metadata.get("trace_ids") != expected_trace_ids
    ):
        shutil.rmtree(resume_slot, ignore_errors=True)
        return None
    position = int(metadata.get("completed_position", -1))
    if position < 0 or position >= len(traces):
        shutil.rmtree(resume_slot, ignore_errors=True)
        return None
    if metadata.get("completed_trace_id") != traces[position].trace_id:
        shutil.rmtree(resume_slot, ignore_errors=True)
        return None
    return position


def _in_place_resume_position(
    checkpoint_dir: Path,
    *,
    traces: Sequence[WorkloadTrace],
) -> int | None:
    """Resume an external backend whose durable state remains in place."""

    completed = [
        (
            checkpoint_dir
            / f"{_safe_name(trace.trace_id)}.json"
        ).is_file()
        for trace in traces
    ]
    if not any(completed):
        return None
    first_missing = next(
        (position for position, present in enumerate(completed) if not present),
        len(completed),
    )
    if any(completed[first_missing:]):
        raise RuntimeError(
            "in-place sequence checkpoints contain a gap; the external "
            "backend cannot be resumed safely"
        )
    return first_missing - 1


def _save_sequence_resume(
    run_state: Path,
    resume_slot: Path,
    *,
    traces: Sequence[WorkloadTrace],
    backend_name: str,
    repetition: int,
    selection_digest: str,
    completed_position: int,
) -> None:
    """Publish a CoW snapshot from which a stopped sequence can resume."""

    resume_slot.parent.mkdir(parents=True, exist_ok=True)
    temporary = resume_slot.with_name(
        f".{resume_slot.name}.tmp-{os.getpid()}-{time.time_ns()}"
    )
    backup = resume_slot.with_name(
        f".{resume_slot.name}.backup-{os.getpid()}-{time.time_ns()}"
    )
    shutil.rmtree(temporary, ignore_errors=True)
    _reflink_tree(run_state, temporary / "state")
    metadata = {
        "schema_version": 1,
        "backend": backend_name,
        "repetition": repetition,
        "selection_digest": selection_digest,
        "trace_ids": [trace.trace_id for trace in traces],
        "completed_position": completed_position,
        "completed_trace_id": traces[completed_position].trace_id,
    }
    (temporary / "resume.json").write_text(
        json.dumps(metadata, indent=2) + "\n",
        encoding="utf-8",
    )
    if resume_slot.exists():
        os.replace(resume_slot, backup)
    os.replace(temporary, resume_slot)
    shutil.rmtree(backup, ignore_errors=True)


def _combined_manifest(
    paths: Sequence[Path],
    manifests: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if len(paths) != len(manifests):
        raise ValueError("snapshot paths and manifests must have equal length")
    if len(manifests) == 1:
        return dict(manifests[0])
    digest_payload = [
        {
            "manifest": str(path),
            "selection_digest": manifest["selection_digest"],
        }
        for path, manifest in zip(paths, manifests, strict=True)
    ]
    return {
        "documents": sum(int(item["documents"]) for item in manifests),
        "selected_documents": sum(
            int(item["selected_documents"]) for item in manifests
        ),
        "chunks": sum(int(item["chunks"]) for item in manifests),
        "bytes": sum(int(item["bytes"]) for item in manifests),
        "selection_digest": hashlib.sha256(
            json.dumps(
                digest_payload,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest(),
        "snapshot_manifests": digest_payload,
    }


_BRANCH_ARGUMENT_KEYS = (
    "branch_id",
    "from_branch",
    "source_branch",
    "target_branch",
)


def _trace_branch_ids(traces: Sequence[WorkloadTrace]) -> set[str]:
    branch_ids: set[str] = set()
    for trace in traces:
        for event in trace.events:
            if event.kind != "mcp":
                continue
            for key in _BRANCH_ARGUMENT_KEYS:
                value = event.arguments.get(key)
                if value is not None:
                    branch_ids.add(str(value))
    return branch_ids


def _isolation_namespace(
    repetition: int,
    trace_id: str,
) -> str:
    digest = hashlib.sha256(
        f"{repetition}\0{trace_id}".encode()
    ).hexdigest()[:16]
    return f"bench/{digest}/"


def _isolated_branch_id(namespace: str, branch_id: str) -> str:
    digest = hashlib.sha256(branch_id.encode()).hexdigest()[:10]
    # Keep the complete identifier below conservative database branch-name
    # limits while retaining enough text to diagnose benchmark artifacts.
    readable = _safe_name(branch_id)[:24] or "branch"
    return f"{namespace}{readable}-{digest}"


def _prepare_isolated_branches(
    backend: Any,
    traces: Sequence[WorkloadTrace],
    *,
    namespace: str,
    pristine_branches: set[str],
) -> dict[str, str]:
    branch_map = {
        branch_id: _isolated_branch_id(namespace, branch_id)
        for branch_id in sorted(_trace_branch_ids(traces))
    }
    for branch_id in sorted(branch_map.keys() & pristine_branches):
        backend.create_branch(
            branch_map[branch_id],
            branch_id,
            {
                "benchmark_isolation": True,
                "source_branch": branch_id,
            },
        )
    return branch_map


def _delete_isolation_namespace(backend: Any, namespace: str) -> None:
    # Backends may delete a whole subtree at once. Refresh after every delete
    # so a child removed with its parent is not deleted twice.
    while True:
        candidates = [
            branch
            for branch in backend.list_branches()
            if branch.startswith(namespace)
        ]
        if not candidates:
            return
        candidate = max(candidates, key=lambda value: (value.count("/"), len(value)))
        backend.delete_branch(candidate)


def _touched_state_digests(
    backend: Any,
    trace: WorkloadTrace,
    replay: Mapping[str, Any],
    *,
    branch_map: Mapping[str, str] | None = None,
) -> dict[str, str]:
    mapped_branches = dict(branch_map or {})

    def physical_branch(branch_id: str) -> str:
        return mapped_branches.get(branch_id, branch_id)

    documents: set[tuple[str, str]] = set()
    deleted_documents: set[tuple[str, str]] = set()
    files: set[tuple[str, str]] = set()
    branches: set[str] = set()
    deleted_branches: set[str] = set()
    events = {
        int(event["sequence"]): event
        for event in replay.get("events") or ()
    }
    for event in trace.events:
        if event.kind != "mcp":
            continue
        arguments = event.arguments
        branch_id = arguments.get("branch_id")
        if branch_id:
            branches.add(str(branch_id))
        result = events.get(event.sequence, {})
        summary = result.get("result_summary")
        summary = summary if isinstance(summary, Mapping) else {}
        if event.name in {
            "knowledge_update_document",
            "knowledge_index_workspace_file",
        }:
            document_id = summary.get("document_id")
            if branch_id and document_id:
                documents.add((str(branch_id), str(document_id)))
            path = summary.get("path") or arguments.get("path")
            if branch_id and path:
                files.add((str(branch_id), str(path)))
        elif event.name == "knowledge_remember":
            memory_id = summary.get("memory_id")
            if branch_id and memory_id:
                documents.add((str(branch_id), str(memory_id)))
            path = summary.get("path")
            if branch_id and path:
                files.add((str(branch_id), str(path)))
        elif event.name == "knowledge_write_artifact":
            path = summary.get("path") or arguments.get("path")
            if branch_id and path:
                files.add((str(branch_id), str(path)))
        elif event.name == "knowledge_delete_document":
            identifier = arguments.get("document_id")
            if branch_id and identifier:
                key = (str(branch_id), str(identifier))
                documents.discard(key)
                deleted_documents.add(key)
        elif event.name == "knowledge_merge":
            source = str(arguments["source_branch"])
            target = str(arguments["target_branch"])
            branches.update((source, target))
            documents.update(
                [
                (target, identifier)
                for branch, identifier in documents
                if branch == source
                ]
            )
            files.update(
                [
                (target, path)
                for branch, path in files
                if branch == source
                ]
            )
        elif event.name == "knowledge_delete_branch" and branch_id:
            deleted_branches.add(str(branch_id))

    current_branches = set(backend.list_branches())
    projection: dict[str, Any] = {
        "branches": {
            branch: physical_branch(branch) in current_branches
            for branch in sorted(branches | deleted_branches)
        },
        "documents": {},
        "deleted_documents": {},
        "files": {},
    }
    for branch, identifier in sorted(documents):
        physical = physical_branch(branch)
        if physical not in current_branches:
            continue
        indexed = backend.get_document(physical, identifier)
        projection["documents"][f"{branch}\0{identifier}"] = (
            indexed_document_digest(indexed)
            if indexed is not None
            else "<missing>"
        )
    for branch, identifier in sorted(deleted_documents):
        physical = physical_branch(branch)
        projection["deleted_documents"][f"{branch}\0{identifier}"] = (
            physical in current_branches
            and backend.get_document(physical, identifier) is None
        )
    for branch, path in sorted(files):
        physical = physical_branch(branch)
        if physical not in current_branches:
            continue
        try:
            content = backend.read_file(physical, path)
        except FileNotFoundError:
            digest = "<missing>"
        else:
            digest = content_hash(content)
        projection["files"][f"{branch}\0{path}"] = digest
    return {
        "touched_state": hashlib.sha256(
            canonical_json(projection).encode()
        ).hexdigest()
    }


def _storage_comparison(
    baseline: Mapping[str, Any],
    final: Mapping[str, Any],
) -> dict[str, Any]:
    observed_change = {
        key: final[key] - baseline[key]
        for key in sorted(baseline.keys() & final.keys())
        if isinstance(baseline[key], (int, float))
        and isinstance(final[key], (int, float))
    }
    return {
        "measurement_scope": (
            "diagnostic samples from a reused live backend; changes include "
            "checkpointing, compaction, and asynchronous reclamation and "
            "must not be interpreted as workflow storage overhead"
        ),
        "attributable_to_workflow": False,
        "baseline": dict(baseline),
        "final": dict(final),
        "observed_live_footprint_change": observed_change,
    }


def _compare_runs(
    trace: WorkloadTrace,
    runs: Sequence[BenchmarkRun],
    *,
    backends: Sequence[str],
    repetitions: int,
    require_result_match: bool,
) -> list[dict[str, Any]]:
    by_key = {
        (run.repetition, run.backend): run
        for run in runs
    }
    reference_backend = backends[0]
    comparisons: list[dict[str, Any]] = []
    for repetition in range(repetitions):
        reference = by_key[(repetition, reference_backend)]
        reference_events = {
            int(event["sequence"]): event
            for event in reference.replay.get("events", [])
        }
        for backend in backends[1:]:
            candidate = by_key[(repetition, backend)]
            candidate_events = {
                int(event["sequence"]): event
                for event in candidate.replay.get("events", [])
            }
            mismatches = [
                sequence
                for sequence in sorted(
                    reference_events.keys() | candidate_events.keys()
                )
                if reference_events.get(sequence, {}).get(
                    "normalized_digest"
                )
                != candidate_events.get(sequence, {}).get(
                    "normalized_digest"
                )
            ]
            reference_baseline = dict(reference.baseline_digests)
            candidate_baseline = dict(candidate.baseline_digests)
            # Sequence position is runner bookkeeping, not a digest of
            # logical backend state.  It can differ when completed backend
            # sequences are packaged from separate invocations.
            reference_baseline.pop("sequence_position", None)
            candidate_baseline.pop("sequence_position", None)
            baseline_match = reference_baseline == candidate_baseline
            final_match = (
                reference.final_digests == candidate.final_digests
            )
            replay_succeeded = bool(
                reference.replay.get("succeeded")
                and candidate.replay.get("succeeded")
            )
            results_matched = not mismatches
            comparisons.append(
                {
                    "trace_id": trace.trace_id,
                    "repetition": repetition,
                    "reference_backend": reference_backend,
                    "candidate_backend": backend,
                    "baseline_state_matched": baseline_match,
                    "final_state_matched": final_match,
                    "replay_succeeded": replay_succeeded,
                    "results_matched": results_matched,
                    "event_mismatches": mismatches,
                    "matched": (
                        baseline_match
                        and final_match
                        and replay_succeeded
                        and (
                            results_matched
                            or not require_result_match
                        )
                    ),
                }
            )
    return comparisons


def _safe_name(value: str) -> str:
    return "".join(
        character
        if character.isalnum() or character in "._-"
        else "-"
        for character in value
    ).strip("-")


__all__ = ["PreparedRootBenchmark"]
