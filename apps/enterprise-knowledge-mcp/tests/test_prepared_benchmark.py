from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from chronos_enterprise_knowledge import prepared_benchmark
from chronos_enterprise_knowledge.backends import (
    ApplicationManagedKnowledgeBackend,
)
from chronos_enterprise_knowledge.models import (
    DocumentChunk,
    IndexedDocument,
    KnowledgeDocument,
)
from chronos_enterprise_knowledge.prepared_benchmark import (
    PreparedRootBenchmark,
    _compare_runs,
)
from chronos_enterprise_knowledge.rollout_benchmark import BenchmarkRun
from chronos_enterprise_knowledge.rollout_trace import (
    WorkloadEvent,
    WorkloadTrace,
)


def _seed_document() -> IndexedDocument:
    document = KnowledgeDocument(
        id="root-handbook",
        path="/knowledge/root-handbook.md",
        title="Root Handbook",
        source="test",
        content="The root handbook describes the company process.",
        metadata={},
    )
    return IndexedDocument(
        document,
        (
            DocumentChunk(
                id="root-handbook:0",
                document_id="root-handbook",
                ordinal=0,
                text=document.content,
                embedding=(0.0, 0.0, 0.0),
                metadata={},
            ),
        ),
    )


def test_run_comparison_ignores_sequence_position_bookkeeping() -> None:
    trace = WorkloadTrace("08-workflow", ())

    def run(backend: str, position: str) -> BenchmarkRun:
        return BenchmarkRun(
            trace_id=trace.trace_id,
            backend=backend,
            repetition=0,
            state_dir=f"/tmp/{backend}",
            seed={},
            baseline_digests={
                "selection_digest": "same-root",
                "sequence_position": position,
            },
            replay={"succeeded": True, "events": []},
            final_digests={"touched_state": "same-final-state"},
            storage={},
        )

    comparison = _compare_runs(
        trace,
        (run("chronos", "7"), run("app-managed", "0")),
        backends=("chronos", "app-managed"),
        repetitions=1,
        require_result_match=False,
    )

    assert comparison[0]["baseline_state_matched"] is True
    assert comparison[0]["matched"] is True


def test_shared_root_sequence_reports_each_workflow_separately(
    tmp_path: Path,
    monkeypatch,
) -> None:
    base = tmp_path / "base"
    backend = ApplicationManagedKnowledgeBackend(base, vector_dimensions=3)
    try:
        backend.set_placeholder_vector_mode(True)
        backend.put_document(
            "main",
            _seed_document(),
            operation_id="seed",
        )
    finally:
        backend.close()
    manifest = tmp_path / "snapshot" / "manifest.json"
    manifest.parent.mkdir()
    manifest.write_text(
        json.dumps(
            {
                "documents": 1,
                "selected_documents": 1,
                "chunks": 1,
                "bytes": 52,
                "selection_digest": "digest",
            }
        ),
        encoding="utf-8",
    )

    def copy_tree(source: Path, destination: Path) -> None:
        shutil.copytree(source, destination)

    monkeypatch.setattr(prepared_benchmark, "_reflink_tree", copy_tree)
    checkout = WorkloadTrace(
        "01-checkout",
        (
            WorkloadEvent(
                0,
                "mcp",
                "knowledge_checkout",
                {
                    "branch_id": "team",
                    "from_branch": "main",
                    "mount": False,
                },
            ),
        ),
    )
    remember = WorkloadTrace(
        "02-remember",
        (
            WorkloadEvent(
                0,
                "mcp",
                "knowledge_remember",
                {
                    "branch_id": "team",
                    "title": "Debugging outcome",
                    "summary": "The team validated the incident workflow.",
                    "kind": "episodic_memory",
                    "memory_id": "memory_debug_outcome",
                    "recorded_at": "2026-01-01T00:00:00+00:00",
                },
            ),
        ),
    )
    benchmark = PreparedRootBenchmark(
        snapshot_manifest=manifest,
        base_states={"app-managed": base},
        work_root=tmp_path / "work",
        output_root=tmp_path / "out",
        repo_dir=tmp_path / "repo",
        dimensions=3,
        embedding_model="zero-test",
        repetitions=1,
    )

    reports = benchmark.run_sequence([checkout, remember])

    assert [report.traces for report in reports] == [
        ("01-checkout",),
        ("02-remember",),
    ]
    assert all(report.matched for report in reports)
    assert (tmp_path / "out/01-checkout/results.json").is_file()
    assert (tmp_path / "out/02-remember/results.json").is_file()
    remember_run = reports[1].runs[0]
    assert remember_run.seed["shared_root_sequence"] is True
    assert remember_run.seed["sequence_position"] == 1
    assert remember_run.replay["succeeded"] is True


def test_isolated_workflows_restore_pristine_branch_state(
    tmp_path: Path,
) -> None:
    base = tmp_path / "base"
    backend = ApplicationManagedKnowledgeBackend(base, vector_dimensions=3)
    try:
        backend.set_placeholder_vector_mode(True)
        backend.put_document("main", _seed_document(), operation_id="seed")
        backend.create_branch("person/alex", "main")
    finally:
        backend.close()
    manifest = tmp_path / "snapshot" / "manifest.json"
    manifest.parent.mkdir()
    manifest.write_text(
        json.dumps(
            {
                "documents": 1,
                "selected_documents": 1,
                "chunks": 1,
                "bytes": 52,
                "selection_digest": "digest",
            }
        ),
        encoding="utf-8",
    )

    def workflow(trace_id: str, memory_id: str) -> WorkloadTrace:
        return WorkloadTrace(
            trace_id,
            (
                WorkloadEvent(
                    0,
                    "mcp",
                    "knowledge_checkout",
                    {
                        "branch_id": "task/alex/debug",
                        "from_branch": "person/alex",
                        "mount": False,
                    },
                ),
                WorkloadEvent(
                    1,
                    "mcp",
                    "knowledge_remember",
                    {
                        "branch_id": "task/alex/debug",
                        "title": "Debugging outcome",
                        "summary": "The isolated workflow found the cause.",
                        "kind": "episodic_memory",
                        "memory_id": memory_id,
                        "recorded_at": "2026-01-01T00:00:00+00:00",
                    },
                ),
            ),
        )

    benchmark = PreparedRootBenchmark(
        snapshot_manifest=manifest,
        base_states={"app-managed": base},
        work_root=tmp_path / "work",
        output_root=tmp_path / "out",
        repo_dir=tmp_path / "repo",
        dimensions=3,
        embedding_model="zero-test",
        repetitions=1,
    )
    reports = benchmark.run_isolated_workflows(
        [workflow("01-first", "memory_first"), workflow("02-second", "memory_second")]
    )

    assert [report.runs[0].seed["branches"] for report in reports] == [3, 3]
    assert all(report.runs[0].seed["isolated_workflow"] for report in reports)
    reopened = ApplicationManagedKnowledgeBackend(base, vector_dimensions=3)
    try:
        assert reopened.list_branches() == ["main", "person/alex"]
        assert reopened.get_document("person/alex", "memory_first") is None
        assert reopened.get_document("person/alex", "memory_second") is None
    finally:
        reopened.close()


def test_isolated_workflow_runs_declared_dependency_inside_same_clone(
    tmp_path: Path,
) -> None:
    base = tmp_path / "base"
    backend = ApplicationManagedKnowledgeBackend(base, vector_dimensions=3)
    try:
        backend.set_placeholder_vector_mode(True)
        backend.put_document("main", _seed_document(), operation_id="seed")
    finally:
        backend.close()
    manifest = tmp_path / "snapshot" / "manifest.json"
    manifest.parent.mkdir()
    manifest.write_text(
        json.dumps(
            {
                "documents": 1,
                "selected_documents": 1,
                "chunks": 1,
                "bytes": 52,
                "selection_digest": "digest",
            }
        ),
        encoding="utf-8",
    )
    setup = WorkloadTrace(
        "01-create-team",
        (
            WorkloadEvent(
                0,
                "mcp",
                "knowledge_checkout",
                {
                    "branch_id": "team/runtime",
                    "from_branch": "main",
                    "mount": False,
                },
            ),
        ),
    )
    dependent = WorkloadTrace(
        "02-team-memory",
        (
            WorkloadEvent(
                0,
                "mcp",
                "knowledge_remember",
                {
                    "branch_id": "team/runtime",
                    "title": "Runtime outcome",
                    "summary": "The dependency created this team branch.",
                    "kind": "episodic_memory",
                    "memory_id": "runtime_outcome",
                    "recorded_at": "2026-01-01T00:00:00+00:00",
                },
            ),
        ),
    )
    benchmark = PreparedRootBenchmark(
        snapshot_manifest=manifest,
        base_states={"app-managed": base},
        work_root=tmp_path / "work",
        output_root=tmp_path / "out",
        repo_dir=tmp_path / "repo",
        dimensions=3,
        embedding_model="zero-test",
        repetitions=1,
    )

    report = benchmark.run_isolated_workflows(
        [dependent],
        setup_traces={dependent.trace_id: (setup,)},
    )[0]

    assert report.runs[0].replay["succeeded"] is True
    assert report.runs[0].seed["setup_traces"] == [setup.trace_id]
    reopened = ApplicationManagedKnowledgeBackend(base, vector_dimensions=3)
    try:
        assert reopened.list_branches() == ["main"]
    finally:
        reopened.close()


def test_shared_root_sequence_resumes_after_completed_workflow(
    tmp_path: Path,
    monkeypatch,
) -> None:
    base = tmp_path / "base"
    backend = ApplicationManagedKnowledgeBackend(base, vector_dimensions=3)
    try:
        backend.set_placeholder_vector_mode(True)
        backend.put_document(
            "main",
            _seed_document(),
            operation_id="seed",
        )
    finally:
        backend.close()
    manifest = tmp_path / "snapshot" / "manifest.json"
    manifest.parent.mkdir()
    manifest.write_text(
        json.dumps(
            {
                "documents": 1,
                "selected_documents": 1,
                "chunks": 1,
                "bytes": 52,
                "selection_digest": "digest",
            }
        ),
        encoding="utf-8",
    )

    def copy_tree(source: Path, destination: Path) -> None:
        shutil.copytree(source, destination)

    monkeypatch.setattr(prepared_benchmark, "_reflink_tree", copy_tree)
    traces = (
        WorkloadTrace(
            "01-checkout",
            (
                WorkloadEvent(
                    0,
                    "mcp",
                    "knowledge_checkout",
                    {
                        "branch_id": "team",
                        "from_branch": "main",
                        "mount": False,
                    },
                ),
            ),
        ),
        WorkloadTrace(
            "02-remember",
            (
                WorkloadEvent(
                    0,
                    "mcp",
                    "knowledge_remember",
                    {
                        "branch_id": "team",
                        "title": "Recovered outcome",
                        "summary": "The resumed workflow retained prior state.",
                        "kind": "episodic_memory",
                        "memory_id": "memory_recovered_outcome",
                        "recorded_at": "2026-01-01T00:00:00+00:00",
                    },
                ),
            ),
        ),
    )
    arguments = {
        "snapshot_manifest": manifest,
        "base_states": {"app-managed": base},
        "work_root": tmp_path / "work",
        "output_root": tmp_path / "out",
        "repo_dir": tmp_path / "repo",
        "dimensions": 3,
        "embedding_model": "zero-test",
        "repetitions": 1,
    }
    interrupted = PreparedRootBenchmark(**arguments)
    original = interrupted._run_sequence_trace

    def stop_after_first(*args, position: int, **kwargs):
        if position == 1:
            raise RuntimeError("simulated interruption")
        return original(*args, position=position, **kwargs)

    monkeypatch.setattr(
        interrupted,
        "_run_sequence_trace",
        stop_after_first,
    )
    with pytest.raises(RuntimeError, match="simulated interruption"):
        interrupted.run_sequence(traces)

    resumed = PreparedRootBenchmark(**arguments)
    resumed_original = resumed._run_sequence_trace
    replayed_positions: list[int] = []

    def record_position(*args, position: int, **kwargs):
        replayed_positions.append(position)
        return resumed_original(*args, position=position, **kwargs)

    monkeypatch.setattr(resumed, "_run_sequence_trace", record_position)
    reports = resumed.run_sequence(traces)

    assert replayed_positions == [1]
    assert all(report.matched for report in reports)
    assert (
        tmp_path
        / "out/sequence-checkpoints/repeat-000-app-managed/.complete"
    ).is_file()
    assert not (
        tmp_path / "work/sequence-resume/repeat-000-app-managed"
    ).exists()


def test_in_place_sequence_resumes_external_backend_state(
    tmp_path: Path,
    monkeypatch,
) -> None:
    base = tmp_path / "base"
    backend = ApplicationManagedKnowledgeBackend(base, vector_dimensions=3)
    try:
        backend.set_placeholder_vector_mode(True)
        backend.put_document("main", _seed_document(), operation_id="seed")
    finally:
        backend.close()
    manifest = tmp_path / "snapshot" / "manifest.json"
    manifest.parent.mkdir()
    manifest.write_text(
        json.dumps(
            {
                "documents": 1,
                "selected_documents": 1,
                "chunks": 1,
                "bytes": 52,
                "selection_digest": "digest",
            }
        ),
        encoding="utf-8",
    )
    traces = (
        WorkloadTrace(
            "01-checkout",
            (
                WorkloadEvent(
                    0,
                    "mcp",
                    "knowledge_checkout",
                    {
                        "branch_id": "team",
                        "from_branch": "main",
                        "mount": False,
                    },
                ),
            ),
        ),
        WorkloadTrace(
            "02-remember",
            (
                WorkloadEvent(
                    0,
                    "mcp",
                    "knowledge_remember",
                    {
                        "branch_id": "team",
                        "title": "Native baseline outcome",
                        "summary": "The in-place sequence retained prior state.",
                        "kind": "episodic_memory",
                        "memory_id": "memory_native_outcome",
                        "recorded_at": "2026-01-01T00:00:00+00:00",
                    },
                ),
            ),
        ),
    )
    arguments = {
        "snapshot_manifest": manifest,
        "base_states": {"app-managed": base},
        "work_root": tmp_path / "work",
        "output_root": tmp_path / "out",
        "repo_dir": tmp_path / "repo",
        "dimensions": 3,
        "embedding_model": "zero-test",
        "repetitions": 1,
        "in_place_backends": ("app-managed",),
    }
    interrupted = PreparedRootBenchmark(**arguments)
    original = interrupted._run_sequence_trace

    def stop_after_first(*args, position: int, **kwargs):
        if position == 1:
            raise RuntimeError("simulated in-place interruption")
        return original(*args, position=position, **kwargs)

    monkeypatch.setattr(
        interrupted,
        "_run_sequence_trace",
        stop_after_first,
    )
    with pytest.raises(RuntimeError, match="simulated in-place interruption"):
        interrupted.run_sequence(traces)

    resumed = PreparedRootBenchmark(**arguments)
    resumed_original = resumed._run_sequence_trace
    replayed_positions: list[int] = []

    def record_position(*args, position: int, **kwargs):
        replayed_positions.append(position)
        return resumed_original(*args, position=position, **kwargs)

    monkeypatch.setattr(resumed, "_run_sequence_trace", record_position)
    reports = resumed.run_sequence(traces)

    assert replayed_positions == [1]
    assert all(report.matched for report in reports)
    assert (
        tmp_path
        / "out/sequence-checkpoints/repeat-000-app-managed/.complete"
    ).is_file()
    assert not (tmp_path / "work/sequence-resume").exists()


def test_prepared_root_combines_layered_snapshot_provenance(
    tmp_path: Path,
) -> None:
    manifests = []
    for name, documents, chunks, size, digest in (
        ("documents", 10, 20, 100, "documents-digest"),
        ("code", 3, 9, 40, "code-digest"),
    ):
        path = tmp_path / name / "manifest.json"
        path.parent.mkdir()
        path.write_text(
            json.dumps(
                {
                    "documents": documents,
                    "selected_documents": documents,
                    "chunks": chunks,
                    "bytes": size,
                    "selection_digest": digest,
                }
            ),
            encoding="utf-8",
        )
        manifests.append(path)
    base = tmp_path / "base"
    base.mkdir()

    benchmark = PreparedRootBenchmark(
        snapshot_manifest=manifests,
        base_states={"app-managed": base},
        work_root=tmp_path / "work",
        output_root=tmp_path / "out",
        repo_dir=tmp_path / "repo",
        dimensions=3,
        embedding_model="zero-test",
        repetitions=1,
    )

    assert benchmark.manifest["documents"] == 13
    assert benchmark.manifest["selected_documents"] == 13
    assert benchmark.manifest["chunks"] == 29
    assert benchmark.manifest["bytes"] == 140
    assert len(benchmark.manifest["selection_digest"]) == 64
