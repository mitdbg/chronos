from __future__ import annotations

import importlib.util
import sys
import threading
from pathlib import Path

from chronos_enterprise_knowledge.models import SearchHit


SCRIPT = (
    Path(__file__).parents[1] / "scripts" / "run_incident_response_swarm.py"
)
SPEC = importlib.util.spec_from_file_location("incident_swarm", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class FakeBackend:
    backend_name = "fake"

    def __init__(self) -> None:
        self.documents: dict[str, object] = {}
        self.files: dict[str, bytes] = {}

    def get_document(self, branch: str, document_id: str):
        del branch
        return self.documents.get(document_id)

    def find_document_id_by_path(self, branch: str, path: str):
        del branch
        for document_id, indexed in self.documents.items():
            if indexed.document.path == path:
                return document_id
        return None

    def read_file(self, branch: str, path: str) -> bytes:
        del branch
        return self.files[path]

    def search(self, branch: str, query: str, embedding, *, limit: int):
        del branch, query, embedding, limit
        return [
            SearchHit(
                document_id=indexed.document.id,
                chunk_id=indexed.chunks[0].id,
                path=indexed.document.path,
                title=indexed.document.title,
                text=indexed.chunks[0].text,
                score=1.0,
                source=indexed.document.source,
            )
            for indexed in self.documents.values()
        ]


def _trace(row_id: str = "incident"):
    from chronos_enterprise_knowledge.rollout_trace import WorkloadEvent, WorkloadTrace

    values = {
        "incident_id": row_id,
        "task_branch": f"task/swarm-{row_id}",
        "artifact_path": f"/artifacts/incidents/swarm-{row_id}.md",
        "document_id": f"incident-report/swarm-{row_id}",
        "memory_id": f"episodic/swarm-{row_id}",
    }
    event = WorkloadEvent(
        sequence=0,
        kind="mcp",
        name="knowledge_checkout",
        arguments={
            "branch_id": values["task_branch"],
            "from_branch": "team/site-reliability",
            "mount": False,
        },
        expected={},
    )
    return WorkloadTrace(
        f"incident-response-swarm-{row_id}",
        (event,),
        {"template_values": values},
    )


def test_worker_limit_is_explicit() -> None:
    assert MODULE.MAX_WORKERS == 128
    row = MODULE.IncidentInput("a", "A", "query", "hint")
    try:
        MODULE._build_plans(
            Path("/does/not/exist"),
            [row],
            129,
            allow_trace_reuse=True,
            run_id="run",
            dimensions=3,
            target_branch="team/site-reliability",
            expanded_dir=Path("/tmp/incident-swarm-test"),
        )
    except ValueError as exc:
        assert "128" in str(exc)
    else:  # pragma: no cover - assertion documents the public limit
        raise AssertionError("worker limit was not enforced")


def test_unlimited_merge_retry_argument_is_explicit() -> None:
    assert MODULE._parse_merge_retries("unlimited") == -1
    assert MODULE._parse_merge_retries("-1") == -1
    assert MODULE._parse_merge_retries("7") == 7


def test_worker_backend_close_keeps_shared_daemon_alive() -> None:
    class ChronosLikeBackend:
        def __init__(self) -> None:
            self.calls: list[bool] = []

        def close(self, *, shutdown_daemon: bool = True) -> None:
            self.calls.append(shutdown_daemon)

    backend = ChronosLikeBackend()
    MODULE._close_worker_backend(backend)
    assert backend.calls == [False]


def test_worker_backend_close_supports_comparison_backends() -> None:
    class ComparisonLikeBackend:
        def __init__(self) -> None:
            self.closed = 0

        def close(self) -> None:
            self.closed += 1

    backend = ComparisonLikeBackend()
    MODULE._close_worker_backend(backend)
    assert backend.closed == 1


def test_maximum_worker_plan_is_namespaced(tmp_path: Path) -> None:
    row = MODULE.IncidentInput("a", "A", "query", "hint")
    trace_path = tmp_path / "incident-response-swarm-a.jsonl"
    _trace("a").write(trace_path)
    plans = MODULE._build_plans(
        tmp_path,
        [row],
        128,
        allow_trace_reuse=True,
        run_id="run",
        dimensions=3,
        target_branch="team/site-reliability",
        expanded_dir=tmp_path / "expanded",
    )
    assert len(plans) == 128
    assert len({plan.bundle.document_id for plan in plans}) == 128
    assert len({plan.bundle.input_id for plan in plans}) == 128
    assert plans[-1].trace_path.endswith("worker-127.jsonl")


def test_overlap_metric_counts_process_intervals() -> None:
    assert MODULE._max_interval_overlap(
        [
            {"replay_started_at": 0.0, "replay_finished_at": 2.0},
            {"replay_started_at": 0.5, "replay_finished_at": 1.5},
            {"replay_started_at": 2.0, "replay_finished_at": 3.0},
        ]
    ) == 2


def test_expand_trace_namespaces_every_worker_state() -> None:
    row = MODULE.IncidentInput("a", "A", "query", "hint")
    expanded, bundle = MODULE._expand_trace(
        _trace("a"),
        row,
        worker_id=7,
        run_id="run",
        dimensions=3,
        target_branch="team/site-reliability",
    )
    event = expanded.events[0]
    assert event.arguments["branch_id"] == "swarm/run/worker-007/task"
    assert event.arguments["from_branch"] == "team/site-reliability"
    assert bundle.task_branch == "swarm/run/worker-007/task"
    assert bundle.artifact_path.endswith("-run-w007.md")
    assert bundle.document_id.endswith("-run-w007")
    assert bundle.memory_id.endswith("-run-w007")


def test_check_bundle_detects_partial_cross_store_publication() -> None:
    from chronos_enterprise_knowledge.models import (
        DocumentChunk,
        IndexedDocument,
        KnowledgeDocument,
    )

    backend = FakeBackend()
    document = KnowledgeDocument(
        id="incident-report/run-w000",
        path="/artifacts/incidents/report.md",
        title="Incident report",
        source="test",
        content="supported diagnosis",
        kind="curated",
    )
    chunk = DocumentChunk(
        id="incident-report/run-w000:chunk-0",
        document_id=document.id,
        ordinal=0,
        text=document.content,
        embedding=(1.0, 0.0, 0.0),
        metadata={},
    )
    indexed = IndexedDocument(document=document, chunks=(chunk,))
    backend.documents[document.id] = indexed
    backend.files[document.path] = document.content.encode()
    state = MODULE._check_bundle(
        backend,
        {
            "input_id": "run-w000",
            "document_id": document.id,
            "artifact_path": document.path,
            "memory_id": "episodic/run-w000",
            "query": "incident",
            "query_embedding": [1.0, 0.0, 0.0],
            "target_branch": "team/site-reliability",
        },
        search=False,
        search_limit=8,
    )
    assert state["any_component"]
    assert not state["complete"]
    assert state["component_presence"] == {"artifact": True, "memory": False}


def test_verifier_state_is_compact() -> None:
    state = {
        "complete": False,
        "any_component": True,
        "component_presence": {"artifact": True, "memory": False},
        "vector_observed": True,
        "vector_hits": [f"hit-{index}" for index in range(1000)],
        "vector_error": None,
        "errors": [],
        "branch_version_start": "1",
        "branch_version_end": "1",
        "branch_version_stable": True,
        "artifact": {
            "present": True,
            "identifier": "incident-report/a",
            "path": "/artifacts/a.md",
            "errors": [],
            "content": "large payload that must not cross the verifier queue",
        },
    }
    compact = MODULE._compact_verifier_state(state)
    assert "vector_hits" not in compact
    assert compact["artifact"] == {
        "present": True,
        "identifier": "incident-report/a",
        "path": "/artifacts/a.md",
        "errors": [],
    }


def test_verifier_shutdown_does_not_wait_on_blocked_queue() -> None:
    class BlockingQueue:
        def get(self):
            threading.Event().wait()

    class FakeEvent:
        def set(self):
            return None

    class FakeProcess:
        pid = 7
        exitcode = -15

        def __init__(self):
            self.alive = True

        def join(self, timeout=None):
            del timeout

        def is_alive(self):
            return self.alive

        def terminate(self):
            self.alive = False

    result = MODULE._stop_verifier(
        FakeProcess(),
        FakeEvent(),
        BlockingQueue(),
    )
    assert result["exitcode"] == -15
    assert "terminated" in result["process_error"]


def test_native_big_lock_serializes_each_operation(tmp_path: Path) -> None:
    controller = MODULE.NativeBigLockController(tmp_path / "swarm.lock")
    try:
        arguments = {
            "source_branch": "task/one",
            "target_branch": "team/site-reliability",
        }
        preview_token = controller.before_operation(
            "knowledge_merge_preview", arguments
        )
        controller.after_operation(
            preview_token,
            "knowledge_merge_preview",
            arguments,
            succeeded=True,
        )
        assert not controller.lock._stack
        read_token = controller.before_operation("knowledge_search", {})
        controller.after_operation(
            read_token,
            "knowledge_search",
            {},
            succeeded=True,
        )
        merge_token = controller.before_operation("knowledge_merge", arguments)
        controller.after_operation(
            merge_token,
            "knowledge_merge",
            arguments,
            succeeded=True,
        )
        assert not controller.lock._stack
    finally:
        controller.close()


def test_native_big_lock_releases_failed_operation(tmp_path: Path) -> None:
    controller = MODULE.NativeBigLockController(tmp_path / "swarm.lock")
    try:
        arguments = {
            "source_branch": "task/one",
            "target_branch": "team/site-reliability",
        }
        token = controller.before_operation("knowledge_merge_preview", arguments)
        controller.after_operation(
            token,
            "knowledge_merge_preview",
            arguments,
            succeeded=False,
        )
        assert not controller.lock._stack
    finally:
        controller.close()


def test_native_big_lock_covers_every_operation(
    tmp_path: Path,
) -> None:
    controller = MODULE.NativeBigLockController(tmp_path / "swarm.lock")
    try:
        read_token = controller.before_operation(
            "exec_command",
            {"cmd": "cat branch-state.txt"},
        )
        assert read_token is True
        controller.after_operation(
            read_token,
            "exec_command",
            {"cmd": "cat branch-state.txt"},
            succeeded=True,
        )

        patch_token = controller.before_operation(
            "apply_patch",
            {"patch": "*** Begin Patch\n*** End Patch"},
        )
        assert patch_token is True
        controller.after_operation(
            patch_token,
            "apply_patch",
            {"patch": "*** Begin Patch\n*** End Patch"},
            succeeded=True,
        )

        compound_token = controller.before_operation(
            "exec_command",
            {"cmd": "cat branch-state.txt; printf changed > branch-state.txt"},
        )
        assert compound_token is True
        controller.after_operation(
            compound_token,
            "exec_command",
            {"cmd": "cat branch-state.txt; printf changed > branch-state.txt"},
            succeeded=True,
        )
        assert controller.lock.acquires == 3
    finally:
        controller.close()


def test_native_big_lock_covers_complete_replay_scope(tmp_path: Path) -> None:
    controller = MODULE.NativeBigLockController(tmp_path / "swarm.lock")
    try:
        replay_token = controller.before_replay("trace-1")
        assert replay_token is True
        assert controller.lock._stack == 1
        # Per-event hooks are nested inside the replay scope and must not
        # release the outer lock while model time or later events run.
        event_token = controller.before_operation("knowledge_search", {})
        assert event_token is None
        controller.after_operation(
            event_token,
            "knowledge_search",
            {},
            succeeded=True,
        )
        assert controller.lock._stack == 1
        controller.after_replay(replay_token, "trace-1", succeeded=True)
        assert controller.lock._stack == 0
        assert controller.metrics()["lock_hold_seconds"] >= 0.0
    finally:
        controller.close()
