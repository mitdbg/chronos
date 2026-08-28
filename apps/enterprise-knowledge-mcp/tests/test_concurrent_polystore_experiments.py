from __future__ import annotations

import importlib.util
import sys
import threading
from pathlib import Path
from typing import Any

from chronos_enterprise_knowledge.models import SearchHit


SCRIPT = (
    Path(__file__).parents[1] / "scripts" / "run_concurrent_polystore_experiments.py"
)
SPEC = importlib.util.spec_from_file_location("concurrent_experiments", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class FakeBackend:
    """Small backend used to test the harness and verifier without services."""

    backend_name = "fake"

    def __init__(self) -> None:
        self._branches: dict[str, dict[str, Any]] = {
            "main": {"documents": {}, "files": {}}
        }
        self._lock = threading.RLock()

    def list_branches(self) -> list[str]:
        return sorted(self._branches)

    def create_branch(self, branch_id: str, parent_branch: str, metadata=None) -> None:
        del metadata
        with self._lock:
            parent = self._branches[parent_branch]
            self._branches[branch_id] = {
                "documents": dict(parent["documents"]),
                "files": dict(parent["files"]),
            }

    def delete_branch(self, branch_id: str) -> None:
        del self._branches[branch_id]

    def put_document(self, branch_id: str, indexed, *, operation_id: str) -> None:
        del operation_id
        with self._lock:
            self._branches[branch_id]["documents"][indexed.document.id] = indexed
            self._branches[branch_id]["files"][indexed.document.path] = (
                indexed.document.content.encode()
            )

    def get_document(self, branch_id: str, document_id: str):
        return self._branches[branch_id]["documents"].get(document_id)

    def find_document_id_by_path(self, branch_id: str, path: str):
        for document_id, indexed in self._branches[branch_id]["documents"].items():
            if indexed.document.path == path:
                return document_id
        return None

    def read_file(self, branch_id: str, path: str) -> bytes:
        try:
            return self._branches[branch_id]["files"][path]
        except KeyError as exc:
            raise FileNotFoundError(path) from exc

    def search(self, branch_id: str, query_text: str, query_embedding, *, limit: int):
        del query_text, query_embedding
        hits = []
        for indexed in self._branches[branch_id]["documents"].values():
            for chunk in indexed.chunks:
                hits.append(
                    SearchHit(
                        document_id=indexed.document.id,
                        chunk_id=chunk.id,
                        path=indexed.document.path,
                        title=indexed.document.title,
                        text=chunk.text,
                        score=1.0,
                        source=indexed.document.source,
                    )
                )
        return hits[:limit]

    def write_file(
        self, branch_id: str, path: str, content: bytes, *, operation_id: str
    ) -> None:
        del operation_id
        self._branches[branch_id]["files"][path] = bytes(content)

    def merge_preview(self, source_branch: str, target_branch: str, *, policy=None):
        del policy
        source = self._branches[source_branch]
        ids = []
        for indexed in source["documents"].values():
            ids.extend(
                (
                    f"document:{indexed.document.id}",
                    f"file:{indexed.document.path}",
                )
            )
        return {
            "source": source_branch,
            "target": target_branch,
            "preview_token": "fake-token",
            "change_ids": ids,
            "selection_groups": {
                "indexed_documents": {
                    indexed.document.id: [
                        f"document:{indexed.document.id}",
                        f"file:{indexed.document.path}",
                    ]
                    for indexed in source["documents"].values()
                }
            },
        }

    def merge(
        self,
        source_branch: str,
        target_branch: str,
        *,
        operation_id: str,
        selected_change_ids=None,
        preview_token=None,
        policy=None,
        conflict_choices=None,
    ):
        del operation_id, preview_token, policy, conflict_choices
        selected = set(selected_change_ids or ())
        source = self._branches[source_branch]
        target = self._branches[target_branch]
        with self._lock:
            for document_id, indexed in source["documents"].items():
                if not selected or f"document:{document_id}" in selected:
                    target["documents"][document_id] = indexed
                    target["files"][indexed.document.path] = (
                        indexed.document.content.encode()
                    )
        return {"status": "merged"}

    def close(self) -> None:
        return None


def test_all_change_ids_and_nested_error_detection() -> None:
    preview = {
        "selection_groups": {
            "indexed_documents": {"doc": ["relational:a", "qdrant:b"]},
            "filesystem_paths": {"/doc": ["filesystem:c"]},
        },
        "stores": {"relational": {"changes": [{"change_id": "relational:a"}]}},
    }
    assert MODULE._all_change_ids(preview) == [
        "filesystem:c",
        "qdrant:b",
        "relational:a",
    ]
    assert MODULE._has_errors({"main": [], "child": ["torn state"]})
    assert not MODULE._has_errors({"main": [], "child": []})


def test_verifier_detects_file_record_mismatch() -> None:
    backend = FakeBackend()
    indexed = MODULE.make_document(
        "doc",
        "/knowledge/doc.md",
        "consistent content",
        dimensions=3,
        vector_index=0,
    )
    backend.put_document("main", indexed, operation_id="seed")
    backend._branches["main"]["files"]["/knowledge/doc.md"] = b"torn content"
    registry = MODULE.BranchRegistry()
    recorder = MODULE.Recorder()
    state = MODULE.RunState()
    verifier = MODULE.InvariantVerifier(
        backend,
        registry,
        lambda: (
            MODULE.DocumentSpec("doc", "/knowledge/doc.md", "doc", (1.0, 0.0, 0.0)),
        ),
        state,
        recorder,
        interval=0.001,
        max_pairs_per_check=1,
    )
    verifier.check_once()
    assert any(item["kind"] == "file-record-mismatch" for item in verifier.violations)


def test_verifier_checker_has_no_background_thread() -> None:
    backend = FakeBackend()
    indexed = MODULE.make_document(
        "doc",
        "/knowledge/doc.md",
        "content",
        dimensions=3,
        vector_index=0,
    )
    backend.put_document("main", indexed, operation_id="seed")
    registry = MODULE.BranchRegistry()
    verifier = MODULE.InvariantVerifier(
        backend,
        registry,
        lambda: (
            MODULE.DocumentSpec("doc", "/knowledge/doc.md", "doc", (1.0, 0.0, 0.0)),
        ),
        MODULE.RunState(),
        MODULE.Recorder(),
        interval=0.001,
        max_pairs_per_check=1,
    )
    stop_event = threading.Event()
    stop_event.set()
    verifier.run_until(stop_event)
    assert verifier.checks == 0
    assert not hasattr(verifier, "thread")


def test_disjoint_and_selective_scenarios_use_backend_neutral_interface() -> None:
    for runner in (MODULE.run_disjoint, MODULE.run_competing, MODULE.run_recursive):
        backend = FakeBackend()
        context = MODULE.ExperimentContext(
            backend=backend,
            backend_name="fake",
            state_dir="/tmp/fake-concurrent-polystore",
            factory_kwargs={},
            registry=MODULE.BranchRegistry(),
            state=MODULE.RunState(),
            recorder=MODULE.Recorder(),
            dimensions=3,
            verify_interval=0.001,
            verify_pairs=2,
            verifier_enabled=False,
        )
        MODULE._seed(context)
        payload = runner(context, 2)
        MODULE._stop_verifier(context)
        assert not MODULE._has_errors(payload.get("final_errors"))
