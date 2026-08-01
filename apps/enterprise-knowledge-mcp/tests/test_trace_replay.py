from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from chronos_enterprise_knowledge.backend import (
    OperationExecutor,
)
from chronos_enterprise_knowledge.models import (
    DocumentChunk,
    IndexedDocument,
    KnowledgeDocument,
    SearchHit,
    canonical_json,
)
from chronos_enterprise_knowledge.trace import TraceRecorder, TraceReplayer


class MemoryBackend:
    def __init__(self, name: str):
        self._name = name
        self.parents: dict[str, str | None] = {"main": None}
        self.documents: dict[str, dict[str, IndexedDocument]] = {"main": {}}
        self.files: dict[str, dict[str, bytes]] = {"main": {}}
        self.operation_ids: list[str] = []

    @property
    def backend_name(self) -> str:
        return self._name

    def create_branch(
        self,
        branch_id: str,
        parent_branch: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self.parents[branch_id] = parent_branch
        self.documents[branch_id] = dict(self.documents[parent_branch])
        self.files[branch_id] = dict(self.files[parent_branch])

    def delete_branch(self, branch_id: str) -> None:
        self.parents.pop(branch_id)
        self.documents.pop(branch_id)
        self.files.pop(branch_id)

    def put_document(
        self,
        branch_id: str,
        indexed: IndexedDocument,
        *,
        operation_id: str,
    ) -> None:
        self.operation_ids.append(operation_id)
        self.documents[branch_id][indexed.document.id] = indexed
        self.files[branch_id][indexed.document.path] = indexed.document.content.encode(
            "utf-8"
        )

    def delete_document(
        self,
        branch_id: str,
        document_id: str,
        *,
        operation_id: str,
    ) -> bool:
        self.operation_ids.append(operation_id)
        indexed = self.documents[branch_id].pop(document_id, None)
        if indexed is None:
            return False
        self.files[branch_id].pop(indexed.document.path, None)
        return True

    def get_document(
        self,
        branch_id: str,
        document_id: str,
    ) -> IndexedDocument | None:
        return self.documents[branch_id].get(document_id)

    def search(
        self,
        branch_id: str,
        query_text: str,
        query_embedding: Sequence[float],
        *,
        limit: int,
    ) -> list[SearchHit]:
        hits: list[SearchHit] = []
        for indexed in self.documents[branch_id].values():
            for chunk in indexed.chunks:
                if query_text.lower() not in chunk.text.lower():
                    continue
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
        self,
        branch_id: str,
        path: str,
        content: bytes,
        *,
        operation_id: str,
    ) -> None:
        self.operation_ids.append(operation_id)
        self.files[branch_id][path] = content

    def delete_file(
        self,
        branch_id: str,
        path: str,
        *,
        operation_id: str,
    ) -> bool:
        self.operation_ids.append(operation_id)
        return self.files[branch_id].pop(path, None) is not None

    def read_file(self, branch_id: str, path: str) -> bytes:
        return self.files[branch_id][path]

    def diff(self, source_branch: str, target_branch: str) -> dict[str, Any]:
        source = set(self.documents[source_branch])
        target = set(self.documents[target_branch])
        return {
            "added": sorted(source - target),
            "deleted": sorted(target - source),
        }

    def merge(
        self,
        source_branch: str,
        target_branch: str,
        *,
        operation_id: str,
    ) -> dict[str, Any]:
        self.operation_ids.append(operation_id)
        self.documents[target_branch] = dict(self.documents[source_branch])
        self.files[target_branch] = dict(self.files[source_branch])
        return {"merged": len(self.documents[source_branch])}

    def state_digest(self, branch_id: str) -> str:
        state = {
            key: value.as_dict()
            for key, value in sorted(self.documents[branch_id].items())
        }
        return hashlib.sha256(canonical_json(state).encode()).hexdigest()

    def close(self) -> None:
        pass


def _large_indexed_document() -> IndexedDocument:
    content = "runtime scheduling and batching\n" * 300
    document = KnowledgeDocument(
        id="runtime-rfc",
        path="/documents/confluence/runtime-rfc.md",
        title="Runtime scheduling RFC",
        source="confluence",
        content=content,
        metadata={"project": "runtime"},
    )
    chunk = DocumentChunk(
        id="runtime-rfc:0",
        document_id=document.id,
        ordinal=0,
        text=content,
        embedding=tuple(float(index) / 100 for index in range(512)),
    )
    return IndexedDocument(document, (chunk,))


def test_trace_replays_mutations_and_reads_across_backends(
    tmp_path: Path,
) -> None:
    source = MemoryBackend("source")
    recorder = TraceRecorder(
        OperationExecutor(source),
        tmp_path / "trace",
        trace_id="enterprise-task",
    )
    indexed = _large_indexed_document()

    recorder.execute(
        "document_put",
        branch_id="main",
        arguments={"indexed_document": indexed.as_dict()},
    )
    recorder.execute(
        "branch_create",
        branch_id="runtime-team",
        arguments={"parent_branch": "main"},
    )
    recorder.execute(
        "file_write",
        branch_id="runtime-team",
        arguments={
            "path": "/artifacts/analysis.md",
            "content": b"candidate analysis",
        },
    )
    recorder.execute(
        "search",
        branch_id="runtime-team",
        arguments={
            "query_text": "scheduling",
            "query_embedding": [0.0] * 512,
            "limit": 5,
        },
    )
    source_digest = recorder.execute(
        "state_digest",
        branch_id="runtime-team",
    )

    target = MemoryBackend("target")
    report = TraceReplayer(
        OperationExecutor(target),
        tmp_path / "trace",
    ).replay()

    assert report.matched
    assert report.operations == 5
    assert report.mutations == 3
    assert report.reads == 2
    assert target.state_digest("runtime-team") == source_digest["digest"]
    assert (
        target.read_file("runtime-team", "/artifacts/analysis.md")
        == b"candidate analysis"
    )
    assert target.operation_ids == [
        "enterprise-task:00000000",
        "enterprise-task:00000002",
    ]
    assert any((tmp_path / "trace" / "blobs").rglob("*"))


def test_trace_can_replay_only_mutations(tmp_path: Path) -> None:
    source = MemoryBackend("source")
    recorder = TraceRecorder(
        OperationExecutor(source),
        tmp_path / "trace",
        trace_id="mutations-only",
    )
    indexed = _large_indexed_document()
    recorder.execute(
        "document_put",
        branch_id="main",
        arguments={"indexed_document": indexed.as_dict()},
    )
    recorder.execute(
        "search",
        branch_id="main",
        arguments={
            "query_text": "runtime",
            "query_embedding": [0.0] * 512,
            "limit": 5,
        },
    )

    target = MemoryBackend("target")
    report = TraceReplayer(
        OperationExecutor(target),
        tmp_path / "trace",
    ).replay(include_reads=False)

    assert report.operations == 1
    assert report.mutations == 1
    assert report.reads == 0
    assert target.get_document("main", "runtime-rfc") is not None
