from __future__ import annotations

from pathlib import Path

from chronos_enterprise_knowledge.backends import ChronosKnowledgeBackend
from chronos_enterprise_knowledge.models import (
    DocumentChunk,
    IndexedDocument,
    KnowledgeDocument,
)


def _indexed(
    document_id: str,
    content: str,
    embedding: tuple[float, float, float],
    *,
    title: str = "Runbook",
    path: str | None = None,
) -> IndexedDocument:
    document = KnowledgeDocument(
        id=document_id,
        path=path or f"/knowledge/{document_id}.md",
        title=title,
        source="test",
        content=content,
    )
    chunk = DocumentChunk(
        id=f"{document_id}:0",
        document_id=document_id,
        ordinal=0,
        text=content,
        embedding=embedding,
    )
    return IndexedDocument(document, (chunk,))


def test_document_state_is_isolated_across_sibling_branches(tmp_path: Path) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=3)
    try:
        root = _indexed(
            "deploy",
            "Production deployment uses a canary and automatic rollback.",
            (1.0, 0.0, 0.0),
        )
        backend.put_document("main", root, operation_id="seed:deploy")
        backend.create_branch("runtime", "main")
        backend.create_branch("platform", "main")

        runtime = _indexed(
            "deploy",
            "Runtime deployment uses a ten percent canary before promotion.",
            (0.0, 1.0, 0.0),
        )
        backend.put_document("runtime", runtime, operation_id="runtime:edit")

        assert (
            backend.get_document("main", "deploy").document.content
            == root.document.content
        )
        assert (
            backend.get_document("platform", "deploy").document.content
            == root.document.content
        )
        assert (
            backend.get_document("runtime", "deploy").document.content
            == runtime.document.content
        )

        root_hits = backend.search(
            "main",
            "automatic rollback",
            (1.0, 0.0, 0.0),
            limit=5,
        )
        runtime_hits = backend.search(
            "runtime",
            "ten percent canary",
            (0.0, 1.0, 0.0),
            limit=5,
        )
        assert [hit.document_id for hit in root_hits] == ["deploy"]
        assert root_hits[0].text == root.document.content
        assert [hit.document_id for hit in runtime_hits] == ["deploy"]
        assert runtime_hits[0].text == runtime.document.content
    finally:
        backend.close()


def test_sibling_updates_with_same_operation_id_keep_distinct_vectors(
    tmp_path: Path,
) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=3)
    try:
        backend.create_branch("candidate-a", "main")
        backend.create_branch("candidate-b", "main")
        candidate_a = _indexed(
            "shared-document",
            "Candidate A changes the scheduler capacity.",
            (1.0, 0.0, 0.0),
        )
        candidate_b = _indexed(
            "shared-document",
            "Candidate B grows the scheduler buffers on demand.",
            (0.0, 1.0, 0.0),
        )

        # Workspace indexing derives the operation id from the stable document
        # id, so sibling branches intentionally use the same value here.
        operation_id = "document:shared-document"
        backend.put_document(
            "candidate-a",
            candidate_a,
            operation_id=operation_id,
        )
        backend.put_document(
            "candidate-b",
            candidate_b,
            operation_id=operation_id,
        )

        loaded_a = backend.get_document("candidate-a", "shared-document")
        loaded_b = backend.get_document("candidate-b", "shared-document")
        assert loaded_a is not None
        assert loaded_b is not None
        assert loaded_a.chunks[0].text == candidate_a.chunks[0].text
        assert loaded_b.chunks[0].text == candidate_b.chunks[0].text

        backend.merge("candidate-a", "main", operation_id="merge:candidate-a")
        merged = backend.get_document("main", "shared-document")
        assert merged is not None
        assert merged.chunks[0].text == candidate_a.chunks[0].text
    finally:
        backend.close()


def test_delete_is_private_and_removes_all_document_state(tmp_path: Path) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=3)
    try:
        indexed = _indexed(
            "incident",
            "Escalate a region-wide outage to the incident commander.",
            (1.0, 0.0, 0.0),
        )
        backend.put_document("main", indexed, operation_id="seed:incident")
        backend.create_branch("support", "main")

        assert backend.delete_document(
            "support",
            "incident",
            operation_id="support:delete",
        )
        assert backend.get_document("support", "incident") is None
        assert (
            backend.search(
                "support",
                "incident commander",
                (1.0, 0.0, 0.0),
                limit=5,
            )
            == []
        )

        inherited = backend.get_document("main", "incident")
        assert inherited is not None
        assert inherited.document.content == indexed.document.content
        assert backend.search(
            "main",
            "incident commander",
            (1.0, 0.0, 0.0),
            limit=5,
        )
    finally:
        backend.close()


def test_generated_artifacts_are_branch_local(tmp_path: Path) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=3)
    try:
        backend.write_file(
            "main",
            "/artifacts/company-policy.txt",
            b"company",
            operation_id="seed:artifact",
        )
        backend.create_branch("task-a", "main")
        backend.create_branch("task-b", "main")
        backend.write_file(
            "task-a",
            "/artifacts/report.md",
            b"task-a result",
            operation_id="task-a:report",
        )
        backend.write_file(
            "task-b",
            "/artifacts/report.md",
            b"task-b result",
            operation_id="task-b:report",
        )

        assert backend.read_file("task-a", "/artifacts/report.md") == b"task-a result"
        assert backend.read_file("task-b", "/artifacts/report.md") == b"task-b result"
        assert (
            backend.read_file("task-a", "/artifacts/company-policy.txt") == b"company"
        )
    finally:
        backend.close()


def test_merge_promotes_coordinated_document_and_file_state(tmp_path: Path) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=3)
    try:
        backend.create_branch("draft", "main")
        indexed = _indexed(
            "oncall",
            "The primary on-call acknowledges P0 incidents within five minutes.",
            (0.0, 0.0, 1.0),
        )
        backend.put_document("draft", indexed, operation_id="draft:oncall")
        backend.write_file(
            "draft",
            "/artifacts/oncall-review.md",
            b"approved",
            operation_id="draft:review",
        )

        result = backend.merge("draft", "main", operation_id="merge:draft")

        assert {"filesystem", "qdrant", "sqlite"} <= set(result)
        merged = backend.get_document("main", "oncall")
        assert merged is not None
        assert merged.document.content == indexed.document.content
        assert backend.read_file("main", "/artifacts/oncall-review.md") == b"approved"
        assert backend.search(
            "main",
            "five minutes",
            (0.0, 0.0, 1.0),
            limit=5,
        )
    finally:
        backend.close()


def test_backend_reopens_with_identical_visible_state(tmp_path: Path) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=3)
    indexed = _indexed(
        "security",
        "Rotate production credentials after a confirmed compromise.",
        (1.0, 0.0, 0.0),
    )
    backend.put_document("main", indexed, operation_id="seed:security")
    backend.create_branch("security-team", "main")
    backend.write_file(
        "security-team",
        "/memory/findings.md",
        b"credential rotation owner: security",
        operation_id="memory:security",
    )
    before = backend.state_digest("security-team")
    backend.close()

    reopened = ChronosKnowledgeBackend(tmp_path, vector_dimensions=3)
    try:
        assert reopened.state_digest("security-team") == before
        loaded = reopened.get_document("security-team", "security")
        assert loaded is not None
        assert loaded.document.content == indexed.document.content
        assert (
            reopened.read_file("security-team", "/memory/findings.md")
            == b"credential rotation owner: security"
        )
    finally:
        reopened.close()


def test_placeholder_vectors_use_lexical_search_and_branch_visibility(
    tmp_path: Path,
) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=3)
    try:
        backend.set_placeholder_vector_mode(True)
        root = _indexed(
            "release",
            "The release runbook requires a cobalt approval token.",
            (0.0, 0.0, 0.0),
        )
        backend.put_document("main", root, operation_id="seed:release")
        backend.create_branch("draft", "main")

        inherited = backend.search(
            "draft",
            "cobalt approval",
            (0.0, 0.0, 0.0),
            limit=5,
        )
        assert [hit.document_id for hit in inherited] == ["release"]

        revised = _indexed(
            "release",
            "The draft runbook requires a saffron review token.",
            (0.0, 0.0, 0.0),
        )
        backend.put_document("draft", revised, operation_id="draft:release")

        assert backend.search(
            "draft",
            "cobalt approval",
            (0.0, 0.0, 0.0),
            limit=5,
        ) == []
        updated = backend.search(
            "draft",
            "saffron review",
            (0.0, 0.0, 0.0),
            limit=5,
        )
        assert [hit.document_id for hit in updated] == ["release"]
        loaded = backend.get_document("draft", "release")
        assert loaded is not None
        assert loaded.chunks[0].embedding == (0.0, 0.0, 0.0)
    finally:
        backend.close()


def test_chronos_indexes_chunks_by_document(tmp_path: Path) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=8)
    try:
        indexes = {
            index.name: index
            for index in backend.sqlite.list_indexes("knowledge_chunks")
        }
        assert indexes["knowledge_chunks_by_document"].columns == (
            "document_id",
        )
    finally:
        backend.close()


def test_diff_is_change_proportional_for_documents_and_files(
    tmp_path: Path,
) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=3)
    try:
        original = _indexed(
            "deploy",
            "Production deployment uses a canary.",
            (1.0, 0.0, 0.0),
        )
        backend.put_document("main", original, operation_id="seed:deploy")
        backend.write_file(
            "main",
            "/artifacts/existing.md",
            b"before\n",
            operation_id="seed:existing",
        )
        backend.write_file(
            "main",
            "/artifacts/remove.md",
            b"remove me\n",
            operation_id="seed:remove",
        )
        backend.create_branch("task", "main")

        revised = _indexed(
            "deploy",
            "Production deployment uses two canary stages.",
            (0.0, 1.0, 0.0),
        )
        backend.put_document("task", revised, operation_id="task:deploy")
        backend.write_file(
            "task",
            "/artifacts/existing.md",
            b"after\n",
            operation_id="task:existing",
        )
        backend.write_file(
            "task",
            "/artifacts/new.md",
            b"new\n",
            operation_id="task:new",
        )
        assert backend.delete_file(
            "task",
            "/artifacts/remove.md",
            operation_id="task:remove",
        )

        # A sparse diff must not fall back to walking the complete filesystem.
        backend._walk_files = lambda *_args: (_ for _ in ()).throw(  # type: ignore[method-assign]
            AssertionError("full filesystem walk")
        )
        original_diff_rows = backend.filesystem.context.diff_rows

        def sparse_file_diff(left: str, right: str, table: str):
            if table == "chronosfs_file_blocks":
                raise AssertionError("block-level filesystem diff")
            return original_diff_rows(left, right, table)

        backend.filesystem.context.diff_rows = sparse_file_diff  # type: ignore[method-assign]
        result = backend.diff("task", "main")

        assert result["documents"] == {
            "added": [],
            "deleted": [],
            "modified": ["deploy"],
        }
        assert result["files"] == {
            "added": ["/artifacts/new.md"],
            "deleted": ["/artifacts/remove.md"],
            "modified": [
                "/artifacts/existing.md",
                "/knowledge/deploy.md",
            ],
        }
    finally:
        backend.close()
