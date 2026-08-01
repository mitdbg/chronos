from __future__ import annotations

from pathlib import Path

import pytest
from chronos_enterprise_knowledge.backend import OperationExecutor
from chronos_enterprise_knowledge.backends import (
    ApplicationManagedKnowledgeBackend,
    ChronosKnowledgeBackend,
    PhysicalCloneKnowledgeBackend,
)
from chronos_enterprise_knowledge.embedding import HashEmbedder
from chronos_enterprise_knowledge.hierarchy import HierarchyBuilder
from chronos_enterprise_knowledge.ingestion import KnowledgeIngestor
from chronos_enterprise_knowledge.models import (
    DocumentChunk,
    IndexedDocument,
    KnowledgeDocument,
    indexed_document_digest,
)
from chronos_enterprise_knowledge.trace import TraceRecorder, TraceReplayer


def _indexed(
    document_id: str,
    content: str,
    vector: tuple[float, float, float],
) -> IndexedDocument:
    document = KnowledgeDocument(
        id=document_id,
        path=f"/knowledge/{document_id}.md",
        title=document_id.replace("-", " ").title(),
        source="test",
        content=content,
        metadata={"owner": "runtime"},
    )
    return IndexedDocument(
        document,
        (
            DocumentChunk(
                id=f"{document_id}:0",
                document_id=document_id,
                ordinal=0,
                text=content,
                embedding=vector,
                metadata={"section": "body"},
            ),
        ),
    )


def test_logical_digest_ignores_backend_vector_rounding() -> None:
    precise = _indexed(
        "scheduler",
        "The scheduler uses continuous batching.",
        (0.042010270059108734, 0.016501853242516518, 0.027844015508890152),
    )
    rounded = _indexed(
        "scheduler",
        "The scheduler uses continuous batching.",
        (0.0419921875, 0.0164947509765625, 0.02783203125),
    )

    assert indexed_document_digest(precise) == indexed_document_digest(rounded)


def test_application_backend_applies_configured_sqlite_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHRONOS_APP_SQLITE_CACHE_SIZE_KIB", "4096")
    backend = ApplicationManagedKnowledgeBackend(
        tmp_path,
        vector_dimensions=3,
    )
    try:
        assert backend._db.execute(  # noqa: SLF001 - configuration invariant
            "PRAGMA cache_size"
        ).fetchone()[0] == -4096
    finally:
        backend.close()


@pytest.mark.parametrize(
    "backend_type",
    [
        ApplicationManagedKnowledgeBackend,
        PhysicalCloneKnowledgeBackend,
    ],
)
def test_comparison_backends_preserve_branch_isolation(
    tmp_path: Path,
    backend_type: type[
        ApplicationManagedKnowledgeBackend | PhysicalCloneKnowledgeBackend
    ],
) -> None:
    backend = backend_type(tmp_path, vector_dimensions=3)
    try:
        original = _indexed(
            "scheduler",
            "The scheduler uses continuous batching.",
            (1.0, 0.0, 0.0),
        )
        backend.put_document("main", original, operation_id="seed")
        backend.create_branch("team-a", "main")
        backend.create_branch("team-b", "main")

        revised = _indexed(
            "scheduler",
            "Team A validates deadline-aware batching.",
            (0.0, 1.0, 0.0),
        )
        backend.put_document("team-a", revised, operation_id="team-a:edit")
        assert backend.delete_document(
            "team-b",
            "scheduler",
            operation_id="team-b:delete",
        )

        assert (
            backend.get_document("main", "scheduler").document.content
            == original.document.content
        )
        assert (
            backend.get_document("team-a", "scheduler").document.content
            == revised.document.content
        )
        assert backend.get_document("team-b", "scheduler") is None
        hits = backend.search(
            "team-a",
            "deadline aware batching",
            (0.0, 1.0, 0.0),
            limit=5,
        )
        assert hits[0].document_id == "scheduler"
        assert (
            backend.search(
                "team-b",
                "continuous batching",
                (1.0, 0.0, 0.0),
                limit=5,
            )
            == []
        )
    finally:
        backend.close()


def test_clone_materializes_while_overlay_records_only_changes(
    tmp_path: Path,
) -> None:
    overlay = ApplicationManagedKnowledgeBackend(
        tmp_path / "overlay",
        vector_dimensions=3,
    )
    clone = PhysicalCloneKnowledgeBackend(
        tmp_path / "clone",
        vector_dimensions=3,
    )
    try:
        for backend in (overlay, clone):
            for index in range(3):
                backend.put_document(
                    "main",
                    _indexed(
                        f"doc-{index}",
                        f"Company document {index}.",
                        (1.0, 0.0, 0.0),
                    ),
                    operation_id=f"seed:{index}",
                )
            backend.create_branch("team", "main")

        overlay_stats = overlay.storage_stats()
        clone_stats = clone.storage_stats()
        assert overlay_stats["document_rows"] == 3
        assert clone_stats["document_rows"] == 6
        assert overlay_stats["chunk_rows"] == 3
        assert clone_stats["chunk_rows"] == 6
        assert clone_stats["file_bytes"] == 2 * overlay_stats["file_bytes"]
    finally:
        overlay.close()
        clone.close()


@pytest.mark.parametrize(
    "backend_type",
    [
        ApplicationManagedKnowledgeBackend,
        PhysicalCloneKnowledgeBackend,
    ],
)
def test_comparison_backend_omits_staged_zero_vectors(
    tmp_path: Path,
    backend_type: type[
        ApplicationManagedKnowledgeBackend | PhysicalCloneKnowledgeBackend
    ],
) -> None:
    state = tmp_path / backend_type.__name__
    backend = backend_type(state, vector_dimensions=3)
    try:
        backend.set_placeholder_vector_mode(True)
        indexed = _indexed(
            "scheduler",
            "Deadline-aware runtime scheduling.",
            (0.0, 0.0, 0.0),
        )
        backend.put_document("main", indexed, operation_id="seed")

        document_row = backend._db.execute(  # noqa: SLF001 - storage invariant test
            """
            SELECT id, path, title, source, kind, content_hash, metadata_json
            FROM document_versions
            WHERE branch_id = 'main' AND id = 'scheduler'
            """
        ).fetchone()
        chunk_row = backend._db.execute(  # noqa: SLF001 - storage invariant test
            """
            SELECT id, document_id, ordinal, content_hash,
                   point_id, metadata_json
            FROM chunk_versions
            WHERE branch_id = 'main' AND document_id = 'scheduler'
            """
        ).fetchone()
        assert document_row is not None
        assert chunk_row is not None
        assert set(document_row.keys()) == {
            "id", "path", "title", "source", "kind",
            "content_hash", "metadata_json",
        }
        assert set(chunk_row.keys()) == {
            "id", "document_id", "ordinal", "content_hash",
            "point_id", "metadata_json",
        }
        assert "Deadline-aware runtime scheduling." not in str(tuple(document_row))
        assert "Deadline-aware runtime scheduling." not in str(tuple(chunk_row))
        assert (
            backend.get_document("main", "scheduler").chunks[0].embedding
            == (0.0, 0.0, 0.0)
        )
        assert backend.search(
            "main",
            "deadline runtime",
            (0.0, 0.0, 0.0),
            limit=3,
        )[0].document_id == "scheduler"
    finally:
        backend.close()

    reopened = backend_type(state, vector_dimensions=3)
    try:
        assert (
            reopened.get_document("main", "scheduler").chunks[0].embedding
            == (0.0, 0.0, 0.0)
        )
    finally:
        reopened.close()


def test_application_backend_persists_snapshot_ingestion_cursor(
    tmp_path: Path,
) -> None:
    backend = ApplicationManagedKnowledgeBackend(
        tmp_path,
        vector_dimensions=3,
    )
    try:
        assert backend.snapshot_ingestion_cursor("main", "snapshot-a") is None
        backend.set_snapshot_ingestion_cursor(
            "main",
            "snapshot-a",
            "sources/slack/record.json",
        )
    finally:
        backend.close()

    reopened = ApplicationManagedKnowledgeBackend(
        tmp_path,
        vector_dimensions=3,
    )
    try:
        assert reopened.snapshot_ingestion_cursor(
            "main",
            "snapshot-a",
        ) == "sources/slack/record.json"
    finally:
        reopened.close()


@pytest.mark.parametrize(
    "backend_type",
    [
        ApplicationManagedKnowledgeBackend,
        PhysicalCloneKnowledgeBackend,
    ],
)
def test_comparison_forks_keep_parent_state_at_fork_time(
    tmp_path: Path,
    backend_type: type[
        ApplicationManagedKnowledgeBackend | PhysicalCloneKnowledgeBackend
    ],
) -> None:
    backend = backend_type(tmp_path, vector_dimensions=3)
    try:
        before = _indexed(
            "policy",
            "Policy version at fork.",
            (1.0, 0.0, 0.0),
        )
        backend.put_document("main", before, operation_id="seed")
        backend.create_branch("child", "main")

        after = _indexed(
            "policy",
            "Policy changed after the fork.",
            (0.0, 1.0, 0.0),
        )
        backend.put_document("main", after, operation_id="parent:update")
        backend.write_file(
            "main",
            "/knowledge/late.md",
            b"created after fork\n",
            operation_id="parent:file",
        )

        assert (
            backend.get_document("child", "policy").document.content
            == before.document.content
        )
        with pytest.raises(FileNotFoundError):
            backend.read_file("child", "/knowledge/late.md")
    finally:
        backend.close()


def test_app_managed_root_file_cache_tracks_fork_cutoffs(
    tmp_path: Path,
) -> None:
    backend = ApplicationManagedKnowledgeBackend(
        tmp_path,
        vector_dimensions=3,
    )
    try:
        backend.write_file(
            "main",
            "/knowledge/original.md",
            b"original\n",
            operation_id="seed",
        )
        backend.create_branch("old-child", "main")
        assert (
            backend._effective_files("old-child")[
                "/knowledge/original.md"
            ]
            == b"original\n"
        )

        backend.write_file(
            "main",
            "/knowledge/new.md",
            b"new\n",
            operation_id="main:update",
        )
        backend.create_branch("new-child", "main")

        assert "/knowledge/new.md" not in backend._effective_files(
            "old-child"
        )
        assert (
            backend._effective_files("new-child")["/knowledge/new.md"]
            == b"new\n"
        )
    finally:
        backend.close()


@pytest.mark.parametrize(
    "backend_type",
    [
        ApplicationManagedKnowledgeBackend,
        PhysicalCloneKnowledgeBackend,
    ],
)
def test_comparison_merge_applies_source_delta_not_whole_snapshot(
    tmp_path: Path,
    backend_type: type[
        ApplicationManagedKnowledgeBackend | PhysicalCloneKnowledgeBackend
    ],
) -> None:
    backend = backend_type(tmp_path, vector_dimensions=3)
    try:
        backend.put_document(
            "main",
            _indexed("shared", "Shared baseline.", (1.0, 0.0, 0.0)),
            operation_id="seed",
        )
        backend.create_branch("candidate", "main")
        backend.put_document(
            "candidate",
            _indexed("shared", "Candidate revision.", (0.0, 1.0, 0.0)),
            operation_id="candidate:update",
        )
        backend.put_document(
            "main",
            _indexed("unrelated", "Independent main work.", (0.0, 0.0, 1.0)),
            operation_id="main:independent",
        )

        backend.merge("candidate", "main", operation_id="merge")

        assert (
            backend.get_document("main", "shared").document.content
            == "Candidate revision."
        )
        assert (
            backend.get_document("main", "unrelated").document.content
            == "Independent main work."
        )
    finally:
        backend.close()


@pytest.mark.parametrize(
    "backend_type",
    [
        ApplicationManagedKnowledgeBackend,
        PhysicalCloneKnowledgeBackend,
    ],
)
def test_nested_merge_carries_changes_since_common_ancestor(
    tmp_path: Path,
    backend_type: type[
        ApplicationManagedKnowledgeBackend | PhysicalCloneKnowledgeBackend
    ],
) -> None:
    backend = backend_type(tmp_path, vector_dimensions=3)
    try:
        backend.put_document(
            "main",
            _indexed("shared", "Initial state.", (1.0, 0.0, 0.0)),
            operation_id="seed",
        )
        backend.create_branch("team", "main")
        backend.put_document(
            "team",
            _indexed("team-note", "Team refinement.", (0.0, 1.0, 0.0)),
            operation_id="team:note",
        )
        backend.create_branch("person", "team")
        backend.put_document(
            "person",
            _indexed("person-note", "Personal refinement.", (0.0, 0.0, 1.0)),
            operation_id="person:note",
        )
        backend.create_branch("task", "person")
        backend.put_document(
            "task",
            _indexed("shared", "Task-approved state.", (0.0, 1.0, 0.0)),
            operation_id="task:update",
        )
        backend.put_document(
            "main",
            _indexed("main-note", "Independent main work.", (1.0, 0.0, 0.0)),
            operation_id="main:note",
        )

        backend.merge("task", "main", operation_id="merge:nested")

        assert {
            document_id: backend.get_document("main", document_id).document.content
            for document_id in (
                "shared",
                "team-note",
                "person-note",
                "main-note",
            )
        } == {
            "shared": "Task-approved state.",
            "team-note": "Team refinement.",
            "person-note": "Personal refinement.",
            "main-note": "Independent main work.",
        }
    finally:
        backend.close()


def test_app_managed_nested_diff_and_merge_do_not_materialize_corpus(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = ApplicationManagedKnowledgeBackend(
        tmp_path,
        vector_dimensions=3,
    )
    try:
        backend.put_document(
            "main",
            _indexed("shared", "Initial state.", (1.0, 0.0, 0.0)),
            operation_id="seed",
        )
        backend.create_branch("reproduction", "main")
        backend.put_document(
            "reproduction",
            _indexed("evidence", "Reproduction.", (0.0, 1.0, 0.0)),
            operation_id="reproduction:evidence",
        )
        backend.create_branch("fix", "reproduction")
        backend.put_document(
            "fix",
            _indexed("shared", "Fixed state.", (0.0, 0.0, 1.0)),
            operation_id="fix:update",
        )
        backend.put_document(
            "main",
            _indexed("unrelated", "Independent work.", (1.0, 0.0, 0.0)),
            operation_id="main:independent",
        )
        monkeypatch.setattr(
            backend,
            "_effective_documents",
            lambda *_args, **_kwargs: pytest.fail(
                "sparse branch comparison must not materialize the corpus"
            ),
        )
        monkeypatch.setattr(
            backend,
            "_effective_files",
            lambda *_args, **_kwargs: pytest.fail(
                "sparse branch comparison must not materialize the filesystem"
            ),
        )

        result = backend.diff("fix", "main")
        assert set(result["documents"]["added"]) == {"evidence"}
        assert set(result["documents"]["modified"]) == {"shared"}
        assert set(result["documents"]["deleted"]) == {"unrelated"}

        backend.merge("fix", "main", operation_id="merge:nested")
        assert (
            backend.get_document("main", "shared").document.content
            == "Fixed state."
        )
        assert (
            backend.get_document("main", "evidence").document.content
            == "Reproduction."
        )
        assert (
            backend.get_document("main", "unrelated").document.content
            == "Independent work."
        )
    finally:
        backend.close()


def test_app_managed_merge_rejects_stale_filesystem_snapshot(
    tmp_path: Path,
) -> None:
    backend = ApplicationManagedKnowledgeBackend(
        tmp_path,
        vector_dimensions=3,
    )
    try:
        backend.create_branch("first", "main")
        backend.create_branch("second", "main")
        backend.mount_branch("first")
        backend.mount_branch("second")
        backend.write_file(
            "first",
            "/artifacts/first.md",
            b"first candidate\n",
            operation_id="first:file",
        )
        backend.write_file(
            "second",
            "/artifacts/second.md",
            b"second candidate\n",
            operation_id="second:file",
        )

        backend.merge("first", "main", operation_id="merge:first")
        with pytest.raises(
            ValueError,
            match="target branch 'main' contains filesystem changes",
        ):
            backend.merge("second", "main", operation_id="merge:stale")

        assert (
            backend.read_file("main", "/artifacts/first.md")
            == b"first candidate\n"
        )
        with pytest.raises(FileNotFoundError):
            backend.read_file("main", "/artifacts/second.md")

        backend.delete_branch("second")
        backend.create_branch("second", "main")
        backend.mount_branch("second")
        backend.write_file(
            "second",
            "/artifacts/second.md",
            b"second candidate\n",
            operation_id="second:file:refreshed",
        )
        backend.merge("second", "main", operation_id="merge:refreshed")

        assert (
            backend.read_file("main", "/artifacts/second.md")
            == b"second candidate\n"
        )
    finally:
        backend.close()


@pytest.mark.parametrize(
    "backend_type",
    [
        ApplicationManagedKnowledgeBackend,
        PhysicalCloneKnowledgeBackend,
    ],
)
def test_comparison_branch_deletion_removes_the_subtree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    backend_type: type[
        ApplicationManagedKnowledgeBackend | PhysicalCloneKnowledgeBackend
    ],
) -> None:
    backend = backend_type(tmp_path, vector_dimensions=3)
    try:
        backend.create_branch("team", "main")
        backend.create_branch("person", "team")
        backend.create_branch("task", "person")
        backend.create_branch("sibling", "main")
        backend.put_document(
            "task",
            _indexed("private", "Task-private state.", (1.0, 0.0, 0.0)),
            operation_id="task:private",
        )
        stored = backend._effective_document("task", "private")
        assert stored is not None
        owner, revision, indexed = stored
        indexed_point_ids = [
            backend._point_id(owner, revision, chunk.id)
            for chunk in indexed.chunks
        ]
        assert backend._qdrant.retrieve(
            collection_name=backend._collection,
            ids=indexed_point_ids,
            with_payload=False,
            with_vectors=False,
        )
        monkeypatch.setattr(
            backend,
            "_synchronize_checkout",
            lambda _branch: pytest.fail(
                "deleting discarded state must not materialize its checkout"
            ),
        )

        backend.delete_branch("team")

        assert backend.list_branches() == ["main", "sibling"]
        assert not backend._qdrant.retrieve(
            collection_name=backend._collection,
            ids=indexed_point_ids,
            with_payload=False,
            with_vectors=False,
        )
        with pytest.raises(ValueError, match="unknown branch"):
            backend.get_document("task", "private")
    finally:
        backend.close()


@pytest.mark.parametrize(
    "backend_type",
    [
        ApplicationManagedKnowledgeBackend,
        PhysicalCloneKnowledgeBackend,
    ],
)
def test_comparison_checkout_imports_direct_agent_file_changes(
    tmp_path: Path,
    backend_type: type[
        ApplicationManagedKnowledgeBackend | PhysicalCloneKnowledgeBackend
    ],
) -> None:
    backend = backend_type(tmp_path, vector_dimensions=3)
    try:
        backend.write_file(
            "main",
            "/knowledge/company.md",
            b"company\n",
            operation_id="seed:file",
        )
        backend.create_branch("task", "main")
        checkout = backend.mount_branch("task")
        artifact = checkout / "artifacts" / "result.md"
        artifact.parent.mkdir(parents=True)
        artifact.write_text("validated result\n")

        digest_before_close = backend.state_digest("task")
        assert (
            backend.read_file(
                "task",
                "/artifacts/result.md",
            )
            == b"validated result\n"
        )
    finally:
        backend.close()

    reopened = backend_type(tmp_path, vector_dimensions=3)
    try:
        assert reopened.state_digest("task") == digest_before_close
        assert (
            reopened.read_file(
                "task",
                "/artifacts/result.md",
            )
            == b"validated result\n"
        )
    finally:
        reopened.close()


def test_app_managed_checkout_materializes_nested_visible_files(
    tmp_path: Path,
) -> None:
    backend = ApplicationManagedKnowledgeBackend(
        tmp_path / "state",
        vector_dimensions=3,
    )
    try:
        backend.write_file(
            "main",
            "/knowledge/company.md",
            b"company\n",
            operation_id="seed:company",
        )
        backend.write_file(
            "main",
            "/knowledge/replaced.md",
            b"old\n",
            operation_id="seed:replaced",
        )
        backend.write_file(
            "main",
            "/knowledge/removed.md",
            b"remove me\n",
            operation_id="seed:removed",
        )
        backend.create_branch("team", "main")
        backend.write_file(
            "team",
            "/knowledge/replaced.md",
            b"new\n",
            operation_id="team:replace",
        )
        backend.delete_file(
            "team",
            "/knowledge/removed.md",
            operation_id="team:remove",
        )
        backend.write_file(
            "team",
            "/knowledge/team.md",
            b"team\n",
            operation_id="team:add",
        )
        checkout = backend.mount_branch("team")

        assert (checkout / "knowledge/company.md").read_bytes() == b"company\n"
        assert (checkout / "knowledge/replaced.md").read_bytes() == b"new\n"
        assert (checkout / "knowledge/team.md").read_bytes() == b"team\n"
        assert not (checkout / "knowledge/removed.md").exists()
    finally:
        backend.close()


def test_app_managed_checkout_sync_skips_unchanged_file_contents(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = ApplicationManagedKnowledgeBackend(
        tmp_path / "state",
        vector_dimensions=3,
    )
    try:
        backend.write_file(
            "main",
            "/knowledge/company.md",
            b"company\n",
            operation_id="seed:company",
        )
        backend.create_branch("task", "main")
        checkout = backend.mount_branch("task")
        original_store = backend._store_file_row
        with monkeypatch.context() as patch:
            patch.setattr(
                backend,
                "_store_file_row",
                lambda *_args, **_kwargs: pytest.fail(
                    "unchanged checkout files must not be read and stored"
                ),
            )
            backend._synchronize_checkout("task")

        (checkout / "knowledge/company.md").write_bytes(b"updated\n")
        stored: list[tuple[str, bytes]] = []
        def record_store(
            branch: str,
            path: str,
            content: bytes,
            **kwargs: object,
        ) -> None:
            stored.append((path, content))
            original_store(branch, path, content, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(
                backend,
                "_store_file_row",
                record_store,
            )
            backend._synchronize_checkout("task")
        assert stored == [("/knowledge/company.md", b"updated\n")]
    finally:
        backend.close()


def test_app_managed_reuses_an_existing_branch_checkout(
    tmp_path: Path,
) -> None:
    backend = ApplicationManagedKnowledgeBackend(
        tmp_path / "state",
        vector_dimensions=3,
    )
    try:
        backend.create_branch("task", "main")
        first = backend.mount_branch("task")
        (first / "artifacts").mkdir()
        (first / "artifacts/result.md").write_text("result\n")

        second = backend.mount_branch("task")

        assert second == first
        assert (second / "artifacts/result.md").read_text() == "result\n"
        assert len(list((backend.state_dir / "checkouts").iterdir())) == 1
    finally:
        backend.close()


def test_app_managed_session_cleanup_ignores_external_symlinks(
    tmp_path: Path,
) -> None:
    backend = ApplicationManagedKnowledgeBackend(
        tmp_path / "state",
        vector_dimensions=3,
    )
    try:
        backend.create_branch("task", "main")
        checkout = backend.mount_branch("task")
        artifact = checkout / "artifacts" / "result.md"
        artifact.parent.mkdir(parents=True)
        artifact.write_text("validated\n")
        external = tmp_path / "external-python"
        external.write_text("#!/bin/sh\n")
        symlink = checkout / ".venv" / "bin" / "python"
        symlink.parent.mkdir(parents=True)
        symlink.symlink_to(external)

        backend.release_session_checkouts()

        assert not checkout.exists()
        assert (
            backend.read_file("task", "/artifacts/result.md")
            == b"validated\n"
        )
        with pytest.raises(FileNotFoundError):
            backend.read_file("task", "/.venv/bin/python")
    finally:
        backend.close()


def test_one_trace_reaches_identical_state_on_all_backends(
    tmp_path: Path,
) -> None:
    trace_dir = tmp_path / "trace"
    source = ChronosKnowledgeBackend(
        tmp_path / "chronos",
        vector_dimensions=3,
    )
    recorder = TraceRecorder(
        OperationExecutor(source),
        trace_dir,
        trace_id="cross-backend",
    )
    try:
        recorder.execute(
            "document_put",
            branch_id="main",
            arguments={
                "indexed_document": _indexed(
                    "runbook",
                    "Rollback after two failed canary checks.",
                    (1.0, 0.0, 0.0),
                ).as_dict()
            },
        )
        recorder.execute(
            "branch_create",
            branch_id="team/runtime",
            arguments={"parent_branch": "main"},
        )
        recorder.execute(
            "branch_create",
            branch_id="team/platform",
            arguments={"parent_branch": "main"},
        )
        recorder.execute(
            "document_put",
            branch_id="team/runtime",
            arguments={
                "indexed_document": _indexed(
                    "runbook",
                    "Runtime rolls back after one failed latency gate.",
                    (0.0, 1.0, 0.0),
                ).as_dict()
            },
        )
        recorder.execute(
            "file_write",
            branch_id="team/runtime",
            arguments={
                "path": "/artifacts/review.md",
                "content": b"approved runtime change\n",
            },
        )
        recorder.execute(
            "document_delete",
            branch_id="team/platform",
            arguments={"document_id": "runbook"},
        )
        recorder.execute(
            "search",
            branch_id="team/runtime",
            arguments={
                "query_text": "latency gate",
                "query_embedding": [0.0, 1.0, 0.0],
                "limit": 5,
            },
        )
        recorder.execute(
            "branch_diff",
            arguments={
                "source_branch": "team/runtime",
                "target_branch": "main",
            },
        )
        recorder.execute(
            "state_digest",
            branch_id="team/runtime",
        )
        recorder.execute(
            "branch_merge",
            arguments={
                "source_branch": "team/runtime",
                "target_branch": "main",
            },
        )
        recorder.execute(
            "branch_delete",
            branch_id="team/platform",
        )
        expected = {
            branch: source.state_digest(branch) for branch in ("main", "team/runtime")
        }
    finally:
        source.close()

    for name, backend_type in (
        ("chronos-replay", ChronosKnowledgeBackend),
        ("app", ApplicationManagedKnowledgeBackend),
        ("clone", PhysicalCloneKnowledgeBackend),
    ):
        target = backend_type(
            tmp_path / name,
            vector_dimensions=3,
        )
        try:
            report = TraceReplayer(
                OperationExecutor(target),
                trace_dir,
            ).replay()
            assert report.matched
            assert report.mutations == 8
            assert report.reads == 3
            assert len(report.timings) == report.operations
            assert set(report.latency_summary()) == {
                "branch_create",
                "branch_delete",
                "branch_diff",
                "branch_merge",
                "document_delete",
                "document_put",
                "file_write",
                "search",
                "state_digest",
            }
            assert {
                branch: target.state_digest(branch) for branch in expected
            } == expected
        finally:
            target.close()


@pytest.mark.parametrize(
    "backend_type",
    [
        ApplicationManagedKnowledgeBackend,
        PhysicalCloneKnowledgeBackend,
    ],
)
def test_curated_hierarchy_is_backend_independent(
    tmp_path: Path,
    backend_type: type[
        ApplicationManagedKnowledgeBackend | PhysicalCloneKnowledgeBackend
    ],
) -> None:
    backend = backend_type(tmp_path, vector_dimensions=16)
    try:
        result = HierarchyBuilder(
            backend,
            KnowledgeIngestor(backend, HashEmbedder(16)),
        ).build(include_people=False)

        assert result == {
            "branches_created": 9,
            "briefs_written": 10,
            "branches_total": 10,
        }
        hits = backend.search(
            "team/runtime-scheduling",
            "company portfolio scheduler instrumentation",
            HashEmbedder(16).embed(["company portfolio scheduler instrumentation"])[0],
            limit=10,
        )
        titles = {hit.title for hit in hits}
        assert "Redwood Inference knowledge brief" in titles
        assert "Engineering knowledge brief" in titles
        assert "Runtime Scheduling knowledge brief" in titles
    finally:
        backend.close()
