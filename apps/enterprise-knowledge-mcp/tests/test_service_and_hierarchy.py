from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from chronos_enterprise_knowledge.backends import ChronosKnowledgeBackend
from chronos_enterprise_knowledge.embedding import HashEmbedder
from chronos_enterprise_knowledge.hierarchy import (
    HierarchyBuilder,
    load_hierarchy,
    load_tasks,
)
from chronos_enterprise_knowledge.ingestion import KnowledgeIngestor
from chronos_enterprise_knowledge.mcp_server import create_mcp_server
from chronos_enterprise_knowledge.service import KnowledgeService


def test_curated_tasks_target_real_person_branches() -> None:
    config = load_hierarchy()
    people = {
        f"person/{member['name'].casefold().replace(' ', '-')}"
        for department in config["departments"]
        for team in department["teams"]
        for member in team["members"]
    }
    tasks = load_tasks()

    assert len(config["departments"]) == 3
    assert sum(len(item["teams"]) for item in config["departments"]) == 6
    assert len(tasks) == 10
    assert {task.branch for task in tasks} <= people
    assert all(task.updates for task in tasks)
    assert all(task.evidence_scopes for task in tasks)
    coding_tasks = tasks[:8]
    assert all(task.upstream_repository for task in coding_tasks)
    assert all(task.upstream_issue for task in coding_tasks)
    assert all(task.upstream_issue_url for task in coding_tasks)
    assert all(task.pinned_commit for task in coding_tasks)
    assert all(task.upstream_issue is None for task in tasks[8:])


def test_hierarchy_builds_inheritance_and_starting_knowledge(
    tmp_path: Path,
) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=32)
    ingestor = KnowledgeIngestor(backend, HashEmbedder(32))
    try:
        result = HierarchyBuilder(backend, ingestor).build(include_people=False)

        assert result["branches_created"] == 9
        assert len(backend.list_branches()) == 10
        hits = backend.search(
            "team/runtime-scheduling",
            "scheduler instrumentation benchmark evidence",
            HashEmbedder(32).embed(["scheduler instrumentation benchmark evidence"])[0],
            limit=10,
        )
        titles = {hit.title for hit in hits}
        assert "Runtime Scheduling knowledge brief" in titles
        assert "Engineering knowledge brief" in titles
        assert "Redwood Inference knowledge brief" in titles
    finally:
        backend.close()


def test_chronos_keeps_each_content_form_in_one_store(tmp_path: Path) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=16)
    service = KnowledgeService(backend, HashEmbedder(16))
    try:
        service.remember(
            "main",
            title="Validated scheduler policy",
            summary="Use deadline-aware admission after the latency gate.",
            kind="semantic_memory",
            evidence=["/knowledge/company/runbooks/scheduler.md"],
        )

        document_columns = {
            str(row["name"])
            for row in backend.sqlite.db.execute(
                "PRAGMA table_info(knowledge_documents)"
            ).fetchall()
        }
        chunk_columns = {
            str(row["name"])
            for row in backend.sqlite.db.execute(
                "PRAGMA table_info(knowledge_chunks)"
            ).fetchall()
        }
        assert "content" not in document_columns
        assert {"text", "embedding"}.isdisjoint(chunk_columns)

        chunk = backend.workspace.checkout("main").qdrant.list_points(
            "knowledge"
        )[0]
        assert "deadline-aware admission" in str(chunk.payload["text"])
        indexed = backend.get_document(
            "main",
            str(chunk.payload["document_id"]),
        )
        assert indexed is not None
        assert "deadline-aware admission" in indexed.document.content
    finally:
        backend.close()


def test_service_keeps_task_memory_and_artifacts_private(tmp_path: Path) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=32)
    service = KnowledgeService(backend, HashEmbedder(32))
    try:
        service.checkout("person/alice", from_branch="main", mount=False)
        service.checkout(
            "task/alice/incident",
            from_branch="person/alice",
            mount=False,
        )
        service.checkout(
            "task/alice/capacity",
            from_branch="person/alice",
            mount=False,
        )
        service.write_artifact(
            "task/alice/incident",
            "/artifacts/timeline.md",
            b"validated incident timeline",
        )
        memory = service.remember(
            "task/alice/incident",
            title="KV cache eviction incident signature",
            summary=(
                "A sharp increase in KV cache eviction accompanied the "
                "streaming latency regression."
            ),
            kind="semantic_memory",
            evidence=["/knowledge/company/incidents/INC-42.json"],
            owner="Alice",
            tags=["incident", "kv-cache"],
        )

        assert service.search(
            "task/alice/incident",
            "KV cache eviction",
            limit=5,
        )
        assert (
            service.search(
                "task/alice/capacity",
                "KV cache eviction",
                limit=5,
            )
            == []
        )
        assert (
            backend.read_file(
                "task/alice/incident",
                "/artifacts/timeline.md",
            )
            == b"validated incident timeline"
        )
        with pytest.raises(FileNotFoundError):
            backend.read_file(
                "task/alice/capacity",
                "/artifacts/timeline.md",
            )
        assert memory["kind"] == "semantic_memory"
    finally:
        backend.close()


def test_workspace_file_can_be_indexed_after_posix_checkout_write(
    tmp_path: Path,
) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=16)
    service = KnowledgeService(backend, HashEmbedder(16))
    try:
        checkout = service.checkout(
            "task/alice/posix-edit",
            from_branch="main",
            mount=True,
        )
        workspace_path = Path(checkout["workspace_path"])
        document_path = "/knowledge/notes/diagnosis.md"

        # Populate the native negative cache before the file is created
        # through the independently served FUSE checkout.
        with pytest.raises(FileNotFoundError):
            backend.read_file("task/alice/posix-edit", document_path)

        mounted_file = workspace_path / document_path.lstrip("/")
        mounted_file.parent.mkdir(parents=True)
        mounted_file.write_text("Validated diagnosis from the mounted workspace.\n")

        indexed = service.index_workspace_file(
            "task/alice/posix-edit",
            path=document_path,
            title="Validated diagnosis",
            source="agent workspace",
        )

        assert indexed["path"] == document_path
        assert service.search(
            "task/alice/posix-edit",
            "validated diagnosis",
            limit=5,
        )
    finally:
        backend.close()


def test_deleted_branch_name_can_be_checked_out_again(tmp_path: Path) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=16)
    service = KnowledgeService(backend, HashEmbedder(16))
    try:
        first = service.checkout(
            "task/alice/retry",
            from_branch="main",
            mount=True,
        )
        first_workspace = Path(first["workspace_path"])
        first_artifact = first_workspace / "artifacts/first-attempt.md"
        first_artifact.parent.mkdir(parents=True)
        first_artifact.write_text("discarded attempt\n", encoding="utf-8")

        service.delete_branch("task/alice/retry")

        second = service.checkout(
            "task/alice/retry",
            from_branch="main",
            mount=True,
        )
        second_workspace = Path(second["workspace_path"])
        second_artifact = second_workspace / "artifacts/second-attempt.md"
        second_artifact.parent.mkdir(parents=True)
        second_artifact.write_text("replacement attempt\n", encoding="utf-8")

        assert second_artifact.read_text(encoding="utf-8") == "replacement attempt\n"
        assert not (second_workspace / "artifacts/first-attempt.md").exists()
    finally:
        backend.close()


def test_semantic_memory_requires_evidence(tmp_path: Path) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=16)
    service = KnowledgeService(backend, HashEmbedder(16))
    try:
        with pytest.raises(ValueError, match="requires at least one evidence"):
            service.remember(
                "main",
                title="Unsupported fact",
                summary="This should not be retained.",
                kind="semantic_memory",
            )
    finally:
        backend.close()


def test_new_memory_can_supersede_outdated_memory(tmp_path: Path) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=16)
    service = KnowledgeService(backend, HashEmbedder(16))
    try:
        old = service.remember(
            "main",
            title="Customer deployment region",
            summary="The customer plans to deploy in us-east.",
            kind="semantic_memory",
            evidence=["gmail/account/old-plan.json"],
            memory_id="memory_old_region",
        )
        new = service.remember(
            "main",
            title="Customer deployment region",
            summary="The approved production region is eu-west.",
            kind="semantic_memory",
            evidence=["fireflies/account/architecture-review.json"],
            supersedes=[old["memory_id"]],
            memory_id="memory_current_region",
        )

        assert backend.get_document("main", old["memory_id"]) is None
        assert backend.get_document("main", new["memory_id"]) is not None
        hits = service.search("main", "approved production region", limit=5)
        assert hits[0].metadata["confidence"] == 1.0
        assert hits[0].metadata["document_kind"] == "semantic_memory"
    finally:
        backend.close()


def test_mcp_exposes_simple_branch_knowledge_tools(tmp_path: Path) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=16)
    try:
        server = create_mcp_server(KnowledgeService(backend, HashEmbedder(16)))
        tools = {tool.name for tool in server._tool_manager.list_tools()}
        assert {
            "knowledge_checkout",
            "knowledge_search",
            "knowledge_get_document",
            "knowledge_update_document",
            "knowledge_index_workspace_file",
            "knowledge_write_artifact",
            "knowledge_remember",
            "knowledge_diff",
            "knowledge_merge_preview",
            "knowledge_merge",
            "knowledge_delete_branch",
        } <= tools
    finally:
        backend.close()


def test_mcp_starts_backend_before_serving(tmp_path: Path) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=16)
    started = 0

    def start() -> None:
        nonlocal started
        started += 1

    backend.start = start  # type: ignore[method-assign]
    try:
        server = create_mcp_server(KnowledgeService(backend, HashEmbedder(16)))

        async def enter_lifespan() -> None:
            async with server._mcp_server.lifespan(server._mcp_server):
                assert started == 1

        asyncio.run(enter_lifespan())
    finally:
        backend.close()


def test_document_update_cannot_bypass_memory_policy(tmp_path: Path) -> None:
    backend = ChronosKnowledgeBackend(tmp_path, vector_dimensions=16)
    service = KnowledgeService(backend, HashEmbedder(16))
    try:
        with pytest.raises(ValueError, match="remember"):
            service.update_document(
                "main",
                path="/memory/unverified.md",
                title="Unverified claim",
                content="This hypothesis has no evidence.",
                source="agent",
                kind="semantic_memory",
            )
    finally:
        backend.close()
