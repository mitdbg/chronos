"""Codex-facing MCP tools for branch-aware enterprise knowledge and memory."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Literal

from mcp.server.fastmcp import FastMCP

from chronos_enterprise_knowledge.service import KnowledgeService

_INSTRUCTIONS = """
Use this server as the durable company knowledge and task workspace.

Start a task by calling knowledge_checkout with a unique task branch and its
person or team branch as from_branch. The returned workspace_path is a
ChronosFS checkout containing inherited documents plus branch-local artifacts.
Every knowledge operation takes an explicit branch_id; do not mix task
branches between concurrent agent sessions.

Use knowledge_search before answering company-specific questions and cite the
returned paths and document IDs. Keep provisional work under /artifacts in the
mounted workspace. Call knowledge_index_workspace_file only when a file should
become searchable knowledge. At task completion, persist only validated facts,
the completed outcome, or a reusable procedure with knowledge_remember. Raw
conversation turns and unsupported hypotheses are not durable memory.
When new evidence changes a durable memory, write the replacement with
supersedes instead of retaining contradictory entries.
""".strip()


def create_mcp_server(
    service: KnowledgeService,
    *,
    name: str = "chronos-enterprise-knowledge",
    host: str = "127.0.0.1",
    port: int = 8000,
) -> FastMCP:
    @asynccontextmanager
    async def lifespan(_server: FastMCP):
        service.start()
        yield None

    mcp = FastMCP(
        name,
        instructions=_INSTRUCTIONS,
        host=host,
        port=port,
        lifespan=lifespan,
    )

    @mcp.tool()
    def knowledge_status() -> dict[str, Any]:
        """List storage components, embedding model, and available branches."""
        return service.status()

    @mcp.tool()
    def knowledge_checkout(
        branch_id: str,
        from_branch: str | None = None,
        mount: bool = True,
        mount_path: str | None = None,
    ) -> dict[str, Any]:
        """Open an isolated branch, creating it from a parent when needed.

        With mount=true, returns a workspace_path where the agent can read
        inherited documents and safely create or modify task artifacts.
        """
        return service.checkout(
            branch_id,
            from_branch=from_branch,
            mount=mount,
            mount_path=mount_path,
        )

    @mcp.tool()
    def knowledge_search(
        branch_id: str,
        query: str,
        limit: int = 8,
    ) -> list[dict[str, Any]]:
        """Hybrid-search branch-visible company knowledge and agent memory."""
        return [
            hit.as_dict()
            for hit in service.search(
                branch_id,
                query,
                limit=limit,
            )
        ]

    @mcp.tool()
    def knowledge_get_document(
        branch_id: str,
        document_id: str,
    ) -> dict[str, Any] | None:
        """Fetch one complete branch-visible document and its indexed chunks."""
        return service.get_document(branch_id, document_id)

    @mcp.tool()
    def knowledge_update_document(
        branch_id: str,
        path: str,
        title: str,
        content: str,
        source: str,
        kind: Literal["source", "curated"] = "curated",
        document_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Atomically update a document, its metadata, and its embeddings."""
        return service.update_document(
            branch_id,
            path=path,
            title=title,
            content=content,
            source=source,
            kind=kind,
            document_id=document_id,
            metadata=metadata,
        )

    @mcp.tool()
    def knowledge_index_workspace_file(
        branch_id: str,
        path: str,
        title: str,
        source: str,
        kind: Literal["source", "curated"] = "curated",
        document_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make an existing UTF-8 ChronosFS file searchable in this branch."""
        return service.index_workspace_file(
            branch_id,
            path=path,
            title=title,
            source=source,
            kind=kind,
            document_id=document_id,
            metadata=metadata,
        )

    @mcp.tool()
    def knowledge_delete_document(branch_id: str, document_id: str) -> dict[str, Any]:
        """Remove a document, chunks, file, and embeddings from this branch."""
        return {
            "document_id": document_id,
            "deleted": service.delete_document(branch_id, document_id),
        }

    @mcp.tool()
    def knowledge_write_artifact(
        branch_id: str,
        path: str,
        content: str,
    ) -> dict[str, Any]:
        """Write a task artifact to ChronosFS without adding it to retrieval."""
        return service.write_artifact(branch_id, path, content.encode())

    @mcp.tool()
    def knowledge_remember(
        branch_id: str,
        title: str,
        summary: str,
        kind: Literal["semantic_memory", "episodic_memory", "playbook"],
        evidence: list[str] | None = None,
        confidence: float = 1.0,
        owner: str | None = None,
        tags: list[str] | None = None,
        outcome: str | None = None,
        supersedes: list[str] | None = None,
        memory_id: str | None = None,
        recorded_at: str | None = None,
    ) -> dict[str, Any]:
        """Persist a validated fact, completed outcome, or reusable procedure.

        Semantic facts require source paths or document IDs in evidence. Do
        not store raw conversation turns or unverified hypotheses. Use
        supersedes when new evidence replaces an existing memory.
        """
        return service.remember(
            branch_id,
            title=title,
            summary=summary,
            kind=kind,
            evidence=evidence or (),
            confidence=confidence,
            owner=owner,
            tags=tags or (),
            outcome=outcome,
            supersedes=supersedes or (),
            memory_id=memory_id,
            recorded_at=recorded_at,
        )

    @mcp.tool()
    def knowledge_diff(source_branch: str, target_branch: str) -> dict[str, Any]:
        """Compare documents, embeddings, metadata, and files across branches."""
        return service.diff(source_branch, target_branch)

    @mcp.tool()
    def knowledge_merge(source_branch: str, target_branch: str) -> dict[str, Any]:
        """Promote reviewed state from one branch into another."""
        return service.merge(source_branch, target_branch)

    @mcp.tool()
    def knowledge_delete_branch(branch_id: str) -> dict[str, Any]:
        """Discard a task branch and all of its private changes."""
        service.delete_branch(branch_id)
        return {"branch_id": branch_id, "deleted": True}

    @mcp.tool()
    def knowledge_experiment_tasks() -> list[dict[str, Any]]:
        """Return the curated role-specific EnterpriseRAG task definitions."""
        return service.experiment_tasks()

    return mcp


__all__ = ["create_mcp_server"]
