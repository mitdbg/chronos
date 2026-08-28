"""Application service used by both the MCP server and experiment driver."""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Mapping, Sequence
from typing import Any

from chronos_enterprise_knowledge.backend import KnowledgeBackend
from chronos_enterprise_knowledge.embedding import Embedder
from chronos_enterprise_knowledge.hierarchy import load_hierarchy, load_tasks
from chronos_enterprise_knowledge.ingestion import KnowledgeIngestor
from chronos_enterprise_knowledge.memory import AgentMemory, MemoryEntry, MemoryKind
from chronos_enterprise_knowledge.models import (
    DocumentKind,
    KnowledgeDocument,
    SearchHit,
    normalize_workspace_path,
)
from chronos_enterprise_knowledge.trace import TraceRecorder


class KnowledgeService:
    """Simple branch-oriented API; storage coordination remains internal."""

    def __init__(
        self,
        backend: KnowledgeBackend,
        embedder: Embedder,
        *,
        recorder: TraceRecorder | None = None,
    ):
        self.backend = backend
        self.embedder = embedder
        self.recorder = recorder
        self.ingestor = KnowledgeIngestor(
            backend,
            embedder,
            recorder=recorder,
        )
        self.memory = AgentMemory(self.ingestor)

    def start(self) -> None:
        """Start backend services required while serving MCP requests."""
        start = getattr(self.backend, "start", None)
        if callable(start):
            start()

    def status(self) -> dict[str, Any]:
        return {
            "backend": self.backend.backend_name,
            "branches": self.backend.list_branches(),
            "embedding_model": self.embedder.model,
            "embedding_dimensions": self.embedder.dimensions,
            "storage": list(
                getattr(
                    self.backend,
                    "storage_components",
                    ["relational", "filesystem", "qdrant"],
                )
            ),
        }

    def checkout(
        self,
        branch_id: str,
        *,
        from_branch: str | None = None,
        mount: bool = True,
        mount_path: str | None = None,
    ) -> dict[str, Any]:
        branches = set(self.backend.list_branches())
        created = False
        if branch_id not in branches:
            if from_branch is None:
                raise ValueError(
                    f"branch {branch_id!r} does not exist; provide from_branch "
                    "to create it"
                )
            if self.recorder is None:
                self.backend.create_branch(branch_id, from_branch)
            else:
                self.recorder.execute(
                    "branch_create",
                    branch_id=branch_id,
                    arguments={"parent_branch": from_branch},
                )
            created = True
        result: dict[str, Any] = {
            "branch_id": branch_id,
            "created": created,
        }
        if from_branch is not None:
            result["from_branch"] = from_branch
        if mount:
            mount_branch = getattr(self.backend, "mount_branch", None)
            if not callable(mount_branch):
                raise RuntimeError(
                    f"backend {self.backend.backend_name!r} cannot mount a workspace"
                )
            result["workspace_path"] = str(mount_branch(branch_id, mount_path))
        return result

    def search(
        self,
        branch_id: str,
        query: str,
        *,
        limit: int = 8,
    ) -> list[SearchHit]:
        embedding = self.embedder.embed([query])[0]
        if self.recorder is None:
            return self.backend.search(
                branch_id,
                query,
                embedding,
                limit=limit,
            )
        result = self.recorder.execute(
            "search",
            branch_id=branch_id,
            arguments={
                "query_text": query,
                "query_embedding": list(embedding),
                "limit": limit,
            },
        )
        return [SearchHit.from_dict(hit) for hit in result]

    def get_document(self, branch_id: str, document_id: str) -> dict[str, Any] | None:
        value = self.backend.get_document(branch_id, document_id)
        return value.as_dict() if value is not None else None

    def update_document(
        self,
        branch_id: str,
        *,
        path: str,
        title: str,
        content: str,
        source: str,
        kind: DocumentKind = "curated",
        document_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if kind not in {"source", "curated"}:
            raise ValueError(
                "agent memory must be written with remember(), which enforces "
                "memory provenance and retention rules"
            )
        normalized = normalize_workspace_path(path)
        identifier = document_id or _document_id_for_path(normalized)
        document = KnowledgeDocument(
            id=identifier,
            path=normalized,
            title=title,
            source=source,
            content=content,
            kind=kind,
            metadata=dict(metadata or {}),
        )
        indexed = self.ingestor.index_document(
            branch_id,
            document,
            context={
                "workspace": branch_id,
                "team": (metadata or {}).get("team", ""),
            },
            operation_id=f"document:{identifier}",
        )
        return {
            "document_id": identifier,
            "path": normalized,
            "sha256": document.sha256,
            "chunks": len(indexed.chunks),
        }

    def index_workspace_file(
        self,
        branch_id: str,
        *,
        path: str,
        title: str,
        source: str,
        kind: DocumentKind = "curated",
        document_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized = normalize_workspace_path(path)
        content = self.backend.read_file(branch_id, normalized).decode("utf-8")
        if document_id is None:
            find_by_path = getattr(self.backend, "find_document_id_by_path", None)
            if callable(find_by_path):
                document_id = find_by_path(branch_id, normalized)
        return self.update_document(
            branch_id,
            path=normalized,
            title=title,
            content=content,
            source=source,
            kind=kind,
            document_id=document_id,
            metadata=metadata,
        )

    def delete_document(self, branch_id: str, document_id: str) -> bool:
        if self.recorder is None:
            return self.backend.delete_document(
                branch_id,
                document_id,
                operation_id=f"document-delete:{document_id}",
            )
        result = self.recorder.execute(
            "document_delete",
            branch_id=branch_id,
            arguments={"document_id": document_id},
        )
        return bool(result["deleted"])

    def remember(
        self,
        branch_id: str,
        *,
        title: str,
        summary: str,
        kind: MemoryKind,
        evidence: Sequence[str] = (),
        confidence: float = 1.0,
        owner: str | None = None,
        tags: Sequence[str] = (),
        outcome: str | None = None,
        supersedes: Sequence[str] = (),
        memory_id: str | None = None,
        recorded_at: str | None = None,
    ) -> dict[str, Any]:
        indexed = self.memory.remember(
            branch_id,
            MemoryEntry(
                title=title,
                summary=summary,
                kind=kind,
                evidence=tuple(evidence),
                confidence=confidence,
                owner=owner,
                tags=tuple(tags),
                outcome=outcome,
                supersedes=tuple(supersedes),
                recorded_at=recorded_at,
            ),
            memory_id=memory_id,
            operation_id=f"memory:{memory_id or title}",
        )
        for previous_id in supersedes:
            if previous_id != indexed.document.id:
                self.delete_document(branch_id, previous_id)
        return {
            "memory_id": indexed.document.id,
            "path": indexed.document.path,
            "kind": indexed.document.kind,
            "chunks": len(indexed.chunks),
        }

    def write_artifact(
        self,
        branch_id: str,
        path: str,
        content: bytes,
    ) -> dict[str, Any]:
        normalized = normalize_workspace_path(path)
        if self.recorder is None:
            self.backend.write_file(
                branch_id,
                normalized,
                content,
                operation_id=f"file:{normalized}",
            )
            return {"path": normalized, "bytes": len(content)}
        return self.recorder.execute(
            "file_write",
            branch_id=branch_id,
            arguments={"path": normalized, "content": content},
        )

    def diff(self, source: str, target: str) -> dict[str, Any]:
        if self.recorder is None:
            return self.backend.diff(source, target)
        return self.recorder.execute(
            "branch_diff",
            arguments={"source_branch": source, "target_branch": target},
        )

    def merge_preview(
        self,
        source: str,
        target: str,
        *,
        policy: Any = None,
    ) -> dict[str, Any]:
        preview = getattr(self.backend, "merge_preview", None)
        if not callable(preview):
            raise NotImplementedError(
                f"backend {self.backend.backend_name!r} does not support "
                "selective merge previews"
            )
        return preview(source, target, policy=policy)

    def merge(
        self,
        source: str,
        target: str,
        *,
        selected_change_ids: Sequence[str] | None = None,
        preview_token: str | None = None,
        prepared_preview: Any | None = None,
        policy: Any = None,
        conflict_choices: Mapping[str, str] | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        if (
            selected_change_ids is not None
            or preview_token is not None
            or policy is not None
            or conflict_choices is not None
        ) and not callable(getattr(self.backend, "merge_preview", None)):
            raise NotImplementedError(
                f"backend {self.backend.backend_name!r} does not support "
                "selective atomic merges"
            )
        merge_operation_id = operation_id or (
            f"merge:{source}:{target}:{uuid.uuid4().hex}"
        )
        arguments: dict[str, Any] = {
            "source_branch": source,
            "target_branch": target,
        }
        if selected_change_ids is not None:
            arguments["selected_change_ids"] = list(selected_change_ids)
        if preview_token is not None:
            arguments["preview_token"] = preview_token
        if policy is not None:
            arguments["policy"] = policy
        if conflict_choices is not None:
            arguments["conflict_choices"] = dict(conflict_choices)
        if self.recorder is None:
            result = self.backend.merge(
                source,
                target,
                operation_id=merge_operation_id,
                **{
                    key: value
                    for key, value in arguments.items()
                    if key not in {"source_branch", "target_branch"}
                },
                prepared_preview=prepared_preview,
            )
            return {
                **result,
                "source_branch": source,
                "target_branch": target,
                "merged": True,
            }
        return self.recorder.execute(
            "branch_merge",
            arguments=arguments,
            operation_id=operation_id,
        )

    def delete_branch(self, branch_id: str) -> None:
        if self.recorder is None:
            self.backend.delete_branch(branch_id)
        else:
            self.recorder.execute("branch_delete", branch_id=branch_id)

    @staticmethod
    def experiment_hierarchy() -> dict[str, Any]:
        return load_hierarchy()

    @staticmethod
    def experiment_tasks() -> list[dict[str, Any]]:
        return [
            {
                "id": task.id,
                "branch": task.branch,
                "role": task.role,
                "objective": task.objective,
                "evidence_scopes": list(task.evidence_scopes),
                "updates": list(task.updates),
            }
            for task in load_tasks()
        ]


def _document_id_for_path(path: str) -> str:
    digest = hashlib.sha256(path.encode()).hexdigest()[:24]
    return f"workspace_{digest}"


__all__ = ["KnowledgeService"]
