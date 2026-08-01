"""Stable logical operations shared by Chronos and comparison backends."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from chronos_enterprise_knowledge.models import (
    IndexedDocument,
    SearchHit,
)

OperationKind = Literal[
    "branch_create",
    "branch_delete",
    "document_put",
    "document_delete",
    "file_write",
    "file_delete",
    "search",
    "branch_merge",
    "branch_diff",
    "state_digest",
]


class BackendOperationError(RuntimeError):
    """Raised for malformed or unsupported replay operations."""


class KnowledgeBackend(Protocol):
    """The storage contract exercised by the MCP and benchmark traces."""

    @property
    def backend_name(self) -> str: ...

    def list_branches(self) -> list[str]: ...

    def create_branch(
        self,
        branch_id: str,
        parent_branch: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None: ...

    def delete_branch(self, branch_id: str) -> None: ...

    def put_document(
        self,
        branch_id: str,
        indexed: IndexedDocument,
        *,
        operation_id: str,
    ) -> None: ...

    def put_documents(
        self,
        branch_id: str,
        indexed_documents: Sequence[IndexedDocument],
        *,
        operation_id: str,
    ) -> None: ...

    def load_documents(
        self,
        branch_id: str,
        indexed_documents: Sequence[IndexedDocument],
        *,
        operation_id: str,
    ) -> None:
        """Bulk-load documents into a new benchmark state."""
        ...

    def delete_document(
        self,
        branch_id: str,
        document_id: str,
        *,
        operation_id: str,
    ) -> bool: ...

    def get_document(
        self,
        branch_id: str,
        document_id: str,
    ) -> IndexedDocument | None: ...

    def search(
        self,
        branch_id: str,
        query_text: str,
        query_embedding: Sequence[float],
        *,
        limit: int,
    ) -> list[SearchHit]: ...

    def write_file(
        self,
        branch_id: str,
        path: str,
        content: bytes,
        *,
        operation_id: str,
    ) -> None: ...

    def delete_file(
        self,
        branch_id: str,
        path: str,
        *,
        operation_id: str,
    ) -> bool: ...

    def read_file(self, branch_id: str, path: str) -> bytes: ...

    def diff(self, source_branch: str, target_branch: str) -> dict[str, Any]: ...

    def merge(
        self,
        source_branch: str,
        target_branch: str,
        *,
        operation_id: str,
    ) -> dict[str, Any]: ...

    def state_digest(self, branch_id: str) -> str: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class BackendOperation:
    operation_id: str
    kind: OperationKind
    branch_id: str | None = None
    arguments: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "operation_id": self.operation_id,
            "kind": self.kind,
            "branch_id": self.branch_id,
            "arguments": dict(self.arguments),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> BackendOperation:
        return cls(
            operation_id=str(value["operation_id"]),
            kind=str(value["kind"]),  # type: ignore[arg-type]
            branch_id=(
                str(value["branch_id"]) if value.get("branch_id") is not None else None
            ),
            arguments=dict(value.get("arguments") or {}),
        )


class OperationExecutor:
    """Validate and dispatch one backend-neutral operation."""

    def __init__(self, backend: KnowledgeBackend):
        self.backend = backend

    def execute(self, operation: BackendOperation) -> Any:
        args = operation.arguments
        branch = operation.branch_id
        if operation.kind == "branch_create":
            self.backend.create_branch(
                self._require_branch(branch),
                str(args["parent_branch"]),
                dict(args.get("metadata") or {}),
            )
            return {"created": branch}
        if operation.kind == "branch_delete":
            self.backend.delete_branch(self._require_branch(branch))
            return {"deleted": branch}
        if operation.kind == "document_put":
            indexed = IndexedDocument.from_dict(args["indexed_document"])
            self.backend.put_document(
                self._require_branch(branch),
                indexed,
                operation_id=operation.operation_id,
            )
            return {
                "document_id": indexed.document.id,
                "sha256": indexed.document.sha256,
                "chunk_count": len(indexed.chunks),
            }
        if operation.kind == "document_delete":
            deleted = self.backend.delete_document(
                self._require_branch(branch),
                str(args["document_id"]),
                operation_id=operation.operation_id,
            )
            return {
                "document_id": str(args["document_id"]),
                "deleted": deleted,
            }
        if operation.kind == "file_write":
            content = args["content"]
            if isinstance(content, str):
                content = content.encode("utf-8")
            if not isinstance(content, (bytes, bytearray)):
                raise BackendOperationError("file_write content must be bytes")
            self.backend.write_file(
                self._require_branch(branch),
                str(args["path"]),
                bytes(content),
                operation_id=operation.operation_id,
            )
            return {"path": str(args["path"]), "bytes": len(content)}
        if operation.kind == "file_delete":
            deleted = self.backend.delete_file(
                self._require_branch(branch),
                str(args["path"]),
                operation_id=operation.operation_id,
            )
            return {"path": str(args["path"]), "deleted": deleted}
        if operation.kind == "search":
            hits = self.backend.search(
                self._require_branch(branch),
                str(args["query_text"]),
                [float(value) for value in args["query_embedding"]],
                limit=int(args.get("limit", 10)),
            )
            return [hit.as_dict() for hit in hits]
        if operation.kind == "branch_diff":
            return self.backend.diff(
                str(args["source_branch"]),
                str(args["target_branch"]),
            )
        if operation.kind == "branch_merge":
            source_branch = str(args["source_branch"])
            target_branch = str(args["target_branch"])
            optional = {
                key: args[key]
                for key in (
                    "selected_change_ids",
                    "preview_token",
                    "policy",
                    "conflict_choices",
                )
                if key in args
            }
            self.backend.merge(
                source_branch,
                target_branch,
                operation_id=operation.operation_id,
                **optional,
            )
            return {
                "source_branch": source_branch,
                "target_branch": target_branch,
                "merged": True,
            }
        if operation.kind == "state_digest":
            return {
                "branch_id": self._require_branch(branch),
                "digest": self.backend.state_digest(self._require_branch(branch)),
            }
        raise BackendOperationError(f"unsupported backend operation: {operation.kind}")

    @staticmethod
    def _require_branch(branch_id: str | None) -> str:
        if branch_id is None or not branch_id:
            raise BackendOperationError("operation requires branch_id")
        return branch_id


__all__ = [
    "BackendOperation",
    "BackendOperationError",
    "KnowledgeBackend",
    "OperationExecutor",
    "OperationKind",
]
