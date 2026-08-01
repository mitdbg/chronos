"""Backend-neutral knowledge objects used by ingestion, MCP, and replay."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any, Literal

DocumentKind = Literal[
    "source", "curated", "semantic_memory", "episodic_memory", "playbook"
]


def content_hash(content: str | bytes) -> str:
    raw = content.encode("utf-8") if isinstance(content, str) else bytes(content)
    return hashlib.sha256(raw).hexdigest()


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def normalize_workspace_path(path: str) -> str:
    normalized = PurePosixPath("/" + path.lstrip("/"))
    if ".." in normalized.parts:
        raise ValueError(f"workspace path may not contain '..': {path}")
    return str(normalized)


@dataclass(frozen=True)
class KnowledgeDocument:
    id: str
    path: str
    title: str
    source: str
    content: str
    kind: DocumentKind = "source"
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", normalize_workspace_path(self.path))
        if not self.id:
            raise ValueError("document id must not be empty")
        if not self.title:
            raise ValueError("document title must not be empty")

    @property
    def sha256(self) -> str:
        return content_hash(self.content)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "path": self.path,
            "title": self.title,
            "source": self.source,
            "content": self.content,
            "kind": self.kind,
            "metadata": dict(self.metadata),
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> KnowledgeDocument:
        return cls(
            id=str(value["id"]),
            path=str(value["path"]),
            title=str(value["title"]),
            source=str(value["source"]),
            content=str(value["content"]),
            kind=str(value.get("kind", "source")),  # type: ignore[arg-type]
            metadata=dict(value.get("metadata") or {}),
        )


@dataclass(frozen=True)
class DocumentChunk:
    id: str
    document_id: str
    ordinal: int
    text: str
    embedding: tuple[float, ...]
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("chunk id must not be empty")
        if self.ordinal < 0:
            raise ValueError("chunk ordinal must be non-negative")
        if (
            getattr(self.embedding, "implicit_zero", False)
            or (
                isinstance(self.embedding, tuple)
                and self.embedding.count(0.0) == len(self.embedding)
            )
        ):
            return
        object.__setattr__(
            self,
            "embedding",
            tuple(float(value) for value in self.embedding),
        )

    @property
    def sha256(self) -> str:
        return content_hash(self.text)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "document_id": self.document_id,
            "ordinal": self.ordinal,
            "text": self.text,
            "embedding": list(self.embedding),
            "metadata": dict(self.metadata),
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> DocumentChunk:
        return cls(
            id=str(value["id"]),
            document_id=str(value["document_id"]),
            ordinal=int(value["ordinal"]),
            text=str(value["text"]),
            embedding=tuple(float(item) for item in value["embedding"]),
            metadata=dict(value.get("metadata") or {}),
        )


@dataclass(frozen=True)
class IndexedDocument:
    document: KnowledgeDocument
    chunks: tuple[DocumentChunk, ...]

    def __post_init__(self) -> None:
        for chunk in self.chunks:
            if chunk.document_id != self.document.id:
                raise ValueError(
                    f"chunk {chunk.id!r} belongs to {chunk.document_id!r}, "
                    f"not {self.document.id!r}"
                )

    def as_dict(self) -> dict[str, Any]:
        return {
            "document": self.document.as_dict(),
            "chunks": [chunk.as_dict() for chunk in self.chunks],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> IndexedDocument:
        return cls(
            document=KnowledgeDocument.from_dict(value["document"]),
            chunks=tuple(
                DocumentChunk.from_dict(chunk) for chunk in value.get("chunks", [])
            ),
        )


@dataclass(frozen=True)
class SearchHit:
    document_id: str
    chunk_id: str
    path: str
    title: str
    text: str
    score: float
    source: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "document_id": self.document_id,
            "chunk_id": self.chunk_id,
            "path": self.path,
            "title": self.title,
            "text": self.text,
            "score": float(self.score),
            "source": self.source,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> SearchHit:
        return cls(
            document_id=str(value["document_id"]),
            chunk_id=str(value["chunk_id"]),
            path=str(value["path"]),
            title=str(value["title"]),
            text=str(value["text"]),
            score=float(value["score"]),
            source=str(value["source"]),
            metadata=dict(value.get("metadata") or {}),
        )


def indexed_document_digest(value: IndexedDocument) -> str:
    return hashlib.sha256(
        canonical_json(_logical_indexed_document(value)).encode("utf-8")
    ).hexdigest()


def hits_digest(hits: Sequence[SearchHit]) -> str:
    return hashlib.sha256(
        canonical_json([hit.as_dict() for hit in hits]).encode("utf-8")
    ).hexdigest()


def knowledge_state_digest(
    documents: Sequence[IndexedDocument],
    files: Sequence[tuple[str, bytes]],
) -> str:
    return knowledge_state_digest_from_projections(
        [_logical_indexed_document(indexed) for indexed in documents],
        files,
    )


def knowledge_state_digest_from_projections(
    documents: Sequence[Mapping[str, Any]],
    files: Sequence[tuple[str, bytes]],
) -> str:
    """Digest logical state without requiring derived vector payload reads."""

    state = {
        "documents": sorted(
            (dict(document) for document in documents),
            key=lambda value: str(value["document"]["id"]),
        ),
        "files": [
            {
                "path": path,
                "sha256": hashlib.sha256(content).hexdigest(),
                "bytes": len(content),
            }
            for path, content in sorted(files)
        ],
    }
    return hashlib.sha256(canonical_json(state).encode()).hexdigest()


def _logical_indexed_document(value: IndexedDocument) -> dict[str, Any]:
    """Represent logical knowledge independently of vector serialization.

    Embeddings are derived indexes over chunk text. Backends may round their
    floating-point representation differently, so state equivalence checks the
    point's presence and dimensionality without comparing its physical bytes.
    """

    return {
        "document": value.document.as_dict(),
        "chunks": [
            {
                "id": chunk.id,
                "document_id": chunk.document_id,
                "ordinal": chunk.ordinal,
                "text": chunk.text,
                "embedding_dimensions": len(chunk.embedding),
                "metadata": dict(chunk.metadata),
                "sha256": chunk.sha256,
            }
            for chunk in value.chunks
        ],
    }


def knowledge_state_diff(
    source_documents: Mapping[str, str],
    target_documents: Mapping[str, str],
    source_files: Mapping[str, str],
    target_files: Mapping[str, str],
) -> dict[str, Any]:
    """Return one backend-independent summary of logical state differences."""

    def changes(
        source: Mapping[str, str],
        target: Mapping[str, str],
    ) -> dict[str, list[str]]:
        return {
            "added": sorted(source.keys() - target.keys()),
            "deleted": sorted(target.keys() - source.keys()),
            "modified": sorted(
                key
                for key in source.keys() & target.keys()
                if source[key] != target[key]
            ),
        }

    return {
        "documents": changes(source_documents, target_documents),
        "files": changes(source_files, target_files),
    }


__all__ = [
    "DocumentChunk",
    "DocumentKind",
    "IndexedDocument",
    "KnowledgeDocument",
    "SearchHit",
    "canonical_json",
    "content_hash",
    "hits_digest",
    "indexed_document_digest",
    "knowledge_state_diff",
    "knowledge_state_digest",
    "knowledge_state_digest_from_projections",
    "normalize_workspace_path",
]
