"""Document ingestion shared by corpus loading and MCP updates."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from chronos_enterprise_knowledge.backend import KnowledgeBackend
from chronos_enterprise_knowledge.chunking import EnterpriseChunker
from chronos_enterprise_knowledge.embedding import Embedder
from chronos_enterprise_knowledge.enterprise_rag import CorpusRecord
from chronos_enterprise_knowledge.models import IndexedDocument, KnowledgeDocument
from chronos_enterprise_knowledge.trace import TraceRecorder


@dataclass(frozen=True)
class IngestionStats:
    documents: int
    chunks: int
    bytes: int

    def as_dict(self) -> dict[str, int]:
        return {
            "documents": self.documents,
            "chunks": self.chunks,
            "bytes": self.bytes,
        }


class KnowledgeIngestor:
    """Create identical filesystem, relational, and vector state."""

    def __init__(
        self,
        backend: KnowledgeBackend,
        embedder: Embedder,
        *,
        chunker: EnterpriseChunker | None = None,
        recorder: TraceRecorder | None = None,
    ):
        if (
            getattr(backend, "vector_dimensions", embedder.dimensions)
            != embedder.dimensions
        ):
            raise ValueError(
                "backend and embedder dimensions do not match: "
                f"{getattr(backend, 'vector_dimensions', 'unknown')} != "
                f"{embedder.dimensions}"
            )
        self.backend = backend
        self.embedder = embedder
        self.chunker = chunker or EnterpriseChunker()
        self.recorder = recorder

    def index_record(
        self,
        branch_id: str,
        record: CorpusRecord,
        *,
        operation_id: str,
    ) -> IndexedDocument:
        return self.index_document(
            branch_id,
            record.document,
            index_text=record.index_text,
            context=record.context,
            operation_id=operation_id,
        )

    def index_document(
        self,
        branch_id: str,
        document: KnowledgeDocument,
        *,
        index_text: str | None = None,
        context: Mapping[str, Any] | None = None,
        operation_id: str,
    ) -> IndexedDocument:
        indexed = self.prepare_document(
            document,
            index_text=index_text,
            context=context,
        )
        if self.recorder is None:
            self.backend.put_document(
                branch_id,
                indexed,
                operation_id=operation_id,
            )
        else:
            self.recorder.execute(
                "document_put",
                branch_id=branch_id,
                arguments={"indexed_document": indexed.as_dict()},
            )
        return indexed

    def prepare_document(
        self,
        document: KnowledgeDocument,
        *,
        index_text: str | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> IndexedDocument:
        pending = self.chunker.chunk_text(
            document,
            index_text if index_text is not None else document.content,
            context=context,
        )
        embeddings = self.embedder.embed([text for text, _ in pending])
        chunks = self.chunker.with_embeddings(document, pending, embeddings)
        return IndexedDocument(document, chunks)

    def ingest_records(
        self,
        branch_id: str,
        records: Iterable[CorpusRecord],
        *,
        operation_prefix: str = "enterprise-rag",
        batch_size: int = 32,
        progress: Callable[[IngestionStats], None] | None = None,
    ) -> IngestionStats:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        document_count = 0
        chunk_count = 0
        byte_count = 0
        batch: list[CorpusRecord] = []
        for record in records:
            batch.append(record)
            if len(batch) < batch_size:
                continue
            indexed_batch = self._ingest_record_batch(
                branch_id,
                batch,
                operation_id=f"{operation_prefix}:batch-{document_count:08d}",
            )
            document_count += len(indexed_batch)
            chunk_count += sum(len(indexed.chunks) for indexed in indexed_batch)
            byte_count += sum(
                len(item.document.content.encode("utf-8")) for item in batch
            )
            if progress is not None:
                progress(IngestionStats(document_count, chunk_count, byte_count))
            batch = []
        if batch:
            indexed_batch = self._ingest_record_batch(
                branch_id,
                batch,
                operation_id=f"{operation_prefix}:batch-{document_count:08d}",
            )
            document_count += len(indexed_batch)
            chunk_count += sum(len(indexed.chunks) for indexed in indexed_batch)
            byte_count += sum(
                len(item.document.content.encode("utf-8")) for item in batch
            )
            if progress is not None:
                progress(IngestionStats(document_count, chunk_count, byte_count))
        return IngestionStats(document_count, chunk_count, byte_count)

    def _ingest_record_batch(
        self,
        branch_id: str,
        records: Sequence[CorpusRecord],
        *,
        operation_id: str,
    ) -> list[IndexedDocument]:
        if self.recorder is not None:
            return [
                self.index_record(
                    branch_id,
                    record,
                    operation_id=f"{operation_id}:{record.document.id}",
                )
                for record in records
            ]
        pending_by_record = [
            self.chunker.chunk_text(
                record.document,
                record.index_text,
                context=record.context,
            )
            for record in records
        ]
        pending = [
            chunk for document_chunks in pending_by_record for chunk in document_chunks
        ]
        embeddings = self.embedder.embed([text for text, _ in pending])
        indexed_documents: list[IndexedDocument] = []
        offset = 0
        for record, document_chunks in zip(
            records,
            pending_by_record,
            strict=True,
        ):
            count = len(document_chunks)
            chunks = self.chunker.with_embeddings(
                record.document,
                document_chunks,
                embeddings[offset : offset + count],
            )
            indexed_documents.append(IndexedDocument(record.document, chunks))
            offset += count
        put_many = getattr(self.backend, "put_documents", None)
        if callable(put_many):
            put_many(
                branch_id,
                indexed_documents,
                operation_id=operation_id,
            )
        else:
            for indexed in indexed_documents:
                self.backend.put_document(
                    branch_id,
                    indexed,
                    operation_id=f"{operation_id}:{indexed.document.id}",
                )
        return indexed_documents


__all__ = [
    "IngestionStats",
    "KnowledgeIngestor",
]
