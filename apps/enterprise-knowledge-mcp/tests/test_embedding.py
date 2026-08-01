from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest
from chronos_enterprise_knowledge.cli import _parser
from chronos_enterprise_knowledge.embedding import (
    CachedEmbedder,
    DEFAULT_SENTENCE_TRANSFORMER_DIMENSIONS,
    DEFAULT_SENTENCE_TRANSFORMER_MODEL,
    EmbeddingCache,
    EmbeddingError,
    SentenceTransformerEmbedder,
    SentenceTransformerEmbeddingConfig,
)


class _FakeSentenceTransformer:
    def __init__(self, dimensions: int = 384):
        self.dimensions = dimensions
        self.calls: list[tuple[str, ...]] = []
        self.options: list[dict[str, object]] = []
        self.active = 0
        self.max_active = 0
        self._lock = threading.Lock()

    def get_sentence_embedding_dimension(self) -> int:
        return self.dimensions

    def encode(self, texts, **options):  # type: ignore[no-untyped-def]
        with self._lock:
            self.calls.append(tuple(texts))
            self.options.append(options)
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            time.sleep(0.02)
            return [
                [float(text)] + [0.0] * (self.dimensions - 1)
                for text in texts
            ]
        finally:
            with self._lock:
                self.active -= 1


def test_sentence_transformer_metadata_and_parallel_batches() -> None:
    encoder = _FakeSentenceTransformer()
    embedder = SentenceTransformerEmbedder(
        config=SentenceTransformerEmbeddingConfig(
            batch_size=2,
            workers=3,
        ),
        encoder=encoder,
    )

    vectors = embedder.embed(["0", "1", "2", "3", "4"])

    assert embedder.model == DEFAULT_SENTENCE_TRANSFORMER_MODEL
    assert embedder.dimensions == DEFAULT_SENTENCE_TRANSFORMER_DIMENSIONS
    assert [vector[0] for vector in vectors] == [0.0, 1.0, 2.0, 3.0, 4.0]
    assert sorted(len(call) for call in encoder.calls) == [1, 2, 2]
    assert encoder.max_active >= 2
    assert all(option["normalize_embeddings"] is True for option in encoder.options)
    assert all(option["convert_to_numpy"] is True for option in encoder.options)


def test_sentence_transformer_reuses_embedding_cache(tmp_path: Path) -> None:
    encoder = _FakeSentenceTransformer()
    provider = SentenceTransformerEmbedder(
        config=SentenceTransformerEmbeddingConfig(
            batch_size=2,
            workers=2,
        ),
        encoder=encoder,
    )
    cache = EmbeddingCache(tmp_path / "embedding-cache.sqlite")
    embedder = CachedEmbedder(provider, cache)
    try:
        first = embedder.embed(["1", "2", "1", "3"])
        call_count = len(encoder.calls)
        second = embedder.embed(["3", "1", "2"])
    finally:
        cache.close()

    assert call_count == 2
    assert len(encoder.calls) == call_count
    assert first[0] == first[2] == second[1]
    assert first[1] == second[2]
    assert first[3] == second[0]


def test_sentence_transformer_rejects_unexpected_dimensions() -> None:
    with pytest.raises(EmbeddingError, match="exposes 3 dimensions; expected 384"):
        SentenceTransformerEmbedder(encoder=_FakeSentenceTransformer(3))


def test_cli_defaults_to_local_minilm_embeddings() -> None:
    args = _parser().parse_args(["tasks"])

    assert args.embedding_provider == "sentence-transformers"
    assert args.embedding_model == DEFAULT_SENTENCE_TRANSFORMER_MODEL
    assert args.dimensions == DEFAULT_SENTENCE_TRANSFORMER_DIMENSIONS
    assert args.embedding_workers > 1


def test_snapshot_defaults_fit_minilm_context_without_duplicate_cache() -> None:
    args = _parser().parse_args(
        ["prepare-snapshot", "/tmp/corpus", "/tmp/snapshot"]
    )

    assert args.target_tokens == 240
    assert args.overlap_tokens == 24
    assert args.chunk_encoding is None
    assert args.cache_snapshot_embeddings is False


def test_bounded_snapshot_ingestion_keeps_required_documents() -> None:
    args = _parser().parse_args(
        [
            "ingest-snapshot",
            "/tmp/snapshot",
            "--max-documents",
            "100",
            "--required-document-id",
            "document-a",
            "--required-document-id",
            "document-b",
        ]
    )

    assert args.max_documents == 100
    assert args.required_document_id == ["document-a", "document-b"]
