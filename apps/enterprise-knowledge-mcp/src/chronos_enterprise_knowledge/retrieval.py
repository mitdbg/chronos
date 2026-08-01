"""Shared Qdrant payload, BM25, and hybrid-query helpers."""

from __future__ import annotations

import os
import threading
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from fastembed import SparseTextEmbedding
from qdrant_client import QdrantClient, models

DENSE_VECTOR = "dense"
BM25_VECTOR = "bm25"
TEXT_FIELD = "text"
BM25_K = 1.2
BM25_B = 0.75
DEFAULT_BM25_AVG_LEN = 256.0


def _positive_environment_integer(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return None
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _nonnegative_environment_integer(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return None
    value = int(raw)
    if value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _boolean_environment(name: str, *, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def remote_qdrant_client(
    url: str,
    *,
    api_key: str | None = None,
    timeout: float = 600.0,
) -> QdrantClient:
    """Create the shared benchmark Qdrant client."""

    return QdrantClient(
        url=url,
        api_key=api_key,
        grpc_port=_positive_environment_integer(
            "CHRONOS_QDRANT_GRPC_PORT"
        ),
        prefer_grpc=_boolean_environment("CHRONOS_QDRANT_PREFER_GRPC"),
        timeout=timeout,
    )


def qdrant_collection_options(*, on_disk: bool) -> dict[str, Any]:
    """Return one ingestion-oriented collection configuration for all backends."""

    indexing_threshold = _nonnegative_environment_integer(
        "CHRONOS_QDRANT_INDEXING_THRESHOLD_KB"
    )
    maximum_optimization_threads = _positive_environment_integer(
        "CHRONOS_QDRANT_MAX_OPTIMIZATION_THREADS"
    )
    maximum_indexing_threads = _positive_environment_integer(
        "CHRONOS_QDRANT_MAX_INDEXING_THREADS"
    )
    hnsw_m = _nonnegative_environment_integer(
        "CHRONOS_QDRANT_HNSW_M"
    )
    shard_number = _positive_environment_integer(
        "CHRONOS_QDRANT_SHARD_NUMBER"
    )
    options: dict[str, Any] = {
        "on_disk_payload": on_disk,
        "hnsw_config": (
            models.HnswConfigDiff(
                m=hnsw_m,
                on_disk=True if on_disk else None,
                max_indexing_threads=maximum_indexing_threads,
            )
            if (
                on_disk
                or maximum_indexing_threads is not None
                or hnsw_m is not None
            )
            else None
        ),
        "optimizers_config": (
            models.OptimizersConfigDiff(
                indexing_threshold=indexing_threshold,
                max_optimization_threads=maximum_optimization_threads,
            )
            if (
                indexing_threshold is not None
                or maximum_optimization_threads is not None
            )
            else None
        ),
    }
    if shard_number is not None:
        options["shard_number"] = shard_number
    return options


def bulk_upsert_points(
    client: QdrantClient,
    collection_name: str,
    points: Sequence[models.PointStruct],
) -> None:
    """Upload a fresh benchmark batch using bounded concurrent requests."""

    if not points:
        return
    batch_size = (
        _positive_environment_integer("CHRONOS_QDRANT_POINT_BATCH_SIZE")
        or 256
    )
    batches = [
        points[offset : offset + batch_size]
        for offset in range(0, len(points), batch_size)
    ]
    workers = min(
        _positive_environment_integer("CHRONOS_QDRANT_UPLOAD_WORKERS") or 1,
        len(batches),
    )

    def upload(batch: Sequence[models.PointStruct]) -> None:
        client.upsert(
            collection_name=collection_name,
            points=batch,
            wait=True,
        )

    if workers == 1:
        for batch in batches:
            upload(batch)
        return
    with ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix="enterprise-qdrant-upload",
    ) as executor:
        list(executor.map(upload, batches))


class Bm25Encoder:
    """Generate deterministic sparse vectors shared by every backend."""

    def __init__(
        self,
        *,
        avg_len: float = DEFAULT_BM25_AVG_LEN,
        cache_dir: str | Path | None = None,
        threads: int | None = None,
    ):
        self.avg_len = float(avg_len)
        if threads is None:
            configured_threads = os.environ.get("CHRONOS_BM25_THREADS")
            if configured_threads:
                threads = int(configured_threads)
                if threads <= 0:
                    raise ValueError(
                        "CHRONOS_BM25_THREADS must be a positive integer"
                    )
        self._lock = threading.RLock()
        self._model = SparseTextEmbedding(
            "Qdrant/bm25",
            cache_dir=str(cache_dir) if cache_dir is not None else None,
            threads=threads,
            k=BM25_K,
            b=BM25_B,
            avg_len=self.avg_len,
            language="english",
            disable_stemmer=True,
            token_max_length=80,
        )

    def documents(self, texts: Sequence[str]) -> list[models.SparseVector]:
        if not texts:
            return []
        with self._lock:
            values = list(self._model.embed(list(texts), batch_size=512))
        return [
            models.SparseVector(
                indices=[int(index) for index in value.indices],
                values=[float(item) for item in value.values],
            )
            for value in values
        ]

    def query(self, text: str) -> models.SparseVector | None:
        if not text.strip():
            return None
        with self._lock:
            value = next(iter(self._model.query_embed(text)))
        if not len(value.indices):
            return None
        return models.SparseVector(
            indices=[int(index) for index in value.indices],
            values=[float(item) for item in value.values],
        )


def named_vector_config(
    dimensions: int,
    *,
    on_disk: bool = False,
) -> tuple[dict[str, models.VectorParams], dict[str, models.SparseVectorParams]]:
    return (
        {
            DENSE_VECTOR: models.VectorParams(
                size=int(dimensions),
                distance=models.Distance.COSINE,
                on_disk=on_disk,
            )
        },
        {
            BM25_VECTOR: models.SparseVectorParams(
                modifier=models.Modifier.IDF,
                index=models.SparseIndexParams(on_disk=on_disk),
            )
        },
    )


def dense_vector_or_none(values: Sequence[float]) -> list[float] | None:
    dense = [float(value) for value in values]
    return None if not dense or not any(dense) else dense


def point_vectors(
    dense: Sequence[float],
    sparse: models.SparseVector,
) -> dict[str, Any]:
    vectors: dict[str, Any] = {BM25_VECTOR: sparse}
    dense_values = dense_vector_or_none(dense)
    if dense_values is not None:
        vectors[DENSE_VECTOR] = dense_values
    return vectors


def hybrid_query(
    client: Any,
    *,
    collection_name: str,
    dense_query: Sequence[float] | None,
    sparse_query: models.SparseVector | None,
    query_filter: Any | None,
    limit: int,
    candidate_limit: int | None = None,
) -> list[Any]:
    """Run dense/sparse RRF, or the one available retrieval leg."""

    if int(limit) <= 0:
        return []
    candidates = int(candidate_limit or max(64, int(limit) * 8))
    prefetch: list[Any] = []
    if dense_query is not None:
        dense = dense_vector_or_none(dense_query)
        if dense is not None:
            prefetch.append(
                models.Prefetch(
                    query=dense,
                    using=DENSE_VECTOR,
                    filter=query_filter,
                    limit=candidates,
                )
            )
    if sparse_query is not None:
        prefetch.append(
            models.Prefetch(
                query=sparse_query,
                using=BM25_VECTOR,
                filter=query_filter,
                limit=candidates,
            )
        )
    if not prefetch:
        points, _ = client.scroll(
            collection_name=collection_name,
            scroll_filter=query_filter,
            limit=int(limit),
            with_payload=True,
            with_vectors=False,
        )
        return list(points)
    if len(prefetch) == 1:
        only = prefetch[0]
        return list(
            client.query_points(
                collection_name=collection_name,
                query=only.query,
                using=only.using,
                query_filter=query_filter,
                limit=int(limit),
                with_payload=True,
                with_vectors=False,
            ).points
        )
    return list(
        client.query_points(
            collection_name=collection_name,
            prefetch=prefetch,
            query=models.FusionQuery(fusion=models.Fusion.RRF),
            limit=int(limit),
            with_payload=True,
            with_vectors=False,
        ).points
    )


def payload_for_chunk(
    *,
    document_id: str,
    chunk_id: str,
    ordinal: int,
    text: str,
    content_hash: str,
    metadata: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "document_id": document_id,
        "chunk_id": chunk_id,
        "ordinal": int(ordinal),
        TEXT_FIELD: text,
        "content_hash": content_hash,
        "chunk_metadata": dict(metadata),
    }


__all__ = [
    "BM25_VECTOR",
    "Bm25Encoder",
    "DENSE_VECTOR",
    "TEXT_FIELD",
    "dense_vector_or_none",
    "hybrid_query",
    "named_vector_config",
    "payload_for_chunk",
    "point_vectors",
]
