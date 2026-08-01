"""Embedding providers and a persistent content-addressed embedding cache."""

from __future__ import annotations

import array
import hashlib
import math
import multiprocessing
import os
import random
import sqlite3
import threading
import time
from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import httpx


class EmbeddingError(RuntimeError):
    """Raised when an embedding provider returns an unusable response."""


class Embedder(Protocol):
    """Minimal provider contract used by ingestion, memory, and search."""

    @property
    def model(self) -> str: ...

    @property
    def dimensions(self) -> int: ...

    def embed(self, texts: Sequence[str]) -> list[tuple[float, ...]]: ...


DEFAULT_SENTENCE_TRANSFORMER_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
DEFAULT_SENTENCE_TRANSFORMER_DIMENSIONS = 384
_PROCESS_ENCODER: Any | None = None
_PROCESS_EMBED_OPTIONS: dict[str, Any] = {}


class _ImplicitZeroVector(tuple[float, ...]):
    """Tuple marker that keeps staged vectors allocation-free downstream."""

    __slots__ = ()
    implicit_zero = True


def _initialize_sentence_transformer_process(
    model: str,
    backend: str,
    device: str | None,
    model_file: str | None,
    batch_size: int,
    normalize_embeddings: bool,
) -> None:
    """Load one independent encoder inside a spawned worker."""

    global _PROCESS_ENCODER, _PROCESS_EMBED_OPTIONS
    from sentence_transformers import SentenceTransformer

    model_kwargs = {"file_name": model_file} if model_file else None
    _PROCESS_ENCODER = SentenceTransformer(
        model,
        device=device,
        backend=backend,
        model_kwargs=model_kwargs,
    )
    _PROCESS_EMBED_OPTIONS = {
        "batch_size": batch_size,
        "show_progress_bar": False,
        "convert_to_numpy": True,
        "normalize_embeddings": normalize_embeddings,
    }


def _encode_sentence_transformer_process(
    texts: Sequence[str],
) -> list[list[float]]:
    if _PROCESS_ENCODER is None:
        raise RuntimeError("embedding worker was not initialized")
    encoded = _PROCESS_ENCODER.encode(
        list(texts),
        **_PROCESS_EMBED_OPTIONS,
    )
    values = encoded.tolist() if hasattr(encoded, "tolist") else encoded
    return [[float(value) for value in vector] for vector in values]


class ZeroEmbedder:
    """Return implicit placeholder vectors for staged corpus preparation."""

    def __init__(self, dimensions: int, *, model: str):
        if dimensions <= 0:
            raise ValueError("embedding dimensions must be positive")
        self._dimensions = int(dimensions)
        self._model = str(model)
        self._zero = _ImplicitZeroVector((0.0,) * self._dimensions)

    @property
    def model(self) -> str:
        return self._model

    @property
    def dimensions(self) -> int:
        return self._dimensions

    def embed(self, texts: Sequence[str]) -> list[tuple[float, ...]]:
        return [self._zero] * len(texts)


class EmbeddingCache:
    """SQLite cache keyed by model, dimensions, and exact input bytes."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS embeddings (
                model TEXT NOT NULL,
                dimensions INTEGER NOT NULL,
                input_hash TEXT NOT NULL,
                vector BLOB NOT NULL,
                PRIMARY KEY (model, dimensions, input_hash)
            )
            """
        )
        self._db.commit()

    @staticmethod
    def input_hash(text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @staticmethod
    def normalize(vector: Sequence[float]) -> tuple[float, ...]:
        values = array.array("f", (float(value) for value in vector))
        return tuple(float(value) for value in values)

    def get(
        self,
        model: str,
        dimensions: int,
        text: str,
    ) -> tuple[float, ...] | None:
        with self._lock:
            row = self._db.execute(
                """
                SELECT vector
                FROM embeddings
                WHERE model = ? AND dimensions = ? AND input_hash = ?
                """,
                (model, dimensions, self.input_hash(text)),
            ).fetchone()
        if row is None:
            return None
        values = array.array("f")
        values.frombytes(bytes(row["vector"]))
        if len(values) != dimensions:
            raise EmbeddingError(
                f"cached embedding has {len(values)} dimensions; expected {dimensions}"
            )
        return tuple(float(value) for value in values)

    def put(
        self,
        model: str,
        dimensions: int,
        text: str,
        vector: Sequence[float],
    ) -> None:
        if len(vector) != dimensions:
            raise EmbeddingError(
                f"embedding has {len(vector)} dimensions; expected {dimensions}"
            )
        values = array.array("f", (float(value) for value in vector))
        with self._lock:
            self._db.execute(
                """
                INSERT OR REPLACE INTO embeddings
                    (model, dimensions, input_hash, vector)
                VALUES (?, ?, ?, ?)
                """,
                (
                    model,
                    dimensions,
                    self.input_hash(text),
                    sqlite3.Binary(values.tobytes()),
                ),
            )
            self._db.commit()

    def put_many(
        self,
        model: str,
        dimensions: int,
        items: Sequence[tuple[str, Sequence[float]]],
    ) -> None:
        rows = []
        for text, vector in items:
            if len(vector) != dimensions:
                raise EmbeddingError(
                    f"embedding has {len(vector)} dimensions; expected {dimensions}"
                )
            values = array.array("f", (float(value) for value in vector))
            rows.append(
                (
                    model,
                    dimensions,
                    self.input_hash(text),
                    sqlite3.Binary(values.tobytes()),
                )
            )
        if not rows:
            return
        with self._lock:
            self._db.executemany(
                """
                INSERT OR REPLACE INTO embeddings
                    (model, dimensions, input_hash, vector)
                VALUES (?, ?, ?, ?)
                """,
                rows,
            )
            self._db.commit()

    def close(self) -> None:
        with self._lock:
            self._db.close()


class CachedEmbedder:
    """Resolve cache hits locally and batch only cache misses."""

    def __init__(self, provider: Embedder, cache: EmbeddingCache):
        self.provider = provider
        self.cache = cache

    @property
    def model(self) -> str:
        return self.provider.model

    @property
    def dimensions(self) -> int:
        return self.provider.dimensions

    def embed(self, texts: Sequence[str]) -> list[tuple[float, ...]]:
        normalized = [str(text) for text in texts]
        result: list[tuple[float, ...] | None] = [None] * len(normalized)
        misses: dict[str, list[int]] = {}
        for index, text in enumerate(normalized):
            cached = self.cache.get(self.model, self.dimensions, text)
            if cached is not None:
                result[index] = cached
            else:
                misses.setdefault(text, []).append(index)
        if misses:
            unique_texts = list(misses)
            vectors = self.provider.embed(unique_texts)
            if len(vectors) != len(unique_texts):
                raise EmbeddingError(
                    "embedding provider returned a different number of vectors "
                    "than inputs"
                )
            normalized = [self.cache.normalize(vector) for vector in vectors]
            self.cache.put_many(
                self.model,
                self.dimensions,
                list(zip(unique_texts, normalized, strict=True)),
            )
            for text, vector in zip(unique_texts, normalized, strict=True):
                for index in misses[text]:
                    result[index] = vector
        return [vector for vector in result if vector is not None]


class CacheOnlyEmbedder:
    """Resolve embeddings from a prepared cache without provider calls."""

    def __init__(
        self,
        cache: EmbeddingCache,
        *,
        model: str,
        dimensions: int,
    ):
        self.cache = cache
        self._model = str(model)
        self._dimensions = int(dimensions)
        if self._dimensions <= 0:
            raise ValueError("embedding dimensions must be positive")

    @property
    def model(self) -> str:
        return self._model

    @property
    def dimensions(self) -> int:
        return self._dimensions

    def embed(self, texts: Sequence[str]) -> list[tuple[float, ...]]:
        result: list[tuple[float, ...]] = []
        missing: list[str] = []
        for text in texts:
            value = self.cache.get(self.model, self.dimensions, str(text))
            if value is None:
                missing.append(EmbeddingCache.input_hash(str(text)))
            else:
                result.append(value)
        if missing:
            preview = ", ".join(missing[:3])
            suffix = "" if len(missing) <= 3 else f", and {len(missing) - 3} more"
            raise EmbeddingError(
                "embedding cache miss during deterministic replay: "
                f"{preview}{suffix}"
            )
        return result


@dataclass(frozen=True)
class SentenceTransformerEmbeddingConfig:
    """Configuration for a local Hugging Face SentenceTransformer model."""

    model: str = DEFAULT_SENTENCE_TRANSFORMER_MODEL
    dimensions: int = DEFAULT_SENTENCE_TRANSFORMER_DIMENSIONS
    batch_size: int = 128
    workers: int = 4
    chunk_size: int = 512
    device: str | None = None
    backend: str = "torch"
    model_file: str | None = None
    normalize_embeddings: bool = True


class SentenceTransformerEmbedder:
    """Local SentenceTransformer encoder with an optional process pool."""

    def __init__(
        self,
        *,
        config: SentenceTransformerEmbeddingConfig | None = None,
        encoder: Any | None = None,
    ):
        self.config = config or SentenceTransformerEmbeddingConfig()
        if self.config.dimensions <= 0:
            raise ValueError("embedding dimensions must be positive")
        if self.config.batch_size <= 0:
            raise ValueError("embedding batch_size must be positive")
        if self.config.workers <= 0:
            raise ValueError("embedding workers must be positive")
        if self.config.chunk_size <= 0:
            raise ValueError("embedding chunk_size must be positive")
        self._process_executor: ProcessPoolExecutor | None = None
        self._spawned_onnx_pool = (
            encoder is None
            and self.config.backend == "onnx"
            and self.config.workers > 1
        )
        if self._spawned_onnx_pool:
            self.encoder = None
            self._pool = None
            self._closed = False
            self._dimensions = self.config.dimensions
            return
        if encoder is None:
            try:
                from sentence_transformers import SentenceTransformer
            except ImportError as exc:  # pragma: no cover - dependency error path
                raise EmbeddingError(
                    "sentence-transformers is required for local embeddings"
                ) from exc
            model_kwargs = (
                {"file_name": self.config.model_file}
                if self.config.model_file
                else None
            )
            encoder = SentenceTransformer(
                self.config.model,
                device=self.config.device,
                backend=self.config.backend,
                model_kwargs=model_kwargs,
            )
        self.encoder = encoder
        self._pool: dict[str, Any] | None = None
        self._closed = False
        try:
            dimension_getter = getattr(
                self.encoder,
                "get_embedding_dimension",
                None,
            ) or getattr(
                self.encoder,
                "get_sentence_embedding_dimension",
            )
            dimensions = dimension_getter()
        except (AttributeError, TypeError, ValueError) as exc:
            raise EmbeddingError(
                "SentenceTransformer did not expose its embedding dimensions"
            ) from exc
        if dimensions is None:
            raise EmbeddingError(
                "SentenceTransformer did not expose its embedding dimensions"
            )
        self._dimensions = int(dimensions)
        if self._dimensions != self.config.dimensions:
            raise EmbeddingError(
                f"SentenceTransformer model {self.config.model!r} exposes "
                f"{self._dimensions} dimensions; expected "
                f"{self.config.dimensions}"
            )

    @property
    def model(self) -> str:
        return sentence_transformer_model_id(self.config)

    @property
    def dimensions(self) -> int:
        return self._dimensions

    def embed(self, texts: Sequence[str]) -> list[tuple[float, ...]]:
        inputs = [str(text) for text in texts]
        if not inputs:
            return []
        if self._closed:
            raise EmbeddingError("SentenceTransformer embedder is closed")
        if self._spawned_onnx_pool:
            if self._process_executor is None:
                self._process_executor = ProcessPoolExecutor(
                    max_workers=self.config.workers,
                    mp_context=multiprocessing.get_context("spawn"),
                    initializer=_initialize_sentence_transformer_process,
                    initargs=(
                        self.config.model,
                        self.config.backend,
                        self.config.device,
                        self.config.model_file,
                        self.config.batch_size,
                        self.config.normalize_embeddings,
                    ),
                )
            batches = [
                inputs[start : start + self.config.chunk_size]
                for start in range(0, len(inputs), self.config.chunk_size)
            ]
            try:
                raw = self._process_executor.map(
                    _encode_sentence_transformer_process,
                    batches,
                )
                vectors = [
                    tuple(vector)
                    for batch in raw
                    for vector in batch
                ]
            except (RuntimeError, TypeError, ValueError) as exc:
                raise EmbeddingError(
                    f"parallel ONNX embedding failed: {exc}"
                ) from exc
            self._validate_vectors(vectors, expected=len(inputs))
            return vectors
        if (
            self.config.workers > 1
            and hasattr(self.encoder, "start_multi_process_pool")
        ):
            if self._pool is None:
                threads = max(
                    1,
                    (os.cpu_count() or self.config.workers)
                    // self.config.workers,
                )
                os.environ.setdefault("OMP_NUM_THREADS", str(threads))
                os.environ.setdefault("MKL_NUM_THREADS", str(threads))
                devices = [
                    self.config.device or "cpu"
                    for _ in range(self.config.workers)
                ]
                self._pool = self.encoder.start_multi_process_pool(
                    target_devices=devices
                )
            return self._encode(inputs, pool=self._pool)

        # Test doubles and older SentenceTransformers releases may not expose
        # the process-pool API. Keep a bounded fallback without weakening the
        # production path, which uses SentenceTransformers' supported pool.
        batches = [
            inputs[start : start + self.config.batch_size]
            for start in range(0, len(inputs), self.config.batch_size)
        ]
        if len(batches) <= 1:
            return self._embed_batch(batches[0]) if batches else []
        vectors: list[tuple[float, ...]] = []
        workers = min(self.config.workers, len(batches))
        with ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="sentence-transformer-embedding",
        ) as executor:
            for batch_vectors in executor.map(self._embed_batch, batches):
                vectors.extend(batch_vectors)
        return vectors

    def _embed_batch(self, texts: Sequence[str]) -> list[tuple[float, ...]]:
        if not texts:
            return []
        return self._encode(texts)

    def _encode(
        self,
        texts: Sequence[str],
        *,
        pool: dict[str, Any] | None = None,
    ) -> list[tuple[float, ...]]:
        try:
            options: dict[str, Any] = {
                "batch_size": self.config.batch_size,
                "show_progress_bar": False,
                "convert_to_numpy": True,
                "normalize_embeddings": self.config.normalize_embeddings,
            }
            if pool is not None:
                options.update(
                    {
                        "pool": pool,
                        "chunk_size": self.config.chunk_size,
                    }
                )
            encoded = self.encoder.encode(
                list(texts),
                **options,
            )
            raw_vectors = encoded.tolist() if hasattr(encoded, "tolist") else encoded
            vectors = [
                tuple(float(value) for value in vector) for vector in raw_vectors
            ]
        except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
            raise EmbeddingError(
                f"SentenceTransformer embedding failed: {exc}"
            ) from exc
        self._validate_vectors(vectors, expected=len(texts))
        return vectors

    def _validate_vectors(
        self,
        vectors: Sequence[Sequence[float]],
        *,
        expected: int,
    ) -> None:
        if len(vectors) != expected:
            raise EmbeddingError(
                "SentenceTransformer returned a different number of embeddings "
                "than inputs"
            )
        for vector in vectors:
            if len(vector) != self.dimensions:
                raise EmbeddingError(
                    f"SentenceTransformer returned {len(vector)} dimensions; "
                    f"expected {self.dimensions}"
                )
            if not all(math.isfinite(value) for value in vector):
                raise EmbeddingError(
                    "SentenceTransformer returned a non-finite embedding"
                )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._pool is not None:
            self.encoder.stop_multi_process_pool(self._pool)
            self._pool = None
        if self._process_executor is not None:
            self._process_executor.shutdown(wait=True, cancel_futures=True)
            self._process_executor = None


def sentence_transformer_model_id(
    config: SentenceTransformerEmbeddingConfig,
) -> str:
    """Return the cache/snapshot identity for an exact local runtime variant."""

    if config.backend == "torch":
        return config.model
    variant = f":{config.model_file}" if config.model_file else ""
    return f"{config.model}#{config.backend}{variant}"


@dataclass(frozen=True)
class OpenRouterEmbeddingConfig:
    model: str = "openai/text-embedding-3-small"
    dimensions: int = 1536
    base_url: str = "https://openrouter.ai/api/v1"
    batch_size: int = 128
    timeout_seconds: float = 60.0
    max_attempts: int = 4
    max_parallel_batches: int = 4


class OpenRouterEmbedder:
    """OpenRouter embeddings client with batching and bounded retries."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        config: OpenRouterEmbeddingConfig | None = None,
        client: httpx.Client | None = None,
        app_url: str | None = None,
        app_name: str = "Chronos Enterprise Knowledge",
    ):
        self.config = config or OpenRouterEmbeddingConfig()
        if self.config.dimensions <= 0:
            raise ValueError("embedding dimensions must be positive")
        if self.config.batch_size <= 0:
            raise ValueError("embedding batch_size must be positive")
        if self.config.max_parallel_batches <= 0:
            raise ValueError("max_parallel_batches must be positive")
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY")
        if not self.api_key:
            raise EmbeddingError(
                "OPENROUTER_API_KEY is required to generate embeddings"
            )
        self._owns_client = client is None
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "X-Title": app_name,
        }
        if app_url:
            headers["HTTP-Referer"] = app_url
        self.client = client or httpx.Client(
            base_url=self.config.base_url.rstrip("/"),
            timeout=self.config.timeout_seconds,
            headers=headers,
        )

    @property
    def model(self) -> str:
        return self.config.model

    @property
    def dimensions(self) -> int:
        return self.config.dimensions

    def embed(self, texts: Sequence[str]) -> list[tuple[float, ...]]:
        inputs = [str(text) for text in texts]
        batches = [
            inputs[start : start + self.config.batch_size]
            for start in range(0, len(inputs), self.config.batch_size)
        ]
        if len(batches) <= 1:
            return self._embed_batch(batches[0]) if batches else []
        vectors: list[tuple[float, ...]] = []
        workers = min(self.config.max_parallel_batches, len(batches))
        with ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="openrouter-embedding",
        ) as executor:
            for batch_vectors in executor.map(self._embed_batch, batches):
                vectors.extend(batch_vectors)
        return vectors

    def _embed_batch(self, texts: Sequence[str]) -> list[tuple[float, ...]]:
        if not texts:
            return []
        payload = {
            "model": self.model,
            "input": list(texts),
            "dimensions": self.dimensions,
            "encoding_format": "float",
        }
        last_error: Exception | None = None
        for attempt in range(self.config.max_attempts):
            try:
                response = self.client.post("/embeddings", json=payload)
                response.raise_for_status()
                body = response.json()
                data = sorted(body["data"], key=lambda item: int(item["index"]))
                if len(data) != len(texts):
                    raise EmbeddingError(
                        "OpenRouter returned a different number of embeddings "
                        "than inputs"
                    )
                result = [
                    tuple(float(value) for value in item["embedding"]) for item in data
                ]
                for vector in result:
                    if len(vector) != self.dimensions:
                        raise EmbeddingError(
                            f"OpenRouter returned {len(vector)} dimensions; "
                            f"expected {self.dimensions}"
                        )
                return result
            except (
                httpx.HTTPError,
                KeyError,
                TypeError,
                ValueError,
                EmbeddingError,
            ) as exc:
                last_error = exc
                if attempt + 1 >= self.config.max_attempts:
                    break
                delay = min(8.0, 0.5 * (2**attempt))
                delay += random.random() * 0.1
                time.sleep(delay)
        raise EmbeddingError(f"OpenRouter embedding request failed: {last_error}")

    def close(self) -> None:
        if self._owns_client:
            self.client.close()


class HashEmbedder:
    """Deterministic local embedder for tests and offline development.

    It hashes lexical tokens into a signed bag-of-words vector. It is not a
    production semantic model, but shared terms produce useful deterministic
    similarity without a network call.
    """

    def __init__(self, dimensions: int = 64):
        if dimensions <= 0:
            raise ValueError("dimensions must be positive")
        self._dimensions = dimensions

    @property
    def model(self) -> str:
        return "chronos/hash-embedding-v1"

    @property
    def dimensions(self) -> int:
        return self._dimensions

    def embed(self, texts: Sequence[str]) -> list[tuple[float, ...]]:
        return [self._embed_one(str(text)) for text in texts]

    def _embed_one(self, text: str) -> tuple[float, ...]:
        vector = [0.0] * self.dimensions
        for token in _lexical_tokens(text):
            digest = hashlib.blake2b(token.encode(), digest_size=16).digest()
            index = int.from_bytes(digest[:8], "big") % self.dimensions
            sign = 1.0 if digest[8] & 1 else -1.0
            vector[index] += sign
        norm = math.sqrt(sum(value * value for value in vector))
        if norm:
            vector = [value / norm for value in vector]
        return tuple(vector)


def _lexical_tokens(text: str) -> list[str]:
    token = []
    result = []
    for character in text.casefold():
        if character.isalnum() or character in "_-":
            token.append(character)
        elif token:
            result.append("".join(token))
            token.clear()
    if token:
        result.append("".join(token))
    return result


__all__ = [
    "CacheOnlyEmbedder",
    "CachedEmbedder",
    "DEFAULT_SENTENCE_TRANSFORMER_DIMENSIONS",
    "DEFAULT_SENTENCE_TRANSFORMER_MODEL",
    "Embedder",
    "EmbeddingCache",
    "EmbeddingError",
    "HashEmbedder",
    "OpenRouterEmbedder",
    "OpenRouterEmbeddingConfig",
    "SentenceTransformerEmbedder",
    "SentenceTransformerEmbeddingConfig",
    "ZeroEmbedder",
    "sentence_transformer_model_id",
]
