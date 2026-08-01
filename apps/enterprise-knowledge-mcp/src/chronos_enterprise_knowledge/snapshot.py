"""Reusable, content-addressed EnterpriseRAG chunk and embedding snapshots."""

from __future__ import annotations

import array
import hashlib
import json
import math
import sqlite3
import time
import zlib
from collections import Counter, defaultdict
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np

from chronos_enterprise_knowledge.backend import KnowledgeBackend
from chronos_enterprise_knowledge.chunking import ChunkingConfig, EnterpriseChunker
from chronos_enterprise_knowledge.embedding import Embedder
from chronos_enterprise_knowledge.enterprise_rag import (
    CorpusRecord,
    EnterpriseRAGCorpus,
)
from chronos_enterprise_knowledge.ingestion import IngestionStats
from chronos_enterprise_knowledge.models import (
    DocumentChunk,
    IndexedDocument,
    KnowledgeDocument,
    canonical_json,
)

_SCHEMA_VERSION = 3


@dataclass(frozen=True)
class SnapshotSpec:
    """Inputs that determine the exact reusable artifact."""

    sample_fraction: float = 0.10
    sample_seed: str = "chronos-enterprise-rag-v1"
    embedding_model: str = "openai/text-embedding-3-small"
    dimensions: int = 1536
    target_tokens: int = 512
    overlap_tokens: int = 64
    encoding: str = "cl100k_base"
    vector_dtype: str = "float16"
    text_compression: str = "zlib"

    def __post_init__(self) -> None:
        if not 0 < self.sample_fraction <= 1:
            raise ValueError("sample_fraction must be in (0, 1]")
        if self.dimensions <= 0:
            raise ValueError("dimensions must be positive")
        if self.vector_dtype not in {"float16", "float32"}:
            raise ValueError("vector_dtype must be float16 or float32")
        if self.text_compression not in {"none", "zlib"}:
            raise ValueError("text_compression must be none or zlib")
        ChunkingConfig(
            target_tokens=self.target_tokens,
            overlap_tokens=self.overlap_tokens,
            encoding=self.encoding,
        )


@dataclass(frozen=True)
class CorpusSelection:
    paths: tuple[Path, ...]
    totals_by_connector: dict[str, int]
    selected_by_connector: dict[str, int]
    digest: str

    @property
    def total_documents(self) -> int:
        return sum(self.totals_by_connector.values())

    @property
    def selected_documents(self) -> int:
        return len(self.paths)


def select_corpus_paths(
    corpus: EnterpriseRAGCorpus,
    *,
    fraction: float,
    seed: str,
    max_documents: int | None = None,
    connectors: Sequence[str] | None = None,
) -> CorpusSelection:
    """Select a stable, connector-stratified sample without reading documents."""

    if not 0 < fraction <= 1:
        raise ValueError("fraction must be in (0, 1]")
    if max_documents is not None and max_documents <= 0:
        raise ValueError("max_documents must be positive")

    allowed = {connector.casefold() for connector in connectors or ()}

    if fraction == 1.0 and max_documents is None:
        # Avoid hashing, ranking, and retaining a second relative-path string for
        # every item when the caller explicitly selected the complete corpus.
        # ``iter_paths`` is stable, but the root documents are not lexicographic,
        # so retain the final sort used by sampled selections.
        selected = sorted(
            (
                path
                for path in corpus.iter_paths(connectors=allowed)
            ),
            key=lambda path: _relative_path_text(corpus.root, path),
        )
        counts: Counter[str] = Counter()
        for path in selected:
            counts[corpus.connector_for_path(path)] += 1
        connector_counts = dict(sorted(counts.items()))
        return CorpusSelection(
            paths=tuple(selected),
            totals_by_connector=connector_counts,
            selected_by_connector=connector_counts,
            digest=_selection_digest(corpus.root, selected),
        )

    grouped: dict[str, list[tuple[Path, str]]] = defaultdict(list)
    for path in corpus.iter_paths(connectors=allowed):
        connector = corpus.connector_for_path(path)
        relative = _relative_path_text(corpus.root, path)
        grouped[connector].append((path, relative))

    selected: list[Path] = []
    selected_counts: dict[str, int] = {}
    for connector, entries in sorted(grouped.items()):
        # The four company-level documents define the shared root and are small
        # enough to retain in every sampled corpus.
        count = (
            len(entries)
            if connector == "company"
            else math.ceil(len(entries) * fraction)
        )
        ranked = sorted(
            entries,
            key=lambda item: (
                hashlib.sha256(f"{seed}:{item[1]}".encode()).digest(),
                item[1],
            ),
        )
        chosen = [path for path, _ in ranked[:count]]
        selected.extend(chosen)
        selected_counts[connector] = len(chosen)

    selected.sort(key=lambda path: _relative_path_text(corpus.root, path))
    if max_documents is not None and len(selected) > max_documents:
        company = [
            path
            for path in selected
            if path.parent == corpus.root
        ]
        company_paths = set(company)
        remainder = [path for path in selected if path not in company_paths]
        ranked = sorted(
            remainder,
            key=lambda path: (
                hashlib.sha256(
                    (
                        f"{seed}:limit:"
                        f"{_relative_path_text(corpus.root, path)}"
                    ).encode()
                ).digest(),
                _relative_path_text(corpus.root, path),
            ),
        )
        selected = (company + ranked)[:max_documents]
        selected.sort(key=lambda path: _relative_path_text(corpus.root, path))
        selected_counts = dict(
            Counter(corpus.connector_for_path(path) for path in selected)
        )

    return CorpusSelection(
        paths=tuple(selected),
        totals_by_connector={
            connector: len(paths) for connector, paths in sorted(grouped.items())
        },
        selected_by_connector=dict(sorted(selected_counts.items())),
        digest=_selection_digest(corpus.root, selected),
    )


def _selection_digest(corpus_root: Path, paths: Sequence[Path]) -> str:
    """Hash the canonical relative-path array without another corpus-sized list."""

    digest = hashlib.sha256()
    digest.update(b"[")
    for index, path in enumerate(paths):
        if index:
            digest.update(b",")
        relative = _relative_path_text(corpus_root, path)
        digest.update(
            json.dumps(
                relative,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
    digest.update(b"]")
    return digest.hexdigest()


def _relative_path_text(corpus_root: Path, path: Path) -> str:
    """Return a trusted corpus path without pathlib's repeated parent walk."""

    root = corpus_root.as_posix().rstrip("/")
    value = path.as_posix()
    prefix = f"{root}/"
    if not value.startswith(prefix):
        raise ValueError(f"document lies outside the corpus: {path}")
    return value[len(prefix) :]


class EmbeddingSnapshot:
    """SQLite artifact containing source documents, chunks, and float32 vectors."""

    def __init__(self, directory: str | Path):
        self.directory = Path(directory).expanduser().resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "snapshot.sqlite"
        self.manifest_path = self.directory / "manifest.json"
        database_exists = self.path.exists()
        self._db = sqlite3.connect(self.path, timeout=30.0)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA busy_timeout=30000")
        if not database_exists:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=NORMAL")
        schema_version = 0
        if database_exists and self.manifest_path.is_file():
            try:
                schema_version = int(
                    json.loads(self.manifest_path.read_text())[
                        "schema_version"
                    ]
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                schema_version = 0
        if schema_version < _SCHEMA_VERSION:
            self._ensure_schema()
        self._dimensions: int | None = None
        self._metadata_cache: dict[str, Any] | None = None

    def _ensure_schema(self) -> None:
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS snapshot_metadata (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                relative_path TEXT NOT NULL UNIQUE,
                connector TEXT NOT NULL,
                path TEXT NOT NULL,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                kind TEXT NOT NULL,
                content TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                byte_count INTEGER NOT NULL,
                chunk_count INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS documents_connector_path
                ON documents(connector, relative_path);
            CREATE TABLE IF NOT EXISTS chunks (
                id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                text TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                embedding BLOB NOT NULL,
                embedding_ready INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY(document_id) REFERENCES documents(id)
            );
            CREATE INDEX IF NOT EXISTS chunks_document_ordinal
                ON chunks(document_id, ordinal);
            """
        )
        columns = {
            str(row["name"])
            for row in self._db.execute("PRAGMA table_info(chunks)")
        }
        if "embedding_ready" not in columns:
            self._db.execute(
                """
                ALTER TABLE chunks
                ADD COLUMN embedding_ready INTEGER NOT NULL DEFAULT 1
                """
            )
        self._db.execute(
            """
            CREATE INDEX IF NOT EXISTS chunks_embedding_ready
            ON chunks(embedding_ready, id)
            """
        )
        self._db.commit()

    def initialize(
        self,
        *,
        corpus_root: Path,
        spec: SnapshotSpec,
        selection: CorpusSelection,
    ) -> None:
        expected = {
            "schema_version": _SCHEMA_VERSION,
            "corpus_root": str(corpus_root),
            "spec": asdict(spec),
            "selection_digest": selection.digest,
            "totals_by_connector": selection.totals_by_connector,
            "selected_by_connector": selection.selected_by_connector,
            "selected_documents": selection.selected_documents,
        }
        current = self.metadata()
        if current:
            mismatches = {
                key: (current.get(key), value)
                for key, value in expected.items()
                if current.get(key) != value
            }
            if mismatches:
                names = ", ".join(sorted(mismatches))
                raise ValueError(
                    "snapshot inputs differ from the existing artifact: "
                    f"{names}. Use a new snapshot directory."
                )
            self._dimensions = spec.dimensions
            return
        self._db.executemany(
            """
            INSERT INTO snapshot_metadata(key, value_json)
            VALUES (?, ?)
            """,
            [
                (key, canonical_json(value))
                for key, value in sorted(expected.items())
            ],
        )
        self._db.commit()
        self._metadata_cache = expected
        self._dimensions = spec.dimensions
        self.write_manifest(complete=False)

    def metadata(self) -> dict[str, Any]:
        if self._metadata_cache is not None:
            return self._metadata_cache
        self._metadata_cache = {
            str(row["key"]): json.loads(str(row["value_json"]))
            for row in self._db.execute(
                "SELECT key, value_json FROM snapshot_metadata ORDER BY key"
            )
        }
        return self._metadata_cache

    @property
    def dimensions(self) -> int:
        if self._dimensions is not None:
            return self._dimensions
        spec = self.metadata().get("spec")
        if not isinstance(spec, Mapping):
            raise RuntimeError("snapshot has not been initialized")
        self._dimensions = int(spec["dimensions"])
        return self._dimensions

    @property
    def _schema_version(self) -> int:
        return int(self.metadata().get("schema_version", 1))

    @property
    def _vector_dtype(self) -> str:
        spec = self.metadata().get("spec")
        if self._schema_version < 2 or not isinstance(spec, Mapping):
            return "float32"
        return str(spec.get("vector_dtype", "float32"))

    @property
    def _text_compression(self) -> str:
        spec = self.metadata().get("spec")
        if self._schema_version < 2 or not isinstance(spec, Mapping):
            return "none"
        return str(spec.get("text_compression", "none"))

    def _encode_text(self, value: str) -> str | sqlite3.Binary:
        if self._text_compression == "zlib":
            return sqlite3.Binary(zlib.compress(value.encode("utf-8"), level=3))
        return value

    def _decode_text(self, value: Any) -> str:
        if self._text_compression == "zlib":
            return zlib.decompress(bytes(value)).decode("utf-8")
        return str(value)

    def has_document(self, document_id: str, content_hash: str) -> bool:
        row = self._db.execute(
            """
            SELECT content_hash, chunk_count,
                   (SELECT COUNT(*) FROM chunks WHERE document_id = documents.id)
                       AS stored_chunks
            FROM documents
            WHERE id = ?
            """,
            (document_id,),
        ).fetchone()
        return bool(
            row is not None
            and str(row["content_hash"]) == content_hash
            and int(row["chunk_count"]) == int(row["stored_chunks"])
        )

    def put_batch(
        self,
        items: Sequence[tuple[CorpusRecord, IndexedDocument, str, str]],
    ) -> None:
        with self._db:
            for record, indexed, relative_path, connector in items:
                self._db.execute(
                    "DELETE FROM chunks WHERE document_id = ?",
                    (indexed.document.id,),
                )
                self._db.execute(
                    """
                    INSERT OR REPLACE INTO documents(
                        id, relative_path, connector, path, title, source, kind,
                        content, content_hash, metadata_json, byte_count, chunk_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        indexed.document.id,
                        relative_path,
                        connector,
                        indexed.document.path,
                        indexed.document.title,
                        indexed.document.source,
                        indexed.document.kind,
                        self._encode_text(indexed.document.content),
                        indexed.document.sha256,
                        canonical_json(indexed.document.metadata),
                        len(indexed.document.content.encode()),
                        len(indexed.chunks),
                    ),
                )
                chunk_rows = []
                for chunk in indexed.chunks:
                    encoded_vector = _vector_bytes(
                        chunk.embedding,
                        dtype=self._vector_dtype,
                    )
                    chunk_rows.append(
                        (
                            chunk.id,
                            chunk.document_id,
                            chunk.ordinal,
                            self._encode_text(chunk.text),
                            chunk.sha256,
                            canonical_json(chunk.metadata),
                            sqlite3.Binary(encoded_vector),
                            int(bool(encoded_vector)),
                        )
                    )
                self._db.executemany(
                    """
                    INSERT INTO chunks(
                        id, document_id, ordinal, text, content_hash,
                        metadata_json, embedding, embedding_ready
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    chunk_rows,
                )

    def stats(self) -> IngestionStats:
        row = self._db.execute(
            """
            SELECT COUNT(*) AS documents,
                   COALESCE(SUM(chunk_count), 0) AS chunks,
                   COALESCE(SUM(byte_count), 0) AS bytes
            FROM documents
            """
        ).fetchone()
        return IngestionStats(
            documents=int(row["documents"]),
            chunks=int(row["chunks"]),
            bytes=int(row["bytes"]),
        )

    def embedding_progress(self) -> dict[str, int | bool]:
        row = self._db.execute(
            """
            SELECT COUNT(*) AS total,
                   COALESCE(SUM(embedding_ready), 0) AS ready
            FROM chunks
            """
        ).fetchone()
        total = int(row["total"])
        ready = int(row["ready"])
        return {
            "total_chunks": total,
            "embedded_chunks": ready,
            "remaining_chunks": total - ready,
            "embeddings_complete": ready == total,
        }

    def backfill_embeddings(
        self,
        embedder: Embedder,
        *,
        batch_size: int = 2_048,
        follow_preparation: bool = False,
        poll_seconds: float = 5.0,
        progress: Callable[[dict[str, int | bool]], None] | None = None,
    ) -> dict[str, int | bool]:
        """Replace implicit zero placeholders without rechunking documents."""

        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        spec = self.metadata().get("spec")
        if not isinstance(spec, Mapping):
            raise RuntimeError("snapshot has not been initialized")
        expected_model = str(spec["embedding_model"])
        if embedder.model != expected_model:
            raise ValueError(
                f"embedder model {embedder.model!r} does not match snapshot "
                f"model {expected_model!r}"
            )
        if embedder.dimensions != self.dimensions:
            raise ValueError("embedder dimensions do not match snapshot")
        state = self.embedding_progress()
        if progress is not None:
            progress(state)
        selected_documents = int(
            self.metadata().get("selected_documents", 0)
        )
        while True:
            rows = self._db.execute(
                """
                SELECT id, text
                FROM chunks
                WHERE embedding_ready = 0
                ORDER BY id
                LIMIT ?
                """,
                (batch_size,),
            ).fetchall()
            if not rows:
                prepared = self.stats().documents == selected_documents
                if not follow_preparation or prepared:
                    break
                time.sleep(poll_seconds)
                state = self.embedding_progress()
                continue
            vectors = embedder.embed(
                [self._decode_text(row["text"]) for row in rows]
            )
            if len(vectors) != len(rows):
                raise RuntimeError(
                    "embedding provider returned a different number of vectors"
                )
            with self._db:
                self._db.executemany(
                    """
                    UPDATE chunks
                    SET embedding = ?, embedding_ready = 1
                    WHERE id = ?
                    """,
                    [
                        (
                            sqlite3.Binary(
                                _vector_bytes(vector, dtype=self._vector_dtype)
                            ),
                            str(row["id"]),
                        )
                        for row, vector in zip(rows, vectors, strict=True)
                    ],
                )
            # Preparation may append more chunks while the encoder is running,
            # so derive progress from the current database instead of a stale
            # initial total.
            state = self.embedding_progress()
            if progress is not None:
                progress(state)
        prepared = self.stats().documents == selected_documents
        if prepared and bool(state["embeddings_complete"]):
            self.write_manifest(complete=True)
        return state

    def import_embeddings(
        self,
        source: EmbeddingSnapshot,
        *,
        batch_size: int = 4_096,
        progress: Callable[[dict[str, int | bool]], None] | None = None,
    ) -> dict[str, int | bool]:
        """Reuse compatible vectors from another partial or complete snapshot."""

        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        target_spec = self.metadata().get("spec")
        source_spec = source.metadata().get("spec")
        if not isinstance(target_spec, Mapping) or not isinstance(
            source_spec,
            Mapping,
        ):
            raise RuntimeError("both snapshots must be initialized")
        compatibility_keys = (
            "embedding_model",
            "dimensions",
            "target_tokens",
            "overlap_tokens",
            "encoding",
            "vector_dtype",
        )
        mismatches = [
            key
            for key in compatibility_keys
            if target_spec.get(key) != source_spec.get(key)
        ]
        if mismatches:
            raise ValueError(
                "source snapshot embeddings are incompatible: "
                + ", ".join(mismatches)
            )

        source_columns = {
            str(row["name"])
            for row in source._db.execute("PRAGMA table_info(chunks)")
        }
        ready_predicate = (
            "embedding_ready = 1 AND length(embedding) > 0"
            if "embedding_ready" in source_columns
            else "length(embedding) > 0"
        )
        cursor = source._db.execute(
            f"""
            SELECT id, content_hash, embedding
            FROM chunks
            WHERE {ready_predicate}
            ORDER BY id
            """
        )
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            with self._db:
                self._db.executemany(
                    """
                    UPDATE chunks
                    SET embedding = ?, embedding_ready = 1
                    WHERE id = ?
                      AND content_hash = ?
                      AND embedding_ready = 0
                    """,
                    [
                        (
                            sqlite3.Binary(bytes(row["embedding"])),
                            str(row["id"]),
                            str(row["content_hash"]),
                        )
                        for row in rows
                    ],
                )
            if progress is not None:
                progress(self.embedding_progress())
        state = self.embedding_progress()
        if progress is not None:
            progress(state)
        return state

    def document_stats(self, document_ids: Sequence[str]) -> IngestionStats:
        """Return aggregate stored sizes for a bounded document batch."""

        if not document_ids:
            return IngestionStats(0, 0, 0)
        placeholders = ", ".join("?" for _ in document_ids)
        row = self._db.execute(
            f"""
            SELECT COUNT(*) AS documents,
                   COALESCE(SUM(chunk_count), 0) AS chunks,
                   COALESCE(SUM(byte_count), 0) AS bytes
            FROM documents
            WHERE id IN ({placeholders})
            """,
            [str(identifier) for identifier in document_ids],
        ).fetchone()
        return IngestionStats(
            documents=int(row["documents"]),
            chunks=int(row["chunks"]),
            bytes=int(row["bytes"]),
        )

    def write_manifest(
        self,
        *,
        complete: bool,
        stats: IngestionStats | None = None,
    ) -> None:
        metadata = self.metadata()
        current_stats = stats or self.stats()
        selected = int(metadata.get("selected_documents", 0))
        embedding_state = self.embedding_progress() if complete else {}
        value = {
            **metadata,
            "artifact": str(self.path),
            "documents": current_stats.documents,
            "chunks": current_stats.chunks,
            "bytes": current_stats.bytes,
            "complete": bool(complete and current_stats.documents == selected),
            **embedding_state,
        }
        temporary = self.manifest_path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(value, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.manifest_path)

    def iter_indexed_documents(
        self,
        *,
        batch_size: int = 32,
        document_ids: Sequence[str] | None = None,
        max_documents: int | None = None,
        after_relative_path: str | None = None,
        load_embeddings: bool = True,
    ) -> Iterator[list[IndexedDocument]]:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        selected = self.selected_document_ids(
            required=document_ids or (),
            max_documents=max_documents,
        )
        if selected is None:
            if after_relative_path is None:
                rows = self._db.execute(
                    """
                    SELECT id, path, title, source, kind, content, metadata_json
                    FROM documents
                    ORDER BY relative_path
                    """
                )
            else:
                rows = self._db.execute(
                    """
                    SELECT id, path, title, source, kind, content, metadata_json
                    FROM documents
                    WHERE relative_path > ?
                    ORDER BY relative_path
                    """,
                    (after_relative_path,),
                )
        elif not selected:
            return
        else:
            placeholders = ", ".join("?" for _ in selected)
            cursor_predicate = (
                ""
                if after_relative_path is None
                else " AND relative_path > ?"
            )
            params: tuple[Any, ...] = tuple(selected)
            if after_relative_path is not None:
                params = (*params, after_relative_path)
            rows = list(
                self._db.execute(
                    f"""
                SELECT id, relative_path, path, title, source, kind, content,
                       metadata_json
                FROM documents
                WHERE id IN ({placeholders})
                {cursor_predicate}
                """,
                    params,
                )
            )
            rows.sort(key=lambda row: str(row["relative_path"]))
        batch: list[IndexedDocument] = []
        for row in rows:
            document = KnowledgeDocument(
                id=str(row["id"]),
                path=str(row["path"]),
                title=str(row["title"]),
                source=str(row["source"]),
                content=self._decode_text(row["content"]),
                kind=str(row["kind"]),  # type: ignore[arg-type]
                metadata=json.loads(str(row["metadata_json"])),
            )
            chunk_rows = self._db.execute(
                """
                SELECT id, document_id, ordinal, text, metadata_json, embedding
                FROM chunks
                WHERE document_id = ?
                ORDER BY ordinal, id
                """,
                (document.id,),
            )
            chunks = tuple(
                DocumentChunk(
                    id=str(chunk["id"]),
                    document_id=str(chunk["document_id"]),
                    ordinal=int(chunk["ordinal"]),
                    text=self._decode_text(chunk["text"]),
                    embedding=(
                        _vector_from_bytes(
                            bytes(chunk["embedding"]),
                            self.dimensions,
                            dtype=self._vector_dtype,
                        )
                        if load_embeddings
                        else ()
                    ),
                    metadata=json.loads(str(chunk["metadata_json"])),
                )
                for chunk in chunk_rows
            )
            batch.append(IndexedDocument(document, chunks))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def selected_document_ids(
        self,
        *,
        required: Sequence[str] = (),
        max_documents: int | None = None,
    ) -> tuple[str, ...] | None:
        """Select a deterministic subset while always retaining required IDs.

        ``None`` means every document in the snapshot. This keeps the default
        ingestion path streaming and avoids materializing a 50K-element ID
        list when no subset was requested.
        """

        if max_documents is not None and max_documents <= 0:
            raise ValueError("max_documents must be positive")
        required_ids = {str(identifier) for identifier in required}
        if not required_ids and max_documents is None:
            return None
        selected = set(required_ids)
        if max_documents is not None:
            selected.update(
                str(row["id"])
                for row in self._db.execute(
                    """
                    SELECT id
                    FROM documents
                    ORDER BY relative_path
                    LIMIT ?
                    """,
                    (max_documents,),
                )
            )
        if required_ids:
            placeholders = ", ".join("?" for _ in required_ids)
            present = {
                str(row["id"])
                for row in self._db.execute(
                    f"SELECT id FROM documents WHERE id IN ({placeholders})",
                    sorted(required_ids),
                )
            }
            missing = sorted(required_ids - present)
            if missing:
                raise KeyError(
                    "required snapshot documents are missing: "
                    + ", ".join(missing)
                )
        if not selected:
            return ()
        placeholders = ", ".join("?" for _ in selected)
        rows = list(
            self._db.execute(
                f"""
                SELECT id, relative_path
                FROM documents
                WHERE id IN ({placeholders})
                """,
                sorted(selected),
            )
        )
        rows.sort(key=lambda row: str(row["relative_path"]))
        return tuple(str(row["id"]) for row in rows)

    def source_catalog(
        self,
        scopes: Sequence[str],
        *,
        per_scope: int = 3,
    ) -> list[dict[str, str]]:
        """Return representative sampled sources for human-readable guides."""

        result: list[dict[str, str]] = []
        seen: set[str] = set()
        for scope in scopes:
            normalized = str(scope).strip().strip("/")
            rows = self._db.execute(
                """
                SELECT id, relative_path, title, source, content
                FROM documents
                WHERE relative_path = ?
                   OR relative_path LIKE ?
                   OR relative_path LIKE ?
                   OR relative_path LIKE ?
                ORDER BY relative_path
                LIMIT ?
                """,
                (
                    normalized,
                    f"{normalized}/%",
                    f"sources/{normalized}/%",
                    f"sources/{normalized}%",
                    int(per_scope),
                ),
            )
            for row in rows:
                identifier = str(row["id"])
                if identifier in seen:
                    continue
                seen.add(identifier)
                excerpt = " ".join(
                    self._decode_text(row["content"]).split()
                )[:240]
                result.append(
                    {
                        "id": identifier,
                        "relative_path": str(row["relative_path"]),
                        "title": str(row["title"]),
                        "source": str(row["source"]),
                        "excerpt": excerpt,
                    }
                )
        return result

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> EmbeddingSnapshot:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class SnapshotBuilder:
    """Prepare a resumable snapshot while embedding every unique chunk once."""

    def __init__(
        self,
        corpus: EnterpriseRAGCorpus,
        snapshot: EmbeddingSnapshot,
        embedder: Embedder,
        spec: SnapshotSpec,
        *,
        chunk_workers: int = 1,
    ):
        if embedder.model != spec.embedding_model:
            raise ValueError(
                f"embedder model {embedder.model!r} does not match snapshot spec "
                f"{spec.embedding_model!r}"
            )
        if embedder.dimensions != spec.dimensions:
            raise ValueError("embedder dimensions do not match snapshot spec")
        if chunk_workers <= 0:
            raise ValueError("chunk_workers must be positive")
        self.corpus = corpus
        self.snapshot = snapshot
        self.embedder = embedder
        self.spec = spec
        self.chunk_workers = int(chunk_workers)
        self.chunker = EnterpriseChunker(
            ChunkingConfig(
                target_tokens=spec.target_tokens,
                overlap_tokens=spec.overlap_tokens,
                encoding=spec.encoding,
            )
        )

    def build(
        self,
        selection: CorpusSelection,
        *,
        batch_size: int = 32,
        checkpoint_every: int = 1_000,
        progress: Callable[[IngestionStats], None] | None = None,
    ) -> IngestionStats:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if checkpoint_every <= 0:
            raise ValueError("checkpoint_every must be positive")
        self.snapshot.initialize(
            corpus_root=self.corpus.root,
            spec=self.spec,
            selection=selection,
        )
        stats = self.snapshot.stats()
        if progress is not None:
            # A resumed run now reports its already-persisted starting point,
            # including the fully-complete case where no new batch is prepared.
            progress(stats)
        pending_records: list[tuple[CorpusRecord, str, str]] = []
        uncheckpointed_documents = 0
        for path in selection.paths:
            record = self.corpus.read_record(path)
            if self.snapshot.has_document(
                record.document.id,
                record.document.sha256,
            ):
                continue
            pending_records.append(
                (
                    record,
                    self.corpus.relative_path(path).as_posix(),
                    self.corpus.connector_for_path(path),
                )
            )
            if len(pending_records) >= batch_size:
                delta = self._prepare_batch(pending_records)
                stats = _add_stats(stats, delta)
                uncheckpointed_documents += len(pending_records)
                pending_records = []
                if uncheckpointed_documents >= checkpoint_every:
                    self.snapshot.write_manifest(complete=False, stats=stats)
                    uncheckpointed_documents = 0
                if progress is not None:
                    progress(stats)
        if pending_records:
            delta = self._prepare_batch(pending_records)
            stats = _add_stats(stats, delta)
            if progress is not None:
                progress(stats)
        # Reconcile with SQLite once at the end. This catches an interrupted
        # replacement batch on resume without turning every batch into a full
        # aggregate scan over a growing corpus.
        stats = self.snapshot.stats()
        self.snapshot.write_manifest(
            complete=stats.documents == selection.selected_documents,
            stats=stats,
        )
        return stats

    def _prepare_batch(
        self,
        records: Sequence[tuple[CorpusRecord, str, str]],
    ) -> IngestionStats:
        identifiers = [record.document.id for record, _, _ in records]
        previous = self.snapshot.document_stats(identifiers)
        if self.chunk_workers == 1:
            pending_by_record = [
                self._chunk_record(record)
                for record, _, _ in records
            ]
        else:
            with ThreadPoolExecutor(
                max_workers=self.chunk_workers,
                thread_name_prefix="enterprise-rag-chunk",
            ) as executor:
                pending_by_record = list(
                    executor.map(
                        self._chunk_record,
                        (record for record, _, _ in records),
                    )
                )
        pending = [chunk for chunks in pending_by_record for chunk in chunks]
        embeddings = self.embedder.embed([text for text, _ in pending])
        indexed_items: list[tuple[CorpusRecord, IndexedDocument, str, str]] = []
        offset = 0
        for (record, relative_path, connector), chunks in zip(
            records,
            pending_by_record,
            strict=True,
        ):
            count = len(chunks)
            embedded = self.chunker.with_embeddings(
                record.document,
                chunks,
                embeddings[offset : offset + count],
            )
            indexed_items.append(
                (
                    record,
                    IndexedDocument(record.document, embedded),
                    relative_path,
                    connector,
                )
            )
            offset += count
        self.snapshot.put_batch(indexed_items)
        current = IngestionStats(
            documents=len(indexed_items),
            chunks=sum(len(indexed.chunks) for _, indexed, _, _ in indexed_items),
            bytes=sum(
                len(indexed.document.content.encode("utf-8"))
                for _, indexed, _, _ in indexed_items
            ),
        )
        return IngestionStats(
            documents=current.documents - previous.documents,
            chunks=current.chunks - previous.chunks,
            bytes=current.bytes - previous.bytes,
        )

    def _chunk_record(
        self,
        record: CorpusRecord,
    ) -> list[tuple[str, dict[str, Any]]]:
        return self.chunker.chunk_text(
            record.document,
            record.index_text,
            context=record.context,
        )


def ingest_snapshot(
    snapshot: EmbeddingSnapshot,
    backend: KnowledgeBackend,
    branch_id: str,
    *,
    batch_size: int = 32,
    document_ids: Sequence[str] | None = None,
    max_documents: int | None = None,
    resume: bool = False,
    follow_preparation: bool = False,
    poll_seconds: float = 5.0,
    operation_prefix: str = "enterprise-rag-snapshot",
    force_zero_embeddings: bool = False,
    progress: Callable[[IngestionStats], None] | None = None,
) -> IngestionStats:
    """Bulk-load a prepared artifact into a new benchmark state."""

    if poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")
    embedding_state = (
        {"embeddings_complete": False}
        if force_zero_embeddings
        else snapshot.embedding_progress()
    )
    placeholder_mode = getattr(backend, "set_placeholder_vector_mode", None)
    if callable(placeholder_mode):
        placeholder_mode(
            force_zero_embeddings
            or not bool(embedding_state["embeddings_complete"])
        )
    documents = 0
    chunks = 0
    byte_count = 0
    metadata = snapshot.metadata()
    snapshot_identity = {
        "selection_digest": metadata.get("selection_digest"),
        "spec": metadata.get("spec"),
    }
    embeddings_complete = (
        bool(embedding_state["embeddings_complete"])
        and not force_zero_embeddings
    )
    legacy_snapshot_id = hashlib.sha256(
        canonical_json(snapshot_identity).encode()
    ).hexdigest()
    embedding_mode = (
        "forced-zero"
        if force_zero_embeddings
        else ("complete" if embeddings_complete else "placeholder")
    )
    snapshot_id = hashlib.sha256(
        canonical_json(
            {
                **snapshot_identity,
                "embedding_mode": embedding_mode,
            }
        ).encode()
    ).hexdigest()
    get_cursor = getattr(backend, "snapshot_ingestion_cursor", None)
    set_cursor = getattr(backend, "set_snapshot_ingestion_cursor", None)
    after_relative_path = None
    if resume and callable(get_cursor):
        after_relative_path = get_cursor(branch_id, snapshot_id)
        # Migrate cursors written before placeholder and complete ingestion
        # were separated. Never apply the legacy cursor to a complete snapshot,
        # because complete ingestion must revisit every document to hydrate
        # Qdrant.
        if (
            after_relative_path is None
            and not embeddings_complete
            and not force_zero_embeddings
        ):
            after_relative_path = get_cursor(
                branch_id,
                legacy_snapshot_id,
            )
            if after_relative_path is not None and callable(set_cursor):
                set_cursor(branch_id, snapshot_id, after_relative_path)
    while True:
        ingested_batch = False
        for batch in snapshot.iter_indexed_documents(
            batch_size=batch_size,
            document_ids=document_ids,
            max_documents=max_documents,
            after_relative_path=after_relative_path,
            load_embeddings=not force_zero_embeddings,
        ):
            operation_id = f"{operation_prefix}:batch-{documents:08d}"
            backend.load_documents(
                branch_id,
                batch,
                operation_id=operation_id,
            )
            documents += len(batch)
            chunks += sum(len(indexed.chunks) for indexed in batch)
            byte_count += sum(
                len(indexed.document.content.encode()) for indexed in batch
            )
            ingested_batch = True
            after_relative_path = str(
                batch[-1].document.metadata["relative_path"]
            )
            if resume and callable(set_cursor):
                set_cursor(branch_id, snapshot_id, after_relative_path)
            if progress is not None:
                progress(IngestionStats(documents, chunks, byte_count))

        if not follow_preparation or document_ids is not None:
            break
        prepared_documents = snapshot.stats().documents
        selected_documents = int(metadata.get("selected_documents", 0))
        target_documents = (
            min(max_documents, selected_documents)
            if max_documents is not None
            else selected_documents
        )
        if prepared_documents >= target_documents:
            break
        if not ingested_batch:
            time.sleep(poll_seconds)
    return IngestionStats(documents, chunks, byte_count)


def _vector_bytes(
    vector: Sequence[float],
    *,
    dtype: str = "float32",
) -> bytes:
    if getattr(vector, "implicit_zero", False):
        return b""
    if isinstance(vector, tuple) and vector.count(0.0) == len(vector):
        return b""
    if dtype == "float16":
        return np.asarray(vector, dtype="<f2").tobytes()
    if dtype == "float32":
        return array.array("f", (float(value) for value in vector)).tobytes()
    raise ValueError(f"unsupported snapshot vector dtype: {dtype}")


def _vector_from_bytes(
    value: bytes,
    dimensions: int,
    *,
    dtype: str = "float32",
) -> tuple[float, ...]:
    if not value:
        return (0.0,) * dimensions
    if dtype == "float16":
        vector = np.frombuffer(value, dtype="<f2").astype(np.float32)
    elif dtype == "float32":
        values = array.array("f")
        values.frombytes(value)
        vector = np.asarray(values, dtype=np.float32)
    else:
        raise RuntimeError(f"unsupported snapshot vector dtype: {dtype}")
    if len(vector) != dimensions:
        raise RuntimeError(
            f"snapshot vector has {len(vector)} dimensions; expected {dimensions}"
        )
    return tuple(float(item) for item in vector)


def _add_stats(left: IngestionStats, right: IngestionStats) -> IngestionStats:
    return IngestionStats(
        documents=left.documents + right.documents,
        chunks=left.chunks + right.chunks,
        bytes=left.bytes + right.bytes,
    )


__all__ = [
    "CorpusSelection",
    "EmbeddingSnapshot",
    "SnapshotBuilder",
    "SnapshotSpec",
    "ingest_snapshot",
    "select_corpus_paths",
]
