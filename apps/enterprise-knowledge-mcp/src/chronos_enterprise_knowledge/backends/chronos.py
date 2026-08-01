"""Chronos implementation of the backend-neutral knowledge contract."""

from __future__ import annotations

import contextlib
import dataclasses
import errno
import functools
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

from chronos_core.branching import ChronosBranchContext, MergeResolution
from chronos_core.workspace import (
    AtomicMergePreview,
    ChronosFSStore,
    ChronosQdrantStore,
    ChronosWorkspaceContext,
    MergeSelection,
    QdrantUpsert,
)
from chronos_core.workspace.chronosfs import (
    shutdown_chronosfs_daemon,
    start_chronosfs_daemon,
    start_chronosfs_mount,
)

from chronos_enterprise_knowledge.models import (
    DocumentChunk,
    IndexedDocument,
    KnowledgeDocument,
    SearchHit,
    canonical_json,
    content_hash,
    knowledge_state_diff,
    knowledge_state_digest_from_projections,
    normalize_workspace_path,
)
from chronos_enterprise_knowledge.retrieval import (
    BM25_VECTOR,
    DENSE_VECTOR,
    Bm25Encoder,
    payload_for_chunk,
    point_vectors,
)

_DOCUMENTS_TABLE = "knowledge_documents"
_CHUNKS_TABLE = "knowledge_chunks"
_BACKEND_STATE_TABLE = "knowledge_backend_state"
_VECTOR_COLLECTION = "knowledge"
_CHUNKS_BY_DOCUMENT_INDEX = "knowledge_chunks_by_document"
_DIRENTS_BY_INODE_INDEX = "chronosfs_dirents_by_inode"
_STORAGE_SCHEMA_VERSION = 3


def _coordinated_write(method: Any) -> Any:
    @functools.wraps(method)
    def wrapped(
        self: ChronosKnowledgeBackend,
        branch_id: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        with self.workspace.branch_write(branch_id):
            return method(self, branch_id, *args, **kwargs)

    return wrapped


class MergeDependencyError(ValueError):
    """A raw selection would publish an inconsistent indexed document."""

    def __init__(
        self,
        missing_change_ids: Sequence[str] = (),
        *,
        stale_index_paths: Sequence[str] = (),
    ):
        self.missing_change_ids = tuple(sorted(set(missing_change_ids)))
        self.stale_index_paths = tuple(sorted(set(stale_index_paths)))
        messages = []
        if self.missing_change_ids:
            messages.append(
                "atomic merge selection is missing dependent changes: "
                + ", ".join(self.missing_change_ids)
            )
        if self.stale_index_paths:
            messages.append(
                "indexed document files changed without updated catalog and "
                "embeddings; reindex before merge: " + ", ".join(self.stale_index_paths)
            )
        super().__init__("; ".join(messages))


class ChronosKnowledgeBackend:
    """SQLite + ChronosFS + Qdrant knowledge workspace."""

    def __init__(
        self,
        state_dir: str | Path,
        *,
        vector_dimensions: int = 1536,
        qdrant_url: str | None = None,
        qdrant_api_key: str | None = None,
        qdrant_storage_dir: str | Path | None = None,
        workspace_metadata_url: str | None = None,
    ):
        self.state_dir = Path(state_dir).expanduser().resolve()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.vector_dimensions = int(vector_dimensions)
        if self.vector_dimensions <= 0:
            raise ValueError("vector_dimensions must be positive")
        namespace = hashlib.sha256(str(self.state_dir).encode()).hexdigest()[:16]
        collection_prefix = (
            f"enterprise_knowledge_{namespace}_"
            if qdrant_url
            else "enterprise_knowledge_"
        )
        self._physical_vector_collection = f"{collection_prefix}{_VECTOR_COLLECTION}"
        self._qdrant_storage_dir = (
            Path(qdrant_storage_dir).expanduser().resolve()
            / "collections"
            / self._physical_vector_collection
            if qdrant_storage_dir is not None
            else self.state_dir / "qdrant"
        )

        metadata_url = workspace_metadata_url or (
            f"sqlite:///{self.state_dir / 'knowledge.sqlite'}"
        )
        self.sqlite = ChronosBranchContext.connect(metadata_url)
        self.filesystem = ChronosFSStore.connect(
            f"sqlite:///{self.state_dir / 'chronosfs.sqlite'}",
            metadata_url=metadata_url,
        )
        self.filesystem.ensure()
        self._ensure_filesystem_indexes()
        if qdrant_url:
            self.qdrant = ChronosQdrantStore.remote(
                metadata_url,
                url=qdrant_url,
                api_key=qdrant_api_key,
                collection_prefix=collection_prefix,
                timeout=600.0,
                context=self.sqlite,
            )
        else:
            self.qdrant = ChronosQdrantStore.local(
                metadata_url,
                path=self.state_dir / "qdrant",
                collection_prefix=collection_prefix,
                context=self.sqlite,
            )
        self.workspace = ChronosWorkspaceContext(
            filesystem=self.filesystem,
            sqlite=self.sqlite,
            qdrant=self.qdrant,
            shared_metadata_url=metadata_url,
        )
        self._active_mounts: dict[str, Path] = {}
        self._ensure_search_schema()
        self._ensure_schema()
        self.sqlite.set_merge_table_scope([_DOCUMENTS_TABLE, _CHUNKS_TABLE])
        self._implicit_zero_vectors = self._state_flag("implicit_zero_vectors")
        self._bm25 = Bm25Encoder()
        self.qdrant.register_collection(
            _VECTOR_COLLECTION,
            self.vector_dimensions,
            distance="cosine",
            dense_vector_name=DENSE_VECTOR,
            sparse_vector_names=(BM25_VECTOR,),
            on_disk=qdrant_url is not None,
        )

    @property
    def backend_name(self) -> str:
        return "chronos"

    @property
    def storage_components(self) -> list[str]:
        return ["sqlite", "chronosfs", "qdrant"]

    def start(self) -> None:
        """Start the shared ChronosFS daemon before accepting MCP requests."""
        start_chronosfs_daemon(self.filesystem)

    def list_branches(self) -> list[str]:
        return self.workspace.list_branches()

    def mount_branch(
        self,
        branch_id: str,
        mount_path: str | Path | None = None,
    ) -> Path:
        if branch_id not in self.list_branches():
            raise ValueError(f"unknown branch: {branch_id}")
        path = (
            Path(mount_path).expanduser()
            if mount_path is not None
            else self.state_dir / "checkouts" / _safe_branch_path(branch_id)
        ).resolve()
        active = self._active_mounts.get(branch_id)
        if active == path and os.path.ismount(path):
            return path
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            filesystem_branch = self.workspace.resolve_branch("filesystem", branch_id)
            start_chronosfs_mount(
                self.filesystem,
                path,
                branch_id=filesystem_branch,
            )
        except OSError as exc:
            if exc.errno != errno.ENOTCONN:
                raise
            self._detach_mount_path(path)
            start_chronosfs_mount(
                self.filesystem,
                path,
                branch_id=filesystem_branch,
            )
        self._active_mounts[branch_id] = path
        return path

    def unmount_branch(self, branch_id: str, *, force: bool = True) -> None:
        path = self._active_mounts.get(branch_id)
        if path is None:
            return
        if not os.path.ismount(path):
            self._active_mounts.pop(branch_id, None)
            return
        errors = []
        for command in (
            ("fusermount3", "-u", str(path)),
            ("fusermount", "-u", str(path)),
            ("umount", str(path)),
        ):
            if shutil.which(command[0]) is None:
                continue
            try:
                subprocess.run(
                    command,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                self._active_mounts.pop(branch_id, None)
                return
            except subprocess.CalledProcessError as exc:
                errors.append(exc)
        if not force:
            error = RuntimeError(
                f"cannot merge while branch workspace is busy: {branch_id}"
            )
            if errors:
                raise error from errors[-1]
            raise error
        if errors:
            self._active_mounts.pop(branch_id, None)
            self._detach_mount_path(path)

    @staticmethod
    def _detach_mount_path(path: Path) -> None:
        for command in (
            ("fusermount3", "-uz", str(path)),
            ("fusermount", "-uz", str(path)),
            ("umount", "-l", str(path)),
        ):
            if shutil.which(command[0]) is None:
                continue
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
            )
            if completed.returncode == 0 or not os.path.ismount(path):
                return
        if os.path.ismount(path):
            raise RuntimeError(f"failed to detach stale ChronosFS mount at {path}")

    def _ensure_schema(self) -> None:
        db = self.sqlite.db
        db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_DOCUMENTS_TABLE} (
                id TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                kind TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            )
            """
        )
        db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_CHUNKS_TABLE} (
                id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                point_id TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            )
            """
        )
        db.commit()
        self.sqlite.register_table(_DOCUMENTS_TABLE, ["id"])
        self.sqlite.register_table(_CHUNKS_TABLE, ["id"])
        index_names = {index.name for index in self.sqlite.list_indexes(_CHUNKS_TABLE)}
        if _CHUNKS_BY_DOCUMENT_INDEX not in index_names:
            self.sqlite.create_index(
                _CHUNKS_TABLE,
                ["document_id"],
                _CHUNKS_BY_DOCUMENT_INDEX,
            )

    def _ensure_filesystem_indexes(self) -> None:
        index_names = {
            index.name
            for index in self.filesystem.context.list_indexes("chronosfs_dirents")
        }
        if _DIRENTS_BY_INODE_INDEX not in index_names:
            self.filesystem.context.create_index(
                "chronosfs_dirents",
                ["inode_id"],
                _DIRENTS_BY_INODE_INDEX,
            )

    def _ensure_search_schema(self) -> None:
        db = self.sqlite.db
        db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_BACKEND_STATE_TABLE} (
                state_key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        version = db.execute(
            f"""
            SELECT value
            FROM {_BACKEND_STATE_TABLE}
            WHERE state_key = 'storage_schema_version'
            """
        ).fetchone()
        if version is None:
            old_tables = db.execute(
                """
                SELECT 1
                FROM sqlite_master
                WHERE type = 'table'
                  AND name IN ('knowledge_documents', 'knowledge_chunks')
                LIMIT 1
                """
            ).fetchone()
            if old_tables is not None:
                raise RuntimeError(
                    "enterprise knowledge state uses the legacy storage layout; "
                    "rebuild it from the prepared snapshot"
                )
            db.execute(
                f"""
                INSERT INTO {_BACKEND_STATE_TABLE}(state_key, value)
                VALUES ('storage_schema_version', ?)
                """,
                (str(_STORAGE_SCHEMA_VERSION),),
            )
        elif int(version["value"]) != _STORAGE_SCHEMA_VERSION:
            raise RuntimeError(
                "enterprise knowledge state schema version does not match; "
                "rebuild it from the prepared snapshot"
            )
        db.commit()

    def _state_flag(self, key: str) -> bool:
        row = self.sqlite.db.execute(
            f"SELECT value FROM {_BACKEND_STATE_TABLE} WHERE state_key = ?",
            (key,),
        ).fetchone()
        return row is not None and str(row["value"]) == "1"

    @contextlib.contextmanager
    def _metadata_transaction(self) -> Iterator[None]:
        """Commit direct backend-state writes through Chronos' SQL adapter."""

        db = self.sqlite.db
        started = not db.in_transaction
        if started:
            db.begin()
        try:
            yield
        except Exception:
            if started:
                db.rollback()
            raise
        else:
            if started:
                db.commit()

    def create_branch(
        self,
        branch_id: str,
        parent_branch: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self.workspace.create_branch(
            branch_id,
            from_branch=parent_branch,
            metadata=dict(metadata or {}),
        )

    def delete_branch(self, branch_id: str) -> None:
        self.unmount_branch(branch_id)
        self.workspace.delete_branch(branch_id)

    def set_placeholder_vector_mode(self, enabled: bool) -> None:
        """Persist whether absent Qdrant points represent staged zero vectors."""

        with self._metadata_transaction():
            self.sqlite.db.execute(
                f"""
                INSERT OR REPLACE INTO {_BACKEND_STATE_TABLE}(state_key, value)
                VALUES ('implicit_zero_vectors', ?)
                """,
                ("1" if enabled else "0",),
            )
        self._implicit_zero_vectors = bool(enabled)

    @staticmethod
    def _snapshot_cursor_key(branch_id: str, snapshot_id: str) -> str:
        digest = hashlib.sha256(f"{branch_id}\0{snapshot_id}".encode()).hexdigest()
        return f"snapshot_ingestion:{digest}"

    def snapshot_ingestion_cursor(
        self,
        branch_id: str,
        snapshot_id: str,
    ) -> str | None:
        key = self._snapshot_cursor_key(branch_id, snapshot_id)
        row = self.sqlite.db.execute(
            f"""
            SELECT value
            FROM {_BACKEND_STATE_TABLE}
            WHERE state_key = ?
            """,
            (key,),
        ).fetchone()
        return None if row is None else str(row["value"])

    def set_snapshot_ingestion_cursor(
        self,
        branch_id: str,
        snapshot_id: str,
        relative_path: str,
    ) -> None:
        key = self._snapshot_cursor_key(branch_id, snapshot_id)
        with self._metadata_transaction():
            self.sqlite.db.execute(
                f"""
                INSERT OR REPLACE INTO {_BACKEND_STATE_TABLE}(state_key, value)
                VALUES (?, ?)
                """,
                (key, relative_path),
            )

    def put_document(
        self,
        branch_id: str,
        indexed: IndexedDocument,
        *,
        operation_id: str,
    ) -> None:
        self.put_documents(
            branch_id,
            [indexed],
            operation_id=operation_id,
        )

    @staticmethod
    def _validate_document_batch(
        indexed_documents: Sequence[IndexedDocument],
    ) -> tuple[list[str], list[str], list[str]]:
        document_ids = [indexed.document.id for indexed in indexed_documents]
        paths = [indexed.document.path for indexed in indexed_documents]
        chunk_ids = [
            chunk.id for indexed in indexed_documents for chunk in indexed.chunks
        ]
        if len(document_ids) != len(set(document_ids)):
            raise ValueError("document batch contains duplicate document ids")
        if len(paths) != len(set(paths)):
            raise ValueError("document batch contains duplicate paths")
        if len(chunk_ids) != len(set(chunk_ids)):
            raise ValueError("document batch contains duplicate chunk ids")
        return document_ids, paths, chunk_ids

    @_coordinated_write
    def load_documents(
        self,
        branch_id: str,
        indexed_documents: Sequence[IndexedDocument],
        *,
        operation_id: str,
    ) -> None:
        """Bulk-load documents into a new benchmark state."""

        if not indexed_documents:
            return
        started = time.monotonic()
        self._validate_document_batch(indexed_documents)
        branch = self.workspace.checkout(branch_id)
        chunks = [chunk for indexed in indexed_documents for chunk in indexed.chunks]
        sparse_vectors = self._bm25.documents([chunk.text for chunk in chunks])
        after_bm25 = time.monotonic()
        written_paths = [indexed.document.path for indexed in indexed_documents]
        try:
            branch.fs.write_files(
                [
                    (
                        indexed.document.path,
                        indexed.document.content,
                    )
                    for indexed in indexed_documents
                ],
                parents=True,
            )
            after_filesystem = time.monotonic()
            with branch.transaction():
                branch.sqlite.upsert_rows(
                    _DOCUMENTS_TABLE,
                    [
                        self._document_row(indexed.document)
                        for indexed in indexed_documents
                    ],
                )
                after_documents = time.monotonic()
                if chunks:
                    branch.sqlite.upsert_rows(
                        _CHUNKS_TABLE,
                        [self._chunk_row(chunk) for chunk in chunks],
                    )
                    after_chunks = time.monotonic()
                    branch.qdrant.load_many(
                        _VECTOR_COLLECTION,
                        [
                            QdrantUpsert(
                                id=chunk.id,
                                vector=point_vectors(
                                    (
                                        ()
                                        if self._implicit_zero_vectors
                                        else chunk.embedding
                                    ),
                                    sparse,
                                ),
                                payload=payload_for_chunk(
                                    document_id=chunk.document_id,
                                    chunk_id=chunk.id,
                                    ordinal=chunk.ordinal,
                                    text=chunk.text,
                                    content_hash=chunk.sha256,
                                    metadata=chunk.metadata,
                                ),
                                revision=(f"{operation_id}:{chunk.id}:{chunk.sha256}"),
                            )
                            for chunk, sparse in zip(
                                chunks,
                                sparse_vectors,
                                strict=True,
                            )
                        ],
                    )
                    after_qdrant = time.monotonic()
                else:
                    after_chunks = after_documents
                    after_qdrant = after_documents
            completed = time.monotonic()
            if os.environ.get("CHRONOS_INGEST_PROFILE") == "1":
                print(
                    json.dumps(
                        {
                            "event": "chronos_ingest_batch",
                            "documents": len(indexed_documents),
                            "chunks": len(chunks),
                            "seconds": {
                                "bm25": round(after_bm25 - started, 3),
                                "filesystem": round(
                                    after_filesystem - after_bm25,
                                    3,
                                ),
                                "document_metadata": round(
                                    after_documents - after_filesystem,
                                    3,
                                ),
                                "chunk_metadata": round(
                                    after_chunks - after_documents,
                                    3,
                                ),
                                "qdrant": round(
                                    after_qdrant - after_chunks,
                                    3,
                                ),
                                "commit": round(
                                    completed - after_qdrant,
                                    3,
                                ),
                                "total": round(completed - started, 3),
                            },
                        }
                    ),
                    file=sys.stderr,
                    flush=True,
                )
        except Exception:
            for path in written_paths:
                if branch.fs.exists(path):
                    branch.fs.unlink(path)
            raise

    @_coordinated_write
    def put_documents(
        self,
        branch_id: str,
        indexed_documents: Sequence[IndexedDocument],
        *,
        operation_id: str,
    ) -> None:
        if not indexed_documents:
            return
        document_ids, _, _ = self._validate_document_batch(indexed_documents)

        branch = self.workspace.checkout(branch_id)
        existing_rows = self._rows_for_ids(
            branch.sqlite,
            _DOCUMENTS_TABLE,
            ("id", "path"),
            document_ids,
        )
        existing_paths = {str(row["id"]): str(row["path"]) for row in existing_rows}
        old_chunk_rows = self._rows_for_ids(
            branch.sqlite,
            _CHUNKS_TABLE,
            ("id", "document_id"),
            document_ids,
            key_column="document_id",
        )
        old_chunks_by_document: dict[str, set[str]] = {}
        for row in old_chunk_rows:
            old_chunks_by_document.setdefault(
                str(row["document_id"]),
                set(),
            ).add(str(row["id"]))

        backups: dict[str, bytes] = {}
        for previous_path in set(existing_paths.values()):
            if branch.fs.exists(previous_path):
                backups[previous_path] = branch.fs.read_file(previous_path)
        written_paths: set[str] = set()
        for indexed in indexed_documents:
            path = indexed.document.path
            branch.fs.write_file(
                path,
                indexed.document.content,
                parents=True,
            )
            written_paths.add(path)
            previous_path = existing_paths.get(indexed.document.id)
            if (
                previous_path
                and previous_path != path
                and branch.fs.exists(previous_path)
            ):
                branch.fs.unlink(previous_path)

        removed_chunk_ids: set[str] = set()
        for indexed in indexed_documents:
            removed_chunk_ids.update(
                old_chunks_by_document.get(indexed.document.id, set())
                - {chunk.id for chunk in indexed.chunks}
            )
        chunks = [chunk for indexed in indexed_documents for chunk in indexed.chunks]
        sparse_vectors = self._bm25.documents([chunk.text for chunk in chunks])
        try:
            with branch.transaction():
                branch.sqlite.upsert_rows(
                    _DOCUMENTS_TABLE,
                    [
                        self._document_row(indexed.document)
                        for indexed in indexed_documents
                    ],
                )
                if removed_chunk_ids:
                    branch.sqlite.delete_keys(
                        _CHUNKS_TABLE,
                        [{"id": chunk_id} for chunk_id in sorted(removed_chunk_ids)],
                    )
                    branch.qdrant.delete_many(
                        _VECTOR_COLLECTION,
                        sorted(removed_chunk_ids),
                    )
                if chunks:
                    branch.sqlite.upsert_rows(
                        _CHUNKS_TABLE,
                        [self._chunk_row(chunk) for chunk in chunks],
                    )
                    branch.qdrant.upsert_many(
                        _VECTOR_COLLECTION,
                        [
                            QdrantUpsert(
                                id=chunk.id,
                                vector=point_vectors(
                                    (
                                        ()
                                        if self._implicit_zero_vectors
                                        else chunk.embedding
                                    ),
                                    sparse,
                                ),
                                payload=payload_for_chunk(
                                    document_id=chunk.document_id,
                                    chunk_id=chunk.id,
                                    ordinal=chunk.ordinal,
                                    text=chunk.text,
                                    content_hash=chunk.sha256,
                                    metadata=chunk.metadata,
                                ),
                                revision=(f"{operation_id}:{chunk.id}:{chunk.sha256}"),
                            )
                            for chunk, sparse in zip(
                                chunks,
                                sparse_vectors,
                                strict=True,
                            )
                        ],
                    )
        except Exception:
            for path in written_paths:
                if branch.fs.exists(path):
                    branch.fs.unlink(path)
            for path, content in backups.items():
                branch.fs.write_file(path, content, parents=True)
            raise

    @staticmethod
    def _rows_for_ids(
        sqlite_session: Any,
        table: str,
        columns: Sequence[str],
        values: Sequence[str],
        *,
        key_column: str = "id",
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for start in range(0, len(values), 400):
            batch = list(values[start : start + 400])
            params = {f"value_{index}": value for index, value in enumerate(batch)}
            placeholders = ",".join(f":value_{index}" for index in range(len(batch)))
            result.extend(
                sqlite_session.query(
                    f"""
                    SELECT {", ".join(columns)}
                    FROM {table}
                    WHERE {key_column} IN ({placeholders})
                    """,
                    params,
                )
            )
        return result

    @staticmethod
    def _document_row(document: KnowledgeDocument) -> dict[str, Any]:
        return {
            "id": document.id,
            "path": document.path,
            "title": document.title,
            "source": document.source,
            "kind": document.kind,
            "content_hash": document.sha256,
            "metadata_json": canonical_json(document.metadata),
        }

    @staticmethod
    def _chunk_row(chunk: DocumentChunk) -> dict[str, Any]:
        return {
            "id": chunk.id,
            "document_id": chunk.document_id,
            "ordinal": chunk.ordinal,
            "content_hash": chunk.sha256,
            "point_id": chunk.id,
            "metadata_json": canonical_json(chunk.metadata),
        }

    @_coordinated_write
    def delete_document(
        self,
        branch_id: str,
        document_id: str,
        *,
        operation_id: str,
    ) -> bool:
        del operation_id
        existing = self.get_document(branch_id, document_id)
        if existing is None:
            return False
        branch = self.workspace.checkout(branch_id)
        if branch.fs.exists(existing.document.path):
            branch.fs.unlink(existing.document.path)
        chunk_ids = [chunk.id for chunk in existing.chunks]
        try:
            with branch.transaction():
                if chunk_ids:
                    branch.sqlite.delete_keys(
                        _CHUNKS_TABLE,
                        [{"id": chunk_id} for chunk_id in chunk_ids],
                    )
                    for chunk_id in chunk_ids:
                        branch.qdrant.delete(_VECTOR_COLLECTION, chunk_id)
                branch.sqlite.delete_keys(
                    _DOCUMENTS_TABLE,
                    [{"id": document_id}],
                )
        except Exception:
            branch.fs.write_file(
                existing.document.path,
                existing.document.content,
                parents=True,
            )
            raise
        return True

    def get_document(
        self,
        branch_id: str,
        document_id: str,
    ) -> IndexedDocument | None:
        branch = self.workspace.checkout(branch_id)
        rows = branch.sqlite.query(
            f"""
            SELECT id, path, title, source, kind, content_hash, metadata_json
            FROM {_DOCUMENTS_TABLE}
            WHERE id = :id
            """,
            {"id": document_id},
        )
        if not rows:
            return None
        row = rows[0]
        path = str(row["path"])
        if not branch.fs.exists(path):
            raise RuntimeError(
                f"document metadata exists but ChronosFS path is missing: {path}"
            )
        content = branch.fs.read_text(path)
        if hashlib.sha256(content.encode()).hexdigest() != row["content_hash"]:
            raise RuntimeError(f"document content hash mismatch: {document_id}")
        document = KnowledgeDocument(
            id=str(row["id"]),
            path=path,
            title=str(row["title"]),
            source=str(row["source"]),
            content=content,
            kind=str(row["kind"]),  # type: ignore[arg-type]
            metadata=_decode_json_object(row["metadata_json"]),
        )
        chunk_rows = branch.sqlite.query(
            f"""
            SELECT id, document_id, ordinal, content_hash,
                   point_id, metadata_json
            FROM {_CHUNKS_TABLE}
            WHERE document_id = :document_id
            ORDER BY ordinal, id
            """,
            {"document_id": document_id},
        )
        points = branch.qdrant.get_many(
            _VECTOR_COLLECTION,
            [str(row["point_id"]) for row in chunk_rows],
        )
        chunks: list[DocumentChunk] = []
        for chunk_row in chunk_rows:
            point_id = str(chunk_row["point_id"])
            point = points.get(point_id)
            if point is None:
                raise RuntimeError(
                    f"chunk catalog exists but Qdrant point is missing: {point_id}"
                )
            payload = point.payload
            text = str(payload.get("text", ""))
            if content_hash(text) != str(chunk_row["content_hash"]):
                raise RuntimeError(f"chunk content hash mismatch: {point_id}")
            vectors = point.vector if isinstance(point.vector, Mapping) else {}
            dense = vectors.get(DENSE_VECTOR)
            embedding = (
                tuple(float(value) for value in dense)
                if isinstance(dense, Sequence)
                else (0.0,) * self.vector_dimensions
            )
            chunks.append(
                DocumentChunk(
                    id=str(chunk_row["id"]),
                    document_id=str(chunk_row["document_id"]),
                    ordinal=int(chunk_row["ordinal"]),
                    text=text,
                    embedding=embedding,
                    metadata=dict(
                        payload.get("chunk_metadata")
                        or _decode_json_object(chunk_row["metadata_json"])
                    ),
                )
            )
        return IndexedDocument(document, tuple(chunks))

    def search(
        self,
        branch_id: str,
        query_text: str,
        query_embedding: Sequence[float],
        *,
        limit: int,
    ) -> list[SearchHit]:
        if limit <= 0:
            return []
        branch = self.workspace.checkout(branch_id)
        results = branch.qdrant.hybrid_search(
            _VECTOR_COLLECTION,
            dense_query=(
                None
                if self._implicit_zero_vectors or _is_zero_vector(query_embedding)
                else query_embedding
            ),
            sparse_query=self._bm25.query(query_text),
            sparse_vector_name=BM25_VECTOR,
            limit=max(limit * 4, 32),
        )
        ordered_ids = [result.id for result in results]
        metadata_by_chunk = self._search_metadata(branch.sqlite, ordered_ids)
        hits: list[SearchHit] = []
        per_document: dict[str, int] = {}
        result_by_id = {result.id: result for result in results}
        for chunk_id in ordered_ids:
            row = metadata_by_chunk.get(chunk_id)
            result = result_by_id[chunk_id]
            if row is None:
                continue
            payload = result.payload
            if str(row["content_hash"]) != str(payload.get("content_hash", "")):
                continue
            document_id = str(row["document_id"])
            if per_document.get(document_id, 0) >= 2:
                continue
            per_document[document_id] = per_document.get(document_id, 0) + 1
            document_metadata = _decode_json_object(row["document_metadata"])
            chunk_metadata = dict(
                payload.get("chunk_metadata")
                or _decode_json_object(row["chunk_metadata"])
            )
            hits.append(
                SearchHit(
                    document_id=document_id,
                    chunk_id=str(row["chunk_id"]),
                    path=str(row["path"]),
                    title=str(row["title"]),
                    text=str(payload.get("text", "")),
                    score=float(result.score),
                    source=str(row["source"]),
                    metadata={
                        **document_metadata,
                        **chunk_metadata,
                        "document_kind": str(row["kind"]),
                    },
                )
            )
            if len(hits) >= limit:
                break
        return hits

    def _search_metadata(
        self,
        sqlite_session: Any,
        chunk_ids: Sequence[str],
    ) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for start in range(0, len(chunk_ids), 300):
            batch = list(chunk_ids[start : start + 300])
            params = {
                f"chunk_{index}": chunk_id for index, chunk_id in enumerate(batch)
            }
            placeholders = ", ".join(f":chunk_{index}" for index in range(len(batch)))
            rows = sqlite_session.query(
                f"""
                SELECT c.id AS chunk_id, c.document_id, c.content_hash,
                       c.point_id, c.metadata_json AS chunk_metadata,
                       d.path, d.title, d.source, d.kind,
                       d.metadata_json AS document_metadata
                FROM {_CHUNKS_TABLE} AS c
                JOIN {_DOCUMENTS_TABLE} AS d ON d.id = c.document_id
                WHERE c.id IN ({placeholders})
                """,
                params,
            )
            result.update({str(row["chunk_id"]): row for row in rows})
        return result

    @_coordinated_write
    def write_file(
        self,
        branch_id: str,
        path: str,
        content: bytes,
        *,
        operation_id: str,
    ) -> None:
        del operation_id
        normalized = normalize_workspace_path(path)
        self.workspace.checkout(branch_id).fs.write_file(
            normalized,
            content,
            parents=True,
        )

    @_coordinated_write
    def delete_file(
        self,
        branch_id: str,
        path: str,
        *,
        operation_id: str,
    ) -> bool:
        del operation_id
        normalized = normalize_workspace_path(path)
        fs = self.workspace.checkout(branch_id).fs
        if not fs.exists(normalized):
            return False
        fs.unlink(normalized)
        return True

    def read_file(self, branch_id: str, path: str) -> bytes:
        # Mounted checkouts share one ChronosFS daemon process. Drop
        # negative/path cache entries before reading files that an agent may
        # have created through normal POSIX tools.
        self.filesystem.refresh_branch(
            self.workspace.resolve_branch("filesystem", branch_id)
        )
        normalized = normalize_workspace_path(path)
        fs = self.workspace.checkout(branch_id).fs
        if not fs.exists(normalized):
            raise FileNotFoundError(normalized)
        try:
            return fs.read_file(normalized)
        except RuntimeError as error:
            if str(error) != "path not found":
                raise
            raise FileNotFoundError(normalized) from error

    def diff(self, source_branch: str, target_branch: str) -> dict[str, Any]:
        source_documents, target_documents = self._changed_document_digests(
            source_branch,
            target_branch,
        )
        source_files, target_files = self._changed_file_digests(
            source_branch,
            target_branch,
        )
        return knowledge_state_diff(
            source_documents,
            target_documents,
            source_files,
            target_files,
        )

    def _changed_document_digests(
        self,
        source_branch: str,
        target_branch: str,
    ) -> tuple[dict[str, str], dict[str, str]]:
        """Project only document rows changed between two branches.

        Chronos's native interval diff is proportional to branch-local
        changes.  Hashing the complete visible document and chunk snapshots
        here would throw that property away for a sparse branch.
        """

        source: dict[str, str] = {}
        target: dict[str, str] = {}
        for diff in self.sqlite.diff_rows(
            self.workspace.resolve_branch("sqlite", target_branch),
            self.workspace.resolve_branch("sqlite", source_branch),
            _DOCUMENTS_TABLE,
        ):
            document_id = str(diff.key["id"])
            if diff.after is not None:
                source[document_id] = content_hash(canonical_json(diff.after))
            if diff.before is not None:
                target[document_id] = content_hash(canonical_json(diff.before))
        return source, target

    def _changed_file_digests(
        self,
        source_branch: str,
        target_branch: str,
    ) -> tuple[dict[str, str], dict[str, str]]:
        """Hash only files whose ChronosFS rows differ between branches."""

        changed_inodes: set[int] = set()
        context = self.filesystem.context
        source_fs_branch = self.workspace.resolve_branch("filesystem", source_branch)
        target_fs_branch = self.workspace.resolve_branch("filesystem", target_branch)
        # ChronosFS updates the inode whenever file content changes. Directory
        # entries cover path additions, removals, and renames. Inspecting block
        # rows as well is redundant and can materialize hundreds of thousands
        # of rows after a build or test run writes many sandbox artifacts.
        for table in ("chronosfs_dirents", "chronosfs_inodes"):
            for diff in context.diff_rows(target_fs_branch, source_fs_branch, table):
                for row in (diff.key, diff.before, diff.after):
                    if row is None:
                        continue
                    inode_id = row.get("inode_id")
                    if inode_id is not None:
                        changed_inodes.add(int(inode_id))

        changed_paths: set[str] = set()
        for inode_id in changed_inodes:
            for branch_id in (source_branch, target_branch):
                path = self._path_for_inode(branch_id, inode_id)
                if path is not None:
                    changed_paths.add(path)

        return (
            self._file_digests_for_paths(source_branch, changed_paths),
            self._file_digests_for_paths(target_branch, changed_paths),
        )

    def _path_for_inode(self, branch_id: str, inode_id: int) -> str | None:
        if inode_id == 1:
            return "/"
        session = self.filesystem.context.checkout(
            self.workspace.resolve_branch("filesystem", branch_id)
        )
        parts: list[str] = []
        current = inode_id
        seen: set[int] = set()
        while current != 1:
            if current in seen:
                raise RuntimeError(
                    f"cycle in ChronosFS directory entries at inode {current}"
                )
            seen.add(current)
            rows = session.query(
                """
                SELECT parent_inode_id, name
                FROM chronosfs_dirents
                WHERE inode_id = :inode_id
                ORDER BY parent_inode_id, name
                LIMIT 1
                """,
                {"inode_id": current},
            )
            if not rows:
                return None
            row = rows[0]
            parts.append(str(row["name"]))
            current = int(row["parent_inode_id"])
        return "/" + "/".join(reversed(parts))

    def _file_digests_for_paths(
        self,
        branch_id: str,
        paths: set[str],
    ) -> dict[str, str]:
        self.filesystem.refresh_branch(
            self.workspace.resolve_branch("filesystem", branch_id)
        )
        branch = self.workspace.checkout(branch_id)
        result: dict[str, str] = {}
        for path in sorted(paths):
            if not branch.fs.exists(path):
                continue
            stat = branch.fs.stat(path)
            if stat.kind == "file":
                result[path] = content_hash(branch.fs.read_file(path))
        return result

    def merge(
        self,
        source_branch: str,
        target_branch: str,
        *,
        operation_id: str,
        selected_change_ids: Sequence[str] | None = None,
        preview_token: str | None = None,
        policy: Any = None,
        conflict_choices: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        # External POSIX processes cannot be paused by the caller. A strict
        # unmount makes their reviewed filesystem state quiescent before the
        # native Chronos branch transaction reserves source and target.
        self.unmount_branch(source_branch, force=False)
        self.unmount_branch(target_branch, force=False)
        effective_policy = (
            "manual_review"
            if policy is None and conflict_choices is not None
            else policy
        )
        preview = self.workspace.merge_atomic_preview(
            source_branch,
            target_branch,
            policy=effective_policy,
        )
        if preview_token is None or preview_token == preview.preview_token:
            selected_for_validation = (
                preview.change_ids
                if selected_change_ids is None
                else frozenset(str(value) for value in selected_change_ids)
            )
            self._validate_merge_dependencies(
                preview,
                selected_for_validation,
            )
        result = self.workspace.merge_atomic(
            source_branch,
            target_branch,
            selection=(
                None
                if selected_change_ids is None
                else MergeSelection.from_ids(selected_change_ids)
            ),
            preview_token=preview_token,
            policy=effective_policy,
            resolution=(
                MergeResolution(dict(conflict_choices))
                if conflict_choices is not None
                else None
            ),
            operation_id=operation_id,
        )
        payload = _jsonable_diff(result)
        return {
            **{
                name: {"applied": count}
                for name, count in payload.get("stores", {}).items()
            },
            **payload,
        }

    def merge_preview(
        self,
        source_branch: str,
        target_branch: str,
        *,
        policy: Any = None,
    ) -> dict[str, Any]:
        preview = self.workspace.merge_atomic_preview(
            source_branch,
            target_branch,
            policy=policy,
        )
        bundles, paths, non_filesystem, content_changes, filesystem_groups = (
            self._merge_dependency_groups(preview)
        )
        payload = _jsonable_diff(preview)
        payload["selection_groups"] = {
            "indexed_documents": {
                document_id: sorted(change_ids)
                for document_id, change_ids in sorted(bundles.items())
            },
            "filesystem_paths": {
                path: sorted(change_ids)
                for path, change_ids in sorted(filesystem_groups.items())
            },
        }
        payload["stale_index_paths"] = sorted(
            path
            for path in content_changes
            if any(
                not non_filesystem.get(document_id)
                for document_id in paths.get(path, ())
            )
        )
        return payload

    def _validate_merge_dependencies(
        self,
        preview: AtomicMergePreview,
        selected: frozenset[str],
    ) -> None:
        bundles, paths, non_filesystem_changes, content_changes, _ = (
            self._merge_dependency_groups(preview)
        )
        missing: set[str] = set()
        for bundle in bundles.values():
            chosen = bundle & selected
            if chosen and chosen != bundle:
                missing.update(bundle - selected)
        if missing:
            raise MergeDependencyError(sorted(missing))
        stale_paths = {
            path
            for path, change_ids in content_changes.items()
            if change_ids & selected
            and any(
                not non_filesystem_changes.get(document_id)
                for document_id in paths.get(path, ())
            )
        }
        if stale_paths:
            raise MergeDependencyError(stale_index_paths=sorted(stale_paths))

    def _merge_dependency_groups(
        self,
        preview: AtomicMergePreview,
    ) -> tuple[
        dict[str, set[str]],
        dict[str, set[str]],
        dict[str, set[str]],
        dict[str, set[str]],
        dict[str, set[str]],
    ]:
        bundles: dict[str, set[str]] = {}
        paths: dict[str, set[str]] = {}
        point_documents: dict[str, set[str]] = {}
        non_filesystem_changes: dict[str, set[str]] = {}
        content_changes: dict[str, set[str]] = {}
        filesystem_groups: dict[str, set[str]] = {}

        for branch_id in (preview.source, preview.target):
            rows = self.workspace.checkout(branch_id).sqlite.query(
                f"SELECT id, path FROM {_DOCUMENTS_TABLE}"
            )
            for row in rows:
                path = str(row["path"])
                document_id = str(row["id"])
                if path and document_id:
                    paths.setdefault(path, set()).add(document_id)

        def add(
            document_id: str,
            change_id: str | None,
            *,
            filesystem: bool = False,
        ) -> None:
            if document_id and change_id:
                bundles.setdefault(document_id, set()).add(change_id)
                if not filesystem:
                    non_filesystem_changes.setdefault(document_id, set()).add(change_id)

        sqlite_preview = preview.stores.get("sqlite")
        if sqlite_preview is not None:
            for change in (*sqlite_preview.changes, *sqlite_preview.conflicts):
                rows = [row for row in (change.before, change.after) if row is not None]
                if change.table == _DOCUMENTS_TABLE:
                    document_id = str(change.key.get("id", ""))
                    add(document_id, change.change_id)
                    for row in rows:
                        path = str(row.get("path", ""))
                        if path:
                            paths.setdefault(path, set()).add(document_id)
                elif change.table == _CHUNKS_TABLE:
                    document_ids = {str(row.get("document_id", "")) for row in rows}
                    for document_id in document_ids:
                        add(document_id, change.change_id)
                    for row in rows:
                        point_id = str(row.get("point_id") or row.get("id") or "")
                        if point_id:
                            point_documents.setdefault(point_id, set()).update(
                                document_ids
                            )

        qdrant_preview = preview.stores.get("qdrant")
        if qdrant_preview is not None:
            for change in (*qdrant_preview.changes, *qdrant_preview.conflicts):
                point_id = str(change.key.get("id", ""))
                document_ids = set(point_documents.get(point_id, ()))
                for row in (change.before, change.after):
                    if row is None:
                        continue
                    payload = row.get("payload") or {}
                    document_id = str(payload.get("document_id", ""))
                    if document_id:
                        document_ids.add(document_id)
                for document_id in document_ids:
                    add(document_id, change.change_id)

        filesystem_preview = preview.stores.get("filesystem")
        if filesystem_preview is not None:
            for change in (*filesystem_preview.changes, *filesystem_preview.conflicts):
                path = str(change.key.get("path", ""))
                if path and change.change_id:
                    filesystem_groups.setdefault(path, set()).add(change.change_id)
                    if change.table in {
                        "chronosfs_file_range",
                        "chronosfs_dirent",
                    }:
                        content_changes.setdefault(path, set()).add(change.change_id)
                for document_id in paths.get(path, ()):
                    add(document_id, change.change_id, filesystem=True)
        return (
            bundles,
            paths,
            non_filesystem_changes,
            content_changes,
            filesystem_groups,
        )

    def state_digest(self, branch_id: str) -> str:
        self.filesystem.refresh_branch(
            self.workspace.resolve_branch("filesystem", branch_id)
        )
        branch = self.workspace.checkout(branch_id)
        document_rows = branch.sqlite.query(
            f"""
            SELECT id, path, title, source, kind, content_hash, metadata_json
            FROM {_DOCUMENTS_TABLE}
            ORDER BY id
            """
        )
        chunk_rows = branch.sqlite.query(
            f"""
            SELECT id, document_id, ordinal, content_hash,
                   point_id, metadata_json
            FROM {_CHUNKS_TABLE}
            ORDER BY document_id, ordinal, id
            """
        )
        points = branch.qdrant.get_many(
            _VECTOR_COLLECTION,
            [str(row["point_id"]) for row in chunk_rows],
        )
        chunks_by_document: dict[str, list[dict[str, Any]]] = {}
        for row in chunk_rows:
            point = points.get(str(row["point_id"]))
            if point is None:
                raise RuntimeError(
                    f"Qdrant point missing during state digest: {row['point_id']}"
                )
            text = str(point.payload.get("text", ""))
            chunks_by_document.setdefault(
                str(row["document_id"]),
                [],
            ).append(
                {
                    "id": str(row["id"]),
                    "document_id": str(row["document_id"]),
                    "ordinal": int(row["ordinal"]),
                    "text": text,
                    "embedding_dimensions": self.vector_dimensions,
                    "metadata": dict(
                        point.payload.get("chunk_metadata")
                        or _decode_json_object(row["metadata_json"])
                    ),
                    "sha256": str(row["content_hash"]),
                }
            )
        documents: list[dict[str, Any]] = []
        for row in document_rows:
            path = str(row["path"])
            content = branch.fs.read_text(path)
            documents.append(
                {
                    "document": {
                        "id": str(row["id"]),
                        "path": path,
                        "title": str(row["title"]),
                        "source": str(row["source"]),
                        "content": content,
                        "kind": str(row["kind"]),
                        "metadata": _decode_json_object(row["metadata_json"]),
                        "sha256": content_hash(content),
                    },
                    "chunks": chunks_by_document.get(str(row["id"]), []),
                }
            )
        files: list[tuple[str, bytes]] = []
        for path in self._walk_files(branch.fs, "/"):
            content = branch.fs.read_file(path)
            files.append((path, content))
        return knowledge_state_digest_from_projections(documents, files)

    def storage_stats(self) -> dict[str, int]:
        # Branch deletion remains asynchronous on the operation path. Storage
        # measurements wait for reclamation so they report retained state
        # rather than transient garbage.
        self.filesystem.wait_for_gc()
        database_paths = [
            self.state_dir / "knowledge.sqlite",
            self.state_dir / "chronosfs.sqlite",
            self.state_dir / "qdrant-metadata.sqlite",
        ]
        sqlite_bytes = sum(_sqlite_family_bytes(path) for path in database_paths)
        qdrant_bytes = _directory_bytes(self._qdrant_storage_dir)
        main = self.workspace.checkout("main")
        document_rows = main.sqlite.query(
            f"SELECT COUNT(*) AS count FROM {_DOCUMENTS_TABLE}"
        )
        chunk_rows = main.sqlite.query(f"SELECT COUNT(*) AS count FROM {_CHUNKS_TABLE}")
        return {
            "branches": len(self.list_branches()),
            "main_documents": int(document_rows[0]["count"]),
            "main_chunks": int(chunk_rows[0]["count"]),
            "sqlite_bytes": sqlite_bytes,
            "qdrant_bytes": qdrant_bytes,
            "total_state_bytes": sqlite_bytes + qdrant_bytes,
        }

    def destroy(self) -> None:
        if self.qdrant.client.collection_exists(self._physical_vector_collection):
            self.qdrant.client.delete_collection(self._physical_vector_collection)

    def _walk_files(self, fs: Any, path: str) -> list[str]:
        result: list[str] = []
        for name in fs.listdir(path):
            child = f"/{name}" if path == "/" else f"{path.rstrip('/')}/{name}"
            stat = fs.stat(child)
            if stat.kind == "directory":
                result.extend(self._walk_files(fs, child))
            elif stat.kind == "file":
                result.append(child)
        return sorted(result)

    def close(self) -> None:
        for branch_id in list(self._active_mounts):
            with contextlib.suppress(Exception):
                self.unmount_branch(branch_id)
        with contextlib.suppress(Exception):
            shutdown_chronosfs_daemon(self.filesystem)
        self.workspace.close()


def _jsonable_diff(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return {
            key: _jsonable_diff(item) for key, item in dataclasses.asdict(value).items()
        }
    if isinstance(value, Mapping):
        return {str(key): _jsonable_diff(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable_diff(item) for item in value]
    return value


def _decode_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    parsed = json.loads(str(value))
    if not isinstance(parsed, Mapping):
        raise RuntimeError(  # noqa: TRY004 - corrupted persisted state
            "stored metadata JSON is not an object"
        )
    return dict(parsed)


def _safe_branch_path(branch_id: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", branch_id).strip("-")
    if not safe:
        raise ValueError("branch_id must contain a path-safe character")
    return safe


def _is_zero_vector(vector: Sequence[float]) -> bool:
    if getattr(vector, "implicit_zero", False):
        return True
    if isinstance(vector, tuple):
        return vector.count(0.0) == len(vector)
    return not any(float(value) != 0.0 for value in vector)


def _sqlite_family_bytes(path: Path) -> int:
    return sum(
        candidate.stat().st_size
        for candidate in (
            path,
            Path(f"{path}-wal"),
            Path(f"{path}-shm"),
        )
        if candidate.exists()
    )


def _directory_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(
        item.stat().st_blocks * 512 for item in path.rglob("*") if item.is_file()
    )


__all__ = ["ChronosKnowledgeBackend"]
