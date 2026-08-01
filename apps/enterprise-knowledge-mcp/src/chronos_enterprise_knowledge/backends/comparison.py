"""Experiment backends that expose the same logical knowledge contract.

These backends intentionally keep branch management in application code.  The
overlay backend stores only branch-local changes and resolves ancestry on every
read.  The physical-clone backend materializes all visible state when a branch
is created.  Both use SQLite for relational metadata, ordinary files for
document contents, and Qdrant for chunk text and search indexes so a recorded
MCP workload can be replayed without changing its logical operations.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import threading
import uuid
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from qdrant_client import QdrantClient, models

from chronos_enterprise_knowledge.models import (
    DocumentChunk,
    IndexedDocument,
    KnowledgeDocument,
    SearchHit,
    canonical_json,
    content_hash,
    indexed_document_digest,
    knowledge_state_diff,
    knowledge_state_digest,
    normalize_workspace_path,
)
from chronos_enterprise_knowledge.retrieval import (
    BM25_VECTOR,
    DENSE_VECTOR,
    Bm25Encoder,
    bulk_upsert_points,
    hybrid_query,
    named_vector_config,
    payload_for_chunk,
    point_vectors,
    qdrant_collection_options,
    remote_qdrant_client,
)

_MAX_REVISION = (1 << 63) - 1
_SCHEMA_VERSION = "3"
_CheckoutFingerprint = tuple[str, int, int, int]


def _positive_environment_integer(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


class _ApplicationStateBackend:
    """Shared implementation for overlay and fully materialized branches."""

    _copy_on_branch = False
    _backend_name = "app-managed"

    def __init__(
        self,
        state_dir: str | Path,
        *,
        vector_dimensions: int = 1536,
        qdrant_url: str | None = None,
        qdrant_api_key: str | None = None,
        qdrant_storage_dir: str | Path | None = None,
    ):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.vector_dimensions = int(vector_dimensions)
        if self.vector_dimensions <= 0:
            raise ValueError("vector_dimensions must be positive")
        self._lock = threading.RLock()
        self._db = sqlite3.connect(
            self.state_dir / "application-state.sqlite",
            check_same_thread=False,
        )
        self._db.row_factory = sqlite3.Row
        cache_size_kib = _positive_environment_integer(
            "CHRONOS_APP_SQLITE_CACHE_SIZE_KIB"
        )
        if cache_size_kib is not None:
            # Negative cache_size values use KiB. SQLite treats this as a soft
            # page-cache ceiling for the connection.
            self._db.execute(f"PRAGMA cache_size=-{cache_size_kib}")
        self._ensure_schema()
        self._file_store = self.state_dir / "files"
        self._file_store.mkdir(parents=True, exist_ok=True)
        self._bm25 = Bm25Encoder()
        placeholder = self._db.execute(
            """
            SELECT value
            FROM backend_state
            WHERE state_key = 'implicit_zero_vectors'
            """
        ).fetchone()
        self._implicit_zero_vectors = bool(
            placeholder is not None and str(placeholder["value"]) == "1"
        )

        remote_qdrant = qdrant_url is not None
        if qdrant_url:
            self._qdrant = remote_qdrant_client(
                qdrant_url,
                api_key=qdrant_api_key,
            )
        else:
            self._qdrant = QdrantClient(path=str(self.state_dir / "qdrant"))
        namespace = hashlib.sha256(str(self.state_dir.resolve()).encode()).hexdigest()[
            :16
        ]
        self._collection = (
            f"enterprise_{self._backend_name.replace('-', '_')}_v2_{namespace}"
        )
        self._qdrant_storage_dir = (
            Path(qdrant_storage_dir).expanduser().resolve()
            / "collections"
            / self._collection
            if qdrant_storage_dir is not None
            else self.state_dir / "qdrant"
        )
        if not self._qdrant.collection_exists(self._collection):
            dense, sparse = named_vector_config(
                self.vector_dimensions,
                on_disk=remote_qdrant,
            )
            self._qdrant.create_collection(
                collection_name=self._collection,
                vectors_config=dense,
                sparse_vectors_config=sparse,
                **qdrant_collection_options(on_disk=remote_qdrant),
            )
            if remote_qdrant:
                self._qdrant.create_payload_index(
                    collection_name=self._collection,
                    field_name="branch_id",
                    field_schema=models.PayloadSchemaType.KEYWORD,
                    wait=True,
                )
                self._qdrant.create_payload_index(
                    collection_name=self._collection,
                    field_name="overwritten_in[].by",
                    field_schema=models.PayloadSchemaType.KEYWORD,
                    wait=True,
                )
                self._qdrant.create_payload_index(
                    collection_name=self._collection,
                    field_name="overwritten_in[].revision",
                    field_schema=models.IntegerIndexParams(
                        type=models.IntegerIndexType.INTEGER,
                        lookup=True,
                        range=True,
                    ),
                    wait=True,
                )
                self._qdrant.create_payload_index(
                    collection_name=self._collection,
                    field_name="revision",
                    field_schema=models.PayloadSchemaType.INTEGER,
                    wait=True,
                )

        self._checkouts: dict[str, Path] = {}
        self._checkout_snapshots: dict[
            str,
            dict[str, _CheckoutFingerprint],
        ] = {}
        self._root_files_cache_revision: int | None = None
        self._root_files_cache: dict[str, bytes | None] | None = None
        if not self._branch_exists("main"):
            self._db.execute(
                """
                INSERT INTO branches(
                    branch_id, parent_id, fork_revision, metadata_json
                ) VALUES ('main', NULL, 0, '{}')
                """
            )
            self._db.commit()

    @property
    def backend_name(self) -> str:
        return self._backend_name

    @property
    def storage_components(self) -> list[str]:
        return ["sqlite", "qdrant", "materialized-checkout"]

    def _ensure_schema(self) -> None:
        old_document_columns = {
            str(row[1])
            for row in self._db.execute("PRAGMA table_info(document_versions)")
        }
        if "value_json" in old_document_columns:
            raise RuntimeError(
                "legacy app-managed state packs chunk catalogs into document "
                "JSON; rebuild the benchmark state with storage schema v3"
            )
        old_file_columns = {
            str(row[1])
            for row in self._db.execute("PRAGMA table_info(file_versions)")
        }
        if old_file_columns and "storage_key" not in old_file_columns:
            raise RuntimeError(
                "legacy app-managed state stores document contents in SQLite; "
                "rebuild the benchmark state with storage schema v2"
            )
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS branches (
                branch_id TEXT PRIMARY KEY,
                parent_id TEXT REFERENCES branches(branch_id),
                fork_revision INTEGER NOT NULL,
                metadata_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS document_versions (
                branch_id TEXT NOT NULL,
                id TEXT NOT NULL,
                revision INTEGER NOT NULL,
                path TEXT,
                title TEXT,
                source TEXT,
                kind TEXT,
                content_hash TEXT,
                metadata_json TEXT,
                deleted INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(branch_id, id, revision)
            );
            CREATE INDEX IF NOT EXISTS document_versions_by_branch
            ON document_versions(branch_id, id, revision DESC);

            CREATE TABLE IF NOT EXISTS chunk_versions (
                branch_id TEXT NOT NULL,
                id TEXT NOT NULL,
                document_id TEXT NOT NULL,
                revision INTEGER NOT NULL,
                ordinal INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                point_id TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                PRIMARY KEY(branch_id, id, revision)
            );
            CREATE INDEX IF NOT EXISTS chunk_versions_by_document
            ON chunk_versions(
                branch_id, document_id, revision, ordinal
            );

            CREATE TABLE IF NOT EXISTS file_versions (
                branch_id TEXT NOT NULL,
                path TEXT NOT NULL,
                revision INTEGER NOT NULL,
                storage_key TEXT,
                content_hash TEXT,
                size_bytes INTEGER NOT NULL DEFAULT 0,
                deleted INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(branch_id, path, revision)
            );
            CREATE INDEX IF NOT EXISTS file_versions_by_branch
            ON file_versions(branch_id, path, revision DESC);

            CREATE TABLE IF NOT EXISTS revision_clock (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                value INTEGER NOT NULL
            );
            INSERT OR IGNORE INTO revision_clock(singleton, value)
            VALUES (1, 0);

            CREATE TABLE IF NOT EXISTS backend_state (
                state_key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS application_storage_stats (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                document_rows INTEGER NOT NULL,
                chunk_rows INTEGER NOT NULL,
                file_rows INTEGER NOT NULL,
                file_bytes INTEGER NOT NULL
            );

            CREATE TRIGGER IF NOT EXISTS document_versions_stats_insert
            AFTER INSERT ON document_versions
            BEGIN
                UPDATE application_storage_stats
                SET document_rows = document_rows + 1
                WHERE singleton = 1;
            END;
            CREATE TRIGGER IF NOT EXISTS document_versions_stats_delete
            AFTER DELETE ON document_versions
            BEGIN
                UPDATE application_storage_stats
                SET document_rows = document_rows - 1
                WHERE singleton = 1;
            END;

            CREATE TRIGGER IF NOT EXISTS file_versions_stats_insert
            AFTER INSERT ON file_versions
            BEGIN
                UPDATE application_storage_stats
                SET file_rows = file_rows + 1,
                    file_bytes = file_bytes + NEW.size_bytes
                WHERE singleton = 1;
            END;
            CREATE TRIGGER IF NOT EXISTS file_versions_stats_delete
            AFTER DELETE ON file_versions
            BEGIN
                UPDATE application_storage_stats
                SET file_rows = file_rows - 1,
                    file_bytes = file_bytes - OLD.size_bytes
                WHERE singleton = 1;
            END;
            CREATE TRIGGER IF NOT EXISTS file_versions_stats_update
            AFTER UPDATE OF size_bytes ON file_versions
            BEGIN
                UPDATE application_storage_stats
                SET file_bytes = (
                    file_bytes
                    - OLD.size_bytes
                    + NEW.size_bytes
                )
                WHERE singleton = 1;
            END;

            """
        )
        # Do not express this migration as INSERT OR IGNORE ... SELECT. SQLite
        # still evaluates the aggregate SELECT when the singleton row already
        # exists, which rescans every full-corpus BLOB whenever the backend is
        # opened. Existing databases pay the scan once; subsequent opens read
        # the trigger-maintained counters.
        stats_initialized = self._db.execute(
            """
            SELECT 1
            FROM application_storage_stats
            WHERE singleton = 1
            """
        ).fetchone()
        if stats_initialized is None:
            self._db.execute(
                """
                INSERT INTO application_storage_stats(
                    singleton, document_rows, chunk_rows,
                    file_rows, file_bytes
                )
                SELECT
                    1,
                    (SELECT COUNT(*) FROM document_versions),
                    (SELECT COUNT(*) FROM chunk_versions),
                    (SELECT COUNT(*) FROM file_versions),
                    (
                        SELECT COALESCE(SUM(size_bytes), 0)
                        FROM file_versions
                    )
                """
            )
        version = self._db.execute(
            """
            SELECT value FROM backend_state
            WHERE state_key = 'storage_schema_version'
            """
        ).fetchone()
        if version is not None and str(version["value"]) != _SCHEMA_VERSION:
            raise RuntimeError(
                "unsupported app-managed storage schema "
                f"{version['value']!r}; rebuild the benchmark state"
            )
        self._db.execute(
            """
            INSERT OR REPLACE INTO backend_state(state_key, value)
            VALUES ('storage_schema_version', ?)
            """,
            (_SCHEMA_VERSION,),
        )
        self._db.commit()

    def set_placeholder_vector_mode(self, enabled: bool) -> None:
        """Omit all-zero dense vectors while retaining searchable Qdrant points."""

        self._implicit_zero_vectors = bool(enabled)
        self._db.execute(
            """
            INSERT OR REPLACE INTO backend_state(state_key, value)
            VALUES ('implicit_zero_vectors', ?)
            """,
            ("1" if enabled else "0",),
        )
        self._db.commit()

    @staticmethod
    def _snapshot_cursor_key(branch_id: str, snapshot_id: str) -> str:
        digest = hashlib.sha256(
            f"{branch_id}\0{snapshot_id}".encode()
        ).hexdigest()
        return f"snapshot_ingestion:{digest}"

    def snapshot_ingestion_cursor(
        self,
        branch_id: str,
        snapshot_id: str,
    ) -> str | None:
        row = self._db.execute(
            """
            SELECT value
            FROM backend_state
            WHERE state_key = ?
            """,
            (self._snapshot_cursor_key(branch_id, snapshot_id),),
        ).fetchone()
        return None if row is None else str(row["value"])

    def set_snapshot_ingestion_cursor(
        self,
        branch_id: str,
        snapshot_id: str,
        relative_path: str,
    ) -> None:
        self._db.execute(
            """
            INSERT OR REPLACE INTO backend_state(state_key, value)
            VALUES (?, ?)
            """,
            (
                self._snapshot_cursor_key(branch_id, snapshot_id),
                relative_path,
            ),
        )
        self._db.commit()

    def list_branches(self) -> list[str]:
        rows = self._db.execute(
            "SELECT branch_id FROM branches ORDER BY branch_id"
        ).fetchall()
        return [str(row["branch_id"]) for row in rows]

    def _branch_exists(self, branch_id: str) -> bool:
        return (
            self._db.execute(
                "SELECT 1 FROM branches WHERE branch_id = ?",
                (branch_id,),
            ).fetchone()
            is not None
        )

    def _require_branch(self, branch_id: str) -> sqlite3.Row:
        row = self._db.execute(
            """
            SELECT branch_id, parent_id, fork_revision, metadata_json
            FROM branches
            WHERE branch_id = ?
            """,
            (branch_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"unknown branch: {branch_id}")
        return row

    def _current_revision(self) -> int:
        row = self._db.execute(
            "SELECT value FROM revision_clock WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise RuntimeError("application revision clock is missing")
        return int(row["value"])

    def _next_revision(self) -> int:
        revision = self._current_revision() + 1
        self._db.execute(
            "UPDATE revision_clock SET value = ? WHERE singleton = 1",
            (revision,),
        )
        return revision

    def _lineage(
        self,
        branch_id: str,
        *,
        cutoff: int = _MAX_REVISION,
    ) -> list[tuple[str, int]]:
        self._require_branch(branch_id)
        if self._copy_on_branch:
            return [(branch_id, cutoff)]
        return self._ancestry_with_cutoffs(branch_id, cutoff=cutoff)

    def _ancestry_with_cutoffs(
        self,
        branch_id: str,
        *,
        cutoff: int = _MAX_REVISION,
    ) -> list[tuple[str, int]]:
        self._require_branch(branch_id)
        lineage: list[tuple[str, int]] = []
        current: str | None = branch_id
        current_cutoff = cutoff
        seen: set[str] = set()
        while current is not None:
            if current in seen:
                raise RuntimeError(f"branch ancestry contains a cycle at {current}")
            seen.add(current)
            lineage.append((current, current_cutoff))
            row = self._require_branch(current)
            current_cutoff = min(
                current_cutoff,
                int(row["fork_revision"]),
            )
            current = str(row["parent_id"]) if row["parent_id"] is not None else None
        return lineage

    def create_branch(
        self,
        branch_id: str,
        parent_branch: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        with self._lock:
            if self._branch_exists(branch_id):
                raise ValueError(f"branch already exists: {branch_id}")
            self._require_branch(parent_branch)
            fork_revision = self._current_revision()
            parent_documents = (
                self._effective_documents(
                    parent_branch,
                    cutoff=fork_revision,
                )
                if self._copy_on_branch
                else {}
            )
            parent_files = (
                self._iter_effective_files(
                    parent_branch,
                    cutoff=fork_revision,
                )
                if self._copy_on_branch
                else ()
            )
            self._db.execute(
                """
                INSERT INTO branches(
                    branch_id, parent_id, fork_revision, metadata_json
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    branch_id,
                    parent_branch,
                    fork_revision,
                    canonical_json(dict(metadata or {})),
                ),
            )
            self._db.commit()
            try:
                if self._copy_on_branch:
                    document_paths = {
                        indexed.document.path
                        for _, _, indexed in parent_documents.values()
                    }
                    for owner, revision, indexed in parent_documents.values():
                        self._put_document_local(
                            branch_id,
                            self._hydrate_vectors(owner, revision, indexed),
                        )
                    for path, content in parent_files:
                        if path not in document_paths:
                            self._store_file_row(branch_id, path, content)
                    self._db.commit()
            except Exception:
                self._remove_branch_state(branch_id)
                raise

    def delete_branch(self, branch_id: str) -> None:
        if branch_id == "main":
            raise ValueError("cannot delete main")
        with self._lock:
            self._require_branch(branch_id)
            subtree = [branch_id]
            for current in subtree:
                children = self._db.execute(
                    """
                    SELECT branch_id
                    FROM branches
                    WHERE parent_id = ?
                    ORDER BY branch_id
                    """,
                    (current,),
                ).fetchall()
                subtree.extend(str(row["branch_id"]) for row in children)
            for current in reversed(subtree):
                self._remove_branch_state(current)
                checkout = self._checkouts.pop(current, None)
                self._checkout_snapshots.pop(current, None)
                if checkout is not None:
                    _remove_checkout_tree(checkout)

    def _remove_branch_state(self, branch_id: str) -> None:
        rows = self._db.execute(
            """
            SELECT point_id
            FROM chunk_versions
            WHERE branch_id = ?
            """,
            (branch_id,),
        ).fetchall()
        point_ids = [str(row["point_id"]) for row in rows]
        self._delete_points(point_ids)
        self._db.execute(
            """
            UPDATE application_storage_stats
            SET chunk_rows = chunk_rows - ?
            WHERE singleton = 1
            """,
            (len(point_ids),),
        )
        self._db.execute(
            "DELETE FROM chunk_versions WHERE branch_id = ?",
            (branch_id,),
        )
        self._db.execute(
            "DELETE FROM document_versions WHERE branch_id = ?",
            (branch_id,),
        )
        self._db.execute(
            "DELETE FROM file_versions WHERE branch_id = ?",
            (branch_id,),
        )
        self._db.execute("DELETE FROM branches WHERE branch_id = ?", (branch_id,))
        self._db.commit()
        shutil.rmtree(
            self._file_store / _safe_branch_path(branch_id),
            ignore_errors=True,
        )
        self._remove_supersession_for_branch(branch_id)

    def _append_supersession(
        self,
        point_ids: Sequence[str],
        branch_id: str,
        revision: int,
    ) -> None:
        if not point_ids:
            return
        records = self._qdrant.retrieve(
            collection_name=self._collection,
            ids=list(point_ids),
            with_payload=True,
            with_vectors=False,
        )
        marker = {"by": branch_id, "revision": int(revision)}
        for record in records:
            payload = record.payload or {}
            overwritten = list(payload.get("overwritten_in") or [])
            if marker not in overwritten:
                overwritten.append(marker)
            self._qdrant.set_payload(
                collection_name=self._collection,
                payload={"overwritten_in": overwritten},
                points=[record.id],
                wait=True,
            )

    def _remove_supersession(
        self,
        point_ids: Sequence[str],
        branch_id: str,
        revision: int,
    ) -> None:
        if not point_ids:
            return
        records = self._qdrant.retrieve(
            collection_name=self._collection,
            ids=list(point_ids),
            with_payload=True,
            with_vectors=False,
        )
        for record in records:
            payload = record.payload or {}
            overwritten = [
                marker
                for marker in payload.get("overwritten_in") or []
                if not (
                    marker.get("by") == branch_id
                    and int(marker.get("revision", -1)) == revision
                )
            ]
            self._qdrant.set_payload(
                collection_name=self._collection,
                payload={"overwritten_in": overwritten},
                points=[record.id],
                wait=True,
            )

    def _remove_supersession_for_branch(self, branch_id: str) -> None:
        marker_filter = models.Filter(
            must=[
                models.NestedCondition(
                    nested=models.Nested(
                        key="overwritten_in",
                        filter=models.Filter(
                            must=[
                                models.FieldCondition(
                                    key="by",
                                    match=models.MatchValue(
                                        value=branch_id
                                    ),
                                )
                            ]
                        ),
                    )
                )
            ]
        )
        offset: int | str | uuid.UUID | None = None
        while True:
            records, offset = self._qdrant.scroll(
                collection_name=self._collection,
                scroll_filter=marker_filter,
                limit=256,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
            for record in records:
                payload = record.payload or {}
                overwritten = [
                    marker
                    for marker in payload.get("overwritten_in") or []
                    if marker.get("by") != branch_id
                ]
                self._qdrant.set_payload(
                    collection_name=self._collection,
                    payload={"overwritten_in": overwritten},
                    points=[record.id],
                    wait=True,
                )
            if offset is None:
                break

    def put_document(
        self,
        branch_id: str,
        indexed: IndexedDocument,
        *,
        operation_id: str,
    ) -> None:
        del operation_id
        with self._lock:
            self._require_branch(branch_id)
            self._put_document_local(branch_id, indexed)
            self._db.commit()

    def put_documents(
        self,
        branch_id: str,
        indexed_documents: Sequence[IndexedDocument],
        *,
        operation_id: str,
    ) -> None:
        self._put_documents(
            branch_id,
            indexed_documents,
            operation_id=operation_id,
            bulk_load=False,
        )

    def load_documents(
        self,
        branch_id: str,
        indexed_documents: Sequence[IndexedDocument],
        *,
        operation_id: str,
    ) -> None:
        """Bulk-load a snapshot batch into a new benchmark state."""

        self._put_documents(
            branch_id,
            indexed_documents,
            operation_id=operation_id,
            bulk_load=True,
        )

    @staticmethod
    def _document_version_values(
        branch_id: str,
        revision: int,
        document: KnowledgeDocument,
    ) -> tuple[Any, ...]:
        return (
            branch_id,
            document.id,
            revision,
            document.path,
            document.title,
            document.source,
            document.kind,
            document.sha256,
            canonical_json(document.metadata),
        )

    def _chunk_version_values(
        self,
        branch_id: str,
        revision: int,
        chunk: DocumentChunk,
    ) -> tuple[Any, ...]:
        return (
            branch_id,
            chunk.id,
            chunk.document_id,
            revision,
            chunk.ordinal,
            chunk.sha256,
            self._point_id(branch_id, revision, chunk.id),
            canonical_json(chunk.metadata),
        )

    @staticmethod
    def _catalog_from_rows(
        document_row: Mapping[str, Any],
        chunk_rows: Sequence[Mapping[str, Any]],
    ) -> IndexedDocument:
        required = (
            "path",
            "title",
            "source",
            "kind",
            "content_hash",
            "metadata_json",
        )
        if any(document_row[field] is None for field in required):
            raise RuntimeError(
                f"document {document_row['id']} is missing catalog fields"
            )
        document = KnowledgeDocument(
            id=str(document_row["id"]),
            path=str(document_row["path"]),
            title=str(document_row["title"]),
            source=str(document_row["source"]),
            content="",
            kind=str(document_row["kind"]),
            metadata=json.loads(str(document_row["metadata_json"])),
        )
        chunks = tuple(
            DocumentChunk(
                id=str(row["id"]),
                document_id=str(row["document_id"]),
                ordinal=int(row["ordinal"]),
                text="",
                embedding=(),
                metadata=json.loads(str(row["metadata_json"])),
            )
            for row in sorted(chunk_rows, key=lambda value: int(value["ordinal"]))
        )
        return IndexedDocument(document, chunks)

    def _put_documents(
        self,
        branch_id: str,
        indexed_documents: Sequence[IndexedDocument],
        *,
        operation_id: str,
        bulk_load: bool,
    ) -> None:
        del operation_id
        if not indexed_documents:
            return
        with self._lock:
            self._require_branch(branch_id)
            if not bulk_load:
                for indexed in indexed_documents:
                    self._put_document_local(branch_id, indexed)
                self._db.commit()
                return

            # A benchmark load assigns one revision to the complete batch,
            # avoiding a revision-clock round trip per document.
            revision = self._next_revision()
            chunks = [
                chunk
                for indexed in indexed_documents
                for chunk in indexed.chunks
            ]
            sparse_vectors = self._bm25.documents(
                [chunk.text for chunk in chunks]
            )
            sparse_by_chunk = {
                chunk.id: sparse
                for chunk, sparse in zip(
                    chunks,
                    sparse_vectors,
                    strict=True,
                )
            }
            points = [
                models.PointStruct(
                    id=self._point_id(branch_id, revision, chunk.id),
                    vector=point_vectors(
                        (
                            ()
                            if self._implicit_zero_vectors
                            else chunk.embedding
                        ),
                        sparse_by_chunk[chunk.id],
                    ),
                    payload={
                        **payload_for_chunk(
                            document_id=indexed.document.id,
                            chunk_id=chunk.id,
                            ordinal=chunk.ordinal,
                            text=chunk.text,
                            content_hash=chunk.sha256,
                            metadata=chunk.metadata,
                        ),
                        "branch_id": branch_id,
                        "revision": revision,
                        "overwritten_in": [],
                    },
                )
                for indexed in indexed_documents
                for chunk in indexed.chunks
            ]
            try:
                bulk_upsert_points(
                    self._qdrant,
                    self._collection,
                    points,
                )
                self._db.executemany(
                    """
                    INSERT INTO document_versions(
                        branch_id, id, revision, path, title, source,
                        kind, content_hash, metadata_json, deleted
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    [
                        self._document_version_values(
                            branch_id, revision, indexed.document
                        )
                        for indexed in indexed_documents
                    ],
                )
                self._db.execute(
                    """
                    UPDATE application_storage_stats
                    SET chunk_rows = chunk_rows + ?
                    WHERE singleton = 1
                    """,
                    (len(chunks),),
                )
                self._db.executemany(
                    """
                    INSERT INTO chunk_versions(
                        branch_id, id, document_id, revision, ordinal,
                        content_hash, point_id, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        self._chunk_version_values(branch_id, revision, chunk)
                        for chunk in chunks
                    ],
                )
                for indexed in indexed_documents:
                    self._store_file_row(
                        branch_id,
                        indexed.document.path,
                        indexed.document.content.encode(),
                        revision=revision,
                    )
                self._db.commit()
            except Exception:
                self._db.rollback()
                self._delete_points([str(point.id) for point in points])
                raise

    def _put_document_local(
        self,
        branch_id: str,
        indexed: IndexedDocument,
        *,
        revision: int | None = None,
    ) -> None:
        write_revision = revision or self._next_revision()
        existing = self._effective_document(
            branch_id,
            indexed.document.id,
        )
        old_point_ids: list[str] = []
        if existing is not None:
            old_point_ids = [
                self._point_id(existing[0], existing[1], chunk.id)
                for chunk in existing[2].chunks
            ]
            self._append_supersession(
                old_point_ids,
                branch_id,
                write_revision,
            )
        if indexed.chunks:
            sparse_vectors = self._bm25.documents(
                [chunk.text for chunk in indexed.chunks]
            )
            self._qdrant.upsert(
                collection_name=self._collection,
                points=[
                    models.PointStruct(
                        id=self._point_id(
                            branch_id,
                            write_revision,
                            chunk.id,
                        ),
                        vector=point_vectors(
                            (
                                ()
                                if self._implicit_zero_vectors
                                else chunk.embedding
                            ),
                            sparse,
                        ),
                        payload={
                            **payload_for_chunk(
                                document_id=indexed.document.id,
                                chunk_id=chunk.id,
                                ordinal=chunk.ordinal,
                                text=chunk.text,
                                content_hash=chunk.sha256,
                                metadata=chunk.metadata,
                            ),
                            "branch_id": branch_id,
                            "revision": write_revision,
                            "overwritten_in": [],
                        },
                    )
                    for chunk, sparse in zip(
                        indexed.chunks,
                        sparse_vectors,
                        strict=True,
                    )
                ],
                wait=True,
            )
        self._db.execute(
            """
            INSERT INTO document_versions(
                branch_id, id, revision, path, title, source,
                kind, content_hash, metadata_json, deleted
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            self._document_version_values(
                branch_id, write_revision, indexed.document
            ),
        )
        self._db.executemany(
            """
            INSERT INTO chunk_versions(
                branch_id, id, document_id, revision, ordinal,
                content_hash, point_id, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                self._chunk_version_values(branch_id, write_revision, chunk)
                for chunk in indexed.chunks
            ],
        )
        self._db.execute(
            """
            UPDATE application_storage_stats
            SET chunk_rows = chunk_rows + ?
            WHERE singleton = 1
            """,
            (len(indexed.chunks),),
        )
        if existing is not None:
            old_path = existing[2].document.path
            if old_path != indexed.document.path:
                self._delete_file_row(
                    branch_id,
                    old_path,
                    revision=write_revision,
                )
        self._store_file_row(
            branch_id,
            indexed.document.path,
            indexed.document.content.encode(),
            revision=write_revision,
        )

    def _effective_documents(
        self,
        branch_id: str,
        *,
        cutoff: int = _MAX_REVISION,
    ) -> dict[str, tuple[str, int, IndexedDocument]]:
        resolved: dict[str, tuple[str, int, IndexedDocument]] = {}
        hidden: set[str] = set()
        for ancestor, ancestor_cutoff in self._lineage(
            branch_id,
            cutoff=cutoff,
        ):
            chunk_rows = self._db.execute(
                """
                SELECT id, document_id, revision, ordinal,
                       content_hash, point_id, metadata_json
                FROM chunk_versions
                WHERE branch_id = ? AND revision <= ?
                ORDER BY document_id, revision, ordinal
                """,
                (ancestor, ancestor_cutoff),
            ).fetchall()
            chunks_by_version: dict[tuple[str, int], list[sqlite3.Row]] = {}
            for chunk_row in chunk_rows:
                chunks_by_version.setdefault(
                    (
                        str(chunk_row["document_id"]),
                        int(chunk_row["revision"]),
                    ),
                    [],
                ).append(chunk_row)
            rows = self._db.execute(
                """
                SELECT id, revision, path, title, source, kind,
                       content_hash, metadata_json, deleted
                FROM document_versions
                WHERE branch_id = ? AND revision <= ?
                ORDER BY id, revision DESC
                """,
                (ancestor, ancestor_cutoff),
            ).fetchall()
            seen_local: set[str] = set()
            for row in rows:
                document_id = str(row["id"])
                if document_id in seen_local:
                    continue
                seen_local.add(document_id)
                if document_id in resolved or document_id in hidden:
                    continue
                if row["deleted"]:
                    hidden.add(document_id)
                    continue
                revision = int(row["revision"])
                resolved[document_id] = (
                    ancestor,
                    revision,
                    self._catalog_from_rows(
                        row,
                        chunks_by_version.get((document_id, revision), ()),
                    ),
                )
        return resolved

    def _effective_document(
        self,
        branch_id: str,
        document_id: str,
        *,
        cutoff: int = _MAX_REVISION,
    ) -> tuple[str, int, IndexedDocument] | None:
        for ancestor, ancestor_cutoff in self._lineage(
            branch_id,
            cutoff=cutoff,
        ):
            row = self._db.execute(
                """
                SELECT id, revision, path, title, source, kind,
                       content_hash, metadata_json, deleted
                FROM document_versions
                WHERE branch_id = ?
                  AND id = ?
                  AND revision <= ?
                ORDER BY revision DESC
                LIMIT 1
                """,
                (ancestor, document_id, ancestor_cutoff),
            ).fetchone()
            if row is None:
                continue
            if row["deleted"]:
                return None
            revision = int(row["revision"])
            chunks = self._db.execute(
                """
                SELECT id, document_id, revision, ordinal,
                       content_hash, point_id, metadata_json
                FROM chunk_versions
                WHERE branch_id = ?
                  AND document_id = ?
                  AND revision = ?
                ORDER BY ordinal
                """,
                (ancestor, document_id, revision),
            ).fetchall()
            return (
                ancestor,
                revision,
                self._catalog_from_rows(row, chunks),
            )
        return None

    def _common_snapshot(
        self,
        source_branch: str,
        target_branch: str,
    ) -> tuple[str, int]:
        source_ancestry = self._ancestry_with_cutoffs(source_branch)
        target_ancestry = dict(
            self._ancestry_with_cutoffs(target_branch)
        )
        common = next(
            (
                (ancestor, min(cutoff, target_ancestry[ancestor]))
                for ancestor, cutoff in source_ancestry
                if ancestor in target_ancestry
            ),
            None,
        )
        if common is None:
            raise ValueError(
                f"branches {source_branch!r} and {target_branch!r} "
                "do not share an ancestor"
            )
        return common

    def _change_keys_since(
        self,
        branch_id: str,
        *,
        common_branch: str,
        common_cutoff: int,
    ) -> tuple[set[str], set[str]]:
        """Return keys changed between a common snapshot and a branch head."""

        document_ids: set[str] = set()
        file_paths: set[str] = set()
        for ancestor, cutoff in self._ancestry_with_cutoffs(branch_id):
            lower_bound = (
                common_cutoff if ancestor == common_branch else -1
            )
            if cutoff > lower_bound:
                document_ids.update(
                    str(row["id"])
                    for row in self._db.execute(
                        """
                        SELECT DISTINCT id
                        FROM document_versions
                        WHERE branch_id = ?
                          AND revision > ?
                          AND revision <= ?
                        """,
                        (ancestor, lower_bound, cutoff),
                    )
                )
                file_paths.update(
                    str(row["path"])
                    for row in self._db.execute(
                        """
                        SELECT DISTINCT path
                        FROM file_versions
                        WHERE branch_id = ?
                          AND revision > ?
                          AND revision <= ?
                        """,
                        (ancestor, lower_bound, cutoff),
                    )
                )
            if ancestor == common_branch:
                return document_ids, file_paths
        raise RuntimeError(
            f"branch {branch_id!r} does not descend from "
            f"{common_branch!r}"
        )

    def _sparse_diff_change_keys(
        self,
        source_branch: str,
        target_branch: str,
    ) -> tuple[set[str], set[str]] | None:
        if self._copy_on_branch:
            return None
        common_branch, common_cutoff = self._common_snapshot(
            source_branch,
            target_branch,
        )
        document_ids, file_paths = self._change_keys_since(
            source_branch,
            common_branch=common_branch,
            common_cutoff=common_cutoff,
        )
        target_document_ids, target_file_paths = self._change_keys_since(
            target_branch,
            common_branch=common_branch,
            common_cutoff=common_cutoff,
        )
        document_ids.update(target_document_ids)
        file_paths.update(target_file_paths)
        return document_ids, file_paths

    def _sparse_merge_change_keys(
        self,
        source_branch: str,
        target_branch: str,
    ) -> tuple[set[str], set[str]] | None:
        if self._copy_on_branch:
            return None
        common_branch, common_cutoff = self._common_snapshot(
            source_branch,
            target_branch,
        )
        _, target_file_paths = self._change_keys_since(
            target_branch,
            common_branch=common_branch,
            common_cutoff=common_cutoff,
        )
        if target_file_paths and source_branch in self._checkouts:
            raise ValueError(
                f"merge conflict: target branch {target_branch!r} "
                "contains filesystem changes made after the source "
                "snapshot; recreate the source branch from the current "
                "target before merging"
            )
        return self._change_keys_since(
            source_branch,
            common_branch=common_branch,
            common_cutoff=common_cutoff,
        )

    def _local_change_keys(
        self,
        branch_id: str,
    ) -> tuple[set[str], set[str]]:
        document_ids = {
            str(row["id"])
            for row in self._db.execute(
                """
                SELECT DISTINCT id
                FROM document_versions
                WHERE branch_id = ?
                """,
                (branch_id,),
            )
        }
        file_paths = {
            str(row["path"])
            for row in self._db.execute(
                """
                SELECT DISTINCT path
                FROM file_versions
                WHERE branch_id = ?
                """,
                (branch_id,),
            )
        }
        return document_ids, file_paths

    def delete_document(
        self,
        branch_id: str,
        document_id: str,
        *,
        operation_id: str,
    ) -> bool:
        del operation_id
        with self._lock:
            existing = self._effective_document(branch_id, document_id)
            if existing is None:
                return False
            revision = self._next_revision()
            point_ids = [
                self._point_id(existing[0], existing[1], chunk.id)
                for chunk in existing[2].chunks
            ]
            self._append_supersession(point_ids, branch_id, revision)
            self._db.execute(
                """
                INSERT INTO document_versions(
                    branch_id, id, revision, path, title, source,
                    kind, content_hash, metadata_json, deleted
                ) VALUES (?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, 1)
                """,
                (branch_id, document_id, revision),
            )
            self._delete_file_row(
                branch_id,
                existing[2].document.path,
                revision=revision,
            )
            self._db.commit()
            return True

    def get_document(
        self,
        branch_id: str,
        document_id: str,
    ) -> IndexedDocument | None:
        value = self._effective_document(branch_id, document_id)
        if value is None:
            return None
        source_branch, revision, indexed = value
        return self._hydrate_vectors(source_branch, revision, indexed)

    def _hydrate_vectors(
        self,
        source_branch: str,
        revision: int,
        indexed: IndexedDocument,
    ) -> IndexedDocument:
        content = self._effective_file(
            source_branch,
            indexed.document.path,
            cutoff=revision,
        )
        if content is None:
            raise RuntimeError(
                f"file state is missing for document {indexed.document.id}"
            )
        document = KnowledgeDocument(
            id=indexed.document.id,
            path=indexed.document.path,
            title=indexed.document.title,
            source=indexed.document.source,
            content=content.decode(),
            kind=indexed.document.kind,
            metadata=dict(indexed.document.metadata),
        )
        if not indexed.chunks:
            return IndexedDocument(document, ())
        records = self._qdrant.retrieve(
            collection_name=self._collection,
            ids=[
                self._point_id(source_branch, revision, chunk.id)
                for chunk in indexed.chunks
            ],
            with_vectors=True,
            with_payload=True,
        )
        by_id = {str(record.id): record for record in records}
        chunks = []
        for chunk in indexed.chunks:
            point_id = self._point_id(source_branch, revision, chunk.id)
            record = by_id.get(point_id)
            if record is None:
                raise RuntimeError(
                    f"vector state is missing for {source_branch}:{chunk.id}"
                )
            payload = record.payload or {}
            vectors = record.vector if isinstance(record.vector, Mapping) else {}
            dense = vectors.get(DENSE_VECTOR)
            chunks.append(
                DocumentChunk(
                    id=chunk.id,
                    document_id=chunk.document_id,
                    ordinal=chunk.ordinal,
                    text=str(payload.get("text", "")),
                    embedding=(
                        tuple(_coerce_vector(dense))
                        if dense is not None
                        else (0.0,) * self.vector_dimensions
                    ),
                    metadata=dict(
                        payload.get("chunk_metadata")
                        or chunk.metadata
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
        points = hybrid_query(
            self._qdrant,
            collection_name=self._collection,
            dense_query=(
                None if self._implicit_zero_vectors else query_embedding
            ),
            sparse_query=self._bm25.query(query_text),
            query_filter=_application_qdrant_filter(
                self._lineage(branch_id)
            ),
            limit=max(limit * 4, 32),
        )
        effective: dict[str, tuple[str, int, IndexedDocument] | None] = {}
        hits: list[SearchHit] = []
        per_document: dict[str, int] = {}
        for point in points:
            payload = point.payload or {}
            document_id = str(payload.get("document_id", ""))
            chunk_id = str(payload.get("chunk_id", ""))
            if document_id not in effective:
                effective[document_id] = self._effective_document(
                    branch_id,
                    document_id,
                )
            current = effective[document_id]
            if current is None:
                continue
            owner, revision, indexed = current
            if (
                owner != str(payload.get("branch_id", ""))
                or revision != int(payload.get("revision", -1))
                or per_document.get(document_id, 0) >= 2
                or not any(chunk.id == chunk_id for chunk in indexed.chunks)
            ):
                continue
            per_document[document_id] = per_document.get(document_id, 0) + 1
            hits.append(
                SearchHit(
                    document_id=document_id,
                    chunk_id=chunk_id,
                    path=indexed.document.path,
                    title=indexed.document.title,
                    text=str(payload.get("text", "")),
                    score=float(getattr(point, "score", 0.0)),
                    source=indexed.document.source,
                    metadata={
                        **dict(indexed.document.metadata),
                        **dict(payload.get("chunk_metadata") or {}),
                        "document_kind": indexed.document.kind,
                    },
                )
            )
            if len(hits) >= limit:
                break
        return hits

    def _effective_files(
        self,
        branch_id: str,
        *,
        cutoff: int = _MAX_REVISION,
    ) -> dict[str, bytes]:
        resolved: dict[str, bytes] = {}
        hidden: set[str] = set()
        for ancestor, ancestor_cutoff in self._lineage(
            branch_id,
            cutoff=cutoff,
        ):
            local: dict[str, bytes | None]
            if ancestor == "main":
                latest = self._db.execute(
                    """
                    SELECT MAX(revision) AS revision
                    FROM file_versions
                    WHERE branch_id = ? AND revision <= ?
                    """,
                    (ancestor, ancestor_cutoff),
                ).fetchone()
                latest_revision = (
                    int(latest["revision"])
                    if latest is not None
                    and latest["revision"] is not None
                    else -1
                )
                if (
                    self._root_files_cache is None
                    or self._root_files_cache_revision != latest_revision
                ):
                    rows = self._db.execute(
                        """
                        SELECT path, storage_key, content_hash, deleted
                        FROM file_versions
                        WHERE branch_id = ? AND revision <= ?
                        ORDER BY path, revision DESC
                        """,
                        (ancestor, ancestor_cutoff),
                    ).fetchall()
                    local = {}
                    for row in rows:
                        path = str(row["path"])
                        if path in local:
                            continue
                        if row["deleted"]:
                            local[path] = None
                            continue
                        local[path] = self._read_stored_file(row)
                    self._root_files_cache_revision = latest_revision
                    self._root_files_cache = local
                else:
                    local = self._root_files_cache
            else:
                rows = self._db.execute(
                    """
                    SELECT path, storage_key, content_hash, deleted
                    FROM file_versions
                    WHERE branch_id = ? AND revision <= ?
                    ORDER BY path, revision DESC
                    """,
                    (ancestor, ancestor_cutoff),
                ).fetchall()
                local = {}
                for row in rows:
                    path = str(row["path"])
                    if path in local:
                        continue
                    if row["deleted"]:
                        local[path] = None
                        continue
                    local[path] = self._read_stored_file(row)
            for path, content in local.items():
                if path in resolved or path in hidden:
                    continue
                if content is None:
                    hidden.add(path)
                    continue
                resolved[path] = content
        return resolved

    def _iter_effective_files(
        self,
        branch_id: str,
        *,
        cutoff: int = _MAX_REVISION,
    ) -> Iterator[tuple[str, bytes]]:
        """Stream visible files without retaining their contents in memory."""

        seen: set[str] = set()
        for ancestor, ancestor_cutoff in self._lineage(
            branch_id,
            cutoff=cutoff,
        ):
            rows = self._db.execute(
                """
                SELECT path, storage_key, content_hash, deleted
                FROM file_versions
                WHERE branch_id = ? AND revision <= ?
                ORDER BY path, revision DESC
                """,
                (ancestor, ancestor_cutoff),
            )
            for row in rows:
                path = str(row["path"])
                if path in seen:
                    continue
                seen.add(path)
                if row["deleted"]:
                    continue
                yield path, self._read_stored_file(row)

    def _effective_file(
        self,
        branch_id: str,
        path: str,
        *,
        cutoff: int = _MAX_REVISION,
    ) -> bytes | None:
        normalized = normalize_workspace_path(path)
        for ancestor, ancestor_cutoff in self._lineage(
            branch_id,
            cutoff=cutoff,
        ):
            row = self._db.execute(
                """
                SELECT storage_key, content_hash, deleted
                FROM file_versions
                WHERE branch_id = ?
                  AND path = ?
                  AND revision <= ?
                ORDER BY revision DESC
                LIMIT 1
                """,
                (ancestor, normalized, ancestor_cutoff),
            ).fetchone()
            if row is None:
                continue
            if row["deleted"]:
                return None
            return self._read_stored_file(row)
        return None

    def _read_stored_file(self, row: Mapping[str, Any]) -> bytes:
        storage_key = row["storage_key"]
        if storage_key is None:
            raise RuntimeError("file version is missing its filesystem key")
        target = self._file_store / str(storage_key)
        if not target.is_file():
            raise RuntimeError(f"file version is missing: {target}")
        content = target.read_bytes()
        expected = row["content_hash"]
        if expected is not None and content_hash(content) != str(expected):
            raise RuntimeError(f"file version hash mismatch: {target}")
        return content

    def _store_file_row(
        self,
        branch_id: str,
        path: str,
        content: bytes,
        *,
        revision: int | None = None,
    ) -> None:
        normalized = normalize_workspace_path(path)
        write_revision = revision or self._next_revision()
        digest = content_hash(content)
        storage_key = (
            Path(_safe_branch_path(branch_id))
            / digest[:2]
            / digest
        )
        target = self._file_store / storage_key
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
            temporary.write_bytes(content)
            temporary.replace(target)
        self._db.execute(
            """
            INSERT INTO file_versions(
                branch_id, path, revision, storage_key,
                content_hash, size_bytes, deleted
            ) VALUES (?, ?, ?, ?, ?, ?, 0)
            """,
            (
                branch_id,
                normalized,
                write_revision,
                storage_key.as_posix(),
                digest,
                len(content),
            ),
        )
        self._mirror_checkout_write(branch_id, normalized, bytes(content))

    def _delete_file_row(
        self,
        branch_id: str,
        path: str,
        *,
        revision: int | None = None,
    ) -> None:
        normalized = normalize_workspace_path(path)
        write_revision = revision or self._next_revision()
        self._db.execute(
            """
            INSERT INTO file_versions(
                branch_id, path, revision, storage_key,
                content_hash, size_bytes, deleted
            ) VALUES (?, ?, ?, NULL, NULL, 0, 1)
            """,
            (branch_id, normalized, write_revision),
        )
        self._mirror_checkout_delete(branch_id, normalized)

    def write_file(
        self,
        branch_id: str,
        path: str,
        content: bytes,
        *,
        operation_id: str,
    ) -> None:
        del operation_id
        with self._lock:
            self._require_branch(branch_id)
            self._store_file_row(branch_id, path, content)
            self._db.commit()

    def delete_file(
        self,
        branch_id: str,
        path: str,
        *,
        operation_id: str,
    ) -> bool:
        del operation_id
        with self._lock:
            normalized = normalize_workspace_path(path)
            if self._effective_file(branch_id, normalized) is None:
                return False
            self._delete_file_row(branch_id, normalized)
            self._db.commit()
            return True

    def read_file(self, branch_id: str, path: str) -> bytes:
        normalized = normalize_workspace_path(path)
        checkout = self._checkouts.get(branch_id)
        if checkout is not None:
            target = _checkout_path(checkout, normalized)
            if target.is_file():
                return target.read_bytes()
        content = self._effective_file(branch_id, normalized)
        if content is None:
            raise FileNotFoundError(normalized)
        return content

    def mount_branch(
        self,
        branch_id: str,
        mount_path: str | Path | None = None,
    ) -> Path:
        self._require_branch(branch_id)
        existing = self._checkouts.get(branch_id)
        if (
            existing is not None
            and existing.exists()
            and (
                mount_path is None
                or Path(mount_path).expanduser().resolve() == existing
            )
        ):
            return existing
        if mount_path is not None:
            path = Path(mount_path).expanduser().resolve()
            if path.exists() and any(path.iterdir()):
                raise ValueError(f"comparison checkout path must be empty: {path}")
        else:
            checkout_root = self.state_dir / "checkouts"
            path = (checkout_root / _safe_branch_path(branch_id)).resolve()
            if path.exists() and any(path.iterdir()):
                path = (
                    checkout_root
                    / f"{_safe_branch_path(branch_id)}-{uuid.uuid4().hex[:8]}"
                ).resolve()
        path.mkdir(parents=True, exist_ok=True)
        fingerprints: dict[str, _CheckoutFingerprint] = {}
        # Stream the inherited corpus from the external file store. This keeps
        # checkout construction bounded by one document rather than retaining
        # the complete enterprise corpus in process memory.
        for workspace_path, content in self._iter_effective_files(branch_id):
            target = _checkout_path(path, workspace_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            stat = target.stat()
            fingerprints[workspace_path] = (
                hashlib.sha256(content).hexdigest(),
                stat.st_size,
                stat.st_mtime_ns,
                stat.st_ctime_ns,
            )
        self._checkouts[branch_id] = path
        self._checkout_snapshots[branch_id] = fingerprints
        return path

    def _synchronize_checkout(self, branch_id: str) -> None:
        checkout = self._checkouts.get(branch_id)
        if checkout is None or not checkout.exists():
            return
        previous = self._checkout_snapshots.get(branch_id, {})
        fingerprints: dict[str, _CheckoutFingerprint] = {}
        for path in checkout.rglob("*"):
            if path.is_symlink():
                continue
            if path.is_file():
                workspace_path = "/" + path.relative_to(checkout).as_posix()
                stat = path.stat()
                previous_fingerprint = previous.get(workspace_path)
                metadata = (
                    stat.st_size,
                    stat.st_mtime_ns,
                    stat.st_ctime_ns,
                )
                if (
                    previous_fingerprint is not None
                    and previous_fingerprint[1:] == metadata
                ):
                    fingerprints[workspace_path] = previous_fingerprint
                    continue
                content = path.read_bytes()
                digest = hashlib.sha256(content).hexdigest()
                fingerprints[workspace_path] = (digest, *metadata)
                if (
                    previous_fingerprint is None
                    or previous_fingerprint[0] != digest
                ):
                    self._store_file_row(
                        branch_id,
                        workspace_path,
                        content,
                    )
        for path in previous.keys() - fingerprints.keys():
            self._delete_file_row(branch_id, path)
        self._db.commit()
        self._checkout_snapshots[branch_id] = fingerprints

    def release_session_checkouts(self) -> None:
        """Persist and remove materialized directories at a replay boundary."""

        with self._lock:
            for branch_id, checkout in list(self._checkouts.items()):
                self._synchronize_checkout(branch_id)
                self._checkouts.pop(branch_id, None)
                self._checkout_snapshots.pop(branch_id, None)
                _remove_checkout_tree(checkout)

    def _mirror_checkout_write(
        self,
        branch_id: str,
        path: str,
        content: bytes,
    ) -> None:
        checkout = self._checkouts.get(branch_id)
        if checkout is None:
            return
        target = _checkout_path(checkout, path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        stat = target.stat()
        self._checkout_snapshots.setdefault(branch_id, {})[path] = (
            hashlib.sha256(content).hexdigest(),
            stat.st_size,
            stat.st_mtime_ns,
            stat.st_ctime_ns,
        )

    def _mirror_checkout_delete(self, branch_id: str, path: str) -> None:
        checkout = self._checkouts.get(branch_id)
        if checkout is not None:
            target = _checkout_path(checkout, path)
            if target.exists():
                target.unlink()
        self._checkout_snapshots.setdefault(branch_id, {}).pop(path, None)

    def diff(self, source_branch: str, target_branch: str) -> dict[str, Any]:
        self._synchronize_checkout(source_branch)
        self._synchronize_checkout(target_branch)
        sparse_changes = self._sparse_diff_change_keys(
            source_branch,
            target_branch,
        )
        if sparse_changes is not None:
            document_ids, file_paths = sparse_changes
            source: dict[str, str] = {}
            target: dict[str, str] = {}
            for document_id in document_ids:
                source_value = self._effective_document(
                    source_branch,
                    document_id,
                )
                target_value = self._effective_document(
                    target_branch,
                    document_id,
                )
                if source_value is not None:
                    source[document_id] = indexed_document_digest(
                        self._hydrate_vectors(*source_value)
                    )
                if target_value is not None:
                    target[document_id] = indexed_document_digest(
                        self._hydrate_vectors(*target_value)
                    )
            source_files: dict[str, str] = {}
            target_files: dict[str, str] = {}
            for path in file_paths:
                source_content = self._effective_file(
                    source_branch,
                    path,
                )
                target_content = self._effective_file(
                    target_branch,
                    path,
                )
                if source_content is not None:
                    source_files[path] = content_hash(source_content)
                if target_content is not None:
                    target_files[path] = content_hash(target_content)
            return knowledge_state_diff(
                source,
                target,
                source_files,
                target_files,
            )
        source = {
            document_id: indexed_document_digest(
                self._hydrate_vectors(
                    source_branch_id,
                    revision,
                    indexed,
                )
            )
            for document_id, (
                source_branch_id,
                revision,
                indexed,
            ) in self._effective_documents(source_branch).items()
        }
        target = {
            document_id: indexed_document_digest(
                self._hydrate_vectors(
                    source_branch_id,
                    revision,
                    indexed,
                )
            )
            for document_id, (
                source_branch_id,
                revision,
                indexed,
            ) in self._effective_documents(target_branch).items()
        }
        source_files = {
            path: content_hash(content)
            for path, content in self._effective_files(source_branch).items()
        }
        target_files = {
            path: content_hash(content)
            for path, content in self._effective_files(target_branch).items()
        }
        return knowledge_state_diff(
            source,
            target,
            source_files,
            target_files,
        )

    def merge(
        self,
        source_branch: str,
        target_branch: str,
        *,
        operation_id: str,
    ) -> dict[str, Any]:
        del operation_id
        if source_branch == target_branch:
            return {"documents": 0, "files": 0}
        with self._lock:
            self._synchronize_checkout(source_branch)
            self._synchronize_checkout(target_branch)
            sparse_changes = self._sparse_merge_change_keys(
                source_branch,
                target_branch,
            )
            if sparse_changes is not None:
                document_ids, file_paths = sparse_changes
                source_documents: dict[str, IndexedDocument | None] = {}
                target_document_digests: dict[str, str | None] = {}
                for document_id in document_ids:
                    source_value = self._effective_document(
                        source_branch,
                        document_id,
                    )
                    target_value = self._effective_document(
                        target_branch,
                        document_id,
                    )
                    source_documents[document_id] = (
                        self._hydrate_vectors(*source_value)
                        if source_value is not None
                        else None
                    )
                    target_document_digests[document_id] = (
                        indexed_document_digest(
                            self._hydrate_vectors(*target_value)
                        )
                        if target_value is not None
                        else None
                    )
                source_files = {
                    path: self._effective_file(source_branch, path)
                    for path in file_paths
                }
                target_files = {
                    path: self._effective_file(target_branch, path)
                    for path in file_paths
                }

                document_changes = 0
                for document_id in sorted(document_ids):
                    source_value = source_documents[document_id]
                    source_digest = (
                        indexed_document_digest(source_value)
                        if source_value is not None
                        else None
                    )
                    if source_digest == target_document_digests[document_id]:
                        continue
                    document_changes += 1
                    if source_value is None:
                        self.delete_document(
                            target_branch,
                            document_id,
                            operation_id=f"merge-delete:{document_id}",
                        )
                    else:
                        self._put_document_local(target_branch, source_value)

                file_changes = 0
                for path in sorted(file_paths):
                    source_content = source_files[path]
                    if source_content == target_files[path]:
                        continue
                    file_changes += 1
                    if self._effective_file(target_branch, path) == source_content:
                        continue
                    if source_content is None:
                        self._delete_file_row(target_branch, path)
                    else:
                        self._store_file_row(
                            target_branch,
                            path,
                            source_content,
                        )
                self._db.commit()
                return {
                    "documents": document_changes,
                    "files": file_changes,
                }
            source_values = self._effective_documents(source_branch)
            source_documents = {
                document_id: self._hydrate_vectors(
                    owner,
                    revision,
                    indexed,
                )
                for document_id, (
                    owner,
                    revision,
                    indexed,
                ) in source_values.items()
            }
            source_ancestry = self._ancestry_with_cutoffs(source_branch)
            target_ancestry = dict(self._ancestry_with_cutoffs(target_branch))
            common = next(
                (
                    (ancestor, min(cutoff, target_ancestry[ancestor]))
                    for ancestor, cutoff in source_ancestry
                    if ancestor in target_ancestry
                ),
                None,
            )
            if common is None:
                raise ValueError(
                    f"branches {source_branch!r} and {target_branch!r} "
                    "do not share an ancestor"
                )
            common_branch, common_cutoff = common
            base_values = self._effective_documents(
                common_branch,
                cutoff=common_cutoff,
            )
            base_documents = {
                document_id: self._hydrate_vectors(
                    owner,
                    revision,
                    indexed,
                )
                for document_id, (
                    owner,
                    revision,
                    indexed,
                ) in base_values.items()
            }
            document_changes = 0
            for document_id in sorted(source_documents.keys() | base_documents.keys()):
                source_value = source_documents.get(document_id)
                base_value = base_documents.get(document_id)
                source_digest = (
                    indexed_document_digest(source_value)
                    if source_value is not None
                    else None
                )
                base_digest = (
                    indexed_document_digest(base_value)
                    if base_value is not None
                    else None
                )
                if source_digest == base_digest:
                    continue
                document_changes += 1
                if source_value is None:
                    self.delete_document(
                        target_branch,
                        document_id,
                        operation_id=f"merge-delete:{document_id}",
                    )
                else:
                    self._put_document_local(target_branch, source_value)

            source_files = self._effective_files(source_branch)
            base_files = self._effective_files(
                common_branch,
                cutoff=common_cutoff,
            )
            file_changes = 0
            for path in sorted(source_files.keys() | base_files.keys()):
                source_content = source_files.get(path)
                base_content = base_files.get(path)
                if source_content == base_content:
                    continue
                file_changes += 1
                if source_content is None:
                    self._delete_file_row(target_branch, path)
                else:
                    self._store_file_row(
                        target_branch,
                        path,
                        source_content,
                    )
            self._db.commit()
            return {
                "documents": document_changes,
                "files": file_changes,
            }

    def state_digest(self, branch_id: str) -> str:
        self._synchronize_checkout(branch_id)
        documents = [
            self._hydrate_vectors(owner, revision, indexed)
            for owner, revision, indexed in
            self._effective_documents(branch_id).values()
        ]
        return knowledge_state_digest(
            documents,
            list(self._effective_files(branch_id).items()),
        )

    def storage_stats(self) -> dict[str, int]:
        stats = self._db.execute(
            """
            SELECT document_rows, chunk_rows, file_rows, file_bytes
            FROM application_storage_stats
            WHERE singleton = 1
            """
        ).fetchone()
        if stats is None:
            raise RuntimeError("application storage statistics are missing")
        branches = self._db.execute("SELECT COUNT(*) AS count FROM branches").fetchone()
        sqlite_bytes = (
            self.state_dir / "application-state.sqlite"
        ).stat().st_size
        qdrant_bytes = _directory_bytes(self._qdrant_storage_dir)
        filesystem_bytes = _directory_bytes(self._file_store)
        checkout_bytes = _directory_bytes(self.state_dir / "checkouts")
        return {
            "branches": int(branches["count"]),
            "document_rows": int(stats["document_rows"]),
            "chunk_rows": int(stats["chunk_rows"]),
            "file_rows": int(stats["file_rows"]),
            "file_bytes": int(stats["file_bytes"]),
            "sqlite_bytes": sqlite_bytes,
            "qdrant_bytes": qdrant_bytes,
            "filesystem_bytes": filesystem_bytes,
            "materialized_checkout_bytes": checkout_bytes,
            "total_state_bytes": (
                sqlite_bytes
                + qdrant_bytes
                + filesystem_bytes
                + checkout_bytes
            ),
        }

    def destroy(self) -> None:
        if self._qdrant.collection_exists(self._collection):
            self._qdrant.delete_collection(self._collection)

    def _delete_points(self, point_ids: Sequence[str]) -> None:
        if point_ids:
            self._qdrant.delete(
                collection_name=self._collection,
                points_selector=models.PointIdsList(points=list(point_ids)),
                wait=True,
            )

    def _point_id(
        self,
        branch_id: str,
        revision: int,
        chunk_id: str,
    ) -> str:
        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{self._collection}:{branch_id}:{revision}:{chunk_id}",
            )
        )

    def close(self) -> None:
        for branch_id in list(self._checkouts):
            self._synchronize_checkout(branch_id)
        self._db.close()
        self._qdrant.close()


class ApplicationManagedKnowledgeBackend(_ApplicationStateBackend):
    """Branch overlays resolved by walking parent branches in application code."""

    _copy_on_branch = False
    _backend_name = "app-managed"


class PhysicalCloneKnowledgeBackend(_ApplicationStateBackend):
    """Fully duplicate visible SQLite, file, and vector state at every fork."""

    _copy_on_branch = True
    _backend_name = "physical-clone"


def _application_qdrant_filter(
    lineage: Sequence[tuple[str, int]],
) -> models.Filter:
    """Select visible point revisions for an application-managed branch."""

    should: list[models.Condition] = []
    must_not: list[models.Condition] = []
    for branch_id, cutoff in lineage:
        should.append(
            models.Filter(
                must=[
                    models.FieldCondition(
                        key="branch_id",
                        match=models.MatchValue(value=branch_id),
                    ),
                    models.FieldCondition(
                        key="revision",
                        range=models.Range(lte=cutoff),
                    ),
                ]
            )
        )
        must_not.append(
            models.NestedCondition(
                nested=models.Nested(
                    key="overwritten_in",
                    filter=models.Filter(
                        must=[
                            models.FieldCondition(
                                key="by",
                                match=models.MatchValue(value=branch_id),
                            ),
                            models.FieldCondition(
                                key="revision",
                                range=models.Range(lte=cutoff),
                            ),
                        ]
                    ),
                )
            )
        )
    return models.Filter(should=should, must_not=must_not)


def _coerce_vector(vector: Any) -> list[float]:
    if isinstance(vector, Mapping):
        if len(vector) != 1:
            raise RuntimeError("named multi-vectors are not supported")
        vector = next(iter(vector.values()))
    if not isinstance(vector, Sequence) or isinstance(
        vector,
        (str, bytes, bytearray),
    ):
        raise RuntimeError(  # noqa: TRY004 - invalid external response
            "Qdrant returned an unsupported vector"
        )
    return [float(value) for value in vector]


def _safe_branch_path(branch_id: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", branch_id).strip("-")[:64]
    digest = hashlib.sha256(branch_id.encode()).hexdigest()[:12]
    return f"{slug or 'branch'}-{digest}"


def _checkout_path(root: Path, workspace_path: str) -> Path:
    target = (root / normalize_workspace_path(workspace_path).lstrip("/")).resolve()
    if not target.is_relative_to(root.resolve()):
        raise ValueError(f"path escapes checkout: {workspace_path}")
    return target


def _remove_checkout_tree(path: Path) -> None:
    """Remove a large materialized checkout using disjoint directory shards."""

    if not path.exists():
        return
    roots: list[Path] = []
    for current, directories, _ in os.walk(path):
        current_path = Path(current)
        depth = len(current_path.relative_to(path).parts)
        if depth < 3:
            continue
        roots.append(current_path)
        directories.clear()
    if len(roots) > 1:
        with ThreadPoolExecutor(max_workers=min(8, len(roots))) as executor:
            list(
                executor.map(
                    lambda root: shutil.rmtree(root, ignore_errors=True),
                    roots,
                )
            )
    shutil.rmtree(path, ignore_errors=True)


def _directory_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(
        item.stat().st_blocks * 512
        for item in path.rglob("*")
        if item.is_file()
    )


__all__ = [
    "ApplicationManagedKnowledgeBackend",
    "PhysicalCloneKnowledgeBackend",
]
