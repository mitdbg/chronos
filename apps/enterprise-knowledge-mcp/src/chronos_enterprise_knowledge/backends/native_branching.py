"""Baseline built from independent native branching mechanisms.

Doltgres branches relational document state, Qdrant filters one shared vector
collection by branch history, and Btrfs snapshots the file workspace. The
application coordinates the three branch implementations behind the same MCP
storage contract used by Chronos.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import re
import threading
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
from qdrant_client import models

from chronos_enterprise_knowledge.backends.btrfs_workspace import (
    BtrfsWorkspaceStore,
)
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
)
from chronos_enterprise_knowledge.retrieval import (
    BM25_VECTOR,
    DENSE_VECTOR,
    Bm25Encoder,
    bulk_upsert_points,
    dense_vector_or_none,
    hybrid_query,
    named_vector_config,
    payload_for_chunk,
    point_vectors,
    qdrant_collection_options,
    remote_qdrant_client,
)

_AUTHOR = "Chronos baseline <chronos-baseline@example.com>"
_POINT_NAMESPACE = uuid.UUID("b609e562-73bd-4f6d-a2c6-7066dc3c5b64")
class DoltgresQdrantBtrfsKnowledgeBackend:
    """Coordinate Doltgres, branch-aware Qdrant, and Btrfs snapshots."""

    def __init__(
        self,
        state_dir: str | Path,
        *,
        vector_dimensions: int = 1536,
        qdrant_url: str | None = None,
        qdrant_api_key: str | None = None,
        doltgres_dsn: str | None = None,
        btrfs_root: str | Path | None = None,
        doltgres_data_dir: str | Path | None = None,
        qdrant_storage_dir: str | Path | None = None,
    ):
        if not doltgres_dsn:
            raise ValueError(
                "doltgres-qdrant-btrfs requires --doltgres-dsn or "
                "CHRONOS_DOLTGRES_DSN"
            )
        if not qdrant_url:
            raise ValueError(
                "doltgres-qdrant-btrfs requires a shared Qdrant service URL"
            )
        self.state_dir = Path(state_dir).expanduser().resolve()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.vector_dimensions = int(vector_dimensions)
        if self.vector_dimensions <= 0:
            raise ValueError("vector_dimensions must be positive")
        self._lock = threading.RLock()
        self._db = psycopg.connect(
            doltgres_dsn,
            autocommit=True,
            row_factory=dict_row,
        )
        self._qdrant = remote_qdrant_client(
            qdrant_url,
            api_key=qdrant_api_key,
        )
        database = str(
            conninfo_to_dict(doltgres_dsn).get("dbname") or "postgres"
        )
        namespace = hashlib.sha256(
            f"{self.state_dir}:{database}".encode()
        ).hexdigest()[:16]
        self._collection = f"enterprise_native_branching_v2_{namespace}"
        self._placeholder_vector_mode = False
        self._bm25 = Bm25Encoder()
        self._doltgres_data_dir = _component_storage_path(
            doltgres_data_dir,
            database,
        )
        self._qdrant_storage_dir = _component_storage_path(
            qdrant_storage_dir,
            f"collections/{self._collection}",
        )
        self._ingestion_cursors_path = (
            self.state_dir / "snapshot-ingestion-cursors.json"
        )
        workspace_root = self.state_dir / "btrfs"
        if btrfs_root is not None:
            workspace_root = (
                Path(btrfs_root).expanduser().resolve()
                / f"enterprise-{namespace}"
            )
        self._files = BtrfsWorkspaceStore(workspace_root)
        self._ensure_qdrant_collection()
        self._ensure_relational_schema()

    @property
    def backend_name(self) -> str:
        return "doltgres-qdrant-btrfs"

    @property
    def storage_components(self) -> list[str]:
        return [
            "doltgres-branches",
            "qdrant-branch-aware-payloads",
            self._files.storage_component,
        ]

    def _ensure_qdrant_collection(self) -> None:
        if not self._qdrant.collection_exists(self._collection):
            self._create_qdrant_collection()
        collection = self._qdrant.get_collection(self._collection)
        vectors = collection.config.params.vectors
        if not isinstance(vectors, Mapping) or DENSE_VECTOR not in vectors:
            raise RuntimeError("native Qdrant collection lacks the dense vector")
        if int(vectors[DENSE_VECTOR].size) != self.vector_dimensions:
            raise RuntimeError(
                "Qdrant collection vector size does not match the backend: "
                f"{vectors[DENSE_VECTOR].size} != {self.vector_dimensions}"
            )
        sparse = collection.config.params.sparse_vectors or {}
        if BM25_VECTOR not in sparse:
            raise RuntimeError("native Qdrant collection lacks the BM25 vector")
        self._ensure_qdrant_payload_indexes()

    def _create_qdrant_collection(self) -> None:
        dense, sparse = named_vector_config(
            self.vector_dimensions,
            on_disk=True,
        )
        self._qdrant.create_collection(
            collection_name=self._collection,
            vectors_config=dense,
            sparse_vectors_config=sparse,
            **qdrant_collection_options(on_disk=True),
        )

    def _ensure_qdrant_payload_indexes(self) -> None:
        payload_schema = self._qdrant.get_collection(
            self._collection
        ).payload_schema
        indexes = (
            (
                "branch",
                models.KeywordIndexParams(
                    type=models.KeywordIndexType.KEYWORD,
                    on_disk=True,
                ),
            ),
            (
                "seq",
                models.IntegerIndexParams(
                    type=models.IntegerIndexType.INTEGER,
                    lookup=True,
                    range=True,
                    on_disk=True,
                ),
            ),
            (
                "overwritten_in[].by",
                models.KeywordIndexParams(
                    type=models.KeywordIndexType.KEYWORD,
                    on_disk=True,
                ),
            ),
            (
                "overwritten_in[].seq",
                models.IntegerIndexParams(
                    type=models.IntegerIndexType.INTEGER,
                    lookup=True,
                    range=True,
                    on_disk=True,
                ),
            ),
        )
        for field, schema in indexes:
            if field in payload_schema:
                continue
            self._qdrant.create_payload_index(
                collection_name=self._collection,
                field_name=field,
                field_schema=schema,
                wait=True,
            )

    def set_placeholder_vector_mode(self, enabled: bool) -> None:
        """Omit dense vectors while retaining text and BM25 retrieval."""

        with self._lock:
            enabled = bool(enabled)
            self._placeholder_vector_mode = enabled

    def snapshot_ingestion_cursor(
        self,
        branch_id: str,
        snapshot_id: str,
    ) -> str | None:
        with self._lock:
            cursors = self._read_ingestion_cursors()
            value = cursors.get(f"{branch_id}\0{snapshot_id}")
            return str(value) if value is not None else None

    def set_snapshot_ingestion_cursor(
        self,
        branch_id: str,
        snapshot_id: str,
        relative_path: str,
    ) -> None:
        with self._lock:
            cursors = self._read_ingestion_cursors()
            cursors[f"{branch_id}\0{snapshot_id}"] = relative_path
            temporary = self._ingestion_cursors_path.with_suffix(
                f".tmp-{uuid.uuid4().hex}"
            )
            temporary.write_text(
                json.dumps(cursors, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            temporary.replace(self._ingestion_cursors_path)

    def _read_ingestion_cursors(self) -> dict[str, str]:
        if not self._ingestion_cursors_path.is_file():
            return {}
        value = json.loads(
            self._ingestion_cursors_path.read_text(encoding="utf-8")
        )
        if not isinstance(value, Mapping):
            raise RuntimeError("invalid native snapshot-ingestion cursor file")
        return {str(key): str(item) for key, item in value.items()}

    def _ensure_relational_schema(self) -> None:
        self._checkout("main")
        statements = (
            """
            CREATE TABLE IF NOT EXISTS cross_store_branch_catalog (
                branch_id TEXT PRIMARY KEY,
                parent_id TEXT,
                parent_cutoff BIGINT,
                current_seq BIGINT NOT NULL,
                metadata_json TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS knowledge_documents (
                id TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                kind TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS knowledge_chunks (
                id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                ordinal BIGINT NOT NULL,
                content_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                point_id TEXT NOT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS knowledge_chunks_by_document
            ON knowledge_chunks(document_id, ordinal)
            """,
            """
            CREATE TABLE IF NOT EXISTS cross_store_branch_changes (
                branch_id TEXT NOT NULL,
                object_kind TEXT NOT NULL,
                object_key TEXT NOT NULL,
                PRIMARY KEY(branch_id, object_kind, object_key)
            )
            """,
        )
        for statement in statements:
            self._db.execute(statement)
        row = self._db.execute(
            """
            SELECT 1 FROM cross_store_branch_catalog
            WHERE branch_id = 'main'
            """
        ).fetchone()
        if row is None:
            self._db.execute(
                """
                INSERT INTO cross_store_branch_catalog(
                    branch_id, parent_id, parent_cutoff,
                    current_seq, metadata_json
                ) VALUES ('main', NULL, NULL, 0, '{}')
                """
            )
        self._commit_current("initialize enterprise knowledge schema")

    def _checkout(self, branch_id: str) -> None:
        active = self._db.execute("SELECT active_branch() AS name").fetchone()
        if active is not None and str(active["name"]) == branch_id:
            return
        self._db.execute("SELECT dolt_checkout(%s)", (branch_id,))

    def _commit_current(self, message: str) -> None:
        if not self._db.execute("SELECT 1 FROM dolt_status LIMIT 1").fetchone():
            return
        self._db.execute("SELECT dolt_add('.')")
        self._db.execute(
            "SELECT dolt_commit('-m', %s, '--author', %s)",
            (message[:240], _AUTHOR),
        )

    def _reset_current(self) -> None:
        self._db.execute("SELECT dolt_reset('--hard')")

    def _registry_rows(self) -> dict[str, dict[str, Any]]:
        active = str(
            self._db.execute("SELECT active_branch() AS name").fetchone()[
                "name"
            ]
        )
        self._checkout("main")
        try:
            rows = self._db.execute(
                """
                SELECT branch_id, parent_id, parent_cutoff,
                       current_seq, metadata_json
                FROM cross_store_branch_catalog
                ORDER BY branch_id
                """
            ).fetchall()
            return {str(row["branch_id"]): dict(row) for row in rows}
        finally:
            self._checkout(active)

    def _require_branch(self, branch_id: str) -> dict[str, Any]:
        rows = self._registry_rows()
        try:
            return rows[branch_id]
        except KeyError as exc:
            raise ValueError(f"unknown branch: {branch_id}") from exc

    def list_branches(self) -> list[str]:
        with self._lock:
            return sorted(self._registry_rows())

    def create_branch(
        self,
        branch_id: str,
        parent_branch: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        with self._lock:
            registry = self._registry_rows()
            if branch_id in registry:
                raise ValueError(f"branch already exists: {branch_id}")
            try:
                parent = registry[parent_branch]
            except KeyError as exc:
                raise ValueError(f"unknown branch: {parent_branch}") from exc
            self._checkout(parent_branch)
            self._db.execute("SELECT dolt_branch(%s)", (branch_id,))
            files_created = False
            try:
                self._files.create_branch(branch_id, parent_branch)
                files_created = True
                self._checkout("main")
                self._db.execute(
                    """
                    INSERT INTO cross_store_branch_catalog(
                        branch_id, parent_id, parent_cutoff,
                        current_seq, metadata_json
                    ) VALUES (%s, %s, CAST(%s AS BIGINT), 0, %s)
                    """,
                    (
                        branch_id,
                        parent_branch,
                        str(parent["current_seq"]),
                        canonical_json(dict(metadata or {})),
                    ),
                )
                self._commit_current(f"register branch {branch_id}")
            except Exception:
                self._checkout("main")
                try:
                    self._db.execute(
                        "SELECT dolt_branch('-D', %s)",
                        (branch_id,),
                    )
                except Exception:
                    pass
                if files_created:
                    self._files.delete_branch(branch_id)
                raise

    def delete_branch(self, branch_id: str) -> None:
        if branch_id == "main":
            raise ValueError("cannot delete main")
        with self._lock:
            registry = self._registry_rows()
            if branch_id not in registry:
                raise ValueError(f"unknown branch: {branch_id}")
            subtree = [branch_id]
            for current in subtree:
                subtree.extend(
                    candidate
                    for candidate, row in registry.items()
                    if row["parent_id"] == current
                )
            for current in reversed(subtree):
                # Remove the filesystem checkout first. If Btrfs rejects the
                # deletion, the relational and vector branch remain intact
                # and the operation can be retried safely.
                self._files.delete_branch(current)
                self._remove_qdrant_branch_state(current)
                self._checkout("main")
                self._db.execute(
                    "SELECT dolt_branch('-D', %s)",
                    (current,),
                )
            self._checkout("main")
            for current in reversed(subtree):
                self._db.execute(
                    """
                    DELETE FROM cross_store_branch_changes
                    WHERE branch_id = %s
                    """,
                    (current,),
                )
                self._db.execute(
                    "DELETE FROM cross_store_branch_catalog WHERE branch_id = %s",
                    (current,),
                )
            self._commit_current(f"delete branch subtree {branch_id}")

    def _lineage(self, branch_id: str) -> list[tuple[str, int]]:
        registry = self._registry_rows()
        if branch_id not in registry:
            raise ValueError(f"unknown branch: {branch_id}")
        lineage: list[tuple[str, int]] = []
        current = branch_id
        cutoff = int(registry[current]["current_seq"])
        seen: set[str] = set()
        while True:
            if current in seen:
                raise RuntimeError(f"branch ancestry contains a cycle at {current}")
            seen.add(current)
            lineage.append((current, cutoff))
            row = registry[current]
            parent = row["parent_id"]
            if parent is None:
                return lineage
            cutoff = int(row["parent_cutoff"])
            current = str(parent)

    def _next_sequence(self, branch_id: str) -> int:
        return int(self._require_branch(branch_id)["current_seq"]) + 1

    def _advance_sequence(
        self,
        branch_id: str,
        sequence: int,
        message: str,
        *,
        changed_documents: Sequence[str] = (),
        changed_files: Sequence[str] = (),
    ) -> None:
        self._checkout("main")
        self._set_current_sequence(branch_id, sequence)
        self._record_change_rows(
            branch_id,
            documents=changed_documents,
            files=changed_files,
        )
        self._commit_current(message)

    def _set_current_sequence(
        self,
        branch_id: str,
        sequence: int,
    ) -> None:
        # Bind BIGINT values as text for compatibility with Doltgres's current
        # extended-protocol receiver.
        self._db.execute(
            """
            UPDATE cross_store_branch_catalog
            SET current_seq = CAST(%s AS BIGINT)
            WHERE branch_id = %s AND current_seq < CAST(%s AS BIGINT)
            """,
            (str(sequence), branch_id, str(sequence)),
        )

    def _record_branch_changes(
        self,
        branch_id: str,
        *,
        documents: Sequence[str] = (),
        files: Sequence[str] = (),
        message: str,
    ) -> None:
        if branch_id == "main":
            return
        self._checkout("main")
        self._record_change_rows(
            branch_id,
            documents=documents,
            files=files,
        )
        self._commit_current(message)

    def _record_change_rows(
        self,
        branch_id: str,
        *,
        documents: Sequence[str] = (),
        files: Sequence[str] = (),
    ) -> None:
        if branch_id == "main":
            return
        for kind, values in (
            ("document", documents),
            ("file", files),
        ):
            for value in values:
                self._db.execute(
                    """
                    INSERT INTO cross_store_branch_changes(
                        branch_id, object_kind, object_key
                    ) VALUES (%s, %s, %s)
                    ON CONFLICT(branch_id, object_kind, object_key)
                    DO NOTHING
                    """,
                    (branch_id, kind, value),
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

    def _put_documents(
        self,
        branch_id: str,
        indexed_documents: Sequence[IndexedDocument],
        *,
        operation_id: str,
        bulk_load: bool,
    ) -> None:
        if not indexed_documents:
            return
        with self._lock:
            self._require_branch(branch_id)
            sequence = self._next_sequence(branch_id)
            self._checkout(branch_id)
            document_ids = [
                indexed.document.id for indexed in indexed_documents
            ]
            if bulk_load:
                existing_ids: set[str] = set()
            else:
                placeholders = sql.SQL(", ").join(
                    sql.Placeholder() for _ in document_ids
                )
                existing_ids = {
                    str(row["id"])
                    for row in self._db.execute(
                        sql.SQL(
                            "SELECT id FROM knowledge_documents "
                            "WHERE id IN ({})"
                        ).format(placeholders),
                        document_ids,
                    ).fetchall()
                }
            old_point_ids = [
                point_id
                for document_id in existing_ids
                for point_id in self._document_point_ids(document_id)
            ]
            self._append_supersession(
                old_point_ids,
                branch_id,
                sequence,
            )
            new_point_ids = self._upsert_document_points(
                branch_id,
                sequence,
                indexed_documents,
            )
            file_backups = {
                indexed.document.path: (
                    _optional_file(
                        self._files,
                        branch_id,
                        indexed.document.path,
                    )
                    if indexed.document.id in existing_ids
                    else None
                )
                for indexed in indexed_documents
            }
            try:
                with self._db.transaction():
                    self._store_document_batch(
                        branch_id,
                        sequence,
                        indexed_documents,
                        existing_ids=existing_ids,
                        bulk_insert=bulk_load,
                    )
                    for indexed in indexed_documents:
                        self._files.write(
                            branch_id,
                            indexed.document.path,
                            indexed.document.content.encode(),
                        )
                    if branch_id == "main":
                        self._set_current_sequence(branch_id, sequence)
                self._commit_current(
                    f"{operation_id}: write {len(indexed_documents)} documents"
                )
                if branch_id != "main":
                    self._advance_sequence(
                        branch_id,
                        sequence,
                        f"{operation_id}: advance vector sequence",
                        changed_documents=document_ids,
                        changed_files=[
                            indexed.document.path
                            for indexed in indexed_documents
                        ],
                    )
            except Exception:
                self._reset_current()
                self._delete_points(new_point_ids)
                self._remove_supersession(
                    old_point_ids,
                    branch_id,
                    sequence,
                )
                for path, previous in file_backups.items():
                    _restore_file(self._files, branch_id, path, previous)
                raise

    def _store_document_batch(
        self,
        branch_id: str,
        sequence: int,
        indexed_documents: Sequence[IndexedDocument],
        *,
        existing_ids: set[str],
        bulk_insert: bool = False,
    ) -> None:
        for document_id in existing_ids:
            self._db.execute(
                "DELETE FROM knowledge_chunks WHERE document_id = %s",
                (document_id,),
            )
            self._db.execute(
                "DELETE FROM knowledge_documents WHERE id = %s",
                (document_id,),
            )
        document_rows = [
            (
                indexed.document.id,
                indexed.document.path,
                indexed.document.title,
                indexed.document.source,
                indexed.document.kind,
                indexed.document.sha256,
                canonical_json(dict(indexed.document.metadata)),
            )
            for indexed in indexed_documents
        ]
        chunk_rows = [
            (
                chunk.id,
                chunk.document_id,
                chunk.ordinal if bulk_insert else str(chunk.ordinal),
                chunk.sha256,
                canonical_json(dict(chunk.metadata)),
                self._point_id(
                    branch_id,
                    sequence,
                    chunk.id,
                ),
            )
            for indexed in indexed_documents
            for chunk in indexed.chunks
        ]
        if bulk_insert:
            self._copy_rows(
                "knowledge_documents",
                (
                    "id",
                    "path",
                    "title",
                    "source",
                    "kind",
                    "content_hash",
                    "metadata_json",
                ),
                document_rows,
            )
            self._copy_rows(
                "knowledge_chunks",
                (
                    "id",
                    "document_id",
                    "ordinal",
                    "content_hash",
                    "metadata_json",
                    "point_id",
                ),
                chunk_rows,
            )
        else:
            self._insert_rows(
                """
                INSERT INTO knowledge_documents(
                    id, path, title, source, kind, content_hash, metadata_json
                )
                """,
                "(%s, %s, %s, %s, %s, %s, %s)",
                document_rows,
            )
            self._insert_rows(
                """
                INSERT INTO knowledge_chunks(
                    id, document_id, ordinal, content_hash,
                    metadata_json, point_id
                )
                """,
                "(%s, %s, CAST(%s AS BIGINT), %s, %s, %s)",
                chunk_rows,
            )

    def _copy_rows(
        self,
        table: str,
        columns: Sequence[str],
        rows: Sequence[Sequence[Any]],
    ) -> None:
        if not rows:
            return
        statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        )
        with self._db.cursor().copy(statement) as copy:
            for row in rows:
                copy.write_row(row)

    def _insert_rows(
        self,
        statement: str,
        row_template: str,
        rows: Sequence[Sequence[Any]],
        *,
        batch_size: int = 512,
    ) -> None:
        for offset in range(0, len(rows), batch_size):
            batch = rows[offset : offset + batch_size]
            values = ", ".join(row_template for _ in batch)
            self._db.execute(
                f"{statement} VALUES {values}",
                tuple(itertools.chain.from_iterable(batch)),
            )

    def _store_document_rows(
        self,
        branch_id: str,
        sequence: int,
        indexed: IndexedDocument,
    ) -> None:
        self._db.execute(
            "DELETE FROM knowledge_chunks WHERE document_id = %s",
            (indexed.document.id,),
        )
        self._db.execute(
            "DELETE FROM knowledge_documents WHERE id = %s",
            (indexed.document.id,),
        )
        self._db.execute(
            """
            INSERT INTO knowledge_documents(
                id, path, title, source, kind, content_hash, metadata_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                indexed.document.id,
                indexed.document.path,
                indexed.document.title,
                indexed.document.source,
                indexed.document.kind,
                indexed.document.sha256,
                canonical_json(dict(indexed.document.metadata)),
            ),
        )
        for chunk in indexed.chunks:
            self._db.execute(
                """
                INSERT INTO knowledge_chunks(
                    id, document_id, ordinal, content_hash,
                    metadata_json, point_id
                ) VALUES (
                    %s, %s, CAST(%s AS BIGINT), %s, %s, %s
                )
                """,
                (
                    chunk.id,
                    chunk.document_id,
                    str(chunk.ordinal),
                    chunk.sha256,
                    canonical_json(dict(chunk.metadata)),
                    self._point_id(
                        branch_id,
                        sequence,
                        chunk.id,
                    ),
                ),
            )

    def _upsert_document_points(
        self,
        branch_id: str,
        sequence: int,
        indexed_documents: Sequence[IndexedDocument],
    ) -> list[str]:
        points: list[models.PointStruct] = []
        point_ids: list[str] = []
        chunks = [
            chunk for indexed in indexed_documents for chunk in indexed.chunks
        ]
        sparse_vectors = self._bm25.documents([chunk.text for chunk in chunks])
        sparse_by_chunk = {
            chunk.id: sparse
            for chunk, sparse in zip(chunks, sparse_vectors, strict=True)
        }
        for indexed in indexed_documents:
            for chunk in indexed.chunks:
                point_id = self._point_id(branch_id, sequence, chunk.id)
                point_ids.append(point_id)
                points.append(
                    models.PointStruct(
                        id=point_id,
                        vector=point_vectors(
                            (
                                ()
                                if self._placeholder_vector_mode
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
                            "branch": branch_id,
                            "seq": sequence,
                            "overwritten_in": [],
                        },
                    )
                )
        try:
            bulk_upsert_points(
                self._qdrant,
                self._collection,
                points,
            )
        except Exception:
            # Point IDs are deterministic, so a clean benchmark restart can
            # safely replace an interrupted bulk load.
            self._delete_points(point_ids)
            raise
        return point_ids

    def delete_document(
        self,
        branch_id: str,
        document_id: str,
        *,
        operation_id: str,
    ) -> bool:
        with self._lock:
            self._require_branch(branch_id)
            self._checkout(branch_id)
            existing = self._get_document(document_id, hydrate=False)
            if existing is None:
                return False
            sequence = self._next_sequence(branch_id)
            point_ids = self._document_point_ids(document_id)
            previous_file = _optional_file(
                self._files,
                branch_id,
                existing.document.path,
            )
            self._append_supersession(point_ids, branch_id, sequence)
            try:
                with self._db.transaction():
                    self._db.execute(
                        "DELETE FROM knowledge_chunks WHERE document_id = %s",
                        (document_id,),
                    )
                    self._db.execute(
                        "DELETE FROM knowledge_documents WHERE id = %s",
                        (document_id,),
                    )
                    self._files.delete(
                        branch_id,
                        existing.document.path,
                    )
                self._commit_current(f"{operation_id}: delete {document_id}")
                self._advance_sequence(
                    branch_id,
                    sequence,
                    f"{operation_id}: advance vector sequence",
                    changed_documents=(document_id,),
                    changed_files=(existing.document.path,),
                )
            except Exception:
                self._reset_current()
                self._remove_supersession(point_ids, branch_id, sequence)
                _restore_file(
                    self._files,
                    branch_id,
                    existing.document.path,
                    previous_file,
                )
                raise
            return True

    def get_document(
        self,
        branch_id: str,
        document_id: str,
    ) -> IndexedDocument | None:
        with self._lock:
            self._require_branch(branch_id)
            self._checkout(branch_id)
            return self._get_document(document_id, hydrate=True)

    def _get_document(
        self,
        document_id: str,
        *,
        hydrate: bool,
    ) -> IndexedDocument | None:
        row = self._db.execute(
            """
            SELECT id, path, title, source, kind, content_hash, metadata_json
            FROM knowledge_documents WHERE id = %s
            """,
            (document_id,),
        ).fetchone()
        if row is None:
            return None
        chunk_rows = self._db.execute(
            """
            SELECT id, document_id, ordinal, content_hash,
                   metadata_json, point_id
            FROM knowledge_chunks
            WHERE document_id = %s
            ORDER BY ordinal, id
            """,
            (document_id,),
        ).fetchall()
        points: dict[str, Any] = {}
        if chunk_rows:
            records = self._qdrant.retrieve(
                collection_name=self._collection,
                ids=[str(chunk["point_id"]) for chunk in chunk_rows],
                with_vectors=hydrate,
                with_payload=True,
            )
            points = {str(record.id): record for record in records}
        document = KnowledgeDocument(
            id=str(row["id"]),
            path=str(row["path"]),
            title=str(row["title"]),
            source=str(row["source"]),
            content=self._files.read(
                str(self._db.execute(
                    "SELECT active_branch() AS name"
                ).fetchone()["name"]),
                str(row["path"]),
            ).decode(),
            kind=str(row["kind"]),  # type: ignore[arg-type]
            metadata=json.loads(str(row["metadata_json"])),
        )
        chunks = []
        for chunk in chunk_rows:
            point_id = str(chunk["point_id"])
            point = points.get(point_id)
            if point is None:
                raise RuntimeError(f"Qdrant point is missing: {point_id}")
            payload = point.payload or {}
            text = str(payload.get("text", ""))
            if content_hash(text) != str(chunk["content_hash"]):
                raise RuntimeError(f"Qdrant chunk hash mismatch: {point_id}")
            metadata = dict(
                payload.get("chunk_metadata")
                or json.loads(str(chunk["metadata_json"]))
            )
            chunks.append(
                DocumentChunk(
                    id=str(chunk["id"]),
                    document_id=str(chunk["document_id"]),
                    ordinal=int(chunk["ordinal"]),
                    text=text,
                    embedding=(
                        self._decode_dense_vector(point.vector)
                        if hydrate
                        else (0.0,) * self.vector_dimensions
                    ),
                    metadata=metadata,
                )
            )
        return IndexedDocument(document, tuple(chunks))

    def _document_point_ids(self, document_id: str) -> list[str]:
        return [
            str(row["point_id"])
            for row in self._db.execute(
                """
                SELECT point_id FROM knowledge_chunks
                WHERE document_id = %s
                ORDER BY ordinal, id
                """,
                (document_id,),
            ).fetchall()
        ]

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
        with self._lock:
            lineage = self._lineage(branch_id)
            points = hybrid_query(
                self._qdrant,
                collection_name=self._collection,
                dense_query=(
                    None
                    if self._placeholder_vector_mode
                    else query_embedding
                ),
                sparse_query=self._bm25.query(query_text),
                query_filter=_qdrant_branch_filter(lineage),
                limit=max(limit * 4, 32),
            )
            self._checkout(branch_id)
            chunk_ids = [
                str((point.payload or {}).get("chunk_id", ""))
                for point in points
            ]
            chunk_rows: dict[str, dict[str, Any]] = {}
            # Doltgres does not reliably turn a large IN predicate into point
            # lookups, and its optimizer scans the table for the chunk-document
            # join. Resolve the bounded candidate set through the two primary
            # keys instead.
            for chunk_id in dict.fromkeys(chunk_ids):
                row = self._db.execute(
                    """
                    SELECT id AS chunk_id, document_id, content_hash,
                           metadata_json, point_id
                    FROM knowledge_chunks
                    WHERE id = %s
                    """,
                    (chunk_id,),
                ).fetchone()
                if row is not None:
                    chunk_rows[str(row["chunk_id"])] = dict(row)
            document_rows: dict[str, dict[str, Any]] = {}
            for document_id in dict.fromkeys(
                str(row["document_id"]) for row in chunk_rows.values()
            ):
                row = self._db.execute(
                    """
                    SELECT id AS document_id, path, title, source, kind,
                           metadata_json AS document_metadata_json
                    FROM knowledge_documents
                    WHERE id = %s
                    """,
                    (document_id,),
                ).fetchone()
                if row is not None:
                    document_rows[document_id] = dict(row)
            metadata_by_chunk: dict[str, dict[str, Any]] = {}
            for chunk_id, chunk_row in chunk_rows.items():
                document_row = document_rows.get(
                    str(chunk_row["document_id"])
                )
                if document_row is not None:
                    metadata_by_chunk[chunk_id] = chunk_row | document_row
            hits: list[SearchHit] = []
            per_document: dict[str, int] = {}
            for point in points:
                payload = point.payload or {}
                chunk_id = str(payload.get("chunk_id", ""))
                row = metadata_by_chunk.get(chunk_id)
                if row is None or str(row["point_id"]) != str(point.id):
                    continue
                if str(row["content_hash"]) != str(payload.get("content_hash", "")):
                    continue
                document_id = str(row["document_id"])
                if per_document.get(document_id, 0) >= 2:
                    continue
                per_document[document_id] = (
                    per_document.get(document_id, 0) + 1
                )
                hits.append(
                    SearchHit(
                        document_id=document_id,
                        chunk_id=str(row["chunk_id"]),
                        path=str(row["path"]),
                        title=str(row["title"]),
                        text=str(payload.get("text", "")),
                        score=float(getattr(point, "score", 0.0)),
                        source=str(row["source"]),
                        metadata={
                            **json.loads(
                                str(row["document_metadata_json"])
                            ),
                            **dict(
                                payload.get("chunk_metadata")
                                or json.loads(str(row["metadata_json"]))
                            ),
                            "document_kind": str(row["kind"]),
                        },
                    )
                )
                if len(hits) >= limit:
                    break
            return hits

    def _decode_dense_vector(self, vector: Any) -> tuple[float, ...]:
        if not isinstance(vector, Mapping):
            raise RuntimeError("Qdrant returned unnamed vectors")
        dense = vector.get(DENSE_VECTOR)
        if dense is None:
            return (0.0,) * self.vector_dimensions
        values = _coerce_vector(dense)
        if len(values) != self.vector_dimensions:
            raise RuntimeError("Qdrant returned a dense vector with wrong dimensions")
        return tuple(values)

    def write_file(
        self,
        branch_id: str,
        path: str,
        content: bytes,
        *,
        operation_id: str,
    ) -> None:
        with self._lock:
            self._require_branch(branch_id)
            self._files.write(branch_id, path, content)
            self._record_branch_changes(
                branch_id,
                files=(path,),
                message=f"{operation_id}: record file write",
            )

    def delete_file(
        self,
        branch_id: str,
        path: str,
        *,
        operation_id: str,
    ) -> bool:
        with self._lock:
            self._require_branch(branch_id)
            deleted = self._files.delete(branch_id, path)
            if deleted:
                self._record_branch_changes(
                    branch_id,
                    files=(path,),
                    message=f"{operation_id}: record file deletion",
                )
            return deleted

    def read_file(self, branch_id: str, path: str) -> bytes:
        with self._lock:
            self._require_branch(branch_id)
            return self._files.read(branch_id, path)

    def mount_branch(
        self,
        branch_id: str,
        mount_path: str | Path | None = None,
    ) -> Path:
        with self._lock:
            self._require_branch(branch_id)
            return self._files.checkout(branch_id, mount_path)

    def _all_documents(
        self,
        branch_id: str,
        *,
        hydrate: bool,
    ) -> dict[str, IndexedDocument]:
        self._checkout(branch_id)
        identifiers = [
            str(row["id"])
            for row in self._db.execute(
                "SELECT id FROM knowledge_documents ORDER BY id"
            ).fetchall()
        ]
        return {
            identifier: value
            for identifier in identifiers
            if (
                value := self._get_document(
                    identifier,
                    hydrate=hydrate,
                )
            )
            is not None
        }

    def diff(self, source_branch: str, target_branch: str) -> dict[str, Any]:
        with self._lock:
            document_ids, file_paths = self._changed_keys(
                source_branch,
                target_branch,
            )
            source = self._document_digests(source_branch, document_ids)
            target = self._document_digests(target_branch, document_ids)
            source_files = self._file_digests(source_branch, file_paths)
            target_files = self._file_digests(target_branch, file_paths)
            return knowledge_state_diff(
                source,
                target,
                source_files,
                target_files,
            )

    def _changed_keys(
        self,
        source_branch: str,
        target_branch: str,
    ) -> tuple[set[str], set[str]]:
        registry = self._registry_rows()
        if source_branch not in registry or target_branch not in registry:
            missing = (
                source_branch
                if source_branch not in registry
                else target_branch
            )
            raise ValueError(f"unknown branch: {missing}")
        related = set(
            _branch_path_to_root(source_branch, registry)
            + _branch_path_to_root(target_branch, registry)
        )
        self._checkout("main")
        placeholders = sql.SQL(", ").join(
            sql.Placeholder() for _ in related
        )
        rows = self._db.execute(
            sql.SQL(
                """
                SELECT object_kind, object_key
                FROM cross_store_branch_changes
                WHERE branch_id IN ({})
                """
            ).format(placeholders),
            sorted(related),
        ).fetchall()
        documents = {
            str(row["object_key"])
            for row in rows
            if row["object_kind"] == "document"
        }
        files = {
            str(row["object_key"])
            for row in rows
            if row["object_kind"] == "file"
        }
        return documents, files

    def _document_digests(
        self,
        branch_id: str,
        document_ids: Sequence[str],
    ) -> dict[str, str]:
        self._checkout(branch_id)
        values: dict[str, str] = {}
        for document_id in document_ids:
            indexed = self._get_document(document_id, hydrate=False)
            if indexed is not None:
                values[document_id] = indexed_document_digest(indexed)
        return values

    def _file_digests(
        self,
        branch_id: str,
        paths: Sequence[str],
    ) -> dict[str, str]:
        values: dict[str, str] = {}
        for path in paths:
            try:
                content = self._files.read(branch_id, path)
            except FileNotFoundError:
                continue
            values[path] = content_hash(content)
        return values

    def merge(
        self,
        source_branch: str,
        target_branch: str,
        *,
        operation_id: str,
    ) -> dict[str, Any]:
        if source_branch == target_branch:
            return {"documents": 0, "files": 0}
        with self._lock:
            registry = self._registry_rows()
            if source_branch not in registry:
                raise ValueError(f"unknown branch: {source_branch}")
            if target_branch not in registry:
                raise ValueError(f"unknown branch: {target_branch}")
            source_lineage = dict(self._lineage(source_branch))
            if target_branch not in source_lineage:
                raise ValueError(
                    "merge target must be an ancestor of the source branch"
                )
            target_sequence = int(registry[target_branch]["current_seq"])
            source_cutoff = int(source_lineage[target_branch])
            if target_sequence != source_cutoff:
                raise ValueError(
                    "merge target advanced after the source branch was created"
                )
            document_ids, file_paths = self._changed_keys(
                source_branch,
                target_branch,
            )
            self._checkout(target_branch)
            before = {
                identifier: value
                for identifier in document_ids
                if (
                    value := self._get_document(
                        identifier,
                        hydrate=False,
                    )
                )
                is not None
            }
            before_point_ids = {
                identifier: self._document_point_ids(identifier)
                for identifier in before
            }
            self._db.execute("SELECT dolt_merge(%s)", (source_branch,))
            base_branch = self._filesystem_merge_base(
                source_branch,
                target_branch,
            )
            files = self._files.apply_paths_delta(
                source_branch,
                base_branch,
                target_branch,
                file_paths,
            )
            self._checkout(target_branch)
            after = {
                identifier: value
                for identifier in document_ids
                if (
                    value := self._get_document(
                        identifier,
                        hydrate=False,
                    )
                )
                is not None
            }
            changed_ids = [
                identifier
                for identifier in sorted(before.keys() | after.keys())
                if _optional_document_digest(before.get(identifier))
                != _optional_document_digest(after.get(identifier))
            ]
            if changed_ids:
                sequence = self._next_sequence(target_branch)
                old_point_ids = [
                    point_id
                    for identifier in changed_ids
                    if identifier in before
                    for point_id in before_point_ids[identifier]
                ]
                changed_documents = [
                    self._get_document(identifier, hydrate=True)
                    for identifier in changed_ids
                    if identifier in after
                ]
                materialized = [
                    value for value in changed_documents if value is not None
                ]
                new_point_ids = self._upsert_document_points(
                    target_branch,
                    sequence,
                    materialized,
                )
                self._append_supersession(
                    old_point_ids,
                    target_branch,
                    sequence,
                )
                try:
                    self._checkout(target_branch)
                    with self._db.transaction():
                        for indexed in materialized:
                            for chunk in indexed.chunks:
                                self._db.execute(
                                    """
                                    UPDATE knowledge_chunks
                                    SET point_id = %s
                                    WHERE id = %s
                                    """,
                                    (
                                        self._point_id(
                                            target_branch,
                                            sequence,
                                            chunk.id,
                                        ),
                                        chunk.id,
                                    ),
                                )
                    self._commit_current(
                        f"{operation_id}: materialize merged vectors"
                    )
                    self._advance_sequence(
                        target_branch,
                        sequence,
                        f"{operation_id}: advance vector sequence",
                        changed_documents=changed_ids,
                        changed_files=sorted(file_paths),
                    )
                except Exception:
                    self._delete_points(new_point_ids)
                    self._remove_supersession(
                        old_point_ids,
                        target_branch,
                        sequence,
                    )
                    raise
            elif files:
                self._record_branch_changes(
                    target_branch,
                    files=sorted(file_paths),
                    message=f"{operation_id}: record merged files",
                )
            return {"documents": len(changed_ids), "files": files}

    def _filesystem_merge_base(
        self,
        source_branch: str,
        target_branch: str,
    ) -> str:
        registry = self._registry_rows()
        source_path = _branch_path_to_root(source_branch, registry)
        target_path = _branch_path_to_root(target_branch, registry)
        target_set = set(target_path)
        common = next(branch for branch in source_path if branch in target_set)
        source_position = source_path.index(common)
        if source_position > 0:
            return source_path[source_position - 1]
        target_position = target_path.index(common)
        if target_position > 0:
            return target_path[target_position - 1]
        raise RuntimeError("merge requires distinct branches")

    def state_digest(self, branch_id: str) -> str:
        with self._lock:
            documents = list(
                self._all_documents(
                    branch_id,
                    hydrate=False,
                ).values()
            )
            return knowledge_state_digest(
                documents,
                list(self._files.files(branch_id).items()),
            )

    def storage_stats(self) -> dict[str, int | None]:
        with self._lock:
            registry = self._registry_rows()
            # Corpus cardinality is a logical validation metric, so count the
            # main branch once. Summing visible rows over every branch both
            # double-counts inherited data and turns storage accounting into
            # a full scan of the corpus for every workflow.
            self._checkout("main")
            main_documents = int(
                self._db.execute(
                    "SELECT COUNT(*) AS count FROM knowledge_documents"
                ).fetchone()["count"]
            )
            main_chunks = int(
                self._db.execute(
                    "SELECT COUNT(*) AS count FROM knowledge_chunks"
                ).fetchone()["count"]
            )
            vector_points = int(
                self._qdrant.count(
                    collection_name=self._collection,
                    exact=True,
                ).count
            )
            btrfs_bytes = self._files.filesystem_used_bytes()
            doltgres_bytes = _directory_bytes(self._doltgres_data_dir)
            qdrant_bytes = _directory_bytes(self._qdrant_storage_dir)
            known_bytes = btrfs_bytes + doltgres_bytes + qdrant_bytes
            return {
                "branches": len(registry),
                "main_documents": main_documents,
                "main_chunks": main_chunks,
                "vector_points": vector_points,
                "btrfs_bytes": btrfs_bytes,
                "doltgres_bytes": doltgres_bytes,
                "qdrant_bytes": qdrant_bytes,
                "total_state_bytes": known_bytes,
            }

    def _append_supersession(
        self,
        point_ids: Sequence[str],
        branch_id: str,
        sequence: int,
    ) -> None:
        if not point_ids:
            return
        records = self._qdrant.retrieve(
            collection_name=self._collection,
            ids=list(point_ids),
            with_payload=True,
            with_vectors=False,
        )
        marker = {"by": branch_id, "seq": sequence}
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
        sequence: int,
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
                    and int(marker.get("seq", -1)) == sequence
                )
            ]
            self._qdrant.set_payload(
                collection_name=self._collection,
                payload={"overwritten_in": overwritten},
                points=[record.id],
                wait=True,
            )

    def _remove_qdrant_branch_state(self, branch_id: str) -> None:
        self._qdrant.delete(
            collection_name=self._collection,
            points_selector=models.FilterSelector(
                filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="branch",
                            match=models.MatchValue(value=branch_id),
                        )
                    ]
                )
            ),
            wait=True,
        )
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

    def _delete_points(self, point_ids: Sequence[str]) -> None:
        if not point_ids:
            return
        self._qdrant.delete(
            collection_name=self._collection,
            points_selector=models.PointIdsList(points=list(point_ids)),
            wait=True,
        )

    def _point_id(
        self,
        branch_id: str,
        sequence: int,
        chunk_id: str,
    ) -> str:
        return str(
            uuid.uuid5(
                _POINT_NAMESPACE,
                f"{self._collection}:{branch_id}:{sequence}:{chunk_id}",
            )
        )

    def close(self) -> None:
        self._files.close()
        self._db.close()
        self._qdrant.close()

    def destroy(self) -> None:
        if self._qdrant.collection_exists(self._collection):
            self._qdrant.delete_collection(self._collection)
        self._files.destroy()


def _qdrant_branch_filter(
    lineage: Sequence[tuple[str, int]],
) -> models.Filter:
    should: list[models.Condition] = []
    must_not: list[models.Condition] = []
    for branch_id, cutoff in lineage:
        should.append(
            models.Filter(
                must=[
                    models.FieldCondition(
                        key="branch",
                        match=models.MatchValue(value=branch_id),
                    ),
                    models.FieldCondition(
                        key="seq",
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
                                key="seq",
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
        raise RuntimeError("Qdrant returned an unsupported vector")
    return [float(value) for value in vector]


def _branch_path_to_root(
    branch_id: str,
    registry: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    path = []
    current: str | None = branch_id
    while current is not None:
        path.append(current)
        parent = registry[current]["parent_id"]
        current = str(parent) if parent is not None else None
    return path


def _optional_document_digest(indexed: IndexedDocument | None) -> str | None:
    return indexed_document_digest(indexed) if indexed is not None else None


def _optional_file(
    files: BtrfsWorkspaceStore,
    branch_id: str,
    path: str,
) -> bytes | None:
    try:
        return files.read(branch_id, path)
    except FileNotFoundError:
        return None


def _restore_file(
    files: BtrfsWorkspaceStore,
    branch_id: str,
    path: str,
    previous: bytes | None,
) -> None:
    if previous is None:
        files.delete(branch_id, path)
    else:
        files.write(branch_id, path, previous)


def _directory_bytes(path: Path | None) -> int:
    if path is None or not path.exists():
        return 0
    return sum(
        item.stat().st_blocks * 512
        for item in path.rglob("*")
        if item.is_file()
    )


def _component_storage_path(
    root: str | Path | None,
    relative: str,
) -> Path | None:
    if root is None:
        return None
    path = Path(root).expanduser().resolve()
    return path / relative


__all__ = [
    "DoltgresQdrantBtrfsKnowledgeBackend",
]
