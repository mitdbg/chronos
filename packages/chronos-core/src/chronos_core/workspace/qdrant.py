"""Interval-versioned Qdrant store for Chronos workspaces.

Qdrant owns vector search and vector payloads.  A small SQLite control database
uses Chronos's existing interval branch manager to allocate branches and plan
record-version splices.  The resulting physical intervals are attached to
Qdrant points, so reads need one Qdrant filter rather than an ancestry walk.

The public API deliberately stays branch oriented: applications create or
checkout branches and then upsert, delete, retrieve, or search points.  The
interval coordinates and recovery marker in this module are internal.
"""

from __future__ import annotations

import contextlib
import json
import os
import threading
import uuid
import warnings
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chronos_core.branching import (
    BranchDiff,
    BranchingError,
    ChronosBranchContext,
    MergePreview,
    MergeResolution,
    MergeResult,
    RowDiff,
)
from chronos_core.branching._common import (
    MergePolicyInput,
    _normalize_merge_policy,
    _preview_with_merge_policy,
    _resolve_merge_changes,
)

try:
    from qdrant_client import QdrantClient, models
except ImportError as exc:  # pragma: no cover - exercised by optional-dependency users
    QdrantClient = Any  # type: ignore[assignment,misc]
    models = None  # type: ignore[assignment]
    _QDRANT_IMPORT_ERROR: ImportError | None = exc
else:
    _QDRANT_IMPORT_ERROR = None


def _positive_int_environment(name: str) -> int | None:
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


def _nonnegative_int_environment(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a non-negative integer") from exc
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


_POINTS_TABLE = "chronos_qdrant_points"
_COLLECTIONS_TABLE = "chronos_qdrant_collections"
_STATE_TABLE = "chronos_qdrant_state"
_STATE_ROW = "default"
_PAYLOAD_PREFIX = "_chronos_"
_PAYLOAD_USER = f"{_PAYLOAD_PREFIX}payload"
_PAYLOAD_LOGICAL_ID = f"{_PAYLOAD_PREFIX}logical_id"
_PAYLOAD_REVISION = f"{_PAYLOAD_PREFIX}revision"
_PAYLOAD_LOW = f"{_PAYLOAD_PREFIX}low"
_PAYLOAD_HIGH = f"{_PAYLOAD_PREFIX}high"
_PAYLOAD_LOW_HI = f"{_PAYLOAD_PREFIX}low_hi"
_PAYLOAD_LOW_LO = f"{_PAYLOAD_PREFIX}low_lo"
_PAYLOAD_HIGH_HI = f"{_PAYLOAD_PREFIX}high_hi"
_PAYLOAD_HIGH_LO = f"{_PAYLOAD_PREFIX}high_lo"
_PAYLOAD_WRITER = f"{_PAYLOAD_PREFIX}writer"
_PAYLOAD_DELETED = f"{_PAYLOAD_PREFIX}deleted"
_PAYLOAD_ACTIVE = f"{_PAYLOAD_PREFIX}active"

# Qdrant range predicates are represented as doubles.  Splitting Chronos's
# signed-64-bit non-negative interval coordinates into base-2^31 digits makes
# every compared value exactly representable while preserving integer order.
_INTERVAL_DIGIT_BASE = 1 << 31


class QdrantStoreError(BranchingError):
    """Raised when a Qdrant workspace operation cannot be completed safely."""


@dataclass(frozen=True)
class QdrantCollectionInfo:
    name: str
    physical_name: str
    dimensions: int
    distance: str
    dense_vector_name: str | None = None
    sparse_vector_names: tuple[str, ...] = ()
    user_text_indexes: tuple[str, ...] = ()
    on_disk: bool = False


@dataclass(frozen=True)
class QdrantPoint:
    id: str
    vector: Any
    payload: dict[str, Any]


@dataclass(frozen=True)
class QdrantUpsert:
    id: str
    vector: Any
    payload: Mapping[str, Any] | None = None
    revision: str | None = None


@dataclass(frozen=True)
class QdrantSearchResult:
    id: str
    score: float
    vector: Any
    payload: dict[str, Any]


def _require_qdrant() -> None:
    if _QDRANT_IMPORT_ERROR is not None:
        raise QdrantStoreError(
            "Qdrant support requires the 'qdrant' extra: "
            "pip install 'chronos-core[qdrant]'"
        ) from _QDRANT_IMPORT_ERROR


def _split_interval_coordinate(value: int) -> tuple[int, int]:
    number = int(value)
    if number < 0:
        raise QdrantStoreError("Chronos Qdrant intervals must be non-negative")
    return divmod(number, _INTERVAL_DIGIT_BASE)


def _point_id(
    physical_collection: str,
    logical_id: str,
    revision: str,
    low: int,
    high: int,
    deleted: bool,
) -> str:
    material = json.dumps(
        [physical_collection, logical_id, revision, int(low), int(high), bool(deleted)],
        separators=(",", ":"),
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"chronos-qdrant:{material}"))


def _coerce_vector(vector: Any) -> Any:
    if isinstance(vector, Mapping):
        result: dict[str, Any] = {}
        for name, value in vector.items():
            if isinstance(value, models.SparseVector):
                result[str(name)] = models.SparseVector(
                    indices=[int(index) for index in value.indices],
                    values=[float(item) for item in value.values],
                )
            elif isinstance(value, Mapping) and {
                "indices",
                "values",
            }.issubset(value):
                result[str(name)] = models.SparseVector(
                    indices=[int(index) for index in value["indices"]],
                    values=[float(item) for item in value["values"]],
                )
            elif isinstance(value, Sequence) and not isinstance(
                value,
                (str, bytes, bytearray),
            ):
                result[str(name)] = [float(item) for item in value]
            else:
                raise QdrantStoreError(
                    f"Qdrant returned an unsupported named vector {name!r}"
                )
        return result
    if not isinstance(vector, Sequence) or isinstance(vector, (str, bytes, bytearray)):
        raise QdrantStoreError("Qdrant returned an unsupported vector representation")
    return [float(value) for value in vector]


class ChronosQdrantStore:
    """A Qdrant vector store that participates in a Chronos workspace.

    ``metadata_url`` must currently be a SQLite URL.  Qdrant can be local
    (including embedded ``:memory:`` mode) or remote; callers may pass an
    already configured client for tests and advanced deployments.
    """

    def __init__(
        self,
        metadata_url: str,
        *,
        client: Any,
        context: ChronosBranchContext | None = None,
        collection_prefix: str = "chronos_",
        owns_client: bool = False,
        ensure_metadata: bool = True,
    ):
        _require_qdrant()
        if not metadata_url.startswith("sqlite://"):
            raise ValueError("ChronosQdrantStore metadata_url must use SQLite")
        self.metadata_url = metadata_url
        self.client = client
        self.collection_prefix = collection_prefix
        self._owns_client = owns_client
        self._lock = threading.RLock()
        self._owns_context = context is None
        self.context = context or ChronosBranchContext.connect(
            metadata_url,
            backend="interval",
            ensure_metadata=ensure_metadata,
        )
        self._physical_points_table: str | None = None
        self._ensure_control_schema()
        if self._is_dirty():
            self.reconcile()

    @classmethod
    def local(
        cls,
        metadata_url: str,
        *,
        path: str | Path = ":memory:",
        collection_prefix: str = "chronos_",
        context: ChronosBranchContext | None = None,
    ) -> ChronosQdrantStore:
        _require_qdrant()
        local_path = str(path)
        client = (
            QdrantClient(":memory:")
            if local_path == ":memory:"
            else QdrantClient(path=local_path)
        )
        return cls(
            metadata_url,
            client=client,
            context=context,
            collection_prefix=collection_prefix,
            owns_client=True,
        )

    @classmethod
    def remote(
        cls,
        metadata_url: str,
        *,
        url: str,
        api_key: str | None = None,
        collection_prefix: str = "chronos_",
        prefer_grpc: bool = False,
        timeout: float | None = None,
        context: ChronosBranchContext | None = None,
    ) -> ChronosQdrantStore:
        _require_qdrant()
        grpc_port = _positive_int_environment("CHRONOS_QDRANT_GRPC_PORT")
        use_grpc = prefer_grpc or _boolean_environment("CHRONOS_QDRANT_PREFER_GRPC")
        client = QdrantClient(
            url=url,
            api_key=api_key,
            grpc_port=grpc_port,
            prefer_grpc=use_grpc,
            timeout=timeout,
        )
        return cls(
            metadata_url,
            client=client,
            context=context,
            collection_prefix=collection_prefix,
            owns_client=True,
        )

    def _ensure_control_schema(self) -> None:
        db = self.context.db
        db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_COLLECTIONS_TABLE} (
                name TEXT PRIMARY KEY,
                physical_name TEXT NOT NULL UNIQUE,
                dimensions INTEGER NOT NULL,
                distance TEXT NOT NULL,
                config_json TEXT NOT NULL DEFAULT '{{}}'
            )
            """
        )
        collection_columns = {
            str(row["name"])
            for row in db.execute(f"PRAGMA table_info({_COLLECTIONS_TABLE})")
        }
        if "config_json" not in collection_columns:
            db.execute(
                f"""
                ALTER TABLE {_COLLECTIONS_TABLE}
                ADD COLUMN config_json TEXT NOT NULL DEFAULT '{{}}'
                """
            )
        db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_STATE_TABLE} (
                state_key TEXT PRIMARY KEY,
                dirty INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        db.execute(
            f"""
            INSERT OR IGNORE INTO {_STATE_TABLE}(state_key, dirty)
            VALUES (?, 0)
            """,
            (_STATE_ROW,),
        )
        registry = db.execute(
            """
            SELECT physical_table
            FROM _chronos_branch_tables
            WHERE table_name = ?
            """,
            (_POINTS_TABLE,),
        ).fetchone()
        if registry is None:
            db.execute(
                f"""
                CREATE TABLE {_POINTS_TABLE} (
                    collection_name TEXT NOT NULL,
                    point_id TEXT NOT NULL,
                    revision TEXT NOT NULL,
                    PRIMARY KEY(collection_name, point_id)
                )
                """
            )
            db.commit()
            self.context.register_table(
                _POINTS_TABLE,
                ["collection_name", "point_id"],
            )
        else:
            db.commit()
            self._physical_points_table = str(registry["physical_table"])

    def _physical_table(self) -> str:
        if self._physical_points_table is None:
            row = self.context.db.execute(
                """
                SELECT physical_table
                FROM _chronos_branch_tables
                WHERE table_name = ?
                """,
                (_POINTS_TABLE,),
            ).fetchone()
            if row is None:
                raise QdrantStoreError("Qdrant interval table is not registered")
            self._physical_points_table = str(row["physical_table"])
        return self._physical_points_table

    def register_collection(
        self,
        name: str,
        dimensions: int,
        *,
        distance: str = "cosine",
        dense_vector_name: str | None = None,
        sparse_vector_names: Sequence[str] = (),
        text_indexes: Sequence[str] = (),
        on_disk: bool = False,
    ) -> QdrantCollectionInfo:
        _require_qdrant()
        logical_name = str(name).strip()
        if not logical_name:
            raise ValueError("collection name must not be empty")
        if int(dimensions) <= 0:
            raise ValueError("collection dimensions must be positive")
        normalized_distance = distance.strip().lower()
        distance_values = {
            "cosine": models.Distance.COSINE,
            "dot": models.Distance.DOT,
            "euclid": models.Distance.EUCLID,
            "manhattan": models.Distance.MANHATTAN,
        }
        if normalized_distance not in distance_values:
            raise ValueError("distance must be one of: cosine, dot, euclid, manhattan")
        normalized_dense_name = (
            str(dense_vector_name).strip() if dense_vector_name is not None else None
        )
        if normalized_dense_name == "":
            raise ValueError("dense vector name must not be empty")
        normalized_sparse_names = tuple(
            dict.fromkeys(str(value).strip() for value in sparse_vector_names)
        )
        if any(not value for value in normalized_sparse_names):
            raise ValueError("sparse vector names must not be empty")
        normalized_text_indexes = tuple(
            dict.fromkeys(str(value).strip() for value in text_indexes)
        )
        if any(not value for value in normalized_text_indexes):
            raise ValueError("text index names must not be empty")
        if normalized_sparse_names and normalized_dense_name is None:
            raise ValueError("named sparse vectors require a named dense vector")
        collection_config = {
            "dense_vector_name": normalized_dense_name,
            "sparse_vector_names": list(normalized_sparse_names),
            "text_indexes": list(normalized_text_indexes),
            "on_disk": bool(on_disk),
        }
        physical_name = f"{self.collection_prefix}{logical_name}"
        max_optimization_threads = _positive_int_environment(
            "CHRONOS_QDRANT_MAX_OPTIMIZATION_THREADS"
        )
        max_indexing_threads = _positive_int_environment(
            "CHRONOS_QDRANT_MAX_INDEXING_THREADS"
        )
        hnsw_m = _nonnegative_int_environment("CHRONOS_QDRANT_HNSW_M")
        indexing_threshold = _nonnegative_int_environment(
            "CHRONOS_QDRANT_INDEXING_THRESHOLD_KB"
        )
        shard_number = _positive_int_environment("CHRONOS_QDRANT_SHARD_NUMBER")
        hnsw_config = (
            models.HnswConfigDiff(
                m=hnsw_m,
                on_disk=True if on_disk else None,
                max_indexing_threads=max_indexing_threads,
            )
            if (on_disk or max_indexing_threads is not None or hnsw_m is not None)
            else None
        )
        optimizers_config = (
            models.OptimizersConfigDiff(
                indexing_threshold=indexing_threshold,
                max_optimization_threads=max_optimization_threads,
            )
            if (indexing_threshold is not None or max_optimization_threads is not None)
            else None
        )
        with self._lock:
            existing = self._collection_info_or_none(logical_name)
            if existing is not None:
                if (
                    existing.dimensions != int(dimensions)
                    or existing.distance != normalized_distance
                    or existing.dense_vector_name != normalized_dense_name
                    or existing.sparse_vector_names != normalized_sparse_names
                    or existing.user_text_indexes != normalized_text_indexes
                    or existing.on_disk != bool(on_disk)
                ):
                    raise QdrantStoreError(
                        f"collection {logical_name!r} already has "
                        f"{existing.dimensions} dimensions and "
                        f"{existing.distance} distance"
                    )
                if hnsw_config is not None or optimizers_config is not None:
                    self.client.update_collection(
                        collection_name=physical_name,
                        hnsw_config=hnsw_config,
                        optimizers_config=optimizers_config,
                    )
                return existing
            if not self.client.collection_exists(physical_name):
                vector_params = models.VectorParams(
                    size=int(dimensions),
                    distance=distance_values[normalized_distance],
                    on_disk=bool(on_disk),
                )
                self.client.create_collection(
                    collection_name=physical_name,
                    vectors_config=(
                        {normalized_dense_name: vector_params}
                        if normalized_dense_name is not None
                        else vector_params
                    ),
                    sparse_vectors_config={
                        sparse_name: models.SparseVectorParams(
                            modifier=models.Modifier.IDF,
                            index=models.SparseIndexParams(on_disk=bool(on_disk)),
                        )
                        for sparse_name in normalized_sparse_names
                    }
                    or None,
                    on_disk_payload=bool(on_disk),
                    shard_number=shard_number,
                    hnsw_config=hnsw_config,
                    optimizers_config=optimizers_config,
                )
            self.context.db.execute(
                f"""
                INSERT INTO {_COLLECTIONS_TABLE}
                    (name, physical_name, dimensions, distance, config_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    logical_name,
                    physical_name,
                    int(dimensions),
                    normalized_distance,
                    json.dumps(collection_config, sort_keys=True),
                ),
            )
            self.context.db.commit()
            self._create_payload_indexes(
                physical_name,
                text_indexes=normalized_text_indexes,
            )
            return QdrantCollectionInfo(
                logical_name,
                physical_name,
                int(dimensions),
                normalized_distance,
                normalized_dense_name,
                normalized_sparse_names,
                normalized_text_indexes,
                bool(on_disk),
            )

    def _create_payload_indexes(
        self,
        physical_name: str,
        *,
        text_indexes: Sequence[str] = (),
    ) -> None:
        field_types = {
            _PAYLOAD_LOGICAL_ID: models.PayloadSchemaType.KEYWORD,
            _PAYLOAD_LOW_HI: models.PayloadSchemaType.INTEGER,
            _PAYLOAD_LOW_LO: models.PayloadSchemaType.INTEGER,
            _PAYLOAD_HIGH_HI: models.PayloadSchemaType.INTEGER,
            _PAYLOAD_HIGH_LO: models.PayloadSchemaType.INTEGER,
            _PAYLOAD_DELETED: models.PayloadSchemaType.BOOL,
            _PAYLOAD_ACTIVE: models.PayloadSchemaType.BOOL,
        }
        for field_name, field_schema in field_types.items():
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        message="Payload indexes have no effect in the local Qdrant.*",
                    )
                    self.client.create_payload_index(
                        collection_name=physical_name,
                        field_name=field_name,
                        field_schema=field_schema,
                        wait=True,
                    )
            except (NotImplementedError, ValueError):
                # Embedded local mode may not implement payload indexes.  Its
                # scan semantics remain correct and are sufficient for tests.
                continue
        for user_field in text_indexes:
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        message="Payload indexes have no effect in the local Qdrant.*",
                    )
                    self.client.create_payload_index(
                        collection_name=physical_name,
                        field_name=f"{_PAYLOAD_USER}.{user_field}",
                        field_schema=models.TextIndexParams(
                            type=models.TextIndexType.TEXT,
                            tokenizer=models.TokenizerType.WORD,
                            min_token_len=1,
                            max_token_len=80,
                            lowercase=True,
                            ascii_folding=True,
                            phrase_matching=True,
                            on_disk=True,
                        ),
                        wait=True,
                    )
            except (NotImplementedError, ValueError):
                continue

    def list_collections(self) -> list[QdrantCollectionInfo]:
        rows = self.context.db.execute(
            f"""
            SELECT name, physical_name, dimensions, distance, config_json
            FROM {_COLLECTIONS_TABLE}
            ORDER BY name
            """
        ).fetchall()
        return [self._collection_info_from_row(row) for row in rows]

    @staticmethod
    def _collection_info_from_row(row: Any) -> QdrantCollectionInfo:
        config = json.loads(str(row["config_json"] or "{}"))
        return QdrantCollectionInfo(
            str(row["name"]),
            str(row["physical_name"]),
            int(row["dimensions"]),
            str(row["distance"]),
            (
                str(config["dense_vector_name"])
                if config.get("dense_vector_name") is not None
                else None
            ),
            tuple(str(value) for value in config.get("sparse_vector_names", [])),
            tuple(str(value) for value in config.get("text_indexes", [])),
            bool(config.get("on_disk", False)),
        )

    def _collection_info_or_none(self, name: str) -> QdrantCollectionInfo | None:
        row = self.context.db.execute(
            f"""
            SELECT name, physical_name, dimensions, distance, config_json
            FROM {_COLLECTIONS_TABLE}
            WHERE name = ?
            """,
            (name,),
        ).fetchone()
        if row is None:
            return None
        return self._collection_info_from_row(row)

    def collection_info(self, name: str) -> QdrantCollectionInfo:
        info = self._collection_info_or_none(name)
        if info is None:
            raise QdrantStoreError(f"Qdrant collection is not registered: {name}")
        return info

    def create_branch(
        self,
        branch_id: str,
        from_branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.context.create_branch(
            branch_id,
            from_branch=from_branch,
            metadata=metadata,
        )

    def create_branch_from_checkpoint(
        self,
        branch_id: str,
        checkpoint: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.context.create_branch_from_checkpoint(branch_id, checkpoint)
        if metadata:
            self.context.update_branch_metadata(branch_id, metadata)

    def delete_branch(self, branch_id: str) -> None:
        self.context.delete_branch(branch_id)

    def checkout(self, branch_id: str = "main") -> QdrantBranchSession:
        return QdrantBranchSession(self, self.context.checkout(branch_id))

    def checkout_checkpoint(self, checkpoint: str) -> QdrantBranchSession:
        return QdrantBranchSession(
            self,
            self.context.checkout_checkpoint(checkpoint),
        )

    def create_checkpoint(
        self,
        checkpoint: str,
        branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        return self.context.create_checkpoint(
            checkpoint,
            branch=branch,
            metadata=metadata,
        )

    def _is_dirty(self) -> bool:
        row = self.context.db.execute(
            f"SELECT dirty FROM {_STATE_TABLE} WHERE state_key = ?",
            (_STATE_ROW,),
        ).fetchone()
        return bool(row and row["dirty"])

    def _set_dirty(self, dirty: bool) -> None:
        self.context.db.execute(
            f"UPDATE {_STATE_TABLE} SET dirty = ? WHERE state_key = ?",
            (1 if dirty else 0, _STATE_ROW),
        )
        self.context.db.commit()

    def _assert_clean(self) -> None:
        if self._is_dirty():
            raise QdrantStoreError(
                "Qdrant branch index is recovering from an interrupted write"
            )

    def _physical_versions(
        self,
        collection: str,
        point_id: str,
    ) -> list[dict[str, Any]]:
        table = self._physical_table()
        rows = self.context.db.execute(
            f"""
            SELECT collection_name, point_id, revision,
                   live_lo, live_hi, writer_segment_id, deleted
            FROM {table}
            WHERE collection_name = ? AND point_id = ?
            ORDER BY live_lo, live_hi, revision
            """,
            (collection, point_id),
        ).fetchall()
        return [dict(row) for row in rows]

    def _all_physical_keys(self) -> list[tuple[str, str]]:
        table = self._physical_table()
        rows = self.context.db.execute(
            f"""
            SELECT DISTINCT collection_name, point_id
            FROM {table}
            ORDER BY collection_name, point_id
            """
        ).fetchall()
        return [(str(row["collection_name"]), str(row["point_id"])) for row in rows]

    def _iter_physical_key_batches(
        self,
        *,
        batch_size: int = 256,
    ) -> Iterator[tuple[str, list[str]]]:
        """Yield catalog keys without materializing the full vector corpus."""

        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        table = self._physical_table()
        collection_rows = self.context.db.execute(
            f"""
            SELECT DISTINCT collection_name
            FROM {table}
            ORDER BY collection_name
            """
        ).fetchall()
        for collection_row in collection_rows:
            collection = str(collection_row["collection_name"])
            after: str | None = None
            while True:
                if after is None:
                    rows = self.context.db.execute(
                        f"""
                        SELECT DISTINCT point_id
                        FROM {table}
                        WHERE collection_name = ?
                        ORDER BY point_id
                        LIMIT ?
                        """,
                        (collection, batch_size),
                    ).fetchall()
                else:
                    rows = self.context.db.execute(
                        f"""
                        SELECT DISTINCT point_id
                        FROM {table}
                        WHERE collection_name = ? AND point_id > ?
                        ORDER BY point_id
                        LIMIT ?
                        """,
                        (collection, after, batch_size),
                    ).fetchall()
                point_ids = [str(row["point_id"]) for row in rows]
                if not point_ids:
                    break
                yield collection, point_ids
                after = point_ids[-1]

    def _logical_filter(self, point_id: str) -> Any:
        return models.Filter(
            must=[
                models.FieldCondition(
                    key=_PAYLOAD_LOGICAL_ID,
                    match=models.MatchValue(value=point_id),
                )
            ]
        )

    def _logical_ids_filter(self, point_ids: Sequence[str]) -> Any:
        return models.Filter(
            must=[
                models.FieldCondition(
                    key=_PAYLOAD_LOGICAL_ID,
                    match=models.MatchAny(any=list(point_ids)),
                )
            ]
        )

    def _scroll_all(
        self,
        physical_name: str,
        *,
        scroll_filter: Any | None = None,
    ) -> list[Any]:
        records: list[Any] = []
        offset: Any | None = None
        while True:
            page, offset = self.client.scroll(
                collection_name=physical_name,
                scroll_filter=scroll_filter,
                limit=256,
                offset=offset,
                with_payload=True,
                with_vectors=True,
            )
            records.extend(page)
            if offset is None:
                return records

    @staticmethod
    def _revision_data(
        records: Sequence[Any],
    ) -> dict[str, tuple[Any, dict[str, Any]]]:
        result: dict[str, tuple[Any, dict[str, Any]]] = {}
        for record in records:
            payload = dict(record.payload or {})
            revision = payload.get(_PAYLOAD_REVISION)
            if revision is None or record.vector is None:
                continue
            user_payload = payload.get(_PAYLOAD_USER) or {}
            result[str(revision)] = (
                _coerce_vector(record.vector),
                dict(user_payload),
            )
        return result

    def _reconcile_key(
        self,
        collection: str,
        point_id: str,
        *,
        revision_data: Mapping[str, tuple[Any, dict[str, Any]]] | None = None,
    ) -> None:
        info = self.collection_info(collection)
        existing = self._scroll_all(
            info.physical_name,
            scroll_filter=self._logical_filter(point_id),
        )
        available = self._revision_data(existing)
        available.update(revision_data or {})
        desired: list[Any] = []
        desired_ids: set[str] = set()
        for row in self._physical_versions(collection, point_id):
            revision = str(row["revision"])
            version_data = available.get(revision)
            if version_data is None:
                raise QdrantStoreError(
                    f"cannot recover vector revision {revision!r} for "
                    f"{collection}/{point_id}"
                )
            vector, user_payload = version_data
            low = int(row["live_lo"])
            high = int(row["live_hi"])
            low_hi, low_lo = _split_interval_coordinate(low)
            high_hi, high_lo = _split_interval_coordinate(high)
            deleted = bool(row["deleted"])
            physical_id = _point_id(
                info.physical_name,
                point_id,
                revision,
                low,
                high,
                deleted,
            )
            desired_ids.add(physical_id)
            desired.append(
                models.PointStruct(
                    id=physical_id,
                    vector=vector,
                    payload={
                        _PAYLOAD_USER: user_payload,
                        _PAYLOAD_LOGICAL_ID: point_id,
                        _PAYLOAD_REVISION: revision,
                        _PAYLOAD_LOW: str(low),
                        _PAYLOAD_HIGH: str(high),
                        _PAYLOAD_LOW_HI: low_hi,
                        _PAYLOAD_LOW_LO: low_lo,
                        _PAYLOAD_HIGH_HI: high_hi,
                        _PAYLOAD_HIGH_LO: high_lo,
                        _PAYLOAD_WRITER: int(row["writer_segment_id"]),
                        _PAYLOAD_DELETED: deleted,
                        _PAYLOAD_ACTIVE: True,
                    },
                )
            )
        stale_ids = [
            record.id for record in existing if str(record.id) not in desired_ids
        ]
        if stale_ids:
            self.client.set_payload(
                collection_name=info.physical_name,
                payload={_PAYLOAD_ACTIVE: False},
                points=stale_ids,
                wait=True,
            )
        if desired:
            self.client.upsert(
                collection_name=info.physical_name,
                points=desired,
                wait=True,
            )

    def _physical_versions_many(
        self,
        collection: str,
        point_ids: Sequence[str],
    ) -> list[dict[str, Any]]:
        if not point_ids:
            return []
        table = self._physical_table()
        result: list[dict[str, Any]] = []
        for start in range(0, len(point_ids), 500):
            batch = list(point_ids[start : start + 500])
            placeholders = ",".join("?" for _ in batch)
            rows = self.context.db.execute(
                f"""
                SELECT collection_name, point_id, revision,
                       live_lo, live_hi, writer_segment_id, deleted
                FROM {table}
                WHERE collection_name = ?
                  AND point_id IN ({placeholders})
                ORDER BY point_id, live_lo, live_hi, revision
                """,
                (collection, *batch),
            ).fetchall()
            result.extend(dict(row) for row in rows)
        return result

    def _reconcile_keys(
        self,
        collection: str,
        point_ids: Sequence[str],
        *,
        revision_data: Mapping[
            tuple[str, str],
            tuple[Any, dict[str, Any]],
        ]
        | None = None,
        new_points: bool = False,
    ) -> None:
        logical_ids = sorted({str(point_id) for point_id in point_ids})
        if not logical_ids:
            return
        info = self.collection_info(collection)
        existing: list[Any] = []
        if not new_points:
            for start in range(0, len(logical_ids), 256):
                existing.extend(
                    self._scroll_all(
                        info.physical_name,
                        scroll_filter=self._logical_ids_filter(
                            logical_ids[start : start + 256]
                        ),
                    )
                )
        available: dict[
            tuple[str, str],
            tuple[Any, dict[str, Any]],
        ] = {}
        for record in existing:
            payload = dict(record.payload or {})
            logical_id = payload.get(_PAYLOAD_LOGICAL_ID)
            revision = payload.get(_PAYLOAD_REVISION)
            if logical_id is None or revision is None or record.vector is None:
                continue
            available[(str(logical_id), str(revision))] = (
                _coerce_vector(record.vector),
                dict(payload.get(_PAYLOAD_USER) or {}),
            )
        available.update(revision_data or {})

        desired: list[Any] = []
        desired_ids: set[str] = set()
        for row in self._physical_versions_many(collection, logical_ids):
            logical_id = str(row["point_id"])
            revision = str(row["revision"])
            version_data = available.get((logical_id, revision))
            if version_data is None:
                raise QdrantStoreError(
                    f"cannot recover vector revision {revision!r} for "
                    f"{collection}/{logical_id}"
                )
            vector, user_payload = version_data
            low = int(row["live_lo"])
            high = int(row["live_hi"])
            low_hi, low_lo = _split_interval_coordinate(low)
            high_hi, high_lo = _split_interval_coordinate(high)
            deleted = bool(row["deleted"])
            physical_id = _point_id(
                info.physical_name,
                logical_id,
                revision,
                low,
                high,
                deleted,
            )
            desired_ids.add(physical_id)
            desired.append(
                models.PointStruct(
                    id=physical_id,
                    vector=vector,
                    payload={
                        _PAYLOAD_USER: user_payload,
                        _PAYLOAD_LOGICAL_ID: logical_id,
                        _PAYLOAD_REVISION: revision,
                        _PAYLOAD_LOW: str(low),
                        _PAYLOAD_HIGH: str(high),
                        _PAYLOAD_LOW_HI: low_hi,
                        _PAYLOAD_LOW_LO: low_lo,
                        _PAYLOAD_HIGH_HI: high_hi,
                        _PAYLOAD_HIGH_LO: high_lo,
                        _PAYLOAD_WRITER: int(row["writer_segment_id"]),
                        _PAYLOAD_DELETED: deleted,
                        _PAYLOAD_ACTIVE: True,
                    },
                )
            )
        stale_ids = [
            record.id for record in existing if str(record.id) not in desired_ids
        ]
        for start in range(0, len(stale_ids), 512):
            self.client.set_payload(
                collection_name=info.physical_name,
                payload={_PAYLOAD_ACTIVE: False},
                points=stale_ids[start : start + 512],
                wait=True,
            )
        point_batch_size = (
            _positive_int_environment("CHRONOS_QDRANT_POINT_BATCH_SIZE") or 256
        )
        batches = [
            desired[start : start + point_batch_size]
            for start in range(0, len(desired), point_batch_size)
        ]
        workers = min(
            _positive_int_environment("CHRONOS_QDRANT_UPLOAD_WORKERS") or 1,
            len(batches),
        )
        if workers <= 1:
            for batch in batches:
                self.client.upsert(
                    collection_name=info.physical_name,
                    points=batch,
                    wait=True,
                )
        else:

            def upload(batch: Sequence[Any]) -> None:
                self.client.upsert(
                    collection_name=info.physical_name,
                    points=batch,
                    wait=True,
                )

            with ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix="chronos-qdrant-upload",
            ) as executor:
                list(executor.map(upload, batches))

    def reconcile(self) -> None:
        """Repair Qdrant payload intervals after an interrupted mutation."""

        with self._lock:
            for info in self.list_collections():
                # Invalidate the collection server-side. This avoids loading
                # every payload and sparse vector into the Python process.
                self.client.set_payload(
                    collection_name=info.physical_name,
                    payload={_PAYLOAD_ACTIVE: False},
                    points=models.Filter(),
                    wait=True,
                )
            for collection, point_ids in self._iter_physical_key_batches():
                self._reconcile_keys(
                    collection,
                    point_ids,
                )
            self._set_dirty(False)

    def _visible_filter(
        self,
        branch_point: int,
        point_id: str | None = None,
        point_ids: Sequence[str] | None = None,
    ) -> Any:
        if point_id is not None and point_ids is not None:
            raise ValueError("provide either point_id or point_ids, not both")
        point_hi, point_lo = _split_interval_coordinate(branch_point)
        low_condition = models.Filter(
            should=[
                models.FieldCondition(
                    key=_PAYLOAD_LOW_HI,
                    range=models.Range(lt=point_hi),
                ),
                models.Filter(
                    must=[
                        models.FieldCondition(
                            key=_PAYLOAD_LOW_HI,
                            match=models.MatchValue(value=point_hi),
                        ),
                        models.FieldCondition(
                            key=_PAYLOAD_LOW_LO,
                            range=models.Range(lte=point_lo),
                        ),
                    ]
                ),
            ]
        )
        high_condition = models.Filter(
            should=[
                models.FieldCondition(
                    key=_PAYLOAD_HIGH_HI,
                    range=models.Range(gt=point_hi),
                ),
                models.Filter(
                    must=[
                        models.FieldCondition(
                            key=_PAYLOAD_HIGH_HI,
                            match=models.MatchValue(value=point_hi),
                        ),
                        models.FieldCondition(
                            key=_PAYLOAD_HIGH_LO,
                            range=models.Range(gt=point_lo),
                        ),
                    ]
                ),
            ]
        )
        must: list[Any] = [
            models.FieldCondition(
                key=_PAYLOAD_ACTIVE,
                match=models.MatchValue(value=True),
            ),
            models.FieldCondition(
                key=_PAYLOAD_DELETED,
                match=models.MatchValue(value=False),
            ),
            low_condition,
            high_condition,
        ]
        if point_id is not None:
            must.append(
                models.FieldCondition(
                    key=_PAYLOAD_LOGICAL_ID,
                    match=models.MatchValue(value=point_id),
                )
            )
        if point_ids is not None:
            must.append(
                models.FieldCondition(
                    key=_PAYLOAD_LOGICAL_ID,
                    match=models.MatchAny(any=list(point_ids)),
                )
            )
        return models.Filter(must=must)

    def _visible_points(
        self,
        collection: str,
        branch_point: int,
    ) -> list[QdrantPoint]:
        info = self.collection_info(collection)
        records = self._scroll_all(
            info.physical_name,
            scroll_filter=self._visible_filter(branch_point),
        )
        return [
            QdrantPoint(
                id=str(record.payload[_PAYLOAD_LOGICAL_ID]),
                vector=_coerce_vector(record.vector),
                payload=dict(record.payload.get(_PAYLOAD_USER) or {}),
            )
            for record in records
        ]

    def diff(self, left: str, right: str) -> BranchDiff:
        left_session = self.checkout(left)
        right_session = self.checkout(right)
        changes: list[RowDiff] = []
        for info in self.list_collections():
            left_points = {
                point.id: point for point in left_session.list_points(info.name)
            }
            right_points = {
                point.id: point for point in right_session.list_points(info.name)
            }
            for point_id in sorted(set(left_points) | set(right_points)):
                before_point = left_points.get(point_id)
                after_point = right_points.get(point_id)
                before = _point_as_row(before_point) if before_point else None
                after = _point_as_row(after_point) if after_point else None
                if before is None:
                    change = "added"
                elif after is None:
                    change = "deleted"
                elif before != after:
                    change = "modified"
                else:
                    continue
                changes.append(
                    RowDiff(
                        table=info.name,
                        key={"id": point_id},
                        change=change,  # type: ignore[arg-type]
                        before=before,
                        after=after,
                    )
                )
        return BranchDiff(left=left, right=right, changes=changes)

    def merge_preview(
        self,
        source: str,
        target: str,
        *,
        policy: MergePolicyInput = None,
    ) -> MergePreview:
        shadow = self.context.merge_preview_tables(
            source,
            target,
            [_POINTS_TABLE],
        )
        source_session = self.checkout(source)
        target_session = self.checkout(target)
        source_points, target_points = self._merge_points(
            (*shadow.changes, *shadow.conflicts),
            source_session,
            target_session,
        )
        changes = [
            self._translate_shadow_change(
                change,
                source_points,
                target_points,
            )
            for change in shadow.changes
        ]
        conflicts = [
            self._translate_shadow_change(
                change,
                source_points,
                target_points,
            )
            for change in shadow.conflicts
        ]
        return _preview_with_merge_policy(
            MergePreview(
                source=source,
                target=target,
                changes=changes,
                conflicts=conflicts,
                resolution=shadow.resolution,
            ),
            policy,
            backend="qdrant",
        )

    def stage_branch_transaction_changes(
        self,
        transaction: Any,
        source: str,
        target: str,
        changes: list[RowDiff],
    ) -> int:
        """Stage selected shadow rows and their Qdrant interval payloads."""

        selected = {(change.table, str(change.key["id"])) for change in changes}
        shadow = self.context.merge_preview_tables(
            source,
            target,
            [_POINTS_TABLE],
        )
        selected_shadow: list[RowDiff] = []
        keys: dict[str, set[str]] = {}
        for change in (*shadow.changes, *shadow.conflicts):
            row = change.after or change.before
            if row is None:
                continue
            collection = str(row["collection_name"])
            point_id = str(row["point_id"])
            if (collection, point_id) not in selected:
                continue
            selected_shadow.append(change)
            keys.setdefault(collection, set()).add(point_id)
        applied = self.context.stage_branch_transaction_changes(
            transaction,
            selected_shadow,
        )
        for collection, point_ids in keys.items():
            self._reconcile_keys(collection, sorted(point_ids))
        return applied

    @staticmethod
    def _merge_points(
        changes: Sequence[RowDiff],
        source_session: QdrantBranchSession,
        target_session: QdrantBranchSession,
    ) -> tuple[
        dict[tuple[str, str], QdrantPoint],
        dict[tuple[str, str], QdrantPoint],
    ]:
        point_ids: dict[str, set[str]] = {}
        for change in changes:
            row = change.after or change.before
            if row is None:
                raise QdrantStoreError("Qdrant merge change is missing its logical key")
            point_ids.setdefault(str(row["collection_name"]), set()).add(
                str(row["point_id"])
            )

        source: dict[tuple[str, str], QdrantPoint] = {}
        target: dict[tuple[str, str], QdrantPoint] = {}
        for collection, identifiers in point_ids.items():
            source.update(
                ((collection, point_id), point)
                for point_id, point in source_session.get_many(
                    collection,
                    sorted(identifiers),
                ).items()
            )
            target.update(
                ((collection, point_id), point)
                for point_id, point in target_session.get_many(
                    collection,
                    sorted(identifiers),
                ).items()
            )
        return source, target

    @staticmethod
    def _translate_shadow_change(
        change: RowDiff,
        source_points: Mapping[tuple[str, str], QdrantPoint],
        target_points: Mapping[tuple[str, str], QdrantPoint],
    ) -> RowDiff:
        row = change.after or change.before
        if row is None:
            raise QdrantStoreError("Qdrant merge change is missing its logical key")
        collection = str(row["collection_name"])
        point_id = str(row["point_id"])
        source_point = source_points.get((collection, point_id))
        target_point = target_points.get((collection, point_id))
        before = _point_as_row(target_point) if target_point else None
        after = _point_as_row(source_point) if source_point else None
        if before is None:
            kind = "added"
        elif after is None:
            kind = "deleted"
        else:
            kind = "modified"
        return RowDiff(
            table=collection,
            key={"id": point_id},
            change=kind,  # type: ignore[arg-type]
            before=before,
            after=after,
            conflict_id=change.conflict_id,
        )

    def merge_apply(
        self,
        source: str,
        target: str,
        resolution: MergeResolution | None = None,
        *,
        policy: MergePolicyInput = None,
    ) -> MergeResult:
        normalized = _normalize_merge_policy(policy)
        preview = self.merge_preview(source, target)
        changes = _resolve_merge_changes(
            preview,
            normalized,
            resolution,
            backend="qdrant",
        )
        target_session = self.checkout(target)
        by_collection: dict[str, list[RowDiff]] = {}
        for change in changes:
            by_collection.setdefault(change.table, []).append(change)
        with target_session.transaction():
            for collection, collection_changes in by_collection.items():
                upserts = [
                    QdrantUpsert(
                        id=str(change.key["id"]),
                        vector=change.after["vector"],
                        payload=change.after["payload"],
                    )
                    for change in collection_changes
                    if change.after is not None
                ]
                deletes = [
                    str(change.key["id"])
                    for change in collection_changes
                    if change.after is None
                ]
                if upserts:
                    target_session.upsert_many(collection, upserts)
                if deletes:
                    target_session.delete_many(
                        collection,
                        deletes,
                    )
        return MergeResult(source=source, target=target, applied=len(changes))

    def close(self) -> None:
        if self._owns_context:
            self.context.close()
        if self._owns_client:
            close = getattr(self.client, "close", None)
            if callable(close):
                close()


class QdrantBranchSession:
    """Branch-bound vector operations used through ``workspace.qdrant``."""

    def __init__(self, store: ChronosQdrantStore, control_session: Any):
        self._store = store
        self._control = control_session
        self._transaction_depth = 0

    @property
    def branch_id(self) -> str:
        return self._control.branch_id

    @property
    def current_ref(self) -> str:
        return self._control.current_ref

    @property
    def _branch_point(self) -> int:
        self._control._ensure_fresh()
        segment = self._control._ref.metadata.get("segment")
        if segment is None:
            raise QdrantStoreError("Qdrant checkout is missing interval metadata")
        return int(segment.branch_point)

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        if self._transaction_depth:
            self._transaction_depth += 1
            try:
                yield
            finally:
                self._transaction_depth -= 1
            return
        with self._store._lock:
            self._store._set_dirty(True)
            self._transaction_depth = 1
            try:
                with self._control.transaction():
                    yield
            except Exception:
                self._transaction_depth = 0
                try:
                    self._store.reconcile()
                except Exception as repair_error:
                    raise QdrantStoreError(
                        "Qdrant mutation failed and automatic repair also failed"
                    ) from repair_error
                raise
            else:
                self._transaction_depth = 0
                self._store._set_dirty(False)

    def upsert(
        self,
        collection: str,
        point_id: str,
        vector: Sequence[float],
        payload: Mapping[str, Any] | None = None,
        *,
        revision: str | None = None,
    ) -> QdrantPoint:
        return self.upsert_many(
            collection,
            [
                QdrantUpsert(
                    id=str(point_id),
                    vector=vector,
                    payload=payload,
                    revision=revision,
                )
            ],
        )[0]

    def upsert_many(
        self,
        collection: str,
        points: Sequence[QdrantUpsert],
    ) -> list[QdrantPoint]:
        return self._write_many(
            collection,
            points,
            new_points=False,
        )

    def load_many(
        self,
        collection: str,
        points: Sequence[QdrantUpsert],
    ) -> list[QdrantPoint]:
        """Bulk-load points that do not yet exist in a benchmark state."""

        return self._write_many(
            collection,
            points,
            new_points=True,
        )

    def _write_many(
        self,
        collection: str,
        points: Sequence[QdrantUpsert],
        *,
        new_points: bool,
    ) -> list[QdrantPoint]:
        if not points:
            return []
        if self._transaction_depth == 0:
            with self.transaction():
                return self._write_many(
                    collection,
                    points,
                    new_points=new_points,
                )
        info = self._store.collection_info(collection)
        normalized: list[tuple[str, str, Any, dict[str, Any]]] = []
        seen: set[str] = set()
        for point in points:
            logical_id = str(point.id)
            if logical_id in seen:
                raise ValueError(f"duplicate point id in Qdrant batch: {logical_id}")
            seen.add(logical_id)
            values = _coerce_vector(point.vector)
            if info.dense_vector_name is None:
                if not isinstance(values, list) or len(values) != info.dimensions:
                    size = len(values) if isinstance(values, list) else "named"
                    raise ValueError(
                        f"collection {collection!r} expects {info.dimensions} "
                        f"dimensions, got {size} for {logical_id!r}"
                    )
            else:
                if not isinstance(values, Mapping):
                    raise ValueError(
                        f"collection {collection!r} requires named vectors"
                    )
                allowed = {
                    info.dense_vector_name,
                    *info.sparse_vector_names,
                }
                unknown = set(values) - allowed
                if unknown:
                    raise ValueError(
                        f"collection {collection!r} received unknown vectors: "
                        f"{sorted(unknown)}"
                    )
                dense = values.get(info.dense_vector_name)
                if dense is not None and (
                    not isinstance(dense, list) or len(dense) != info.dimensions
                ):
                    size = len(dense) if isinstance(dense, list) else "sparse"
                    raise ValueError(
                        f"collection {collection!r} expects dense vector "
                        f"{info.dense_vector_name!r} with {info.dimensions} "
                        f"dimensions, got {size}"
                    )
                for sparse_name in info.sparse_vector_names:
                    sparse = values.get(sparse_name)
                    if sparse is not None and not isinstance(
                        sparse,
                        models.SparseVector,
                    ):
                        raise ValueError(
                            f"collection {collection!r} expects sparse vector "
                            f"{sparse_name!r}"
                        )
            normalized.append(
                (
                    logical_id,
                    point.revision or str(uuid.uuid4()),
                    values,
                    dict(point.payload or {}),
                )
            )
        self._control.upsert_rows(
            _POINTS_TABLE,
            [
                {
                    "collection_name": collection,
                    "point_id": logical_id,
                    "revision": version,
                }
                for logical_id, version, _, _ in normalized
            ],
        )
        self._store._reconcile_keys(
            collection,
            [logical_id for logical_id, _, _, _ in normalized],
            revision_data={
                (logical_id, version): (values, user_payload)
                for logical_id, version, values, user_payload in normalized
            },
            new_points=new_points,
        )
        return [
            QdrantPoint(logical_id, values, user_payload)
            for logical_id, _, values, user_payload in normalized
        ]

    def delete(self, collection: str, point_id: str) -> bool:
        return bool(self.delete_many(collection, [point_id]))

    def delete_many(
        self,
        collection: str,
        point_ids: Sequence[str],
    ) -> list[str]:
        logical_ids = sorted({str(point_id) for point_id in point_ids})
        if not logical_ids:
            return []
        if self._transaction_depth == 0:
            with self.transaction():
                return self.delete_many(collection, logical_ids)
        info = self._store.collection_info(collection)
        records: list[Any] = []
        for start in range(0, len(logical_ids), 256):
            records.extend(
                self._store._scroll_all(
                    info.physical_name,
                    scroll_filter=self._store._visible_filter(
                        self._branch_point,
                        point_ids=logical_ids[start : start + 256],
                    ),
                )
            )
        existing = {str(record.payload[_PAYLOAD_LOGICAL_ID]) for record in records}
        if not existing:
            return []
        self._control.delete_keys(
            _POINTS_TABLE,
            [
                {"collection_name": collection, "point_id": logical_id}
                for logical_id in sorted(existing)
            ],
        )
        self._store._reconcile_keys(collection, sorted(existing))
        return sorted(existing)

    def get(self, collection: str, point_id: str) -> QdrantPoint | None:
        return self.get_many(collection, [point_id]).get(str(point_id))

    def get_many(
        self,
        collection: str,
        point_ids: Sequence[str],
    ) -> dict[str, QdrantPoint]:
        if self._transaction_depth == 0:
            self._store._assert_clean()
        logical_ids = sorted({str(point_id) for point_id in point_ids})
        if not logical_ids:
            return {}
        info = self._store.collection_info(collection)
        records: list[Any] = []
        for start in range(0, len(logical_ids), 256):
            records.extend(
                self._store._scroll_all(
                    info.physical_name,
                    scroll_filter=self._store._visible_filter(
                        self._branch_point,
                        point_ids=logical_ids[start : start + 256],
                    ),
                )
            )
        result: dict[str, QdrantPoint] = {}
        for record in records:
            point_id = str(record.payload[_PAYLOAD_LOGICAL_ID])
            if point_id in result:
                raise QdrantStoreError(
                    f"multiple visible vector versions for {collection}/{point_id}"
                )
            result[point_id] = QdrantPoint(
                id=point_id,
                vector=_coerce_vector(record.vector),
                payload=dict(record.payload.get(_PAYLOAD_USER) or {}),
            )
        return result

    def list_points(self, collection: str) -> list[QdrantPoint]:
        if self._transaction_depth == 0:
            self._store._assert_clean()
        return self._store._visible_points(
            collection,
            self._branch_point,
        )

    def search(
        self,
        collection: str,
        query_vector: Sequence[float],
        *,
        limit: int = 10,
        score_threshold: float | None = None,
    ) -> list[QdrantSearchResult]:
        if self._transaction_depth == 0:
            self._store._assert_clean()
        info = self._store.collection_info(collection)
        values = [float(value) for value in query_vector]
        if len(values) != info.dimensions:
            raise ValueError(
                f"collection {collection!r} expects {info.dimensions} dimensions, "
                f"got {len(values)}"
            )
        response = self._store.client.query_points(
            collection_name=info.physical_name,
            query=values,
            using=info.dense_vector_name,
            query_filter=self._store._visible_filter(self._branch_point),
            limit=int(limit),
            score_threshold=score_threshold,
            with_payload=True,
            with_vectors=True,
        )
        return [
            QdrantSearchResult(
                id=str(point.payload[_PAYLOAD_LOGICAL_ID]),
                score=float(point.score),
                vector=_coerce_vector(point.vector),
                payload=dict(point.payload.get(_PAYLOAD_USER) or {}),
            )
            for point in response.points
        ]

    def hybrid_search(
        self,
        collection: str,
        *,
        dense_query: Sequence[float] | None,
        sparse_query: Any | None,
        sparse_vector_name: str,
        limit: int = 10,
        candidate_limit: int | None = None,
        exact_text: str | None = None,
        exact_phrase: str | None = None,
    ) -> list[QdrantSearchResult]:
        """Fuse branch-visible dense and sparse retrieval inside Qdrant."""

        if self._transaction_depth == 0:
            self._store._assert_clean()
        info = self._store.collection_info(collection)
        if info.dense_vector_name is None:
            raise QdrantStoreError(
                f"collection {collection!r} does not use named vectors"
            )
        if sparse_vector_name not in info.sparse_vector_names:
            raise QdrantStoreError(
                f"collection {collection!r} has no sparse vector {sparse_vector_name!r}"
            )
        if int(limit) <= 0:
            return []

        filters: list[Any] = [self._store._visible_filter(self._branch_point)]
        if exact_text:
            filters.append(
                models.FieldCondition(
                    key=f"{_PAYLOAD_USER}.text",
                    match=models.MatchText(text=str(exact_text)),
                )
            )
        if exact_phrase:
            filters.append(
                models.FieldCondition(
                    key=f"{_PAYLOAD_USER}.text",
                    match=models.MatchPhrase(phrase=str(exact_phrase)),
                )
            )
        query_filter = models.Filter(must=filters)
        fetch_limit = int(candidate_limit or max(64, int(limit) * 8))
        prefetch: list[Any] = []

        if dense_query is not None:
            dense = [float(value) for value in dense_query]
            if len(dense) != info.dimensions:
                raise ValueError(
                    f"collection {collection!r} expects {info.dimensions} "
                    f"dense dimensions, got {len(dense)}"
                )
            prefetch.append(
                models.Prefetch(
                    query=dense,
                    using=info.dense_vector_name,
                    filter=query_filter,
                    limit=fetch_limit,
                )
            )
        if sparse_query is not None:
            sparse = _coerce_vector({sparse_vector_name: sparse_query})[
                sparse_vector_name
            ]
            prefetch.append(
                models.Prefetch(
                    query=sparse,
                    using=sparse_vector_name,
                    filter=query_filter,
                    limit=fetch_limit,
                )
            )
        if not prefetch:
            return []

        if len(prefetch) == 1:
            only = prefetch[0]
            response = self._store.client.query_points(
                collection_name=info.physical_name,
                query=only.query,
                using=only.using,
                query_filter=query_filter,
                limit=int(limit),
                with_payload=True,
                with_vectors=False,
            )
        else:
            response = self._store.client.query_points(
                collection_name=info.physical_name,
                prefetch=prefetch,
                query=models.FusionQuery(fusion=models.Fusion.RRF),
                limit=int(limit),
                with_payload=True,
                with_vectors=False,
            )
        return [
            QdrantSearchResult(
                id=str(point.payload[_PAYLOAD_LOGICAL_ID]),
                score=float(point.score),
                vector={},
                payload=dict(point.payload.get(_PAYLOAD_USER) or {}),
            )
            for point in response.points
        ]


def _point_as_row(point: QdrantPoint | None) -> dict[str, Any] | None:
    if point is None:
        return None
    return {
        "id": point.id,
        # ``list(mapping)`` keeps only vector names and silently corrupts
        # named dense/sparse collections during merge. Preserve the complete
        # representation and normalize a defensive copy instead.
        "vector": _coerce_vector(point.vector),
        "payload": dict(point.payload),
    }


__all__ = [
    "ChronosQdrantStore",
    "QdrantBranchSession",
    "QdrantCollectionInfo",
    "QdrantPoint",
    "QdrantSearchResult",
    "QdrantStoreError",
    "QdrantUpsert",
]
