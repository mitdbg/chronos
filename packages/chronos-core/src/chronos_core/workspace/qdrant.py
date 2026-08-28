"""Interval-versioned Qdrant store for Chronos workspaces.

Qdrant owns vector search, payloads, and physical point versions. Chronos's
shared metadata plane allocates branch intervals; this shim attaches those
intervals to Qdrant points and performs each record-version splice with one
Qdrant batch update. Reads need one Qdrant filter rather than an ancestry walk.

The public API deliberately stays branch oriented: applications create or
checkout branches and then upsert, delete, retrieve, or search points.  The
interval coordinates in this module are internal.
"""

from __future__ import annotations

import contextlib
import contextvars
import json
import os
import threading
import uuid
import warnings
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from numbers import Integral
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


def _qdrant_pool_size() -> int:
    """Return the bounded per-client Qdrant transport pool size."""

    return _positive_int_environment("CHRONOS_QDRANT_POOL_SIZE") or 1


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


def _query_timeout_seconds() -> int:
    """Return the server-side deadline used for branch-visible queries.

    The Qdrant client timeout controls the transport, while ``query_points``
    also accepts a server-side operation deadline.  Keep that deadline
    explicit for Chronos reads so a cold, on-disk collection does not fall
    back to Qdrant's shorter default.
    """

    raw = os.environ.get("CHRONOS_QDRANT_QUERY_TIMEOUT_SECONDS", "600")
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(
            "CHRONOS_QDRANT_QUERY_TIMEOUT_SECONDS must be a positive integer"
        ) from exc
    if value <= 0:
        raise ValueError(
            "CHRONOS_QDRANT_QUERY_TIMEOUT_SECONDS must be a positive integer"
        )
    return value


def _query_search_params() -> Any | None:
    """Use indexed vectors only when the benchmark requests that hint."""

    if not _boolean_environment("CHRONOS_QDRANT_QUERY_INDEXED_ONLY"):
        return None
    return models.SearchParams(indexed_only=True)


_COLLECTIONS_TABLE = "chronos_qdrant_collections"
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
# Segment identifiers are metadata-plane integer coordinates and can exceed
# Qdrant's signed 64-bit payload-integer range.  Keep a string copy for exact
# writer matching; the legacy integer field is emitted only when representable.
_PAYLOAD_WRITER_KEY = f"{_PAYLOAD_PREFIX}writer_key"
_PAYLOAD_DELETED = f"{_PAYLOAD_PREFIX}deleted"

# Qdrant range predicates are represented as doubles.  Chronos coordinates
# may be wider than 64 bits, so represent each non-negative coordinate as two
# base-2^52 digits.  Each digit is an exact IEEE-754 integer and fits Qdrant's
# signed 64-bit payload-integer type; the two digits cover coordinates below
# 2^104 (the coordinate range used by the enterprise workload).
_INTERVAL_DIGIT_BASE = 1 << 52


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


@dataclass(frozen=True)
class _PhysicalPointVersion:
    physical_id: Any
    logical_id: str
    revision: str
    vector: Any
    payload: dict[str, Any]
    low: int
    high: int
    writer: int
    deleted: bool


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


_QDRANT_SIGNED_INT64_MIN = -(1 << 63)
_QDRANT_SIGNED_INT64_MAX = (1 << 63) - 1


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


def _qdrant_payload_value(value: Any) -> Any:
    """Keep arbitrary user JSON representable by Qdrant's gRPC payload type.

    Qdrant's integer payload arm is signed 64-bit, while application metadata
    is not required to use that bound.  Preserve oversized integers exactly as
    decimal strings instead of allowing the client conversion to fail during
    an otherwise valid point upload.
    """

    if isinstance(value, bool):
        return value
    if isinstance(value, Integral):
        integer = int(value)
        if _QDRANT_SIGNED_INT64_MIN <= integer <= _QDRANT_SIGNED_INT64_MAX:
            return integer
        return str(integer)
    if isinstance(value, Mapping):
        return {
            str(key): _qdrant_payload_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_qdrant_payload_value(item) for item in value]
    return value


class ChronosQdrantStore:
    """A Qdrant vector store that participates in a Chronos workspace.

    The collection catalog is stored in the supplied Chronos metadata
    database.  SQLite and PostgreSQL metadata URLs are supported.  Qdrant can
    be local (including embedded ``:memory:`` mode) or remote; callers may pass
    an already configured client for tests and advanced deployments.
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
        self._ensure_control_schema()

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
        transport_options: dict[str, Any]
        if use_grpc:
            # ``pool_size`` controls Qdrant's gRPC channel pool.  It cannot be
            # combined with HTTPX ``limits`` by qdrant-client.
            transport_options = {"pool_size": _qdrant_pool_size()}
        else:
            # qdrant-client leaves localhost HTTP connections unbounded by
            # default.  Explicit HTTPX limits prevent one client per worker
            # from creating an unbounded socket fan-out.
            import httpx

            pool_size = _qdrant_pool_size()
            transport_options = {
                "limits": httpx.Limits(
                    max_connections=pool_size,
                    max_keepalive_connections=pool_size,
                )
            }
        client = QdrantClient(
            url=url,
            api_key=api_key,
            grpc_port=grpc_port,
            prefer_grpc=use_grpc,
            timeout=timeout,
            **transport_options,
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
        # Use the adapter's portable schema introspection.  ``PRAGMA`` is
        # SQLite-specific and would make a shared PostgreSQL metadata plane
        # fail during Qdrant catalog bootstrap.
        collection_columns = set(db.table_defs(_COLLECTIONS_TABLE)[0])
        if "config_json" not in collection_columns:
            db.execute(
                f"""
                ALTER TABLE {_COLLECTIONS_TABLE}
                ADD COLUMN config_json TEXT NOT NULL DEFAULT '{{}}'
                """
            )
        db.commit()

    @contextlib.contextmanager
    def _metadata_read(self) -> Iterator[None]:
        """Keep standalone catalog reads from leaving PostgreSQL transactions open.

        Psycopg starts a transaction for a plain ``SELECT``.  Catalog lookups
        used by Qdrant operations are normally independent of the caller's
        branch transaction, so close the implicit transaction they create.
        If a caller already owns a metadata transaction, leave it untouched.
        """

        db = self.context.db
        started = not db.in_transaction
        try:
            yield
        except Exception:
            if started and db.in_transaction:
                db.rollback()
            raise
        else:
            if started and db.in_transaction:
                db.commit()

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
                # The metadata plane owns the logical-to-physical mapping.
                # A later session may use a different local namespace (for
                # example after moving a workspace), but it must continue to
                # use the physical collection recorded with the logical
                # collection rather than deriving a new name from that
                # session's path.
                physical_name = existing.physical_name
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
                if not self.client.collection_exists(physical_name):
                    raise QdrantStoreError(
                        f"collection metadata for {logical_name!r} references "
                        f"missing Qdrant collection {physical_name!r}"
                    )
                # Opening another workspace session must not rewrite a large
                # existing collection.  In particular, Qdrant's
                # ``update_collection`` waits for optimizer/configuration
                # work and becomes a startup bottleneck when many workers
                # connect concurrently.  Collection creation above already
                # applies these settings; an operator that intentionally
                # wants to reapply environment tuning can opt in.
                if (
                    (hnsw_config is not None or optimizers_config is not None)
                    and _boolean_environment(
                        "CHRONOS_QDRANT_REAPPLY_EXISTING_CONFIG"
                    )
                ):
                    self.client.update_collection(
                        collection_name=physical_name,
                        hnsw_config=hnsw_config,
                        optimizers_config=optimizers_config,
                    )
                self._create_payload_indexes(
                    physical_name,
                    text_indexes=normalized_text_indexes,
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
        payload_schema = self.client.get_collection(physical_name).payload_schema
        field_types = {
            _PAYLOAD_LOGICAL_ID: models.PayloadSchemaType.KEYWORD,
            _PAYLOAD_LOW_HI: models.PayloadSchemaType.INTEGER,
            _PAYLOAD_LOW_LO: models.PayloadSchemaType.INTEGER,
            _PAYLOAD_HIGH_HI: models.PayloadSchemaType.INTEGER,
            _PAYLOAD_HIGH_LO: models.PayloadSchemaType.INTEGER,
            _PAYLOAD_WRITER_KEY: models.PayloadSchemaType.KEYWORD,
            # Keep the old numeric index for collections written by earlier
            # Chronos versions.  New points omit this field when the segment
            # identifier does not fit Qdrant's integer representation.
            _PAYLOAD_WRITER: models.PayloadSchemaType.INTEGER,
            _PAYLOAD_DELETED: models.PayloadSchemaType.BOOL,
        }
        for field_name, field_schema in field_types.items():
            if field_name in payload_schema:
                continue
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
            field_name = f"{_PAYLOAD_USER}.{user_field}"
            if field_name in payload_schema:
                continue
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        message="Payload indexes have no effect in the local Qdrant.*",
                    )
                    self.client.create_payload_index(
                        collection_name=physical_name,
                        field_name=field_name,
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
        with self._metadata_read():
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
        with self._metadata_read():
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

    def checkout_control(self, control_session: Any) -> QdrantBranchSession:
        """Use a workspace's already checked-out interval control session."""

        if getattr(control_session, "_context", None) is not self.context:
            raise ValueError(
                "Qdrant control session belongs to a different branch context"
            )
        return QdrantBranchSession(self, control_session)

    def checkout_ref(
        self,
        branch_id: str,
        current_ref: str | int,
    ) -> QdrantBranchSession:
        """Check out the branch at a workspace-coordinated live interval."""

        return QdrantBranchSession(
            self,
            self.context.checkout_ref(branch_id, current_ref),
        )

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
    def _version_from_record(record: Any) -> _PhysicalPointVersion:
        payload = dict(record.payload or {})
        required = (
            _PAYLOAD_LOGICAL_ID,
            _PAYLOAD_REVISION,
            _PAYLOAD_LOW,
            _PAYLOAD_HIGH,
            _PAYLOAD_DELETED,
        )
        missing = [field for field in required if field not in payload]
        writer_value = payload.get(_PAYLOAD_WRITER_KEY, payload.get(_PAYLOAD_WRITER))
        if writer_value is None:
            missing.append(_PAYLOAD_WRITER_KEY)
        if missing or record.vector is None:
            raise QdrantStoreError(
                "Qdrant point is missing Chronos interval metadata: "
                + ", ".join(missing)
            )
        return _PhysicalPointVersion(
            physical_id=record.id,
            logical_id=str(payload[_PAYLOAD_LOGICAL_ID]),
            revision=str(payload[_PAYLOAD_REVISION]),
            vector=_coerce_vector(record.vector),
            payload=dict(payload.get(_PAYLOAD_USER) or {}),
            low=int(payload[_PAYLOAD_LOW]),
            high=int(payload[_PAYLOAD_HIGH]),
            writer=int(writer_value),
            deleted=bool(payload[_PAYLOAD_DELETED]),
        )

    @staticmethod
    def _point_struct(
        info: QdrantCollectionInfo,
        version: _PhysicalPointVersion,
    ) -> Any:
        low_hi, low_lo = _split_interval_coordinate(version.low)
        high_hi, high_lo = _split_interval_coordinate(version.high)
        payload = {
            _PAYLOAD_USER: _qdrant_payload_value(version.payload),
            _PAYLOAD_LOGICAL_ID: version.logical_id,
            _PAYLOAD_REVISION: version.revision,
            _PAYLOAD_LOW: str(version.low),
            _PAYLOAD_HIGH: str(version.high),
            _PAYLOAD_LOW_HI: low_hi,
            _PAYLOAD_LOW_LO: low_lo,
            _PAYLOAD_HIGH_HI: high_hi,
            _PAYLOAD_HIGH_LO: high_lo,
            _PAYLOAD_WRITER_KEY: str(version.writer),
            _PAYLOAD_DELETED: version.deleted,
        }
        # Keep the legacy integer field only while it is representable.  A
        # metadata-plane segment identifier can be wider than Qdrant's
        # signed-64-bit payload integer, in which case the string key above is
        # the authoritative writer identity.
        if _QDRANT_SIGNED_INT64_MIN <= version.writer <= _QDRANT_SIGNED_INT64_MAX:
            payload[_PAYLOAD_WRITER] = version.writer
        return models.PointStruct(
            id=(
                version.physical_id
                if version.physical_id is not None
                else _point_id(
                    info.physical_name,
                    version.logical_id,
                    version.revision,
                    version.low,
                    version.high,
                    version.deleted,
                )
            ),
            vector=version.vector,
            payload=payload,
        )

    def _versions_for_ids(
        self,
        collection: str,
        point_ids: Sequence[str],
    ) -> dict[str, list[_PhysicalPointVersion]]:
        logical_ids = sorted({str(point_id) for point_id in point_ids})
        versions = {point_id: [] for point_id in logical_ids}
        if not logical_ids:
            return versions
        info = self.collection_info(collection)
        for start in range(0, len(logical_ids), 256):
            records = self._scroll_all(
                info.physical_name,
                scroll_filter=self._logical_ids_filter(
                    logical_ids[start : start + 256]
                ),
            )
            for record in records:
                version = self._version_from_record(record)
                versions.setdefault(version.logical_id, []).append(version)
        for values in versions.values():
            values.sort(key=lambda version: (version.low, version.high))
        return versions

    def _replace_versions(
        self,
        collection: str,
        stale_ids: Sequence[Any],
        replacements: Sequence[_PhysicalPointVersion],
        *,
        info: QdrantCollectionInfo | None = None,
    ) -> None:
        """Install one interval splice as a waited, strongly ordered batch."""

        info = info or self.collection_info(collection)
        operations: list[Any] = []
        if stale_ids:
            operations.append(
                models.DeleteOperation(
                    delete=models.PointIdsList(points=list(stale_ids))
                )
            )
        if replacements:
            points = [
                self._point_struct(info, version)
                for version in replacements
            ]
            # Pydantic may copy or normalize user payload mappings while
            # constructing PointStruct.  Sanitize once more at the wire
            # boundary so no oversized application integer reaches gRPC.
            for point in points:
                if point.payload is not None:
                    point.payload = _qdrant_payload_value(point.payload)
            operations.append(
                models.UpsertOperation(
                    upsert=models.PointsList(
                        points=points
                    )
                )
            )
        if operations:
            self.client.batch_update_points(
                collection_name=info.physical_name,
                update_operations=operations,
                wait=True,
                ordering=models.WriteOrdering.STRONG,
            )

    def _splice_many(
        self,
        collection: str,
        replacements: Mapping[
            str,
            tuple[str, Any, dict[str, Any], bool],
        ],
        *,
        write_low: int,
        write_high: int,
        writer: int,
        existing: Mapping[str, Sequence[_PhysicalPointVersion]] | None = None,
        new_points: bool = False,
        _info: QdrantCollectionInfo | None = None,
    ) -> None:
        """Splice logical point updates directly into Qdrant intervals."""

        if write_low >= write_high:
            raise QdrantStoreError("Qdrant write interval is empty")
        # Resolve the registry row before fanning out uploads.  The registry
        # lives in the shared metadata connection, which is not safe to query
        # concurrently from the worker threads used for large batches.
        info = _info or self.collection_info(collection)
        point_batch_size = (
            _positive_int_environment("CHRONOS_QDRANT_POINT_BATCH_SIZE") or 256
        )
        if new_points and len(replacements) > point_batch_size:
            items = list(replacements.items())
            batches = [
                dict(items[start : start + point_batch_size])
                for start in range(0, len(items), point_batch_size)
            ]

            def upload(
                batch: Mapping[str, tuple[str, Any, dict[str, Any], bool]],
            ) -> None:
                self._splice_many(
                    collection,
                    batch,
                    write_low=write_low,
                    write_high=write_high,
                    writer=writer,
                    new_points=True,
                    _info=info,
                )

            workers = min(
                _positive_int_environment("CHRONOS_QDRANT_UPLOAD_WORKERS") or 1,
                len(batches),
            )
            if workers == 1:
                for batch in batches:
                    upload(batch)
            else:
                contexts = [contextvars.copy_context() for _ in batches]

                def upload_in_context(
                    item: tuple[contextvars.Context, Mapping[str, tuple[str, Any, dict[str, Any], bool]]],
                ) -> None:
                    context, batch = item
                    context.run(upload, batch)

                with ThreadPoolExecutor(
                    max_workers=workers,
                    thread_name_prefix="chronos-qdrant-upload",
                ) as executor:
                    list(
                        executor.map(
                            upload_in_context,
                            zip(contexts, batches, strict=True),
                        )
                    )
            return
        old_by_id = (
            {point_id: [] for point_id in replacements}
            if new_points
            else dict(existing or self._versions_for_ids(collection, replacements))
        )
        desired: list[_PhysicalPointVersion] = []
        for logical_id, (revision, vector, payload, deleted) in replacements.items():
            overlapping = [
                version
                for version in old_by_id.get(logical_id, ())
                if version.low < write_high and write_low < version.high
            ]
            replacement_id: Any | None = None
            if len(overlapping) > 1:
                raise QdrantStoreError(
                    f"overlapping physical versions for {collection}/{logical_id}"
                )
            if overlapping:
                old = overlapping[0]
                # Reuse the superseded point ID for the replacement. The
                # complete splice then fits in one Qdrant upsert operation;
                # no separately committed delete is required.
                replacement_id = old.physical_id
                overlap_low = max(old.low, write_low)
                overlap_high = min(old.high, write_high)
                if old.low < overlap_low:
                    desired.append(
                        _PhysicalPointVersion(
                            None,
                            old.logical_id,
                            old.revision,
                            old.vector,
                            old.payload,
                            old.low,
                            overlap_low,
                            old.writer,
                            old.deleted,
                        )
                    )
                if overlap_high < old.high:
                    desired.append(
                        _PhysicalPointVersion(
                            None,
                            old.logical_id,
                            old.revision,
                            old.vector,
                            old.payload,
                            overlap_high,
                            old.high,
                            old.writer,
                            old.deleted,
                        )
                    )
            else:
                overlap_low, overlap_high = write_low, write_high
            desired.append(
                _PhysicalPointVersion(
                    replacement_id,
                    logical_id,
                    revision,
                    vector,
                    payload,
                    overlap_low,
                    overlap_high,
                    writer,
                    deleted,
                )
            )
        self._replace_versions(collection, (), desired, info=info)

    def _restore_versions(
        self,
        collection: str,
        snapshots: Mapping[str, Sequence[_PhysicalPointVersion]],
    ) -> None:
        current = self._versions_for_ids(collection, snapshots)
        stale = [
            version.physical_id for versions in current.values() for version in versions
        ]
        desired = [version for versions in snapshots.values() for version in versions]
        self._replace_versions(collection, stale, desired)

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

    def _branch_ancestry(self, branch_id: str) -> list[tuple[int, int]]:
        """Return ``(segment_id, branch_point)`` from head to root."""
        with self._metadata_read():
            branch = self.context.db.execute(
                """
                SELECT current_segment_id
                FROM _chronos_branch_interval_branches
                WHERE branch_id = ?
                """,
                (branch_id,),
            ).fetchone()
            if branch is None:
                raise QdrantStoreError(f"branch not found: {branch_id}")
            ancestry: list[tuple[int, int]] = []
            segment_id: int | None = int(branch["current_segment_id"])
            while segment_id is not None:
                row = self.context.db.execute(
                    """
                    SELECT segment_id, parent_segment_id, branch_point
                    FROM _chronos_branch_interval_segments
                    WHERE segment_id = ?
                    """,
                    (segment_id,),
                ).fetchone()
                if row is None:
                    raise QdrantStoreError(
                        f"interval segment not found: {segment_id}"
                    )
                ancestry.append((int(row["segment_id"]), int(row["branch_point"])))
                parent = row["parent_segment_id"]
                segment_id = int(parent) if parent is not None else None
            return ancestry

    @staticmethod
    def _point_at(
        versions: Sequence[_PhysicalPointVersion],
        branch_point: int,
    ) -> QdrantPoint | None:
        visible = [
            version
            for version in versions
            if version.low <= branch_point < version.high
        ]
        if len(visible) > 1:
            raise QdrantStoreError("multiple physical versions are visible")
        if not visible or visible[0].deleted:
            return None
        version = visible[0]
        return QdrantPoint(version.logical_id, version.vector, version.payload)

    def _merge_coordinates(
        self,
        source: str,
        target: str,
    ) -> tuple[int, int, int, set[int]]:
        source_ancestry = self._branch_ancestry(source)
        target_ancestry = self._branch_ancestry(target)
        target_positions = {
            segment_id: index for index, (segment_id, _) in enumerate(target_ancestry)
        }
        common_id: int | None = None
        base_point: int | None = None
        source_common_index = 0
        for index, (segment_id, branch_point) in enumerate(source_ancestry):
            if segment_id in target_positions:
                common_id = segment_id
                base_point = branch_point
                source_common_index = index
                break
        if common_id is None or base_point is None:
            raise QdrantStoreError(
                f"branches {source!r} and {target!r} have no common ancestor"
            )
        target_common_index = target_positions[common_id]
        divergent_writers = {
            segment_id for segment_id, _ in source_ancestry[:source_common_index]
        } | {segment_id for segment_id, _ in target_ancestry[:target_common_index]}
        return (
            source_ancestry[0][1],
            target_ancestry[0][1],
            base_point,
            divergent_writers,
        )

    def _changed_point_ids(
        self,
        info: QdrantCollectionInfo,
        writers: set[int],
    ) -> set[str]:
        if not writers:
            return set()
        logical_ids: set[str] = set()
        # The embedded Qdrant client does not report payload indexes in its
        # collection schema, but it still evaluates filters correctly.  Issue
        # the predicate regardless of index presence; remote deployments use
        # the index when available and otherwise fall back to a server scan.
        records = self._scroll_all(
            info.physical_name,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key=_PAYLOAD_WRITER_KEY,
                        match=models.MatchAny(
                            any=sorted(str(writer) for writer in writers)
                        ),
                    )
                ]
            ),
        )
        logical_ids.update(
            str(record.payload[_PAYLOAD_LOGICAL_ID]) for record in records
        )
        # Read legacy collections that only contain the numeric writer field.
        # Large identifiers are intentionally excluded from this query because
        # Qdrant cannot represent them as integer payload values.
        legacy_writers = sorted(
            writer
            for writer in writers
            if _QDRANT_SIGNED_INT64_MIN <= writer <= _QDRANT_SIGNED_INT64_MAX
        )
        if legacy_writers:
            records = self._scroll_all(
                info.physical_name,
                scroll_filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key=_PAYLOAD_WRITER,
                            match=models.MatchAny(any=legacy_writers),
                        )
                    ]
                ),
            )
            logical_ids.update(
                str(record.payload[_PAYLOAD_LOGICAL_ID]) for record in records
            )
        return logical_ids

    def merge_preview(
        self,
        source: str,
        target: str,
        *,
        policy: MergePolicyInput = None,
    ) -> MergePreview:
        source_point, target_point, base_point, writers = self._merge_coordinates(
            source, target
        )
        changes: list[RowDiff] = []
        conflicts: list[RowDiff] = []
        for info in self.list_collections():
            point_ids = sorted(self._changed_point_ids(info, writers))
            versions = self._versions_for_ids(info.name, point_ids)
            for point_id in point_ids:
                physical = versions.get(point_id, [])
                base = _point_as_row(self._point_at(physical, base_point))
                source_row = _point_as_row(self._point_at(physical, source_point))
                target_row = _point_as_row(self._point_at(physical, target_point))
                if source_row == base or source_row == target_row:
                    continue
                if target_row is None:
                    kind = "added"
                elif source_row is None:
                    kind = "deleted"
                else:
                    kind = "modified"
                change = RowDiff(
                    table=info.name,
                    key={"id": point_id},
                    change=kind,  # type: ignore[arg-type]
                    before=target_row,
                    after=source_row,
                )
                (changes if target_row == base else conflicts).append(change)
        return _preview_with_merge_policy(
            MergePreview(
                source=source,
                target=target,
                changes=changes,
                conflicts=conflicts,
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
        """Stage point versions in the transaction's unpublished interval."""

        by_collection: dict[str, list[RowDiff]] = {}
        for change in changes:
            by_collection.setdefault(change.table, []).append(change)
        applied = 0
        for collection, collection_changes in by_collection.items():
            logical_ids = [str(change.key["id"]) for change in collection_changes]
            existing = self._versions_for_ids(collection, logical_ids)
            target_session = self.checkout(target)
            target_points = target_session.get_many(collection, logical_ids)
            updates: dict[str, tuple[str, Any, dict[str, Any], bool]] = {}
            for change in collection_changes:
                point_id = str(change.key["id"])
                if change.after is None:
                    previous = target_points.get(point_id)
                    if previous is None:
                        continue
                    updates[point_id] = (
                        str(uuid.uuid4()),
                        previous.vector,
                        previous.payload,
                        True,
                    )
                else:
                    updates[point_id] = (
                        str(uuid.uuid4()),
                        _coerce_vector(change.after["vector"]),
                        dict(change.after["payload"]),
                        False,
                    )
            self._splice_many(
                collection,
                updates,
                write_low=int(transaction.merge_live_lo),
                write_high=int(transaction.merge_live_hi),
                writer=int(transaction.merge_segment_id),
                existing=existing,
            )
            applied += len(updates)
        return applied

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

    # Keep Qdrant outside the shared relational transaction so a failure in a
    # later participant reaches this context and restores its in-memory undo
    # snapshots before the workspace transaction returns.
    _workspace_transaction_priority = -100

    def __init__(self, store: ChronosQdrantStore, control_session: Any):
        self._store = store
        self._control = control_session
        self._transaction_depth = 0
        self._undo_versions: dict[
            str,
            dict[str, list[_PhysicalPointVersion]],
        ] = {}

    @property
    def branch_id(self) -> str:
        return self._control.branch_id

    @property
    def current_ref(self) -> str:
        return self._control.current_ref

    @property
    def _branch_point(self) -> int:
        return int(self._segment.branch_point)

    @property
    def _segment(self) -> Any:
        self._control._ensure_fresh()
        segment = self._control._ref.metadata.get("segment")
        if segment is None:
            raise QdrantStoreError("Qdrant checkout is missing interval metadata")
        return segment

    def _capture_undo(
        self,
        collection: str,
        point_ids: Sequence[str],
        *,
        known_new: bool = False,
    ) -> dict[str, list[_PhysicalPointVersion]]:
        snapshots = self._undo_versions.setdefault(collection, {})
        missing = [point_id for point_id in point_ids if point_id not in snapshots]
        if missing:
            captured = (
                {point_id: [] for point_id in missing}
                if known_new
                else self._store._versions_for_ids(collection, missing)
            )
            snapshots.update(captured)
        return {point_id: list(snapshots[point_id]) for point_id in point_ids}

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
            self._undo_versions = {}
            self._transaction_depth = 1
            try:
                with self._control.transaction():
                    yield
            except Exception:
                self._transaction_depth = 0
                try:
                    for collection, snapshots in self._undo_versions.items():
                        self._store._restore_versions(collection, snapshots)
                except Exception as repair_error:
                    raise QdrantStoreError(
                        "Qdrant mutation failed and rollback also failed"
                    ) from repair_error
                raise
            else:
                self._transaction_depth = 0
            finally:
                self._undo_versions = {}

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
        if self._control._ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
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
        logical_ids = [logical_id for logical_id, _, _, _ in normalized]
        self._capture_undo(
            collection,
            logical_ids,
            known_new=new_points,
        )
        existing = (
            {logical_id: [] for logical_id in logical_ids}
            if new_points
            else self._store._versions_for_ids(collection, logical_ids)
        )
        segment = self._segment
        self._store._splice_many(
            collection,
            {
                logical_id: (version, values, user_payload, False)
                for logical_id, version, values, user_payload in normalized
            },
            write_low=int(segment.live_lo),
            write_high=int(segment.live_hi),
            writer=int(segment.segment_id),
            existing=existing,
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
        if self._control._ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
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
        self._capture_undo(collection, sorted(existing))
        current = self._store._versions_for_ids(collection, sorted(existing))
        by_id = {
            str(record.payload[_PAYLOAD_LOGICAL_ID]): self._store._version_from_record(
                record
            )
            for record in records
        }
        segment = self._segment
        self._store._splice_many(
            collection,
            {
                logical_id: (
                    str(uuid.uuid4()),
                    by_id[logical_id].vector,
                    by_id[logical_id].payload,
                    True,
                )
                for logical_id in sorted(existing)
            },
            write_low=int(segment.live_lo),
            write_high=int(segment.live_hi),
            writer=int(segment.segment_id),
            existing=current,
        )
        return sorted(existing)

    def get(self, collection: str, point_id: str) -> QdrantPoint | None:
        with self._control._operation_epoch():
            return self.get_many(collection, [point_id]).get(str(point_id))

    def get_many(
        self,
        collection: str,
        point_ids: Sequence[str],
    ) -> dict[str, QdrantPoint]:
        with self._control._operation_epoch():
            return self._get_many_unfenced(collection, point_ids)

    def _get_many_unfenced(
        self,
        collection: str,
        point_ids: Sequence[str],
    ) -> dict[str, QdrantPoint]:
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
        with self._control._operation_epoch():
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
        with self._control._operation_epoch():
            return self._search_unfenced(
                collection,
                query_vector,
                limit=limit,
                score_threshold=score_threshold,
            )

    def _search_unfenced(
        self,
        collection: str,
        query_vector: Sequence[float],
        *,
        limit: int,
        score_threshold: float | None,
    ) -> list[QdrantSearchResult]:
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
            search_params=_query_search_params(),
            timeout=_query_timeout_seconds(),
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

        with self._control._operation_epoch():
            return self._hybrid_search_unfenced(
                collection,
                dense_query=dense_query,
                sparse_query=sparse_query,
                sparse_vector_name=sparse_vector_name,
                limit=limit,
                candidate_limit=candidate_limit,
                exact_text=exact_text,
                exact_phrase=exact_phrase,
            )

    def _hybrid_search_unfenced(
        self,
        collection: str,
        *,
        dense_query: Sequence[float] | None,
        sparse_query: Any | None,
        sparse_vector_name: str,
        limit: int,
        candidate_limit: int | None,
        exact_text: str | None,
        exact_phrase: str | None,
    ) -> list[QdrantSearchResult]:

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
        search_params = _query_search_params()
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
                    params=search_params,
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
                    params=search_params,
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
                search_params=search_params,
                timeout=_query_timeout_seconds(),
            )
        else:
            response = self._store.client.query_points(
                collection_name=info.physical_name,
                prefetch=prefetch,
                query=models.FusionQuery(fusion=models.Fusion.RRF),
                limit=int(limit),
                with_payload=True,
                with_vectors=False,
                search_params=search_params,
                timeout=_query_timeout_seconds(),
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
