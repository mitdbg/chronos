from __future__ import annotations

from chronos_core.branching._common import *

import time
import threading
from decimal import Decimal
from typing import Mapping

_SQL_CACHE_MAX_ENTRIES = 4096


def _wait_for_all_async_schema_index_jobs() -> None:
    """Compatibility hook; native secondary-index workers are fire-and-forget."""


def _wait_for_all_interval_gc_jobs() -> None:
    """Compatibility hook; interval GC now runs through the native backend."""


class _IntervalBackend(_SQLBranchBackend):
    """Visibility-interval backend.

    Each logical row version is stored once with a half-open numeric interval.
    Reading a branch becomes a constant-size predicate over the branch point:
    live_lo <= point < live_hi and deleted = FALSE. Writes maintain correctness by
    splitting any overlapping physical rows for the branch's current interval.
    """

    name = "interval"

    def __init__(
        self,
        db: SQLDatabaseAdapter,
        continuation_percent: int = _INTERVAL_CONTINUATION_PERCENT,
        child_width: int | None = None,
        allocation_strategy: IntervalAllocationStrategy = "adaptive",
        enable_schema_branching: bool = False,
        create_secondary_indexes: bool = True,
        create_writer_segment_index: bool = True,
    ):
        super().__init__(db)
        self.continuation_percent = _validate_interval_continuation_percent(
            continuation_percent
        )
        self.child_width = _validate_interval_child_width(child_width)
        self.allocation_strategy = _validate_interval_allocation_strategy(
            allocation_strategy
        )
        self.enable_schema_branching = bool(enable_schema_branching)
        self.create_secondary_indexes = bool(create_secondary_indexes)
        self.create_writer_segment_index = bool(create_writer_segment_index)
        self._interval_gc_requested = False
        self._interval_gc_pending = False
        self._interval_gc_running = False
        self._interval_gc_shutdown = False
        self._interval_gc_thread: threading.Thread | None = None
        self._interval_gc_lock = threading.Lock()
        self._interval_gc_condition = threading.Condition(self._interval_gc_lock)
        self._interval_gc_ready = threading.Event()
        self._interval_gc_error: BaseException | None = None
        self._interval_gc_store = None
        self._native_branch_store = None
        self._native_branch_sessions: dict[tuple[str, int], Any] = {}
        self._native_branch_session_segment_hints: dict[str, int] = {}
        self._native_branch_transaction_active = False
        self._initialize_native_branch_store()
        database_url = str(getattr(self.db, "database_url", "") or "")
        self._interval_gc_async = self.db.dialect == "postgres" or (
            self.db.dialect == "sqlite"
            and database_url
            and ":memory:" not in database_url
        )
        if self.db.dialect == "postgres":
            try:
                self.db._chronos_after_commit = self._commit_native_branch_store  # type: ignore[attr-defined]
                self.db._chronos_after_rollback = self._rollback_native_branch_store  # type: ignore[attr-defined]
            except Exception:
                pass
        if self._interval_gc_async:
            self._initialize_interval_gc_worker()

    def _initialize_native_branch_store(self) -> None:
        if self._native_branch_store is None:
            self._native_branch_store = self._create_native_branch_store()
            set_create_secondary_indexes = getattr(
                self._native_branch_store,
                "set_create_secondary_indexes",
                None,
            )
            if callable(set_create_secondary_indexes):
                set_create_secondary_indexes(self.create_secondary_indexes)
            set_create_writer_segment_index = getattr(
                self._native_branch_store,
                "set_create_writer_segment_index",
                None,
            )
            if callable(set_create_writer_segment_index):
                set_create_writer_segment_index(self.create_writer_segment_index)

    def refresh_native_connections(self) -> None:
        self._invalidate_native_branch_sessions()
        self._native_branch_store = None
        self._initialize_native_branch_store()

    def _commit_native_branch_store(self) -> None:
        if self._native_branch_store is not None:
            self._native_branch_store.commit()

    def _rollback_native_branch_store(self) -> None:
        if self._native_branch_store is not None:
            try:
                self._native_branch_store.rollback()
            except Exception:
                pass
        self._invalidate_native_branch_sessions()

    def _manage_native_autocommit(self) -> bool:
        return (
            self._native_branch_store is not None
            and not self.db.in_transaction
            and not self._native_branch_transaction_active
        )

    def _commit_native_autocommit(self, manage: bool) -> None:
        if (
            manage
            and self._native_branch_store is not None
            and self._native_branch_store.in_transaction()
        ):
            self._native_branch_store.commit()

    def _rollback_native_autocommit(self, manage: bool) -> None:
        if (
            manage
            and self._native_branch_store is not None
            and self._native_branch_store.in_transaction()
        ):
            try:
                self._native_branch_store.rollback()
            except Exception:
                pass
            self._invalidate_native_branch_sessions()

    def close(self) -> None:
        try:
            self.wait_for_interval_gc()
        finally:
            self._shutdown_interval_gc_worker()
            self._invalidate_native_branch_sessions()
            self._native_branch_store = None

    def _start_async_schema_indexes(self) -> None:
        if self._native_branch_store is None:
            return
        flush_deferred = getattr(
            self._native_branch_store,
            "flush_deferred_schema_indexes",
            None,
        )
        if callable(flush_deferred):
            flush_deferred()

    def _invalidate_native_branch_sessions(self) -> None:
        self._native_branch_sessions.clear()
        self._native_branch_session_segment_hints.clear()

    def _create_native_branch_store(self) -> Any | None:
        try:
            from chronos_core import _native_interval

            data_db = getattr(self.db, "data_db", None)
            metadata_db = getattr(self.db, "metadata_db", None)
            if data_db is not None and metadata_db is not None:
                data_url = getattr(data_db, "database_url", None)
                if getattr(data_db, "dialect", None) == "duckdb":
                    metadata_url = getattr(metadata_db, "database_url", None)
                    if metadata_url and metadata_url != "sqlite:///:memory:":
                        return _native_interval.NativeBranchStore(
                            data_db.raw_connection,
                            metadata_url,
                        )
                    return _native_interval.NativeBranchStore(
                        data_db.raw_connection,
                        metadata_db.dialect,
                        metadata_db.raw_connection,
                    )
                if data_url:
                    metadata_url = getattr(metadata_db, "database_url", None)
                    if metadata_url:
                        return _native_interval.NativeBranchStore(
                            data_url, metadata_url
                        )
            if self.db.dialect == "postgres":
                database_url = getattr(self.db, "database_url", None)
                if not database_url:
                    raise BranchingError(
                        "native PostgreSQL interval backend requires a database URL"
                    )
                return _native_interval.NativeBranchStore(database_url)
            if self.db.dialect == "duckdb":
                raise BranchingError(
                    "DuckDB interval stores require SQLite/PostgreSQL metadata"
                )
            return _native_interval.NativeBranchStore.from_connection(
                self.db.dialect,
                self.db.raw_connection,
            )
        except Exception as exc:
            raise BranchingError("native interval branch store is required") from exc

    def _native_branch_session(self, branch_id: str) -> Any:
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        segment_id = self._native_branch_session_segment_hints.get(branch_id)
        if segment_id is None:
            try:
                segment_id = self._branch_segment_id(branch_id)
            except Exception:
                segment_id = None
        if segment_id is not None:
            cache_key = (branch_id, segment_id)
            cached = self._native_branch_sessions.get(cache_key)
            if cached is not None:
                return cached
        try:
            session = self._native_branch_store.checkout(branch_id)
        except Exception as exc:
            raise BranchingError(
                f"native checkout failed for branch: {branch_id}"
            ) from exc
        if segment_id is not None:
            if len(self._native_branch_sessions) >= _SQL_CACHE_MAX_ENTRIES:
                self._native_branch_sessions.clear()
            self._native_branch_sessions[(branch_id, segment_id)] = session
        return session

    def _native_branch_session_for_segment(
        self,
        branch_id: str,
        segment: _IntervalSegment,
    ) -> Any:
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        cache_key = (branch_id, segment.segment_id)
        cached = self._native_branch_sessions.get(cache_key)
        if cached is not None:
            return cached
        try:
            checkout_segment = getattr(
                self._native_branch_store, "checkout_segment", None
            )
            if callable(checkout_segment):
                session = checkout_segment(
                    branch_id,
                    segment.segment_id,
                    str(segment.live_lo),
                    str(segment.live_hi),
                    str(segment.branch_point),
                )
            else:
                current_segment_id = self._branch_segment_id(branch_id)
                if current_segment_id != segment.segment_id:
                    raise BranchingError(
                        "native checkout_segment support is required for non-current segments"
                    )
                session = self._native_branch_store.checkout(branch_id)
        except Exception as exc:
            raise BranchingError(
                f"native checkout failed for branch segment: {branch_id}:{segment.segment_id}"
            ) from exc
        if len(self._native_branch_sessions) >= _SQL_CACHE_MAX_ENTRIES:
            self._native_branch_sessions.clear()
        self._native_branch_sessions[cache_key] = session
        return session

    def refresh_registries(self) -> None:
        super().refresh_registries()
        self._invalidate_native_branch_sessions()

    def after_commit(self) -> None:
        self._commit_native_branch_store()
        self._start_async_schema_indexes()
        self._start_pending_interval_gc()

    def after_rollback(self) -> None:
        self._rollback_native_branch_store()
        if self._native_branch_store is not None:
            clear_deferred = getattr(
                self._native_branch_store,
                "clear_deferred_schema_indexes",
                None,
            )
            if callable(clear_deferred):
                clear_deferred()
        self._interval_gc_requested = False

    def wait_for_async_schema_indexes(self) -> None:
        self._start_async_schema_indexes()

    def wait_for_interval_gc(self) -> None:
        if not self._interval_gc_async:
            return
        with self._interval_gc_condition:
            while self._interval_gc_pending or self._interval_gc_running:
                self._interval_gc_condition.wait()
            error = self._interval_gc_error
            self._interval_gc_error = None
        if error is not None:
            raise BranchingError("background interval GC failed") from error

    def _start_pending_interval_gc(self) -> None:
        if not self._interval_gc_requested:
            return
        self._interval_gc_requested = False
        if not self._interval_gc_async:
            if self._native_branch_store is not None:
                self._native_branch_store.collect_interval_garbage()
                return
            raise BranchingError("native interval branch store is unavailable")

        with self._interval_gc_condition:
            self._interval_gc_pending = True
            self._interval_gc_condition.notify()

    def _initialize_interval_gc_worker(self) -> None:
        thread = threading.Thread(
            target=self._run_interval_gc_worker,
            name="chronos-interval-gc",
            daemon=True,
        )
        self._interval_gc_thread = thread
        thread.start()
        self._interval_gc_ready.wait()
        with self._interval_gc_condition:
            error = self._interval_gc_error
        if error is not None:
            self._shutdown_interval_gc_worker()
            raise BranchingError(
                "failed to initialize background interval GC"
            ) from error

    def _shutdown_interval_gc_worker(self) -> None:
        thread = self._interval_gc_thread
        if thread is None:
            return
        with self._interval_gc_condition:
            self._interval_gc_shutdown = True
            self._interval_gc_condition.notify_all()
        thread.join()
        self._interval_gc_thread = None
        self._interval_gc_store = None

    def _run_interval_gc_worker(self) -> None:
        try:
            database_url = getattr(self.db, "database_url", None)
            if not database_url:
                raise BranchingError("PostgreSQL interval GC requires a database URL")
            from chronos_core import _native_interval

            self._interval_gc_store = _native_interval.NativeBranchStore(database_url)
        except BaseException as exc:
            with self._interval_gc_condition:
                self._interval_gc_error = exc
                self._interval_gc_shutdown = True
                self._interval_gc_ready.set()
                self._interval_gc_condition.notify_all()
            return

        self._interval_gc_ready.set()
        while True:
            with self._interval_gc_condition:
                while not self._interval_gc_pending and not self._interval_gc_shutdown:
                    self._interval_gc_condition.wait()
                if self._interval_gc_shutdown and not self._interval_gc_pending:
                    return
                self._interval_gc_pending = False
                self._interval_gc_running = True
            try:
                self._collect_interval_garbage_background()
            except BaseException as exc:
                with self._interval_gc_condition:
                    self._interval_gc_error = exc
                    self._interval_gc_pending = False
                    self._interval_gc_running = False
                    self._interval_gc_shutdown = True
                    self._interval_gc_condition.notify_all()
                return
            with self._interval_gc_condition:
                self._interval_gc_running = False
                self._interval_gc_condition.notify_all()

    def _collect_interval_garbage_background(self) -> None:
        if self._interval_gc_store is None:
            raise BranchingError("background interval GC store is unavailable")
        self._interval_gc_store.collect_interval_garbage()

    def ensure(self) -> None:
        if self._native_branch_store is not None:
            if self.db.in_transaction:
                self.db.commit()
            self._native_branch_store.ensure(self.enable_schema_branching)
            # Keep the compatibility adapter transaction boundary closed after
            # native bootstrap so legacy callers do not inherit an open setup tx.
            self.db.commit()
            self.refresh_registries()
            return
        raise BranchingError("native interval branch store is unavailable")

    def register_table(self, table: str, primary_key: list[str]) -> None:
        if self._native_branch_store is not None:
            try:
                self._native_branch_store.register_table(
                    table,
                    primary_key,
                    self.enable_schema_branching,
                )
            except Exception as exc:
                message = str(exc)
                if "primary key columns missing" in message:
                    raise TableNotRegisteredError(message) from exc
                raise
            self.refresh_registries()
            return
        raise BranchingError("native interval branch store is unavailable")

    def create_index(
        self, table: str, columns: list[str], name: str | None = None
    ) -> IndexInfo:
        if self._native_branch_store is not None:
            try:
                index_name = self._native_branch_store.create_index(
                    table,
                    columns,
                    name or "",
                )
            except ValueError:
                raise
            except Exception as exc:
                message = str(exc)
                if (
                    "table is not registered for interval branching" in message
                    or "index columns missing" in message
                ):
                    raise TableNotRegisteredError(message) from exc
                if "index columns cannot be empty" in message:
                    raise ValueError(message) from exc
                raise
            self.refresh_registries()
            index = self.indexes[index_name]
            return IndexInfo(index.name, index.table, index.columns, index.backend)
        raise BranchingError("native interval branch store is unavailable")

    def create_branch(
        self,
        branch_id: str,
        from_branch: str,
        metadata: dict[str, Any] | None = None,
        *,
        terminal: bool = False,
    ) -> None:
        self._create_branch_unlocked(
            branch_id, from_branch, metadata, terminal=terminal
        )

    def _create_branch_unlocked(
        self,
        branch_id: str,
        from_branch: str,
        metadata: dict[str, Any] | None = None,
        *,
        terminal: bool = False,
    ) -> None:
        if self._native_branch_store is not None:
            for attempt in range(8):
                try:
                    self._native_branch_store.create_branch(
                        branch_id,
                        from_branch,
                        terminal,
                        _json_dumps(metadata),
                        self.continuation_percent,
                        self.child_width or 0,
                        self.allocation_strategy,
                    )
                    self._invalidate_native_branch_sessions()
                    return
                except Exception as exc:
                    message = str(exc)
                    if "branch already exists" in message:
                        raise BranchAlreadyExistsError(branch_id) from exc
                    if "branch not found" in message:
                        raise BranchNotFoundError(from_branch) from exc
                    if (
                        "terminal branch is not branchable" in message
                        or "interval space exhausted" in message
                        or "branch head changed" in message
                    ):
                        raise BranchingError(message) from exc
                    if (
                        self.db.dialect == "sqlite"
                        and "database is locked" in message.lower()
                        and attempt < 7
                    ):
                        time.sleep(0.05 * (2**attempt))
                        continue
                    raise
        raise BranchingError("native interval branch store is unavailable")

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
        self._create_branch_from_checkpoint_unlocked(branch_id, checkpoint)

    def _create_branch_from_checkpoint_unlocked(
        self, branch_id: str, checkpoint: str
    ) -> None:
        if self._native_branch_store is not None:
            try:
                self._native_branch_store.create_branch_from_checkpoint(
                    branch_id, checkpoint
                )
            except Exception as exc:
                message = str(exc)
                if "branch already exists" in message:
                    raise BranchAlreadyExistsError(branch_id) from exc
                if "branch not found" in message:
                    raise BranchNotFoundError(f"checkpoint:{checkpoint}") from exc
                raise
            self._invalidate_native_branch_sessions()
            return
        raise BranchingError("native interval branch store is unavailable")

    def update_branch_metadata(
        self, branch_id: str, metadata: dict[str, Any]
    ) -> BranchInfo:
        if self._native_branch_store is not None:
            try:
                row = self._native_branch_store.update_branch_metadata(
                    branch_id,
                    _json_dumps(metadata),
                )
            except Exception as exc:
                if "branch not found" in str(exc):
                    raise BranchNotFoundError(branch_id) from exc
                raise
            return BranchInfo(
                branch_id=row["branch_id"],
                current_ref=row["current_ref"],
                backend=self.name,
                created_at=row["created_at"],
                metadata=_json_loads(row["metadata_json"]),
            )
        raise BranchingError("native interval branch store is unavailable")

    def delete_branch(self, branch_id: str) -> None:
        if self._native_branch_store is not None:
            try:
                self._native_branch_store.delete_branch(branch_id)
            except Exception as exc:
                message = str(exc)
                if "main cannot be deleted" in message:
                    raise BranchingError("main cannot be deleted") from exc
                if "branch not found" in message:
                    raise BranchNotFoundError(branch_id) from exc
                raise
            self._interval_gc_requested = True
            self._invalidate_native_branch_sessions()
            return
        raise BranchingError("native interval branch store is unavailable")

    def list_branches(self) -> list[BranchInfo]:
        if self._native_branch_store is not None:
            manage_native_tx = self._manage_native_autocommit()
            try:
                rows = self._native_branch_store.list_branch_infos()
            except Exception:
                self._rollback_native_autocommit(manage_native_tx)
                raise
            self._commit_native_autocommit(manage_native_tx)
            return [
                BranchInfo(
                    branch_id=row["branch_id"],
                    current_ref=row["current_ref"],
                    backend=self.name,
                    created_at=row["created_at"],
                    metadata=_json_loads(row["metadata_json"]),
                )
                for row in rows
            ]
        raise BranchingError("native interval branch store is unavailable")

    def get_branch(self, branch_id: str) -> BranchInfo:
        if self._native_branch_store is None:
            self._initialize_native_branch_store()
        manage_native_tx = self._manage_native_autocommit()
        try:
            row = self._native_branch_store.get_branch_info(branch_id)
        except Exception as exc:
            self._rollback_native_autocommit(manage_native_tx)
            if "branch not found" in str(exc):
                raise BranchNotFoundError(branch_id) from exc
            raise
        self._commit_native_autocommit(manage_native_tx)
        return BranchInfo(
            branch_id=row["branch_id"],
            current_ref=row["current_ref"],
            backend=self.name,
            created_at=row["created_at"],
            metadata=_json_loads(row["metadata_json"]),
        )

    def create_checkpoint(
        self, checkpoint: str, branch: str, metadata: dict[str, Any] | None = None
    ) -> CheckpointInfo:
        return self._create_checkpoint_unlocked(checkpoint, branch, metadata)

    def _create_checkpoint_unlocked(
        self, checkpoint: str, branch: str, metadata: dict[str, Any] | None = None
    ) -> CheckpointInfo:
        if self._native_branch_store is not None:
            try:
                row = self._native_branch_store.create_checkpoint(
                    checkpoint,
                    branch,
                    _json_dumps(metadata),
                    self.continuation_percent,
                )
            except Exception as exc:
                message = str(exc)
                if "branch already exists" in message:
                    raise BranchAlreadyExistsError(checkpoint) from exc
                if "branch not found" in message:
                    raise BranchNotFoundError(branch) from exc
                if "terminal branch cannot be checkpointed" in message:
                    raise BranchingError(message) from exc
                raise
            self._invalidate_native_branch_sessions()
            return CheckpointInfo(
                row["checkpoint_id"],
                row["branch_id"],
                row["ref"],
                row["created_at"],
                _json_loads(row["metadata_json"]),
            )
        raise BranchingError("native interval branch store is unavailable")

    def get_checkpoint(self, checkpoint: str) -> CheckpointInfo:
        if self._native_branch_store is not None:
            manage_native_tx = self._manage_native_autocommit()
            try:
                cp = self._native_branch_store.get_checkpoint_info(checkpoint)
            except Exception as exc:
                self._rollback_native_autocommit(manage_native_tx)
                if "branch not found" in str(exc):
                    raise BranchNotFoundError(f"checkpoint:{checkpoint}") from exc
                raise
            self._commit_native_autocommit(manage_native_tx)
            return CheckpointInfo(
                cp["checkpoint_id"],
                cp["branch_id"],
                cp["ref"],
                cp["created_at"],
                _json_loads(cp["metadata_json"]),
            )
        raise BranchingError("native interval branch store is unavailable")

    def list_checkpoints(
        self,
        branch: str | None = None,
        metadata_filter: dict[str, Any] | None = None,
    ) -> list[CheckpointInfo]:
        if self._native_branch_store is not None:
            manage_native_tx = self._manage_native_autocommit()
            try:
                rows = self._native_branch_store.list_checkpoint_infos(branch or "")
            except Exception:
                self._rollback_native_autocommit(manage_native_tx)
                raise
            self._commit_native_autocommit(manage_native_tx)
            infos = [
                CheckpointInfo(
                    row["checkpoint_id"],
                    row["branch_id"],
                    row["ref"],
                    row["created_at"],
                    _json_loads(row["metadata_json"]),
                )
                for row in rows
            ]
            if metadata_filter:
                infos = [
                    info
                    for info in infos
                    if all(
                        info.metadata.get(key) == value
                        for key, value in metadata_filter.items()
                    )
                ]
            return infos
        raise BranchingError("native interval branch store is unavailable")

    def checkout_checkpoint(self, checkpoint: str) -> _BranchRef:
        cp = self.get_checkpoint(checkpoint)
        return _BranchRef(cp.branch_id, cp.ref, readonly=True)

    def prepare_ref(self, ref: _BranchRef) -> _PreparedBranchRef:
        # Cache the segment and table rewrites once per checkout. For repeated
        # queries this avoids metadata SELECTs before every statement.
        segment, tables, known_schema_tables = self._native_prepare_ref_metadata(
            int(ref.ref)
        )
        self._native_branch_session_segment_hints[ref.branch_id] = segment.segment_id
        has_decimal_columns = any(
            "DECIMAL" in definition.upper() or "NUMERIC" in definition.upper()
            for meta in tables.values()
            for definition in meta.column_defs
        )
        return _PreparedBranchRef(
            ref.branch_id,
            ref.ref,
            ref.readonly,
            {
                "segment": segment,
                "tables": tables,
                "has_decimal_columns": has_decimal_columns,
                "known_schema_tables": known_schema_tables,
            },
        )

    def _native_prepare_ref_metadata(
        self, segment_id: int
    ) -> tuple[_IntervalSegment, dict[str, _TableMeta], set[str]]:
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        manage_native_tx = self._manage_native_autocommit()
        try:
            info = self._native_branch_store.prepare_ref_info(
                int(segment_id),
                self.enable_schema_branching,
            )
        except Exception as exc:
            self._rollback_native_autocommit(manage_native_tx)
            message = str(exc)
            if "segment not found" in message:
                raise BranchNotFoundError(f"segment:{segment_id}") from exc
            raise
        self._commit_native_autocommit(manage_native_tx)

        segment = self._segment_from_native_dict(info["segment"])
        tables = {
            row["table_name"]: self._table_meta_from_native_dict(row)
            for row in info["tables"]
        }
        known_schema_tables = set(info["known_schema_tables"])
        return segment, tables, known_schema_tables

    def _segment_from_native_dict(self, row: Mapping[str, Any]) -> _IntervalSegment:
        return _IntervalSegment(
            segment_id=int(row["segment_id"]),
            live_lo=int(row["live_lo"]),
            live_hi=int(row["live_hi"]),
            branch_point=int(row["branch_point"]),
        )

    def _table_meta_from_native_dict(self, row: Mapping[str, Any]) -> _TableMeta:
        return _TableMeta(
            name=str(row["table_name"]),
            physical_name=str(row["physical_table"]),
            pk_columns=tuple(row["pk_columns"]),
            columns=tuple(row["columns"]),
            column_defs=tuple(row["column_defs"]),
            backend=self.name,
        )

    def refresh_ref_after_execute(self, ref: _PreparedBranchRef) -> _PreparedBranchRef:
        if not self.enable_schema_branching:
            return ref
        return self.prepare_ref(_BranchRef(ref.branch_id, ref.ref, ref.readonly))

    def adapter_transaction_required(self, ref: _PreparedBranchRef) -> bool:
        return False

    def prepare_transaction(self, ref: _PreparedBranchRef) -> None:
        if ref.readonly:
            return
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        self._native_branch_transaction_active = True

    def commit_transaction(self, ref: _PreparedBranchRef) -> _PreparedBranchRef:
        try:
            if (
                self._native_branch_store is not None
                and self._native_branch_store.in_transaction()
            ):
                self._native_branch_store.commit()
            return self.refresh_ref_after_execute(ref)
        finally:
            self._native_branch_transaction_active = False

    def rollback_transaction(self, ref: _PreparedBranchRef) -> None:
        try:
            if (
                self._native_branch_store is not None
                and self._native_branch_store.in_transaction()
            ):
                self._native_branch_store.rollback()
            return None
        finally:
            self._native_branch_transaction_active = False

    def query(
        self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        segment = self._prepared_segment(ref)
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        native_session = self._native_branch_session_for_segment(ref.branch_id, segment)
        manage_native_tx = self._manage_native_autocommit()
        try:
            rows = native_session.query(sql, params)
            rows = self._coerce_native_query_rows(ref, sql, rows)
        except Exception as exc:
            self._rollback_native_autocommit(manage_native_tx)
            message = str(exc)
            if "chronos_table_not_registered:" in message:
                table = message.rsplit("chronos_table_not_registered:", 1)[-1].strip()
                raise TableNotRegisteredError(table) from exc
            if "table is not registered for interval branching" in message:
                raise TableNotRegisteredError(message) from exc
            raise
        self._commit_native_autocommit(manage_native_tx)
        return rows

    def explain(
        self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        segment = self._prepared_segment(ref)
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        native_session = self._native_branch_session_for_segment(ref.branch_id, segment)
        manage_native_tx = self._manage_native_autocommit()
        try:
            rows = native_session.explain(sql, params)
        except Exception as exc:
            self._rollback_native_autocommit(manage_native_tx)
            message = str(exc)
            if "chronos_table_not_registered:" in message:
                table = message.rsplit("chronos_table_not_registered:", 1)[-1].strip()
                raise TableNotRegisteredError(table) from exc
            if "table is not registered for interval branching" in message:
                raise TableNotRegisteredError(message) from exc
            raise
        self._commit_native_autocommit(manage_native_tx)
        return rows

    def rewrite_query(
        self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]
    ) -> str:
        segment = self._prepared_segment(ref)
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        native_session = self._native_branch_session_for_segment(ref.branch_id, segment)
        manage_native_tx = self._manage_native_autocommit()
        try:
            rewritten = native_session.rewrite_query(sql, params)
        except Exception as exc:
            self._rollback_native_autocommit(manage_native_tx)
            message = str(exc)
            if "chronos_table_not_registered:" in message:
                table = message.rsplit("chronos_table_not_registered:", 1)[-1].strip()
                raise TableNotRegisteredError(table) from exc
            if "table is not registered for interval branching" in message:
                raise TableNotRegisteredError(message) from exc
            raise
        self._commit_native_autocommit(manage_native_tx)
        return rewritten

    def _coerce_native_query_rows(
        self,
        ref: _PreparedBranchRef,
        sql: str,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if self.db.dialect != "postgres" or not rows:
            return rows
        ref_tables = self._tables_for_ref(ref)
        if not ref.metadata.get("has_decimal_columns", False):
            return rows
        decimal_columns = {
            column
            for meta in ref_tables.values()
            for column, definition in zip(meta.columns, meta.column_defs)
            if "DECIMAL" in definition.upper() or "NUMERIC" in definition.upper()
        }
        if not decimal_columns:
            return rows
        for row in rows:
            for column in decimal_columns & set(row):
                value = row[column]
                if isinstance(value, str):
                    row[column] = Decimal(value)
        return rows

    def execute(
        self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]
    ) -> ExecuteResult:
        if ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
        segment = self._prepared_segment(ref)
        if self._is_schema_statement(sql):
            if not self.enable_schema_branching:
                raise UnsupportedSQLError("branch-local schema changes are disabled")
            if self._native_branch_store is None:
                raise BranchingError("native interval branch store is unavailable")
            native_session = self._native_branch_session_for_segment(
                ref.branch_id,
                segment,
            )
            try:
                result = ExecuteResult(native_session.execute_schema(sql))
            except Exception as exc:
                message = str(exc)
                unsupported_markers = (
                    "unsupported",
                    "dropping primary key columns",
                    "CREATE TABLE requires",
                    "column constraints",
                )
                if any(marker in message for marker in unsupported_markers):
                    raise UnsupportedSQLError(message) from exc
                if "branch already exists" in message:
                    raise BranchAlreadyExistsError(message) from exc
                if "table is not registered" in message:
                    raise TableNotRegisteredError(message) from exc
                raise
            if (
                not self.db.in_transaction
                and not self._native_branch_transaction_active
            ):
                self._start_async_schema_indexes()
            ref.metadata.clear()
            ref.metadata.update(
                self.prepare_ref(
                    _BranchRef(ref.branch_id, ref.ref, ref.readonly)
                ).metadata
            )
            return result
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        native_session = self._native_branch_session(ref.branch_id)
        manage_native_tx = (
            not self.db.in_transaction and not self._native_branch_transaction_active
        )
        started_native_tx = False
        try:
            if not self._native_branch_store.in_transaction():
                begin = getattr(native_session, "begin", None)
                if callable(begin):
                    begin()
                else:
                    self._native_branch_store.checkout(ref.branch_id).begin()
                started_native_tx = True
            result = ExecuteResult(native_session.execute(sql, params))
            if manage_native_tx and started_native_tx:
                commit = getattr(native_session, "commit", None)
                if callable(commit):
                    commit()
                else:
                    self._native_branch_store.commit()
            return result
        except Exception as exc:
            if started_native_tx:
                try:
                    rollback = getattr(native_session, "rollback", None)
                    if callable(rollback):
                        rollback()
                    else:
                        self._native_branch_store.rollback()
                except Exception:
                    pass
            if "duplicate key" in str(exc).lower():
                raise DuplicateKeyError(str(exc)) from exc
            if "unsupported native branch SQL statement" in str(exc):
                raise UnsupportedSQLError(str(exc)) from exc
            raise

    @staticmethod
    def _is_schema_statement(sql: str) -> bool:
        head = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
        return head in {"CREATE", "ALTER", "DROP"}

    def visible_rows(self, branch_id: str, table: str) -> list[dict[str, Any]]:
        ref = self.prepare_ref(
            _BranchRef(branch_id, self._branch_segment_id(branch_id))
        )
        return self.query(ref, f"SELECT * FROM {_quote_table_name(table)}", {})

    def diff_tables(self) -> list[str]:
        if not self.enable_schema_branching:
            return super().diff_tables()
        return sorted(self._known_schema_tables())

    def table_meta_for_branch(self, branch_id: str, table: str) -> _TableMeta:
        if not self.enable_schema_branching:
            return super().table_meta_for_branch(branch_id, table)
        return self._meta_for_segment(self._current_segment(branch_id), table)

    def diff_rows(self, left: str, right: str, table: str) -> list[RowDiff] | None:
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        manage_native_tx = self._manage_native_autocommit()
        try:
            native_diffs = self._native_branch_store.diff_rows(left, right, table)
        except Exception as exc:
            self._rollback_native_autocommit(manage_native_tx)
            message = str(exc)
            if "table is not registered for interval branching" in message:
                return []
            if "chronos_native_diff_unsupported" in message:
                raise UnsupportedSQLError(message) from exc
            raise
        self._commit_native_autocommit(manage_native_tx)
        return [self._native_row_diff(diff) for diff in native_diffs]

    def _native_row_diff(self, diff: Mapping[str, Any]) -> RowDiff:
        return RowDiff(
            table=str(diff["table"]),
            key=dict(diff["key"]),
            change=diff["change"],
            before=dict(diff["before"]) if diff["before"] is not None else None,
            after=dict(diff["after"]) if diff["after"] is not None else None,
        )

    def _row_diff_to_native_dict(self, diff: RowDiff) -> dict[str, Any]:
        return {
            "table": diff.table,
            "key": dict(diff.key),
            "change": diff.change,
            "before": dict(diff.before) if diff.before is not None else None,
            "after": dict(diff.after) if diff.after is not None else None,
        }

    def _snapshot_diff_rows(
        self, left: str, right: str, table: str, meta: _TableMeta
    ) -> list[RowDiff]:
        """Compatibility slow-path used by sparse-diff benchmarks.

        Production diff/merge uses the native change-proportional path above.
        This helper deliberately materializes both branch-visible snapshots
        through native sessions so tests can keep comparing the optimized path
        against a full-scan baseline without reintroducing Python SQL rewrites.
        """

        left_rows = {
            self._key_tuple(meta, self._row_key(meta, row)): row
            for row in self.visible_rows(left, table)
        }
        right_rows = {
            self._key_tuple(meta, self._row_key(meta, row)): row
            for row in self.visible_rows(right, table)
        }
        diffs: list[RowDiff] = []
        for key_tuple in sorted(set(left_rows) | set(right_rows)):
            before = left_rows.get(key_tuple)
            after = right_rows.get(key_tuple)
            if before == after:
                continue
            key = dict(zip(meta.pk_columns, key_tuple))
            if before is None:
                change = "added"
            elif after is None:
                change = "removed"
            else:
                change = "modified"
            diffs.append(RowDiff(table, key, change, before, after))
        native_order = {
            self._key_tuple(meta, diff.key): offset
            for offset, diff in enumerate(self.diff_rows(left, right, table) or [])
        }
        diffs.sort(
            key=lambda diff: native_order.get(
                self._key_tuple(meta, diff.key),
                len(native_order),
            )
        )
        return diffs

    def merge_preview(self, source: str, target: str) -> MergePreview | None:
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        manage_native_tx = self._manage_native_autocommit()
        try:
            preview = self._native_branch_store.merge_preview(source, target)
        except Exception as exc:
            self._rollback_native_autocommit(manage_native_tx)
            message = str(exc)
            if "chronos_native_merge_unsupported" in message:
                raise UnsupportedSQLError(message) from exc
            raise
        self._commit_native_autocommit(manage_native_tx)
        return MergePreview(
            source=source,
            target=target,
            changes=[self._native_row_diff(diff) for diff in preview["changes"]],
            conflicts=[self._native_row_diff(diff) for diff in preview["conflicts"]],
        )

    def merge_preview_tables(
        self,
        source: str,
        target: str,
        tables: list[str],
    ) -> MergePreview:
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        manage_native_tx = self._manage_native_autocommit()
        try:
            preview = self._native_branch_store.merge_preview_tables(
                source, target, tables
            )
        except Exception as exc:
            self._rollback_native_autocommit(manage_native_tx)
            message = str(exc)
            if "chronos_native_merge_unsupported" in message:
                raise UnsupportedSQLError(message) from exc
            raise
        self._commit_native_autocommit(manage_native_tx)
        return MergePreview(
            source=source,
            target=target,
            changes=[self._native_row_diff(diff) for diff in preview["changes"]],
            conflicts=[self._native_row_diff(diff) for diff in preview["conflicts"]],
        )

    def reserve_branch_transaction(
        self,
        source: str,
        target: str,
        participant_stores: list[str],
        metadata: dict[str, Any] | None = None,
    ) -> BranchTransaction:
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        native = self._native_branch_store.reserve_branch_transaction(
            source,
            target,
            _json_dumps(sorted(set(participant_stores))),
            _json_dumps(metadata),
        )
        self._invalidate_native_branch_sessions()
        return BranchTransaction(
            source=source,
            target=target,
            merge_segment_id=int(native.merge_segment_id),
            continuation_segment_id=int(native.continuation_segment_id),
            old_target_segment_id=int(native.old_target_segment_id),
            merge_live_lo=str(native.merge_live_lo),
            merge_live_hi=str(native.merge_live_hi),
            merge_branch_point=str(native.merge_branch_point),
            continuation_live_lo=str(native.continuation_live_lo),
            continuation_live_hi=str(native.continuation_live_hi),
            continuation_branch_point=str(native.continuation_branch_point),
            _native=native,
        )

    def stage_branch_transaction_changes(
        self,
        transaction: BranchTransaction,
        changes: list[RowDiff],
    ) -> int:
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        return int(
            self._native_branch_store.stage_branch_transaction_changes(
                transaction._native,
                [self._row_diff_to_native_dict(change) for change in changes],
            )
        )

    def publish_branch_transaction(self, transaction: BranchTransaction) -> None:
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        self._native_branch_store.publish_branch_transaction(
            transaction.target, transaction._native
        )
        self._invalidate_native_branch_sessions()

    def abort_branch_transaction(self, transaction: BranchTransaction) -> None:
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        self._native_branch_store.abort_branch_transaction(
            transaction.target, transaction._native
        )
        self._invalidate_native_branch_sessions()

    def merge_apply(
        self,
        source: str,
        target: str,
        resolution: MergeResolution | None = None,
    ) -> MergeResult | None:
        if resolution is not None:
            preview = self.merge_preview(source, target)
            if preview is None:
                raise UnsupportedSQLError(
                    "native interval merge preview is unavailable"
                )
            changes = _resolve_merge_changes(
                preview,
                "manual_review",
                resolution,
                backend=self.name,
            )
            applied = self.apply_merge_changes(source, target, changes)
            if applied is None:
                raise UnsupportedSQLError(
                    "native interval resolved merge apply is unavailable"
                )
            return MergeResult(source=source, target=target, applied=applied)
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        try:
            applied = int(self._native_branch_store.merge_apply(source, target))
        except Exception as exc:
            message = str(exc)
            if "chronos_native_merge_conflict" in message:
                raise BranchingError(
                    "write-write conflict during native interval merge"
                ) from exc
            if "chronos_native_merge_unsupported" in message:
                raise UnsupportedSQLError(message) from exc
            raise
        self._invalidate_native_branch_sessions()
        return MergeResult(source=source, target=target, applied=applied)

    def apply_merge_changes(
        self,
        source: str,
        target: str,
        changes: list[RowDiff],
    ) -> int | None:
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        try:
            applied = int(
                self._native_branch_store.apply_merge_changes(
                    source,
                    target,
                    [self._row_diff_to_native_dict(change) for change in changes],
                )
            )
            self._invalidate_native_branch_sessions()
            return applied
        except Exception as exc:
            message = str(exc)
            if "chronos_native_merge_unsupported" in message:
                raise UnsupportedSQLError(message) from exc
            raise

    def upsert_row(self, branch_id: str, table: str, row: dict[str, Any]) -> None:
        self.upsert_rows(branch_id, table, [row])

    def upsert_rows(
        self, branch_id: str, table: str, rows: list[dict[str, Any]]
    ) -> None:
        if not rows:
            return
        segment = self._current_segment(branch_id)
        meta = self._meta_for_segment(segment, table)
        self._upsert_rows_in_segment(branch_id, table, rows, segment, meta)

    def _upsert_rows_in_segment(
        self,
        branch_id: str,
        table: str,
        rows: list[dict[str, Any]],
        segment: _IntervalSegment,
        meta: _TableMeta,
    ) -> None:
        keyed: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in rows:
            keyed[self._key_tuple(meta, self._row_key(meta, row))] = row
        deduped = list(keyed.values())
        self._try_native_interval_bulk_upsert(branch_id, table, deduped, segment, meta)

    def _try_native_interval_bulk_upsert(
        self,
        branch_id: str,
        table: str,
        rows: list[dict[str, Any]],
        segment: _IntervalSegment,
        meta: _TableMeta,
    ) -> bool:
        if not rows:
            return True
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        native_session = self._native_branch_session_for_segment(branch_id, segment)
        if native_session is not None:
            started_db_tx = False
            started_native_tx = False
            try:
                if (
                    not self.db.in_transaction
                    and not self._native_branch_transaction_active
                ):
                    self.db.begin()
                    started_db_tx = True
                if not self._native_branch_store.in_transaction():
                    native_session.begin()
                    started_native_tx = True
                native_session.upsert_rows(
                    table,
                    list(meta.columns),
                    list(meta.pk_columns),
                    rows,
                )
                return True
            except Exception:
                if started_native_tx:
                    try:
                        native_session.rollback()
                    except Exception:
                        pass
                if started_db_tx:
                    try:
                        self.db.rollback()
                    except Exception:
                        pass
                raise
        raise BranchingError("native interval DML executor is unavailable")

    def _try_native_interval_splice(
        self,
        branch_id: str,
        table: str,
        key: dict[str, Any],
        row: dict[str, Any] | None,
        deleted: bool,
        segment: _IntervalSegment,
        meta: _TableMeta,
    ) -> bool:
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        native_session = self._native_branch_session_for_segment(branch_id, segment)
        if native_session is not None:
            if row is None and deleted:
                replacement = {column: None for column in meta.columns}
                replacement.update(key)
            else:
                replacement = {column: row.get(column) for column in meta.columns}  # type: ignore[union-attr]
            started_db_tx = False
            started_native_tx = False
            if (
                not self.db.in_transaction
                and not self._native_branch_transaction_active
            ):
                self.db.begin()
                started_db_tx = True
            try:
                if not self._native_branch_store.in_transaction():
                    native_session.begin()
                    started_native_tx = True
                if deleted:
                    native_session.delete_rows(
                        table,
                        list(meta.columns),
                        list(meta.pk_columns),
                        [replacement],
                    )
                else:
                    native_session.upsert_rows(
                        table,
                        list(meta.columns),
                        list(meta.pk_columns),
                        [replacement],
                    )
                return True
            except Exception:
                if started_native_tx:
                    try:
                        native_session.rollback()
                    except Exception:
                        pass
                if started_db_tx:
                    try:
                        self.db.rollback()
                    except Exception:
                        pass
                raise
        raise BranchingError("native interval DML executor is unavailable")

    def delete_key(self, branch_id: str, table: str, key: dict[str, Any]) -> None:
        segment = self._current_segment(branch_id)
        meta = self._meta_for_segment(segment, table)
        self._splice_row(table, key, None, True, branch_id, segment=segment, meta=meta)

    def delete_keys(
        self, branch_id: str, table: str, keys: list[dict[str, Any]]
    ) -> None:
        if not keys:
            return
        segment = self._current_segment(branch_id)
        meta = self._meta_for_segment(segment, table)
        self._delete_keys_in_segment(branch_id, table, keys, segment, meta)

    def _delete_keys_in_segment(
        self,
        branch_id: str,
        table: str,
        keys: list[dict[str, Any]],
        segment: _IntervalSegment,
        meta: _TableMeta,
    ) -> None:
        keyed: dict[tuple[Any, ...], dict[str, Any]] = {}
        for key in keys:
            keyed[self._key_tuple(meta, key)] = key
        deduped = list(keyed.values())
        if not deduped:
            return
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        native_session = self._native_branch_session_for_segment(branch_id, segment)
        if native_session is None:
            raise BranchingError("native interval DML executor is unavailable")
        tombstones = []
        for key in deduped:
            tombstone = {column: None for column in meta.columns}
            tombstone.update(key)
            tombstones.append(tombstone)
        started_db_tx = False
        started_native_tx = False
        try:
            if (
                not self.db.in_transaction
                and not self._native_branch_transaction_active
            ):
                self.db.begin()
                started_db_tx = True
            if not self._native_branch_store.in_transaction():
                native_session.begin()
                started_native_tx = True
            native_session.delete_rows(
                table,
                list(meta.columns),
                list(meta.pk_columns),
                tombstones,
            )
        except Exception:
            if started_native_tx:
                try:
                    native_session.rollback()
                except Exception:
                    pass
            if started_db_tx:
                try:
                    self.db.rollback()
                except Exception:
                    pass
            raise

    def _tables_for_ref(self, ref: _PreparedBranchRef) -> dict[str, _TableMeta]:
        tables = ref.metadata.get("tables")
        if tables is not None:
            return tables
        return self.tables

    def _meta_for_segment(self, segment: _IntervalSegment, table: str) -> _TableMeta:
        if not self.enable_schema_branching:
            return self._require_table(table)
        _, active, _ = self._native_prepare_ref_metadata(segment.segment_id)
        try:
            return active[table]
        except KeyError as exc:
            raise TableNotRegisteredError(table) from exc

    def _known_schema_tables(self) -> set[str]:
        if not self.enable_schema_branching:
            return set(self.tables)
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        manage_native_tx = self._manage_native_autocommit()
        try:
            tables = set(self._native_branch_store.known_schema_tables())
        except Exception:
            self._rollback_native_autocommit(manage_native_tx)
            raise
        self._commit_native_autocommit(manage_native_tx)
        return tables

    def _key_tuple(self, meta: _TableMeta, key: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(key[column] for column in meta.pk_columns)

    def _splice_row(
        self,
        table: str,
        key: dict[str, Any],
        row: dict[str, Any] | None,
        deleted: bool,
        branch_id: str,
        segment: _IntervalSegment | None = None,
        meta: _TableMeta | None = None,
    ) -> None:
        segment = segment or self._current_segment(branch_id)
        meta = meta or self._meta_for_segment(segment, table)
        self._try_native_interval_splice(
            branch_id, table, key, row, deleted, segment, meta
        )

    def _segment_ancestry_rows(self, segment_id: int) -> list[dict[str, Any]]:
        """Compatibility probe; native merge must not depend on this Python walk."""

        raise BranchingError("segment ancestry is owned by the native interval backend")

    def lock_branches_for_merge(self, source: str, target: str) -> None:
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        lock_branches = getattr(
            self._native_branch_store, "lock_branches_for_merge", None
        )
        if callable(lock_branches):
            try:
                lock_branches(source, target)
            except Exception as exc:
                if "branch not found" in str(exc):
                    missing = source if source in str(exc) else target
                    raise BranchNotFoundError(missing) from exc
                raise
            return
        raise BranchingError("native merge branch locking is unavailable")

    def _branch_segment_id(self, branch_id: str) -> int:
        if self._native_branch_store is None:
            self._initialize_native_branch_store()
        if self._native_branch_store is None:
            raise BranchingError("native interval branch store is unavailable")
        manage_native_tx = self._manage_native_autocommit()
        try:
            row = self._native_branch_store.get_branch_info(branch_id)
        except Exception as exc:
            self._rollback_native_autocommit(manage_native_tx)
            if "branch not found" in str(exc):
                raise BranchNotFoundError(branch_id) from exc
            raise
        self._commit_native_autocommit(manage_native_tx)
        return int(row["current_ref"])

    def _segment(self, segment_id: int | str) -> _IntervalSegment:
        segment, _, _ = self._native_prepare_ref_metadata(int(segment_id))
        return segment

    def _segment_for_ref(self, ref: _BranchRef) -> _IntervalSegment:
        return self._segment(ref.ref)

    def _current_segment(self, branch_id: str) -> _IntervalSegment:
        return self._segment(self._branch_segment_id(branch_id))

    def _prepared_segment(self, ref: _PreparedBranchRef) -> _IntervalSegment:
        segment = ref.metadata.get("segment")
        if isinstance(segment, _IntervalSegment):
            return segment
        return self._segment_for_ref(_BranchRef(ref.branch_id, ref.ref, ref.readonly))
