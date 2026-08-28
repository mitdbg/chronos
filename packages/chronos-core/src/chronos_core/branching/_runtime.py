from __future__ import annotations

import functools

from chronos_core.branching._common import *
from chronos_core.branching._copy_backend import _CopyBackend
from chronos_core.branching._interval_backend import _IntervalBackend
from chronos_core.branching._litetree_backend import _LiteTreeBackend
from chronos_core.branching._log_backend import _LogBackend
from chronos_core.branching._orpheus_backend import _OrpheusBackend
from chronos_core.branching._session_epoch import (
    SessionEpochCoordinator,
    SessionEpochHandle,
)

_INTERVAL_UNIQUE_RETRY_LIMIT = 3


def _session_epoch_operation(method: Any) -> Any:
    @functools.wraps(method)
    def wrapped(self: BranchSession, *args: Any, **kwargs: Any) -> Any:
        with self._operation_epoch():
            return method(self, *args, **kwargs)

    return wrapped


def _is_retryable_interval_unique_violation(exc: Exception) -> bool:
    message = str(exc)
    return (
        "UniqueViolation" in type(exc).__name__
        or "duplicate key value violates unique constraint" in message
    ) and "_chronos_b_interval_" in message


class BranchSession:
    """Checked-out branch handle used by agents and applications.

    A session owns a prepared branch reference. It is cheap to reuse for many
    statements, and its transaction context groups several SQL mutations into
    one database transaction.
    """

    def __init__(self, context: ChronosBranchContext, ref: _PreparedBranchRef):
        self._context = context
        self._ref = ref
        self._transaction_depth = 0
        self._epoch_handle: SessionEpochHandle | None = None
        self._closed = False

    @property
    def branch_id(self) -> str:
        return self._ref.branch_id

    @property
    @_session_epoch_operation
    def current_ref(self) -> str:
        """Current writable branch reference observed by this live session."""

        self._ensure_fresh()
        return self._ref.ref

    @_session_epoch_operation
    def query(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        self._ensure_fresh()
        try:
            rows = self._context._backend.query(self._ref, sql, _ensure_params(params))
        except Exception:
            if self._transaction_depth == 0:
                self._context._rollback_autocommit()
            raise
        if self._transaction_depth == 0 and self._context._db.in_transaction:
            self._context._commit_autocommit()
        return rows

    @_session_epoch_operation
    def explain(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        self._ensure_fresh()
        try:
            rows = self._context._backend.explain(
                self._ref, sql, _ensure_params(params)
            )
        except Exception:
            if self._transaction_depth == 0:
                self._context._rollback_autocommit()
            raise
        if self._transaction_depth == 0 and self._context._db.in_transaction:
            self._context._commit_autocommit()
        return rows

    @_session_epoch_operation
    def rewrite_query(self, sql: str, params: dict[str, Any] | None = None) -> str:
        self._ensure_fresh()
        try:
            return self._context._backend.rewrite_query(
                self._ref, sql, _ensure_params(params)
            )
        except Exception:
            if self._transaction_depth == 0:
                self._context._rollback_autocommit()
            raise

    @_session_epoch_operation
    def execute(self, sql: str, params: dict[str, Any] | None = None) -> ExecuteResult:
        self._ensure_fresh()
        if self._transaction_depth == 0 and self._context._db.in_transaction:
            self._context._db.commit()
            self._context._backend.after_commit()
        if self._transaction_depth == 0 and not self._context._autocommit:
            self._context._db.begin()
        retry_limit = (
            _INTERVAL_UNIQUE_RETRY_LIMIT if self._transaction_depth == 0 else 1
        )
        for attempt in range(retry_limit):
            try:
                result = self._context._backend.execute(
                    self._ref, sql, _ensure_params(params)
                )
                # Some backends mutate the branch head on write. Refresh the prepared
                # metadata so later reads in the same session see their own writes.
                self._ref = self._context._backend.refresh_ref_after_execute(self._ref)
                self._context._stamp_prepared_ref(self._ref)
            except Exception as exc:
                if self._transaction_depth == 0:
                    self._context._rollback_autocommit()
                    if (
                        attempt + 1 < retry_limit
                        and _is_retryable_interval_unique_violation(exc)
                    ):
                        self._ensure_fresh()
                        continue
                raise
            if self._transaction_depth == 0:
                self._context._commit_autocommit()
            return result
        raise AssertionError("unreachable")

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        """Group several session writes in one underlying SQL transaction."""

        with self._operation_epoch():
            with self._transaction_impl():
                yield

    @contextlib.contextmanager
    def _transaction_impl(self) -> Iterator[None]:

        root = self._transaction_depth == 0
        shared_root = root and self._context._shared_transaction_depth == 0
        use_adapter_transaction = True
        if root and not shared_root:
            self._ensure_fresh()
        if shared_root:
            self._ensure_fresh()
            if self._context._db.in_transaction:
                self._context._db.commit()
                self._context._backend.after_commit()
            required = getattr(
                self._context._backend,
                "adapter_transaction_required",
                None,
            )
            if callable(required):
                use_adapter_transaction = bool(required(self._ref))
            self._context._backend.prepare_transaction(self._ref)
            if use_adapter_transaction:
                self._context._db.begin()
            self._context._shared_transaction_failed = False
        if root:
            self._context._shared_transaction_depth += 1
        self._transaction_depth += 1
        try:
            yield
        except Exception:
            self._transaction_depth -= 1
            if root:
                self._context._shared_transaction_depth -= 1
                self._context._shared_transaction_failed = True
            if shared_root:
                rollback = getattr(self._context._backend, "rollback_transaction", None)
                if callable(rollback):
                    rollback(self._ref)
                if use_adapter_transaction:
                    self._context._db.rollback()
                self._context._backend.after_rollback()
            raise
        else:
            self._transaction_depth -= 1
            if root:
                self._context._shared_transaction_depth -= 1
            if shared_root:
                try:
                    if self._context._shared_transaction_failed:
                        raise BranchingError("nested branch transaction failed")
                    commit = getattr(self._context._backend, "commit_transaction", None)
                    if callable(commit):
                        self._ref = commit(self._ref)
                        self._context._stamp_prepared_ref(self._ref)
                    if use_adapter_transaction:
                        self._context._db.commit()
                    self._context._backend.after_commit()
                except Exception:
                    rollback = getattr(
                        self._context._backend, "rollback_transaction", None
                    )
                    if callable(rollback):
                        rollback(self._ref)
                    if use_adapter_transaction:
                        self._context._db.rollback()
                    self._context._backend.after_rollback()
                    raise

    def branch_info(self) -> BranchInfo:
        return self._context.get_branch(self.branch_id)

    @_session_epoch_operation
    def upsert_rows(self, table: str, rows: list[dict[str, Any]]) -> ExecuteResult:
        self._ensure_fresh()
        if self._ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
        if self._transaction_depth == 0 and self._context._db.in_transaction:
            self._context._db.commit()
            self._context._backend.after_commit()
        try:
            self._context._backend.upsert_rows(self.branch_id, table, rows)
            self._ref = self._context._backend.refresh_ref_after_execute(self._ref)
            self._context._stamp_prepared_ref(self._ref)
        except Exception:
            if self._transaction_depth == 0:
                self._context._rollback_autocommit()
            raise
        if self._transaction_depth == 0:
            self._context._commit_autocommit()
        return ExecuteResult(len(rows))

    @_session_epoch_operation
    def delete_keys(self, table: str, keys: list[dict[str, Any]]) -> ExecuteResult:
        self._ensure_fresh()
        if self._ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
        if self._transaction_depth == 0 and self._context._db.in_transaction:
            self._context._db.commit()
            self._context._backend.after_commit()
        try:
            self._context._backend.delete_keys(self.branch_id, table, keys)
            self._ref = self._context._backend.refresh_ref_after_execute(self._ref)
            self._context._stamp_prepared_ref(self._ref)
        except Exception:
            if self._transaction_depth == 0:
                self._context._rollback_autocommit()
            raise
        if self._transaction_depth == 0:
            self._context._commit_autocommit()
        return ExecuteResult(len(keys))

    def _ensure_fresh(self) -> None:
        if self._ref.readonly:
            return
        # Branch creation/checkpointing can move interval segments. Existing
        # sessions refresh lazily when the context metadata epoch changes.
        if self._ref.metadata.get("_context_epoch") != self._context._metadata_epoch:
            current_ref = self._context.get_branch(self.branch_id).current_ref
            self._ref = self._context._prepare_ref(
                _BranchRef(self._ref.branch_id, current_ref, self._ref.readonly)
            )

    def _refresh_after_epoch(self) -> None:
        if self._ref.readonly:
            return
        current_ref = self._context.get_branch(self.branch_id).current_ref
        self._ref = self._context._prepare_ref(
            _BranchRef(self.branch_id, current_ref, self._ref.readonly)
        )

    @contextlib.contextmanager
    def _operation_epoch(self) -> Iterator[None]:
        if self._closed:
            raise RuntimeError("Chronos branch session is closed")
        coordinator = self._context._session_epochs
        if (
            self._epoch_handle is None
            and coordinator is not None
            and not self._ref.readonly
        ):
            self._epoch_handle = coordinator.register(self.branch_id)
            self._epoch_handle._needs_refresh = True
        if self._epoch_handle is None:
            self._ensure_fresh()
            yield
            return
        with self._epoch_handle.operation(self._refresh_after_epoch):
            self._ensure_fresh()
            yield

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._epoch_handle is not None:
            self._epoch_handle.close()
            self._epoch_handle = None

    def __enter__(self) -> BranchSession:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class ChronosBranchContext:
    """Branch manager for SQL-backed relational data."""

    def __init__(
        self,
        db: SQLDatabaseAdapter,
        backend: _SQLBranchBackend,
        autocommit: bool = True,
        metadata_db: SQLDatabaseAdapter | None = None,
        enable_session_epochs: bool = True,
    ):
        self._db = db
        self._metadata_db = metadata_db or db
        self._backend = backend
        self._autocommit = autocommit
        # Incremented whenever branch/table metadata changes. Checked-out
        # sessions compare against this to invalidate prepared metadata.
        self._metadata_epoch = 0
        self._merge_table_scope: tuple[str, ...] | None = None
        self._shared_transaction_depth = 0
        self._shared_transaction_failed = False
        # A workspace can expose the same interval context through multiple
        # participating stores (for example, relational rows and ChronosFS).
        # Make context teardown idempotent so closing one store does not try
        # to close the shared adapters and native backend a second time.
        self._closed = False
        self._session_epochs_enabled = bool(enable_session_epochs)
        self._session_epochs = (
            SessionEpochCoordinator.for_context(
                backend.name,
                self._metadata_db,
            )
            if self._session_epochs_enabled
            else None
        )
        set_epoch_managed = getattr(backend, "set_session_epoch_managed", None)
        if callable(set_epoch_managed):
            set_epoch_managed(
                self._session_epochs is not None
                and self._metadata_db.dialect == "postgres"
            )

    @classmethod
    def connect(
        cls,
        database_url: str,
        backend: BranchBackendName = "interval",
        autocommit: bool = True,
        interval_child_width: int | None = None,
        interval_reserve_bits: IntervalReserveBits | None = None,
        interval_harmonic_reserve: int = _INTERVAL_HARMONIC_RESERVE,
        interval_coordinate_bits: int = 0,
        interval_create_secondary_indexes: bool = True,
        interval_create_writer_segment_index: bool = True,
        ensure_metadata: bool = True,
        enable_schema_branching: bool = False,
        enable_diff_merge_tracking: bool = False,
        enable_session_epochs: bool = True,
    ) -> ChronosBranchContext:
        db = connect_sql_database(database_url)
        return cls.from_database_adapter(
            db,
            backend=backend,
            autocommit=autocommit,
            interval_child_width=interval_child_width,
            interval_reserve_bits=interval_reserve_bits,
            interval_harmonic_reserve=interval_harmonic_reserve,
            interval_coordinate_bits=interval_coordinate_bits,
            interval_create_secondary_indexes=interval_create_secondary_indexes,
            interval_create_writer_segment_index=interval_create_writer_segment_index,
            ensure_metadata=ensure_metadata,
            enable_schema_branching=enable_schema_branching,
            enable_diff_merge_tracking=enable_diff_merge_tracking,
            enable_session_epochs=enable_session_epochs,
        )

    @classmethod
    def from_database_adapter(
        cls,
        db: SQLDatabaseAdapter,
        backend: BranchBackendName = "interval",
        autocommit: bool = True,
        metadata_db: SQLDatabaseAdapter | None = None,
        interval_child_width: int | None = None,
        interval_reserve_bits: IntervalReserveBits | None = None,
        interval_harmonic_reserve: int = _INTERVAL_HARMONIC_RESERVE,
        interval_coordinate_bits: int = 0,
        interval_create_secondary_indexes: bool = True,
        interval_create_writer_segment_index: bool = True,
        ensure_metadata: bool = True,
        enable_schema_branching: bool = False,
        enable_diff_merge_tracking: bool = False,
        enable_session_epochs: bool = True,
    ) -> ChronosBranchContext:
        if backend == "interval" and db.dialect == "duckdb" and metadata_db is None:
            raise ValueError(
                "DuckDB interval stores require a transactional metadata_db; "
                "use ChronosBranchContext.connect_split(data_url, metadata_url) "
                "or ChronosDuckDBStore(data_url, metadata_url)."
            )
        if metadata_db is not None:
            if backend != "interval":
                raise ValueError(
                    "split metadata/data stores are supported only by the interval backend"
                )
            if enable_schema_branching:
                raise ValueError(
                    "schema branching is not supported for split metadata/data interval stores"
                )
            from chronos_core.branching.sql_adapters import (
                RoutedIntervalDatabaseAdapter,
            )

            db = RoutedIntervalDatabaseAdapter(db, metadata_db)

        def build_backend() -> _SQLBranchBackend:
            if backend == "interval":
                return _IntervalBackend(
                    db,
                    child_width=interval_child_width,
                    reserve_bits=interval_reserve_bits,
                    harmonic_reserve=interval_harmonic_reserve,
                    interval_coordinate_bits=interval_coordinate_bits,
                    enable_schema_branching=enable_schema_branching,
                    create_secondary_indexes=interval_create_secondary_indexes,
                    create_writer_segment_index=interval_create_writer_segment_index,
                )
            if enable_schema_branching and backend not in {"copy", "orpheus"}:
                raise ValueError(
                    "schema branching is currently supported only by the interval, copy, and orpheus backends"
                )
            if backend == "log":
                return _LogBackend(db)
            if backend == "copy":
                return _CopyBackend(db, enable_schema_branching=enable_schema_branching)
            if backend == "orpheus":
                return _OrpheusBackend(
                    db,
                    enable_schema_branching=enable_schema_branching,
                    enable_diff_merge_tracking=enable_diff_merge_tracking,
                )
            if backend == "litetree":
                return _LiteTreeBackend(db)
            raise ValueError(f"unknown branch backend: {backend}")

        impl = build_backend()
        if ensure_metadata:
            # The native interval store owns a separate PostgreSQL connection.
            # Its bootstrap lock therefore has to be acquired by the native
            # metadata driver; holding the Python adapter lock here would not
            # serialize the native DDL connection.
            if backend == "interval":
                impl.ensure()
            else:
                with _chronos_metadata_lock(metadata_db or db):
                    impl.ensure()
        else:
            initialize_native = getattr(impl, "_initialize_native_branch_store", None)
            if callable(initialize_native):
                initialize_native()
        return cls(
            db,
            impl,
            autocommit=autocommit,
            metadata_db=metadata_db,
            enable_session_epochs=enable_session_epochs,
        )

    @classmethod
    def connect_split(
        cls,
        data_url: str,
        metadata_url: str,
        backend: BranchBackendName = "interval",
        autocommit: bool = True,
        interval_child_width: int | None = None,
        interval_reserve_bits: IntervalReserveBits | None = None,
        interval_harmonic_reserve: int = _INTERVAL_HARMONIC_RESERVE,
        interval_coordinate_bits: int = 0,
        interval_create_secondary_indexes: bool = True,
        interval_create_writer_segment_index: bool = True,
        ensure_metadata: bool = True,
        enable_schema_branching: bool = False,
        enable_session_epochs: bool = True,
    ) -> ChronosBranchContext:
        data_db = connect_sql_database(data_url)
        metadata_db = connect_sql_database(metadata_url)
        if metadata_db.dialect not in {"postgres", "sqlite"}:
            raise ValueError(
                "split interval metadata store must be PostgreSQL or SQLite"
            )
        return cls.from_database_adapter(
            data_db,
            backend=backend,
            autocommit=autocommit,
            metadata_db=metadata_db,
            interval_child_width=interval_child_width,
            interval_reserve_bits=interval_reserve_bits,
            interval_harmonic_reserve=interval_harmonic_reserve,
            interval_coordinate_bits=interval_coordinate_bits,
            interval_create_secondary_indexes=interval_create_secondary_indexes,
            interval_create_writer_segment_index=interval_create_writer_segment_index,
            ensure_metadata=ensure_metadata,
            enable_schema_branching=enable_schema_branching,
            enable_session_epochs=enable_session_epochs,
        )

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, enabled: bool) -> None:
        self._autocommit = bool(enabled)

    @property
    def backend_name(self) -> str:
        return self._backend.name

    @property
    def db(self) -> SQLDatabaseAdapter:
        return self._db

    @property
    def metadata_db(self) -> SQLDatabaseAdapter:
        return self._metadata_db

    @property
    def conn(self) -> Any:
        """Return the underlying driver connection for low-level tests/tools."""
        return self._db.raw_connection

    @property
    def metadata_conn(self) -> Any:
        """Return the underlying metadata connection for split-store tools."""
        return self._metadata_db.raw_connection

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._session_epochs is not None:
            self._session_epochs.close()
            self._session_epochs = None
        close_backend = getattr(self._backend, "close", None)
        if callable(close_backend):
            close_backend()
        self._db.close()

    @contextlib.contextmanager
    def _local_session_branch_operation(
        self, branches: list[str]
    ) -> Iterator[None]:
        if self._session_epochs is None:
            yield
            return
        with self._session_epochs.local_branch_operation(branches):
            yield

    def wait_for_background_work(self) -> None:
        if self._db.in_transaction:
            self._db.commit()
        wait = getattr(self._backend, "wait_for_async_schema_indexes", None)
        if callable(wait):
            wait()
        wait_gc = getattr(self._backend, "wait_for_interval_gc", None)
        if callable(wait_gc):
            wait_gc()

    def register_table(self, table: str, primary_key: list[str]) -> None:
        try:
            with _chronos_metadata_lock(self._db):
                self._backend.refresh_registries()
                self._backend.register_table(table, primary_key)
                self._commit_autocommit()
        except Exception:
            self._rollback_autocommit()
            raise
        self._metadata_epoch += 1

    def create_index(
        self, table: str, columns: list[str], name: str | None = None
    ) -> IndexInfo:
        try:
            with _chronos_metadata_lock(self._db):
                self._backend.refresh_registries()
                info = self._backend.create_index(table, columns, name)
                self._commit_autocommit()
        except Exception:
            self._rollback_autocommit()
            raise
        self._metadata_epoch += 1
        return info

    def list_indexes(self, table: str | None = None) -> list[IndexInfo]:
        return self._backend.list_indexes(table)

    def create_branch(
        self,
        branch_id: str,
        from_branch: str = "main",
        metadata: dict[str, Any] | None = None,
        *,
        terminal: bool = False,
        fanout: int | None = None,
    ) -> None:
        try:
            with self._local_session_branch_operation([from_branch]):
                if self._backend.name == "interval":
                    self._backend.create_branch(
                        branch_id,
                        from_branch,
                        metadata,
                        terminal=terminal,
                        fanout=fanout,
                    )
                else:
                    self._backend.create_branch(
                        branch_id, from_branch, metadata, terminal=terminal
                    )
        except Exception:
            self._rollback_autocommit()
            raise
        self._commit_autocommit()
        self._metadata_epoch += 1

    def update_branch_metadata(
        self, branch_id: str, metadata: dict[str, Any]
    ) -> BranchInfo:
        try:
            info = self._backend.update_branch_metadata(branch_id, metadata)
        except Exception:
            self._rollback_autocommit()
            raise
        self._commit_autocommit()
        self._metadata_epoch += 1
        return info

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
        try:
            self._backend.create_branch_from_checkpoint(branch_id, checkpoint)
        except Exception:
            self._rollback_autocommit()
            raise
        self._commit_autocommit()
        self._metadata_epoch += 1

    def delete_branch(self, branch_id: str) -> None:
        try:
            self._backend.delete_branch(branch_id)
        except Exception:
            self._rollback_autocommit()
            raise
        self._commit_autocommit()
        self._metadata_epoch += 1

    def list_branches(self) -> list[BranchInfo]:
        return self._backend.list_branches()

    def get_branch(self, branch_id: str) -> BranchInfo:
        return self._backend.get_branch(branch_id)

    def checkout(self, branch_id: str) -> BranchSession:
        try:
            info = self._backend.get_branch(branch_id)
            return self.checkout_ref(branch_id, info.current_ref)
        except Exception:
            self._rollback_autocommit()
            raise

    def checkout_ref(self, branch_id: str, current_ref: str | int) -> BranchSession:
        """Check out a known current branch reference without rereading its head.

        Workspace coordination reads the shared branch head once and passes the
        resulting interval reference to every participating store.  The
        returned session remains writable and retains the normal lazy refresh
        behavior when this context later observes branch metadata changes.
        """

        try:
            prepared = self._prepare_ref(
                _BranchRef(branch_id, str(current_ref))
            )
        except Exception:
            self._rollback_autocommit()
            raise
        if self._db.in_transaction:
            self._commit_autocommit()
        return BranchSession(self, prepared)

    def checkout_checkpoint(self, checkpoint: str) -> BranchSession:
        try:
            prepared = self._prepare_ref(self._backend.checkout_checkpoint(checkpoint))
        except Exception:
            self._rollback_autocommit()
            raise
        if self._db.in_transaction:
            self._commit_autocommit()
        return BranchSession(self, prepared)

    def create_checkpoint(
        self,
        checkpoint: str,
        branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> CheckpointInfo:
        try:
            with self._local_session_branch_operation([branch]):
                info = self._backend.create_checkpoint(checkpoint, branch, metadata)
        except Exception:
            self._rollback_autocommit()
            raise
        self._commit_autocommit()
        self._metadata_epoch += 1
        return info

    def get_checkpoint(self, checkpoint: str) -> CheckpointInfo:
        return self._backend.get_checkpoint(checkpoint)

    def list_checkpoints(
        self,
        branch: str | None = None,
        metadata_filter: dict[str, Any] | None = None,
    ) -> list[CheckpointInfo]:
        return self._backend.list_checkpoints(branch, metadata_filter)

    def _commit_autocommit(self) -> None:
        if self._autocommit:
            self._db.commit()
            self._backend.after_commit()

    def _rollback_autocommit(self) -> None:
        if self._autocommit and self._db.in_transaction:
            self._db.rollback()
            self._backend.after_rollback()

    def _prepare_ref(self, ref: _BranchRef) -> _PreparedBranchRef:
        prepared = self._backend.prepare_ref(ref)
        return self._stamp_prepared_ref(prepared)

    def _stamp_prepared_ref(self, ref: _PreparedBranchRef) -> _PreparedBranchRef:
        ref.metadata["_context_epoch"] = self._metadata_epoch
        return ref

    def diff(self, left: str, right: str) -> BranchDiff:
        changes: list[RowDiff] = []
        for table in self._backend.diff_tables():
            changes.extend(self.diff_rows(left, right, table))
        return BranchDiff(left=left, right=right, changes=changes)

    def diff_rows(self, left: str, right: str, table: str) -> list[RowDiff]:
        backend_diffs = self._backend.diff_rows(left, right, table)
        if backend_diffs is not None:
            return backend_diffs
        try:
            left_meta = self._backend.table_meta_for_branch(left, table)
        except TableNotRegisteredError:
            left_meta = None
        try:
            right_meta = self._backend.table_meta_for_branch(right, table)
        except TableNotRegisteredError:
            right_meta = None
        meta = left_meta or right_meta
        if meta is None:
            return []
        left_rows = self._rows_by_key(left, table, meta)
        right_rows = self._rows_by_key(right, table, meta)
        diffs: list[RowDiff] = []
        for key in sorted(set(left_rows) | set(right_rows), key=repr):
            before = left_rows.get(key)
            after = right_rows.get(key)
            key_dict = dict(zip(meta.pk_columns, key))
            if before is None and after is not None:
                diffs.append(RowDiff(table, key_dict, "added", None, after))
            elif before is not None and after is None:
                diffs.append(RowDiff(table, key_dict, "deleted", before, None))
            elif before != after:
                diffs.append(RowDiff(table, key_dict, "modified", before, after))
        return diffs

    def merge_preview(
        self, source: str, target: str, *, policy: MergePolicyInput = None
    ) -> MergePreview:
        preview_tables = getattr(self._backend, "merge_preview_tables", None)
        backend_preview = (
            preview_tables(source, target, list(self._merge_table_scope))
            if self._merge_table_scope is not None and callable(preview_tables)
            else self._backend.merge_preview(source, target)
        )
        if backend_preview is not None:
            return _preview_with_merge_policy(
                backend_preview, policy, backend=self._backend.name
            )
        changes = self.diff(target, source).changes
        return _preview_with_merge_policy(
            MergePreview(source=source, target=target, changes=changes, conflicts=[]),
            policy,
            backend=self._backend.name,
        )

    def set_merge_table_scope(self, tables: list[str]) -> None:
        """Limit this context's merge surface within a shared metadata plane."""

        self._merge_table_scope = tuple(dict.fromkeys(str(table) for table in tables))

    def merge_apply(
        self,
        source: str,
        target: str,
        resolution: MergeResolution | None = None,
        *,
        policy: MergePolicyInput = None,
    ) -> MergeResult:
        normalized_policy = _normalize_merge_policy(policy)
        if self._backend.name == "orpheus" and (
            normalized_policy.mode != "abort_on_conflict"
            or (resolution is not None and resolution.conflict_choices)
        ):
            raise BranchingError(
                "custom merge policies are not yet supported by the Orpheus backend"
            )
        can_use_backend_direct_apply = (
            normalized_policy.mode in {"abort_on_conflict", "snapshot_isolation"}
            and not normalized_policy.validators
            and normalized_policy.resolver is None
            and resolution is None
        )
        if can_use_backend_direct_apply:
            try:
                with self._local_session_branch_operation([source, target]):
                    backend_result = self._backend.merge_apply(
                        source, target, resolution
                    )
            except Exception:
                self._rollback_autocommit()
                raise
            if backend_result is not None:
                self._commit_autocommit()
                self._metadata_epoch += 1
                return backend_result
        if self._backend.name == "interval":
            for attempt in range(3):
                source_ref = self.get_branch(source).current_ref
                target_ref = self.get_branch(target).current_ref
                preview = self.merge_preview(source, target)
                changes = _resolve_merge_changes(
                    preview,
                    normalized_policy,
                    resolution,
                    backend=self._backend.name,
                )
                try:
                    with self._local_session_branch_operation([source, target]):
                        backend_applied = self._backend.apply_merge_changes(
                            source,
                            target,
                            changes,
                            expected_source_ref=source_ref,
                            expected_target_ref=target_ref,
                        )
                except BranchingError as exc:
                    if (
                        "merge preview became stale" in str(exc)
                        and attempt < 2
                    ):
                        continue
                    raise
                if backend_applied is not None:
                    self._metadata_epoch += 1
                    return MergeResult(
                        source=source,
                        target=target,
                        applied=backend_applied,
                    )
                break

        applied = 0
        target_session = self.checkout(target)
        with target_session.transaction():
            self._backend.lock_branches_for_merge(source, target)
            preview = self.merge_preview(source, target)
            changes = _resolve_merge_changes(
                preview, normalized_policy, resolution, backend=self._backend.name
            )
            applied = self._apply_merge_changes(target_session, changes)
        self._metadata_epoch += 1
        return MergeResult(source=source, target=target, applied=applied)

    def merge_preview_tables(
        self,
        source: str,
        target: str,
        tables: list[str],
        *,
        policy: MergePolicyInput = None,
    ) -> MergePreview:
        preview_tables = getattr(self._backend, "merge_preview_tables", None)
        if not callable(preview_tables):
            raise UnsupportedSQLError(
                "table-scoped merge preview requires the interval backend"
            )
        preview = preview_tables(source, target, tables)
        return _preview_with_merge_policy(
            preview,
            policy,
            backend=self._backend.name,
        )

    def reserve_branch_transaction(
        self,
        source: str,
        target: str,
        participant_stores: list[str],
        *,
        metadata: dict[str, Any] | None = None,
    ) -> BranchTransaction:
        reserve = getattr(self._backend, "reserve_branch_transaction", None)
        if not callable(reserve):
            raise UnsupportedSQLError(
                "branch transactions require the interval backend"
            )
        with self._local_session_branch_operation([source, target]):
            transaction = reserve(source, target, participant_stores, metadata)
        self._metadata_epoch += 1
        return transaction

    def stage_branch_transaction_changes(
        self,
        transaction: BranchTransaction,
        changes: list[RowDiff],
    ) -> int:
        stage = getattr(self._backend, "stage_branch_transaction_changes", None)
        if not callable(stage):
            raise UnsupportedSQLError(
                "branch transactions require the interval backend"
            )
        return int(stage(transaction, changes))

    def publish_branch_transaction(self, transaction: BranchTransaction) -> None:
        publish = getattr(self._backend, "publish_branch_transaction", None)
        if not callable(publish):
            raise UnsupportedSQLError(
                "branch transactions require the interval backend"
            )
        publish(transaction)
        self._metadata_epoch += 1

    def abort_branch_transaction(self, transaction: BranchTransaction) -> None:
        abort = getattr(self._backend, "abort_branch_transaction", None)
        if not callable(abort):
            raise UnsupportedSQLError(
                "branch transactions require the interval backend"
            )
        abort(transaction)
        self._metadata_epoch += 1

    def _apply_merge_changes(
        self, target_session: BranchSession, changes: list[RowDiff]
    ) -> int:
        pending_deletes: dict[str, list[dict[str, Any]]] = {}
        pending_upserts: dict[str, list[dict[str, Any]]] = {}
        table_order: list[str] = []
        seen_tables: set[str] = set()
        for change in changes:
            if change.table not in seen_tables:
                seen_tables.add(change.table)
                table_order.append(change.table)
            if change.change == "deleted":
                pending_deletes.setdefault(change.table, []).append(change.key)
            else:
                assert change.after is not None
                pending_upserts.setdefault(change.table, []).append(change.after)

        applied = 0
        for table in table_order:
            deletes = pending_deletes.get(table, [])
            if deletes:
                target_session.delete_keys(table, deletes)
                applied += len(deletes)
            upserts = pending_upserts.get(table, [])
            if upserts:
                target_session.upsert_rows(table, upserts)
                applied += len(upserts)
        return applied

    def _rows_by_key(
        self, branch_id: str, table: str, meta: _TableMeta
    ) -> dict[tuple[Any, ...], dict[str, Any]]:
        try:
            rows = self._backend.visible_rows(branch_id, table)
        except TableNotRegisteredError:
            rows = []
        return {tuple(row[column] for column in meta.pk_columns): row for row in rows}
