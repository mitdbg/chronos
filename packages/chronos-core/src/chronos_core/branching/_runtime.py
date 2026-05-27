from __future__ import annotations

from chronos_core.branching._common import *
from chronos_core.branching._copy_backend import _CopyBackend
from chronos_core.branching._interval_backend import _IntervalBackend
from chronos_core.branching._litetree_backend import _LiteTreeBackend
from chronos_core.branching._log_backend import _LogBackend
from chronos_core.branching._orpheus_backend import _OrpheusBackend

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

    @property
    def branch_id(self) -> str:
        return self._ref.branch_id

    @property
    def current_ref(self) -> str:
        """Stable backend reference captured by this checkout."""

        return self._ref.ref

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self._ensure_fresh()
        try:
            rows = self._context._backend.query(self._ref, sql, _ensure_params(params))
        except Exception:
            if self._transaction_depth == 0:
                self._context._rollback_autocommit()
            raise
        if self._transaction_depth == 0:
            self._context._commit_autocommit()
        return rows

    def execute(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> ExecuteResult:
        self._ensure_fresh()
        try:
            result = self._context._backend.execute(self._ref, sql, _ensure_params(params))
            # Some backends mutate the branch head on write. Refresh the prepared
            # metadata so later reads in the same session see their own writes.
            self._ref = self._context._backend.refresh_ref_after_execute(self._ref)
            self._context._stamp_prepared_ref(self._ref)
        except Exception:
            if self._transaction_depth == 0:
                self._context._rollback_autocommit()
            raise
        if self._transaction_depth == 0:
            self._context._commit_autocommit()
        return result

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        """Group several session writes in one underlying SQL transaction."""

        root = self._transaction_depth == 0
        if root:
            self._ensure_fresh()
            if self._context._db.in_transaction:
                self._context._db.commit()
            self._context._backend.prepare_transaction(self._ref)
            self._context._db.begin()
        self._transaction_depth += 1
        try:
            yield
        except Exception:
            self._transaction_depth -= 1
            if root:
                rollback = getattr(self._context._backend, "rollback_transaction", None)
                if callable(rollback):
                    rollback(self._ref)
                self._context._db.rollback()
            raise
        else:
            self._transaction_depth -= 1
            if root:
                try:
                    commit = getattr(self._context._backend, "commit_transaction", None)
                    if callable(commit):
                        self._ref = commit(self._ref)
                        self._context._stamp_prepared_ref(self._ref)
                    self._context._db.commit()
                except Exception:
                    rollback = getattr(self._context._backend, "rollback_transaction", None)
                    if callable(rollback):
                        rollback(self._ref)
                    self._context._db.rollback()
                    raise

    def branch_info(self) -> BranchInfo:
        return self._context.get_branch(self.branch_id)

    def upsert_rows(self, table: str, rows: list[dict[str, Any]]) -> ExecuteResult:
        self._ensure_fresh()
        if self._ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
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

    def delete_keys(self, table: str, keys: list[dict[str, Any]]) -> ExecuteResult:
        self._ensure_fresh()
        if self._ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
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


class ChronosBranchContext:
    """Branch manager for SQL-backed relational data."""

    def __init__(
        self,
        db: SQLDatabaseAdapter,
        backend: _SQLBranchBackend,
        autocommit: bool = True,
    ):
        self._db = db
        self._backend = backend
        self._autocommit = autocommit
        # Incremented whenever branch/table metadata changes. Checked-out
        # sessions compare against this to invalidate prepared metadata.
        self._metadata_epoch = 0

    @classmethod
    def connect(
        cls,
        database_url: str,
        backend: BranchBackendName = "interval",
        autocommit: bool = True,
        interval_continuation_percent: int = _INTERVAL_CONTINUATION_PERCENT,
        ensure_metadata: bool = True,
    ) -> ChronosBranchContext:
        db = connect_sql_database(database_url)
        return cls.from_database_adapter(
            db,
            backend=backend,
            autocommit=autocommit,
            interval_continuation_percent=interval_continuation_percent,
            ensure_metadata=ensure_metadata,
        )

    @classmethod
    def from_database_adapter(
        cls,
        db: SQLDatabaseAdapter,
        backend: BranchBackendName = "interval",
        autocommit: bool = True,
        interval_continuation_percent: int = _INTERVAL_CONTINUATION_PERCENT,
        ensure_metadata: bool = True,
    ) -> ChronosBranchContext:
        def build_backend() -> _SQLBranchBackend:
            if backend == "interval":
                return _IntervalBackend(
                    db,
                    continuation_percent=interval_continuation_percent,
                )
            if backend == "log":
                return _LogBackend(db)
            if backend == "copy":
                return _CopyBackend(db)
            if backend == "orpheus":
                return _OrpheusBackend(db)
            if backend == "litetree":
                return _LiteTreeBackend(db)
            raise ValueError(f"unknown branch backend: {backend}")

        if ensure_metadata:
            with _chronos_metadata_lock(db):
                impl = build_backend()
                impl.ensure()
        else:
            impl = build_backend()
        return cls(db, impl, autocommit=autocommit)

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
    def conn(self) -> Any:
        """Return the underlying driver connection for low-level tests/tools."""
        return self._db.raw_connection

    def close(self) -> None:
        self._db.close()

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
    ) -> None:
        try:
            self._backend.create_branch(branch_id, from_branch, metadata)
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
        info = self._backend.get_branch(branch_id)
        return BranchSession(
            self,
            self._prepare_ref(_BranchRef(branch_id, info.current_ref)),
        )

    def checkout_checkpoint(self, checkpoint: str) -> BranchSession:
        return BranchSession(
            self,
            self._prepare_ref(self._backend.checkout_checkpoint(checkpoint)),
        )

    def create_checkpoint(
        self,
        checkpoint: str,
        branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> CheckpointInfo:
        try:
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

    def _rollback_autocommit(self) -> None:
        if self._autocommit and self._db.in_transaction:
            self._db.rollback()

    def _prepare_ref(self, ref: _BranchRef) -> _PreparedBranchRef:
        prepared = self._backend.prepare_ref(ref)
        return self._stamp_prepared_ref(prepared)

    def _stamp_prepared_ref(self, ref: _PreparedBranchRef) -> _PreparedBranchRef:
        ref.metadata["_context_epoch"] = self._metadata_epoch
        return ref

    def diff(self, left: str, right: str) -> BranchDiff:
        changes: list[RowDiff] = []
        for table in self._backend.tables:
            changes.extend(self.diff_rows(left, right, table))
        return BranchDiff(left=left, right=right, changes=changes)

    def diff_rows(self, left: str, right: str, table: str) -> list[RowDiff]:
        meta = self._backend._require_table(table)
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

    def merge_preview(self, source: str, target: str) -> MergePreview:
        changes = self.diff(target, source).changes
        return MergePreview(source=source, target=target, changes=changes, conflicts=[])

    def merge_apply(
        self,
        source: str,
        target: str,
        resolution: MergeResolution | None = None,
    ) -> MergeResult:
        preview = self.merge_preview(source, target)
        if preview.conflicts and resolution is None:
            raise BranchingError("merge has unresolved conflicts")
        applied = 0
        with self.checkout(target).transaction():
            for change in preview.changes:
                if change.change == "deleted":
                    self._backend.delete_key(target, change.table, change.key)
                else:
                    assert change.after is not None
                    self._backend.upsert_row(target, change.table, change.after)
                applied += 1
        return MergeResult(source=source, target=target, applied=applied)

    def _rows_by_key(
        self, branch_id: str, table: str, meta: _TableMeta
    ) -> dict[tuple[Any, ...], dict[str, Any]]:
        rows = self._backend.visible_rows(branch_id, table)
        return {
            tuple(row[column] for column in meta.pk_columns): row
            for row in rows
        }
