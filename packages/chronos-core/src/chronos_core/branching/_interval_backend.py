from __future__ import annotations

from chronos_core.branching._common import *

import concurrent.futures
import math
import os
import threading

import psycopg

_UPSERT_BATCH_CHUNK = 1000
_SQLITE_UPSERT_BATCH_CHUNK = 250
_UPSERT_BATCH_PARAM_BUDGET = 60_000
_SQL_CACHE_MAX_ENTRIES = 4096
_SCHEMA_BINDING_LOCK_NAMESPACE = 1720812902
_ASYNC_SCHEMA_INDEX_LOCK = threading.Lock()
_ASYNC_SCHEMA_INDEX_EXECUTOR: concurrent.futures.ThreadPoolExecutor | None = None
_ASYNC_SCHEMA_INDEX_FUTURES: set[concurrent.futures.Future[None]] = set()
_INTERVAL_GC_LOCK = threading.Lock()
_INTERVAL_GC_EXECUTOR: concurrent.futures.ThreadPoolExecutor | None = None
_INTERVAL_GC_FUTURES: set[concurrent.futures.Future[None]] = set()
_INTERVAL_GC_BY_KEY: dict[tuple[str, str], concurrent.futures.Future[None]] = {}
_INTERVAL_GC_DIRTY: set[tuple[str, str]] = set()
_INTERVAL_GC_LOCK_NAMESPACE = 1720812904


def _async_schema_index_enabled_by_env() -> bool:
    return os.environ.get("CHRONOS_ASYNC_SCHEMA_INDEXES", "1").lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


def _async_schema_index_executor() -> concurrent.futures.ThreadPoolExecutor:
    global _ASYNC_SCHEMA_INDEX_EXECUTOR
    with _ASYNC_SCHEMA_INDEX_LOCK:
        if _ASYNC_SCHEMA_INDEX_EXECUTOR is None:
            workers = max(1, int(os.environ.get("CHRONOS_ASYNC_SCHEMA_INDEX_WORKERS", "2")))
            _ASYNC_SCHEMA_INDEX_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix="chronos-schema-index",
            )
        return _ASYNC_SCHEMA_INDEX_EXECUTOR


def _interval_gc_executor() -> concurrent.futures.ThreadPoolExecutor:
    global _INTERVAL_GC_EXECUTOR
    with _INTERVAL_GC_LOCK:
        if _INTERVAL_GC_EXECUTOR is None:
            workers = max(1, int(os.environ.get("CHRONOS_INTERVAL_GC_WORKERS", "1")))
            _INTERVAL_GC_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix="chronos-interval-gc",
            )
        return _INTERVAL_GC_EXECUTOR


def _run_postgres_autocommit_sqls(database_url: str, sqls: tuple[str, ...]) -> None:
    with psycopg.connect(database_url, autocommit=True) as conn:
        for sql in sqls:
            conn.execute(sql)


def _submit_async_schema_index_sqls(
    database_url: str, sqls: tuple[str, ...]
) -> concurrent.futures.Future[None]:
    future = _async_schema_index_executor().submit(
        _run_postgres_autocommit_sqls, database_url, sqls
    )
    with _ASYNC_SCHEMA_INDEX_LOCK:
        _ASYNC_SCHEMA_INDEX_FUTURES.add(future)

    def _discard(done: concurrent.futures.Future[None]) -> None:
        if done.exception() is not None:
            return
        with _ASYNC_SCHEMA_INDEX_LOCK:
            _ASYNC_SCHEMA_INDEX_FUTURES.discard(done)

    future.add_done_callback(_discard)
    return future


def _wait_for_all_async_schema_index_jobs() -> None:
    """Drain process-local schema index workers before destructive test cleanup."""

    while True:
        with _ASYNC_SCHEMA_INDEX_LOCK:
            futures = tuple(_ASYNC_SCHEMA_INDEX_FUTURES)
        if not futures:
            return
        for future in futures:
            future.result()
        with _ASYNC_SCHEMA_INDEX_LOCK:
            for future in futures:
                if future.done() and future.exception() is None:
                    _ASYNC_SCHEMA_INDEX_FUTURES.discard(future)


def _wait_for_all_interval_gc_jobs() -> None:
    """Drain process-local interval GC workers before destructive cleanup."""

    while True:
        with _INTERVAL_GC_LOCK:
            futures = tuple(_INTERVAL_GC_FUTURES)
        if not futures:
            return
        for future in futures:
            future.result()
        with _INTERVAL_GC_LOCK:
            for future in futures:
                if future.done():
                    _INTERVAL_GC_FUTURES.discard(future)


def _submit_interval_gc(
    database_url: str, backend: str
) -> concurrent.futures.Future[None]:
    key = (database_url, backend)
    executor = _interval_gc_executor()
    with _INTERVAL_GC_LOCK:
        _INTERVAL_GC_DIRTY.add(key)
        existing = _INTERVAL_GC_BY_KEY.get(key)
        if existing is not None and not existing.done():
            return existing
        future = executor.submit(
            _run_interval_gc_loop_for_url, database_url, backend, key
        )
        _INTERVAL_GC_BY_KEY[key] = future
        _INTERVAL_GC_FUTURES.add(future)

    def _discard(done: concurrent.futures.Future[None]) -> None:
        with _INTERVAL_GC_LOCK:
            if _INTERVAL_GC_BY_KEY.get(key) is done:
                _INTERVAL_GC_BY_KEY.pop(key, None)
            _INTERVAL_GC_FUTURES.discard(done)

    future.add_done_callback(_discard)
    return future


def _run_interval_gc_loop_for_url(
    database_url: str, backend: str, key: tuple[str, str]
) -> None:
    while True:
        with _INTERVAL_GC_LOCK:
            if key not in _INTERVAL_GC_DIRTY:
                return
            _INTERVAL_GC_DIRTY.discard(key)
        _run_interval_gc_for_url(database_url, backend)


def _run_interval_gc_for_url(database_url: str, backend: str) -> None:
    from chronos_core.branching.sql_adapters import connect_sql_database

    db = connect_sql_database(database_url)
    try:
        if db.dialect == "postgres":
            lock = db.execute(
                "SELECT pg_try_advisory_lock(?, ?) AS locked",
                (_INTERVAL_GC_LOCK_NAMESPACE, 1),
            ).fetchone()
            if lock is None or not bool(lock["locked"]):
                return
        _collect_interval_garbage(db, backend)
        db.commit()
    finally:
        try:
            if db.dialect == "postgres":
                db.execute(
                    "SELECT pg_advisory_unlock(?, ?)",
                    (_INTERVAL_GC_LOCK_NAMESPACE, 1),
                )
                db.commit()
        finally:
            db.close()


def _collect_interval_garbage(db: SQLDatabaseAdapter, backend: str) -> None:
    dead_segment_ids = _interval_dead_segment_ids(db)
    if not dead_segment_ids:
        return
    for physical in _interval_physical_tables_for_gc(db, backend):
        _delete_writer_rows_for_segments(db, physical, dead_segment_ids)
    _delete_dead_interval_segments(db, dead_segment_ids)


def _interval_dead_segment_ids(db: SQLDatabaseAdapter) -> list[int]:
    rows = db.execute(
        """
        WITH RECURSIVE reachable(segment_id) AS (
            SELECT current_segment_id
            FROM _chronos_branch_interval_branches
          UNION
            SELECT segment_id
            FROM _chronos_branch_interval_checkpoints
          UNION
            SELECT parent.segment_id
            FROM _chronos_branch_interval_segments AS child
            JOIN reachable
              ON reachable.segment_id = child.segment_id
            JOIN _chronos_branch_interval_segments AS parent
              ON parent.segment_id = child.parent_segment_id
        )
        SELECT segment_id
        FROM _chronos_branch_interval_segments
        WHERE segment_id NOT IN (SELECT segment_id FROM reachable)
        ORDER BY segment_id
        """
    ).fetchall()
    return [int(row["segment_id"]) for row in rows]


def _interval_physical_tables_for_gc(db: SQLDatabaseAdapter, backend: str) -> list[str]:
    tables: set[str] = set()
    rows = db.execute(
        """
        SELECT physical_table
        FROM _chronos_branch_tables
        WHERE backend = ?
        """,
        (backend,),
    ).fetchall()
    tables.update(str(row["physical_table"]) for row in rows)

    columns, _defs = db.table_defs("_chronos_branch_table_schema_versions")
    if columns:
        rows = db.execute(
            """
            SELECT physical_table
            FROM _chronos_branch_table_schema_versions
            WHERE backend = ?
            """,
            (backend,),
        ).fetchall()
        tables.update(str(row["physical_table"]) for row in rows)
    return sorted(tables)


def _delete_writer_rows_for_segments(
    db: SQLDatabaseAdapter, physical: str, segment_ids: list[int]
) -> None:
    if not segment_ids:
        return
    for start in range(0, len(segment_ids), _UPSERT_BATCH_CHUNK):
        chunk = segment_ids[start : start + _UPSERT_BATCH_CHUNK]
        db.execute(
            f"""
            DELETE FROM {_quote(physical)}
            WHERE writer_segment_id IN ({_placeholders(len(chunk))})
            """,
            tuple(chunk),
        )


def _delete_dead_interval_segments(
    db: SQLDatabaseAdapter, segment_ids: list[int]
) -> None:
    for start in range(0, len(segment_ids), _UPSERT_BATCH_CHUNK):
        chunk = segment_ids[start : start + _UPSERT_BATCH_CHUNK]
        db.execute(
            f"""
            DELETE FROM _chronos_branch_interval_segments
            WHERE segment_id IN ({_placeholders(len(chunk))})
            """,
            tuple(chunk),
        )


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
        self.async_schema_indexes = (
            self.db.dialect == "postgres" and _async_schema_index_enabled_by_env()
        )
        self._pending_async_schema_index_sqls: list[tuple[str, ...]] = []
        self._async_schema_index_futures: list[concurrent.futures.Future[None]] = []
        self._interval_gc_requested = False
        self._interval_gc_futures: list[concurrent.futures.Future[None]] = []
        self._query_rewrite_cache: dict[tuple[str, str, tuple[tuple[str, str], ...]], str] = {}
        self._statement_plan_cache: dict[tuple[str, str], _StatementPlan] = {}

    def _metadata_dialect(self) -> str:
        metadata_db = getattr(self.db, "metadata_db", self.db)
        return metadata_db.dialect

    def _metadata_raw_connection(self) -> Any:
        metadata_db = getattr(self.db, "metadata_db", self.db)
        return metadata_db.raw_connection

    def after_commit(self) -> None:
        self._start_pending_interval_gc()
        if not self._pending_async_schema_index_sqls:
            return
        sql_groups = self._pending_async_schema_index_sqls
        self._pending_async_schema_index_sqls = []
        database_url = getattr(self.db, "database_url", None)
        if not database_url:
            for sqls in sql_groups:
                for sql in sqls:
                    self.db.execute(sql.replace("CREATE INDEX CONCURRENTLY", "CREATE INDEX"))
            self.db.commit()
            return
        for sqls in sql_groups:
            self._async_schema_index_futures.append(
                _submit_async_schema_index_sqls(database_url, sqls)
            )

    def after_rollback(self) -> None:
        self._pending_async_schema_index_sqls = []
        self._interval_gc_requested = False

    def wait_for_async_schema_indexes(self) -> None:
        futures = list(self._async_schema_index_futures)
        for future in futures:
            future.result()
        self._async_schema_index_futures = [
            future for future in self._async_schema_index_futures if not future.done()
        ]

    def wait_for_interval_gc(self) -> None:
        futures = list(self._interval_gc_futures)
        for future in futures:
            future.result()
        self._interval_gc_futures = [
            future for future in self._interval_gc_futures if not future.done()
        ]

    def _start_pending_interval_gc(self) -> None:
        if not self._interval_gc_requested:
            return
        self._interval_gc_requested = False
        database_url = getattr(self.db, "database_url", None)
        if database_url:
            self._interval_gc_futures = [
                future for future in self._interval_gc_futures if not future.done()
            ]
            future = _submit_interval_gc(database_url, self.name)
            if future not in self._interval_gc_futures:
                self._interval_gc_futures.append(future)
            return
        _collect_interval_garbage(self.db, self.name)
        self.db.commit()

    def _interval_sql_type(self) -> str:
        if self.db.dialect == "postgres":
            return f"NUMERIC({_POSTGRES_INTERVAL_PRECISION},0)"
        return "BIGINT"

    def _max_interval(self) -> int:
        if self.db.dialect == "postgres":
            return _POSTGRES_MAX_INTERVAL
        return _MAX_INTERVAL

    def ensure(self) -> None:
        interval_type = self._interval_sql_type()
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_interval_branches (
              branch_id TEXT PRIMARY KEY,
              current_segment_id INTEGER NOT NULL,
              parent_branch_id TEXT,
              child_count INTEGER NOT NULL DEFAULT 0,
              branch_kind TEXT NOT NULL DEFAULT 'mutable',
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )
        branch_columns, _branch_defs = self.db.table_defs(
            "_chronos_branch_interval_branches"
        )
        added_parent_branch_id = "parent_branch_id" not in branch_columns
        added_child_count = "child_count" not in branch_columns
        added_branch_kind = "branch_kind" not in branch_columns
        if "parent_branch_id" not in branch_columns:
            self.db.execute(
                """
                ALTER TABLE _chronos_branch_interval_branches
                ADD COLUMN parent_branch_id TEXT
                """
            )
        if "child_count" not in branch_columns:
            self.db.execute(
                """
                ALTER TABLE _chronos_branch_interval_branches
                ADD COLUMN child_count INTEGER NOT NULL DEFAULT 0
                """
            )
        if "branch_kind" not in branch_columns:
            self.db.execute(
                """
                ALTER TABLE _chronos_branch_interval_branches
                ADD COLUMN branch_kind TEXT NOT NULL DEFAULT 'mutable'
                """
            )
        self.db.execute(
            """
            CREATE INDEX IF NOT EXISTS _chronos_idx_interval_branches_parent
            ON _chronos_branch_interval_branches (parent_branch_id)
            """
        )
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS _chronos_branch_interval_segments (
              segment_id INTEGER PRIMARY KEY,
              parent_segment_id INTEGER,
              owner_branch_id TEXT,
              segment_kind TEXT NOT NULL DEFAULT 'mutable',
              live_lo {interval_type} NOT NULL,
              live_hi {interval_type} NOT NULL,
              branch_point {interval_type} NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL,
              CHECK (live_lo <= branch_point),
              CHECK (branch_point < live_hi)
            )
            """
        )
        segment_columns, _segment_defs = self.db.table_defs("_chronos_branch_interval_segments")
        if "segment_kind" not in segment_columns:
            self.db.execute(
                """
                ALTER TABLE _chronos_branch_interval_segments
                ADD COLUMN segment_kind TEXT NOT NULL DEFAULT 'mutable'
                """
            )
        self._ensure_segment_id_allocator()
        exists = self.db.execute(
            "SELECT 1 FROM _chronos_branch_interval_branches WHERE branch_id = 'main'"
        ).fetchone()
        if exists is None:
            segment = 1
            self.db.execute(
                """
                INSERT INTO _chronos_branch_interval_segments
                (segment_id, parent_segment_id, owner_branch_id, segment_kind, live_lo, live_hi,
                 branch_point, created_at, metadata)
                VALUES (?, NULL, ?, 'mutable', ?, ?, ?, ?, ?)
                """,
                (
                    segment,
                    "main",
                    0,
                    self._max_interval(),
                    self._max_interval() // 2,
                    _utc_now(),
                    "{}",
                ),
            )
            self.db.execute(
                """
            INSERT INTO _chronos_branch_interval_branches
                (branch_id, current_segment_id, parent_branch_id, child_count, branch_kind,
                 created_at, metadata)
                VALUES (?, ?, NULL, 0, 'mutable', ?, ?)
                """,
                ("main", segment, _utc_now(), "{}"),
            )
        if added_parent_branch_id:
            self._backfill_interval_branch_parent_ids()
        if added_parent_branch_id or added_child_count:
            self._backfill_interval_branch_child_counts()
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_interval_checkpoints (
              checkpoint_id TEXT PRIMARY KEY,
              branch_id TEXT NOT NULL,
              segment_id INTEGER NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )
        self.db.execute(
            """
            UPDATE _chronos_branch_interval_segments
               SET segment_kind = 'checkpoint'
             WHERE segment_id IN (
               SELECT segment_id FROM _chronos_branch_interval_checkpoints
             )
               AND segment_kind = 'mutable'
            """
        )
        if self.enable_schema_branching:
            self._ensure_schema_branching_tables()
        self.db.commit()

    def _backfill_interval_branch_parent_ids(self) -> None:
        rows = self.db.execute(
            """
            SELECT branch_id, current_segment_id
            FROM _chronos_branch_interval_branches
            WHERE branch_id <> 'main'
              AND parent_branch_id IS NULL
            """
        ).fetchall()
        for row in rows:
            parent = self._infer_parent_branch_id_from_segments(
                row["branch_id"], int(row["current_segment_id"])
            )
            if parent is None:
                continue
            self.db.execute(
                """
                UPDATE _chronos_branch_interval_branches
                   SET parent_branch_id = ?
                 WHERE branch_id = ?
                   AND parent_branch_id IS NULL
                """,
                (parent, row["branch_id"]),
            )

    def _infer_parent_branch_id_from_segments(
        self, branch_id: str, segment_id: int
    ) -> str | None:
        row = self.db.execute(
            """
            WITH RECURSIVE ancestry(
              segment_id, parent_segment_id, owner_branch_id, depth
            ) AS (
              SELECT segment_id, parent_segment_id, owner_branch_id, 0
              FROM _chronos_branch_interval_segments
              WHERE segment_id = ?
            UNION ALL
              SELECT parent.segment_id, parent.parent_segment_id,
                     parent.owner_branch_id, ancestry.depth + 1
              FROM _chronos_branch_interval_segments AS parent
              JOIN ancestry ON parent.segment_id = ancestry.parent_segment_id
            )
            SELECT owner_branch_id
            FROM ancestry
            WHERE owner_branch_id IS NOT NULL
              AND owner_branch_id <> ?
            ORDER BY depth
            LIMIT 1
            """,
            (segment_id, branch_id),
        ).fetchone()
        return str(row["owner_branch_id"]) if row is not None else None

    def _backfill_interval_branch_child_counts(self) -> None:
        self.db.execute(
            """
            UPDATE _chronos_branch_interval_branches
               SET child_count = (
                 SELECT COUNT(*)
                 FROM _chronos_branch_interval_branches AS child
                 WHERE child.parent_branch_id =
                       _chronos_branch_interval_branches.branch_id
               )
            """
        )

    def _ensure_segment_id_allocator(self) -> None:
        if self._metadata_dialect() == "postgres":
            exists = self.db.execute(
                "SELECT to_regclass('public._chronos_branch_interval_segment_id_seq') AS seq"
            ).fetchone()
            if exists is not None and exists["seq"] is not None:
                return
            self.db.execute(
                "CREATE SEQUENCE IF NOT EXISTS _chronos_branch_interval_segment_id_seq "
                "AS integer START WITH 2"
            )
            self.db.execute(
                """
                SELECT setval(
                  '_chronos_branch_interval_segment_id_seq',
                  GREATEST(
                    2,
                    COALESCE(
                      (SELECT MAX(segment_id) + 1 FROM _chronos_branch_interval_segments),
                      2
                    )
                  ),
                  false
                )
                """
            )
            return
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_interval_segment_id_alloc (
              singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
              next_segment_id INTEGER NOT NULL
            )
            """
        )
        self.db.execute(
            """
            INSERT OR IGNORE INTO _chronos_branch_interval_segment_id_alloc
            (singleton, next_segment_id)
            SELECT
              1,
              COALESCE((SELECT MAX(segment_id) + 1 FROM _chronos_branch_interval_segments), 2)
            """
        )

    def _allocate_segment_ids(self, count: int) -> list[int]:
        if count <= 0:
            return []
        if self._metadata_dialect() == "postgres":
            rows = self.db.execute(
                """
                SELECT nextval('_chronos_branch_interval_segment_id_seq')::integer AS segment_id
                FROM generate_series(1, ?)
                """,
                (count,),
            ).fetchall()
            return [int(row["segment_id"]) for row in rows]
        row = self.db.execute(
            """
            SELECT next_segment_id
            FROM _chronos_branch_interval_segment_id_alloc
            WHERE singleton = 1
            """
        ).fetchone()
        if row is None:
            raise BranchingError("interval segment id allocator is missing")
        start = int(row["next_segment_id"])
        self.db.execute(
            """
            UPDATE _chronos_branch_interval_segment_id_alloc
            SET next_segment_id = ?
            WHERE singleton = 1
            """,
            (start + count,),
        )
        return list(range(start, start + count))

    def _ensure_schema_branching_tables(self) -> None:
        interval_type = self._interval_sql_type()
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_table_schema_versions (
              backend TEXT NOT NULL,
              table_name TEXT NOT NULL,
              schema_version_id TEXT NOT NULL,
              parent_schema_version_id TEXT,
              physical_table TEXT NOT NULL,
              pk_columns TEXT NOT NULL,
              columns TEXT NOT NULL,
              column_defs TEXT NOT NULL,
              ddl_op TEXT NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL,
              PRIMARY KEY (backend, schema_version_id)
            )
            """
        )
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS _chronos_branch_table_bindings (
              backend TEXT NOT NULL,
              table_name TEXT NOT NULL,
              schema_version_id TEXT,
              tombstone INTEGER NOT NULL DEFAULT 0,
              live_lo {interval_type} NOT NULL,
              live_hi {interval_type} NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL,
              PRIMARY KEY (backend, table_name, live_lo),
              CHECK (live_lo < live_hi)
            )
            """
        )

    def register_table(self, table: str, primary_key: list[str]) -> None:
        columns, defs = _table_defs(self.db, table)
        missing = set(primary_key) - set(columns)
        if missing:
            raise TableNotRegisteredError(f"primary key columns missing from {table}: {missing}")
        if table in self.tables:
            meta = self.tables[table]
            existing = set(meta.columns)
            additions = [
                (column, definition)
                for column, definition in zip(columns, defs)
                if column not in existing
            ]
            if not additions:
                if self.enable_schema_branching:
                    self._ensure_base_schema_version(meta, ddl_op="register")
                return
            for _column, definition in additions:
                self.db.execute(
                    f"ALTER TABLE {_quote(meta.physical_name)} "
                    f"ADD COLUMN {definition}"
                )
            self.db.execute(
                """
                UPDATE _chronos_branch_tables
                   SET columns = ?, column_defs = ?
                 WHERE backend = ? AND table_name = ?
                """,
                (
                    json.dumps(columns),
                    json.dumps(defs),
                    self.name,
                    table,
                ),
            )
            self.tables[table] = _TableMeta(
                name=meta.name,
                physical_name=meta.physical_name,
                pk_columns=meta.pk_columns,
                columns=columns,
                column_defs=defs,
                backend=meta.backend,
            )
            if self.enable_schema_branching:
                self._ensure_base_schema_version(self.tables[table], ddl_op="register")
            return
        interval_type = self._interval_sql_type()
        physical = f"_chronos_b_interval_{_physical_table_suffix(table)}"
        user_defs = ", ".join(defs)
        pk_sql = ", ".join(_quote(c) for c in primary_key)
        # The physical table keeps user columns plus visibility metadata. The
        # primary key includes live_lo because a logical key may have multiple
        # non-overlapping physical rows across branch intervals.
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_quote(physical)} (
              {user_defs},
              live_lo {interval_type} NOT NULL,
              live_hi {interval_type} NOT NULL,
              writer_segment_id INTEGER NOT NULL,
              deleted BOOLEAN NOT NULL DEFAULT FALSE,
              PRIMARY KEY ({pk_sql}, live_lo),
              CHECK (live_lo < live_hi)
            )
            """
        )
        self.db.execute(self._pk_hi_index_sql(physical, primary_key))
        self.db.execute(self._writer_segment_index_sql(physical, primary_key))
        cols = ", ".join(_quote(c) for c in columns)
        self.db.execute(
            f"""
            INSERT INTO {_quote(physical)}
            ({cols}, live_lo, live_hi, writer_segment_id, deleted)
            SELECT {cols}, 0, ?, ?, FALSE FROM {_quote_table_name(table)}
            """,
            (self._max_interval(), 1),
        )
        self.db.execute(
            """
            INSERT INTO _chronos_branch_tables
            (table_name, physical_table, pk_columns, columns, column_defs, backend)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                table,
                physical,
                json.dumps(primary_key),
                json.dumps(columns),
                json.dumps(defs),
                self.name,
            ),
        )
        meta = _TableMeta(
            name=table,
            physical_name=physical,
            pk_columns=tuple(primary_key),
            columns=columns,
            column_defs=defs,
            backend=self.name,
        )
        self.tables[table] = meta
        if self.enable_schema_branching:
            self._ensure_base_schema_version(meta, ddl_op="register")

    def _ensure_base_schema_version(self, meta: _TableMeta, ddl_op: str) -> str:
        self._ensure_schema_branching_tables()
        existing = self.db.execute(
            """
            SELECT schema_version_id
            FROM _chronos_branch_table_schema_versions
            WHERE backend = ? AND table_name = ? AND physical_table = ?
            """,
            (self.name, meta.name, meta.physical_name),
        ).fetchone()
        if existing is not None:
            return existing["schema_version_id"]
        schema_version_id = self._schema_version_id(meta.name)
        now = _utc_now()
        self.db.execute(
            """
            INSERT INTO _chronos_branch_table_schema_versions
            (backend, table_name, schema_version_id, parent_schema_version_id,
             physical_table, pk_columns, columns, column_defs, ddl_op,
             created_at, metadata)
            VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.name,
                meta.name,
                schema_version_id,
                meta.physical_name,
                json.dumps(meta.pk_columns),
                json.dumps(meta.columns),
                json.dumps(meta.column_defs),
                ddl_op,
                now,
                "{}",
            ),
        )
        self.db.execute(
            """
            INSERT INTO _chronos_branch_table_bindings
            (backend, table_name, schema_version_id, tombstone, live_lo, live_hi,
             created_at, metadata)
            VALUES (?, ?, ?, 0, ?, ?, ?, ?)
            """,
            (
                self.name,
                meta.name,
                schema_version_id,
                0,
                self._max_interval(),
                now,
                "{}",
            ),
        )
        return schema_version_id

    def _record_schema_version(
        self,
        table: str,
        physical: str,
        pk_columns: tuple[str, ...],
        columns: tuple[str, ...],
        column_defs: tuple[str, ...],
        ddl_op: str,
        parent_schema_version_id: str | None,
    ) -> str:
        schema_version_id = self._schema_version_id(table)
        self.db.execute(
            """
            INSERT INTO _chronos_branch_table_schema_versions
            (backend, table_name, schema_version_id, parent_schema_version_id,
             physical_table, pk_columns, columns, column_defs, ddl_op,
             created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.name,
                table,
                schema_version_id,
                parent_schema_version_id,
                physical,
                json.dumps(pk_columns),
                json.dumps(columns),
                json.dumps(column_defs),
                ddl_op,
                _utc_now(),
                "{}",
            ),
        )
        return schema_version_id

    def _update_schema_version_metadata(
        self,
        schema_version_id: str,
        meta: _TableMeta,
        ddl_op: str,
    ) -> None:
        self.db.execute(
            """
            UPDATE _chronos_branch_table_schema_versions
               SET pk_columns = ?,
                   columns = ?,
                   column_defs = ?,
                   ddl_op = ?,
                   metadata = ?
             WHERE backend = ?
               AND schema_version_id = ?
            """,
            (
                json.dumps(meta.pk_columns),
                json.dumps(meta.columns),
                json.dumps(meta.column_defs),
                ddl_op,
                "{}",
                self.name,
                schema_version_id,
            ),
        )

    def _schema_version_is_private_to_ref(
        self,
        ref: _PreparedBranchRef,
        table: str,
        schema_version_id: str | None,
        segment: _IntervalSegment,
    ) -> bool:
        # A schema version is safe to ALTER in place only when the current mutable
        # branch is the sole active ref that can resolve to it. Children and
        # checkpoints must keep seeing their original schema, so they force the
        # copy-and-splice path even if the current branch is their ancestor.
        if schema_version_id is None or ref.readonly:
            return False
        current = self._branch_row(ref.branch_id)
        if current is None or current["current_segment_id"] != segment.segment_id:
            return False
        other_branch = self.db.execute(
            """
            SELECT 1
            FROM _chronos_branch_interval_branches b
            JOIN _chronos_branch_interval_segments s
              ON s.segment_id = b.current_segment_id
            JOIN _chronos_branch_table_bindings tb
              ON tb.live_lo <= s.branch_point
             AND s.branch_point < tb.live_hi
            WHERE b.branch_id <> ?
              AND tb.backend = ?
              AND tb.table_name = ?
              AND tb.schema_version_id = ?
              AND tb.tombstone = 0
            LIMIT 1
            """,
            (ref.branch_id, self.name, table, schema_version_id),
        ).fetchone()
        if other_branch is not None:
            return False
        checkpoint = self.db.execute(
            """
            SELECT 1
            FROM _chronos_branch_interval_checkpoints cp
            JOIN _chronos_branch_interval_segments s
              ON s.segment_id = cp.segment_id
            JOIN _chronos_branch_table_bindings tb
              ON tb.live_lo <= s.branch_point
             AND s.branch_point < tb.live_hi
            WHERE tb.backend = ?
              AND tb.table_name = ?
              AND tb.schema_version_id = ?
              AND tb.tombstone = 0
            LIMIT 1
            """,
            (self.name, table, schema_version_id),
        ).fetchone()
        if checkpoint is not None:
            return False
        fork_base = self.db.execute(
            """
            SELECT 1
            FROM _chronos_branch_interval_segments s
            JOIN _chronos_branch_table_bindings tb
              ON tb.live_lo <= s.branch_point
             AND s.branch_point < tb.live_hi
            WHERE s.segment_kind = 'fork_base'
              AND tb.backend = ?
              AND tb.table_name = ?
              AND tb.schema_version_id = ?
              AND tb.tombstone = 0
            LIMIT 1
            """,
            (self.name, table, schema_version_id),
        ).fetchone()
        return fork_base is None

    def _schema_version_id(self, table: str) -> str:
        return f"sv_{_identifier_token(table)}_{uuid.uuid4().hex[:12]}"

    def _physical_schema_table_name(self, table: str) -> str:
        return f"_chronos_b_interval_{_physical_table_suffix(table)}_{uuid.uuid4().hex[:8]}"

    def create_index(
        self, table: str, columns: list[str], name: str | None = None
    ) -> IndexInfo:
        meta, index = self._validate_index(table, columns, name)
        if index.name not in self.indexes:
            self._record_index(index)
            self._create_physical_logical_indexes(table, index)
        return IndexInfo(index.name, index.table, index.columns, index.backend)

    def _create_physical_logical_indexes(
        self, table: str, index: _IndexMeta
    ) -> None:
        if self.enable_schema_branching:
            for meta in self._known_physical_metas_for_table(table):
                self._create_physical_logical_index(meta, index)
            return
        self._create_physical_logical_index(self._require_table(table), index)

    def _create_physical_logical_index(
        self, meta: _TableMeta, index: _IndexMeta
    ) -> None:
        missing = set(index.columns) - set(meta.columns)
        if missing:
            return
        self.db.execute(self._physical_logical_index_sql(meta, index))

    def _physical_logical_index_sql(
        self,
        meta: _TableMeta,
        index: _IndexMeta,
        *,
        concurrently: bool = False,
    ) -> str:
        indexed_columns = [*index.columns, "live_lo", "live_hi", "deleted"]
        concurrent = "CONCURRENTLY " if concurrently else ""
        return (
            f"CREATE INDEX {concurrent}IF NOT EXISTS "
            f"{_quote(self._physical_logical_index_name(meta, index))} "
            f"ON {_quote(meta.physical_name)} "
            f"({', '.join(_quote(c) for c in indexed_columns)})"
        )

    def _pk_hi_index_sql(
        self,
        physical: str,
        pk_columns: tuple[str, ...] | list[str],
        *,
        concurrently: bool = False,
    ) -> str:
        pk_sql = ", ".join(_quote(c) for c in pk_columns)
        concurrent = "CONCURRENTLY " if concurrently else ""
        return (
            f"CREATE INDEX {concurrent}IF NOT EXISTS "
            f"{_quote(f'idx_{physical}_pk_hi')} "
            f"ON {_quote(physical)} ({pk_sql}, live_hi)"
        )

    def _writer_segment_index_sql(
        self,
        physical: str,
        pk_columns: tuple[str, ...] | list[str],
        *,
        concurrently: bool = False,
    ) -> str:
        pk_sql = ", ".join(_quote(c) for c in pk_columns)
        concurrent = "CONCURRENTLY " if concurrently else ""
        return (
            f"CREATE INDEX {concurrent}IF NOT EXISTS "
            f"{_quote(f'idx_{physical}_writer_segment')} "
            f"ON {_quote(physical)} (writer_segment_id, {pk_sql})"
        )

    def _physical_logical_index_name(
        self, meta: _TableMeta, index: _IndexMeta
    ) -> str:
        base_meta = self.tables.get(index.table)
        if base_meta is not None and base_meta.physical_name == meta.physical_name:
            return f"_chronos_idx_interval_{index.name}"
        digest = hashlib.sha1(
            f"{index.name}:{meta.physical_name}".encode("utf-8")
        ).hexdigest()[:24]
        return f"_chronos_idx_interval_sv_{digest}"

    def _known_physical_metas_for_table(self, table: str) -> list[_TableMeta]:
        metas: dict[str, _TableMeta] = {}
        base = self.tables.get(table)
        if base is not None:
            metas[base.physical_name] = base
        if not self.enable_schema_branching:
            return list(metas.values())
        self._ensure_schema_branching_tables()
        rows = self.db.execute(
            """
            SELECT physical_table, pk_columns, columns, column_defs
            FROM _chronos_branch_table_schema_versions
            WHERE backend = ? AND table_name = ?
            """,
            (self.name, table),
        ).fetchall()
        for row in rows:
            metas[row["physical_table"]] = _TableMeta(
                name=table,
                physical_name=row["physical_table"],
                pk_columns=tuple(json.loads(row["pk_columns"])),
                columns=tuple(json.loads(row["columns"])),
                column_defs=tuple(json.loads(row["column_defs"])),
                backend=self.name,
            )
        return list(metas.values())

    def _copy_logical_indexes_to_schema_version(self, meta: _TableMeta) -> None:
        for index in self.indexes.values():
            if index.table == meta.name:
                self._create_physical_logical_index(meta, index)

    def _schema_version_secondary_index_sqls(
        self, meta: _TableMeta, *, concurrently: bool = False
    ) -> list[str]:
        sqls = [
            self._pk_hi_index_sql(
                meta.physical_name, meta.pk_columns, concurrently=concurrently
            ),
            self._writer_segment_index_sql(
                meta.physical_name, meta.pk_columns, concurrently=concurrently
            ),
        ]
        for index in self.indexes.values():
            if index.table == meta.name and not (set(index.columns) - set(meta.columns)):
                sqls.append(
                    self._physical_logical_index_sql(
                        meta, index, concurrently=concurrently
                    )
                )
        return sqls

    def _create_or_defer_schema_version_secondary_indexes(self, meta: _TableMeta) -> None:
        if self.async_schema_indexes and getattr(self.db, "database_url", None):
            self._pending_async_schema_index_sqls.append(
                tuple(self._schema_version_secondary_index_sqls(meta, concurrently=True))
            )
            return
        for sql in self._schema_version_secondary_index_sqls(meta):
            self.db.execute(sql)

    def create_branch(
        self,
        branch_id: str,
        from_branch: str,
        metadata: dict[str, Any] | None = None,
        *,
        terminal: bool = False,
    ) -> None:
        self._create_branch_unlocked(branch_id, from_branch, metadata, terminal=terminal)

    def _create_branch_unlocked(
        self,
        branch_id: str,
        from_branch: str,
        metadata: dict[str, Any] | None = None,
        *,
        terminal: bool = False,
    ) -> None:
        if self._metadata_dialect() == "postgres":
            self._create_branch_postgres_locked(
                branch_id, from_branch, metadata, terminal=terminal
            )
            return
        if self._branch_row(branch_id) is not None:
            raise BranchAlreadyExistsError(branch_id)
        source = self._branch_row(from_branch)
        if source is None:
            raise BranchNotFoundError(from_branch)
        if source["branch_kind"] == "terminal":
            raise BranchingError(f"terminal branch is not branchable: {from_branch}")
        source_segment = self._segment(source["current_segment_id"])
        # Branching splits the source segment into two mutable descendants plus
        # a one-point immutable fork base. The fork base is the stable merge base
        # for the two mutable branches; it is readable but never writable or
        # branchable.
        continuation, child, fork_base = self._split_segment_with_fork_base(
            source_segment, terminal=terminal
        )
        now = _utc_now()
        self._insert_segment(fork_base, source_segment.segment_id, None, "fork_base", now)
        self._insert_segment(continuation, fork_base["segment_id"], from_branch, "mutable", now)
        self._insert_segment(child, fork_base["segment_id"], branch_id, "mutable", now)
        self.db.execute(
            """
            UPDATE _chronos_branch_interval_branches
            SET current_segment_id = ?,
                child_count = child_count + 1
            WHERE branch_id = ?
            """,
            (continuation["segment_id"], from_branch),
        )
        self.db.execute(
            """
            INSERT INTO _chronos_branch_interval_branches
            (branch_id, current_segment_id, parent_branch_id, child_count, branch_kind,
             created_at, metadata)
            VALUES (?, ?, ?, 0, ?, ?, ?)
            """,
            (
                branch_id,
                child["segment_id"],
                from_branch,
                "terminal" if terminal else "mutable",
                now,
                _json_dumps(metadata),
            ),
        )

    def _create_branch_postgres_locked(
        self,
        branch_id: str,
        from_branch: str,
        metadata: dict[str, Any] | None = None,
        *,
        terminal: bool = False,
    ) -> None:
        """Create a branch while serializing concurrent forks of one parent.

        The parent branch row is locked before reading its current segment. This
        avoids stale segment reads when many workers branch from the same parent
        concurrently, as in the simulation benchmark's 1000-wide star.
        """

        if self._branch_row(branch_id) is not None:
            raise BranchAlreadyExistsError(branch_id)
        source = self.db.execute(
            """
            SELECT *
            FROM _chronos_branch_interval_branches
            WHERE branch_id = ?
            FOR UPDATE
            """,
            (from_branch,),
        ).fetchone()
        if source is None:
            raise BranchNotFoundError(from_branch)
        if source["branch_kind"] == "terminal":
            raise BranchingError(f"terminal branch is not branchable: {from_branch}")
        source_segment = self._segment(source["current_segment_id"])
        continuation, child, fork_base = self._split_segment_with_fork_base(
            source_segment, terminal=terminal
        )
        now = _utc_now()
        raw = self._metadata_raw_connection()
        if not hasattr(raw, "pipeline"):
            raise BranchingError("PostgreSQL adapter does not expose pipeline mode")
        with raw.pipeline():
            self.db.execute(
                """
                INSERT INTO _chronos_branch_interval_segments
                (segment_id, parent_segment_id, owner_branch_id, segment_kind, live_lo, live_hi,
                 branch_point, created_at, metadata)
                VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?),
                  (?, ?, ?, ?, ?, ?, ?, ?, ?),
                  (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fork_base["segment_id"],
                    source_segment.segment_id,
                    None,
                    "fork_base",
                    fork_base["live_lo"],
                    fork_base["live_hi"],
                    fork_base["branch_point"],
                    now,
                    "{}",
                    continuation["segment_id"],
                    fork_base["segment_id"],
                    from_branch,
                    "mutable",
                    continuation["live_lo"],
                    continuation["live_hi"],
                    continuation["branch_point"],
                    now,
                    "{}",
                    child["segment_id"],
                    fork_base["segment_id"],
                    branch_id,
                    "mutable",
                    child["live_lo"],
                    child["live_hi"],
                    child["branch_point"],
                    now,
                    "{}",
                ),
            )
            self.db.execute(
                """
                UPDATE _chronos_branch_interval_branches
                SET current_segment_id = ?,
                    child_count = child_count + 1
                WHERE branch_id = ?
                  AND current_segment_id = ?
                """,
                (continuation["segment_id"], from_branch, source_segment.segment_id),
            )
            self.db.execute(
                """
                INSERT INTO _chronos_branch_interval_branches
                (branch_id, current_segment_id, parent_branch_id, child_count, branch_kind,
                 created_at, metadata)
                VALUES (?, ?, ?, 0, ?, ?, ?)
                """,
                (
                    branch_id,
                    child["segment_id"],
                    from_branch,
                    "terminal" if terminal else "mutable",
                    now,
                    _json_dumps(metadata),
                ),
            )

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
        self._create_branch_from_checkpoint_unlocked(branch_id, checkpoint)

    def _create_branch_from_checkpoint_unlocked(self, branch_id: str, checkpoint: str) -> None:
        if self._branch_row(branch_id) is not None:
            raise BranchAlreadyExistsError(branch_id)
        cp = self.db.execute(
            "SELECT * FROM _chronos_branch_interval_checkpoints WHERE checkpoint_id = ?",
            (checkpoint,),
        ).fetchone()
        if cp is None:
            raise BranchNotFoundError(f"checkpoint:{checkpoint}")
        segment = self._segment(cp["segment_id"])
        lo = segment.live_lo
        hi = segment.live_hi
        if hi - lo < 4:
            raise BranchingError("interval space exhausted for checkpoint branch")
        child_lo = lo + (hi - lo) // 2
        child_hi = hi
        child_segment_id = self._allocate_segment_ids(1)[0]
        child = {
            "segment_id": child_segment_id,
            "live_lo": child_lo,
            "live_hi": child_hi,
            "branch_point": child_lo + (child_hi - child_lo) // 2,
        }
        now = _utc_now()
        self.db.execute(
            """
            INSERT INTO _chronos_branch_interval_segments
            (segment_id, parent_segment_id, owner_branch_id, live_lo, live_hi,
             branch_point, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                child["segment_id"],
                segment.segment_id,
                branch_id,
                child["live_lo"],
                child["live_hi"],
                child["branch_point"],
                now,
                "{}",
            ),
        )
        self.db.execute(
            """
            INSERT INTO _chronos_branch_interval_branches
            (branch_id, current_segment_id, parent_branch_id, child_count, branch_kind,
             created_at, metadata)
            VALUES (?, ?, ?, 0, 'mutable', ?, ?)
            """,
            (branch_id, child["segment_id"], cp["branch_id"], now, "{}"),
        )
        self.db.execute(
            """
            UPDATE _chronos_branch_interval_branches
               SET child_count = child_count + 1
             WHERE branch_id = ?
            """,
            (cp["branch_id"],),
        )

    def update_branch_metadata(
        self, branch_id: str, metadata: dict[str, Any]
    ) -> BranchInfo:
        if self._branch_row(branch_id) is None:
            raise BranchNotFoundError(branch_id)
        self.db.execute(
            "UPDATE _chronos_branch_interval_branches SET metadata = ? WHERE branch_id = ?",
            (_json_dumps(metadata), branch_id),
        )
        return self.get_branch(branch_id)

    def delete_branch(self, branch_id: str) -> None:
        if branch_id == "main":
            raise BranchingError("main cannot be deleted")

        branch = self._branch_row_for_update(branch_id)
        if branch is None:
            raise BranchNotFoundError(branch_id)

        parent_branch_id = branch["parent_branch_id"]
        if int(branch["child_count"]) == 0:
            cur = self.db.execute(
                """
                DELETE FROM _chronos_branch_interval_branches
                WHERE branch_id = ?
                """,
                (branch_id,),
            )
            if cur.rowcount == 0:
                raise BranchNotFoundError(branch_id)
            self._decrement_interval_branch_child_count(parent_branch_id)
            self._interval_gc_requested = True
            return

        branches = self._branches_to_delete_cascade_by_parent(branch_id)
        cur = self.db.execute(
            f"""
            DELETE FROM _chronos_branch_interval_branches
            WHERE branch_id IN ({_placeholders(len(branches))})
            """,
            tuple(branches),
        )
        if cur.rowcount == 0:
            raise BranchNotFoundError(branch_id)
        self._decrement_interval_branch_child_count(parent_branch_id)
        self._interval_gc_requested = True

    def _branches_to_delete_cascade_by_parent(self, branch_id: str) -> list[str]:
        rows = self.db.execute(
            """
            WITH RECURSIVE subtree(branch_id) AS (
              SELECT branch_id
              FROM _chronos_branch_interval_branches
              WHERE branch_id = ?
            UNION ALL
              SELECT child.branch_id
              FROM _chronos_branch_interval_branches AS child
              JOIN subtree ON child.parent_branch_id = subtree.branch_id
              WHERE child.branch_id <> 'main'
            )
            SELECT branch_id
            FROM subtree
            ORDER BY branch_id
            """,
            (branch_id,),
        ).fetchall()
        return [row["branch_id"] for row in rows]

    def _decrement_interval_branch_child_count(self, branch_id: str | None) -> None:
        if branch_id is None:
            return
        self.db.execute(
            """
            UPDATE _chronos_branch_interval_branches
               SET child_count = CASE
                   WHEN child_count > 0 THEN child_count - 1
                   ELSE 0
               END
             WHERE branch_id = ?
            """,
            (branch_id,),
        )

    def list_branches(self) -> list[BranchInfo]:
        rows = self.db.execute(
            """
            SELECT branch_id, current_segment_id, created_at, metadata
            FROM _chronos_branch_interval_branches
            ORDER BY branch_id
            """
        ).fetchall()
        return [
            BranchInfo(
                branch_id=row["branch_id"],
                current_ref=str(row["current_segment_id"]),
                backend=self.name,
                created_at=row["created_at"],
                metadata=_json_loads(row["metadata"]),
            )
            for row in rows
        ]

    def get_branch(self, branch_id: str) -> BranchInfo:
        row = self._branch_row(branch_id)
        if row is None:
            raise BranchNotFoundError(branch_id)
        return BranchInfo(
            branch_id=row["branch_id"],
            current_ref=str(row["current_segment_id"]),
            backend=self.name,
            created_at=row["created_at"],
            metadata=_json_loads(row["metadata"]),
        )

    def create_checkpoint(
        self, checkpoint: str, branch: str, metadata: dict[str, Any] | None = None
    ) -> CheckpointInfo:
        return self._create_checkpoint_unlocked(checkpoint, branch, metadata)

    def _create_checkpoint_unlocked(
        self, checkpoint: str, branch: str, metadata: dict[str, Any] | None = None
    ) -> CheckpointInfo:
        if (
            self.db.execute(
                "SELECT 1 FROM _chronos_branch_interval_checkpoints WHERE checkpoint_id = ?",
                (checkpoint,),
            ).fetchone()
            is not None
        ):
            raise BranchAlreadyExistsError(checkpoint)
        source = self._branch_row_for_update(branch)
        if source is None:
            raise BranchNotFoundError(branch)
        if source["branch_kind"] == "terminal":
            raise BranchingError(f"terminal branch cannot be checkpointed: {branch}")
        source_segment = self._segment(source["current_segment_id"])
        # A checkpoint is a read-only segment cut from the current branch. The
        # mutable branch keeps the continuation segment, so later branch writes
        # cannot alter the checkpoint view.
        continuation, snapshot = self._split_segment(source_segment)
        now = _utc_now()
        for segment, owner, kind in ((continuation, branch, "mutable"), (snapshot, None, "checkpoint")):
            self.db.execute(
                """
                INSERT INTO _chronos_branch_interval_segments
                (segment_id, parent_segment_id, owner_branch_id, segment_kind, live_lo, live_hi,
                 branch_point, created_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    segment["segment_id"],
                    source_segment.segment_id,
                    owner,
                    kind,
                    segment["live_lo"],
                    segment["live_hi"],
                    segment["branch_point"],
                    now,
                    "{}",
                ),
            )
        self.db.execute(
            """
            UPDATE _chronos_branch_interval_branches
            SET current_segment_id = ?
            WHERE branch_id = ?
            """,
            (continuation["segment_id"], branch),
        )
        self.db.execute(
            """
            INSERT INTO _chronos_branch_interval_checkpoints
            (checkpoint_id, branch_id, segment_id, created_at, metadata)
            VALUES (?, ?, ?, ?, ?)
            """,
            (checkpoint, branch, snapshot["segment_id"], now, _json_dumps(metadata)),
        )
        return CheckpointInfo(checkpoint, branch, str(snapshot["segment_id"]), now, metadata or {})

    def get_checkpoint(self, checkpoint: str) -> CheckpointInfo:
        cp = self.db.execute(
            "SELECT * FROM _chronos_branch_interval_checkpoints WHERE checkpoint_id = ?",
            (checkpoint,),
        ).fetchone()
        if cp is None:
            raise BranchNotFoundError(f"checkpoint:{checkpoint}")
        return CheckpointInfo(
            cp["checkpoint_id"],
            cp["branch_id"],
            str(cp["segment_id"]),
            cp["created_at"],
            _json_loads(cp["metadata"]),
        )

    def list_checkpoints(
        self,
        branch: str | None = None,
        metadata_filter: dict[str, Any] | None = None,
    ) -> list[CheckpointInfo]:
        params: list[Any] = []
        where = ""
        if branch is not None:
            where = "WHERE branch_id = ?"
            params.append(branch)
        rows = self.db.execute(
            f"""
            SELECT checkpoint_id, branch_id, segment_id, created_at, metadata
            FROM _chronos_branch_interval_checkpoints
            {where}
            ORDER BY created_at, checkpoint_id
            """,
            tuple(params),
        ).fetchall()
        infos = [
            CheckpointInfo(
                row["checkpoint_id"],
                row["branch_id"],
                str(row["segment_id"]),
                row["created_at"],
                _json_loads(row["metadata"]),
            )
            for row in rows
        ]
        if metadata_filter:
            infos = [
                info
                for info in infos
                if all(info.metadata.get(key) == value for key, value in metadata_filter.items())
            ]
        return infos

    def checkout_checkpoint(self, checkpoint: str) -> _BranchRef:
        cp = self.get_checkpoint(checkpoint)
        return _BranchRef(cp.branch_id, cp.ref, readonly=True)

    def prepare_ref(self, ref: _BranchRef) -> _PreparedBranchRef:
        # Cache the segment and table rewrites once per checkout. For repeated
        # queries this avoids metadata SELECTs before every statement.
        segment = self._segment_for_ref(ref)
        tables = self._active_tables_for_segment(segment) if self.enable_schema_branching else self.tables
        return _PreparedBranchRef(
            ref.branch_id,
            ref.ref,
            ref.readonly,
            {
                "segment": segment,
                "tables": tables,
                "replacements": {
                    name: self._visible_subquery(meta)
                    for name, meta in tables.items()
                },
                "query_rewrite_cache": {},
                "statement_plan_cache": {},
            },
        )

    def refresh_ref_after_execute(self, ref: _PreparedBranchRef) -> _PreparedBranchRef:
        if not self.enable_schema_branching:
            return ref
        return self.prepare_ref(_BranchRef(ref.branch_id, ref.ref, ref.readonly))

    def query(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        segment = self._prepared_segment(ref)
        replacements = ref.metadata.get("replacements")
        if replacements is None:
            tables = self._tables_for_ref(ref)
            replacements = {name: self._visible_subquery(meta) for name, meta in tables.items()}
        if self.enable_schema_branching:
            self._validate_query_tables_visible(ref, sql)
        rewrite_key = (
            self.db.dialect,
            sql,
            tuple(sorted((name, replacement) for name, replacement in replacements.items())),
        )
        rewritten = self._query_rewrite_cache.get(rewrite_key)
        if rewritten is None:
            rewritten = _rewrite_tables(sql, replacements, self.db.dialect)
            if len(self._query_rewrite_cache) >= _SQL_CACHE_MAX_ENTRIES:
                self._query_rewrite_cache.clear()
            self._query_rewrite_cache[rewrite_key] = rewritten
        bound = dict(params)
        bound["_chronos_branch_point"] = segment.branch_point
        rows = self.db.execute(rewritten, bound).fetchall()
        return [dict(row) for row in rows]

    def execute(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> ExecuteResult:
        if ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
        segment = self._prepared_segment(ref)
        if self._is_schema_statement(sql):
            tree = sqlglot.parse_one(sql, read=self.db.dialect)
            if not self.enable_schema_branching:
                raise UnsupportedSQLError("branch-local schema changes are disabled")
            if not isinstance(tree, (exp.Create, exp.Alter, exp.Drop)):
                raise UnsupportedSQLError("unsupported schema statement")
            # PostgreSQL's CREATE INDEX CONCURRENTLY still conflicts with
            # later ALTER TABLE operations on the same physical table. Drain
            # this context's deferred schema-version indexes before the next
            # branch-local DDL so the DDL path stays deadlock-free.
            self.wait_for_async_schema_indexes()
            self._lock_branch_for_schema_change(ref)
            result = self._execute_schema_ddl(ref, tree)
            ref.metadata.clear()
            ref.metadata.update(self.prepare_ref(_BranchRef(ref.branch_id, ref.ref, ref.readonly)).metadata)
            return result
        plan = self._statement_plan(ref, sql)
        if isinstance(plan, _InsertPlan):
            table = plan.table
            rows = _insert_rows_from_plan(plan, params)
            meta = self._meta_for_ref(ref, table)
            defaults = self._column_default_values(meta)
            full_rows = [
                {
                    column: row[column] if column in row else defaults.get(column)
                    for column in meta.columns
                }
                for row in rows
            ]
            inserted = self._insert_visible_batch(
                ref.branch_id,
                table,
                full_rows,
                segment=segment,
                meta=meta,
                ignore_conflicts=plan.ignore_conflicts,
            )
            return ExecuteResult(inserted)
        if isinstance(plan, _UpdatePlan):
            table = plan.table
            meta = self._meta_for_ref(ref, table)
            private_canonical = self._private_schema_table_is_canonical_for_ref(
                ref, table, segment
            )
            private_count = self._try_update_private_schema_table_in_place(
                ref,
                table,
                meta,
                plan,
                params,
                segment,
                private_canonical=private_canonical,
            )
            if private_count is not None:
                return ExecuteResult(private_count)
            batch_count = self._try_update_full_table_with_batch_splice(
                ref, table, meta, plan, params, segment
            )
            if batch_count is not None:
                return ExecuteResult(batch_count)
            current_rows = self._select_matching_rows(
                ref, table, plan.where_sql, params, direct_filter=plan.direct_filter
            )
            count = 0
            for current in current_rows:
                new_row = dict(current)
                assignments = _update_assignments_from_plan(plan, params, current)
                new_row.update(assignments)
                self._splice_row(
                    table,
                    self._row_key(meta, new_row),
                    new_row,
                    False,
                    ref.branch_id,
                    segment=segment,
                    meta=meta,
                )
                count += 1
            if private_canonical:
                self._canonicalize_private_schema_table(meta, segment)
            return ExecuteResult(count)
        if isinstance(plan, _DeletePlan):
            table = plan.table
            meta = self._meta_for_ref(ref, table)
            private_canonical = self._private_schema_table_is_canonical_for_ref(
                ref, table, segment
            )
            private_count = self._try_delete_private_schema_table_in_place(
                ref,
                table,
                meta,
                plan,
                params,
                segment,
                private_canonical=private_canonical,
            )
            if private_count is not None:
                return ExecuteResult(private_count)
            keys = self._select_matching_keys(
                ref, table, plan.where_sql, params, direct_filter=plan.direct_filter
            )
            count = 0
            for key in keys:
                self._splice_row(table, key, None, True, ref.branch_id, segment=segment, meta=meta)
                count += 1
            if private_canonical:
                self._canonicalize_private_schema_table(meta, segment)
            return ExecuteResult(count)
        raise UnsupportedSQLError("only SELECT, INSERT, UPDATE, and DELETE are supported")

    @staticmethod
    def _is_schema_statement(sql: str) -> bool:
        head = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
        return head in {"CREATE", "ALTER", "DROP"}

    def visible_rows(self, branch_id: str, table: str) -> list[dict[str, Any]]:
        ref = self.prepare_ref(_BranchRef(branch_id, self._branch_segment_id(branch_id)))
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
        try:
            left_segment = self._current_segment(left)
            left_meta = self._meta_for_segment(left_segment, table)
        except TableNotRegisteredError:
            left_segment = None
            left_meta = None
        try:
            right_segment = self._current_segment(right)
            right_meta = self._meta_for_segment(right_segment, table)
        except TableNotRegisteredError:
            right_segment = None
            right_meta = None
        if left_meta is None and right_meta is None:
            return []
        if (
            left_segment is None
            or right_segment is None
            or left_meta is None
            or right_meta is None
            or left_meta.physical_name != right_meta.physical_name
            or left_meta.pk_columns != right_meta.pk_columns
            or left_meta.columns != right_meta.columns
        ):
            return self._snapshot_diff_rows(left, right, table, left_meta or right_meta)

        writer_segments = self._divergent_segment_ids(
            left_segment.segment_id, right_segment.segment_id
        )
        if not writer_segments:
            return []
        keys = self._candidate_keys_for_writer_segments(left_meta, writer_segments)
        if not keys:
            return []

        left_rows = self._rows_by_key_for_keys(table, keys, left_segment, left_meta)
        right_rows = self._rows_by_key_for_keys(table, keys, right_segment, right_meta)
        return self._classify_row_diffs(table, left_meta, left_rows, right_rows)

    def merge_preview(self, source: str, target: str) -> MergePreview | None:
        source_segment, target_segment = self._current_segments_for_merge(source, target)
        direct = self._direct_sibling_merge_base(
            source_segment.segment_id, target_segment.segment_id
        )
        if direct is None:
            source_path = self._segment_ancestry_rows(source_segment.segment_id)
            target_path = self._segment_ancestry_rows(target_segment.segment_id)
            base_segment = self._nearest_shared_fork_base_from_paths(source_path, target_path)
            if base_segment is None:
                # Existing stores created before fork-base metadata cannot provide a
                # correct three-way base. Let the runtime keep using its legacy
                # two-way behavior for compatibility.
                return None

            source_writer_segments = self._mutable_segment_ids_after_base_from_path(
                source_path, base_segment.segment_id
            )
            target_writer_segments = self._mutable_segment_ids_after_base_from_path(
                target_path, base_segment.segment_id
            )
        else:
            base_segment, source_writer_segments, target_writer_segments = direct

        changes: list[RowDiff] = []
        conflicts: list[RowDiff] = []
        candidate_writer_segments = sorted(set(source_writer_segments) | set(target_writer_segments))

        for table in self.diff_tables():
            table_changes, table_conflicts = self._merge_preview_table(
                table,
                base_segment,
                source_segment,
                target_segment,
                candidate_writer_segments,
            )
            changes.extend(table_changes)
            conflicts.extend(table_conflicts)
        return MergePreview(source=source, target=target, changes=changes, conflicts=conflicts)

    def _current_segments_for_merge(
        self, source: str, target: str
    ) -> tuple[_IntervalSegment, _IntervalSegment]:
        if self.db.dialect != "postgres" or not hasattr(self.db.raw_connection, "pipeline"):
            return self._current_segment(source), self._current_segment(target)

        raw = self._metadata_raw_connection()
        with raw.pipeline():
            source_cur = self.db.execute(
                """
                SELECT s.segment_id, s.live_lo, s.live_hi, s.branch_point
                FROM _chronos_branch_interval_branches AS b
                JOIN _chronos_branch_interval_segments AS s
                  ON s.segment_id = b.current_segment_id
                WHERE b.branch_id = ?
                """,
                (source,),
            )
            target_cur = self.db.execute(
                """
                SELECT s.segment_id, s.live_lo, s.live_hi, s.branch_point
                FROM _chronos_branch_interval_branches AS b
                JOIN _chronos_branch_interval_segments AS s
                  ON s.segment_id = b.current_segment_id
                WHERE b.branch_id = ?
                """,
                (target,),
            )
        source_row = source_cur.fetchone()
        if source_row is None:
            raise BranchNotFoundError(source)
        target_row = target_cur.fetchone()
        if target_row is None:
            raise BranchNotFoundError(target)
        return self._segment_from_row(source_row), self._segment_from_row(target_row)

    def _merge_preview_table_metas(
        self,
        table: str,
        source_segment: _IntervalSegment,
        target_segment: _IntervalSegment,
        base_segment: _IntervalSegment,
    ) -> tuple[_TableMeta | None, _TableMeta | None, _TableMeta | None]:
        if not self.enable_schema_branching:
            try:
                meta = self._require_table(table)
            except TableNotRegisteredError:
                return None, None, None
            return meta, meta, meta

        if self.db.dialect != "postgres" or not hasattr(self.db.raw_connection, "pipeline"):
            return (
                self._schema_meta_for_segment_table(source_segment, table),
                self._schema_meta_for_segment_table(target_segment, table),
                self._schema_meta_for_segment_table(base_segment, table),
            )

        raw = self.db.raw_connection
        with raw.pipeline():
            source_cur = self._schema_meta_cursor_for_segment_table(source_segment, table)
            target_cur = self._schema_meta_cursor_for_segment_table(target_segment, table)
            base_cur = self._schema_meta_cursor_for_segment_table(base_segment, table)
        return (
            self._schema_meta_from_row(source_cur.fetchone()),
            self._schema_meta_from_row(target_cur.fetchone()),
            self._schema_meta_from_row(base_cur.fetchone()),
        )

    def _merge_preview_table(
        self,
        table: str,
        base_segment: _IntervalSegment,
        source_segment: _IntervalSegment,
        target_segment: _IntervalSegment,
        candidate_writer_segments: list[int],
    ) -> tuple[list[RowDiff], list[RowDiff]]:
        source_meta, target_meta, base_meta = self._merge_preview_table_metas(
            table, source_segment, target_segment, base_segment
        )

        if source_meta is None and target_meta is None:
            return [], []
        merge_meta = source_meta or target_meta or base_meta
        if merge_meta is None:
            return [], []
        if (
            source_meta is None
            or target_meta is None
            or base_meta is None
            or source_meta.physical_name != target_meta.physical_name
            or source_meta.physical_name != base_meta.physical_name
            or source_meta.pk_columns != target_meta.pk_columns
            or source_meta.pk_columns != base_meta.pk_columns
            or source_meta.columns != target_meta.columns
            or source_meta.columns != base_meta.columns
        ):
            # Be conservative around schema divergence until schema-aware merge
            # has explicit policies. Existing two-way diffs become conflicts.
            target_rows = (
                self._snapshot_rows_by_key_for_segment(table, target_meta, target_segment)
                if target_meta is not None
                else {}
            )
            source_rows = (
                self._snapshot_rows_by_key_for_segment(table, source_meta, source_segment)
                if source_meta is not None
                else {}
            )
            return [], self._classify_row_diffs(table, merge_meta, target_rows, source_rows)

        keys = self._candidate_keys_for_writer_segments(source_meta, candidate_writer_segments)
        if not keys:
            return [], []

        base_rows, source_rows, target_rows = self._merge_rows_by_key_for_keys(
            table,
            keys,
            base_segment,
            base_meta,
            source_segment,
            source_meta,
            target_segment,
            target_meta,
        )

        changes: list[RowDiff] = []
        conflicts: list[RowDiff] = []
        for key in sorted(set(base_rows) | set(source_rows) | set(target_rows), key=repr):
            base_row = base_rows.get(key)
            source_row = source_rows.get(key)
            target_row = target_rows.get(key)
            if source_row == target_row:
                if source_row != base_row:
                    # Both sides wrote the row after the fork but converged on
                    # the same image. Treat this as a write-write conflict so
                    # snapshot isolation remains first-committer-wins rather
                    # than value-equivalence-wins.
                    key_dict = dict(zip(source_meta.pk_columns, key))
                    conflicts.append(
                        RowDiff(table, key_dict, "modified", target_row, source_row)
                    )
                continue
            if source_row == base_row:
                continue
            if target_row == base_row:
                diff = self._row_diff_from_target_to_source(table, source_meta, key, target_row, source_row)
                if diff is not None:
                    changes.append(diff)
                continue
            diff = self._row_diff_from_target_to_source(table, source_meta, key, target_row, source_row)
            if diff is not None:
                conflicts.append(diff)
        return changes, conflicts

    def _row_diff_from_target_to_source(
        self,
        table: str,
        meta: _TableMeta,
        key: tuple[Any, ...],
        target_row: dict[str, Any] | None,
        source_row: dict[str, Any] | None,
    ) -> RowDiff | None:
        if target_row == source_row:
            return None
        key_dict = dict(zip(meta.pk_columns, key))
        if target_row is None and source_row is not None:
            return RowDiff(table, key_dict, "added", None, source_row)
        if target_row is not None and source_row is None:
            return RowDiff(table, key_dict, "deleted", target_row, None)
        return RowDiff(table, key_dict, "modified", target_row, source_row)

    def _merge_rows_by_key_for_keys(
        self,
        table: str,
        keys: list[dict[str, Any]],
        base_segment: _IntervalSegment,
        base_meta: _TableMeta,
        source_segment: _IntervalSegment,
        source_meta: _TableMeta,
        target_segment: _IntervalSegment,
        target_meta: _TableMeta,
    ) -> tuple[
        dict[tuple[Any, ...], dict[str, Any]],
        dict[tuple[Any, ...], dict[str, Any]],
        dict[tuple[Any, ...], dict[str, Any]],
    ]:
        if self.db.dialect != "postgres" or not hasattr(self.db.raw_connection, "pipeline"):
            return (
                self._rows_by_key_for_keys(table, keys, base_segment, base_meta),
                self._rows_by_key_for_keys(table, keys, source_segment, source_meta),
                self._rows_by_key_for_keys(table, keys, target_segment, target_meta),
            )

        raw = self.db.raw_connection
        with raw.pipeline():
            base_cur = self._visible_rows_for_keys_cursor(keys, base_segment, base_meta)
            source_cur = self._visible_rows_for_keys_cursor(keys, source_segment, source_meta)
            target_cur = self._visible_rows_for_keys_cursor(keys, target_segment, target_meta)
        return (
            self._rows_by_key_from_rows(base_meta, base_cur.fetchall()),
            self._rows_by_key_from_rows(source_meta, source_cur.fetchall()),
            self._rows_by_key_from_rows(target_meta, target_cur.fetchall()),
        )

    def apply_merge_changes(
        self,
        source: str,
        target: str,
        changes: list[RowDiff],
    ) -> int | None:
        _source_segment, current_segment = self._current_segments_for_merge(source, target)
        successor = self._merge_successor_segment(current_segment)
        now = _utc_now()
        self._insert_segment(
            successor,
            current_segment.segment_id,
            target,
            "mutable",
            now,
        )
        successor_segment = _IntervalSegment(
            segment_id=int(successor["segment_id"]),
            live_lo=int(successor["live_lo"]),
            live_hi=int(successor["live_hi"]),
            branch_point=int(successor["branch_point"]),
        )

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
                if change.after is None:
                    raise BranchingError("merge change is missing source row")
                pending_upserts.setdefault(change.table, []).append(change.after)

        applied = 0
        for table in table_order:
            meta = self._meta_for_segment(successor_segment, table)
            deletes = pending_deletes.get(table, [])
            if deletes:
                self._delete_keys_in_segment(
                    target,
                    table,
                    deletes,
                    successor_segment,
                    meta,
                )
                applied += len(deletes)
            upserts = pending_upserts.get(table, [])
            if upserts:
                self._upsert_rows_in_segment(
                    target,
                    table,
                    upserts,
                    successor_segment,
                    meta,
                )
                applied += len(upserts)

        self.db.execute(
            """
            UPDATE _chronos_branch_interval_branches
               SET current_segment_id = ?
             WHERE branch_id = ?
               AND current_segment_id = ?
            """,
            (successor_segment.segment_id, target, current_segment.segment_id),
        )
        return applied

    def _snapshot_diff_rows(
        self,
        left: str,
        right: str,
        table: str,
        meta: _TableMeta | None,
    ) -> list[RowDiff]:
        if meta is None:
            return []
        left_rows = self._snapshot_rows_by_key(left, table, meta)
        right_rows = self._snapshot_rows_by_key(right, table, meta)
        return self._classify_row_diffs(table, meta, left_rows, right_rows)

    def _snapshot_rows_by_key(
        self, branch_id: str, table: str, meta: _TableMeta
    ) -> dict[tuple[Any, ...], dict[str, Any]]:
        try:
            rows = self.visible_rows(branch_id, table)
        except TableNotRegisteredError:
            rows = []
        return {
            tuple(row[column] for column in meta.pk_columns): row
            for row in rows
        }

    def _classify_row_diffs(
        self,
        table: str,
        meta: _TableMeta,
        left_rows: dict[tuple[Any, ...], dict[str, Any]],
        right_rows: dict[tuple[Any, ...], dict[str, Any]],
    ) -> list[RowDiff]:
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

    def _segment_ancestry_ids(self, segment_id: int) -> list[int]:
        return [int(row["segment_id"]) for row in self._segment_ancestry_rows(segment_id)]

    def _direct_sibling_merge_base(
        self, source_segment_id: int, target_segment_id: int
    ) -> tuple[_IntervalSegment, list[int], list[int]] | None:
        row = self.db.execute(
            """
            SELECT
              base.segment_id AS base_segment_id,
              base.live_lo AS base_live_lo,
              base.live_hi AS base_live_hi,
              base.branch_point AS base_branch_point,
              source.segment_kind AS source_kind,
              target.segment_kind AS target_kind
            FROM _chronos_branch_interval_segments AS source
            JOIN _chronos_branch_interval_segments AS target
              ON target.segment_id = ?
             AND target.parent_segment_id = source.parent_segment_id
            JOIN _chronos_branch_interval_segments AS base
              ON base.segment_id = source.parent_segment_id
             AND base.segment_kind = 'fork_base'
            WHERE source.segment_id = ?
            """,
            (int(target_segment_id), int(source_segment_id)),
        ).fetchone()
        if row is None:
            return None
        if row["source_kind"] != "mutable" or row["target_kind"] != "mutable":
            return None
        return (
            _IntervalSegment(
                segment_id=int(row["base_segment_id"]),
                live_lo=int(row["base_live_lo"]),
                live_hi=int(row["base_live_hi"]),
                branch_point=int(row["base_branch_point"]),
            ),
            [int(source_segment_id)],
            [int(target_segment_id)],
        )

    def _segment_ancestry_rows(self, segment_id: int) -> list[dict[str, Any]]:
        # Merge and diff paths can walk long serial branch histories. Fetch the
        # full parent chain in one statement so ancestry cost is proportional to
        # path length inside the database, not to path length in network round trips.
        rows = self.db.execute(
            """
            WITH RECURSIVE ancestry AS (
                SELECT segment_id, parent_segment_id, segment_kind, live_lo, live_hi,
                       branch_point, 0 AS depth
                FROM _chronos_branch_interval_segments
                WHERE segment_id = ?
              UNION ALL
                SELECT parent.segment_id, parent.parent_segment_id, parent.segment_kind,
                       parent.live_lo, parent.live_hi, parent.branch_point,
                       ancestry.depth + 1 AS depth
                FROM _chronos_branch_interval_segments AS parent
                JOIN ancestry ON parent.segment_id = ancestry.parent_segment_id
            )
            SELECT segment_id, parent_segment_id, segment_kind, live_lo, live_hi,
                   branch_point
            FROM ancestry
            ORDER BY depth DESC
            """,
            (int(segment_id),),
        ).fetchall()
        if not rows:
            raise BranchNotFoundError(f"segment:{segment_id}")
        return [dict(row) for row in rows]

    def _nearest_shared_fork_base_from_paths(
        self, left_path: list[dict[str, Any]], right_path: list[dict[str, Any]]
    ) -> _IntervalSegment | None:
        right_ids = {int(row["segment_id"]) for row in right_path}
        for row in reversed(left_path):
            if int(row["segment_id"]) in right_ids and row["segment_kind"] == "fork_base":
                return _IntervalSegment(
                    segment_id=int(row["segment_id"]),
                    live_lo=int(row["live_lo"]),
                    live_hi=int(row["live_hi"]),
                    branch_point=int(row["branch_point"]),
                )
        return None

    def _nearest_shared_fork_base(
        self, left_segment_id: int, right_segment_id: int
    ) -> _IntervalSegment | None:
        left_path = self._segment_ancestry_rows(left_segment_id)
        right_path = self._segment_ancestry_rows(right_segment_id)
        return self._nearest_shared_fork_base_from_paths(left_path, right_path)

    def _mutable_segment_ids_after_base_from_path(
        self, path: list[dict[str, Any]], base_segment_id: int
    ) -> list[int]:
        after_base = False
        ids: list[int] = []
        for row in path:
            row_id = int(row["segment_id"])
            if row_id == base_segment_id:
                after_base = True
                continue
            if after_base and row["segment_kind"] == "mutable":
                ids.append(row_id)
        if not after_base:
            raise BranchingError(
                f"segment {base_segment_id} is not an ancestor of segment {path[-1]['segment_id']}"
            )
        return ids

    def _mutable_segment_ids_after_base(
        self, segment_id: int, base_segment_id: int
    ) -> list[int]:
        path = self._segment_ancestry_rows(segment_id)
        return self._mutable_segment_ids_after_base_from_path(path, base_segment_id)

    def _divergent_segment_ids(self, left_segment_id: int, right_segment_id: int) -> list[int]:
        left_path = self._segment_ancestry_ids(left_segment_id)
        right_path = self._segment_ancestry_ids(right_segment_id)
        right_set = set(right_path)
        left_set = set(left_path)
        divergent = (left_set - right_set) | (right_set - left_set)
        return sorted(divergent)

    def _candidate_keys_for_writer_segments(
        self, meta: _TableMeta, writer_segment_ids: list[int]
    ) -> list[dict[str, Any]]:
        if not writer_segment_ids:
            return []
        pk_sql = ", ".join(_quote(column) for column in meta.pk_columns)
        placeholders = _placeholders(len(writer_segment_ids))
        rows = self.db.execute(
            f"""
            SELECT DISTINCT {pk_sql}
            FROM {_quote(meta.physical_name)}
            WHERE writer_segment_id IN ({placeholders})
            """,
            tuple(writer_segment_ids),
        ).fetchall()
        return [
            {column: row[column] for column in meta.pk_columns}
            for row in rows
        ]

    def _rows_by_key_for_keys(
        self,
        table: str,
        keys: list[dict[str, Any]],
        segment: _IntervalSegment,
        meta: _TableMeta,
    ) -> dict[tuple[Any, ...], dict[str, Any]]:
        rows = self._visible_rows_for_keys(table, keys, segment, meta=meta)
        return self._rows_by_key_from_rows(meta, rows)

    def _rows_by_key_from_rows(
        self, meta: _TableMeta, rows: list[Any]
    ) -> dict[tuple[Any, ...], dict[str, Any]]:
        return {
            tuple(row[column] for column in meta.pk_columns): dict(row)
            for row in rows
        }

    def _snapshot_rows_by_key_for_segment(
        self, table: str, meta: _TableMeta, segment: _IntervalSegment
    ) -> dict[tuple[Any, ...], dict[str, Any]]:
        cols = ", ".join(_quote(c) for c in meta.columns)
        rows = self.db.execute(
            f"""
            SELECT {cols}
            FROM {_quote(meta.physical_name)}
            WHERE live_lo <= ?
              AND ? < live_hi
              AND deleted = FALSE
            """,
            (segment.branch_point, segment.branch_point),
        ).fetchall()
        return {
            tuple(row[column] for column in meta.pk_columns): dict(row)
            for row in rows
        }

    def upsert_row(self, branch_id: str, table: str, row: dict[str, Any]) -> None:
        self._insert_visible(branch_id, table, row, allow_replace=True)

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
        keys = [self._row_key(meta, row) for row in deduped]
        chunk_size = self._upsert_batch_chunk_size(meta)

        physical_keys: set[tuple[Any, ...]] = set()
        key_chunks = [keys[start : start + chunk_size] for start in range(0, len(keys), chunk_size)]
        if self.db.dialect == "postgres" and hasattr(self.db.raw_connection, "pipeline"):
            raw = self.db.raw_connection
            with raw.pipeline():
                cursors = [
                    self._physical_row_key_tuples_for_keys_cursor(chunk, segment, meta)
                    for chunk in key_chunks
                ]
            for cursor in cursors:
                physical_keys |= self._physical_row_key_tuples_from_rows(
                    meta, cursor.fetchall()
                )
        else:
            for chunk in key_chunks:
                physical_keys |= self._physical_row_key_tuples_for_keys(
                    table,
                    chunk,
                    segment,
                    meta=meta,
                )

        direct_rows: list[dict[str, Any]] = []
        for row in deduped:
            key = self._row_key(meta, row)
            if self._key_tuple(meta, key) in physical_keys:
                self._splice_row(table, key, row, False, branch_id, segment=segment, meta=meta)
            else:
                direct_rows.append(row)

        for start in range(0, len(direct_rows), _UPSERT_BATCH_CHUNK):
            self._insert_physical_rows(
                meta,
                direct_rows[start : start + _UPSERT_BATCH_CHUNK],
                segment.live_lo,
                segment.live_hi,
                False,
                segment.segment_id,
            )

    def _upsert_batch_chunk_size(self, meta: _TableMeta) -> int:
        max_keys = (
            _SQLITE_UPSERT_BATCH_CHUNK
            if self.db.dialect == "sqlite"
            else _UPSERT_BATCH_CHUNK
        )
        return max(
            1,
            min(
                max_keys,
                _UPSERT_BATCH_PARAM_BUDGET // max(1, len(meta.pk_columns)),
            ),
        )

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
        chunk_size = self._upsert_batch_chunk_size(meta)

        physical_keys: set[tuple[Any, ...]] = set()
        for start in range(0, len(deduped), chunk_size):
            physical_keys |= self._physical_row_key_tuples_for_keys(
                table,
                deduped[start : start + chunk_size],
                segment,
                meta=meta,
            )

        direct_tombstones: list[dict[str, Any]] = []
        for key in deduped:
            key_tuple = self._key_tuple(meta, key)
            if key_tuple in physical_keys:
                self._splice_row(table, key, None, True, branch_id, segment=segment, meta=meta)
            else:
                tombstone = {column: None for column in meta.columns}
                tombstone.update(key)
                direct_tombstones.append(tombstone)

        for start in range(0, len(direct_tombstones), _UPSERT_BATCH_CHUNK):
            self._insert_physical_rows(
                meta,
                direct_tombstones[start : start + _UPSERT_BATCH_CHUNK],
                segment.live_lo,
                segment.live_hi,
                True,
                segment.segment_id,
            )

    def _visible_subquery(self, meta: _TableMeta) -> str:
        cols = ", ".join(_quote(c) for c in meta.columns)
        return (
            f"SELECT {cols} FROM {_quote(meta.physical_name)} "
            "WHERE live_lo <= :_chronos_branch_point "
            "AND :_chronos_branch_point < live_hi "
            "AND deleted = FALSE"
        )

    def _tables_for_ref(self, ref: _PreparedBranchRef) -> dict[str, _TableMeta]:
        tables = ref.metadata.get("tables")
        if tables is not None:
            return tables
        return self.tables

    def _meta_for_ref(self, ref: _PreparedBranchRef, table: str) -> _TableMeta:
        try:
            return self._tables_for_ref(ref)[table]
        except KeyError as exc:
            raise TableNotRegisteredError(table) from exc

    def _meta_for_segment(self, segment: _IntervalSegment, table: str) -> _TableMeta:
        if not self.enable_schema_branching:
            return self._require_table(table)
        active = self._active_tables_for_segment(segment)
        try:
            return active[table]
        except KeyError as exc:
            raise TableNotRegisteredError(table) from exc

    def _active_tables_for_segment(self, segment: _IntervalSegment) -> dict[str, _TableMeta]:
        self._ensure_schema_branching_tables()
        rows = self.db.execute(
            """
            SELECT b.table_name, b.tombstone, v.*
            FROM _chronos_branch_table_bindings b
            LEFT JOIN _chronos_branch_table_schema_versions v
              ON v.backend = b.backend
             AND v.schema_version_id = b.schema_version_id
            WHERE b.backend = ?
              AND b.live_lo <= ?
              AND ? < b.live_hi
              AND b.tombstone = 0
            """,
            (self.name, segment.branch_point, segment.branch_point),
        ).fetchall()
        tables: dict[str, _TableMeta] = {}
        for row in rows:
            meta = self._schema_meta_from_row(row)
            if meta is not None:
                tables[row["table_name"]] = meta
        return tables

    def _schema_meta_cursor_for_segment_table(
        self, segment: _IntervalSegment, table: str
    ) -> Any:
        return self.db.execute(
            """
            SELECT b.table_name, b.tombstone, v.*
            FROM _chronos_branch_table_bindings b
            LEFT JOIN _chronos_branch_table_schema_versions v
              ON v.backend = b.backend
             AND v.schema_version_id = b.schema_version_id
            WHERE b.backend = ?
              AND b.table_name = ?
              AND b.live_lo <= ?
              AND ? < b.live_hi
              AND b.tombstone = 0
            """,
            (self.name, table, segment.branch_point, segment.branch_point),
        )

    def _schema_meta_for_segment_table(
        self, segment: _IntervalSegment, table: str
    ) -> _TableMeta | None:
        return self._schema_meta_from_row(
            self._schema_meta_cursor_for_segment_table(segment, table).fetchone()
        )

    def _schema_meta_from_row(self, row: Any | None) -> _TableMeta | None:
        if row is None or bool(row["tombstone"]):
            return None
        physical_table = row["physical_table"]
        if physical_table is None:
            return None
        return _TableMeta(
            name=row["table_name"],
            physical_name=physical_table,
            pk_columns=tuple(json.loads(row["pk_columns"])),
            columns=tuple(json.loads(row["columns"])),
            column_defs=tuple(json.loads(row["column_defs"])),
            backend=self.name,
        )

    def _known_schema_tables(self) -> set[str]:
        if not self.enable_schema_branching:
            return set(self.tables)
        rows = self.db.execute(
            """
            SELECT DISTINCT table_name
            FROM _chronos_branch_table_bindings
            WHERE backend = ?
            """,
            (self.name,),
        ).fetchall()
        return {row["table_name"] for row in rows} | set(self.tables)

    def _validate_query_tables_visible(self, ref: _PreparedBranchRef, sql: str) -> None:
        active = self._tables_for_ref(ref)
        known = self._known_schema_tables()
        tree = sqlglot.parse_one(sql, read=self.db.dialect)
        for table_node in tree.find_all(exp.Table):
            table = _table_key(table_node)
            if table in known and table not in active:
                raise TableNotRegisteredError(table)

    def _execute_schema_ddl(
        self,
        ref: _PreparedBranchRef,
        tree: exp.Expression,
    ) -> ExecuteResult:
        segment = self._prepared_segment(ref)
        if isinstance(tree, exp.Create):
            return self._execute_create_table_ddl(tree, segment)
        if isinstance(tree, exp.Alter):
            return self._execute_alter_table_ddl(ref, tree, segment)
        if isinstance(tree, exp.Drop):
            return self._execute_drop_table_ddl(tree, segment)
        raise UnsupportedSQLError("unsupported schema statement")

    def _execute_create_table_ddl(
        self,
        tree: exp.Create,
        segment: _IntervalSegment,
    ) -> ExecuteResult:
        if str(tree.args.get("kind", "")).upper() != "TABLE":
            raise UnsupportedSQLError("only CREATE TABLE is supported")
        schema = tree.this
        if not isinstance(schema, exp.Schema) or not isinstance(schema.this, exp.Table):
            raise UnsupportedSQLError("CREATE TABLE must define columns")
        table = _table_key(schema.this)
        active = self._active_binding_for_table(table, segment)
        if active is not None and not bool(active["tombstone"]):
            raise BranchAlreadyExistsError(table)
        columns, defs, pk_columns = self._column_defs_from_schema(schema)
        if not pk_columns:
            raise UnsupportedSQLError("CREATE TABLE requires an inline primary key")
        physical = self._physical_schema_table_name(table)
        self._create_interval_physical_table(physical, defs, pk_columns)
        schema_version_id = self._record_schema_version(
            table,
            physical,
            tuple(pk_columns),
            tuple(columns),
            tuple(defs),
            "create_table",
            None,
        )
        self._splice_table_binding(table, schema_version_id, False, segment)
        return ExecuteResult(0)

    def _execute_alter_table_ddl(
        self,
        ref: _PreparedBranchRef,
        tree: exp.Alter,
        segment: _IntervalSegment,
    ) -> ExecuteResult:
        if str(tree.args.get("kind", "")).upper() != "TABLE":
            raise UnsupportedSQLError("only ALTER TABLE is supported")
        if not isinstance(tree.this, exp.Table):
            raise UnsupportedSQLError("ALTER TABLE must target a table")
        table = _table_key(tree.this)
        old_meta = self._meta_for_ref(ref, table)
        active = self._active_binding_for_table(table, segment)
        if active is None or bool(active["tombstone"]):
            raise TableNotRegisteredError(table)
        actions = list(tree.args.get("actions") or [])
        if len(actions) != 1:
            raise UnsupportedSQLError(
                "only single-action ALTER TABLE statements are supported"
            )
        action = actions[0]
        if isinstance(action, exp.ColumnDef):
            return self._execute_alter_table_add_column_ddl(
                ref, table, old_meta, active, action, segment
            )
        if isinstance(action, exp.AlterColumn):
            return self._execute_alter_table_type_ddl(
                ref, table, old_meta, active, action, segment
            )
        if isinstance(action, exp.Drop):
            return self._execute_alter_table_drop_column_ddl(
                ref, table, old_meta, active, action, segment
            )
        raise UnsupportedSQLError(
            "only ALTER TABLE ADD COLUMN, DROP COLUMN, and ALTER COLUMN TYPE are supported"
        )

    def _execute_alter_table_add_column_ddl(
        self,
        ref: _PreparedBranchRef,
        table: str,
        old_meta: _TableMeta,
        active: Any,
        action: exp.ColumnDef,
        segment: _IntervalSegment,
    ) -> ExecuteResult:
        column = self._column_def_name(action)
        if column in old_meta.columns:
            raise BranchingError(f"column already exists: {column}")
        default_sql = self._column_def_default_sql(action)
        new_def = self._column_def_sql(action)
        columns = tuple([*old_meta.columns, column])
        defs = tuple([*old_meta.column_defs, new_def])
        schema_version_id = active["schema_version_id"]
        # Fast path for repeated DDL on a branch-private schema table. Once a
        # child/checkpoint shares this schema version, fall back to creating a new
        # physical table so the old ref remains stable.
        if self._schema_version_is_private_to_ref(ref, table, schema_version_id, segment):
            new_meta = _TableMeta(
                name=table,
                physical_name=old_meta.physical_name,
                pk_columns=old_meta.pk_columns,
                columns=columns,
                column_defs=defs,
                backend=self.name,
            )
            self.db.execute(
                f"ALTER TABLE {_quote(old_meta.physical_name)} ADD COLUMN {new_def}"
            )
            self._update_schema_version_metadata(
                schema_version_id,
                new_meta,
                "alter_table_add_column",
            )
            return ExecuteResult(0)
        physical = self._physical_schema_table_name(table)
        self._create_interval_physical_table(
            physical, defs, old_meta.pk_columns, create_pk_hi=False
        )
        new_meta = _TableMeta(
            name=table,
            physical_name=physical,
            pk_columns=old_meta.pk_columns,
            columns=columns,
            column_defs=defs,
            backend=self.name,
        )
        parent_schema_version_id = active["schema_version_id"]
        new_schema_version_id = self._record_schema_version(
            table,
            physical,
            old_meta.pk_columns,
            columns,
            defs,
            "alter_table_add_column",
            parent_schema_version_id,
        )
        self._copy_visible_rows_to_schema_version(
            old_meta,
            new_meta,
            segment,
            default_sql_by_column={column: default_sql} if default_sql else None,
        )
        self._splice_table_binding(table, new_schema_version_id, False, segment)
        self._create_or_defer_schema_version_secondary_indexes(new_meta)
        return ExecuteResult(0)

    def _execute_alter_table_drop_column_ddl(
        self,
        ref: _PreparedBranchRef,
        table: str,
        old_meta: _TableMeta,
        active: Any,
        action: exp.Drop,
        segment: _IntervalSegment,
    ) -> ExecuteResult:
        if str(action.args.get("kind", "")).upper() != "COLUMN":
            raise UnsupportedSQLError(
                "only ALTER TABLE DROP COLUMN is supported"
            )
        column = self._drop_column_name(action)
        if column not in old_meta.columns:
            raise TableNotRegisteredError(
                f"column missing from {table}: {column}"
            )
        if column in old_meta.pk_columns:
            raise UnsupportedSQLError("dropping primary key columns is not supported")
        kept = [
            (old_column, old_def)
            for old_column, old_def in zip(old_meta.columns, old_meta.column_defs)
            if old_column != column
        ]
        columns = tuple(old_column for old_column, _old_def in kept)
        defs = tuple(old_def for _old_column, old_def in kept)
        schema_version_id = active["schema_version_id"]
        # Dropping a column in place is safe only for an unshared schema version;
        # shared versions need a new physical table without the dropped column.
        if self._schema_version_is_private_to_ref(ref, table, schema_version_id, segment):
            new_meta = _TableMeta(
                name=table,
                physical_name=old_meta.physical_name,
                pk_columns=old_meta.pk_columns,
                columns=columns,
                column_defs=defs,
                backend=self.name,
            )
            self.db.execute(
                f"ALTER TABLE {_quote(old_meta.physical_name)} DROP COLUMN {_quote(column)}"
            )
            self._update_schema_version_metadata(
                schema_version_id,
                new_meta,
                "alter_table_drop_column",
            )
            return ExecuteResult(0)
        physical = self._physical_schema_table_name(table)
        self._create_interval_physical_table(
            physical, defs, old_meta.pk_columns, create_pk_hi=False
        )
        new_meta = _TableMeta(
            name=table,
            physical_name=physical,
            pk_columns=old_meta.pk_columns,
            columns=columns,
            column_defs=defs,
            backend=self.name,
        )
        parent_schema_version_id = active["schema_version_id"]
        new_schema_version_id = self._record_schema_version(
            table,
            physical,
            old_meta.pk_columns,
            columns,
            defs,
            "alter_table_drop_column",
            parent_schema_version_id,
        )
        self._copy_visible_rows_to_schema_version(old_meta, new_meta, segment)
        self._splice_table_binding(table, new_schema_version_id, False, segment)
        self._create_or_defer_schema_version_secondary_indexes(new_meta)
        return ExecuteResult(0)

    def _execute_alter_table_type_ddl(
        self,
        ref: _PreparedBranchRef,
        table: str,
        old_meta: _TableMeta,
        active: Any,
        action: exp.AlterColumn,
        segment: _IntervalSegment,
    ) -> ExecuteResult:
        column = self._alter_column_name(action)
        if column not in old_meta.columns:
            raise TableNotRegisteredError(
                f"column missing from {table}: {column}"
            )
        dtype = action.args.get("dtype")
        if not isinstance(dtype, exp.Expression):
            raise UnsupportedSQLError("ALTER COLUMN TYPE must specify a type")
        type_sql = dtype.sql(dialect=self.db.dialect)
        column_index = old_meta.columns.index(column)
        defs = list(old_meta.column_defs)
        defs[column_index] = self._replace_column_type_sql(
            defs[column_index], column, type_sql
        )
        using = action.args.get("using")
        if isinstance(using, exp.Expression):
            select_sql = using.sql(dialect=self.db.dialect)
        else:
            select_sql = f"CAST({_quote(column)} AS {type_sql})"
        schema_version_id = active["schema_version_id"]
        # PostgreSQL can perform the type rewrite directly on a private physical
        # table. Other dialects and shared schema versions use the existing
        # SELECT-into-new-version path, which also handles SQLite compatibility.
        if (
            self.db.dialect == "postgres"
            and self._schema_version_is_private_to_ref(ref, table, schema_version_id, segment)
        ):
            new_meta = _TableMeta(
                name=table,
                physical_name=old_meta.physical_name,
                pk_columns=old_meta.pk_columns,
                columns=old_meta.columns,
                column_defs=tuple(defs),
                backend=self.name,
            )
            self.db.execute(
                f"ALTER TABLE {_quote(old_meta.physical_name)} "
                f"ALTER COLUMN {_quote(column)} TYPE {type_sql} USING {select_sql}"
            )
            self._update_schema_version_metadata(
                schema_version_id,
                new_meta,
                "alter_table_alter_column_type",
            )
            return ExecuteResult(0)
        physical = self._physical_schema_table_name(table)
        self._create_interval_physical_table(
            physical, defs, old_meta.pk_columns, create_pk_hi=False
        )
        new_meta = _TableMeta(
            name=table,
            physical_name=physical,
            pk_columns=old_meta.pk_columns,
            columns=old_meta.columns,
            column_defs=tuple(defs),
            backend=self.name,
        )
        parent_schema_version_id = active["schema_version_id"]
        new_schema_version_id = self._record_schema_version(
            table,
            physical,
            old_meta.pk_columns,
            old_meta.columns,
            tuple(defs),
            "alter_table_alter_column_type",
            parent_schema_version_id,
        )
        self._copy_visible_rows_to_schema_version(
            old_meta,
            new_meta,
            segment,
            select_sql_by_column={column: select_sql},
        )
        self._splice_table_binding(table, new_schema_version_id, False, segment)
        self._create_or_defer_schema_version_secondary_indexes(new_meta)
        return ExecuteResult(0)

    def _execute_drop_table_ddl(
        self,
        tree: exp.Drop,
        segment: _IntervalSegment,
    ) -> ExecuteResult:
        if str(tree.args.get("kind", "")).upper() != "TABLE":
            raise UnsupportedSQLError("only DROP TABLE is supported")
        if not isinstance(tree.this, exp.Table):
            raise UnsupportedSQLError("DROP TABLE must target a table")
        table = _table_key(tree.this)
        if self._active_binding_for_table(table, segment) is None:
            raise TableNotRegisteredError(table)
        self._splice_table_binding(table, None, True, segment)
        return ExecuteResult(0)

    def _column_defs_from_schema(
        self, schema: exp.Schema
    ) -> tuple[list[str], list[str], list[str]]:
        columns: list[str] = []
        defs: list[str] = []
        pk_columns: list[str] = []
        for expression in schema.expressions:
            if not isinstance(expression, exp.ColumnDef):
                raise UnsupportedSQLError("only column definitions are supported")
            column = self._column_def_name(expression)
            columns.append(column)
            defs.append(self._column_def_sql(expression, allow_primary_key=True))
            if self._column_def_has_inline_primary_key(expression):
                pk_columns.append(column)
        return columns, defs, pk_columns

    def _column_def_name(self, column_def: exp.ColumnDef) -> str:
        identifier = column_def.this
        if isinstance(identifier, exp.Identifier):
            return identifier.name
        return str(identifier)

    def _alter_column_name(self, action: exp.AlterColumn) -> str:
        identifier = action.this
        if isinstance(identifier, exp.Identifier):
            return identifier.name
        return str(identifier)

    def _drop_column_name(self, action: exp.Drop) -> str:
        target = action.this
        if isinstance(target, exp.Column):
            identifier = target.this
            if isinstance(identifier, exp.Identifier):
                return identifier.name
            return str(identifier)
        if isinstance(target, exp.Identifier):
            return target.name
        return str(target)

    def _column_def_sql(
        self, column_def: exp.ColumnDef, *, allow_primary_key: bool = False
    ) -> str:
        default_sql: str | None = None
        for constraint in column_def.args.get("constraints") or []:
            kind = (
                constraint.args.get("kind")
                if isinstance(constraint, exp.ColumnConstraint)
                else None
            )
            if allow_primary_key and isinstance(kind, exp.PrimaryKeyColumnConstraint):
                continue
            if isinstance(kind, exp.DefaultColumnConstraint):
                default_expr = kind.this
                default_sql = self._constant_default_sql(default_expr)
                continue
            raise UnsupportedSQLError(
                "column constraints other than inline primary key and constant DEFAULT are not supported"
            )
        kind = column_def.args.get("kind")
        type_sql = kind.sql(dialect=self.db.dialect) if isinstance(kind, exp.Expression) else "TEXT"
        default_clause = f" DEFAULT {default_sql}" if default_sql is not None else ""
        return f"{_quote(self._column_def_name(column_def))} {type_sql}{default_clause}"

    def _column_def_default_sql(self, column_def: exp.ColumnDef) -> str | None:
        for constraint in column_def.args.get("constraints") or []:
            kind = (
                constraint.args.get("kind")
                if isinstance(constraint, exp.ColumnConstraint)
                else None
            )
            if isinstance(kind, exp.DefaultColumnConstraint):
                return self._constant_default_sql(kind.this)
        return None

    def _constant_default_sql(self, expression: exp.Expression) -> str:
        try:
            _expr_value(expression, {})
        except UnsupportedSQLError as exc:
            raise UnsupportedSQLError("only constant DEFAULT expressions are supported") from exc
        return expression.sql(dialect=self.db.dialect)

    def _column_default_values(self, meta: _TableMeta) -> dict[str, Any]:
        defaults: dict[str, Any] = {}
        for column, definition in zip(meta.columns, meta.column_defs):
            _head, marker, default_sql = definition.partition(" DEFAULT ")
            if not marker:
                continue
            try:
                tree = sqlglot.parse_one(f"SELECT {default_sql}", read=self.db.dialect)
                expression = tree.expressions[0] if isinstance(tree, exp.Select) else tree
                defaults[column] = _expr_value(expression, {})
            except Exception:
                continue
        return defaults

    def _replace_column_type_sql(
        self, definition: str, column: str, type_sql: str
    ) -> str:
        _head, marker, default_sql = definition.partition(" DEFAULT ")
        default_clause = f" DEFAULT {default_sql}" if marker else ""
        return f"{_quote(column)} {type_sql}{default_clause}"

    def _column_def_has_inline_primary_key(self, column_def: exp.ColumnDef) -> bool:
        for constraint in column_def.args.get("constraints") or []:
            kind = constraint.args.get("kind") if isinstance(constraint, exp.ColumnConstraint) else None
            if isinstance(kind, exp.PrimaryKeyColumnConstraint):
                return True
        return False

    def _create_interval_physical_table(
        self,
        physical: str,
        column_defs: tuple[str, ...] | list[str],
        pk_columns: tuple[str, ...] | list[str],
        *,
        create_pk_hi: bool = True,
    ) -> None:
        interval_type = self._interval_sql_type()
        user_defs = ", ".join(column_defs)
        pk_sql = ", ".join(_quote(c) for c in pk_columns)
        self.db.execute(
            f"""
            CREATE TABLE {_quote(physical)} (
              {user_defs},
              live_lo {interval_type} NOT NULL,
              live_hi {interval_type} NOT NULL,
              writer_segment_id INTEGER NOT NULL,
              deleted BOOLEAN NOT NULL DEFAULT FALSE,
              PRIMARY KEY ({pk_sql}, live_lo),
              CHECK (live_lo < live_hi)
            )
            """
        )
        if create_pk_hi:
            self.db.execute(self._pk_hi_index_sql(physical, pk_columns))
            self.db.execute(self._writer_segment_index_sql(physical, pk_columns))

    def _copy_visible_rows_to_schema_version(
        self,
        old_meta: _TableMeta,
        new_meta: _TableMeta,
        segment: _IntervalSegment,
        *,
        default_sql_by_column: dict[str, str] | None = None,
        select_sql_by_column: dict[str, str] | None = None,
    ) -> None:
        default_sql_by_column = default_sql_by_column or {}
        select_sql_by_column = select_sql_by_column or {}
        target_columns = [
            *new_meta.columns,
            "live_lo",
            "live_hi",
            "writer_segment_id",
            "deleted",
        ]
        target_sql = ", ".join(_quote(column) for column in target_columns)
        old_columns = set(old_meta.columns)
        select_exprs = []
        for column in new_meta.columns:
            if column in select_sql_by_column:
                select_exprs.append(f"{select_sql_by_column[column]} AS {_quote(column)}")
            elif column in old_columns:
                select_exprs.append(_quote(column))
            elif column in default_sql_by_column:
                select_exprs.append(f"{default_sql_by_column[column]} AS {_quote(column)}")
            else:
                select_exprs.append(f"NULL AS {_quote(column)}")
        select_exprs.extend(["?", "?", _quote("writer_segment_id"), "FALSE"])
        select_sql = ", ".join(select_exprs)
        self.db.execute(
            f"""
            INSERT INTO {_quote(new_meta.physical_name)}
            ({target_sql})
            SELECT {select_sql}
            FROM {_quote(old_meta.physical_name)}
            WHERE {_quote("live_lo")} <= ?
              AND ? < {_quote("live_hi")}
              AND deleted = FALSE
            """,
            (
                segment.live_lo,
                segment.live_hi,
                segment.branch_point,
                segment.branch_point,
            ),
        )

    def _active_binding_for_table(self, table: str, segment: _IntervalSegment) -> Any | None:
        return self.db.execute(
            """
            SELECT *
            FROM _chronos_branch_table_bindings
            WHERE backend = ?
              AND table_name = ?
              AND live_lo <= ?
              AND ? < live_hi
            """,
            (self.name, table, segment.branch_point, segment.branch_point),
        ).fetchone()

    def _splice_table_binding(
        self,
        table: str,
        schema_version_id: str | None,
        tombstone: bool,
        segment: _IntervalSegment,
    ) -> None:
        # Splicing rewrites the interval map by deleting overlapping rows and
        # inserting split replacements. Row locks alone are not enough under
        # PostgreSQL READ COMMITTED because concurrent statements can miss rows
        # inserted by an earlier waiter. Serialize only splices for this logical
        # table; branch creation and DDL on other tables can still proceed.
        self._lock_schema_binding_table(table)
        u_lo = segment.live_lo
        u_hi = segment.live_hi
        lock_clause = "FOR UPDATE" if self.db.dialect == "postgres" else ""
        rows = self.db.execute(
            f"""
            SELECT *
            FROM _chronos_branch_table_bindings
            WHERE backend = ?
              AND table_name = ?
              AND live_lo < ?
              AND ? < live_hi
            ORDER BY live_lo
            {lock_clause}
            """,
            (self.name, table, u_hi, u_lo),
        ).fetchall()
        for row in rows:
            a = row["live_lo"]
            b = row["live_hi"]
            overlap_lo = max(a, u_lo)
            overlap_hi = min(b, u_hi)
            self.db.execute(
                """
                DELETE FROM _chronos_branch_table_bindings
                WHERE backend = ? AND table_name = ? AND live_lo = ?
                """,
                (self.name, table, a),
            )
            if a < overlap_lo:
                self._insert_table_binding(
                    table,
                    row["schema_version_id"],
                    bool(row["tombstone"]),
                    a,
                    overlap_lo,
                    row["metadata"],
                )
            if overlap_hi < b:
                self._insert_table_binding(
                    table,
                    row["schema_version_id"],
                    bool(row["tombstone"]),
                    overlap_hi,
                    b,
                    row["metadata"],
                )
        self._insert_table_binding(table, schema_version_id, tombstone, u_lo, u_hi, "{}")

    def _insert_table_binding(
        self,
        table: str,
        schema_version_id: str | None,
        tombstone: bool,
        live_lo: int,
        live_hi: int,
        metadata: str,
    ) -> None:
        self.db.execute(
            """
            INSERT INTO _chronos_branch_table_bindings
            (backend, table_name, schema_version_id, tombstone, live_lo, live_hi,
             created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.name,
                table,
                schema_version_id,
                1 if tombstone else 0,
                live_lo,
                live_hi,
                _utc_now(),
                metadata,
            ),
        )

    def _statement_plan(self, ref: _PreparedBranchRef, sql: str) -> _StatementPlan:
        cache_key = (self.db.dialect, sql)
        plan = self._statement_plan_cache.get(cache_key)
        if plan is not None:
            return plan
        tree = sqlglot.parse_one(sql, read=self.db.dialect)
        if isinstance(tree, exp.Insert):
            plan = _build_insert_plan(tree)
        elif isinstance(tree, exp.Update):
            plan = _build_update_plan(tree, self.db.dialect)
        elif isinstance(tree, exp.Delete):
            plan = _build_delete_plan(tree, self.db.dialect)
        else:
            raise UnsupportedSQLError("only SELECT, INSERT, UPDATE, and DELETE are supported")
        if len(self._statement_plan_cache) >= _SQL_CACHE_MAX_ENTRIES:
            self._statement_plan_cache.clear()
        self._statement_plan_cache[cache_key] = plan
        return plan

    def _insert_visible(
        self,
        branch_id: str,
        table: str,
        row: dict[str, Any],
        allow_replace: bool = False,
        segment: _IntervalSegment | None = None,
    ) -> None:
        segment = segment or self._current_segment(branch_id)
        meta = self._meta_for_segment(segment, table)
        key = self._row_key(meta, row)
        visible = self._visible_row(branch_id, table, key, segment=segment)
        if visible is not None and not allow_replace:
            raise DuplicateKeyError(f"duplicate key on branch {branch_id}: {key}")
        self._splice_row(table, key, row, False, branch_id, segment=segment, meta=meta)

    def _insert_visible_batch(
        self,
        branch_id: str,
        table: str,
        rows: list[dict[str, Any]],
        segment: _IntervalSegment,
        meta: _TableMeta | None = None,
        ignore_conflicts: bool = False,
    ) -> int:
        if not rows:
            return 0
        meta = meta or self._meta_for_segment(segment, table)
        if ignore_conflicts:
            inserted = 0
            seen_keys: set[tuple[Any, ...]] = set()
            for row in rows:
                key = self._row_key(meta, row)
                key_tuple = self._key_tuple(meta, key)
                if key_tuple in seen_keys:
                    continue
                seen_keys.add(key_tuple)
                try:
                    self._insert_visible(
                        branch_id,
                        table,
                        row,
                        allow_replace=False,
                        segment=segment,
                    )
                except DuplicateKeyError:
                    continue
                inserted += 1
            return inserted
        keyed_rows = [(self._row_key(meta, row), row) for row in rows]
        seen_keys: set[tuple[Any, ...]] = set()
        for key, _ in keyed_rows:
            key_tuple = self._key_tuple(meta, key)
            if key_tuple in seen_keys:
                raise DuplicateKeyError(f"duplicate key on branch {branch_id}: {key}")
            seen_keys.add(key_tuple)
        keys = [key for key, _ in keyed_rows]
        if self.db.dialect == "postgres" and hasattr(self.db.raw_connection, "pipeline"):
            raw = self.db.raw_connection
            with raw.pipeline():
                visible_cur = self._visible_rows_for_keys_cursor(keys, segment, meta)
                physical_cur = self._physical_row_key_tuples_for_keys_cursor(
                    keys, segment, meta
                )
            visible = [dict(row) for row in visible_cur.fetchall()]
            physical_row_keys = self._physical_row_key_tuples_from_rows(
                meta, physical_cur.fetchall()
            )
        else:
            visible = self._visible_rows_for_keys(table, keys, segment, meta=meta)
            physical_row_keys = self._physical_row_key_tuples_for_keys(
                table, keys, segment, meta=meta
            )
        if visible:
            duplicate = self._row_key(meta, visible[0])
            raise DuplicateKeyError(f"duplicate key on branch {branch_id}: {duplicate}")

        direct_rows: list[dict[str, Any]] = []
        for key, row in keyed_rows:
            if self._key_tuple(meta, key) in physical_row_keys:
                self._splice_row(table, key, row, False, branch_id, segment=segment)
            else:
                direct_rows.append(row)
        self._insert_physical_rows(
            meta,
            direct_rows,
            segment.live_lo,
            segment.live_hi,
            False,
            segment.segment_id,
        )
        return len(rows)

    def _visible_row(
        self,
        branch_id: str,
        table: str,
        key: dict[str, Any],
        segment: _IntervalSegment | None = None,
        meta: _TableMeta | None = None,
    ) -> dict[str, Any] | None:
        segment = segment or self._current_segment(branch_id)
        meta = meta or self._meta_for_segment(segment, table)
        point = segment.branch_point
        cols = ", ".join(_quote(c) for c in meta.columns)
        where = self._key_where(meta)
        row = self.db.execute(
            f"""
            SELECT {cols}
            FROM {_quote(meta.physical_name)}
            WHERE {where}
              AND live_lo <= ?
              AND ? < live_hi
              AND deleted = FALSE
            """,
            [*self._key_values(meta, key), point, point],
        ).fetchone()
        return dict(row) if row is not None else None

    def _select_matching_rows(
        self,
        ref: _PreparedBranchRef,
        table: str,
        where: str,
        params: dict[str, Any],
        direct_filter: bool = True,
    ) -> list[dict[str, Any]]:
        if not direct_filter:
            return self._select_matching_rows_via_query(ref, table, where, params)
        meta = self._meta_for_ref(ref, table)
        select_cols = ", ".join(_quote(c) for c in meta.columns)
        segment = self._prepared_segment(ref)
        visible_where = self._visible_where_for_user_filter(where)
        bound = dict(params)
        bound["_chronos_branch_point"] = segment.branch_point
        rows = self.db.execute(
            f"""
            SELECT {select_cols}
            FROM {_quote(meta.physical_name)}
            WHERE {visible_where}
            """,
            bound,
        ).fetchall()
        return [dict(row) for row in rows]

    def _try_update_full_table_with_batch_splice(
        self,
        ref: _PreparedBranchRef,
        table: str,
        meta: _TableMeta,
        plan: _UpdatePlan,
        params: dict[str, Any],
        segment: _IntervalSegment,
    ) -> int | None:
        if self.db.dialect != "postgres":
            return None
        if plan.where_sql.strip():
            return None
        if any(column in meta.pk_columns for column, _expr in plan.assignments):
            return None
        if any(column not in meta.columns for column, _expr in plan.assignments):
            return None
        if any(expr.find(exp.Select) is not None for _column, expr in plan.assignments):
            return None

        suffix = uuid.uuid4().hex
        visible_table = f"_chronos_tmp_update_visible_{suffix}"
        overlap_table = f"_chronos_tmp_update_overlap_{suffix}"
        assigned = {column: expr for column, expr in plan.assignments}
        source_alias = "src"
        visible_selects = []
        for column in meta.columns:
            expr = assigned.get(column)
            if expr is None:
                select_sql = f"{source_alias}.{_quote(column)}"
            else:
                select_sql = self._source_expression_sql(expr, source_alias, meta)
            visible_selects.append(f"{select_sql} AS {_quote(column)}")

        pk_join = self._join_on_columns("p", "v", meta.pk_columns)
        delete_join = " AND ".join(
            [
                self._join_on_columns("p", "o", meta.pk_columns),
                f"p.{_quote('live_lo')} = o.{_quote('live_lo')}",
                f"p.{_quote('live_hi')} = o.{_quote('live_hi')}",
            ]
        )
        overlap_visible_join = self._join_on_columns("o", "v", meta.pk_columns)
        target_cols = [*meta.columns, "live_lo", "live_hi", "writer_segment_id", "deleted"]
        target_sql = ", ".join(_quote(column) for column in target_cols)
        old_cols = ", ".join(f"o.{_quote(column)}" for column in meta.columns)
        new_cols = ", ".join(f"v.{_quote(column)}" for column in meta.columns)
        update_set = ", ".join(
            [f"{_quote(column)} = v.{_quote(column)}" for column in meta.columns]
            + [
                f"{_quote('writer_segment_id')} = :__chronos_batch_update_writer_segment_id",
                f"{_quote('deleted')} = FALSE",
            ]
        )
        bound = dict(params)
        bound.update(
            {
                "__chronos_batch_update_branch_point": segment.branch_point,
                "__chronos_batch_update_live_lo": segment.live_lo,
                "__chronos_batch_update_live_hi": segment.live_hi,
                "__chronos_batch_update_writer_segment_id": segment.segment_id,
            }
        )

        try:
            self.db.execute(
                f"""
                CREATE TEMP TABLE {_quote(visible_table)} ON COMMIT DROP AS
                SELECT {", ".join(visible_selects)}
                FROM {_quote(meta.physical_name)} AS {source_alias}
                WHERE {source_alias}.{_quote("live_lo")} <= :__chronos_batch_update_branch_point
                  AND :__chronos_batch_update_branch_point < {source_alias}.{_quote("live_hi")}
                  AND {source_alias}.{_quote("deleted")} = FALSE
                """,
                bound,
            )
            count_row = self.db.execute(
                f"SELECT COUNT(*) AS count FROM {_quote(visible_table)}"
            ).fetchone()
            count = int(count_row["count"])
            if count == 0:
                return 0

            self.db.execute(
                f"""
                UPDATE {_quote(meta.physical_name)} AS p
                SET {update_set}
                FROM {_quote(visible_table)} AS v
                WHERE {pk_join}
                  AND :__chronos_batch_update_live_lo <= p.{_quote("live_lo")}
                  AND p.{_quote("live_hi")} <= :__chronos_batch_update_live_hi
                """,
                bound,
            )
            self.db.execute(
                f"""
                CREATE TEMP TABLE {_quote(overlap_table)} ON COMMIT DROP AS
                SELECT p.*
                FROM {_quote(meta.physical_name)} AS p
                JOIN {_quote(visible_table)} AS v ON {pk_join}
                WHERE p.{_quote("live_lo")} < :__chronos_batch_update_live_hi
                  AND :__chronos_batch_update_live_lo < p.{_quote("live_hi")}
                  AND NOT (
                    :__chronos_batch_update_live_lo <= p.{_quote("live_lo")}
                    AND p.{_quote("live_hi")} <= :__chronos_batch_update_live_hi
                  )
                """,
                bound,
            )
            self.db.execute(
                f"""
                DELETE FROM {_quote(meta.physical_name)} AS p
                USING {_quote(overlap_table)} AS o
                WHERE {delete_join}
                """
            )
            self.db.execute(
                f"""
                INSERT INTO {_quote(meta.physical_name)} ({target_sql})
                SELECT {old_cols},
                       o.{_quote("live_lo")},
                       :__chronos_batch_update_live_lo,
                       o.{_quote("writer_segment_id")},
                       o.{_quote("deleted")}
                FROM {_quote(overlap_table)} AS o
                WHERE o.{_quote("live_lo")} < :__chronos_batch_update_live_lo
                """,
                bound,
            )
            self.db.execute(
                f"""
                INSERT INTO {_quote(meta.physical_name)} ({target_sql})
                SELECT {new_cols},
                       GREATEST(o.{_quote("live_lo")}, :__chronos_batch_update_live_lo),
                       LEAST(o.{_quote("live_hi")}, :__chronos_batch_update_live_hi),
                       :__chronos_batch_update_writer_segment_id,
                       FALSE
                FROM {_quote(overlap_table)} AS o
                JOIN {_quote(visible_table)} AS v ON {overlap_visible_join}
                WHERE GREATEST(o.{_quote("live_lo")}, :__chronos_batch_update_live_lo)
                    < LEAST(o.{_quote("live_hi")}, :__chronos_batch_update_live_hi)
                """,
                bound,
            )
            self.db.execute(
                f"""
                INSERT INTO {_quote(meta.physical_name)} ({target_sql})
                SELECT {old_cols},
                       :__chronos_batch_update_live_hi,
                       o.{_quote("live_hi")},
                       o.{_quote("writer_segment_id")},
                       o.{_quote("deleted")}
                FROM {_quote(overlap_table)} AS o
                WHERE :__chronos_batch_update_live_hi < o.{_quote("live_hi")}
                """,
                bound,
            )
            return count
        finally:
            for temp_table in (overlap_table, visible_table):
                try:
                    self.db.execute(f"DROP TABLE IF EXISTS {_quote(temp_table)}")
                except Exception:
                    pass

    def _try_update_private_schema_table_in_place(
        self,
        ref: _PreparedBranchRef,
        table: str,
        meta: _TableMeta,
        plan: _UpdatePlan,
        params: dict[str, Any],
        segment: _IntervalSegment,
        *,
        private_canonical: bool | None = None,
    ) -> int | None:
        if private_canonical is None:
            private_canonical = self._private_schema_table_is_canonical_for_ref(
                ref, table, segment
            )
        if not private_canonical:
            return None
        if not plan.direct_filter:
            return None
        if any(column in meta.pk_columns for column, _expr in plan.assignments):
            return None
        if any(column not in meta.columns for column, _expr in plan.assignments):
            return None
        if any(expr.find(exp.Select) is not None for _column, expr in plan.assignments):
            return None

        # Canonical private schema tables contain only the current branch's rows.
        # Direct DML preserves that shape; once the table is shared by a child or
        # checkpoint, callers stop using this path and return to interval splicing.
        assignments = []
        for column, expr in plan.assignments:
            value_sql = self._physical_expression_sql(expr, meta)
            assignments.append(f"{_quote(column)} = {value_sql}")
        assignments.append(f"{_quote('writer_segment_id')} = :__chronos_writer_segment_id")
        assignments.append(f"{_quote('deleted')} = FALSE")
        if not assignments:
            return 0

        where_sql = self._user_where_filter(plan.where_sql)
        bound = dict(params)
        bound["__chronos_writer_segment_id"] = segment.segment_id
        result = self.db.execute(
            f"""
            UPDATE {_quote(meta.physical_name)}
            SET {", ".join(assignments)}
            {where_sql}
            """,
            bound,
        )
        rowcount = getattr(result, "rowcount", None)
        if rowcount is None or rowcount < 0:
            return None
        return int(rowcount)

    def _try_delete_private_schema_table_in_place(
        self,
        ref: _PreparedBranchRef,
        table: str,
        meta: _TableMeta,
        plan: _DeletePlan,
        params: dict[str, Any],
        segment: _IntervalSegment,
        *,
        private_canonical: bool | None = None,
    ) -> int | None:
        if private_canonical is None:
            private_canonical = self._private_schema_table_is_canonical_for_ref(
                ref, table, segment
            )
        if not private_canonical or not plan.direct_filter:
            return None
        where_sql = self._user_where_filter(plan.where_sql)
        result = self.db.execute(
            f"""
            DELETE FROM {_quote(meta.physical_name)}
            {where_sql}
            """,
            dict(params),
        )
        rowcount = getattr(result, "rowcount", None)
        if rowcount is None or rowcount < 0:
            return None
        return int(rowcount)

    def _private_schema_table_is_canonical_for_ref(
        self,
        ref: _PreparedBranchRef,
        table: str,
        segment: _IntervalSegment,
    ) -> bool:
        if not self.enable_schema_branching:
            return False
        active = self._active_binding_for_table(table, segment)
        if active is None or bool(active["tombstone"]):
            return False
        schema_version_id = active["schema_version_id"]
        if not self._schema_version_is_private_to_ref(
            ref, table, schema_version_id, segment
        ):
            return False
        row = self.db.execute(
            """
            SELECT ddl_op
            FROM _chronos_branch_table_schema_versions
            WHERE backend = ?
              AND schema_version_id = ?
            """,
            (self.name, schema_version_id),
        ).fetchone()
        if row is None:
            return False
        ddl_op = row["ddl_op"]
        if ddl_op != "register":
            return True
        return self._segment_is_root(segment)

    def _segment_is_root(self, segment: _IntervalSegment) -> bool:
        row = self.db.execute(
            """
            SELECT parent_segment_id
            FROM _chronos_branch_interval_segments
            WHERE segment_id = ?
            """,
            (segment.segment_id,),
        ).fetchone()
        return row is not None and row["parent_segment_id"] is None

    def _canonicalize_private_schema_table(
        self,
        meta: _TableMeta,
        segment: _IntervalSegment,
    ) -> None:
        suffix = uuid.uuid4().hex
        visible_table = f"_chronos_tmp_canonical_visible_{suffix}"
        cols = ", ".join(_quote(column) for column in meta.columns)
        visible_cols = ", ".join(
            _quote(column) for column in [*meta.columns, "writer_segment_id"]
        )
        target_cols = ", ".join(
            _quote(column)
            for column in [*meta.columns, "live_lo", "live_hi", "writer_segment_id", "deleted"]
        )
        try:
            self.db.execute(
                f"""
                CREATE TEMP TABLE {_quote(visible_table)} ON COMMIT DROP AS
                SELECT {visible_cols}
                FROM {_quote(meta.physical_name)}
                WHERE {_quote("live_lo")} <= :__chronos_canonical_branch_point
                  AND :__chronos_canonical_branch_point < {_quote("live_hi")}
                  AND {_quote("deleted")} = FALSE
                """,
                {"__chronos_canonical_branch_point": segment.branch_point},
            )
            self.db.execute(f"DELETE FROM {_quote(meta.physical_name)}")
            self.db.execute(
                f"""
                INSERT INTO {_quote(meta.physical_name)} ({target_cols})
                SELECT {cols},
                       :__chronos_canonical_live_lo,
                       :__chronos_canonical_live_hi,
                       {_quote("writer_segment_id")},
                       FALSE
                FROM {_quote(visible_table)}
                """,
                {
                    "__chronos_canonical_live_lo": segment.live_lo,
                    "__chronos_canonical_live_hi": segment.live_hi,
                },
            )
        finally:
            try:
                self.db.execute(f"DROP TABLE IF EXISTS {_quote(visible_table)}")
            except Exception:
                pass

    def _source_expression_sql(
        self, expr: exp.Expression, source_alias: str, meta: _TableMeta
    ) -> str:
        return self._physical_expression_sql(expr, meta, source_alias=source_alias)

    def _physical_expression_sql(
        self,
        expr: exp.Expression,
        meta: _TableMeta,
        source_alias: str | None = None,
    ) -> str:
        columns = set(meta.columns)

        def replace(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Column) and node.name in columns:
                if source_alias is None:
                    return exp.column(node.name, quoted=True)
                return exp.column(node.name, table=source_alias, quoted=True)
            return node

        return expr.copy().transform(replace).sql(dialect=self.db.dialect)

    def _join_on_columns(
        self, left_alias: str, right_alias: str, columns: tuple[str, ...] | list[str]
    ) -> str:
        return " AND ".join(
            f"{left_alias}.{_quote(column)} = {right_alias}.{_quote(column)}"
            for column in columns
        )

    def _select_matching_keys(
        self,
        ref: _PreparedBranchRef,
        table: str,
        where: str,
        params: dict[str, Any],
        direct_filter: bool = True,
    ) -> list[dict[str, Any]]:
        if not direct_filter:
            meta = self._meta_for_ref(ref, table)
            select_cols = ", ".join(_quote(c) for c in meta.pk_columns)
            return self.query(
                ref,
                f"SELECT {select_cols} FROM {_quote_table_name(table)}{where}",
                params,
            )
        meta = self._meta_for_ref(ref, table)
        select_cols = ", ".join(_quote(c) for c in meta.pk_columns)
        segment = self._prepared_segment(ref)
        visible_where = self._visible_where_for_user_filter(where)
        bound = dict(params)
        bound["_chronos_branch_point"] = segment.branch_point
        rows = self.db.execute(
            f"""
            SELECT {select_cols}
            FROM {_quote(meta.physical_name)}
            WHERE {visible_where}
            """,
            bound,
        ).fetchall()
        return [dict(row) for row in rows]

    def _select_matching_rows_via_query(
        self,
        ref: _PreparedBranchRef,
        table: str,
        where: str,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        meta = self._meta_for_ref(ref, table)
        select_cols = ", ".join(_quote(c) for c in meta.columns)
        return self.query(
            ref,
            f"SELECT {select_cols} FROM {_quote_table_name(table)}{where}",
            params,
        )

    def _visible_where_for_user_filter(self, where: str) -> str:
        visible = (
            "live_lo <= :_chronos_branch_point "
            "AND :_chronos_branch_point < live_hi "
            "AND deleted = FALSE"
        )
        stripped = where.strip()
        if not stripped:
            return visible
        if not stripped.upper().startswith("WHERE "):
            raise UnsupportedSQLError("unsupported WHERE clause")
        return f"{visible} AND ({stripped[6:]})"

    def _user_where_filter(self, where: str) -> str:
        stripped = where.strip()
        if not stripped:
            return ""
        if not stripped.upper().startswith("WHERE "):
            raise UnsupportedSQLError("unsupported WHERE clause")
        return f"WHERE {stripped[6:]}"

    def _visible_rows_for_keys(
        self,
        table: str,
        keys: list[dict[str, Any]],
        segment: _IntervalSegment,
        meta: _TableMeta | None = None,
    ) -> list[dict[str, Any]]:
        if not keys:
            return []
        meta = meta or self._meta_for_segment(segment, table)
        rows = self._visible_rows_for_keys_cursor(keys, segment, meta).fetchall()
        return [dict(row) for row in rows]

    def _visible_rows_for_keys_cursor(
        self,
        keys: list[dict[str, Any]],
        segment: _IntervalSegment,
        meta: _TableMeta,
    ) -> Any:
        predicate, values = self._keys_predicate(meta, keys)
        cols = ", ".join(_quote(c) for c in meta.columns)
        return self.db.execute(
            f"""
            SELECT {cols}
            FROM {_quote(meta.physical_name)}
            WHERE ({predicate})
              AND live_lo <= ?
              AND ? < live_hi
              AND deleted = FALSE
            """,
            [*values, segment.branch_point, segment.branch_point],
        )

    def _physical_row_key_tuples_for_keys(
        self,
        table: str,
        keys: list[dict[str, Any]],
        segment: _IntervalSegment,
        meta: _TableMeta | None = None,
    ) -> set[tuple[Any, ...]]:
        if not keys:
            return set()
        meta = meta or self._meta_for_segment(segment, table)
        rows = self._physical_row_key_tuples_for_keys_cursor(
            keys, segment, meta
        ).fetchall()
        return self._physical_row_key_tuples_from_rows(meta, rows)

    def _physical_row_key_tuples_for_keys_cursor(
        self,
        keys: list[dict[str, Any]],
        segment: _IntervalSegment,
        meta: _TableMeta,
    ) -> Any:
        predicate, values = self._keys_predicate(meta, keys)
        pk_cols = ", ".join(_quote(c) for c in meta.pk_columns)
        return self.db.execute(
            f"""
            SELECT DISTINCT {pk_cols}
            FROM {_quote(meta.physical_name)}
            WHERE ({predicate})
              AND live_lo < ?
              AND ? < live_hi
            """,
            [*values, segment.live_hi, segment.live_lo],
        )

    def _physical_row_key_tuples_from_rows(
        self, meta: _TableMeta, rows: list[Any]
    ) -> set[tuple[Any, ...]]:
        return {
            tuple(row[column] for column in meta.pk_columns)
            for row in rows
        }

    def _keys_predicate(
        self, meta: _TableMeta, keys: list[dict[str, Any]]
    ) -> tuple[str, list[Any]]:
        parts: list[str] = []
        values: list[Any] = []
        for key in keys:
            parts.append(f"({self._key_where(meta)})")
            values.extend(self._key_values(meta, key))
        return " OR ".join(parts), values

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
        u_lo = segment.live_lo
        u_hi = segment.live_hi
        where = self._key_where(meta)
        # All physical rows overlapping this branch interval are replaced by up to
        # three physical rows: left remainder, branch-local replacement, and right
        # remainder. This makes future reads pure visibility predicates.
        physical_rows = self.db.execute(
            f"""
            SELECT *
            FROM {_quote(meta.physical_name)}
            WHERE {where}
              AND live_lo < ?
              AND ? < live_hi
            ORDER BY live_lo
            """,
            [*self._key_values(meta, key), u_hi, u_lo],
        ).fetchall()
        if not physical_rows:
            if row is None and deleted:
                replacement = {column: None for column in meta.columns}
                replacement.update(key)
            else:
                replacement = {column: row.get(column) for column in meta.columns}  # type: ignore[union-attr]
            self._insert_physical_row(
                meta, replacement, u_lo, u_hi, deleted, segment.segment_id
            )
            return
        for physical_row in physical_rows:
            a = physical_row["live_lo"]
            b = physical_row["live_hi"]
            overlap_lo = max(a, u_lo)
            overlap_hi = min(b, u_hi)
            old_row = {column: physical_row[column] for column in meta.columns}
            self.db.execute(
                f"""
                DELETE FROM {_quote(meta.physical_name)}
                WHERE {where}
                  AND live_lo = ?
                """,
                [*self._key_values(meta, key), a],
            )
            if a < overlap_lo:
                self._insert_physical_row(
                    meta,
                    old_row,
                    a,
                    overlap_lo,
                    bool(physical_row["deleted"]),
                    int(physical_row["writer_segment_id"]),
                )
            if row is None and deleted:
                replacement = dict(old_row)
            else:
                replacement = {column: row.get(column) for column in meta.columns}  # type: ignore[union-attr]
            self._insert_physical_row(
                meta,
                replacement,
                overlap_lo,
                overlap_hi,
                deleted,
                segment.segment_id,
            )
            if overlap_hi < b:
                self._insert_physical_row(
                    meta,
                    old_row,
                    overlap_hi,
                    b,
                    bool(physical_row["deleted"]),
                    int(physical_row["writer_segment_id"]),
                )

    def _insert_physical_row(
        self,
        meta: _TableMeta,
        row: dict[str, Any],
        live_lo: int,
        live_hi: int,
        deleted: bool,
        writer_segment_id: int,
    ) -> None:
        cols = [*meta.columns, "live_lo", "live_hi", "writer_segment_id", "deleted"]
        values = [row.get(column) for column in meta.columns] + [
            live_lo,
            live_hi,
            writer_segment_id,
            deleted,
        ]
        self.db.execute(
            f"""
            INSERT INTO {_quote(meta.physical_name)}
            ({", ".join(_quote(c) for c in cols)})
            VALUES ({_placeholders(len(cols))})
            """,
            values,
        )

    def _insert_physical_rows(
        self,
        meta: _TableMeta,
        rows: list[dict[str, Any]],
        live_lo: int,
        live_hi: int,
        deleted: bool,
        writer_segment_id: int,
    ) -> None:
        if not rows:
            return
        cols = [*meta.columns, "live_lo", "live_hi", "writer_segment_id", "deleted"]
        values = [
            [row.get(column) for column in meta.columns]
            + [live_lo, live_hi, writer_segment_id, deleted]
            for row in rows
        ]
        self.db.executemany(
            f"""
            INSERT INTO {_quote(meta.physical_name)}
            ({", ".join(_quote(c) for c in cols)})
            VALUES ({_placeholders(len(cols))})
            """,
            values,
        )

    def _branch_row(self, branch_id: str) -> Any | None:
        return self.db.execute(
            "SELECT * FROM _chronos_branch_interval_branches WHERE branch_id = ?",
            (branch_id,),
        ).fetchone()

    def _branch_row_for_update(self, branch_id: str) -> Any | None:
        if self._metadata_dialect() != "postgres":
            return self._branch_row(branch_id)
        return self.db.execute(
            """
            SELECT *
            FROM _chronos_branch_interval_branches
            WHERE branch_id = ?
            FOR UPDATE
            """,
            (branch_id,),
        ).fetchone()

    def lock_branches_for_merge(self, source: str, target: str) -> None:
        if self._metadata_dialect() != "postgres":
            return
        # Merge preview and apply must observe a stable pair of branch heads.
        # Lock in deterministic order so concurrent merges into the same target
        # serialize and snapshot isolation becomes true first-committer-wins.
        branch_ids = sorted({source, target})
        raw = self._metadata_raw_connection()
        if not hasattr(raw, "pipeline"):
            for branch_id in branch_ids:
                if self._branch_row_for_update(branch_id) is None:
                    raise BranchNotFoundError(branch_id)
            return
        with raw.pipeline():
            cursors = [
                self.db.execute(
                    """
                    SELECT *
                    FROM _chronos_branch_interval_branches
                    WHERE branch_id = ?
                    FOR UPDATE
                    """,
                    (branch_id,),
                )
                for branch_id in branch_ids
            ]
        for branch_id, cursor in zip(branch_ids, cursors):
            if cursor.fetchone() is None:
                raise BranchNotFoundError(branch_id)

    def _lock_branch_for_schema_change(self, ref: _PreparedBranchRef) -> None:
        # Branch-local DDL may ALTER a private physical schema table in place.
        # Locking only this mutable branch row prevents concurrent fork/checkpoint
        # from attaching to the old schema version between the privacy check and
        # the metadata/physical-table mutation. Unrelated branches can still run
        # schema changes and branch creation concurrently.
        if self._metadata_dialect() != "postgres":
            return
        if self._branch_row_for_update(ref.branch_id) is None:
            raise BranchNotFoundError(ref.branch_id)

    def _lock_schema_binding_table(self, table: str) -> None:
        if self._metadata_dialect() != "postgres":
            return
        digest = hashlib.blake2s(table.encode("utf-8"), digest_size=4).digest()
        key = int.from_bytes(digest, "big", signed=True)
        self.db.execute(
            "SELECT pg_advisory_xact_lock(?, ?)",
            (_SCHEMA_BINDING_LOCK_NAMESPACE, key),
        )

    def _branch_segment_id(self, branch_id: str) -> int:
        row = self._branch_row(branch_id)
        if row is None:
            raise BranchNotFoundError(branch_id)
        return int(row["current_segment_id"])

    def _segment(self, segment_id: int | str) -> _IntervalSegment:
        row = self.db.execute(
            "SELECT * FROM _chronos_branch_interval_segments WHERE segment_id = ?",
            (int(segment_id),),
        ).fetchone()
        if row is None:
            raise BranchNotFoundError(f"segment:{segment_id}")
        return self._segment_from_row(row)

    def _segment_from_row(self, row: Any) -> _IntervalSegment:
        return _IntervalSegment(
            segment_id=int(row["segment_id"]),
            live_lo=int(row["live_lo"]),
            live_hi=int(row["live_hi"]),
            branch_point=int(row["branch_point"]),
        )

    def _insert_segment(
        self,
        segment: dict[str, Any],
        parent_segment_id: int | None,
        owner_branch_id: str | None,
        segment_kind: str,
        created_at: str,
    ) -> None:
        self.db.execute(
            """
            INSERT INTO _chronos_branch_interval_segments
            (segment_id, parent_segment_id, owner_branch_id, segment_kind,
             live_lo, live_hi, branch_point, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                segment["segment_id"],
                parent_segment_id,
                owner_branch_id,
                segment_kind,
                segment["live_lo"],
                segment["live_hi"],
                segment["branch_point"],
                created_at,
                "{}",
            ),
        )

    def _split_segment_with_fork_base(
        self, segment: _IntervalSegment, *, terminal: bool = False
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        lo = segment.live_lo
        hi = segment.live_hi
        # A fork base is a one-unit immutable interval [x, x+1). Allocate it
        # from the low end so repeated forks consume interval space forward:
        # fork_base, child, then source-branch continuation.
        # Mutable descendants still need width >= 2 so they have an interior
        # split point for future branching/checkpointing.
        if hi - lo < (_MIN_SPLIT_WIDTH * 2) + 1:
            raise BranchingError("interval space exhausted")
        fork_base_hi = lo + 1
        width = hi - fork_base_hi
        if terminal:
            # Terminal branches are private workspaces that can be written,
            # merged, and deleted, but never forked. Width 2 is the minimum
            # mutable segment, so flat branch-transaction/simulation workloads
            # maximize root fanout without affecting read predicates.
            child_width = _INTERVAL_TERMINAL_CHILD_WIDTH
        elif self.child_width is not None:
            # Compatibility path for existing benchmark harnesses. Public
            # branch creation should use terminal=True instead of numeric widths.
            child_width = self.child_width
        elif (
            self.allocation_strategy == "adaptive"
            and width > _INTERVAL_ADAPTIVE_SQRT_THRESHOLD
        ):
            # Adaptive allocation optimizes for shallow, wide trees near the
            # root without requiring users to choose a maximum depth. A child
            # gets sqrt(width), so the parent can create about sqrt(width)
            # siblings while each child receives comparable future capacity.
            child_width = math.isqrt(width)
        else:
            # Once a segment is below the sqrt threshold, switch back to the
            # existing depth-biased split. This gives long descendant chains
            # without asking users to provide a max-depth budget.
            continuation_width = (
                width * self.continuation_percent
            ) // _INTERVAL_PERCENT_DENOMINATOR
            child_width = width - continuation_width
        child_width = max(_MIN_SPLIT_WIDTH, min(width - _MIN_SPLIT_WIDTH, child_width))
        child_hi = fork_base_hi + child_width
        (
            continuation_segment_id,
            child_segment_id,
            fork_base_segment_id,
        ) = self._allocate_segment_ids(3)
        continuation = {
            "segment_id": continuation_segment_id,
            "live_lo": child_hi,
            "live_hi": hi,
            "branch_point": child_hi,
        }
        child = {
            "segment_id": child_segment_id,
            "live_lo": fork_base_hi,
            "live_hi": child_hi,
            "branch_point": fork_base_hi + (child_hi - fork_base_hi) // 2,
        }
        fork_base = {
            "segment_id": fork_base_segment_id,
            "live_lo": lo,
            "live_hi": fork_base_hi,
            "branch_point": lo,
        }
        return continuation, child, fork_base

    def _split_segment(
        self, segment: _IntervalSegment
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        # Biased splitting gives the new snapshot most of the numeric space.
        # Allocate the snapshot from the low end so interval consumption moves
        # forward consistently with branch/fork-base allocation. The source
        # branch keeps the high-end continuation segment, preserving O(1)
        # checkpoint creation and constant-size read predicates.
        lo = segment.live_lo
        hi = segment.live_hi
        if hi - lo < 4:
            raise BranchingError("interval space exhausted")
        width = hi - lo
        continuation_width = (
            width * self.continuation_percent
        ) // _INTERVAL_PERCENT_DENOMINATOR
        snapshot_width = width - continuation_width
        snapshot_width = max(
            _MIN_SPLIT_WIDTH, min(width - _MIN_SPLIT_WIDTH, snapshot_width)
        )
        split = lo + snapshot_width
        continuation_segment_id, snapshot_segment_id = self._allocate_segment_ids(2)
        continuation = {
            "segment_id": continuation_segment_id,
            "live_lo": split,
            "live_hi": hi,
            "branch_point": split + (hi - split) // 2,
        }
        snapshot = {
            "segment_id": snapshot_segment_id,
            "live_lo": lo,
            "live_hi": split,
            "branch_point": lo + (split - lo) // 2,
        }
        return continuation, snapshot

    def _merge_successor_segment(self, segment: _IntervalSegment) -> dict[str, Any]:
        # Staged merge rows must not contain the old branch point; otherwise
        # readers already holding the old target segment could observe them
        # before the metadata publish step. Prefer the high side and place the
        # successor point at its low edge so repeated publishes consume one
        # point at a time after the initial split.
        min_width = (_MIN_SPLIT_WIDTH * 2) + 1
        right_lo = segment.branch_point + 1
        if segment.live_hi - right_lo >= min_width:
            successor_id = self._allocate_segment_ids(1)[0]
            return {
                "segment_id": successor_id,
                "live_lo": right_lo,
                "live_hi": segment.live_hi,
                "branch_point": right_lo,
            }
        left_hi = segment.branch_point
        if left_hi - segment.live_lo >= min_width:
            successor_id = self._allocate_segment_ids(1)[0]
            return {
                "segment_id": successor_id,
                "live_lo": segment.live_lo,
                "live_hi": left_hi,
                "branch_point": left_hi - 1,
            }
        raise BranchingError("interval space exhausted")

    def _segment_for_ref(self, ref: _BranchRef) -> _IntervalSegment:
        return self._segment(ref.ref)

    def _current_segment(self, branch_id: str) -> _IntervalSegment:
        return self._segment(self._branch_segment_id(branch_id))

    def _prepared_segment(self, ref: _PreparedBranchRef) -> _IntervalSegment:
        segment = ref.metadata.get("segment")
        if isinstance(segment, _IntervalSegment):
            return segment
        return self._segment_for_ref(_BranchRef(ref.branch_id, ref.ref, ref.readonly))
