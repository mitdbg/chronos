from __future__ import annotations

from chronos_core.branching._common import *

_UPSERT_BATCH_CHUNK = 1000
_SQLITE_UPSERT_BATCH_CHUNK = 250
_UPSERT_BATCH_PARAM_BUDGET = 60_000


class _IntervalBackend(_SQLBranchBackend):
    """Visibility-interval backend.

    Each logical row version is stored once with a half-open numeric interval.
    Reading a branch becomes a constant-size predicate over the branch point:
    live_lo <= point < live_hi and deleted = 0. Writes maintain correctness by
    splitting any overlapping physical rows for the branch's current interval.
    """

    name = "interval"

    def __init__(
        self,
        db: SQLDatabaseAdapter,
        continuation_percent: int = _INTERVAL_CONTINUATION_PERCENT,
        enable_schema_branching: bool = False,
    ):
        super().__init__(db)
        self.continuation_percent = _validate_interval_continuation_percent(
            continuation_percent
        )
        self.enable_schema_branching = bool(enable_schema_branching)

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
              current_segment_id TEXT NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS _chronos_branch_interval_segments (
              segment_id TEXT PRIMARY KEY,
              parent_segment_id TEXT,
              owner_branch_id TEXT,
              live_lo {interval_type} NOT NULL,
              live_hi {interval_type} NOT NULL,
              branch_point {interval_type} NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL,
              CHECK (live_lo < branch_point),
              CHECK (branch_point < live_hi)
            )
            """
        )
        exists = self.db.execute(
            "SELECT 1 FROM _chronos_branch_interval_branches WHERE branch_id = 'main'"
        ).fetchone()
        if exists is None:
            segment = "seg_main"
            self.db.execute(
                """
                INSERT INTO _chronos_branch_interval_segments
                (segment_id, parent_segment_id, owner_branch_id, live_lo, live_hi,
                 branch_point, created_at, metadata)
                VALUES (?, NULL, ?, ?, ?, ?, ?, ?)
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
                (branch_id, current_segment_id, created_at, metadata)
                VALUES (?, ?, ?, ?)
                """,
                ("main", segment, _utc_now(), "{}"),
            )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_interval_checkpoints (
              checkpoint_id TEXT PRIMARY KEY,
              branch_id TEXT NOT NULL,
              segment_id TEXT NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )
        if self.enable_schema_branching:
            self._ensure_schema_branching_tables()
        self.db.commit()

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
              deleted INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY ({pk_sql}, live_lo),
              CHECK (live_lo < live_hi)
            )
            """
        )
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS {_quote(f'idx_{physical}_visible')} "
            f"ON {_quote(physical)} (live_lo, live_hi, deleted)"
        )
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS {_quote(f'idx_{physical}_pk_hi')} "
            f"ON {_quote(physical)} ({pk_sql}, live_hi)"
        )
        cols = ", ".join(_quote(c) for c in columns)
        self.db.execute(
            f"""
            INSERT INTO {_quote(physical)}
            ({cols}, live_lo, live_hi, deleted)
            SELECT {cols}, 0, ?, 0 FROM {_quote_table_name(table)}
            """,
            (self._max_interval(),),
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
        indexed_columns = [*index.columns, "live_lo", "live_hi", "deleted"]
        self.db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {_quote(self._physical_logical_index_name(meta, index))}
            ON {_quote(meta.physical_name)}
            ({", ".join(_quote(c) for c in indexed_columns)})
            """
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

    def create_branch(
        self, branch_id: str, from_branch: str, metadata: dict[str, Any] | None = None
    ) -> None:
        if self.db.dialect == "postgres":
            self._create_branch_postgres_locked(branch_id, from_branch, metadata)
            return
        if self._branch_row(branch_id) is not None:
            raise BranchAlreadyExistsError(branch_id)
        source = self._branch_row(from_branch)
        if source is None:
            raise BranchNotFoundError(from_branch)
        source_segment = self._segment(source["current_segment_id"])
        # Branching splits the source segment into two sibling intervals: a
        # continuation for the source branch and a child interval for the new
        # branch. Existing physical rows remain untouched until a write occurs.
        continuation, child = self._split_segment(source_segment)
        now = _utc_now()
        self.db.execute(
            """
            INSERT INTO _chronos_branch_interval_segments
            (segment_id, parent_segment_id, owner_branch_id, live_lo, live_hi,
             branch_point, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
            (
                continuation["segment_id"],
                source_segment.segment_id,
                from_branch,
                continuation["live_lo"],
                continuation["live_hi"],
                continuation["branch_point"],
                now,
                "{}",
            ),
        )
        self.db.execute(
            """
            INSERT INTO _chronos_branch_interval_segments
            (segment_id, parent_segment_id, owner_branch_id, live_lo, live_hi,
             branch_point, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                child["segment_id"],
                source_segment.segment_id,
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
            UPDATE _chronos_branch_interval_branches
            SET current_segment_id = ?
            WHERE branch_id = ?
            """,
            (continuation["segment_id"], from_branch),
        )
        self.db.execute(
            """
            INSERT INTO _chronos_branch_interval_branches
            (branch_id, current_segment_id, created_at, metadata)
            VALUES (?, ?, ?, ?)
            """,
            (branch_id, child["segment_id"], now, _json_dumps(metadata)),
        )

    def _create_branch_postgres_locked(
        self, branch_id: str, from_branch: str, metadata: dict[str, Any] | None = None
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
        source_segment = self._segment(source["current_segment_id"])
        continuation, child = self._split_segment(source_segment)
        now = _utc_now()
        raw = self.db.raw_connection
        if not hasattr(raw, "pipeline"):
            raise BranchingError("PostgreSQL adapter does not expose pipeline mode")
        with raw.pipeline():
            self.db.execute(
                """
                INSERT INTO _chronos_branch_interval_segments
                (segment_id, parent_segment_id, owner_branch_id, live_lo, live_hi,
                 branch_point, created_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    continuation["segment_id"],
                    source_segment.segment_id,
                    from_branch,
                    continuation["live_lo"],
                    continuation["live_hi"],
                    continuation["branch_point"],
                    now,
                    "{}",
                ),
            )
            self.db.execute(
                """
                INSERT INTO _chronos_branch_interval_segments
                (segment_id, parent_segment_id, owner_branch_id, live_lo, live_hi,
                 branch_point, created_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    child["segment_id"],
                    source_segment.segment_id,
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
                UPDATE _chronos_branch_interval_branches
                SET current_segment_id = ?
                WHERE branch_id = ?
                  AND current_segment_id = ?
                """,
                (continuation["segment_id"], from_branch, source_segment.segment_id),
            )
            self.db.execute(
                """
                INSERT INTO _chronos_branch_interval_branches
                (branch_id, current_segment_id, created_at, metadata)
                VALUES (?, ?, ?, ?)
                """,
                (branch_id, child["segment_id"], now, _json_dumps(metadata)),
            )

    def _create_branch_postgres_pipeline(self, branch_id: str, from_branch: str) -> None:
        """Create an interval branch using one metadata read plus pipelined DML."""

        row = self.db.execute(
            """
            SELECT
              EXISTS (
                SELECT 1
                FROM _chronos_branch_interval_branches
                WHERE branch_id = :branch_id
              ) AS duplicate_branch,
              b.current_segment_id AS source_segment_id,
              s.segment_id,
              s.live_lo,
              s.live_hi,
              s.branch_point
            FROM (SELECT 1) AS seed
            LEFT JOIN _chronos_branch_interval_branches b
              ON b.branch_id = :from_branch
            LEFT JOIN _chronos_branch_interval_segments s
              ON s.segment_id = b.current_segment_id
            """,
            {"branch_id": branch_id, "from_branch": from_branch},
        ).fetchone()
        if row is None:
            raise BranchingError("PostgreSQL interval branch metadata lookup returned no status")
        if row["duplicate_branch"]:
            raise BranchAlreadyExistsError(branch_id)
        if row["source_segment_id"] is None:
            raise BranchNotFoundError(from_branch)
        if row["segment_id"] is None:
            raise BranchNotFoundError(f"segment for branch:{from_branch}")

        source_segment = _IntervalSegment(
            segment_id=row["segment_id"],
            live_lo=int(row["live_lo"]),
            live_hi=int(row["live_hi"]),
            branch_point=int(row["branch_point"]),
        )
        continuation, child = self._split_segment(source_segment)
        now = _utc_now()
        raw = self.db.raw_connection
        if not hasattr(raw, "pipeline"):
            raise BranchingError("PostgreSQL adapter does not expose pipeline mode")
        with raw.pipeline():
            self.db.execute(
                """
                INSERT INTO _chronos_branch_interval_segments
                (segment_id, parent_segment_id, owner_branch_id, live_lo, live_hi,
                 branch_point, created_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    continuation["segment_id"],
                    source_segment.segment_id,
                    from_branch,
                    continuation["live_lo"],
                    continuation["live_hi"],
                    continuation["branch_point"],
                    now,
                    "{}",
                ),
            )
            self.db.execute(
                """
                INSERT INTO _chronos_branch_interval_segments
                (segment_id, parent_segment_id, owner_branch_id, live_lo, live_hi,
                 branch_point, created_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    child["segment_id"],
                    source_segment.segment_id,
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
                UPDATE _chronos_branch_interval_branches
                SET current_segment_id = ?
                WHERE branch_id = ?
                """,
                (continuation["segment_id"], from_branch),
            )
            self.db.execute(
                """
                INSERT INTO _chronos_branch_interval_branches
                (branch_id, current_segment_id, created_at, metadata)
                VALUES (?, ?, ?, ?)
                """,
                (branch_id, child["segment_id"], now, "{}"),
            )

    def _create_branch_postgres_cte(self, branch_id: str, from_branch: str) -> None:
        """Create an interval branch with one PostgreSQL round trip.

        The portable path above is intentionally simple but issues several
        metadata statements. PostgreSQL can perform the same conditional lookup,
        interval split, parent-head update, and child-branch insert as one
        data-modifying CTE inside the current transaction.
        """

        now = _utc_now()
        continuation_segment_id = f"seg_{uuid.uuid4().hex}"
        child_segment_id = f"seg_{uuid.uuid4().hex}"
        row = self.db.execute(
            """
            WITH duplicate_branch AS (
              SELECT 1
              FROM _chronos_branch_interval_branches
              WHERE branch_id = :branch_id
            ),
            source_branch AS (
              SELECT branch_id, current_segment_id
              FROM _chronos_branch_interval_branches
              WHERE branch_id = :from_branch
              FOR UPDATE
            ),
            source_segment AS (
              SELECT s.*
              FROM _chronos_branch_interval_segments s
              JOIN source_branch b
                ON b.current_segment_id = s.segment_id
            ),
            valid_source AS (
              SELECT
                segment_id,
                live_lo,
                live_hi,
                live_lo + GREATEST(
                  :min_split_width,
                  LEAST(
                    (live_hi - live_lo) - :min_split_width,
                    FLOOR(
                      ((live_hi - live_lo)::numeric * :continuation_percent)
                        / :percent_denominator
                    )
                  )
                ) AS split
              FROM source_segment
              WHERE NOT EXISTS (SELECT 1 FROM duplicate_branch)
                AND live_hi - live_lo >= :min_segment_width
            ),
            insert_continuation AS (
              INSERT INTO _chronos_branch_interval_segments
              (segment_id, parent_segment_id, owner_branch_id, live_lo, live_hi,
               branch_point, created_at, metadata)
              SELECT
                :continuation_segment_id,
                segment_id,
                :from_branch,
                live_lo,
                split,
                live_lo + FLOOR((split - live_lo) / 2),
                :created_at,
                '{}'
              FROM valid_source
              RETURNING segment_id
            ),
            insert_child AS (
              INSERT INTO _chronos_branch_interval_segments
              (segment_id, parent_segment_id, owner_branch_id, live_lo, live_hi,
               branch_point, created_at, metadata)
              SELECT
                :child_segment_id,
                segment_id,
                :branch_id,
                split,
                live_hi,
                split + FLOOR((live_hi - split) / 2),
                :created_at,
                '{}'
              FROM valid_source
              RETURNING segment_id
            ),
            update_parent AS (
              UPDATE _chronos_branch_interval_branches
              SET current_segment_id = :continuation_segment_id
              WHERE branch_id = :from_branch
                AND current_segment_id = (
                  SELECT segment_id FROM source_segment
                )
                AND EXISTS (SELECT 1 FROM valid_source)
              RETURNING branch_id
            ),
            insert_branch AS (
              INSERT INTO _chronos_branch_interval_branches
              (branch_id, current_segment_id, created_at, metadata)
              SELECT :branch_id, :child_segment_id, :created_at, '{}'
              WHERE EXISTS (SELECT 1 FROM update_parent)
              ON CONFLICT (branch_id) DO NOTHING
              RETURNING branch_id
            )
            SELECT
              (SELECT COUNT(*) FROM duplicate_branch) AS duplicate_count,
              (SELECT COUNT(*) FROM source_branch) AS source_count,
              (SELECT COUNT(*) FROM source_segment) AS source_segment_count,
              (SELECT COUNT(*) FROM valid_source) AS valid_source_count,
              (SELECT COUNT(*) FROM insert_continuation) AS continuation_count,
              (SELECT COUNT(*) FROM insert_child) AS child_count,
              (SELECT COUNT(*) FROM update_parent) AS parent_update_count,
              (SELECT COUNT(*) FROM insert_branch) AS branch_insert_count
            """,
            {
                "branch_id": branch_id,
                "from_branch": from_branch,
                "continuation_segment_id": continuation_segment_id,
                "child_segment_id": child_segment_id,
                "created_at": now,
                "continuation_percent": self.continuation_percent,
                "percent_denominator": _INTERVAL_PERCENT_DENOMINATOR,
                "min_split_width": _MIN_SPLIT_WIDTH,
                "min_segment_width": _MIN_SPLIT_WIDTH * 2,
            },
        ).fetchone()
        if row is None:
            raise BranchingError("PostgreSQL interval branch creation returned no status")
        if int(row["duplicate_count"]):
            raise BranchAlreadyExistsError(branch_id)
        if int(row["source_count"]) == 0:
            raise BranchNotFoundError(from_branch)
        if int(row["source_segment_count"]) == 0:
            raise BranchNotFoundError(f"segment for branch:{from_branch}")
        if int(row["valid_source_count"]) == 0:
            raise BranchingError("interval space exhausted")
        if int(row["branch_insert_count"]) == 0:
            raise BranchAlreadyExistsError(branch_id)
        if (
            int(row["continuation_count"]) != 1
            or int(row["child_count"]) != 1
            or int(row["parent_update_count"]) != 1
        ):
            raise BranchingError("PostgreSQL interval branch creation was incomplete")

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
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
        child = {
            "segment_id": f"seg_{uuid.uuid4().hex}",
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
            (branch_id, current_segment_id, created_at, metadata)
            VALUES (?, ?, ?, ?)
            """,
            (branch_id, child["segment_id"], now, "{}"),
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
        cur = self.db.execute(
            "DELETE FROM _chronos_branch_interval_branches WHERE branch_id = ?",
            (branch_id,),
        )
        if cur.rowcount == 0:
            raise BranchNotFoundError(branch_id)

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
                current_ref=row["current_segment_id"],
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
            current_ref=row["current_segment_id"],
            backend=self.name,
            created_at=row["created_at"],
            metadata=_json_loads(row["metadata"]),
        )

    def create_checkpoint(
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
        source = self._branch_row(branch)
        if source is None:
            raise BranchNotFoundError(branch)
        source_segment = self._segment(source["current_segment_id"])
        # A checkpoint is a read-only segment cut from the current branch. The
        # mutable branch keeps the continuation segment, so later branch writes
        # cannot alter the checkpoint view.
        continuation, snapshot = self._split_segment(source_segment)
        now = _utc_now()
        for segment, owner in ((continuation, branch), (snapshot, None)):
            self.db.execute(
                """
                INSERT INTO _chronos_branch_interval_segments
                (segment_id, parent_segment_id, owner_branch_id, live_lo, live_hi,
                 branch_point, created_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    segment["segment_id"],
                    source_segment.segment_id,
                    owner,
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
        return CheckpointInfo(checkpoint, branch, snapshot["segment_id"], now, metadata or {})

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
            cp["segment_id"],
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
                row["segment_id"],
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
        rewrite_cache = ref.metadata.setdefault("query_rewrite_cache", {})
        rewritten = rewrite_cache.get(sql)
        if rewritten is None:
            rewritten = _rewrite_tables(sql, replacements, self.db.dialect)
            rewrite_cache[sql] = rewritten
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
            self._insert_visible_batch(ref.branch_id, table, full_rows, segment=segment, meta=meta)
            return ExecuteResult(len(full_rows))
        if isinstance(plan, _UpdatePlan):
            table = plan.table
            current_rows = self._select_matching_rows(
                ref, table, plan.where_sql, params, direct_filter=plan.direct_filter
            )
            meta = self._meta_for_ref(ref, table)
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
            return ExecuteResult(count)
        if isinstance(plan, _DeletePlan):
            table = plan.table
            meta = self._meta_for_ref(ref, table)
            keys = self._select_matching_keys(
                ref, table, plan.where_sql, params, direct_filter=plan.direct_filter
            )
            count = 0
            for key in keys:
                self._splice_row(table, key, None, True, ref.branch_id, segment=segment, meta=meta)
                count += 1
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

    def upsert_row(self, branch_id: str, table: str, row: dict[str, Any]) -> None:
        self._insert_visible(branch_id, table, row, allow_replace=True)

    def upsert_rows(
        self, branch_id: str, table: str, rows: list[dict[str, Any]]
    ) -> None:
        if not rows:
            return
        segment = self._current_segment(branch_id)
        meta = self._meta_for_segment(segment, table)

        keyed: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in rows:
            keyed[self._key_tuple(meta, self._row_key(meta, row))] = row
        deduped = list(keyed.values())
        keys = [self._row_key(meta, row) for row in deduped]
        chunk_size = self._upsert_batch_chunk_size(meta)

        physical_keys: set[tuple[Any, ...]] = set()
        for start in range(0, len(keys), chunk_size):
            physical_keys |= self._physical_row_key_tuples_for_keys(
                table,
                keys[start : start + chunk_size],
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

    def _visible_subquery(self, meta: _TableMeta) -> str:
        cols = ", ".join(_quote(c) for c in meta.columns)
        return (
            f"SELECT {cols} FROM {_quote(meta.physical_name)} "
            "WHERE live_lo <= :_chronos_branch_point "
            "AND :_chronos_branch_point < live_hi "
            "AND deleted = 0"
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
            tables[row["table_name"]] = _TableMeta(
                name=row["table_name"],
                physical_name=row["physical_table"],
                pk_columns=tuple(json.loads(row["pk_columns"])),
                columns=tuple(json.loads(row["columns"])),
                column_defs=tuple(json.loads(row["column_defs"])),
                backend=self.name,
            )
        return tables

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
                table, old_meta, active, action, segment
            )
        if isinstance(action, exp.AlterColumn):
            return self._execute_alter_table_type_ddl(
                table, old_meta, active, action, segment
            )
        if isinstance(action, exp.Drop):
            return self._execute_alter_table_drop_column_ddl(
                table, old_meta, active, action, segment
            )
        raise UnsupportedSQLError(
            "only ALTER TABLE ADD COLUMN, DROP COLUMN, and ALTER COLUMN TYPE are supported"
        )

    def _execute_alter_table_add_column_ddl(
        self,
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
        physical = self._physical_schema_table_name(table)
        self._create_interval_physical_table(physical, defs, old_meta.pk_columns)
        new_meta = _TableMeta(
            name=table,
            physical_name=physical,
            pk_columns=old_meta.pk_columns,
            columns=columns,
            column_defs=defs,
            backend=self.name,
        )
        self._copy_logical_indexes_to_schema_version(new_meta)
        parent_schema_version_id = active["schema_version_id"]
        schema_version_id = self._record_schema_version(
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
        self._splice_table_binding(table, schema_version_id, False, segment)
        return ExecuteResult(0)

    def _execute_alter_table_drop_column_ddl(
        self,
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
        physical = self._physical_schema_table_name(table)
        self._create_interval_physical_table(physical, defs, old_meta.pk_columns)
        new_meta = _TableMeta(
            name=table,
            physical_name=physical,
            pk_columns=old_meta.pk_columns,
            columns=columns,
            column_defs=defs,
            backend=self.name,
        )
        self._copy_logical_indexes_to_schema_version(new_meta)
        parent_schema_version_id = active["schema_version_id"]
        schema_version_id = self._record_schema_version(
            table,
            physical,
            old_meta.pk_columns,
            columns,
            defs,
            "alter_table_drop_column",
            parent_schema_version_id,
        )
        self._copy_visible_rows_to_schema_version(old_meta, new_meta, segment)
        self._splice_table_binding(table, schema_version_id, False, segment)
        return ExecuteResult(0)

    def _execute_alter_table_type_ddl(
        self,
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
        physical = self._physical_schema_table_name(table)
        self._create_interval_physical_table(physical, defs, old_meta.pk_columns)
        new_meta = _TableMeta(
            name=table,
            physical_name=physical,
            pk_columns=old_meta.pk_columns,
            columns=old_meta.columns,
            column_defs=tuple(defs),
            backend=self.name,
        )
        self._copy_logical_indexes_to_schema_version(new_meta)
        parent_schema_version_id = active["schema_version_id"]
        schema_version_id = self._record_schema_version(
            table,
            physical,
            old_meta.pk_columns,
            old_meta.columns,
            tuple(defs),
            "alter_table_alter_column_type",
            parent_schema_version_id,
        )
        using = action.args.get("using")
        if isinstance(using, exp.Expression):
            select_sql = using.sql(dialect=self.db.dialect)
        else:
            select_sql = f"CAST({_quote(column)} AS {type_sql})"
        self._copy_visible_rows_to_schema_version(
            old_meta,
            new_meta,
            segment,
            select_sql_by_column={column: select_sql},
        )
        self._splice_table_binding(table, schema_version_id, False, segment)
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
              deleted INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY ({pk_sql}, live_lo),
              CHECK (live_lo < live_hi)
            )
            """
        )
        self.db.execute(
            f"CREATE INDEX {_quote(f'idx_{physical}_visible')} "
            f"ON {_quote(physical)} (live_lo, live_hi, deleted)"
        )
        self.db.execute(
            f"CREATE INDEX {_quote(f'idx_{physical}_pk_hi')} "
            f"ON {_quote(physical)} ({pk_sql}, live_hi)"
        )

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
        target_columns = [*new_meta.columns, "live_lo", "live_hi", "deleted"]
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
        select_exprs.extend(["?", "?", "0"])
        select_sql = ", ".join(select_exprs)
        self.db.execute(
            f"""
            INSERT INTO {_quote(new_meta.physical_name)}
            ({target_sql})
            SELECT {select_sql}
            FROM {_quote(old_meta.physical_name)}
            WHERE {_quote("live_lo")} <= ?
              AND ? < {_quote("live_hi")}
              AND deleted = 0
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
        u_lo = segment.live_lo
        u_hi = segment.live_hi
        rows = self.db.execute(
            """
            SELECT *
            FROM _chronos_branch_table_bindings
            WHERE backend = ?
              AND table_name = ?
              AND live_lo < ?
              AND ? < live_hi
            ORDER BY live_lo
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
        plan_cache = ref.metadata.setdefault("statement_plan_cache", {})
        plan = plan_cache.get(sql)
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
        plan_cache[sql] = plan
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
    ) -> None:
        if not rows:
            return
        meta = meta or self._meta_for_segment(segment, table)
        keyed_rows = [(self._row_key(meta, row), row) for row in rows]
        seen_keys: set[tuple[Any, ...]] = set()
        for key, _ in keyed_rows:
            key_tuple = self._key_tuple(meta, key)
            if key_tuple in seen_keys:
                raise DuplicateKeyError(f"duplicate key on branch {branch_id}: {key}")
            seen_keys.add(key_tuple)
        visible = self._visible_rows_for_keys(table, [key for key, _ in keyed_rows], segment, meta=meta)
        if visible:
            duplicate = self._row_key(meta, visible[0])
            raise DuplicateKeyError(f"duplicate key on branch {branch_id}: {duplicate}")

        physical_row_keys = self._physical_row_key_tuples_for_keys(
            table, [key for key, _ in keyed_rows], segment, meta=meta
        )
        direct_rows: list[dict[str, Any]] = []
        for key, row in keyed_rows:
            if self._key_tuple(meta, key) in physical_row_keys:
                self._splice_row(table, key, row, False, branch_id, segment=segment)
            else:
                direct_rows.append(row)
        self._insert_physical_rows(meta, direct_rows, segment.live_lo, segment.live_hi, False)

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
              AND deleted = 0
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
            "AND deleted = 0"
        )
        stripped = where.strip()
        if not stripped:
            return visible
        if not stripped.upper().startswith("WHERE "):
            raise UnsupportedSQLError("unsupported WHERE clause")
        return f"{visible} AND ({stripped[6:]})"

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
        predicate, values = self._keys_predicate(meta, keys)
        cols = ", ".join(_quote(c) for c in meta.columns)
        rows = self.db.execute(
            f"""
            SELECT {cols}
            FROM {_quote(meta.physical_name)}
            WHERE ({predicate})
              AND live_lo <= ?
              AND ? < live_hi
              AND deleted = 0
            """,
            [*values, segment.branch_point, segment.branch_point],
        ).fetchall()
        return [dict(row) for row in rows]

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
        predicate, values = self._keys_predicate(meta, keys)
        pk_cols = ", ".join(_quote(c) for c in meta.pk_columns)
        rows = self.db.execute(
            f"""
            SELECT DISTINCT {pk_cols}
            FROM {_quote(meta.physical_name)}
            WHERE ({predicate})
              AND live_lo < ?
              AND ? < live_hi
            """,
            [*values, segment.live_hi, segment.live_lo],
        ).fetchall()
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
            self._insert_physical_row(meta, replacement, u_lo, u_hi, deleted)
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
                    meta, old_row, a, overlap_lo, bool(physical_row["deleted"])
                )
            if row is None and deleted:
                replacement = dict(old_row)
            else:
                replacement = {column: row.get(column) for column in meta.columns}  # type: ignore[union-attr]
            self._insert_physical_row(meta, replacement, overlap_lo, overlap_hi, deleted)
            if overlap_hi < b:
                self._insert_physical_row(
                    meta, old_row, overlap_hi, b, bool(physical_row["deleted"])
                )

    def _insert_physical_row(
        self,
        meta: _TableMeta,
        row: dict[str, Any],
        live_lo: int,
        live_hi: int,
        deleted: bool,
    ) -> None:
        cols = [*meta.columns, "live_lo", "live_hi", "deleted"]
        values = [row.get(column) for column in meta.columns] + [
            live_lo,
            live_hi,
            1 if deleted else 0,
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
    ) -> None:
        if not rows:
            return
        cols = [*meta.columns, "live_lo", "live_hi", "deleted"]
        values = [
            [row.get(column) for column in meta.columns]
            + [live_lo, live_hi, 1 if deleted else 0]
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

    def _branch_segment_id(self, branch_id: str) -> str:
        row = self._branch_row(branch_id)
        if row is None:
            raise BranchNotFoundError(branch_id)
        return row["current_segment_id"]

    def _segment(self, segment_id: str) -> _IntervalSegment:
        row = self.db.execute(
            "SELECT * FROM _chronos_branch_interval_segments WHERE segment_id = ?",
            (segment_id,),
        ).fetchone()
        if row is None:
            raise BranchNotFoundError(f"segment:{segment_id}")
        return _IntervalSegment(
            segment_id=row["segment_id"],
            live_lo=int(row["live_lo"]),
            live_hi=int(row["live_hi"]),
            branch_point=int(row["branch_point"]),
        )

    def _split_segment(
        self, segment: _IntervalSegment
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        # Biased splitting gives the new child most of the numeric space. This
        # keeps deep spine/MCTS-style workloads alive much longer than midpoint
        # splitting while preserving O(1) branch creation and constant-size read
        # predicates. The source branch still keeps a small continuation segment
        # so later writes to the source remain isolated from the child.
        lo = segment.live_lo
        hi = segment.live_hi
        if hi - lo < 4:
            raise BranchingError("interval space exhausted")
        width = hi - lo
        left_width = (width * self.continuation_percent) // _INTERVAL_PERCENT_DENOMINATOR
        left_width = max(_MIN_SPLIT_WIDTH, min(width - _MIN_SPLIT_WIDTH, left_width))
        split = lo + left_width
        left = {
            "segment_id": f"seg_{uuid.uuid4().hex}",
            "live_lo": lo,
            "live_hi": split,
            "branch_point": lo + (split - lo) // 2,
        }
        right = {
            "segment_id": f"seg_{uuid.uuid4().hex}",
            "live_lo": split,
            "live_hi": hi,
            "branch_point": split + (hi - split) // 2,
        }
        return left, right

    def _segment_for_ref(self, ref: _BranchRef) -> _IntervalSegment:
        return self._segment(ref.ref)

    def _current_segment(self, branch_id: str) -> _IntervalSegment:
        return self._segment(self._branch_segment_id(branch_id))

    def _prepared_segment(self, ref: _PreparedBranchRef) -> _IntervalSegment:
        segment = ref.metadata.get("segment")
        if isinstance(segment, _IntervalSegment):
            return segment
        return self._segment_for_ref(_BranchRef(ref.branch_id, ref.ref, ref.readonly))
