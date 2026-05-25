from __future__ import annotations

from chronos_core.branching._common import *

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
    ):
        super().__init__(db)
        self.continuation_percent = _validate_interval_continuation_percent(
            continuation_percent
        )

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
        self.db.commit()

    def register_table(self, table: str, primary_key: list[str]) -> None:
        if table in self.tables:
            return
        interval_type = self._interval_sql_type()
        columns, defs = _table_defs(self.db, table)
        missing = set(primary_key) - set(columns)
        if missing:
            raise TableNotRegisteredError(f"primary key columns missing from {table}: {missing}")
        physical = f"_chronos_b_interval_{table}"
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
            f"CREATE INDEX {_quote(f'idx_{physical}_visible')} "
            f"ON {_quote(physical)} (live_lo, live_hi, deleted)"
        )
        self.db.execute(
            f"CREATE INDEX {_quote(f'idx_{physical}_pk_hi')} "
            f"ON {_quote(physical)} ({pk_sql}, live_hi)"
        )
        cols = ", ".join(_quote(c) for c in columns)
        self.db.execute(
            f"""
            INSERT INTO {_quote(physical)}
            ({cols}, live_lo, live_hi, deleted)
            SELECT {cols}, 0, ?, 0 FROM {_quote(table)}
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

    def create_index(
        self, table: str, columns: list[str], name: str | None = None
    ) -> IndexInfo:
        meta, index = self._validate_index(table, columns, name)
        if index.name not in self.indexes:
            indexed_columns = [*index.columns, "live_lo", "live_hi", "deleted"]
            self.db.execute(
                f"""
                CREATE INDEX {_quote(f'_chronos_idx_interval_{index.name}')}
                ON {_quote(meta.physical_name)}
                ({", ".join(_quote(c) for c in indexed_columns)})
                """
            )
            self._record_index(index)
        return IndexInfo(index.name, index.table, index.columns, index.backend)

    def create_branch(self, branch_id: str, from_branch: str) -> None:
        if self.db.dialect == "postgres":
            self._create_branch_postgres_locked(branch_id, from_branch)
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
            (branch_id, child["segment_id"], now, "{}"),
        )

    def _create_branch_postgres_locked(self, branch_id: str, from_branch: str) -> None:
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
                (branch_id, child["segment_id"], now, "{}"),
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

    def create_checkpoint(self, checkpoint: str, branch: str) -> CheckpointInfo:
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
            (checkpoint, branch, snapshot["segment_id"], now, "{}"),
        )
        return CheckpointInfo(checkpoint, branch, snapshot["segment_id"], now)

    def checkout_checkpoint(self, checkpoint: str) -> _BranchRef:
        cp = self.db.execute(
            "SELECT * FROM _chronos_branch_interval_checkpoints WHERE checkpoint_id = ?",
            (checkpoint,),
        ).fetchone()
        if cp is None:
            raise BranchNotFoundError(f"checkpoint:{checkpoint}")
        return _BranchRef(cp["branch_id"], cp["segment_id"], readonly=True)

    def prepare_ref(self, ref: _BranchRef) -> _PreparedBranchRef:
        # Cache the segment and table rewrites once per checkout. For repeated
        # queries this avoids metadata SELECTs before every statement.
        return _PreparedBranchRef(
            ref.branch_id,
            ref.ref,
            ref.readonly,
            {
                "segment": self._segment_for_ref(ref),
                "replacements": self._all_user_tables_replacements(
                    lambda meta: self._visible_subquery(meta)
                ),
                "query_rewrite_cache": {},
                "statement_plan_cache": {},
            },
        )

    def query(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        segment = self._prepared_segment(ref)
        replacements = ref.metadata.get("replacements")
        if replacements is None:
            replacements = self._all_user_tables_replacements(
                lambda meta: self._visible_subquery(meta)
            )
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
        plan = self._statement_plan(ref, sql)
        if isinstance(plan, _InsertPlan):
            table = plan.table
            rows = _insert_rows_from_plan(plan, params)
            meta = self._require_table(table)
            full_rows = [
                {column: row.get(column) for column in meta.columns}
                for row in rows
            ]
            self._insert_visible_batch(ref.branch_id, table, full_rows, segment=segment)
            return ExecuteResult(len(full_rows))
        if isinstance(plan, _UpdatePlan):
            table = plan.table
            current_rows = self._select_matching_rows(
                ref, table, plan.where_sql, params, direct_filter=plan.direct_filter
            )
            meta = self._require_table(table)
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
                )
                count += 1
            return ExecuteResult(count)
        if isinstance(plan, _DeletePlan):
            table = plan.table
            keys = self._select_matching_keys(
                ref, table, plan.where_sql, params, direct_filter=plan.direct_filter
            )
            count = 0
            for key in keys:
                self._splice_row(table, key, None, True, ref.branch_id, segment=segment)
                count += 1
            return ExecuteResult(count)
        raise UnsupportedSQLError("only SELECT, INSERT, UPDATE, and DELETE are supported")

    def visible_rows(self, branch_id: str, table: str) -> list[dict[str, Any]]:
        ref = self.prepare_ref(_BranchRef(branch_id, self._branch_segment_id(branch_id)))
        return self.query(ref, f"SELECT * FROM {_quote(table)}", {})

    def upsert_row(self, branch_id: str, table: str, row: dict[str, Any]) -> None:
        self._insert_visible(branch_id, table, row, allow_replace=True)

    def delete_key(self, branch_id: str, table: str, key: dict[str, Any]) -> None:
        self._splice_row(table, key, None, True, branch_id)

    def _visible_subquery(self, meta: _TableMeta) -> str:
        cols = ", ".join(_quote(c) for c in meta.columns)
        return (
            f"SELECT {cols} FROM {_quote(meta.physical_name)} "
            "WHERE live_lo <= :_chronos_branch_point "
            "AND :_chronos_branch_point < live_hi "
            "AND deleted = 0"
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
        meta = self._require_table(table)
        key = self._row_key(meta, row)
        visible = self._visible_row(branch_id, table, key, segment=segment)
        if visible is not None and not allow_replace:
            raise DuplicateKeyError(f"duplicate key on branch {branch_id}: {key}")
        self._splice_row(table, key, row, False, branch_id, segment=segment)

    def _insert_visible_batch(
        self,
        branch_id: str,
        table: str,
        rows: list[dict[str, Any]],
        segment: _IntervalSegment,
    ) -> None:
        if not rows:
            return
        meta = self._require_table(table)
        keyed_rows = [(self._row_key(meta, row), row) for row in rows]
        seen_keys: set[tuple[Any, ...]] = set()
        for key, _ in keyed_rows:
            key_tuple = self._key_tuple(meta, key)
            if key_tuple in seen_keys:
                raise DuplicateKeyError(f"duplicate key on branch {branch_id}: {key}")
            seen_keys.add(key_tuple)
        visible = self._visible_rows_for_keys(table, [key for key, _ in keyed_rows], segment)
        if visible:
            duplicate = self._row_key(meta, visible[0])
            raise DuplicateKeyError(f"duplicate key on branch {branch_id}: {duplicate}")

        physical_row_keys = self._physical_row_key_tuples_for_keys(
            table, [key for key, _ in keyed_rows], segment
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
    ) -> dict[str, Any] | None:
        meta = self._require_table(table)
        point = (segment or self._current_segment(branch_id)).branch_point
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
        meta = self._require_table(table)
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
            return super()._select_matching_keys(ref, table, where, params)
        meta = self._require_table(table)
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
        meta = self._require_table(table)
        select_cols = ", ".join(_quote(c) for c in meta.columns)
        return self.query(ref, f"SELECT {select_cols} FROM {_quote(table)}{where}", params)

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
    ) -> list[dict[str, Any]]:
        if not keys:
            return []
        meta = self._require_table(table)
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
    ) -> set[tuple[Any, ...]]:
        if not keys:
            return set()
        meta = self._require_table(table)
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
    ) -> None:
        meta = self._require_table(table)
        segment = segment or self._current_segment(branch_id)
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
