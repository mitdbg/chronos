from __future__ import annotations

import contextlib

from chronos_core.branching._common import *
from chronos_core.branching._orpheus_implementation import orpheus_dataset_tables


class _OrpheusBackend(_SQLBranchBackend):
    """Chronos adapter for the OrpheusDB implementation model.

    The upstream implementation stores each CVD as a datatable, an indextable,
    and a versiontable. Chronos defaults to the paper's preferred
    split-by-rlist variant: immutable records live in the datatable, while the
    indextable maps each committed ``vid`` to the explicit list of ``rid``
    values in that version.
    """

    name = "orpheus"

    def __init__(
        self,
        db: SQLDatabaseAdapter,
        enable_schema_branching: bool = False,
        enable_diff_merge_tracking: bool = False,
    ):
        super().__init__(db)
        self.enable_schema_branching = bool(enable_schema_branching)
        self.enable_diff_merge_tracking = bool(enable_diff_merge_tracking)
        self._base_version_rid_limit_cache: dict[tuple[str, str], int] = {}

    def ensure(self) -> None:
        if self.db.dialect != "postgres":
            raise BranchingError(
                "Orpheus backend requires PostgreSQL because it uses "
                "array-backed rlist storage"
            )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_orpheus_versiontable (
              vid INTEGER PRIMARY KEY,
              author TEXT,
              num_records INTEGER,
              parent INTEGER[],
              children INTEGER[],
              create_time TIMESTAMP,
              commit_time TIMESTAMP,
              commit_msg TEXT
            )
            """
        )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_orpheus_branches (
              branch_id TEXT PRIMARY KEY,
              current_vid INTEGER NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_orpheus_checkpoints (
              checkpoint_id TEXT PRIMARY KEY,
              branch_id TEXT NOT NULL,
              vid INTEGER NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_orpheus_workspace (
              branch_id TEXT NOT NULL,
              table_name TEXT NOT NULL,
              key_text TEXT NOT NULL,
              rid INTEGER,
              deleted BOOLEAN NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (branch_id, table_name, key_text)
            )
            """
        )
        self.db.execute(
            """
            CREATE INDEX IF NOT EXISTS _chronos_idx_orpheus_workspace_branch
            ON _chronos_branch_orpheus_workspace (branch_id, table_name)
            """
        )
        if self.enable_diff_merge_tracking:
            self._ensure_diff_merge_tracking_tables()
        if self.enable_schema_branching:
            self._ensure_schema_branching_tables()
        if self._version_row(1) is None:
            now = _utc_now()
            self.db.execute(
                """
                INSERT INTO _chronos_branch_orpheus_versiontable
                (vid, author, num_records, parent, children, create_time, commit_time, commit_msg)
                VALUES (1, 'chronos', 0, ?::integer[], ?::integer[], ?, ?, 'init commit')
                """,
                ([-1], [], now, now),
            )
        self.db.execute("CREATE SEQUENCE IF NOT EXISTS _chronos_branch_orpheus_vid_seq")
        self.db.execute(
            """
            SELECT setval(
              '_chronos_branch_orpheus_vid_seq',
              (SELECT GREATEST(COALESCE(MAX(vid), 1), 1)
               FROM _chronos_branch_orpheus_versiontable),
              true
            )
            """
        )
        if self._branch_row("main") is None:
            self.db.execute(
                """
                INSERT INTO _chronos_branch_orpheus_branches
                (branch_id, current_vid, created_at, metadata)
                VALUES ('main', 1, ?, '{}')
                """,
                (_utc_now(),),
            )
        self.db.commit()

    def _ensure_diff_merge_tracking_tables(self) -> None:
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_orpheus_version_delta (
              vid INTEGER NOT NULL,
              writer_branch_id TEXT NOT NULL,
              table_name TEXT NOT NULL,
              key_text TEXT NOT NULL,
              op TEXT NOT NULL,
              before_rid INTEGER,
              after_rid INTEGER,
              created_at TEXT NOT NULL,
              PRIMARY KEY (vid, table_name, key_text)
            )
            """
        )
        self.db.execute(
            """
            CREATE INDEX IF NOT EXISTS _chronos_idx_orpheus_delta_vid_table
            ON _chronos_branch_orpheus_version_delta (vid, table_name)
            """
        )
        self.db.execute(
            """
            CREATE INDEX IF NOT EXISTS _chronos_idx_orpheus_delta_writer_table
            ON _chronos_branch_orpheus_version_delta (writer_branch_id, table_name)
            """
        )
        self.db.execute(
            """
            CREATE INDEX IF NOT EXISTS _chronos_idx_orpheus_delta_table_key
            ON _chronos_branch_orpheus_version_delta (table_name, key_text, vid)
            """
        )

    def register_table(self, table: str, primary_key: list[str]) -> None:
        columns, defs = _table_defs(self.db, table)
        missing = set(primary_key) - set(columns)
        if missing:
            raise TableNotRegisteredError(
                f"primary key columns missing from {table}: {missing}"
            )
        if table in self.tables:
            if self.enable_schema_branching:
                self._record_table_binding("branch", "main", self.tables[table], False)
            meta = self.tables[table]
            existing = set(meta.columns)
            additions = [
                (column, definition)
                for column, definition in zip(columns, defs)
                if column not in existing
            ]
            if not additions:
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
                (json.dumps(columns), json.dumps(defs), self.name, table),
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
                self._record_table_binding("branch", "main", self.tables[table], False)
            return

        physical = orpheus_dataset_tables(
            f"_chronos_b_orpheus_{_physical_table_suffix(table)}"
        ).datatable
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_quote(physical)} (
              rid SERIAL PRIMARY KEY,
              {", ".join(defs)}
            )
            """
        )
        index_table = self._index_table_for_physical(physical)
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_quote(index_table)} (
              vid INTEGER PRIMARY KEY,
              rlist INTEGER[] NOT NULL
            )
            """
        )
        self.db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS
            {_quote(f'_chronos_idx_orpheus_{_physical_table_suffix(table)}_pk')}
            ON {_quote(physical)}
            ({", ".join(_quote(column) for column in primary_key)})
            """
        )
        self.db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS
            {_quote(f'_chronos_idx_orpheus_{_physical_table_suffix(table)}_rlist')}
            ON {_quote(index_table)} USING GIN (rlist)
            """
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
            self._record_table_binding("branch", "main", meta, False)
        self._copy_source_rows_to_version(meta, table, 1)
        self._refresh_version_record_count(1)

    def create_index(
        self, table: str, columns: list[str], name: str | None = None
    ) -> IndexInfo:
        meta, index = self._validate_index(table, columns, name)
        if index.name not in self.indexes:
            self._record_index(index)
            metas = (
                [
                    self._meta_from_binding(row)
                    for row in self.db.execute(
                        """
                        SELECT *
                        FROM _chronos_branch_orpheus_table_bindings
                        WHERE table_name = ? AND tombstone = 0
                        """,
                        (table,),
                    ).fetchall()
                ]
                if self.enable_schema_branching
                else [meta]
            )
            seen: set[str] = set()
            for active_meta in metas:
                if active_meta.physical_name in seen:
                    continue
                seen.add(active_meta.physical_name)
                self._create_physical_index(active_meta, index)
        return IndexInfo(index.name, index.table, index.columns, index.backend)

    def create_branch(
        self,
        branch_id: str,
        from_branch: str,
        metadata: dict[str, Any] | None = None,
        *,
        terminal: bool = False,
    ) -> None:
        if terminal:
            raise BranchingError("terminal branches are supported only by the interval backend")
        if self._branch_row(branch_id) is not None:
            raise BranchAlreadyExistsError(branch_id)
        source = self._branch_row(from_branch)
        if source is None:
            raise BranchNotFoundError(from_branch)
        source_vid = self._current_version_id(from_branch)
        if self.enable_schema_branching:
            self._copy_tombstone_bindings("branch", from_branch, "branch", branch_id)
            for meta in self._active_metas_for_owner("branch", from_branch).values():
                self._record_table_binding("branch", branch_id, meta, False)
        self.db.execute(
            """
            INSERT INTO _chronos_branch_orpheus_branches
            (branch_id, current_vid, created_at, metadata)
            VALUES (?, ?, ?, ?)
            """,
            (
                branch_id,
                source_vid,
                _utc_now(),
                _json_dumps(metadata),
            ),
        )
        self._copy_workspace(from_branch, branch_id)

    def update_branch_metadata(
        self, branch_id: str, metadata: dict[str, Any]
    ) -> BranchInfo:
        if self._branch_row(branch_id) is None:
            raise BranchNotFoundError(branch_id)
        self.db.execute(
            """
            UPDATE _chronos_branch_orpheus_branches
               SET metadata = ?
             WHERE branch_id = ?
            """,
            (_json_dumps(metadata), branch_id),
        )
        return self.get_branch(branch_id)

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
        if self._branch_row(branch_id) is not None:
            raise BranchAlreadyExistsError(branch_id)
        cp = self.get_checkpoint(checkpoint)
        if self.enable_schema_branching:
            self._copy_tombstone_bindings("checkpoint", checkpoint, "branch", branch_id)
            for meta in self._active_metas_for_owner("checkpoint", checkpoint).values():
                self._record_table_binding("branch", branch_id, meta, False)
        self.db.execute(
            """
            INSERT INTO _chronos_branch_orpheus_branches
            (branch_id, current_vid, created_at, metadata)
            VALUES (?, ?, ?, '{}')
            """,
            (branch_id, self._vid(cp.ref), _utc_now()),
        )

    def delete_branch(self, branch_id: str) -> None:
        if branch_id == "main":
            raise BranchingError("main cannot be deleted")
        if self._branch_row(branch_id) is None:
            raise BranchNotFoundError(branch_id)
        self.db.execute(
            "DELETE FROM _chronos_branch_orpheus_branches WHERE branch_id = ?",
            (branch_id,),
        )
        if self.enable_schema_branching:
            self.db.execute(
                """
                DELETE FROM _chronos_branch_orpheus_table_bindings
                WHERE owner_kind = 'branch' AND owner_id = ?
                """,
                (branch_id,),
            )

    def list_branches(self) -> list[BranchInfo]:
        rows = self.db.execute(
            """
            SELECT branch_id, current_vid, created_at, metadata
            FROM _chronos_branch_orpheus_branches
            ORDER BY branch_id
            """
        ).fetchall()
        return [
            BranchInfo(
                branch_id=row["branch_id"],
                current_ref=str(row["current_vid"]),
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
            current_ref=str(row["current_vid"]),
            backend=self.name,
            created_at=row["created_at"],
            metadata=_json_loads(row["metadata"]),
        )

    def create_checkpoint(
        self, checkpoint: str, branch: str, metadata: dict[str, Any] | None = None
    ) -> CheckpointInfo:
        if self._checkpoint_row(checkpoint) is not None:
            raise BranchAlreadyExistsError(checkpoint)
        branch_row = self._branch_row(branch)
        if branch_row is None:
            raise BranchNotFoundError(branch)
        checkpoint_vid = self._materialize_workspace(branch, "chronos checkpoint")
        now = _utc_now()
        if self.enable_schema_branching:
            self._copy_tombstone_bindings("branch", branch, "checkpoint", checkpoint)
            for meta in self._active_metas_for_owner("branch", branch).values():
                self._record_table_binding("checkpoint", checkpoint, meta, False)
        self.db.execute(
            """
            INSERT INTO _chronos_branch_orpheus_checkpoints
            (checkpoint_id, branch_id, vid, created_at, metadata)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                checkpoint,
                branch,
                checkpoint_vid,
                now,
                _json_dumps(metadata),
            ),
        )
        return CheckpointInfo(
            checkpoint,
            branch,
            str(checkpoint_vid),
            now,
            metadata or {},
        )

    def get_checkpoint(self, checkpoint: str) -> CheckpointInfo:
        row = self._checkpoint_row(checkpoint)
        if row is None:
            raise BranchNotFoundError(f"checkpoint:{checkpoint}")
        return CheckpointInfo(
            row["checkpoint_id"],
            row["branch_id"],
            str(row["vid"]),
            row["created_at"],
            _json_loads(row["metadata"]),
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
            SELECT checkpoint_id, branch_id, vid, created_at, metadata
            FROM _chronos_branch_orpheus_checkpoints
            {where}
            ORDER BY created_at, checkpoint_id
            """,
            tuple(params),
        ).fetchall()
        infos = [
            CheckpointInfo(
                row["checkpoint_id"],
                row["branch_id"],
                str(row["vid"]),
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
        return _PreparedBranchRef(
            ref.branch_id,
            ref.ref,
            ref.readonly,
            {
                "replacements": self._replacements(ref, include_rid=False),
                "tables": self._active_metas_for_ref(ref) if self.enable_schema_branching else self.tables,
                "query_rewrite_cache": {},
                "statement_plan_cache": {},
            },
        )

    def refresh_ref_after_execute(self, ref: _PreparedBranchRef) -> _PreparedBranchRef:
        if self.enable_schema_branching:
            return self.prepare_ref(_BranchRef(ref.branch_id, self._current_version_id(ref.branch_id), ref.readonly))
        return ref

    def prepare_transaction(self, ref: _PreparedBranchRef) -> None:
        ref.metadata["defer_orpheus_materialize"] = True
        return None

    def commit_transaction(self, ref: _PreparedBranchRef) -> _PreparedBranchRef:
        ref.metadata.pop("defer_orpheus_materialize", None)
        return ref

    def rollback_transaction(self, ref: _PreparedBranchRef) -> None:
        ref.metadata.pop("defer_orpheus_materialize", None)
        return None

    def query(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        ref = self._fresh_ref(ref)
        if self.enable_schema_branching:
            self._validate_query_tables_visible(ref, sql)
        rewrite_cache = ref.metadata.setdefault("query_rewrite_cache", {})
        cache_key = (ref.ref, sql)
        rewritten = rewrite_cache.get(cache_key)
        if rewritten is None:
            rewritten = _rewrite_tables(sql, self._prepared_replacements(ref), self.db.dialect)
            rewrite_cache[cache_key] = rewritten
        bound = dict(params)
        bound["_chronos_version_id"] = ref.ref
        rows = self.db.execute(rewritten, bound).fetchall()
        return [dict(row) for row in rows]

    def execute(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> ExecuteResult:
        if ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
        ref = self._fresh_ref(ref)
        if self._is_schema_statement(sql):
            tree = sqlglot.parse_one(sql, read=self.db.dialect)
            if not self.enable_schema_branching:
                raise UnsupportedSQLError("branch-local schema changes are disabled")
            if not isinstance(tree, (exp.Create, exp.Alter, exp.Drop)):
                raise UnsupportedSQLError("unsupported schema statement")
            return self._execute_schema_ddl(ref, tree)
        if self.enable_schema_branching:
            self._validate_query_tables_visible(ref, sql)
        plan = self._statement_plan(ref, sql)
        if isinstance(plan, _InsertPlan):
            rows = _insert_rows_from_plan(plan, params)
            full_rows = self._full_rows(ref, plan.table, rows)
            self._apply_workspace_delta(ref.branch_id, plan.table, [], full_rows)
            return ExecuteResult(len(full_rows))
        if isinstance(plan, _UpdatePlan):
            if not (set(column for column, _expr in plan.assignments) & set(self._require_table_for_ref(ref, plan.table).pk_columns)):
                rowcount = self._apply_update_plan_bulk(ref, plan, params)
                return ExecuteResult(rowcount)
            current_rows = self._select_matching_rows(
                ref, plan.table, plan.where_sql, params, plan.direct_filter
            )
            new_rows: list[dict[str, Any]] = []
            meta = self._require_table_for_ref(ref, plan.table)
            for current in current_rows:
                old_row = {column: current[column] for column in meta.columns}
                new_row = dict(old_row)
                new_row.update(_update_assignments_from_plan(plan, params, old_row))
                new_rows.append(new_row)
            self._apply_workspace_delta(ref.branch_id, plan.table, current_rows, new_rows)
            return ExecuteResult(len(current_rows))
        if isinstance(plan, _DeletePlan):
            current_rows = self._select_matching_rows(
                ref, plan.table, plan.where_sql, params, plan.direct_filter
            )
            self._apply_workspace_delta(ref.branch_id, plan.table, current_rows, [])
            return ExecuteResult(len(current_rows))
        raise UnsupportedSQLError("only SELECT, INSERT, UPDATE, and DELETE are supported")

    def _apply_update_plan_bulk(
        self,
        ref: _PreparedBranchRef,
        plan: _UpdatePlan,
        params: dict[str, Any],
    ) -> int:
        meta = self._require_table_for_ref(ref, plan.table)
        assignment_sql = {
            column: expr.sql(dialect=self.db.dialect)
            for column, expr in plan.assignments
        }
        select_cols = ", ".join(
            (
                f"{assignment_sql[column]} AS {_quote(column)}"
                if column in assignment_sql
                else _quote(column)
            )
            for column in meta.columns
        )
        visible = self._branch_visible_subquery(
            meta,
            ref.branch_id,
            self._current_version_id(ref.branch_id),
            include_rid=False,
        )
        source = f"SELECT * FROM ({visible}) AS visible_rows"
        stripped = plan.where_sql.strip()
        if not stripped and not ref.metadata.get("defer_orpheus_materialize"):
            return self._apply_full_update_plan_materialized(
                ref,
                plan.table,
                meta,
                source,
                select_cols,
                params,
            )
        if stripped:
            if not stripped.upper().startswith("WHERE "):
                raise UnsupportedSQLError("unsupported WHERE clause")
            source = f"{source} WHERE {stripped[6:]}"
        cols = ", ".join(_quote(column) for column in meta.columns)
        pk_returning = ", ".join(_quote(column) for column in meta.pk_columns)
        row = self.db.execute(
            f"""
            WITH source_rows AS (
              {source}
            ),
            inserted AS (
              INSERT INTO {_quote(meta.physical_name)}
              ({cols})
              SELECT {select_cols}
              FROM source_rows
              RETURNING rid, {pk_returning}
            ),
            upserted AS (
              INSERT INTO _chronos_branch_orpheus_workspace
              (branch_id, table_name, key_text, rid, deleted, updated_at)
              SELECT :__chronos_branch_id, :__chronos_table_name,
                     {self._key_json_sql(meta, alias='inserted')},
                     rid, FALSE, :__chronos_updated_at
              FROM inserted
              ON CONFLICT (branch_id, table_name, key_text)
              DO UPDATE SET
                rid = EXCLUDED.rid,
                deleted = FALSE,
                updated_at = EXCLUDED.updated_at
              RETURNING 1
            )
            SELECT COUNT(*) AS count FROM upserted
            """,
            {
                **params,
                "__chronos_branch_id": ref.branch_id,
                "__chronos_table_name": meta.name,
                "__chronos_updated_at": _utc_now(),
            },
        ).fetchone()
        count = int(row["count"]) if row is not None else 0
        if count > 1000 and not ref.metadata.get("defer_orpheus_materialize"):
            # Full-table macrobench backfills can touch every visible row. If
            # those replacements remain in the workspace, every subsequent
            # read pays an anti-join against one marker per row. Materializing
            # keeps Orpheus reads on compact committed rlists.
            self._materialize_workspace(ref.branch_id, "chronos bulk update")
        return count

    def _apply_full_update_plan_materialized(
        self,
        ref: _PreparedBranchRef,
        table: str,
        meta: _TableMeta,
        source: str,
        select_cols: str,
        params: dict[str, Any],
    ) -> int:
        with self._materialization_lock():
            parent_version = self._vid(self._current_version_id(ref.branch_id))
            new_version = self._new_version_id()
            now = _utc_now()
            # Chronos only follows the parent pointer when resolving Orpheus
            # versions. Maintaining the reverse children array would update a
            # shared parent row from every child materialization and serialize
            # wide branching workloads on that row.
            self.db.execute(
                """
                INSERT INTO _chronos_branch_orpheus_versiontable
                (vid, author, num_records, parent, children, create_time, commit_time, commit_msg)
                VALUES (?, 'chronos', 0, ?::integer[], ?::integer[], ?, ?, ?)
                """,
                (
                    new_version,
                    [parent_version],
                    [],
                    now,
                    now,
                    "chronos full update",
                ),
            )
            cols = ", ".join(_quote(column) for column in meta.columns)
            row = self.db.execute(
                f"""
                WITH source_rows AS (
                  {source}
                ),
                inserted AS (
                  INSERT INTO {_quote(meta.physical_name)}
                  ({cols})
                  SELECT {select_cols}
                  FROM source_rows
                  RETURNING rid
                )
                INSERT INTO {_quote(self._index_table(meta))}
                (vid, rlist)
                SELECT :__chronos_new_version,
                       COALESCE(array_agg(rid ORDER BY rid), ARRAY[]::integer[])
                FROM inserted
                RETURNING cardinality(rlist) AS count
                """,
                {
                    **params,
                    "__chronos_new_version": new_version,
                },
            ).fetchone()
            for other_meta in self._active_metas_for_owner("branch", ref.branch_id).values():
                if other_meta.name == table:
                    continue
                if self._workspace_has_table_changes(ref.branch_id, other_meta.name):
                    self._insert_materialized_rlist(
                        other_meta,
                        ref.branch_id,
                        parent_version,
                        new_version,
                    )
                    self._record_version_deltas_from_workspace(
                        other_meta,
                        ref.branch_id,
                        parent_version,
                        new_version,
                    )
                else:
                    self._carry_forward_rlist(other_meta, parent_version, new_version)
            self._record_version_deltas_by_comparing_versions(
                meta,
                ref.branch_id,
                parent_version,
                new_version,
            )
            self.db.execute(
                """
                UPDATE _chronos_branch_orpheus_branches
                   SET current_vid = ?
                 WHERE branch_id = ?
                """,
                (new_version, ref.branch_id),
            )
            self._refresh_version_record_count(new_version)
            self.db.execute(
                "DELETE FROM _chronos_branch_orpheus_workspace WHERE branch_id = ?",
                (ref.branch_id,),
            )
            return int(row["count"]) if row is not None else 0

    def _workspace_has_table_changes(self, branch_id: str, table: str) -> bool:
        row = self.db.execute(
            """
            SELECT 1
            FROM _chronos_branch_orpheus_workspace
            WHERE branch_id = ? AND table_name = ?
            LIMIT 1
            """,
            (branch_id, table),
        ).fetchone()
        return row is not None

    @contextlib.contextmanager
    def _materialization_lock(self) -> Iterator[None]:
        yield

    def _carry_forward_rlist(
        self,
        meta: _TableMeta,
        parent_version: int,
        new_version: int,
    ) -> None:
        self.db.execute(
            f"""
            INSERT INTO {_quote(self._index_table(meta))}
            (vid, rlist)
            SELECT ?, COALESCE(rlist, ARRAY[]::integer[])
            FROM {_quote(self._index_table(meta))}
            WHERE vid = ?
            ON CONFLICT (vid)
            DO UPDATE SET rlist = EXCLUDED.rlist
            """,
            (new_version, parent_version),
        )

    def visible_rows(self, branch_id: str, table: str) -> list[dict[str, Any]]:
        ref = self.prepare_ref(_BranchRef(branch_id, self._current_version_id(branch_id)))
        return self.query(ref, f"SELECT * FROM {_quote_table_name(table)}", {})

    def diff_rows(self, left: str, right: str, table: str) -> list[RowDiff] | None:
        if not self.enable_diff_merge_tracking:
            return None
        meta = self._compatible_meta_for_table(table, left, right)
        if meta is None:
            return None
        left_vid = self._vid(self._current_version_id(left))
        right_vid = self._vid(self._current_version_id(right))
        base_vid = self._nearest_common_version(left_vid, right_vid)
        if base_vid is None:
            return None
        candidate_keys = self._candidate_key_texts_between(
            table,
            base_vid,
            left_vid,
            right_vid,
            left,
            right,
        )
        if not candidate_keys:
            return []
        left_rows = self._rows_by_key_text_for_branch(meta, left, left_vid, candidate_keys)
        right_rows = self._rows_by_key_text_for_branch(meta, right, right_vid, candidate_keys)
        return self._classify_key_text_diffs(table, meta, left_rows, right_rows)

    def merge_preview(self, source: str, target: str) -> MergePreview | None:
        if not self.enable_diff_merge_tracking:
            return None
        preview = self._merge_preview_from_tracking(source, target)
        return MergePreview(
            source=source,
            target=target,
            changes=preview[0],
            conflicts=preview[1],
        )

    def merge_apply(
        self,
        source: str,
        target: str,
        resolution: MergeResolution | None = None,
    ) -> MergeResult | None:
        if not self.enable_diff_merge_tracking:
            return None
        with self._materialization_lock():
            self._lock_branch_for_update(source)
            self._lock_branch_for_update(target)
            source_vid = self._materialize_workspace(source, "chronos merge source")
            target_vid = self._materialize_workspace(target, "chronos merge target")
            changes, conflicts = self._merge_preview_from_tracking(source, target)
            if conflicts:
                raise BranchingError("merge has unresolved conflicts")
            if not changes:
                return MergeResult(source=source, target=target, applied=0)

            new_version = self._new_version_id()
            now = _utc_now()
            self.db.execute(
                """
                INSERT INTO _chronos_branch_orpheus_versiontable
                (vid, author, num_records, parent, children, create_time, commit_time, commit_msg)
                VALUES (?, 'chronos', 0, ?::integer[], ?::integer[], ?, ?, ?)
                """,
                (
                    new_version,
                    [target_vid, source_vid],
                    [],
                    now,
                    now,
                    f"chronos merge {source} into {target}",
                ),
            )

            changes_by_table: dict[str, list[RowDiff]] = {}
            for change in changes:
                changes_by_table.setdefault(change.table, []).append(change)
            table_metas = (
                self._active_metas_for_owner("branch", target).values()
                if self.enable_schema_branching
                else self.tables.values()
            )
            for meta in table_metas:
                table_changes = changes_by_table.get(meta.name, [])
                if not table_changes:
                    self._carry_forward_rlist(meta, target_vid, new_version)
                    continue
                key_texts = [
                    self._key_text_from_key(meta, change.key)
                    for change in table_changes
                ]
                self._insert_merged_rlist(
                    meta,
                    target_vid,
                    source_vid,
                    key_texts,
                    new_version,
                )
                self._record_merge_version_deltas(
                    meta,
                    target,
                    target_vid,
                    source_vid,
                    new_version,
                    table_changes,
                )
            self.db.execute(
                """
                UPDATE _chronos_branch_orpheus_branches
                   SET current_vid = ?
                 WHERE branch_id = ?
                """,
                (new_version, target),
            )
            self._refresh_version_record_count(new_version)
            return MergeResult(source=source, target=target, applied=len(changes))

    def _lock_branch_for_update(self, branch_id: str) -> None:
        row = self.db.execute(
            """
            SELECT 1
            FROM _chronos_branch_orpheus_branches
            WHERE branch_id = ?
            FOR UPDATE
            """,
            (branch_id,),
        ).fetchone()
        if row is None:
            raise BranchNotFoundError(branch_id)

    def _insert_merged_rlist(
        self,
        meta: _TableMeta,
        target_version: int,
        source_version: int,
        key_texts: list[str],
        new_version: int,
    ) -> None:
        if not key_texts:
            self._carry_forward_rlist(meta, target_version, new_version)
            return
        values = ", ".join("(?)" for _ in key_texts)
        self.db.execute(
            f"""
            INSERT INTO {_quote(self._index_table(meta))}
            (vid, rlist)
            WITH keys(key_text) AS (VALUES {values}),
            target_rows AS (
              SELECT d.rid
              FROM {_quote(meta.physical_name)} AS d
              JOIN {_quote(self._index_table(meta))} AS i
                ON i.vid = ?
               AND d.rid = ANY(i.rlist)
              WHERE NOT EXISTS (
                SELECT 1
                FROM keys
                WHERE keys.key_text = {self._key_json_sql(meta, alias='d')}
              )
            ),
            source_rows AS (
              SELECT d.rid
              FROM keys
              JOIN {_quote(meta.physical_name)} AS d
                ON {self._key_json_sql(meta, alias='d')} = keys.key_text
              JOIN {_quote(self._index_table(meta))} AS i
                ON i.vid = ?
               AND d.rid = ANY(i.rlist)
            ),
            materialized AS (
              SELECT rid FROM target_rows
              UNION ALL
              SELECT rid FROM source_rows
            )
            SELECT ?, COALESCE(array_agg(rid ORDER BY rid), ARRAY[]::integer[])
            FROM materialized
            ON CONFLICT (vid)
            DO UPDATE SET rlist = EXCLUDED.rlist
            """,
            [*key_texts, target_version, source_version, new_version],
        )

    def _record_merge_version_deltas(
        self,
        meta: _TableMeta,
        target_branch: str,
        target_version: int,
        source_version: int,
        new_version: int,
        changes: list[RowDiff],
    ) -> None:
        if not self.enable_diff_merge_tracking or not changes:
            return
        key_texts = [self._key_text_from_key(meta, change.key) for change in changes]
        target_rows = self._rows_by_key_text_for_version(
            meta, target_version, key_texts, include_rid=True
        )
        source_rows = self._rows_by_key_text_for_version(
            meta, source_version, key_texts, include_rid=True
        )
        now = _utc_now()
        rows = []
        for change in changes:
            key_text = self._key_text_from_key(meta, change.key)
            before = target_rows.get(key_text)
            after = source_rows.get(key_text)
            rows.append(
                (
                    new_version,
                    target_branch,
                    meta.name,
                    key_text,
                    change.change,
                    before.get("rid") if before is not None else None,
                    after.get("rid") if after is not None else None,
                    now,
                )
            )
        self.db.executemany(
            """
            INSERT INTO _chronos_branch_orpheus_version_delta
            (vid, writer_branch_id, table_name, key_text, op, before_rid, after_rid, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (vid, table_name, key_text)
            DO UPDATE SET
              writer_branch_id = EXCLUDED.writer_branch_id,
              op = EXCLUDED.op,
              before_rid = EXCLUDED.before_rid,
              after_rid = EXCLUDED.after_rid,
              created_at = EXCLUDED.created_at
            """,
            rows,
        )

    def _merge_preview_from_tracking(
        self, source: str, target: str
    ) -> tuple[list[RowDiff], list[RowDiff]]:
        source_vid = self._vid(self._current_version_id(source))
        target_vid = self._vid(self._current_version_id(target))
        base_vid = self._nearest_common_version(source_vid, target_vid)
        if base_vid is None:
            raise BranchingError("cannot merge Orpheus branches without a common version")

        changes: list[RowDiff] = []
        conflicts: list[RowDiff] = []
        for table in self.diff_tables():
            meta = self._compatible_meta_for_table(table, source, target, base_vid)
            if meta is None:
                raise UnsupportedSQLError(
                    "Orpheus merge with schema divergence is not supported"
                )
            candidate_keys = self._candidate_key_texts_between(
                table,
                base_vid,
                source_vid,
                target_vid,
                source,
                target,
            )
            if not candidate_keys:
                continue
            base_rows = self._rows_by_key_text_for_version(meta, base_vid, candidate_keys)
            source_rows = self._rows_by_key_text_for_branch(
                meta, source, source_vid, candidate_keys
            )
            target_rows = self._rows_by_key_text_for_branch(
                meta, target, target_vid, candidate_keys
            )
            for key_text in sorted(candidate_keys):
                base_row = base_rows.get(key_text)
                source_row = source_rows.get(key_text)
                target_row = target_rows.get(key_text)
                if source_row == target_row:
                    continue
                if source_row == base_row:
                    continue
                diff = self._row_diff_from_target_to_source_by_key_text(
                    table,
                    meta,
                    key_text,
                    target_row,
                    source_row,
                )
                if diff is None:
                    continue
                if target_row == base_row:
                    changes.append(diff)
                else:
                    conflicts.append(diff)
        return changes, conflicts

    def _compatible_meta_for_table(
        self,
        table: str,
        left_branch: str,
        right_branch: str,
        base_vid: int | None = None,
    ) -> _TableMeta | None:
        left_meta = self._meta_for_owner("branch", left_branch, table)
        right_meta = self._meta_for_owner("branch", right_branch, table)
        base_meta = (
            self._active_meta_for_version(base_vid, table)
            if base_vid is not None and table in self._active_metas_for_version(base_vid)
            else left_meta or right_meta
        )
        if left_meta is None and right_meta is None:
            return None
        if left_meta is None or right_meta is None or base_meta is None:
            return None
        if (
            left_meta.physical_name != right_meta.physical_name
            or left_meta.physical_name != base_meta.physical_name
            or left_meta.pk_columns != right_meta.pk_columns
            or left_meta.pk_columns != base_meta.pk_columns
            or left_meta.columns != right_meta.columns
            or left_meta.columns != base_meta.columns
        ):
            return None
        return left_meta

    def _nearest_common_version(self, left_vid: int, right_vid: int) -> int | None:
        row = self.db.execute(
            """
            WITH RECURSIVE
            left_path(vid, depth) AS (
              VALUES (?::integer, 0::integer)
              UNION
              SELECT p.parent_vid, left_path.depth + 1
              FROM left_path
              JOIN _chronos_branch_orpheus_versiontable AS v ON v.vid = left_path.vid
              CROSS JOIN LATERAL unnest(v.parent) AS p(parent_vid)
              WHERE p.parent_vid > 0
            ),
            right_path(vid, depth) AS (
              VALUES (?::integer, 0::integer)
              UNION
              SELECT p.parent_vid, right_path.depth + 1
              FROM right_path
              JOIN _chronos_branch_orpheus_versiontable AS v ON v.vid = right_path.vid
              CROSS JOIN LATERAL unnest(v.parent) AS p(parent_vid)
              WHERE p.parent_vid > 0
            )
            SELECT left_path.vid
            FROM left_path
            JOIN right_path USING (vid)
            ORDER BY left_path.depth + right_path.depth, left_path.depth, right_path.depth
            LIMIT 1
            """,
            (left_vid, right_vid),
        ).fetchone()
        return int(row["vid"]) if row is not None else None

    def _version_ids_after_base(self, head_vid: int, base_vid: int) -> list[int]:
        rows = self.db.execute(
            """
            WITH RECURSIVE path(vid) AS (
              VALUES (?::integer)
              UNION
              SELECT p.parent_vid
              FROM path
              JOIN _chronos_branch_orpheus_versiontable AS v ON v.vid = path.vid
              CROSS JOIN LATERAL unnest(v.parent) AS p(parent_vid)
              WHERE p.parent_vid > 0
                AND path.vid <> ?
            )
            SELECT vid
            FROM path
            WHERE vid <> ?
            """,
            (head_vid, base_vid, base_vid),
        ).fetchall()
        return [int(row["vid"]) for row in rows]

    def _candidate_key_texts_between(
        self,
        table: str,
        base_vid: int,
        left_vid: int,
        right_vid: int,
        left_branch: str,
        right_branch: str,
    ) -> list[str]:
        version_ids = sorted(
            set(self._version_ids_after_base(left_vid, base_vid))
            | set(self._version_ids_after_base(right_vid, base_vid))
        )
        keys: set[str] = set()
        if version_ids:
            placeholders = ", ".join("?" for _ in version_ids)
            rows = self.db.execute(
                f"""
                SELECT DISTINCT key_text
                FROM _chronos_branch_orpheus_version_delta
                WHERE table_name = ?
                  AND vid IN ({placeholders})
                """,
                [table, *version_ids],
            ).fetchall()
            keys.update(str(row["key_text"]) for row in rows)
        keys.update(self._workspace_key_texts(left_branch, table))
        keys.update(self._workspace_key_texts(right_branch, table))
        return sorted(keys)

    def _workspace_key_texts(self, branch_id: str, table: str) -> list[str]:
        rows = self.db.execute(
            """
            SELECT key_text
            FROM _chronos_branch_orpheus_workspace
            WHERE branch_id = ? AND table_name = ?
            """,
            (branch_id, table),
        ).fetchall()
        return [str(row["key_text"]) for row in rows]

    def _rows_by_key_text_for_version(
        self,
        meta: _TableMeta,
        version_id: int,
        key_texts: list[str],
        *,
        include_rid: bool = False,
    ) -> dict[str, dict[str, Any]]:
        if not key_texts:
            return {}
        values = ", ".join("(?)" for _ in key_texts)
        rid_col = "d.rid, " if include_rid else ""
        cols = ", ".join(f"d.{_quote(column)}" for column in meta.columns)
        rows = self.db.execute(
            f"""
            WITH keys(key_text) AS (VALUES {values})
            SELECT keys.key_text, {rid_col}{cols}
            FROM keys
            JOIN {_quote(meta.physical_name)} AS d
              ON {self._key_json_sql(meta, alias='d')} = keys.key_text
            JOIN {_quote(self._index_table(meta))} AS i
              ON i.vid = ?
             AND d.rid = ANY(i.rlist)
            """,
            [*key_texts, version_id],
        ).fetchall()
        return {str(row["key_text"]): self._row_from_result(row, meta, include_rid) for row in rows}

    def _rows_by_key_text_for_branch(
        self,
        meta: _TableMeta,
        branch_id: str,
        version_id: int,
        key_texts: list[str],
        *,
        include_rid: bool = False,
    ) -> dict[str, dict[str, Any]]:
        if not key_texts:
            return {}
        values = ", ".join("(?)" for _ in key_texts)
        rid_col = "d.rid, " if include_rid else ""
        cols = ", ".join(f"d.{_quote(column)}" for column in meta.columns)
        rows = self.db.execute(
            f"""
            WITH keys(key_text) AS (VALUES {values}),
            committed_rows AS (
              SELECT keys.key_text, {rid_col}{cols}
              FROM keys
              JOIN {_quote(meta.physical_name)} AS d
                ON {self._key_json_sql(meta, alias='d')} = keys.key_text
              JOIN {_quote(self._index_table(meta))} AS i
                ON i.vid = ?
               AND d.rid = ANY(i.rlist)
              WHERE NOT EXISTS (
                SELECT 1
                FROM _chronos_branch_orpheus_workspace AS w
                WHERE w.branch_id = ?
                  AND w.table_name = ?
                  AND w.key_text = keys.key_text
              )
            ),
            workspace_rows AS (
              SELECT keys.key_text, {rid_col}{cols}
              FROM keys
              JOIN _chronos_branch_orpheus_workspace AS w
                ON w.branch_id = ?
               AND w.table_name = ?
               AND w.key_text = keys.key_text
               AND w.deleted = FALSE
              JOIN {_quote(meta.physical_name)} AS d ON d.rid = w.rid
            )
            SELECT * FROM committed_rows
            UNION ALL
            SELECT * FROM workspace_rows
            """,
            [*key_texts, version_id, branch_id, meta.name, branch_id, meta.name],
        ).fetchall()
        return {str(row["key_text"]): self._row_from_result(row, meta, include_rid) for row in rows}

    def _row_from_result(
        self, row: Any, meta: _TableMeta, include_rid: bool
    ) -> dict[str, Any]:
        result = {column: row[column] for column in meta.columns}
        if include_rid:
            result["rid"] = int(row["rid"])
        return result

    def _classify_key_text_diffs(
        self,
        table: str,
        meta: _TableMeta,
        left_rows: dict[str, dict[str, Any]],
        right_rows: dict[str, dict[str, Any]],
    ) -> list[RowDiff]:
        diffs: list[RowDiff] = []
        for key_text in sorted(set(left_rows) | set(right_rows)):
            diff = self._row_diff_from_target_to_source_by_key_text(
                table,
                meta,
                key_text,
                left_rows.get(key_text),
                right_rows.get(key_text),
            )
            if diff is not None:
                diffs.append(diff)
        return diffs

    def _row_diff_from_target_to_source_by_key_text(
        self,
        table: str,
        meta: _TableMeta,
        key_text: str,
        target_row: dict[str, Any] | None,
        source_row: dict[str, Any] | None,
    ) -> RowDiff | None:
        if target_row == source_row:
            return None
        key_dict = self._key_dict_from_text(meta, key_text)
        before = self._strip_internal_row(target_row)
        after = self._strip_internal_row(source_row)
        if before is None and after is not None:
            return RowDiff(table, key_dict, "added", None, after)
        if before is not None and after is None:
            return RowDiff(table, key_dict, "deleted", before, None)
        return RowDiff(table, key_dict, "modified", before, after)

    def _strip_internal_row(
        self, row: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        if row is None:
            return None
        return {key: value for key, value in row.items() if key != "rid"}

    def _key_dict_from_text(self, meta: _TableMeta, key_text: str) -> dict[str, Any]:
        values = json.loads(key_text)
        return dict(zip(meta.pk_columns, values))

    def _key_text_from_key(self, meta: _TableMeta, key: dict[str, Any]) -> str:
        return json.dumps([key[column] for column in meta.pk_columns], default=str)

    def upsert_row(self, branch_id: str, table: str, row: dict[str, Any]) -> None:
        self.upsert_rows(branch_id, table, [row])

    def upsert_rows(
        self, branch_id: str, table: str, rows: list[dict[str, Any]]
    ) -> None:
        if not rows:
            return
        meta = self._meta_for_owner("branch", branch_id, table) if self.enable_schema_branching else self._require_table(table)
        if meta is None:
            raise TableNotRegisteredError(table)
        version_id = self._current_version_id(branch_id)
        remove_rows: list[dict[str, Any]] = []
        for row in rows:
            visible = self._visible_row_by_key_for_branch(
                branch_id,
                version_id,
                table,
                self._row_key(meta, row),
            )
            if visible is not None:
                remove_rows.append(visible)
        self._apply_workspace_delta(
            branch_id,
            table,
            remove_rows,
            self._full_rows_for_meta(meta, rows),
        )

    def delete_key(self, branch_id: str, table: str, key: dict[str, Any]) -> None:
        self.delete_keys(branch_id, table, [key])

    def delete_keys(
        self, branch_id: str, table: str, keys: list[dict[str, Any]]
    ) -> None:
        if not keys:
            return
        version_id = self._current_version_id(branch_id)
        remove_rows = []
        for key in keys:
            visible = self._visible_row_by_key_for_branch(
                branch_id,
                version_id,
                table,
                key,
            )
            if visible is not None:
                remove_rows.append(visible)
        self._apply_workspace_delta(branch_id, table, remove_rows, [])

    def _apply_workspace_delta(
        self,
        branch_id: str,
        table: str,
        remove_rows: list[dict[str, Any]],
        add_rows: list[dict[str, Any]],
    ) -> None:
        meta = self._meta_for_owner("branch", branch_id, table) if self.enable_schema_branching else self._require_table(table)
        if meta is None:
            raise TableNotRegisteredError(table)
        version_id = self._current_version_id(branch_id)
        removed_keys: set[tuple[Any, ...]] = set()
        for row in remove_rows:
            key = self._key_tuple(meta, row)
            removed_keys.add(key)
        add_keys = {self._key_tuple(meta, row) for row in add_rows}
        for row in remove_rows:
            key = self._key_tuple(meta, row)
            if key not in add_keys:
                self._upsert_workspace_marker(branch_id, meta, row, None, deleted=True)
        for row in add_rows:
            key_dict = self._row_key(meta, row)
            key = self._key_tuple(meta, row)
            visible = self._visible_row_by_key_for_branch(
                branch_id,
                version_id,
                table,
                key_dict,
            )
            if visible is not None and key not in removed_keys:
                raise DuplicateKeyError(f"duplicate key on branch {branch_id}: {key_dict}")
            rid = self._insert_workspace_row(meta, row)
            self._upsert_workspace_marker(branch_id, meta, row, rid, deleted=False)

    def _materialize_workspace(self, branch_id: str, commit_msg: str) -> int:
        with self._materialization_lock():
            parent_version = self._vid(self._current_version_id(branch_id))
            if not self._workspace_is_dirty(branch_id):
                return parent_version
            new_version = self._new_version_id()
            now = _utc_now()
            # Keep the parent pointer but avoid updating the parent's reverse
            # children array; concurrent materializations from sibling branches
            # would otherwise contend on the same versiontable tuple.
            self.db.execute(
                """
                INSERT INTO _chronos_branch_orpheus_versiontable
                (vid, author, num_records, parent, children, create_time, commit_time, commit_msg)
                VALUES (?, 'chronos', 0, ?::integer[], ?::integer[], ?, ?, ?)
                """,
                (
                    new_version,
                    [parent_version],
                    [],
                    now,
                    now,
                    commit_msg,
                ),
            )
            table_metas = (
                self._active_metas_for_owner("branch", branch_id).values()
                if self.enable_schema_branching
                else self.tables.values()
            )
            for table_meta in table_metas:
                self._insert_materialized_rlist(
                    table_meta,
                    branch_id,
                    parent_version,
                    new_version,
                )
                self._record_version_deltas_from_workspace(
                    table_meta,
                    branch_id,
                    parent_version,
                    new_version,
                )
            self.db.execute(
                """
                UPDATE _chronos_branch_orpheus_branches
                   SET current_vid = ?
                 WHERE branch_id = ?
                """,
                (new_version, branch_id),
            )
            self._refresh_version_record_count(new_version)
            self.db.execute(
                "DELETE FROM _chronos_branch_orpheus_workspace WHERE branch_id = ?",
                (branch_id,),
            )
            return new_version

    def _workspace_is_dirty(self, branch_id: str) -> bool:
        row = self.db.execute(
            """
            SELECT 1
            FROM _chronos_branch_orpheus_workspace
            WHERE branch_id = ?
            LIMIT 1
            """,
            (branch_id,),
        ).fetchone()
        return row is not None

    def _copy_workspace(self, source_branch: str, dest_branch: str) -> None:
        self.db.execute(
            """
            INSERT INTO _chronos_branch_orpheus_workspace
            (branch_id, table_name, key_text, rid, deleted, updated_at)
            SELECT ?, table_name, key_text, rid, deleted, ?
            FROM _chronos_branch_orpheus_workspace
            WHERE branch_id = ?
            """,
            (dest_branch, _utc_now(), source_branch),
        )

    def _insert_materialized_rlist(
        self,
        meta: _TableMeta,
        branch_id: str,
        parent_version: int,
        new_version: int,
    ) -> None:
        self.db.execute(
            f"""
            INSERT INTO {_quote(self._index_table(meta))}
            (vid, rlist)
            WITH parent_rows AS (
              SELECT unnest(
                COALESCE(
                  (
                    SELECT rlist
                      FROM {_quote(self._index_table(meta))}
                     WHERE vid = ?
                  ),
                  ARRAY[]::integer[]
                )
              ) AS rid
            ),
            live_parent_rows AS (
              SELECT p.rid
                FROM parent_rows AS p
                JOIN {_quote(meta.physical_name)} AS d ON d.rid = p.rid
               WHERE NOT EXISTS (
                 SELECT 1
                   FROM _chronos_branch_orpheus_workspace AS w
                  WHERE w.branch_id = ?
                    AND w.table_name = ?
                    AND w.key_text = {self._key_json_sql(meta, alias='d')}
               )
            ),
            live_workspace_rows AS (
              SELECT w.rid
                FROM _chronos_branch_orpheus_workspace AS w
               WHERE w.branch_id = ?
                 AND w.table_name = ?
                 AND w.deleted = FALSE
            ),
            materialized AS (
              SELECT rid FROM live_parent_rows
              UNION ALL
              SELECT rid FROM live_workspace_rows
            )
            SELECT ?, COALESCE(array_agg(rid ORDER BY rid), ARRAY[]::integer[])
              FROM materialized
            """,
            (
                parent_version,
                branch_id,
                meta.name,
                branch_id,
                meta.name,
                new_version,
            ),
        )

    def _record_version_deltas_from_workspace(
        self,
        meta: _TableMeta,
        branch_id: str,
        parent_version: int,
        new_version: int,
    ) -> None:
        if not self.enable_diff_merge_tracking:
            return
        self._ensure_diff_merge_tracking_tables()
        self.db.execute(
            f"""
            INSERT INTO _chronos_branch_orpheus_version_delta
            (vid, writer_branch_id, table_name, key_text, op, before_rid, after_rid, created_at)
            WITH parent_rows AS (
              SELECT d.rid, {self._key_json_sql(meta, alias='d')} AS key_text
              FROM {_quote(meta.physical_name)} AS d
              JOIN {_quote(self._index_table(meta))} AS i
                ON i.vid = ?
               AND d.rid = ANY(i.rlist)
            )
            SELECT ?, ?, ?, w.key_text,
                   CASE
                     WHEN w.deleted THEN 'deleted'
                     WHEN p.rid IS NULL THEN 'added'
                     ELSE 'modified'
                   END,
                   p.rid,
                   CASE WHEN w.deleted THEN NULL ELSE w.rid END,
                   ?
            FROM _chronos_branch_orpheus_workspace AS w
            LEFT JOIN parent_rows AS p ON p.key_text = w.key_text
            WHERE w.branch_id = ?
              AND w.table_name = ?
              AND NOT (w.deleted AND p.rid IS NULL)
            ON CONFLICT (vid, table_name, key_text)
            DO UPDATE SET
              writer_branch_id = EXCLUDED.writer_branch_id,
              op = EXCLUDED.op,
              before_rid = EXCLUDED.before_rid,
              after_rid = EXCLUDED.after_rid,
              created_at = EXCLUDED.created_at
            """,
            (
                parent_version,
                new_version,
                branch_id,
                meta.name,
                _utc_now(),
                branch_id,
                meta.name,
            ),
        )

    def _record_version_deltas_by_comparing_versions(
        self,
        meta: _TableMeta,
        branch_id: str,
        parent_version: int,
        new_version: int,
    ) -> None:
        if not self.enable_diff_merge_tracking:
            return
        self._ensure_diff_merge_tracking_tables()
        self.db.execute(
            f"""
            INSERT INTO _chronos_branch_orpheus_version_delta
            (vid, writer_branch_id, table_name, key_text, op, before_rid, after_rid, created_at)
            WITH parent_rows AS (
              SELECT d.rid, {self._key_json_sql(meta, alias='d')} AS key_text
              FROM {_quote(meta.physical_name)} AS d
              JOIN {_quote(self._index_table(meta))} AS i
                ON i.vid = ?
               AND d.rid = ANY(i.rlist)
            ),
            new_rows AS (
              SELECT d.rid, {self._key_json_sql(meta, alias='d')} AS key_text
              FROM {_quote(meta.physical_name)} AS d
              JOIN {_quote(self._index_table(meta))} AS i
                ON i.vid = ?
               AND d.rid = ANY(i.rlist)
            )
            SELECT ?, ?, ?, COALESCE(parent_rows.key_text, new_rows.key_text),
                   CASE
                     WHEN parent_rows.rid IS NULL THEN 'added'
                     WHEN new_rows.rid IS NULL THEN 'deleted'
                     ELSE 'modified'
                   END,
                   parent_rows.rid,
                   new_rows.rid,
                   ?
            FROM parent_rows
            FULL OUTER JOIN new_rows ON new_rows.key_text = parent_rows.key_text
            WHERE parent_rows.rid IS DISTINCT FROM new_rows.rid
            ON CONFLICT (vid, table_name, key_text)
            DO UPDATE SET
              writer_branch_id = EXCLUDED.writer_branch_id,
              op = EXCLUDED.op,
              before_rid = EXCLUDED.before_rid,
              after_rid = EXCLUDED.after_rid,
              created_at = EXCLUDED.created_at
            """,
            (
                parent_version,
                new_version,
                new_version,
                branch_id,
                meta.name,
                _utc_now(),
            ),
        )

    def _ensure_schema_branching_tables(self) -> None:
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_orpheus_table_bindings (
              owner_kind TEXT NOT NULL,
              owner_id TEXT NOT NULL,
              table_name TEXT NOT NULL,
              physical_table TEXT,
              pk_columns TEXT,
              columns TEXT,
              column_defs TEXT,
              tombstone INTEGER NOT NULL DEFAULT 0,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL,
              PRIMARY KEY (owner_kind, owner_id, table_name)
            )
            """
        )

    def _active_metas_for_ref(self, ref: _BranchRef | _PreparedBranchRef) -> dict[str, _TableMeta]:
        owner_kind = "checkpoint" if ref.readonly else "branch"
        owner_id = ref.ref if ref.readonly else ref.branch_id
        return self._active_metas_for_owner(owner_kind, owner_id)

    def _active_metas_for_owner(self, owner_kind: str, owner_id: str) -> dict[str, _TableMeta]:
        if not self.enable_schema_branching:
            return dict(self.tables)
        self._ensure_schema_branching_tables()
        rows = self.db.execute(
            """
            SELECT *
            FROM _chronos_branch_orpheus_table_bindings
            WHERE owner_kind = ? AND owner_id = ?
            """,
            (owner_kind, owner_id),
        ).fetchall()
        if not rows:
            return dict(self.tables)
        metas: dict[str, _TableMeta] = {}
        for row in rows:
            if bool(row["tombstone"]):
                continue
            metas[row["table_name"]] = self._meta_from_binding(row)
        return metas

    def _meta_for_owner(
        self, owner_kind: str, owner_id: str, table: str
    ) -> _TableMeta | None:
        if not self.enable_schema_branching:
            return self.tables.get(table)
        row = self.db.execute(
            """
            SELECT *
            FROM _chronos_branch_orpheus_table_bindings
            WHERE owner_kind = ? AND owner_id = ? AND table_name = ?
            """,
            (owner_kind, owner_id, table),
        ).fetchone()
        if row is None:
            return self.tables.get(table)
        if bool(row["tombstone"]):
            return None
        return self._meta_from_binding(row)

    def _meta_from_binding(self, row: Any) -> _TableMeta:
        return _TableMeta(
            name=row["table_name"],
            physical_name=row["physical_table"],
            pk_columns=tuple(json.loads(row["pk_columns"])),
            columns=tuple(json.loads(row["columns"])),
            column_defs=tuple(json.loads(row["column_defs"])),
            backend=self.name,
        )

    def _record_table_binding(
        self,
        owner_kind: str,
        owner_id: str,
        meta: _TableMeta,
        tombstone: bool,
    ) -> None:
        self._ensure_schema_branching_tables()
        self.db.execute(
            """
            INSERT INTO _chronos_branch_orpheus_table_bindings
            (owner_kind, owner_id, table_name, physical_table, pk_columns,
             columns, column_defs, tombstone, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(owner_kind, owner_id, table_name) DO UPDATE SET
              physical_table = excluded.physical_table,
              pk_columns = excluded.pk_columns,
              columns = excluded.columns,
              column_defs = excluded.column_defs,
              tombstone = excluded.tombstone,
              created_at = excluded.created_at,
              metadata = excluded.metadata
            """,
            (
                owner_kind,
                owner_id,
                meta.name,
                meta.physical_name,
                json.dumps(meta.pk_columns),
                json.dumps(meta.columns),
                json.dumps(meta.column_defs),
                1 if tombstone else 0,
                _utc_now(),
                "{}",
            ),
        )

    def _record_tombstone(self, owner_kind: str, owner_id: str, table: str) -> None:
        self._ensure_schema_branching_tables()
        self.db.execute(
            """
            INSERT INTO _chronos_branch_orpheus_table_bindings
            (owner_kind, owner_id, table_name, physical_table, pk_columns,
             columns, column_defs, tombstone, created_at, metadata)
            VALUES (?, ?, ?, NULL, NULL, NULL, NULL, 1, ?, ?)
            ON CONFLICT(owner_kind, owner_id, table_name) DO UPDATE SET
              physical_table = NULL,
              pk_columns = NULL,
              columns = NULL,
              column_defs = NULL,
              tombstone = 1,
              created_at = excluded.created_at,
              metadata = excluded.metadata
            """,
            (owner_kind, owner_id, table, _utc_now(), "{}"),
        )

    def _copy_tombstone_bindings(
        self,
        source_kind: str,
        source_id: str,
        dest_kind: str,
        dest_id: str,
    ) -> None:
        for row in self.db.execute(
            """
            SELECT table_name
            FROM _chronos_branch_orpheus_table_bindings
            WHERE owner_kind = ? AND owner_id = ? AND tombstone = 1
            """,
            (source_kind, source_id),
        ).fetchall():
            self._record_tombstone(dest_kind, dest_id, row["table_name"])

    def _require_table_for_ref(
        self, ref: _PreparedBranchRef, table: str
    ) -> _TableMeta:
        if not self.enable_schema_branching:
            return self._require_table(table)
        metas = ref.metadata.get("tables")
        if not isinstance(metas, dict):
            metas = self._active_metas_for_ref(ref)
        meta = metas.get(table)
        if meta is None:
            raise TableNotRegisteredError(table)
        return meta

    def _active_metas_for_version(self, version_id: str | int) -> dict[str, _TableMeta]:
        if not self.enable_schema_branching:
            return dict(self.tables)
        branch = self.db.execute(
            """
            SELECT branch_id
            FROM _chronos_branch_orpheus_branches
            WHERE current_vid = ?
            ORDER BY branch_id
            LIMIT 1
            """,
            (self._vid(version_id),),
        ).fetchone()
        if branch is not None:
            return self._active_metas_for_owner("branch", branch["branch_id"])
        checkpoint = self.db.execute(
            """
            SELECT checkpoint_id
            FROM _chronos_branch_orpheus_checkpoints
            WHERE vid = ?
            ORDER BY checkpoint_id
            LIMIT 1
            """,
            (self._vid(version_id),),
        ).fetchone()
        if checkpoint is not None:
            return self._active_metas_for_owner("checkpoint", checkpoint["checkpoint_id"])
        return dict(self.tables)

    def _active_meta_for_version(self, version_id: str | int, table: str) -> _TableMeta:
        meta = self._active_metas_for_version(version_id).get(table)
        if meta is None:
            raise TableNotRegisteredError(table)
        return meta

    def _known_tables(self) -> set[str]:
        known = set(self.tables)
        if not self.enable_schema_branching:
            return known
        for row in self.db.execute(
            "SELECT DISTINCT table_name FROM _chronos_branch_orpheus_table_bindings"
        ).fetchall():
            known.add(row["table_name"])
        return known

    def _validate_query_tables_visible(self, ref: _PreparedBranchRef, sql: str) -> None:
        active = set(self._active_metas_for_ref(ref))
        known = self._known_tables()
        tree = sqlglot.parse_one(sql, read=self.db.dialect)
        for table_node in tree.find_all(exp.Table):
            table = _table_key(table_node)
            if table in known and table not in active:
                raise TableNotRegisteredError(table)

    @staticmethod
    def _is_schema_statement(sql: str) -> bool:
        head = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
        return head in {"CREATE", "ALTER", "DROP"}

    def _execute_schema_ddl(
        self,
        ref: _PreparedBranchRef,
        tree: exp.Expression,
    ) -> ExecuteResult:
        with _chronos_metadata_lock(self.db):
            if isinstance(tree, exp.Create):
                return self._execute_create_table_ddl(ref, tree)
            if isinstance(tree, exp.Alter):
                return self._execute_alter_table_ddl(ref, tree)
            if isinstance(tree, exp.Drop):
                return self._execute_drop_table_ddl(ref, tree)
        raise UnsupportedSQLError("unsupported schema statement")

    def _execute_create_table_ddl(
        self,
        ref: _PreparedBranchRef,
        tree: exp.Create,
    ) -> ExecuteResult:
        if str(tree.args.get("kind", "")).upper() != "TABLE":
            raise UnsupportedSQLError("only CREATE TABLE is supported")
        schema = tree.this
        if not isinstance(schema, exp.Schema) or not isinstance(schema.this, exp.Table):
            raise UnsupportedSQLError("CREATE TABLE must define columns")
        table = _table_key(schema.this)
        if self._meta_for_owner("branch", ref.branch_id, table) is not None:
            raise BranchAlreadyExistsError(table)
        columns, defs, pk_columns = self._column_defs_from_schema(schema)
        if not pk_columns:
            raise UnsupportedSQLError("CREATE TABLE requires an inline primary key")
        meta = _TableMeta(
            name=table,
            physical_name=self._new_schema_physical_table(table),
            pk_columns=tuple(pk_columns),
            columns=tuple(columns),
            column_defs=tuple(defs),
            backend=self.name,
        )
        self._create_orpheus_physical_table(meta)
        self._insert_version_rlist(meta, self._current_version_id(ref.branch_id), [])
        self._record_table_binding("branch", ref.branch_id, meta, False)
        self._create_indexes_for_table(meta)
        return ExecuteResult(0)

    def _execute_alter_table_ddl(
        self,
        ref: _PreparedBranchRef,
        tree: exp.Alter,
    ) -> ExecuteResult:
        if str(tree.args.get("kind", "")).upper() != "TABLE":
            raise UnsupportedSQLError("only ALTER TABLE is supported")
        if not isinstance(tree.this, exp.Table):
            raise UnsupportedSQLError("ALTER TABLE must target a table")
        table = _table_key(tree.this)
        old_meta = self._meta_for_owner("branch", ref.branch_id, table)
        if old_meta is None:
            raise TableNotRegisteredError(table)
        actions = list(tree.args.get("actions") or [])
        if len(actions) != 1:
            raise UnsupportedSQLError(
                "only single-action ALTER TABLE statements are supported"
            )
        action = actions[0]
        if isinstance(action, exp.ColumnDef):
            return self._execute_alter_table_add_column_ddl(ref, table, old_meta, action)
        if isinstance(action, exp.Drop):
            return self._execute_alter_table_drop_column_ddl(ref, table, old_meta, action)
        if isinstance(action, exp.AlterColumn):
            return self._execute_alter_table_type_ddl(ref, table, old_meta, action)
        raise UnsupportedSQLError(
            "only ALTER TABLE ADD COLUMN, DROP COLUMN, and ALTER COLUMN TYPE are supported"
        )

    def _execute_alter_table_add_column_ddl(
        self,
        ref: _PreparedBranchRef,
        table: str,
        old_meta: _TableMeta,
        action: exp.ColumnDef,
    ) -> ExecuteResult:
        column = self._column_def_name(action)
        if column in old_meta.columns:
            raise BranchingError(f"column already exists: {column}")
        new_def = self._column_def_sql(action)
        meta = _TableMeta(
            name=table,
            physical_name=old_meta.physical_name,
            pk_columns=old_meta.pk_columns,
            columns=tuple([*old_meta.columns, column]),
            column_defs=tuple([*old_meta.column_defs, new_def]),
            backend=self.name,
        )
        self._materialize_workspace(ref.branch_id, "chronos schema change")
        if self._schema_physical_is_private_to_branch(ref.branch_id, old_meta.physical_name):
            self.db.execute(
                f"ALTER TABLE {_quote(old_meta.physical_name)} ADD COLUMN {new_def}"
            )
            self._record_table_binding("branch", ref.branch_id, meta, False)
            return ExecuteResult(0)
        default_sql = self._column_default_sql(action)
        self._copy_visible_rows_to_new_schema(
            ref.branch_id,
            old_meta,
            meta,
            select_sql_by_column={column: default_sql or "NULL"},
        )
        return ExecuteResult(0)

    def _execute_alter_table_drop_column_ddl(
        self,
        ref: _PreparedBranchRef,
        table: str,
        old_meta: _TableMeta,
        action: exp.Drop,
    ) -> ExecuteResult:
        if str(action.args.get("kind", "")).upper() != "COLUMN":
            raise UnsupportedSQLError("only ALTER TABLE DROP COLUMN is supported")
        column = self._drop_column_name(action)
        if column not in old_meta.columns:
            raise TableNotRegisteredError(f"column missing from {table}: {column}")
        if column in old_meta.pk_columns:
            raise UnsupportedSQLError("dropping primary key columns is not supported")
        kept = [
            (old_column, old_def)
            for old_column, old_def in zip(old_meta.columns, old_meta.column_defs)
            if old_column != column
        ]
        meta = _TableMeta(
            name=table,
            physical_name=old_meta.physical_name,
            pk_columns=old_meta.pk_columns,
            columns=tuple(old_column for old_column, _old_def in kept),
            column_defs=tuple(old_def for _old_column, old_def in kept),
            backend=self.name,
        )
        self._materialize_workspace(ref.branch_id, "chronos schema change")
        if self._schema_physical_is_private_to_branch(ref.branch_id, old_meta.physical_name):
            self.db.execute(
                f"ALTER TABLE {_quote(old_meta.physical_name)} DROP COLUMN {_quote(column)}"
            )
            self._record_table_binding("branch", ref.branch_id, meta, False)
            return ExecuteResult(0)
        self._copy_visible_rows_to_new_schema(ref.branch_id, old_meta, meta)
        return ExecuteResult(0)

    def _execute_alter_table_type_ddl(
        self,
        ref: _PreparedBranchRef,
        table: str,
        old_meta: _TableMeta,
        action: exp.AlterColumn,
    ) -> ExecuteResult:
        column = self._alter_column_name(action)
        if column not in old_meta.columns:
            raise TableNotRegisteredError(f"column missing from {table}: {column}")
        dtype = action.args.get("dtype")
        if not isinstance(dtype, exp.Expression):
            raise UnsupportedSQLError("ALTER COLUMN TYPE must specify a type")
        type_sql = dtype.sql(dialect=self.db.dialect)
        column_index = old_meta.columns.index(column)
        defs = list(old_meta.column_defs)
        defs[column_index] = self._replace_column_type_sql(
            defs[column_index], column, type_sql
        )
        meta = _TableMeta(
            name=table,
            physical_name=old_meta.physical_name,
            pk_columns=old_meta.pk_columns,
            columns=old_meta.columns,
            column_defs=tuple(defs),
            backend=self.name,
        )
        using = action.args.get("using")
        select_sql = (
            using.sql(dialect=self.db.dialect)
            if isinstance(using, exp.Expression)
            else f"CAST({_quote(column)} AS {type_sql})"
        )
        self._materialize_workspace(ref.branch_id, "chronos schema change")
        if self._schema_physical_is_private_to_branch(ref.branch_id, old_meta.physical_name):
            self.db.execute(
                f"ALTER TABLE {_quote(old_meta.physical_name)} "
                f"ALTER COLUMN {_quote(column)} TYPE {type_sql} USING {select_sql}"
            )
            self._record_table_binding("branch", ref.branch_id, meta, False)
            return ExecuteResult(0)
        self._copy_visible_rows_to_new_schema(
            ref.branch_id,
            old_meta,
            meta,
            select_sql_by_column={column: select_sql},
        )
        return ExecuteResult(0)

    def _execute_drop_table_ddl(
        self,
        ref: _PreparedBranchRef,
        tree: exp.Drop,
    ) -> ExecuteResult:
        if str(tree.args.get("kind", "")).upper() != "TABLE":
            raise UnsupportedSQLError("only DROP TABLE is supported")
        if not isinstance(tree.this, exp.Table):
            raise UnsupportedSQLError("DROP TABLE must target a table")
        table = _table_key(tree.this)
        if self._meta_for_owner("branch", ref.branch_id, table) is None:
            raise TableNotRegisteredError(table)
        self._record_tombstone("branch", ref.branch_id, table)
        return ExecuteResult(0)

    def _copy_visible_rows_to_new_schema(
        self,
        branch_id: str,
        old_meta: _TableMeta,
        new_meta: _TableMeta,
        *,
        select_sql_by_column: dict[str, str] | None = None,
    ) -> None:
        select_sql_by_column = select_sql_by_column or {}
        new_physical = self._new_schema_physical_table(old_meta.name)
        materialized_meta = _TableMeta(
            name=new_meta.name,
            physical_name=new_physical,
            pk_columns=new_meta.pk_columns,
            columns=new_meta.columns,
            column_defs=new_meta.column_defs,
            backend=self.name,
        )
        self._create_orpheus_physical_table(materialized_meta)
        old_cols = set(old_meta.columns)
        insert_cols = ", ".join(_quote(column) for column in materialized_meta.columns)
        select_cols = ", ".join(
            (
                f"{select_sql_by_column[column]} AS {_quote(column)}"
                if column in select_sql_by_column
                else f"{_quote(column)}"
                if column in old_cols
                else "NULL"
            )
            for column in materialized_meta.columns
        )
        vid = self._vid(self._current_version_id(branch_id))
        visible = self._branch_visible_subquery(
            old_meta,
            branch_id,
            str(vid),
            include_rid=False,
        )
        self.db.execute(
            f"""
            WITH inserted AS (
              INSERT INTO {_quote(materialized_meta.physical_name)}
              ({insert_cols})
              SELECT {select_cols}
              FROM ({visible}) AS visible_rows
              RETURNING rid
            )
            INSERT INTO {_quote(self._index_table(materialized_meta))}
            (vid, rlist)
            SELECT ?, COALESCE(array_agg(rid ORDER BY rid), ARRAY[]::integer[])
            FROM inserted
            ON CONFLICT (vid)
            DO UPDATE SET rlist = EXCLUDED.rlist
            """,
            (vid,),
        )
        self._record_table_binding("branch", branch_id, materialized_meta, False)
        self._create_indexes_for_table(materialized_meta)

    def _schema_physical_is_private_to_branch(self, branch_id: str, physical: str) -> bool:
        if not self.enable_schema_branching:
            return False
        row = self.db.execute(
            """
            SELECT COUNT(*) AS owners
            FROM _chronos_branch_orpheus_table_bindings
            WHERE physical_table = ?
              AND tombstone = 0
              AND NOT (owner_kind = 'branch' AND owner_id = ?)
            """,
            (physical, branch_id),
        ).fetchone()
        return row is not None and int(row["owners"]) == 0

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
        default_sql = self._column_default_sql(column_def)
        for constraint in column_def.args.get("constraints") or []:
            kind = (
                constraint.args.get("kind")
                if isinstance(constraint, exp.ColumnConstraint)
                else None
            )
            if allow_primary_key and isinstance(kind, exp.PrimaryKeyColumnConstraint):
                continue
            if isinstance(kind, exp.DefaultColumnConstraint):
                continue
            raise UnsupportedSQLError(
                "column constraints other than inline primary key and constant DEFAULT are not supported"
            )
        kind = column_def.args.get("kind")
        type_sql = kind.sql(dialect=self.db.dialect) if isinstance(kind, exp.Expression) else "TEXT"
        default_clause = f" DEFAULT {default_sql}" if default_sql is not None else ""
        return f"{_quote(self._column_def_name(column_def))} {type_sql}{default_clause}"

    def _column_default_sql(self, column_def: exp.ColumnDef) -> str | None:
        default_sql: str | None = None
        for constraint in column_def.args.get("constraints") or []:
            kind = (
                constraint.args.get("kind")
                if isinstance(constraint, exp.ColumnConstraint)
                else None
            )
            if isinstance(kind, exp.DefaultColumnConstraint):
                try:
                    _expr_value(kind.this, {})
                except UnsupportedSQLError as exc:
                    raise UnsupportedSQLError("only constant DEFAULT expressions are supported") from exc
                default_sql = kind.this.sql(dialect=self.db.dialect)
        return default_sql

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

    def _create_orpheus_physical_table(self, meta: _TableMeta) -> None:
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_quote(meta.physical_name)} (
              rid SERIAL PRIMARY KEY,
              {", ".join(meta.column_defs)}
            )
            """
        )
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_quote(self._index_table(meta))} (
              vid INTEGER PRIMARY KEY,
              rlist INTEGER[] NOT NULL
            )
            """
        )
        self.db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS
            {_quote(f'_chronos_idx_orpheus_{_identifier_token(meta.physical_name)}_pk')}
            ON {_quote(meta.physical_name)}
            ({", ".join(_quote(column) for column in meta.pk_columns)})
            """
        )
        self.db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS
            {_quote(f'_chronos_idx_orpheus_{_identifier_token(meta.physical_name)}_rlist')}
            ON {_quote(self._index_table(meta))} USING GIN (rlist)
            """
        )

    def _create_indexes_for_table(self, meta: _TableMeta) -> None:
        for index in self.indexes.values():
            if index.table == meta.name:
                self._create_physical_index(meta, index)

    def _create_physical_index(self, meta: _TableMeta, index: _IndexMeta) -> None:
        if set(index.columns) - set(meta.columns):
            return
        index_name = (
            f"_chronos_idx_orpheus_{_identifier_token(meta.physical_name)}_{_identifier_token(index.name)}"
            if self.enable_schema_branching
            else f"_chronos_idx_orpheus_{index.name}"
        )
        self.db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {_quote(index_name)}
            ON {_quote(meta.physical_name)}
            ({", ".join(_quote(column) for column in index.columns)})
            """
        )

    def _new_schema_physical_table(self, table: str) -> str:
        return orpheus_dataset_tables(
            f"_chronos_b_orpheus_{_physical_table_suffix(table)}_{uuid.uuid4().hex[:8]}"
        ).datatable

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

    def _fresh_ref(self, ref: _PreparedBranchRef) -> _PreparedBranchRef:
        if ref.readonly:
            return ref
        current = self._current_version_id(ref.branch_id)
        if current == ref.ref:
            return ref
        return self.prepare_ref(_BranchRef(ref.branch_id, current, False))

    def _replacements(self, ref: _BranchRef, include_rid: bool) -> dict[str, str]:
        version_id = ref.ref if ref.readonly else self._current_version_id(ref.branch_id)
        metas = self._active_metas_for_ref(ref) if self.enable_schema_branching else self.tables
        return {
            table: (
                self._committed_visible_subquery(meta, version_id, include_rid)
                if ref.readonly
                else self._branch_visible_subquery(
                    meta,
                    ref.branch_id,
                    version_id,
                    include_rid,
                )
            )
            for table, meta in metas.items()
        }

    def _prepared_replacements(self, ref: _PreparedBranchRef) -> dict[str, str]:
        replacements = ref.metadata.get("replacements")
        if isinstance(replacements, dict):
            return replacements
        return self._replacements(_BranchRef(ref.branch_id, ref.ref, ref.readonly), False)

    def _committed_visible_subquery(
        self, meta: _TableMeta, version_id: str, include_rid: bool
    ) -> str:
        cols = [f"d.{_quote(column)}" for column in meta.columns]
        if include_rid:
            cols.insert(0, "d.rid")
        if self._vid(version_id) == 1:
            rid_limit = self._base_version_rid_limit(meta, version_id)
            # Version 1 is loaded before any branch-local rows are appended to
            # the shared datatable, so its rid set is a stable prefix. Scanning
            # that prefix lets PostgreSQL push filters into the physical table
            # instead of expanding the whole rlist array for every read.
            return (
                f"SELECT {', '.join(cols)} "
                f"FROM {_quote(meta.physical_name)} AS d "
                f"WHERE d.rid <= {rid_limit}"
            )
        return (
            f"SELECT {', '.join(cols)} "
            f"FROM {_quote(meta.physical_name)} AS d "
            f"JOIN {_quote(self._index_table(meta))} AS i "
            f"  ON i.vid = {self._vid(version_id)} "
            f" AND d.rid = ANY(i.rlist)"
        )

    def _branch_visible_subquery(
        self,
        meta: _TableMeta,
        branch_id: str,
        version_id: str,
        include_rid: bool,
    ) -> str:
        cols = [f"d.{_quote(column)}" for column in meta.columns]
        if include_rid:
            cols.insert(0, "d.rid")
        select_cols = ", ".join(cols)
        branch_literal = self._sql_literal(branch_id)
        table_literal = self._sql_literal(meta.name)
        if self._vid(version_id) == 1:
            rid_limit = self._base_version_rid_limit(meta, version_id)
            base_from = (
                f"FROM {_quote(meta.physical_name)} AS d "
                f"WHERE d.rid <= {rid_limit} "
            )
        else:
            base_from = (
                f"FROM {_quote(meta.physical_name)} AS d "
                f"JOIN {_quote(self._index_table(meta))} AS i "
                f"  ON i.vid = {self._vid(version_id)} "
                f" AND d.rid = ANY(i.rlist) "
                f"WHERE TRUE "
            )
        return (
            f"SELECT {select_cols} "
            f"{base_from}"
            f"AND NOT EXISTS ("
            f"  SELECT 1 FROM _chronos_branch_orpheus_workspace AS w "
            f"  WHERE w.branch_id = {branch_literal} "
            f"    AND w.table_name = {table_literal} "
            f"    AND w.key_text = {self._key_json_sql(meta, alias='d')}"
            f") "
            f"UNION ALL "
            f"SELECT {select_cols} "
            f"FROM _chronos_branch_orpheus_workspace AS w "
            f"JOIN {_quote(meta.physical_name)} AS d ON d.rid = w.rid "
            f"WHERE w.branch_id = {branch_literal} "
            f"  AND w.table_name = {table_literal} "
            f"  AND w.deleted = FALSE"
        )

    def _base_version_rid_limit(self, meta: _TableMeta, version_id: str) -> int:
        key = (meta.physical_name, str(self._vid(version_id)))
        cached = self._base_version_rid_limit_cache.get(key)
        if cached is not None:
            return cached
        row = self.db.execute(
            f"""
            SELECT COALESCE(MAX(r.rid), 0) AS rid_limit
            FROM {_quote(self._index_table(meta))} AS i
            CROSS JOIN LATERAL unnest(i.rlist) AS r(rid)
            WHERE i.vid = ?
            """,
            (self._vid(version_id),),
        ).fetchone()
        rid_limit = int(row["rid_limit"]) if row is not None else 0
        self._base_version_rid_limit_cache[key] = rid_limit
        return rid_limit

    def _select_matching_rows(
        self,
        ref: _PreparedBranchRef,
        table: str,
        where: str,
        params: dict[str, Any],
        direct_filter: bool,
    ) -> list[dict[str, Any]]:
        if not direct_filter:
            meta = self._require_table_for_ref(ref, table)
            select_cols = ", ".join(["rid", *[_quote(column) for column in meta.columns]])
            sql = f"SELECT {select_cols} FROM {_quote_table_name(table)}{where}"
            rewritten = _rewrite_tables(
                sql,
                self._replacements(
                    _BranchRef(ref.branch_id, ref.ref, ref.readonly),
                    include_rid=True,
                ),
                self.db.dialect,
            )
            rows = self.db.execute(rewritten, params).fetchall()
            return [dict(row) for row in rows]
        meta = self._require_table_for_ref(ref, table)
        visible = (
            self._committed_visible_subquery(meta, ref.ref, include_rid=True)
            if ref.readonly
            else self._branch_visible_subquery(
                meta,
                ref.branch_id,
                self._current_version_id(ref.branch_id),
                include_rid=True,
            )
        )
        stripped = where.strip()
        if stripped:
            if not stripped.upper().startswith("WHERE "):
                raise UnsupportedSQLError("unsupported WHERE clause")
            visible = f"SELECT * FROM ({visible}) AS visible_rows WHERE {stripped[6:]}"
        rows = self.db.execute(visible, params).fetchall()
        return [dict(row) for row in rows]

    def _full_rows(
        self, ref: _PreparedBranchRef, table: str, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        meta = self._require_table_for_ref(ref, table)
        return self._full_rows_for_meta(meta, rows)

    def _full_rows_for_meta(
        self, meta: _TableMeta, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        return [{column: row.get(column) for column in meta.columns} for row in rows]

    def _key_tuple(self, meta: _TableMeta, row: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(row[column] for column in meta.pk_columns)

    def _visible_row_by_key(
        self, version_id: str | int, table: str, key: dict[str, Any]
    ) -> dict[str, Any] | None:
        meta = self._active_meta_for_version(version_id, table)
        cols = ", ".join(f"d.{_quote(column)}" for column in meta.columns)
        row = self.db.execute(
            f"""
            SELECT d.rid, {cols}
            FROM {_quote(meta.physical_name)} AS d
            JOIN {_quote(self._index_table(meta))} AS i
              ON i.vid = ?
             AND d.rid = ANY(i.rlist)
            WHERE {self._key_where(meta, alias='d')}
            LIMIT 1
            """,
            [self._vid(version_id), *self._key_values(meta, key)],
        ).fetchone()
        return dict(row) if row is not None else None

    def _visible_row_by_key_for_branch(
        self,
        branch_id: str,
        version_id: str | int,
        table: str,
        key: dict[str, Any],
    ) -> dict[str, Any] | None:
        meta = self._meta_for_owner("branch", branch_id, table) if self.enable_schema_branching else self._require_table(table)
        if meta is None:
            raise TableNotRegisteredError(table)
        marker = self.db.execute(
            """
            SELECT rid, deleted
            FROM _chronos_branch_orpheus_workspace
            WHERE branch_id = ?
              AND table_name = ?
              AND key_text = ?
            """,
            (branch_id, table, self._workspace_key_text(meta, key)),
        ).fetchone()
        if marker is not None:
            if marker["deleted"]:
                return None
            return self._row_by_rid(meta, int(marker["rid"]))
        return self._visible_row_by_key_in_meta(meta, version_id, key)

    def _row_by_rid(self, meta: _TableMeta, rid: int) -> dict[str, Any] | None:
        cols = ", ".join(f"d.{_quote(column)}" for column in meta.columns)
        row = self.db.execute(
            f"""
            SELECT d.rid, {cols}
            FROM {_quote(meta.physical_name)} AS d
            WHERE d.rid = ?
            LIMIT 1
            """,
            (rid,),
        ).fetchone()
        return dict(row) if row is not None else None

    def _visible_row_by_key_in_meta(
        self,
        meta: _TableMeta,
        version_id: str | int,
        key: dict[str, Any],
    ) -> dict[str, Any] | None:
        cols = ", ".join(f"d.{_quote(column)}" for column in meta.columns)
        row = self.db.execute(
            f"""
            SELECT d.rid, {cols}
            FROM {_quote(meta.physical_name)} AS d
            JOIN {_quote(self._index_table(meta))} AS i
              ON i.vid = ?
             AND d.rid = ANY(i.rlist)
            WHERE {self._key_where(meta, alias='d')}
            LIMIT 1
            """,
            [self._vid(version_id), *self._key_values(meta, key)],
        ).fetchone()
        return dict(row) if row is not None else None

    def _upsert_workspace_marker(
        self,
        branch_id: str,
        meta: _TableMeta,
        row: dict[str, Any],
        rid: int | None,
        *,
        deleted: bool,
    ) -> None:
        self.db.execute(
            """
            INSERT INTO _chronos_branch_orpheus_workspace
            (branch_id, table_name, key_text, rid, deleted, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (branch_id, table_name, key_text)
            DO UPDATE SET
              rid = EXCLUDED.rid,
              deleted = EXCLUDED.deleted,
              updated_at = EXCLUDED.updated_at
            """,
            (
                branch_id,
                meta.name,
                self._workspace_key_text(meta, row),
                rid,
                deleted,
                _utc_now(),
            ),
        )

    def _workspace_key_text(self, meta: _TableMeta, row: dict[str, Any]) -> str:
        return json.dumps([row[column] for column in meta.pk_columns], default=str)

    def _key_json_sql(self, meta: _TableMeta, alias: str) -> str:
        columns = ", ".join(f"{_quote(alias)}.{_quote(column)}" for column in meta.pk_columns)
        return f"jsonb_build_array({columns})::text"

    @staticmethod
    def _sql_literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _insert_version_rlist(
        self,
        meta: _TableMeta,
        version_id: str | int,
        rids: list[int],
    ) -> None:
        self.db.execute(
            f"""
            INSERT INTO {_quote(self._index_table(meta))}
            (vid, rlist)
            VALUES (?, ?::integer[])
            ON CONFLICT (vid)
            DO UPDATE SET rlist = EXCLUDED.rlist
            """,
            (self._vid(version_id), rids),
        )

    def _copy_source_rows_to_version(
        self,
        meta: _TableMeta,
        source_table: str,
        version_id: str | int,
    ) -> None:
        cols = ", ".join(_quote(column) for column in meta.columns)
        self.db.execute(
            f"""
            WITH inserted AS (
              INSERT INTO {_quote(meta.physical_name)}
              ({cols})
              SELECT {cols}
              FROM {_quote_table_name(source_table)}
              RETURNING rid
            )
            INSERT INTO {_quote(self._index_table(meta))}
            (vid, rlist)
            SELECT ?, COALESCE(array_agg(rid ORDER BY rid), ARRAY[]::integer[])
            FROM inserted
            ON CONFLICT (vid)
            DO UPDATE SET rlist = EXCLUDED.rlist
            """,
            (self._vid(version_id),),
        )

    def _insert_workspace_row(self, meta: _TableMeta, row: dict[str, Any]) -> int:
        return self._insert_physical_row(meta, row)

    def _insert_physical_row(
        self,
        meta: _TableMeta,
        row: dict[str, Any],
    ) -> int:
        cols = list(meta.columns)
        inserted = self.db.execute(
            f"""
            INSERT INTO {_quote(meta.physical_name)}
            ({", ".join(_quote(column) for column in cols)})
            VALUES ({_placeholders(len(cols))})
            RETURNING rid
            """,
            [row.get(column) for column in meta.columns],
        ).fetchone()
        return int(inserted["rid"])

    def _index_table(self, meta: _TableMeta) -> str:
        return self._index_table_for_physical(meta.physical_name)

    @staticmethod
    def _index_table_for_physical(physical_name: str) -> str:
        if physical_name.endswith("_datatable"):
            return f"{physical_name[:-len('_datatable')]}_indextable"
        return f"{physical_name}_indextable"

    def _current_version_id(self, branch_id: str) -> str:
        row = self._branch_row(branch_id)
        if row is None:
            raise BranchNotFoundError(branch_id)
        return str(row["current_vid"])

    def _branch_row(self, branch_id: str) -> Any | None:
        return self.db.execute(
            "SELECT * FROM _chronos_branch_orpheus_branches WHERE branch_id = ?",
            (branch_id,),
        ).fetchone()

    def _version_row(self, version_id: str | int) -> Any | None:
        return self.db.execute(
            "SELECT * FROM _chronos_branch_orpheus_versiontable WHERE vid = ?",
            (self._vid(version_id),),
        ).fetchone()

    def _checkpoint_row(self, checkpoint: str) -> Any | None:
        return self.db.execute(
            """
            SELECT *
            FROM _chronos_branch_orpheus_checkpoints
            WHERE checkpoint_id = ?
            """,
            (checkpoint,),
        ).fetchone()

    def _refresh_version_record_count(
        self, version_id: str | int, delta: int | None = None
    ) -> None:
        vid = self._vid(version_id)
        if delta is not None:
            self.db.execute(
                """
                UPDATE _chronos_branch_orpheus_versiontable AS child
                   SET num_records = GREATEST(parent.num_records + ?, 0)
                  FROM _chronos_branch_orpheus_versiontable AS parent
                 WHERE child.vid = ?
                   AND parent.vid = (
                     SELECT parent_vid
                     FROM unnest(child.parent) AS p(parent_vid)
                     WHERE parent_vid > 0
                     LIMIT 1
                   )
                """,
                (delta, vid),
            )
            return
        total = 0
        metas = self._active_metas_for_version(version_id).values() if self.enable_schema_branching else self.tables.values()
        for meta in metas:
            row = self.db.execute(
                f"""
                SELECT COALESCE(cardinality(rlist), 0) AS count
                FROM {_quote(self._index_table(meta))}
                WHERE vid = ?
                """,
                (vid,),
            ).fetchone()
            total += int(row["count"]) if row is not None else 0
        self.db.execute(
            """
            UPDATE _chronos_branch_orpheus_versiontable
               SET num_records = ?
             WHERE vid = ?
            """,
            (total, vid),
        )

    def _new_version_id(self) -> int:
        row = self.db.execute(
            "SELECT nextval('_chronos_branch_orpheus_vid_seq') AS vid"
        ).fetchone()
        return int(row["vid"])

    @staticmethod
    def _vid(value: str | int) -> int:
        return int(value)
