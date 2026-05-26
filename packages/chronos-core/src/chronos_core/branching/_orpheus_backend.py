from __future__ import annotations

from chronos_core.branching._common import *
from chronos_core.branching._orpheus_implementation import orpheus_dataset_tables


class _OrpheusBackend(_SQLBranchBackend):
    """Chronos adapter for the OrpheusDB implementation model.

    The upstream implementation stores each CVD as a datatable, an indextable,
    and a versiontable. Chronos defaults to the paper's split-by-vlist variant:
    immutable records live in the datatable, while the indextable maps each
    ``rid`` to the explicit list of versions containing that record.
    """

    name = "orpheus"

    def ensure(self) -> None:
        if self.db.dialect != "postgres":
            raise BranchingError(
                "Orpheus backend requires PostgreSQL because it uses "
                "array-backed vlist storage"
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

    def register_table(self, table: str, primary_key: list[str]) -> None:
        columns, defs = _table_defs(self.db, table)
        missing = set(primary_key) - set(columns)
        if missing:
            raise TableNotRegisteredError(
                f"primary key columns missing from {table}: {missing}"
            )
        if table in self.tables:
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
        vlist_table = self._vlist_table_for_physical(physical)
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_quote(vlist_table)} (
              rid INTEGER PRIMARY KEY,
              vlist INTEGER[] NOT NULL
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
            {_quote(f'_chronos_idx_orpheus_{_physical_table_suffix(table)}_vlist')}
            ON {_quote(vlist_table)} USING GIN (vlist)
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
        source_cols = ", ".join(_quote(column) for column in columns)
        rows = self.db.execute(
            f"SELECT {source_cols} FROM {_quote_table_name(table)}"
        ).fetchall()
        for row in rows:
            self._insert_physical_row(meta, dict(row), initial_version=1)
        self._refresh_version_record_count(1)

    def create_index(
        self, table: str, columns: list[str], name: str | None = None
    ) -> IndexInfo:
        meta, index = self._validate_index(table, columns, name)
        if index.name not in self.indexes:
            self.db.execute(
                f"""
                CREATE INDEX {_quote(f'_chronos_idx_orpheus_{index.name}')}
                ON {_quote(meta.physical_name)}
                ({", ".join(_quote(column) for column in index.columns)})
                """
            )
            self._record_index(index)
        return IndexInfo(index.name, index.table, index.columns, index.backend)

    def create_branch(
        self, branch_id: str, from_branch: str, metadata: dict[str, Any] | None = None
    ) -> None:
        if self._branch_row(branch_id) is not None:
            raise BranchAlreadyExistsError(branch_id)
        source = self._branch_row(from_branch)
        if source is None:
            raise BranchNotFoundError(from_branch)
        self.db.execute(
            """
            INSERT INTO _chronos_branch_orpheus_branches
            (branch_id, current_vid, created_at, metadata)
            VALUES (?, ?, ?, ?)
            """,
            (
                branch_id,
                source["current_vid"],
                _utc_now(),
                _json_dumps(metadata),
            ),
        )

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
        now = _utc_now()
        self.db.execute(
            """
            INSERT INTO _chronos_branch_orpheus_checkpoints
            (checkpoint_id, branch_id, vid, created_at, metadata)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                checkpoint,
                branch,
                branch_row["current_vid"],
                now,
                _json_dumps(metadata),
            ),
        )
        return CheckpointInfo(
            checkpoint,
            branch,
            str(branch_row["current_vid"]),
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
                "query_rewrite_cache": {},
                "statement_plan_cache": {},
            },
        )

    def refresh_ref_after_execute(self, ref: _PreparedBranchRef) -> _PreparedBranchRef:
        if ref.readonly:
            return ref
        current = self._current_version_id(ref.branch_id)
        return self.prepare_ref(_BranchRef(ref.branch_id, current, False))

    def query(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        ref = self._fresh_ref(ref)
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
        plan = self._statement_plan(ref, sql)
        if isinstance(plan, _InsertPlan):
            rows = _insert_rows_from_plan(plan, params)
            full_rows = self._full_rows(plan.table, rows)
            self._commit_table_delta(ref.branch_id, plan.table, [], full_rows)
            return ExecuteResult(len(full_rows))
        if isinstance(plan, _UpdatePlan):
            current_rows = self._select_matching_rows(
                ref, plan.table, plan.where_sql, params, plan.direct_filter
            )
            new_rows: list[dict[str, Any]] = []
            meta = self._require_table(plan.table)
            remove_rids: list[int] = []
            for current in current_rows:
                remove_rids.append(int(current["rid"]))
                old_row = {column: current[column] for column in meta.columns}
                new_row = dict(old_row)
                new_row.update(_update_assignments_from_plan(plan, params, old_row))
                new_rows.append(new_row)
            self._commit_table_delta(ref.branch_id, plan.table, remove_rids, new_rows)
            return ExecuteResult(len(current_rows))
        if isinstance(plan, _DeletePlan):
            current_rows = self._select_matching_rows(
                ref, plan.table, plan.where_sql, params, plan.direct_filter
            )
            self._commit_table_delta(
                ref.branch_id,
                plan.table,
                [int(row["rid"]) for row in current_rows],
                [],
            )
            return ExecuteResult(len(current_rows))
        raise UnsupportedSQLError("only SELECT, INSERT, UPDATE, and DELETE are supported")

    def visible_rows(self, branch_id: str, table: str) -> list[dict[str, Any]]:
        ref = self.prepare_ref(_BranchRef(branch_id, self._current_version_id(branch_id)))
        return self.query(ref, f"SELECT * FROM {_quote_table_name(table)}", {})

    def upsert_row(self, branch_id: str, table: str, row: dict[str, Any]) -> None:
        self.upsert_rows(branch_id, table, [row])

    def upsert_rows(
        self, branch_id: str, table: str, rows: list[dict[str, Any]]
    ) -> None:
        if not rows:
            return
        meta = self._require_table(table)
        version_id = self._current_version_id(branch_id)
        remove: list[int] = []
        for row in rows:
            visible = self._visible_row_by_key(version_id, table, self._row_key(meta, row))
            if visible is not None:
                remove.append(int(visible["rid"]))
        self._commit_table_delta(branch_id, table, remove, self._full_rows(table, rows))

    def delete_key(self, branch_id: str, table: str, key: dict[str, Any]) -> None:
        self.delete_keys(branch_id, table, [key])

    def delete_keys(
        self, branch_id: str, table: str, keys: list[dict[str, Any]]
    ) -> None:
        if not keys:
            return
        version_id = self._current_version_id(branch_id)
        remove = []
        for key in keys:
            visible = self._visible_row_by_key(version_id, table, key)
            if visible is not None:
                remove.append(int(visible["rid"]))
        self._commit_table_delta(branch_id, table, remove, [])

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
        return {
            table: self._visible_subquery(meta, version_id, include_rid)
            for table, meta in self.tables.items()
        }

    def _prepared_replacements(self, ref: _PreparedBranchRef) -> dict[str, str]:
        replacements = ref.metadata.get("replacements")
        if isinstance(replacements, dict):
            return replacements
        return self._replacements(_BranchRef(ref.branch_id, ref.ref, ref.readonly), False)

    def _visible_subquery(
        self, meta: _TableMeta, version_id: str, include_rid: bool
    ) -> str:
        cols = [f"d.{_quote(column)}" for column in meta.columns]
        if include_rid:
            cols.insert(0, "d.rid")
        return (
            f"SELECT {', '.join(cols)} "
            f"FROM {_quote(meta.physical_name)} AS d "
            f"JOIN {_quote(self._vlist_table(meta))} AS v ON v.rid = d.rid "
            f"WHERE {self._vid(version_id)} = ANY(v.vlist)"
        )

    def _select_matching_rows(
        self,
        ref: _PreparedBranchRef,
        table: str,
        where: str,
        params: dict[str, Any],
        direct_filter: bool,
    ) -> list[dict[str, Any]]:
        if not direct_filter:
            meta = self._require_table(table)
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
        meta = self._require_table(table)
        visible = self._visible_subquery(meta, ref.ref, include_rid=True)
        stripped = where.strip()
        if stripped:
            if not stripped.upper().startswith("WHERE "):
                raise UnsupportedSQLError("unsupported WHERE clause")
            visible = f"SELECT * FROM ({visible}) AS visible_rows WHERE {stripped[6:]}"
        rows = self.db.execute(visible, params).fetchall()
        return [dict(row) for row in rows]

    def _full_rows(
        self, table: str, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        meta = self._require_table(table)
        return [{column: row.get(column) for column in meta.columns} for row in rows]

    def _commit_table_delta(
        self,
        branch_id: str,
        table: str,
        remove_rids: list[int],
        add_rows: list[dict[str, Any]],
    ) -> str:
        parent_version = self._vid(self._current_version_id(branch_id))
        meta = self._require_table(table)
        add_keys = [self._row_key(meta, row) for row in add_rows]
        remove_set = set(remove_rids)
        for key in add_keys:
            visible = self._visible_row_by_key(parent_version, table, key)
            if visible is not None and int(visible["rid"]) not in remove_set:
                raise DuplicateKeyError(f"duplicate key on branch {branch_id}: {key}")
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
                [parent_version],
                [],
                now,
                now,
                f"chronos update {table}",
            ),
        )
        self.db.execute(
            """
            UPDATE _chronos_branch_orpheus_versiontable
               SET children = array_append(children, ?)
             WHERE vid = ?
            """,
            (new_version, parent_version),
        )
        for table_meta in self.tables.values():
            if table_meta.name == table and remove_rids:
                self.db.execute(
                    f"""
                    UPDATE {_quote(self._vlist_table(table_meta))}
                       SET vlist = array_append(vlist, ?)
                     WHERE ? = ANY(vlist)
                       AND NOT rid = ANY(?::integer[])
                    """,
                    (new_version, parent_version, remove_rids),
                )
            else:
                self.db.execute(
                    f"""
                    UPDATE {_quote(self._vlist_table(table_meta))}
                       SET vlist = array_append(vlist, ?)
                     WHERE ? = ANY(vlist)
                    """,
                    (new_version, parent_version),
                )
        add_rids: list[int] = []
        for row in add_rows:
            rid = self._insert_physical_row(meta, row, initial_version=new_version)
            add_rids.append(rid)
        self._refresh_version_record_count(
            new_version,
            delta=len(add_rows) - len(remove_rids),
        )
        self.db.execute(
            """
            UPDATE _chronos_branch_orpheus_branches
               SET current_vid = ?
             WHERE branch_id = ?
            """,
            (new_version, branch_id),
        )
        return str(new_version)

    def _visible_row_by_key(
        self, version_id: str | int, table: str, key: dict[str, Any]
    ) -> dict[str, Any] | None:
        meta = self._require_table(table)
        cols = ", ".join(f"d.{_quote(column)}" for column in meta.columns)
        row = self.db.execute(
            f"""
            SELECT d.rid, {cols}
            FROM {_quote(meta.physical_name)} AS d
            JOIN {_quote(self._vlist_table(meta))} AS v ON v.rid = d.rid
            WHERE ? = ANY(v.vlist)
              AND {self._key_where(meta, alias='d')}
            LIMIT 1
            """,
            [self._vid(version_id), *self._key_values(meta, key)],
        ).fetchone()
        return dict(row) if row is not None else None

    def _insert_physical_row(
        self,
        meta: _TableMeta,
        row: dict[str, Any],
        *,
        initial_version: str | int,
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
        rid = int(inserted["rid"])
        self.db.execute(
            f"""
            INSERT INTO {_quote(self._vlist_table(meta))}
            (rid, vlist)
            VALUES (?, ?::integer[])
            """,
            (rid, [self._vid(initial_version)]),
        )
        return rid

    def _vlist_table(self, meta: _TableMeta) -> str:
        return self._vlist_table_for_physical(meta.physical_name)

    @staticmethod
    def _vlist_table_for_physical(physical_name: str) -> str:
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
        for meta in self.tables.values():
            row = self.db.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM {_quote(self._vlist_table(meta))}
                WHERE ? = ANY(vlist)
                """,
                (vid,),
            ).fetchone()
            total += int(row["count"])
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
            "SELECT COALESCE(MAX(vid), 0) + 1 AS vid FROM _chronos_branch_orpheus_versiontable"
        ).fetchone()
        return int(row["vid"])

    @staticmethod
    def _vid(value: str | int) -> int:
        return int(value)
