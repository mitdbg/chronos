from __future__ import annotations

from chronos_core.branching._common import *

class _LogBackend(_SQLBranchBackend):
    """Append-only log backend.

    Branches share log prefixes by storing a parent branch and fork transaction.
    Reads reconstruct the visible table from the branch lineage with a windowed
    "latest operation per primary key" query. Writes are cheap appends, while
    reads pay for lineage replay and row dominance resolution.
    """

    name = "log"

    def ensure(self) -> None:
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_log_branches (
              branch_id TEXT PRIMARY KEY,
              parent_branch_id TEXT,
              fork_txn_id BIGINT,
              head_txn_id BIGINT NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_log_checkpoints (
              checkpoint_id TEXT PRIMARY KEY,
              branch_id TEXT NOT NULL,
              head_txn_id BIGINT NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )
        if (
            self.db.execute(
                "SELECT 1 FROM _chronos_branch_log_branches WHERE branch_id = 'main'"
            ).fetchone()
            is None
        ):
            self.db.execute(
                """
                INSERT INTO _chronos_branch_log_branches
                (branch_id, parent_branch_id, fork_txn_id, head_txn_id, created_at, metadata)
                VALUES ('main', NULL, NULL, 0, ?, '{}')
                """,
                (_utc_now(),),
            )
        self.db.commit()

    def register_table(self, table: str, primary_key: list[str]) -> None:
        if table in self.tables:
            return
        columns, defs = _table_defs(self.db, table)
        missing = set(primary_key) - set(columns)
        if missing:
            raise TableNotRegisteredError(f"primary key columns missing from {table}: {missing}")
        physical = f"_chronos_b_log_{_physical_table_suffix(table)}"
        user_defs = ", ".join(defs)
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_quote(physical)} (
              log_id {self.db.auto_increment_primary_key},
              txn_id BIGINT NOT NULL,
              branch_id TEXT NOT NULL,
              op TEXT NOT NULL,
              {user_defs}
            )
            """
        )
        self.db.execute(
            f"CREATE INDEX {_quote(f'idx_{physical}_branch_txn')} "
            f"ON {_quote(physical)} (branch_id, txn_id, log_id)"
        )
        pk_index_columns = ["branch_id", *primary_key, "txn_id", "log_id"]
        self.db.execute(
            f"CREATE INDEX {_quote(f'idx_{physical}_branch_pk_txn')} "
            f"ON {_quote(physical)} "
            f"({', '.join(_quote(c) for c in pk_index_columns)})"
        )
        cols = ", ".join(_quote(c) for c in columns)
        self.db.execute(
            f"""
            INSERT INTO {_quote(physical)}
            (txn_id, branch_id, op, {cols})
            SELECT 0, 'main', 'upsert', {cols} FROM {_quote_table_name(table)}
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
        self.tables[table] = _TableMeta(
            name=table,
            physical_name=physical,
            pk_columns=tuple(primary_key),
            columns=columns,
            column_defs=defs,
            backend=self.name,
        )

    def create_index(
        self, table: str, columns: list[str], name: str | None = None
    ) -> IndexInfo:
        meta, index = self._validate_index(table, columns, name)
        if index.name not in self.indexes:
            indexed_columns = ["branch_id", "txn_id", *index.columns]
            self.db.execute(
                f"""
                CREATE INDEX {_quote(f'_chronos_idx_log_{index.name}')}
                ON {_quote(meta.physical_name)}
                ({", ".join(_quote(c) for c in indexed_columns)})
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
            INSERT INTO _chronos_branch_log_branches
            (branch_id, parent_branch_id, fork_txn_id, head_txn_id, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                branch_id,
                from_branch,
                source["head_txn_id"],
                source["head_txn_id"],
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
            "UPDATE _chronos_branch_log_branches SET metadata = ? WHERE branch_id = ?",
            (_json_dumps(metadata), branch_id),
        )
        return self.get_branch(branch_id)

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
        if self._branch_row(branch_id) is not None:
            raise BranchAlreadyExistsError(branch_id)
        cp = self.db.execute(
            "SELECT * FROM _chronos_branch_log_checkpoints WHERE checkpoint_id = ?",
            (checkpoint,),
        ).fetchone()
        if cp is None:
            raise BranchNotFoundError(f"checkpoint:{checkpoint}")
        self.db.execute(
            """
            INSERT INTO _chronos_branch_log_branches
            (branch_id, parent_branch_id, fork_txn_id, head_txn_id, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                branch_id,
                cp["branch_id"],
                cp["head_txn_id"],
                cp["head_txn_id"],
                _utc_now(),
                "{}",
            ),
        )

    def delete_branch(self, branch_id: str) -> None:
        if branch_id == "main":
            raise BranchingError("main cannot be deleted")
        cur = self.db.execute(
            "DELETE FROM _chronos_branch_log_branches WHERE branch_id = ?",
            (branch_id,),
        )
        if cur.rowcount == 0:
            raise BranchNotFoundError(branch_id)

    def list_branches(self) -> list[BranchInfo]:
        rows = self.db.execute(
            """
            SELECT branch_id, head_txn_id, created_at, metadata
            FROM _chronos_branch_log_branches
            ORDER BY branch_id
            """
        ).fetchall()
        return [
            BranchInfo(
                branch_id=row["branch_id"],
                current_ref=str(row["head_txn_id"]),
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
            current_ref=str(row["head_txn_id"]),
            backend=self.name,
            created_at=row["created_at"],
            metadata=_json_loads(row["metadata"]),
        )

    def create_checkpoint(
        self, checkpoint: str, branch: str, metadata: dict[str, Any] | None = None
    ) -> CheckpointInfo:
        if (
            self.db.execute(
                "SELECT 1 FROM _chronos_branch_log_checkpoints WHERE checkpoint_id = ?",
                (checkpoint,),
            ).fetchone()
            is not None
        ):
            raise BranchAlreadyExistsError(checkpoint)
        source = self._branch_row(branch)
        if source is None:
            raise BranchNotFoundError(branch)
        now = _utc_now()
        self.db.execute(
            """
            INSERT INTO _chronos_branch_log_checkpoints
            (checkpoint_id, branch_id, head_txn_id, created_at, metadata)
            VALUES (?, ?, ?, ?, ?)
            """,
            (checkpoint, branch, source["head_txn_id"], now, _json_dumps(metadata)),
        )
        return CheckpointInfo(
            checkpoint, branch, str(source["head_txn_id"]), now, metadata or {}
        )

    def get_checkpoint(self, checkpoint: str) -> CheckpointInfo:
        cp = self.db.execute(
            "SELECT * FROM _chronos_branch_log_checkpoints WHERE checkpoint_id = ?",
            (checkpoint,),
        ).fetchone()
        if cp is None:
            raise BranchNotFoundError(f"checkpoint:{checkpoint}")
        return CheckpointInfo(
            cp["checkpoint_id"],
            cp["branch_id"],
            str(cp["head_txn_id"]),
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
            SELECT checkpoint_id, branch_id, head_txn_id, created_at, metadata
            FROM _chronos_branch_log_checkpoints
            {where}
            ORDER BY created_at, checkpoint_id
            """,
            tuple(params),
        ).fetchall()
        infos = [
            CheckpointInfo(
                row["checkpoint_id"],
                row["branch_id"],
                str(row["head_txn_id"]),
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
        # Lineage lookup is proportional to branch depth, so it is cached in the
        # session and refreshed only after this session writes a new head. The
        # visible table subqueries only depend on lineage, not user parameters,
        # so they can be reused for every query in the checkout.
        lineage = self._lineage(ref)
        lineage_params: dict[str, Any] = {}
        replacements: dict[str, str] = {}
        for table, meta in self.tables.items():
            subquery, subquery_params = self._visible_subquery_for_lineage(meta, lineage)
            replacements[table] = subquery
            lineage_params.update(subquery_params)
        return _PreparedBranchRef(
            ref.branch_id,
            ref.ref,
            ref.readonly,
            {
                "lineage": lineage,
                "lineage_params": lineage_params,
                "replacements": replacements,
                "query_rewrite_cache": {},
                "statement_plan_cache": {},
            },
        )

    def refresh_ref_after_execute(self, ref: _PreparedBranchRef) -> _PreparedBranchRef:
        if ref.readonly:
            return ref
        return self.prepare_ref(_BranchRef(ref.branch_id, ref.ref, ref.readonly))

    def query(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        bound = dict(params)
        lineage_params = ref.metadata.get("lineage_params")
        replacements = ref.metadata.get("replacements")
        if lineage_params is None or replacements is None:
            lineage_params = {}
            replacements = {}
            for table, meta in self.tables.items():
                subquery, subquery_params = self._visible_subquery_for_lineage(
                    meta,
                    ref.metadata.get("lineage")
                    or self._lineage(_BranchRef(ref.branch_id, ref.ref, ref.readonly)),
                )
                replacements[table] = subquery
                lineage_params.update(subquery_params)
        bound.update(lineage_params)
        rewrite_cache = ref.metadata.setdefault("query_rewrite_cache", {})
        rewritten = rewrite_cache.get(sql)
        if rewritten is None:
            rewritten = _rewrite_tables(sql, replacements, self.db.dialect)
            rewrite_cache[sql] = rewritten
        rows = self.db.execute(rewritten, bound).fetchall()
        return [dict(row) for row in rows]

    def execute(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> ExecuteResult:
        if ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
        plan = self._statement_plan(ref, sql)
        if isinstance(plan, _InsertPlan):
            table = plan.table
            rows = _insert_rows_from_plan(plan, params)
            meta = self._require_table(table)
            full_rows = [
                {column: row.get(column) for column in meta.columns}
                for row in rows
            ]
            self._insert_visible_batch(ref, table, full_rows)
            return ExecuteResult(len(full_rows))
        if isinstance(plan, _UpdatePlan):
            table = plan.table
            current_rows = self._select_matching_rows(ref, table, plan.where_sql, params)
            new_rows: list[dict[str, Any]] = []
            for current in current_rows:
                new_row = dict(current)
                assignments = _update_assignments_from_plan(plan, params, current)
                new_row.update(assignments)
                new_rows.append(new_row)
            self._upsert_rows(ref.branch_id, table, new_rows)
            return ExecuteResult(len(new_rows))
        if isinstance(plan, _DeletePlan):
            table = plan.table
            keys = self._select_matching_keys(
                ref, table, plan.where_sql, params
            )
            self._delete_keys(ref.branch_id, table, keys)
            return ExecuteResult(len(keys))
        raise UnsupportedSQLError("only SELECT, INSERT, UPDATE, and DELETE are supported")

    def visible_rows(self, branch_id: str, table: str) -> list[dict[str, Any]]:
        ref = self.prepare_ref(_BranchRef(branch_id, str(self._branch_head(branch_id))))
        return self.query(ref, f"SELECT * FROM {_quote_table_name(table)}", {})

    def upsert_row(self, branch_id: str, table: str, row: dict[str, Any]) -> None:
        meta = self._require_table(table)
        full_row = {column: row.get(column) for column in meta.columns}
        self._upsert_rows(branch_id, table, [full_row])

    def delete_key(self, branch_id: str, table: str, key: dict[str, Any]) -> None:
        meta = self._require_table(table)
        row = {column: None for column in meta.columns}
        row.update(key)
        self._append_rows(branch_id, meta, "delete", [row])

    def upsert_rows(
        self, branch_id: str, table: str, rows: list[dict[str, Any]]
    ) -> None:
        self._upsert_rows(branch_id, table, rows)

    def delete_keys(
        self, branch_id: str, table: str, keys: list[dict[str, Any]]
    ) -> None:
        self._delete_keys(branch_id, table, keys)

    def _insert_visible(
        self, ref: _BranchRef, table: str, row: dict[str, Any]
    ) -> None:
        meta = self._require_table(table)
        full_row = {column: row.get(column) for column in meta.columns}
        key = {column: full_row[column] for column in meta.pk_columns}
        if self._visible_row(ref, table, key) is not None:
            raise DuplicateKeyError(f"duplicate key on branch {ref.branch_id}: {key}")
        self._upsert_rows(ref.branch_id, table, [full_row])

    def _insert_visible_batch(
        self,
        ref: _PreparedBranchRef,
        table: str,
        rows: list[dict[str, Any]],
    ) -> None:
        if not rows:
            return
        meta = self._require_table(table)
        keyed_rows = [(self._row_key(meta, row), row) for row in rows]
        seen_keys: set[tuple[Any, ...]] = set()
        for key, _ in keyed_rows:
            key_tuple = tuple(key[column] for column in meta.pk_columns)
            if key_tuple in seen_keys:
                raise DuplicateKeyError(f"duplicate key on branch {ref.branch_id}: {key}")
            seen_keys.add(key_tuple)
        visible = self._visible_rows_for_keys(ref, table, [key for key, _ in keyed_rows])
        if visible:
            duplicate = self._row_key(meta, visible[0])
            raise DuplicateKeyError(f"duplicate key on branch {ref.branch_id}: {duplicate}")
        self._upsert_rows(ref.branch_id, table, [row for _, row in keyed_rows])

    def _visible_row(
        self, ref: _BranchRef, table: str, key: dict[str, Any]
    ) -> dict[str, Any] | None:
        meta = self._require_table(table)
        where_parts: list[str] = []
        params: dict[str, Any] = {}
        for idx, column in enumerate(meta.pk_columns):
            param_name = f"_chronos_key_{idx}"
            where_parts.append(f"{_quote(column)} = :{param_name}")
            params[param_name] = key[column]
        rows = self.query(
            ref,
            f"""
            SELECT *
            FROM {_quote_table_name(table)}
            WHERE {" AND ".join(where_parts)}
            """,
            params,
        )
        return rows[0] if rows else None

    def _visible_rows_for_keys(
        self,
        ref: _PreparedBranchRef,
        table: str,
        keys: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not keys:
            return []
        meta = self._require_table(table)
        predicate, bound = self._keys_predicate_named(meta, keys)
        cols = ", ".join(_quote(c) for c in meta.columns)
        return self.query(
            ref,
            f"""
            SELECT {cols}
            FROM {_quote_table_name(table)}
            WHERE {predicate}
            """,
            bound,
        )

    def _select_matching_rows(
        self,
        ref: _PreparedBranchRef,
        table: str,
        where: str,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        meta = self._require_table(table)
        select_cols = ", ".join(_quote(c) for c in meta.columns)
        return self.query(
            ref,
            f"SELECT {select_cols} FROM {_quote_table_name(table)}{where}",
            params,
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

    def _visible_subquery(
        self, ref: _PreparedBranchRef, meta: _TableMeta, params: dict[str, Any]
    ) -> str:
        subquery, subquery_params = self._visible_subquery_for_lineage(
            meta,
            ref.metadata.get("lineage")
            or self._lineage(_BranchRef(ref.branch_id, ref.ref, ref.readonly)),
        )
        params.update(subquery_params)
        return subquery

    def _visible_subquery_for_lineage(
        self, meta: _TableMeta, lineage: list[tuple[str, int]]
    ) -> tuple[str, dict[str, Any]]:
        cols = ", ".join(_quote(c) for c in meta.columns)
        partition_cols = ", ".join(_quote(c) for c in meta.pk_columns)
        union_parts: list[str] = []
        params: dict[str, Any] = {}
        table_token = _identifier_token(meta.name)
        # Each lineage element contributes the log prefix visible from that
        # ancestor. The window function then chooses the nearest/newest operation
        # for each logical key and filters tombstones.
        for lineage_rank, (branch_id, max_txn) in enumerate(lineage):
            branch_param = f"_chronos_log_{table_token}_{lineage_rank}_branch"
            txn_param = f"_chronos_log_{table_token}_{lineage_rank}_txn"
            params[branch_param] = branch_id
            params[txn_param] = max_txn
            union_parts.append(
                f"""
                SELECT
                  {lineage_rank} AS _chronos_lineage_rank,
                  log_id,
                  txn_id,
                  op,
                  {cols}
                FROM {_quote(meta.physical_name)}
                WHERE branch_id = :{branch_param}
                  AND txn_id <= :{txn_param}
                """
            )
        union_sql = "\nUNION ALL\n".join(union_parts)
        return f"""
            SELECT {cols}
            FROM (
              SELECT
                {cols},
                op,
                ROW_NUMBER() OVER (
                  PARTITION BY {partition_cols}
                  ORDER BY _chronos_lineage_rank DESC, txn_id DESC, log_id DESC
                ) AS _chronos_rn
              FROM (
                {union_sql}
              ) AS _chronos_log_candidates
            ) AS _chronos_ranked
            WHERE _chronos_rn = 1
              AND op <> 'delete'
        """, params

    def _lineage(self, ref: _BranchRef) -> list[tuple[str, int]]:
        if ref.readonly:
            branch_id = ref.branch_id
            max_txn = int(ref.ref)
        else:
            row = self._branch_row(ref.branch_id)
            if row is None:
                raise BranchNotFoundError(ref.branch_id)
            branch_id = row["branch_id"]
            max_txn = int(row["head_txn_id"])
        lineage: list[tuple[str, int]] = []
        while True:
            row = self._branch_row(branch_id)
            if row is None:
                raise BranchNotFoundError(branch_id)
            lineage.append((branch_id, max_txn))
            parent = row["parent_branch_id"]
            if parent is None:
                break
            max_txn = int(row["fork_txn_id"])
            branch_id = parent
        lineage.reverse()
        return lineage

    def _append_log(
        self,
        meta: _TableMeta,
        txn_id: int,
        branch_id: str,
        op: str,
        row: dict[str, Any],
    ) -> None:
        cols = ["txn_id", "branch_id", "op", *meta.columns]
        values = [txn_id, branch_id, op, *[row.get(column) for column in meta.columns]]
        self.db.execute(
            f"""
            INSERT INTO {_quote(meta.physical_name)}
            ({", ".join(_quote(c) for c in cols)})
            VALUES ({_placeholders(len(cols))})
            """,
            values,
        )

    def _append_log_rows(
        self,
        meta: _TableMeta,
        txn_id: int,
        branch_id: str,
        op: str,
        rows: list[dict[str, Any]],
    ) -> None:
        if not rows:
            return
        cols = ["txn_id", "branch_id", "op", *meta.columns]
        values = [
            [txn_id, branch_id, op, *[row.get(column) for column in meta.columns]]
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

    def _upsert_rows(
        self, branch_id: str, table: str, rows: list[dict[str, Any]]
    ) -> None:
        if not rows:
            return
        meta = self._require_table(table)
        full_rows = [
            {column: row.get(column) for column in meta.columns}
            for row in rows
        ]
        self._append_rows(branch_id, meta, "upsert", full_rows)

    def _delete_keys(
        self, branch_id: str, table: str, keys: list[dict[str, Any]]
    ) -> None:
        if not keys:
            return
        meta = self._require_table(table)
        rows: list[dict[str, Any]] = []
        for key in keys:
            row = {column: None for column in meta.columns}
            row.update(key)
            rows.append(row)
        self._append_rows(branch_id, meta, "delete", rows)

    def _append_rows(
        self,
        branch_id: str,
        meta: _TableMeta,
        op: str,
        rows: list[dict[str, Any]],
    ) -> None:
        if not rows:
            return
        txn_id = self._new_txn(branch_id)
        self._append_log_rows(meta, txn_id, branch_id, op, rows)
        self._set_branch_head(branch_id, txn_id)

    def _keys_predicate_named(
        self, meta: _TableMeta, keys: list[dict[str, Any]]
    ) -> tuple[str, dict[str, Any]]:
        parts: list[str] = []
        params: dict[str, Any] = {}
        for key_idx, key in enumerate(keys):
            key_parts: list[str] = []
            for column_idx, column in enumerate(meta.pk_columns):
                param_name = f"_chronos_key_{key_idx}_{column_idx}"
                key_parts.append(f"{_quote(column)} = :{param_name}")
                params[param_name] = key[column]
            parts.append("(" + " AND ".join(key_parts) + ")")
        return " OR ".join(parts), params

    def _new_txn(self, branch_id: str) -> int:
        row = self._branch_row(branch_id)
        if row is None:
            raise BranchNotFoundError(branch_id)
        return int(row["head_txn_id"]) + 1

    def _set_branch_head(self, branch_id: str, txn_id: int) -> None:
        self.db.execute(
            "UPDATE _chronos_branch_log_branches SET head_txn_id = ? WHERE branch_id = ?",
            (txn_id, branch_id),
        )

    def _branch_row(self, branch_id: str) -> Any | None:
        return self.db.execute(
            "SELECT * FROM _chronos_branch_log_branches WHERE branch_id = ?",
            (branch_id,),
        ).fetchone()

    def _branch_head(self, branch_id: str) -> int:
        row = self._branch_row(branch_id)
        if row is None:
            raise BranchNotFoundError(branch_id)
        return int(row["head_txn_id"])
