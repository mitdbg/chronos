from __future__ import annotations

from chronos_core.branching._common import *

class _CopyBackend(_SQLBranchBackend):
    """Reference backend that physically copies every registered table.

    This backend is intentionally simple and expensive on branch creation. It is
    useful as a correctness baseline because query execution runs directly
    against ordinary per-branch physical tables.
    """

    name = "copy"

    def __init__(
        self,
        db: SQLDatabaseAdapter,
        enable_schema_branching: bool = False,
    ):
        super().__init__(db)
        self.enable_schema_branching = bool(enable_schema_branching)

    def ensure(self) -> None:
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_copy_branches (
              branch_id TEXT PRIMARY KEY,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_copy_checkpoints (
              checkpoint_id TEXT PRIMARY KEY,
              branch_id TEXT NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )
        if self.enable_schema_branching:
            self._ensure_schema_branching_tables()
        if (
            self.db.execute(
                "SELECT 1 FROM _chronos_branch_copy_branches WHERE branch_id = 'main'"
            ).fetchone()
            is None
        ):
            self.db.execute(
                """
                INSERT INTO _chronos_branch_copy_branches
                (branch_id, created_at, metadata)
                VALUES ('main', ?, '{}')
                """,
                (_utc_now(),),
            )
        self.db.commit()

    def _ensure_schema_branching_tables(self) -> None:
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_copy_table_bindings (
              owner_kind TEXT NOT NULL,
              owner_id TEXT NOT NULL,
              table_name TEXT NOT NULL,
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

    def register_table(self, table: str, primary_key: list[str]) -> None:
        if table in self.tables:
            if self.enable_schema_branching:
                self._record_copy_binding("branch", "main", self.tables[table], False)
            return
        columns, defs = _table_defs(self.db, table)
        missing = set(primary_key) - set(columns)
        if missing:
            raise TableNotRegisteredError(f"primary key columns missing from {table}: {missing}")
        physical = self._branch_table("main", table)
        self._create_physical_table(physical, defs, primary_key)
        cols = ", ".join(_quote(c) for c in columns)
        self.db.execute(
            f"""
            INSERT INTO {_quote(physical)} ({cols})
            SELECT {cols} FROM {_quote_table_name(table)}
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
        if self.enable_schema_branching:
            self._record_copy_binding("branch", "main", self.tables[table], False)

    def create_index(
        self, table: str, columns: list[str], name: str | None = None
    ) -> IndexInfo:
        meta, index = self._validate_index(table, columns, name)
        if index.name not in self.indexes:
            self._record_index(index)
            if self.enable_schema_branching:
                for branch in self._branch_ids():
                    branch_meta = self._meta_for_owner("branch", branch, table)
                    if branch_meta is not None:
                        self._create_physical_index(branch_meta, index, branch, checkpoint=False)
                for checkpoint in self._checkpoint_ids():
                    checkpoint_meta = self._meta_for_owner("checkpoint", checkpoint, table)
                    if checkpoint_meta is not None:
                        self._create_physical_index(checkpoint_meta, index, checkpoint, checkpoint=True)
            else:
                for branch in self._branch_ids():
                    self._create_physical_index(meta, index, branch, checkpoint=False)
                for checkpoint in self._checkpoint_ids():
                    self._create_physical_index(meta, index, checkpoint, checkpoint=True)
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
        if self._branch_row(from_branch) is None:
            raise BranchNotFoundError(from_branch)
        # Full physical copy: slow for large databases, but subsequent reads and
        # writes are normal SQL over one branch's private tables.
        source_metas = (
            self._active_metas_for_owner("branch", from_branch)
            if self.enable_schema_branching
            else self.tables
        )
        if self.enable_schema_branching:
            self._copy_tombstone_bindings("branch", from_branch, "branch", branch_id)
        for meta in source_metas.values():
            self._copy_table(
                self._branch_table(from_branch, meta.name),
                self._branch_table(branch_id, meta.name),
                meta,
            )
            if self.enable_schema_branching:
                self._record_copy_binding("branch", branch_id, meta, False)
            self._create_indexes_for_table(meta, branch_id, checkpoint=False)
        self.db.execute(
            """
            INSERT INTO _chronos_branch_copy_branches
            (branch_id, created_at, metadata)
            VALUES (?, ?, ?)
            """,
            (branch_id, _utc_now(), _json_dumps(metadata)),
        )

    def update_branch_metadata(
        self, branch_id: str, metadata: dict[str, Any]
    ) -> BranchInfo:
        if self._branch_row(branch_id) is None:
            raise BranchNotFoundError(branch_id)
        self.db.execute(
            "UPDATE _chronos_branch_copy_branches SET metadata = ? WHERE branch_id = ?",
            (_json_dumps(metadata), branch_id),
        )
        return self.get_branch(branch_id)

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
        if self._branch_row(branch_id) is not None:
            raise BranchAlreadyExistsError(branch_id)
        if self._checkpoint_row(checkpoint) is None:
            raise BranchNotFoundError(f"checkpoint:{checkpoint}")
        source_metas = (
            self._active_metas_for_owner("checkpoint", checkpoint)
            if self.enable_schema_branching
            else self.tables
        )
        if self.enable_schema_branching:
            self._copy_tombstone_bindings("checkpoint", checkpoint, "branch", branch_id)
        for meta in source_metas.values():
            self._copy_table(
                self._checkpoint_table(checkpoint, meta.name),
                self._branch_table(branch_id, meta.name),
                meta,
            )
            if self.enable_schema_branching:
                self._record_copy_binding("branch", branch_id, meta, False)
            self._create_indexes_for_table(meta, branch_id, checkpoint=False)
        self.db.execute(
            """
            INSERT INTO _chronos_branch_copy_branches
            (branch_id, created_at, metadata)
            VALUES (?, ?, ?)
            """,
            (branch_id, _utc_now(), "{}"),
        )

    def delete_branch(self, branch_id: str) -> None:
        if branch_id == "main":
            raise BranchingError("main cannot be deleted")
        if self._branch_row(branch_id) is None:
            raise BranchNotFoundError(branch_id)
        metas = (
            self._active_metas_for_owner("branch", branch_id)
            if self.enable_schema_branching
            else self.tables
        )
        for meta in metas.values():
            self.db.drop_table(self._branch_table(branch_id, meta.name))
        if self.enable_schema_branching:
            self.db.execute(
                """
                DELETE FROM _chronos_branch_copy_table_bindings
                WHERE owner_kind = 'branch' AND owner_id = ?
                """,
                (branch_id,),
            )
        self.db.execute(
            "DELETE FROM _chronos_branch_copy_branches WHERE branch_id = ?",
            (branch_id,),
        )

    def list_branches(self) -> list[BranchInfo]:
        rows = self.db.execute(
            """
            SELECT branch_id, created_at, metadata
            FROM _chronos_branch_copy_branches
            ORDER BY branch_id
            """
        ).fetchall()
        return [
            BranchInfo(
                branch_id=row["branch_id"],
                current_ref=row["branch_id"],
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
            current_ref=row["branch_id"],
            backend=self.name,
            created_at=row["created_at"],
            metadata=_json_loads(row["metadata"]),
        )

    def create_checkpoint(
        self, checkpoint: str, branch: str, metadata: dict[str, Any] | None = None
    ) -> CheckpointInfo:
        if self._checkpoint_row(checkpoint) is not None:
            raise BranchAlreadyExistsError(checkpoint)
        if self._branch_row(branch) is None:
            raise BranchNotFoundError(branch)
        source_metas = (
            self._active_metas_for_owner("branch", branch)
            if self.enable_schema_branching
            else self.tables
        )
        if self.enable_schema_branching:
            self._copy_tombstone_bindings("branch", branch, "checkpoint", checkpoint)
        for meta in source_metas.values():
            self._copy_table(
                self._branch_table(branch, meta.name),
                self._checkpoint_table(checkpoint, meta.name),
                meta,
            )
            if self.enable_schema_branching:
                self._record_copy_binding("checkpoint", checkpoint, meta, False)
            self._create_indexes_for_table(meta, checkpoint, checkpoint=True)
        now = _utc_now()
        self.db.execute(
            """
            INSERT INTO _chronos_branch_copy_checkpoints
            (checkpoint_id, branch_id, created_at, metadata)
            VALUES (?, ?, ?, ?)
            """,
            (checkpoint, branch, now, _json_dumps(metadata)),
        )
        return CheckpointInfo(checkpoint, branch, checkpoint, now, metadata or {})

    def get_checkpoint(self, checkpoint: str) -> CheckpointInfo:
        row = self._checkpoint_row(checkpoint)
        if row is None:
            raise BranchNotFoundError(f"checkpoint:{checkpoint}")
        return CheckpointInfo(
            row["checkpoint_id"],
            row["branch_id"],
            row["checkpoint_id"],
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
            SELECT checkpoint_id, branch_id, created_at, metadata
            FROM _chronos_branch_copy_checkpoints
            {where}
            ORDER BY created_at, checkpoint_id
            """,
            tuple(params),
        ).fetchall()
        infos = [
            CheckpointInfo(
                row["checkpoint_id"],
                row["branch_id"],
                row["checkpoint_id"],
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
                "replacements": self._replacements(ref),
                "tables": self._active_metas_for_ref(ref) if self.enable_schema_branching else self.tables,
                "query_rewrite_cache": {},
                "statement_plan_cache": {},
            },
        )

    def refresh_ref_after_execute(self, ref: _PreparedBranchRef) -> _PreparedBranchRef:
        if not self.enable_schema_branching:
            return ref
        return self.prepare_ref(_BranchRef(ref.branch_id, ref.ref, ref.readonly))

    def query(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if self.enable_schema_branching:
            self._validate_query_tables_visible(ref, sql)
        rewrite_cache = ref.metadata.setdefault("query_rewrite_cache", {})
        rewritten = rewrite_cache.get(sql)
        if rewritten is None:
            rewritten = _rewrite_tables(sql, self._prepared_replacements(ref), self.db.dialect)
            rewrite_cache[sql] = rewritten
        rows = self.db.execute(rewritten, params).fetchall()
        return [dict(row) for row in rows]

    def execute(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> ExecuteResult:
        if ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
        if self._is_schema_statement(sql):
            tree = sqlglot.parse_one(sql, read=self.db.dialect)
            if not self.enable_schema_branching:
                raise UnsupportedSQLError("branch-local schema changes are disabled")
            if not isinstance(tree, (exp.Create, exp.Alter, exp.Drop)):
                raise UnsupportedSQLError("unsupported schema statement")
            return self._execute_schema_ddl(ref, tree)
        if self.enable_schema_branching:
            self._validate_query_tables_visible(ref, sql)
        plan_cache = ref.metadata.setdefault("statement_plan_cache", {})
        cached = plan_cache.get(sql)
        if cached is None:
            tree = sqlglot.parse_one(sql, read=self.db.dialect)
            if isinstance(tree, exp.Update):
                plan = _build_update_plan(tree, self.db.dialect)
            elif isinstance(tree, exp.Insert):
                plan = _build_insert_plan(tree)
            else:
                plan = None
            rewritten = _rewrite_tree_tables(
                tree, self._prepared_replacements(ref), self.db.dialect
            )
            cached = (plan, rewritten)
            plan_cache[sql] = cached
        plan, rewritten = cached
        try:
            cur = self.db.execute(rewritten, params)
        except Exception as exc:
            if isinstance(plan, _InsertPlan) and self._is_duplicate_key_error(exc):
                raise DuplicateKeyError(f"duplicate key on branch {ref.branch_id}") from exc
            raise
        return ExecuteResult(max(int(getattr(cur, "rowcount", 0)), 0))

    def visible_rows(self, branch_id: str, table: str) -> list[dict[str, Any]]:
        ref = self.prepare_ref(_BranchRef(branch_id, branch_id))
        return self.query(ref, f"SELECT * FROM {_quote_table_name(table)}", {})

    def diff_tables(self) -> list[str]:
        if not self.enable_schema_branching:
            return super().diff_tables()
        return sorted(self._known_copy_tables())

    def table_meta_for_branch(self, branch_id: str, table: str) -> _TableMeta:
        if not self.enable_schema_branching:
            return super().table_meta_for_branch(branch_id, table)
        meta = self._meta_for_owner("branch", branch_id, table)
        if meta is None:
            raise TableNotRegisteredError(table)
        return meta

    def upsert_row(self, branch_id: str, table: str, row: dict[str, Any]) -> None:
        meta = (
            self._meta_for_owner("branch", branch_id, table)
            if self.enable_schema_branching
            else self._require_table(table)
        )
        if meta is None:
            raise TableNotRegisteredError(table)
        key = {column: row[column] for column in meta.pk_columns}
        physical = self._branch_table(branch_id, table)
        if self._visible_row(branch_id, table, key) is None:
            cols = ", ".join(_quote(c) for c in meta.columns)
            self.db.execute(
                f"""
                INSERT INTO {_quote(physical)} ({cols})
                VALUES ({_placeholders(len(meta.columns))})
                """,
                [row.get(column) for column in meta.columns],
            )
            return
        set_cols = [column for column in meta.columns if column not in meta.pk_columns]
        set_sql = ", ".join(f"{_quote(column)} = ?" for column in set_cols)
        self.db.execute(
            f"""
            UPDATE {_quote(physical)}
            SET {set_sql}
            WHERE {self._key_where(meta)}
            """,
            [*[row.get(column) for column in set_cols], *self._key_values(meta, key)],
        )

    def delete_key(self, branch_id: str, table: str, key: dict[str, Any]) -> None:
        meta = (
            self._meta_for_owner("branch", branch_id, table)
            if self.enable_schema_branching
            else self._require_table(table)
        )
        if meta is None:
            raise TableNotRegisteredError(table)
        self.db.execute(
            f"""
            DELETE FROM {_quote(self._branch_table(branch_id, table))}
            WHERE {self._key_where(meta)}
            """,
            self._key_values(meta, key),
        )

    def _visible_row(
        self, branch_id: str, table: str, key: dict[str, Any]
    ) -> dict[str, Any] | None:
        meta = (
            self._meta_for_owner("branch", branch_id, table)
            if self.enable_schema_branching
            else self._require_table(table)
        )
        if meta is None:
            raise TableNotRegisteredError(table)
        row = self.db.execute(
            f"""
            SELECT *
            FROM {_quote(self._branch_table(branch_id, table))}
            WHERE {self._key_where(meta)}
            """,
            self._key_values(meta, key),
        ).fetchone()
        return dict(row) if row is not None else None

    def _replacements(self, ref: _BranchRef) -> dict[str, str]:
        if self.enable_schema_branching:
            return {
                table: meta.physical_name
                for table, meta in self._active_metas_for_ref(ref).items()
            }
        if ref.readonly:
            return {
                table: self._checkpoint_table(ref.ref, table)
                for table in self.tables
            }
        if self._branch_row(ref.branch_id) is None:
            raise BranchNotFoundError(ref.branch_id)
        return {table: self._branch_table(ref.branch_id, table) for table in self.tables}

    def _prepared_replacements(self, ref: _PreparedBranchRef) -> dict[str, str]:
        replacements = ref.metadata.get("replacements")
        if isinstance(replacements, dict):
            return replacements
        return self._replacements(_BranchRef(ref.branch_id, ref.ref, ref.readonly))

    def _active_metas_for_ref(self, ref: _BranchRef | _PreparedBranchRef) -> dict[str, _TableMeta]:
        owner_kind = "checkpoint" if ref.readonly else "branch"
        owner_id = ref.ref if ref.readonly else ref.branch_id
        return self._active_metas_for_owner(owner_kind, owner_id)

    def _active_metas_for_owner(self, owner_kind: str, owner_id: str) -> dict[str, _TableMeta]:
        if not self.enable_schema_branching:
            return dict(self.tables)
        self._ensure_schema_branching_tables()
        metas: dict[str, _TableMeta] = {}
        rows = self.db.execute(
            """
            SELECT *
            FROM _chronos_branch_copy_table_bindings
            WHERE owner_kind = ? AND owner_id = ?
            """,
            (owner_kind, owner_id),
        ).fetchall()
        for row in rows:
            if bool(row["tombstone"]):
                continue
            metas[row["table_name"]] = self._meta_from_binding(row, owner_kind, owner_id)
        if rows:
            return metas
        return dict(self.tables)

    def _meta_for_owner(
        self, owner_kind: str, owner_id: str, table: str
    ) -> _TableMeta | None:
        if not self.enable_schema_branching:
            return self.tables.get(table)
        row = self.db.execute(
            """
            SELECT *
            FROM _chronos_branch_copy_table_bindings
            WHERE owner_kind = ? AND owner_id = ? AND table_name = ?
            """,
            (owner_kind, owner_id, table),
        ).fetchone()
        if row is None:
            return self.tables.get(table)
        if bool(row["tombstone"]):
            return None
        return self._meta_from_binding(row, owner_kind, owner_id)

    def _meta_from_binding(
        self, row: Any, owner_kind: str, owner_id: str
    ) -> _TableMeta:
        table = row["table_name"]
        physical = (
            self._checkpoint_table(owner_id, table)
            if owner_kind == "checkpoint"
            else self._branch_table(owner_id, table)
        )
        return _TableMeta(
            name=table,
            physical_name=physical,
            pk_columns=tuple(json.loads(row["pk_columns"])),
            columns=tuple(json.loads(row["columns"])),
            column_defs=tuple(json.loads(row["column_defs"])),
            backend=self.name,
        )

    def _record_copy_binding(
        self,
        owner_kind: str,
        owner_id: str,
        meta: _TableMeta,
        tombstone: bool,
    ) -> None:
        self._ensure_schema_branching_tables()
        self.db.execute(
            """
            INSERT INTO _chronos_branch_copy_table_bindings
            (owner_kind, owner_id, table_name, pk_columns, columns, column_defs,
             tombstone, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(owner_kind, owner_id, table_name) DO UPDATE SET
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
                json.dumps(meta.pk_columns),
                json.dumps(meta.columns),
                json.dumps(meta.column_defs),
                1 if tombstone else 0,
                _utc_now(),
                "{}",
            ),
        )

    def _record_copy_tombstone(
        self, owner_kind: str, owner_id: str, table: str
    ) -> None:
        self._ensure_schema_branching_tables()
        self.db.execute(
            """
            INSERT INTO _chronos_branch_copy_table_bindings
            (owner_kind, owner_id, table_name, pk_columns, columns, column_defs,
             tombstone, created_at, metadata)
            VALUES (?, ?, ?, NULL, NULL, NULL, 1, ?, ?)
            ON CONFLICT(owner_kind, owner_id, table_name) DO UPDATE SET
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
            FROM _chronos_branch_copy_table_bindings
            WHERE owner_kind = ? AND owner_id = ? AND tombstone = 1
            """,
            (source_kind, source_id),
        ).fetchall():
            self._record_copy_tombstone(dest_kind, dest_id, row["table_name"])

    def _known_copy_tables(self) -> set[str]:
        known = set(self.tables)
        if not self.enable_schema_branching:
            return known
        self._ensure_schema_branching_tables()
        for row in self.db.execute(
            "SELECT DISTINCT table_name FROM _chronos_branch_copy_table_bindings"
        ).fetchall():
            known.add(row["table_name"])
        return known

    def _validate_query_tables_visible(self, ref: _PreparedBranchRef, sql: str) -> None:
        active = set(self._active_metas_for_ref(ref))
        known = self._known_copy_tables()
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
            physical_name=self._branch_table(ref.branch_id, table),
            pk_columns=tuple(pk_columns),
            columns=tuple(columns),
            column_defs=tuple(defs),
            backend=self.name,
        )
        self._create_physical_table(meta.physical_name, meta.column_defs, list(meta.pk_columns))
        self._record_copy_binding("branch", ref.branch_id, meta, False)
        self._create_indexes_for_table(meta, ref.branch_id, checkpoint=False)
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
        if isinstance(action, exp.AlterColumn):
            return self._execute_alter_table_type_ddl(ref, table, old_meta, action)
        if isinstance(action, exp.Drop):
            return self._execute_alter_table_drop_column_ddl(ref, table, old_meta, action)
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
        self.db.execute(
            f"ALTER TABLE {_quote(old_meta.physical_name)} ADD COLUMN {new_def}"
        )
        meta = _TableMeta(
            name=table,
            physical_name=old_meta.physical_name,
            pk_columns=old_meta.pk_columns,
            columns=tuple([*old_meta.columns, column]),
            column_defs=tuple([*old_meta.column_defs, new_def]),
            backend=self.name,
        )
        self._record_copy_binding("branch", ref.branch_id, meta, False)
        return ExecuteResult(0)

    def _execute_alter_table_drop_column_ddl(
        self,
        ref: _PreparedBranchRef,
        table: str,
        old_meta: _TableMeta,
        action: exp.Drop,
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
        meta = _TableMeta(
            name=table,
            physical_name=old_meta.physical_name,
            pk_columns=old_meta.pk_columns,
            columns=tuple(old_column for old_column, _old_def in kept),
            column_defs=tuple(old_def for _old_column, old_def in kept),
            backend=self.name,
        )
        replacement_physical = f"{old_meta.physical_name}_schema_{uuid.uuid4().hex[:8]}"
        self._create_physical_table(replacement_physical, meta.column_defs, list(meta.pk_columns))
        self._copy_table_rows(meta, replacement_physical)
        self.db.drop_table(old_meta.physical_name)
        self.db.execute(
            f"ALTER TABLE {_quote(replacement_physical)} RENAME TO {_quote(old_meta.physical_name)}"
        )
        self._record_copy_binding("branch", ref.branch_id, meta, False)
        self._create_indexes_for_table(meta, ref.branch_id, checkpoint=False)
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
        meta = _TableMeta(
            name=table,
            physical_name=old_meta.physical_name,
            pk_columns=old_meta.pk_columns,
            columns=old_meta.columns,
            column_defs=tuple(defs),
            backend=self.name,
        )
        using = action.args.get("using")
        if isinstance(using, exp.Expression):
            select_sql = using.sql(dialect=self.db.dialect)
        else:
            select_sql = f"CAST({_quote(column)} AS {type_sql})"
        replacement_physical = f"{old_meta.physical_name}_schema_{uuid.uuid4().hex[:8]}"
        self._create_physical_table(replacement_physical, meta.column_defs, list(meta.pk_columns))
        self._copy_table_rows(
            old_meta,
            replacement_physical,
            select_sql_by_column={column: select_sql},
        )
        self.db.drop_table(old_meta.physical_name)
        self.db.execute(
            f"ALTER TABLE {_quote(replacement_physical)} RENAME TO {_quote(old_meta.physical_name)}"
        )
        self._record_copy_binding("branch", ref.branch_id, meta, False)
        self._create_indexes_for_table(meta, ref.branch_id, checkpoint=False)
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
        self.db.drop_table(self._branch_table(ref.branch_id, table))
        self._record_copy_tombstone("branch", ref.branch_id, table)
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
                default_sql = self._constant_default_sql(kind.this)
                continue
            raise UnsupportedSQLError(
                "column constraints other than inline primary key and constant DEFAULT are not supported"
            )
        kind = column_def.args.get("kind")
        type_sql = kind.sql(dialect=self.db.dialect) if isinstance(kind, exp.Expression) else "TEXT"
        default_clause = f" DEFAULT {default_sql}" if default_sql is not None else ""
        return f"{_quote(self._column_def_name(column_def))} {type_sql}{default_clause}"

    def _constant_default_sql(self, expression: exp.Expression) -> str:
        try:
            _expr_value(expression, {})
        except UnsupportedSQLError as exc:
            raise UnsupportedSQLError("only constant DEFAULT expressions are supported") from exc
        return expression.sql(dialect=self.db.dialect)

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

    def _is_duplicate_key_error(self, exc: Exception) -> bool:
        names = {cls.__name__ for cls in type(exc).mro()}
        if names.intersection({"IntegrityError", "UniqueViolation"}):
            return True
        message = str(exc).lower()
        return "duplicate key" in message or "unique constraint" in message

    def _copy_table(self, source: str, dest: str, meta: _TableMeta) -> None:
        self._create_physical_table(dest, meta.column_defs, list(meta.pk_columns))
        self._copy_table_rows(meta, dest, source_physical=source)

    def _copy_table_rows(
        self,
        meta: _TableMeta,
        dest: str,
        *,
        source_physical: str | None = None,
        select_sql_by_column: dict[str, str] | None = None,
    ) -> None:
        source_physical = source_physical or meta.physical_name
        select_sql_by_column = select_sql_by_column or {}
        cols = ", ".join(_quote(c) for c in meta.columns)
        select_cols = ", ".join(
            (
                f"{select_sql_by_column[column]} AS {_quote(column)}"
                if column in select_sql_by_column
                else _quote(column)
            )
            for column in meta.columns
        )
        self.db.execute(
            f"""
            INSERT INTO {_quote(dest)} ({cols})
            SELECT {select_cols} FROM {_quote(source_physical)}
            """
        )

    def _create_physical_table(
        self, physical: str, defs: tuple[str, ...], primary_key: list[str]
    ) -> None:
        pk_sql = ", ".join(_quote(c) for c in primary_key)
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_quote(physical)} (
              {", ".join(defs)},
              PRIMARY KEY ({pk_sql})
            )
            """
        )

    def _create_indexes_for_table(
        self, meta: _TableMeta, owner_id: str, checkpoint: bool
    ) -> None:
        for index in self.indexes.values():
            if index.table == meta.name:
                self._create_physical_index(meta, index, owner_id, checkpoint)

    def _create_physical_index(
        self, meta: _TableMeta, index: _IndexMeta, owner_id: str, checkpoint: bool
    ) -> None:
        if set(index.columns) - set(meta.columns):
            return
        physical = (
            self._checkpoint_table(owner_id, meta.name)
            if checkpoint
            else self._branch_table(owner_id, meta.name)
        )
        index_name = (
            f"_chronos_idx_copy_cp_{_identifier_token(owner_id)}_{_identifier_token(index.name)}"
            if checkpoint
            else f"_chronos_idx_copy_{_identifier_token(owner_id)}_{_identifier_token(index.name)}"
        )
        self.db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {_quote(index_name)}
            ON {_quote(physical)}
            ({", ".join(_quote(c) for c in index.columns)})
            """
        )

    def _branch_table(self, branch_id: str, table: str) -> str:
        return f"_chronos_b_copy_{_identifier_token(branch_id)}_{_identifier_token(table)}"

    def _checkpoint_table(self, checkpoint: str, table: str) -> str:
        return f"_chronos_b_copy_cp_{_identifier_token(checkpoint)}_{_identifier_token(table)}"

    def _branch_row(self, branch_id: str) -> Any | None:
        return self.db.execute(
            "SELECT * FROM _chronos_branch_copy_branches WHERE branch_id = ?",
            (branch_id,),
        ).fetchone()

    def _checkpoint_row(self, checkpoint: str) -> Any | None:
        return self.db.execute(
            "SELECT * FROM _chronos_branch_copy_checkpoints WHERE checkpoint_id = ?",
            (checkpoint,),
        ).fetchone()

    def _branch_ids(self) -> list[str]:
        return [
            row["branch_id"]
            for row in self.db.execute(
                "SELECT branch_id FROM _chronos_branch_copy_branches"
            ).fetchall()
        ]

    def _checkpoint_ids(self) -> list[str]:
        return [
            row["checkpoint_id"]
            for row in self.db.execute(
                "SELECT checkpoint_id FROM _chronos_branch_copy_checkpoints"
            ).fetchall()
        ]
