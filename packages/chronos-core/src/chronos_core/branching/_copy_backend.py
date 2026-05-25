from __future__ import annotations

from chronos_core.branching._common import *

class _CopyBackend(_SQLBranchBackend):
    """Reference backend that physically copies every registered table.

    This backend is intentionally simple and expensive on branch creation. It is
    useful as a correctness baseline because query execution runs directly
    against ordinary per-branch physical tables.
    """

    name = "copy"

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

    def register_table(self, table: str, primary_key: list[str]) -> None:
        if table in self.tables:
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
            SELECT {cols} FROM {_quote(table)}
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
            self._record_index(index)
            for branch in self._branch_ids():
                self._create_physical_index(meta, index, branch, checkpoint=False)
            for checkpoint in self._checkpoint_ids():
                self._create_physical_index(meta, index, checkpoint, checkpoint=True)
        return IndexInfo(index.name, index.table, index.columns, index.backend)

    def create_branch(self, branch_id: str, from_branch: str) -> None:
        if self._branch_row(branch_id) is not None:
            raise BranchAlreadyExistsError(branch_id)
        if self._branch_row(from_branch) is None:
            raise BranchNotFoundError(from_branch)
        # Full physical copy: slow for large databases, but subsequent reads and
        # writes are normal SQL over one branch's private tables.
        for meta in self.tables.values():
            self._copy_table(
                self._branch_table(from_branch, meta.name),
                self._branch_table(branch_id, meta.name),
                meta,
            )
            self._create_indexes_for_table(meta, branch_id, checkpoint=False)
        self.db.execute(
            """
            INSERT INTO _chronos_branch_copy_branches
            (branch_id, created_at, metadata)
            VALUES (?, ?, ?)
            """,
            (branch_id, _utc_now(), "{}"),
        )

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
        if self._branch_row(branch_id) is not None:
            raise BranchAlreadyExistsError(branch_id)
        if self._checkpoint_row(checkpoint) is None:
            raise BranchNotFoundError(f"checkpoint:{checkpoint}")
        for meta in self.tables.values():
            self._copy_table(
                self._checkpoint_table(checkpoint, meta.name),
                self._branch_table(branch_id, meta.name),
                meta,
            )
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
        for meta in self.tables.values():
            self.db.drop_table(self._branch_table(branch_id, meta.name))
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

    def create_checkpoint(self, checkpoint: str, branch: str) -> CheckpointInfo:
        if self._checkpoint_row(checkpoint) is not None:
            raise BranchAlreadyExistsError(checkpoint)
        if self._branch_row(branch) is None:
            raise BranchNotFoundError(branch)
        for meta in self.tables.values():
            self._copy_table(
                self._branch_table(branch, meta.name),
                self._checkpoint_table(checkpoint, meta.name),
                meta,
            )
            self._create_indexes_for_table(meta, checkpoint, checkpoint=True)
        now = _utc_now()
        self.db.execute(
            """
            INSERT INTO _chronos_branch_copy_checkpoints
            (checkpoint_id, branch_id, created_at, metadata)
            VALUES (?, ?, ?, ?)
            """,
            (checkpoint, branch, now, "{}"),
        )
        return CheckpointInfo(checkpoint, branch, checkpoint, now)

    def checkout_checkpoint(self, checkpoint: str) -> _BranchRef:
        row = self._checkpoint_row(checkpoint)
        if row is None:
            raise BranchNotFoundError(f"checkpoint:{checkpoint}")
        return _BranchRef(row["branch_id"], checkpoint, readonly=True)

    def prepare_ref(self, ref: _BranchRef) -> _PreparedBranchRef:
        return _PreparedBranchRef(
            ref.branch_id,
            ref.ref,
            ref.readonly,
            {
                "replacements": self._replacements(ref),
                "query_rewrite_cache": {},
                "statement_plan_cache": {},
            },
        )

    def query(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
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
        return self.query(ref, f"SELECT * FROM {_quote(table)}", {})

    def upsert_row(self, branch_id: str, table: str, row: dict[str, Any]) -> None:
        meta = self._require_table(table)
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
        meta = self._require_table(table)
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
        meta = self._require_table(table)
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

    def _is_duplicate_key_error(self, exc: Exception) -> bool:
        names = {cls.__name__ for cls in type(exc).mro()}
        if names.intersection({"IntegrityError", "UniqueViolation"}):
            return True
        message = str(exc).lower()
        return "duplicate key" in message or "unique constraint" in message

    def _copy_table(self, source: str, dest: str, meta: _TableMeta) -> None:
        self._create_physical_table(dest, meta.column_defs, list(meta.pk_columns))
        cols = ", ".join(_quote(c) for c in meta.columns)
        self.db.execute(
            f"""
            INSERT INTO {_quote(dest)} ({cols})
            SELECT {cols} FROM {_quote(source)}
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
            CREATE INDEX {_quote(index_name)}
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
