from __future__ import annotations

import sqlite3

from chronos_core.branching._common import *


class _LiteTreeBackend(_SQLBranchBackend):
    """Native LiteTree backend.

    LiteTree is a modified SQLite library that versions the whole database with
    branch-aware PRAGMAs. This backend exposes that native machinery through the
    Chronos branch API. It requires the process to be linked against LiteTree's
    SQLite library and the database to be opened with ``branches=on``.
    """

    name = "litetree"

    def ensure(self) -> None:
        if self.db.dialect != "sqlite":
            raise BranchingError("LiteTree backend requires a SQLite/LiteTree database")
        mode = self.db.execute("PRAGMA journal_mode").fetchone()
        if mode is None or str(mode[0]).lower() != "branches":
            raise BranchingError(
                "LiteTree backend requires aergoio/litetree SQLite with "
                "journal_mode=branches; open the DB as a SQLite URI with branches=on"
            )
        self._ensure_metadata_store()
        if "main" not in self._native_branches():
            self.db.execute("PRAGMA new_branch=main at master")
        if self._branch_row("main") is None:
            self._metadata_execute(
                """
                INSERT INTO _chronos_branch_litetree_branches
                (branch_id, created_at, metadata)
                VALUES ('main', ?, '{}')
                """,
                (_utc_now(),),
            )
        self.db.commit()
        self._set_native_ref("main")

    def register_table(self, table: str, primary_key: list[str]) -> None:
        if table in self.tables:
            return
        columns, defs = _table_defs(self.db, table)
        missing = set(primary_key) - set(columns)
        if missing:
            raise TableNotRegisteredError(f"primary key columns missing from {table}: {missing}")
        self.db.execute(
            """
            INSERT INTO _chronos_branch_tables
            (table_name, physical_table, pk_columns, columns, column_defs, backend)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                table,
                table,
                json.dumps(primary_key),
                json.dumps(columns),
                json.dumps(defs),
                self.name,
            ),
        )
        self.tables[table] = _TableMeta(
            name=table,
            physical_name=table,
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
            self._set_native_ref("main")
            self.db.execute(
                f"""
                CREATE INDEX {_quote(index.name)}
                ON {_quote_table_name(meta.name)}
                ({", ".join(_quote(column) for column in index.columns)})
                """
            )
            self._record_index(index)
        return IndexInfo(index.name, index.table, index.columns, index.backend)

    def create_branch(
        self, branch_id: str, from_branch: str, metadata: dict[str, Any] | None = None
    ) -> None:
        self._validate_branch_name(branch_id)
        if branch_id in self._native_branches():
            raise BranchAlreadyExistsError(branch_id)
        if from_branch not in self._native_branches():
            raise BranchNotFoundError(from_branch)
        source_ref = self.get_branch(from_branch).current_ref
        self.db.execute(f"PRAGMA new_branch={branch_id} at {source_ref}")
        self._metadata_execute(
            """
            INSERT INTO _chronos_branch_litetree_branches
            (branch_id, created_at, metadata)
            VALUES (?, ?, ?)
            """,
            (branch_id, _utc_now(), _json_dumps(metadata)),
        )

    def update_branch_metadata(
        self, branch_id: str, metadata: dict[str, Any]
    ) -> BranchInfo:
        if branch_id not in self._native_branches():
            raise BranchNotFoundError(branch_id)
        self._metadata_execute(
            """
            UPDATE _chronos_branch_litetree_branches
               SET metadata = ?
             WHERE branch_id = ?
            """,
            (_json_dumps(metadata), branch_id),
        )
        return self.get_branch(branch_id)

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
        self._validate_branch_name(branch_id)
        if branch_id in self._native_branches():
            raise BranchAlreadyExistsError(branch_id)
        cp = self.get_checkpoint(checkpoint)
        self.db.execute(f"PRAGMA new_branch={branch_id} at {cp.ref}")
        self._metadata_execute(
            """
            INSERT INTO _chronos_branch_litetree_branches
            (branch_id, created_at, metadata)
            VALUES (?, ?, '{}')
            """,
            (branch_id, _utc_now()),
        )

    def delete_branch(self, branch_id: str) -> None:
        if branch_id == "main":
            raise BranchingError("main cannot be deleted")
        if branch_id not in self._native_branches():
            raise BranchNotFoundError(branch_id)
        self._set_native_ref("main")
        self.db.execute(f"PRAGMA del_branch({branch_id})")
        self._metadata_execute(
            "DELETE FROM _chronos_branch_litetree_branches WHERE branch_id = ?",
            (branch_id,),
        )
        self._metadata_execute(
            "DELETE FROM _chronos_branch_litetree_checkpoints WHERE branch_id = ?",
            (branch_id,),
        )

    def list_branches(self) -> list[BranchInfo]:
        infos = []
        for branch_id in sorted(self._native_branches() - {"master"}):
            row = self._branch_row(branch_id)
            infos.append(
                BranchInfo(
                    branch_id=branch_id,
                    current_ref=self._head_ref(branch_id),
                    backend=self.name,
                    created_at=row["created_at"] if row is not None else "",
                    metadata=_json_loads(row["metadata"] if row is not None else "{}"),
                )
            )
        return infos

    def get_branch(self, branch_id: str) -> BranchInfo:
        if branch_id not in self._native_branches():
            raise BranchNotFoundError(branch_id)
        row = self._branch_row(branch_id)
        return BranchInfo(
            branch_id=branch_id,
            current_ref=self._head_ref(branch_id),
            backend=self.name,
            created_at=row["created_at"] if row is not None else "",
            metadata=_json_loads(row["metadata"] if row is not None else "{}"),
        )

    def create_checkpoint(
        self, checkpoint: str, branch: str, metadata: dict[str, Any] | None = None
    ) -> CheckpointInfo:
        if self._checkpoint_row(checkpoint) is not None:
            raise BranchAlreadyExistsError(checkpoint)
        if branch not in self._native_branches():
            raise BranchNotFoundError(branch)
        ref = self._head_ref(branch)
        now = _utc_now()
        self._metadata_execute(
            """
            INSERT INTO _chronos_branch_litetree_checkpoints
            (checkpoint_id, branch_id, ref, created_at, metadata)
            VALUES (?, ?, ?, ?, ?)
            """,
            (checkpoint, branch, ref, now, _json_dumps(metadata)),
        )
        return CheckpointInfo(checkpoint, branch, ref, now, metadata or {})

    def get_checkpoint(self, checkpoint: str) -> CheckpointInfo:
        row = self._checkpoint_row(checkpoint)
        if row is not None:
            return CheckpointInfo(
                row["checkpoint_id"],
                row["branch_id"],
                row["ref"],
                row["created_at"],
                _json_loads(row["metadata"]),
            )
        raise BranchNotFoundError(f"checkpoint:{checkpoint}")

    def list_checkpoints(
        self,
        branch: str | None = None,
        metadata_filter: dict[str, Any] | None = None,
    ) -> list[CheckpointInfo]:
        branch_ids = [branch] if branch is not None else sorted(
            self._native_branches() - {"master"}
        )
        native = self._native_branches()
        for branch_id in branch_ids:
            if branch_id not in native:
                raise BranchNotFoundError(branch_id)
        where = ""
        params: list[Any] = []
        if branch is not None:
            where = "WHERE branch_id = ?"
            params.append(branch)
        rows = self._metadata_fetchall(
            f"""
            SELECT checkpoint_id, branch_id, ref, created_at, metadata
            FROM _chronos_branch_litetree_checkpoints
            {where}
            ORDER BY created_at, checkpoint_id
            """,
            tuple(params),
        )
        infos = [
            CheckpointInfo(
                row["checkpoint_id"],
                row["branch_id"],
                row["ref"],
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
        self._set_native_ref(ref.ref if ref.readonly else ref.branch_id)
        return _PreparedBranchRef(ref.branch_id, ref.ref, ref.readonly)

    def prepare_transaction(self, ref: _PreparedBranchRef) -> None:
        self._set_native_ref(ref.ref if ref.readonly else ref.branch_id)

    def query(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        self._set_native_ref_for_statement(ref.ref if ref.readonly else ref.branch_id)
        rows = self.db.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def execute(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> ExecuteResult:
        if ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
        self._set_native_ref_for_statement(ref.branch_id)
        try:
            cur = self.db.execute(sql, params)
        except sqlite3.IntegrityError as exc:
            raise DuplicateKeyError(str(exc)) from exc
        return ExecuteResult(max(int(getattr(cur, "rowcount", 0)), 0))

    def visible_rows(self, branch_id: str, table: str) -> list[dict[str, Any]]:
        return self.query(_PreparedBranchRef(branch_id, branch_id), f"SELECT * FROM {_quote_table_name(table)}", {})

    def upsert_row(self, branch_id: str, table: str, row: dict[str, Any]) -> None:
        meta = self._require_table(table)
        key = self._row_key(meta, row)
        self._set_native_ref_for_statement(branch_id)
        if self._visible_row(table, key) is None:
            cols = ", ".join(_quote(column) for column in meta.columns)
            self.db.execute(
                f"""
                INSERT INTO {_quote_table_name(table)} ({cols})
                VALUES ({_placeholders(len(meta.columns))})
                """,
                [row.get(column) for column in meta.columns],
            )
            return
        set_cols = [column for column in meta.columns if column not in meta.pk_columns]
        set_sql = ", ".join(f"{_quote(column)} = ?" for column in set_cols)
        self.db.execute(
            f"""
            UPDATE {_quote_table_name(table)}
            SET {set_sql}
            WHERE {self._key_where(meta)}
            """,
            [*[row.get(column) for column in set_cols], *self._key_values(meta, key)],
        )

    def delete_key(self, branch_id: str, table: str, key: dict[str, Any]) -> None:
        self._set_native_ref_for_statement(branch_id)
        meta = self._require_table(table)
        self.db.execute(
            f"DELETE FROM {_quote_table_name(table)} WHERE {self._key_where(meta)}",
            self._key_values(meta, key),
        )

    def _visible_row(self, table: str, key: dict[str, Any]) -> dict[str, Any] | None:
        meta = self._require_table(table)
        row = self.db.execute(
            f"SELECT * FROM {_quote_table_name(table)} WHERE {self._key_where(meta)}",
            self._key_values(meta, key),
        ).fetchone()
        return dict(row) if row is not None else None

    def _head_ref(self, branch_id: str) -> str:
        row = self.db.execute(f"PRAGMA branch_info({branch_id})").fetchone()
        if row is None:
            raise BranchNotFoundError(branch_id)
        info = _json_loads(row[0])
        return f"{branch_id}.{info.get('total_commits', '*')}"

    def _set_native_ref(self, ref: str) -> None:
        self.db.execute(f"PRAGMA branch={ref}")

    def _set_native_ref_for_statement(self, ref: str) -> None:
        if not self.db.in_transaction:
            self._set_native_ref(ref)

    def _native_branches(self) -> set[str]:
        return {str(row[0]) for row in self.db.execute("PRAGMA branches").fetchall()}

    def _branch_row(self, branch_id: str) -> Any | None:
        return self._metadata_fetchone(
            "SELECT * FROM _chronos_branch_litetree_branches WHERE branch_id = ?",
            (branch_id,),
        )

    def _checkpoint_row(self, checkpoint: str) -> Any | None:
        return self._metadata_fetchone(
            """
            SELECT *
            FROM _chronos_branch_litetree_checkpoints
            WHERE checkpoint_id = ?
            """,
            (checkpoint,),
        )

    def _validate_branch_name(self, branch_id: str) -> None:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", branch_id):
            raise BranchingError(f"unsupported LiteTree branch name: {branch_id}")

    def _ensure_metadata_store(self) -> None:
        self._metadata_execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_litetree_branches (
              branch_id TEXT PRIMARY KEY,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )
        self._metadata_execute(
            """
            CREATE TABLE IF NOT EXISTS _chronos_branch_litetree_checkpoints (
              checkpoint_id TEXT PRIMARY KEY,
              branch_id TEXT NOT NULL,
              ref TEXT NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )

    def _metadata_path(self) -> str:
        database_path = getattr(self.db, "database_path", "")
        if not database_path or database_path == ":memory:":
            raise BranchingError("LiteTree metadata requires a file-backed database")
        return f"{database_path}.chronos-meta"

    def _metadata_execute(
        self, sql: str, params: tuple[Any, ...] = ()
    ) -> None:
        with sqlite3.connect(self._metadata_path()) as conn:
            conn.execute(sql, params)
            conn.commit()

    def _metadata_fetchone(
        self, sql: str, params: tuple[Any, ...] = ()
    ) -> dict[str, Any] | None:
        with sqlite3.connect(self._metadata_path()) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(sql, params).fetchone()
            return dict(row) if row is not None else None

    def _metadata_fetchall(
        self, sql: str, params: tuple[Any, ...] = ()
    ) -> list[dict[str, Any]]:
        with sqlite3.connect(self._metadata_path()) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(sql, params).fetchall()]
