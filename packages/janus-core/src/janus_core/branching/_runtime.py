from __future__ import annotations

import contextlib
import hashlib
import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator, Literal

import sqlglot
from sqlglot import exp

from janus_core.branching.sql_adapters import SQLDatabaseAdapter, connect_sql_database

BranchBackendName = Literal["interval", "log", "copy"]

# Interval backends assign branches/subtrees numeric visibility ranges. This is
# intentionally below SQLite's signed 64-bit maximum so midpoint arithmetic has
# headroom and remains portable across common SQL engines.
_MAX_INTERVAL = 9_000_000_000_000_000_000
_META_PREFIX = "_janus_branch_"


class BranchingError(Exception):
    """Base error for branch-layer failures."""


class BranchNotFoundError(BranchingError):
    """Raised when a requested branch does not exist."""


class BranchAlreadyExistsError(BranchingError):
    """Raised when creating a duplicate branch."""


class TableNotRegisteredError(BranchingError):
    """Raised when SQL targets an unregistered branchable table."""


class UnsupportedSQLError(BranchingError):
    """Raised when a write statement is outside the supported SQL subset."""


class DuplicateKeyError(BranchingError):
    """Raised when a branch-local insert violates a logical primary key."""


@dataclass(frozen=True)
class ExecuteResult:
    rowcount: int


@dataclass(frozen=True)
class BranchInfo:
    branch_id: str
    current_ref: str
    backend: str
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CheckpointInfo:
    checkpoint_id: str
    branch_id: str
    ref: str
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RowDiff:
    table: str
    key: dict[str, Any]
    change: Literal["added", "deleted", "modified"]
    before: dict[str, Any] | None
    after: dict[str, Any] | None


@dataclass(frozen=True)
class BranchDiff:
    left: str
    right: str
    changes: list[RowDiff]


@dataclass(frozen=True)
class MergeResolution:
    conflict_choices: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class MergePreview:
    source: str
    target: str
    changes: list[RowDiff]
    conflicts: list[RowDiff]
    resolution: MergeResolution = field(default_factory=MergeResolution)


@dataclass(frozen=True)
class MergeResult:
    source: str
    target: str
    applied: int


@dataclass(frozen=True)
class IndexInfo:
    name: str
    table: str
    columns: tuple[str, ...]
    backend: str


@dataclass(frozen=True)
class _TableMeta:
    name: str
    physical_name: str
    pk_columns: tuple[str, ...]
    columns: tuple[str, ...]
    column_defs: tuple[str, ...]
    backend: str


@dataclass(frozen=True)
class _IndexMeta:
    name: str
    table: str
    columns: tuple[str, ...]
    backend: str


@dataclass(frozen=True)
class _BranchRef:
    """Stable logical reference returned by branch/checkpoint lookup."""

    branch_id: str
    ref: str
    readonly: bool = False


@dataclass(frozen=True)
class _PreparedBranchRef:
    """Per-checkout state cached by a BranchSession.

    Backends put expensive lookup results here, such as interval segment bounds
    or log lineage. Query execution should be able to reuse this object rather
    than re-reading branch metadata for every SQL statement.
    """

    branch_id: str
    ref: str
    readonly: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class _IntervalSegment:
    """Numeric visibility range owned by a branch/checkpoint in interval mode."""

    segment_id: str
    live_lo: int
    live_hi: int
    branch_point: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(value: dict[str, Any] | None) -> str:
    return json.dumps(value or {}, sort_keys=True)


def _json_loads(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    return json.loads(value)


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _placeholders(count: int) -> str:
    return ", ".join("?" for _ in range(count))


def _default_index_name(table: str, columns: list[str]) -> str:
    return f"idx_{table}_{'_'.join(columns)}"


def _identifier_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_") or "value"
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:10]
    return f"{token[:48]}_{digest}"


def _table_defs(db: SQLDatabaseAdapter, table: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    columns, defs = db.table_defs(table)
    if not columns:
        raise TableNotRegisteredError(f"table does not exist: {table}")
    return columns, defs


def _parse_table_registry(db: SQLDatabaseAdapter, backend: str) -> dict[str, _TableMeta]:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS _janus_branch_tables (
          table_name TEXT PRIMARY KEY,
          physical_table TEXT NOT NULL,
          pk_columns TEXT NOT NULL,
          columns TEXT NOT NULL,
          column_defs TEXT NOT NULL,
          backend TEXT NOT NULL
        )
        """
    )
    tables: dict[str, _TableMeta] = {}
    for row in db.execute(
        "SELECT * FROM _janus_branch_tables WHERE backend = ?", (backend,)
    ):
        tables[row["table_name"]] = _TableMeta(
            name=row["table_name"],
            physical_name=row["physical_table"],
            pk_columns=tuple(json.loads(row["pk_columns"])),
            columns=tuple(json.loads(row["columns"])),
            column_defs=tuple(json.loads(row["column_defs"])),
            backend=row["backend"],
        )
    return tables


def _ensure_index_registry(db: SQLDatabaseAdapter) -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS _janus_branch_indexes (
          backend TEXT NOT NULL,
          index_name TEXT NOT NULL,
          table_name TEXT NOT NULL,
          columns TEXT NOT NULL,
          PRIMARY KEY (backend, index_name)
        )
        """
    )


def _parse_index_registry(db: SQLDatabaseAdapter, backend: str) -> dict[str, _IndexMeta]:
    _ensure_index_registry(db)
    indexes: dict[str, _IndexMeta] = {}
    for row in db.execute(
        "SELECT * FROM _janus_branch_indexes WHERE backend = ?", (backend,)
    ):
        indexes[row["index_name"]] = _IndexMeta(
            name=row["index_name"],
            table=row["table_name"],
            columns=tuple(json.loads(row["columns"])),
            backend=row["backend"],
        )
    return indexes


def _ensure_params(params: dict[str, Any] | None) -> dict[str, Any]:
    return dict(params or {})


def _expr_value(node: exp.Expression, params: dict[str, Any]) -> Any:
    if isinstance(node, exp.Placeholder):
        key = str(node.this)
        if key not in params:
            raise UnsupportedSQLError(f"missing SQL parameter: {key}")
        return params[key]
    if isinstance(node, exp.Literal):
        if node.is_string:
            return node.this
        text = str(node.this)
        try:
            return int(text)
        except ValueError:
            return float(text)
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    if isinstance(node, exp.Neg):
        value = _expr_value(node.this, params)
        return -value
    raise UnsupportedSQLError(f"unsupported write expression: {node.sql()}")


def _rewrite_tables(sql: str, replacements: dict[str, str], dialect: str) -> str:
    """Rewrite user table names to backend-specific physical tables/subqueries."""

    tree = sqlglot.parse_one(sql, read=dialect)

    def replace(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Table) and node.name in replacements:
            alias = node.args.get("alias")
            replacement = replacements[node.name]
            if replacement.lstrip().upper().startswith("SELECT"):
                parsed = sqlglot.parse_one(replacement, read=dialect)
                return exp.Subquery(this=parsed, alias=alias)
            table = exp.Table(this=exp.to_identifier(replacement, quoted=True))
            if alias is not None:
                table.set("alias", alias)
            return table
        return node

    return tree.transform(replace).sql(dialect=dialect)


def _target_table(tree: exp.Expression) -> str:
    table = next(tree.find_all(exp.Table), None)
    if table is None:
        raise UnsupportedSQLError("statement does not target a table")
    return table.name


def _insert_rows(tree: exp.Insert, params: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    schema = tree.this
    if not isinstance(schema, exp.Schema) or not isinstance(schema.this, exp.Table):
        raise UnsupportedSQLError("INSERT must specify a table and column list")
    table = schema.this.name
    columns = [c.name for c in schema.expressions]
    values = tree.expression
    if not isinstance(values, exp.Values):
        raise UnsupportedSQLError("only INSERT ... VALUES is supported")
    rows: list[dict[str, Any]] = []
    for tup in values.expressions:
        if not isinstance(tup, exp.Tuple):
            raise UnsupportedSQLError("only tuple VALUES are supported")
        if len(tup.expressions) != len(columns):
            raise UnsupportedSQLError("INSERT column/value count mismatch")
        rows.append(
            {
                column: _expr_value(value, params)
                for column, value in zip(columns, tup.expressions)
            }
        )
    return table, rows


def _update_assignments(tree: exp.Update, params: dict[str, Any]) -> dict[str, Any]:
    assignments: dict[str, Any] = {}
    for assignment in tree.expressions:
        if not isinstance(assignment, exp.EQ) or not isinstance(
            assignment.this, exp.Column
        ):
            raise UnsupportedSQLError("only simple column assignments are supported")
        assignments[assignment.this.name] = _expr_value(assignment.expression, params)
    return assignments


def _where_sql(tree: exp.Expression, dialect: str) -> str:
    where = tree.args.get("where")
    if where is None:
        return ""
    return " " + where.sql(dialect=dialect)


class _SQLBranchBackend:
    """Internal interface implemented by all physical branch backends.

    Public callers only see JanusBranchContext/BranchSession. The backend owns
    table registration, branch metadata, SQL rewriting targets, and row-level
    merge/diff primitives for one physical representation.
    """

    name: str

    def __init__(self, db: SQLDatabaseAdapter):
        self.db = db
        self.tables = _parse_table_registry(db, self.name)
        self.indexes = _parse_index_registry(db, self.name)

    def ensure(self) -> None:
        raise NotImplementedError

    def register_table(self, table: str, primary_key: list[str]) -> None:
        raise NotImplementedError

    def create_branch(self, branch_id: str, from_branch: str) -> None:
        raise NotImplementedError

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
        raise NotImplementedError

    def delete_branch(self, branch_id: str) -> None:
        raise NotImplementedError

    def list_branches(self) -> list[BranchInfo]:
        raise NotImplementedError

    def get_branch(self, branch_id: str) -> BranchInfo:
        raise NotImplementedError

    def create_checkpoint(self, checkpoint: str, branch: str) -> CheckpointInfo:
        raise NotImplementedError

    def checkout_checkpoint(self, checkpoint: str) -> _BranchRef:
        raise NotImplementedError

    def prepare_ref(self, ref: _BranchRef) -> _PreparedBranchRef:
        """Resolve per-session metadata once at checkout time."""

        return _PreparedBranchRef(ref.branch_id, ref.ref, ref.readonly)

    def refresh_ref_after_execute(self, ref: _PreparedBranchRef) -> _PreparedBranchRef:
        """Refresh cached metadata when a write changes the branch head."""

        return ref

    def query(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        raise NotImplementedError

    def execute(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> ExecuteResult:
        raise NotImplementedError

    def visible_rows(self, branch_id: str, table: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    def upsert_row(self, branch_id: str, table: str, row: dict[str, Any]) -> None:
        raise NotImplementedError

    def delete_key(self, branch_id: str, table: str, key: dict[str, Any]) -> None:
        raise NotImplementedError

    def create_index(
        self, table: str, columns: list[str], name: str | None = None
    ) -> IndexInfo:
        raise NotImplementedError

    def list_indexes(self, table: str | None = None) -> list[IndexInfo]:
        indexes = self.indexes.values()
        if table is not None:
            self._require_table(table)
            indexes = [index for index in indexes if index.table == table]
        return [
            IndexInfo(
                name=index.name,
                table=index.table,
                columns=index.columns,
                backend=index.backend,
            )
            for index in sorted(indexes, key=lambda i: (i.table, i.name))
        ]

    def _require_table(self, table: str) -> _TableMeta:
        try:
            return self.tables[table]
        except KeyError as exc:
            raise TableNotRegisteredError(table) from exc

    def _all_user_tables_replacements(self, replacement_for: Any) -> dict[str, str]:
        return {name: replacement_for(meta) for name, meta in self.tables.items()}

    def _select_matching_keys(
        self,
        ref: _BranchRef,
        table: str,
        where: str,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        meta = self._require_table(table)
        select_cols = ", ".join(_quote(c) for c in meta.pk_columns)
        sql = f"SELECT {select_cols} FROM {_quote(table)}{where}"
        return self.query(ref, sql, params)

    def _row_key(self, meta: _TableMeta, row: dict[str, Any]) -> dict[str, Any]:
        return {column: row[column] for column in meta.pk_columns}

    def _key_where(self, meta: _TableMeta, alias: str | None = None) -> str:
        parts = []
        prefix = f"{_quote(alias)}." if alias else ""
        for column in meta.pk_columns:
            parts.append(f"{prefix}{_quote(column)} = ?")
        return " AND ".join(parts)

    def _key_values(self, meta: _TableMeta, key: dict[str, Any]) -> list[Any]:
        return [key[column] for column in meta.pk_columns]

    def _validate_index(
        self, table: str, columns: list[str], name: str | None
    ) -> tuple[_TableMeta, _IndexMeta]:
        meta = self._require_table(table)
        if not columns:
            raise ValueError("index columns cannot be empty")
        missing = set(columns) - set(meta.columns)
        if missing:
            raise TableNotRegisteredError(
                f"index columns missing from {table}: {sorted(missing)}"
            )
        index_name = name or _default_index_name(table, columns)
        existing = self.indexes.get(index_name)
        if existing is not None:
            if existing.table != table or existing.columns != tuple(columns):
                raise BranchingError(f"index already exists with different definition: {index_name}")
            return meta, existing
        return meta, _IndexMeta(
            name=index_name,
            table=table,
            columns=tuple(columns),
            backend=self.name,
        )

    def _record_index(self, index: _IndexMeta) -> None:
        self.db.execute(
            """
            INSERT INTO _janus_branch_indexes
            (backend, index_name, table_name, columns)
            VALUES (?, ?, ?, ?)
            """,
            (self.name, index.name, index.table, json.dumps(index.columns)),
        )
        self.indexes[index.name] = index


class _IntervalBackend(_SQLBranchBackend):
    """Visibility-interval backend.

    Each logical row version is stored once with a half-open numeric interval.
    Reading a branch becomes a constant-size predicate over the branch point:
    live_lo <= point < live_hi and deleted = 0. Writes maintain correctness by
    splitting any overlapping fragments for the branch's current interval.
    """

    name = "interval"

    def ensure(self) -> None:
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _janus_branch_interval_branches (
              branch_id TEXT PRIMARY KEY,
              current_segment_id TEXT NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _janus_branch_interval_segments (
              segment_id TEXT PRIMARY KEY,
              parent_segment_id TEXT,
              owner_branch_id TEXT,
              live_lo INTEGER NOT NULL,
              live_hi INTEGER NOT NULL,
              branch_point INTEGER NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL,
              CHECK (live_lo < branch_point),
              CHECK (branch_point < live_hi)
            )
            """
        )
        exists = self.db.execute(
            "SELECT 1 FROM _janus_branch_interval_branches WHERE branch_id = 'main'"
        ).fetchone()
        if exists is None:
            segment = "seg_main"
            self.db.execute(
                """
                INSERT INTO _janus_branch_interval_segments
                (segment_id, parent_segment_id, owner_branch_id, live_lo, live_hi,
                 branch_point, created_at, metadata)
                VALUES (?, NULL, ?, ?, ?, ?, ?, ?)
                """,
                (
                    segment,
                    "main",
                    0,
                    _MAX_INTERVAL,
                    _MAX_INTERVAL // 2,
                    _utc_now(),
                    "{}",
                ),
            )
            self.db.execute(
                """
                INSERT INTO _janus_branch_interval_branches
                (branch_id, current_segment_id, created_at, metadata)
                VALUES (?, ?, ?, ?)
                """,
                ("main", segment, _utc_now(), "{}"),
            )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _janus_branch_interval_checkpoints (
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
        columns, defs = _table_defs(self.db, table)
        missing = set(primary_key) - set(columns)
        if missing:
            raise TableNotRegisteredError(f"primary key columns missing from {table}: {missing}")
        physical = f"_janus_b_interval_{table}"
        user_defs = ", ".join(defs)
        pk_sql = ", ".join(_quote(c) for c in primary_key)
        # The physical table keeps user columns plus visibility metadata. The
        # primary key includes live_lo because a logical key may have multiple
        # non-overlapping fragments across branch intervals.
        self.db.execute(
            f"""
            CREATE TABLE {_quote(physical)} (
              {user_defs},
              live_lo INTEGER NOT NULL,
              live_hi INTEGER NOT NULL,
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
            (_MAX_INTERVAL,),
        )
        self.db.execute(
            """
            INSERT INTO _janus_branch_tables
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
        self.db.commit()
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
                CREATE INDEX {_quote(f'_janus_idx_interval_{index.name}')}
                ON {_quote(meta.physical_name)}
                ({", ".join(_quote(c) for c in indexed_columns)})
                """
            )
            self._record_index(index)
            self.db.commit()
        return IndexInfo(index.name, index.table, index.columns, index.backend)

    def create_branch(self, branch_id: str, from_branch: str) -> None:
        if self._branch_row(branch_id) is not None:
            raise BranchAlreadyExistsError(branch_id)
        source = self._branch_row(from_branch)
        if source is None:
            raise BranchNotFoundError(from_branch)
        source_segment = self._segment(source["current_segment_id"])
        # Branching splits the source segment into two sibling intervals: a
        # continuation for the source branch and a child interval for the new
        # branch. Existing row fragments remain untouched until a write occurs.
        continuation, child = self._split_segment(source_segment)
        now = _utc_now()
        self.db.execute(
            """
            INSERT INTO _janus_branch_interval_segments
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
            INSERT INTO _janus_branch_interval_segments
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
            UPDATE _janus_branch_interval_branches
            SET current_segment_id = ?
            WHERE branch_id = ?
            """,
            (continuation["segment_id"], from_branch),
        )
        self.db.execute(
            """
            INSERT INTO _janus_branch_interval_branches
            (branch_id, current_segment_id, created_at, metadata)
            VALUES (?, ?, ?, ?)
            """,
            (branch_id, child["segment_id"], now, "{}"),
        )

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
        if self._branch_row(branch_id) is not None:
            raise BranchAlreadyExistsError(branch_id)
        cp = self.db.execute(
            "SELECT * FROM _janus_branch_interval_checkpoints WHERE checkpoint_id = ?",
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
            INSERT INTO _janus_branch_interval_segments
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
            INSERT INTO _janus_branch_interval_branches
            (branch_id, current_segment_id, created_at, metadata)
            VALUES (?, ?, ?, ?)
            """,
            (branch_id, child["segment_id"], now, "{}"),
        )

    def delete_branch(self, branch_id: str) -> None:
        if branch_id == "main":
            raise BranchingError("main cannot be deleted")
        cur = self.db.execute(
            "DELETE FROM _janus_branch_interval_branches WHERE branch_id = ?",
            (branch_id,),
        )
        if cur.rowcount == 0:
            raise BranchNotFoundError(branch_id)

    def list_branches(self) -> list[BranchInfo]:
        rows = self.db.execute(
            """
            SELECT branch_id, current_segment_id, created_at, metadata
            FROM _janus_branch_interval_branches
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
                "SELECT 1 FROM _janus_branch_interval_checkpoints WHERE checkpoint_id = ?",
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
                INSERT INTO _janus_branch_interval_segments
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
            UPDATE _janus_branch_interval_branches
            SET current_segment_id = ?
            WHERE branch_id = ?
            """,
            (continuation["segment_id"], branch),
        )
        self.db.execute(
            """
            INSERT INTO _janus_branch_interval_checkpoints
            (checkpoint_id, branch_id, segment_id, created_at, metadata)
            VALUES (?, ?, ?, ?, ?)
            """,
            (checkpoint, branch, snapshot["segment_id"], now, "{}"),
        )
        return CheckpointInfo(checkpoint, branch, snapshot["segment_id"], now)

    def checkout_checkpoint(self, checkpoint: str) -> _BranchRef:
        cp = self.db.execute(
            "SELECT * FROM _janus_branch_interval_checkpoints WHERE checkpoint_id = ?",
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
            },
        )

    def query(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        segment = self._prepared_segment(ref)
        replacements = ref.metadata.get("replacements")
        if replacements is None:
            replacements = self._all_user_tables_replacements(
                lambda meta: self._visible_subquery(meta)
            )
        rewritten = _rewrite_tables(sql, replacements, self.db.dialect)
        bound = dict(params)
        bound["_janus_branch_point"] = segment.branch_point
        rows = self.db.execute(rewritten, bound).fetchall()
        return [dict(row) for row in rows]

    def execute(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> ExecuteResult:
        if ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
        segment = self._prepared_segment(ref)
        tree = sqlglot.parse_one(sql, read=self.db.dialect)
        if isinstance(tree, exp.Insert):
            table, rows = _insert_rows(tree, params)
            meta = self._require_table(table)
            count = 0
            for row in rows:
                full_row = {column: row.get(column) for column in meta.columns}
                self._insert_visible(ref.branch_id, table, full_row, segment=segment)
                count += 1
            return ExecuteResult(count)
        if isinstance(tree, exp.Update):
            table = _target_table(tree)
            assignments = _update_assignments(tree, params)
            keys = self._select_matching_keys(
                ref, table, _where_sql(tree, self.db.dialect), params
            )
            meta = self._require_table(table)
            count = 0
            for key in keys:
                current = self._visible_row(ref.branch_id, table, key, segment=segment)
                if current is None:
                    continue
                new_row = dict(current)
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
        if isinstance(tree, exp.Delete):
            table = _target_table(tree)
            keys = self._select_matching_keys(
                ref, table, _where_sql(tree, self.db.dialect), params
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
            "WHERE live_lo <= :_janus_branch_point "
            "AND :_janus_branch_point < live_hi "
            "AND deleted = 0"
        )

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
        # All fragments overlapping this branch interval are replaced by up to
        # three fragments: left remainder, branch-local replacement, and right
        # remainder. This makes future reads pure visibility predicates.
        fragments = self.db.execute(
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
        if not fragments:
            if row is None and deleted:
                replacement = {column: None for column in meta.columns}
                replacement.update(key)
            else:
                replacement = {column: row.get(column) for column in meta.columns}  # type: ignore[union-attr]
            self._insert_fragment(meta, replacement, u_lo, u_hi, deleted)
            return
        for fragment in fragments:
            a = fragment["live_lo"]
            b = fragment["live_hi"]
            overlap_lo = max(a, u_lo)
            overlap_hi = min(b, u_hi)
            old_row = {column: fragment[column] for column in meta.columns}
            self.db.execute(
                f"""
                DELETE FROM {_quote(meta.physical_name)}
                WHERE {where}
                  AND live_lo = ?
                """,
                [*self._key_values(meta, key), a],
            )
            if a < overlap_lo:
                self._insert_fragment(
                    meta, old_row, a, overlap_lo, bool(fragment["deleted"])
                )
            if row is None and deleted:
                replacement = dict(old_row)
            else:
                replacement = {column: row.get(column) for column in meta.columns}  # type: ignore[union-attr]
            self._insert_fragment(meta, replacement, overlap_lo, overlap_hi, deleted)
            if overlap_hi < b:
                self._insert_fragment(
                    meta, old_row, overlap_hi, b, bool(fragment["deleted"])
                )

    def _insert_fragment(
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

    def _branch_row(self, branch_id: str) -> Any | None:
        return self.db.execute(
            "SELECT * FROM _janus_branch_interval_branches WHERE branch_id = ?",
            (branch_id,),
        ).fetchone()

    def _branch_segment_id(self, branch_id: str) -> str:
        row = self._branch_row(branch_id)
        if row is None:
            raise BranchNotFoundError(branch_id)
        return row["current_segment_id"]

    def _segment(self, segment_id: str) -> _IntervalSegment:
        row = self.db.execute(
            "SELECT * FROM _janus_branch_interval_segments WHERE segment_id = ?",
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
        # Midpoint splitting gives fast branch creation without moving row data.
        # A pathological single-child chain eventually exhausts integer space;
        # callers see BranchingError and can switch to a larger numeric type or
        # rebalance/rebase intervals in a future implementation.
        lo = segment.live_lo
        hi = segment.live_hi
        if hi - lo < 4:
            raise BranchingError("interval space exhausted")
        mid = lo + (hi - lo) // 2
        left = {
            "segment_id": f"seg_{uuid.uuid4().hex}",
            "live_lo": lo,
            "live_hi": mid,
            "branch_point": lo + (mid - lo) // 2,
        }
        right = {
            "segment_id": f"seg_{uuid.uuid4().hex}",
            "live_lo": mid,
            "live_hi": hi,
            "branch_point": mid + (hi - mid) // 2,
        }
        return left, right

    def _segment_for_ref(self, ref: _BranchRef) -> _IntervalSegment:
        if ref.readonly:
            return self._segment(ref.ref)
        return self._current_segment(ref.branch_id)

    def _current_segment(self, branch_id: str) -> _IntervalSegment:
        return self._segment(self._branch_segment_id(branch_id))

    def _prepared_segment(self, ref: _PreparedBranchRef) -> _IntervalSegment:
        segment = ref.metadata.get("segment")
        if isinstance(segment, _IntervalSegment):
            return segment
        return self._segment_for_ref(_BranchRef(ref.branch_id, ref.ref, ref.readonly))


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
            CREATE TABLE IF NOT EXISTS _janus_branch_log_branches (
              branch_id TEXT PRIMARY KEY,
              parent_branch_id TEXT,
              fork_txn_id INTEGER,
              head_txn_id INTEGER NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _janus_branch_log_checkpoints (
              checkpoint_id TEXT PRIMARY KEY,
              branch_id TEXT NOT NULL,
              head_txn_id INTEGER NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )
        if (
            self.db.execute(
                "SELECT 1 FROM _janus_branch_log_branches WHERE branch_id = 'main'"
            ).fetchone()
            is None
        ):
            self.db.execute(
                """
                INSERT INTO _janus_branch_log_branches
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
        physical = f"_janus_b_log_{table}"
        user_defs = ", ".join(defs)
        self.db.execute(
            f"""
            CREATE TABLE {_quote(physical)} (
              log_id {self.db.auto_increment_primary_key},
              txn_id INTEGER NOT NULL,
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
        cols = ", ".join(_quote(c) for c in columns)
        self.db.execute(
            f"""
            INSERT INTO {_quote(physical)}
            (txn_id, branch_id, op, {cols})
            SELECT 0, 'main', 'upsert', {cols} FROM {_quote(table)}
            """
        )
        self.db.execute(
            """
            INSERT INTO _janus_branch_tables
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
        self.db.commit()
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
                CREATE INDEX {_quote(f'_janus_idx_log_{index.name}')}
                ON {_quote(meta.physical_name)}
                ({", ".join(_quote(c) for c in indexed_columns)})
                """
            )
            self._record_index(index)
            self.db.commit()
        return IndexInfo(index.name, index.table, index.columns, index.backend)

    def create_branch(self, branch_id: str, from_branch: str) -> None:
        if self._branch_row(branch_id) is not None:
            raise BranchAlreadyExistsError(branch_id)
        source = self._branch_row(from_branch)
        if source is None:
            raise BranchNotFoundError(from_branch)
        self.db.execute(
            """
            INSERT INTO _janus_branch_log_branches
            (branch_id, parent_branch_id, fork_txn_id, head_txn_id, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                branch_id,
                from_branch,
                source["head_txn_id"],
                source["head_txn_id"],
                _utc_now(),
                "{}",
            ),
        )

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
        if self._branch_row(branch_id) is not None:
            raise BranchAlreadyExistsError(branch_id)
        cp = self.db.execute(
            "SELECT * FROM _janus_branch_log_checkpoints WHERE checkpoint_id = ?",
            (checkpoint,),
        ).fetchone()
        if cp is None:
            raise BranchNotFoundError(f"checkpoint:{checkpoint}")
        self.db.execute(
            """
            INSERT INTO _janus_branch_log_branches
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
            "DELETE FROM _janus_branch_log_branches WHERE branch_id = ?",
            (branch_id,),
        )
        if cur.rowcount == 0:
            raise BranchNotFoundError(branch_id)

    def list_branches(self) -> list[BranchInfo]:
        rows = self.db.execute(
            """
            SELECT branch_id, head_txn_id, created_at, metadata
            FROM _janus_branch_log_branches
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

    def create_checkpoint(self, checkpoint: str, branch: str) -> CheckpointInfo:
        if (
            self.db.execute(
                "SELECT 1 FROM _janus_branch_log_checkpoints WHERE checkpoint_id = ?",
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
            INSERT INTO _janus_branch_log_checkpoints
            (checkpoint_id, branch_id, head_txn_id, created_at, metadata)
            VALUES (?, ?, ?, ?, ?)
            """,
            (checkpoint, branch, source["head_txn_id"], now, "{}"),
        )
        return CheckpointInfo(checkpoint, branch, str(source["head_txn_id"]), now)

    def checkout_checkpoint(self, checkpoint: str) -> _BranchRef:
        cp = self.db.execute(
            "SELECT * FROM _janus_branch_log_checkpoints WHERE checkpoint_id = ?",
            (checkpoint,),
        ).fetchone()
        if cp is None:
            raise BranchNotFoundError(f"checkpoint:{checkpoint}")
        return _BranchRef(cp["branch_id"], str(cp["head_txn_id"]), readonly=True)

    def prepare_ref(self, ref: _BranchRef) -> _PreparedBranchRef:
        # Lineage lookup is proportional to branch depth, so it is cached in the
        # session and refreshed only after this session writes a new head.
        return _PreparedBranchRef(
            ref.branch_id,
            ref.ref,
            ref.readonly,
            {"lineage": self._lineage(ref)},
        )

    def refresh_ref_after_execute(self, ref: _PreparedBranchRef) -> _PreparedBranchRef:
        if ref.readonly:
            return ref
        return self.prepare_ref(_BranchRef(ref.branch_id, ref.ref, ref.readonly))

    def query(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        bound = dict(params)
        replacements = {
            table: self._visible_subquery(ref, meta, bound)
            for table, meta in self.tables.items()
        }
        rewritten = _rewrite_tables(sql, replacements, self.db.dialect)
        rows = self.db.execute(rewritten, bound).fetchall()
        return [dict(row) for row in rows]

    def execute(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> ExecuteResult:
        if ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
        tree = sqlglot.parse_one(sql, read=self.db.dialect)
        if isinstance(tree, exp.Insert):
            table, rows = _insert_rows(tree, params)
            count = 0
            for row in rows:
                self._insert_visible(ref, table, row)
                count += 1
            return ExecuteResult(count)
        if isinstance(tree, exp.Update):
            table = _target_table(tree)
            assignments = _update_assignments(tree, params)
            keys = self._select_matching_keys(
                ref, table, _where_sql(tree, self.db.dialect), params
            )
            meta = self._require_table(table)
            count = 0
            for key in keys:
                current = self._visible_row(ref, table, key)
                if current is None:
                    continue
                new_row = dict(current)
                new_row.update(assignments)
                self.upsert_row(ref.branch_id, table, new_row)
                count += 1
            return ExecuteResult(count)
        if isinstance(tree, exp.Delete):
            table = _target_table(tree)
            keys = self._select_matching_keys(
                ref, table, _where_sql(tree, self.db.dialect), params
            )
            count = 0
            for key in keys:
                self.delete_key(ref.branch_id, table, key)
                count += 1
            return ExecuteResult(count)
        raise UnsupportedSQLError("only SELECT, INSERT, UPDATE, and DELETE are supported")

    def visible_rows(self, branch_id: str, table: str) -> list[dict[str, Any]]:
        ref = self.prepare_ref(_BranchRef(branch_id, str(self._branch_head(branch_id))))
        return self.query(ref, f"SELECT * FROM {_quote(table)}", {})

    def upsert_row(self, branch_id: str, table: str, row: dict[str, Any]) -> None:
        meta = self._require_table(table)
        full_row = {column: row.get(column) for column in meta.columns}
        txn_id = self._new_txn(branch_id)
        self._append_log(meta, txn_id, branch_id, "upsert", full_row)
        self._set_branch_head(branch_id, txn_id)

    def delete_key(self, branch_id: str, table: str, key: dict[str, Any]) -> None:
        meta = self._require_table(table)
        row = {column: None for column in meta.columns}
        row.update(key)
        txn_id = self._new_txn(branch_id)
        self._append_log(meta, txn_id, branch_id, "delete", row)
        self._set_branch_head(branch_id, txn_id)

    def _insert_visible(
        self, ref: _BranchRef, table: str, row: dict[str, Any]
    ) -> None:
        meta = self._require_table(table)
        full_row = {column: row.get(column) for column in meta.columns}
        key = {column: full_row[column] for column in meta.pk_columns}
        if self._visible_row(ref, table, key) is not None:
            raise DuplicateKeyError(f"duplicate key on branch {ref.branch_id}: {key}")
        self.upsert_row(ref.branch_id, table, full_row)

    def _visible_row(
        self, ref: _BranchRef, table: str, key: dict[str, Any]
    ) -> dict[str, Any] | None:
        meta = self._require_table(table)
        where_parts: list[str] = []
        params: dict[str, Any] = {}
        for idx, column in enumerate(meta.pk_columns):
            param_name = f"_janus_key_{idx}"
            where_parts.append(f"{_quote(column)} = :{param_name}")
            params[param_name] = key[column]
        rows = self.query(
            ref,
            f"""
            SELECT *
            FROM {_quote(table)}
            WHERE {" AND ".join(where_parts)}
            """,
            params,
        )
        return rows[0] if rows else None

    def _visible_subquery(
        self, ref: _PreparedBranchRef, meta: _TableMeta, params: dict[str, Any]
    ) -> str:
        cols = ", ".join(_quote(c) for c in meta.columns)
        partition_cols = ", ".join(_quote(c) for c in meta.pk_columns)
        union_parts: list[str] = []
        table_token = _identifier_token(meta.name)
        lineage = ref.metadata.get("lineage")
        if lineage is None:
            lineage = self._lineage(_BranchRef(ref.branch_id, ref.ref, ref.readonly))
        # Each lineage element contributes the log prefix visible from that
        # ancestor. The window function then chooses the nearest/newest operation
        # for each logical key and filters tombstones.
        for lineage_rank, (branch_id, max_txn) in enumerate(lineage):
            branch_param = f"_janus_log_{table_token}_{lineage_rank}_branch"
            txn_param = f"_janus_log_{table_token}_{lineage_rank}_txn"
            params[branch_param] = branch_id
            params[txn_param] = max_txn
            union_parts.append(
                f"""
                SELECT
                  {lineage_rank} AS _janus_lineage_rank,
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
                  ORDER BY _janus_lineage_rank DESC, txn_id DESC, log_id DESC
                ) AS _janus_rn
              FROM (
                {union_sql}
              ) AS _janus_log_candidates
            ) AS _janus_ranked
            WHERE _janus_rn = 1
              AND op <> 'delete'
        """

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

    def _new_txn(self, branch_id: str) -> int:
        row = self._branch_row(branch_id)
        if row is None:
            raise BranchNotFoundError(branch_id)
        return int(row["head_txn_id"]) + 1

    def _set_branch_head(self, branch_id: str, txn_id: int) -> None:
        self.db.execute(
            "UPDATE _janus_branch_log_branches SET head_txn_id = ? WHERE branch_id = ?",
            (txn_id, branch_id),
        )

    def _branch_row(self, branch_id: str) -> Any | None:
        return self.db.execute(
            "SELECT * FROM _janus_branch_log_branches WHERE branch_id = ?",
            (branch_id,),
        ).fetchone()

    def _branch_head(self, branch_id: str) -> int:
        row = self._branch_row(branch_id)
        if row is None:
            raise BranchNotFoundError(branch_id)
        return int(row["head_txn_id"])


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
            CREATE TABLE IF NOT EXISTS _janus_branch_copy_branches (
              branch_id TEXT PRIMARY KEY,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _janus_branch_copy_checkpoints (
              checkpoint_id TEXT PRIMARY KEY,
              branch_id TEXT NOT NULL,
              created_at TEXT NOT NULL,
              metadata TEXT NOT NULL
            )
            """
        )
        if (
            self.db.execute(
                "SELECT 1 FROM _janus_branch_copy_branches WHERE branch_id = 'main'"
            ).fetchone()
            is None
        ):
            self.db.execute(
                """
                INSERT INTO _janus_branch_copy_branches
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
            INSERT INTO _janus_branch_tables
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
        self.db.commit()
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
            self.db.commit()
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
            INSERT INTO _janus_branch_copy_branches
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
            INSERT INTO _janus_branch_copy_branches
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
            "DELETE FROM _janus_branch_copy_branches WHERE branch_id = ?",
            (branch_id,),
        )

    def list_branches(self) -> list[BranchInfo]:
        rows = self.db.execute(
            """
            SELECT branch_id, created_at, metadata
            FROM _janus_branch_copy_branches
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
            INSERT INTO _janus_branch_copy_checkpoints
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
            {"replacements": self._replacements(ref)},
        )

    def query(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        rewritten = _rewrite_tables(sql, self._prepared_replacements(ref), self.db.dialect)
        rows = self.db.execute(rewritten, params).fetchall()
        return [dict(row) for row in rows]

    def execute(self, ref: _PreparedBranchRef, sql: str, params: dict[str, Any]) -> ExecuteResult:
        if ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
        tree = sqlglot.parse_one(sql, read=self.db.dialect)
        if isinstance(tree, exp.Insert):
            table, rows = _insert_rows(tree, params)
            meta = self._require_table(table)
            for row in rows:
                key = {column: row[column] for column in meta.pk_columns}
                if self._visible_row(ref.branch_id, table, key) is not None:
                    raise DuplicateKeyError(f"duplicate key on branch {ref.branch_id}: {key}")
        elif isinstance(tree, exp.Update):
            _update_assignments(tree, params)
        rewritten = _rewrite_tables(sql, self._prepared_replacements(ref), self.db.dialect)
        cur = self.db.execute(rewritten, params)
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
            CREATE TABLE {_quote(physical)} (
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
            f"_janus_idx_copy_cp_{_identifier_token(owner_id)}_{_identifier_token(index.name)}"
            if checkpoint
            else f"_janus_idx_copy_{_identifier_token(owner_id)}_{_identifier_token(index.name)}"
        )
        self.db.execute(
            f"""
            CREATE INDEX {_quote(index_name)}
            ON {_quote(physical)}
            ({", ".join(_quote(c) for c in index.columns)})
            """
        )

    def _branch_table(self, branch_id: str, table: str) -> str:
        return f"_janus_b_copy_{_identifier_token(branch_id)}_{_identifier_token(table)}"

    def _checkpoint_table(self, checkpoint: str, table: str) -> str:
        return f"_janus_b_copy_cp_{_identifier_token(checkpoint)}_{_identifier_token(table)}"

    def _branch_row(self, branch_id: str) -> Any | None:
        return self.db.execute(
            "SELECT * FROM _janus_branch_copy_branches WHERE branch_id = ?",
            (branch_id,),
        ).fetchone()

    def _checkpoint_row(self, checkpoint: str) -> Any | None:
        return self.db.execute(
            "SELECT * FROM _janus_branch_copy_checkpoints WHERE checkpoint_id = ?",
            (checkpoint,),
        ).fetchone()

    def _branch_ids(self) -> list[str]:
        return [
            row["branch_id"]
            for row in self.db.execute(
                "SELECT branch_id FROM _janus_branch_copy_branches"
            ).fetchall()
        ]

    def _checkpoint_ids(self) -> list[str]:
        return [
            row["checkpoint_id"]
            for row in self.db.execute(
                "SELECT checkpoint_id FROM _janus_branch_copy_checkpoints"
            ).fetchall()
        ]


class BranchSession:
    """Checked-out branch handle used by agents and applications.

    A session owns a prepared branch reference. It is cheap to reuse for many
    statements, and its transaction context groups several SQL mutations into
    one database transaction.
    """

    def __init__(self, context: JanusBranchContext, ref: _PreparedBranchRef):
        self._context = context
        self._ref = ref
        self._transaction_depth = 0

    @property
    def branch_id(self) -> str:
        return self._ref.branch_id

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self._ensure_fresh()
        return self._context._backend.query(self._ref, sql, _ensure_params(params))

    def execute(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> ExecuteResult:
        self._ensure_fresh()
        result = self._context._backend.execute(self._ref, sql, _ensure_params(params))
        # Some backends mutate the branch head on write. Refresh the prepared
        # metadata so later reads in the same session see their own writes.
        self._ref = self._context._backend.refresh_ref_after_execute(self._ref)
        self._context._stamp_prepared_ref(self._ref)
        if self._transaction_depth == 0:
            self._context._db.commit()
        return result

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        """Group several session writes in one underlying SQL transaction."""

        root = self._transaction_depth == 0
        if root:
            if self._context._db.in_transaction:
                self._context._db.commit()
            self._context._db.begin()
        self._transaction_depth += 1
        try:
            yield
        except Exception:
            self._transaction_depth -= 1
            if root:
                self._context._db.rollback()
            raise
        else:
            self._transaction_depth -= 1
            if root:
                self._context._db.commit()

    def branch_info(self) -> BranchInfo:
        return self._context.get_branch(self.branch_id)

    def _ensure_fresh(self) -> None:
        if self._ref.readonly:
            return
        # Branch creation/checkpointing can move interval segments. Existing
        # sessions refresh lazily when the context metadata epoch changes.
        if self._ref.metadata.get("_context_epoch") != self._context._metadata_epoch:
            self._ref = self._context._prepare_ref(
                _BranchRef(self._ref.branch_id, self._ref.ref, self._ref.readonly)
            )


class JanusBranchContext:
    """Branch manager for SQL-backed relational data."""

    def __init__(self, db: SQLDatabaseAdapter, backend: _SQLBranchBackend):
        self._db = db
        self._backend = backend
        # Incremented whenever branch/table metadata changes. Checked-out
        # sessions compare against this to invalidate prepared metadata.
        self._metadata_epoch = 0

    @classmethod
    def connect(
        cls, database_url: str, backend: BranchBackendName = "interval"
    ) -> JanusBranchContext:
        db = connect_sql_database(database_url)
        if backend == "interval":
            impl: _SQLBranchBackend = _IntervalBackend(db)
        elif backend == "log":
            impl = _LogBackend(db)
        elif backend == "copy":
            impl = _CopyBackend(db)
        else:
            raise ValueError(f"unknown branch backend: {backend}")
        impl.ensure()
        return cls(db, impl)

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
        self._backend.register_table(table, primary_key)
        self._metadata_epoch += 1

    def create_index(
        self, table: str, columns: list[str], name: str | None = None
    ) -> IndexInfo:
        info = self._backend.create_index(table, columns, name)
        self._metadata_epoch += 1
        return info

    def list_indexes(self, table: str | None = None) -> list[IndexInfo]:
        return self._backend.list_indexes(table)

    def create_branch(self, branch_id: str, from_branch: str = "main") -> None:
        self._backend.create_branch(branch_id, from_branch)
        self._db.commit()
        self._metadata_epoch += 1

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
        self._backend.create_branch_from_checkpoint(branch_id, checkpoint)
        self._db.commit()
        self._metadata_epoch += 1

    def delete_branch(self, branch_id: str) -> None:
        self._backend.delete_branch(branch_id)
        self._db.commit()
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

    def create_checkpoint(self, checkpoint: str, branch: str = "main") -> CheckpointInfo:
        info = self._backend.create_checkpoint(checkpoint, branch)
        self._db.commit()
        self._metadata_epoch += 1
        return info

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
