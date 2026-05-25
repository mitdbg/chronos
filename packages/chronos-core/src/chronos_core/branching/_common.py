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

from chronos_core.branching.sql_adapters import SQLDatabaseAdapter, connect_sql_database

BranchBackendName = Literal["interval", "log", "copy"]

# Interval backends assign branches/subtrees numeric visibility ranges. SQLite
# is limited to signed 64-bit integers. PostgreSQL can use exact NUMERIC
# integers, so the interval backend selects a much larger PostgreSQL-only range.
_MAX_INTERVAL = 9_000_000_000_000_000_000
_POSTGRES_INTERVAL_PRECISION = 32
_POSTGRES_MAX_INTERVAL = 10**31
_INTERVAL_CONTINUATION_PERCENT = 5
_INTERVAL_PERCENT_DENOMINATOR = 100
_MIN_SPLIT_WIDTH = 2
_META_PREFIX = "_chronos_branch_"


def _validate_interval_continuation_percent(value: int) -> int:
    percent = int(value)
    if not 1 <= percent <= 99:
        raise ValueError("interval_continuation_percent must be between 1 and 99")
    return percent


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


@dataclass(frozen=True)
class _InsertPlan:
    table: str
    columns: tuple[str, ...]
    value_tuples: tuple[tuple[exp.Expression, ...], ...]


@dataclass(frozen=True)
class _UpdatePlan:
    table: str
    assignments: tuple[tuple[str, exp.Expression], ...]
    where_sql: str
    direct_filter: bool


@dataclass(frozen=True)
class _DeletePlan:
    table: str
    where_sql: str
    direct_filter: bool


_StatementPlan = _InsertPlan | _UpdatePlan | _DeletePlan


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


@contextlib.contextmanager
def _chronos_metadata_lock(db: SQLDatabaseAdapter) -> Iterator[None]:
    """Serialize Chronos metadata DDL for PostgreSQL-backed contexts.

    PostgreSQL's `CREATE TABLE IF NOT EXISTS` is idempotent for sequential
    callers, but concurrent sessions can still race while creating the
    associated composite type. A session-level advisory lock keeps independent
    ChronosBranchContext instances from initializing/registering metadata at
    the same time.
    """

    if db.dialect != "postgres":
        yield
        return
    db.execute("SELECT pg_advisory_lock(1720812901, 19840717)")
    try:
        yield
    finally:
        try:
            db.execute("SELECT pg_advisory_unlock(1720812901, 19840717)")
        except Exception:
            db.rollback()
            db.execute("SELECT pg_advisory_unlock(1720812901, 19840717)")


def _parse_table_registry(db: SQLDatabaseAdapter, backend: str) -> dict[str, _TableMeta]:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS _chronos_branch_tables (
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
        "SELECT * FROM _chronos_branch_tables WHERE backend = ?", (backend,)
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
        CREATE TABLE IF NOT EXISTS _chronos_branch_indexes (
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
        "SELECT * FROM _chronos_branch_indexes WHERE backend = ?", (backend,)
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


def _expr_value(
    node: exp.Expression,
    params: dict[str, Any],
    row: dict[str, Any] | None = None,
) -> Any:
    if isinstance(node, exp.Placeholder):
        key = str(node.this)
        if key not in params:
            raise UnsupportedSQLError(f"missing SQL parameter: {key}")
        return params[key]
    if isinstance(node, exp.Column):
        if row is None or node.name not in row:
            raise UnsupportedSQLError(f"unsupported write expression: {node.sql()}")
        return row[node.name]
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
        value = _expr_value(node.this, params, row)
        return -value
    if isinstance(node, exp.Paren):
        return _expr_value(node.this, params, row)
    if isinstance(node, exp.Add):
        return _expr_value(node.this, params, row) + _expr_value(node.expression, params, row)
    if isinstance(node, exp.Sub):
        return _expr_value(node.this, params, row) - _expr_value(node.expression, params, row)
    if isinstance(node, exp.Mul):
        return _expr_value(node.this, params, row) * _expr_value(node.expression, params, row)
    if isinstance(node, exp.Div):
        return _expr_value(node.this, params, row) / _expr_value(node.expression, params, row)
    if isinstance(node, exp.Mod):
        return _expr_value(node.this, params, row) % _expr_value(node.expression, params, row)
    raise UnsupportedSQLError(f"unsupported write expression: {node.sql()}")


def _rewrite_tables(sql: str, replacements: dict[str, str], dialect: str) -> str:
    """Rewrite user table names to backend-specific physical tables/subqueries."""

    tree = sqlglot.parse_one(sql, read=dialect)
    return _rewrite_tree_tables(tree, replacements, dialect)


def _rewrite_tree_tables(
    tree: exp.Expression, replacements: dict[str, str], dialect: str
) -> str:
    """Rewrite table names in an already parsed SQL tree.

    Write paths often parse once to understand the mutation shape before
    sending SQL to the underlying database. Reusing that tree avoids paying
    sqlglot parse/tokenize cost a second time on hot write loops.
    """

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


def _build_insert_plan(tree: exp.Insert) -> _InsertPlan:
    schema = tree.this
    if not isinstance(schema, exp.Schema) or not isinstance(schema.this, exp.Table):
        raise UnsupportedSQLError("INSERT must specify a table and column list")
    table = schema.this.name
    columns = _insert_columns(tree)
    values = tree.expression
    if not isinstance(values, exp.Values):
        raise UnsupportedSQLError("only INSERT ... VALUES is supported")
    value_tuples: list[tuple[exp.Expression, ...]] = []
    for tup in values.expressions:
        if not isinstance(tup, exp.Tuple):
            raise UnsupportedSQLError("only tuple VALUES are supported")
        if len(tup.expressions) != len(columns):
            raise UnsupportedSQLError("INSERT column/value count mismatch")
        value_tuples.append(tuple(tup.expressions))
    return _InsertPlan(table, tuple(columns), tuple(value_tuples))


def _insert_rows(tree: exp.Insert, params: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    plan = _build_insert_plan(tree)
    return plan.table, _insert_rows_from_plan(plan, params)


def _insert_rows_from_plan(plan: _InsertPlan, params: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for tup in plan.value_tuples:
        rows.append(
            {
                column: _expr_value(value, params)
                for column, value in zip(plan.columns, tup)
            }
        )
    return rows


def _insert_columns(tree: exp.Insert) -> list[str]:
    schema = tree.this
    if not isinstance(schema, exp.Schema) or not isinstance(schema.this, exp.Table):
        raise UnsupportedSQLError("INSERT must specify a table and column list")
    return [c.name for c in schema.expressions]


def _update_assignments(
    tree: exp.Update,
    params: dict[str, Any],
    row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        column: _expr_value(value, params, row)
        for column, value in _update_assignment_expressions(tree)
    }


def _update_assignment_expressions(tree: exp.Update) -> tuple[tuple[str, exp.Expression], ...]:
    assignments: list[tuple[str, exp.Expression]] = []
    for assignment in tree.expressions:
        if not isinstance(assignment, exp.EQ) or not isinstance(
            assignment.this, exp.Column
        ):
            raise UnsupportedSQLError("only simple column assignments are supported")
        assignments.append((assignment.this.name, assignment.expression))
    return tuple(assignments)


def _build_update_plan(tree: exp.Update, dialect: str) -> _UpdatePlan:
    return _UpdatePlan(
        _target_table(tree),
        _update_assignment_expressions(tree),
        _where_sql(tree, dialect),
        _where_can_filter_physical_table(tree),
    )


def _update_assignments_from_plan(
    plan: _UpdatePlan,
    params: dict[str, Any],
    row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        column: _expr_value(value, params, row)
        for column, value in plan.assignments
    }


def _build_delete_plan(tree: exp.Delete, dialect: str) -> _DeletePlan:
    return _DeletePlan(
        _target_table(tree),
        _where_sql(tree, dialect),
        _where_can_filter_physical_table(tree),
    )


def _where_sql(tree: exp.Expression, dialect: str) -> str:
    where = tree.args.get("where")
    if where is None:
        return ""
    return " " + where.sql(dialect=dialect)


def _where_can_filter_physical_table(tree: exp.Expression) -> bool:
    where = tree.args.get("where")
    if where is None:
        return True
    return not any(where.find_all(exp.Select))


class _SQLBranchBackend:
    """Internal interface implemented by all physical branch backends.

    Public callers only see ChronosBranchContext/BranchSession. The backend owns
    table registration, branch metadata, SQL rewriting targets, and row-level
    merge/diff primitives for one physical representation.
    """

    name: str

    def __init__(self, db: SQLDatabaseAdapter):
        self.db = db
        self.tables = _parse_table_registry(db, self.name)
        self.indexes = _parse_index_registry(db, self.name)

    def refresh_registries(self) -> None:
        self.tables = _parse_table_registry(self.db, self.name)
        self.indexes = _parse_index_registry(self.db, self.name)

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
            INSERT INTO _chronos_branch_indexes
            (backend, index_name, table_name, columns)
            VALUES (?, ?, ?, ?)
            """,
            (self.name, index.name, index.table, json.dumps(index.columns)),
        )
        self.indexes[index.name] = index

__all__ = [name for name in globals() if not name.startswith("__")]
