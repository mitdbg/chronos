from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import unquote, urlparse

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row


SQLRow = Mapping[str, Any]


class SQLAdapterError(Exception):
    """Base error raised by SQL database adapters."""


class SQLIntegrityError(SQLAdapterError):
    """Raised for logical integrity violations independent of a DB driver."""


class SQLDatabaseAdapter(ABC):
    """Small portability boundary between branching logic and a SQL database.

    The branching layer intentionally depends on this interface rather than a
    concrete DB-API driver. New database engines should add an adapter here and
    keep branch metadata, interval maintenance, and log replay unchanged.
    """

    dialect: str

    @abstractmethod
    def execute(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()
    ) -> Any:
        raise NotImplementedError

    @abstractmethod
    def executemany(self, sql: str, params: Iterable[Sequence[Any]]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def commit(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def rollback(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def begin(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def in_transaction(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def raw_connection(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def table_defs(self, table: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
        raise NotImplementedError

    @abstractmethod
    def last_insert_id(self) -> int:
        raise NotImplementedError

    @property
    @abstractmethod
    def auto_increment_primary_key(self) -> str:
        raise NotImplementedError

    def quote_identifier(self, identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    def placeholders(self, count: int) -> str:
        return ", ".join("?" for _ in range(count))

    def create_temp_table(self, name: str, column_defs: Sequence[str]) -> None:
        self.execute(
            f"CREATE TEMP TABLE {self.quote_identifier(name)} "
            f"({', '.join(column_defs)})"
        )

    def drop_table(self, name: str) -> None:
        self.execute(f"DROP TABLE IF EXISTS {self.quote_identifier(name)}")


class SQLiteDatabaseAdapter(SQLDatabaseAdapter):
    dialect = "sqlite"

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    @classmethod
    def connect(cls, database_url: str) -> SQLiteDatabaseAdapter:
        parsed = urlparse(database_url)
        if parsed.scheme not in ("", "sqlite"):
            raise ValueError(f"not a SQLite URL: {database_url}")
        if database_url in (":memory:", "sqlite:///:memory:"):
            path = ":memory:"
        elif parsed.scheme == "":
            path = database_url
        elif parsed.netloc and parsed.netloc not in ("", "localhost"):
            raise ValueError(f"Only local SQLite URLs are supported: {database_url}")
        else:
            path = unquote(parsed.path)
            if path.startswith("/") and database_url.startswith("sqlite:///"):
                pass
            elif path:
                path = path.lstrip("/")
            if not path:
                raise ValueError(f"SQLite URL is missing a path: {database_url}")
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("PRAGMA journal_mode=WAL")
        return cls(conn)

    def execute(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()
    ) -> sqlite3.Cursor:
        return self._conn.execute(sql, params)

    def executemany(self, sql: str, params: Iterable[Sequence[Any]]) -> sqlite3.Cursor:
        return self._conn.executemany(sql, params)

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def begin(self) -> None:
        self._conn.execute("BEGIN")

    def close(self) -> None:
        self._conn.close()

    @property
    def in_transaction(self) -> bool:
        return self._conn.in_transaction

    @property
    def raw_connection(self) -> sqlite3.Connection:
        return self._conn

    @property
    def auto_increment_primary_key(self) -> str:
        return "INTEGER PRIMARY KEY AUTOINCREMENT"

    def table_defs(self, table: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
        rows = self.execute(
            f"PRAGMA table_info({self.quote_identifier(table)})"
        ).fetchall()
        if not rows:
            return (), ()
        columns: list[str] = []
        defs: list[str] = []
        for row in rows:
            name = row["name"]
            col_type = row["type"] or "TEXT"
            columns.append(name)
            defs.append(f"{self.quote_identifier(name)} {col_type}")
        return tuple(columns), tuple(defs)

    def last_insert_id(self) -> int:
        return int(self.execute("SELECT last_insert_rowid()").fetchone()[0])


class PostgresDatabaseAdapter(SQLDatabaseAdapter):
    dialect = "postgres"

    _named_param = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")

    def __init__(self, conn: psycopg.Connection[Any]):
        self._conn = conn

    @classmethod
    def connect(cls, database_url: str) -> PostgresDatabaseAdapter:
        conn = psycopg.connect(database_url, row_factory=dict_row)
        return cls(conn)

    def execute(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()
    ) -> psycopg.Cursor[Any]:
        if not params:
            return self._conn.execute(sql)
        translated_sql, translated_params = self._translate_params(sql, params)
        return self._conn.execute(translated_sql, translated_params)

    def executemany(
        self, sql: str, params: Iterable[Sequence[Any]]
    ) -> psycopg.Cursor[Any]:
        translated_sql = self._translate_qmark(sql)
        cursor = self._conn.cursor()
        cursor.executemany(translated_sql, params)
        return cursor

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def begin(self) -> None:
        self._conn.execute("BEGIN")

    def close(self) -> None:
        self._conn.close()

    @property
    def in_transaction(self) -> bool:
        return self._conn.info.transaction_status != TransactionStatus.IDLE

    @property
    def raw_connection(self) -> psycopg.Connection[Any]:
        return self._conn

    @property
    def auto_increment_primary_key(self) -> str:
        return "BIGSERIAL PRIMARY KEY"

    def create_temp_table(self, name: str, column_defs: Sequence[str]) -> None:
        self.execute(
            f"CREATE TEMP TABLE {self.quote_identifier(name)} "
            f"({', '.join(column_defs)}) ON COMMIT DROP"
        )

    def table_defs(self, table: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
        schema, table_name = self._split_table_name(table)
        rows = self.execute(
            """
            SELECT column_name, data_type, udt_name, character_maximum_length,
                   numeric_precision, numeric_scale, is_nullable
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name = :table
            ORDER BY ordinal_position
            """,
            {"schema": schema, "table": table_name},
        ).fetchall()
        if not rows:
            return (), ()
        columns: list[str] = []
        defs: list[str] = []
        for row in rows:
            name = row["column_name"]
            columns.append(name)
            # Branched physical tables must be able to represent tombstones and
            # partial log records, so source-table NOT NULL constraints are not
            # copied into the branch storage table definitions.
            defs.append(f"{self.quote_identifier(name)} {self._column_type(row)}")
        return tuple(columns), tuple(defs)

    def last_insert_id(self) -> int:
        row = self.execute("SELECT LASTVAL() AS id").fetchone()
        return int(row["id"])

    def _translate_params(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any]
    ) -> tuple[str, Sequence[Any] | Mapping[str, Any]]:
        if isinstance(params, Mapping):
            return self._translate_named(sql), params
        return self._translate_qmark(sql), params

    @classmethod
    def _translate_named(cls, sql: str) -> str:
        return cls._named_param.sub(r"%(\1)s", sql)

    @staticmethod
    def _translate_qmark(sql: str) -> str:
        return sql.replace("?", "%s")

    @staticmethod
    def _split_table_name(table: str) -> tuple[str, str]:
        if "." in table:
            schema, table_name = table.split(".", 1)
            return schema, table_name
        return "public", table

    @staticmethod
    def _column_type(row: Mapping[str, Any]) -> str:
        data_type = row["data_type"]
        udt_name = row["udt_name"]
        if data_type == "USER-DEFINED":
            return str(udt_name)
        if data_type == "ARRAY":
            return str(udt_name)
        if data_type == "character varying":
            length = row["character_maximum_length"]
            return f"VARCHAR({length})" if length else "VARCHAR"
        if data_type == "character":
            length = row["character_maximum_length"]
            return f"CHAR({length})" if length else "CHAR"
        if data_type == "numeric":
            precision = row["numeric_precision"]
            scale = row["numeric_scale"]
            if precision is not None and scale is not None:
                return f"NUMERIC({precision}, {scale})"
            return "NUMERIC"
        if data_type == "timestamp without time zone":
            return "TIMESTAMP"
        if data_type == "timestamp with time zone":
            return "TIMESTAMPTZ"
        return str(data_type).upper()


def connect_sql_database(database_url: str) -> SQLDatabaseAdapter:
    parsed = urlparse(database_url)
    if parsed.scheme in ("", "sqlite"):
        return SQLiteDatabaseAdapter.connect(database_url)
    if parsed.scheme in ("postgres", "postgresql"):
        return PostgresDatabaseAdapter.connect(database_url)
    raise ValueError(f"unsupported SQL database URL for Chronos branching: {database_url}")
