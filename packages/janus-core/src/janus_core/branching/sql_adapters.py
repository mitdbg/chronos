from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import unquote, urlparse


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


def connect_sql_database(database_url: str) -> SQLDatabaseAdapter:
    parsed = urlparse(database_url)
    if parsed.scheme in ("", "sqlite"):
        return SQLiteDatabaseAdapter.connect(database_url)
    raise ValueError(
        "JanusBranchContext currently has a SQLite adapter only; add a "
        "SQLDatabaseAdapter implementation for this database URL"
    )
