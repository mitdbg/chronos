"""Janus SQLite tool — transactional MVCC database operations.

Wraps ``SQLiteShim`` as a LangChain tool, giving agents a transactional
key-value / relational store that participates in the same 2PC protocol
as the OverlayFS filesystem tools.  All reads use the MVCC visibility
predicate so concurrent and nested transactions see consistent snapshots,
and all writes are versioned with ``_begin_txn`` / ``_end_txn`` columns.

The tool exposes high-level operations:
  - **register_table** — define a table schema (DDL).
  - **put** — upsert a row (MVCC write).
  - **get** — read a single row by primary key.
  - **query** — filtered scan with ordering and limit.
  - **delete** — logically delete a row (MVCC tombstone).
  - **list_tables** — show registered tables.
  - **sql** — execute raw SELECT for advanced queries.

The ``SQLiteShim`` is enrolled in the ``TransactionCoordinator`` so
commit / abort / savepoint / rollback are fully coordinated with the
filesystem overlay.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from langchain_core.callbacks import CallbackManagerForToolRun
from langchain_core.tools import BaseTool
from pydantic import BaseModel, ConfigDict, Field

from janus_core.transaction.shim_sqlite import SQLiteShim
from janus_core.transaction.types import TransactionHandle

logger = logging.getLogger(__name__)


class JanusSQLiteInput(BaseModel):
    """Input schema for the Janus SQLite tool."""

    command: str = Field(
        description=(
            "The database operation to perform. One of: "
            "'register_table' — Define a new table (columns, pk). "
            "'put' — Insert or update a row. "
            "'get' — Read a single row by primary key. "
            "'query' — Filtered scan with optional order_by/limit. "
            "'delete' — Delete a row by primary key. "
            "'list_tables' — List all registered tables. "
            "'sql' — Execute a raw SELECT statement."
        )
    )
    table: str | None = Field(
        default=None,
        description="Table name (required for put/get/query/delete/register_table).",
    )
    row: dict[str, Any] | None = Field(
        default=None,
        description="Row data as a JSON object (required for 'put').",
    )
    pk_value: str | None = Field(
        default=None,
        description="Primary key value (required for 'get' and 'delete').",
    )
    columns: list[str] | None = Field(
        default=None,
        description=(
            "Column definitions for 'register_table', "
            "e.g. ['id TEXT', 'name TEXT', 'age INTEGER']. "
            "The first column is the primary key unless pk_column is set."
        ),
    )
    pk_column: str | None = Field(
        default=None,
        description="Explicit primary key column name (optional for register_table).",
    )
    filters: dict[str, Any] | None = Field(
        default=None,
        description="Column equality filters for 'query', e.g. {'status': 'active'}.",
    )
    order_by: str | None = Field(
        default=None,
        description="Column to order by for 'query'.",
    )
    limit: int | None = Field(
        default=None,
        description="Maximum number of rows for 'query'.",
    )
    sql: str | None = Field(
        default=None,
        description="Raw SQL SELECT statement for 'sql' command.",
    )
    params: list[Any] | None = Field(
        default=None,
        description="Parameters for the raw SQL statement.",
    )
    seed_data: list[dict[str, Any]] | None = Field(
        default=None,
        description=(
            "Rows to seed into the table after registration "
            "(optional, used with 'register_table')."
        ),
    )


class JanusSQLite(BaseTool):
    """Transactional SQLite tool with MVCC-based virtual branching.

    All reads and writes use the MVCC visibility predicate from the
    current transaction's snapshot.  Combined with the OverlayFS tools,
    this gives the agent a fully transactional environment covering
    both the filesystem and structured data.

    The underlying ``SQLiteShim`` is enrolled in the
    ``TransactionCoordinator`` so it participates in 2PC commit / abort
    and in subtransaction (savepoint) operations.

    Example::

        sqlite = JanusSQLite(shim=my_shim, txn=my_txn)
        sqlite.invoke({
            "command": "register_table",
            "table": "users",
            "columns": ["id TEXT", "name TEXT", "email TEXT"],
        })
        sqlite.invoke({
            "command": "put",
            "table": "users",
            "row": {"id": "1", "name": "Alice", "email": "alice@example.com"},
        })
        result = sqlite.invoke({"command": "get", "table": "users", "pk_value": "1"})
    """

    name: str = "janus_sqlite"
    description: str = (
        "Transactional SQLite database with MVCC isolation. "
        "Supports register_table, put, get, query, delete, list_tables, sql. "
        "All operations are isolated within the current transaction."
    )
    args_schema: type[BaseModel] = JanusSQLiteInput

    shim: SQLiteShim
    """The underlying SQLiteShim."""

    txn: TransactionHandle
    """The current transaction handle (for visibility predicate)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def _run(
        self,
        command: str,
        table: str | None = None,
        row: dict[str, Any] | None = None,
        pk_value: str | None = None,
        columns: list[str] | None = None,
        pk_column: str | None = None,
        filters: dict[str, Any] | None = None,
        order_by: str | None = None,
        limit: int | None = None,
        sql: str | None = None,
        params: list[Any] | None = None,
        seed_data: list[dict[str, Any]] | None = None,
        run_manager: CallbackManagerForToolRun | None = None,
    ) -> str:
        """Execute a SQLite operation."""
        try:
            if command == "register_table":
                return self._handle_register_table(
                    table, columns, pk_column, seed_data
                )
            elif command == "put":
                return self._handle_put(table, row)
            elif command == "get":
                return self._handle_get(table, pk_value)
            elif command == "query":
                return self._handle_query(table, filters, order_by, limit)
            elif command == "delete":
                return self._handle_delete(table, pk_value)
            elif command == "list_tables":
                return self._handle_list_tables()
            elif command == "sql":
                return self._handle_sql(sql, params)
            else:
                return f"Error: Unknown command '{command}'."
        except Exception as e:
            return f"Error: {e}"

    # ── Handlers ─────────────────────────────────────────────────────

    def _handle_register_table(
        self,
        table: str | None,
        columns: list[str] | None,
        pk_column: str | None,
        seed_data: list[dict[str, Any]] | None,
    ) -> str:
        if not table:
            return "Error: 'table' is required for register_table."
        if not columns:
            return "Error: 'columns' is required for register_table."

        self.shim.register_table(table, columns, pk_column)

        msg = f"Table '{table}' registered with columns: {columns}"
        if pk_column:
            msg += f" (pk={pk_column})"

        if seed_data:
            self.shim.seed_data(table, seed_data)
            msg += f"\n  Seeded {len(seed_data)} row(s)."

        return msg

    def _handle_put(
        self, table: str | None, row: dict[str, Any] | None
    ) -> str:
        if not table:
            return "Error: 'table' is required for put."
        if not row:
            return "Error: 'row' is required for put."

        self.shim.put(self.txn, table, row)
        return f"Row inserted/updated in '{table}'."

    def _handle_get(
        self, table: str | None, pk_value: str | None
    ) -> str:
        if not table:
            return "Error: 'table' is required for get."
        if pk_value is None:
            return "Error: 'pk_value' is required for get."

        result = self.shim.get(self.txn, table, pk_value)
        if result is None:
            return f"No row found in '{table}' with pk='{pk_value}'."
        return json.dumps(result, indent=2, default=str)

    def _handle_query(
        self,
        table: str | None,
        filters: dict[str, Any] | None,
        order_by: str | None,
        limit: int | None,
    ) -> str:
        if not table:
            return "Error: 'table' is required for query."

        rows = self.shim.query(
            self.txn, table, filters=filters, order_by=order_by, limit=limit
        )
        if not rows:
            return f"No rows found in '{table}'."

        return json.dumps(rows, indent=2, default=str)

    def _handle_delete(
        self, table: str | None, pk_value: str | None
    ) -> str:
        if not table:
            return "Error: 'table' is required for delete."
        if pk_value is None:
            return "Error: 'pk_value' is required for delete."

        deleted = self.shim.delete(self.txn, table, pk_value)
        if deleted:
            return f"Row deleted from '{table}' (pk='{pk_value}')."
        return f"No row found to delete in '{table}' (pk='{pk_value}')."

    def _handle_list_tables(self) -> str:
        tables = list(self.shim._tables.keys())
        if not tables:
            return "No tables registered."
        lines = ["Registered tables:"]
        for t in sorted(tables):
            meta = self.shim._tables[t]
            lines.append(f"  {t} (pk={meta.pk}, columns={meta.columns})")
        return "\n".join(lines)

    def _handle_sql(
        self, sql: str | None, params: list[Any] | None
    ) -> str:
        if not sql:
            return "Error: 'sql' is required for sql command."

        rows = self.shim.execute_sql(self.txn, sql, params)
        if not rows:
            return "Query returned no results."
        return json.dumps(rows, indent=2, default=str)
