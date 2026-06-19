from __future__ import annotations

import pytest

from chronos_core.branching import ChronosBranchContext
from chronos_core.branching.sql_adapters import (
    RoutedIntervalDatabaseAdapter,
    SQLiteDatabaseAdapter,
)
from chronos_core.workspace import ChronosWorkspaceContext


def _sqlite_memory() -> SQLiteDatabaseAdapter:
    return SQLiteDatabaseAdapter.connect("sqlite:///:memory:")


def test_routed_interval_adapter_sends_metadata_sql_to_metadata_db() -> None:
    data = _sqlite_memory()
    metadata = _sqlite_memory()
    routed = RoutedIntervalDatabaseAdapter(data, metadata)
    try:
        routed.execute(
            """
            CREATE TABLE _chronos_branch_tables (
              table_name TEXT PRIMARY KEY,
              physical_table TEXT,
              pk_columns TEXT,
              columns TEXT,
              column_defs TEXT,
              backend TEXT
            )
            """
        )
        routed.execute(
            """
            INSERT INTO _chronos_branch_tables
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("docs", "_chronos_b_interval_docs", '["id"]', '["id"]', '["id TEXT"]', "interval"),
        )
        routed.commit()

        assert [dict(row) for row in metadata.execute(
            "SELECT table_name FROM _chronos_branch_tables"
        ).fetchall()] == [{"table_name": "docs"}]
        with pytest.raises(Exception):
            data.execute("SELECT table_name FROM _chronos_branch_tables").fetchall()
    finally:
        routed.close()


def test_workspace_named_stores_are_additive_and_branch_isolated() -> None:
    ctx = ChronosBranchContext.connect("sqlite:///:memory:", backend="interval")
    workspace = ChronosWorkspaceContext(postgresql=ctx)
    try:
        ctx.db.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        ctx.db.execute("INSERT INTO docs VALUES (?, ?)", ("d1", "main"))
        ctx.db.commit()
        ctx.register_table("docs", ["id"])

        workspace.create_branch("agent", from_branch="main")
        agent = workspace.checkout("agent")
        assert agent.fs is None
        assert agent.stores is not None
        assert agent.postgresql is agent.stores["postgresql"]
        agent.postgresql.execute(
            "UPDATE docs SET body = :body WHERE id = :id",
            {"id": "d1", "body": "agent"},
        )

        main = workspace.checkout("main")
        assert main.stores is not None
        assert main.postgresql.query(
            "SELECT body FROM docs WHERE id = :id", {"id": "d1"}
        ) == [{"body": "main"}]
        assert agent.postgresql.query(
            "SELECT body FROM docs WHERE id = :id", {"id": "d1"}
        ) == [{"body": "agent"}]
        assert workspace.diff("main", "agent")["postgresql"].changes
    finally:
        workspace.close()


def test_workspace_store_attribute_names_reject_session_collisions() -> None:
    ctx = ChronosBranchContext.connect("sqlite:///:memory:", backend="interval")
    try:
        with pytest.raises(ValueError, match="session attributes"):
            ChronosWorkspaceContext(transaction=ctx)
    finally:
        ctx.close()


def test_workspace_named_store_transaction_rolls_back() -> None:
    ctx = ChronosBranchContext.connect("sqlite:///:memory:", backend="interval")
    workspace = ChronosWorkspaceContext(postgresql=ctx)
    try:
        ctx.db.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        ctx.db.execute("INSERT INTO docs VALUES (?, ?)", ("d1", "main"))
        ctx.db.commit()
        ctx.register_table("docs", ["id"])
        workspace.create_branch("agent", from_branch="main")
        agent = workspace.checkout("agent")
        assert agent.stores is not None

        with pytest.raises(RuntimeError):
            with agent.transaction():
                agent.postgresql.execute(
                    "UPDATE docs SET body = :body WHERE id = :id",
                    {"id": "d1", "body": "agent"},
                )
                raise RuntimeError("rollback")

        refreshed = workspace.checkout("agent")
        assert refreshed.stores is not None
        assert refreshed.postgresql.query(
            "SELECT body FROM docs WHERE id = :id", {"id": "d1"}
        ) == [{"body": "main"}]
    finally:
        workspace.close()


def test_duckdb_adapter() -> None:
    from chronos_core.branching.sql_adapters import DuckDBDatabaseAdapter

    db = DuckDBDatabaseAdapter.connect("duckdb:///:memory:")
    try:
        db.execute("CREATE TABLE docs (id VARCHAR PRIMARY KEY, score INTEGER)")
        db.execute("INSERT INTO docs VALUES (:id, :score)", {"id": "d1", "score": 7})
        db.commit()
        assert db.execute("SELECT score FROM docs WHERE id = :id", {"id": "d1"}).fetchall() == [
            {"score": 7}
        ]
        assert db.table_defs("docs")[0] == ("id", "score")
    finally:
        db.close()


def test_duckdb_split_interval_store_with_local_metadata_adapter() -> None:
    from chronos_core.branching.sql_adapters import DuckDBDatabaseAdapter

    data = DuckDBDatabaseAdapter.connect("duckdb:///:memory:")
    metadata = _sqlite_memory()
    ctx = ChronosBranchContext.from_database_adapter(
        data,
        backend="interval",
        metadata_db=metadata,
    )
    try:
        ctx.db.execute("CREATE TABLE docs (id VARCHAR PRIMARY KEY, score INTEGER)")
        ctx.db.execute("INSERT INTO docs VALUES (?, ?)", ("d1", 1))
        ctx.db.commit()
        ctx.register_table("docs", ["id"])

        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        agent.execute(
            "UPDATE docs SET score = :score WHERE id = :id",
            {"id": "d1", "score": 5},
        )

        assert ctx.checkout("main").query("SELECT sum(score) AS total FROM docs") == [
            {"total": 1}
        ]
        assert agent.query("SELECT sum(score) AS total FROM docs") == [{"total": 5}]
        assert [dict(row) for row in metadata.execute(
            "SELECT branch_id FROM _chronos_branch_interval_branches WHERE branch_id = ?",
            ("agent",),
        ).fetchall()] == [{"branch_id": "agent"}]
        assert ctx.db.execute("SELECT count(*) AS c FROM _chronos_b_interval_docs").fetchone()[
            "c"
        ] >= 2
    finally:
        ctx.close()


def test_duckdb_split_interval_store_uses_postgres_metadata() -> None:
    from test_branching import _postgres_dsn, _reset_postgres_schema

    _reset_postgres_schema()
    ctx = ChronosBranchContext.connect_split(
        "duckdb:///:memory:",
        _postgres_dsn(),
        backend="interval",
    )
    try:
        ctx.db.execute("CREATE TABLE docs (id VARCHAR PRIMARY KEY, score INTEGER)")
        ctx.db.execute("INSERT INTO docs VALUES (?, ?)", ("d1", 1))
        ctx.db.commit()
        ctx.register_table("docs", ["id"])

        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        agent.execute(
            "UPDATE docs SET score = :score WHERE id = :id",
            {"id": "d1", "score": 3},
        )
        assert ctx.checkout("main").query("SELECT sum(score) AS total FROM docs") == [
            {"total": 1}
        ]
        assert agent.query("SELECT sum(score) AS total FROM docs") == [{"total": 3}]

        assert ctx.metadata_db.execute(
            "SELECT branch_id FROM _chronos_branch_interval_branches WHERE branch_id = ?",
            ("agent",),
        ).fetchall() == [{"branch_id": "agent"}]
        assert ctx.db.execute("SELECT count(*) AS c FROM _chronos_b_interval_docs").fetchone()[
            "c"
        ] >= 2
    finally:
        ctx.close()
