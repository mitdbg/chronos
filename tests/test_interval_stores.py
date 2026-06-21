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


def test_workspace_polystore_duckdb_uses_row_store_metadata() -> None:
    from chronos_core.branching.sql_adapters import DuckDBDatabaseAdapter

    sqlite_store = ChronosBranchContext.connect("sqlite:///:memory:", backend="interval")
    duck_data = DuckDBDatabaseAdapter.connect("duckdb:///:memory:")
    duck_metadata = _sqlite_memory()
    duck_store = ChronosBranchContext.from_database_adapter(
        duck_data,
        backend="interval",
        metadata_db=duck_metadata,
    )
    workspace = ChronosWorkspaceContext(sqlite=sqlite_store, duckdb=duck_store)
    try:
        sqlite_store.db.execute("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)")
        sqlite_store.db.execute("INSERT INTO users VALUES (?, ?)", ("u1", "main"))
        sqlite_store.db.commit()
        sqlite_store.register_table("users", ["id"])

        duck_store.db.execute("CREATE TABLE facts (id TEXT PRIMARY KEY, amount BIGINT)")
        duck_store.db.execute("INSERT INTO facts VALUES (?, ?)", ("f1", 10))
        duck_store.db.commit()
        duck_store.register_table("facts", ["id"])

        workspace.create_branch("agent", from_branch="main")
        agent = workspace.checkout("agent")
        agent.sqlite.execute(
            "UPDATE users SET name = :name WHERE id = :id",
            {"id": "u1", "name": "agent"},
        )
        agent.duckdb.execute(
            "UPDATE facts SET amount = :amount WHERE id = :id",
            {"id": "f1", "amount": 25},
        )

        main = workspace.checkout("main")
        assert main.sqlite.query("SELECT name FROM users WHERE id = :id", {"id": "u1"}) == [
            {"name": "main"}
        ]
        assert main.duckdb.query("SELECT sum(amount) AS total FROM facts") == [
            {"total": 10}
        ]
        assert agent.duckdb.query("SELECT sum(amount) AS total FROM facts") == [
            {"total": 25}
        ]
        assert [dict(row) for row in duck_metadata.execute(
            "SELECT branch_id FROM _chronos_branch_interval_branches WHERE branch_id = ?",
            ("agent",),
        ).fetchall()] == [{"branch_id": "agent"}]
        assert duck_data.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name LIKE '_chronos_branch_interval_%'
            """
        ).fetchall() == []
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
        assert ctx._backend._native_branch_store is not None
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
        assert data.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name LIKE '_chronos_branch_interval_%'
            """
        ).fetchall() == []
        assert ctx.db.execute("SELECT count(*) AS c FROM _chronos_b_interval_docs").fetchone()[
            "c"
        ] >= 2
    finally:
        ctx.close()


def test_duckdb_split_interval_create_index_uses_data_plane() -> None:
    from chronos_core.branching.sql_adapters import DuckDBDatabaseAdapter

    data = DuckDBDatabaseAdapter.connect("duckdb:///:memory:")
    metadata = _sqlite_memory()
    ctx = ChronosBranchContext.from_database_adapter(
        data,
        backend="interval",
        metadata_db=metadata,
    )
    try:
        ctx.db.execute("CREATE TABLE facts (id VARCHAR PRIMARY KEY, amount BIGINT)")
        ctx.db.execute("INSERT INTO facts VALUES (?, ?)", ("f1", 10))
        ctx.db.commit()
        ctx.register_table("facts", ["id"])

        index = ctx.create_index("facts", ["amount"], name="facts_amount")

        assert index.name == "facts_amount"
        assert [dict(row) for row in metadata.execute(
            """
            SELECT index_name, table_name, columns
            FROM _chronos_branch_indexes
            WHERE backend = ? AND index_name = ?
            """,
            ("interval", "facts_amount"),
        ).fetchall()] == [
            {
                "index_name": "facts_amount",
                "table_name": "facts",
                "columns": '["amount"]',
            }
        ]
        assert ctx.db.execute(
            """
            SELECT index_name
            FROM duckdb_indexes()
            WHERE index_name = '_chronos_idx_interval_facts_amount'
            """
        ).fetchall() == [{"index_name": "_chronos_idx_interval_facts_amount"}]
        assert metadata.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'index'
              AND name = ?
            """,
            ("_chronos_idx_interval_facts_amount",),
        ).fetchall() == []
    finally:
        ctx.close()


def test_duckdb_split_interval_store_uses_native_backend_for_file_data(tmp_path) -> None:
    from chronos_core.branching.sql_adapters import DuckDBDatabaseAdapter

    data_url = f"duckdb:///{tmp_path / 'facts.duckdb'}"
    metadata = SQLiteDatabaseAdapter.connect(f"sqlite:///{tmp_path / 'metadata.sqlite'}")
    ctx = ChronosBranchContext.from_database_adapter(
        DuckDBDatabaseAdapter.connect(data_url),
        backend="interval",
        metadata_db=metadata,
    )
    try:
        assert ctx._backend._native_branch_store is not None
        ctx.db.execute("CREATE TABLE facts (id VARCHAR PRIMARY KEY, amount BIGINT)")
        ctx.db.execute("INSERT INTO facts VALUES (?, ?)", ("f1", 10))
        ctx.db.commit()
        ctx.register_table("facts", ["id"])

        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        agent.execute(
            "UPDATE facts SET amount = :amount WHERE id = :id",
            {"id": "f1", "amount": 30},
        )

        assert ctx.checkout("main").query("SELECT sum(amount) AS total FROM facts") == [
            {"total": 10}
        ]
        assert agent.query("SELECT sum(amount) AS total FROM facts") == [{"total": 30}]
        assert [dict(row) for row in metadata.execute(
            "SELECT branch_id FROM _chronos_branch_interval_branches WHERE branch_id = ?",
            ("agent",),
        ).fetchall()] == [{"branch_id": "agent"}]
        assert ctx.db.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = '_chronos_branch_interval_branches'
            """
        ).fetchall() == []
    finally:
        ctx.close()


def test_duckdb_split_interval_native_gc_removes_dead_writer_rows(tmp_path) -> None:
    from chronos_core.branching.sql_adapters import DuckDBDatabaseAdapter
    from chronos_core import _native_interval

    data_url = f"duckdb:///{tmp_path / 'gc.duckdb'}"
    metadata = SQLiteDatabaseAdapter.connect(f"sqlite:///{tmp_path / 'gc_metadata.sqlite'}")
    ctx = ChronosBranchContext.from_database_adapter(
        DuckDBDatabaseAdapter.connect(data_url),
        backend="interval",
        metadata_db=metadata,
    )
    try:
        ctx.db.execute("CREATE TABLE facts (id VARCHAR PRIMARY KEY, amount BIGINT)")
        ctx.db.execute("INSERT INTO facts VALUES (?, ?)", ("f1", 10))
        ctx.db.commit()
        ctx.register_table("facts", ["id"])
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        agent.execute(
            "UPDATE facts SET amount = :amount WHERE id = :id",
            {"id": "f1", "amount": 99},
        )
        writer_segment = int(ctx.get_branch("agent").current_ref)

        def writer_rows() -> list[dict[str, object]]:
            inspector = _native_interval.NativeSqlConnection(data_url)
            return inspector.query_sql_dict(
                "SELECT 1 FROM _chronos_b_interval_facts WHERE writer_segment_id = ?",
                (writer_segment,),
            )

        assert agent.query("SELECT amount FROM facts WHERE id = :id", {"id": "f1"}) == [
            {"amount": 99}
        ]

        ctx.delete_branch("agent")
        ctx.wait_for_background_work()

        assert writer_rows() == []
        assert [dict(row) for row in metadata.execute(
            "SELECT 1 FROM _chronos_branch_interval_segments WHERE segment_id = ?",
            (writer_segment,),
        ).fetchall()] == []
    finally:
        ctx.close()


def test_duckdb_single_store_requires_transactional_metadata(tmp_path) -> None:
    with pytest.raises(ValueError, match="DuckDB interval stores require"):
        ChronosBranchContext.connect(
            f"duckdb:///{tmp_path / 'single.duckdb'}",
            backend="interval",
        )


def test_duckdb_split_interval_store_uses_postgres_metadata() -> None:
    from tests.test_branching import _postgres_dsn, _reset_postgres_schema

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
        assert ctx._backend._native_branch_store is not None
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
        assert ctx.db.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name LIKE '_chronos_branch_interval_%'
            """
        ).fetchall() == []
        assert ctx.db.execute("SELECT count(*) AS c FROM _chronos_b_interval_docs").fetchone()[
            "c"
        ] >= 2
    finally:
        ctx.close()
