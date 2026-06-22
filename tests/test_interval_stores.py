from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from chronos_core.branching import (
    BranchingError,
    ChronosBranchContext,
    MergePolicy,
    MergeResolution,
    MergeValidationResult,
)
from chronos_core.branching.sql_adapters import (
    RoutedIntervalDatabaseAdapter,
    SQLiteDatabaseAdapter,
)
from chronos_core.workspace import ChronosFSStore, ChronosWorkspaceContext


def _sqlite_memory() -> SQLiteDatabaseAdapter:
    return SQLiteDatabaseAdapter.connect("sqlite:///:memory:")


def _make_polystore_workspace() -> ChronosWorkspaceContext:
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

    sqlite_store.db.execute("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)")
    sqlite_store.db.execute("INSERT INTO users VALUES (?, ?)", ("u1", "main"))
    sqlite_store.db.commit()
    sqlite_store.register_table("users", ["id"])

    duck_store.db.execute("CREATE TABLE facts (id TEXT PRIMARY KEY, amount BIGINT)")
    duck_store.db.execute("INSERT INTO facts VALUES (?, ?)", ("f1", 10))
    duck_store.db.commit()
    duck_store.register_table("facts", ["id"])
    return workspace


def _make_polystore_workspace_with_chronosfs(
    tmp_path,
    *,
    row_count: int = 1,
) -> ChronosWorkspaceContext:
    from chronos_core.branching.sql_adapters import DuckDBDatabaseAdapter

    sqlite_store = ChronosBranchContext.connect("sqlite:///:memory:", backend="interval")
    duck_data = DuckDBDatabaseAdapter.connect("duckdb:///:memory:")
    duck_metadata = _sqlite_memory()
    duck_store = ChronosBranchContext.from_database_adapter(
        duck_data,
        backend="interval",
        metadata_db=duck_metadata,
    )
    fs_store = ChronosFSStore.connect(
        f"sqlite:///{tmp_path / 'chronosfs.sqlite'}",
        backend="interval",
        block_size=8,
    )
    fs_store.ensure()

    sqlite_store.db.execute("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)")
    sqlite_store.db.execute("INSERT INTO users VALUES (?, ?)", ("u1", "main"))
    for idx in range(2, row_count + 1):
        sqlite_store.db.execute(
            "INSERT INTO users VALUES (?, ?)",
            (f"u{idx}", "main"),
        )
    sqlite_store.db.commit()
    sqlite_store.register_table("users", ["id"])

    duck_store.db.execute("CREATE TABLE facts (id TEXT PRIMARY KEY, amount BIGINT)")
    duck_store.db.execute("INSERT INTO facts VALUES (?, ?)", ("f1", 10))
    for idx in range(2, row_count + 1):
        duck_store.db.execute("INSERT INTO facts VALUES (?, ?)", (f"f{idx}", 10))
    duck_store.db.commit()
    duck_store.register_table("facts", ["id"])

    fs_store.write_file("main", "/plan.txt", "main\n", parents=True)
    for idx in range(1, row_count + 1):
        fs_store.write_file("main", f"/items/u{idx}.txt", "main\n", parents=True)
    return ChronosWorkspaceContext(
        sqlite=sqlite_store,
        duckdb=duck_store,
        filesystem=fs_store,
    )


def _open_file_polystore_workspace_with_chronosfs(
    sqlite_url: str,
    duckdb_url: str,
    duckdb_metadata_url: str,
    chronosfs_url: str,
) -> ChronosWorkspaceContext:
    sqlite_store = ChronosBranchContext.connect(sqlite_url, backend="interval")
    duck_store = ChronosBranchContext.connect_split(
        duckdb_url,
        duckdb_metadata_url,
        backend="interval",
    )
    fs_store = ChronosFSStore.connect(
        chronosfs_url,
        backend="interval",
        block_size=8,
    )
    fs_store.ensure()
    return ChronosWorkspaceContext(
        sqlite=sqlite_store,
        duckdb=duck_store,
        filesystem=fs_store,
    )


def _make_file_polystore_workspace_with_chronosfs(
    tmp_path,
    *,
    row_count: int,
):
    sqlite_url = f"sqlite:///{tmp_path / 'sqlite.db'}"
    duckdb_url = f"duckdb:///{tmp_path / 'facts.duckdb'}"
    duckdb_metadata_url = f"sqlite:///{tmp_path / 'duckdb_metadata.db'}"
    chronosfs_url = f"sqlite:///{tmp_path / 'chronosfs.sqlite'}"

    def open_workspace() -> ChronosWorkspaceContext:
        return _open_file_polystore_workspace_with_chronosfs(
            sqlite_url,
            duckdb_url,
            duckdb_metadata_url,
            chronosfs_url,
        )

    workspace = open_workspace()
    sqlite_store = workspace.stores["sqlite"]
    duck_store = workspace.stores["duckdb"]
    fs_store = workspace.filesystem
    assert fs_store is not None

    sqlite_store.db.execute("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)")
    for idx in range(1, row_count + 1):
        sqlite_store.db.execute(
            "INSERT INTO users VALUES (?, ?)",
            (f"u{idx}", "main"),
        )
    sqlite_store.db.commit()
    sqlite_store.register_table("users", ["id"])

    duck_store.db.execute("CREATE TABLE facts (id TEXT PRIMARY KEY, amount BIGINT)")
    for idx in range(1, row_count + 1):
        duck_store.db.execute("INSERT INTO facts VALUES (?, ?)", (f"f{idx}", 10))
    duck_store.db.commit()
    duck_store.register_table("facts", ["id"])

    fs_store.write_file("main", "/plan.txt", "main\n", parents=True)
    for idx in range(1, row_count + 1):
        fs_store.write_file("main", f"/items/u{idx}.txt", "main\n", parents=True)
    return workspace, open_workspace


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


def test_workspace_polystore_snapshot_isolation_prevalidates_all_stores() -> None:
    workspace = _make_polystore_workspace()
    try:
        workspace.create_branch("agent", from_branch="main")
        agent = workspace.checkout("agent")
        main = workspace.checkout("main")

        agent.sqlite.execute(
            "UPDATE users SET name = :name WHERE id = :id",
            {"id": "u1", "name": "agent"},
        )
        agent.duckdb.execute(
            "UPDATE facts SET amount = :amount WHERE id = :id",
            {"id": "f1", "amount": 25},
        )
        main.duckdb.execute(
            "UPDATE facts SET amount = :amount WHERE id = :id",
            {"id": "f1", "amount": 30},
        )

        with pytest.raises(BranchingError, match="duckdb"):
            workspace.merge_apply("agent", "main", policy="snapshot_isolation")

        assert main.sqlite.query("SELECT name FROM users WHERE id = :id", {"id": "u1"}) == [
            {"name": "main"}
        ]
        assert main.duckdb.query(
            "SELECT amount FROM facts WHERE id = :id", {"id": "f1"}
        ) == [{"amount": 30}]
    finally:
        workspace.close()


def test_workspace_polystore_weak_snapshot_isolation_source_wins() -> None:
    workspace = _make_polystore_workspace()
    try:
        workspace.create_branch("agent", from_branch="main")
        agent = workspace.checkout("agent")
        main = workspace.checkout("main")

        agent.sqlite.execute(
            "UPDATE users SET name = :name WHERE id = :id",
            {"id": "u1", "name": "agent"},
        )
        agent.duckdb.execute(
            "UPDATE facts SET amount = :amount WHERE id = :id",
            {"id": "f1", "amount": 25},
        )
        main.sqlite.execute(
            "UPDATE users SET name = :name WHERE id = :id",
            {"id": "u1", "name": "main-updated"},
        )
        main.duckdb.execute(
            "UPDATE facts SET amount = :amount WHERE id = :id",
            {"id": "f1", "amount": 30},
        )

        result = workspace.merge_apply(
            "agent",
            "main",
            policy="weak_snapshot_isolation",
        )

        assert result["sqlite"].applied == 1
        assert result["duckdb"].applied == 1
        refreshed = workspace.checkout("main")
        assert refreshed.sqlite.query(
            "SELECT name FROM users WHERE id = :id", {"id": "u1"}
        ) == [{"name": "agent"}]
        assert refreshed.duckdb.query(
            "SELECT amount FROM facts WHERE id = :id", {"id": "f1"}
        ) == [{"amount": 25}]
    finally:
        workspace.close()


def test_workspace_polystore_manual_review_resolution_spans_stores() -> None:
    workspace = _make_polystore_workspace()
    try:
        workspace.create_branch("agent", from_branch="main")
        agent = workspace.checkout("agent")
        main = workspace.checkout("main")

        agent.sqlite.execute(
            "UPDATE users SET name = :name WHERE id = :id",
            {"id": "u1", "name": "agent"},
        )
        agent.duckdb.execute(
            "UPDATE facts SET amount = :amount WHERE id = :id",
            {"id": "f1", "amount": 25},
        )
        main.sqlite.execute(
            "UPDATE users SET name = :name WHERE id = :id",
            {"id": "u1", "name": "main-updated"},
        )
        main.duckdb.execute(
            "UPDATE facts SET amount = :amount WHERE id = :id",
            {"id": "f1", "amount": 30},
        )

        previews = workspace.merge_preview("agent", "main", policy="manual_review")
        assert len(previews["sqlite"].conflicts) == 1
        assert len(previews["duckdb"].conflicts) == 1

        combined_resolution = MergeResolution(
            {
                conflict.conflict_id: "source"
                for preview in previews.values()
                for conflict in preview.conflicts
                if conflict.conflict_id is not None
            }
        )
        result = workspace.merge_apply(
            "agent",
            "main",
            combined_resolution,
            policy="manual_review",
        )

        assert result["sqlite"].applied == 1
        assert result["duckdb"].applied == 1
        refreshed = workspace.checkout("main")
        assert refreshed.sqlite.query(
            "SELECT name FROM users WHERE id = :id", {"id": "u1"}
        ) == [{"name": "agent"}]
        assert refreshed.duckdb.query(
            "SELECT amount FROM facts WHERE id = :id", {"id": "f1"}
        ) == [{"amount": 25}]
    finally:
        workspace.close()


def test_workspace_polystore_manual_review_requires_each_store_resolution() -> None:
    workspace = _make_polystore_workspace()
    try:
        workspace.create_branch("agent", from_branch="main")
        agent = workspace.checkout("agent")
        main = workspace.checkout("main")

        agent.sqlite.execute(
            "UPDATE users SET name = :name WHERE id = :id",
            {"id": "u1", "name": "agent"},
        )
        agent.duckdb.execute(
            "UPDATE facts SET amount = :amount WHERE id = :id",
            {"id": "f1", "amount": 25},
        )
        main.sqlite.execute(
            "UPDATE users SET name = :name WHERE id = :id",
            {"id": "u1", "name": "main-updated"},
        )
        main.duckdb.execute(
            "UPDATE facts SET amount = :amount WHERE id = :id",
            {"id": "f1", "amount": 30},
        )

        previews = workspace.merge_preview("agent", "main", policy="manual_review")
        sqlite_conflict = previews["sqlite"].conflicts[0]
        assert sqlite_conflict.conflict_id is not None

        with pytest.raises(BranchingError, match="duckdb"):
            workspace.merge_apply(
                "agent",
                "main",
                MergeResolution({sqlite_conflict.conflict_id: "source"}),
                policy="manual_review",
            )

        assert main.sqlite.query("SELECT name FROM users WHERE id = :id", {"id": "u1"}) == [
            {"name": "main-updated"}
        ]
        assert main.duckdb.query(
            "SELECT amount FROM facts WHERE id = :id", {"id": "f1"}
        ) == [{"amount": 30}]
    finally:
        workspace.close()


def test_workspace_polystore_custom_validator_rejects_before_any_store_publish() -> None:
    class RejectLargeDuckDBAmount:
        def validate(self, context, preview):
            for change in preview.changes:
                if change.after is not None and change.after.get("amount", 0) > 20:
                    return MergeValidationResult.reject("amount too high")
            return MergeValidationResult.accept()

    workspace = _make_polystore_workspace()
    try:
        workspace.create_branch("agent", from_branch="main")
        agent = workspace.checkout("agent")
        main = workspace.checkout("main")

        agent.sqlite.execute(
            "UPDATE users SET name = :name WHERE id = :id",
            {"id": "u1", "name": "agent"},
        )
        agent.duckdb.execute(
            "UPDATE facts SET amount = :amount WHERE id = :id",
            {"id": "f1", "amount": 25},
        )
        policy = MergePolicy(
            name="reject_large_duckdb_amount",
            mode="custom",
            validators=(RejectLargeDuckDBAmount(),),
        )

        with pytest.raises(BranchingError, match="duckdb.*amount too high"):
            workspace.merge_apply("agent", "main", policy=policy)

        assert main.sqlite.query("SELECT name FROM users WHERE id = :id", {"id": "u1"}) == [
            {"name": "main"}
        ]
        assert main.duckdb.query(
            "SELECT amount FROM facts WHERE id = :id", {"id": "f1"}
        ) == [{"amount": 10}]
    finally:
        workspace.close()


def test_workspace_polystore_transaction_commits_chronosfs_with_sql_stores(tmp_path) -> None:
    workspace = _make_polystore_workspace_with_chronosfs(tmp_path)
    try:
        workspace.create_branch("agent", from_branch="main")
        agent = workspace.checkout("agent")
        assert agent.fs is not None

        agent.sqlite.execute(
            "UPDATE users SET name = :name WHERE id = :id",
            {"id": "u1", "name": "agent"},
        )
        agent.duckdb.execute(
            "UPDATE facts SET amount = :amount WHERE id = :id",
            {"id": "f1", "amount": 25},
        )
        agent.fs.write_file("/plan.txt", "agent\n")
        agent.fs.write_file("/notes/private.txt", "private\n", parents=True)

        main = workspace.checkout("main")
        assert main.fs is not None
        assert main.sqlite.query("SELECT name FROM users WHERE id = :id", {"id": "u1"}) == [
            {"name": "main"}
        ]
        assert main.duckdb.query(
            "SELECT amount FROM facts WHERE id = :id", {"id": "f1"}
        ) == [{"amount": 10}]
        assert main.fs.read_text("/plan.txt") == "main\n"
        assert not main.fs.exists("/notes/private.txt")

        result = workspace.merge_apply("agent", "main")

        assert result["filesystem"].applied >= 2
        assert result["sqlite"].applied == 1
        assert result["duckdb"].applied == 1
        refreshed = workspace.checkout("main")
        assert refreshed.fs is not None
        assert refreshed.sqlite.query(
            "SELECT name FROM users WHERE id = :id", {"id": "u1"}
        ) == [{"name": "agent"}]
        assert refreshed.duckdb.query(
            "SELECT amount FROM facts WHERE id = :id", {"id": "f1"}
        ) == [{"amount": 25}]
        assert refreshed.fs.read_text("/plan.txt") == "agent\n"
        assert refreshed.fs.read_text("/notes/private.txt") == "private\n"
    finally:
        workspace.close()


def test_workspace_create_branch_from_checkpoint_with_chronosfs(tmp_path) -> None:
    workspace = _make_polystore_workspace_with_chronosfs(tmp_path)
    try:
        workspace.create_branch("agent", from_branch="main")
        agent = workspace.checkout("agent")
        assert agent.fs is not None
        agent.sqlite.execute(
            "UPDATE users SET name = :name WHERE id = :id",
            {"id": "u1", "name": "agent"},
        )
        agent.fs.write_file("/plan.txt", "agent\n")

        workspace.create_checkpoint("snap", branch="agent")
        workspace.create_branch_from_checkpoint("retry", checkpoint="snap")
        retry = workspace.checkout("retry")

        assert retry.fs is not None
        assert retry.sqlite.query(
            "SELECT name FROM users WHERE id = :id",
            {"id": "u1"},
        ) == [{"name": "agent"}]
        assert retry.fs.read_text("/plan.txt") == "agent\n"
    finally:
        workspace.close()


def test_workspace_polystore_sql_conflict_leaves_chronosfs_private(tmp_path) -> None:
    workspace = _make_polystore_workspace_with_chronosfs(tmp_path)
    try:
        workspace.create_branch("agent", from_branch="main")
        agent = workspace.checkout("agent")
        main = workspace.checkout("main")
        assert agent.fs is not None
        assert main.fs is not None

        agent.sqlite.execute(
            "UPDATE users SET name = :name WHERE id = :id",
            {"id": "u1", "name": "agent"},
        )
        agent.fs.write_file("/plan.txt", "agent\n")
        agent.fs.write_file("/notes/private.txt", "private\n", parents=True)
        main.sqlite.execute(
            "UPDATE users SET name = :name WHERE id = :id",
            {"id": "u1", "name": "main-updated"},
        )

        with pytest.raises(BranchingError, match="sqlite"):
            workspace.merge_apply("agent", "main")

        refreshed_main = workspace.checkout("main")
        refreshed_agent = workspace.checkout("agent")
        assert refreshed_main.fs is not None
        assert refreshed_agent.fs is not None
        assert refreshed_main.sqlite.query(
            "SELECT name FROM users WHERE id = :id", {"id": "u1"}
        ) == [{"name": "main-updated"}]
        assert refreshed_main.fs.read_text("/plan.txt") == "main\n"
        assert not refreshed_main.fs.exists("/notes/private.txt")
        assert refreshed_agent.fs.read_text("/plan.txt") == "agent\n"
        assert refreshed_agent.fs.read_text("/notes/private.txt") == "private\n"
    finally:
        workspace.close()


def test_workspace_polystore_weak_snapshot_resolves_chronosfs_block_conflict(
    tmp_path,
) -> None:
    workspace = _make_polystore_workspace_with_chronosfs(tmp_path)
    try:
        assert workspace.filesystem is not None
        workspace.filesystem.write_file("main", "/blob.bin", b"aaaaaaaabbbbbbbb")
        workspace.create_branch("agent", from_branch="main")
        agent = workspace.checkout("agent")
        main = workspace.checkout("main")
        assert agent.fs is not None
        assert main.fs is not None

        agent.sqlite.execute(
            "UPDATE users SET name = :name WHERE id = :id",
            {"id": "u1", "name": "agent"},
        )
        main.sqlite.execute(
            "UPDATE users SET name = :name WHERE id = :id",
            {"id": "u1", "name": "main-updated"},
        )
        agent.fs.write_at("/blob.bin", 8, b"AGNT")
        main.fs.write_at("/blob.bin", 8, b"MAIN")

        previews = workspace.merge_preview("agent", "main", policy="manual_review")
        filesystem_conflicts = [
            conflict
            for conflict in previews["filesystem"].conflicts
            if conflict.table == "chronosfs_file_range"
        ]
        assert len(filesystem_conflicts) == 1
        assert filesystem_conflicts[0].key == {
            "path": "/blob.bin",
            "byte_range": {"start": 8, "end": 16},
        }
        assert filesystem_conflicts[0].after is not None
        assert "unified_diff" in filesystem_conflicts[0].after
        assert "block_index" not in repr(filesystem_conflicts[0])

        result = workspace.merge_apply(
            "agent",
            "main",
            policy="weak_snapshot_isolation",
        )

        assert result["filesystem"].applied >= 1
        assert result["sqlite"].applied == 1
        refreshed = workspace.checkout("main")
        assert refreshed.fs is not None
        assert refreshed.fs.read_file("/blob.bin") == b"aaaaaaaaAGNTbbbb"
        assert refreshed.sqlite.query(
            "SELECT name FROM users WHERE id = :id",
            {"id": "u1"},
        ) == [{"name": "agent"}]
    finally:
        workspace.close()


def test_workspace_polystore_concurrent_disjoint_branch_transactions_all_stores(
    tmp_path,
) -> None:
    worker_count = 6
    workspace, open_workspace = _make_file_polystore_workspace_with_chronosfs(
        tmp_path,
        row_count=worker_count,
    )
    try:
        for idx in range(1, worker_count + 1):
            workspace.create_branch(f"agent_{idx}", from_branch="main")
        worker_workspaces = [open_workspace() for _ in range(worker_count)]
        try:

            def run_branch(idx: int) -> tuple[int, dict[str, int]]:
                branch_id = f"agent_{idx}"
                worker_workspace = worker_workspaces[idx - 1]
                branch = worker_workspace.checkout(branch_id)
                assert branch.fs is not None
                branch.sqlite.execute(
                    "UPDATE users SET name = :name WHERE id = :id",
                    {"id": f"u{idx}", "name": branch_id},
                )
                branch.duckdb.execute(
                    "UPDATE facts SET amount = :amount WHERE id = :id",
                    {"id": f"f{idx}", "amount": 100 + idx},
                )
                branch.fs.write_file(f"/items/u{idx}.txt", f"{branch_id}\n")
                result = worker_workspace.merge_apply(branch_id, "main")
                return idx, {
                    "sqlite": result["sqlite"].applied,
                    "duckdb": result["duckdb"].applied,
                    "filesystem": result["filesystem"].applied,
                }

            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [
                    executor.submit(run_branch, idx)
                    for idx in range(1, worker_count + 1)
                ]
                results = [future.result() for future in as_completed(futures)]

            assert sorted(idx for idx, _ in results) == list(range(1, worker_count + 1))
            for _, applied in results:
                assert applied["sqlite"] == 1
                assert applied["duckdb"] == 1
                assert applied["filesystem"] >= 1

            reader = open_workspace()
            try:
                main = reader.checkout("main")
                assert main.fs is not None
                for idx in range(1, worker_count + 1):
                    assert main.sqlite.query(
                        "SELECT name FROM users WHERE id = :id",
                        {"id": f"u{idx}"},
                    ) == [{"name": f"agent_{idx}"}]
                    assert main.duckdb.query(
                        "SELECT amount FROM facts WHERE id = :id",
                        {"id": f"f{idx}"},
                    ) == [{"amount": 100 + idx}]
                    assert main.fs.read_text(f"/items/u{idx}.txt") == f"agent_{idx}\n"
            finally:
                reader.close()
        finally:
            for worker_workspace in worker_workspaces:
                worker_workspace.close()
    finally:
        workspace.close()


def test_workspace_polystore_concurrent_conflicting_branch_transactions_all_stores(
    tmp_path,
) -> None:
    workspace, open_workspace = _make_file_polystore_workspace_with_chronosfs(
        tmp_path,
        row_count=1,
    )
    try:
        workspace.create_branch("agent_a", from_branch="main")
        workspace.create_branch("agent_b", from_branch="main")
        worker_workspaces = {
            "agent_a": open_workspace(),
            "agent_b": open_workspace(),
        }
        try:

            def run_branch(branch_id: str, value: str, amount: int) -> tuple[str, str]:
                worker_workspace = worker_workspaces[branch_id]
                branch = worker_workspace.checkout(branch_id)
                assert branch.fs is not None
                branch.sqlite.execute(
                    "UPDATE users SET name = :name WHERE id = :id",
                    {"id": "u1", "name": value},
                )
                branch.duckdb.execute(
                    "UPDATE facts SET amount = :amount WHERE id = :id",
                    {"id": "f1", "amount": amount},
                )
                branch.fs.write_file("/plan.txt", f"{value}\n")
                try:
                    worker_workspace.merge_apply(branch_id, "main")
                except BranchingError:
                    return branch_id, "conflict"
                return branch_id, "committed"

            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(run_branch, "agent_a", "a", 11),
                    executor.submit(run_branch, "agent_b", "b", 12),
                ]
                outcomes = dict(future.result() for future in as_completed(futures))

            assert sorted(outcomes.values()) == ["committed", "conflict"]
            committed_branch = next(
                branch_id
                for branch_id, outcome in outcomes.items()
                if outcome == "committed"
            )
            expected_value = "a" if committed_branch == "agent_a" else "b"
            expected_amount = 11 if committed_branch == "agent_a" else 12

            reader = open_workspace()
            try:
                main = reader.checkout("main")
                assert main.fs is not None
                assert main.sqlite.query(
                    "SELECT name FROM users WHERE id = :id",
                    {"id": "u1"},
                ) == [{"name": expected_value}]
                assert main.duckdb.query(
                    "SELECT amount FROM facts WHERE id = :id",
                    {"id": "f1"},
                ) == [{"amount": expected_amount}]
                assert main.fs.read_text("/plan.txt") == f"{expected_value}\n"
            finally:
                reader.close()
        finally:
            for worker_workspace in worker_workspaces.values():
                worker_workspace.close()
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


def test_duckdb_split_interval_merge_apply_uses_row_store_commit_metadata() -> None:
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

        before = metadata.execute(
            """
            SELECT current_segment_id
            FROM _chronos_branch_interval_branches
            WHERE branch_id = ?
            """,
            ("main",),
        ).fetchone()
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        agent.execute(
            "UPDATE facts SET amount = :amount WHERE id = :id",
            {"id": "f1", "amount": 25},
        )

        result = ctx.merge_apply(source="agent", target="main")

        after = metadata.execute(
            """
            SELECT current_segment_id
            FROM _chronos_branch_interval_branches
            WHERE branch_id = ?
            """,
            ("main",),
        ).fetchone()
        assert result.applied == 1
        assert int(after["current_segment_id"]) != int(before["current_segment_id"])
        assert metadata.execute(
            "SELECT count(*) AS c FROM _chronos_branch_transaction_commits"
        ).fetchone()["c"] == 0
        assert ctx.checkout("main").query("SELECT sum(amount) AS total FROM facts") == [
            {"total": 25}
        ]
        assert ctx.db.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = '_chronos_branch_transaction_commits'
            """
        ).fetchall() == []
    finally:
        ctx.close()


def test_duckdb_split_interval_commit_record_blocks_competing_merge() -> None:
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
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        agent.execute(
            "UPDATE facts SET amount = :amount WHERE id = :id",
            {"id": "f1", "amount": 25},
        )
        target = metadata.execute(
            """
            SELECT current_segment_id
            FROM _chronos_branch_interval_branches
            WHERE branch_id = ?
            """,
            ("main",),
        ).fetchone()
        metadata.execute(
            """
            INSERT INTO _chronos_branch_transaction_commits
            (merge_segment_id, continuation_segment_id, target_branch_id,
             old_target_segment_id, source_branch_id, participant_stores, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                999999,
                1000000,
                "main",
                int(target["current_segment_id"]),
                "agent",
                '["duckdb"]',
                "test",
                "{}",
            ),
        )
        metadata.commit()

        with pytest.raises(Exception, match="write-write conflict|commit already in progress"):
            ctx.merge_apply(source="agent", target="main")

        assert ctx.checkout("main").query("SELECT amount FROM facts WHERE id = :id", {"id": "f1"}) == [
            {"amount": 10}
        ]
    finally:
        try:
            metadata.execute(
                "DELETE FROM _chronos_branch_transaction_commits WHERE target_branch_id = ?",
                ("main",),
            )
            metadata.commit()
        except Exception:
            pass
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
