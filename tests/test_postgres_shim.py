import os
import uuid

import pytest

from janus_core.transaction import PostgresShim, TransactionCoordinator
from janus_core.transaction.coordinator import CommitConflictError
from janus_core.transaction.shim_postgres import WriteConflictError


def _dsn() -> str:
    dsn = os.environ.get("JANUS_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set JANUS_POSTGRES_DSN to run PostgreSQL shim tests")
    return dsn


def _table(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def _drop_table(dsn: str, table: str) -> None:
    import psycopg

    with psycopg.connect(dsn) as conn:
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.commit()


def _cleanup(dsn: str, table: str, shim: PostgresShim | None) -> None:
    if shim is not None:
        shim.close()
    _drop_table(dsn, table)


def _coordinator_with_users(table: str) -> tuple[PostgresShim, TransactionCoordinator]:
    shim = PostgresShim(_dsn())
    shim.register_table(
        table,
        ["id TEXT", "name TEXT", "credits INTEGER"],
        pk_column="id",
    )
    shim.seed_data(
        table,
        [{"id": "u1", "name": "Alice", "credits": 100}],
    )
    coordinator = TransactionCoordinator()
    coordinator.register_shim(shim)
    return shim, coordinator


def test_postgres_shim_commits_visible_update() -> None:
    dsn = _dsn()
    table = _table("tar_pg_users")
    shim: PostgresShim | None = None
    try:
        shim, coordinator = _coordinator_with_users(table)

        txn = coordinator.begin()
        before = shim.get(txn, table, "u1")
        assert before is not None
        assert before["credits"] == 100

        shim.put(txn, table, {"id": "u1", "name": "Alice", "credits": 90})
        in_txn = shim.get(txn, table, "u1")
        assert in_txn is not None
        assert in_txn["credits"] == 90

        coordinator.commit(txn.id)

        txn2 = coordinator.begin()
        after = shim.get(txn2, table, "u1")
        assert after is not None
        assert after["credits"] == 90
        coordinator.rollback(txn2.id)
    finally:
        _cleanup(dsn, table, shim)


def test_postgres_shim_rollback_discards_update() -> None:
    dsn = _dsn()
    table = _table("tar_pg_items")
    shim: PostgresShim | None = None
    try:
        shim = PostgresShim(dsn)
        shim.register_table(table, ["id TEXT", "value TEXT"], pk_column="id")
        shim.seed_data(table, [{"id": "i1", "value": "base"}])
        coordinator = TransactionCoordinator()
        coordinator.register_shim(shim)

        txn = coordinator.begin()
        shim.put(txn, table, {"id": "i1", "value": "changed"})
        coordinator.rollback(txn.id)

        txn2 = coordinator.begin()
        row = shim.get(txn2, table, "i1")
        assert row is not None
        assert row["value"] == "base"
        coordinator.rollback(txn2.id)
    finally:
        _cleanup(dsn, table, shim)


def test_postgres_shim_delete_hides_row_after_commit() -> None:
    dsn = _dsn()
    table = _table("tar_pg_delete")
    shim: PostgresShim | None = None
    try:
        shim, coordinator = _coordinator_with_users(table)

        txn = coordinator.begin()
        assert shim.delete(txn, table, "u1") is True
        assert shim.get(txn, table, "u1") is None
        coordinator.commit(txn.id)

        txn2 = coordinator.begin()
        assert shim.get(txn2, table, "u1") is None
        coordinator.rollback(txn2.id)
    finally:
        _cleanup(dsn, table, shim)


def test_postgres_shim_query_deduplicates_latest_visible_versions() -> None:
    dsn = _dsn()
    table = _table("tar_pg_query")
    shim: PostgresShim | None = None
    try:
        shim, coordinator = _coordinator_with_users(table)

        txn = coordinator.begin()
        shim.put(txn, table, {"id": "u1", "name": "Alice", "credits": 80})
        shim.put(txn, table, {"id": "u2", "name": "Bob", "credits": 50})
        coordinator.commit(txn.id)

        txn2 = coordinator.begin()
        rows = shim.query(txn2, table, order_by="id")
        assert rows == [
            {"id": "u1", "name": "Alice", "credits": 80},
            {"id": "u2", "name": "Bob", "credits": 50},
        ]
        filtered = shim.query(txn2, table, filters={"id": "u2"})
        assert filtered == [{"id": "u2", "name": "Bob", "credits": 50}]
        coordinator.rollback(txn2.id)
    finally:
        _cleanup(dsn, table, shim)


def test_postgres_shim_rejects_stale_lost_update() -> None:
    dsn = _dsn()
    table = _table("tar_pg_conflict")
    shim: PostgresShim | None = None
    try:
        shim, coordinator = _coordinator_with_users(table)

        stale = coordinator.begin()
        fresh = coordinator.begin()

        shim.put(fresh, table, {"id": "u1", "name": "Alice", "credits": 90})
        coordinator.commit(fresh.id)

        with pytest.raises(WriteConflictError):
            shim.put(stale, table, {"id": "u1", "name": "Alice", "credits": 80})

        coordinator.rollback(stale.id)
    finally:
        _cleanup(dsn, table, shim)


def test_postgres_shim_prepare_detects_stale_write_when_locks_are_weak() -> None:
    dsn = _dsn()
    table = _table("tar_pg_prepare_conflict")
    shim: PostgresShim | None = None
    try:
        shim = PostgresShim(
            dsn,
            enforce_write_locks=False,
            enforce_snapshot_validation=True,
        )
        shim.register_table(
            table,
            ["id TEXT", "name TEXT", "credits INTEGER"],
            pk_column="id",
        )
        shim.seed_data(table, [{"id": "u1", "name": "Alice", "credits": 100}])
        coordinator = TransactionCoordinator()
        coordinator.register_shim(shim)

        stale = coordinator.begin()
        shim.put(stale, table, {"id": "u1", "name": "Alice", "credits": 80})
        fresh = coordinator.begin()
        shim.put(fresh, table, {"id": "u1", "name": "Alice", "credits": 90})
        coordinator.commit(fresh.id)

        with pytest.raises(CommitConflictError):
            coordinator.commit(stale.id)
    finally:
        _cleanup(dsn, table, shim)


def test_postgres_shim_child_commit_and_abort_semantics() -> None:
    dsn = _dsn()
    table = _table("tar_pg_child")
    shim: PostgresShim | None = None
    try:
        shim, coordinator = _coordinator_with_users(table)

        parent = coordinator.begin()
        child = coordinator.begin_child(parent.id)
        shim.put(child, table, {"id": "u1", "name": "Alice", "credits": 75})
        coordinator.commit_child(child.id)
        assert shim.get(parent, table, "u1")["credits"] == 75

        child2 = coordinator.begin_child(parent.id)
        shim.put(child2, table, {"id": "u1", "name": "Alice", "credits": 20})
        coordinator.abort_child(child2.id)
        assert shim.get(parent, table, "u1")["credits"] == 75

        coordinator.commit(parent.id)

        txn = coordinator.begin()
        assert shim.get(txn, table, "u1")["credits"] == 75
        coordinator.rollback(txn.id)
    finally:
        _cleanup(dsn, table, shim)


def test_postgres_shim_savepoint_rollback_discards_later_insert() -> None:
    dsn = _dsn()
    table = _table("tar_pg_savepoint")
    shim: PostgresShim | None = None
    try:
        shim, coordinator = _coordinator_with_users(table)

        txn = coordinator.begin()
        shim.put(txn, table, {"id": "u2", "name": "Bob", "credits": 50})
        coordinator.savepoint("after_bob", txn.id)
        shim.put(txn, table, {"id": "u3", "name": "Carol", "credits": 25})

        coordinator.rollback_to_savepoint("after_bob", txn.id)
        assert shim.get(txn, table, "u2") == {
            "id": "u2",
            "name": "Bob",
            "credits": 50,
        }
        assert shim.get(txn, table, "u3") is None

        coordinator.commit(txn.id)

        txn2 = coordinator.begin()
        rows = shim.query(txn2, table, order_by="id")
        assert rows == [
            {"id": "u1", "name": "Alice", "credits": 100},
            {"id": "u2", "name": "Bob", "credits": 50},
        ]
        coordinator.rollback(txn2.id)
    finally:
        _cleanup(dsn, table, shim)


def test_postgres_shim_increment_and_gc() -> None:
    dsn = _dsn()
    table = _table("tar_pg_increment")
    shim: PostgresShim | None = None
    try:
        shim, coordinator = _coordinator_with_users(table)

        txn = coordinator.begin()
        row = shim.increment(txn, table, "u1", "credits", -15)
        assert row is not None
        assert row["credits"] == 85
        coordinator.commit(txn.id)

        txn2 = coordinator.begin()
        assert shim.get(txn2, table, "u1")["credits"] == 85
        coordinator.rollback(txn2.id)

        assert shim.gc(committed_below=txn.numeric_id + 1) == 1

        txn3 = coordinator.begin()
        assert shim.get(txn3, table, "u1")["credits"] == 85
        coordinator.rollback(txn3.id)
    finally:
        _cleanup(dsn, table, shim)


def test_postgres_shim_query_limit_applies_after_version_deduplication() -> None:
    dsn = _dsn()
    table = _table("tar_pg_limit")
    shim: PostgresShim | None = None
    try:
        shim, coordinator = _coordinator_with_users(table)

        txn = coordinator.begin()
        shim.put(txn, table, {"id": "u1", "name": "Alice", "credits": 90})
        shim.put(txn, table, {"id": "u2", "name": "Bob", "credits": 50})
        coordinator.commit(txn.id)

        txn2 = coordinator.begin()
        assert shim.query(txn2, table, order_by="id", limit=2) == [
            {"id": "u1", "name": "Alice", "credits": 90},
            {"id": "u2", "name": "Bob", "credits": 50},
        ]
        coordinator.rollback(txn2.id)
    finally:
        _cleanup(dsn, table, shim)


def test_postgres_shim_quotes_registered_identifiers() -> None:
    dsn = _dsn()
    table = _table("tar_pg_ident")
    shim: PostgresShim | None = None
    try:
        shim = PostgresShim(dsn)
        shim.register_table(
            table,
            ["id TEXT", "select TEXT", "credits INTEGER"],
            pk_column="id",
        )
        shim.seed_data(
            table,
            [{"id": "u1", "select": "reserved", "credits": 1}],
        )
        coordinator = TransactionCoordinator()
        coordinator.register_shim(shim)

        txn = coordinator.begin()
        shim.put(txn, table, {"id": "u1", "select": "still-safe", "credits": 2})
        coordinator.commit(txn.id)

        txn2 = coordinator.begin()
        assert shim.query(txn2, table, filters={"select": "still-safe"}) == [
            {"id": "u1", "select": "still-safe", "credits": 2}
        ]
        coordinator.rollback(txn2.id)
    finally:
        _cleanup(dsn, table, shim)
