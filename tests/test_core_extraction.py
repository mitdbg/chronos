from chronos_core.transaction import SQLiteShim, TransactionCoordinator


def test_sqlite_shim_commits_visible_update() -> None:
    shim = SQLiteShim(":memory:")
    shim.register_table(
        "users",
        ["id TEXT", "name TEXT", "credits INTEGER"],
        pk_column="id",
    )
    shim.seed_data(
        "users",
        [{"id": "u1", "name": "Alice", "credits": 100}],
    )

    coordinator = TransactionCoordinator()
    coordinator.register_shim(shim)

    txn = coordinator.begin()
    before = shim.get(txn, "users", "u1")
    assert before is not None
    assert before["credits"] == 100

    shim.put(txn, "users", {"id": "u1", "name": "Alice", "credits": 90})
    in_txn = shim.get(txn, "users", "u1")
    assert in_txn is not None
    assert in_txn["credits"] == 90

    coordinator.commit(txn.id)

    txn2 = coordinator.begin()
    after = shim.get(txn2, "users", "u1")
    assert after is not None
    assert after["credits"] == 90
    coordinator.rollback(txn2.id)


def test_sqlite_shim_rollback_discards_update() -> None:
    shim = SQLiteShim(":memory:")
    shim.register_table(
        "items",
        ["id TEXT", "value TEXT"],
        pk_column="id",
    )
    shim.seed_data("items", [{"id": "i1", "value": "base"}])

    coordinator = TransactionCoordinator()
    coordinator.register_shim(shim)

    txn = coordinator.begin()
    shim.put(txn, "items", {"id": "i1", "value": "changed"})
    coordinator.rollback(txn.id)

    txn2 = coordinator.begin()
    row = shim.get(txn2, "items", "i1")
    assert row is not None
    assert row["value"] == "base"
    coordinator.rollback(txn2.id)

