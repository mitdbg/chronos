import pytest

from chronos_core.branching import ChronosBranchContext


@pytest.mark.parametrize("engine", ["sqlite", "postgres"])
def test_cached_session_bulk_write_rollback_after_commit(tmp_path, engine):
    if engine == "postgres":
        from tests.test_branching import _postgres_dsn, _reset_postgres_schema
        url = _postgres_dsn()
        _reset_postgres_schema()
    else:
        url = f"sqlite:///{tmp_path / 'reuse.sqlite'}"
    ctx = ChronosBranchContext.connect(url)
    try:
        ctx.db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
        ctx.db.commit()
        ctx.register_table("items", ["id"])
        with ctx.checkout("main") as session:
            for index in range(4):
                with session.transaction():
                    session.upsert_rows("items", [{"id": 1, "value": str(index)}])
                with pytest.raises(RuntimeError, match="abort"):
                    with session.transaction():
                        session.upsert_rows("items", [{"id": 2, "value": "aborted"}])
                        session.upsert_rows("items", [{"id": 1, "value": "aborted"}])
                        raise RuntimeError("abort")
                assert session.query("SELECT * FROM items") == [{"id": 1, "value": str(index)}]
    finally:
        ctx.close()


@pytest.mark.parametrize("engine", ["sqlite", "postgres"])
@pytest.mark.parametrize("write", ["execute", "upsert_rows", "delete_keys"])
def test_autocommit_false_keeps_caller_transaction_open(tmp_path, engine, write):
    if engine == "postgres":
        from tests.test_branching import _postgres_dsn, _reset_postgres_schema
        url = _postgres_dsn()
        _reset_postgres_schema()
    else:
        url = f"sqlite:///{tmp_path / 'caller.sqlite'}"
    ctx = ChronosBranchContext.connect(url, autocommit=False)
    try:
        ctx.db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
        ctx.db.execute("CREATE TABLE audit (id INTEGER PRIMARY KEY)")
        ctx.db.execute("INSERT INTO items VALUES (1, 'old')")
        ctx.db.commit()
        ctx.register_table("items", ["id"])
        session = ctx.checkout("main")
        ctx.db.commit()

        ctx.db.execute("INSERT INTO audit VALUES (1)")
        if write == "execute":
            session.execute("UPDATE items SET value = 'new' WHERE id = 1")
        elif write == "upsert_rows":
            session.upsert_rows("items", [{"id": 1, "value": "new"}])
        else:
            session.delete_keys("items", [{"id": 1}])
        ctx.db.rollback()

        assert ctx.db.execute("SELECT id FROM audit").fetchall() == []
        assert session.query("SELECT * FROM items") == [{"id": 1, "value": "old"}]
    finally:
        ctx.close()
