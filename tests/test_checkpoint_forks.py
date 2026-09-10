"""Checkpoint allocation must survive parallel forks, deletion and reconnect."""
from concurrent.futures import ThreadPoolExecutor

import pytest

from chronos_core.branching import ChronosBranchContext
from chronos_core.branching._interval_backend import _wait_for_all_interval_gc_jobs


@pytest.mark.parametrize("engine", ["sqlite", "postgres"])
def test_checkpoint_forks_are_disjoint_and_never_reused(tmp_path, engine):
    if engine == "postgres":
        from tests.test_branching import _postgres_dsn, _reset_postgres_schema
        url = _postgres_dsn()
        _reset_postgres_schema()
    else:
        url = f"sqlite:///{tmp_path / 'checkpoint.sqlite'}"
    ctx = ChronosBranchContext.connect(url, backend="interval")
    try:
        ctx.db.execute("CREATE TABLE tickets (id INTEGER PRIMARY KEY, priority INTEGER)")
        ctx.db.execute("INSERT INTO tickets VALUES (1, 0)")
        ctx.db.commit()
        ctx.register_table("tickets", ["id"])
        ctx.create_checkpoint("baseline", branch="main")
        checkpoint = ctx.checkout_checkpoint("baseline")
        before = ctx.db.execute(
            "SELECT s.* FROM _chronos_branch_interval_segments s "
            "JOIN _chronos_branch_interval_checkpoints c ON s.segment_id = c.segment_id"
        ).fetchone()

        def episode(index):
            worker = ChronosBranchContext.connect(url, backend="interval")
            name = f"episode_{index}"
            try:
                worker.create_branch_from_checkpoint(name, checkpoint="baseline")
                with worker.checkout(name) as session:
                    assert session.query("SELECT priority FROM tickets") == [{"priority": 0}]
                    session.execute("UPDATE tickets SET priority = :value", {"value": index + 1})
                    assert session.query("SELECT priority FROM tickets") == [{"priority": index + 1}]
                rows = worker.db.execute(
                    "SELECT live_lo, live_hi FROM _chronos_branch_interval_segments "
                    "WHERE owner_branch_id = :branch ORDER BY segment_id LIMIT 1",
                    {"branch": name},
                ).fetchone()
                return name, int(rows["live_lo"]), int(rows["live_hi"])
            finally:
                worker.close()

        reservations = []
        for batch in range(3):
            with ThreadPoolExecutor(max_workers=4) as pool:
                current = list(pool.map(episode, range(batch * 8, (batch + 1) * 8)))
            reservations.extend(current)
            for name, _, _ in current:
                with ctx.checkout(name) as session:
                    assert session.query("SELECT priority FROM tickets") == [
                        {"priority": int(name.split("_")[1]) + 1}
                    ]
                ctx.delete_branch(name)
            _wait_for_all_interval_gc_jobs()
            assert checkpoint.query("SELECT priority FROM tickets") == [{"priority": 0}]
        spans = sorted((lo, hi) for _, lo, hi in reservations)
        assert all(a[1] <= b[0] for a, b in zip(spans, spans[1:]))
        after = ctx.db.execute(
            "SELECT s.* FROM _chronos_branch_interval_segments s "
            "JOIN _chronos_branch_interval_checkpoints c ON s.segment_id = c.segment_id"
        ).fetchone()
        assert dict(before) == dict(after)
        checkpoint.close()
        with ctx.checkout("main") as session:
            assert session.query("SELECT priority FROM tickets") == [{"priority": 0}]
    finally:
        ctx.close()


def test_postgres_point_update_waiting_for_sibling_split():
    """Force the waiting statement's snapshot to predate physical fragments."""
    import time
    import psycopg
    from tests.test_branching import _postgres_dsn, _reset_postgres_schema
    url = _postgres_dsn()
    _reset_postgres_schema()
    ctx = ChronosBranchContext.connect(url)
    other = ChronosBranchContext.connect(url)
    try:
        ctx.db.execute("CREATE TABLE tickets (id INTEGER PRIMARY KEY, priority INTEGER)")
        ctx.db.execute("INSERT INTO tickets VALUES (1, 0)")
        ctx.db.commit()
        ctx.register_table("tickets", ["id"])
        ctx.create_checkpoint("baseline", branch="main")
        ctx.create_branch_from_checkpoint("first", checkpoint="baseline")
        ctx.create_branch_from_checkpoint("second", checkpoint="baseline")
        # Open the second context after registration so its table cache is current.
        other.close()
        other = ChronosBranchContext.connect(url)
        with ctx.checkout("first") as first, other.checkout("second") as second:
            with ThreadPoolExecutor(max_workers=1) as pool:
                with first.transaction():
                    first.execute("UPDATE tickets SET priority = 1 WHERE id = 1")
                    future = pool.submit(second.execute, "UPDATE tickets SET priority = 2 WHERE id = 1")
                    with psycopg.connect(url, autocommit=True) as observer:
                        deadline = time.monotonic() + 10
                        while time.monotonic() < deadline:
                            blocked = observer.execute(
                                "SELECT count(*) FROM pg_stat_activity "
                                "WHERE datname = current_database() AND wait_event_type = 'Lock' "
                                "AND query LIKE '%private_update AS%'"
                            ).fetchone()[0]
                            if blocked:
                                break
                            time.sleep(0.01)
                        else:
                            pytest.fail("second UPDATE did not reach the physical row lock")
                future.result(timeout=10)
                assert first.query("SELECT priority FROM tickets") == [{"priority": 1}]
                assert second.query("SELECT priority FROM tickets") == [{"priority": 2}]
        with ctx.checkout_checkpoint("baseline") as baseline:
            assert baseline.query("SELECT priority FROM tickets") == [{"priority": 0}]
    finally:
        other.close()
        ctx.close()
