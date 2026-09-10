from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from chronos_core.branching import BranchNotFoundError, ChronosBranchContext


def _context(path: Path) -> ChronosBranchContext:
    return ChronosBranchContext.connect(f"sqlite:///{path}", backend="interval")


def _seed(path: Path) -> ChronosBranchContext:
    context = _context(path)
    context.db.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
    context.db.execute("INSERT INTO docs VALUES (?, ?)", ("d1", "base"))
    context.db.commit()
    context.register_table("docs", ["id"])
    return context


def _body(session: Any) -> str:
    return str(
        session.query("SELECT body FROM docs WHERE id = :id", {"id": "d1"})[0][
            "body"
        ]
    )


def _postgres_dsn() -> str:
    dsn = os.environ.get("CHRONOS_BRANCH_POSTGRES_DSN") or os.environ.get(
        "CHRONOS_POSTGRES_DSN"
    )
    if not dsn:
        pytest.skip("PostgreSQL session-epoch test requires a configured DSN")
    return dsn


def _seed_postgres() -> tuple[ChronosBranchContext, str]:
    dsn = _postgres_dsn()
    bootstrap = ChronosBranchContext.connect(dsn, backend="interval")
    bootstrap.db.execute("DROP SCHEMA public CASCADE")
    bootstrap.db.execute("CREATE SCHEMA public")
    bootstrap.db.commit()
    bootstrap.close()
    context = ChronosBranchContext.connect(dsn, backend="interval")
    context.db.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
    context.db.execute("INSERT INTO docs VALUES (?, ?)", ("d1", "base"))
    context.db.commit()
    context.register_table("docs", ["id"])
    return context, dsn


def test_checked_out_parent_advances_epoch_after_fork(tmp_path: Path) -> None:
    path = tmp_path / "fork.sqlite"
    context = _seed(path)
    try:
        parent = context.checkout("main")
        old_ref = parent.current_ref

        context.create_branch("child", from_branch="main")

        assert parent.current_ref != old_ref
        parent.execute(
            "UPDATE docs SET body = :body WHERE id = :id",
            {"body": "parent", "id": "d1"},
        )
        assert _body(parent) == "parent"
        child = context.checkout("child")
        try:
            assert _body(child) == "base"
        finally:
            child.close()
        parent.close()
    finally:
        context.close()


def test_cross_context_stale_writer_refreshes_under_write_guard(
    tmp_path: Path,
) -> None:
    path = tmp_path / "stale-writer.sqlite"
    writer_context = _seed(path)
    manager_context = _context(path)
    writer = writer_context.checkout("main")
    try:
        manager_context.create_branch("child", from_branch="main")
        writer.execute(
            "UPDATE docs SET body = :body WHERE id = :id",
            {"body": "parent", "id": "d1"},
        )
        child = manager_context.checkout("child")
        try:
            assert _body(child) == "base"
        finally:
            child.close()
    finally:
        writer.close()
        writer_context.close()
        manager_context.close()


def test_fork_waits_for_transaction_and_child_includes_committed_write(
    tmp_path: Path,
) -> None:
    path = tmp_path / "writer.sqlite"
    writer_context = _seed(path)
    manager_context = _context(path)
    writer = writer_context.checkout("main")
    write_done = threading.Event()
    release = threading.Event()
    fork_done = threading.Event()

    def write_transaction() -> None:
        with writer.transaction():
            writer.execute(
                "UPDATE docs SET body = :body WHERE id = :id",
                {"body": "committed-before-fork", "id": "d1"},
            )
            write_done.set()
            assert release.wait(timeout=5)

    def fork() -> None:
        manager_context.create_branch("child", from_branch="main")
        fork_done.set()

    write_thread = threading.Thread(target=write_transaction, daemon=True)
    fork_thread = threading.Thread(target=fork, daemon=True)
    try:
        write_thread.start()
        assert write_done.wait(timeout=2)
        fork_thread.start()
        time.sleep(0.05)
        assert not fork_done.is_set()

        release.set()
        write_thread.join(timeout=5)
        fork_thread.join(timeout=5)
        assert fork_done.is_set()
        child = manager_context.checkout("child")
        try:
            assert _body(child) == "committed-before-fork"
        finally:
            child.close()
    finally:
        release.set()
        writer.close()
        writer_context.close()
        manager_context.close()


def test_checkpoint_advances_live_session_epoch(tmp_path: Path) -> None:
    path = tmp_path / "checkpoint.sqlite"
    context = _seed(path)
    try:
        session = context.checkout("main")
        old_ref = session.current_ref
        context.create_checkpoint("cp", branch="main")
        assert session.current_ref != old_ref
        checkpoint = context.checkout_checkpoint("cp")
        try:
            assert _body(checkpoint) == "base"
        finally:
            checkpoint.close()
        session.close()
    finally:
        context.close()


def test_merge_fences_source_and_target_and_refreshes_target(tmp_path: Path) -> None:
    path = tmp_path / "merge.sqlite"
    context = _seed(path)
    try:
        target = context.checkout("main")
        context.create_branch("source", from_branch="main")
        source = context.checkout("source")
        source.execute(
            "UPDATE docs SET body = :body WHERE id = :id",
            {"body": "merged", "id": "d1"},
        )
        old_target_ref = target.current_ref

        result = context.merge_apply("source", "main")

        assert result.applied == 1
        assert target.current_ref != old_target_ref
        assert _body(target) == "merged"
        source.close()
        target.close()
    finally:
        context.close()


def test_delete_fences_entire_subtree(tmp_path: Path) -> None:
    path = tmp_path / "delete.sqlite"
    context = _seed(path)
    manager = _context(path)
    try:
        context.create_branch("parent", from_branch="main")
        context.create_branch("child", from_branch="parent")
        child = context.checkout("child")
        manager.delete_branch("parent")
        with pytest.raises(BranchNotFoundError):
            manager.get_branch("child")
        child.close()
    finally:
        context.close()
        manager.close()


def test_postgres_fork_waits_for_active_reader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader_context, dsn = _seed_postgres()
    manager_context = ChronosBranchContext.connect(dsn, backend="interval")
    reader = reader_context.checkout("main")
    entered = threading.Event()
    release = threading.Event()
    fork_done = threading.Event()
    errors: list[BaseException] = []
    original_query = reader_context._backend.query

    def held_query(ref: Any, sql: str, params: dict[str, Any]) -> Any:
        if "epoch_hold" in sql:
            entered.set()
            assert release.wait(timeout=5)
        return original_query(ref, sql, params)

    def fork() -> None:
        try:
            manager_context.create_branch("child", from_branch="main")
            fork_done.set()
        except BaseException as exc:
            errors.append(exc)

    monkeypatch.setattr(reader_context._backend, "query", held_query)
    read_thread = threading.Thread(
        target=lambda: reader.query("SELECT body FROM docs /* epoch_hold */"),
        daemon=True,
    )
    fork_thread = threading.Thread(target=fork, daemon=True)
    try:
        read_thread.start()
        assert entered.wait(timeout=2)
        fork_thread.start()
        time.sleep(0.05)
        assert not fork_done.is_set()
        release.set()
        read_thread.join(timeout=5)
        fork_thread.join(timeout=5)
        assert not errors
        assert fork_done.is_set()
        assert reader.current_ref == manager_context.get_branch("main").current_ref
    finally:
        release.set()
        reader.close()
        reader_context.close()
        manager_context.close()


def test_postgres_subtree_delete_waits_for_active_descendant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader_context, dsn = _seed_postgres()
    manager_context = ChronosBranchContext.connect(dsn, backend="interval")
    reader_context.create_branch("parent", from_branch="main")
    reader_context.create_branch("child", from_branch="parent")
    reader = reader_context.checkout("child")
    entered = threading.Event()
    release = threading.Event()
    delete_done = threading.Event()
    errors: list[BaseException] = []
    original_query = reader_context._backend.query

    def held_query(ref: Any, sql: str, params: dict[str, Any]) -> Any:
        if "epoch_hold" in sql:
            entered.set()
            assert release.wait(timeout=5)
        return original_query(ref, sql, params)

    def delete() -> None:
        try:
            manager_context.delete_branch("parent")
            delete_done.set()
        except BaseException as exc:
            errors.append(exc)

    monkeypatch.setattr(reader_context._backend, "query", held_query)
    read_thread = threading.Thread(
        target=lambda: reader.query("SELECT body FROM docs /* epoch_hold */"),
        daemon=True,
    )
    delete_thread = threading.Thread(target=delete, daemon=True)
    try:
        read_thread.start()
        assert entered.wait(timeout=2)
        delete_thread.start()
        time.sleep(0.05)
        assert not delete_done.is_set()

        release.set()
        read_thread.join(timeout=5)
        delete_thread.join(timeout=5)
        assert not errors
        assert delete_done.is_set()
        with pytest.raises(BranchNotFoundError):
            manager_context.get_branch("parent")
        with pytest.raises(BranchNotFoundError):
            manager_context.get_branch("child")
    finally:
        release.set()
        reader.close()
        reader_context.close()
        manager_context.close()


def test_postgres_stale_writer_refreshes_after_fork() -> None:
    writer_context, dsn = _seed_postgres()
    manager_context = ChronosBranchContext.connect(dsn, backend="interval")
    writer = writer_context.checkout("main")
    try:
        old_ref = writer.current_ref
        manager_context.create_branch("child", from_branch="main")

        writer.execute(
            "UPDATE docs SET body = :body WHERE id = :id",
            {"body": "parent", "id": "d1"},
        )

        assert writer.current_ref != old_ref
        child = manager_context.checkout("child")
        try:
            assert _body(child) == "base"
        finally:
            child.close()
    finally:
        writer.close()
        writer_context.close()
        manager_context.close()


def test_postgres_lazy_session_refreshes_before_first_write() -> None:
    writer_context, dsn = _seed_postgres()
    manager_context = ChronosBranchContext.connect(dsn, backend="interval")
    writer = writer_context.checkout("main")
    try:
        manager_context.create_branch("child", from_branch="main")

        writer.execute(
            "UPDATE docs SET body = :body WHERE id = :id",
            {"body": "parent", "id": "d1"},
        )

        child = manager_context.checkout("child")
        try:
            assert _body(child) == "base"
        finally:
            child.close()
    finally:
        writer.close()
        writer_context.close()
        manager_context.close()


def test_postgres_checkout_registers_lazily_and_close_releases_fence() -> None:
    context, _ = _seed_postgres()
    session = context.checkout("main")
    try:
        coordinator = context._session_epochs
        assert coordinator is not None
        # Opening a context does not itself require a session-epoch control
        # connection.  It is allocated only when this writable session first
        # performs an operation.
        assert coordinator._db is None
        row = context.metadata_db.execute(
            "SELECT COUNT(*) AS count FROM _chronos_branch_sessions"
        ).fetchone()
        assert row is not None
        assert int(row["count"]) == 0

        assert _body(session) == "base"
        assert coordinator._db is not None
        row = context.metadata_db.execute(
            "SELECT COUNT(*) AS count FROM _chronos_branch_sessions"
        ).fetchone()
        assert row is not None
        assert int(row["count"]) == 1

        session.close()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            row = context.metadata_db.execute(
                "SELECT COUNT(*) AS count FROM _chronos_branch_sessions"
            ).fetchone()
            assert row is not None
            if int(row["count"]) == 0:
                break
            time.sleep(0.01)
        else:
            pytest.fail("closed session registration was not removed")

        context.create_branch("child", from_branch="main")
    finally:
        session.close()
        context.close()


def test_postgres_fork_waits_for_active_write_transaction() -> None:
    writer_context, dsn = _seed_postgres()
    manager_context = ChronosBranchContext.connect(dsn, backend="interval")
    writer = writer_context.checkout("main")
    write_done = threading.Event()
    release = threading.Event()
    fork_done = threading.Event()
    errors: list[BaseException] = []

    def write_transaction() -> None:
        try:
            with writer.transaction():
                writer.execute(
                    "UPDATE docs SET body = :body WHERE id = :id",
                    {"body": "committed-before-fork", "id": "d1"},
                )
                write_done.set()
                assert release.wait(timeout=5)
        except BaseException as exc:
            errors.append(exc)

    def fork() -> None:
        try:
            manager_context.create_branch("child", from_branch="main")
            fork_done.set()
        except BaseException as exc:
            errors.append(exc)

    write_thread = threading.Thread(target=write_transaction, daemon=True)
    fork_thread = threading.Thread(target=fork, daemon=True)
    try:
        write_thread.start()
        assert write_done.wait(timeout=2)
        fork_thread.start()
        time.sleep(0.05)
        assert not fork_done.is_set()

        release.set()
        write_thread.join(timeout=5)
        fork_thread.join(timeout=5)
        assert not errors
        assert fork_done.is_set()
        child = manager_context.checkout("child")
        try:
            assert _body(child) == "committed-before-fork"
        finally:
            child.close()
    finally:
        release.set()
        writer.close()
        writer_context.close()
        manager_context.close()


def test_postgres_deleted_branch_rejects_repeated_session_access() -> None:
    context, dsn = _seed_postgres()
    manager = ChronosBranchContext.connect(dsn, backend="interval")
    context.create_branch("doomed", from_branch="main")
    session = context.checkout("doomed")
    try:
        manager.delete_branch("doomed")
        for _ in range(2):
            with pytest.raises(BranchNotFoundError):
                session.query("SELECT body FROM docs")
    finally:
        session.close()
        context.close()
        manager.close()


def test_postgres_rapid_fork_delete_does_not_leak_session_fence() -> None:
    context, _ = _seed_postgres()
    session = context.checkout("main")
    try:
        for _ in range(25):
            context.create_branch("candidate", from_branch="main", fanout=64)
            context.delete_branch("candidate")

        assert session.current_ref == context.get_branch("main").current_ref
        barriers = context.metadata_db.execute(
            "SELECT COUNT(*) AS count FROM _chronos_branch_session_barriers"
        ).fetchone()
        assert barriers is not None
        assert int(barriers["count"]) == 0
    finally:
        session.close()
        context.close()


def test_postgres_connect_does_not_rebuild_existing_session_indexes() -> None:
    import psycopg
    context, dsn = _seed_postgres()
    context.close()
    with psycopg.connect(dsn) as blocker:
        blocker.execute("LOCK TABLE _chronos_branch_sessions IN ROW EXCLUSIVE MODE")
        result = subprocess.run(
            [sys.executable, "-I", "-c",
             "import sys; from chronos_core.branching import ChronosBranchContext; "
             "c=ChronosBranchContext.connect(sys.argv[1]); c.close(); print('ready')", dsn],
            capture_output=True, text=True, timeout=10,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "ready"


def test_postgres_process_crash_releases_session_fence() -> None:
    bootstrap, dsn = _seed_postgres()
    bootstrap.close()
    child_code = r"""
import sys
import time
from chronos_core.branching import ChronosBranchContext

context = ChronosBranchContext.connect(sys.argv[1], backend="interval")
session = context.checkout("main")
print(session.current_ref, flush=True)
time.sleep(60)
"""
    child = subprocess.Popen(
        [sys.executable, "-c", child_code, dsn],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert child.stdout is not None
    assert child.stdout.readline().strip()
    os.kill(child.pid, signal.SIGSTOP)
    manager = ChronosBranchContext.connect(dsn, backend="interval")
    fork_done = threading.Event()
    errors: list[BaseException] = []

    def fork() -> None:
        try:
            manager.create_branch("after-crash", from_branch="main")
            fork_done.set()
        except BaseException as exc:
            errors.append(exc)

    fork_thread = threading.Thread(target=fork, daemon=True)
    try:
        fork_thread.start()
        time.sleep(0.05)
        assert not fork_done.is_set()
        child.kill()
        child.wait(timeout=5)
        fork_thread.join(timeout=5)
        assert not errors
        assert fork_done.is_set()
        assert manager.get_branch("after-crash").branch_id == "after-crash"
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
        manager.close()
