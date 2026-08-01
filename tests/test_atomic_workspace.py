from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest
from chronos_core.branching import ChronosBranchContext, MergeResolution
from chronos_core.workspace import (
    AtomicMergeError,
    ChronosWorkspaceContext,
    MergeSelection,
    StaleAtomicMergePreviewError,
    WorkspaceMergeCoordinator,
)


def _context(path: Path, table: str, row_id: str, value: str) -> ChronosBranchContext:
    context = ChronosBranchContext.connect(f"sqlite:///{path}")
    context.db.execute(
        f"CREATE TABLE {table} (id TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    context.db.execute(
        f"INSERT INTO {table}(id, value) VALUES (?, ?)",
        (row_id, value),
    )
    context.db.commit()
    context.register_table(table, ["id"])
    return context


def _workspace(tmp_path: Path) -> ChronosWorkspaceContext:
    return ChronosWorkspaceContext(
        first=_context(tmp_path / "first.sqlite", "first_items", "a", "main-a"),
        second=_context(tmp_path / "second.sqlite", "second_items", "b", "main-b"),
        atomic_metadata_url=f"sqlite:///{tmp_path / 'first.sqlite'}",
        atomic_workspace_id="test",
        atomic_write_wait_timeout=2.0,
    )


def _reopen_context(path: Path, table: str) -> ChronosBranchContext:
    context = ChronosBranchContext.connect(f"sqlite:///{path}")
    context.register_table(table, ["id"])
    return context


def _reopen_workspace(tmp_path: Path) -> ChronosWorkspaceContext:
    return ChronosWorkspaceContext(
        first=_reopen_context(tmp_path / "first.sqlite", "first_items"),
        second=_reopen_context(tmp_path / "second.sqlite", "second_items"),
        atomic_metadata_url=f"sqlite:///{tmp_path / 'first.sqlite'}",
        atomic_workspace_id="test",
        atomic_write_wait_timeout=2.0,
    )


def test_atomic_workspace_reuses_interval_branch_metadata(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    try:
        rows = workspace._atomic.db.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
        names = {str(row["name"]) for row in rows}
        assert "_chronos_branch_interval_branches" in names
        assert not any(name.startswith("_chronos_workspace_") for name in names)
        assert not (tmp_path / "workspace.sqlite").exists()
    finally:
        workspace.close()


def test_atomic_merge_selects_raw_changes_and_publishes_one_manifest(
    tmp_path: Path,
) -> None:
    workspace = _workspace(tmp_path)
    try:
        workspace.create_branch("agent")
        agent = workspace.checkout("agent")
        agent.first.execute(
            "UPDATE first_items SET value = :value WHERE id = :id",
            {"id": "a", "value": "agent-a"},
        )
        agent.second.execute(
            "UPDATE second_items SET value = :value WHERE id = :id",
            {"id": "b", "value": "agent-b"},
        )
        old_main = workspace.checkout("main")
        preview = workspace.merge_atomic_preview("agent", "main")
        first_id = preview.stores["first"].changes[0].change_id
        assert first_id is not None
        noop = workspace.merge_atomic(
            "agent",
            "main",
            selection=MergeSelection(),
            preview_token=preview.preview_token,
            operation_id="select-nothing",
        )
        assert noop.status == "noop"
        assert noop.new_target_token == noop.old_target_token

        result = workspace.merge_atomic(
            "agent",
            "main",
            selection=MergeSelection.from_ids([first_id]),
            preview_token=preview.preview_token,
            operation_id="select-first",
        )

        assert result.status == "committed"
        assert result.selected == 1
        assert result.skipped == 1
        new_main = workspace.checkout("main")
        assert new_main.first.query("SELECT value FROM first_items") == [
            {"value": "agent-a"}
        ]
        assert new_main.second.query("SELECT value FROM second_items") == [
            {"value": "main-b"}
        ]
        # A checkout that resolved the manifest before publication remains old.
        assert old_main.first.query("SELECT value FROM first_items") == [
            {"value": "main-a"}
        ]
        assert old_main.second.query("SELECT value FROM second_items") == [
            {"value": "main-b"}
        ]
    finally:
        workspace.close()


def test_atomic_merge_rejects_stale_preview_and_is_idempotent(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    try:
        workspace.create_branch("agent")
        workspace.checkout("agent").first.execute(
            "UPDATE first_items SET value = 'agent' WHERE id = 'a'"
        )
        preview = workspace.merge_atomic_preview("agent", "main")
        with workspace.branch_write("main"):
            workspace.checkout("main").second.execute(
                "UPDATE second_items SET value = 'new-main' WHERE id = 'b'"
            )
        with pytest.raises(StaleAtomicMergePreviewError):
            workspace.merge_atomic(
                "agent",
                "main",
                preview_token=preview.preview_token,
                operation_id="stale",
            )

        fresh = workspace.merge_atomic_preview("agent", "main")
        first = workspace.merge_atomic(
            "agent",
            "main",
            preview_token=fresh.preview_token,
            operation_id="idempotent",
        )
        second = workspace.merge_atomic(
            "agent",
            "main",
            preview_token=fresh.preview_token,
            operation_id="idempotent",
        )
        assert second == first
        with pytest.raises(AtomicMergeError, match="different merge"):
            workspace.merge_atomic(
                "main",
                "agent",
                operation_id="idempotent",
            )
    finally:
        workspace.close()


def test_concurrent_same_operation_id_returns_one_committed_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _workspace(tmp_path)
    try:
        workspace.create_branch("agent")
        workspace.checkout("agent").first.execute(
            "UPDATE first_items SET value = 'agent' WHERE id = 'a'"
        )
        preview = workspace.merge_atomic_preview("agent", "main")
        initial_checks = threading.Barrier(2)
        check_lock = threading.Lock()
        initial_count = 0
        completed = workspace._completed_atomic_result

        def synchronized_initial_check(operation_id, source, target):
            nonlocal initial_count
            with check_lock:
                initial_count += 1
                current = initial_count
            if current <= 2:
                initial_checks.wait(timeout=2)
                return None
            return completed(operation_id, source, target)

        monkeypatch.setattr(
            workspace,
            "_completed_atomic_result",
            synchronized_initial_check,
        )
        results = []

        def merge() -> None:
            results.append(
                workspace.merge_atomic(
                    "agent",
                    "main",
                    preview_token=preview.preview_token,
                    operation_id="concurrent-idempotent",
                )
            )

        threads = [threading.Thread(target=merge) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)

        assert len(results) == 2
        assert results[0] == results[1]
        assert results[0].status == "committed"
    finally:
        workspace.close()


def test_atomic_merge_failure_keeps_target_and_records_abort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _workspace(tmp_path)
    try:
        workspace.create_branch("agent")
        agent = workspace.checkout("agent")
        agent.first.execute("UPDATE first_items SET value = 'agent-a' WHERE id = 'a'")
        agent.second.execute("UPDATE second_items SET value = 'agent-b' WHERE id = 'b'")
        preview = workspace.merge_atomic_preview("agent", "main")
        original = workspace._stage_atomic_changes

        def fail_second(name, store, source_branch, stage_branch, changes):
            if name == "second":
                raise RuntimeError("injected second-store failure")
            return original(name, store, source_branch, stage_branch, changes)

        monkeypatch.setattr(workspace, "_stage_atomic_changes", fail_second)
        with pytest.raises(RuntimeError, match="injected"):
            workspace.merge_atomic(
                "agent",
                "main",
                preview_token=preview.preview_token,
                operation_id="failed",
            )
        main = workspace.checkout("main")
        assert main.first.query("SELECT value FROM first_items")[0]["value"] == "main-a"
        assert (
            main.second.query("SELECT value FROM second_items")[0]["value"] == "main-b"
        )
        with pytest.raises(AtomicMergeError, match="injected"):
            workspace.merge_atomic(
                "agent",
                "main",
                operation_id="failed",
            )
    finally:
        workspace.close()


def test_publish_response_failure_recovers_committed_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _workspace(tmp_path)
    try:
        workspace.create_branch("agent")
        workspace.checkout("agent").first.execute(
            "UPDATE first_items SET value = 'agent' WHERE id = 'a'"
        )
        preview = workspace.merge_atomic_preview("agent", "main")
        publish = workspace._atomic.publish

        def commit_then_disconnect(reservation, result):
            publish(reservation, result)
            raise ConnectionError("response lost after commit")

        monkeypatch.setattr(workspace._atomic, "publish", commit_then_disconnect)
        result = workspace.merge_atomic(
            "agent",
            "main",
            preview_token=preview.preview_token,
            operation_id="uncertain-publish",
        )

        assert result.status == "committed"
        assert workspace.checkout("main").first.query(
            "SELECT value FROM first_items"
        ) == [{"value": "agent"}]
    finally:
        workspace.close()


def test_atomic_merge_applies_explicit_cross_store_conflict_resolution(
    tmp_path: Path,
) -> None:
    workspace = _workspace(tmp_path)
    try:
        workspace.create_branch("agent")
        workspace.checkout("agent").first.execute(
            "UPDATE first_items SET value = 'agent' WHERE id = 'a'"
        )
        with workspace.branch_write("main"):
            workspace.checkout("main").first.execute(
                "UPDATE first_items SET value = 'target' WHERE id = 'a'"
            )
        preview = workspace.merge_atomic_preview("agent", "main")
        conflict = preview.stores["first"].conflicts[0]
        assert conflict.change_id is not None
        assert conflict.conflict_id is not None

        result = workspace.merge_atomic(
            "agent",
            "main",
            selection=MergeSelection.from_ids([conflict.change_id]),
            preview_token=preview.preview_token,
            resolution=MergeResolution({conflict.conflict_id: "source"}),
            policy="manual_review",
            operation_id="resolved-conflict",
        )

        assert result.status == "committed"
        assert workspace.checkout("main").first.query(
            "SELECT value FROM first_items"
        ) == [{"value": "agent"}]
    finally:
        workspace.close()


@pytest.mark.parametrize("branch_id", ["main", "agent"])
def test_atomic_participant_writer_waits_for_reservation(
    tmp_path: Path,
    branch_id: str,
) -> None:
    workspace = _workspace(tmp_path)
    try:
        workspace.create_branch("agent")
        source = workspace._atomic.branch("agent")
        target = workspace._atomic.branch("main")
        reservation = workspace._atomic.reserve(
            "held",
            source,
            target,
            preview_token="held-preview",
            staging_manifest=target.manifest,
        )
        acquired = threading.Event()

        def writer() -> None:
            with workspace.branch_write(branch_id):
                acquired.set()

        thread = threading.Thread(target=writer)
        thread.start()
        time.sleep(0.1)
        assert not acquired.is_set()
        workspace._atomic.abort(
            reservation,
            {"operation_id": "held", "status": "aborted", "error": "test"},
        )
        thread.join(timeout=2)
        assert acquired.is_set()
    finally:
        workspace.close()


def test_restart_aborts_unpublished_staging_and_keeps_old_target(
    tmp_path: Path,
) -> None:
    workspace = _workspace(tmp_path)
    workspace.create_branch("agent")
    workspace.checkout("agent").first.execute(
        "UPDATE first_items SET value = 'agent' WHERE id = 'a'"
    )
    preview = workspace.merge_atomic_preview("agent", "main")
    source = workspace._atomic.branch("agent")
    target = workspace._atomic.branch("main")
    staging_manifest = dict(target.manifest)
    staging_manifest["first"] = "__crash_stage_first"
    workspace._atomic.reserve(
        "crashed",
        source,
        target,
        preview_token=preview.preview_token,
        staging_manifest=staging_manifest,
    )
    workspace.stores["first"].create_branch(
        "__crash_stage_first",
        from_branch=target.manifest["first"],
    )
    row = workspace._atomic._branch_row("main")
    metadata = workspace._atomic._metadata(row)
    state = workspace._atomic._state(metadata)
    state["active_merge"]["owner_pid"] = -1
    workspace._atomic._write_metadata(
        "main",
        workspace._atomic._set_state(metadata, state),
    )
    row = workspace._atomic._branch_row("agent")
    metadata = workspace._atomic._metadata(row)
    state = workspace._atomic._state(metadata)
    state["active_merge"]["owner_pid"] = -1
    workspace._atomic._write_metadata(
        "agent",
        workspace._atomic._set_state(metadata, state),
    )
    workspace._atomic.db.commit()
    workspace.close()

    reopened = _reopen_workspace(tmp_path)
    try:
        assert reopened.resolve_branch("first", "main") == "main"
        assert reopened.checkout("main").first.query(
            "SELECT value FROM first_items"
        ) == [{"value": "main-a"}]
        assert "__crash_stage_first" not in {
            branch.branch_id for branch in reopened.stores["first"].list_branches()
        }
        with pytest.raises(AtomicMergeError, match="recovery aborted"):
            reopened.merge_atomic("agent", "main", operation_id="crashed")
    finally:
        reopened.close()


def test_committed_result_and_manifest_survive_restart(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    workspace.create_branch("agent")
    workspace.checkout("agent").first.execute(
        "UPDATE first_items SET value = 'agent' WHERE id = 'a'"
    )
    preview = workspace.merge_atomic_preview("agent", "main")
    committed = workspace.merge_atomic(
        "agent",
        "main",
        preview_token=preview.preview_token,
        operation_id="committed-before-restart",
    )
    workspace.close()

    reopened = _reopen_workspace(tmp_path)
    try:
        assert reopened.checkout("main").first.query(
            "SELECT value FROM first_items"
        ) == [{"value": "agent"}]
        retried = reopened.merge_atomic(
            "agent",
            "main",
            preview_token=preview.preview_token,
            operation_id="committed-before-restart",
        )
        assert retried == committed
    finally:
        reopened.close()


def test_postgres_coordinator_publishes_manifest_with_one_cas(tmp_path: Path) -> None:
    from tests.test_branching import _postgres_dsn, _reset_postgres_schema

    metadata_url = _postgres_dsn()
    for attempt in range(5):
        try:
            _reset_postgres_schema()
            break
        except Exception:
            if attempt == 4:
                raise
            time.sleep(1)
    first = ChronosBranchContext.connect(metadata_url)
    first.db.execute(
        "CREATE TABLE first_items (id TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    first.db.execute(
        "INSERT INTO first_items(id, value) VALUES (?, ?)",
        ("a", "main-a"),
    )
    first.db.commit()
    first.register_table("first_items", ["id"])
    workspace = ChronosWorkspaceContext(
        first=first,
        second=_context(tmp_path / "second.sqlite", "second_items", "b", "main-b"),
        atomic_metadata_url=metadata_url,
        atomic_workspace_id="postgres-coordinator-test",
        atomic_write_wait_timeout=2.0,
    )
    try:
        workspace.create_branch("agent")
        agent = workspace.checkout("agent")
        agent.first.execute("UPDATE first_items SET value = 'agent-a' WHERE id = 'a'")
        agent.second.execute("UPDATE second_items SET value = 'agent-b' WHERE id = 'b'")
        preview = workspace.merge_atomic_preview("agent", "main")

        result = workspace.merge_atomic(
            "agent",
            "main",
            preview_token=preview.preview_token,
            operation_id="postgres-publish",
        )

        assert result.status == "committed"
        assert result.new_target_token.generation == 1
        main = workspace.checkout("main")
        assert main.first.query("SELECT value FROM first_items") == [
            {"value": "agent-a"}
        ]
        assert main.second.query("SELECT value FROM second_items") == [
            {"value": "agent-b"}
        ]
        observer = WorkspaceMergeCoordinator(
            metadata_url,
            workspace_id="postgres-coordinator-test",
        )
        try:
            assert observer.branch("main").generation == 1
            persisted = observer.completed_result("postgres-publish")
            assert persisted is not None
            assert persisted["status"] == "committed"
        finally:
            observer.close()
    finally:
        workspace.close()
