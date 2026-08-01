from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from chronos_core.branching import ChronosBranchContext
from chronos_core.workspace import (
    ChronosWorkspaceContext,
    MergeSelection,
    StaleAtomicMergePreviewError,
)


def _workspace(
    tmp_path: Path,
) -> tuple[
    ChronosWorkspaceContext,
    ChronosBranchContext,
    ChronosBranchContext,
    str,
]:
    url = f"sqlite:///{tmp_path / 'metadata-and-relational.sqlite'}"
    first = ChronosBranchContext.connect(url)
    first.db.execute("CREATE TABLE first_state (id TEXT PRIMARY KEY, value TEXT)")
    first.db.commit()
    first.register_table("first_state", ["id"])
    second = ChronosBranchContext.connect(url)
    second.db.execute("CREATE TABLE second_state (id TEXT PRIMARY KEY, value TEXT)")
    second.db.commit()
    second.register_table("second_state", ["id"])
    first.set_merge_table_scope(["first_state"])
    second.set_merge_table_scope(["second_state"])
    workspace = ChronosWorkspaceContext(
        stores={"first": first, "second": second},
        shared_metadata_url=url,
    )
    return workspace, first, second, url


def _seed_and_branch(
    workspace: ChronosWorkspaceContext,
    first: ChronosBranchContext,
    second: ChronosBranchContext,
) -> None:
    first.checkout("main").upsert_rows(
        "first_state", [{"id": "item", "value": "old-first"}]
    )
    second.checkout("main").upsert_rows(
        "second_state", [{"id": "item", "value": "old-second"}]
    )
    workspace.create_branch("agent", "main")
    first.checkout("agent").upsert_rows(
        "first_state", [{"id": "item", "value": "new-first"}]
    )
    second.checkout("agent").upsert_rows(
        "second_state", [{"id": "item", "value": "new-second"}]
    )


def _value(context: ChronosBranchContext, branch: str, table: str) -> str:
    return str(context.checkout(branch).query(f"SELECT value FROM {table}")[0]["value"])


def test_selective_merge_uses_one_native_head_and_no_workspace_metadata(
    tmp_path: Path,
) -> None:
    workspace, first, second, url = _workspace(tmp_path)
    try:
        _seed_and_branch(workspace, first, second)
        preview = workspace.merge_atomic_preview("agent", "main")
        selected = {
            change.change_id
            for change in preview.stores["first"].changes
            if change.change_id is not None
        }
        old_head = int(first.get_branch("main").current_ref)
        result = workspace.merge_atomic(
            "agent",
            "main",
            selection=MergeSelection.from_ids(sorted(selected)),
            preview_token=preview.preview_token,
            operation_id="select-first",
        )

        new_head = int(first.get_branch("main").current_ref)
        assert result.old_target_token.current_segment_id == old_head
        assert result.new_target_token.current_segment_id == new_head
        assert int(second.get_branch("main").current_ref) == new_head
        assert _value(first, "main", "first_state") == "new-first"
        assert _value(second, "main", "second_state") == "old-second"

        db = sqlite3.connect(url.removeprefix("sqlite:///"))
        try:
            metadata = db.execute(
                "SELECT metadata FROM _chronos_branch_interval_branches "
                "WHERE branch_id = 'main'"
            ).fetchone()[0]
            assert "_chronos_workspace" not in metadata
            names = {
                row[0]
                for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            assert not any(name.startswith("_chronos_workspace_") for name in names)
        finally:
            db.close()
    finally:
        workspace.close()


def test_reserved_changes_are_invisible_until_native_head_publish(
    tmp_path: Path,
) -> None:
    workspace, first, second, _ = _workspace(tmp_path)
    try:
        _seed_and_branch(workspace, first, second)
        preview = first.merge_preview("agent", "main")
        transaction = first.reserve_branch_transaction(
            "agent", "main", ["first"], metadata={"operation_id": "manual"}
        )
        first.stage_branch_transaction_changes(transaction, preview.changes)

        assert _value(first, "main", "first_state") == "old-first"
        first.publish_branch_transaction(transaction)
        assert _value(first, "main", "first_state") == "new-first"
        assert int(second.get_branch("main").current_ref) == (
            transaction.continuation_segment_id
        )
    finally:
        workspace.close()


def test_native_commit_record_blocks_source_and_target_writers(
    tmp_path: Path,
) -> None:
    workspace, first, second, _ = _workspace(tmp_path)
    try:
        _seed_and_branch(workspace, first, second)
        transaction = first.reserve_branch_transaction(
            "agent", "main", ["first", "second"]
        )
        for branch in ("agent", "main"):
            with pytest.raises(Exception, match="transaction_in_progress"):
                second.checkout(branch).upsert_rows(
                    "second_state", [{"id": "late", "value": "blocked"}]
                )
        first.abort_branch_transaction(transaction)
        second.checkout("main").upsert_rows(
            "second_state", [{"id": "late", "value": "allowed"}]
        )
    finally:
        workspace.close()


def test_abort_preserves_head_and_removes_native_reservation(tmp_path: Path) -> None:
    workspace, first, second, _ = _workspace(tmp_path)
    try:
        _seed_and_branch(workspace, first, second)
        old_head = int(first.get_branch("main").current_ref)
        transaction = first.reserve_branch_transaction(
            "agent", "main", ["first", "second"]
        )
        first.stage_branch_transaction_changes(
            transaction,
            first.merge_preview("agent", "main").changes,
        )
        first.abort_branch_transaction(transaction)

        assert int(first.get_branch("main").current_ref) == old_head
        assert _value(first, "main", "first_state") == "old-first"
        assert (
            first.db.execute(
                "SELECT count(*) AS count FROM _chronos_branch_transaction_commits"
            ).fetchone()["count"]
            == 0
        )
    finally:
        workspace.close()


def test_stale_preview_is_rejected_by_native_head_token(tmp_path: Path) -> None:
    workspace, first, second, _ = _workspace(tmp_path)
    try:
        _seed_and_branch(workspace, first, second)
        preview = workspace.merge_atomic_preview("agent", "main")
        workspace.create_branch("other", "main")
        with pytest.raises(StaleAtomicMergePreviewError):
            workspace.merge_atomic(
                "agent",
                "main",
                preview_token=preview.preview_token,
                operation_id="stale",
            )
    finally:
        workspace.close()


def test_writer_between_preview_and_reservation_is_revalidated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace, first, second, _ = _workspace(tmp_path)
    try:
        _seed_and_branch(workspace, first, second)
        preview = workspace.merge_atomic_preview("agent", "main")
        original = first.reserve_branch_transaction

        def write_then_reserve(*args, **kwargs):
            second.checkout("main").upsert_rows(
                "second_state", [{"id": "item", "value": "racing-target"}]
            )
            return original(*args, **kwargs)

        monkeypatch.setattr(first, "reserve_branch_transaction", write_then_reserve)
        with pytest.raises(StaleAtomicMergePreviewError, match="while reserving"):
            workspace.merge_atomic(
                "agent",
                "main",
                preview_token=preview.preview_token,
                operation_id="raced",
            )
        assert (
            first.db.execute(
                "SELECT count(*) AS count FROM _chronos_branch_transaction_commits"
            ).fetchone()["count"]
            == 0
        )
    finally:
        workspace.close()


def test_atomic_workspace_rejects_independent_metadata_planes(tmp_path: Path) -> None:
    first = ChronosBranchContext.connect(f"sqlite:///{tmp_path / 'first.sqlite'}")
    second = ChronosBranchContext.connect(f"sqlite:///{tmp_path / 'second.sqlite'}")
    try:
        with pytest.raises(ValueError, match="shared interval metadata plane"):
            ChronosWorkspaceContext(
                stores={"first": first, "second": second},
                shared_metadata_url=f"sqlite:///{tmp_path / 'first.sqlite'}",
            )
    finally:
        first.close()
        second.close()
