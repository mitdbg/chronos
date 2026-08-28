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
from chronos_core.workspace.chronosfs import ChronosFSStore


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


def test_atomic_filesystem_merge_ignores_created_then_deleted_tree(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'atomic-filesystem.sqlite'}"
    control = ChronosBranchContext.connect(url)
    filesystem = ChronosFSStore(control, block_size=8)
    filesystem.ensure()
    workspace = ChronosWorkspaceContext(
        filesystem=filesystem,
        shared_metadata_url=url,
    )
    try:
        workspace.create_branch("agent", "main")
        filesystem.mkdir("agent", "/.venv")
        filesystem.write_file(
            "agent",
            "/.venv/transient.py",
            b"temporary dependency\n" * 32,
        )
        filesystem.unlink("agent", "/.venv/transient.py")
        filesystem.rmdir("agent", "/.venv")

        preview = workspace.merge_atomic_preview("agent", "main")
        assert preview.change_ids == frozenset()

        old_head = control.get_branch("main").current_ref
        result = workspace.merge_atomic(
            "agent",
            "main",
            preview_token=preview.preview_token,
            operation_id="ignore-transient-tree",
        )
        assert result.status == "noop"
        assert control.get_branch("main").current_ref == old_head
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
                _prepared_preview=preview,
                _allow_stable_selection_rebase=True,
            )
        assert (
            first.db.execute(
                "SELECT count(*) AS count FROM _chronos_branch_transaction_commits"
            ).fetchone()["count"]
            == 0
        )
    finally:
        workspace.close()


def test_prepared_atomic_preview_is_reused_before_reservation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace, first, second, _ = _workspace(tmp_path)
    try:
        _seed_and_branch(workspace, first, second)
        calls = {"first": 0, "second": 0}
        first_preview = first.merge_preview
        second_preview = second.merge_preview

        def counted_first(source: str, target: str, *, policy=None):
            calls["first"] += 1
            return first_preview(source, target, policy=policy)

        def counted_second(source: str, target: str, *, policy=None):
            calls["second"] += 1
            return second_preview(source, target, policy=policy)

        monkeypatch.setattr(first, "merge_preview", counted_first)
        monkeypatch.setattr(second, "merge_preview", counted_second)

        preview = workspace.merge_atomic_preview("agent", "main")
        workspace.merge_atomic(
            "agent",
            "main",
            preview_token=preview.preview_token,
            operation_id="reuse-prepared-preview",
            _prepared_preview=preview,
        )

        # One reviewed preview plus one post-reservation revalidation. The
        # atomic apply path must not compute another preview before reserving.
        assert calls == {"first": 2, "second": 2}
    finally:
        workspace.close()


def test_source_change_after_prepared_preview_is_not_silently_rebased(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace, first, second, _ = _workspace(tmp_path)
    try:
        _seed_and_branch(workspace, first, second)
        preview = workspace.merge_atomic_preview("agent", "main")
        selected = {
            change.change_id
            for change in preview.stores["first"].changes
            if change.change_id is not None
        }
        original = first.reserve_branch_transaction

        def write_source_then_reserve(*args, **kwargs):
            second.checkout("agent").upsert_rows(
                "second_state", [{"id": "late", "value": "unreviewed"}]
            )
            return original(*args, **kwargs)

        monkeypatch.setattr(
            first,
            "reserve_branch_transaction",
            write_source_then_reserve,
        )
        with pytest.raises(StaleAtomicMergePreviewError, match="while reserving"):
            workspace.merge_atomic(
                "agent",
                "main",
                selection=MergeSelection.from_ids(sorted(selected)),
                preview_token=preview.preview_token,
                operation_id="source-raced",
                _prepared_preview=preview,
                _allow_stable_selection_rebase=True,
            )
    finally:
        workspace.close()


def test_stable_disjoint_selection_rebases_after_target_advance(
    tmp_path: Path,
) -> None:
    workspace, first, second, _ = _workspace(tmp_path)
    try:
        first.checkout("main").upsert_rows(
            "first_state", [{"id": "first", "value": "old-first"}]
        )
        second.checkout("main").upsert_rows(
            "second_state", [{"id": "second", "value": "old-second"}]
        )
        workspace.create_branch("first-agent", "main")
        workspace.create_branch("second-agent", "main")
        first.checkout("first-agent").upsert_rows(
            "first_state", [{"id": "first", "value": "new-first"}]
        )
        second.checkout("second-agent").upsert_rows(
            "second_state", [{"id": "second", "value": "new-second"}]
        )
        first_preview = workspace.merge_atomic_preview("first-agent", "main")
        second_preview = workspace.merge_atomic_preview("second-agent", "main")

        workspace.merge_atomic(
            "first-agent",
            "main",
            selection=MergeSelection.from_ids(sorted(first_preview.change_ids)),
            preview_token=first_preview.preview_token,
            operation_id="merge-first",
            _prepared_preview=first_preview,
            _allow_stable_selection_rebase=True,
        )
        result = workspace.merge_atomic(
            "second-agent",
            "main",
            selection=MergeSelection.from_ids(sorted(second_preview.change_ids)),
            preview_token=second_preview.preview_token,
            operation_id="merge-second",
            _prepared_preview=second_preview,
            _allow_stable_selection_rebase=True,
        )

        assert result.status == "committed"
        assert _value(first, "main", "first_state") == "new-first"
        assert _value(second, "main", "second_state") == "new-second"
    finally:
        workspace.close()


def test_competing_selection_is_not_rebased_after_target_advance(
    tmp_path: Path,
) -> None:
    workspace, first, second, _ = _workspace(tmp_path)
    try:
        first.checkout("main").upsert_rows(
            "first_state", [{"id": "item", "value": "old"}]
        )
        workspace.create_branch("first-agent", "main")
        workspace.create_branch("second-agent", "main")
        first.checkout("first-agent").upsert_rows(
            "first_state", [{"id": "item", "value": "first"}]
        )
        first.checkout("second-agent").upsert_rows(
            "first_state", [{"id": "item", "value": "second"}]
        )
        first_preview = workspace.merge_atomic_preview("first-agent", "main")
        second_preview = workspace.merge_atomic_preview("second-agent", "main")

        workspace.merge_atomic(
            "first-agent",
            "main",
            selection=MergeSelection.from_ids(sorted(first_preview.change_ids)),
            preview_token=first_preview.preview_token,
            operation_id="merge-first",
            _prepared_preview=first_preview,
            _allow_stable_selection_rebase=True,
        )
        with pytest.raises(StaleAtomicMergePreviewError, match="while reserving"):
            workspace.merge_atomic(
                "second-agent",
                "main",
                selection=MergeSelection.from_ids(sorted(second_preview.change_ids)),
                preview_token=second_preview.preview_token,
                operation_id="merge-second",
                _prepared_preview=second_preview,
                _allow_stable_selection_rebase=True,
            )

        assert _value(first, "main", "first_state") == "first"
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


def test_checkout_reads_shared_head_once_and_remains_writable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace, first, second, _ = _workspace(tmp_path)
    try:
        first_get_branch = first._backend.get_branch
        second_get_branch = second._backend.get_branch
        calls = {"first": 0, "second": 0}

        def counted_first(branch_id: str):
            calls["first"] += 1
            return first_get_branch(branch_id)

        def counted_second(branch_id: str):
            calls["second"] += 1
            return second_get_branch(branch_id)

        monkeypatch.setattr(first._backend, "get_branch", counted_first)
        monkeypatch.setattr(second._backend, "get_branch", counted_second)

        session = workspace.checkout("main")

        # The workspace obtains the head from its relational control plane
        # once. Participants prepare the same interval directly rather than
        # independently rereading branch metadata.
        assert calls == {"first": 1, "second": 0}
        assert session.first.current_ref == session.second.current_ref

        session.first.upsert_rows("first_state", [{"id": "one", "value": "first"}])
        session.second.upsert_rows("second_state", [{"id": "two", "value": "second"}])
        assert session.first.query("SELECT value FROM first_state") == [
            {"value": "first"}
        ]
        assert session.second.query("SELECT value FROM second_state") == [
            {"value": "second"}
        ]
    finally:
        workspace.close()


def test_checked_out_workspace_session_follows_live_branch_after_split(
    tmp_path: Path,
) -> None:
    workspace, first, second, _ = _workspace(tmp_path)
    try:
        live = workspace.checkout("main")
        original_ref = live.first.current_ref

        workspace.create_branch("child", "main")

        # Branch creation advances the writable main interval. An existing
        # workspace session refreshes lazily and writes to that new live
        # interval instead of remaining pinned to the checkout-time snapshot.
        live.first.upsert_rows("first_state", [{"id": "after", "value": "live"}])
        assert live.first.current_ref != original_ref
        assert live.first.current_ref == live.second.current_ref
        assert first.checkout("main").query(
            "SELECT value FROM first_state WHERE id = 'after'"
        ) == [{"value": "live"}]
        assert (
            first.checkout("child").query(
                "SELECT value FROM first_state WHERE id = 'after'"
            )
            == []
        )
    finally:
        workspace.close()
