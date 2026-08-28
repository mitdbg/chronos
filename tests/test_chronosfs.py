from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import textwrap
import time
import asyncio
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest

from chronos_core.branching import ChronosBranchContext, MergeResolution
from chronos_core.workspace import ChronosWorkspaceContext
from chronos_core.workspace.chronosfs import (
    ChronosFSError,
    ChronosFSStore,
    ChronosFuseOperations,
)


@pytest.fixture
def chronosfs(tmp_path: Path) -> ChronosFSStore:
    ctx = ChronosBranchContext.connect(f"sqlite:///{tmp_path / 'chronosfs.db'}", backend="interval")
    store = ChronosFSStore(ctx, block_size=8)
    store.ensure()
    try:
        yield store
    finally:
        store.close()


def test_direct_file_operations_are_branch_isolated(chronosfs: ChronosFSStore) -> None:
    chronosfs.mkdir("main", "/notes")
    chronosfs.write_file("main", "/notes/plan.txt", "main plan\n")
    chronosfs.create_branch("agent", from_branch="main")

    chronosfs.write_at("agent", "/notes/plan.txt", len("main "), "branch")
    chronosfs.write_file("agent", "/notes/agent.txt", b"private\n")

    assert chronosfs.read_text("main", "/notes/plan.txt") == "main plan\n"
    assert not chronosfs.exists("main", "/notes/agent.txt")
    assert chronosfs.read_text("agent", "/notes/plan.txt") == "main branch"
    assert chronosfs.read_text("agent", "/notes/agent.txt") == "private\n"

    main = chronosfs.manifest("main")
    agent = chronosfs.manifest("agent")
    assert "notes/plan.txt" in main
    assert "notes/agent.txt" not in main
    assert "notes/agent.txt" in agent


def test_workspace_checkout_reuses_chronosfs_interval_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata_url = f"sqlite:///{tmp_path / 'knowledge.sqlite'}"
    filesystem = ChronosFSStore.connect(
        f"sqlite:///{tmp_path / 'chronosfs.sqlite'}",
        metadata_url=metadata_url,
        block_size=8,
    )
    filesystem.ensure()
    relational = ChronosBranchContext.connect(metadata_url)
    workspace = ChronosWorkspaceContext(
        filesystem=filesystem,
        relational=relational,
        shared_metadata_url=metadata_url,
    )
    checkout_calls = 0
    metadata_checkout_calls = 0

    class CountedNative:
        def __getattr__(self, name: str):
            return getattr(native, name)

        def checkout_segment(self, *args, **kwargs):
            nonlocal checkout_calls
            checkout_calls += 1
            return native.checkout_segment(*args, **kwargs)

    native = filesystem._native
    monkeypatch.setattr(filesystem, "_native", CountedNative())
    filesystem_checkout_ref = filesystem.context.checkout_ref

    def counted_metadata_checkout_ref(*args, **kwargs):
        nonlocal metadata_checkout_calls
        metadata_checkout_calls += 1
        return filesystem_checkout_ref(*args, **kwargs)

    monkeypatch.setattr(
        filesystem.context,
        "checkout_ref",
        counted_metadata_checkout_ref,
    )
    try:
        first = workspace.checkout("main")
        second = workspace.checkout("main")

        # Repeated checkout at the same head must not reconstruct the native
        # filesystem view or create a separate filesystem metadata session.
        # Both handles remain writable live branch handles.
        assert checkout_calls == 1
        assert metadata_checkout_calls == 0
        first.fs.write_file("/one.txt", b"one")
        second.fs.write_file("/two.txt", b"two")
        assert first.fs.read_file("/two.txt") == b"two"

        workspace.create_branch("child", "main")
        current = workspace.checkout("main")
        assert checkout_calls == 2
        current.fs.write_file("/main-only.txt", b"main")
        assert not workspace.checkout("child").fs.exists("/main-only.txt")
    finally:
        workspace.close()


def test_fixed_blocks_use_chronos_interval_cow(chronosfs: ChronosFSStore) -> None:
    chronosfs.write_file("main", "/blob.bin", b"aaaaaaaabbbbbbbbcccccccc")
    chronosfs.create_branch("child", from_branch="main")

    chronosfs.write_at("child", "/blob.bin", 10, b"XX")

    assert chronosfs.read_file("main", "/blob.bin") == b"aaaaaaaabbbbbbbbcccccccc"
    assert chronosfs.read_file("child", "/blob.bin") == b"aaaaaaaabbXXbbbbcccccccc"

    rows = chronosfs.context.db.execute(
        """
        SELECT byte_start, count(*) AS c
        FROM _chronos_b_interval_chronosfs_file_blocks
        GROUP BY byte_start
        ORDER BY byte_start
        """
    ).fetchall()
    counts = {int(row["byte_start"]): int(row["c"]) for row in rows}
    assert counts[0] == 1
    assert counts[8] > counts[0]
    assert counts[16] == 1


def test_sqlite_fixed_block_private_rewrite_stops_at_child_branch(
    chronosfs: ChronosFSStore,
) -> None:
    chronosfs.write_file("main", "/blob.bin", b"aaaaaaaabbbbbbbbcccccccc")

    def block_count(byte_start: int) -> int:
        return chronosfs.context.db.execute(
            """
            SELECT COUNT(*) AS count
            FROM _chronos_b_interval_chronosfs_file_blocks
            WHERE byte_start = ?
            """,
            (byte_start,),
        ).fetchone()["count"]

    base_count = block_count(8)
    chronosfs.write_at("main", "/blob.bin", 8, b"BBBBBBBB")
    after_private_rewrite = block_count(8)
    assert after_private_rewrite == base_count
    assert chronosfs.read_file("main", "/blob.bin") == b"aaaaaaaaBBBBBBBBcccccccc"

    chronosfs.create_branch("child", from_branch="main")
    assert chronosfs.read_file("child", "/blob.bin") == b"aaaaaaaaBBBBBBBBcccccccc"

    chronosfs.write_at("main", "/blob.bin", 8, b"CCCCCCCC")
    assert block_count(8) > after_private_rewrite
    assert chronosfs.read_file("main", "/blob.bin") == b"aaaaaaaaCCCCCCCCcccccccc"
    assert chronosfs.read_file("child", "/blob.bin") == b"aaaaaaaaBBBBBBBBcccccccc"


def test_path_cache_invalidates_exact_file_and_subtree(
    chronosfs: ChronosFSStore,
) -> None:
    chronosfs.mkdir("main", "/cached", parents=True)
    chronosfs.write_file("main", "/cached/a.txt", b"a")
    chronosfs.write_file("main", "/cached/b.txt", b"b")
    assert chronosfs.listdir("main", "/cached") == ["a.txt", "b.txt"]

    chronosfs.unlink("main", "/cached/a.txt")
    assert not chronosfs.exists("main", "/cached/a.txt")
    assert chronosfs.read_file("main", "/cached/b.txt") == b"b"

    chronosfs.mkdir("main", "/old/sub", parents=True)
    chronosfs.write_file("main", "/old/sub/file.txt", b"payload")
    assert chronosfs.listdir("main", "/old") == ["sub"]
    assert chronosfs.listdir("main", "/old/sub") == ["file.txt"]

    chronosfs.rename("main", "/old", "/new")
    assert not chronosfs.exists("main", "/old/sub/file.txt")
    assert chronosfs.read_file("main", "/new/sub/file.txt") == b"payload"


def test_unlink_defers_leaf_branch_block_reclamation_until_branch_gc(
    chronosfs: ChronosFSStore,
) -> None:
    chronosfs.create_branch("child", from_branch="main")
    chronosfs.write_file("child", "/temporary.bin", b"temporary payload")
    inode_id = chronosfs.stat("child", "/temporary.bin").inode_id
    child_segment = int(chronosfs.context.get_branch("child").current_ref)

    chronosfs.unlink("child", "/temporary.bin")

    assert not chronosfs.exists("child", "/temporary.bin")
    assert chronosfs.diff("main", "child").changes == []
    chronosfs.merge_apply("child", "main")

    # Dead extents are excluded from merge without synchronously rewriting a
    # potentially large sandbox. They remain owned by the source segment until
    # asynchronous branch garbage collection reclaims that segment.
    assert chronosfs.context.db.execute(
        """
        SELECT 1
        FROM _chronos_b_interval_chronosfs_file_blocks
        WHERE inode_id = ?
          AND writer_segment_id = ?
        """,
        (inode_id, child_segment),
    ).fetchone() is not None

    chronosfs.delete_branch("child")
    chronosfs._native.wait_for_gc()
    assert chronosfs.context.db.execute(
        """
        SELECT 1
        FROM _chronos_b_interval_chronosfs_file_blocks
        WHERE inode_id = ?
          AND writer_segment_id = ?
        """,
        (inode_id, child_segment),
    ).fetchone() is None


def test_unlink_of_inherited_file_still_merges_deletion(
    chronosfs: ChronosFSStore,
) -> None:
    chronosfs.write_file("main", "/inherited.bin", b"inherited payload")
    chronosfs.create_branch("child", from_branch="main")

    chronosfs.unlink("child", "/inherited.bin")
    chronosfs.merge_apply("child", "main")

    assert not chronosfs.exists("main", "/inherited.bin")


def test_lazy_import_records_external_extent_and_reads_source(
    chronosfs: ChronosFSStore,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    payload = b"abcdefgh" * 32
    (source / "large.bin").write_bytes(payload)
    (source / "sub").mkdir()
    (source / "sub" / "note.txt").write_text("nested\n")

    chronosfs.import_tree("main", source)

    assert chronosfs.read_file("main", "/large.bin") == payload
    assert chronosfs.read_text("main", "/sub/note.txt") == "nested\n"
    rows = chronosfs.context.db.execute(
        """
        SELECT byte_start, byte_end, external, length(data) AS data_len
        FROM _chronos_b_interval_chronosfs_file_blocks
        WHERE byte_end = ?
        """,
        (len(payload),),
    ).fetchall()
    assert [dict(row) for row in rows] == [
        {
            "byte_start": 0,
            "byte_end": len(payload),
            "external": 1,
            "data_len": None,
        }
    ]


def test_lazy_import_overwrite_splits_external_extent_by_block(
    chronosfs: ChronosFSStore,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "data.bin").write_bytes(b"aaaaaaaabbbbbbbbcccccccc")
    chronosfs.import_tree("main", source)
    chronosfs.create_branch("child", from_branch="main")

    chronosfs.write_at("child", "/data.bin", 10, b"XX")

    assert chronosfs.read_file("main", "/data.bin") == b"aaaaaaaabbbbbbbbcccccccc"
    assert chronosfs.read_file("child", "/data.bin") == b"aaaaaaaabbXXbbbbcccccccc"
    rows = chronosfs.context.checkout("child").query(
        """
        SELECT byte_start, byte_end, external, length(data) AS data_len
        FROM chronosfs_file_blocks
        ORDER BY byte_start
        """
    )
    assert [dict(row) for row in rows] == [
        {"byte_start": 0, "byte_end": 8, "external": 1, "data_len": None},
        {"byte_start": 8, "byte_end": 16, "external": 0, "data_len": 8},
        {"byte_start": 16, "byte_end": 24, "external": 1, "data_len": None},
    ]


def test_lazy_import_nested_branch_inherits_external_and_inline_ranges(
    chronosfs: ChronosFSStore,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "data.bin").write_bytes(b"aaaaaaaabbbbbbbbcccccccc")
    chronosfs.import_tree("main", source)

    chronosfs.create_branch("a", from_branch="main")
    chronosfs.write_at("a", "/data.bin", 8, b"BBBB")
    chronosfs.create_branch("b", from_branch="a")
    chronosfs.write_at("b", "/data.bin", 16, b"CCCC")

    assert chronosfs.read_file("main", "/data.bin") == b"aaaaaaaabbbbbbbbcccccccc"
    assert chronosfs.read_file("a", "/data.bin") == b"aaaaaaaaBBBBbbbbcccccccc"
    assert chronosfs.read_file("b", "/data.bin") == b"aaaaaaaaBBBBbbbbCCCCcccc"


def test_lazy_import_delete_and_rename_are_metadata_only(
    chronosfs: ChronosFSStore,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "keep.txt").write_text("keep\n")
    (source / "move.txt").write_text("move\n")
    chronosfs.import_tree("main", source)
    chronosfs.create_branch("child", from_branch="main")

    chronosfs.unlink("child", "/keep.txt")
    chronosfs.rename("child", "/move.txt", "/moved.txt")

    assert chronosfs.exists("main", "/keep.txt")
    assert chronosfs.read_text("main", "/move.txt") == "move\n"
    assert not chronosfs.exists("child", "/keep.txt")
    assert chronosfs.read_text("child", "/moved.txt") == "move\n"
    assert (source / "keep.txt").read_text() == "keep\n"
    assert (source / "move.txt").read_text() == "move\n"


def test_write_at_patches_touched_blocks(chronosfs: ChronosFSStore) -> None:
    chronosfs.write_file("main", "/blob.bin", b"aaaaaaaabbbbbbbbcccccccc")

    chronosfs.write_at("main", "/blob.bin", 6, b"XXYYZZQQ")

    assert chronosfs.read_file("main", "/blob.bin") == b"aaaaaaXXYYZZQQbbcccccccc"


def test_native_inode_allocator_reserves_monotonic_inode_ids(
    chronosfs: ChronosFSStore,
) -> None:
    def next_inode_id() -> int:
        row = chronosfs.context.db.execute(
            "SELECT next_inode_id FROM _chronosfs_inode_allocator WHERE id = 1"
        ).fetchone()
        return int(row["next_inode_id"])

    before = next_inode_id()
    chronosfs.write_file("main", "/created.bin", b"committed")

    after_first = next_inode_id()
    assert after_first > before
    assert chronosfs.exists("main", "/created.bin")
    first_inode = chronosfs.stat("main", "/created.bin").inode_id

    chronosfs.write_file("main", "/created2.bin", b"committed")

    second_inode = chronosfs.stat("main", "/created2.bin").inode_id
    assert second_inode > first_inode
    assert next_inode_id() >= after_first


def test_truncate_and_sparse_reads(chronosfs: ChronosFSStore) -> None:
    chronosfs.write_file("main", "/sparse.bin", b"abc")
    chronosfs.truncate("main", "/sparse.bin", 20)
    assert chronosfs.read_file("main", "/sparse.bin") == b"abc" + (b"\x00" * 17)

    chronosfs.write_at("main", "/sparse.bin", 12, b"XYZ")
    data = chronosfs.read_file("main", "/sparse.bin")
    assert len(data) == 20
    assert data[:3] == b"abc"
    assert data[3:12] == b"\x00" * 9
    assert data[12:15] == b"XYZ"

    chronosfs.truncate("main", "/sparse.bin", 2)
    assert chronosfs.read_file("main", "/sparse.bin") == b"ab"


def test_truncate_shrink_then_grow_does_not_expose_old_blocks(
    chronosfs: ChronosFSStore,
) -> None:
    chronosfs.write_file("main", "/grow.bin", b"abcdefghABCDEFGH")

    chronosfs.truncate("main", "/grow.bin", 2)
    chronosfs.truncate("main", "/grow.bin", 16)

    assert chronosfs.read_file("main", "/grow.bin") == b"ab" + (b"\x00" * 14)


def test_inode_range_read_reads_overlapping_blocks(
    chronosfs: ChronosFSStore,
) -> None:
    chronosfs.write_file("main", "/range.bin", b"aaaaaaaabbbbbbbbcccccccc")
    inode_id = chronosfs.stat("main", "/range.bin").inode_id

    assert chronosfs.read_inode_range("main", inode_id, 9, 5) == b"bbbbb"


def test_inode_range_read_handles_sparse_and_cross_block_reads(
    chronosfs: ChronosFSStore,
) -> None:
    chronosfs.write_file("main", "/range-sparse.bin", b"abc")
    chronosfs.truncate("main", "/range-sparse.bin", 20)
    chronosfs.write_at("main", "/range-sparse.bin", 12, b"XYZ")
    inode_id = chronosfs.stat("main", "/range-sparse.bin").inode_id

    assert chronosfs.read_inode_range("main", inode_id, 2, 14) == (
        b"c" + (b"\x00" * 9) + b"XYZ" + b"\x00"
    )
    assert chronosfs.read_inode_range("main", inode_id, 20, 10) == b""


def test_fuse_read_dispatches_to_public_inode_range(
    chronosfs: ChronosFSStore,
) -> None:
    chronosfs.write_file("main", "/file.txt", b"0123456789")
    inode_id = chronosfs.stat("main", "/file.txt").inode_id
    ops = ChronosFuseOperations(chronosfs, branch_id="main")
    fh = ops._new_handle(inode_id)

    assert asyncio.run(ops.read(fh, 3, 3)) == b"345"


def test_fuse_readdir_uses_public_inode_lookup(
    chronosfs: ChronosFSStore,
) -> None:
    chronosfs.mkdir("main", "/dir")
    chronosfs.write_file("main", "/dir/a.txt", b"a")
    chronosfs.write_file("main", "/dir/b.txt", b"b")
    inode_id = chronosfs.stat("main", "/dir").inode_id
    ops = ChronosFuseOperations(chronosfs, branch_id="main")

    entries = ops._regular_readdir(inode_id)
    assert [name for name, _attr in entries] == [".", "..", "a.txt", "b.txt"]


def test_checkpoint_and_restore_are_stable(chronosfs: ChronosFSStore) -> None:
    chronosfs.write_file("main", "/plan.txt", "base\n")
    chronosfs.create_branch("work", from_branch="main")
    chronosfs.write_file("work", "/plan.txt", "checkpoint\n")
    chronosfs.write_file("work", "/only-at-checkpoint.txt", "yes\n")
    chronosfs.create_checkpoint("snap", branch="work")

    chronosfs.write_file("work", "/plan.txt", "later\n")
    chronosfs.unlink("work", "/only-at-checkpoint.txt")
    chronosfs.create_branch_from_checkpoint("restored", "snap")

    assert chronosfs.read_text("restored", "/plan.txt") == "checkpoint\n"
    assert chronosfs.read_text("restored", "/only-at-checkpoint.txt") == "yes\n"
    assert chronosfs.read_text("work", "/plan.txt") == "later\n"
    assert not chronosfs.exists("work", "/only-at-checkpoint.txt")


def test_rename_symlink_and_merge_apply(chronosfs: ChronosFSStore) -> None:
    chronosfs.write_file("main", "/target.txt", "main target\n")
    chronosfs.mkdir("main", "/dir")
    chronosfs.write_file("main", "/dir/nested.txt", "nested\n")
    chronosfs.create_branch("source", from_branch="main")

    chronosfs.rename("source", "/target.txt", "/renamed.txt")
    chronosfs.symlink("source", "dir/nested.txt", "/link.txt")
    chronosfs.write_file("source", "/dir/nested.txt", "changed\n")

    diff = chronosfs.diff("main", "source")
    assert {"target.txt", "renamed.txt", "link.txt", "dir/nested.txt"} <= {
        change.path for change in diff.changes
    }

    result = chronosfs.merge_apply("source", "main")
    assert result.applied >= 4
    assert not chronosfs.exists("main", "/target.txt")
    assert chronosfs.read_text("main", "/renamed.txt") == "main target\n"
    assert chronosfs.readlink("main", "/link.txt") == "dir/nested.txt"
    assert chronosfs.read_text("main", "/link.txt") == "changed\n"


def test_merge_preview_omits_created_then_deleted_tree(
    chronosfs: ChronosFSStore,
) -> None:
    chronosfs.create_branch("agent", from_branch="main")
    chronosfs.mkdir("agent", "/.venv")
    chronosfs.mkdir("agent", "/.venv/lib")
    chronosfs.write_file(
        "agent",
        "/.venv/lib/transient.py",
        b"temporary dependency\n" * 32,
    )
    chronosfs.unlink("agent", "/.venv/lib/transient.py")
    chronosfs.rmdir("agent", "/.venv/lib")
    chronosfs.rmdir("agent", "/.venv")

    preview = chronosfs.merge_preview("agent", "main")

    assert preview.changes == []
    assert preview.conflicts == []


def test_merge_preview_does_not_use_a_live_stat_side_read(
    chronosfs: ChronosFSStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Path metadata comes from the preview's pinned interval sessions."""

    chronosfs.write_file("main", "/incident.md", b"base")
    chronosfs.create_branch("worker", from_branch="main")
    chronosfs.write_file("worker", "/incident.md", b"worker")

    def forbidden_stat(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("merge preview must not perform a live stat lookup")

    monkeypatch.setattr(chronosfs, "stat", forbidden_stat)

    preview = chronosfs.merge_preview("worker", "main")

    assert any(change.key.get("path") == "/incident.md" for change in preview.changes)


def test_merge_preview_holds_metadata_snapshot_lock_during_path_resolution(
    chronosfs: ChronosFSStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chronosfs.write_file("main", "/incident.md", b"base")
    chronosfs.create_branch("worker", from_branch="main")
    chronosfs.write_file("worker", "/incident.md", b"worker-v1")

    original_path_for_inode = chronosfs._path_for_inode_in_branch
    observed_lock = False

    def observe_lock(
        branch_id: str,
        inode_id: int,
        *,
        session: object,
    ) -> str | None:
        nonlocal observed_lock
        observed_lock = observed_lock or chronosfs.context.metadata_db.in_transaction
        return original_path_for_inode(branch_id, inode_id, session=session)

    monkeypatch.setattr(chronosfs, "_path_for_inode_in_branch", observe_lock)

    preview = chronosfs.merge_preview("worker", "main")

    assert observed_lock
    assert any(change.key.get("path") == "/incident.md" for change in preview.changes)


def test_policy_merge_resolves_chronosfs_content_at_block_granularity(
    chronosfs: ChronosFSStore,
) -> None:
    chronosfs.write_file("main", "/data.bin", b"aaaaaaaabbbbbbbb")
    chronosfs.create_branch("agent", from_branch="main")

    chronosfs.write_at("main", "/data.bin", 0, b"MAIN")
    chronosfs.write_at("agent", "/data.bin", 8, b"AGNT")

    preview = chronosfs.merge_preview("agent", "main", policy="snapshot_isolation")
    assert preview.conflicts == []

    result = chronosfs.merge_apply("agent", "main", policy="snapshot_isolation")

    assert result.applied >= 1
    assert chronosfs.read_file("main", "/data.bin") == b"MAINaaaaAGNTbbbb"


def test_weak_snapshot_chronosfs_same_block_conflict_source_wins(
    chronosfs: ChronosFSStore,
) -> None:
    chronosfs.write_file("main", "/data.bin", b"aaaaaaaabbbbbbbb")
    chronosfs.create_branch("agent", from_branch="main")

    chronosfs.write_at("main", "/data.bin", 8, b"MAIN")
    chronosfs.write_at("agent", "/data.bin", 8, b"AGNT")

    preview = chronosfs.merge_preview("agent", "main", policy="manual_review")
    block_conflicts = [
        conflict
        for conflict in preview.conflicts
        if conflict.table == "chronosfs_file_range"
    ]
    assert len(block_conflicts) == 1
    assert block_conflicts[0].key == {
        "path": "/data.bin",
        "byte_range": {"start": 8, "end": 16},
    }
    assert block_conflicts[0].after is not None
    assert "unified_diff" in block_conflicts[0].after
    assert "block_index" not in repr(block_conflicts[0])

    result = chronosfs.merge_apply("agent", "main", policy="weak_snapshot_isolation")

    assert result.applied >= 1
    assert chronosfs.read_file("main", "/data.bin") == b"aaaaaaaaAGNTbbbb"


def test_manual_review_chronosfs_file_range_resolution_uses_public_conflict_id(
    chronosfs: ChronosFSStore,
) -> None:
    chronosfs.write_file("main", "/data.txt", b"aaaaaaaa\nbbbbbbbb\n")
    chronosfs.create_branch("agent", from_branch="main")

    chronosfs.write_at("main", "/data.txt", 9, b"MAIN")
    chronosfs.write_at("agent", "/data.txt", 9, b"AGNT")

    preview = chronosfs.merge_preview("agent", "main", policy="manual_review")
    assert len(preview.conflicts) == 1
    conflict = preview.conflicts[0]
    assert conflict.table == "chronosfs_file_range"
    assert conflict.conflict_id is not None
    assert conflict.key["path"] == "/data.txt"
    assert conflict.after is not None
    assert "AGNT" in conflict.after["unified_diff"]

    result = chronosfs.merge_apply(
        "agent",
        "main",
        MergeResolution({conflict.conflict_id: "source"}),
        policy="manual_review",
    )

    assert result.applied >= 1
    assert chronosfs.read_file("main", "/data.txt") == b"aaaaaaaa\nAGNTbbbb\n"


def test_errors_for_reserved_and_invalid_paths(chronosfs: ChronosFSStore) -> None:
    with pytest.raises(ChronosFSError):
        chronosfs.write_file("main", ".chronos/current", "bad")
    with pytest.raises(ChronosFSError):
        chronosfs.write_file("main", "../escape", "bad")


def test_postgres_direct_file_blocks_are_branch_isolated() -> None:
    dsn = _reset_and_get_postgres_dsn()
    store = ChronosFSStore.connect(dsn, backend="interval", block_size=8)
    store.ensure()
    try:
        store.write_file("main", "/blob.bin", b"aaaaaaaabbbbbbbbcccccccc")
        store.create_branch("agent", from_branch="main")
        store.write_at("agent", "/blob.bin", 10, b"XX")

        assert store.read_file("main", "/blob.bin") == b"aaaaaaaabbbbbbbbcccccccc"
        assert store.read_file("agent", "/blob.bin") == b"aaaaaaaabbXXbbbbcccccccc"

        rows = store.context.db.execute(
            """
            SELECT byte_start, count(*) AS c
            FROM _chronos_b_interval_chronosfs_file_blocks
            GROUP BY byte_start
            ORDER BY byte_start
            """
        ).fetchall()
        counts = {int(row["byte_start"]): int(row["c"]) for row in rows}
        assert counts[0] == 1
        assert counts[8] > counts[0]
        assert counts[16] == 1
    finally:
        store.close()


def test_postgres_fuse_mount_supports_normal_bash_tools(tmp_path: Path) -> None:
    _require_fuse_tools()
    dsn = _reset_and_get_postgres_dsn()
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    with _mounted_chronosfs_database(dsn, mountpoint, script_dir=tmp_path):
        result = _run_bash(
            mountpoint,
            "\n".join(
                [
                    "set -euo pipefail",
                    "mkdir -p pg",
                    "printf 'postgres fuse\\n' > pg/a.txt",
                    "cp pg/a.txt pg/b.txt",
                    "truncate -s 12288 pg/blob.bin",
                    "printf CHRONOS | dd of=pg/blob.bin bs=1 seek=4097 conv=notrunc status=none",
                    "cat pg/b.txt",
                ]
            ),
        )
        assert result.stdout == "postgres fuse\n"
        assert (mountpoint / "pg" / "blob.bin").read_bytes()[4097:4104] == b"CHRONOS"

    store = ChronosFSStore.connect(dsn, backend="interval")
    store.ensure()
    try:
        assert store.read_text("main", "/pg/b.txt") == "postgres fuse\n"
        data = store.read_file("main", "/pg/blob.bin")
        assert len(data) == 12288
        assert data[4097:4104] == b"CHRONOS"
    finally:
        store.close()


def test_fuse_mount_supports_normal_bash_tools(tmp_path: Path) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    with _mounted_chronosfs(db_path, mountpoint):
        result = _run_bash(
            mountpoint,
            "\n".join(
                [
                    "set -euo pipefail",
                    "mkdir -p notes",
                    "printf 'hello from bash\\n' > notes/a.txt",
                    "cp notes/a.txt notes/b.txt",
                    "mv notes/b.txt notes/c.txt",
                    "chmod +x notes/c.txt",
                    "touch notes/c.txt",
                    "test -x notes/c.txt",
                    "rm notes/a.txt",
                    "truncate -s 20 sparse.bin",
                    "printf XYZ | dd of=sparse.bin bs=1 seek=5 conv=notrunc status=none",
                    "ln -s notes/c.txt link.txt",
                    "cat notes/c.txt",
                ]
            ),
        )
        assert result.stdout == "hello from bash\n"
        assert (mountpoint / "link.txt").read_text() == "hello from bash\n"

    store = _open_store(db_path)
    try:
        assert not store.exists("main", "/notes/a.txt")
        assert store.read_text("main", "/notes/c.txt") == "hello from bash\n"
        assert store.stat("main", "/notes/c.txt").mode & 0o111
        sparse = store.read_file("main", "/sparse.bin")
        assert len(sparse) == 20
        assert sparse[5:8] == b"XYZ"
        assert store.readlink("main", "/link.txt") == "notes/c.txt"
    finally:
        store.close()


def test_fuse_shell_redirection_truncates_existing_file(tmp_path: Path) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    with _mounted_chronosfs(db_path, mountpoint):
        result = _run_bash(
            mountpoint,
            "\n".join(
                [
                    "set -euo pipefail",
                    "printf 'old-old-old-old-old-old-old-old-old\\n' > Makefile.dep",
                    "printf 'abc\\ndef\\n' > Makefile.dep",
                    "python3 - <<'PY'",
                    "from pathlib import Path",
                    "data = Path('Makefile.dep').read_bytes()",
                    "assert data == b'abc\\ndef\\n', data",
                    "print(data.decode(), end='')",
                    "PY",
                    ": > Makefile.dep",
                    "test ! -s Makefile.dep",
                    "printf 'x\\n' >> Makefile.dep",
                    "python3 - <<'PY'",
                    "from pathlib import Path",
                    "data = Path('Makefile.dep').read_bytes()",
                    "assert data == b'x\\n', data",
                    "PY",
                ]
            ),
        )
        assert result.stdout == "abc\ndef\n"

    store = _open_store(db_path)
    try:
        assert store.read_file("main", "/Makefile.dep") == b"x\n"
    finally:
        store.close()


def test_fuse_sequential_rewrites_observe_complete_inherited_file(
    tmp_path: Path,
) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    store = _open_store(db_path)
    try:
        store.write_file("main", "/module.py", b"x" * 3838)
        store.create_branch("agent", "main")
    finally:
        store.close()

    payloads = [
        b"a" * 5491,
        b"b" * 5300,
        b"c" * 5301,
        b"d" * 5325,
        b"e" * 5357,
    ]
    with _mounted_chronosfs(db_path, mountpoint, branch_id="agent"):
        target = mountpoint / "module.py"
        for payload in payloads:
            target.write_bytes(payload)
            assert target.read_bytes() == payload

    store = _open_store(db_path)
    try:
        assert store.read_file("agent", "/module.py") == payloads[-1]
    finally:
        store.close()


def test_fuse_open_reader_sees_write_from_other_handle(tmp_path: Path) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    with _mounted_chronosfs(db_path, mountpoint):
        _run_bash(
            mountpoint,
            "\n".join(
                [
                    "set -euo pipefail",
                    "python3 - <<'PY'",
                    "import os",
                    "with open('shared.txt', 'wb') as f:",
                    "    f.write(b'old\\n')",
                    "reader = os.open('shared.txt', os.O_RDONLY)",
                    "try:",
                    "    assert os.read(reader, 4) == b'old\\n'",
                    "    writer = os.open('shared.txt', os.O_WRONLY | os.O_TRUNC)",
                    "    try:",
                    "        os.write(writer, b'new\\n')",
                    "    finally:",
                    "        os.close(writer)",
                    "    os.lseek(reader, 0, os.SEEK_SET)",
                    "    data = os.read(reader, 4)",
                    "    assert data == b'new\\n', data",
                    "finally:",
                    "    os.close(reader)",
                    "PY",
                ]
            ),
        )


def test_fuse_fsync_publishes_buffered_handle_writes(tmp_path: Path) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    with _mounted_chronosfs(db_path, mountpoint):
        _run_bash(
            mountpoint,
            "\n".join(
                [
                    "set -euo pipefail",
                    "python3 - <<'PY'",
                    "import os",
                    "writer = os.open('buffered.txt', os.O_CREAT | os.O_RDWR, 0o644)",
                    "try:",
                    "    os.write(writer, b'buffered-write\\n')",
                    "    os.fsync(writer)",
                    "    reader = os.open('buffered.txt', os.O_RDONLY)",
                    "    try:",
                    "        assert os.read(reader, 15) == b'buffered-write\\n'",
                    "    finally:",
                    "        os.close(reader)",
                    "finally:",
                    "    os.close(writer)",
                    "PY",
                ]
            ),
        )


def test_fuse_write_batch_is_published_before_direct_branch_creation(
    tmp_path: Path,
) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    with _mounted_chronosfs(db_path, mountpoint):
        (mountpoint / "small.txt").write_bytes(b"small payload")
        (mountpoint / "large.bin").write_bytes(b"x" * (128 * 1024))

        store = _open_store(db_path)
        try:
            # create_branch crosses from the mount daemon to the direct
            # control-plane connection. It must flush the daemon's short write
            # batch before capturing the child's starting state.
            store.create_branch("child", "main")
            assert store.read_file("child", "/small.txt") == b"small payload"
            assert store.read_file("child", "/large.bin") == b"x" * (128 * 1024)
        finally:
            store.close()


def test_fuse_external_object_is_reclaimed_after_branch_deletion(
    tmp_path: Path,
) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    store = _open_store(db_path)
    try:
        store.create_branch("child", "main")
    finally:
        store.close()

    payload = b"x" * (128 * 1024)
    with _mounted_chronosfs(
        db_path,
        mountpoint,
        branch_id="child",
    ):
        (mountpoint / "temporary.bin").write_bytes(payload)

    object_dir = Path(str(db_path) + ".chronosfs-objects")
    objects = list(object_dir.glob("object-*"))
    assert len(objects) == 1

    store = _open_store(db_path)
    try:
        assert store.read_file("child", "/temporary.bin") == payload
        store.delete_branch("child")
        store.wait_for_gc()
        assert list(object_dir.glob("object-*")) == []
        assert (
            store.context.db.execute(
                "SELECT COUNT(*) AS count FROM _chronosfs_objects"
            ).fetchone()["count"]
            == 0
        )
    finally:
        store.close()


def test_fuse_external_object_is_retained_while_parent_references_it(
    tmp_path: Path,
) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()
    payload = b"shared" * (24 * 1024)

    with _mounted_chronosfs(db_path, mountpoint):
        (mountpoint / "shared.bin").write_bytes(payload)
        store = _open_store(db_path)
        try:
            store.create_branch("child", "main")
        finally:
            store.close()

    object_dir = Path(str(db_path) + ".chronosfs-objects")
    objects = list(object_dir.glob("object-*"))
    assert len(objects) == 1

    store = _open_store(db_path)
    try:
        store.delete_branch("child")
        store.wait_for_gc()
        assert list(object_dir.glob("object-*")) == objects
        assert store.read_file("main", "/shared.bin") == payload
    finally:
        store.close()


def test_fuse_buffered_handles_merge_nonoverlapping_same_block_writes(
    tmp_path: Path,
) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    with _mounted_chronosfs(db_path, mountpoint):
        _run_bash(
            mountpoint,
            "\n".join(
                [
                    "set -euo pipefail",
                    "python3 - <<'PY'",
                    "import os",
                    "with open('shared-block.bin', 'wb') as stream:",
                    "    stream.write(b'00000000')",
                    "left = os.open('shared-block.bin', os.O_RDWR)",
                    "right = os.open('shared-block.bin', os.O_RDWR)",
                    "try:",
                    "    os.pwrite(left, b'AA', 0)",
                    "    os.pwrite(right, b'BB', 2)",
                    "    os.close(right)",
                    "    right = -1",
                    "    os.close(left)",
                    "    left = -1",
                    "    assert open('shared-block.bin', 'rb').read() == b'AABB0000'",
                    "finally:",
                    "    if right >= 0:",
                    "        os.close(right)",
                    "    if left >= 0:",
                    "        os.close(left)",
                    "PY",
                ]
            ),
        )


def test_fuse_buffered_handle_does_not_restore_concurrently_truncated_size(
    tmp_path: Path,
) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    with _mounted_chronosfs(db_path, mountpoint):
        _run_bash(
            mountpoint,
            "\n".join(
                [
                    "set -euo pipefail",
                    "python3 - <<'PY'",
                    "import os",
                    "with open('truncate-race.bin', 'wb') as stream:",
                    "    stream.write(b'12345678')",
                    "stale = os.open('truncate-race.bin', os.O_RDWR)",
                    "truncater = os.open('truncate-race.bin', os.O_RDWR)",
                    "try:",
                    "    os.ftruncate(truncater, 0)",
                    "    os.close(truncater)",
                    "    truncater = -1",
                    "    os.pwrite(stale, b'Z', 0)",
                    "    os.close(stale)",
                    "    stale = -1",
                    "    assert open('truncate-race.bin', 'rb').read() == b'Z'",
                    "finally:",
                    "    if truncater >= 0:",
                    "        os.close(truncater)",
                    "    if stale >= 0:",
                    "        os.close(stale)",
                    "PY",
                ]
            ),
        )


def test_fuse_mount_compiles_redis_smoke(tmp_path: Path) -> None:
    _require_fuse_tools()
    _require_tools("git", "make", "gcc")
    source = _redis_source(tmp_path)
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    with _mounted_chronosfs(db_path, mountpoint):
        shutil.copytree(
            source,
            mountpoint / "redis",
            ignore=shutil.ignore_patterns(
                ".git",
                "*.o",
                "*.a",
                "*.so",
                "Makefile.dep",
                "redis-server",
                "redis-cli",
                "redis-benchmark",
            ),
        )
        _run_bash(
            mountpoint,
            "set -euo pipefail\n"
            "cd redis\n"
            f"make -j{min(2, os.cpu_count() or 1)} BUILD_TLS=no MALLOC=libc redis-server\n"
            "test -x src/redis-server\n",
            timeout=900,
        )


def test_fuse_control_paths_create_and_checkout_branch(tmp_path: Path) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    with _mounted_chronosfs(db_path, mountpoint):
        _run_bash(
            mountpoint,
            "\n".join(
                [
                    "set -euo pipefail",
                    "mkdir .chronos/branches/agent",
                    "printf 'agent\\n' > .chronos/current",
                    "test \"$(cat .chronos/current)\" = agent",
                    "printf 'branch local\\n' > branch.txt",
                    "cat branch.txt",
                ]
            ),
        )

    store = _open_store(db_path)
    try:
        assert "agent" in store.branches
        assert store.read_text("agent", "/branch.txt") == "branch local\n"
        assert not store.exists("main", "/branch.txt")
    finally:
        store.close()


def test_fuse_control_paths_preview_and_apply_file_conflict(tmp_path: Path) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    with _mounted_chronosfs(db_path, mountpoint):
        (mountpoint / "data.txt").write_bytes(b"aaaaaaaa\nbbbbbbbb\n")
        (mountpoint / ".chronos" / "branches" / "agent").mkdir()
        (mountpoint / ".chronos" / "current").write_text("agent\n")
        with (mountpoint / "data.txt").open("r+b") as handle:
            handle.seek(9)
            handle.write(b"AGNT")
        (mountpoint / ".chronos" / "current").write_text("main\n")
        with (mountpoint / "data.txt").open("r+b") as handle:
            handle.seek(9)
            handle.write(b"MAIN")

        raw_preview = (mountpoint / ".chronos" / "merge-preview" / "agent..main.json").read_text()
        preview = json.loads(raw_preview)
        assert preview["source"] == "agent"
        assert preview["target"] == "main"
        assert len(preview["conflicts"]) == 1
        conflict = preview["conflicts"][0]
        assert conflict["table"] == "chronosfs_file_range"
        assert conflict["key"]["path"] == "/data.txt"
        assert "byte_range" in conflict["key"]
        assert "unified_diff" in conflict["after"]
        assert "AGNT" in conflict["after"]["unified_diff"]
        assert "block_index" not in raw_preview
        assert "inode_id" not in raw_preview

        resolution = {
            "policy": "manual_review",
            "conflicts": {conflict["conflict_id"]: "source"},
        }
        (mountpoint / ".chronos" / "merge-apply" / "agent..main").write_text(
            json.dumps(resolution)
        )

        assert (mountpoint / "data.txt").read_bytes() == b"aaaaaaaa\nAGNTbbbb\n"


def test_fuse_control_merge_from_main_mountpoint_across_mounts(tmp_path: Path) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    main_mount = tmp_path / "mnt-main"
    worker_mount = tmp_path / "mnt-worker"
    main_mount.mkdir()
    worker_mount.mkdir()

    with _mounted_chronosfs(db_path, main_mount):
        with _mounted_chronosfs(db_path, worker_mount):
            (main_mount / "plan.txt").write_text("base line\n")
            (main_mount / ".chronos" / "branches" / "agent").mkdir()

            (worker_mount / ".chronos" / "current").write_text("agent\n")
            (worker_mount / "plan.txt").write_text("agent line\n")

            (main_mount / ".chronos" / "current").write_text("main\n")
            (main_mount / "plan.txt").write_text("main line\n")

            raw_preview = (
                main_mount / ".chronos" / "merge-preview" / "agent..main.json"
            ).read_text()
            preview = json.loads(raw_preview)
            assert len(preview["conflicts"]) == 1
            conflict_id = preview["conflicts"][0]["conflict_id"]
            assert preview["conflicts"][0]["key"]["path"] == "/plan.txt"

            (main_mount / ".chronos" / "merge-apply" / "agent..main.json").write_text(
                json.dumps(
                    {
                        "policy": "manual_review",
                        "conflicts": {conflict_id: "source"},
                    }
                )
            )

            assert (main_mount / "plan.txt").read_text() == "agent line\n"


def test_multiple_local_mountpoints_share_daemon_cache(tmp_path: Path) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mount_a = tmp_path / "mnt-a"
    mount_b = tmp_path / "mnt-b"
    mount_a.mkdir()
    mount_b.mkdir()

    with _mounted_chronosfs(db_path, mount_a):
        with _mounted_chronosfs(db_path, mount_b):
            _run_bash(mount_b, "set -euo pipefail\ntest ! -e shared.txt\n")
            _run_bash(mount_a, "set -euo pipefail\nprintf 'shared\\n' > shared.txt\n")
            result = _run_bash(mount_b, "set -euo pipefail\ncat shared.txt\n")
            assert result.stdout == "shared\n"


def test_shared_daemon_serializes_writes_across_branch_mounts(tmp_path: Path) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mount_a = tmp_path / "mnt-a"
    mount_b = tmp_path / "mnt-b"
    mount_a.mkdir()
    mount_b.mkdir()

    store = _open_store(db_path)
    try:
        store.create_branch("attempt-a")
        store.create_branch("attempt-b")
    finally:
        store.close()

    with _mounted_chronosfs(db_path, mount_a, branch_id="attempt-a"):
        with _mounted_chronosfs(db_path, mount_b, branch_id="attempt-b"):
            writer_a = subprocess.Popen(
                ["bash", "-lc", "for i in $(seq 1 100); do printf 'a-%s\\n' \"$i\" > a-$i.txt; done"],
                cwd=mount_a,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            writer_b = subprocess.Popen(
                ["bash", "-lc", "for i in $(seq 1 100); do printf 'b-%s\\n' \"$i\" > b-$i.txt; done"],
                cwd=mount_b,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            control = _open_store(db_path)
            try:
                control.context.update_branch_metadata(
                    "attempt-a", {"published_by": "coordinator"}
                )
            finally:
                control.close()

            for writer in (writer_a, writer_b):
                _stdout, stderr = writer.communicate(timeout=30)
                assert writer.returncode == 0, stderr

            assert (mount_a / "a-100.txt").read_text() == "a-100\n"
            assert not (mount_a / "b-100.txt").exists()
            assert (mount_b / "b-100.txt").read_text() == "b-100\n"
            assert not (mount_b / "a-100.txt").exists()


def test_shared_daemon_identity_is_store_scoped() -> None:
    import inspect

    from chronos_core.workspace.chronosfs.fuse import _daemon_key

    assert list(inspect.signature(_daemon_key).parameters) == [
        "database_url",
        "block_size",
    ]


def test_shared_daemon_shutdown_waits_for_mounts() -> None:
    from chronos_core.workspace.chronosfs.daemon import _DaemonState

    state = _DaemonState()
    state.request_shutdown()
    state.active_mounts = 1
    assert not state.should_exit(0.0, 0.0)
    state.active_mounts = 0
    state.retain()
    assert not state.should_exit(0.0, 0.0)
    state.request_shutdown(force=True)
    assert state.should_exit(0.0, 0.0)


def test_fuse_large_scale_blocks_and_unix_tools(tmp_path: Path) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    with _mounted_chronosfs(db_path, mountpoint):
        result = _run_bash(
            mountpoint,
            "\n".join(
                [
                    "set -euo pipefail",
                    "mkdir -p tree",
                    "for i in $(seq 1 200); do",
                    "  mkdir -p tree/dir_$((i % 10))",
                    "  printf 'file-%03d:%090d\\n' \"$i\" \"$i\" > tree/dir_$((i % 10))/file_$i.txt",
                    "done",
                    "dd if=/dev/urandom of=big.bin bs=1M count=2 status=none",
                    "sha256sum big.bin | awk '{print $1}'",
                    "find tree -type f | wc -l",
                ]
            ),
            timeout=60,
        )
        lines = result.stdout.splitlines()
        digest = lines[0]
        assert lines[1] == "200"
        result = _run_bash(
            mountpoint,
            "set -euo pipefail\nsha256sum big.bin | awk '{print $1}'\nwc -c < big.bin",
            timeout=60,
        )
        assert result.stdout.splitlines() == [digest, str(2 * 1024 * 1024)]

    store = _open_store(db_path)
    try:
        assert len(store.read_file("main", "/big.bin")) == 2 * 1024 * 1024
        assert len([path for path in store.manifest("main") if path.startswith("tree/")]) >= 210
    finally:
        store.close()


def _require_fuse_tools() -> None:
    missing = [
        name
        for name in ("bash", "mountpoint", "fusermount3")
        if shutil.which(name) is None
    ]
    if missing:
        pytest.skip(f"missing FUSE test tools: {', '.join(missing)}")
    if not Path("/dev/fuse").exists():
        pytest.skip("/dev/fuse is not available")


def _require_tools(*names: str) -> None:
    missing = [name for name in names if shutil.which(name) is None]
    if missing:
        pytest.skip(f"missing test tools: {', '.join(missing)}")


def _redis_source(tmp_path: Path) -> Path:
    configured = os.environ.get("CHRONOS_TEST_REDIS_SOURCE")
    if configured:
        source = Path(configured).resolve()
        if not source.is_dir():
            pytest.fail(f"CHRONOS_TEST_REDIS_SOURCE is not a directory: {source}")
        return source

    cache = Path(
        os.environ.get(
            "CHRONOS_TEST_REDIS_CACHE",
            str(Path.cwd() / ".pytest_cache" / "chronos-redis-7.2.5"),
        )
    )
    if (cache / "src" / "server.c").exists():
        return cache
    cache.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [
            "git",
            "clone",
            "--depth",
            "1",
            "--branch",
            "7.2.5",
            "https://github.com/redis/redis.git",
            str(cache),
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=300,
    )
    if result.returncode != 0:
        pytest.skip(f"could not fetch Redis source: {result.stderr[-500:]}")
    return cache


@contextmanager
def _mounted_chronosfs(
    db_path: Path,
    mountpoint: Path,
    *,
    branch_id: str = "main",
) -> Iterator[None]:
    database_url = "sqlite:///" + str(db_path)
    with _mounted_chronosfs_database(
        database_url,
        mountpoint,
        script_dir=db_path.parent,
        branch_id=branch_id,
    ):
        yield


@contextmanager
def _mounted_chronosfs_database(
    database_url: str,
    mountpoint: Path,
    *,
    script_dir: Path,
    branch_id: str = "main",
) -> Iterator[None]:
    script = script_dir / "mount_chronosfs.py"
    script.write_text(
        textwrap.dedent(
            f"""
            from chronos_core.workspace.chronosfs import ChronosFSStore, mount_chronosfs

            store = ChronosFSStore.connect({database_url!r}, backend="interval")
            store.ensure()
            try:
                mount_chronosfs(
                    store,
                    {str(mountpoint)!r},
                    branch_id={branch_id!r},
                    shutdown_daemon_on_unmount=True,
                )
            finally:
                store.close()
            """
        )
    )
    env = os.environ.copy()
    src = Path(__file__).resolve().parents[1] / "packages" / "chronos-core" / "src"
    tests = Path(__file__).resolve().parents[0]
    env["PYTHONPATH"] = os.pathsep.join(
        [str(src), str(tests), env.get("PYTHONPATH", "")]
    )
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                stdout, stderr = proc.communicate()
                raise AssertionError(
                    f"ChronosFS mount exited early with {proc.returncode}\n"
                    f"stdout:\n{stdout}\nstderr:\n{stderr}"
                )
            probe = subprocess.run(
                ["mountpoint", "-q", str(mountpoint)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if probe.returncode == 0:
                break
            time.sleep(0.05)
        else:
            proc.terminate()
            stdout, stderr = proc.communicate(timeout=5)
            raise AssertionError(
                "timed out waiting for ChronosFS mount\n"
                f"stdout:\n{stdout}\nstderr:\n{stderr}"
            )
        yield
    finally:
        for _ in range(20):
            subprocess.run(
                ["fusermount3", "-u", str(mountpoint)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            probe = subprocess.run(
                ["mountpoint", "-q", str(mountpoint)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if probe.returncode != 0:
                break
            time.sleep(0.25)
        else:
            subprocess.run(
                ["fusermount3", "-uz", str(mountpoint)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            subprocess.run(
                ["fusermount3", "-uz", str(mountpoint)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        stdout = proc.stdout.read() if proc.stdout is not None else ""
        stderr = proc.stderr.read() if proc.stderr is not None else ""
        assert proc.returncode == 0, f"mount process failed\nstdout:\n{stdout}\nstderr:\n{stderr}"


def _run_bash(mountpoint: Path, script: str, *, timeout: float = 20) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["bash", "-lc", script],
        cwd=mountpoint,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    assert result.returncode == 0, result.stderr
    return result


def _open_store(db_path: Path) -> ChronosFSStore:
    store = ChronosFSStore.connect(f"sqlite:///{db_path}", backend="interval")
    store.ensure()
    return store


def _reset_and_get_postgres_dsn() -> str:
    pytest.importorskip("psycopg")
    from tests.test_branching import _postgres_dsn, _reset_postgres_schema

    _reset_postgres_schema()
    return _postgres_dsn()
