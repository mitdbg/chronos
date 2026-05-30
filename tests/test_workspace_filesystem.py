from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from chronos_core.branching import ChronosBranchContext
from chronos_core.workspace import ChronosFilesystemStore, ChronosWorkspaceContext


def _require_fuse_overlayfs() -> None:
    if shutil.which("fuse-overlayfs") is None:
        pytest.skip("fuse-overlayfs is not installed")
    if shutil.which("fusermount3") is None and shutil.which("fusermount") is None:
        pytest.skip("fusermount3 or fusermount is not installed")


@pytest.fixture
def base_tree(tmp_path: Path) -> Path:
    root = tmp_path / "base"
    root.mkdir()
    (root / "a.txt").write_text("A-base\n")
    (root / "b.txt").write_text("B-base\n")
    (root / "c.txt").write_text("C-base\n")
    (root / "dir").mkdir()
    (root / "dir" / "nested.txt").write_text("nested-base\n")
    (root / "target.txt").write_text("target-base\n")
    (root / "link.txt").symlink_to("target.txt")
    return root


@pytest.fixture
def fs_store(tmp_path: Path, base_tree: Path):
    _require_fuse_overlayfs()
    store = ChronosFilesystemStore(base_tree, state_dir=tmp_path / "state")
    try:
        yield store
    finally:
        store.cleanup()


def test_child_branch_uses_stable_parent_checkpoint(
    fs_store: ChronosFilesystemStore,
    base_tree: Path,
) -> None:
    parent = fs_store.checkout("main")
    (parent.path / "b.txt").write_text("B-parent-v1\n")
    (parent.path / "c.txt").unlink()
    (parent.path / "d.txt").write_text("D-parent-v1\n")

    fs_store.create_branch("child", from_branch="main")
    child = fs_store.checkout("child")

    assert (child.path / "a.txt").read_text() == "A-base\n"
    assert (child.path / "b.txt").read_text() == "B-parent-v1\n"
    assert not (child.path / "c.txt").exists()
    assert (child.path / "d.txt").read_text() == "D-parent-v1\n"

    (parent.path / "b.txt").write_text("B-parent-v2\n")
    (parent.path / "e.txt").write_text("E-parent-v2\n")
    (parent.path / "a.txt").unlink()

    assert (child.path / "a.txt").read_text() == "A-base\n"
    assert (child.path / "b.txt").read_text() == "B-parent-v1\n"
    assert not (child.path / "e.txt").exists()

    assert not (parent.path / "a.txt").exists()
    assert (parent.path / "b.txt").read_text() == "B-parent-v2\n"
    assert (parent.path / "e.txt").read_text() == "E-parent-v2\n"

    assert (base_tree / "a.txt").read_text() == "A-base\n"
    assert (base_tree / "b.txt").read_text() == "B-base\n"
    assert (base_tree / "c.txt").read_text() == "C-base\n"


def test_branch_on_branch_uses_layer_stack(
    fs_store: ChronosFilesystemStore,
) -> None:
    parent = fs_store.checkout("main")
    (parent.path / "b.txt").write_text("B-parent\n")
    (parent.path / "d.txt").write_text("D-parent\n")
    (parent.path / "c.txt").unlink()

    fs_store.create_branch("child", from_branch="main")
    child = fs_store.checkout("child")
    (child.path / "a.txt").write_text("A-child\n")
    (child.path / "d.txt").unlink()
    (child.path / "f.txt").write_text("F-child\n")

    fs_store.create_branch("grandchild", from_branch="child")
    grandchild = fs_store.checkout("grandchild")

    assert (grandchild.path / "a.txt").read_text() == "A-child\n"
    assert (grandchild.path / "b.txt").read_text() == "B-parent\n"
    assert not (grandchild.path / "c.txt").exists()
    assert not (grandchild.path / "d.txt").exists()
    assert (grandchild.path / "f.txt").read_text() == "F-child\n"

    assert len(fs_store.layers_for_branch("grandchild")) == 2


def test_wide_sibling_branches_are_isolated(
    fs_store: ChronosFilesystemStore,
) -> None:
    siblings = [f"sibling_{idx}" for idx in range(12)]
    for idx, branch_id in enumerate(siblings):
        fs_store.create_branch(branch_id, from_branch="main")
        branch = fs_store.checkout(branch_id)
        (branch.path / "shared.txt").write_text(f"branch {idx}\n")
        (branch.path / f"only_{idx}.txt").write_text(f"only {idx}\n")
        if idx % 2 == 0:
            (branch.path / "a.txt").write_text(f"A-{idx}\n")
        else:
            (branch.path / "b.txt").write_text(f"B-{idx}\n")

    main = fs_store.checkout("main")
    assert not (main.path / "shared.txt").exists()
    assert (main.path / "a.txt").read_text() == "A-base\n"
    assert (main.path / "b.txt").read_text() == "B-base\n"

    for idx, branch_id in enumerate(siblings):
        branch = fs_store.checkout(branch_id)
        assert (branch.path / "shared.txt").read_text() == f"branch {idx}\n"
        assert (branch.path / f"only_{idx}.txt").read_text() == f"only {idx}\n"
        for other in range(len(siblings)):
            if other != idx:
                assert not (branch.path / f"only_{other}.txt").exists()


def test_deep_branch_chain_preserves_each_fork_point(
    fs_store: ChronosFilesystemStore,
) -> None:
    parent = "main"
    for depth in range(8):
        child = f"depth_{depth}"
        fs_store.create_branch(child, from_branch=parent)
        session = fs_store.checkout(child)
        (session.path / "lineage.txt").write_text(f"depth {depth}\n")
        (session.path / f"marker_{depth}.txt").write_text(f"marker {depth}\n")
        parent = child

    leaf = fs_store.checkout("depth_7")
    assert (leaf.path / "lineage.txt").read_text() == "depth 7\n"
    for depth in range(8):
        assert (leaf.path / f"marker_{depth}.txt").read_text() == f"marker {depth}\n"

    middle = fs_store.checkout("depth_3")
    assert (middle.path / "lineage.txt").read_text() == "depth 3\n"
    for depth in range(4):
        assert (middle.path / f"marker_{depth}.txt").exists()
    for depth in range(4, 8):
        assert not (middle.path / f"marker_{depth}.txt").exists()

    # depth_7 has seven sealed ancestor layers; its own writes are still in
    # its live upperdir until it is checkpointed or used as a parent.
    assert len(fs_store.layers_for_branch("depth_7")) == 7


def test_checkpoint_restore_is_stable_after_later_branch_writes(
    fs_store: ChronosFilesystemStore,
) -> None:
    fs_store.create_branch("work", from_branch="main")
    work = fs_store.checkout("work")
    (work.path / "a.txt").write_text("A-checkpoint\n")
    (work.path / "checkpoint_only.txt").write_text("checkpoint\n")
    fs_store.create_checkpoint("snap", branch="work")

    (work.path / "a.txt").write_text("A-later\n")
    (work.path / "later_only.txt").write_text("later\n")
    (work.path / "checkpoint_only.txt").unlink()

    fs_store.create_branch_from_checkpoint("restored", "snap")
    restored = fs_store.checkout("restored")

    assert (restored.path / "a.txt").read_text() == "A-checkpoint\n"
    assert (restored.path / "checkpoint_only.txt").read_text() == "checkpoint\n"
    assert not (restored.path / "later_only.txt").exists()

    assert (work.path / "a.txt").read_text() == "A-later\n"
    assert not (work.path / "checkpoint_only.txt").exists()
    assert (work.path / "later_only.txt").read_text() == "later\n"


def test_symlink_and_nested_directory_changes_are_branch_local_and_mergeable(
    fs_store: ChronosFilesystemStore,
) -> None:
    fs_store.create_branch("links", from_branch="main")
    links = fs_store.checkout("links")
    (links.path / "target.txt").write_text("target-branch\n")
    (links.path / "link.txt").unlink()
    (links.path / "link.txt").symlink_to("dir/nested.txt")
    (links.path / "dir" / "nested.txt").write_text("nested-branch\n")
    (links.path / "dir" / "new_nested.txt").write_text("new nested\n")

    main = fs_store.checkout("main")
    assert (main.path / "target.txt").read_text() == "target-base\n"
    assert (main.path / "link.txt").read_text() == "target-base\n"
    assert (main.path / "dir" / "nested.txt").read_text() == "nested-base\n"
    assert not (main.path / "dir" / "new_nested.txt").exists()

    diff = fs_store.diff("main", "links")
    changed_paths = {change.path for change in diff.changes}
    assert {"target.txt", "link.txt", "dir/nested.txt", "dir/new_nested.txt"} <= changed_paths

    fs_store.merge_apply("links", "main")
    assert (main.path / "target.txt").read_text() == "target-branch\n"
    assert (main.path / "link.txt").is_symlink()
    assert (main.path / "link.txt").read_text() == "nested-branch\n"
    assert (main.path / "dir" / "nested.txt").read_text() == "nested-branch\n"
    assert (main.path / "dir" / "new_nested.txt").read_text() == "new nested\n"


def test_out_of_band_writes_under_mount_are_branch_local(
    fs_store: ChronosFilesystemStore,
    base_tree: Path,
) -> None:
    fs_store.create_branch("agent", from_branch="main")
    agent = fs_store.checkout("agent")

    (agent.path / "agent.txt").write_text("created through POSIX path\n")
    result = agent.run(
        [
            "python3",
            "-c",
            "from pathlib import Path; Path('subprocess.txt').write_text('from subprocess\\n')",
        ]
    )

    assert result.returncode == 0
    assert (agent.path / "agent.txt").read_text() == "created through POSIX path\n"
    assert (agent.path / "subprocess.txt").read_text() == "from subprocess\n"
    assert not (base_tree / "agent.txt").exists()
    assert not (base_tree / "subprocess.txt").exists()

    fs_store.create_checkpoint("agent-snap", branch="agent")
    fs_store.create_branch("review", from_branch="agent")
    review = fs_store.checkout("review")

    assert (review.path / "agent.txt").read_text() == "created through POSIX path\n"
    assert (review.path / "subprocess.txt").read_text() == "from subprocess\n"


def test_filesystem_merge_apply_writes_source_changes_to_target(
    fs_store: ChronosFilesystemStore,
) -> None:
    fs_store.create_branch("source", from_branch="main")
    source = fs_store.checkout("source")
    (source.path / "a.txt").write_text("A-source\n")
    (source.path / "new.txt").write_text("new from source\n")
    (source.path / "c.txt").unlink()

    result = fs_store.merge_apply("source", "main")
    main = fs_store.checkout("main")

    assert result.applied == 3
    assert (main.path / "a.txt").read_text() == "A-source\n"
    assert (main.path / "new.txt").read_text() == "new from source\n"
    assert not (main.path / "c.txt").exists()


def test_workspace_context_composes_relational_and_filesystem(
    tmp_path: Path,
    base_tree: Path,
) -> None:
    _require_fuse_overlayfs()
    sql = ChronosBranchContext.connect("sqlite:///:memory:")
    fs = ChronosFilesystemStore(base_tree, state_dir=tmp_path / "workspace-state")
    workspace = ChronosWorkspaceContext(relational=sql, filesystem=fs)
    try:
        sql.db.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        sql.db.execute("INSERT INTO docs VALUES (?, ?)", ("d1", "main"))
        sql.db.commit()
        sql.register_table("docs", ["id"])

        workspace.create_branch("agent", from_branch="main")
        agent = workspace.checkout("agent")
        assert agent.sql is not None
        assert agent.fs is not None

        agent.sql.execute(
            "UPDATE docs SET body = :body WHERE id = :id",
            {"id": "d1", "body": "agent"},
        )
        (agent.fs.path / "report.md").write_text("# Agent report\n")

        assert sql.checkout("main").query("SELECT body FROM docs WHERE id = :id", {"id": "d1"}) == [
            {"body": "main"}
        ]
        assert agent.sql.query("SELECT body FROM docs WHERE id = :id", {"id": "d1"}) == [
            {"body": "agent"}
        ]
        assert not (base_tree / "report.md").exists()
        assert (agent.fs.path / "report.md").read_text() == "# Agent report\n"

        diff = workspace.diff("main", "agent")
        assert diff["relational"].changes
        fs_changes = diff["filesystem"].changes
        assert any(change.path == "report.md" and change.change == "added" for change in fs_changes)
    finally:
        workspace.close()


def test_workspace_wide_branches_isolate_sql_and_filesystem_state(
    tmp_path: Path,
    base_tree: Path,
) -> None:
    _require_fuse_overlayfs()
    sql = ChronosBranchContext.connect("sqlite:///:memory:")
    fs = ChronosFilesystemStore(base_tree, state_dir=tmp_path / "multi-state")
    workspace = ChronosWorkspaceContext(relational=sql, filesystem=fs)
    try:
        sql.db.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        sql.db.execute("INSERT INTO docs VALUES (?, ?)", ("d1", "main"))
        sql.db.commit()
        sql.register_table("docs", ["id"])

        for idx in range(6):
            branch_id = f"agent_{idx}"
            workspace.create_branch(branch_id, from_branch="main")
            branch = workspace.checkout(branch_id)
            assert branch.sql is not None
            assert branch.fs is not None
            branch.sql.execute(
                "UPDATE docs SET body = :body WHERE id = :id",
                {"id": "d1", "body": branch_id},
            )
            (branch.fs.path / "branch.txt").write_text(f"{branch_id}\n")
            (branch.fs.path / f"{branch_id}.txt").write_text("private\n")

        main = workspace.checkout("main")
        assert main.sql is not None
        assert main.fs is not None
        assert main.sql.query("SELECT body FROM docs WHERE id = :id", {"id": "d1"}) == [
            {"body": "main"}
        ]
        assert not (main.fs.path / "branch.txt").exists()

        for idx in range(6):
            branch_id = f"agent_{idx}"
            branch = workspace.checkout(branch_id)
            assert branch.sql is not None
            assert branch.fs is not None
            assert branch.sql.query("SELECT body FROM docs WHERE id = :id", {"id": "d1"}) == [
                {"body": branch_id}
            ]
            assert (branch.fs.path / "branch.txt").read_text() == f"{branch_id}\n"
            assert (branch.fs.path / f"{branch_id}.txt").read_text() == "private\n"
            for other in range(6):
                if other != idx:
                    assert not (branch.fs.path / f"agent_{other}.txt").exists()
    finally:
        workspace.close()


def test_workspace_checkpoint_restore_covers_sql_and_filesystem(
    tmp_path: Path,
    base_tree: Path,
) -> None:
    _require_fuse_overlayfs()
    sql = ChronosBranchContext.connect("sqlite:///:memory:")
    fs = ChronosFilesystemStore(base_tree, state_dir=tmp_path / "checkpoint-state")
    workspace = ChronosWorkspaceContext(relational=sql, filesystem=fs)
    try:
        sql.db.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        sql.db.execute("INSERT INTO docs VALUES (?, ?)", ("d1", "main"))
        sql.db.commit()
        sql.register_table("docs", ["id"])

        workspace.create_branch("work", from_branch="main")
        work = workspace.checkout("work")
        assert work.sql is not None
        assert work.fs is not None
        work.sql.execute(
            "UPDATE docs SET body = :body WHERE id = :id",
            {"id": "d1", "body": "checkpoint"},
        )
        (work.fs.path / "checkpoint.txt").write_text("checkpoint\n")
        workspace.create_checkpoint("workspace-snap", branch="work")

        work.sql.execute(
            "UPDATE docs SET body = :body WHERE id = :id",
            {"id": "d1", "body": "later"},
        )
        (work.fs.path / "checkpoint.txt").unlink()
        (work.fs.path / "later.txt").write_text("later\n")

        workspace.create_branch_from_checkpoint("restored", "workspace-snap")
        restored = workspace.checkout("restored")
        assert restored.sql is not None
        assert restored.fs is not None
        assert restored.sql.query("SELECT body FROM docs WHERE id = :id", {"id": "d1"}) == [
            {"body": "checkpoint"}
        ]
        assert (restored.fs.path / "checkpoint.txt").read_text() == "checkpoint\n"
        assert not (restored.fs.path / "later.txt").exists()

        work = workspace.checkout("work")
        assert work.sql is not None
        assert work.fs is not None
        assert work.sql.query("SELECT body FROM docs WHERE id = :id", {"id": "d1"}) == [
            {"body": "later"}
        ]
        assert not (work.fs.path / "checkpoint.txt").exists()
        assert (work.fs.path / "later.txt").read_text() == "later\n"
    finally:
        workspace.close()
