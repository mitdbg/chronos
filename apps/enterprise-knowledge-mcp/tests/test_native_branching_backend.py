from __future__ import annotations

import os
import subprocess
import uuid
from pathlib import Path

import psycopg
import pytest
from chronos_enterprise_knowledge.backends.btrfs_workspace import (
    BtrfsWorkspaceStore,
    _btrfs_dump_candidate_paths,
    _btrfs_dump_candidates,
    _btrfs_dump_candidates_with_presence,
    _different_candidate_file_paths,
)
from chronos_enterprise_knowledge.backends.native_branching import (
    DoltgresQdrantBtrfsKnowledgeBackend,
    _qdrant_branch_filter,
)
from chronos_enterprise_knowledge.models import (
    DocumentChunk,
    IndexedDocument,
    KnowledgeDocument,
)
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo


def _indexed(
    document_id: str,
    content: str,
    embedding: tuple[float, float, float],
) -> IndexedDocument:
    document = KnowledgeDocument(
        id=document_id,
        path=f"/knowledge/{document_id}.md",
        title=document_id,
        source="test",
        content=content,
    )
    chunk = DocumentChunk(
        id=f"{document_id}:0",
        document_id=document_id,
        ordinal=0,
        text=content,
        embedding=embedding,
    )
    return IndexedDocument(document, (chunk,))


def test_qdrant_filter_caps_each_ancestor_and_excludes_overwrites() -> None:
    value = _qdrant_branch_filter(
        [("team/runtime", 7), ("department/engineering", 11), ("main", 23)]
    ).model_dump(exclude_none=True)

    assert [
        condition["must"][0]["match"]["value"]
        for condition in value["should"]
    ] == ["team/runtime", "department/engineering", "main"]
    assert [
        condition["must"][1]["range"]["lte"]
        for condition in value["should"]
    ] == [7.0, 11.0, 23.0]
    assert [
        condition["nested"]["filter"]["must"][0]["match"]["value"]
        for condition in value["must_not"]
    ] == ["team/runtime", "department/engineering", "main"]


def test_qdrant_filter_can_omit_empty_overwrite_exclusion() -> None:
    value = _qdrant_branch_filter(
        [("main", 23)],
        include_overwrites=False,
    ).model_dump(exclude_none=True)

    assert value["should"][0]["must"][0]["match"]["value"] == "main"
    assert "must_not" not in value


def test_btrfs_branch_delete_does_not_force_a_filesystem_commit(
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []

    def run(command: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    store = BtrfsWorkspaceStore(
        tmp_path / "workspaces",
        command_runner=run,
        filesystem_type="btrfs",
    )
    branch = store.branch_path("task")
    branch.mkdir(parents=True)
    store._is_subvolume = lambda path: path == branch  # type: ignore[method-assign]

    store._delete_if_subvolume(branch)

    assert ["btrfs", "subvolume", "delete", str(branch)] in calls


def test_btrfs_filesystem_used_bytes_counts_shared_storage_once(
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []

    def run(command: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        output = (
            "Overall:\n    Used:\t123456789\n"
            if "usage" in command
            else ""
        )
        return subprocess.CompletedProcess(command, 0, output, "")

    store = BtrfsWorkspaceStore(
        tmp_path / "workspaces",
        command_runner=run,
        filesystem_type="btrfs",
    )

    assert store.filesystem_used_bytes() == 123456789
    assert [
        "btrfs",
        "filesystem",
        "sync",
        str(store.root),
    ] in calls
    assert [
        "btrfs",
        "filesystem",
        "usage",
        "--raw",
        str(store.root),
    ] in calls


def test_btrfs_usage_reports_allocator_and_subvolume_scope(
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []

    def run(command: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[1:3] == ["filesystem", "usage"]:
            output = (
                "Overall:\n    Used:\t1000\n"
                "Data,single: Size:4096, Used:700 (17.1%)\n"
                "Metadata,DUP: Size:4096, Used:200 (4.8%)\n"
                "System,DUP: Size:4096, Used:100 (2.4%)\n"
            )
        elif command[1:3] == ["filesystem", "du"]:
            output = (
                "Total Exclusive Set shared Filename\n"
                "100 40 60 /workspaces/branches/task\n"
                "80 20 60 /workspaces/fork-bases/task\n"
            )
        else:
            output = ""
        return subprocess.CompletedProcess(command, 0, output, "")

    store = BtrfsWorkspaceStore(
        tmp_path / "workspaces",
        command_runner=run,
        filesystem_type="btrfs",
    )
    branch = store.branch_path("task")
    base = store.base_path("task")
    branch.mkdir(parents=True)
    base.mkdir(parents=True)
    store._is_subvolume = lambda path: path in {branch, base}  # type: ignore[method-assign]

    assert store.filesystem_usage() == {
        "filesystem_used_bytes": 1000,
        "data_used_bytes": 700,
        "metadata_used_bytes": 200,
        "system_used_bytes": 100,
    }
    assert store.subvolume_usage() == {
        "subvolume_count": 2,
        "subvolume_total_bytes": 180,
        "subvolume_exclusive_bytes": 60,
        "subvolume_shared_bytes": 120,
    }
    assert [
        "btrfs",
        "filesystem",
        "du",
        "--raw",
        "-s",
        str(branch),
        str(base),
    ] in calls


def test_btrfs_changed_paths_use_final_tree_not_mutation_history(
    tmp_path: Path,
) -> None:
    def run(command: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, "", "")

    store = BtrfsWorkspaceStore(
        tmp_path / "workspaces",
        command_runner=run,
        filesystem_type="btrfs",
    )
    source = store.branch_path("task")
    base = store.base_path("task")
    source.mkdir(parents=True)
    base.mkdir(parents=True)
    store._is_subvolume = lambda path: path == source  # type: ignore[method-assign]

    (base / "unchanged.txt").write_text("same\n")
    (source / "unchanged.txt").write_text("same\n")
    (base / "modified.txt").write_text("before\n")
    (source / "modified.txt").write_text("after\n")
    (base / "deleted.txt").write_text("removed\n")
    (source / "created.txt").write_text("new\n")
    # A file created and deleted during the branch lifetime is absent from
    # both final trees and therefore has no entry in either directory.

    assert store.changed_file_paths("task", "task") == {
        "/created.txt",
        "/deleted.txt",
        "/modified.txt",
    }


def test_btrfs_detects_three_way_path_conflicts(tmp_path: Path) -> None:
    def run(command: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, "", "")

    store = BtrfsWorkspaceStore(
        tmp_path / "workspaces",
        command_runner=run,
        filesystem_type="btrfs",
    )
    source = store.branch_path("task")
    base = store.base_path("task")
    target = store.branch_path("main")
    source.mkdir(parents=True)
    base.mkdir(parents=True)
    target.mkdir(parents=True, exist_ok=True)
    store._is_subvolume = lambda path: path in {  # type: ignore[method-assign]
        source,
        target,
    }

    for root in (source, base, target):
        (root / "same.txt").write_text("same\n")
    (base / "conflict.txt").write_text("base\n")
    (source / "conflict.txt").write_text("source\n")
    (target / "conflict.txt").write_text("target\n")
    (base / "source-only.txt").write_text("base\n")
    (source / "source-only.txt").write_text("source\n")
    (target / "source-only.txt").write_text("base\n")

    assert store.conflicting_paths(
        "task",
        "task",
        "main",
        {"/same.txt", "/conflict.txt", "/source-only.txt"},
    ) == {"/conflict.txt"}


def test_btrfs_dump_paths_include_rename_source_and_destination() -> None:
    output = """\
snapshot        ./left-token                    uuid=abc
link            ./left-token/renamed.txt        dest=dir/changed.txt
unlink          ./left-token/dir/changed.txt
mkfile          ./left-token/o261-10-0
rename          ./left-token/o261-10-0           dest=./left-token/new.txt
utimes          ./left-token/dir                 atime=0
"""

    assert _btrfs_dump_candidate_paths(output, "left-token") == {
        "/dir/changed.txt",
        "/new.txt",
        "/renamed.txt",
    }

    _, recursive = _btrfs_dump_candidates(output, "left-token")
    assert recursive == {"/new.txt"}


def test_btrfs_dump_presence_marks_final_one_sided_files_only() -> None:
    output = """\
mkfile          ./left-token/temporary.txt
unlink          ./left-token/temporary.txt
mkfile          ./left-token/generated.txt
unlink          ./left-token/old.txt
"""

    (
        candidates,
        _,
        one_sided,
        one_sided_files,
    ) = _btrfs_dump_candidates_with_presence(
        output,
        "left-token",
    )
    assert candidates == {
        "/generated.txt",
        "/old.txt",
    }
    assert one_sided == {"/generated.txt", "/old.txt"}
    assert one_sided_files == {"/generated.txt", "/old.txt"}


def test_btrfs_candidate_diff_ignores_metadata_only_directories(
    tmp_path: Path,
) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    for root in (left, right):
        (root / "knowledge" / "runbooks").mkdir(parents=True)
        (root / "knowledge" / "unchanged.txt").write_text("same\n")
    (left / "knowledge" / "runbooks" / "new.md").write_text("new\n")

    assert _different_candidate_file_paths(
        left,
        right,
        {"/knowledge", "/knowledge/runbooks", "/knowledge/runbooks/new.md"},
        recursive_candidates={"/knowledge/runbooks"},
    ) == {"/knowledge/runbooks/new.md"}


def test_btrfs_candidate_diff_does_not_read_one_sided_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    left.mkdir()
    right.mkdir()
    (left / "generated.bin").write_bytes(b"x" * (8 * 1024 * 1024))

    def unexpected_stat(*args: object, **kwargs: object) -> bool:
        raise AssertionError("one-sided candidates must not stat file contents")

    monkeypatch.setattr(Path, "is_file", unexpected_stat)
    assert _different_candidate_file_paths(
        left,
        right,
        {"/generated.bin"},
        one_sided_candidates={"/generated.bin"},
        one_sided_file_candidates={"/generated.bin"},
    ) == {"/generated.bin"}


def test_btrfs_candidate_diff_compares_two_sided_contents(
    tmp_path: Path,
) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    left.mkdir()
    right.mkdir()
    (left / "same.txt").write_text("same\n")
    (right / "same.txt").write_text("same\n")
    (left / "changed.txt").write_text("left\n")
    (right / "changed.txt").write_text("right\n")

    assert _different_candidate_file_paths(
        left,
        right,
        {"/same.txt", "/changed.txt"},
    ) == {"/changed.txt"}


@pytest.mark.integration
def test_native_placeholder_vectors_round_trip_across_reopen(
    tmp_path: Path,
) -> None:
    admin_dsn = os.environ.get("ENTERPRISE_NATIVE_DOLTGRES_DSN")
    qdrant_url = os.environ.get("ENTERPRISE_NATIVE_QDRANT_URL")
    btrfs_root = os.environ.get("ENTERPRISE_NATIVE_BTRFS_ROOT")
    if not admin_dsn or not qdrant_url or not btrfs_root:
        pytest.skip(
            "set ENTERPRISE_NATIVE_DOLTGRES_DSN, "
            "ENTERPRISE_NATIVE_QDRANT_URL, and "
            "ENTERPRISE_NATIVE_BTRFS_ROOT"
        )

    database = f"enterprise_native_test_{uuid.uuid4().hex[:16]}"
    with psycopg.connect(admin_dsn, autocommit=True) as connection:
        connection.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database))
        )
    parameters = conninfo_to_dict(admin_dsn)
    parameters["dbname"] = database
    backend_dsn = make_conninfo(**parameters)
    backend: DoltgresQdrantBtrfsKnowledgeBackend | None = None
    try:
        backend = DoltgresQdrantBtrfsKnowledgeBackend(
            tmp_path,
            vector_dimensions=3,
            qdrant_url=qdrant_url,
            doltgres_dsn=backend_dsn,
            btrfs_root=btrfs_root,
        )
        backend.set_placeholder_vector_mode(True)
        backend.load_documents(
            "main",
            [_indexed("zero", "placeholder knowledge", (0.0, 0.0, 0.0))],
            operation_id="seed",
        )
        chunk_columns = {
            str(row["column_name"])
            for row in backend._db.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'knowledge_chunks'
                """
            ).fetchall()
        }
        assert {"text", "embedding"}.isdisjoint(chunk_columns)
        backend.set_snapshot_ingestion_cursor(
            "main",
            "snapshot-zero",
            "documents/zero.md",
        )
        backend.close()
        backend = DoltgresQdrantBtrfsKnowledgeBackend(
            tmp_path,
            vector_dimensions=3,
            qdrant_url=qdrant_url,
            doltgres_dsn=backend_dsn,
            btrfs_root=btrfs_root,
        )

        indexed = backend.get_document("main", "zero")
        assert indexed is not None
        assert indexed.chunks[0].embedding == (0.0, 0.0, 0.0)
        assert (
            backend.snapshot_ingestion_cursor("main", "snapshot-zero")
            == "documents/zero.md"
        )
        assert [
            hit.text
            for hit in backend.search(
                "main",
                "",
                (0.0, 0.0, 0.0),
                limit=1,
            )
        ] == ["placeholder knowledge"]
    finally:
        if backend is not None:
            try:
                backend.destroy()
            finally:
                backend.close()
        with psycopg.connect(admin_dsn, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(
                    sql.Identifier(database)
                )
            )


@pytest.mark.integration
def test_native_components_preserve_cross_store_branch_semantics(
    tmp_path: Path,
) -> None:
    admin_dsn = os.environ.get("ENTERPRISE_NATIVE_DOLTGRES_DSN")
    qdrant_url = os.environ.get("ENTERPRISE_NATIVE_QDRANT_URL")
    btrfs_root = os.environ.get("ENTERPRISE_NATIVE_BTRFS_ROOT")
    if not admin_dsn or not qdrant_url or not btrfs_root:
        pytest.skip(
            "set ENTERPRISE_NATIVE_DOLTGRES_DSN, "
            "ENTERPRISE_NATIVE_QDRANT_URL, and "
            "ENTERPRISE_NATIVE_BTRFS_ROOT"
        )

    database = f"enterprise_native_test_{uuid.uuid4().hex[:16]}"
    with psycopg.connect(admin_dsn, autocommit=True) as connection:
        connection.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database))
        )
    parameters = conninfo_to_dict(admin_dsn)
    parameters["dbname"] = database
    backend_dsn = make_conninfo(**parameters)
    backend: DoltgresQdrantBtrfsKnowledgeBackend | None = None
    try:
        backend = DoltgresQdrantBtrfsKnowledgeBackend(
            tmp_path,
            vector_dimensions=3,
            qdrant_url=qdrant_url,
            doltgres_dsn=backend_dsn,
            btrfs_root=btrfs_root,
        )
        assert "document_id" in backend._qdrant.get_collection(
            backend._collection
        ).payload_schema
        backend.put_document(
            "main",
            _indexed("scheduler", "root scheduler", (1.0, 0.0, 0.0)),
            operation_id="seed",
        )
        backend.create_branch("team/a", "main")
        backend.create_branch("team/b", "main")
        backend.put_document(
            "main",
            _indexed("scheduler", "late root update", (0.0, 0.0, 1.0)),
            operation_id="root:late",
        )
        backend.put_document(
            "team/a",
            _indexed("scheduler", "team a scheduler", (0.0, 1.0, 0.0)),
            operation_id="team:a",
        )
        backend.delete_document(
            "team/b",
            "scheduler",
            operation_id="team:b:delete",
        )
        backend.create_branch("team/a/child", "team/a")
        backend.put_document(
            "team/a",
            _indexed("scheduler", "late team update", (1.0, 1.0, 0.0)),
            operation_id="team:a:late",
        )

        assert (
            backend.get_document("main", "scheduler").document.content
            == "late root update"
        )
        assert (
            backend.get_document("team/a", "scheduler").document.content
            == "late team update"
        )
        assert (
            backend.get_document("team/a/child", "scheduler").document.content
            == "team a scheduler"
        )
        assert backend.get_document("team/b", "scheduler") is None
        assert [
            hit.text
            for hit in backend.search(
                "team/a/child",
                "",
                (0.0, 1.0, 0.0),
                limit=3,
            )
        ] == ["team a scheduler"]
        assert [
            hit.text
            for hit in backend.search(
                "team/a/child",
                "scheduler",
                (0.0, 1.0, 0.0),
                limit=3,
            )
        ] == ["team a scheduler"]
        assert (
            backend.read_file("team/a/child", "/knowledge/scheduler.md")
            == b"team a scheduler"
        )
        assert backend.diff("team/a", "main") == {
            "documents": {
                "added": [],
                "deleted": [],
                "modified": ["scheduler"],
            },
            "files": {
                "added": [],
                "deleted": [],
                "modified": ["/knowledge/scheduler.md"],
            },
        }

        # A vector-only branch change is discovered through the chunk table's
        # Dolt diff and compared through Qdrant's branch-aware live view. The
        # document catalog and Btrfs file are intentionally unchanged.
        backend.create_branch("vector/diff", "main")
        backend.put_document(
            "vector/diff",
            _indexed("scheduler", "late root update", (1.0, 0.0, 0.0)),
            operation_id="vector:only",
        )
        assert backend._qdrant_changed_document_ids(
            "vector/diff",
            "main",
        ) == {"scheduler"}
        assert backend.diff("vector/diff", "main") == {
            "documents": {
                "added": [],
                "deleted": [],
                "modified": ["scheduler"],
            },
            "files": {"added": [], "deleted": [], "modified": []},
        }
        assert backend.merge(
            "vector/diff",
            "main",
            operation_id="merge:vector-only",
        ) == {
            "documents": 1,
            "files": 0,
            "status": "applied",
            "atomic": False,
        }
        assert backend.get_document("main", "scheduler").chunks[
            0
        ].embedding == (1.0, 0.0, 0.0)

        backend.create_branch("merge/source", "main")
        backend.put_document(
            "merge/source",
            _indexed("source-note", "source knowledge", (1.0, 0.0, 0.0)),
            operation_id="source:note",
        )
        backend.put_document(
            "main",
            _indexed("target-note", "target knowledge", (0.0, 1.0, 0.0)),
            operation_id="target:note",
        )
        with pytest.raises(
            ValueError,
            match="merge target advanced",
        ):
            backend.merge(
                "merge/source",
                "main",
                operation_id="stale-merge",
            )
        backend.delete_branch("merge/source")
        backend.create_branch("merge/source", "main")
        backend.put_document(
            "merge/source",
            _indexed("source-note", "source knowledge", (1.0, 0.0, 0.0)),
            operation_id="source:note:rebased",
        )
        assert backend.merge(
            "merge/source",
            "main",
            operation_id="merge",
        ) == {
            "documents": 1,
            "files": 1,
            "status": "applied",
            "atomic": False,
        }
        assert (
            backend.get_document("main", "source-note").document.content
            == "source knowledge"
        )
        assert (
            backend.get_document("main", "target-note").document.content
            == "target knowledge"
        )

        backend.create_branch("merge/selective", "main")
        backend.put_document(
            "merge/selective",
            _indexed("reviewed", "reviewed knowledge", (0.0, 0.0, 1.0)),
            operation_id="selective:reviewed",
        )
        backend.write_file(
            "merge/selective",
            "/artifacts/private-notes.md",
            b"do not publish",
            operation_id="selective:private",
        )
        preview = backend.merge_preview("merge/selective", "main")
        selected = preview["selection_groups"]["indexed_documents"][
            "reviewed"
        ]
        result = backend.merge(
            "merge/selective",
            "main",
            operation_id="merge:selective",
            selected_change_ids=selected,
            preview_token=preview["preview_token"],
        )
        assert result["atomic"] is False
        assert backend.get_document("main", "reviewed") is not None
        with pytest.raises(FileNotFoundError):
            backend.read_file("main", "/artifacts/private-notes.md")

        backend.delete_branch("team/a")
        assert "team/a" not in backend.list_branches()
        assert "team/a/child" not in backend.list_branches()
        assert backend.get_document("main", "scheduler") is not None
    finally:
        if backend is not None:
            try:
                backend.destroy()
            finally:
                backend.close()
        with psycopg.connect(admin_dsn, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(
                    sql.Identifier(database)
                )
            )
