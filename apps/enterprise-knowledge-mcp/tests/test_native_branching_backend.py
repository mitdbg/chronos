from __future__ import annotations

import os
import subprocess
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from chronos_enterprise_knowledge.backends.native_branching import (
    DoltgresQdrantBtrfsKnowledgeBackend,
    _qdrant_branch_filter,
)
from chronos_enterprise_knowledge.backends.btrfs_workspace import (
    BtrfsWorkspaceStore,
)
from chronos_enterprise_knowledge.models import (
    DocumentChunk,
    IndexedDocument,
    KnowledgeDocument,
)


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
        ) == {"documents": 1, "files": 1}
        assert (
            backend.get_document("main", "source-note").document.content
            == "source knowledge"
        )
        assert (
            backend.get_document("main", "target-note").document.content
            == "target knowledge"
        )

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
