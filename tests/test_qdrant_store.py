from __future__ import annotations

from pathlib import Path

import pytest
from chronos_core.branching import BranchingError
from chronos_core.workspace import (
    ChronosFSStore,
    ChronosQdrantStore,
    ChronosWorkspaceContext,
    QdrantStoreError,
    QdrantUpsert,
)
from qdrant_client import models


@pytest.fixture
def qdrant_store() -> ChronosQdrantStore:
    store = ChronosQdrantStore.local("sqlite:///:memory:")
    store.register_collection("documents", 3)
    try:
        yield store
    finally:
        store.close()


def test_qdrant_store_upsert_get_and_search(
    qdrant_store: ChronosQdrantStore,
) -> None:
    main = qdrant_store.checkout("main")
    main.upsert(
        "documents",
        "runtime",
        [1.0, 0.0, 0.0],
        {"title": "Runtime design"},
    )
    main.upsert(
        "documents",
        "billing",
        [0.0, 1.0, 0.0],
        {"title": "Billing policy"},
    )

    runtime = main.get("documents", "runtime")
    assert runtime is not None
    assert runtime.payload == {"title": "Runtime design"}
    assert runtime.vector == [1.0, 0.0, 0.0]

    results = main.search("documents", [1.0, 0.0, 0.0], limit=2)
    assert [result.id for result in results] == ["runtime", "billing"]
    assert results[0].score == pytest.approx(1.0)


def test_qdrant_search_passes_explicit_server_timeout(
    qdrant_store: ChronosQdrantStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []
    original = qdrant_store.client.query_points

    def record_query(**kwargs: object):
        calls.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(qdrant_store.client, "query_points", record_query)
    session = qdrant_store.checkout("main")
    session.upsert("documents", "runtime", [1.0, 0.0, 0.0])
    session.search("documents", [1.0, 0.0, 0.0], limit=1)

    assert calls
    assert calls[-1]["timeout"] == 600


def test_qdrant_store_isolates_updates_and_deletes(
    qdrant_store: ChronosQdrantStore,
) -> None:
    main = qdrant_store.checkout("main")
    main.upsert("documents", "shared", [1.0, 0.0, 0.0], {"value": "main"})
    main.upsert("documents", "retained", [0.0, 1.0, 0.0], {"value": "main"})

    qdrant_store.create_branch("team-a", from_branch="main")
    qdrant_store.create_branch("team-b", from_branch="main")
    team_a = qdrant_store.checkout("team-a")
    team_b = qdrant_store.checkout("team-b")
    team_a.upsert(
        "documents",
        "shared",
        [0.0, 0.0, 1.0],
        {"value": "team-a"},
    )
    assert team_a.delete("documents", "retained")
    team_a.upsert("documents", "private", [0.5, 0.5, 0.0], {"team": "a"})

    assert main.get("documents", "shared").payload == {"value": "main"}  # type: ignore[union-attr]
    assert main.get("documents", "retained") is not None
    assert main.get("documents", "private") is None

    assert team_b.get("documents", "shared").payload == {"value": "main"}  # type: ignore[union-attr]
    assert team_b.get("documents", "retained") is not None
    assert team_b.get("documents", "private") is None

    assert team_a.get("documents", "shared").payload == {"value": "team-a"}  # type: ignore[union-attr]
    assert team_a.get("documents", "retained") is None
    assert team_a.get("documents", "private") is not None


def test_qdrant_store_nested_branches_preserve_visibility(
    qdrant_store: ChronosQdrantStore,
) -> None:
    qdrant_store.checkout("main").upsert(
        "documents",
        "doc",
        [1.0, 0.0, 0.0],
        {"owner": "company"},
    )
    parent = "main"
    expected = "company"
    for depth in range(1, 24):
        branch = f"depth-{depth}"
        qdrant_store.create_branch(branch, from_branch=parent)
        session = qdrant_store.checkout(branch)
        if depth in {3, 11, 23}:
            expected = branch
            session.upsert(
                "documents",
                "doc",
                [1.0, float(depth), 0.0],
                {"owner": branch},
            )
        assert session.get("documents", "doc").payload == {"owner": expected}  # type: ignore[union-attr]
        parent = branch

    assert qdrant_store.checkout("main").get("documents", "doc").payload == {
        "owner": "company"
    }  # type: ignore[union-attr]


def test_qdrant_store_transaction_rollback_repairs_index(
    qdrant_store: ChronosQdrantStore,
) -> None:
    main = qdrant_store.checkout("main")
    main.upsert("documents", "doc", [1.0, 0.0, 0.0], {"value": "before"})
    qdrant_store.create_branch("agent", from_branch="main")
    agent = qdrant_store.checkout("agent")

    with pytest.raises(RuntimeError), agent.transaction():
        agent.upsert(
            "documents",
            "doc",
            [0.0, 1.0, 0.0],
            {"value": "uncommitted"},
        )
        agent.upsert(
            "documents",
            "new",
            [0.0, 0.0, 1.0],
            {"value": "uncommitted"},
        )
        raise RuntimeError("abort")

    refreshed = qdrant_store.checkout("agent")
    assert refreshed.get("documents", "doc").payload == {"value": "before"}  # type: ignore[union-attr]
    assert refreshed.get("documents", "new") is None


def test_qdrant_store_diff_and_merge(
    qdrant_store: ChronosQdrantStore,
) -> None:
    main = qdrant_store.checkout("main")
    main.upsert("documents", "changed", [1.0, 0.0, 0.0], {"value": "main"})
    main.upsert("documents", "removed", [0.0, 1.0, 0.0], {"value": "main"})
    qdrant_store.create_branch("candidate", from_branch="main")
    candidate = qdrant_store.checkout("candidate")
    candidate.upsert(
        "documents",
        "changed",
        [0.0, 0.0, 1.0],
        {"value": "candidate"},
    )
    candidate.delete("documents", "removed")
    candidate.upsert("documents", "added", [0.5, 0.5, 0.0], {"new": True})

    diff = qdrant_store.diff("main", "candidate")
    assert {(change.key["id"], change.change) for change in diff.changes} == {
        ("changed", "modified"),
        ("removed", "deleted"),
        ("added", "added"),
    }
    preview = qdrant_store.merge_preview("candidate", "main")
    assert len(preview.changes) == 3
    assert not preview.conflicts

    result = qdrant_store.merge_apply("candidate", "main")
    assert result.applied == 3
    merged = qdrant_store.checkout("main")
    assert merged.get("documents", "changed").payload == {"value": "candidate"}  # type: ignore[union-attr]
    assert merged.get("documents", "removed") is None
    assert merged.get("documents", "added") is not None


def test_qdrant_store_detects_and_resolves_merge_conflict(
    qdrant_store: ChronosQdrantStore,
) -> None:
    qdrant_store.checkout("main").upsert(
        "documents",
        "doc",
        [1.0, 0.0, 0.0],
        {"value": "base"},
    )
    qdrant_store.create_branch("candidate", from_branch="main")
    qdrant_store.checkout("candidate").upsert(
        "documents",
        "doc",
        [0.0, 1.0, 0.0],
        {"value": "candidate"},
    )
    qdrant_store.checkout("main").upsert(
        "documents",
        "doc",
        [0.0, 0.0, 1.0],
        {"value": "target"},
    )

    preview = qdrant_store.merge_preview("candidate", "main")
    assert len(preview.conflicts) == 1
    with pytest.raises(BranchingError):
        qdrant_store.merge_apply("candidate", "main")

    qdrant_store.merge_apply("candidate", "main", policy="source_wins")
    merged = qdrant_store.checkout("main").get("documents", "doc")
    assert merged is not None
    assert merged.payload == {"value": "candidate"}


def test_qdrant_store_checkpoint_is_read_only(
    qdrant_store: ChronosQdrantStore,
) -> None:
    qdrant_store.checkout("main").upsert(
        "documents",
        "doc",
        [1.0, 0.0, 0.0],
        {"value": "snapshot"},
    )
    qdrant_store.create_checkpoint("release", branch="main")
    checkpoint = qdrant_store.checkout_checkpoint("release")
    assert checkpoint.get("documents", "doc") is not None

    with pytest.raises(BranchingError):
        checkpoint.upsert(
            "documents",
            "doc",
            [0.0, 1.0, 0.0],
            {"value": "invalid"},
        )
    assert checkpoint.get("documents", "doc").payload == {"value": "snapshot"}  # type: ignore[union-attr]


def test_qdrant_store_persists_local_data(tmp_path: Path) -> None:
    metadata_url = f"sqlite:///{tmp_path / 'qdrant-metadata.sqlite'}"
    qdrant_path = tmp_path / "qdrant"

    first = ChronosQdrantStore.local(metadata_url, path=qdrant_path)
    first.register_collection("documents", 3)
    first.checkout("main").upsert(
        "documents",
        "doc",
        [1.0, 0.0, 0.0],
        {"value": "persisted"},
    )
    first.create_branch("team", from_branch="main")
    first.checkout("team").upsert(
        "documents",
        "doc",
        [0.0, 1.0, 0.0],
        {"value": "team"},
    )
    first.close()

    second = ChronosQdrantStore.local(metadata_url, path=qdrant_path)
    try:
        assert second.checkout("main").get("documents", "doc").payload == {
            "value": "persisted"
        }  # type: ignore[union-attr]
        assert second.checkout("team").get("documents", "doc").payload == {
            "value": "team"
        }  # type: ignore[union-attr]
    finally:
        second.close()


def test_register_collection_reuses_metadata_physical_name(tmp_path: Path) -> None:
    metadata_url = f"sqlite:///{tmp_path / 'qdrant-metadata.sqlite'}"
    qdrant_path = tmp_path / "qdrant"

    first = ChronosQdrantStore.local(
        metadata_url,
        path=qdrant_path,
        collection_prefix="first_",
    )
    first_info = first.register_collection("documents", 3)
    first.close()

    second = ChronosQdrantStore.local(
        metadata_url,
        path=qdrant_path,
        collection_prefix="second_",
    )
    try:
        second_info = second.register_collection("documents", 3)
        assert second_info.physical_name == first_info.physical_name
    finally:
        second.close()


def test_register_collection_rejects_missing_physical_collection(
    tmp_path: Path,
) -> None:
    store = ChronosQdrantStore.local(
        f"sqlite:///{tmp_path / 'metadata.sqlite'}",
        path=tmp_path / "qdrant",
    )
    try:
        info = store.register_collection("documents", 3)
        store.client.delete_collection(info.physical_name)

        with pytest.raises(QdrantStoreError, match="missing Qdrant collection"):
            store.register_collection("documents", 3)
    finally:
        store.close()


def test_qdrant_versions_live_only_in_qdrant(
    qdrant_store: ChronosQdrantStore,
) -> None:
    info = qdrant_store.collection_info("documents")
    main = qdrant_store.checkout("main")
    main.upsert("documents", "doc", [1.0, 0.0, 0.0], {"value": "main"})
    qdrant_store.create_branch("team", from_branch="main")
    qdrant_store.checkout("team").upsert(
        "documents", "doc", [0.0, 1.0, 0.0], {"value": "team"}
    )

    sql_objects = {
        row["name"]
        for row in qdrant_store.context.db.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
        )
    }
    assert "chronos_qdrant_points" not in sql_objects
    assert "chronos_qdrant_state" not in sql_objects
    assert (
        qdrant_store.context.db.execute(
            "SELECT 1 FROM _chronos_branch_tables WHERE table_name = ?",
            ("chronos_qdrant_points",),
        ).fetchone()
        is None
    )

    records = qdrant_store._scroll_all(  # noqa: SLF001 - storage invariant
        info.physical_name,
        scroll_filter=qdrant_store._logical_filter("doc"),  # noqa: SLF001
    )
    assert len(records) == 3
    for record in records:
        assert "_chronos_low" in record.payload
        assert "_chronos_high" in record.payload
        assert "_chronos_writer" in record.payload
        assert "_chronos_active" not in record.payload


def test_qdrant_splice_uses_one_batch_operation(
    qdrant_store: ChronosQdrantStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main = qdrant_store.checkout("main")
    main.upsert("documents", "doc", [1.0, 0.0, 0.0])
    qdrant_store.create_branch("team", from_branch="main")
    calls: list[dict[str, object]] = []
    original = qdrant_store.client.batch_update_points

    def record_batch(**kwargs: object) -> object:
        calls.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(qdrant_store.client, "batch_update_points", record_batch)
    qdrant_store.checkout("team").upsert("documents", "doc", [0.0, 1.0, 0.0])

    assert len(calls) == 1
    operations = calls[0]["update_operations"]
    assert isinstance(operations, list)
    assert [type(operation).__name__ for operation in operations] == ["UpsertOperation"]
    assert calls[0]["wait"] is True
    assert calls[0]["ordering"] == models.WriteOrdering.STRONG


def test_qdrant_store_composes_with_sqlite_and_chronosfs(
    tmp_path: Path,
) -> None:
    from chronos_core.branching import ChronosBranchContext

    sqlite = ChronosBranchContext.connect(
        f"sqlite:///{tmp_path / 'knowledge.sqlite'}",
        backend="interval",
    )
    chronosfs = ChronosFSStore.connect(
        f"sqlite:///{tmp_path / 'chronosfs.sqlite'}",
        backend="interval",
    )
    chronosfs.ensure()
    qdrant = ChronosQdrantStore.local(
        f"sqlite:///{tmp_path / 'qdrant-metadata.sqlite'}",
        path=tmp_path / "qdrant",
    )
    qdrant.register_collection("documents", 3)
    sqlite.db.execute("CREATE TABLE documents (id TEXT PRIMARY KEY, title TEXT)")
    sqlite.db.execute(
        "INSERT INTO documents VALUES (?, ?)",
        ("doc", "Company"),
    )
    sqlite.db.commit()
    sqlite.register_table("documents", ["id"])
    chronosfs.write_file("main", "/documents/doc.md", "company\n", parents=True)
    qdrant.checkout("main").upsert(
        "documents",
        "doc",
        [1.0, 0.0, 0.0],
        {"title": "Company"},
    )

    workspace = ChronosWorkspaceContext(
        filesystem=chronosfs,
        sqlite=sqlite,
        qdrant=qdrant,
    )
    try:
        workspace.create_branch("team", from_branch="main")
        team = workspace.checkout("team")
        team.sqlite.execute(
            "UPDATE documents SET title = :title WHERE id = :id",
            {"id": "doc", "title": "Team"},
        )
        team.fs.write_file("/documents/doc.md", "team\n")
        team.qdrant.upsert(
            "documents",
            "doc",
            [0.0, 1.0, 0.0],
            {"title": "Team"},
        )

        main = workspace.checkout("main")
        assert main.sqlite.query("SELECT title FROM documents") == [
            {"title": "Company"}
        ]
        assert main.fs.read_text("/documents/doc.md") == "company\n"
        assert main.qdrant.get("documents", "doc").payload == {"title": "Company"}

        assert team.sqlite.query("SELECT title FROM documents") == [{"title": "Team"}]
        assert team.fs.read_text("/documents/doc.md") == "team\n"
        assert team.qdrant.get("documents", "doc").payload == {"title": "Team"}
    finally:
        workspace.close()


def test_atomic_workspace_reuses_relational_session_for_qdrant(
    tmp_path: Path,
) -> None:
    from chronos_core.branching import ChronosBranchContext

    metadata_url = f"sqlite:///{tmp_path / 'knowledge.sqlite'}"
    sqlite = ChronosBranchContext.connect(metadata_url, backend="interval")
    sqlite.db.execute("CREATE TABLE documents (id TEXT PRIMARY KEY, title TEXT)")
    sqlite.db.commit()
    sqlite.register_table("documents", ["id"])
    qdrant = ChronosQdrantStore.local(
        metadata_url,
        path=tmp_path / "qdrant",
        context=sqlite,
    )
    qdrant.register_collection("documents", 3)
    workspace = ChronosWorkspaceContext(
        sqlite=sqlite,
        qdrant=qdrant,
        shared_metadata_url=metadata_url,
    )
    try:
        session = workspace.checkout("main")

        # Relational queries and Qdrant visibility use the exact same live
        # interval-control session, not two separately sampled branch heads.
        assert session.qdrant._session._control is session.sqlite._session

        with session.transaction():
            session.sqlite.upsert_rows("documents", [{"id": "doc", "title": "Shared"}])
            session.qdrant.upsert(
                "documents",
                "doc",
                [1.0, 0.0, 0.0],
                {"title": "Shared"},
            )

        assert session.sqlite.query("SELECT title FROM documents") == [
            {"title": "Shared"}
        ]
        assert session.qdrant.get("documents", "doc").payload == {"title": "Shared"}

        with pytest.raises(RuntimeError, match="abort shared transaction"):
            with session.transaction():
                session.sqlite.upsert_rows(
                    "documents", [{"id": "aborted", "title": "Aborted"}]
                )
                session.qdrant.upsert(
                    "documents",
                    "aborted",
                    [0.0, 1.0, 0.0],
                    {"title": "Aborted"},
                )
                raise RuntimeError("abort shared transaction")

        assert (
            session.sqlite.query("SELECT title FROM documents WHERE id = 'aborted'")
            == []
        )
        assert session.qdrant.get("documents", "aborted") is None
    finally:
        workspace.close()


def test_qdrant_bulk_load_upsert_and_delete_preserve_branch_isolation(
    tmp_path: Path,
) -> None:
    store = ChronosQdrantStore.local(
        f"sqlite:///{tmp_path / 'metadata.sqlite'}",
        path=tmp_path / "qdrant",
    )
    try:
        store.register_collection("docs", dimensions=3)
        root = store.checkout("main")
        root.load_many(
            "docs",
            [
                QdrantUpsert(
                    id=f"doc-{index}",
                    vector=[float(index), 1.0, 0.0],
                    payload={"index": index},
                    revision=f"seed:{index}",
                )
                for index in range(20)
            ],
        )
        store.create_branch("team", from_branch="main")
        team = store.checkout("team")
        team.upsert_many(
            "docs",
            [
                QdrantUpsert(
                    id=f"doc-{index}",
                    vector=[0.0, float(index), 1.0],
                    payload={"index": index, "team": True},
                    revision=f"team:{index}",
                )
                for index in range(5)
            ],
        )
        assert team.delete_many("docs", ["doc-5", "doc-6", "missing"]) == [
            "doc-5",
            "doc-6",
        ]

        assert len(root.list_points("docs")) == 20
        assert len(team.list_points("docs")) == 18
        assert root.get("docs", "doc-0").payload == {"index": 0}
        assert team.get("docs", "doc-0").payload == {
            "index": 0,
            "team": True,
        }
        assert root.get("docs", "doc-5") is not None
        assert team.get("docs", "doc-5") is None
    finally:
        store.close()
