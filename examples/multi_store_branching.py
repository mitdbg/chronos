from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from chronos_core.branching import ChronosBranchContext
from chronos_core.workspace import ChronosFSStore, ChronosWorkspaceContext


def main() -> None:
    with TemporaryDirectory(prefix="chronos-example-") as temp_dir:
        root = Path(temp_dir)
        sqlite = ChronosBranchContext.connect(
            f"sqlite:///{root / 'app.sqlite'}",
            backend="interval",
        )
        filesystem = ChronosFSStore.connect(
            sqlite.db.database_url,
            backend="interval",
        )
        filesystem.ensure()

        chronos = ChronosWorkspaceContext(
            sqlite=sqlite, filesystem=filesystem,
            shared_metadata_url=sqlite.db.database_url,
        )
        try:
            sqlite.db.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
            sqlite.db.execute("INSERT INTO docs VALUES (?, ?)", ("d1", "main"))
            sqlite.db.commit()
            sqlite.register_table("docs", ["id"])
            sqlite.set_merge_table_scope(["docs"])
            filesystem.write_file("main", "/reports/summary.md", "main\n", parents=True)

            chronos.create_branch("agent", from_branch="main")
            agent = chronos.checkout("agent")
            agent.sqlite.execute(
                "UPDATE docs SET body = :body WHERE id = :id",
                {"id": "d1", "body": "candidate"},
            )
            agent.fs.write_file("/reports/summary.md", "candidate\n", parents=True)

            main = chronos.checkout("main")
            assert main.sqlite.query(
                "SELECT body FROM docs WHERE id = :id",
                {"id": "d1"},
            ) == [{"body": "main"}]
            assert main.fs.read_text("/reports/summary.md") == "main\n"

            preview = chronos.merge_atomic_preview("agent", "main", policy="manual_review")
            assert len(preview.stores["sqlite"].changes) == 1
            assert len(preview.stores["filesystem"].changes) >= 1

            result = chronos.merge_atomic("agent", "main", policy="snapshot_isolation", operation_id="example-merge")
            assert result.stores["sqlite"] == 1
            assert result.stores["filesystem"] >= 1

            refreshed = chronos.checkout("main")
            assert refreshed.sqlite.query(
                "SELECT body FROM docs WHERE id = :id",
                {"id": "d1"},
            ) == [{"body": "candidate"}]
            assert refreshed.fs.read_text("/reports/summary.md") == "candidate\n"
        finally:
            chronos.close()

    print("multi-store branching example passed")


if __name__ == "__main__":
    main()
