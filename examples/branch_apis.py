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
            f"sqlite:///{root / 'chronosfs.sqlite'}",
            backend="interval",
        )
        filesystem.ensure()
        chronos = ChronosWorkspaceContext(sqlite=sqlite, filesystem=filesystem)

        try:
            sqlite.db.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
            sqlite.db.execute("INSERT INTO docs VALUES (?, ?)", ("d1", "main"))
            sqlite.db.commit()
            sqlite.register_table("docs", ["id"])
            filesystem.write_file("main", "/reports/summary.md", "main\n", parents=True)

            chronos.create_branch("agent", from_branch="main")
            branch = chronos.checkout("agent")
            branch.sqlite.execute(
                "UPDATE docs SET body = :body WHERE id = :id",
                {"id": "d1", "body": "agent"},
            )
            branch.fs.write_file("/reports/summary.md", "agent\n", parents=True)

            chronos.create_checkpoint("before_merge", branch="agent")
            readonly = chronos.checkout_checkpoint("before_merge")
            assert readonly.sqlite.query("SELECT body FROM docs WHERE id = :id", {"id": "d1"}) == [
                {"body": "agent"}
            ]
            assert readonly.fs.read_text("/reports/summary.md") == "agent\n"

            chronos.create_branch_from_checkpoint("retry", checkpoint="before_merge")
            retry = chronos.checkout("retry")
            retry.sqlite.execute(
                "UPDATE docs SET body = :body WHERE id = :id",
                {"id": "d1", "body": "retry"},
            )
            retry.fs.write_file("/reports/summary.md", "retry\n", parents=True)

            preview = chronos.merge_preview("retry", "main", policy="manual_review")
            assert preview["sqlite"].changes
            assert preview["filesystem"].changes

            chronos.merge_apply("retry", "main", policy="snapshot_isolation")
            merged = chronos.checkout("main")
            assert merged.sqlite.query("SELECT body FROM docs WHERE id = :id", {"id": "d1"}) == [
                {"body": "retry"}
            ]
            assert merged.fs.read_text("/reports/summary.md") == "retry\n"

            chronos.delete_branch("retry")
            chronos.delete_branch("agent")
        finally:
            chronos.close()

    print("branch API example passed")


if __name__ == "__main__":
    main()
