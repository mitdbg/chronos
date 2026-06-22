from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from chronos_core.workspace import ChronosFSStore


def main() -> None:
    with TemporaryDirectory(prefix="chronos-example-") as temp_dir:
        db_path = Path(temp_dir) / "chronosfs.sqlite"
        fs = ChronosFSStore.connect(f"sqlite:///{db_path}", backend="interval")
        fs.ensure()
        try:
            fs.write_file("main", "/src/solution.py", "print('main')\n", parents=True)
            fs.create_branch("agent", from_branch="main")
            fs.write_file("agent", "/src/solution.py", "print('agent')\n", parents=True)
            fs.write_file("agent", "/reports/result.txt", "candidate\n", parents=True)

            assert fs.read_text("main", "/src/solution.py") == "print('main')\n"
            assert not fs.exists("main", "/reports/result.txt")
            assert fs.read_text("agent", "/src/solution.py") == "print('agent')\n"

            preview = fs.merge_preview("agent", "main", policy="manual_review")
            assert preview.changes
            assert preview.conflicts == []

            result = fs.merge_apply("agent", "main", policy="snapshot_isolation")
            assert result.applied >= 1
            assert fs.read_text("main", "/src/solution.py") == "print('agent')\n"
            assert fs.read_text("main", "/reports/result.txt") == "candidate\n"
        finally:
            fs.close()

    print("ChronosFS direct API example passed")


if __name__ == "__main__":
    main()
