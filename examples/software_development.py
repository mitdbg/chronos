"""Reproduce a bug, test a fix, and atomically merge its code and data."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory

from chronos_core.branching import ChronosBranchContext
from chronos_core.workspace import ChronosFSStore, ChronosWorkspaceContext


BROKEN = "def total(price, discount):\n    return price + discount\n"
FIXED = "def total(price, discount):\n    return price - discount\n"


def run_tests(session, directory: Path) -> subprocess.CompletedProcess[str]:
    """Export known tutorial inputs; a real project can use a FUSE mount."""
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "pricing.py").write_text(session.fs.read_text("/pricing.py"))
    rows = session.app.query("SELECT price, discount FROM offers WHERE id = 1")
    (directory / "fixture.json").write_text(json.dumps(rows[0]))
    check = (
        "import json; from pricing import total; "
        "v=json.load(open('fixture.json')); "
        "assert total(v['price'],v['discount']) == 90"
    )
    # The repository-owned code is trusted. subprocess alone is not a sandbox.
    return subprocess.run(
        [sys.executable, "-B", "-c", check], cwd=directory,
        capture_output=True, text=True, timeout=10,
    )


def main() -> None:
    with TemporaryDirectory(prefix="chronos-sde-") as tmp:
        root = Path(tmp)
        url = f"sqlite:///{root / 'state.sqlite'}"
        app = ChronosBranchContext.connect(url)
        app.db.execute(
            "CREATE TABLE offers (id INTEGER PRIMARY KEY, price INTEGER, discount INTEGER)"
        )
        app.db.execute("INSERT INTO offers VALUES (1, 100, 0)")
        app.db.commit()
        app.register_table("offers", ["id"])
        fs = ChronosFSStore.connect(url)
        fs.ensure()
        fs.write_file("main", "/pricing.py", BROKEN, parents=True)
        app.set_merge_table_scope(["offers"])
        chronos = ChronosWorkspaceContext(
            app=app, filesystem=fs, shared_metadata_url=url,
        )
        try:
            chronos.create_branch("fix")
            candidate = chronos.checkout("fix")
            try:
                candidate.app.execute("UPDATE offers SET discount = 10 WHERE id = 1")
                failed = run_tests(candidate, root / "checkout")
                assert failed.returncode != 0, "the original bug must reproduce"
                candidate.fs.write_file("/pricing.py", FIXED)
                passed = run_tests(candidate, root / "checkout")
                assert passed.returncode == 0, passed.stderr
            finally:
                candidate.app.close()

            baseline = chronos.checkout("main")
            try:
                assert baseline.fs.read_text("/pricing.py") == BROKEN
                assert baseline.app.query("SELECT discount FROM offers") == [{"discount": 0}]
            finally:
                baseline.app.close()

            preview = chronos.merge_atomic_preview("fix", "main", policy="snapshot_isolation")
            assert preview.stores["app"].changes
            assert preview.stores["filesystem"].changes
            result = chronos.merge_atomic(
                "fix", "main", preview_token=preview.preview_token,
                policy="snapshot_isolation", operation_id="accept-pricing-fix",
            )
            assert result.status == "committed"
            accepted = chronos.checkout("main")
            try:
                passed = run_tests(accepted, root / "accepted")
                assert passed.returncode == 0, passed.stderr
                assert accepted.fs.read_text("/pricing.py") == FIXED
            finally:
                accepted.app.close()
            chronos.delete_branch("fix")
        finally:
            chronos.close()
    print("SDE tutorial passed: reproduced bug, tested fix, atomically merged code and data")


if __name__ == "__main__":
    main()
