from __future__ import annotations

import subprocess
import sys
from pathlib import Path


EXAMPLES = [
    "single_store_branching.py",
    "multi_store_branching.py",
    "branch_apis.py",
    "chronosfs_direct.py",
    "branch_transactions.py",
    "software_development.py",
    "chronosfs_fuse_control.py",
]


def main() -> None:
    examples_dir = Path(__file__).resolve().parent
    for example in EXAMPLES:
        path = examples_dir / example
        print(f"running {path.relative_to(examples_dir.parent)}", flush=True)
        subprocess.run([sys.executable, str(path)], check=True)


if __name__ == "__main__":
    main()
