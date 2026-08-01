#!/usr/bin/env python3
"""Remove non-root branches through the public knowledge-backend contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from chronos_enterprise_knowledge.backends import create_knowledge_backend


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--backend", default="chronos")
    parser.add_argument("--dimensions", type=int, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    backend = create_knowledge_backend(
        args.backend,
        args.state_dir.expanduser().resolve(),
        vector_dimensions=args.dimensions,
    )
    deleted: list[str] = []
    try:
        while True:
            branches = [
                branch
                for branch in backend.list_branches()
                if branch != "main"
            ]
            if not branches:
                break
            branch = min(branches, key=lambda value: (value.count("/"), value))
            backend.delete_branch(branch)
            deleted.append(branch)
        remaining = backend.list_branches()
        if remaining != ["main"]:
            raise RuntimeError(
                f"expected only main after hierarchy reset, found {remaining}"
            )
    finally:
        backend.close()
    print(
        json.dumps(
            {
                "deleted_roots": deleted,
                "remaining": ["main"],
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
