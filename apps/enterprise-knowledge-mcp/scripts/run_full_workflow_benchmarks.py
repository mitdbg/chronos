#!/usr/bin/env python3
"""Run each real Codex workflow as an independent full-corpus benchmark."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from chronos_enterprise_knowledge.prepared_benchmark import (
    PreparedRootBenchmark,
)
from chronos_enterprise_knowledge.rollout_trace import WorkloadTrace

_LEGACY_DEPENDENCIES = {
    "01": (),
    "02": ("01",),
    "03": ("01", "02"),
    "04": ("01", "02"),
    "05": ("01", "02"),
    "06": ("01",),
    "07": (),
    "08": (),
    "09": (),
    "10": (),
    "11": (),
    "12": (),
    "13": (),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--traces-dir", type=Path, required=True)
    parser.add_argument(
        "--snapshot-manifest",
        action="append",
        type=Path,
        required=True,
        help=(
            "Manifest for one snapshot present in every prepared root; "
            "repeat for layered company-document and source-code snapshots."
        ),
    )
    parser.add_argument(
        "--base-state",
        action="append",
        required=True,
        metavar="BACKEND=PATH",
    )
    parser.add_argument(
        "--in-place-backend",
        action="append",
        default=[],
        help=(
            "Use the prepared backend state directly instead of making a "
            "local reflink. Intended for external native services."
        ),
    )
    parser.add_argument("--qdrant-url")
    parser.add_argument("--qdrant-api-key")
    parser.add_argument("--doltgres-dsn")
    parser.add_argument("--btrfs-root", type=Path)
    parser.add_argument("--doltgres-data-dir", type=Path)
    parser.add_argument("--qdrant-storage-dir", type=Path)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--repo-dir", type=Path, required=True)
    parser.add_argument("--dimensions", type=int, required=True)
    parser.add_argument("--embedding-model", required=True)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument(
        "--max-shell-interrupt-seconds",
        type=float,
        help=(
            "cap recorded delays before interrupting asynchronous shell "
            "commands; useful for excluding human polling time"
        ),
    )
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--end", type=int, default=13)
    parser.add_argument(
        "--exclude",
        action="append",
        type=int,
        default=[],
        help="Workflow number to omit; repeat for multiple workflows.",
    )
    parser.add_argument(
        "--dependency-mode",
        choices=("legacy", "independent"),
        default="legacy",
        help=(
            "Use legacy setup-trace dependencies, or treat every workflow as "
            "self-contained against the prepared organizational hierarchy."
        ),
    )
    parser.add_argument(
        "--shared-root-sequence",
        action="store_true",
        help=(
            "copy each prepared backend root once per repetition and replay "
            "the selected workflows sequentially, while reporting each "
            "workflow separately"
        ),
    )
    parser.add_argument(
        "--isolated-workflows",
        action="store_true",
        help=(
            "fork a private copy of each workflow's referenced starting "
            "branches and delete it after that workflow"
        ),
    )
    parser.add_argument(
        "--summarizer",
        type=Path,
        required=True,
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.shared_root_sequence and args.isolated_workflows:
        raise SystemExit(
            "--shared-root-sequence and --isolated-workflows are mutually "
            "exclusive"
        )
    base_states = {}
    for value in args.base_state:
        backend, separator, path = value.partition("=")
        if not separator or not backend or not path:
            raise SystemExit(f"invalid --base-state value: {value}")
        base_states[backend] = Path(path)
    traces = {
        path.name[:2]: WorkloadTrace.load(path)
        for path in sorted(args.traces_dir.glob("[0-9][0-9]-*.jsonl"))
    }
    selected = [
        key
        for key in sorted(traces)
        if (
            args.start <= int(key) <= args.end
            and int(key) not in set(args.exclude)
        )
    ]
    dependencies = (
        _LEGACY_DEPENDENCIES
        if args.dependency_mode == "legacy"
        else {key: () for key in traces}
    )
    missing = [
        dependency
        for key in selected
        for dependency in dependencies[key]
        if dependency not in traces
    ]
    if missing:
        raise SystemExit(f"missing setup traces: {sorted(set(missing))}")
    benchmark = PreparedRootBenchmark(
        snapshot_manifest=args.snapshot_manifest,
        base_states=base_states,
        work_root=args.work_root,
        output_root=args.output_root,
        repo_dir=args.repo_dir,
        dimensions=args.dimensions,
        embedding_model=args.embedding_model,
        repetitions=args.repetitions,
        max_shell_interrupt_seconds=args.max_shell_interrupt_seconds,
        backend_options={
            backend: {
                key: value
                for key, value in {
                    "qdrant_url": args.qdrant_url,
                    "qdrant_api_key": args.qdrant_api_key,
                    "qdrant_storage_dir": args.qdrant_storage_dir,
                    **(
                        {
                            "doltgres_dsn": args.doltgres_dsn,
                            "btrfs_root": args.btrfs_root,
                            "doltgres_data_dir": args.doltgres_data_dir,
                        }
                        if backend == "doltgres-qdrant-btrfs"
                        else {}
                    ),
                }.items()
                if value is not None
            }
            for backend in base_states
        },
        in_place_backends=args.in_place_backend,
    )
    completed = []
    if args.isolated_workflows:
        reports = {
            report.traces[0]: report
            for report in benchmark.run_isolated_workflows(
                [traces[key] for key in selected],
                setup_traces={
                    traces[key].trace_id: tuple(
                        traces[dependency]
                        for dependency in dependencies[key]
                    )
                    for key in selected
                },
            )
        }
    elif args.shared_root_sequence:
        reports = {
            report.traces[0]: report
            for report in benchmark.run_sequence(
                [traces[key] for key in selected]
            )
        }
    else:
        reports = {}
        for key in selected:
            trace = traces[key]
            reports[trace.trace_id] = benchmark.run_workflow(
                trace,
                setup_traces=[
                    traces[dependency]
                    for dependency in dependencies[key]
                ],
            )
    for key in selected:
        trace = traces[key]
        report = reports[trace.trace_id]
        output = args.output_root / trace.trace_id
        subprocess.run(
            [
                sys.executable,
                str(args.summarizer),
                str(output / "results.json"),
                "--output-dir",
                str(output / "summary"),
            ],
            check=True,
        )
        completed.append(
            {
                "workflow": key,
                "trace_id": trace.trace_id,
                "matched": report.matched,
                "shared_root_sequence": args.shared_root_sequence,
                "isolated_workflows": args.isolated_workflows,
                "results": str(output / "results.json"),
                "figure": str(output / "summary/backend_comparison.pdf"),
            }
        )
        print(json.dumps(completed[-1]), flush=True)
    (args.output_root / "workflow-index.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "snapshot_manifests": [
                    str(path.resolve()) for path in args.snapshot_manifest
                ],
                "backends": list(base_states),
                "repetitions": args.repetitions,
                "dependency_mode": args.dependency_mode,
                "excluded_workflows": sorted(set(args.exclude)),
                "max_shell_interrupt_seconds": (
                    args.max_shell_interrupt_seconds
                ),
                "workflows": completed,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return 0 if all(item["matched"] for item in completed) else 2


if __name__ == "__main__":
    raise SystemExit(main())
