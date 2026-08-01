#!/usr/bin/env python3
"""Build workflow reports and figures from durable sequence checkpoints."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from chronos_enterprise_knowledge.prepared_benchmark import _compare_runs
from chronos_enterprise_knowledge.rollout_benchmark import (
    BenchmarkReport,
    BenchmarkRun,
)
from chronos_enterprise_knowledge.rollout_trace import WorkloadTrace


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkpoint-root", type=Path, required=True)
    parser.add_argument("--traces-dir", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--summarizer", type=Path, required=True)
    parser.add_argument("--backend", action="append", required=True)
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--end", type=int, default=13)
    parser.add_argument("--exclude", action="append", type=int, default=[])
    parser.add_argument("--snapshot", required=True)
    args = parser.parse_args()

    excluded = set(args.exclude)
    traces = [
        WorkloadTrace.load(path)
        for path in sorted(args.traces_dir.glob("[0-9][0-9]-*.jsonl"))
        if (
            args.start <= int(path.name[:2]) <= args.end
            and int(path.name[:2]) not in excluded
        )
    ]
    index = []
    for trace in traces:
        runs = []
        available_backends = []
        for backend in args.backend:
            checkpoint = (
                args.checkpoint_root
                / f"repeat-000-{backend}"
                / f"{trace.trace_id}.json"
            )
            if not checkpoint.is_file():
                continue
            runs.append(
                BenchmarkRun.from_dict(
                    json.loads(checkpoint.read_text(encoding="utf-8"))
                )
            )
            available_backends.append(backend)
        if not runs:
            raise FileNotFoundError(
                f"no requested checkpoint exists for {trace.trace_id}"
            )
        comparisons = _compare_runs(
            trace,
            runs,
            backends=available_backends,
            repetitions=1,
            require_result_match=False,
        )
        report = BenchmarkReport(
            snapshot=args.snapshot,
            embedding_cache="<forced-zero>",
            backends=tuple(available_backends),
            traces=(trace.trace_id,),
            require_result_match=False,
            runs=tuple(runs),
            comparisons=tuple(comparisons),
        )
        workflow_root = args.output_root / trace.trace_id
        results = report.write(workflow_root / "results.json")
        subprocess.run(
            [
                sys.executable,
                str(args.summarizer),
                str(results),
                "--output-dir",
                str(workflow_root / "summary"),
            ],
            check=True,
        )
        index.append(
            {
                "trace_id": trace.trace_id,
                "backends": available_backends,
                "matched": report.matched,
                "results": str(results.resolve()),
                "figure": str(
                    (
                        workflow_root / "summary/backend_comparison.pdf"
                    ).resolve()
                ),
            }
        )
    args.output_root.mkdir(parents=True, exist_ok=True)
    (args.output_root / "materialized-workflow-index.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "checkpoint_root": str(args.checkpoint_root.resolve()),
                "workflows": index,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps(index, indent=2))
    return 0 if all(value["matched"] for value in index) else 2


if __name__ == "__main__":
    raise SystemExit(main())
