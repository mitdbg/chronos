#!/usr/bin/env python3
"""Merge independently replayed backends into per-workflow reports."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

from chronos_enterprise_knowledge.prepared_benchmark import _compare_runs
from chronos_enterprise_knowledge.rollout_benchmark import (
    BenchmarkReport,
    BenchmarkRun,
)
from chronos_enterprise_knowledge.rollout_trace import WorkloadTrace

_BACKEND_ORDER = (
    "chronos",
    "doltgres-qdrant-btrfs",
    "app-managed",
    "physical-clone",
)


def _reports(root: Path) -> dict[str, Path]:
    return {
        path.parent.name: path
        for path in sorted(root.glob("[0-9][0-9]-*/results.json"))
    }


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_run(run: BenchmarkRun) -> BenchmarkRun:
    baseline = dict(run.baseline_digests)
    # Sequence position describes runner order, not logical starting state.
    # A suite that intentionally omits a workflow can therefore be compared
    # with one captured from the same prepared root in the original order.
    baseline.pop("sequence_position", None)
    return BenchmarkRun(
        trace_id=run.trace_id,
        backend=run.backend,
        repetition=run.repetition,
        state_dir=run.state_dir,
        seed=dict(run.seed),
        baseline_digests=baseline,
        replay=dict(run.replay),
        final_digests=dict(run.final_digests),
        storage=dict(run.storage),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--primary-root", type=Path, required=True)
    parser.add_argument("--additional-root", type=Path, required=True)
    parser.add_argument("--traces-dir", type=Path, required=True)
    parser.add_argument("--summarizer", type=Path, required=True)
    parser.add_argument("--output-root", type=Path)
    args = parser.parse_args()

    primary_paths = _reports(args.primary_root)
    additional_paths = _reports(args.additional_root)
    workflows = sorted(primary_paths.keys() & additional_paths.keys())
    if not workflows:
        raise SystemExit("the result roots have no workflows in common")
    output_root = args.output_root or args.primary_root
    output_root.mkdir(parents=True, exist_ok=True)
    traces = {
        path.stem: WorkloadTrace.load(path)
        for path in args.traces_dir.glob("[0-9][0-9]-*.jsonl")
    }
    index = []
    for workflow in workflows:
        primary_path = primary_paths[workflow]
        additional_path = additional_paths[workflow]
        primary = _load(primary_path)
        additional = _load(additional_path)
        trace_ids = {
            *(str(value) for value in primary["traces"]),
            *(str(value) for value in additional["traces"]),
        }
        if trace_ids != {workflow}:
            raise RuntimeError(
                f"inconsistent trace identity for {workflow}: {trace_ids}"
            )
        runs_by_key = {}
        for value in (*primary["runs"], *additional["runs"]):
            run = _normalize_run(BenchmarkRun.from_dict(value))
            key = (run.backend, run.repetition)
            if key in runs_by_key:
                raise RuntimeError(f"duplicate run for {workflow}: {key}")
            runs_by_key[key] = run
        backends = tuple(
            backend
            for backend in _BACKEND_ORDER
            if any(key[0] == backend for key in runs_by_key)
        )
        unexpected = {key[0] for key in runs_by_key} - set(backends)
        if unexpected:
            raise RuntimeError(
                f"unsupported backends for {workflow}: {sorted(unexpected)}"
            )
        repetitions = {
            repetition for _, repetition in runs_by_key
        }
        if repetitions != set(range(len(repetitions))):
            raise RuntimeError(
                f"non-contiguous repetitions for {workflow}: "
                f"{sorted(repetitions)}"
            )
        for repetition in repetitions:
            missing = [
                backend
                for backend in backends
                if (backend, repetition) not in runs_by_key
            ]
            if missing:
                raise RuntimeError(
                    f"missing runs for {workflow}, repetition "
                    f"{repetition}: {missing}"
                )
        runs = tuple(
            runs_by_key[(backend, repetition)]
            for repetition in sorted(repetitions)
            for backend in backends
        )
        trace = traces[workflow]
        comparisons = _compare_runs(
            trace,
            runs,
            backends=backends,
            repetitions=len(repetitions),
            # Search uses backend-native ranking. Equal-score boundary ties can
            # produce a different ordering while the logical post-workflow
            # state remains identical, so equivalence is enforced on the
            # baseline and final state rather than exact top-k identities.
            require_result_match=False,
        )
        report = BenchmarkReport(
            snapshot=str(primary["snapshot"]),
            embedding_cache=str(primary["embedding_cache"]),
            backends=backends,
            traces=(workflow,),
            require_result_match=False,
            runs=runs,
            comparisons=tuple(comparisons),
        )
        destination = output_root / workflow
        destination.mkdir(parents=True, exist_ok=True)
        if destination.resolve() == primary_path.parent.resolve():
            shutil.copy2(
                primary_path,
                destination / "results.chronos-app-managed.json",
            )
        shutil.copy2(
            additional_path,
            destination / "results.doltgres-qdrant-btrfs.json",
        )
        results_path = report.write(destination / "results.json")
        subprocess.run(
            [
                sys.executable,
                str(args.summarizer),
                str(results_path),
                "--output-dir",
                str(destination / "summary"),
            ],
            check=True,
        )
        index.append(
            {
                "trace_id": workflow,
                "backends": list(backends),
                "matched": report.matched,
                "results": str(results_path.resolve()),
                "figure": str(
                    (
                        destination / "summary/backend_comparison.pdf"
                    ).resolve()
                ),
            }
        )
    (output_root / "merged-workflow-index.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "primary_root": str(args.primary_root.resolve()),
                "additional_root": str(args.additional_root.resolve()),
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
