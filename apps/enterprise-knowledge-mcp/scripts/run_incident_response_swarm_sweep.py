#!/usr/bin/env python3
"""Run the incident-response swarm over a process-worker-count sweep.

The three systems are run sequentially.  Before every trial the shared
publication branch is deleted and recreated from ``main`` so a worker count
never inherits packages produced by an earlier trial.  The underlying Docker
services and their configuration are supplied by the caller and are kept
identical for the two native-store variants.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


WORKER_COUNTS = (1, 2, 4, 8, 16, 32, 64, 128)
SYSTEMS = ("chronos", "native-branching", "native-branching-lock")
WORKER_THREAD_ENV = {
    # Each replay is an OS process.  Keep numerical runtimes from creating a
    # second, hidden thread fan-out inside every process; the same limits are
    # applied to every backend in the sweep.
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "NUMEXPR_NUM_THREADS": "1",
    "ORT_NUM_THREADS": "1",
    "TOKENIZERS_PARALLELISM": "false",
    "CHRONOS_BM25_THREADS": "1",
    # One bounded transport pool per worker; the same setting is inherited by
    # Chronos and native-branching clients.
    "CHRONOS_QDRANT_POOL_SIZE": "1",
    # Reclaim deleted Chronos branches on the worker's existing native
    # connection instead of creating one background GC connection per worker.
    "CHRONOS_INTERVAL_GC_SYNCHRONOUS": "1",
}


def _parse_merge_retries(value: str) -> int:
    """Parse a finite retry count or the explicit unlimited sentinel."""

    normalized = str(value).strip().casefold()
    if normalized in {"unlimited", "infinite", "inf", "-1"}:
        return -1
    try:
        retries = int(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "merge retries must be a non-negative integer or 'unlimited'"
        ) from exc
    if retries < 0:
        raise argparse.ArgumentTypeError(
            "merge retries must be a non-negative integer or 'unlimited'"
        )
    return retries


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--traces-dir", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--repo-dir", type=Path, required=True)
    parser.add_argument("--chronos-state-dir", type=Path, required=True)
    parser.add_argument("--native-state-dir", type=Path, required=True)
    parser.add_argument("--chronos-qdrant-url", required=True)
    parser.add_argument("--native-qdrant-url", required=True)
    parser.add_argument("--chronos-qdrant-storage-dir", type=Path, required=True)
    parser.add_argument("--native-qdrant-storage-dir", type=Path, required=True)
    parser.add_argument("--chronos-postgres-dsn", required=True)
    parser.add_argument("--chronos-postgres-data-dir", type=Path, required=True)
    parser.add_argument("--doltgres-dsn", required=True)
    parser.add_argument("--doltgres-data-dir", type=Path, required=True)
    parser.add_argument("--btrfs-root", type=Path, required=True)
    parser.add_argument("--dimensions", type=int, default=384)
    parser.add_argument(
        "--merge-retries",
        type=_parse_merge_retries,
        default=11,
        help=(
            "Retry transient merge races; use -1 or 'unlimited' to retry "
            "until success."
        ),
    )
    parser.add_argument(
        "--no-session-epochs",
        action="store_true",
        help=(
            "Disable the per-worker Chronos session-epoch coordinator. "
            "Chronos keeps its native branch guard while avoiding one "
            "PostgreSQL control connection and heartbeat thread per worker."
        ),
    )
    parser.add_argument("--target-branch", default="team/site-reliability")
    parser.add_argument(
        "--workers",
        type=int,
        nargs="*",
        default=list(WORKER_COUNTS),
        help="Worker counts to run; defaults to 1,2,4,8,16,32,64,128.",
    )
    parser.add_argument(
        "--systems",
        nargs="+",
        choices=SYSTEMS,
        default=list(SYSTEMS),
        help="Systems to rerun; existing results for other systems are retained.",
    )
    parser.add_argument(
        "--startup-batch-size",
        type=int,
        default=4,
        help=(
            "Number of worker processes admitted per backend-initialization "
            "batch (startup only; replay concurrency is unchanged)."
        ),
    )
    return parser.parse_args()


def _backend_config(args: argparse.Namespace, system: str) -> dict[str, Any]:
    if system == "chronos":
        return {
            "backend": "chronos",
            "state_dir": args.chronos_state_dir,
            "qdrant_url": args.chronos_qdrant_url,
            "qdrant_storage_dir": args.chronos_qdrant_storage_dir,
            "chronos_postgres_dsn": args.chronos_postgres_dsn,
            "chronos_postgres_data_dir": args.chronos_postgres_data_dir,
            "chronos_enable_session_epochs": not bool(args.no_session_epochs),
        }
    return {
        "backend": "doltgres-qdrant-btrfs",
        "state_dir": args.native_state_dir,
        "qdrant_url": args.native_qdrant_url,
        "qdrant_storage_dir": args.native_qdrant_storage_dir,
        "doltgres_dsn": args.doltgres_dsn,
        "doltgres_data_dir": args.doltgres_data_dir,
        "btrfs_root": args.btrfs_root,
    }


def _reset_target(args: argparse.Namespace, system: str) -> None:
    """Reset the publication branch while preserving the prepared main state."""

    # Imports are delayed so the sweep can still emit its manifest when a
    # service is unavailable; the actual trial then fails explicitly.
    from chronos_enterprise_knowledge.backends.factory import (
        create_knowledge_backend,
    )

    config = _backend_config(args, system)
    backend_name = str(config.pop("backend"))
    state_dir = Path(config.pop("state_dir"))
    enable_session_epochs = bool(config.pop("chronos_enable_session_epochs", True))
    backend = create_knowledge_backend(
        backend_name,
        state_dir=state_dir,
        vector_dimensions=args.dimensions,
        **{
            key: str(value)
            for key, value in config.items()
            if key != "chronos_enable_session_epochs"
        },
        chronos_enable_session_epochs=enable_session_epochs,
    )
    try:
        # Trials are serialized by this sweep, so no merge can be active when
        # a publication branch is reset.  Remove a reservation left by an
        # interrupted earlier trial before recreating the branch.  Without
        # this cleanup, Chronos treats the orphaned reservation as an active
        # merge and an unlimited-retry replay can spin forever.
        if backend_name == "chronos":
            relational = getattr(backend, "relational", None)
            db = getattr(relational, "db", None)
            if db is not None:
                db.execute(
                    "DELETE FROM _chronos_branch_transaction_commits "
                    "WHERE target_branch_id = :target_branch",
                    {"target_branch": args.target_branch},
                )
                db.commit()
        branches = set(backend.list_branches())
        if args.target_branch in branches:
            backend.delete_branch(args.target_branch)
        backend.create_branch(
            args.target_branch,
            "main",
            {"purpose": "incident-response-swarm-worker-sweep"},
        )
    finally:
        backend.close()


def _runner_command(
    args: argparse.Namespace,
    system: str,
    workers: int,
    output: Path,
) -> list[str]:
    config = _backend_config(args, system)
    backend_name = str(config["backend"])
    command = [
        sys.executable,
        str(Path(__file__).with_name("run_incident_response_swarm.py")),
        "--traces-dir",
        str(args.traces_dir.resolve()),
        "--inputs",
        str(args.inputs.resolve()),
        "--state-dir",
        f"{backend_name}={Path(config['state_dir']).resolve()}",
        "--backend",
        backend_name,
        "--qdrant-url",
        str(config["qdrant_url"]),
        "--qdrant-storage-dir",
        str(Path(config["qdrant_storage_dir"]).resolve()),
        "--workers",
        str(workers),
        "--allow-trace-reuse",
        "--dimensions",
        str(args.dimensions),
        "--repo-dir",
        str(args.repo_dir.resolve()),
        "--target-branch",
        args.target_branch,
        "--merge-retries",
        str(args.merge_retries),
        "--startup-batch-size",
        str(args.startup_batch_size),
        # The captured incident traces are trusted Codex runs and include
        # shell/patch steps in addition to MCP calls.  Replaying the complete
        # agent execution keeps every system on the same workload; omitting
        # this flag would deterministically fail those traces before merge.
        "--allow-shell",
        "--output",
        str(output.resolve()),
    ]
    if system == "chronos":
        command.extend(
            [
                "--chronos-postgres-dsn",
                args.chronos_postgres_dsn,
                "--chronos-postgres-data-dir",
                str(args.chronos_postgres_data_dir.resolve()),
            ]
        )
        if args.no_session_epochs:
            command.append("--no-session-epochs")
    else:
        command.extend(
            [
                "--doltgres-dsn",
                args.doltgres_dsn,
                "--doltgres-data-dir",
                str(args.doltgres_data_dir.resolve()),
                "--btrfs-root",
                str(args.btrfs_root.resolve()),
            ]
        )
        if system == "native-branching-lock":
            command.append("--native-big-lock")
    return command


def _result_row(
    system: str,
    workers: int,
    output: Path,
    returncode: int,
) -> dict[str, Any]:
    result_path = output / "results.json"
    row: dict[str, Any] = {
        "system": system,
        "workers": workers,
        "output": str(output),
        "runner_returncode": returncode,
        "status": "launch-error",
        "successful_workers": 0,
        "elapsed_seconds": None,
        "throughput_bundles_per_second": 0.0,
        "verifier_violations": None,
        "incomplete_bundles": None,
    }
    try:
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        result = (payload.get("results") or [{}])[0]
        row.update(
            {
                "status": result.get("status", "unknown"),
                "successful_workers": int(result.get("successful_workers", 0)),
                "elapsed_seconds": result.get("elapsed_seconds"),
                "throughput_bundles_per_second": result.get(
                    "throughput_bundles_per_second", 0.0
                ),
                "verifier_violations": int(
                    (result.get("verifier") or {}).get("violation_count", 0)
                ),
                "incomplete_bundles": len(
                    (result.get("final_state") or {}).get("incomplete_bundles", [])
                ),
            }
        )
    except (OSError, ValueError, TypeError, IndexError, AttributeError) as exc:
        row["parse_error"] = f"{type(exc).__name__}: {exc}"
    return row


def _write_summary(root: Path, rows: list[dict[str, Any]]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "sweep-results.json").write_text(
        json.dumps(rows, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    fields = [
        "system",
        "workers",
        "status",
        "successful_workers",
        "elapsed_seconds",
        "throughput_bundles_per_second",
        "verifier_violations",
        "incomplete_bundles",
        "runner_returncode",
        "output",
    ]
    with (root / "sweep-results.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows({key: row.get(key) for key in fields} for row in rows)

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.font_manager as fm
        import matplotlib.pyplot as plt
    except ImportError as exc:
        (root / "plot-error.txt").write_text(str(exc) + "\n", encoding="utf-8")
        return

    summary = root / "summary"
    summary.mkdir(exist_ok=True)
    biolinum = Path("/usr/share/fonts/opentype/linux-libertine/LinBiolinum_R.otf")
    if biolinum.exists():
        fm.fontManager.addfont(str(biolinum))
    plt.style.use("seaborn-v0_8-whitegrid")
    plt.rcParams.update(
        {
            "font.family": "Linux Biolinum O",
            "font.size": 9,
            "axes.labelsize": 10,
            "xtick.labelsize": 8.5,
            "ytick.labelsize": 8.5,
            "legend.fontsize": 8.0,
            "lines.linewidth": 1.35,
            "lines.markersize": 4.5,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    )
    colors = {
        "chronos": "#9467bd",
        "native-branching": "#d95f02",
        "native-branching-lock": "#4c78a8",
    }
    labels = {
        "chronos": "Chronos",
        "native-branching": "Native",
        "native-branching-lock": "Native + lock",
    }
    # Render completion reliability and elapsed runtime as separate panels;
    # throughput remains available in the machine-readable sweep results.
    from matplotlib.ticker import PercentFormatter

    fig, axis = plt.subplots(figsize=(2.28, 1.95))
    for system in SYSTEMS:
        values = sorted(
            (row for row in rows if row["system"] == system),
            key=lambda row: int(row["workers"]),
        )
        if not values:
            continue
        x = [int(row["workers"]) for row in values]
        success = [
            float(row.get("successful_workers") or 0) / max(1, int(row["workers"]))
            for row in values
        ]
        style = {
            "color": colors[system],
            "marker": "o",
            "markersize": 4.5,
            "linewidth": 1.35,
            "label": labels[system],
        }
        axis.plot(x, success, **style)
    axis.set_ylabel("Workflow success rate")
    axis.set_xlabel("Concurrent Workflows")
    axis.set_ylim(-0.03, 1.03)
    axis.yaxis.set_major_formatter(PercentFormatter(1.0))
    axis.set_xscale("log", base=2)
    axis.set_xticks(list(WORKER_COUNTS))
    axis.set_xticklabels([str(value) for value in WORKER_COUNTS])
    axis.grid(axis="y", color="#d0d0d0", linewidth=0.45)
    axis.grid(axis="x", visible=False)
    axis.set_axisbelow(True)
    fig.subplots_adjust(left=0.21, right=0.99, bottom=0.23, top=0.90)
    fig.savefig(summary / "worker_sweep.pdf")
    fig.savefig(summary / "worker_sweep.png", dpi=180)
    plt.close(fig)

    runtime_fig, runtime_axis = plt.subplots(figsize=(2.28, 1.95))
    for system in SYSTEMS:
        values = sorted(
            (row for row in rows if row["system"] == system),
            key=lambda row: int(row["workers"]),
        )
        values = [
            row
            for row in values
            if row.get("elapsed_seconds") not in (None, "", "None")
        ]
        if not values:
            continue
        x = [int(row["workers"]) for row in values]
        elapsed = [float(row["elapsed_seconds"]) for row in values]
        style = {
            "color": colors[system],
            "marker": "o",
            "markersize": 4.5,
            "linewidth": 1.35,
            "label": labels[system],
        }
        runtime_axis.plot(x, elapsed, **style)
    runtime_axis.set_ylabel("Runtime (s)")
    runtime_axis.set_xlabel("Concurrent Workflows")
    runtime_axis.set_yscale("log")
    runtime_axis.set_xscale("log", base=2)
    runtime_axis.set_xticks(list(WORKER_COUNTS))
    runtime_axis.set_xticklabels([str(value) for value in WORKER_COUNTS])
    runtime_axis.grid(axis="y", color="#d0d0d0", linewidth=0.45)
    runtime_axis.grid(axis="x", visible=False)
    runtime_axis.set_axisbelow(True)
    runtime_fig.subplots_adjust(left=0.21, right=0.99, bottom=0.23, top=0.90)
    runtime_fig.savefig(summary / "worker_runtime.pdf")
    runtime_fig.savefig(summary / "worker_runtime.png", dpi=180)
    plt.close(runtime_fig)

def main() -> int:
    args = _parse_args()
    args.output_root = args.output_root.expanduser().resolve()
    args.output_root.mkdir(parents=True, exist_ok=True)
    workers = tuple(sorted(set(int(value) for value in args.workers)))
    systems = tuple(dict.fromkeys(args.systems))
    if not workers or any(value < 1 or value > 128 for value in workers):
        raise SystemExit("worker counts must be between 1 and 128")
    if args.startup_batch_size < 1:
        raise SystemExit("--startup-batch-size must be positive")
    manifest = {
        "experiment": "incident-response-swarm-worker-sweep-v1",
        "systems": list(SYSTEMS),
        "selected_systems": list(systems),
        "workers": list(workers),
        "startup_batch_size": int(args.startup_batch_size),
        "allow_shell": True,
        "replay_llm_latency": True,
        "merge_retries": (
            "unlimited" if args.merge_retries == -1 else int(args.merge_retries)
        ),
        "chronos_session_epochs": not bool(args.no_session_epochs),
        "worker_thread_limits": dict(WORKER_THREAD_ENV),
        "qdrant_client_pool_size": int(
            WORKER_THREAD_ENV["CHRONOS_QDRANT_POOL_SIZE"]
        ),
        "chronos_interval_gc": "synchronous",
        "target_branch": args.target_branch,
        "traces_dir": str(args.traces_dir.resolve()),
        "inputs": str(args.inputs.resolve()),
        "service_configuration": {
            "chronos_qdrant_url": args.chronos_qdrant_url,
            "native_qdrant_url": args.native_qdrant_url,
            "qdrant_image": "qdrant/qdrant:v1.18.2",
            "qdrant_memory": "8g",
            "qdrant_max_workers": 8,
        },
        "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (args.output_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    rows: list[dict[str, Any]] = []
    prior_summary = args.output_root / "sweep-results.json"
    rerun_keys = {(system, worker_count) for system in systems for worker_count in workers}
    if prior_summary.is_file():
        try:
            prior_rows = json.loads(prior_summary.read_text(encoding="utf-8"))
            if isinstance(prior_rows, list):
                rows = [
                    row
                    for row in prior_rows
                    if isinstance(row, dict)
                    and (
                        str(row.get("system")),
                        int(row.get("workers", -1)),
                    ) not in rerun_keys
                ]
        except (OSError, ValueError, TypeError):
            rows = []
    # A previous interrupted sweep may have written per-trial results before
    # its summary was flushed.  Preserve those completed trials when rerunning
    # only failed worker counts; never reuse a directory whose worker count is
    # in the explicit rerun set.
    known_keys = {
        (str(row.get("system")), int(row.get("workers", -1)))
        for row in rows
        if isinstance(row, dict)
    }
    for system in systems:
        system_dir = args.output_root / system
        if not system_dir.is_dir():
            continue
        for output in sorted(system_dir.glob("workers-*")):
            try:
                worker_count = int(output.name.rsplit("-", 1)[1])
            except (IndexError, ValueError):
                continue
            key = (system, worker_count)
            if worker_count in workers or key in known_keys:
                continue
            if (output / "results.json").is_file():
                rows.append(_result_row(system, worker_count, output, 0))
                known_keys.add(key)
    log_path = args.output_root / "sweep.log"
    with log_path.open("a", encoding="utf-8", buffering=1) as log:
        for system in systems:
            for worker_count in workers:
                output = args.output_root / system / f"workers-{worker_count:03d}"
                output.mkdir(parents=True, exist_ok=True)
                stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                log.write(f"{stamp} start system={system} workers={worker_count}\n")
                try:
                    _reset_target(args, "chronos" if system == "chronos" else "native-branching")
                    command = _runner_command(args, system, worker_count, output)
                    env = os.environ.copy()
                    env["CHRONOSFS_DISABLE_METADATA_CACHE"] = "1"
                    env.update(WORKER_THREAD_ENV)
                    env["PYTHONPATH"] = os.pathsep.join(
                        [
                            str(args.repo_dir / "apps/enterprise-knowledge-mcp/src"),
                            str(args.repo_dir / "packages/chronos-core/src"),
                            env.get("PYTHONPATH", ""),
                        ]
                    )
                    with (output / "run.log").open("w", encoding="utf-8") as stream:
                        completed = subprocess.run(
                            command,
                            cwd=args.repo_dir,
                            env=env,
                            stdout=stream,
                            stderr=subprocess.STDOUT,
                            check=False,
                        )
                    returncode = int(completed.returncode)
                except BaseException as exc:
                    returncode = 125
                    (output / "launcher-error.txt").write_text(
                        f"{type(exc).__name__}: {exc}\n", encoding="utf-8"
                    )
                row = _result_row(system, worker_count, output, returncode)
                rows.append(row)
                _write_summary(args.output_root, rows)
                log.write(
                    f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} "
                    f"finish system={system} workers={worker_count} "
                    f"status={row['status']} successful={row['successful_workers']} "
                    f"elapsed={row['elapsed_seconds']}\n"
                )
    manifest["finished_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    (args.output_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    return 0 if all(row["status"] == "ok" for row in rows) else 2


if __name__ == "__main__":
    raise SystemExit(main())
