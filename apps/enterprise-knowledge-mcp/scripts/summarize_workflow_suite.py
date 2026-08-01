#!/usr/bin/env python3
"""Aggregate independent workflow results with equal weight per workflow."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch


BACKENDS = (
    "chronos",
    "app-managed",
    "physical-clone",
    "doltgres-qdrant-btrfs",
)
LABELS = {
    "chronos": "Chronos",
    "app-managed": "App-managed",
    "physical-clone": "Physical clone",
    "doltgres-qdrant-btrfs": "Doltgres + Qdrant + Btrfs",
}
TICK_LABELS = {
    **LABELS,
    "app-managed": "App-\nmanaged",
    "physical-clone": "Physical\nclone",
    "doltgres-qdrant-btrfs": "Doltgres +\nQdrant + Btrfs",
}
COLORS = {
    "chronos": "#8f63c6",
    "app-managed": "#2f78b7",
    "physical-clone": "#31a354",
    "doltgres-qdrant-btrfs": "#d95f02",
}
OPERATIONS = (
    ("mcp:knowledge_checkout", "Branch\ncheckout"),
    ("mcp:knowledge_search", "Search"),
    ("mcp:knowledge_get_document", "Document\nfetch"),
    ("mcp:knowledge_write_artifact", "Artifact\nwrite"),
    ("mcp:knowledge_remember", "Memory\nwrite"),
    ("mcp:knowledge_index_workspace_file", "File\nindex"),
    ("mcp:knowledge_diff", "Branch\ndiff"),
    ("mcp:knowledge_merge", "Branch\nmerge"),
    ("mcp:knowledge_delete_branch", "Branch\ndelete"),
)


def configure_style() -> None:
    font = Path("/usr/share/fonts/opentype/linux-libertine/LinBiolinum_R.otf")
    if font.exists():
        fm.fontManager.addfont(str(font))
    plt.style.use("seaborn-v0_8-whitegrid")
    plt.rcParams.update(
        {
            "font.family": "Linux Biolinum O",
            "font.size": 15,
            "axes.labelsize": 17,
            "axes.titlesize": 18,
            "xtick.labelsize": 13,
            "ytick.labelsize": 13,
            "legend.fontsize": 13,
            "figure.dpi": 150,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    )


def median(values: list[float]) -> float:
    if not values:
        raise ValueError("cannot summarize an empty measurement")
    return float(statistics.median(values))


def mean(values: list[float]) -> float:
    if not values:
        raise ValueError("cannot average an empty measurement")
    return float(statistics.fmean(values))


def load_workflows(
    benchmark_root: Path,
) -> tuple[
    tuple[str, ...],
    dict[str, dict[str, dict[str, Any]]],
]:
    reports: dict[str, dict[str, dict[str, Any]]] = {}
    backends: tuple[str, ...] | None = None
    paths = sorted(benchmark_root.glob("[0-9][0-9]-*/results.json"))
    if not paths:
        raise FileNotFoundError(
            f"no workflow results found under {benchmark_root}"
        )
    for path in paths:
        report = json.loads(path.read_text(encoding="utf-8"))
        if not report.get("matched"):
            raise RuntimeError(
                f"workflow did not preserve equivalent state: {path.parent.name}"
            )
        workflow_backends = tuple(str(item) for item in report["backends"])
        if backends is None:
            backends = workflow_backends
        elif workflow_backends != backends:
            raise RuntimeError(
                f"inconsistent backend order in {path}"
            )
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for run in report["runs"]:
            if not run["replay"].get("succeeded"):
                raise RuntimeError(
                    "failed replay in "
                    f"{path.parent.name}: {run['backend']} "
                    f"repetition {run['repetition']}"
                )
            grouped[str(run["backend"])].append(run)
        if set(grouped) != set(workflow_backends):
            raise RuntimeError(f"incomplete backend results in {path}")
        reports[path.parent.name] = {
            backend: {
                "replay_ms": median(
                    [
                        float(run["replay"]["wall_time_ms"])
                        for run in grouped[backend]
                    ]
                ),
                "operations": {
                    operation: median(
                        [
                            float(
                                run["replay"]["latency_by_operation"][
                                    operation
                                ]["p50_ms"]
                            )
                            for run in grouped[backend]
                        ]
                    )
                    for operation in set.intersection(
                        *[
                            set(run["replay"]["latency_by_operation"])
                            for run in grouped[backend]
                        ]
                    )
                },
            }
            for backend in workflow_backends
        }
    assert backends is not None
    unsupported = set(backends) - set(BACKENDS)
    if unsupported:
        raise ValueError(f"unsupported backends: {sorted(unsupported)}")
    return backends, reports


def load_prepared_storage(
    benchmark_root: Path,
    backends: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    """Load one physical footprint measured before workflow replay.

    Live before/after directory sizes are not attributable to one workflow:
    database checkpoints, vector-segment compaction, and asynchronous branch
    reclamation can change them during replay.  The pipeline records a clean
    measurement after ingestion and hierarchy construction, before any
    workflow runs.  Use that measurement for the storage panel.
    """

    candidates = [benchmark_root / "ingestion-storage.json"]
    candidates.extend(
        sorted(benchmark_root.parent.glob("*/ingestion-storage.json"))
    )
    footprints: dict[str, dict[str, Any]] = {}
    sources: dict[str, Path] = {}
    for path in candidates:
        if not path.is_file():
            continue
        value = json.loads(path.read_text(encoding="utf-8"))
        backend = str(value["backend"])
        if backend not in backends or backend in footprints:
            continue
        storage = dict(value["storage"])
        total = storage.get("total_state_bytes")
        if not isinstance(total, int) or total <= 0:
            raise RuntimeError(f"invalid storage footprint in {path}")
        footprints[backend] = storage
        sources[backend] = path.resolve()
    missing = set(backends) - footprints.keys()
    if missing:
        raise FileNotFoundError(
            "missing post-ingestion storage measurements for "
            f"{sorted(missing)} under {benchmark_root.parent}"
        )
    branch_counts = {
        int(footprints[backend]["branches"]) for backend in backends
    }
    if len(branch_counts) != 1:
        raise RuntimeError(
            "storage footprints describe different branch hierarchies: "
            f"{branch_counts}"
        )
    for backend in backends:
        footprints[backend]["measurement_source"] = str(sources[backend])
    return footprints


def aggregate(
    backends: tuple[str, ...],
    workflows: dict[str, dict[str, dict[str, Any]]],
    prepared_storage: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    summary = []
    for backend in backends:
        replay_values = [
            float(values[backend]["replay_ms"])
            for values in workflows.values()
        ]
        summary.append(
            {
                "backend": backend,
                "workflows": len(workflows),
                "replay_ms_workflow_mean": mean(replay_values),
                "replay_ms_workflow_min": min(replay_values),
                "replay_ms_workflow_max": max(replay_values),
                "prepared_state_bytes": int(
                    prepared_storage[backend]["total_state_bytes"]
                ),
            }
        )

    operation_rows = []
    for operation, _ in OPERATIONS:
        eligible = [
            workflow
            for workflow, values in workflows.items()
            if all(
                operation in values[backend]["operations"]
                for backend in backends
            )
        ]
        if not eligible:
            continue
        for backend in backends:
            values = [
                float(workflows[workflow][backend]["operations"][operation])
                for workflow in eligible
            ]
            operation_rows.append(
                {
                    "backend": backend,
                    "operation": operation,
                    "workflows": len(eligible),
                    "p50_ms_workflow_mean": mean(values),
                    "p50_ms_workflow_min": min(values),
                    "p50_ms_workflow_max": max(values),
                }
            )
    return summary, operation_rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"no rows to write to {path}")
    with path.open("w", encoding="utf-8", newline="") as destination:
        writer = csv.DictWriter(destination, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def annotate_bars(
    axis: plt.Axes,
    bars: Any,
    values: list[float],
    *,
    suffix: str,
) -> None:
    for bar, value in zip(bars, values, strict=True):
        axis.annotate(
            f"{value:.1f}{suffix}",
            (bar.get_x() + bar.get_width() / 2, bar.get_height()),
            xytext=(0, 5),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=12,
        )


def plot(
    output: Path,
    summary: list[dict[str, Any]],
    operations: list[dict[str, Any]],
) -> None:
    configure_style()
    figure, axes = plt.subplots(
        1,
        3,
        figsize=(17.5, 4.6),
        gridspec_kw={"width_ratios": (1.0, 1.85, 1.0)},
    )
    backends = tuple(str(row["backend"]) for row in summary)
    x = np.arange(len(backends))
    colors = [COLORS[backend] for backend in backends]
    labels = [TICK_LABELS[backend] for backend in backends]

    replay_seconds = [
        float(row["replay_ms_workflow_mean"]) / 1000
        for row in summary
    ]
    bars = axes[0].bar(x, replay_seconds, color=colors, edgecolor="white")
    axes[0].set_title("(a) End-to-end workflow")
    axes[0].set_ylabel("Mean replay time (s)")
    axes[0].set_xticks(x, labels)
    axes[0].set_ylim(0, max(replay_seconds) * 1.18)
    annotate_bars(axes[0], bars, replay_seconds, suffix=" s")

    lookup = {
        (str(row["backend"]), str(row["operation"])): row
        for row in operations
    }
    operation_order = [
        (operation, label)
        for operation, label in OPERATIONS
        if all((backend, operation) in lookup for backend in backends)
    ]
    width = min(0.24, 0.72 / len(backends))
    operation_x = np.arange(len(operation_order))
    for offset, backend in enumerate(backends):
        values = [
            float(
                lookup[(backend, operation)]["p50_ms_workflow_mean"]
            )
            for operation, _ in operation_order
        ]
        axes[1].bar(
            operation_x
            + (offset - (len(backends) - 1) / 2) * width,
            values,
            width,
            color=COLORS[backend],
            edgecolor="white",
        )
    axes[1].set_title("(b) Operation latency")
    axes[1].set_ylabel("Mean latency (p50 ms)")
    axes[1].set_yscale("log")
    axes[1].set_xticks(
        operation_x,
        [label for _, label in operation_order],
        rotation=32,
        ha="right",
        rotation_mode="anchor",
    )
    axes[1].tick_params(axis="x", labelsize=11, pad=2)

    prepared_gib = [
        float(row["prepared_state_bytes"]) / (1024**3)
        for row in summary
    ]
    bars = axes[2].bar(x, prepared_gib, color=colors, edgecolor="white")
    axes[2].set_title("(c) Prepared-state footprint")
    axes[2].set_ylabel("Physical storage (GiB)")
    axes[2].set_xticks(x, labels)
    axes[2].set_ylim(0, max(prepared_gib) * 1.18)
    annotate_bars(axes[2], bars, prepared_gib, suffix=" GiB")

    for axis in axes:
        axis.grid(axis="x", visible=False)
        axis.set_axisbelow(True)
    figure.legend(
        handles=[
            Patch(facecolor=COLORS[backend], label=LABELS[backend])
            for backend in backends
        ],
        loc="upper center",
        bbox_to_anchor=(0.5, 1.02),
        frameon=False,
        ncol=len(backends),
    )
    figure.tight_layout(rect=(0, 0, 1, 0.90), w_pad=1.8)
    figure.savefig(output, bbox_inches="tight", dpi=300)
    plt.close(figure)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("benchmark_root", type=Path)
    parser.add_argument("--output-dir", type=Path)
    arguments = parser.parse_args()

    output_dir = (
        arguments.output_dir
        or arguments.benchmark_root / "summary"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    backends, workflows = load_workflows(arguments.benchmark_root)
    prepared_storage = load_prepared_storage(
        arguments.benchmark_root,
        backends,
    )
    summary, operations = aggregate(
        backends,
        workflows,
        prepared_storage,
    )
    write_csv(output_dir / "workflow_average.csv", summary)
    write_csv(output_dir / "operation_latency_average.csv", operations)
    plot(output_dir / "backend_comparison.pdf", summary, operations)
    (output_dir / "summary.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": str(arguments.benchmark_root.resolve()),
                "aggregation": (
                    "arithmetic mean of per-workflow medians; "
                    "each workflow has equal weight"
                ),
                "workflow_count": len(workflows),
                "workflows": sorted(workflows),
                "summary": summary,
                "operation_latency": operations,
                "storage_measurement": {
                    "definition": (
                        "physical bytes after fresh ingestion and hierarchy "
                        "construction, before workflow replay"
                    ),
                    "branch_count": next(
                        iter(
                            {
                                int(value["branches"])
                                for value in prepared_storage.values()
                            }
                        )
                    ),
                    "components": prepared_storage,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
