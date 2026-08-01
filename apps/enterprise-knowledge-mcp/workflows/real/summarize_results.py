#!/usr/bin/env python3
"""Summarize and plot a cross-backend Codex rollout benchmark."""

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


def load_runs(path: Path) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    report = json.loads(path.read_text(encoding="utf-8"))
    if not report.get("matched"):
        raise RuntimeError("benchmark did not preserve equivalent logical state")
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for run in report["runs"]:
        if not run["replay"].get("succeeded"):
            raise RuntimeError(
                f"failed replay: {run['backend']} repetition {run['repetition']}"
            )
        grouped[str(run["backend"])].append(run)
    requested = tuple(str(backend) for backend in report["backends"])
    unsupported = set(requested) - set(BACKENDS)
    if unsupported:
        raise ValueError(f"unsupported backends: {sorted(unsupported)}")
    missing = set(requested) - grouped.keys()
    if missing:
        raise ValueError(f"results are missing backends: {sorted(missing)}")
    return report, grouped


def summarize(
    grouped: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    summary: list[dict[str, Any]] = []
    operations: list[dict[str, Any]] = []
    for backend in (candidate for candidate in BACKENDS if candidate in grouped):
        runs = grouped[backend]
        wall = [float(run["replay"]["wall_time_ms"]) for run in runs]
        seed = [float(run["seed"]["elapsed_ms"]) for run in runs]
        summary.append(
            {
                "backend": backend,
                "repetitions": len(runs),
                "seed_ms_median": median(seed),
                "replay_ms_median": median(wall),
                "replay_ms_min": min(wall),
                "replay_ms_max": max(wall),
            }
        )
        available_operations = set.intersection(
            *[
                set(run["replay"]["latency_by_operation"])
                for run in runs
            ]
        )
        for operation, _ in OPERATIONS:
            if operation not in available_operations:
                continue
            values = [
                float(run["replay"]["latency_by_operation"][operation]["p50_ms"])
                for run in runs
            ]
            operations.append(
                {
                    "backend": backend,
                    "operation": operation,
                    "p50_ms_median": median(values),
                    "p50_ms_min": min(values),
                    "p50_ms_max": max(values),
                }
            )
    return summary, operations


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as destination:
        writer = csv.DictWriter(destination, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def asymmetric_error(values: list[dict[str, Any]], prefix: str) -> np.ndarray:
    centers = np.array([float(row[f"{prefix}_median"]) for row in values])
    lows = np.array([float(row[f"{prefix}_min"]) for row in values])
    highs = np.array([float(row[f"{prefix}_max"]) for row in values])
    return np.vstack((centers - lows, highs - centers))


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
        2,
        figsize=(13.5, 4.6),
        gridspec_kw={"width_ratios": (1.0, 1.85)},
    )
    backends = tuple(str(row["backend"]) for row in summary)
    x = np.arange(len(backends))
    colors = [COLORS[backend] for backend in backends]
    labels = [TICK_LABELS[backend] for backend in backends]

    wall_seconds = [float(row["replay_ms_median"]) / 1000 for row in summary]
    wall_error = asymmetric_error(summary, "replay_ms") / 1000
    bars = axes[0].bar(
        x,
        wall_seconds,
        color=colors,
        yerr=wall_error,
        capsize=4,
        edgecolor="white",
    )
    axes[0].set_title("(a) End-to-end workflow")
    axes[0].set_ylabel("Replay time (s)")
    axes[0].set_xticks(x, labels)
    axes[0].set_ylim(0, max(wall_seconds) * 1.18)
    annotate_bars(axes[0], bars, wall_seconds, suffix=" s")

    width = min(0.24, 0.72 / len(backends))
    operation_lookup = {
        (row["backend"], row["operation"]): row for row in operations
    }
    operation_order = [
        (operation, label)
        for operation, label in OPERATIONS
        if all((backend, operation) in operation_lookup for backend in backends)
    ]
    if not operation_order:
        raise ValueError("no common measured operation to plot")
    operation_x = np.arange(len(operation_order))
    for offset, backend in enumerate(backends):
        values = [
            float(operation_lookup[(backend, operation)]["p50_ms_median"])
            for operation, _ in operation_order
        ]
        axes[1].bar(
            operation_x
            + (offset - (len(backends) - 1) / 2) * width,
            values,
            width,
            label=LABELS[backend],
            color=COLORS[backend],
            edgecolor="white",
        )
    axes[1].set_title("(b) Operation latency")
    axes[1].set_ylabel("Latency (p50 ms)")
    axes[1].set_yscale("log")
    axes[1].set_xticks(
        operation_x,
        [label for _, label in operation_order],
        rotation=32,
        ha="right",
        rotation_mode="anchor",
    )
    axes[1].tick_params(axis="x", labelsize=11, pad=2)

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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("results", type=Path)
    parser.add_argument("--output-dir", type=Path)
    arguments = parser.parse_args()

    output_dir = arguments.output_dir or arguments.results.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    report, grouped = load_runs(arguments.results)
    summary, operations = summarize(grouped)
    write_csv(output_dir / "summary.csv", summary)
    write_csv(output_dir / "operation_latency.csv", operations)
    plot(output_dir / "backend_comparison.pdf", summary, operations)
    (output_dir / "summary.json").write_text(
        json.dumps(
            {
                "source_results": str(arguments.results.resolve()),
                "matched": report["matched"],
                "summary": summary,
                "operation_latency": operations,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
