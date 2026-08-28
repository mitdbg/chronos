#!/usr/bin/env python3
"""Aggregate independent workflow results with equal weight per workflow."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import textwrap
from collections import defaultdict
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch

from chronos_enterprise_knowledge.rollout_trace import WorkloadTrace


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
MODEL_OPERATION = "model:inference"
MODEL_OPERATION_LABEL = "Model\ninference"


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


def summarize_run_storage(
    runs: list[dict[str, Any]],
    *,
    workflow: str,
    backend: str,
) -> dict[str, Any]:
    """Summarize the storage samples captured around successful replay."""

    samples = []
    for run in runs:
        storage = run.get("storage")
        if not isinstance(storage, dict):
            raise RuntimeError(
                f"missing storage samples for {workflow}: {backend}"
            )
        baseline = storage.get("baseline")
        final = storage.get("final")
        if not isinstance(baseline, dict) or not isinstance(final, dict):
            raise RuntimeError(
                f"incomplete storage samples for {workflow}: {backend}"
            )
        baseline_bytes = baseline.get("total_state_bytes")
        final_bytes = final.get("total_state_bytes")
        for name, value in (
            ("baseline.total_state_bytes", baseline_bytes),
            ("final.total_state_bytes", final_bytes),
        ):
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or value < 0
            ):
                raise RuntimeError(
                    f"invalid {name} for {workflow}: {backend}"
                )
        scope = storage.get("measurement_scope")
        attributable = storage.get("attributable_to_workflow")
        if not isinstance(scope, str) or not scope:
            raise RuntimeError(
                f"missing storage measurement scope for {workflow}: {backend}"
            )
        if not isinstance(attributable, bool):
            raise RuntimeError(
                "missing storage attribution flag for "
                f"{workflow}: {backend}"
            )
        samples.append(
            {
                "baseline_bytes": float(baseline_bytes),
                "final_bytes": float(final_bytes),
                "change_bytes": float(final_bytes) - float(baseline_bytes),
                "measurement_scope": scope,
                "attributable_to_workflow": attributable,
            }
        )

    scopes = {str(sample["measurement_scope"]) for sample in samples}
    attribution = {
        bool(sample["attributable_to_workflow"]) for sample in samples
    }
    if len(scopes) != 1 or len(attribution) != 1:
        raise RuntimeError(
            f"inconsistent storage metadata for {workflow}: {backend}"
        )
    baseline_bytes = median(
        [float(sample["baseline_bytes"]) for sample in samples]
    )
    final_bytes = median(
        [float(sample["final_bytes"]) for sample in samples]
    )
    change_bytes = median(
        [float(sample["change_bytes"]) for sample in samples]
    )
    return {
        "pre_workflow_state_bytes": baseline_bytes,
        "post_workflow_state_bytes": final_bytes,
        "observed_live_footprint_change_bytes": change_bytes,
        "measurement_scope": scopes.pop(),
        "attributable_to_workflow": attribution.pop(),
    }


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
        workflow = path.parent.name
        reports[workflow] = {}
        for backend in workflow_backends:
            timing_samples = [
                run["replay"].get("store_timing_ms")
                for run in grouped[backend]
            ]
            if any(not isinstance(sample, dict) for sample in timing_samples):
                raise RuntimeError(
                    "workflow replay lacks store timing instrumentation: "
                    f"{workflow}: {backend}"
                )
            reports[workflow][backend] = {
                "replay_ms": median(
                    [
                        float(run["replay"]["wall_time_ms"])
                        for run in grouped[backend]
                    ]
                ),
                "store_timing_ms": {
                    category: median(
                        [
                            float(
                                (run["replay"].get("store_timing_ms") or {})
                                .get(category, 0.0)
                            )
                            for run in grouped[backend]
                        ]
                    )
                    for category in (
                        "relational_db",
                        "vector_db",
                        "filesystem",
                        "others",
                    )
                },
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
                "storage": summarize_run_storage(
                    grouped[backend],
                    workflow=workflow,
                    backend=backend,
                ),
            }
    assert backends is not None
    unsupported = set(backends) - set(BACKENDS)
    if unsupported:
        raise ValueError(f"unsupported backends: {sorted(unsupported)}")
    return backends, reports


def load_llm_timings(
    traces_dir: Path | None,
    workflows: dict[str, dict[str, dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    """Load model-response timing from the exact captured workflow traces.

    Replay timing is measured by the backend and excludes model execution.
    The trace timing is therefore kept as a separate input and never folded
    into any backend's measured operation latency.  This prevents the model
    from being counted as a backend advantage or disadvantage.
    """

    if traces_dir is None:
        return {}
    timings: dict[str, dict[str, Any]] = {}
    for workflow in sorted(workflows):
        path = traces_dir / f"{workflow}.jsonl"
        if not path.is_file():
            raise FileNotFoundError(
                f"missing trace for {workflow}: {path}"
            )
        trace = WorkloadTrace.load(path)
        timing = trace.metadata.get("llm_timing")
        if not isinstance(timing, dict):
            raise RuntimeError(
                f"trace {path} does not contain llm_timing metadata"
            )
        total_ms = float(timing.get("total_ms") or 0.0)
        p50_ms = float(timing.get("p50_ms") or 0.0)
        call_count = int(timing.get("call_count") or 0)
        if total_ms < 0 or p50_ms < 0 or call_count < 0:
            raise RuntimeError(f"invalid llm_timing in {path}")
        timings[workflow] = {
            "total_ms": total_ms,
            "p50_ms": p50_ms,
            "call_count": call_count,
            "measurement": str(timing.get("measurement") or ""),
            "source": str(path.resolve()),
        }
    return timings


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
    # Branch-row counts are backend-specific physical metadata.  A relational
    # overlay, a native multi-store implementation, and an application-owned
    # catalog need not expose the same number of rows even when their replay
    # seeds describe the same logical branch set.  Keep each measurement
    # instead of rejecting an otherwise comparable cross-backend run.
    for backend in backends:
        branches = footprints[backend].get("branches")
        if not isinstance(branches, int) or branches <= 0:
            raise RuntimeError(
                f"invalid branch count for {backend}: {branches!r}"
            )
    for backend in backends:
        footprints[backend]["measurement_source"] = str(sources[backend])
    return footprints


def aggregate(
    backends: tuple[str, ...],
    workflows: dict[str, dict[str, dict[str, Any]]],
    prepared_storage: dict[str, dict[str, Any]],
    llm_timings: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    llm_timings = llm_timings or {}
    summary = []
    for backend in backends:
        replay_values = [
            float(values[backend]["replay_ms"])
            for values in workflows.values()
        ]
        row = {
            "backend": backend,
            "workflows": len(workflows),
            "replay_ms_workflow_mean": mean(replay_values),
            "replay_ms_workflow_min": min(replay_values),
            "replay_ms_workflow_max": max(replay_values),
            "prepared_state_bytes": int(
                prepared_storage[backend]["total_state_bytes"]
            ),
        }
        for category in (
            "relational_db",
            "vector_db",
            "filesystem",
            "others",
        ):
            row[f"{category}_ms_workflow_mean"] = mean(
                [
                    float(
                        workflows[workflow][backend]["store_timing_ms"].get(
                            category,
                            0.0,
                        )
                    )
                    for workflow in workflows
                ]
            )
        if llm_timings:
            model_values = [
                float(llm_timings[workflow]["total_ms"])
                for workflow in workflows
            ]
            row.update(
                {
                    "model_inference_ms_workflow_mean": mean(model_values),
                    "model_inference_ms_workflow_min": min(model_values),
                    "model_inference_ms_workflow_max": max(model_values),
                    "end_to_end_ms_workflow_mean": mean(
                        replay + model
                        for replay, model in zip(
                            replay_values,
                            model_values,
                            strict=True,
                        )
                    ),
                }
            )
        summary.append(row)

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
    if llm_timings:
        model_values = [
            float(llm_timings[workflow]["p50_ms"])
            for workflow in workflows
        ]
        for backend in backends:
            operation_rows.append(
                {
                    "backend": backend,
                    "operation": MODEL_OPERATION,
                    "workflows": len(workflows),
                    "p50_ms_workflow_mean": mean(model_values),
                    "p50_ms_workflow_min": min(model_values),
                    "p50_ms_workflow_max": max(model_values),
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


def annotate_stack_totals(
    axis: plt.Axes,
    bars: Any,
    values: list[float],
    *,
    suffix: str,
) -> None:
    for bar, value in zip(bars, values, strict=True):
        axis.annotate(
            f"{value:.1f}{suffix}",
            (
                bar.get_x() + bar.get_width() / 2,
                bar.get_y() + bar.get_height(),
            ),
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

    tool_seconds = [
        float(row["replay_ms_workflow_mean"]) / 1000
        for row in summary
    ]
    model_seconds = [
        float(row.get("model_inference_ms_workflow_mean", 0.0)) / 1000
        for row in summary
    ]
    totals = [
        tool + model for tool, model in zip(tool_seconds, model_seconds, strict=True)
    ]
    tool_bars = axes[0].bar(
        x,
        tool_seconds,
        color=colors,
        edgecolor="white",
        label="Tool calls",
    )
    if any(model_seconds):
        model_bars = axes[0].bar(
            x,
            model_seconds,
            bottom=tool_seconds,
            color="#b7b7b7",
            edgecolor="white",
            hatch="//",
            label="Model inference",
        )
        annotate_stack_totals(axes[0], model_bars, totals, suffix=" s")
    else:
        annotate_bars(axes[0], tool_bars, totals, suffix=" s")
    axes[0].set_title("(a) End-to-end workflow")
    axes[0].set_ylabel("Mean time (s)")
    axes[0].set_xticks(x, labels)
    axes[0].set_ylim(0, max(totals) * 1.18)

    lookup = {
        (str(row["backend"]), str(row["operation"])): row
        for row in operations
    }
    operation_order = [
        (operation, label)
        for operation, label in OPERATIONS
        if all((backend, operation) in lookup for backend in backends)
    ]
    if all(
        (backend, MODEL_OPERATION) in lookup
        for backend in backends
    ):
        operation_order.append((MODEL_OPERATION, MODEL_OPERATION_LABEL))
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
    legend_handles = [
            Patch(facecolor=COLORS[backend], label=LABELS[backend])
            for backend in backends
        ]
    if any(model_seconds):
        legend_handles.extend(
            [
                Patch(
                    facecolor="#b7b7b7",
                    edgecolor="white",
                    hatch="//",
                    label="Model inference",
                ),
            ]
        )
    figure.legend(
        handles=legend_handles,
        loc="upper center",
        bbox_to_anchor=(0.5, 1.02),
        frameon=False,
        ncol=min(len(legend_handles), 4),
    )
    figure.tight_layout(rect=(0, 0, 1, 0.90), w_pad=1.8)
    figure.savefig(output, bbox_inches="tight", dpi=300)
    plt.close(figure)


def plot_workflow_replay(
    output: Path,
    backends: tuple[str, ...],
    workflows: dict[str, dict[str, dict[str, Any]]],
) -> None:
    """Plot one grouped replay-time pair for every workflow."""

    configure_style()
    workflow_names = sorted(workflows)
    x = np.arange(len(workflow_names))
    width = min(0.72 / len(backends), 0.38)
    figure, axis = plt.subplots(figsize=(14.5, 5.4))

    for offset, backend in enumerate(backends):
        values = [
            float(workflows[workflow][backend]["replay_ms"]) / 1000
            for workflow in workflow_names
        ]
        bars = axis.bar(
            x + (offset - (len(backends) - 1) / 2) * width,
            values,
            width,
            color=COLORS[backend],
            edgecolor="white",
            label=(
                "Native branching"
                if backend == "doltgres-qdrant-btrfs"
                else LABELS[backend]
            ),
        )
        # Keep the plot readable with sixteen groups; exact values are in the
        # accompanying CSV and the aggregate summary.
        for bar in bars:
            bar.set_linewidth(0.4)

    axis.set_title("Workflow replay time by workload")
    axis.set_ylabel("Replay time (s)")
    axis.set_xticks(
        x,
        [textwrap.fill(workflow, width=18) for workflow in workflow_names],
        rotation=45,
        ha="right",
        rotation_mode="anchor",
    )
    axis.tick_params(axis="x", labelsize=9, pad=3)
    axis.grid(axis="x", visible=False)
    axis.set_axisbelow(True)
    axis.legend(frameon=False, ncol=len(backends), loc="upper left")
    figure.tight_layout()
    figure.savefig(output, bbox_inches="tight", dpi=300)
    plt.close(figure)


def plot_workflow_storage_growth(
    output: Path,
    backends: tuple[str, ...],
    workflows: dict[str, dict[str, dict[str, Any]]],
) -> None:
    """Plot the signed physical-footprint change observed during replay."""

    configure_style()
    workflow_names = sorted(workflows)
    x = np.arange(len(workflow_names))
    width = min(0.72 / len(backends), 0.38)
    figure, axis = plt.subplots(figsize=(14.5, 5.4))
    minimum = 0.0
    maximum = 0.0

    for offset, backend in enumerate(backends):
        values = [
            float(
                workflows[workflow][backend]["storage"][
                    "observed_live_footprint_change_bytes"
                ]
            )
            / (1024**2)
            for workflow in workflow_names
        ]
        minimum = min(minimum, min(values))
        maximum = max(maximum, max(values))
        bars = axis.bar(
            x + (offset - (len(backends) - 1) / 2) * width,
            values,
            width,
            color=COLORS[backend],
            edgecolor="white",
            label=(
                "Native branching"
                if backend == "doltgres-qdrant-btrfs"
                else LABELS[backend]
            ),
        )
        for bar in bars:
            bar.set_linewidth(0.4)

    axis.set_title("Observed physical storage growth by workload")
    axis.set_ylabel("Post-replay − pre-replay storage (MiB)")
    axis.set_xticks(
        x,
        [textwrap.fill(workflow, width=18) for workflow in workflow_names],
        rotation=45,
        ha="right",
        rotation_mode="anchor",
    )
    axis.tick_params(axis="x", labelsize=9, pad=3)
    span = maximum - minimum
    margin = max(span * 0.08, 1.0)
    axis.set_ylim(minimum - margin, maximum + margin)
    axis.axhline(0, color="#555555", linewidth=0.8)
    axis.grid(axis="x", visible=False)
    axis.set_axisbelow(True)
    axis.legend(frameon=False, ncol=len(backends), loc="upper right")
    figure.tight_layout()
    figure.savefig(output, bbox_inches="tight", dpi=300)
    plt.close(figure)


def plot_workflow_replay_with_llm(
    output: Path,
    backends: tuple[str, ...],
    workflows: dict[str, dict[str, dict[str, Any]]],
    llm_timings: dict[str, dict[str, Any]],
) -> None:
    """Plot tool and model time for every workflow/backend pair."""

    configure_style()
    workflow_names = sorted(workflows)
    x = np.arange(len(workflow_names))
    width = min(0.72 / len(backends), 0.38)
    figure, axis = plt.subplots(figsize=(14.5, 5.4))

    for offset, backend in enumerate(backends):
        positions = x + (offset - (len(backends) - 1) / 2) * width
        tool_values = [
            float(workflows[workflow][backend]["replay_ms"]) / 1000
            for workflow in workflow_names
        ]
        model_values = [
            float(llm_timings[workflow]["total_ms"]) / 1000
            for workflow in workflow_names
        ]
        axis.bar(
            positions,
            tool_values,
            width,
            color=COLORS[backend],
            edgecolor="white",
            linewidth=0.4,
            label=(
                "Native branching"
                if backend == "doltgres-qdrant-btrfs"
                else LABELS[backend]
            ),
        )
        axis.bar(
            positions,
            model_values,
            width,
            bottom=tool_values,
            color="#b7b7b7",
            edgecolor="white",
            linewidth=0.4,
            hatch="//",
        )

    axis.set_title("Workflow end-to-end time by workload")
    axis.set_ylabel("Time (s)")
    axis.set_xticks(
        x,
        [textwrap.fill(workflow, width=18) for workflow in workflow_names],
        rotation=45,
        ha="right",
        rotation_mode="anchor",
    )
    axis.tick_params(axis="x", labelsize=9, pad=3)
    axis.grid(axis="x", visible=False)
    axis.set_axisbelow(True)
    handles, labels = axis.get_legend_handles_labels()
    handles.append(
        Patch(
            facecolor="#b7b7b7",
            edgecolor="white",
            hatch="//",
            label="Model inference",
        )
    )
    axis.legend(handles=handles, frameon=False, ncol=len(backends) + 1, loc="upper left")
    figure.tight_layout()
    figure.savefig(output, bbox_inches="tight", dpi=300)
    plt.close(figure)


STORE_BREAKDOWN = (
    ("relational_db", "Relational DB", "#4c78a8"),
    ("vector_db", "Vector DB", "#f58518"),
    ("filesystem", "Filesystem", "#54a24b"),
    ("model_inference", "Model inference", "#8f63c6"),
    ("others", "Other", "#b7b7b7"),
)
BACKEND_HATCHES = {
    "chronos": "",
    "doltgres-qdrant-btrfs": "///",
}


def _store_breakdown_values(
    workflow: str,
    backend: str,
    workflows: dict[str, dict[str, dict[str, Any]]],
    llm_timings: dict[str, dict[str, Any]],
) -> list[float]:
    measured = workflows[workflow][backend]["store_timing_ms"]
    return [
        float(measured.get(category, 0.0)) / 1000
        for category in ("relational_db", "vector_db", "filesystem")
    ] + [
        float(llm_timings.get(workflow, {}).get("total_ms", 0.0)) / 1000,
        float(measured.get("others", 0.0)) / 1000,
    ]


def _plot_store_breakdown(
    output: Path,
    backends: tuple[str, ...],
    workflows: dict[str, dict[str, dict[str, Any]]],
    llm_timings: dict[str, dict[str, Any]],
    *,
    mean_only: bool,
) -> None:
    """Plot measured store spans plus the separately captured model time."""

    configure_style()
    workflow_names = sorted(workflows)
    if mean_only:
        labels = ["All workflows"]
        groups = [workflow_names]
        figure, axis = plt.subplots(figsize=(7.5, 5.2))
    else:
        labels = [textwrap.fill(workflow, width=17) for workflow in workflow_names]
        groups = [[workflow] for workflow in workflow_names]
        figure, axis = plt.subplots(figsize=(15.5, 5.8))
    x = np.arange(len(labels))
    width = min(0.72 / len(backends), 0.36)
    for offset, backend in enumerate(backends):
        bottoms = np.zeros(len(groups))
        values_by_category: list[list[float]] = [[] for _ in STORE_BREAKDOWN]
        for group in groups:
            rows = [
                _store_breakdown_values(workflow, backend, workflows, llm_timings)
                for workflow in group
            ]
            for index in range(len(STORE_BREAKDOWN)):
                values_by_category[index].append(
                    statistics.fmean(row[index] for row in rows)
                )
        position = x + (offset - (len(backends) - 1) / 2) * width
        for index, (_, category_label, color) in enumerate(STORE_BREAKDOWN):
            values = np.asarray(values_by_category[index], dtype=float)
            axis.bar(
                position,
                values,
                width,
                bottom=bottoms,
                color=color,
                edgecolor="white",
                linewidth=0.35,
                hatch=BACKEND_HATCHES.get(backend, ""),
                label=category_label if offset == 0 else None,
            )
            bottoms += values
    axis.set_title(
        "Mean workflow time by component"
        if mean_only
        else "Workflow time by component"
    )
    axis.set_ylabel("Time (s)")
    axis.set_xticks(x, labels)
    if not mean_only:
        axis.tick_params(axis="x", labelsize=9, pad=3)
        axis.set_xticklabels(labels, rotation=45, ha="right", rotation_mode="anchor")
    axis.grid(axis="x", visible=False)
    axis.set_axisbelow(True)
    backend_handles = [
        Patch(
            facecolor="white",
            edgecolor="#555555",
            hatch=BACKEND_HATCHES.get(backend, ""),
            label=(
                "Native branching"
                if backend == "doltgres-qdrant-btrfs"
                else LABELS[backend]
            ),
        )
        for backend in backends
    ]
    category_handles = [Patch(facecolor=color, label=label) for _, label, color in STORE_BREAKDOWN]
    axis.legend(
        handles=backend_handles + category_handles,
        frameon=False,
        ncol=3,
        loc="upper left",
        bbox_to_anchor=(0, 1.02),
    )
    figure.tight_layout()
    figure.savefig(output, bbox_inches="tight", dpi=300)
    plt.close(figure)


def plot_store_breakdown_mean(
    output: Path,
    backends: tuple[str, ...],
    workflows: dict[str, dict[str, dict[str, Any]]],
    llm_timings: dict[str, dict[str, Any]],
) -> None:
    _plot_store_breakdown(
        output,
        backends,
        workflows,
        llm_timings,
        mean_only=True,
    )


def plot_store_breakdown_by_workflow(
    output: Path,
    backends: tuple[str, ...],
    workflows: dict[str, dict[str, dict[str, Any]]],
    llm_timings: dict[str, dict[str, Any]],
) -> None:
    _plot_store_breakdown(
        output,
        backends,
        workflows,
        llm_timings,
        mean_only=False,
    )


def workflow_replay_rows(
    backends: tuple[str, ...],
    workflows: dict[str, dict[str, dict[str, Any]]],
    llm_timings: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    llm_timings = llm_timings or {}
    return [
        {
            "workflow": workflow,
            "backend": backend,
            "replay_ms": float(workflows[workflow][backend]["replay_ms"]),
            "tool_call_ms": float(workflows[workflow][backend]["replay_ms"]),
            "model_inference_ms": (
                float(llm_timings[workflow]["total_ms"])
                if workflow in llm_timings
                else 0.0
            ),
            "end_to_end_ms": (
                float(workflows[workflow][backend]["replay_ms"])
                + float(llm_timings[workflow]["total_ms"])
                if workflow in llm_timings
                else float(workflows[workflow][backend]["replay_ms"])
            ),
            **{
                f"{category}_ms": float(
                    workflows[workflow][backend]["store_timing_ms"].get(category, 0.0)
                )
                for category in ("relational_db", "vector_db", "filesystem", "others")
            },
        }
        for workflow in sorted(workflows)
        for backend in backends
    ]


def workflow_storage_rows(
    backends: tuple[str, ...],
    workflows: dict[str, dict[str, dict[str, Any]]],
) -> list[dict[str, Any]]:
    rows = []
    for workflow in sorted(workflows):
        for backend in backends:
            storage = workflows[workflow][backend]["storage"]
            pre_bytes = float(storage["pre_workflow_state_bytes"])
            post_bytes = float(storage["post_workflow_state_bytes"])
            rows.append(
                {
                    "workflow": workflow,
                    "backend": backend,
                    "pre_workflow_state_bytes": pre_bytes,
                    "post_workflow_state_bytes": post_bytes,
                    "post_workflow_state_gib": post_bytes / (1024**3),
                    "observed_live_footprint_change_bytes": float(
                        storage["observed_live_footprint_change_bytes"]
                    ),
                    "observed_live_footprint_change_mib": float(
                        storage["observed_live_footprint_change_bytes"]
                    )
                    / (1024**2),
                    "attributable_to_workflow": bool(
                        storage["attributable_to_workflow"]
                    ),
                    "measurement_scope": str(storage["measurement_scope"]),
                }
            )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("benchmark_root", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--traces-dir",
        type=Path,
        help=(
            "captured traces containing llm_timing metadata; model time is "
            "reported separately from backend replay time"
        ),
    )
    arguments = parser.parse_args()

    output_dir = (
        arguments.output_dir
        or arguments.benchmark_root / "summary"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    backends, workflows = load_workflows(arguments.benchmark_root)
    llm_timings = load_llm_timings(arguments.traces_dir, workflows)
    prepared_storage = load_prepared_storage(
        arguments.benchmark_root,
        backends,
    )
    summary, operations = aggregate(
        backends,
        workflows,
        prepared_storage,
        llm_timings,
    )
    storage_rows = workflow_storage_rows(backends, workflows)
    write_csv(output_dir / "workflow_average.csv", summary)
    write_csv(output_dir / "operation_latency_average.csv", operations)
    write_csv(
        output_dir / "workflow_replay.csv",
        workflow_replay_rows(backends, workflows, llm_timings),
    )
    write_csv(output_dir / "workflow_storage.csv", storage_rows)
    plot(output_dir / "backend_comparison.pdf", summary, operations)
    plot_workflow_replay(
        output_dir / "workflow_replay_time.pdf",
        backends,
        workflows,
    )
    plot_workflow_storage_growth(
        output_dir / "workflow_storage_growth.pdf",
        backends,
        workflows,
    )
    plot_store_breakdown_mean(
        output_dir / "store_time_breakdown_mean.pdf",
        backends,
        workflows,
        llm_timings,
    )
    plot_store_breakdown_by_workflow(
        output_dir / "store_time_breakdown_by_workflow.pdf",
        backends,
        workflows,
        llm_timings,
    )
    if llm_timings:
        plot_workflow_replay_with_llm(
            output_dir / "workflow_replay_time_with_llm.pdf",
            backends,
            workflows,
            llm_timings,
        )
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
                "store_timing": {
                    "categories": [
                        "relational_db",
                        "vector_db",
                        "filesystem",
                        "model_inference",
                        "others",
                    ],
                    "measurement": (
                        "exclusive elapsed time at concrete relational, vector, "
                        "and filesystem client boundaries during replay; "
                        "other is the non-store remainder of each event; model "
                        "inference comes from the captured trace"
                    ),
                    "workflow_rows": [
                        {
                            "workflow": workflow,
                            "backend": backend,
                            **{
                                category: float(
                                    workflows[workflow][backend][
                                        "store_timing_ms"
                                    ].get(category, 0.0)
                                )
                                for category in (
                                    "relational_db",
                                    "vector_db",
                                    "filesystem",
                                    "others",
                                )
                            },
                            "model_inference": float(
                                llm_timings.get(workflow, {}).get(
                                    "total_ms",
                                    0.0,
                                )
                            ),
                        }
                        for workflow in sorted(workflows)
                        for backend in backends
                    ],
                },
                "llm_timing": {
                    "source": (
                        str(arguments.traces_dir.resolve())
                        if arguments.traces_dir is not None
                        else None
                    ),
                    "measurement": (
                        next(iter(llm_timings.values()))["measurement"]
                        if llm_timings
                        else None
                    ),
                    "workflows": llm_timings,
                },
                "storage_measurement": {
                    "definition": (
                        "physical bytes after fresh ingestion and hierarchy "
                        "construction, before workflow replay"
                    ),
                    "branch_count": (
                        next(
                            iter(
                                {
                                    int(value["branches"])
                                    for value in prepared_storage.values()
                                }
                            )
                        )
                        if len(
                            {
                                int(value["branches"])
                                for value in prepared_storage.values()
                            }
                        )
                        == 1
                        else None
                    ),
                    "branch_counts": {
                        backend: int(value["branches"])
                        for backend, value in prepared_storage.items()
                    },
                    "components": prepared_storage,
                },
                "workflow_storage_measurement": {
                    "definition": (
                        "signed change in physical bytes between samples "
                        "taken immediately before and after successful "
                        "workflow replay, before its isolated branch "
                        "namespace is removed"
                    ),
                    "aggregation": "median across repetitions",
                    "attributable_to_workflow": all(
                        bool(row["attributable_to_workflow"])
                        for row in storage_rows
                    ),
                    "caveat": (
                        "samples come from reused live backends; checkpointing, "
                        "compaction, and asynchronous reclamation can change "
                        "the physical footprint, so final-minus-baseline is "
                        "not per-workflow storage overhead"
                    ),
                    "rows": storage_rows,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
