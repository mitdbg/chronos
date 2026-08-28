#!/usr/bin/env python3
"""Measure concurrent branch-merge throughput for the enterprise backends.

This is a performance microbenchmark, separate from the anomaly experiments.
It prepares one disjoint branch per timed operation and then measures only the
backend-neutral ``merge_preview`` + ``merge`` sequence.  Branch creation,
document writes, embedding construction, service startup, and verifier work
are outside the timed interval.

The ``*-big-lock`` variants are intentionally simple baselines: one
``threading.Lock`` in this benchmark serializes the complete preview-and-merge
call for all workers.  The lock is not part of Chronos or either comparison
backend.  Consequently the variants use exactly the same stores, schemas,
Qdrant service, and merge API as their unlocked counterparts; only the
application-level serialization policy changes.

Example:

    uv run python apps/enterprise-knowledge-mcp/scripts/\
run_merge_throughput_microbench.py \
      --backend app-managed --backend app-managed-big-lock \
      --backend doltgres-qdrant-btrfs --backend doltgres-qdrant-btrfs-big-lock \
      --worker-counts 1,2,4,8 --merges-per-worker 4 --repetitions 3 \
      --qdrant-url http://127.0.0.1:6339 \
      --doltgres-dsn postgresql://postgres:password@127.0.0.1:55439/knowledge \
      --btrfs-root /mnt/chronos-enterprise-state-division-v2 \
      --output .enterprise-knowledge/concurrency/merge-throughput-v1

The output contains JSON and CSV records plus throughput and latency PDFs.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
import statistics
import tempfile
import threading
import time
import traceback
from typing import Any, Callable

from chronos_enterprise_knowledge.backends.factory import create_knowledge_backend
from chronos_enterprise_knowledge.models import (
    DocumentChunk,
    IndexedDocument,
    KnowledgeDocument,
)


BACKEND_CHOICES = (
    "chronos",
    "app-managed",
    "app-managed-big-lock",
    "doltgres-qdrant-btrfs",
    "doltgres-qdrant-btrfs-big-lock",
)


@dataclass(frozen=True)
class Variant:
    label: str
    backend_name: str
    serialized: bool


VARIANTS = {
    "chronos": Variant("chronos", "chronos", False),
    "app-managed": Variant("app-managed", "app-managed", False),
    "app-managed-big-lock": Variant("app-managed-big-lock", "app-managed", True),
    "doltgres-qdrant-btrfs": Variant(
        "doltgres-qdrant-btrfs", "doltgres-qdrant-btrfs", False
    ),
    "doltgres-qdrant-btrfs-big-lock": Variant(
        "doltgres-qdrant-btrfs-big-lock", "doltgres-qdrant-btrfs", True
    ),
}


@dataclass(frozen=True)
class PreparedOperation:
    worker: int
    ordinal: int
    branch: str
    document_id: str


class MergeGate:
    """Benchmark-only gate around a complete preview-and-merge operation."""

    def __init__(self) -> None:
        self._lock = threading.Lock()

    def execute(
        self,
        operation: Callable[[], Any],
    ) -> tuple[Any, dict[str, float]]:
        wait_started = time.perf_counter()
        self._lock.acquire()
        acquired = time.perf_counter()
        timing = {
            "lock_wait_ms": (acquired - wait_started) * 1000.0,
            "critical_section_ms": 0.0,
        }
        try:
            value = operation()
            return value, timing
        finally:
            timing["critical_section_ms"] = (time.perf_counter() - acquired) * 1000.0
            self._lock.release()


def _vector(index: int, dimensions: int) -> tuple[float, ...]:
    values = [0.0] * dimensions
    values[index % dimensions] = 1.0
    return tuple(values)


def _make_document(
    document_id: str,
    worker: int,
    ordinal: int,
    dimensions: int,
    *,
    baseline: bool = False,
) -> IndexedDocument:
    path = f"/knowledge/merge-throughput/{document_id}.md"
    content = (
        f"Baseline record for worker {worker}, operation {ordinal}."
        if baseline
        else (
            f"Worker {worker} prepared independent merge operation {ordinal}. "
            "The document, file, relational record, and vector index are one "
            "logical bundle."
        )
    )
    document = KnowledgeDocument(
        id=document_id,
        path=path,
        title=f"Merge throughput {worker}-{ordinal}",
        source="merge-throughput-microbench",
        content=content,
        kind="curated",
        metadata={"benchmark": "merge-throughput-v1", "worker": worker},
    )
    chunk = DocumentChunk(
        id=f"{document_id}:chunk-0",
        document_id=document_id,
        ordinal=0,
        text=content,
        embedding=_vector(worker + ordinal, dimensions),
        metadata={"benchmark": "merge-throughput-v1"},
    )
    return IndexedDocument(document=document, chunks=(chunk,))


def _all_change_ids(preview: Mapping[str, Any]) -> list[str]:
    """Extract every selectable change ID from either preview shape."""

    values = {str(value) for value in preview.get("change_ids", ()) or ()}
    groups = preview.get("selection_groups") or {}
    if isinstance(groups, Mapping):
        for group in groups.values():
            if not isinstance(group, Mapping):
                continue
            for change_ids in group.values():
                if isinstance(change_ids, (list, tuple, set, frozenset)):
                    values.update(str(value) for value in change_ids)
    for key in ("changes", "conflicts"):
        entries = preview.get(key, ()) or ()
        if not isinstance(entries, (list, tuple)):
            continue
        for entry in entries:
            if isinstance(entry, Mapping):
                value = entry.get("change_id") or entry.get("id")
                if value is not None:
                    values.add(str(value))
    return sorted(values)


def _factory_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "qdrant_url": args.qdrant_url,
        "qdrant_api_key": args.qdrant_api_key,
        "doltgres_dsn": args.doltgres_dsn,
        "btrfs_root": args.btrfs_root,
        "doltgres_data_dir": args.doltgres_data_dir,
        "qdrant_storage_dir": args.qdrant_storage_dir,
    }


def _open_backend(
    variant: Variant,
    state_dir: Path,
    dimensions: int,
    factory_kwargs: Mapping[str, Any],
) -> Any:
    return create_knowledge_backend(
        variant.backend_name,
        state_dir=str(state_dir),
        vector_dimensions=dimensions,
        **dict(factory_kwargs),
    )


def _prepare_operations(
    backend: Any,
    run_tag: str,
    workers: int,
    merges_per_worker: int,
    dimensions: int,
) -> list[PreparedOperation]:
    operations: list[PreparedOperation] = []
    indexed_documents: list[tuple[PreparedOperation, IndexedDocument]] = []
    for worker in range(workers):
        for ordinal in range(merges_per_worker):
            branch = f"bench/{run_tag}/worker-{worker:03d}/op-{ordinal:03d}"
            document_id = f"merge-throughput-{run_tag}-{worker:03d}-{ordinal:03d}"
            operation = PreparedOperation(worker, ordinal, branch, document_id)
            indexed_documents.append(
                (
                    operation,
                    _make_document(document_id, worker, ordinal, dimensions),
                )
            )

    # Setup is deliberately serial and outside the measurement interval.  It
    # also avoids branch/schema initialization races in the comparison stores.
    # Seed every path before forking so the timed updates do not accidentally
    # measure a shared-directory inode conflict in the filesystem participant.
    for operation, _ in indexed_documents:
        baseline = _make_document(
            operation.document_id,
            operation.worker,
            operation.ordinal,
            dimensions,
            baseline=True,
        )
        backend.put_document(
            "main",
            baseline,
            operation_id=(
                f"prepare-baseline-{run_tag}-{operation.worker}-{operation.ordinal}"
            ),
        )
    for operation, indexed in indexed_documents:
        backend.create_branch(operation.branch, "main")
        backend.put_document(
            operation.branch,
            indexed,
            operation_id=(f"prepare-{run_tag}-{operation.worker}-{operation.ordinal}"),
        )
        operations.append(operation)
    return operations


def _merge_once(
    backend: Any,
    operation: PreparedOperation,
    *,
    max_attempts: int = 5,
) -> tuple[Any, dict[str, float]]:
    """Run the normal preview/apply call, retrying transient head races.

    The existing workflow harness retries stale previews and transient commit
    errors.  Applying the same bounded retry policy here keeps the microbench
    from treating normal concurrent-head races as backend failures while
    leaving semantic merge conflicts non-retryable.
    """

    retry_tokens = (
        "stale",
        "advanced",
        "changed after",
        "preview",
        "transaction",
        "commit",
        "nothing to commit",
        "database is locked",
        "busy",
        "in progress",
    )
    timing = {
        "preview_ms": 0.0,
        "apply_ms": 0.0,
        "attempts": 0.0,
    }
    for attempt in range(max_attempts):
        timing["attempts"] += 1.0
        preview_started = time.perf_counter()
        preview = backend.merge_preview(operation.branch, "main")
        timing["preview_ms"] += (time.perf_counter() - preview_started) * 1000.0
        change_ids = _all_change_ids(preview)
        if not change_ids:
            raise RuntimeError(
                f"merge preview returned no selectable changes for {operation.branch}"
            )
        try:
            apply_started = time.perf_counter()
            result = backend.merge(
                operation.branch,
                "main",
                operation_id=(f"timed-{operation.branch}-{attempt}-{time.time_ns()}"),
                selected_change_ids=change_ids,
                preview_token=preview.get("preview_token"),
                prepared_preview=preview,
            )
            timing["apply_ms"] += (time.perf_counter() - apply_started) * 1000.0
            return result, timing
        except BaseException as exc:
            timing["apply_ms"] += (time.perf_counter() - apply_started) * 1000.0
            message = str(exc).lower()
            if attempt + 1 < max_attempts and any(
                token in message for token in retry_tokens
            ):
                continue
            raise
    raise AssertionError("merge retry loop exhausted without returning or raising")


def _percentile(values: Sequence[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(len(ordered) * percentile / 100.0))
    return float(ordered[rank - 1])


def _run_timed(
    variant: Variant,
    worker_backends: Sequence[Any],
    operations: Sequence[PreparedOperation],
    merge_retries: int,
) -> dict[str, Any]:
    by_worker: dict[int, list[PreparedOperation]] = {}
    for operation in operations:
        by_worker.setdefault(operation.worker, []).append(operation)
    for values in by_worker.values():
        values.sort(key=lambda operation: operation.ordinal)

    gate = MergeGate() if variant.serialized else None
    ready = threading.Barrier(len(worker_backends) + 1)
    start = threading.Event()
    records: list[dict[str, Any]] = []
    record_lock = threading.Lock()

    def worker(worker_index: int) -> None:
        ready.wait()
        start.wait()
        active_backend = worker_backends[worker_index]
        for operation in by_worker.get(worker_index, ()):
            operation_started = time.perf_counter()
            timing = {
                "lock_wait_ms": 0.0,
                "critical_section_ms": 0.0,
                "preview_ms": 0.0,
                "apply_ms": 0.0,
                "attempts": 0.0,
            }
            error: str | None = None
            try:
                if gate is None:
                    _, call_timing = _merge_once(
                        active_backend,
                        operation,
                        max_attempts=merge_retries,
                    )
                    timing.update(call_timing)
                else:
                    value, gate_timing = gate.execute(
                        lambda: _merge_once(
                            active_backend,
                            operation,
                            max_attempts=merge_retries,
                        )
                    )
                    _, call_timing = value
                    timing.update(gate_timing)
                    timing.update(call_timing)
            except BaseException as exc:  # keep all attempted operations visible
                error = repr(exc)
            elapsed_ms = (time.perf_counter() - operation_started) * 1000.0
            record = {
                "worker": operation.worker,
                "ordinal": operation.ordinal,
                "branch": operation.branch,
                "document_id": operation.document_id,
                "latency_ms": elapsed_ms,
                **timing,
                "status": "ok" if error is None else "error",
            }
            if error is not None:
                record["error"] = error
            with record_lock:
                records.append(record)

    with ThreadPoolExecutor(max_workers=len(worker_backends)) as pool:
        futures = [
            pool.submit(worker, worker_index)
            for worker_index in range(len(worker_backends))
        ]
        ready.wait()
        started = time.perf_counter()
        start.set()
        for future in as_completed(futures):
            future.result()
        finished = time.perf_counter()

    records.sort(key=lambda value: (int(value["worker"]), int(value["ordinal"])))
    successful = [record for record in records if record.get("status") == "ok"]
    latencies = [float(record["latency_ms"]) for record in successful]
    lock_waits = [float(record["lock_wait_ms"]) for record in successful]
    preview_times = [float(record["preview_ms"]) for record in successful]
    apply_times = [float(record["apply_ms"]) for record in successful]
    attempts = [float(record["attempts"]) for record in successful]
    critical = [
        float(record["critical_section_ms"])
        for record in successful
        if variant.serialized
    ]
    wall_time_s = finished - started
    return {
        "operations": len(records),
        "successful_operations": len(successful),
        "failed_operations": len(records) - len(successful),
        "success_rate": (len(successful) / len(records) if records else None),
        "wall_time_s": wall_time_s,
        "throughput_ops_s": (
            len(successful) / wall_time_s if wall_time_s > 0 else None
        ),
        "latency_ms_mean": statistics.fmean(latencies) if latencies else None,
        "latency_ms_p50": _percentile(latencies, 50),
        "latency_ms_p95": _percentile(latencies, 95),
        "lock_wait_ms_mean": statistics.fmean(lock_waits)
        if variant.serialized
        else 0.0,
        "lock_wait_ms_p95": _percentile(lock_waits, 95) if variant.serialized else 0.0,
        "critical_section_ms_mean": (statistics.fmean(critical) if critical else 0.0),
        "preview_ms_mean": statistics.fmean(preview_times) if preview_times else None,
        "apply_ms_mean": statistics.fmean(apply_times) if apply_times else None,
        "attempts_mean": statistics.fmean(attempts) if attempts else None,
        "records": records,
    }


def _run_one(
    variant: Variant,
    workers: int,
    merges_per_worker: int,
    repetition: int,
    args: argparse.Namespace,
    output_root: Path,
) -> dict[str, Any]:
    factory_kwargs = _factory_kwargs(args)
    output_root.mkdir(parents=True, exist_ok=True)
    state_dir = Path(
        tempfile.mkdtemp(
            prefix=f"{variant.label.replace('-', '_')}-w{workers}-r{repetition}-",
            dir=str(output_root),
        )
    )
    backend: Any | None = None
    worker_backends: list[Any] = []
    started = time.time()
    run_tag = f"{variant.label.replace('-', '_')}-r{repetition:02d}-w{workers:03d}"
    try:
        backend = _open_backend(variant, state_dir, args.dimensions, factory_kwargs)
        operations = _prepare_operations(
            backend,
            run_tag,
            workers,
            merges_per_worker,
            args.dimensions,
        )
        # Open one independent session/backend per worker, serially, just as
        # the concurrent workflow harness does.  No connection setup is timed.
        for _ in range(workers):
            worker_backends.append(
                _open_backend(variant, state_dir, args.dimensions, factory_kwargs)
            )
        metrics = _run_timed(
            variant,
            worker_backends,
            operations,
            args.merge_retries,
        )
        status = "ok" if metrics["failed_operations"] == 0 else "error"
        return {
            "schema": "merge-throughput-v1",
            "variant": variant.label,
            "backend": variant.backend_name,
            "status": status,
            "serialized_preview_and_merge": variant.serialized,
            "workers": workers,
            "merges_per_worker": merges_per_worker,
            "merge_retries": args.merge_retries,
            "repetition": repetition,
            "state_dir": str(state_dir),
            "duration_s": time.time() - started,
            **metrics,
        }
    except BaseException as exc:
        return {
            "schema": "merge-throughput-v1",
            "variant": variant.label,
            "backend": variant.backend_name,
            "serialized_preview_and_merge": variant.serialized,
            "workers": workers,
            "merges_per_worker": merges_per_worker,
            "merge_retries": args.merge_retries,
            "repetition": repetition,
            "state_dir": str(state_dir),
            "duration_s": time.time() - started,
            "status": "error",
            "error": repr(exc),
            "traceback": traceback.format_exc(),
        }
    finally:
        for worker_backend in worker_backends:
            try:
                worker_backend.close()
            except BaseException:
                pass
        if backend is not None:
            try:
                backend.close()
            except BaseException:
                pass


def _configure_plot_style() -> None:
    import matplotlib.font_manager as fm
    import matplotlib.pyplot as plt

    font = Path("/usr/share/fonts/opentype/linux-libertine/LinBiolinum_R.otf")
    if font.exists():
        fm.fontManager.addfont(str(font))
    plt.style.use("seaborn-v0_8-whitegrid")
    plt.rcParams.update(
        {
            "font.family": "Linux Biolinum O",
            "font.size": 14,
            "axes.labelsize": 16,
            "axes.titlesize": 17,
            "xtick.labelsize": 13,
            "ytick.labelsize": 13,
            "legend.fontsize": 11,
            "figure.dpi": 150,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    )


def _write_figures(rows: Sequence[Mapping[str, Any]], output: Path) -> list[str]:
    import matplotlib.pyplot as plt

    _configure_plot_style()
    styles = {
        "chronos": {
            "color": "#7b2cbf",
            "marker": "o",
            "linestyle": "-",
            "hatch": "",
        },
        "app-managed": {
            "color": "#0072b2",
            "marker": "o",
            "linestyle": "-",
            "hatch": "",
        },
        "app-managed-big-lock": {
            "color": "#56b4e9",
            "marker": "s",
            "linestyle": "--",
            "hatch": "///",
        },
        "doltgres-qdrant-btrfs": {
            "color": "#d55e00",
            "marker": "o",
            "linestyle": "-",
            "hatch": "",
        },
        "doltgres-qdrant-btrfs-big-lock": {
            "color": "#e69f00",
            "marker": "s",
            "linestyle": "--",
            "hatch": "///",
        },
    }
    labels = {
        "chronos": "Chronos",
        "app-managed": "App-managed",
        "app-managed-big-lock": "App-managed + big lock",
        "doltgres-qdrant-btrfs": "Native branching",
        "doltgres-qdrant-btrfs-big-lock": "Native branching + big lock",
    }
    variants = list(dict.fromkeys(str(row["variant"]) for row in rows))
    workers = sorted({int(row["workers"]) for row in rows})
    aggregates: dict[tuple[str, int], dict[str, float]] = {}
    for variant in variants:
        for worker_count in workers:
            values = [
                float(row["throughput_ops_s"])
                for row in rows
                if row["variant"] == variant
                and int(row["workers"]) == worker_count
                and row.get("status", "ok") != "error"
            ]
            latencies = [
                float(row["latency_ms_p50"])
                for row in rows
                if row["variant"] == variant
                and int(row["workers"]) == worker_count
                and row.get("status", "ok") != "error"
            ]
            success_rates = [
                float(
                    row.get(
                        "success_rate",
                        float(row["successful_operations"])
                        / max(1, int(row["operations"])),
                    )
                )
                for row in rows
                if row["variant"] == variant
                and int(row["workers"]) == worker_count
                and row.get("status", "ok") != "error"
            ]
            if values:
                aggregates[(variant, worker_count)] = {
                    "throughput": statistics.fmean(values),
                    "latency": statistics.fmean(latencies),
                    "success_rate": statistics.fmean(success_rates),
                }

    output.mkdir(parents=True, exist_ok=True)
    generated: list[str] = []
    figure, axis = plt.subplots(figsize=(10.5, 5.4))
    bar_width = 0.15
    center = (len(variants) - 1) / 2.0
    worker_positions = list(range(len(workers)))
    max_throughput = 0.0
    for index, variant in enumerate(variants):
        values = [
            aggregates.get((variant, worker_count), {}).get("throughput", 0.0)
            for worker_count in workers
        ]
        max_throughput = max(max_throughput, max(values, default=0.0))
        style = styles.get(
            variant,
            {"color": "#444444", "marker": "o", "linestyle": "-", "hatch": ""},
        )
        positions = [
            position + (index - center) * bar_width for position in worker_positions
        ]
        bars = axis.bar(
            positions,
            values,
            bar_width * 0.92,
            color=style["color"],
            edgecolor="#333333",
            linewidth=0.6,
            hatch=style["hatch"],
            label=labels.get(variant, variant),
        )
        for bar, worker_count in zip(bars, workers, strict=True):
            failed = any(
                int(row.get("failed_operations", 0)) > 0
                for row in rows
                if row["variant"] == variant and int(row["workers"]) == worker_count
            )
            if failed:
                axis.text(
                    bar.get_x() + bar.get_width() / 2.0,
                    bar.get_height() + max_throughput * 0.025,
                    "×",
                    ha="center",
                    va="bottom",
                    fontsize=16,
                    color="#111111",
                )
    axis.set_xlabel("Concurrent workers")
    axis.set_ylabel("Successful merges / second")
    axis.set_title("Concurrent merge throughput (× marks incomplete runs)")
    axis.set_xticks(worker_positions, [str(worker_count) for worker_count in workers])
    axis.legend(frameon=True, ncol=2)
    figure.tight_layout()
    path = output / "merge_throughput_vs_workers.pdf"
    figure.savefig(path, bbox_inches="tight")
    plt.close(figure)
    generated.append(str(path))

    figure, axis = plt.subplots(figsize=(8.0, 4.8))
    for variant in variants:
        points = [
            (worker_count, aggregates[(variant, worker_count)]["success_rate"] * 100.0)
            for worker_count in workers
            if (variant, worker_count) in aggregates
        ]
        if not points:
            continue
        style = styles.get(
            variant, {"color": "#444444", "marker": "o", "linestyle": "-"}
        )
        axis.plot(
            [point[0] for point in points],
            [point[1] for point in points],
            marker=style["marker"],
            linewidth=2.2,
            linestyle=style["linestyle"],
            color=style["color"],
            label=labels.get(variant, variant),
        )
    axis.set_xlabel("Concurrent workers")
    axis.set_ylabel("Successful operations (%)")
    axis.set_title("Merge completion under concurrency")
    axis.set_ylim(0.0, 105.0)
    axis.set_xticks(workers)
    if axis.lines:
        axis.legend(frameon=True, ncol=1)
    figure.tight_layout()
    path = output / "merge_completion_vs_workers.pdf"
    figure.savefig(path, bbox_inches="tight")
    plt.close(figure)
    generated.append(str(path))

    figure, axis = plt.subplots(figsize=(8.0, 4.8))
    for variant in variants:
        points = [
            (worker_count, aggregates[(variant, worker_count)]["latency"])
            for worker_count in workers
            if (variant, worker_count) in aggregates
        ]
        if not points:
            continue
        style = styles.get(
            variant, {"color": "#444444", "marker": "o", "linestyle": "-"}
        )
        axis.plot(
            [point[0] for point in points],
            [point[1] for point in points],
            marker=style["marker"],
            linewidth=2.2,
            linestyle=style["linestyle"],
            color=style["color"],
            label=labels.get(variant, variant),
        )
    axis.set_xlabel("Concurrent workers")
    axis.set_ylabel("Median merge latency (ms)")
    axis.set_title("Merge latency under concurrency")
    axis.set_xticks(workers)
    if axis.lines:
        axis.legend(frameon=True, ncol=1)
    figure.tight_layout()
    path = output / "merge_latency_vs_workers.pdf"
    figure.savefig(path, bbox_inches="tight")
    plt.close(figure)
    generated.append(str(path))
    return generated


def _parse_positive_list(value: str, name: str) -> list[int]:
    try:
        parsed = sorted({int(item.strip()) for item in value.split(",")})
    except ValueError as exc:
        raise SystemExit(f"{name} must be a comma-separated list of integers") from exc
    if not parsed or any(item <= 0 for item in parsed):
        raise SystemExit(f"{name} must contain only positive integers")
    return parsed


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backend",
        action="append",
        choices=BACKEND_CHOICES,
        help="variant to run (repeatable; default: all five variants)",
    )
    parser.add_argument("--worker-counts", default="1,2,4,8")
    parser.add_argument("--merges-per-worker", type=int, default=4)
    parser.add_argument("--merge-retries", type=int, default=32)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--dimensions", type=int, default=3)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("merge-throughput-results"),
    )
    parser.add_argument("--state-root", type=Path)
    parser.add_argument("--qdrant-url")
    parser.add_argument("--qdrant-api-key")
    parser.add_argument("--doltgres-dsn")
    parser.add_argument("--btrfs-root")
    parser.add_argument("--doltgres-data-dir")
    parser.add_argument("--qdrant-storage-dir")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.merges_per_worker <= 0:
        raise SystemExit("--merges-per-worker must be positive")
    if args.merge_retries <= 0:
        raise SystemExit("--merge-retries must be positive")
    if args.repetitions <= 0:
        raise SystemExit("--repetitions must be positive")
    if args.dimensions <= 0:
        raise SystemExit("--dimensions must be positive")
    workers = _parse_positive_list(args.worker_counts, "--worker-counts")
    labels = args.backend or list(BACKEND_CHOICES)
    variants = [VARIANTS[label] for label in labels]
    native_requested = any(
        variant.backend_name == "doltgres-qdrant-btrfs" for variant in variants
    )
    # The timed run opens one independent backend session per worker.  A
    # local embedded Qdrant path cannot be opened by those sessions; requiring
    # the same service for every variant also prevents an accidental setup
    # advantage for one backend.
    if not args.qdrant_url:
        raise SystemExit(
            "all variants require --qdrant-url so every worker uses the same "
            "Docker Qdrant service"
        )
    if native_requested and not args.doltgres_dsn:
        raise SystemExit("native variants require --doltgres-dsn")
    if native_requested and not args.btrfs_root:
        raise SystemExit("native variants require --btrfs-root")

    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    state_root = (
        args.state_root.resolve() if args.state_root is not None else output / "state"
    )
    state_root.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    for variant in variants:
        for worker_count in workers:
            for repetition in range(args.repetitions):
                row = _run_one(
                    variant,
                    worker_count,
                    args.merges_per_worker,
                    repetition,
                    args,
                    state_root,
                )
                rows.append(row)
                status = row.get("status", "ok")
                throughput = row.get("throughput_ops_s")
                print(
                    f"{variant.label:34s} workers={worker_count:2d} "
                    f"rep={repetition:2d} status={status:5s} "
                    f"throughput={throughput if throughput is not None else 'n/a'}"
                )

    (output / "results.json").write_text(
        json.dumps(rows, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    fieldnames = sorted({key for row in rows for key in row if key != "records"})
    with (output / "results.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    figures: list[str] = []
    try:
        figures = _write_figures(rows, output)
    except ImportError as exc:
        (output / "figure-error.txt").write_text(
            f"matplotlib is unavailable: {exc}\n", encoding="utf-8"
        )

    summary = {
        "schema": "merge-throughput-v1",
        "variants": [variant.label for variant in variants],
        "worker_counts": workers,
        "merges_per_worker": args.merges_per_worker,
        "merge_retries": args.merge_retries,
        "repetitions": args.repetitions,
        "timed_operation": "merge_preview + merge",
        "llm_inference_included": False,
        "setup_included": False,
        "figures": figures,
        "results_file": str(output / "results.json"),
    }
    (output / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0 if all(row.get("status", "ok") == "ok" for row in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
