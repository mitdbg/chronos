from __future__ import annotations

import argparse
import csv
import json
import math
import os
import statistics
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from janus_core.branching import JanusBranchContext


BACKENDS = ("copy", "interval", "log")
METRICS = (
    "branch_create",
    "branch_delete",
    "point_read",
    "join_aggregate_read",
    "update_write",
    "insert_write",
    "delete_write",
)


@dataclass(frozen=True)
class BenchCase:
    backend: str
    dataset_size: int
    depth: int


def parse_int_list(value: str) -> list[int]:
    parsed = [int(part.strip()) for part in value.split(",") if part.strip()]
    if not parsed:
        raise argparse.ArgumentTypeError("expected at least one integer")
    if any(item < 0 for item in parsed):
        raise argparse.ArgumentTypeError("integer lists must be non-negative")
    return parsed


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * pct
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def stats_from_timings(timings_ms: list[float], total_ms: float | None = None) -> dict[str, float]:
    if total_ms is None:
        total_ms = sum(timings_ms)
    return {
        "operations": float(len(timings_ms)),
        "median_ms": statistics.median(timings_ms) if timings_ms else 0.0,
        "avg_ms": statistics.fmean(timings_ms) if timings_ms else 0.0,
        "p95_ms": percentile(timings_ms, 0.95),
        "total_ms": total_ms,
    }


def measure_each(ops: list[Callable[[], Any]]) -> dict[str, float]:
    timings_ms: list[float] = []
    start_total = time.perf_counter_ns()
    for op in ops:
        start = time.perf_counter_ns()
        op()
        timings_ms.append((time.perf_counter_ns() - start) / 1_000_000)
    total_ms = (time.perf_counter_ns() - start_total) / 1_000_000
    return stats_from_timings(timings_ms, total_ms)


def measure_each_in_transaction(session: Any, ops: list[Callable[[], Any]]) -> dict[str, float]:
    # The branching API is expected to support a batch of work on one checked
    # out branch. This helper measures per-operation latency while committing
    # the batch once, matching the intended agent workflow more closely than an
    # implicit commit after every statement.
    timings_ms: list[float] = []
    start_total = time.perf_counter_ns()
    with session.transaction():
        for op in ops:
            start = time.perf_counter_ns()
            op()
            timings_ms.append((time.perf_counter_ns() - start) / 1_000_000)
    total_ms = (time.perf_counter_ns() - start_total) / 1_000_000
    return stats_from_timings(timings_ms, total_ms)


def make_context(
    backend: str,
    dataset_size: int,
    database_url: str,
) -> JanusBranchContext:
    ctx = JanusBranchContext.connect(database_url, backend=backend)
    db = ctx.db
    if database_url.startswith(("postgres://", "postgresql://")):
        rows = db.execute(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
              AND (tablename IN ('products', 'orders') OR tablename LIKE '_janus%')
            """
        ).fetchall()
        for row in rows:
            db.drop_table(row["tablename"])
        db.commit()
        ctx.close()
        ctx = JanusBranchContext.connect(database_url, backend=backend)
        db = ctx.db
    db.execute(
        """
        CREATE TABLE products (
          sku TEXT PRIMARY KEY,
          category TEXT NOT NULL,
          price INTEGER NOT NULL,
          stock INTEGER NOT NULL
        )
        """
    )
    db.execute(
        """
        CREATE TABLE orders (
          order_id TEXT PRIMARY KEY,
          sku TEXT NOT NULL,
          quantity INTEGER NOT NULL
        )
        """
    )
    products = [
        (
            f"sku_{idx:08d}",
            f"cat_{idx % 16:02d}",
            10 + (idx % 200),
            1000 - (idx % 100),
        )
        for idx in range(dataset_size)
    ]
    orders = [
        (
            f"order_{idx:08d}",
            f"sku_{idx % dataset_size:08d}",
            1 + (idx % 5),
        )
        for idx in range(max(dataset_size, 1))
    ]
    db.executemany("INSERT INTO products VALUES (?, ?, ?, ?)", products)
    db.executemany("INSERT INTO orders VALUES (?, ?, ?)", orders)
    db.commit()
    ctx.register_table("products", ["sku"])
    ctx.register_table("orders", ["order_id"])
    ctx.create_index("products", ["sku"], name="products_sku_lookup")
    ctx.create_index("products", ["price", "category"], name="products_price_category")
    ctx.create_index("orders", ["sku"], name="orders_sku_lookup")
    return ctx


def mutate_branch_state(
    ctx: JanusBranchContext,
    branch_id: str,
    case: BenchCase,
    level: int,
    mutations_per_branch: int,
) -> None:
    if mutations_per_branch <= 0:
        return
    session = ctx.checkout(branch_id)
    mutation_count = min(mutations_per_branch, case.dataset_size)
    with session.transaction():
        for idx in range(mutation_count):
            base_idx = (level * mutations_per_branch + idx) % case.dataset_size
            sku = f"sku_{base_idx:08d}"
            order_id = f"order_{base_idx:08d}"
            fork_sku = f"fork_{level:04d}_{idx:04d}"
            fork_order = f"fork_order_{level:04d}_{idx:04d}"
            delete_idx = (case.dataset_size - 1 - base_idx) % case.dataset_size
            session.execute(
                """
                UPDATE products
                SET price = :price, stock = :stock
                WHERE sku = :sku
                """,
                {
                    "price": 5000 + level * 100 + idx,
                    "stock": 250 + idx,
                    "sku": sku,
                },
            )
            session.execute(
                "UPDATE orders SET quantity = :quantity WHERE order_id = :order_id",
                {"quantity": 7 + (idx % 3), "order_id": order_id},
            )
            session.execute(
                """
                INSERT INTO products (sku, category, price, stock)
                VALUES (:sku, :category, :price, :stock)
                """,
                {
                    "sku": fork_sku,
                    "category": f"fork_{level % 16:02d}",
                    "price": 6000 + idx,
                    "stock": 100,
                },
            )
            session.execute(
                """
                INSERT INTO orders (order_id, sku, quantity)
                VALUES (:order_id, :sku, :quantity)
                """,
                {"order_id": fork_order, "sku": fork_sku, "quantity": 2 + idx},
            )
            session.execute(
                "DELETE FROM orders WHERE order_id = :order_id",
                {"order_id": f"order_{delete_idx:08d}"},
            )


def build_depth_chain(
    ctx: JanusBranchContext,
    case: BenchCase,
    mutations_per_branch: int,
) -> tuple[str, dict[str, float], list[str]]:
    branch_names = [f"depth_{idx}" for idx in range(case.depth)]
    create_timings_ms: list[float] = []
    parent = "main"
    for level, branch in enumerate(branch_names):
        start = time.perf_counter_ns()
        ctx.create_branch(branch, from_branch=parent)
        create_timings_ms.append((time.perf_counter_ns() - start) / 1_000_000)
        mutate_branch_state(ctx, branch, case, level, mutations_per_branch)
        parent = branch
    terminal = branch_names[-1] if branch_names else "main"
    return terminal, stats_from_timings(create_timings_ms), branch_names


def benchmark_branch_deletes(
    ctx: JanusBranchContext, case: BenchCase, branch_names: list[str]
) -> dict[str, Any]:
    delete_ops = [
        (lambda branch=branch: ctx.delete_branch(branch))
        for branch in reversed(branch_names)
    ]
    delete_stats = measure_each(delete_ops)
    return result_row(case, "branch_delete", delete_stats)


def benchmark_reads(
    ctx: JanusBranchContext,
    branch_id: str,
    case: BenchCase,
    read_ops: int,
    include_join_aggregate: bool,
) -> list[dict[str, Any]]:
    # Checkout prepares backend metadata once. The measured reads reuse the
    # same session so interval segments or log lineage are not reloaded for
    # every query.
    session = ctx.checkout(branch_id)
    keys = [f"sku_{idx % case.dataset_size:08d}" for idx in range(read_ops)]

    point_ops = [
        (
            lambda key=key: session.query(
                "SELECT sku, price FROM products WHERE sku = :sku",
                {"sku": key},
            )
        )
        for key in keys
    ]
    point_stats = measure_each(point_ops)
    rows = [result_row(case, "point_read", point_stats)]

    if not include_join_aggregate:
        return rows

    aggregate_sql = """
        SELECT p.category, COUNT(*) AS rows, SUM(p.price * o.quantity) AS revenue
        FROM products AS p
        JOIN orders AS o ON o.sku = p.sku
        WHERE p.price >= :min_price
        GROUP BY p.category
        ORDER BY revenue DESC
        LIMIT 8
    """
    aggregate_ops = [
        (
            lambda min_price=10 + (idx % 150): session.query(
                aggregate_sql, {"min_price": min_price}
            )
        )
        for idx in range(read_ops)
    ]
    aggregate_stats = measure_each(aggregate_ops)
    rows.append(result_row(case, "join_aggregate_read", aggregate_stats))

    return rows


def benchmark_writes(
    ctx: JanusBranchContext,
    branch_id: str,
    case: BenchCase,
    write_ops: int,
) -> list[dict[str, Any]]:
    # Writes are measured on the terminal branch after the depth chain has been
    # built and mutated. Each write category uses one transaction to isolate
    # branch-backend costs from repeated commit overhead.
    session = ctx.checkout(branch_id)
    update_count = min(write_ops, case.dataset_size)
    delete_count = min(write_ops, case.dataset_size)

    update_ops = [
        (
            lambda idx=idx: session.execute(
                "UPDATE products SET price = :price WHERE sku = :sku",
                {"price": 1000 + idx, "sku": f"sku_{idx:08d}"},
            )
        )
        for idx in range(update_count)
    ]
    update_stats = measure_each_in_transaction(session, update_ops)

    insert_ops = [
        (
            lambda idx=idx: session.execute(
                """
                INSERT INTO products (sku, category, price, stock)
                VALUES (:sku, :category, :price, :stock)
                """,
                {
                    "sku": f"new_{case.depth}_{idx:08d}",
                    "category": f"new_{idx % 8:02d}",
                    "price": 2000 + idx,
                    "stock": 500,
                },
            )
        )
        for idx in range(write_ops)
    ]
    insert_stats = measure_each_in_transaction(session, insert_ops)

    delete_ops = [
        (
            lambda idx=idx: session.execute(
                "DELETE FROM products WHERE sku = :sku",
                {"sku": f"sku_{case.dataset_size - 1 - idx:08d}"},
            )
        )
        for idx in range(delete_count)
    ]
    delete_stats = measure_each_in_transaction(session, delete_ops)

    return [
        result_row(case, "update_write", update_stats),
        result_row(case, "insert_write", insert_stats),
        result_row(case, "delete_write", delete_stats),
    ]


def result_row(
    case: BenchCase,
    metric: str,
    stats: dict[str, float],
) -> dict[str, Any]:
    return {
        "backend": case.backend,
        "dataset_size": case.dataset_size,
        "depth": case.depth,
        "metric": metric,
        "operations": int(stats["operations"]),
        "median_ms": stats["median_ms"],
        "avg_ms": stats["avg_ms"],
        "p95_ms": stats["p95_ms"],
        "total_ms": stats["total_ms"],
    }


def run_case(
    case: BenchCase,
    database_url: str,
    read_ops: int,
    write_ops: int,
    mutations_per_branch: int,
    include_join_aggregate: bool,
) -> list[dict[str, Any]]:
    ctx = make_context(case.backend, case.dataset_size, database_url)
    try:
        terminal_branch, create_stats, branch_names = build_depth_chain(
            ctx, case, mutations_per_branch
        )
        rows = [result_row(case, "branch_create", create_stats)]
        rows.extend(
            benchmark_reads(
                ctx,
                terminal_branch,
                case,
                read_ops,
                include_join_aggregate,
            )
        )
        rows.extend(benchmark_writes(ctx, terminal_branch, case, write_ops))
        rows.append(benchmark_branch_deletes(ctx, case, branch_names))
        return rows
    finally:
        ctx.close()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "backend",
        "dataset_size",
        "depth",
        "metric",
        "operations",
        "median_ms",
        "avg_ms",
        "p95_ms",
        "total_ms",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, rows: list[dict[str, Any]], config: dict[str, Any]) -> None:
    path.write_text(
        json.dumps({"config": config, "results": rows}, indent=2, sort_keys=True)
    )


def config_label(row: dict[str, Any]) -> str:
    return f"N={row['dataset_size']} d={row['depth']}"


def write_metric_plot(output_dir: Path, rows: list[dict[str, Any]], metric: str) -> Path:
    metric_rows = [row for row in rows if row["metric"] == metric]
    groups = sorted({config_label(row) for row in metric_rows})
    values = {
        (config_label(row), row["backend"]): float(row["median_ms"])
        for row in metric_rows
    }
    x_positions = list(range(len(groups)))
    width = min(0.8 / len(BACKENDS), 0.28)
    fig_width = max(10, len(groups) * 0.9)
    fig, ax = plt.subplots(figsize=(fig_width, 5.5), constrained_layout=True)
    colors = {"copy": "#16a34a", "interval": "#2563eb", "log": "#dc2626"}
    for backend_idx, backend in enumerate(BACKENDS):
        offset = (backend_idx - (len(BACKENDS) - 1) / 2) * width
        y_values = [values.get((group, backend), 0.0) for group in groups]
        ax.bar(
            [x + offset for x in x_positions],
            y_values,
            width=width,
            label=backend,
            color=colors[backend],
        )
    ax.set_title(metric.replace("_", " ").title())
    ax.set_ylabel("Median milliseconds per operation")
    ax.set_xlabel("Dataset / branch depth")
    ax.set_xticks(x_positions)
    ax.set_xticklabels(groups, rotation=35, ha="right")
    ax.grid(axis="y", alpha=0.25)
    ax.legend()
    output_path = output_dir / f"{metric}.png"
    fig.savefig(output_path, dpi=160)
    plt.close(fig)
    return output_path


def write_summary_plot(output_dir: Path, rows: list[dict[str, Any]]) -> Path:
    summary: dict[tuple[str, str], list[float]] = {}
    for row in rows:
        summary.setdefault((row["metric"], row["backend"]), []).append(
            float(row["median_ms"])
        )
    metrics = [metric for metric in METRICS if any(row["metric"] == metric for row in rows)]
    x_positions = list(range(len(metrics)))
    width = min(0.8 / len(BACKENDS), 0.28)
    fig, ax = plt.subplots(figsize=(12, 5.5), constrained_layout=True)
    colors = {"copy": "#16a34a", "interval": "#2563eb", "log": "#dc2626"}
    for backend_idx, backend in enumerate(BACKENDS):
        offset = (backend_idx - (len(BACKENDS) - 1) / 2) * width
        y_values = [
            statistics.median(summary.get((metric, backend), [0.0]))
            for metric in metrics
        ]
        ax.bar(
            [x + offset for x in x_positions],
            y_values,
            width=width,
            label=backend,
            color=colors[backend],
        )
    ax.set_title("Backend Comparison Summary")
    ax.set_ylabel("Median of per-case median milliseconds")
    ax.set_xticks(x_positions)
    ax.set_xticklabels([metric.replace("_", "\n") for metric in metrics])
    ax.grid(axis="y", alpha=0.25)
    ax.legend()
    output_path = output_dir / "summary.png"
    fig.savefig(output_path, dpi=160)
    plt.close(fig)
    return output_path


def write_plots(output_dir: Path, rows: list[dict[str, Any]]) -> list[Path]:
    paths = [write_summary_plot(output_dir, rows)]
    paths.extend(
        write_metric_plot(output_dir, rows, metric)
        for metric in METRICS
        if any(row["metric"] == metric for row in rows)
    )
    return paths


def write_markdown_summary(
    path: Path,
    rows: list[dict[str, Any]],
    config: dict[str, Any],
    plot_paths: list[Path],
) -> None:
    image_lines = "\n".join(
        f"![{plot.stem}]({plot.name})" for plot in plot_paths
    )
    path.write_text(
        "# Janus Branching Backend Benchmark\n\n"
        f"Generated at `{config['generated_at']}`.\n\n"
        "## Config\n\n"
        f"```json\n{json.dumps(config, indent=2, sort_keys=True)}\n```\n\n"
        "## Plots\n\n"
        f"{image_lines}\n\n"
        "Raw results are in `results.csv` and `results.json`.\n"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark Janus branch physical backends."
    )
    parser.add_argument("--backends", default="copy,interval,log")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("JANUS_BRANCH_DATABASE_URL", "sqlite:///:memory:"),
        help=(
            "SQL database URL for benchmark setup. Defaults to sqlite:///:memory:. "
            "Set JANUS_BRANCH_DATABASE_URL to use PostgreSQL without changing commands."
        ),
    )
    parser.add_argument("--dataset-sizes", type=parse_int_list, default=[100, 1000])
    parser.add_argument("--depths", type=parse_int_list, default=[0, 4, 8])
    parser.add_argument("--read-ops", type=int, default=20)
    parser.add_argument("--write-ops", type=int, default=20)
    parser.add_argument(
        "--branch-mutations",
        type=int,
        default=5,
        help="Number of update/insert/delete mutation groups applied after each branch creation.",
    )
    parser.add_argument(
        "--include-join-aggregate",
        action="store_true",
        help="Also run the expensive join/group/order read benchmark.",
    )
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--output-dir", type=Path)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    backends = tuple(backend.strip() for backend in args.backends.split(",") if backend.strip())
    unknown = set(backends) - set(BACKENDS)
    if unknown:
        raise SystemExit(f"unknown backend(s): {', '.join(sorted(unknown))}")
    if args.quick:
        dataset_sizes = [100]
        depths = [0, 4]
        read_ops = min(args.read_ops, 5)
        write_ops = min(args.write_ops, 5)
        branch_mutations = min(args.branch_mutations, 3)
    else:
        dataset_sizes = args.dataset_sizes
        depths = args.depths
        read_ops = args.read_ops
        write_ops = args.write_ops
        branch_mutations = args.branch_mutations
    if min(dataset_sizes) <= 0:
        raise SystemExit("dataset sizes must be positive")
    if branch_mutations < 0:
        raise SystemExit("branch mutations must be non-negative")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = args.output_dir or Path(".benchmarks") / f"branching-{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "backends": list(backends),
        "dataset_sizes": dataset_sizes,
        "depths": depths,
        "read_ops": read_ops,
        "write_ops": write_ops,
        "branch_mutations": branch_mutations,
        "include_join_aggregate": args.include_join_aggregate,
        "database_url": args.database_url,
    }
    rows: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="janus-branch-bench-"):
        for backend in backends:
            for dataset_size in dataset_sizes:
                for depth in depths:
                    case = BenchCase(backend, dataset_size, depth)
                    print(
                        f"backend={backend} dataset={dataset_size} depth={depth}",
                        flush=True,
                    )
                    rows.extend(
                        run_case(
                            case,
                            args.database_url,
                            read_ops,
                            write_ops,
                            branch_mutations,
                            args.include_join_aggregate,
                        )
                    )

    write_csv(output_dir / "results.csv", rows)
    write_json(output_dir / "results.json", rows, config)
    plot_paths = write_plots(output_dir, rows)
    write_markdown_summary(output_dir / "README.md", rows, config, plot_paths)
    print(f"\nWrote benchmark results to {output_dir}")
    print(f"Wrote matplotlib plots: {', '.join(path.name for path in plot_paths)}")


if __name__ == "__main__":
    main()
