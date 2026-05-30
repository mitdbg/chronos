from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import statistics
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterator

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from chronos_core.branching import ChronosBranchContext, UnsupportedSQLError
from chronos_core.branching.sql_adapters import SQLDatabaseAdapter, connect_sql_database


BACKENDS = ("interval", "copy", "doltgres")
DDL_VARIANTS = ("no_default", "default")
METRICS = (
    "branch_create",
    "add_column_ddl",
    "first_point_read_new_column",
    "point_read_new_column",
    "range_read_new_column",
    "count_new_column",
    "update_new_column",
    "insert_after_schema_change",
)
CSV_FIELDS = [
    "backend",
    "ddl_variant",
    "dataset_size",
    "metric",
    "operations",
    "median_ms",
    "avg_ms",
    "p95_ms",
    "total_ms",
    "status",
    "error",
]


@dataclass(frozen=True)
class SchemaBenchCase:
    backend: str
    ddl_variant: str
    dataset_size: int


class DoltgresSession:
    def __init__(self, context: DoltgresBranchContext, branch_id: str):
        self._context = context
        self.branch_id = branch_id
        self._transaction_depth = 0

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        rows = self._context.db.execute(sql, params or {}).fetchall()
        return [dict(row) for row in rows]

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> Any:
        cursor = self._context.db.execute(sql, params or {})
        if self._transaction_depth == 0:
            self._context.db.commit()
        return cursor

    @contextmanager
    def transaction(self) -> Iterator[None]:
        root = self._transaction_depth == 0
        if root:
            if self._context.db.in_transaction:
                self._context.db.commit()
            self._context.db.begin()
        self._transaction_depth += 1
        try:
            yield
        except Exception:
            self._transaction_depth -= 1
            if root:
                self._context.db.rollback()
            raise
        else:
            self._transaction_depth -= 1
            if root:
                self._context.db.commit()


class DoltgresBranchContext:
    backend_name = "doltgres"

    def __init__(self, db: SQLDatabaseAdapter):
        self.db = db
        self._active_branch: str | None = None

    @classmethod
    def connect(cls, database_url: str) -> DoltgresBranchContext:
        context = cls(connect_sql_database(database_url))
        context._reset_for_benchmark()
        return context

    def checkout(self, branch_id: str) -> DoltgresSession:
        self._checkout(branch_id)
        return DoltgresSession(self, branch_id)

    def create_branch(self, branch_id: str, from_branch: str) -> None:
        self._checkout(from_branch)
        self.db.execute("SELECT dolt_checkout('-b', ?)", (branch_id,))
        self.db.commit()
        self._active_branch = branch_id

    def commit_working_set(self, branch_id: str, message: str) -> None:
        self._checkout(branch_id)
        self.db.execute("SELECT dolt_commit('-A', '-m', ?)", (message,))
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def _checkout(self, branch_id: str) -> None:
        if self._active_branch == branch_id:
            return
        self.db.execute("SELECT dolt_checkout(?)", (branch_id,))
        self.db.commit()
        self._active_branch = branch_id

    def _reset_for_benchmark(self) -> None:
        self.db.execute("SELECT dolt_checkout('main')")
        self.db.commit()
        branches = self.db.execute(
            "SELECT name FROM dolt_branches WHERE name <> 'main'"
        ).fetchall()
        for row in branches:
            self.db.execute("SELECT dolt_branch('-D', ?)", (row["name"],))
            self.db.commit()
        self.db.execute("DROP TABLE IF EXISTS products")
        self.db.commit()
        self._commit_if_changed("reset schema branching benchmark")
        self._active_branch = "main"

    def _commit_if_changed(self, message: str) -> None:
        try:
            self.db.execute("SELECT dolt_commit('-A', '-m', ?)", (message,))
            self.db.commit()
        except Exception:
            self.db.rollback()


def parse_int_list(value: str) -> list[int]:
    parsed = [int(part.strip()) for part in value.split(",") if part.strip()]
    if not parsed:
        raise argparse.ArgumentTypeError("expected at least one integer")
    if any(item <= 0 for item in parsed):
        raise argparse.ArgumentTypeError("dataset sizes must be positive")
    return parsed


def parse_csv(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in value.split(",") if part.strip())


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


def progress(message: str) -> None:
    print(f"  progress: {message}", flush=True)


def case_label(case: SchemaBenchCase) -> str:
    return (
        f"backend={case.backend} ddl={case.ddl_variant} "
        f"dataset={case.dataset_size}"
    )


def result_row(
    case: SchemaBenchCase,
    metric: str,
    stats: dict[str, float],
    *,
    status: str = "ok",
    error: str = "",
) -> dict[str, Any]:
    return {
        "backend": case.backend,
        "ddl_variant": case.ddl_variant,
        "dataset_size": case.dataset_size,
        "metric": metric,
        "operations": int(stats["operations"]),
        "median_ms": stats["median_ms"],
        "avg_ms": stats["avg_ms"],
        "p95_ms": stats["p95_ms"],
        "total_ms": stats["total_ms"],
        "status": status,
        "error": error,
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


def run_warmup(label: str, ops: list[Callable[[], Any]], warmup_ops: int) -> None:
    if warmup_ops <= 0 or not ops:
        return
    progress(f"{label} warmup start ops={min(warmup_ops, len(ops))}")
    for op in ops[:warmup_ops]:
        op()


def measure_in_transaction(session: Any, ops: list[Callable[[], Any]]) -> dict[str, float]:
    timings_ms: list[float] = []
    start_total = time.perf_counter_ns()
    with session.transaction():
        for op in ops:
            start = time.perf_counter_ns()
            op()
            timings_ms.append((time.perf_counter_ns() - start) / 1_000_000)
    total_ms = (time.perf_counter_ns() - start_total) / 1_000_000
    return stats_from_timings(timings_ms, total_ms)


def reset_postgres_schema(db: SQLDatabaseAdapter) -> None:
    rows = db.execute(
        """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        """
    ).fetchall()
    for row in rows:
        db.drop_table(row["tablename"])
    db.commit()


def product_rows(start: int, stop: int) -> list[tuple[Any, ...]]:
    return [
        (
            f"sku_{idx:08d}",
            f"cat_{idx % 16:02d}",
            10 + (idx % 200),
            1000 - (idx % 100),
        )
        for idx in range(start, stop)
    ]


def insert_products_chunked(
    db: SQLDatabaseAdapter,
    dataset_size: int,
    *,
    chunk_size: int,
) -> None:
    width = 4
    for start in range(0, dataset_size, chunk_size):
        stop = min(start + chunk_size, dataset_size)
        chunk = product_rows(start, stop)
        placeholders = ", ".join(
            f"({', '.join('?' for _ in range(width))})" for _ in chunk
        )
        values = [value for row in chunk for value in row]
        db.execute(f"INSERT INTO products VALUES {placeholders}", values)
        progress(f"loaded rows={stop}/{dataset_size}")


def make_context(
    case: SchemaBenchCase,
    database_url: str,
    *,
    insert_chunk_size: int,
) -> ChronosBranchContext | DoltgresBranchContext:
    progress(f"{case_label(case)} context start")
    if case.backend == "doltgres":
        ctx = DoltgresBranchContext.connect(database_url)
        db = ctx.db
    else:
        ctx = ChronosBranchContext.connect(
            database_url,
            backend=case.backend,
            enable_schema_branching=True,
        )
        db = ctx.db
        if db.dialect == "postgres":
            reset_postgres_schema(db)
            ctx.close()
            ctx = ChronosBranchContext.connect(
                database_url,
                backend=case.backend,
                enable_schema_branching=True,
            )
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
    insert_products_chunked(db, case.dataset_size, chunk_size=insert_chunk_size)
    db.execute("CREATE INDEX products_price_category ON products (price, category)")
    db.commit()
    if isinstance(ctx, DoltgresBranchContext):
        ctx.commit_working_set("main", "schema branching benchmark base")
    else:
        ctx.register_table("products", ["sku"])
        ctx.create_index("products", ["price", "category"], name="products_price_category")
    progress(f"{case_label(case)} context ready")
    return ctx


def schema_column(case: SchemaBenchCase) -> str:
    return f"bench_{case.ddl_variant}"


def alter_sql(case: SchemaBenchCase) -> str:
    column = schema_column(case)
    if case.ddl_variant == "no_default":
        return f"ALTER TABLE products ADD COLUMN {column} INTEGER"
    if case.ddl_variant == "default":
        return f"ALTER TABLE products ADD COLUMN {column} INTEGER DEFAULT 7"
    raise ValueError(f"unknown DDL variant: {case.ddl_variant}")


def expected_new_column_value(case: SchemaBenchCase) -> int | None:
    if case.ddl_variant == "default":
        return 7
    return None


def deterministic_skus(case: SchemaBenchCase, metric: str, count: int, *, unique: bool) -> list[str]:
    rng = random.Random(f"schema-branch:{case.backend}:{case.ddl_variant}:{case.dataset_size}:{metric}")
    if unique:
        indexes = rng.sample(range(case.dataset_size), min(count, case.dataset_size))
    else:
        indexes = [rng.randrange(case.dataset_size) for _ in range(count)]
    return [f"sku_{idx:08d}" for idx in indexes]


def unsupported_row(case: SchemaBenchCase, elapsed_ms: float, exc: Exception) -> dict[str, Any]:
    status = "unsupported" if isinstance(exc, UnsupportedSQLError) else "error"
    return result_row(
        case,
        "add_column_ddl",
        stats_from_timings([elapsed_ms]),
        status=status,
        error=f"{type(exc).__name__}: {exc}",
    )


def validate_first_read(
    rows: list[dict[str, Any]],
    case: SchemaBenchCase,
) -> None:
    if len(rows) != 1:
        raise RuntimeError(f"expected one row, got {len(rows)}")
    value = rows[0][schema_column(case)]
    if value != expected_new_column_value(case):
        raise RuntimeError(
            f"unexpected {schema_column(case)} value: {value!r}; "
            f"expected {expected_new_column_value(case)!r}"
        )


def run_case(
    case: SchemaBenchCase,
    database_url: str,
    *,
    read_ops: int,
    range_read_ops: int,
    write_ops: int,
    warmup_ops: int,
    insert_chunk_size: int,
) -> list[dict[str, Any]]:
    ctx = make_context(case, database_url, insert_chunk_size=insert_chunk_size)
    rows: list[dict[str, Any]] = []
    try:
        start = time.perf_counter_ns()
        ctx.create_branch("schema_exp", from_branch="main")
        rows.append(
            result_row(
                case,
                "branch_create",
                stats_from_timings([(time.perf_counter_ns() - start) / 1_000_000]),
            )
        )
        session = ctx.checkout("schema_exp")

        progress(f"{case_label(case)} alter start sql={alter_sql(case)!r}")
        start = time.perf_counter_ns()
        try:
            session.execute(alter_sql(case))
        except Exception as exc:
            elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
            rows.append(unsupported_row(case, elapsed_ms, exc))
            progress(f"{case_label(case)} alter failed {type(exc).__name__}: {exc}")
            return rows
        rows.append(
            result_row(
                case,
                "add_column_ddl",
                stats_from_timings([(time.perf_counter_ns() - start) / 1_000_000]),
            )
        )
        if isinstance(ctx, DoltgresBranchContext):
            ctx.commit_working_set("schema_exp", "schema branch add column")

        column = schema_column(case)
        first_key = f"sku_{case.dataset_size // 2:08d}"
        first_stats = measure_each(
            [
                lambda: validate_first_read(
                    session.query(
                        f"SELECT sku, {column} FROM products WHERE sku = :sku",
                        {"sku": first_key},
                    ),
                    case,
                )
            ]
        )
        rows.append(result_row(case, "first_point_read_new_column", first_stats))

        point_keys = deterministic_skus(case, "point_read", read_ops, unique=False)
        point_read_ops = [
            (
                lambda key=key: session.query(
                    f"SELECT sku, {column} FROM products WHERE sku = :sku",
                    {"sku": key},
                )
            )
            for key in point_keys
        ]
        run_warmup(
            f"{case_label(case)} point_read_new_column",
            point_read_ops,
            warmup_ops,
        )
        rows.append(
            result_row(
                case,
                "point_read_new_column",
                measure_each(point_read_ops),
            )
        )

        range_width = min(100, case.dataset_size)
        max_start = max(case.dataset_size - range_width, 0)
        range_rng = random.Random(
            f"schema-branch:{case.backend}:{case.ddl_variant}:{case.dataset_size}:range"
        )
        range_starts = [range_rng.randint(0, max_start) for _ in range(range_read_ops)]
        range_read_callables = [
            (
                lambda start=start: session.query(
                    f"""
                    SELECT sku, {column}
                    FROM products
                    WHERE sku >= :start_sku AND sku <= :end_sku
                    LIMIT 100
                    """,
                    {
                        "start_sku": f"sku_{start:08d}",
                        "end_sku": f"sku_{min(start + range_width - 1, case.dataset_size - 1):08d}",
                    },
                )
            )
            for start in range_starts
        ]
        run_warmup(
            f"{case_label(case)} range_read_new_column",
            range_read_callables,
            warmup_ops,
        )
        rows.append(
            result_row(
                case,
                "range_read_new_column",
                measure_each(range_read_callables),
            )
        )

        if case.ddl_variant == "default":
            count_sql = f"SELECT count(*) AS rows FROM products WHERE {column} = 7"
        else:
            count_sql = f"SELECT count(*) AS rows FROM products WHERE {column} IS NULL"
        count_ops = [lambda: session.query(count_sql)]
        run_warmup(f"{case_label(case)} count_new_column", count_ops, warmup_ops)
        rows.append(
            result_row(
                case,
                "count_new_column",
                measure_each(count_ops),
            )
        )

        update_keys = deterministic_skus(case, "update", write_ops, unique=True)
        update_ops = [
            (
                lambda idx=idx, key=key: session.execute(
                    f"UPDATE products SET {column} = :value WHERE sku = :sku",
                    {"value": 1000 + idx, "sku": key},
                )
            )
            for idx, key in enumerate(update_keys)
        ]
        update_warmup_key = f"sku_{0:08d}"
        run_warmup(
            f"{case_label(case)} update_new_column",
            [
                lambda: session.execute(
                    f"UPDATE products SET {column} = :value WHERE sku = :sku",
                    {"value": -1, "sku": update_warmup_key},
                )
            ],
            warmup_ops,
        )
        rows.append(
            result_row(
                case,
                "update_new_column",
                measure_in_transaction(session, update_ops),
            )
        )

        insert_count = max(0, write_ops)
        insert_ops = [
            (
                lambda idx=idx: session.execute(
                    f"""
                    INSERT INTO products
                    (sku, category, price, stock, {column})
                    VALUES (:sku, :category, :price, :stock, :value)
                    """,
                    {
                        "sku": f"new_{case.ddl_variant}_{idx:08d}",
                        "category": "new",
                        "price": 5000 + idx,
                        "stock": 1,
                        "value": 2000 + idx,
                    },
                )
            )
            for idx in range(insert_count)
        ]
        run_warmup(
            f"{case_label(case)} insert_after_schema_change",
            [
                lambda: session.execute(
                    f"""
                    INSERT INTO products
                    (sku, category, price, stock, {column})
                    VALUES (:sku, :category, :price, :stock, :value)
                    """,
                    {
                        "sku": f"warmup_{case.ddl_variant}",
                        "category": "warmup",
                        "price": -1,
                        "stock": 0,
                        "value": -1,
                    },
                )
            ],
            warmup_ops,
        )
        rows.append(
            result_row(
                case,
                "insert_after_schema_change",
                measure_in_transaction(session, insert_ops),
            )
        )
        return rows
    finally:
        ctx.close()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def init_stream_csv(path: Path) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        handle.flush()


def append_csv_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writerows(rows)
        handle.flush()


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def write_json(path: Path, rows: list[dict[str, Any]], config: dict[str, Any]) -> None:
    path.write_text(
        json.dumps({"config": config, "results": rows}, indent=2, sort_keys=True)
    )


def write_metric_plot(output_dir: Path, rows: list[dict[str, Any]], metric: str) -> Path:
    metric_rows = [
        row for row in rows if row["metric"] == metric and row["status"] == "ok"
    ]
    groups = sorted({int(row["dataset_size"]) for row in metric_rows})
    series = sorted(
        {
            (row["backend"], row["ddl_variant"])
            for row in metric_rows
        }
    )
    values = {
        (int(row["dataset_size"]), row["backend"], row["ddl_variant"]): float(row["median_ms"])
        for row in metric_rows
    }
    positive_values = [value for value in values.values() if value > 0]
    fig_width = max(10, len(groups) * 1.1)
    fig, ax = plt.subplots(figsize=(fig_width, 5.5), constrained_layout=True)
    x_positions = list(range(len(groups)))
    width = min(0.8 / max(len(series), 1), 0.18)
    colors = {
        ("interval", "no_default"): "#2563eb",
        ("interval", "default"): "#60a5fa",
        ("copy", "no_default"): "#16a34a",
        ("copy", "default"): "#86efac",
        ("doltgres", "no_default"): "#7c3aed",
        ("doltgres", "default"): "#c084fc",
    }
    for idx, (backend, ddl_variant) in enumerate(series):
        offset = (idx - (len(series) - 1) / 2) * width
        y_values = [
            values.get((dataset_size, backend, ddl_variant), 0.0)
            for dataset_size in groups
        ]
        ax.bar(
            [x + offset for x in x_positions],
            y_values,
            width=width,
            label=f"{backend} {ddl_variant}",
            color=colors.get((backend, ddl_variant), "#525252"),
        )
    ax.set_title(metric.replace("_", " ").title())
    if positive_values and max(positive_values) / min(positive_values) >= 50:
        ax.set_yscale("log")
        ax.set_ylabel("Median milliseconds (log scale)")
    else:
        ax.set_ylabel("Median milliseconds")
    ax.set_xlabel("Dataset rows")
    ax.set_xticks(x_positions)
    ax.set_xticklabels([str(group) for group in groups], rotation=25, ha="right")
    ax.grid(axis="y", alpha=0.25)
    ax.legend()
    output_path = output_dir / f"{metric}.png"
    fig.savefig(output_path, dpi=160)
    plt.close(fig)
    return output_path


def write_plots(output_dir: Path, rows: list[dict[str, Any]]) -> list[Path]:
    return [
        write_metric_plot(output_dir, rows, metric)
        for metric in METRICS
        if any(row["metric"] == metric and row["status"] == "ok" for row in rows)
    ]


def write_markdown_summary(
    path: Path,
    rows: list[dict[str, Any]],
    config: dict[str, Any],
    plot_paths: list[Path],
) -> None:
    unsupported = [
        row for row in rows if row["status"] != "ok"
    ]
    unsupported_lines = "\n".join(
        f"- `{row['backend']}` `{row['ddl_variant']}` N={row['dataset_size']}: "
        f"{row['status']} {row['error']}"
        for row in unsupported
    )
    image_lines = "\n".join(
        f"![{plot.stem}]({plot.name})" for plot in plot_paths
    )
    path.write_text(
        "# Schema Branching Benchmark\n\n"
        f"Generated at `{config['generated_at']}`.\n\n"
        "This benchmark compares branch-local `ALTER TABLE ... ADD COLUMN` "
        "on Chronos interval, Chronos copy, and native Doltgres branches. "
        "`default` uses `ADD COLUMN bench_default INTEGER DEFAULT 7`; "
        "`no_default` uses `ADD COLUMN bench_no_default INTEGER`.\n\n"
        "## Config\n\n"
        f"```json\n{json.dumps(config, indent=2, sort_keys=True)}\n```\n\n"
        "## Unsupported Or Failed Cases\n\n"
        f"{unsupported_lines or '- none'}\n\n"
        "## Plots\n\n"
        f"{image_lines}\n\n"
        "Raw results are in `results.csv` and `results.json`.\n"
    )


def write_summary_from_existing_results(output_dir: Path) -> None:
    results_path = output_dir / "results.csv"
    if not results_path.exists():
        raise SystemExit(f"missing results.csv: {results_path}")
    rows = read_csv(results_path)
    config = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": str(results_path),
    }
    write_json(output_dir / "results.json", rows, config)
    plot_paths = write_plots(output_dir, rows)
    write_markdown_summary(output_dir / "README.md", rows, config, plot_paths)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark branch-local schema changes across Chronos and Doltgres."
    )
    parser.add_argument("--backends", default="interval,copy")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("CHRONOS_BRANCH_DATABASE_URL", "sqlite:///:memory:"),
    )
    parser.add_argument("--dataset-sizes", type=parse_int_list, default=[100000, 1000000])
    parser.add_argument("--ddl-variants", default="no_default,default")
    parser.add_argument("--read-ops", type=int, default=100)
    parser.add_argument("--range-read-ops", type=int, default=20)
    parser.add_argument("--write-ops", type=int, default=100)
    parser.add_argument(
        "--warmup-ops",
        type=int,
        default=1,
        help=(
            "Unmeasured operations to run before each steady-state phase. "
            "The cold first read after DDL remains separately measured."
        ),
    )
    parser.add_argument("--insert-chunk-size", type=int, default=5000)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--summarize-existing", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.summarize_existing:
        if args.output_dir is None:
            raise SystemExit("--summarize-existing requires --output-dir")
        write_summary_from_existing_results(args.output_dir)
        return

    backends = parse_csv(args.backends)
    ddl_variants = parse_csv(args.ddl_variants)
    unknown_backends = set(backends) - set(BACKENDS)
    if unknown_backends:
        raise SystemExit(f"unknown backend(s): {', '.join(sorted(unknown_backends))}")
    unknown_variants = set(ddl_variants) - set(DDL_VARIANTS)
    if unknown_variants:
        raise SystemExit(f"unknown DDL variant(s): {', '.join(sorted(unknown_variants))}")

    if args.quick:
        dataset_sizes = [1000] if args.dataset_sizes == [100000, 1000000] else args.dataset_sizes
        read_ops = min(args.read_ops, 5)
        range_read_ops = min(args.range_read_ops, 3)
        write_ops = min(args.write_ops, 5)
        warmup_ops = min(args.warmup_ops, 1)
    else:
        dataset_sizes = args.dataset_sizes
        read_ops = args.read_ops
        range_read_ops = args.range_read_ops
        write_ops = args.write_ops
        warmup_ops = args.warmup_ops

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = args.output_dir or Path(".benchmarks") / f"schema-branching-{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    results_csv = output_dir / "results.csv"
    init_stream_csv(results_csv)
    config = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "backends": list(backends),
        "ddl_variants": list(ddl_variants),
        "dataset_sizes": dataset_sizes,
        "read_ops": read_ops,
        "range_read_ops": range_read_ops,
        "write_ops": write_ops,
        "warmup_ops": warmup_ops,
        "insert_chunk_size": args.insert_chunk_size,
        "database_url": args.database_url,
    }
    rows: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="chronos-schema-branch-bench-"):
        for backend in backends:
            for dataset_size in dataset_sizes:
                for ddl_variant in ddl_variants:
                    case = SchemaBenchCase(
                        backend=backend,
                        ddl_variant=ddl_variant,
                        dataset_size=dataset_size,
                    )
                    print(
                        f"backend={backend} ddl={ddl_variant} dataset={dataset_size}",
                        flush=True,
                    )
                    case_rows = run_case(
                        case,
                        args.database_url,
                        read_ops=read_ops,
                        range_read_ops=range_read_ops,
                        write_ops=write_ops,
                        warmup_ops=warmup_ops,
                        insert_chunk_size=args.insert_chunk_size,
                    )
                    rows.extend(case_rows)
                    append_csv_rows(results_csv, case_rows)

    write_json(output_dir / "results.json", rows, config)
    plot_paths = write_plots(output_dir, rows)
    write_markdown_summary(output_dir / "README.md", rows, config, plot_paths)
    print(f"\nWrote schema branching benchmark results to {output_dir}")
    print(f"Wrote matplotlib plots: {', '.join(path.name for path in plot_paths)}")


if __name__ == "__main__":
    main()
