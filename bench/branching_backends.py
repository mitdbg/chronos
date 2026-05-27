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

from chronos_core.branching import ChronosBranchContext
from chronos_core.branching.sql_adapters import SQLDatabaseAdapter, connect_sql_database


BACKENDS = ("copy", "interval", "log", "orpheus", "litetree", "doltgres")
COPY_MAX_BRANCH_SPAN = 32
BACKEND_LABELS = {
    "copy": "copy",
    "interval": "chronos",
    "log": "log",
    "orpheus": "orpheusdb",
    "litetree": "sqlite-branch",
    "doltgres": "doltgres",
}
METRICS = (
    "branch_create",
    "branch_delete",
    "point_read",
    "range_read",
    "join_aggregate_read",
    "update_write",
    "insert_write",
    "delete_write",
)
CSV_FIELDS = [
    "backend",
    "shape",
    "interval_continuation_percent",
    "dataset_size",
    "depth",
    "width",
    "span",
    "metric",
    "operations",
    "median_ms",
    "avg_ms",
    "p95_ms",
    "total_ms",
]


@dataclass(frozen=True)
class BenchCase:
    backend: str
    dataset_size: int
    depth: int
    width: int = 0
    shape: str = "depth"
    interval_continuation_percent: int = 5

    @property
    def span(self) -> int:
        return self.width if self.shape == "width" else self.depth


class DoltgresSession:
    """Checked-out Doltgres branch session used by the benchmark baseline."""

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
    """Native Doltgres branching adapter for benchmark comparison.

    Doltgres exposes branch operations as SQL functions. The benchmark uses the
    branch working set for regular reads and writes, and commits setup mutations
    only when creating a depth chain so child branches inherit parent changes.
    """

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

    def delete_branch(self, branch_id: str) -> None:
        if branch_id == "main":
            return
        self._checkout("main")
        self.db.execute("SELECT dolt_branch('-D', ?)", (branch_id,))
        self.db.commit()

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
        self.db.execute("DROP TABLE IF EXISTS orders")
        self.db.execute("DROP TABLE IF EXISTS products")
        self.db.commit()
        self._commit_if_changed("reset benchmark schema")
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


def progress(message: str) -> None:
    print(f"  progress: {message}", flush=True)


def case_label(case: BenchCase) -> str:
    dimension = f"width={case.width}" if case.shape == "width" else f"depth={case.depth}"
    return (
        f"backend={case.backend} shape={case.shape} "
        f"dataset={case.dataset_size} {dimension}"
    )


def progress_interval(case: BenchCase, operations: int) -> int:
    if operations <= 0:
        return 1
    return max(1, operations // 10)


def should_report_progress(index: int, total: int, every: int) -> bool:
    return index == 1 or index == total or index % every == 0


def progress_percent(index: int, total: int) -> str:
    if total <= 0:
        return "100%"
    percent = (index / total) * 100
    if percent < 1:
        return f"{percent:.1f}%"
    return f"{percent:.0f}%"


def measure_each(
    ops: list[Callable[[], Any]],
    *,
    label: str | None = None,
    report_every: int | None = None,
) -> dict[str, float]:
    timings_ms: list[float] = []
    start_total = time.perf_counter_ns()
    total = len(ops)
    for idx, op in enumerate(ops, start=1):
        if label and report_every and should_report_progress(idx, total, report_every):
            progress(
                f"{label} {progress_percent(idx, total)} "
                f"({idx}/{total}) start"
            )
        start = time.perf_counter_ns()
        op()
        elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
        timings_ms.append(elapsed_ms)
        if label and report_every and should_report_progress(idx, total, report_every):
            progress(
                f"{label} {progress_percent(idx, total)} "
                f"({idx}/{total}) done elapsed_ms={elapsed_ms:.3f}"
            )
    total_ms = (time.perf_counter_ns() - start_total) / 1_000_000
    if label:
        progress(f"{label} done operations={total} total_ms={total_ms:.3f}")
    return stats_from_timings(timings_ms, total_ms)


def warmup_each(
    ops: list[Callable[[], Any]],
    warmup_ops: int,
    *,
    label: str | None = None,
    report_every: int | None = None,
) -> None:
    if warmup_ops <= 0 or not ops:
        return
    for idx in range(warmup_ops):
        op_number = idx + 1
        if label and report_every and should_report_progress(
            op_number, warmup_ops, report_every
        ):
            progress(
                f"{label} warmup {progress_percent(op_number, warmup_ops)} "
                f"({op_number}/{warmup_ops}) start"
            )
        ops[idx % len(ops)]()
        if label and report_every and should_report_progress(
            op_number, warmup_ops, report_every
        ):
            progress(
                f"{label} warmup {progress_percent(op_number, warmup_ops)} "
                f"({op_number}/{warmup_ops}) done"
            )


def measure_each_in_transaction(
    session: Any,
    ops: list[Callable[[], Any]],
    *,
    label: str | None = None,
    report_every: int | None = None,
) -> dict[str, float]:
    # The branching API is expected to support a batch of work on one checked
    # out branch. This helper measures per-operation latency while committing
    # the batch once, matching the intended agent workflow more closely than an
    # implicit commit after every statement.
    timings_ms: list[float] = []
    start_total = time.perf_counter_ns()
    total = len(ops)
    if label:
        progress(f"{label} transaction start operations={total}")
    with session.transaction():
        for idx, op in enumerate(ops, start=1):
            if label and report_every and should_report_progress(idx, total, report_every):
                progress(
                    f"{label} {progress_percent(idx, total)} "
                    f"({idx}/{total}) start"
                )
            start = time.perf_counter_ns()
            op()
            elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
            timings_ms.append(elapsed_ms)
            if label and report_every and should_report_progress(idx, total, report_every):
                progress(
                    f"{label} {progress_percent(idx, total)} "
                    f"({idx}/{total}) done elapsed_ms={elapsed_ms:.3f}"
                )
    total_ms = (time.perf_counter_ns() - start_total) / 1_000_000
    if label:
        progress(f"{label} transaction done operations={total} total_ms={total_ms:.3f}")
    return stats_from_timings(timings_ms, total_ms)


def workload_rng(case: BenchCase, branch_id: str, metric: str) -> random.Random:
    # Keep random primary-key workloads reproducible and comparable across
    # backends. The backend name is intentionally excluded from the seed.
    return random.Random(
        f"chronos-bench:{case.shape}:{case.dataset_size}:{case.span}:{branch_id}:{metric}"
    )


def random_existing_skus(
    case: BenchCase,
    branch_id: str,
    metric: str,
    count: int,
    *,
    unique: bool,
) -> list[str]:
    if count <= 0 or case.dataset_size <= 0:
        return []
    rng = workload_rng(case, branch_id, metric)
    if unique:
        indices = rng.sample(range(case.dataset_size), min(count, case.dataset_size))
    else:
        indices = [rng.randrange(case.dataset_size) for _ in range(count)]
    return [f"sku_{idx:08d}" for idx in indices]


def random_insert_skus(case: BenchCase, branch_id: str, count: int) -> list[str]:
    rng = workload_rng(case, branch_id, "insert_write")
    skus: list[str] = []
    seen: set[int] = set()
    while len(skus) < count:
        value = rng.getrandbits(63)
        if value in seen:
            continue
        seen.add(value)
        skus.append(f"new_{case.shape}_{case.span}_{branch_id}_{value:016x}")
    return skus


def make_context(
    case: BenchCase,
    database_url: str,
) -> ChronosBranchContext | DoltgresBranchContext:
    backend = case.backend
    dataset_size = case.dataset_size
    progress(f"{case_label(case)} context start")
    if backend == "doltgres":
        ctx = DoltgresBranchContext.connect(database_url)
        db = ctx.db
    else:
        if backend == "litetree" and database_url == "sqlite:///:memory:":
            path = tempfile.mktemp(prefix="chronos-litetree-bench-", suffix=".db")
            database_url = f"file:{path}?branches=on"
        ctx = ChronosBranchContext.connect(
            database_url,
            backend=backend,
            interval_continuation_percent=case.interval_continuation_percent,
        )
        db = ctx.db
        if database_url.startswith(("postgres://", "postgresql://")):
            rows = db.execute(
                """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
                  AND (tablename IN ('products', 'orders') OR tablename LIKE ?)
                """,
                ("_chronos%",),
            ).fetchall()
            for row in rows:
                db.drop_table(row["tablename"])
            db.commit()
            ctx.close()
            ctx = ChronosBranchContext.connect(
                database_url,
                backend=backend,
                interval_continuation_percent=case.interval_continuation_percent,
            )
            db = ctx.db
    progress(f"{case_label(case)} create logical tables")
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
    if isinstance(ctx, DoltgresBranchContext):
        _insert_rows_chunked(db, "products", 4, products)
        _insert_rows_chunked(db, "orders", 3, orders)
    else:
        progress(f"{case_label(case)} load products rows={len(products)}")
        db.executemany("INSERT INTO products VALUES (?, ?, ?, ?)", products)
        progress(f"{case_label(case)} load orders rows={len(orders)}")
        db.executemany("INSERT INTO orders VALUES (?, ?, ?)", orders)
    db.commit()
    progress(f"{case_label(case)} base data committed")
    if isinstance(ctx, DoltgresBranchContext):
        db.execute("CREATE INDEX products_sku_lookup ON products (sku)")
        db.execute("CREATE INDEX products_price_category ON products (price, category)")
        db.execute("CREATE INDEX orders_sku_lookup ON orders (sku)")
        db.commit()
        ctx.commit_working_set("main", "benchmark base")
    else:
        ctx.register_table("products", ["sku"])
        ctx.register_table("orders", ["order_id"])
        ctx.create_index("products", ["sku"], name="products_sku_lookup")
        ctx.create_index("products", ["price", "category"], name="products_price_category")
        ctx.create_index("orders", ["sku"], name="orders_sku_lookup")
    progress(f"{case_label(case)} context ready")
    return ctx


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def chronos_warmup_tables(ctx: ChronosBranchContext, branch_ids: list[str]) -> list[str]:
    backend = ctx._backend  # type: ignore[attr-defined]
    if ctx.backend_name == "copy":
        return [
            backend._branch_table(branch_id, table)  # type: ignore[attr-defined]
            for branch_id in branch_ids
            for table in backend.tables
        ]
    return [meta.physical_name for meta in backend.tables.values()]


def postgres_indexes_for_tables(db: SQLDatabaseAdapter, tables: list[str]) -> list[str]:
    if not tables:
        return []
    placeholders = db.placeholders(len(tables))
    rows = db.execute(
        f"""
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename IN ({placeholders})
        """,
        tuple(tables),
    ).fetchall()
    return [row["indexname"] for row in rows]


def postgres_prewarm_relations(db: SQLDatabaseAdapter, relations: list[str]) -> bool:
    """Load PostgreSQL relations into shared buffers before timed operations.

    The extension is named pg_prewarm. If it is unavailable in the image or
    database, the caller falls back to ordinary sequential scans.
    """

    if not relations:
        return True
    try:
        db.execute("CREATE EXTENSION IF NOT EXISTS pg_prewarm")
        db.commit()
    except Exception:
        db.rollback()
        return False
    for relation in dict.fromkeys(relations):
        try:
            db.execute("SELECT pg_prewarm(to_regclass(?))", (relation,)).fetchone()
        except Exception:
            db.rollback()
            return False
    db.commit()
    return True


def sequential_scan_tables(db: SQLDatabaseAdapter, tables: list[str]) -> None:
    for table in dict.fromkeys(tables):
        try:
            db.execute(f"SELECT count(*) FROM {quote_identifier(table)}").fetchone()
        except Exception:
            db.rollback()
            raise
    db.commit()


def warm_after_branching(
    ctx: ChronosBranchContext | DoltgresBranchContext,
    branch_ids: list[str],
    mode: str,
) -> None:
    if mode == "off":
        return
    if isinstance(ctx, DoltgresBranchContext):
        # Doltgres does not support pg_prewarm. Checkout each measured branch
        # and scan the logical tables once so cold page reconstruction is not
        # charged to the first measured reads.
        for branch_id in branch_ids:
            session = ctx.checkout(branch_id)
            session.query("SELECT count(*) AS rows FROM products")
            session.query("SELECT count(*) AS rows FROM orders")
        return

    if ctx.db.dialect != "postgres":
        return

    tables = chronos_warmup_tables(ctx, branch_ids)
    indexes = postgres_indexes_for_tables(ctx.db, tables)
    if not postgres_prewarm_relations(ctx.db, [*tables, *indexes]):
        sequential_scan_tables(ctx.db, tables)


def _insert_rows_chunked(
    db: SQLDatabaseAdapter,
    table: str,
    width: int,
    rows: list[tuple[Any, ...]],
    chunk_size: int = 1000,
) -> None:
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start : start + chunk_size]
        placeholders = ", ".join(
            f"({', '.join('?' for _ in range(width))})" for _ in chunk
        )
        values = [value for row in chunk for value in row]
        db.execute(f"INSERT INTO {table} VALUES {placeholders}", values)


def mutate_branch_state(
    ctx: Any,
    branch_id: str,
    case: BenchCase,
    level: int,
    mutations_per_branch: int,
) -> None:
    if mutations_per_branch <= 0:
        return
    label = f"{case_label(case)} branch={branch_id} setup_mutations"
    progress(f"{label} start iterations={mutations_per_branch}")
    session = ctx.checkout(branch_id)
    mutation_count = min(mutations_per_branch, case.dataset_size)
    with session.transaction():
        for idx in range(mutation_count):
            progress(f"{label} iteration {idx + 1}/{mutation_count} start")
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
            progress(f"{label} iteration {idx + 1}/{mutation_count} done")
    progress(f"{label} done")


def build_depth_chain(
    ctx: Any,
    case: BenchCase,
    mutations_per_branch: int,
) -> tuple[str, dict[str, float], list[str]]:
    branch_names = [f"depth_{idx}" for idx in range(case.depth)]
    create_timings_ms: list[float] = []
    parent = "main"
    for level, branch in enumerate(branch_names):
        progress(
            f"{case_label(case)} create depth branch {level + 1}/{case.depth} "
            f"branch={branch} parent={parent}"
        )
        start = time.perf_counter_ns()
        ctx.create_branch(branch, from_branch=parent)
        create_timings_ms.append((time.perf_counter_ns() - start) / 1_000_000)
        progress(f"{case_label(case)} created branch={branch}")
        mutate_branch_state(ctx, branch, case, level, mutations_per_branch)
        if mutations_per_branch > 0 and hasattr(ctx, "commit_working_set"):
            ctx.commit_working_set(branch, f"benchmark mutations depth {level}")
        parent = branch
    terminal = branch_names[-1] if branch_names else "main"
    return terminal, stats_from_timings(create_timings_ms), branch_names


def build_width_fanout(
    ctx: Any,
    case: BenchCase,
    mutations_per_branch: int,
) -> tuple[list[str], dict[str, float]]:
    branch_names = [f"width_{idx}" for idx in range(case.width)]
    create_timings_ms: list[float] = []
    for level, branch in enumerate(branch_names):
        progress(
            f"{case_label(case)} create width branch {level + 1}/{case.width} "
            f"branch={branch} parent=main"
        )
        start = time.perf_counter_ns()
        ctx.create_branch(branch, from_branch="main")
        create_timings_ms.append((time.perf_counter_ns() - start) / 1_000_000)
        progress(f"{case_label(case)} created branch={branch}")
        mutate_branch_state(ctx, branch, case, level, mutations_per_branch)
        if mutations_per_branch > 0 and hasattr(ctx, "commit_working_set"):
            ctx.commit_working_set(branch, f"benchmark mutations width {level}")
    return branch_names, stats_from_timings(create_timings_ms)


def benchmark_branch_deletes(
    ctx: Any, case: BenchCase, branch_names: list[str]
) -> dict[str, Any]:
    delete_ops = [
        (lambda branch=branch: ctx.delete_branch(branch))
        for branch in reversed(branch_names)
    ]
    label = f"{case_label(case)} branch_delete"
    delete_stats = measure_each(
        delete_ops,
        label=label,
        report_every=progress_interval(case, len(delete_ops)),
    )
    return result_row(case, "branch_delete", delete_stats)


def combine_stats(stats: list[dict[str, float]]) -> dict[str, float]:
    operations = sum(int(stat["operations"]) for stat in stats)
    total_ms = sum(float(stat["total_ms"]) for stat in stats)
    if not stats or operations == 0:
        return stats_from_timings([], total_ms)
    weighted_avg = sum(
        float(stat["avg_ms"]) * int(stat["operations"]) for stat in stats
    ) / operations
    return {
        "operations": float(operations),
        "median_ms": statistics.median(float(stat["median_ms"]) for stat in stats),
        "avg_ms": weighted_avg,
        "p95_ms": max(float(stat["p95_ms"]) for stat in stats),
        "total_ms": total_ms,
    }


def benchmark_reads(
    ctx: Any,
    branch_id: str,
    case: BenchCase,
    read_ops: int,
    range_read_ops: int,
    warmup_ops: int,
    include_join_aggregate: bool,
) -> list[dict[str, Any]]:
    # Checkout prepares backend metadata once. The measured reads reuse the
    # same session so interval segments or log lineage are not reloaded for
    # every query.
    base_label = f"{case_label(case)} branch={branch_id}"
    progress(f"{base_label} reads checkout")
    session = ctx.checkout(branch_id)
    keys = random_existing_skus(
        case, branch_id, "point_read", read_ops, unique=False
    )
    point_label = f"{base_label} point_read"

    point_ops = [
        (
            lambda key=key: session.query(
                "SELECT sku, price FROM products WHERE sku = :sku",
                {"sku": key},
            )
        )
        for key in keys
    ]
    warmup_each(
        point_ops,
        warmup_ops,
        label=point_label,
        report_every=progress_interval(case, warmup_ops),
    )
    point_stats = measure_each(
        point_ops,
        label=point_label,
        report_every=progress_interval(case, len(point_ops)),
    )
    rows = [result_row(case, "point_read", point_stats)]

    range_width = min(100, case.dataset_size)
    max_start = max(case.dataset_size - range_width, 0)
    range_rng = random.Random(
        f"{case.backend}:{case.shape}:{case.dataset_size}:{case.span}:range"
    )
    range_starts = [
        range_rng.randint(0, max_start)
        for _ in range(range_read_ops)
    ]
    range_ops = [
        (
            lambda start=start: session.query(
                """
                SELECT sku, category, price, stock
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
    range_label = f"{base_label} range_read"
    warmup_each(
        range_ops,
        warmup_ops,
        label=range_label,
        report_every=progress_interval(case, warmup_ops),
    )
    range_stats = measure_each(
        range_ops,
        label=range_label,
        report_every=progress_interval(case, len(range_ops)),
    )
    rows.append(result_row(case, "range_read", range_stats))

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
    aggregate_label = f"{base_label} join_aggregate_read"
    warmup_each(
        aggregate_ops,
        warmup_ops,
        label=aggregate_label,
        report_every=progress_interval(case, warmup_ops),
    )
    aggregate_stats = measure_each(
        aggregate_ops,
        label=aggregate_label,
        report_every=progress_interval(case, len(aggregate_ops)),
    )
    rows.append(result_row(case, "join_aggregate_read", aggregate_stats))

    return rows


def benchmark_reads_across_branches(
    ctx: Any,
    branch_ids: list[str],
    case: BenchCase,
    read_ops: int,
    range_read_ops: int,
    warmup_ops: int,
    include_join_aggregate: bool,
) -> list[dict[str, Any]]:
    metric_stats: dict[str, list[dict[str, float]]] = {}
    for idx, branch_id in enumerate(branch_ids, start=1):
        progress(
            f"{case_label(case)} reads branch {idx}/{len(branch_ids)} "
            f"branch={branch_id}"
        )
        for row in benchmark_reads(
            ctx,
            branch_id,
            case,
            read_ops,
            range_read_ops,
            warmup_ops,
            include_join_aggregate,
        ):
            metric_stats.setdefault(row["metric"], []).append(
                {
                    "operations": float(row["operations"]),
                    "median_ms": float(row["median_ms"]),
                    "avg_ms": float(row["avg_ms"]),
                    "p95_ms": float(row["p95_ms"]),
                    "total_ms": float(row["total_ms"]),
                }
            )
    return [
        result_row(case, metric, combine_stats(stats))
        for metric, stats in metric_stats.items()
    ]


def benchmark_writes(
    ctx: Any,
    branch_id: str,
    case: BenchCase,
    write_ops: int,
) -> list[dict[str, Any]]:
    # Writes are measured on the terminal branch after the depth chain has been
    # built and mutated. Each write category uses one transaction to isolate
    # branch-backend costs from repeated commit overhead.
    base_label = f"{case_label(case)} branch={branch_id}"
    progress(f"{base_label} writes checkout")
    session = ctx.checkout(branch_id)
    update_count = min(write_ops, case.dataset_size)
    delete_count = min(write_ops, case.dataset_size)
    update_keys = random_existing_skus(
        case, branch_id, "update_write", update_count, unique=True
    )
    insert_keys = random_insert_skus(case, branch_id, write_ops)
    delete_keys = random_existing_skus(
        case, branch_id, "delete_write", delete_count, unique=True
    )

    update_ops = [
        (
            lambda idx=idx, key=key: session.execute(
                "UPDATE products SET price = :price WHERE sku = :sku",
                {"price": 1000 + idx, "sku": key},
            )
        )
        for idx, key in enumerate(update_keys)
    ]
    update_label = f"{base_label} update_write"
    update_stats = measure_each_in_transaction(
        session,
        update_ops,
        label=update_label,
        report_every=progress_interval(case, len(update_ops)),
    )

    insert_ops = [
        (
            lambda idx=idx, key=key: session.execute(
                """
                INSERT INTO products (sku, category, price, stock)
                VALUES (:sku, :category, :price, :stock)
                """,
                {
                    "sku": key,
                    "category": f"new_{idx % 8:02d}",
                    "price": 2000 + idx,
                    "stock": 500,
                },
            )
        )
        for idx, key in enumerate(insert_keys)
    ]
    insert_label = f"{base_label} insert_write"
    insert_stats = measure_each_in_transaction(
        session,
        insert_ops,
        label=insert_label,
        report_every=progress_interval(case, len(insert_ops)),
    )

    delete_ops = [
        (
            lambda key=key: session.execute(
                "DELETE FROM products WHERE sku = :sku",
                {"sku": key},
            )
        )
        for key in delete_keys
    ]
    delete_label = f"{base_label} delete_write"
    delete_stats = measure_each_in_transaction(
        session,
        delete_ops,
        label=delete_label,
        report_every=progress_interval(case, len(delete_ops)),
    )

    return [
        result_row(case, "update_write", update_stats),
        result_row(case, "insert_write", insert_stats),
        result_row(case, "delete_write", delete_stats),
    ]


def benchmark_writes_across_branches(
    ctx: Any,
    branch_ids: list[str],
    case: BenchCase,
    write_ops: int,
) -> list[dict[str, Any]]:
    metric_stats: dict[str, list[dict[str, float]]] = {}
    for idx, branch_id in enumerate(branch_ids, start=1):
        progress(
            f"{case_label(case)} writes branch {idx}/{len(branch_ids)} "
            f"branch={branch_id}"
        )
        for row in benchmark_writes(ctx, branch_id, case, write_ops):
            metric_stats.setdefault(row["metric"], []).append(
                {
                    "operations": float(row["operations"]),
                    "median_ms": float(row["median_ms"]),
                    "avg_ms": float(row["avg_ms"]),
                    "p95_ms": float(row["p95_ms"]),
                    "total_ms": float(row["total_ms"]),
                }
            )
    return [
        result_row(case, metric, combine_stats(stats))
        for metric, stats in metric_stats.items()
    ]


def result_row(
    case: BenchCase,
    metric: str,
    stats: dict[str, float],
) -> dict[str, Any]:
    return {
        "backend": case.backend,
        "shape": case.shape,
        "interval_continuation_percent": case.interval_continuation_percent,
        "dataset_size": case.dataset_size,
        "depth": case.depth,
        "width": case.width,
        "span": case.span,
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
    range_read_ops: int,
    write_ops: int,
    warmup_ops: int,
    mutations_per_branch: int,
    include_join_aggregate: bool,
    post_branch_warmup: str,
) -> list[dict[str, Any]]:
    label = case_label(case)
    progress(f"{label} case start")
    ctx = make_context(case, database_url)
    try:
        if case.shape == "width":
            progress(f"{label} branch construction start")
            branch_names, create_stats = build_width_fanout(
                ctx, case, mutations_per_branch
            )
            target_branches = branch_names or ["main"]
        else:
            progress(f"{label} branch construction start")
            terminal_branch, create_stats, branch_names = build_depth_chain(
                ctx, case, mutations_per_branch
            )
            target_branches = [terminal_branch]
        progress(
            f"{label} branch construction done target_branches="
            f"{','.join(target_branches)}"
        )
        progress(f"{label} post-branch warmup start mode={post_branch_warmup}")
        warm_after_branching(ctx, target_branches, post_branch_warmup)
        progress(f"{label} post-branch warmup done")
        rows = [result_row(case, "branch_create", create_stats)]
        if case.shape == "width":
            progress(f"{label} read benchmarks start")
            rows.extend(
                benchmark_reads_across_branches(
                    ctx,
                    target_branches,
                    case,
                    read_ops,
                    range_read_ops,
                    warmup_ops,
                    include_join_aggregate,
                )
            )
            progress(f"{label} read benchmarks done")
            progress(f"{label} write benchmarks start")
            rows.extend(
                benchmark_writes_across_branches(
                    ctx,
                    target_branches,
                    case,
                    write_ops,
                )
            )
            progress(f"{label} write benchmarks done")
        else:
            progress(f"{label} read benchmarks start")
            rows.extend(
                benchmark_reads(
                    ctx,
                    target_branches[0],
                    case,
                    read_ops,
                    range_read_ops,
                    warmup_ops,
                    include_join_aggregate,
                )
            )
            progress(f"{label} read benchmarks done")
            progress(f"{label} write benchmarks start")
            rows.extend(benchmark_writes(ctx, target_branches[0], case, write_ops))
            progress(f"{label} write benchmarks done")
        progress(f"{label} branch delete benchmark start")
        rows.append(benchmark_branch_deletes(ctx, case, branch_names))
        progress(f"{label} branch delete benchmark done")
        progress(f"{label} case done")
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


def config_label(row: dict[str, Any]) -> str:
    shape = row.get("shape", "depth")
    span = row.get("span")
    if span is None or span == "":
        span = row.get("width") if shape == "width" else row.get("depth")
    marker = "w" if shape == "width" else "d"
    return f"N={row['dataset_size']} {marker}={span}"


def config_sort_key(row: dict[str, Any]) -> tuple[int, int]:
    shape = row.get("shape", "depth")
    span = row.get("span")
    if span is None or span == "":
        span = row.get("width") if shape == "width" else row.get("depth")
    return (int(row["dataset_size"]), int(span))


def ordered_backends(rows: list[dict[str, Any]]) -> list[str]:
    present = {row["backend"] for row in rows}
    return [backend for backend in BACKENDS if backend in present]


def should_skip_case(backend: str, shape: str, span: int) -> bool:
    return backend == "copy" and shape in {"depth", "width"} and span > COPY_MAX_BRANCH_SPAN


def write_metric_plot(
    output_dir: Path, rows: list[dict[str, Any]], metric: str, prefix: str = ""
) -> Path:
    metric_rows = [row for row in rows if row["metric"] == metric]
    label_order = {
        config_label(row): config_sort_key(row)
        for row in metric_rows
    }
    groups = sorted(label_order, key=lambda label: label_order[label])
    backends = ordered_backends(metric_rows)
    values = {
        (config_label(row), row["backend"]): float(row["median_ms"])
        for row in metric_rows
    }
    x_positions = list(range(len(groups)))
    width = min(0.8 / max(len(backends), 1), 0.28)
    fig_width = max(10, len(groups) * 0.9)
    fig, ax = plt.subplots(figsize=(fig_width, 5.5), constrained_layout=True)
    colors = {
        "copy": "#16a34a",
        "interval": "#2563eb",
        "log": "#dc2626",
        "orpheus": "#ea580c",
        "litetree": "#0891b2",
        "doltgres": "#7c3aed",
    }
    for backend_idx, backend in enumerate(backends):
        offset = (backend_idx - (len(backends) - 1) / 2) * width
        y_values = [values.get((group, backend), 0.0) for group in groups]
        ax.bar(
            [x + offset for x in x_positions],
            y_values,
            width=width,
            label=BACKEND_LABELS.get(backend, backend),
            color=colors.get(backend, "#525252"),
        )
    title_prefix = f"{prefix.title()} " if prefix else ""
    ax.set_title(f"{title_prefix}{metric.replace('_', ' ').title()}")
    ax.set_ylabel("Median milliseconds per operation")
    axis = "width" if prefix == "width" else "depth"
    ax.set_xlabel(f"Dataset / branch {axis}")
    ax.set_xticks(x_positions)
    ax.set_xticklabels(groups, rotation=35, ha="right")
    ax.grid(axis="y", alpha=0.25)
    ax.legend()
    output_path = output_dir / f"{prefix + '_' if prefix else ''}{metric}.png"
    fig.savefig(output_path, dpi=160)
    plt.close(fig)
    return output_path


def write_summary_plot(
    output_dir: Path, rows: list[dict[str, Any]], prefix: str = ""
) -> Path:
    summary: dict[tuple[str, str], list[float]] = {}
    for row in rows:
        summary.setdefault((row["metric"], row["backend"]), []).append(
            float(row["median_ms"])
        )
    metrics = [metric for metric in METRICS if any(row["metric"] == metric for row in rows)]
    backends = ordered_backends(rows)
    x_positions = list(range(len(metrics)))
    width = min(0.8 / max(len(backends), 1), 0.28)
    fig, ax = plt.subplots(figsize=(12, 5.5), constrained_layout=True)
    colors = {
        "copy": "#16a34a",
        "interval": "#2563eb",
        "log": "#dc2626",
        "orpheus": "#ea580c",
        "litetree": "#0891b2",
        "doltgres": "#7c3aed",
    }
    for backend_idx, backend in enumerate(backends):
        offset = (backend_idx - (len(backends) - 1) / 2) * width
        y_values = [
            statistics.median(summary.get((metric, backend), [0.0]))
            for metric in metrics
        ]
        ax.bar(
            [x + offset for x in x_positions],
            y_values,
            width=width,
            label=BACKEND_LABELS.get(backend, backend),
            color=colors.get(backend, "#525252"),
        )
    title_prefix = f"{prefix.title()} " if prefix else ""
    ax.set_title(f"{title_prefix}Backend Comparison Summary")
    ax.set_ylabel("Median of per-case median milliseconds")
    ax.set_xticks(x_positions)
    ax.set_xticklabels([metric.replace("_", "\n") for metric in metrics])
    ax.grid(axis="y", alpha=0.25)
    ax.legend()
    output_path = output_dir / f"{prefix + '_' if prefix else ''}summary.png"
    fig.savefig(output_path, dpi=160)
    plt.close(fig)
    return output_path


def write_plots(output_dir: Path, rows: list[dict[str, Any]]) -> list[Path]:
    paths: list[Path] = []
    shapes = [
        shape
        for shape in ("depth", "width")
        if any(row.get("shape", "depth") == shape for row in rows)
    ]
    for shape in shapes:
        shape_rows = [row for row in rows if row.get("shape", "depth") == shape]
        paths.append(write_summary_plot(output_dir, shape_rows, prefix=shape))
        paths.extend(
            write_metric_plot(output_dir, shape_rows, metric, prefix=shape)
            for metric in METRICS
            if any(row["metric"] == metric for row in shape_rows)
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
        "# Chronos Branching Backend Benchmark\n\n"
        f"Generated at `{config['generated_at']}`.\n\n"
        "## Config\n\n"
        f"```json\n{json.dumps(config, indent=2, sort_keys=True)}\n```\n\n"
        "## Plots\n\n"
        f"{image_lines}\n\n"
        "Raw results are in `results.csv` and `results.json`.\n"
    )


def write_summary_from_existing_results(
    output_dir: Path,
    config: dict[str, Any] | None = None,
) -> None:
    results_path = output_dir / "results.csv"
    if not results_path.exists():
        raise SystemExit(f"missing results.csv: {results_path}")
    rows = read_csv(results_path)
    if config is None:
        config = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "source": str(results_path),
        }
    write_json(output_dir / "results.json", rows, config)
    plot_paths = write_plots(output_dir, rows)
    write_markdown_summary(output_dir / "README.md", rows, config, plot_paths)
    print(f"\nWrote merged benchmark summary to {output_dir}")
    print(f"Wrote matplotlib plots: {', '.join(path.name for path in plot_paths)}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark Chronos branch physical backends."
    )
    parser.add_argument("--backends", default="copy,interval,log")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("CHRONOS_BRANCH_DATABASE_URL", "sqlite:///:memory:"),
        help=(
            "SQL database URL for benchmark setup. Defaults to sqlite:///:memory:. "
            "Set CHRONOS_BRANCH_DATABASE_URL to use PostgreSQL without changing commands."
        ),
    )
    parser.add_argument("--dataset-sizes", type=parse_int_list, default=[100, 1000])
    parser.add_argument("--depths", type=parse_int_list, default=[0, 4, 8])
    parser.add_argument("--widths", type=parse_int_list, default=[1, 4, 8])
    parser.add_argument(
        "--benchmark-shapes",
        default="depth,width",
        help="Comma-separated benchmark shapes to run: depth,width.",
    )
    parser.add_argument("--read-ops", type=int, default=20)
    parser.add_argument("--range-read-ops", type=int, default=100)
    parser.add_argument("--write-ops", type=int, default=20)
    parser.add_argument(
        "--warmup-ops",
        type=int,
        default=100,
        help=(
            "Number of unmeasured read operations to run before timed read "
            "metrics. Write metrics are not warmed with writes because that "
            "would mutate the measured branch state."
        ),
    )
    parser.add_argument(
        "--branch-mutations",
        type=int,
        default=5,
        help="Number of update/insert/delete mutation groups applied after each branch creation.",
    )
    parser.add_argument(
        "--interval-depth-continuation-percent",
        type=int,
        default=5,
        help="Chronos interval source-branch continuation percent for depth cases.",
    )
    parser.add_argument(
        "--interval-width-continuation-percent",
        type=int,
        default=98,
        help="Chronos interval source-branch continuation percent for width cases.",
    )
    parser.add_argument(
        "--include-join-aggregate",
        action="store_true",
        help="Also run the expensive join/group/order read benchmark.",
    )
    parser.add_argument(
        "--post-branch-warmup",
        choices=("auto", "off"),
        default="auto",
        help=(
            "Warm physical data after branch creation and before timed operations. "
            "PostgreSQL Chronos backends use pg_prewarm when available and fall "
            "back to table scans. Doltgres uses logical sequential scans."
        ),
    )
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--summarize-existing",
        action="store_true",
        help="Regenerate results.json, README.md, and plots from output-dir/results.csv.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.summarize_existing:
        if args.output_dir is None:
            raise SystemExit("--summarize-existing requires --output-dir")
        write_summary_from_existing_results(args.output_dir)
        return

    backends = tuple(backend.strip() for backend in args.backends.split(",") if backend.strip())
    shapes = tuple(
        shape.strip()
        for shape in args.benchmark_shapes.split(",")
        if shape.strip()
    )
    unknown = set(backends) - set(BACKENDS)
    if unknown:
        raise SystemExit(f"unknown backend(s): {', '.join(sorted(unknown))}")
    unknown_shapes = set(shapes) - {"depth", "width"}
    if unknown_shapes:
        raise SystemExit(f"unknown benchmark shape(s): {', '.join(sorted(unknown_shapes))}")
    if args.quick:
        dataset_sizes = [100] if args.dataset_sizes == [100, 1000] else args.dataset_sizes
        depths = [0, 4] if args.depths == [0, 4, 8] else args.depths
        widths = [1, 4] if args.widths == [1, 4, 8] else args.widths
        read_ops = min(args.read_ops, 5)
        range_read_ops = min(args.range_read_ops, 5)
        write_ops = min(args.write_ops, 5)
        warmup_ops = min(args.warmup_ops, 2)
        branch_mutations = min(args.branch_mutations, 3)
    else:
        dataset_sizes = args.dataset_sizes
        depths = args.depths
        widths = args.widths
        read_ops = args.read_ops
        range_read_ops = args.range_read_ops
        write_ops = args.write_ops
        warmup_ops = args.warmup_ops
        branch_mutations = args.branch_mutations
    if min(dataset_sizes) <= 0:
        raise SystemExit("dataset sizes must be positive")
    if branch_mutations < 0:
        raise SystemExit("branch mutations must be non-negative")
    if range_read_ops < 0:
        raise SystemExit("range read ops must be non-negative")
    if warmup_ops < 0:
        raise SystemExit("warmup ops must be non-negative")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = args.output_dir or Path(".benchmarks") / f"branching-{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    results_csv = output_dir / "results.csv"
    init_stream_csv(results_csv)

    config = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "backends": list(backends),
        "benchmark_shapes": list(shapes),
        "dataset_sizes": dataset_sizes,
        "depths": depths,
        "widths": widths,
        "read_ops": read_ops,
        "range_read_ops": range_read_ops,
        "write_ops": write_ops,
        "warmup_ops": warmup_ops,
        "post_branch_warmup": args.post_branch_warmup,
        "branch_mutations": branch_mutations,
        "interval_depth_continuation_percent": args.interval_depth_continuation_percent,
        "interval_width_continuation_percent": args.interval_width_continuation_percent,
        "include_join_aggregate": args.include_join_aggregate,
        "database_url": args.database_url,
    }
    rows: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="chronos-branch-bench-"):
        for backend in backends:
            for dataset_size in dataset_sizes:
                if "depth" in shapes:
                    for depth in depths:
                        if should_skip_case(backend, "depth", depth):
                            print(
                                f"skip shape=depth backend={backend} dataset={dataset_size} depth={depth} "
                                f"(copy max {COPY_MAX_BRANCH_SPAN})",
                                flush=True,
                            )
                            continue
                        case = BenchCase(
                            backend=backend,
                            dataset_size=dataset_size,
                            depth=depth,
                            width=0,
                            shape="depth",
                            interval_continuation_percent=(
                                args.interval_depth_continuation_percent
                                if backend == "interval"
                                else 5
                            ),
                        )
                        print(
                            f"shape=depth backend={backend} dataset={dataset_size} depth={depth}",
                            flush=True,
                        )
                        case_rows = run_case(
                            case,
                            args.database_url,
                            read_ops,
                            range_read_ops,
                            write_ops,
                            warmup_ops,
                            branch_mutations,
                            args.include_join_aggregate,
                            args.post_branch_warmup,
                        )
                        rows.extend(case_rows)
                        append_csv_rows(results_csv, case_rows)
                if "width" in shapes:
                    for width in widths:
                        if should_skip_case(backend, "width", width):
                            print(
                                f"skip shape=width backend={backend} dataset={dataset_size} width={width} "
                                f"(copy max {COPY_MAX_BRANCH_SPAN})",
                                flush=True,
                            )
                            continue
                        case = BenchCase(
                            backend=backend,
                            dataset_size=dataset_size,
                            depth=0,
                            width=width,
                            shape="width",
                            interval_continuation_percent=(
                                args.interval_width_continuation_percent
                                if backend == "interval"
                                else 5
                            ),
                        )
                        print(
                            f"shape=width backend={backend} dataset={dataset_size} width={width}",
                            flush=True,
                        )
                        case_rows = run_case(
                            case,
                            args.database_url,
                            read_ops,
                            range_read_ops,
                            write_ops,
                            warmup_ops,
                            branch_mutations,
                            args.include_join_aggregate,
                            args.post_branch_warmup,
                        )
                        rows.extend(case_rows)
                        append_csv_rows(results_csv, case_rows)

    write_json(output_dir / "results.json", rows, config)
    plot_paths = write_plots(output_dir, rows)
    write_markdown_summary(output_dir / "README.md", rows, config, plot_paths)
    print(f"\nWrote benchmark results to {output_dir}")
    print(f"Wrote matplotlib plots: {', '.join(path.name for path in plot_paths)}")


if __name__ == "__main__":
    main()
