from __future__ import annotations

import argparse
import csv
import math
import os
import statistics
import subprocess
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from chronos_core.branching import ChronosBranchContext
from chronos_core.branching.sql_adapters import SQLDatabaseAdapter, connect_sql_database


BACKENDS = ("chronos", "orpheus", "doltgres", "native_txn")
DEFAULT_BACKENDS = ("chronos", "doltgres", "native_txn")
PHASES = (
    "branch_create",
    "branch_workload",
    "merge_apply",
    "branch_delete",
    "txn_workload",
    "txn_commit",
    "total",
)
DETAIL_FIELDS = [
    "backend",
    "dataset_size",
    "transaction_count",
    "warmup_iterations",
    "iteration",
    "phase",
    "ms",
    "changes_per_operation",
    "read_count",
    "rows_read",
    "applied",
]
SUMMARY_FIELDS = [
    "backend",
    "dataset_size",
    "transaction_count",
    "warmup_iterations",
    "phase",
    "iterations",
    "median_ms",
    "avg_ms",
    "p95_ms",
    "total_ms",
    "final_memory_bytes",
    "final_memory_mb",
    "peak_memory_bytes",
    "peak_memory_mb",
]
VERIFICATION_FIELDS = [
    "backend",
    "dataset_size",
    "transaction_count",
    "warmup_iterations",
    "changes_per_operation",
    "read_count",
    "delete_branches",
    "matched",
    "reference_row_count",
    "backend_row_count",
    "reference_quantity_sum",
    "backend_quantity_sum",
    "reference_changed_rows",
    "backend_changed_rows",
    "message",
]


@dataclass(frozen=True)
class TxnCase:
    backend: str
    dataset_size: int
    iterations: int
    changes: int
    read_count: int
    warmup_iterations: int = 100
    delete_branches: bool = False
    interval_child_width: int | None = 2


@dataclass(frozen=True)
class TableState:
    row_count: int
    quantity_sum: int
    changed_rows: tuple[tuple[str, int, str, str], ...]


class Timer:
    def __init__(self) -> None:
        self.start = time.perf_counter()

    def lap_ms(self) -> float:
        now = time.perf_counter()
        elapsed = (now - self.start) * 1000
        self.start = now
        return elapsed

    def elapsed_ms(self) -> float:
        return (time.perf_counter() - self.start) * 1000


def progress(message: str) -> None:
    print(f"  progress: {message}", flush=True)


_MEMORY_UNITS = {
    "B": 1,
    "KB": 1_000,
    "MB": 1_000_000,
    "GB": 1_000_000_000,
    "TB": 1_000_000_000_000,
    "KIB": 1024,
    "MIB": 1024**2,
    "GIB": 1024**3,
    "TIB": 1024**4,
}


class MemoryTracker:
    def __init__(self, container_name: str | None, interval_seconds: float = 1.0):
        self.container_name = container_name
        self.interval_seconds = interval_seconds
        self.final_bytes: int | None = None
        self.peak_bytes: int | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> MemoryTracker:
        if self.container_name:
            self._sample()
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.interval_seconds * 2))
        self._sample()

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            self._sample()

    def _sample(self) -> None:
        if not self.container_name:
            return
        memory = docker_container_memory_bytes(self.container_name)
        if memory is None:
            return
        self.final_bytes = memory
        if self.peak_bytes is None or memory > self.peak_bytes:
            self.peak_bytes = memory


def docker_container_memory_bytes(container_name: str) -> int | None:
    try:
        result = subprocess.run(
            [
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{.MemUsage}}",
                container_name,
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    first_line = result.stdout.strip().splitlines()
    if not first_line:
        return None
    usage = first_line[0].split("/", 1)[0].strip()
    return parse_memory_value(usage)


def parse_memory_value(value: str) -> int | None:
    compact = value.strip().replace(" ", "").upper()
    if not compact:
        return None
    for unit in sorted(_MEMORY_UNITS, key=len, reverse=True):
        if compact.endswith(unit):
            number = compact[: -len(unit)]
            try:
                return int(float(number) * _MEMORY_UNITS[unit])
            except ValueError:
                return None
    return None


def memory_mb(value: int | None) -> float | None:
    return None if value is None else value / 1_000_000


def parse_int_list(value: str) -> list[int]:
    parsed = [int(part.strip()) for part in value.split(",") if part.strip()]
    if not parsed:
        raise argparse.ArgumentTypeError("expected at least one integer")
    if any(item <= 0 for item in parsed):
        raise argparse.ArgumentTypeError("values must be positive")
    return parsed


def parse_backends(value: str) -> list[str]:
    backends = [part.strip() for part in value.split(",") if part.strip()]
    unknown = sorted(set(backends) - set(BACKENDS))
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown backends: {', '.join(unknown)}")
    if not backends:
        raise argparse.ArgumentTypeError("expected at least one backend")
    return backends


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


def summarize(values: list[float]) -> dict[str, float]:
    return {
        "iterations": float(len(values)),
        "median_ms": statistics.median(values) if values else 0.0,
        "avg_ms": statistics.fmean(values) if values else 0.0,
        "p95_ms": percentile(values, 0.95),
        "total_ms": sum(values),
    }


def validate_case(case: TxnCase) -> None:
    required = max(case.read_count, case.changes, 1)
    if case.dataset_size < required:
        raise ValueError(
            f"dataset_size={case.dataset_size} is too small for iterations={case.iterations}, "
            f"changes={case.changes}, read_count={case.read_count}; need at least {required}"
        )


def reset_postgres_schema(db: SQLDatabaseAdapter) -> None:
    db.execute("DROP SCHEMA IF EXISTS public CASCADE")
    db.execute("CREATE SCHEMA public")
    db.commit()


def create_items_table(db: SQLDatabaseAdapter, dataset_size: int) -> None:
    db.execute(
        """
        CREATE TABLE txn_items (
          id TEXT PRIMARY KEY,
          quantity INTEGER NOT NULL,
          status TEXT NOT NULL,
          note TEXT NOT NULL
        )
        """
    )
    db.execute(
        """
        INSERT INTO txn_items
        SELECT 'user:' || lpad(g::text, 12, '0'), (g % 100), 'open', 'seed'
        FROM generate_series(1, ?) AS g
        """,
        (dataset_size,),
    )
    db.commit()


def create_items_table_with_chunked_load(
    db: SQLDatabaseAdapter, dataset_size: int, *, chunk_size: int = 10_000
) -> None:
    db.execute(
        """
        CREATE TABLE txn_items (
          id TEXT PRIMARY KEY,
          quantity INTEGER NOT NULL,
          status TEXT NOT NULL,
          note TEXT NOT NULL
        )
        """
    )
    for start in range(1, dataset_size + 1, chunk_size):
        stop = min(dataset_size + 1, start + chunk_size)
        # Doltgres currently mis-adapts integer bind parameters through psycopg
        # in this benchmark path (e.g. 1 round-trips as 65537). The seed data is
        # deterministic and generated locally, so use SQL literals here while
        # keeping the workload operations themselves parameterized.
        rows = ", ".join(
            f"({sql_text_literal(record_key(idx))}, {idx % 100}, 'open', 'seed')"
            for idx in range(start, stop)
        )
        db.execute(f"INSERT INTO txn_items VALUES {rows}")
        db.commit()


def record_key(value: int) -> str:
    return f"user:{value:012d}"


def sql_text_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def ycsb_key(case: TxnCase, iteration: int, op_index: int, *, stream: int) -> str:
    # Deterministic point keys, similar to YCSB's repeated primary-key access.
    # The constants are primes to spread operations across the dataset while
    # keeping every backend on the same key sequence.
    value = (iteration * 1_000_003) + (op_index * 154_858_63) + stream
    return record_key(1 + (value % case.dataset_size))


def insert_rows(case: TxnCase, iteration: int) -> list[dict[str, Any]]:
    return [
        {
            "id": record_key(case.dataset_size + iteration * case.changes + offset + 1),
            "quantity": iteration + offset,
            "status": "branch",
            "note": f"insert:{iteration}:{offset}",
        }
        for offset in range(case.changes)
    ]


def execute_workload_sql(session: Any, case: TxnCase, iteration: int) -> int:
    rows_read = 0
    for op_index in range(case.read_count):
        rows = session.query(
            """
            SELECT quantity
            FROM txn_items
            WHERE id = :id
            """,
            {"id": ycsb_key(case, iteration, op_index, stream=17)},
        )
        rows_read += len(rows)
    for op_index in range(case.changes):
        session.execute(
            """
            UPDATE txn_items
            SET quantity = quantity + 1,
                note = :note
            WHERE id = :id
            """,
            {
                "note": f"update:{iteration}:{op_index}",
                "id": ycsb_key(case, iteration, op_index, stream=29),
            },
        )
    return rows_read


def execute_workload_db(db: SQLDatabaseAdapter, case: TxnCase, iteration: int) -> int:
    rows_read = 0
    for op_index in range(case.read_count):
        rows = db.execute(
            "SELECT quantity FROM txn_items WHERE id = ?",
            (ycsb_key(case, iteration, op_index, stream=17),),
        ).fetchall()
        rows_read += len(rows)
    for op_index in range(case.changes):
        db.execute(
            """
            UPDATE txn_items
            SET quantity = quantity + 1,
                note = ?
            WHERE id = ?
            """,
            (
                f"update:{iteration}:{op_index}",
                ycsb_key(case, iteration, op_index, stream=29),
            ),
        )
    return rows_read


def table_state_from_rows(
    aggregate_rows: list[dict[str, Any]],
    changed_rows: list[dict[str, Any]],
) -> TableState:
    aggregate = aggregate_rows[0]
    return TableState(
        row_count=int(aggregate["row_count"]),
        quantity_sum=int(aggregate["quantity_sum"] or 0),
        changed_rows=tuple(
            (
                str(row["id"]),
                int(row["quantity"]),
                str(row["status"]),
                str(row["note"]),
            )
            for row in changed_rows
        ),
    )


def table_state_from_query(query: Any) -> TableState:
    aggregate_rows = query(
        """
        SELECT COUNT(*) AS row_count,
               COALESCE(SUM(quantity), 0) AS quantity_sum
        FROM txn_items
        """
    )
    changed_rows = query(
        """
        SELECT id, quantity, status, note
        FROM txn_items
        WHERE note <> 'seed' OR status <> 'open'
        ORDER BY id
        """
    )
    return table_state_from_rows(aggregate_rows, changed_rows)


def table_state_from_db(db: SQLDatabaseAdapter) -> TableState:
    def query(sql: str) -> list[dict[str, Any]]:
        return [dict(row) for row in db.execute(sql).fetchall()]

    return table_state_from_query(query)


class DoltgresSession:
    def __init__(self, context: DoltgresBranchContext, branch_id: str):
        self._context = context
        self.branch_id = branch_id

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        rows = self._context.db.execute(sql, params or {}).fetchall()
        return [dict(row) for row in rows]

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> Any:
        return self._context.db.execute(sql, params or {})

    def upsert_rows(self, table: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        placeholders = ", ".join(["(?, ?, ?, ?)"] * len(rows))
        values = [
            value
            for row in rows
            for value in (row["id"], row["quantity"], row["status"], row["note"])
        ]
        self._context.db.execute(
            f"""
            INSERT INTO {table} (id, quantity, status, note)
            VALUES {placeholders}
            """,
            values,
        )

    @contextmanager
    def transaction(self) -> Iterator[None]:
        if self._context.db.in_transaction:
            self._context.db.commit()
        self._context.db.begin()
        try:
            yield
        except Exception:
            self._context.db.rollback()
            raise
        else:
            self._context.db.commit()


class DoltgresBranchContext:
    def __init__(self, db: SQLDatabaseAdapter):
        self.db = db
        self._active_branch: str | None = None

    @classmethod
    def connect(cls, database_url: str) -> DoltgresBranchContext:
        context = cls(connect_sql_database(database_url))
        context.reset()
        return context

    def reset(self) -> None:
        self.db.execute("SELECT dolt_checkout('main')")
        self.db.commit()
        branches = self.db.execute(
            "SELECT name FROM dolt_branches WHERE name <> 'main'"
        ).fetchall()
        for row in branches:
            self.db.execute("SELECT dolt_branch('-D', ?)", (row["name"],))
            self.db.commit()
        self.db.execute("DROP TABLE IF EXISTS txn_items")
        self.db.commit()
        self._commit_if_changed("reset branching transaction benchmark")
        self._active_branch = "main"

    def checkout(self, branch_id: str) -> DoltgresSession:
        self._checkout(branch_id)
        return DoltgresSession(self, branch_id)

    def create_branch(self, branch_id: str, from_branch: str) -> None:
        self._checkout(from_branch)
        self.db.execute("SELECT dolt_checkout('-b', ?)", (branch_id,))
        self.db.commit()
        self._active_branch = branch_id

    def commit_branch(self, branch_id: str, message: str) -> bool:
        self._checkout(branch_id)
        try:
            self.db.execute("SELECT dolt_commit('-A', '-m', ?)", (message,))
            self.db.commit()
            return True
        except Exception as exc:
            self.db.rollback()
            if "nothing to commit" in str(exc).lower():
                return False
            raise

    def merge_into_main(self, branch_id: str) -> None:
        self._checkout("main")
        if self.db.in_transaction:
            self.db.commit()
        self.db.begin()
        try:
            self.db.execute("SELECT * FROM dolt_merge(?)", (branch_id,)).fetchone()
        except Exception:
            self.db.rollback()
            raise
        else:
            self.db.commit()

    def delete_branch(self, branch_id: str) -> None:
        self._checkout("main")
        self.db.execute("SELECT dolt_branch('-D', ?)", (branch_id,))
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def _checkout(self, branch_id: str) -> None:
        if self._active_branch == branch_id:
            return
        self.db.execute("SELECT dolt_checkout(?)", (branch_id,))
        self.db.commit()
        self._active_branch = branch_id

    def _commit_if_changed(self, message: str) -> None:
        try:
            self.db.execute("SELECT dolt_commit('-A', '-m', ?)", (message,))
            self.db.commit()
        except Exception:
            self.db.rollback()


def record(
    detail_rows: list[dict[str, Any]],
    case: TxnCase,
    iteration: int,
    phase: str,
    ms: float,
    *,
    rows_read: int = 0,
    applied: int = 0,
) -> None:
    detail_rows.append(
        {
            "backend": case.backend,
            "dataset_size": case.dataset_size,
            "transaction_count": case.iterations,
            "warmup_iterations": case.warmup_iterations,
            "iteration": iteration,
            "phase": phase,
            "ms": ms,
            "changes_per_operation": case.changes,
            "read_count": case.read_count,
            "rows_read": rows_read,
            "applied": applied,
        }
    )


def run_chronos_case(
    database_url: str,
    case: TxnCase,
    *,
    branch_backend: str = "interval",
) -> list[dict[str, Any]]:
    validate_case(case)
    db = connect_sql_database(database_url)
    reset_postgres_schema(db)
    create_items_table(db, case.dataset_size)
    db.close()

    connect_kwargs: dict[str, Any] = {"backend": branch_backend}
    if branch_backend == "interval":
        connect_kwargs["interval_child_width"] = case.interval_child_width
    if branch_backend == "orpheus":
        connect_kwargs["enable_diff_merge_tracking"] = True
    ctx = ChronosBranchContext.connect(
        database_url,
        **connect_kwargs,
    )
    detail_rows: list[dict[str, Any]] = []
    try:
        ctx.register_table("txn_items", ["id"])
        def run_one(iteration: int, *, measured: bool, prefix: str) -> None:
            branch_id = f"{prefix}_{case.dataset_size}_{iteration}"
            total = Timer()
            phase = Timer()
            ctx.create_branch(
                branch_id,
                from_branch="main",
                terminal=branch_backend == "interval",
            )
            branch_create_ms = phase.lap_ms()
            if measured:
                record(detail_rows, case, iteration, "branch_create", branch_create_ms)

            session = ctx.checkout(branch_id)
            rows_read = 0
            with session.transaction():
                rows_read = execute_workload_sql(session, case, iteration)
            branch_workload_ms = phase.lap_ms()
            if measured:
                record(detail_rows, case, iteration, "branch_workload", branch_workload_ms, rows_read=rows_read)

            merge_kwargs = (
                {"policy": "snapshot_isolation"} if branch_backend == "interval" else {}
            )
            result = ctx.merge_apply(source=branch_id, target="main", **merge_kwargs)
            merge_apply_ms = phase.lap_ms()
            if measured:
                record(
                    detail_rows,
                    case,
                    iteration,
                    "merge_apply",
                    merge_apply_ms,
                    applied=result.applied,
                )

            if case.delete_branches:
                ctx.delete_branch(branch_id)
                branch_delete_ms = phase.lap_ms()
                if measured:
                    record(detail_rows, case, iteration, "branch_delete", branch_delete_ms)
            if measured:
                record(detail_rows, case, iteration, "total", total.elapsed_ms())

        for iteration in range(case.warmup_iterations):
            run_one(iteration, measured=False, prefix="warmup")
        ctx.wait_for_background_work()
        for iteration in range(case.iterations):
            run_one(iteration, measured=True, prefix="txn")
    finally:
        ctx.close()
    return detail_rows


def run_doltgres_case(database_url: str, case: TxnCase) -> list[dict[str, Any]]:
    validate_case(case)
    ctx = DoltgresBranchContext.connect(database_url)
    detail_rows: list[dict[str, Any]] = []
    try:
        create_items_table_with_chunked_load(ctx.db, case.dataset_size)
        ctx.commit_branch("main", f"load {case.dataset_size} rows")
        def run_one(iteration: int, *, measured: bool, prefix: str) -> None:
            branch_id = f"{prefix}_{case.dataset_size}_{iteration}"
            total = Timer()
            phase = Timer()
            ctx.create_branch(branch_id, from_branch="main")
            branch_create_ms = phase.lap_ms()
            if measured:
                record(detail_rows, case, iteration, "branch_create", branch_create_ms)

            session = ctx.checkout(branch_id)
            with session.transaction():
                rows_read = execute_workload_sql(session, case, iteration)
            committed = (
                ctx.commit_branch(branch_id, f"branch txn {iteration}")
                if case.changes > 0
                else False
            )
            branch_workload_ms = phase.lap_ms()
            if measured:
                record(detail_rows, case, iteration, "branch_workload", branch_workload_ms, rows_read=rows_read)

            if committed:
                ctx.merge_into_main(branch_id)
            merge_apply_ms = phase.lap_ms()
            if measured:
                record(detail_rows, case, iteration, "merge_apply", merge_apply_ms)

            if case.delete_branches:
                ctx.delete_branch(branch_id)
                branch_delete_ms = phase.lap_ms()
                if measured:
                    record(detail_rows, case, iteration, "branch_delete", branch_delete_ms)
            if measured:
                record(detail_rows, case, iteration, "total", total.elapsed_ms())

        for iteration in range(case.warmup_iterations):
            run_one(iteration, measured=False, prefix="warmup")
        for iteration in range(case.iterations):
            run_one(iteration, measured=True, prefix="txn")
    finally:
        ctx.close()
    return detail_rows


def run_native_txn_case(database_url: str, case: TxnCase) -> list[dict[str, Any]]:
    validate_case(case)
    db = connect_sql_database(database_url)
    reset_postgres_schema(db)
    create_items_table(db, case.dataset_size)
    detail_rows: list[dict[str, Any]] = []
    try:
        def run_one(iteration: int, *, measured: bool) -> None:
            total = Timer()
            phase = Timer()
            db.begin()
            begin_ms = phase.lap_ms()
            if measured:
                record(detail_rows, case, iteration, "branch_create", begin_ms)
            try:
                rows_read = execute_workload_db(db, case, iteration)
                txn_workload_ms = phase.lap_ms()
                if measured:
                    record(detail_rows, case, iteration, "txn_workload", txn_workload_ms, rows_read=rows_read)
                db.commit()
            except Exception:
                db.rollback()
                raise
            txn_commit_ms = phase.lap_ms()
            if measured:
                record(detail_rows, case, iteration, "txn_commit", txn_commit_ms)
                record(detail_rows, case, iteration, "total", total.elapsed_ms())

        for iteration in range(case.warmup_iterations):
            run_one(iteration, measured=False)
        for iteration in range(case.iterations):
            run_one(iteration, measured=True)
    finally:
        db.close()
    return detail_rows


def run_chronos_final_state(
    database_url: str,
    case: TxnCase,
    *,
    branch_backend: str = "interval",
) -> TableState:
    validate_case(case)
    db = connect_sql_database(database_url)
    reset_postgres_schema(db)
    create_items_table(db, case.dataset_size)
    db.close()

    connect_kwargs: dict[str, Any] = {"backend": branch_backend}
    if branch_backend == "interval":
        connect_kwargs["interval_child_width"] = case.interval_child_width
    if branch_backend == "orpheus":
        connect_kwargs["enable_diff_merge_tracking"] = True
    ctx = ChronosBranchContext.connect(database_url, **connect_kwargs)
    try:
        ctx.register_table("txn_items", ["id"])

        def run_one(iteration: int, *, prefix: str) -> None:
            branch_id = f"{prefix}_{case.dataset_size}_{iteration}"
            ctx.create_branch(
                branch_id,
                from_branch="main",
                terminal=branch_backend == "interval",
            )
            session = ctx.checkout(branch_id)
            with session.transaction():
                execute_workload_sql(session, case, iteration)
            merge_kwargs = (
                {"policy": "snapshot_isolation"} if branch_backend == "interval" else {}
            )
            ctx.merge_apply(source=branch_id, target="main", **merge_kwargs)
            if case.delete_branches:
                ctx.delete_branch(branch_id)

        for iteration in range(case.warmup_iterations):
            run_one(iteration, prefix="warmup")
        ctx.wait_for_background_work()
        for iteration in range(case.iterations):
            run_one(iteration, prefix="txn")
        ctx.wait_for_background_work()
        return table_state_from_query(ctx.checkout("main").query)
    finally:
        ctx.close()


def run_doltgres_final_state(database_url: str, case: TxnCase) -> TableState:
    validate_case(case)
    ctx = DoltgresBranchContext.connect(database_url)
    try:
        create_items_table_with_chunked_load(ctx.db, case.dataset_size)
        ctx.commit_branch("main", f"load {case.dataset_size} rows")

        def run_one(iteration: int, *, prefix: str) -> None:
            branch_id = f"{prefix}_{case.dataset_size}_{iteration}"
            ctx.create_branch(branch_id, from_branch="main")
            session = ctx.checkout(branch_id)
            with session.transaction():
                execute_workload_sql(session, case, iteration)
            committed = (
                ctx.commit_branch(branch_id, f"branch txn {iteration}")
                if case.changes > 0
                else False
            )
            if committed:
                ctx.merge_into_main(branch_id)
            if case.delete_branches:
                ctx.delete_branch(branch_id)

        for iteration in range(case.warmup_iterations):
            run_one(iteration, prefix="warmup")
        for iteration in range(case.iterations):
            run_one(iteration, prefix="txn")
        ctx._checkout("main")
        return table_state_from_db(ctx.db)
    finally:
        ctx.close()


def run_native_txn_final_state(database_url: str, case: TxnCase) -> TableState:
    validate_case(case)
    db = connect_sql_database(database_url)
    reset_postgres_schema(db)
    create_items_table(db, case.dataset_size)
    try:
        def run_one(iteration: int) -> None:
            db.begin()
            try:
                execute_workload_db(db, case, iteration)
                db.commit()
            except Exception:
                db.rollback()
                raise

        for iteration in range(case.warmup_iterations):
            run_one(iteration)
        for iteration in range(case.iterations):
            run_one(iteration)
        return table_state_from_db(db)
    finally:
        db.close()


def run_backend_final_state(
    postgres_url: str | None,
    doltgres_url: str | None,
    case: TxnCase,
) -> TableState:
    if case.backend == "native_txn":
        if not postgres_url:
            raise SystemExit("--postgres-url or CHRONOS_BRANCH_POSTGRES_DSN is required")
        return run_native_txn_final_state(postgres_url, case)
    if case.backend == "chronos":
        if not postgres_url:
            raise SystemExit("--postgres-url or CHRONOS_BRANCH_POSTGRES_DSN is required")
        return run_chronos_final_state(postgres_url, case)
    if case.backend == "orpheus":
        if not postgres_url:
            raise SystemExit("--postgres-url or CHRONOS_BRANCH_POSTGRES_DSN is required")
        return run_chronos_final_state(postgres_url, case, branch_backend="orpheus")
    if case.backend == "doltgres":
        if not doltgres_url:
            raise SystemExit("--doltgres-url or CHRONOS_BRANCH_DOLTGRES_DSN is required")
        return run_doltgres_final_state(doltgres_url, case)
    raise AssertionError(case.backend)


def verification_message(reference: TableState, actual: TableState) -> str:
    if reference == actual:
        return "ok"
    if reference.row_count != actual.row_count:
        return f"row count mismatch: native={reference.row_count} backend={actual.row_count}"
    if reference.quantity_sum != actual.quantity_sum:
        return (
            "quantity sum mismatch: "
            f"native={reference.quantity_sum} backend={actual.quantity_sum}"
        )
    if len(reference.changed_rows) != len(actual.changed_rows):
        return (
            "changed row count mismatch: "
            f"native={len(reference.changed_rows)} backend={len(actual.changed_rows)}"
        )
    for index, (expected, observed) in enumerate(
        zip(reference.changed_rows, actual.changed_rows)
    ):
        if expected != observed:
            return f"changed row mismatch at {index}: native={expected} backend={observed}"
    return "table state mismatch"


def verification_row(
    case: TxnCase,
    reference: TableState,
    actual: TableState,
) -> dict[str, Any]:
    message = verification_message(reference, actual)
    return {
        "backend": case.backend,
        "dataset_size": case.dataset_size,
        "transaction_count": case.iterations,
        "warmup_iterations": case.warmup_iterations,
        "changes_per_operation": case.changes,
        "read_count": case.read_count,
        "delete_branches": case.delete_branches,
        "matched": message == "ok",
        "reference_row_count": reference.row_count,
        "backend_row_count": actual.row_count,
        "reference_quantity_sum": reference.quantity_sum,
        "backend_quantity_sum": actual.quantity_sum,
        "reference_changed_rows": len(reference.changed_rows),
        "backend_changed_rows": len(actual.changed_rows),
        "message": message,
    }


def summarize_detail_rows(
    detail_rows: list[dict[str, Any]],
    *,
    final_memory_bytes: int | None = None,
    peak_memory_bytes: int | None = None,
) -> list[dict[str, Any]]:
    by_group: dict[tuple[str, int, int, int, str], list[float]] = {}
    for row in detail_rows:
        by_group.setdefault(
            (
                row["backend"],
                int(row["dataset_size"]),
                int(row["transaction_count"]),
                int(row["warmup_iterations"]),
                row["phase"],
            ),
            [],
        ).append(float(row["ms"]))

    summary_rows: list[dict[str, Any]] = []
    for (backend, dataset_size, transaction_count, warmup_iterations, phase), values in sorted(by_group.items()):
        stats = summarize(values)
        summary_rows.append(
            {
                "backend": backend,
                "dataset_size": dataset_size,
                "transaction_count": transaction_count,
                "warmup_iterations": warmup_iterations,
                "phase": phase,
                "iterations": int(stats["iterations"]),
                "median_ms": stats["median_ms"],
                "avg_ms": stats["avg_ms"],
                "p95_ms": stats["p95_ms"],
                "total_ms": stats["total_ms"],
                "final_memory_bytes": final_memory_bytes,
                "final_memory_mb": memory_mb(final_memory_bytes),
                "peak_memory_bytes": peak_memory_bytes,
                "peak_memory_mb": memory_mb(peak_memory_bytes),
            }
        )
    return summary_rows


def initialize_result_files(output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    detail_path = output_dir / "branching_transaction_details.csv"
    summary_path = output_dir / "branching_transaction_summary.csv"
    with detail_path.open("w", newline="") as f:
        csv.DictWriter(f, fieldnames=DETAIL_FIELDS).writeheader()
    with summary_path.open("w", newline="") as f:
        csv.DictWriter(f, fieldnames=SUMMARY_FIELDS).writeheader()
    return detail_path, summary_path


def initialize_verification_file(output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    verification_path = output_dir / "branching_transaction_verification.csv"
    with verification_path.open("w", newline="") as f:
        csv.DictWriter(f, fieldnames=VERIFICATION_FIELDS).writeheader()
    return verification_path


def append_verification_row(output_dir: Path, row: dict[str, Any]) -> None:
    with (output_dir / "branching_transaction_verification.csv").open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=VERIFICATION_FIELDS)
        writer.writerow(row)


def append_result_rows(
    output_dir: Path,
    detail_rows: list[dict[str, Any]],
    *,
    final_memory_bytes: int | None = None,
    peak_memory_bytes: int | None = None,
) -> list[dict[str, Any]]:
    if not detail_rows:
        return []
    detail_path = output_dir / "branching_transaction_details.csv"
    summary_path = output_dir / "branching_transaction_summary.csv"
    summary_rows = summarize_detail_rows(
        detail_rows,
        final_memory_bytes=final_memory_bytes,
        peak_memory_bytes=peak_memory_bytes,
    )
    with detail_path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DETAIL_FIELDS)
        writer.writerows(detail_rows)
    with summary_path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS)
        writer.writerows(summary_rows)
    return summary_rows


def write_results(output_dir: Path, detail_rows: list[dict[str, Any]]) -> None:
    detail_path, summary_path = initialize_result_files(output_dir)
    summary_rows = append_result_rows(output_dir, detail_rows)
    plot_results(output_dir, summary_rows)
    print(f"Wrote detail results to {detail_path}")
    print(f"Wrote summary results to {summary_path}")


def plot_results(output_dir: Path, summary_rows: list[dict[str, Any]]) -> None:
    phase_rows = [row for row in summary_rows if row["phase"] != "total"]
    if not phase_rows:
        return
    plot_stacked_breakdown(
        output_dir,
        phase_rows,
        x_field="dataset_size",
        facet_field="transaction_count",
        output_name="branching_transaction_by_dataset.png",
        title="Branching Transaction Latency by Dataset Size",
        show_facet_title=True,
    )
    for dataset_size in sorted({_summary_int(row, "dataset_size") for row in phase_rows}):
        dataset_rows = [
            row for row in phase_rows
            if _summary_int(row, "dataset_size") == dataset_size
        ]
        plot_stacked_breakdown(
            output_dir,
            dataset_rows,
            x_field="transaction_count",
            facet_field="dataset_size",
            output_name=f"branching_transaction_by_iterations_dataset_{dataset_size}.png",
            title=f"Branching Transaction Latency by Transaction Count ({dataset_size:,} rows)",
            show_facet_title=False,
        )


def plot_stacked_breakdown(
    output_dir: Path,
    phase_rows: list[dict[str, Any]],
    *,
    x_field: str,
    facet_field: str,
    output_name: str,
    title: str,
    show_facet_title: bool,
) -> None:
    backends = [backend for backend in BACKENDS if any(row["backend"] == backend for row in phase_rows)]
    x_values = sorted({_summary_int(row, x_field) for row in phase_rows})
    facet_values = sorted({_summary_int(row, facet_field) for row in phase_rows})
    labels = {
        "chronos": "Chronos",
        "orpheus": "OrpheusDB",
        "doltgres": "Doltgres",
        "native_txn": "PostgreSQL txn",
    }
    stage_order = ["fork / begin", "workload", "merge / commit", "delete"]
    phase_to_stage = {
        "branch_create": "fork / begin",
        "branch_workload": "workload",
        "txn_workload": "workload",
        "merge_apply": "merge / commit",
        "txn_commit": "merge / commit",
        "branch_delete": "delete",
    }
    stage_colors = {
        "fork / begin": "#4c78a8",
        "workload": "#f58518",
        "merge / commit": "#54a24b",
        "delete": "#b279a2",
    }
    values: dict[tuple[str, int, int, str], float] = {}
    for row in phase_rows:
        stage = phase_to_stage.get(str(row["phase"]))
        if stage is None:
            continue
        key = (
            str(row["backend"]),
            _summary_int(row, x_field),
            _summary_int(row, facet_field),
            stage,
        )
        values[key] = values.get(key, 0.0) + float(row["avg_ms"])

    fig_width = max(9, 2.2 * len(x_values) * max(1, len(facet_values)))
    fig, axes = plt.subplots(
        1,
        len(facet_values),
        figsize=(fig_width, 5.8),
        sharey=True,
        squeeze=False,
    )
    group_width = 0.8
    bar_width = group_width / max(1, len(backends))
    legend_handles: dict[str, Any] = {}
    max_total = 0.0
    for facet_idx, facet_value in enumerate(facet_values):
        ax = axes[0][facet_idx]
        x_positions = list(range(len(x_values)))
        for backend_idx, backend in enumerate(backends):
            offsets = [
                item - group_width / 2 + bar_width * (backend_idx + 0.5)
                for item in x_positions
            ]
            bottoms = [0.0 for _ in x_values]
            for stage in stage_order:
                heights = [
                    values.get((backend, x_value, facet_value, stage), 0.0)
                    for x_value in x_values
                ]
                bars = ax.bar(
                    offsets,
                    heights,
                    width=bar_width,
                    bottom=bottoms,
                    label=stage,
                    color=stage_colors[stage],
                    edgecolor="white",
                    linewidth=0.5,
                )
                legend_handles.setdefault(stage, bars[0])
                bottoms = [bottom + height for bottom, height in zip(bottoms, heights)]
            for xpos, total in zip(offsets, bottoms):
                max_total = max(max_total, total)
                if total > 0:
                    ax.text(
                        xpos,
                        total,
                        f"{total:.0f}ms",
                        ha="center",
                        va="bottom",
                        fontsize=8,
                        rotation=90,
                    )

        xticks: list[float] = []
        xticklabels: list[str] = []
        for item, x_value in zip(x_positions, x_values):
            for backend_idx, backend in enumerate(backends):
                xticks.append(item - group_width / 2 + bar_width * (backend_idx + 0.5))
                xticklabels.append(labels[backend])
            ax.text(
                item,
                -0.18,
                _axis_value_label(x_field, x_value),
                ha="center",
                va="top",
                transform=ax.get_xaxis_transform(),
                fontsize=9,
            )
        ax.set_xticks(xticks)
        ax.set_xticklabels(xticklabels, rotation=25, ha="right")
        ax.set_title(_facet_title(facet_field, facet_value) if show_facet_title else "")
        ax.grid(axis="y", alpha=0.25)
        if facet_idx == 0:
            ax.set_ylabel("average stage latency (ms)")
    for ax in axes[0]:
        if max_total > 0:
            ax.set_ylim(top=max_total * 1.18)

    fig.suptitle(title, y=0.98)
    fig.legend(
        [legend_handles[stage] for stage in stage_order if stage in legend_handles],
        [stage for stage in stage_order if stage in legend_handles],
        loc="upper center",
        bbox_to_anchor=(0.5, 0.93),
        ncols=min(4, len(legend_handles)),
        frameon=False,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    plot_path = output_dir / output_name
    fig.savefig(plot_path, dpi=180)
    plt.close(fig)


def _summary_int(row: dict[str, Any], field: str) -> int:
    if field == "transaction_count":
        value = row.get("transaction_count") or row.get("iterations")
    else:
        value = row[field]
    return int(value)


def _axis_value_label(field: str, value: int) -> str:
    if field == "dataset_size":
        return f"{value:,} rows"
    if field == "transaction_count":
        return f"{value:,} txns"
    return f"{value:,}"


def _facet_title(field: str, value: int) -> str:
    if field == "dataset_size":
        return f"dataset={value:,} rows"
    if field == "transaction_count":
        return f"transactions={value:,}"
    return f"{field}={value:,}"


def resolve_output_dir(value: str | None) -> Path:
    if value is None:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        return Path(".benchmarks") / f"branching-transaction-{stamp}"
    path = Path(value)
    return path if path.is_absolute() else Path.cwd() / path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Single-threaded benchmark for branch-transaction latency using "
            "Chronos interval branches, OrpheusDB-style versioning, "
            "native Doltgres branches, and plain PostgreSQL transactions."
        )
    )
    parser.add_argument("--postgres-url", default=os.environ.get("CHRONOS_BRANCH_POSTGRES_DSN"))
    parser.add_argument("--doltgres-url", default=os.environ.get("CHRONOS_BRANCH_DOLTGRES_DSN"))
    parser.add_argument(
        "--postgres-container",
        default=os.environ.get("CHRONOS_BENCH_POSTGRES_CONTAINER"),
        help="Docker container name used to sample memory for Chronos/native PostgreSQL configs.",
    )
    parser.add_argument(
        "--doltgres-container",
        default=os.environ.get("CHRONOS_BENCH_DOLTGRES_CONTAINER"),
        help="Docker container name used to sample memory for Doltgres configs.",
    )
    parser.add_argument(
        "--memory-sample-interval",
        type=float,
        default=float(os.environ.get("CHRONOS_BENCH_MEMORY_SAMPLE_INTERVAL", "1.0")),
        help="Seconds between Docker memory samples. Default: 1.0.",
    )
    parser.add_argument("--backends", type=parse_backends, default=list(DEFAULT_BACKENDS))
    parser.add_argument(
        "--dataset-sizes",
        type=parse_int_list,
        default=[10000000],
        help="Comma-separated dataset sizes to run separately. Default: 10000000.",
    )
    parser.add_argument(
        "--iterations",
        type=parse_int_list,
        default=[100, 1000, 10000, 100000],
        help="Comma-separated transaction counts to run separately. Default: 100,1000,10000.",
    )
    parser.add_argument("--changes", type=int, default=1)
    parser.add_argument("--read-count", type=int, default=3)
    parser.add_argument(
        "--warmup-iterations",
        type=int,
        default=int(os.environ.get("CHRONOS_BRANCH_WARMUP_ITERATIONS", "100")),
        help="Unrecorded transactions to run before each benchmark config. Default: 100.",
    )
    parser.add_argument(
        "--delete-branches",
        action="store_true",
        default=os.environ.get("CHRONOS_BRANCH_DELETE_BRANCHES", "0") == "1",
        help="Delete each branch after merge and include branch_delete latency. Default: disabled.",
    )
    parser.add_argument(
        "--interval-child-width",
        type=int,
        default=int(os.environ.get("CHRONOS_INTERVAL_CHILD_WIDTH", "2")),
        help=(
            "Fixed mutable interval width assigned to each branch-transaction child. "
            "This makes serial branches from main consume interval space linearly. "
            "Default: 2."
        ),
    )
    parser.add_argument("--output-dir", default=os.environ.get("CHRONOS_BENCH_OUTPUT_DIR"))
    parser.add_argument(
        "--verify-against-native",
        action="store_true",
        help=(
            "Replay each non-native backend case and compare the final table "
            "state against a native PostgreSQL transaction replay."
        ),
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Run final-state verification without collecting timing results.",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Use a small smoke-test configuration.",
    )
    args = parser.parse_args()
    if args.quick:
        args.dataset_sizes = [1_000]
        args.iterations = [2]
        args.changes = 10
        args.read_count = 10

    if args.verify_only:
        args.verify_against_native = True

    if (
        any(backend in args.backends for backend in ("chronos", "orpheus", "native_txn"))
        or args.verify_against_native
    ) and not args.postgres_url:
        raise SystemExit("--postgres-url or CHRONOS_BRANCH_POSTGRES_DSN is required")
    if "doltgres" in args.backends and not args.doltgres_url:
        raise SystemExit("--doltgres-url or CHRONOS_BRANCH_DOLTGRES_DSN is required")

    output_dir = resolve_output_dir(args.output_dir)
    if args.verify_only:
        detail_path = summary_path = None
    else:
        detail_path, summary_path = initialize_result_files(output_dir)
    verification_path = (
        initialize_verification_file(output_dir)
        if args.verify_against_native
        else None
    )
    all_summary_rows: list[dict[str, Any]] = []
    native_state_cache: dict[tuple[int, int, int, int, int, bool], TableState] = {}
    backend_containers = {
        "chronos": args.postgres_container,
        "orpheus": args.postgres_container,
        "native_txn": args.postgres_container,
        "doltgres": args.doltgres_container,
    }
    for backend in args.backends:
        for dataset_size in args.dataset_sizes:
            for iteration_count in args.iterations:
                case = TxnCase(
                    backend=backend,
                    dataset_size=dataset_size,
                    iterations=iteration_count,
                    changes=args.changes,
                    read_count=args.read_count,
                    warmup_iterations=args.warmup_iterations,
                    delete_branches=args.delete_branches,
                    interval_child_width=args.interval_child_width,
                )
                progress(
                    f"backend={backend} dataset={dataset_size} iterations={iteration_count} "
                    f"warmup={args.warmup_iterations} changes={args.changes} "
                    f"read_count={args.read_count} delete_branches={args.delete_branches}"
                )
                if not args.verify_only:
                    container_name = backend_containers[backend]
                    with MemoryTracker(
                        container_name,
                        interval_seconds=max(0.1, args.memory_sample_interval),
                    ) as memory:
                        if backend == "chronos":
                            detail_rows = run_chronos_case(args.postgres_url, case)
                        elif backend == "orpheus":
                            detail_rows = run_chronos_case(
                                args.postgres_url,
                                case,
                                branch_backend="orpheus",
                            )
                        elif backend == "doltgres":
                            detail_rows = run_doltgres_case(args.doltgres_url, case)
                        elif backend == "native_txn":
                            detail_rows = run_native_txn_case(args.postgres_url, case)
                        else:
                            raise AssertionError(backend)
                    all_summary_rows.extend(
                        append_result_rows(
                            output_dir,
                            detail_rows,
                            final_memory_bytes=memory.final_bytes,
                            peak_memory_bytes=memory.peak_bytes,
                        )
                    )
                if args.verify_against_native and backend != "native_txn":
                    progress(
                        f"verify backend={backend} dataset={dataset_size} "
                        f"iterations={iteration_count}"
                    )
                    cache_key = (
                        dataset_size,
                        iteration_count,
                        args.warmup_iterations,
                        args.changes,
                        args.read_count,
                        args.delete_branches,
                    )
                    reference = native_state_cache.get(cache_key)
                    if reference is None:
                        native_case = TxnCase(
                            backend="native_txn",
                            dataset_size=dataset_size,
                            iterations=iteration_count,
                            changes=args.changes,
                            read_count=args.read_count,
                            warmup_iterations=args.warmup_iterations,
                            delete_branches=args.delete_branches,
                            interval_child_width=args.interval_child_width,
                        )
                        reference = run_native_txn_final_state(
                            args.postgres_url,
                            native_case,
                        )
                        native_state_cache[cache_key] = reference
                    actual = run_backend_final_state(
                        args.postgres_url,
                        args.doltgres_url,
                        case,
                    )
                    row = verification_row(case, reference, actual)
                    append_verification_row(output_dir, row)
                    if not row["matched"]:
                        raise SystemExit(
                            f"verification failed for {backend}: {row['message']}"
                        )

    if not args.verify_only:
        plot_results(output_dir, all_summary_rows)
        print(f"Wrote detail results to {detail_path}")
        print(f"Wrote summary results to {summary_path}")
    if verification_path is not None:
        print(f"Wrote verification results to {verification_path}")


if __name__ == "__main__":
    main()
