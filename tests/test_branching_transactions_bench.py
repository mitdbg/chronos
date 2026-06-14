from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from test_branching import _postgres_dsn


def _load_branching_transactions_module():
    module_name = "_chronos_bench_branching_transactions"
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing
    module_path = (
        Path(__file__).resolve().parents[1]
        / "bench"
        / "transactions"
        / "branching_transactions.py"
    )
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _small_case(backend: str, *, delete_branches: bool = True):
    bench = _load_branching_transactions_module()
    return bench.TxnCase(
        backend=backend,
        dataset_size=1_000,
        iterations=5,
        changes=2,
        read_count=3,
        warmup_iterations=3,
        delete_branches=delete_branches,
        interval_child_width=2,
    )


def test_branching_transaction_defaults_do_not_include_orpheus() -> None:
    bench = _load_branching_transactions_module()

    assert "orpheus" in bench.BACKENDS
    assert "orpheus" not in bench.DEFAULT_BACKENDS


def test_chronos_interval_branching_transactions_match_native() -> None:
    bench = _load_branching_transactions_module()
    dsn = _postgres_dsn()
    for delete_branches in (False, True):
        native_case = _small_case("native_txn", delete_branches=delete_branches)
        chronos_case = _small_case("chronos", delete_branches=delete_branches)

        reference = bench.run_native_txn_final_state(dsn, native_case)
        actual = bench.run_chronos_final_state(dsn, chronos_case)

        assert bench.verification_message(reference, actual) == "ok"
        row = bench.verification_row(chronos_case, reference, actual)
        assert row["matched"] is True
        assert row["reference_row_count"] == row["backend_row_count"] == 1_000
        assert row["reference_changed_rows"] == row["backend_changed_rows"] == 10


def test_orpheus_branching_transactions_match_native() -> None:
    bench = _load_branching_transactions_module()
    dsn = _postgres_dsn()
    native_case = _small_case("native_txn")
    orpheus_case = _small_case("orpheus")

    reference = bench.run_native_txn_final_state(dsn, native_case)
    actual = bench.run_chronos_final_state(
        dsn,
        orpheus_case,
        branch_backend="orpheus",
    )

    assert bench.verification_message(reference, actual) == "ok"
    row = bench.verification_row(orpheus_case, reference, actual)
    assert row["matched"] is True
    assert row["reference_quantity_sum"] == row["backend_quantity_sum"]
