from __future__ import annotations

import importlib.util
import sys
import threading
import time
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "run_merge_throughput_microbench.py"
SPEC = importlib.util.spec_from_file_location("merge_throughput_microbench", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_big_lock_serializes_complete_operation() -> None:
    gate = MODULE.MergeGate()
    active = 0
    maximum = 0
    guard = threading.Lock()
    timings: list[dict[str, float]] = []

    def operation() -> None:
        nonlocal active, maximum
        with guard:
            active += 1
            maximum = max(maximum, active)
        time.sleep(0.01)
        with guard:
            active -= 1

    def worker() -> None:
        _, timing = gate.execute(operation)
        timings.append(timing)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert maximum == 1
    assert len(timings) == 2
    assert all(item["critical_section_ms"] >= 5.0 for item in timings)
    assert any(item["lock_wait_ms"] >= 5.0 for item in timings)


def test_all_change_ids_collects_store_groups_and_flat_entries() -> None:
    preview = {
        "change_ids": ["relational:one"],
        "selection_groups": {
            "indexed_documents": {"doc": ["qdrant:two"]},
            "filesystem_paths": {"/doc.md": ["filesystem:three"]},
        },
        "conflicts": [{"change_id": "conflict:four"}],
    }
    assert MODULE._all_change_ids(preview) == [
        "conflict:four",
        "filesystem:three",
        "qdrant:two",
        "relational:one",
    ]
