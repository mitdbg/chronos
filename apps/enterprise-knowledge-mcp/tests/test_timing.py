from __future__ import annotations

import time
from pathlib import Path

from chronos_enterprise_knowledge.timing import (
    StoreTimingCollector,
    instrument_object,
)


def test_store_timing_charges_nested_spans_exclusively() -> None:
    collector = StoreTimingCollector()
    with collector.active():
        with collector.span("relational_db"):
            time.sleep(0.001)
            with collector.span("vector_db"):
                time.sleep(0.001)
    values = collector.snapshot_ms()
    assert values["relational_db"] > 0
    assert values["vector_db"] > 0
    assert values["relational_db"] < values["vector_db"] * 2.5


def test_store_proxy_times_cursor_fetches() -> None:
    class Cursor:
        def fetchone(self):
            time.sleep(0.001)
            return {"value": 1}

    class Connection:
        def execute(self, query):
            del query
            return Cursor()

    collector = StoreTimingCollector()
    connection = instrument_object(Connection(), "relational_db")
    with collector.active():
        assert connection.execute("select 1").fetchone()["value"] == 1
    assert collector.snapshot_ms()["relational_db"] > 0


def test_store_proxy_leaves_path_values_unwrapped() -> None:
    class Filesystem:
        def checkout(self):
            return Path("/tmp/branch-checkout")

    filesystem = instrument_object(Filesystem(), "filesystem")
    checkout = filesystem.checkout()
    assert isinstance(checkout, Path)
    assert str(checkout) == "/tmp/branch-checkout"
