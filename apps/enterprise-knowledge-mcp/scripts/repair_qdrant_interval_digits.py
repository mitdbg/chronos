#!/usr/bin/env python3
"""Rewrite Chronos interval payload digits after a coordinate-base change.

Chronos stores the exact interval endpoints as decimal strings in Qdrant and
uses the split integer fields for range predicates.  This utility recomputes
the split fields from those exact strings without touching vectors or user
payloads.  It is intended for an in-place repair of a prepared experiment
state; new states use the current encoding during ingestion.
"""

from __future__ import annotations

import argparse
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from qdrant_client import QdrantClient, models


BASE = 1 << 52


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:6340")
    parser.add_argument("--collection", required=True)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--log", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive")
    log = args.log.open("a", encoding="utf-8") if args.log else None

    def report(value: dict[str, Any]) -> None:
        line = json.dumps(value, sort_keys=True)
        print(line, flush=True)
        if log is not None:
            log.write(line + "\n")
            log.flush()

    client = QdrantClient(url=args.url, timeout=3600)
    offset: Any = None
    scanned = 0
    updated = 0
    started = time.monotonic()
    while True:
        points, next_offset = client.scroll(
            collection_name=args.collection,
            offset=offset,
            limit=args.batch_size,
            with_payload=[
                "_chronos_low",
                "_chronos_high",
                "_chronos_low_hi",
                "_chronos_low_lo",
                "_chronos_high_hi",
                "_chronos_high_lo",
            ],
            with_vectors=False,
            timeout=3600,
        )
        if not points:
            break
        groups: dict[tuple[int, int, int, int], list[Any]] = defaultdict(list)
        for point in points:
            payload = point.payload or {}
            low = int(payload["_chronos_low"])
            high = int(payload["_chronos_high"])
            low_hi, low_lo = divmod(low, BASE)
            high_hi, high_lo = divmod(high, BASE)
            current = (
                payload.get("_chronos_low_hi"),
                payload.get("_chronos_low_lo"),
                payload.get("_chronos_high_hi"),
                payload.get("_chronos_high_lo"),
            )
            desired = (low_hi, low_lo, high_hi, high_lo)
            if tuple(int(value) for value in current) != desired:
                groups[desired].append(point.id)
        for (low_hi, low_lo, high_hi, high_lo), point_ids in groups.items():
            client.set_payload(
                collection_name=args.collection,
                payload={
                    "_chronos_low_hi": low_hi,
                    "_chronos_low_lo": low_lo,
                    "_chronos_high_hi": high_hi,
                    "_chronos_high_lo": high_lo,
                },
                points=point_ids,
                wait=True,
                ordering=models.WriteOrdering.STRONG,
                timeout=3600,
            )
            updated += len(point_ids)
        scanned += len(points)
        report(
            {
                "scanned": scanned,
                "updated": updated,
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "offset": str(next_offset),
            }
        )
        if next_offset is None:
            break
        offset = next_offset
    report({"complete": True, "scanned": scanned, "updated": updated})
    if log is not None:
        log.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
