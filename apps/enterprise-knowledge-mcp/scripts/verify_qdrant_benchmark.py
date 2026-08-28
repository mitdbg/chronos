#!/usr/bin/env python3
"""Fail closed when a benchmark Qdrant collection is misconfigured."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path

from qdrant_client import QdrantClient, models

from chronos_enterprise_knowledge.retrieval import BM25_VECTOR, DENSE_VECTOR


_REQUIRED_PAYLOAD_INDEXES = {
    "chronos": {
        "_chronos_logical_id",
        "_chronos_low_hi",
        "_chronos_low_lo",
        "_chronos_high_hi",
        "_chronos_high_lo",
        "_chronos_writer",
        "_chronos_deleted",
    },
    "app-managed": {
        "branch_id",
        "overwritten_in[].by",
        "overwritten_in[].revision",
        "revision",
    },
    "doltgres-qdrant-btrfs": {
        "branch",
        "seq",
        "overwritten_in[].by",
        "overwritten_in[].seq",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--qdrant-url", required=True)
    parser.add_argument(
        "--backend",
        choices=tuple(_REQUIRED_PAYLOAD_INDEXES),
        required=True,
    )
    parser.add_argument("--dimensions", type=int, required=True)
    parser.add_argument("--shards", type=int, required=True)
    parser.add_argument(
        "--require-dense-vectors",
        action="store_true",
        help="Require a nonzero dense vector in a sample of stored points.",
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = QdrantClient(url=args.qdrant_url, timeout=60)
    try:
        collections = client.get_collections().collections
        if len(collections) != 1:
            raise SystemExit(
                "expected exactly one collection in fresh benchmark Qdrant "
                f"storage, found {[item.name for item in collections]}"
            )
        name = collections[0].name
        info = client.get_collection(name)
        params = info.config.params
        vectors = params.vectors
        if not isinstance(vectors, Mapping) or DENSE_VECTOR not in vectors:
            raise SystemExit(f"{name}: missing named dense vector {DENSE_VECTOR!r}")
        dense = vectors[DENSE_VECTOR]
        if int(dense.size) != args.dimensions:
            raise SystemExit(
                f"{name}: dense dimensions {dense.size} != {args.dimensions}"
            )
        if dense.distance != models.Distance.COSINE:
            raise SystemExit(f"{name}: dense distance is not cosine")
        if not dense.on_disk or not params.on_disk_payload:
            raise SystemExit(f"{name}: vectors and payloads must be on disk")
        sparse = params.sparse_vectors or {}
        if BM25_VECTOR not in sparse:
            raise SystemExit(f"{name}: missing sparse vector {BM25_VECTOR!r}")
        if int(params.shard_number) != args.shards:
            raise SystemExit(
                f"{name}: shard count {params.shard_number} != {args.shards}"
            )
        payload_indexes = set(info.payload_schema)
        required_indexes = _REQUIRED_PAYLOAD_INDEXES[args.backend]
        missing_indexes = sorted(required_indexes - payload_indexes)
        if missing_indexes:
            raise SystemExit(
                f"{name}: missing payload indexes: {', '.join(missing_indexes)}"
            )
        dense_sample = 0
        if args.require_dense_vectors:
            points, _ = client.scroll(
                collection_name=name,
                limit=16,
                with_payload=False,
                with_vectors=True,
            )
            for point in points:
                vector = point.vector
                if isinstance(vector, Mapping):
                    vector = vector.get(DENSE_VECTOR)
                if vector is not None and any(
                    abs(float(value)) > 1e-8 for value in vector
                ):
                    dense_sample += 1
            if not dense_sample:
                raise SystemExit(
                    f"{name}: sampled points do not contain nonzero dense vectors"
                )
        report = {
            "schema_version": 1,
            "backend": args.backend,
            "qdrant_url": args.qdrant_url,
            "collection": name,
            "dimensions": int(dense.size),
            "distance": str(dense.distance),
            "dense_vector": DENSE_VECTOR,
            "sparse_vectors": sorted(sparse),
            "on_disk_vectors": bool(dense.on_disk),
            "on_disk_payload": bool(params.on_disk_payload),
            "shards": int(params.shard_number),
            "required_payload_indexes": sorted(required_indexes),
            "payload_indexes": sorted(payload_indexes),
            "dense_sample_points": dense_sample,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(report, indent=2) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(report, sort_keys=True))
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
