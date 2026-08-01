#!/usr/bin/env python3
"""Measure local Chroma ingestion over a prepared EnterpriseRAG prefix.

Local Chroma does not support sparse-vector indexes.  This probe therefore
stores chunk text, the interval-filter metadata needed by Chronos, and a
one-dimensional placeholder dense vector.  It reports Chroma write time
separately from snapshot decoding so the result is not mistaken for an
equivalent BM25 benchmark.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import chromadb
from chromadb import FtsIndexConfig, K, Schema, StringInvertedIndexConfig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--max-documents", type=int, default=2_048)
    parser.add_argument("--point-batch-size", type=int, default=250)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--progress-every", type=int, default=25_000)
    return parser.parse_args()


def text_compression(db: sqlite3.Connection) -> str:
    row = db.execute(
        """
        SELECT value_json
        FROM snapshot_metadata
        WHERE key = 'spec'
        """
    ).fetchone()
    if row is None:
        return "none"
    value = json.loads(str(row[0]))
    return str(value.get("text_compression", "none"))


def decode_text(value: Any, compression: str) -> str:
    if compression == "zlib":
        return zlib.decompress(bytes(value)).decode("utf-8")
    return str(value)


def selected_document_ids(
    db: sqlite3.Connection,
    maximum: int,
) -> list[str]:
    return [
        str(row[0])
        for row in db.execute(
            """
            SELECT id
            FROM documents
            ORDER BY relative_path
            LIMIT ?
            """,
            (maximum,),
        )
    ]


def iter_chunk_batches(
    db: sqlite3.Connection,
    document_ids: list[str],
    *,
    compression: str,
    batch_size: int,
):
    ids: list[str] = []
    documents: list[str] = []
    metadatas: list[dict[str, Any]] = []
    for document_id in document_ids:
        for row in db.execute(
            """
            SELECT id, ordinal, text
            FROM chunks
            WHERE document_id = ?
            ORDER BY ordinal, id
            """,
            (document_id,),
        ):
            ids.append(str(row[0]))
            documents.append(decode_text(row[2], compression))
            metadatas.append(
                {
                    # Root-visible interval metadata. Chroma builds numeric
                    # and boolean indexes for these fields.
                    "low_hi": 0,
                    "low_lo": 0,
                    "high_hi": (1 << 31) - 1,
                    "high_lo": (1 << 31) - 1,
                    "active": True,
                    "deleted": False,
                    "ordinal": int(row[1]),
                }
            )
            if len(ids) == batch_size:
                yield ids, documents, metadatas
                ids, documents, metadatas = [], [], []
    if ids:
        yield ids, documents, metadatas


def directory_bytes(path: Path) -> int:
    return sum(
        item.stat().st_size
        for item in path.rglob("*")
        if item.is_file()
    )


def main() -> int:
    args = parse_args()
    if args.max_documents <= 0:
        raise ValueError("--max-documents must be positive")
    if args.point_batch_size <= 0:
        raise ValueError("--point-batch-size must be positive")
    if args.workers <= 0:
        raise ValueError("--workers must be positive")

    snapshot_db = args.snapshot.expanduser().resolve() / "snapshot.sqlite"
    output = args.output.expanduser().resolve()
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)

    source = sqlite3.connect(
        f"file:{snapshot_db}?mode=ro&immutable=1",
        uri=True,
    )
    source.execute("PRAGMA mmap_size=1073741824")
    compression = text_compression(source)
    document_ids = selected_document_ids(source, args.max_documents)

    schema = Schema()
    schema.delete_index(config=FtsIndexConfig(), key=K.DOCUMENT)
    schema.delete_index(config=StringInvertedIndexConfig())
    client = chromadb.PersistentClient(path=output)
    collection = client.create_collection(
        name="enterprise_chunks",
        schema=schema,
        embedding_function=None,
    )

    started = time.monotonic()
    write_seconds = 0.0
    chunks = 0
    next_progress = args.progress_every

    def add(batch: tuple[list[str], list[str], list[dict[str, Any]]]):
        batch_ids, batch_documents, batch_metadata = batch
        add_started = time.monotonic()
        collection.add(
            ids=batch_ids,
            documents=batch_documents,
            metadatas=batch_metadata,
            embeddings=[[0.0]] * len(batch_ids),
        )
        return len(batch_ids), time.monotonic() - add_started

    pending = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        for batch in iter_chunk_batches(
            source,
            document_ids,
            compression=compression,
            batch_size=args.point_batch_size,
        ):
            pending.append(executor.submit(add, batch))
            if len(pending) < args.workers * 2:
                continue
            completed = pending.pop(0).result()
            chunks += completed[0]
            write_seconds += completed[1]
            if chunks >= next_progress:
                elapsed = time.monotonic() - started
                print(
                    json.dumps(
                        {
                            "chunks": chunks,
                            "elapsed_seconds": round(elapsed, 3),
                            "chunks_per_second": round(chunks / elapsed, 3),
                        }
                    ),
                    flush=True,
                )
                next_progress += args.progress_every
        for future in pending:
            completed = future.result()
            chunks += completed[0]
            write_seconds += completed[1]

    elapsed = time.monotonic() - started
    exact_count = collection.count()
    if exact_count != chunks:
        raise RuntimeError(
            f"Chroma count mismatch: expected {chunks}, found {exact_count}"
        )
    result = {
        "backend": "chroma-local",
        "chroma_version": chromadb.__version__,
        "semantic_scope": (
            "text+interval metadata+1D placeholder HNSW; "
            "local Chroma has no sparse/BM25 index"
        ),
        "documents": len(document_ids),
        "chunks": chunks,
        "workers": args.workers,
        "point_batch_size": args.point_batch_size,
        "elapsed_seconds": round(elapsed, 3),
        "chunks_per_second": round(chunks / elapsed, 3),
        "aggregate_request_seconds": round(write_seconds, 3),
        "storage_bytes": directory_bytes(output),
        "count": exact_count,
    }
    result_path = output.parent / f"{output.name}-result.json"
    result_path.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
