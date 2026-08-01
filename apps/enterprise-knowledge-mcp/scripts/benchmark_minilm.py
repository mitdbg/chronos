"""Measure MiniLM throughput on real EnterpriseRAG chunks."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from chronos_enterprise_knowledge.chunking import ChunkingConfig, EnterpriseChunker
from chronos_enterprise_knowledge.embedding import (
    SentenceTransformerEmbedder,
    SentenceTransformerEmbeddingConfig,
)
from chronos_enterprise_knowledge.enterprise_rag import EnterpriseRAGCorpus
from chronos_enterprise_knowledge.snapshot import select_corpus_paths


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("corpus", type=Path)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--documents", type=int, default=1_000)
    parser.add_argument("--chunks", type=int, default=4_096)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--chunk-size", type=int, default=512)
    parser.add_argument("--backend", choices=("torch", "onnx"), default="torch")
    parser.add_argument("--model-file")
    args = parser.parse_args()

    corpus = EnterpriseRAGCorpus(args.corpus)
    selection = select_corpus_paths(
        corpus,
        fraction=0.01,
        seed="minilm-throughput-v1",
        max_documents=args.documents,
    )
    chunker = EnterpriseChunker(
        ChunkingConfig(
            target_tokens=240,
            overlap_tokens=24,
            encoding="hf:sentence-transformers/all-MiniLM-L6-v2",
        )
    )
    texts: list[str] = []
    for path in selection.paths:
        record = corpus.read_record(path)
        texts.extend(
            text
            for text, _ in chunker.chunk_text(
                record.document,
                record.index_text,
                context=record.context,
            )
        )
        if len(texts) >= args.chunks:
            break
    texts = texts[: args.chunks]

    embedder = SentenceTransformerEmbedder(
        config=SentenceTransformerEmbeddingConfig(
            workers=args.workers,
            batch_size=args.batch_size,
            chunk_size=args.chunk_size,
            device="cpu",
            backend=args.backend,
            model_file=args.model_file,
        )
    )
    try:
        # Exclude model loading and process startup from steady-state throughput.
        embedder.embed(texts[: min(256, len(texts))])
        started = time.perf_counter()
        vectors = embedder.embed(texts)
        elapsed = time.perf_counter() - started
    finally:
        embedder.close()

    print(
        json.dumps(
            {
                "workers": args.workers,
                "backend": args.backend,
                "model_file": args.model_file,
                "documents_sampled": len(selection.paths),
                "chunks": len(texts),
                "elapsed_seconds": elapsed,
                "chunks_per_second": len(texts) / elapsed,
                "vectors": len(vectors),
                "dimensions": len(vectors[0]) if vectors else 0,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
