"""Command-line entry point for ingestion, hierarchy setup, and MCP serving."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Self

from chronos_enterprise_knowledge.backend import (
    KnowledgeBackend,
    OperationExecutor,
)
from chronos_enterprise_knowledge.backends import create_knowledge_backend
from chronos_enterprise_knowledge.embedding import (
    CacheOnlyEmbedder,
    CachedEmbedder,
    DEFAULT_SENTENCE_TRANSFORMER_DIMENSIONS,
    DEFAULT_SENTENCE_TRANSFORMER_MODEL,
    Embedder,
    EmbeddingCache,
    HashEmbedder,
    OpenRouterEmbedder,
    OpenRouterEmbeddingConfig,
    SentenceTransformerEmbedder,
    SentenceTransformerEmbeddingConfig,
    ZeroEmbedder,
    sentence_transformer_model_id,
)
from chronos_enterprise_knowledge.enterprise_rag import EnterpriseRAGCorpus
from chronos_enterprise_knowledge.evaluation import (
    evaluate_retrieval,
    read_questions,
)
from chronos_enterprise_knowledge.hierarchy import HierarchyBuilder, load_tasks
from chronos_enterprise_knowledge.ingestion import KnowledgeIngestor
from chronos_enterprise_knowledge.mcp_server import create_mcp_server
from chronos_enterprise_knowledge.rollout_benchmark import RolloutBenchmark
from chronos_enterprise_knowledge.rollout_trace import (
    RolloutAnalyzer,
    WorkloadReplayer,
    WorkloadTrace,
    combine_workload_traces,
    resolve_memory_timestamps,
)
from chronos_enterprise_knowledge.service import KnowledgeService
from chronos_enterprise_knowledge.snapshot import (
    EmbeddingSnapshot,
    SnapshotBuilder,
    SnapshotSpec,
    ingest_snapshot,
    select_corpus_paths,
)
from chronos_enterprise_knowledge.trace import TraceRecorder, TraceReplayer


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chronos-enterprise-knowledge",
        description=(
            "Branch-aware EnterpriseRAG knowledge and memory using SQLite, "
            "ChronosFS, and Qdrant"
        ),
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path(os.environ.get("CHRONOS_KNOWLEDGE_STATE", ".chronos-knowledge")),
    )
    parser.add_argument("--qdrant-url", default=os.environ.get("QDRANT_URL"))
    parser.add_argument(
        "--qdrant-api-key",
        default=os.environ.get("QDRANT_API_KEY"),
    )
    parser.add_argument(
        "--doltgres-dsn",
        default=os.environ.get("CHRONOS_DOLTGRES_DSN"),
        help=(
            "Doltgres DSN for the native-component baseline. Direct MCP use "
            "requires a dedicated database; rollout benchmarks use it as an "
            "administrative connection and create one database per run."
        ),
    )
    parser.add_argument(
        "--btrfs-root",
        type=Path,
        default=(
            Path(os.environ["CHRONOS_BTRFS_ROOT"])
            if os.environ.get("CHRONOS_BTRFS_ROOT")
            else None
        ),
        help="Btrfs mount used for comparison-backend subvolume snapshots.",
    )
    parser.add_argument(
        "--doltgres-data-dir",
        type=Path,
        default=(
            Path(os.environ["CHRONOS_DOLTGRES_DATA_DIR"])
            if os.environ.get("CHRONOS_DOLTGRES_DATA_DIR")
            else None
        ),
        help="Optional Doltgres storage directory used for byte accounting.",
    )
    parser.add_argument(
        "--qdrant-storage-dir",
        type=Path,
        default=(
            Path(os.environ["CHRONOS_QDRANT_STORAGE_DIR"])
            if os.environ.get("CHRONOS_QDRANT_STORAGE_DIR")
            else None
        ),
        help="Optional Qdrant storage directory used for byte accounting.",
    )
    parser.add_argument(
        "--embedding-model",
        default=DEFAULT_SENTENCE_TRANSFORMER_MODEL,
    )
    parser.add_argument(
        "--embedding-provider",
        choices=("sentence-transformers", "openrouter"),
        default="sentence-transformers",
        help="Generate embeddings locally by default, or opt into OpenRouter.",
    )
    parser.add_argument(
        "--dimensions",
        type=int,
        default=DEFAULT_SENTENCE_TRANSFORMER_DIMENSIONS,
    )
    parser.add_argument(
        "--embedding-batch-size",
        type=int,
        default=128,
        help="Texts encoded in each local or remote embedding batch.",
    )
    parser.add_argument(
        "--embedding-workers",
        type=int,
        default=2,
        help="Local encoder processes or concurrent remote batches.",
    )
    parser.add_argument(
        "--embedding-chunk-size",
        type=int,
        default=512,
        help="Texts sent to each local encoder process per work item.",
    )
    parser.add_argument(
        "--embedding-device",
        help="SentenceTransformer device, for example cpu, cuda, or mps.",
    )
    parser.add_argument(
        "--embedding-backend",
        choices=("torch", "onnx"),
        default="torch",
        help="SentenceTransformer inference runtime.",
    )
    parser.add_argument(
        "--embedding-model-file",
        help=(
            "Optional model-repository runtime file, such as an ONNX "
            "quantized graph."
        ),
    )
    parser.add_argument(
        "--offline-hash-embeddings",
        action="store_true",
        help="Use deterministic local embeddings for tests; not production RAG.",
    )
    parser.add_argument(
        "--placeholder-zero-embeddings",
        action="store_true",
        help=(
            "Use zero vectors for staged corpus work or forced-zero rollout "
            "validation; do not use this mode for semantic retrieval quality."
        ),
    )
    parser.add_argument(
        "--backend",
        choices=(
            "chronos",
            "app-managed",
            "physical-clone",
            "doltgres-qdrant-btrfs",
        ),
        default="chronos",
        help=(
            "Storage implementation. The MCP defaults to Chronos; comparison "
            "backends are intended for trace replay experiments."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser(
        "ingest",
        help="Ingest EnterpriseRAG documents into the company branch.",
    )
    ingest.add_argument("corpus", type=Path)
    ingest.add_argument("--branch", default="main")
    ingest.add_argument("--limit", type=int)
    ingest.add_argument(
        "--connector",
        action="append",
        dest="connectors",
        help="Restrict ingestion to one connector; may be repeated.",
    )
    ingest.add_argument("--progress-every", type=int, default=100)
    ingest.add_argument("--batch-size", type=int, default=32)

    prepare_snapshot = subparsers.add_parser(
        "prepare-snapshot",
        help="Chunk and embed a deterministic corpus sample for later reuse.",
    )
    prepare_snapshot.add_argument("corpus", type=Path)
    prepare_snapshot.add_argument("snapshot_dir", type=Path)
    prepare_snapshot.add_argument("--sample-fraction", type=float, default=0.10)
    prepare_snapshot.add_argument(
        "--sample-seed",
        default="chronos-enterprise-rag-v1",
    )
    prepare_snapshot.add_argument(
        "--connector",
        action="append",
        dest="connectors",
        help=(
            "Restrict preparation to one connector; may be repeated. "
            "Use codebase to prepare the checked-out source repositories."
        ),
    )

    backfill_snapshot = subparsers.add_parser(
        "backfill-snapshot-embeddings",
        help="Replace staged zero vectors without rechunking documents.",
    )
    backfill_snapshot.add_argument("snapshot_dir", type=Path)
    backfill_snapshot.add_argument("--batch-size", type=int, default=2_048)
    backfill_snapshot.add_argument("--progress-every", type=int, default=20_000)
    backfill_snapshot.add_argument(
        "--follow-preparation",
        action="store_true",
        help="Continue encoding chunks appended by a concurrent snapshot build.",
    )
    backfill_snapshot.add_argument("--poll-seconds", type=float, default=5.0)
    import_snapshot = subparsers.add_parser(
        "import-snapshot-embeddings",
        help="Reuse compatible vectors from another prepared snapshot.",
    )
    import_snapshot.add_argument("snapshot_dir", type=Path)
    import_snapshot.add_argument("source_snapshot_dir", type=Path)
    import_snapshot.add_argument("--batch-size", type=int, default=4_096)
    import_snapshot.add_argument("--progress-every", type=int, default=20_000)
    prepare_snapshot.add_argument(
        "--max-documents",
        type=int,
        help="Bound a smoke artifact after sampling; omit for the full sample.",
    )
    prepare_snapshot.add_argument(
        "--batch-size",
        type=int,
        default=256,
        help="Documents chunked and embedded together per snapshot transaction.",
    )
    prepare_snapshot.add_argument(
        "--chunk-workers",
        type=int,
        default=2,
        help="Parallel tokenizer workers; two is the measured CPU default.",
    )
    prepare_snapshot.add_argument("--progress-every", type=int, default=100)
    prepare_snapshot.add_argument("--target-tokens", type=int, default=240)
    prepare_snapshot.add_argument("--overlap-tokens", type=int, default=24)
    prepare_snapshot.add_argument(
        "--chunk-encoding",
        help=(
            "Tokenizer used to bound chunks. Defaults to the local embedding "
            "model tokenizer, or cl100k_base for OpenRouter."
        ),
    )
    prepare_snapshot.add_argument(
        "--cache-snapshot-embeddings",
        action="store_true",
        help=(
            "Also duplicate local vectors into an embedding cache. The snapshot "
            "is already resumable and reusable, so this is normally unnecessary."
        ),
    )

    ingest_prepared = subparsers.add_parser(
        "ingest-snapshot",
        help="Load prepared documents and vectors without rechunking or embedding.",
    )
    ingest_prepared.add_argument("snapshot_dir", type=Path)
    ingest_prepared.add_argument("--branch", default="main")
    ingest_prepared.add_argument("--batch-size", type=int, default=32)
    ingest_prepared.add_argument("--progress-every", type=int, default=100)
    ingest_prepared.add_argument(
        "--max-documents",
        type=int,
        help="Load a deterministic prefix for ingestion smoke tests.",
    )
    ingest_prepared.add_argument(
        "--required-document-id",
        action="append",
        default=[],
        help=(
            "Always load this snapshot document in a bounded ingestion; "
            "may be repeated."
        ),
    )
    ingest_prepared.add_argument(
        "--resume",
        action="store_true",
        help="Resume after the last successfully ingested snapshot path.",
    )
    ingest_prepared.add_argument(
        "--follow-preparation",
        action="store_true",
        help="Ingest new committed snapshot batches until preparation finishes.",
    )
    ingest_prepared.add_argument("--poll-seconds", type=float, default=5.0)

    hierarchy = subparsers.add_parser(
        "init-hierarchy",
        help="Create curated department, team, and personal branches.",
    )
    hierarchy.add_argument(
        "--without-people",
        action="store_true",
        help="Create company, department, and team branches only.",
    )
    hierarchy.add_argument(
        "--snapshot",
        type=Path,
        help="Ground department and team briefs in prepared corpus sources.",
    )

    serve = subparsers.add_parser(
        "serve",
        help="Run the Codex-facing MCP server.",
    )
    serve.add_argument(
        "--transport",
        choices=("stdio", "streamable-http"),
        default="stdio",
    )
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument(
        "--trace-dir",
        type=Path,
        help="Record logical task operations for cross-backend replay.",
    )
    serve.add_argument("--trace-id", default="enterprise-agent-task")

    subparsers.add_parser(
        "tasks",
        help="Print the curated role-specific experiment tasks.",
    )
    subparsers.add_parser(
        "storage-stats",
        help="Print backend component and total storage accounting.",
    )
    subparsers.add_parser(
        "destroy-state",
        help="Delete backend-managed external state after a benchmark run.",
    )
    evaluate = subparsers.add_parser(
        "evaluate-retrieval",
        help="Measure EnterpriseRAG document recall through knowledge_search.",
    )
    evaluate.add_argument("questions", type=Path)
    evaluate.add_argument("--branch", default="main")
    evaluate.add_argument("--top-k", type=int, default=20)
    evaluate.add_argument("--limit", type=int)
    evaluate.add_argument("--include-results", action="store_true")
    replay = subparsers.add_parser(
        "replay-trace",
        help="Replay one backend-neutral task trace against the selected backend.",
    )
    replay.add_argument("trace_dir", type=Path)
    replay.add_argument(
        "--skip-reads",
        action="store_true",
        help="Replay mutations only; useful when search implementations differ.",
    )
    replay.add_argument(
        "--no-verify-results",
        action="store_true",
        help="Execute reads without requiring byte-identical result payloads.",
    )
    replay.add_argument(
        "--verify-branch",
        action="append",
        default=[],
        help="Report the final logical state digest for a branch; may be repeated.",
    )

    analyze_rollout = subparsers.add_parser(
        "analyze-rollout",
        help="Extract replayable MCP and shell calls from a Codex rollout JSONL.",
    )
    analyze_rollout.add_argument("rollout", type=Path)
    analyze_rollout.add_argument("--output", type=Path)
    analyze_rollout.add_argument("--trace-id")

    combine_rollouts = subparsers.add_parser(
        "combine-rollout-traces",
        help="Combine dependent real-session traces in recorded timestamp order.",
    )
    combine_rollouts.add_argument("traces", type=Path, nargs="+")
    combine_rollouts.add_argument("--output", type=Path, required=True)
    combine_rollouts.add_argument(
        "--trace-id",
        default="enterprise-knowledge-real-workflow",
    )

    resolve_memories = subparsers.add_parser(
        "resolve-rollout-memory-times",
        help="Recover generated memory timestamps from the captured backend state.",
    )
    resolve_memories.add_argument("trace", type=Path)
    resolve_memories.add_argument("--output", type=Path, required=True)

    replay_rollout = subparsers.add_parser(
        "replay-rollout",
        help="Replay a normalized Codex workload against the selected backend.",
    )
    replay_rollout.add_argument("trace", type=Path)
    replay_rollout.add_argument("--repo-dir", type=Path, default=Path.cwd())
    replay_rollout.add_argument(
        "--embedding-cache",
        type=Path,
        help="Use cached embeddings only; provider calls are disabled.",
    )
    replay_rollout.add_argument(
        "--allow-shell",
        action="store_true",
        help="Execute shell events from this trusted trace.",
    )
    replay_rollout.add_argument(
        "--continue-on-error",
        action="store_true",
    )
    replay_rollout.add_argument(
        "--require-result-match",
        action="store_true",
        help="Fail when replayed results differ from recorded result digests.",
    )
    replay_rollout.add_argument(
        "--shell-timeout",
        type=float,
        default=300,
    )

    benchmark_rollouts = subparsers.add_parser(
        "benchmark-rollouts",
        help="Seed identical states and replay workloads across storage backends.",
    )
    benchmark_rollouts.add_argument("traces", type=Path, nargs="+")
    benchmark_rollouts.add_argument("--snapshot", type=Path, required=True)
    benchmark_rollouts.add_argument(
        "--embedding-cache",
        type=Path,
        required=True,
        help="Prepared query cache; benchmark replay never calls a provider.",
    )
    benchmark_rollouts.add_argument("--output-dir", type=Path, required=True)
    benchmark_rollouts.add_argument(
        "--benchmark-backend",
        action="append",
        choices=(
            "chronos",
            "app-managed",
            "physical-clone",
            "doltgres-qdrant-btrfs",
        ),
        dest="benchmark_backends",
    )
    benchmark_rollouts.add_argument("--repo-dir", type=Path, default=Path.cwd())
    document_selection = benchmark_rollouts.add_mutually_exclusive_group()
    document_selection.add_argument(
        "--max-documents",
        type=int,
        default=None,
        help=(
            "Use an explicitly bounded deterministic snapshot subset plus "
            "workflow-required documents. By default, seed the complete "
            "prepared snapshot."
        ),
    )
    document_selection.add_argument(
        "--all-documents",
        action="store_true",
        help=(
            "Seed every document in the prepared snapshot. This is already "
            "the default and is retained as an explicit experiment marker."
        ),
    )
    benchmark_rollouts.add_argument("--repetitions", type=int, default=1)
    benchmark_rollouts.add_argument("--allow-shell", action="store_true")
    benchmark_rollouts.add_argument(
        "--require-result-match",
        action="store_true",
        help=(
            "Fail when native read results differ; state and execution "
            "equivalence are always checked."
        ),
    )
    benchmark_rollouts.add_argument(
        "--discard-states",
        action="store_true",
        help="Remove per-run backend state after collecting metrics.",
    )
    return parser


def _create_embedder(
    args: argparse.Namespace,
    cache: EmbeddingCache,
) -> tuple[Embedder, Embedder]:
    local_config = SentenceTransformerEmbeddingConfig(
        model=args.embedding_model,
        dimensions=args.dimensions,
        batch_size=args.embedding_batch_size,
        workers=args.embedding_workers,
        chunk_size=args.embedding_chunk_size,
        device=args.embedding_device,
        backend=args.embedding_backend,
        model_file=args.embedding_model_file,
    )
    if args.placeholder_zero_embeddings:
        provider = ZeroEmbedder(
            args.dimensions,
            model=sentence_transformer_model_id(local_config),
        )
        return provider, provider
    if args.offline_hash_embeddings:
        provider: Embedder = HashEmbedder(args.dimensions)
        return provider, provider
    if args.embedding_provider == "sentence-transformers":
        provider = SentenceTransformerEmbedder(
            config=local_config
        )
    else:
        provider = OpenRouterEmbedder(
            config=OpenRouterEmbeddingConfig(
                model=args.embedding_model,
                dimensions=args.dimensions,
                batch_size=args.embedding_batch_size,
                max_parallel_batches=args.embedding_workers,
            )
        )
    return provider, CachedEmbedder(provider, cache)


def _create_backend(
    args: argparse.Namespace,
    state_dir: str | Path,
) -> KnowledgeBackend:
    return create_knowledge_backend(
        args.backend,
        state_dir,
        vector_dimensions=args.dimensions,
        qdrant_url=args.qdrant_url,
        qdrant_api_key=args.qdrant_api_key,
        doltgres_dsn=args.doltgres_dsn,
        btrfs_root=args.btrfs_root,
        doltgres_data_dir=args.doltgres_data_dir,
        qdrant_storage_dir=args.qdrant_storage_dir,
    )


class _Application:
    def __init__(self, args: argparse.Namespace, *, recorder: bool = False):
        self.args = args
        args.state_dir = args.state_dir.expanduser().resolve()
        args.state_dir.mkdir(parents=True, exist_ok=True)
        self.backend = _create_backend(args, args.state_dir)
        self.cache = EmbeddingCache(args.state_dir / "embedding-cache.sqlite")
        self.provider, self.embedder = _create_embedder(args, self.cache)
        trace_recorder = None
        if recorder and args.trace_dir:
            trace_recorder = TraceRecorder(
                OperationExecutor(self.backend),
                args.trace_dir,
                trace_id=args.trace_id,
            )
        self.service = KnowledgeService(
            self.backend,
            self.embedder,
            recorder=trace_recorder,
        )

    def close(self) -> None:
        close = getattr(self.provider, "close", None)
        try:
            self.backend.close()
        finally:
            try:
                self.cache.close()
            finally:
                if callable(close):
                    close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _ingest(args: argparse.Namespace) -> int:
    with _Application(args) as application:
        corpus = EnterpriseRAGCorpus(args.corpus)
        ingestor = KnowledgeIngestor(application.backend, application.embedder)
        records = corpus.iter_records(
            connectors=args.connectors,
            limit=args.limit,
        )
        last_progress = 0

        def report(stats: Any) -> None:
            nonlocal last_progress
            if (
                args.progress_every > 0
                and stats.documents - last_progress >= args.progress_every
            ):
                print(
                    json.dumps(stats.as_dict()),
                    file=sys.stderr,
                    flush=True,
                )
                last_progress = stats.documents

        stats = ingestor.ingest_records(
            args.branch,
            records,
            batch_size=args.batch_size,
            progress=report,
        )
        print(
            json.dumps(
                {
                    "branch": args.branch,
                    **stats.as_dict(),
                },
                indent=2,
            )
        )
    return 0


def _init_hierarchy(args: argparse.Namespace) -> int:
    with _Application(args) as application:
        snapshot = EmbeddingSnapshot(args.snapshot) if args.snapshot else None
        try:
            builder = HierarchyBuilder(
                application.backend,
                KnowledgeIngestor(application.backend, application.embedder),
                source_snapshot=snapshot,
            )
            result = builder.build(include_people=not args.without_people)
        finally:
            if snapshot is not None:
                snapshot.close()
        print(json.dumps(result, indent=2))
    return 0


def _prepare_snapshot(args: argparse.Namespace) -> int:
    corpus = EnterpriseRAGCorpus(args.corpus)
    snapshot_dir = args.snapshot_dir.expanduser().resolve()
    snapshot = EmbeddingSnapshot(snapshot_dir)
    cache = EmbeddingCache(snapshot_dir / "embedding-cache.sqlite")
    provider, cached_embedder = _create_embedder(args, cache)
    embedder = (
        cached_embedder
        if args.cache_snapshot_embeddings
        or args.embedding_provider == "openrouter"
        or args.offline_hash_embeddings
        else provider
    )
    spec = SnapshotSpec(
        sample_fraction=args.sample_fraction,
        sample_seed=args.sample_seed,
        embedding_model=embedder.model,
        dimensions=embedder.dimensions,
        target_tokens=args.target_tokens,
        overlap_tokens=args.overlap_tokens,
        encoding=(
            args.chunk_encoding
            or (
                f"hf:{args.embedding_model}"
                if args.embedding_provider == "sentence-transformers"
                and not args.offline_hash_embeddings
                else "cl100k_base"
            )
        ),
    )
    selection = select_corpus_paths(
        corpus,
        fraction=spec.sample_fraction,
        seed=spec.sample_seed,
        max_documents=args.max_documents,
        connectors=args.connectors,
    )
    last_progress = 0

    def report(stats: Any) -> None:
        nonlocal last_progress
        if (
            args.progress_every > 0
            and stats.documents - last_progress >= args.progress_every
        ):
            print(json.dumps(stats.as_dict()), file=sys.stderr, flush=True)
            last_progress = stats.documents

    try:
        stats = SnapshotBuilder(
            corpus,
            snapshot,
            embedder,
            spec,
            chunk_workers=args.chunk_workers,
        ).build(
            selection,
            batch_size=args.batch_size,
            progress=report,
        )
        print(
            json.dumps(
                {
                    "snapshot": str(snapshot_dir),
                    "selection_digest": selection.digest,
                    "corpus_documents": selection.total_documents,
                    "selected_by_connector": selection.selected_by_connector,
                    **stats.as_dict(),
                },
                indent=2,
            )
        )
    finally:
        close = getattr(provider, "close", None)
        try:
            cache.close()
        finally:
            try:
                snapshot.close()
            finally:
                if callable(close):
                    close()
    return 0


def _ingest_prepared(args: argparse.Namespace) -> int:
    args.state_dir = args.state_dir.expanduser().resolve()
    args.state_dir.mkdir(parents=True, exist_ok=True)
    snapshot = EmbeddingSnapshot(args.snapshot_dir)
    if snapshot.dimensions != args.dimensions:
        snapshot.close()
        raise ValueError(
            f"snapshot uses {snapshot.dimensions} dimensions, "
            f"but --dimensions is {args.dimensions}"
        )
    backend = _create_backend(args, args.state_dir)
    last_progress = 0
    started = time.monotonic()
    last_reported_at = started
    last_reported_documents = 0
    last_reported_chunks = 0

    def report(stats: Any) -> None:
        nonlocal last_progress, last_reported_at
        nonlocal last_reported_documents, last_reported_chunks
        if (
            args.progress_every > 0
            and stats.documents - last_progress >= args.progress_every
        ):
            now = time.monotonic()
            elapsed = max(now - started, 1e-9)
            window = max(now - last_reported_at, 1e-9)
            print(
                json.dumps(
                    {
                        **stats.as_dict(),
                        "elapsed_seconds": round(elapsed, 3),
                        "documents_per_second": round(
                            stats.documents / elapsed,
                            3,
                        ),
                        "chunks_per_second": round(
                            stats.chunks / elapsed,
                            3,
                        ),
                        "mib_per_second": round(
                            stats.bytes / elapsed / (1024 * 1024),
                            3,
                        ),
                        "window_documents_per_second": round(
                            (
                                stats.documents
                                - last_reported_documents
                            )
                            / window,
                            3,
                        ),
                        "window_chunks_per_second": round(
                            (stats.chunks - last_reported_chunks) / window,
                            3,
                        ),
                    }
                ),
                file=sys.stderr,
                flush=True,
            )
            last_progress = stats.documents
            last_reported_at = now
            last_reported_documents = stats.documents
            last_reported_chunks = stats.chunks

    try:
        stats = ingest_snapshot(
            snapshot,
            backend,
            args.branch,
            batch_size=args.batch_size,
            document_ids=args.required_document_id or None,
            max_documents=args.max_documents,
            resume=args.resume,
            follow_preparation=args.follow_preparation,
            poll_seconds=args.poll_seconds,
            force_zero_embeddings=args.placeholder_zero_embeddings,
            progress=report,
        )
        elapsed = max(time.monotonic() - started, 1e-9)
        print(
            json.dumps(
                {
                    "branch": args.branch,
                    **stats.as_dict(),
                    "elapsed_seconds": round(elapsed, 3),
                    "documents_per_second": round(
                        stats.documents / elapsed,
                        3,
                    ),
                    "chunks_per_second": round(
                        stats.chunks / elapsed,
                        3,
                    ),
                    "mib_per_second": round(
                        stats.bytes / elapsed / (1024 * 1024),
                        3,
                    ),
                },
                indent=2,
            )
        )
    finally:
        try:
            backend.close()
        finally:
            snapshot.close()
    return 0


def _backfill_snapshot_embeddings(args: argparse.Namespace) -> int:
    if args.placeholder_zero_embeddings or args.offline_hash_embeddings:
        raise ValueError(
            "backfill requires a real embedding provider, not placeholder or "
            "hash embeddings"
        )
    snapshot = EmbeddingSnapshot(args.snapshot_dir)
    cache = EmbeddingCache(args.snapshot_dir / "query-embedding-cache.sqlite")
    provider, _ = _create_embedder(args, cache)
    last_reported = -1

    def report(state: dict[str, int | bool]) -> None:
        nonlocal last_reported
        embedded = int(state["embedded_chunks"])
        if (
            last_reported < 0
            or embedded - last_reported >= args.progress_every
            or bool(state["embeddings_complete"])
        ):
            print(json.dumps(state), file=sys.stderr, flush=True)
            last_reported = embedded

    try:
        state = snapshot.backfill_embeddings(
            provider,
            batch_size=args.batch_size,
            follow_preparation=args.follow_preparation,
            poll_seconds=args.poll_seconds,
            progress=report,
        )
        print(
            json.dumps(
                {
                    "snapshot": str(args.snapshot_dir.expanduser().resolve()),
                    **state,
                },
                indent=2,
            )
        )
    finally:
        close = getattr(provider, "close", None)
        try:
            cache.close()
        finally:
            try:
                snapshot.close()
            finally:
                if callable(close):
                    close()
    return 0


def _import_snapshot_embeddings(args: argparse.Namespace) -> int:
    snapshot = EmbeddingSnapshot(args.snapshot_dir)
    source = EmbeddingSnapshot(args.source_snapshot_dir)
    last_reported = -1

    def report(state: dict[str, int | bool]) -> None:
        nonlocal last_reported
        embedded = int(state["embedded_chunks"])
        if (
            last_reported < 0
            or embedded - last_reported >= args.progress_every
            or bool(state["embeddings_complete"])
        ):
            print(json.dumps(state), file=sys.stderr, flush=True)
            last_reported = embedded

    try:
        state = snapshot.import_embeddings(
            source,
            batch_size=args.batch_size,
            progress=report,
        )
        print(
            json.dumps(
                {
                    "snapshot": str(args.snapshot_dir.expanduser().resolve()),
                    "source_snapshot": str(
                        args.source_snapshot_dir.expanduser().resolve()
                    ),
                    **state,
                },
                indent=2,
            )
        )
    finally:
        try:
            source.close()
        finally:
            snapshot.close()
    return 0


def _serve(args: argparse.Namespace) -> int:
    with _Application(args, recorder=True) as application:
        server = create_mcp_server(
            application.service,
            host=args.host,
            port=args.port,
        )
        server.run(transport=args.transport)
    return 0


def _tasks() -> int:
    print(
        json.dumps(
            [
                {
                    "id": task.id,
                    "branch": task.branch,
                    "role": task.role,
                    "objective": task.objective,
                    "evidence_scopes": list(task.evidence_scopes),
                    "updates": list(task.updates),
                }
                for task in load_tasks()
            ],
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


def _evaluate_retrieval(args: argparse.Namespace) -> int:
    with _Application(args) as application:
        report = evaluate_retrieval(
            application.service,
            args.branch,
            read_questions(args.questions, limit=args.limit),
            top_k=args.top_k,
        )
        result = report.as_dict(include_results=args.include_results)
        if args.placeholder_zero_embeddings:
            retrieval_mode = "lexical-placeholder"
        elif args.offline_hash_embeddings:
            retrieval_mode = "offline-hash-vector"
        else:
            retrieval_mode = "semantic-vector"
        result.update(
            {
                "retrieval_mode": retrieval_mode,
                "embedding_model": application.embedder.model,
                "embedding_dimensions": application.embedder.dimensions,
            }
        )
        print(
            json.dumps(
                result,
                indent=2,
            )
        )
    return 0


def _replay_trace(args: argparse.Namespace) -> int:
    args.state_dir = args.state_dir.expanduser().resolve()
    args.state_dir.mkdir(parents=True, exist_ok=True)
    backend = _create_backend(args, args.state_dir)
    try:
        report = TraceReplayer(
            OperationExecutor(backend),
            args.trace_dir,
        ).replay(
            include_reads=not args.skip_reads,
            verify_results=not args.no_verify_results,
        )
        value = report.as_dict()
        storage_stats = getattr(backend, "storage_stats", None)
        if callable(storage_stats):
            value["storage"] = storage_stats()
        value["state_digests"] = {
            branch_id: backend.state_digest(branch_id)
            for branch_id in args.verify_branch
        }
        print(json.dumps(value, indent=2))
        return 0 if report.matched else 2
    finally:
        backend.close()


def _storage_stats(args: argparse.Namespace) -> int:
    args.state_dir = args.state_dir.expanduser().resolve()
    args.state_dir.mkdir(parents=True, exist_ok=True)
    backend = _create_backend(args, args.state_dir)
    try:
        storage_stats = getattr(backend, "storage_stats", None)
        if not callable(storage_stats):
            raise RuntimeError(
                f"backend {backend.backend_name!r} does not report storage"
            )
        print(
            json.dumps(
                {
                    "backend": backend.backend_name,
                    "storage": storage_stats(),
                },
                indent=2,
            )
        )
        return 0
    finally:
        backend.close()


def _destroy_state(args: argparse.Namespace) -> int:
    args.state_dir = args.state_dir.expanduser().resolve()
    backend = _create_backend(args, args.state_dir)
    try:
        destroy = getattr(backend, "destroy", None)
        if not callable(destroy):
            raise RuntimeError(
                f"backend {backend.backend_name!r} cannot destroy state"
            )
        destroy()
        print(json.dumps({"backend": backend.backend_name, "destroyed": True}))
        return 0
    finally:
        backend.close()


def _analyze_rollout(args: argparse.Namespace) -> int:
    analysis = RolloutAnalyzer().analyze(
        args.rollout,
        trace_id=args.trace_id,
    )
    value = analysis.as_dict()
    if args.output is not None:
        destination = analysis.trace.write(args.output)
        value["output"] = str(destination.resolve())
    print(json.dumps(value, indent=2))
    return 0


def _replay_rollout(args: argparse.Namespace) -> int:
    trace = WorkloadTrace.load(args.trace)
    args.state_dir = args.state_dir.expanduser().resolve()
    args.state_dir.mkdir(parents=True, exist_ok=True)
    if args.embedding_cache is None:
        with _Application(args) as application:
            report = WorkloadReplayer(
                application.service,
                repo_dir=args.repo_dir,
                allow_shell=args.allow_shell,
                shell_timeout_seconds=args.shell_timeout,
                continue_on_error=args.continue_on_error,
            ).replay(trace)
            value = report.as_dict()
            storage_stats = getattr(application.backend, "storage_stats", None)
            if callable(storage_stats):
                value["storage"] = storage_stats()
    else:
        backend = _create_backend(args, args.state_dir)
        cache = EmbeddingCache(args.embedding_cache)
        try:
            service = KnowledgeService(
                backend,
                CacheOnlyEmbedder(
                    cache,
                    model=args.embedding_model,
                    dimensions=args.dimensions,
                ),
            )
            report = WorkloadReplayer(
                service,
                repo_dir=args.repo_dir,
                allow_shell=args.allow_shell,
                shell_timeout_seconds=args.shell_timeout,
                continue_on_error=args.continue_on_error,
            ).replay(trace)
            value = report.as_dict()
            storage_stats = getattr(backend, "storage_stats", None)
            if callable(storage_stats):
                value["storage"] = storage_stats()
        finally:
            cache.close()
            backend.close()
    print(json.dumps(value, indent=2))
    matched = report.matched or not args.require_result_match
    return 0 if report.succeeded and matched else 2


def _combine_rollout_traces(args: argparse.Namespace) -> int:
    traces = [WorkloadTrace.load(path) for path in args.traces]
    combined = combine_workload_traces(traces, trace_id=args.trace_id)
    destination = combined.write(args.output)
    print(
        json.dumps(
            {
                **combined.summary(),
                "component_traces": combined.metadata["component_traces"],
                "required_documents": len(
                    combined.metadata["seed"]["document_ids"]
                ),
                "verify_branches": combined.metadata["verify_branches"],
                "output": str(destination.resolve()),
            },
            indent=2,
        )
    )
    return 0


def _resolve_rollout_memory_times(args: argparse.Namespace) -> int:
    trace = WorkloadTrace.load(args.trace)
    backend = _create_backend(
        args,
        args.state_dir.expanduser().resolve(),
    )
    try:
        resolved_trace, resolved = resolve_memory_timestamps(trace, backend)
        destination = resolved_trace.write(args.output)
    finally:
        backend.close()
    print(
        json.dumps(
            {
                "trace_id": trace.trace_id,
                "memory_timestamps_resolved": resolved,
                "output": str(destination.resolve()),
            },
            indent=2,
        )
    )
    return 0


def _benchmark_rollouts(args: argparse.Namespace) -> int:
    traces = [WorkloadTrace.load(path) for path in args.traces]
    backends = tuple(
        args.benchmark_backends
        or ("chronos", "app-managed", "physical-clone")
    )
    benchmark = RolloutBenchmark(
        snapshot_dir=args.snapshot,
        embedding_cache=args.embedding_cache,
        output_dir=args.output_dir,
        repo_dir=args.repo_dir,
        backends=backends,
        max_documents=None if args.all_documents else args.max_documents,
        repetitions=args.repetitions,
        allow_shell=args.allow_shell,
        keep_states=not args.discard_states,
        require_result_match=args.require_result_match,
        qdrant_url=args.qdrant_url,
        qdrant_api_key=args.qdrant_api_key,
        doltgres_dsn=args.doltgres_dsn,
        btrfs_root=args.btrfs_root,
        doltgres_data_dir=args.doltgres_data_dir,
        qdrant_storage_dir=args.qdrant_storage_dir,
        force_zero_embeddings=args.placeholder_zero_embeddings,
    )
    report = benchmark.run(traces)
    destination = report.write(args.output_dir / "results.json")
    value = report.as_dict()
    value["results_path"] = str(destination)
    print(json.dumps(value, indent=2))
    return 0 if report.matched else 2


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "ingest":
        return _ingest(args)
    if args.command == "prepare-snapshot":
        return _prepare_snapshot(args)
    if args.command == "backfill-snapshot-embeddings":
        return _backfill_snapshot_embeddings(args)
    if args.command == "import-snapshot-embeddings":
        return _import_snapshot_embeddings(args)
    if args.command == "ingest-snapshot":
        return _ingest_prepared(args)
    if args.command == "init-hierarchy":
        return _init_hierarchy(args)
    if args.command == "serve":
        return _serve(args)
    if args.command == "tasks":
        return _tasks()
    if args.command == "storage-stats":
        return _storage_stats(args)
    if args.command == "destroy-state":
        return _destroy_state(args)
    if args.command == "evaluate-retrieval":
        return _evaluate_retrieval(args)
    if args.command == "replay-trace":
        return _replay_trace(args)
    if args.command == "analyze-rollout":
        return _analyze_rollout(args)
    if args.command == "combine-rollout-traces":
        return _combine_rollout_traces(args)
    if args.command == "resolve-rollout-memory-times":
        return _resolve_rollout_memory_times(args)
    if args.command == "replay-rollout":
        return _replay_rollout(args)
    if args.command == "benchmark-rollouts":
        return _benchmark_rollouts(args)
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
