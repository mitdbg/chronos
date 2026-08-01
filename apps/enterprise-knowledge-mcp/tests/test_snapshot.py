from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from chronos_enterprise_knowledge.backends import ChronosKnowledgeBackend
from chronos_enterprise_knowledge.embedding import (
    CachedEmbedder,
    EmbeddingCache,
    HashEmbedder,
    ZeroEmbedder,
)
from chronos_enterprise_knowledge.enterprise_rag import EnterpriseRAGCorpus
from chronos_enterprise_knowledge.ingestion import IngestionStats
from chronos_enterprise_knowledge.models import canonical_json
from chronos_enterprise_knowledge.snapshot import (
    EmbeddingSnapshot,
    SnapshotBuilder,
    SnapshotSpec,
    ingest_snapshot,
    select_corpus_paths,
)


class _CountingEmbedder(HashEmbedder):
    def __init__(self, dimensions: int):
        super().__init__(dimensions)
        self.inputs = 0

    def embed(self, texts):  # type: ignore[no-untyped-def]
        self.inputs += len(texts)
        return super().embed(texts)


class _NamedCountingEmbedder(_CountingEmbedder):
    @property
    def model(self) -> str:
        return "sentence-transformers/test-model#onnx:test.onnx"


def _make_corpus(root: Path) -> EnterpriseRAGCorpus:
    (root / "company_overview.md").parent.mkdir(parents=True, exist_ok=True)
    (root / "company_overview.md").write_text("# Redwood\n")
    identifiers = {}
    for connector in ("github", "slack"):
        directory = root / "sources" / connector / "redwood"
        directory.mkdir(parents=True)
        for index in range(10):
            path = directory / f"item-{index:02d}.json"
            path.write_text(
                json.dumps(
                    {
                        "title": f"{connector} item {index}",
                        "body": f"Evidence for scheduler project {index}.",
                    }
                )
            )
            identifiers[f"{connector}_{index}"] = (
                f"{connector}/redwood/{path.name}"
            )
    (root / "uuid_index.json").write_text(json.dumps(identifiers))
    return EnterpriseRAGCorpus(root)


def test_codebase_corpus_mapping_and_exclusions(tmp_path: Path) -> None:
    root = tmp_path / "corpus"
    corpus = _make_corpus(root)
    repository = root / "codebases" / "demo"
    (repository / "src").mkdir(parents=True)
    (repository / "src" / "service.py").write_text(
        "def serve() -> str:\n    return 'ready'\n"
    )
    (repository / "AGENTS.md").write_text("# Repository instructions\n")
    (repository / "CLAUDE.md").symlink_to("AGENTS.md")
    (repository / "package-lock.json").write_text("{}")
    (repository / ".git").mkdir()
    (repository / ".git" / "config").write_text("[core]\n")
    (repository / "node_modules").mkdir()
    (repository / "node_modules" / "ignored.js").write_text("ignored")

    selection = select_corpus_paths(
        corpus,
        fraction=1.0,
        seed="unused",
        connectors=["codebase"],
    )
    relative_paths = [
        path.relative_to(corpus.root).as_posix() for path in selection.paths
    ]

    assert relative_paths == [
        "codebases/demo/AGENTS.md",
        "codebases/demo/CLAUDE.md",
        "codebases/demo/src/service.py",
    ]
    assert selection.selected_by_connector == {"codebase": 3}
    record = corpus.read_record(repository / "src" / "service.py")
    assert record.document.path == "/code/demo/src/service.py"
    assert record.document.source == "EnterpriseRAG/codebase/demo"
    assert record.context == {
        "connector": "codebase",
        "workspace": "demo",
        "repository": "demo",
        "language": "py",
    }
    assert record.document.content.startswith("def serve")


def test_selection_is_stable_and_stratified(tmp_path: Path) -> None:
    corpus = _make_corpus(tmp_path / "corpus")
    first = select_corpus_paths(corpus, fraction=0.2, seed="seed")
    second = select_corpus_paths(corpus, fraction=0.2, seed="seed")

    assert first.digest == second.digest
    assert first.paths == second.paths
    assert first.selected_by_connector == {
        "company": 1,
        "github": 2,
        "slack": 2,
    }


def test_complete_selection_preserves_canonical_order_and_digest(
    tmp_path: Path,
) -> None:
    corpus = _make_corpus(tmp_path / "corpus")
    selection = select_corpus_paths(corpus, fraction=1.0, seed="unused")
    relative_paths = [
        path.relative_to(corpus.root).as_posix() for path in selection.paths
    ]

    assert relative_paths == sorted(relative_paths)
    assert selection.total_documents == 21
    assert selection.selected_by_connector == {
        "company": 1,
        "github": 10,
        "slack": 10,
    }
    assert selection.digest == hashlib.sha256(
        canonical_json(relative_paths).encode()
    ).hexdigest()


def test_parallel_chunking_preserves_snapshot_contents(tmp_path: Path) -> None:
    corpus = _make_corpus(tmp_path / "corpus")
    selection = select_corpus_paths(corpus, fraction=1.0, seed="seed")
    embedder = HashEmbedder(16)
    spec = SnapshotSpec(
        sample_fraction=1.0,
        sample_seed="seed",
        embedding_model=embedder.model,
        dimensions=16,
        target_tokens=128,
        overlap_tokens=16,
    )
    serial = EmbeddingSnapshot(tmp_path / "serial")
    parallel = EmbeddingSnapshot(tmp_path / "parallel")
    try:
        serial_stats = SnapshotBuilder(
            corpus,
            serial,
            embedder,
            spec,
            chunk_workers=1,
        ).build(selection, batch_size=4)
        parallel_stats = SnapshotBuilder(
            corpus,
            parallel,
            HashEmbedder(16),
            spec,
            chunk_workers=2,
        ).build(selection, batch_size=4)

        serial_items = [
            item.as_dict()
            for batch in serial.iter_indexed_documents(batch_size=5)
            for item in batch
        ]
        parallel_items = [
            item.as_dict()
            for batch in parallel.iter_indexed_documents(batch_size=5)
            for item in batch
        ]

        assert parallel_stats == serial_stats
        assert parallel_items == serial_items
    finally:
        serial.close()
        parallel.close()


def test_snapshot_resumes_without_reembedding_and_loads(
    tmp_path: Path,
) -> None:
    corpus = _make_corpus(tmp_path / "corpus")
    selection = select_corpus_paths(corpus, fraction=0.2, seed="seed")
    snapshot = EmbeddingSnapshot(tmp_path / "snapshot")
    provider = _CountingEmbedder(16)
    cache = EmbeddingCache(tmp_path / "snapshot" / "embedding-cache.sqlite")
    embedder = CachedEmbedder(provider, cache)
    spec = SnapshotSpec(
        sample_fraction=0.2,
        sample_seed="seed",
        embedding_model=embedder.model,
        dimensions=16,
        target_tokens=128,
        overlap_tokens=16,
    )
    try:
        first = SnapshotBuilder(
            corpus,
            snapshot,
            embedder,
            spec,
        ).build(selection, batch_size=2)
        embedded_inputs = provider.inputs
        resumed_progress: list[IngestionStats] = []
        second = SnapshotBuilder(
            corpus,
            snapshot,
            embedder,
            spec,
        ).build(
            selection,
            batch_size=2,
            progress=resumed_progress.append,
        )

        assert first == second
        assert provider.inputs == embedded_inputs
        assert resumed_progress == [first]
        assert json.loads(snapshot.manifest_path.read_text())["complete"] is True

        backend = ChronosKnowledgeBackend(
            tmp_path / "state",
            vector_dimensions=16,
        )
        try:
            loaded = ingest_snapshot(snapshot, backend, "main", batch_size=2)
            assert loaded == first
            indexed = next(snapshot.iter_indexed_documents(batch_size=1))[0]
            stored = backend.get_document("main", indexed.document.id)
            assert stored is not None
            assert stored.document == indexed.document
            assert stored.chunks[0].embedding == pytest.approx(
                indexed.chunks[0].embedding,
                abs=1e-4,
            )
        finally:
            backend.close()
    finally:
        cache.close()
        snapshot.close()


def test_snapshot_ingestion_can_force_zero_embeddings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = _make_corpus(tmp_path / "corpus")
    selection = select_corpus_paths(corpus, fraction=0.2, seed="seed")
    snapshot = EmbeddingSnapshot(tmp_path / "snapshot")
    embedder = HashEmbedder(16)
    spec = SnapshotSpec(
        sample_fraction=0.2,
        sample_seed="seed",
        embedding_model=embedder.model,
        dimensions=16,
        target_tokens=128,
        overlap_tokens=16,
    )
    backend = ChronosKnowledgeBackend(
        tmp_path / "state",
        vector_dimensions=16,
    )
    loaded_batches: list[int] = []
    put_documents = backend.put_documents

    def load_documents(
        branch_id, indexed_documents, *, operation_id  # type: ignore[no-untyped-def]
    ) -> None:
        loaded_batches.append(len(indexed_documents))
        put_documents(
            branch_id,
            indexed_documents,
            operation_id=operation_id,
        )

    monkeypatch.setattr(
        backend,
        "load_documents",
        load_documents,
        raising=False,
    )
    try:
        SnapshotBuilder(
            corpus,
            snapshot,
            embedder,
            spec,
        ).build(selection, batch_size=2)
        selected = snapshot.selected_document_ids(max_documents=2)
        assert selected is not None
        monkeypatch.setattr(
            snapshot,
            "embedding_progress",
            lambda: pytest.fail("forced-zero ingestion scanned all chunks"),
        )
        monkeypatch.setattr(
            snapshot,
            "stats",
            lambda: pytest.fail("bounded ingestion scanned all documents"),
        )
        loaded = ingest_snapshot(
            snapshot,
            backend,
            "main",
            batch_size=2,
            document_ids=selected,
            force_zero_embeddings=True,
        )
        assert loaded.documents == 2
        assert loaded_batches == [2]
        indexed = next(
            snapshot.iter_indexed_documents(
                batch_size=1,
                document_ids=selected,
            )
        )[0]
        assert any(indexed.chunks[0].embedding)

        stored = backend.get_document("main", indexed.document.id)
        assert stored is not None
        assert len(stored.chunks[0].embedding) == 16
        assert not any(stored.chunks[0].embedding)
    finally:
        backend.close()
        snapshot.close()


def test_snapshot_ingestion_resumes_after_last_committed_path(
    tmp_path: Path,
) -> None:
    corpus = _make_corpus(tmp_path / "corpus")
    selection = select_corpus_paths(corpus, fraction=1.0, seed="seed")
    snapshot = EmbeddingSnapshot(tmp_path / "snapshot")
    embedder = HashEmbedder(16)
    spec = SnapshotSpec(
        sample_fraction=1.0,
        sample_seed="seed",
        embedding_model=embedder.model,
        dimensions=16,
        target_tokens=128,
        overlap_tokens=16,
    )
    backend = ChronosKnowledgeBackend(
        tmp_path / "state",
        vector_dimensions=16,
    )
    try:
        SnapshotBuilder(
            corpus,
            snapshot,
            embedder,
            spec,
        ).build(selection, batch_size=4)

        first = ingest_snapshot(
            snapshot,
            backend,
            "main",
            batch_size=2,
            max_documents=5,
            resume=True,
        )
        second = ingest_snapshot(
            snapshot,
            backend,
            "main",
            batch_size=2,
            max_documents=8,
            resume=True,
        )
        repeated = ingest_snapshot(
            snapshot,
            backend,
            "main",
            batch_size=2,
            max_documents=8,
            resume=True,
        )

        assert first.documents == 5
        assert second.documents == 3
        assert repeated.documents == 0
        assert backend.storage_stats()["main_documents"] == 8
    finally:
        backend.close()
        snapshot.close()


def test_zero_snapshot_backfills_without_rechunking(tmp_path: Path) -> None:
    corpus = _make_corpus(tmp_path / "corpus")
    selection = select_corpus_paths(corpus, fraction=0.2, seed="seed")
    snapshot = EmbeddingSnapshot(tmp_path / "snapshot")
    model = "sentence-transformers/test-model#onnx:test.onnx"
    spec = SnapshotSpec(
        sample_fraction=0.2,
        sample_seed="seed",
        embedding_model=model,
        dimensions=16,
        target_tokens=128,
        overlap_tokens=16,
    )
    try:
        prepared = SnapshotBuilder(
            corpus,
            snapshot,
            ZeroEmbedder(16, model=model),
            spec,
        ).build(selection, batch_size=2)
        placeholder_state = snapshot.embedding_progress()
        before = next(snapshot.iter_indexed_documents(batch_size=1))[0]

        assert prepared.documents == selection.selected_documents
        assert placeholder_state["embedded_chunks"] == 0
        assert placeholder_state["remaining_chunks"] == prepared.chunks
        assert not any(before.chunks[0].embedding)

        provider = _NamedCountingEmbedder(16)
        state = snapshot.backfill_embeddings(provider, batch_size=3)
        after = next(snapshot.iter_indexed_documents(batch_size=1))[0]

        assert state["embeddings_complete"] is True
        assert state["embedded_chunks"] == prepared.chunks
        assert provider.inputs == prepared.chunks
        assert any(after.chunks[0].embedding)
        assert before.document == after.document
        assert [chunk.text for chunk in before.chunks] == [
            chunk.text for chunk in after.chunks
        ]
    finally:
        snapshot.close()


def test_zero_snapshot_reuses_compatible_partial_embeddings(
    tmp_path: Path,
) -> None:
    corpus = _make_corpus(tmp_path / "corpus")
    selection = select_corpus_paths(corpus, fraction=0.2, seed="seed")
    model = "sentence-transformers/test-model#onnx:test.onnx"
    spec = SnapshotSpec(
        sample_fraction=0.2,
        sample_seed="seed",
        embedding_model=model,
        dimensions=16,
        target_tokens=128,
        overlap_tokens=16,
    )
    source = EmbeddingSnapshot(tmp_path / "source")
    target = EmbeddingSnapshot(tmp_path / "target")
    try:
        SnapshotBuilder(
            corpus,
            source,
            _NamedCountingEmbedder(16),
            spec,
        ).build(selection, batch_size=2)
        SnapshotBuilder(
            corpus,
            target,
            ZeroEmbedder(16, model=model),
            spec,
        ).build(selection, batch_size=2)

        state = target.import_embeddings(source, batch_size=3)
        source_document = next(
            source.iter_indexed_documents(batch_size=1)
        )[0]
        target_document = next(
            target.iter_indexed_documents(batch_size=1)
        )[0]

        assert state["embeddings_complete"] is True
        assert source_document.document == target_document.document
        assert target_document.chunks[0].embedding == pytest.approx(
            source_document.chunks[0].embedding,
            abs=1e-4,
        )
    finally:
        source.close()
        target.close()


def test_complete_embeddings_revisit_placeholder_ingestion(
    tmp_path: Path,
) -> None:
    corpus = _make_corpus(tmp_path / "corpus")
    selection = select_corpus_paths(corpus, fraction=0.2, seed="seed")
    model = "sentence-transformers/test-model#onnx:test.onnx"
    spec = SnapshotSpec(
        sample_fraction=0.2,
        sample_seed="seed",
        embedding_model=model,
        dimensions=16,
        target_tokens=128,
        overlap_tokens=16,
    )
    snapshot = EmbeddingSnapshot(tmp_path / "snapshot")
    backend = ChronosKnowledgeBackend(
        tmp_path / "state",
        vector_dimensions=16,
    )
    try:
        prepared = SnapshotBuilder(
            corpus,
            snapshot,
            ZeroEmbedder(16, model=model),
            spec,
        ).build(selection, batch_size=2)
        placeholder = ingest_snapshot(
            snapshot,
            backend,
            "main",
            batch_size=2,
            resume=True,
        )

        snapshot.backfill_embeddings(
            _NamedCountingEmbedder(16),
            batch_size=3,
        )
        hydrated = ingest_snapshot(
            snapshot,
            backend,
            "main",
            batch_size=2,
            resume=True,
        )
        repeated = ingest_snapshot(
            snapshot,
            backend,
            "main",
            batch_size=2,
            resume=True,
        )

        assert placeholder.documents == prepared.documents
        assert hydrated.documents == prepared.documents
        assert repeated.documents == 0
        indexed = next(snapshot.iter_indexed_documents(batch_size=1))[0]
        stored = backend.get_document("main", indexed.document.id)
        assert stored is not None
        assert any(stored.chunks[0].embedding)
    finally:
        backend.close()
        snapshot.close()
