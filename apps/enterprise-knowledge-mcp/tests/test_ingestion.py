from __future__ import annotations

import json
from pathlib import Path

import pytest
from chronos_enterprise_knowledge.backends import ChronosKnowledgeBackend
from chronos_enterprise_knowledge.chunking import ChunkingConfig, EnterpriseChunker
from chronos_enterprise_knowledge.embedding import (
    CachedEmbedder,
    EmbeddingCache,
    HashEmbedder,
)
from chronos_enterprise_knowledge.enterprise_rag import EnterpriseRAGCorpus
from chronos_enterprise_knowledge.ingestion import KnowledgeIngestor
from chronos_enterprise_knowledge.models import KnowledgeDocument


class _CountingEmbedder(HashEmbedder):
    def __init__(self, dimensions: int):
        super().__init__(dimensions)
        self.inputs = 0

    def embed(self, texts):  # type: ignore[no-untyped-def]
        self.inputs += len(texts)
        return super().embed(texts)


def _corpus(root: Path) -> EnterpriseRAGCorpus:
    source = root / "sources" / "jira" / "eng-runtime"
    source.mkdir(parents=True)
    relative = "jira/eng-runtime/ENG-42.json"
    (source / "ENG-42.json").write_text(
        json.dumps(
            {
                "key": "ENG-42",
                "summary": "Reduce scheduler tail latency",
                "status": "In Progress",
                "priority": "P0",
                "description": (
                    "The runtime scheduler delays short requests behind long "
                    "prefill work. Add a deadline-aware batching policy."
                ),
            }
        )
    )
    (root / "uuid_index.json").write_text(json.dumps({"dsid_scheduler": relative}))
    return EnterpriseRAGCorpus(root)


def test_enterprise_record_preserves_original_and_extracts_context(
    tmp_path: Path,
) -> None:
    corpus = _corpus(tmp_path)
    record = next(corpus.iter_records(include_root_documents=False))

    assert record.document.id == "dsid_scheduler"
    assert record.document.title == "Reduce scheduler tail latency"
    assert record.document.path.endswith("/sources/jira/eng-runtime/ENG-42.json")
    assert record.context["connector"] == "jira"
    assert record.context["workspace"] == "eng-runtime"
    assert record.context["priority"] == "P0"
    assert "deadline-aware batching policy" in record.index_text
    assert json.loads(record.document.content)["key"] == "ENG-42"


def test_public_github_record_exposes_cutoff_metadata(tmp_path: Path) -> None:
    source = tmp_path / "sources" / "github_public" / "vllm-project" / "vllm" / "issues"
    source.mkdir(parents=True)
    path = source / "50026.json"
    path.write_text(
        json.dumps(
            {
                "title": "Batch endpoint contract",
                "body": "Reject unsupported streaming.",
                "repository": "vllm-project/vllm",
                "artifact_type": "issue",
                "number": 50026,
                "state_at_cutoff": "open",
                "dataset_doc_uuid": "gh_fixture",
                "title_field_name": "title",
                "content_field_names": ["body"],
            }
        )
    )
    (tmp_path / "uuid_index.json").write_text(
        json.dumps({"gh_fixture": "github_public/vllm-project/vllm/issues/50026.json"})
    )

    record = EnterpriseRAGCorpus(tmp_path).read_record(path)

    assert record.context == {
        "connector": "github_public",
        "workspace": "vllm-project/vllm",
        "repository": "vllm-project/vllm",
        "artifact_type": "issue",
        "number": 50026,
        "state_at_cutoff": "open",
    }
    assert record.document.id == "gh_fixture"


def test_connector_declared_title_field_is_used(tmp_path: Path) -> None:
    source = tmp_path / "sources" / "slack" / "eng-runtime"
    source.mkdir(parents=True)
    path = source / "infra-v1-slack-000.json"
    path.write_text(
        json.dumps(
            {
                "channel": "eng-runtime",
                "title_field_name": "channel",
                "content_field_names": ["messages"],
                "messages": ["Track the upstream qualification decision."],
                "dataset_doc_uuid": "infra_fixture",
            }
        )
    )
    record = EnterpriseRAGCorpus(tmp_path).read_record(path)
    assert record.document.title == "eng-runtime"


def test_cached_embedder_deduplicates_exact_inputs(tmp_path: Path) -> None:
    provider = _CountingEmbedder(16)
    cache = EmbeddingCache(tmp_path / "embeddings.sqlite")
    embedder = CachedEmbedder(provider, cache)
    try:
        first = embedder.embed(["alpha beta", "alpha beta", "gamma"])
        second = embedder.embed(["gamma", "alpha beta"])
    finally:
        cache.close()

    assert provider.inputs == 2
    assert first[0] == first[1] == second[1]
    assert first[2] == second[0]


def test_chunker_treats_tokenizer_sentinels_as_source_text() -> None:
    document = KnowledgeDocument(
        id="prompt-template",
        path="/knowledge/company/prompt.txt",
        title="Prompt template",
        source="test",
        content="Prefix <|endoftext|> suffix",
    )
    chunks = EnterpriseChunker(
        ChunkingConfig(target_tokens=128, overlap_tokens=16)
    ).chunk_text(document, document.content)

    assert len(chunks) == 1
    assert "<|endoftext|>" in chunks[0][0]


def test_ingestion_keeps_files_metadata_and_vectors_in_lockstep(
    tmp_path: Path,
) -> None:
    corpus_root = tmp_path / "corpus"
    corpus = _corpus(corpus_root)
    record = next(corpus.iter_records(include_root_documents=False))
    backend = ChronosKnowledgeBackend(tmp_path / "state", vector_dimensions=32)
    ingestor = KnowledgeIngestor(
        backend,
        HashEmbedder(32),
        chunker=EnterpriseChunker(ChunkingConfig(target_tokens=128, overlap_tokens=16)),
    )
    try:
        indexed = ingestor.index_record(
            "main",
            record,
            operation_id="ingest:scheduler",
        )
        loaded = backend.get_document("main", record.document.id)
        assert loaded is not None
        assert loaded.document == indexed.document
        assert len(loaded.chunks) == len(indexed.chunks)
        assert loaded.chunks[0].text == indexed.chunks[0].text
        assert loaded.chunks[0].metadata == indexed.chunks[0].metadata
        assert loaded.chunks[0].embedding == pytest.approx(
            indexed.chunks[0].embedding,
            abs=1e-6,
        )
        assert backend.read_file("main", record.document.path) == (
            record.document.content.encode()
        )

        hits = backend.search(
            "main",
            "deadline aware scheduler",
            HashEmbedder(32).embed(["deadline aware scheduler"])[0],
            limit=5,
        )
        assert hits
        assert hits[0].document_id == record.document.id
        assert "Connector: jira" in hits[0].text
        assert "Workspace: eng-runtime" in hits[0].text
    finally:
        backend.close()
