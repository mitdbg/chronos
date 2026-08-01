from __future__ import annotations

import json
from pathlib import Path

from chronos_enterprise_knowledge.backends import ChronosKnowledgeBackend
from chronos_enterprise_knowledge.embedding import HashEmbedder
from chronos_enterprise_knowledge.evaluation import (
    RetrievalQuestion,
    evaluate_retrieval,
    read_questions,
)
from chronos_enterprise_knowledge.service import KnowledgeService


def test_retrieval_report_uses_the_mcp_search_path(tmp_path: Path) -> None:
    backend = ChronosKnowledgeBackend(tmp_path / "state", vector_dimensions=32)
    service = KnowledgeService(backend, HashEmbedder(32))
    try:
        service.update_document(
            "main",
            document_id="dsid_streaming",
            path="/knowledge/company/github/streaming.md",
            title="Streaming timeout metric",
            content=(
                "The stream.timebox_finalized metric counts sessions finalized "
                "after the server-side streaming time limit."
            ),
            source="EnterpriseRAG/github",
        )
        service.update_document(
            "main",
            document_id="dsid_unrelated",
            path="/knowledge/company/confluence/billing.md",
            title="Billing",
            content="Invoices are issued monthly.",
            source="EnterpriseRAG/confluence",
        )
        questions = tmp_path / "questions.jsonl"
        questions.write_text(
            json.dumps(
                {
                    "question_id": "qst_0002",
                    "question_type": "basic",
                    "source_types": ["github"],
                    "question": (
                        "What metric tracks streaming sessions finalized after "
                        "hitting the time limit?"
                    ),
                    "expected_doc_ids": ["dsid_streaming"],
                }
            )
            + "\n"
        )

        report = evaluate_retrieval(
            service,
            "main",
            read_questions(questions),
            top_k=2,
        )

        assert report.questions == 1
        assert report.scored_questions == 1
        assert report.recall_at_k == 1.0
        assert report.mean_reciprocal_rank == 1.0
        assert report.results[0].retrieved_doc_ids[0] == "dsid_streaming"
    finally:
        backend.close()


def test_retrieval_metrics_exclude_questions_without_gold_documents(
    tmp_path: Path,
) -> None:
    backend = ChronosKnowledgeBackend(
        tmp_path / "state",
        vector_dimensions=32,
    )
    service = KnowledgeService(backend, HashEmbedder(32))
    try:
        service.update_document(
            "main",
            document_id="doc-1",
            path="/knowledge/company/scheduler.md",
            title="Scheduler",
            content="The scheduler performs continuous batching.",
            source="EnterpriseRAG/github",
        )
        questions = [
            RetrievalQuestion(
                "answerable",
                "continuous batching scheduler",
                ("doc-1",),
                "basic",
                ("github",),
            ),
            RetrievalQuestion(
                "synthesis",
                "summarize the product direction",
                (),
                "high_level",
                (),
            ),
        ]

        report = evaluate_retrieval(
            service,
            "main",
            questions,
            top_k=2,
        )

        assert report.questions == 2
        assert report.scored_questions == 1
        assert report.hits == 1
        assert report.recall_at_k == 1.0
        assert report.mean_reciprocal_rank == 1.0
        assert report.results[1].scorable is False
        assert report.as_dict()["unscored_questions"] == 1
    finally:
        backend.close()
