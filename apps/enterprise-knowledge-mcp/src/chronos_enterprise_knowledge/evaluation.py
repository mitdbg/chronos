"""EnterpriseRAG retrieval evaluation for the MCP knowledge tool."""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chronos_enterprise_knowledge.service import KnowledgeService


@dataclass(frozen=True)
class RetrievalQuestion:
    question_id: str
    question: str
    expected_doc_ids: tuple[str, ...]
    question_type: str
    source_types: tuple[str, ...]
    gold_answer: str | None = None

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> RetrievalQuestion:
        return cls(
            question_id=str(value["question_id"]),
            question=str(value["question"]),
            expected_doc_ids=tuple(str(item) for item in value["expected_doc_ids"]),
            question_type=str(value.get("question_type", "unknown")),
            source_types=tuple(str(item) for item in value.get("source_types", [])),
            gold_answer=(
                str(value["gold_answer"])
                if value.get("gold_answer") is not None
                else None
            ),
        )


@dataclass(frozen=True)
class RetrievalResult:
    question_id: str
    expected_doc_ids: tuple[str, ...]
    retrieved_doc_ids: tuple[str, ...]
    first_relevant_rank: int | None

    @property
    def scorable(self) -> bool:
        return bool(self.expected_doc_ids)

    @property
    def hit(self) -> bool:
        return self.scorable and self.first_relevant_rank is not None

    def as_dict(self) -> dict[str, Any]:
        return {
            "question_id": self.question_id,
            "expected_doc_ids": list(self.expected_doc_ids),
            "retrieved_doc_ids": list(self.retrieved_doc_ids),
            "first_relevant_rank": self.first_relevant_rank,
            "scorable": self.scorable,
            "hit": self.hit,
        }


@dataclass(frozen=True)
class RetrievalReport:
    branch_id: str
    top_k: int
    questions: int
    scored_questions: int
    hits: int
    recall_at_k: float
    mean_reciprocal_rank: float
    results: tuple[RetrievalResult, ...]

    def as_dict(self, *, include_results: bool = False) -> dict[str, Any]:
        value: dict[str, Any] = {
            "branch_id": self.branch_id,
            "top_k": self.top_k,
            "questions": self.questions,
            "scored_questions": self.scored_questions,
            "unscored_questions": self.questions - self.scored_questions,
            "hits": self.hits,
            "recall_at_k": self.recall_at_k,
            "mean_reciprocal_rank": self.mean_reciprocal_rank,
        }
        if include_results:
            value["results"] = [result.as_dict() for result in self.results]
        return value


def read_questions(
    path: str | Path,
    *,
    limit: int | None = None,
) -> Iterator[RetrievalQuestion]:
    with Path(path).open("r", encoding="utf-8") as handle:
        for index, line in enumerate(handle):
            if limit is not None and index >= limit:
                return
            if line.strip():
                yield RetrievalQuestion.from_dict(json.loads(line))


def evaluate_retrieval(
    service: KnowledgeService,
    branch_id: str,
    questions: Iterable[RetrievalQuestion],
    *,
    top_k: int = 20,
) -> RetrievalReport:
    if top_k <= 0:
        raise ValueError("top_k must be positive")
    results: list[RetrievalResult] = []
    reciprocal_rank = 0.0
    hits = 0
    scored_questions = 0
    for question in questions:
        search_hits = service.search(
            branch_id,
            question.question,
            limit=top_k,
        )
        retrieved = tuple(dict.fromkeys(hit.document_id for hit in search_hits))
        expected = set(question.expected_doc_ids)
        if expected:
            scored_questions += 1
        rank = next(
            (
                index
                for index, document_id in enumerate(retrieved, start=1)
                if document_id in expected
            ),
            None,
        )
        if rank is not None:
            hits += 1
            reciprocal_rank += 1.0 / rank
        results.append(
            RetrievalResult(
                question.question_id,
                question.expected_doc_ids,
                retrieved,
                rank,
            )
        )
    count = len(results)
    return RetrievalReport(
        branch_id=branch_id,
        top_k=top_k,
        questions=count,
        scored_questions=scored_questions,
        hits=hits,
        recall_at_k=(
            hits / scored_questions if scored_questions else 0.0
        ),
        mean_reciprocal_rank=(
            reciprocal_rank / scored_questions if scored_questions else 0.0
        ),
        results=tuple(results),
    )


__all__ = [
    "RetrievalQuestion",
    "RetrievalReport",
    "RetrievalResult",
    "evaluate_retrieval",
    "read_questions",
]
