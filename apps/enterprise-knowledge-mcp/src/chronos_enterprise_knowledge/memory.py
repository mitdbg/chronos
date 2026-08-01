"""Durable, provenance-bearing agent memory stored as branch-local knowledge."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

from chronos_enterprise_knowledge.ingestion import KnowledgeIngestor
from chronos_enterprise_knowledge.models import IndexedDocument, KnowledgeDocument

MemoryKind = Literal["semantic_memory", "episodic_memory", "playbook"]
_SAFE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class MemoryEntry:
    title: str
    summary: str
    kind: MemoryKind
    evidence: tuple[str, ...] = ()
    confidence: float = 1.0
    owner: str | None = None
    tags: tuple[str, ...] = ()
    outcome: str | None = None
    supersedes: tuple[str, ...] = ()
    recorded_at: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.title.strip():
            raise ValueError("memory title must not be empty")
        if not self.summary.strip():
            raise ValueError("memory summary must not be empty")
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError("memory confidence must be between 0 and 1")
        if self.kind == "semantic_memory" and not self.evidence:
            raise ValueError("semantic memory requires at least one evidence reference")


class AgentMemory:
    """Store only durable facts, task outcomes, and reusable procedures.

    Transient conversation turns do not become memory. Callers explicitly
    persist validated facts, completed task outcomes, or reusable playbooks.
    """

    def __init__(self, ingestor: KnowledgeIngestor):
        self.ingestor = ingestor

    def remember(
        self,
        branch_id: str,
        entry: MemoryEntry,
        *,
        memory_id: str | None = None,
        operation_id: str,
    ) -> IndexedDocument:
        identifier = memory_id or self._memory_id(entry)
        content = self._render(entry)
        document = KnowledgeDocument(
            id=identifier,
            path=f"/memory/{entry.kind}/{_slug(entry.title)}-{identifier[-8:]}.md",
            title=entry.title,
            source="agent-memory",
            content=content,
            kind=entry.kind,
            metadata={
                "memory_kind": entry.kind,
                "evidence": list(entry.evidence),
                "confidence": entry.confidence,
                "owner": entry.owner,
                "tags": list(entry.tags),
                "outcome": entry.outcome,
                "supersedes": list(entry.supersedes),
                **dict(entry.metadata),
            },
        )
        return self.ingestor.index_document(
            branch_id,
            document,
            index_text=content,
            context={
                "workspace": "agent-memory",
                "team": entry.owner or "",
                "memory_kind": entry.kind,
                "confidence": entry.confidence,
                "evidence": list(entry.evidence),
                "tags": list(entry.tags),
                "supersedes": list(entry.supersedes),
            },
            operation_id=operation_id,
        )

    @staticmethod
    def _memory_id(entry: MemoryEntry) -> str:
        material = "\n".join(
            [
                entry.kind,
                entry.owner or "",
                entry.title.strip().casefold(),
            ]
        )
        return f"memory_{hashlib.sha256(material.encode()).hexdigest()[:24]}"

    @staticmethod
    def _render(entry: MemoryEntry) -> str:
        recorded = entry.recorded_at or datetime.now(timezone.utc).isoformat()
        lines = [
            f"# {entry.title}",
            "",
            f"Memory type: {entry.kind}",
            f"Recorded: {recorded}",
            f"Confidence: {entry.confidence:.2f}",
        ]
        if entry.owner:
            lines.append(f"Owner: {entry.owner}")
        if entry.tags:
            lines.append(f"Tags: {', '.join(entry.tags)}")
        lines.extend(["", "## Summary", "", entry.summary.strip()])
        if entry.outcome:
            lines.extend(["", "## Outcome", "", entry.outcome.strip()])
        if entry.evidence:
            lines.extend(
                [
                    "",
                    "## Evidence",
                    "",
                    *[f"- {reference}" for reference in entry.evidence],
                ]
            )
        if entry.supersedes:
            lines.extend(
                [
                    "",
                    "## Supersedes",
                    "",
                    *[f"- {memory}" for memory in entry.supersedes],
                ]
            )
        return "\n".join(lines).strip() + "\n"


def _slug(value: str) -> str:
    slug = _SAFE.sub("-", value.casefold()).strip("-")
    return slug[:64] or "memory"


__all__ = [
    "AgentMemory",
    "MemoryEntry",
    "MemoryKind",
]
