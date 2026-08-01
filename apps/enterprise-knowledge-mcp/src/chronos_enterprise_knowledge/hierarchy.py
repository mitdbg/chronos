"""Curated Redwood organization hierarchy and experiment branch setup."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from importlib import resources
from typing import TYPE_CHECKING, Any

from chronos_enterprise_knowledge.backend import KnowledgeBackend
from chronos_enterprise_knowledge.ingestion import KnowledgeIngestor
from chronos_enterprise_knowledge.models import KnowledgeDocument

if TYPE_CHECKING:
    from chronos_enterprise_knowledge.snapshot import EmbeddingSnapshot

_SAFE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class ExperimentTask:
    id: str
    branch: str
    role: str
    objective: str
    evidence_scopes: tuple[str, ...]
    updates: tuple[str, ...]
    upstream_repository: str | None = None
    upstream_issue: int | None = None
    upstream_issue_url: str | None = None
    pinned_commit: str | None = None

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ExperimentTask:
        return cls(
            id=str(value["id"]),
            branch=str(value["branch"]),
            role=str(value["role"]),
            objective=str(value["objective"]),
            evidence_scopes=tuple(str(item) for item in value["evidence_scopes"]),
            updates=tuple(str(item) for item in value["updates"]),
            upstream_repository=(
                str(value["upstream_repository"])
                if value.get("upstream_repository")
                else None
            ),
            upstream_issue=(
                int(value["upstream_issue"])
                if value.get("upstream_issue") is not None
                else None
            ),
            upstream_issue_url=(
                str(value["upstream_issue_url"])
                if value.get("upstream_issue_url")
                else None
            ),
            pinned_commit=(
                str(value["pinned_commit"])
                if value.get("pinned_commit")
                else None
            ),
        )


def load_hierarchy() -> dict[str, Any]:
    return _load_resource("hierarchy.json")


def load_tasks() -> list[ExperimentTask]:
    value = _load_resource("tasks.json")
    return [ExperimentTask.from_dict(task) for task in value["tasks"]]


class HierarchyBuilder:
    """Create department, team, and personal branches plus starting briefs."""

    def __init__(
        self,
        backend: KnowledgeBackend,
        ingestor: KnowledgeIngestor,
        *,
        source_snapshot: EmbeddingSnapshot | None = None,
    ):
        self.backend = backend
        self.ingestor = ingestor
        self.source_snapshot = source_snapshot
        self.config = load_hierarchy()

    def build(self, *, include_people: bool = True) -> dict[str, int]:
        existing = set(self.backend.list_branches())
        branches_created = 0
        briefs_written = 0
        company = self.config["company"]
        root = str(company["branch_id"])
        self._put_brief(root, company, "company")
        briefs_written += 1
        for department in self.config["departments"]:
            department_branch = str(department["branch_id"])
            if department_branch not in existing:
                self.backend.create_branch(
                    department_branch,
                    root,
                    {"kind": "department", "name": department["name"]},
                )
                existing.add(department_branch)
                branches_created += 1
            self._put_brief(department_branch, department, "department")
            briefs_written += 1
            for team in department["teams"]:
                team_branch = str(team["branch_id"])
                if team_branch not in existing:
                    self.backend.create_branch(
                        team_branch,
                        department_branch,
                        {"kind": "team", "name": team["name"]},
                    )
                    existing.add(team_branch)
                    branches_created += 1
                self._put_brief(team_branch, team, "team")
                briefs_written += 1
                if not include_people:
                    continue
                for member in team["members"]:
                    person_branch = f"person/{_slug(member['name'])}"
                    if person_branch not in existing:
                        self.backend.create_branch(
                            person_branch,
                            team_branch,
                            {
                                "kind": "person",
                                "name": member["name"],
                                "title": member["title"],
                            },
                        )
                        existing.add(person_branch)
                        branches_created += 1
                    self._put_person_brief(person_branch, team, member)
                    briefs_written += 1
        return {
            "branches_created": branches_created,
            "briefs_written": briefs_written,
            "branches_total": len(existing),
        }

    def iter_nodes(self) -> Iterator[dict[str, Any]]:
        company = self.config["company"]
        yield {"kind": "company", **company}
        for department in self.config["departments"]:
            yield {"kind": "department", **department}
            for team in department["teams"]:
                yield {
                    "kind": "team",
                    "parent": department["branch_id"],
                    **team,
                }
                for member in team["members"]:
                    yield {
                        "kind": "person",
                        "branch_id": f"person/{_slug(member['name'])}",
                        "parent": team["branch_id"],
                        **member,
                    }

    def _put_brief(
        self,
        branch_id: str,
        node: Mapping[str, Any],
        kind: str,
    ) -> None:
        scopes = [str(item) for item in node.get("source_scopes", [])]
        lines = [
            f"# {node['name']} knowledge brief",
            "",
            str(node["starting_knowledge"]),
        ]
        if scopes:
            lines.extend(
                [
                    "",
                    "## High-value source areas",
                    "",
                    "These are retrieval hints, not access-control filters.",
                    "",
                    *[f"- {scope}" for scope in scopes],
                ]
            )
        if node.get("lead"):
            lines.extend(["", f"Lead: {node['lead']}"])
        catalog = (
            self.source_snapshot.source_catalog(scopes, per_scope=2)
            if self.source_snapshot is not None and scopes
            else []
        )
        if catalog:
            lines.extend(
                [
                    "",
                    "## Representative sources in this corpus",
                    "",
                    *[
                        (
                            f"- `{item['id']}` — {item['title']} "
                            f"(`{item['relative_path']}`)"
                        )
                        for item in catalog
                    ],
                ]
            )
        content = "\n".join(lines).strip() + "\n"
        identifier = _stable_id("brief", branch_id)
        document = KnowledgeDocument(
            id=identifier,
            path=f"/knowledge/curated/{_slug(branch_id)}/brief.md",
            title=f"{node['name']} knowledge brief",
            source="experiment-curation",
            content=content,
            kind="curated",
            metadata={
                "branch_id": branch_id,
                "organization_kind": kind,
                "source_scopes": scopes,
                "representative_source_ids": [
                    item["id"] for item in catalog
                ],
            },
        )
        self.ingestor.index_document(
            branch_id,
            document,
            context={"workspace": branch_id, "team": node["name"]},
            operation_id=f"hierarchy:{identifier}",
        )

    def _put_person_brief(
        self,
        branch_id: str,
        team: Mapping[str, Any],
        member: Mapping[str, Any],
    ) -> None:
        content = "\n".join(
            [
                f"# Working context for {member['name']}",
                "",
                f"Role: {member['title']}",
                f"Team: {team['name']}",
                f"Team lead: {team['lead']}",
                "",
                (
                    "This personal branch inherits company, department, and "
                    "team knowledge. Store only validated facts, completed task "
                    "outcomes, and reusable procedures as durable memory. Keep "
                    "hypotheses and generated artifacts in a task branch until "
                    "reviewed."
                ),
            ]
        )
        identifier = _stable_id("person-brief", branch_id)
        document = KnowledgeDocument(
            id=identifier,
            path=f"/knowledge/curated/{_slug(branch_id)}/working-context.md",
            title=f"Working context for {member['name']}",
            source="experiment-curation",
            content=content,
            kind="curated",
            metadata={
                "branch_id": branch_id,
                "person": member["name"],
                "role": member["title"],
                "team": team["name"],
            },
        )
        self.ingestor.index_document(
            branch_id,
            document,
            context={"workspace": branch_id, "team": team["name"]},
            operation_id=f"hierarchy:{identifier}",
        )


def _load_resource(name: str) -> dict[str, Any]:
    text = (
        resources.files("chronos_enterprise_knowledge.resources")
        .joinpath(name)
        .read_text(encoding="utf-8")
    )
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError(  # noqa: TRY004 - invalid resource value
            f"resource {name} must contain a JSON object"
        )
    return value


def _slug(value: str) -> str:
    return _SAFE.sub("-", str(value).casefold()).strip("-")


def _stable_id(kind: str, branch_id: str) -> str:
    digest = hashlib.sha256(f"{kind}:{branch_id}".encode()).hexdigest()[:24]
    return f"curated_{digest}"


__all__ = [
    "ExperimentTask",
    "HierarchyBuilder",
    "load_hierarchy",
    "load_tasks",
]
