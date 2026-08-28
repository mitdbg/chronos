"""Backend-neutral planning for selective, non-atomic comparison merges."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chronos_enterprise_knowledge.models import (
    IndexedDocument,
    canonical_json,
    content_hash,
)


class SelectiveMergeDependencyError(ValueError):
    """A selected baseline change omits part of a logical state bundle."""

    def __init__(
        self,
        missing_change_ids: Sequence[str] = (),
        *,
        stale_index_paths: Sequence[str] = (),
    ):
        self.missing_change_ids = tuple(sorted(set(missing_change_ids)))
        self.stale_index_paths = tuple(sorted(set(stale_index_paths)))
        messages = []
        if self.missing_change_ids:
            messages.append(
                "merge selection is missing dependent changes: "
                + ", ".join(self.missing_change_ids)
            )
        if self.stale_index_paths:
            messages.append(
                "indexed document files changed without updated catalog and "
                "embeddings; reindex before merge: " + ", ".join(self.stale_index_paths)
            )
        super().__init__("; ".join(messages))


class PreparedMergePreview(dict[str, Any]):
    """Serializable preview payload carrying its in-process logical plan.

    The plan is deliberately kept as an attribute rather than inserted into
    the mapping, so the result remains JSON serializable for MCP callers.  A
    caller in the same process can pass the preview result directly to
    ``merge(prepared_preview=...)`` and avoid rebuilding the plan.
    """

    __slots__ = ("plan",)

    def __init__(self, payload: Mapping[str, Any], plan: "LogicalMergePlan"):
        super().__init__(payload)
        self.plan = plan


@dataclass(frozen=True)
class LogicalMergePlan:
    source: str
    target: str
    preview_token: str
    change_ids: frozenset[str]
    document_groups: Mapping[str, frozenset[str]]
    filesystem_groups: Mapping[str, frozenset[str]]
    changed_documents: tuple[str, ...]
    changed_files: tuple[str, ...]
    stale_index_paths: tuple[str, ...]
    stores: Mapping[str, Mapping[str, Any]]

    def _selection_filesystem_groups(self) -> dict[str, frozenset[str]]:
        """Expose file and containing-directory aliases for selection."""

        groups = {
            path: set(change_ids)
            for path, change_ids in self.filesystem_groups.items()
        }
        for path, change_ids in self.filesystem_groups.items():
            parent = Path(path).parent
            while str(parent) not in {"", ".", "/"}:
                groups.setdefault(str(parent), set()).update(change_ids)
                parent = parent.parent
        return {
            path: frozenset(change_ids)
            for path, change_ids in groups.items()
        }

    def preview(self) -> PreparedMergePreview:
        filesystem_groups = self._selection_filesystem_groups()
        payload = {
            "source": self.source,
            "target": self.target,
            "preview_token": self.preview_token,
            "change_ids": sorted(self.change_ids),
            "stores": self.stores,
            "selection_groups": {
                "indexed_documents": {
                    key: sorted(values)
                    for key, values in sorted(self.document_groups.items())
                },
                "filesystem_paths": {
                    key: sorted(values)
                    for key, values in sorted(filesystem_groups.items())
                },
            },
            "stale_index_paths": list(self.stale_index_paths),
            "atomic": False,
        }
        return PreparedMergePreview(payload, self)

    def resolve_selection(
        self,
        selected_change_ids: Sequence[str] | None,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        selected = (
            self.change_ids
            if selected_change_ids is None
            else frozenset(str(value) for value in selected_change_ids)
        )
        unknown = selected - self.change_ids
        if unknown:
            raise ValueError(
                "merge selection contains unknown change IDs: "
                + ", ".join(sorted(unknown))
            )
        missing: set[str] = set()
        chosen_documents: list[str] = []
        for document_id, group in self.document_groups.items():
            chosen = group & selected
            if not chosen:
                continue
            if chosen != group:
                missing.update(group - selected)
            else:
                chosen_documents.append(document_id)
        if missing:
            raise SelectiveMergeDependencyError(sorted(missing))
        selected_stale_paths = [
            path
            for path in self.stale_index_paths
            if self.filesystem_groups[path] & selected
        ]
        if selected_stale_paths:
            raise SelectiveMergeDependencyError(stale_index_paths=selected_stale_paths)
        chosen_files = [
            path for path, group in self.filesystem_groups.items() if group <= selected
        ]
        return tuple(sorted(chosen_documents)), tuple(sorted(chosen_files))


def build_logical_merge_plan(
    source: str,
    target: str,
    *,
    source_documents: Mapping[str, IndexedDocument],
    target_documents: Mapping[str, IndexedDocument],
    source_document_digests: Mapping[str, str],
    target_document_digests: Mapping[str, str],
    source_files: Mapping[str, bytes | None],
    target_files: Mapping[str, bytes | None],
) -> LogicalMergePlan:
    """Describe selectable logical changes without supplying atomic commit."""

    document_ids = sorted(
        identifier
        for identifier in source_document_digests.keys()
        | target_document_digests.keys()
        if source_document_digests.get(identifier)
        != target_document_digests.get(identifier)
    )
    file_paths = sorted(
        path
        for path in source_files.keys() | target_files.keys()
        if source_files.get(path) != target_files.get(path)
    )

    filesystem_groups: dict[str, set[str]] = {}
    filesystem_changes: list[dict[str, Any]] = []
    for path in file_paths:
        before = _optional_content_hash(target_files.get(path))
        after = _optional_content_hash(source_files.get(path))
        change_id = _change_id(
            "filesystem",
            {"path": path, "before": before, "after": after},
        )
        filesystem_groups[path] = {change_id}
        filesystem_changes.append(
            {
                "change_id": change_id,
                "table": "files",
                "key": {"path": path},
                "before": before,
                "after": after,
            }
        )

    relational_changes: list[dict[str, Any]] = []
    qdrant_changes: list[dict[str, Any]] = []
    document_groups: dict[str, frozenset[str]] = {}
    indexed_paths: dict[str, set[str]] = {}
    for identifier, indexed in list(source_documents.items()) + list(
        target_documents.items()
    ):
        indexed_paths.setdefault(indexed.document.path, set()).add(identifier)

    for document_id in document_ids:
        before = target_document_digests.get(document_id)
        after = source_document_digests.get(document_id)
        relational_id = _change_id(
            "relational",
            {"document_id": document_id, "before": before, "after": after},
        )
        group = {relational_id}
        relational_changes.append(
            {
                "change_id": relational_id,
                "table": "knowledge_documents",
                "key": {"id": document_id},
                "before": before,
                "after": after,
            }
        )
        source_indexed = source_documents.get(document_id)
        target_indexed = target_documents.get(document_id)
        if (source_indexed and source_indexed.chunks) or (
            target_indexed and target_indexed.chunks
        ):
            qdrant_id = _change_id(
                "qdrant",
                {
                    "document_id": document_id,
                    "before": before,
                    "after": after,
                },
            )
            group.add(qdrant_id)
            qdrant_changes.append(
                {
                    "change_id": qdrant_id,
                    "table": "knowledge_chunks",
                    "key": {"document_id": document_id},
                    "before": before,
                    "after": after,
                }
            )
        for indexed in (source_indexed, target_indexed):
            if indexed is not None and indexed.document.path in filesystem_groups:
                group.update(filesystem_groups[indexed.document.path])
        document_groups[document_id] = frozenset(group)

    changed_document_set = set(document_ids)
    stale_index_paths = tuple(
        path
        for path in file_paths
        if indexed_paths.get(path) and not indexed_paths[path] & changed_document_set
    )
    stores: dict[str, Mapping[str, Any]] = {
        "relational": {"changes": relational_changes, "conflicts": []},
        "qdrant": {"changes": qdrant_changes, "conflicts": []},
        "filesystem": {"changes": filesystem_changes, "conflicts": []},
    }
    change_ids = frozenset(
        change["change_id"] for store in stores.values() for change in store["changes"]
    )
    preview_token = hashlib.sha256(
        canonical_json(
            {
                "source": source,
                "target": target,
                "stores": stores,
                "selection_groups": {
                    key: sorted(value) for key, value in sorted(document_groups.items())
                },
                "stale_index_paths": stale_index_paths,
            }
        ).encode()
    ).hexdigest()
    return LogicalMergePlan(
        source=source,
        target=target,
        preview_token=preview_token,
        change_ids=change_ids,
        document_groups=document_groups,
        filesystem_groups={
            key: frozenset(values) for key, values in filesystem_groups.items()
        },
        changed_documents=tuple(document_ids),
        changed_files=tuple(file_paths),
        stale_index_paths=stale_index_paths,
        stores=stores,
    )


def catalog_digest(
    document: Mapping[str, Any],
    chunks: Sequence[Mapping[str, Any]],
) -> str:
    """Hash stored catalog/index metadata without reading workspace content."""

    return hashlib.sha256(
        canonical_json(
            {
                "document": dict(document),
                "chunks": [dict(row) for row in chunks],
            }
        ).encode()
    ).hexdigest()


def _change_id(store: str, value: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(canonical_json(value).encode()).hexdigest()[:24]
    return f"{store}:{digest}"


def _optional_content_hash(content: bytes | None) -> str | None:
    return content_hash(content) if content is not None else None


__all__ = [
    "LogicalMergePlan",
    "SelectiveMergeDependencyError",
    "build_logical_merge_plan",
    "catalog_digest",
]
