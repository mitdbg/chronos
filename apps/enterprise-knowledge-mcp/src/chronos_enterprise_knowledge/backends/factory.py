"""Construct interchangeable storage backends for MCP and trace replay."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from chronos_enterprise_knowledge.backend import KnowledgeBackend
from chronos_enterprise_knowledge.backends.chronos import ChronosKnowledgeBackend
from chronos_enterprise_knowledge.backends.comparison import (
    ApplicationManagedKnowledgeBackend,
    PhysicalCloneKnowledgeBackend,
)
from chronos_enterprise_knowledge.backends.native_branching import (
    DoltgresQdrantBtrfsKnowledgeBackend,
)

BackendName = Literal[
    "chronos",
    "app-managed",
    "physical-clone",
    "doltgres-qdrant-btrfs",
]


def create_knowledge_backend(
    name: BackendName,
    state_dir: str | Path,
    *,
    vector_dimensions: int,
    qdrant_url: str | None = None,
    qdrant_api_key: str | None = None,
    doltgres_dsn: str | None = None,
    btrfs_root: str | Path | None = None,
    doltgres_data_dir: str | Path | None = None,
    qdrant_storage_dir: str | Path | None = None,
) -> KnowledgeBackend:
    implementations = {
        "chronos": ChronosKnowledgeBackend,
        "app-managed": ApplicationManagedKnowledgeBackend,
        "physical-clone": PhysicalCloneKnowledgeBackend,
        "doltgres-qdrant-btrfs": DoltgresQdrantBtrfsKnowledgeBackend,
    }
    try:
        implementation = implementations[name]
    except KeyError as exc:
        raise ValueError(f"unknown knowledge backend: {name}") from exc
    common = {
        "vector_dimensions": vector_dimensions,
        "qdrant_url": qdrant_url,
        "qdrant_api_key": qdrant_api_key,
    }
    if name == "doltgres-qdrant-btrfs":
        return implementation(
            state_dir,
            **common,
            doltgres_dsn=doltgres_dsn,
            btrfs_root=btrfs_root,
            doltgres_data_dir=doltgres_data_dir,
            qdrant_storage_dir=qdrant_storage_dir,
        )
    return implementation(
        state_dir,
        **common,
        qdrant_storage_dir=qdrant_storage_dir,
    )


__all__ = ["BackendName", "create_knowledge_backend"]
