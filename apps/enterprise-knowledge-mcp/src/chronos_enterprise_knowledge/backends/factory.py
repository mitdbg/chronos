"""Construct interchangeable storage backends for MCP and trace replay."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

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

# Enterprise service and benchmark runs share this PostgreSQL deployment.  A
# missing CLI option must not silently switch Chronos to a per-process SQLite
# file, since that changes the concurrency behavior being measured.
DEFAULT_CHRONOS_POSTGRES_DSN = (
    "postgresql://postgres:password@127.0.0.1:55441/"
    "chronos_enterprise_state_v2"
)


def default_chronos_postgres_dsn() -> str:
    """Return the benchmark default, with an environment override."""

    return os.environ.get("CHRONOS_POSTGRES_DSN", DEFAULT_CHRONOS_POSTGRES_DSN)


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
    chronos_postgres_dsn: str | None = None,
    chronos_postgres_data_dir: str | Path | None = None,
    chronos_enable_session_epochs: bool = True,
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
    if name == "chronos":
        postgres_dsn = chronos_postgres_dsn or default_chronos_postgres_dsn()
        if urlparse(postgres_dsn).scheme not in {"postgres", "postgresql"}:
            raise ValueError(
                "Chronos enterprise backends require PostgreSQL; "
                "SQLite URLs are reserved for direct low-level adapter tests"
            )
        return implementation(
            state_dir,
            **common,
            qdrant_storage_dir=qdrant_storage_dir,
            relational_url=postgres_dsn,
            relational_storage_dir=chronos_postgres_data_dir,
            enable_session_epochs=chronos_enable_session_epochs,
        )
    return implementation(
        state_dir,
        **common,
        qdrant_storage_dir=qdrant_storage_dir,
    )


__all__ = [
    "BackendName",
    "DEFAULT_CHRONOS_POSTGRES_DSN",
    "create_knowledge_backend",
    "default_chronos_postgres_dsn",
]
