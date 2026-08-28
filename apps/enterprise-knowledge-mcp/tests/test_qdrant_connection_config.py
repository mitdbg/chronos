from __future__ import annotations

import httpx

from chronos_core.workspace import qdrant as chronos_qdrant
from chronos_enterprise_knowledge import retrieval


def test_remote_qdrant_client_bounds_http_connections(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(retrieval, "QdrantClient", FakeClient)
    monkeypatch.delenv("CHRONOS_QDRANT_PREFER_GRPC", raising=False)
    monkeypatch.setenv("CHRONOS_QDRANT_POOL_SIZE", "2")

    retrieval.remote_qdrant_client("http://127.0.0.1:6340")

    limits = captured["limits"]
    assert isinstance(limits, httpx.Limits)
    assert limits.max_connections == 2
    assert limits.max_keepalive_connections == 2
    assert "pool_size" not in captured


def test_remote_qdrant_client_bounds_grpc_channels(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(retrieval, "QdrantClient", FakeClient)
    monkeypatch.setenv("CHRONOS_QDRANT_PREFER_GRPC", "true")
    monkeypatch.setenv("CHRONOS_QDRANT_POOL_SIZE", "3")

    retrieval.remote_qdrant_client("http://127.0.0.1:6340")

    assert captured["pool_size"] == 3
    assert "limits" not in captured


def test_chronos_qdrant_remote_uses_same_http_bound(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(chronos_qdrant, "QdrantClient", FakeClient)
    monkeypatch.setattr(chronos_qdrant, "_require_qdrant", lambda: None)
    monkeypatch.setattr(
        chronos_qdrant.ChronosQdrantStore,
        "__init__",
        lambda self, *args, **kwargs: None,
    )
    monkeypatch.delenv("CHRONOS_QDRANT_PREFER_GRPC", raising=False)
    monkeypatch.setenv("CHRONOS_QDRANT_POOL_SIZE", "1")

    chronos_qdrant.ChronosQdrantStore.remote(
        "postgresql://example/metadata",
        url="http://127.0.0.1:6340",
    )

    limits = captured["limits"]
    assert isinstance(limits, httpx.Limits)
    assert limits.max_connections == 1
    assert limits.max_keepalive_connections == 1
