"""Tests for orchestrator external worker adapter integration."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from janus_code.agent.orchestrator import Orchestrator
from janus_code.config import Config
from janus_code.janus_integration.session_manager import TxnContext


class _FakeAdapter:
    def __init__(self) -> None:
        self.runtime = "codex"
        self.calls: list[dict] = []

    def build_branch_session_id(self, *, txn_id: str, agent_type: str) -> str:
        return f"branch-{txn_id}-{agent_type}"

    async def run_worker(self, **kwargs):
        self.calls.append(dict(kwargs))
        return "external summary"


class _FakeEmbeddedServer:
    def __init__(self) -> None:
        self.url = "http://127.0.0.1:8765/mcp"
        self.started = 0
        self.registered: list[tuple[str, object]] = []
        self.unregistered: list[str] = []

    def ensure_started(self) -> None:
        self.started += 1

    def register_session(self, session_id: str, janus_context: object) -> None:
        self.registered.append((session_id, janus_context))

    def unregister_session(self, session_id: str) -> None:
        self.unregistered.append(session_id)


@pytest.mark.asyncio
async def test_run_external_worker_uses_embedded_mcp_url(tmp_path: Path) -> None:
    orch = Orchestrator(
        config=Config(),
        working_dir=tmp_path,
        tools={},
        llm_fn=lambda _m, _s: {"content": "ok", "tool_calls": None},
    )
    fake_adapter = _FakeAdapter()
    fake_server = _FakeEmbeddedServer()
    orch._external_worker_adapter = fake_adapter
    orch._embedded_mcp_server = fake_server

    janus_ctx = SimpleNamespace(is_active=True)
    txn = TxnContext(txn_id="txn7", janus_context=janus_ctx)

    summary = await orch._run_external_worker(
        agent_type="implement",
        objective="Implement feature",
        task_contract={"node_id": "n1", "objective": "Implement feature"},
        txn=txn,
        working_dir=tmp_path,
        session=None,
    )

    assert summary == "external summary"
    assert fake_server.started == 1
    assert len(fake_server.registered) == 1
    sid, registered_ctx = fake_server.registered[0]
    assert sid == "branch-txn7-implement"
    assert registered_ctx is janus_ctx
    assert fake_server.unregistered == ["branch-txn7-implement"]
    assert fake_adapter.calls[0]["mcp_url"] == fake_server.url


@pytest.mark.asyncio
async def test_run_external_worker_without_embedded_server(tmp_path: Path) -> None:
    orch = Orchestrator(
        config=Config(),
        working_dir=tmp_path,
        tools={},
        llm_fn=lambda _m, _s: {"content": "ok", "tool_calls": None},
    )
    fake_adapter = _FakeAdapter()
    orch._external_worker_adapter = fake_adapter
    orch._embedded_mcp_server = None

    txn = TxnContext(txn_id="txn9")
    summary = await orch._run_external_worker(
        agent_type="explore",
        objective="Inspect project",
        task_contract={"node_id": "n2", "objective": "Inspect project"},
        txn=txn,
        working_dir=tmp_path,
        session=None,
    )
    assert summary == "external summary"
    assert fake_adapter.calls[0]["mcp_url"] is None
