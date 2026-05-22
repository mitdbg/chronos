"""Unit tests for Chronos MCP runtime session and transaction behavior."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from chronos_code.mcp_server.runtime import ChronosMcpRuntime, ChronosMcpRuntimeConfig


class _FakeTool:
    def __init__(self, label: str) -> None:
        self.label = label
        self.calls: list[dict] = []

    def invoke(self, payload: dict) -> str:
        self.calls.append(dict(payload))
        command = payload.get("command", "run")
        return f"{self.label}:{command}"


class _FakeContext:
    def __init__(self, base_path: Path, **_kwargs) -> None:
        self.base_path = Path(base_path)
        self.working_dir = self.base_path / ".overlay"
        self.working_dir.mkdir(parents=True, exist_ok=True)
        self.is_active = False
        self.txn = None
        self.file_editor = _FakeTool("file")
        self.bash = _FakeTool("bash")
        self.sqlite = _FakeTool("sqlite")
        self._next_id = 0
        self._changes: list[SimpleNamespace] = []
        self.savepoints: list[str] = []

    def begin(self):
        self._next_id += 1
        self.txn = SimpleNamespace(id=f"txn{self._next_id}")
        self.is_active = True
        return self.txn

    def commit(self) -> None:
        if not self.is_active:
            raise RuntimeError("No active transaction")
        self.is_active = False
        self._changes.clear()

    def abort(self) -> None:
        self.is_active = False
        self._changes.clear()

    def savepoint(self, name: str) -> None:
        if not self.is_active:
            raise RuntimeError("No active transaction")
        self.savepoints.append(name)

    def rollback(self, name: str | None = None) -> None:
        if not self.is_active:
            raise RuntimeError("No active transaction")
        if name and name not in self.savepoints:
            raise RuntimeError(f"Unknown savepoint: {name}")

    def get_changes(self):
        return list(self._changes)


def _build_runtime(tmp_path: Path) -> ChronosMcpRuntime:
    return ChronosMcpRuntime(
        ChronosMcpRuntimeConfig(project_dir=tmp_path),
        context_factory=lambda base_path, **kwargs: _FakeContext(base_path, **kwargs),
    )


def _build_enforced_runtime(tmp_path: Path, enforced_session_id: str) -> ChronosMcpRuntime:
    return ChronosMcpRuntime(
        ChronosMcpRuntimeConfig(
            project_dir=tmp_path,
            enforced_session_id=enforced_session_id,
        ),
        context_factory=lambda base_path, **kwargs: _FakeContext(base_path, **kwargs),
    )


def test_tools_require_active_transaction(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    output = runtime.chronos_file_editor(
        command="view",
        path="README.md",
        session_id="agent-a",
    )
    assert "No active transaction" in output


def test_transaction_lifecycle_status_and_savepoints(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    sid = "agent-a"

    start = runtime.chronos_txn(action="begin", session_id=sid)
    assert "Transaction started" in start

    status_active = runtime.chronos_txn(action="status", session_id=sid)
    assert "Transaction active" in status_active
    assert "session=agent-a" in status_active

    sp = runtime.chronos_txn(action="savepoint", session_id=sid, name="before_refactor")
    assert "Savepoint 'before_refactor' created" in sp

    rb = runtime.chronos_txn(action="rollback", session_id=sid, name="before_refactor")
    assert "Rolled back savepoint 'before_refactor'" in rb

    done = runtime.chronos_txn(action="commit", session_id=sid)
    assert "Transaction committed" in done
    assert "No active transaction" in runtime.chronos_txn(action="status", session_id=sid)


def test_sessions_are_isolated(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)

    runtime.chronos_txn(action="begin", session_id="agent-a")
    runtime.chronos_txn(action="begin", session_id="agent-b")

    out_a = runtime.chronos_file_editor(
        command="create",
        path="a.txt",
        file_text="hello-a",
        session_id="agent-a",
    )
    out_b = runtime.chronos_file_editor(
        command="create",
        path="b.txt",
        file_text="hello-b",
        session_id="agent-b",
    )

    assert out_a == "file:create"
    assert out_b == "file:create"

    ctx_a = runtime._contexts["agent-a"]
    ctx_b = runtime._contexts["agent-b"]
    assert ctx_a.file_editor.calls[0]["path"] == "a.txt"
    assert ctx_b.file_editor.calls[0]["path"] == "b.txt"


def test_close_session_aborts_active_txn(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    runtime.chronos_txn(action="begin", session_id="agent-a")
    out = runtime.close_session("agent-a", abort_active=True)
    assert out == "Session 'agent-a' closed."
    assert runtime.list_sessions() == []


def test_enforced_session_id_maps_default_and_rejects_others(tmp_path: Path) -> None:
    runtime = _build_enforced_runtime(tmp_path, enforced_session_id="branch-a")

    start = runtime.chronos_txn(action="begin")  # default alias
    assert "session=branch-a" in start

    ok = runtime.chronos_file_editor(
        command="create",
        path="x.txt",
        file_text="hello",
        session_id="default",
    )
    assert ok == "file:create"

    denied = runtime.chronos_bash(command="pwd", session_id="other-branch")
    assert "enforced as 'branch-a'" in denied
