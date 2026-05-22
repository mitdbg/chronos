"""Unit tests for external Codex/Claude worker adapter layer."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from janus_code.agent.external_workers import CommandResult, ExternalWorkerAdapter
from janus_code.config import Config


@pytest.mark.asyncio
async def test_external_worker_writes_mcp_config_and_prompt(tmp_path: Path) -> None:
    seen: dict[str, object] = {}

    async def fake_runner(command: list[str], cwd: Path, timeout_sec: int) -> CommandResult:
        seen["command"] = list(command)
        seen["cwd"] = cwd
        seen["timeout"] = timeout_sec
        return CommandResult(returncode=0, stdout="external-summary", stderr="")

    cfg = Config(worker_runtime="codex")
    adapter = ExternalWorkerAdapter(
        cfg,
        root_dir=tmp_path,
        command_runner=fake_runner,
    )

    summary = await adapter.run_worker(
        runtime="codex",
        agent_type="implement",
        objective="Implement feature X",
        task_contract={
            "node_id": "n1",
            "objective": "Implement feature X",
            "allowed_tools": ["janus_txn", "janus_file_editor"],
        },
        branch_session_id="branch-txn5-implement",
        working_dir=tmp_path,
        txn_id="txn5",
    )

    assert summary == "external-summary"
    command = seen["command"]
    assert isinstance(command, list)
    assert command[0] == "codex"
    run_dir = seen["cwd"]
    assert isinstance(run_dir, Path)
    mcp_cfg_path = run_dir / ".mcp.json"
    assert mcp_cfg_path.exists()

    payload = json.loads(mcp_cfg_path.read_text(encoding="utf-8"))
    server = payload["mcpServers"][cfg.external_worker_mcp_server_name]
    assert server["command"] == "janus-code-mcp"
    assert "--enforced-session-id" in server["args"]
    assert "branch-txn5-implement" in server["args"]

    prompt_path = run_dir / "prompt.md"
    prompt = prompt_path.read_text(encoding="utf-8")
    assert "branch-txn5-implement" in prompt
    assert '"node_id": "n1"' in prompt


@pytest.mark.asyncio
async def test_external_worker_uses_embedded_mcp_url_when_provided(tmp_path: Path) -> None:
    seen: dict[str, object] = {}

    async def fake_runner(command: list[str], cwd: Path, timeout_sec: int) -> CommandResult:
        seen["command"] = list(command)
        return CommandResult(returncode=0, stdout="ok", stderr="")

    cfg = Config(worker_runtime="codex")
    adapter = ExternalWorkerAdapter(cfg, root_dir=tmp_path, command_runner=fake_runner)

    await adapter.run_worker(
        runtime="codex",
        agent_type="explore",
        objective="Inspect",
        task_contract={"node_id": "n3", "objective": "Inspect"},
        branch_session_id="branch-txn1-explore",
        working_dir=tmp_path,
        txn_id="txn1",
        mcp_url="http://127.0.0.1:8765/mcp",
    )

    command = seen["command"]
    assert "-c" in command
    cfg_value = command[command.index("-c") + 1]
    assert (
        cfg_value
        == f'mcp_servers.{cfg.external_worker_mcp_server_name}.url="http://127.0.0.1:8765/mcp"'
    )


@pytest.mark.asyncio
async def test_external_worker_emits_condensed_codex_stream_events(tmp_path: Path) -> None:
    codex_lines = [
        {
            "type": "item.started",
            "item": {
                "id": "item_1",
                "type": "mcp_tool_call",
                "tool": "janus_bash",
                "arguments": {"session_id": "branch-x", "command": "ls -la"},
            },
        },
        {
            "type": "item.completed",
            "item": {
                "id": "item_1",
                "type": "mcp_tool_call",
                "tool": "janus_bash",
                "arguments": {"session_id": "branch-x", "command": "ls -la"},
                "result": {
                    "structured_content": {"result": "ok"},
                },
            },
        },
        {
            "type": "item.completed",
            "item": {
                "id": "item_2",
                "type": "agent_message",
                "text": "I inspected the project.",
            },
        },
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 11,
                "cached_input_tokens": 5,
                "output_tokens": 3,
            },
        },
    ]
    stdout = "\n".join(json.dumps(line) for line in codex_lines) + "\n"

    async def fake_runner(_command: list[str], _cwd: Path, _timeout_sec: int) -> CommandResult:
        return CommandResult(returncode=0, stdout=stdout, stderr="")

    events: list[dict[str, object]] = []

    cfg = Config(worker_runtime="codex")
    adapter = ExternalWorkerAdapter(
        cfg,
        root_dir=tmp_path,
        event_sink=lambda payload: events.append(payload),
        command_runner=fake_runner,
    )

    summary = await adapter.run_worker(
        runtime="codex",
        agent_type="explore",
        objective="Inspect",
        task_contract={"node_id": "n1", "objective": "Inspect"},
        branch_session_id="branch-x",
        working_dir=tmp_path,
        txn_id="txn1",
    )

    assert summary == "I inspected the project."
    event_types = [str(e.get("type")) for e in events]
    assert "external_worker_tool_call" in event_types
    assert "external_worker_tool_result" in event_types
    assert "external_worker_message" in event_types
    assert "external_worker_usage" in event_types
    assert "external_worker_done" in event_types


@pytest.mark.asyncio
async def test_external_worker_skips_mcp_tool_events_when_embedded_server_traces(tmp_path: Path) -> None:
    stdout = "\n".join(
        [
            json.dumps(
                {
                    "type": "item.started",
                    "item": {
                        "id": "item_1",
                        "type": "mcp_tool_call",
                        "tool": "janus_txn",
                        "arguments": {"session_id": "branch-x", "action": "status"},
                    },
                }
            ),
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {
                        "id": "item_2",
                        "type": "agent_message",
                        "text": "done",
                    },
                }
            ),
        ]
    ) + "\n"

    async def fake_runner(_command: list[str], _cwd: Path, _timeout_sec: int) -> CommandResult:
        return CommandResult(returncode=0, stdout=stdout, stderr="")

    events: list[dict[str, object]] = []
    cfg = Config(worker_runtime="codex")
    adapter = ExternalWorkerAdapter(
        cfg,
        root_dir=tmp_path,
        event_sink=lambda payload: events.append(payload),
        command_runner=fake_runner,
    )

    await adapter.run_worker(
        runtime="codex",
        agent_type="implement",
        objective="Check",
        task_contract={"node_id": "n2", "objective": "Check"},
        branch_session_id="branch-x",
        working_dir=tmp_path,
        txn_id="txn2",
        mcp_url="http://127.0.0.1:8765/mcp",
    )

    event_types = [str(e.get("type")) for e in events]
    assert "external_worker_message" in event_types
    assert "external_worker_tool_call" not in event_types


@pytest.mark.asyncio
async def test_external_worker_nonzero_exit_raises(tmp_path: Path) -> None:
    async def fake_runner(_command: list[str], _cwd: Path, _timeout: int) -> CommandResult:
        return CommandResult(returncode=2, stdout="", stderr="bad arguments")

    cfg = Config(worker_runtime="claude")
    adapter = ExternalWorkerAdapter(cfg, root_dir=tmp_path, command_runner=fake_runner)

    with pytest.raises(RuntimeError, match="External worker failed"):
        await adapter.run_worker(
            runtime="claude",
            agent_type="explore",
            objective="Inspect code",
            task_contract={"node_id": "n2", "objective": "Inspect"},
            branch_session_id="branch-x",
            working_dir=tmp_path,
            txn_id="txn9",
        )


def test_build_branch_session_id_sanitizes_value(tmp_path: Path) -> None:
    cfg = Config()
    adapter = ExternalWorkerAdapter(cfg, root_dir=tmp_path)
    sid = adapter.build_branch_session_id(
        txn_id="txn/7:bad",
        agent_type="implement*phase",
    )
    assert sid.startswith("branch-")
    assert "/" not in sid
    assert "*" not in sid
