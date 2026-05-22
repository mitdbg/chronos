"""Unit tests for Janus MCP bootstrap helpers."""

from __future__ import annotations

import json
from pathlib import Path

from janus_code.mcp_server.bootstrap import bootstrap_project


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_bootstrap_writes_mcp_and_instruction_assets(tmp_path: Path) -> None:
    result = bootstrap_project(project_dir=tmp_path, runtime="both")

    mcp_path = tmp_path / ".mcp.json"
    assert result.mcp_config_path == mcp_path
    assert mcp_path.exists()

    config = _load_json(mcp_path)
    assert "mcpServers" in config
    tar_server = config["mcpServers"]["janus"]
    assert tar_server["command"] == "janus-code-mcp"
    assert "--project" in tar_server["args"]
    assert str(tmp_path.resolve()) in tar_server["args"]

    codex_instruction = tmp_path / ".janus-code/bootstrap/codex/AGENTS.janus.md"
    codex_skill = (
        tmp_path / ".janus-code/bootstrap/codex/skills/janus-transaction-protocol/SKILL.md"
    )
    claude_instruction = tmp_path / ".janus-code/bootstrap/claude/CLAUDE.janus.md"
    claude_script = tmp_path / ".janus-code/bootstrap/claude/register_mcp.sh"

    for path in (codex_instruction, codex_skill, claude_instruction, claude_script):
        assert path.exists()
        assert path in result.written_files


def test_bootstrap_keeps_existing_server_entry_without_overwrite(tmp_path: Path) -> None:
    mcp_path = tmp_path / ".mcp.json"
    mcp_path.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "janus": {"command": "existing-command", "args": ["--old"]},
                    "other": {"url": "http://localhost:1234/mcp"},
                }
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    result = bootstrap_project(
        project_dir=tmp_path,
        runtime="codex",
        overwrite_server=False,
    )

    config = _load_json(mcp_path)
    assert config["mcpServers"]["janus"]["command"] == "existing-command"
    assert config["mcpServers"]["other"]["url"] == "http://localhost:1234/mcp"
    assert len(result.warnings) == 1
    assert "already exists" in result.warnings[0]


def test_bootstrap_overwrites_existing_entry_when_requested(tmp_path: Path) -> None:
    mcp_path = tmp_path / ".mcp.json"
    mcp_path.write_text(
        json.dumps({"mcpServers": {"janus": {"command": "old-cmd"}}}, indent=2) + "\n",
        encoding="utf-8",
    )

    bootstrap_project(
        project_dir=tmp_path,
        runtime="codex",
        overwrite_server=True,
        mcp_command="custom-janus-mcp",
    )

    config = _load_json(mcp_path)
    assert config["mcpServers"]["janus"]["command"] == "custom-janus-mcp"


def test_bootstrap_http_transport_writes_url_entry(tmp_path: Path) -> None:
    bootstrap_project(
        project_dir=tmp_path,
        runtime="claude",
        transport="streamable-http",
        http_url="http://127.0.0.1:9876/mcp",
    )

    config = _load_json(tmp_path / ".mcp.json")
    assert config["mcpServers"]["janus"]["url"] == "http://127.0.0.1:9876/mcp"
