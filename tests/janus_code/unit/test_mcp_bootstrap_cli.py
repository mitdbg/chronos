"""CLI tests for Janus MCP bootstrap entrypoint."""

from __future__ import annotations

from click.testing import CliRunner

from janus_code.mcp_server.bootstrap_cli import main


def test_help_includes_bootstrap_options() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "--runtime" in result.output
    assert "--transport" in result.output
    assert "--overwrite-server" in result.output


def test_bootstrap_cli_writes_mcp_config(tmp_path) -> None:
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--project",
            str(tmp_path),
            "--runtime",
            "codex",
        ],
    )
    assert result.exit_code == 0
    assert "Updated MCP config" in result.output
    assert (tmp_path / ".mcp.json").exists()
