"""CLI entrypoint for Janus MCP bootstrap scaffolding."""

from __future__ import annotations

from pathlib import Path

import click

from janus_code.mcp_server.bootstrap import bootstrap_project


@click.command()
@click.option(
    "--project",
    "-p",
    default=".",
    help="Project directory where bootstrap artifacts should be written.",
)
@click.option(
    "--runtime",
    type=click.Choice(["codex", "claude", "both"]),
    default="both",
    show_default=True,
    help="Which runtime-specific instruction assets to scaffold.",
)
@click.option(
    "--server-name",
    default="janus",
    show_default=True,
    help="MCP server key/name to register in `.mcp.json`.",
)
@click.option(
    "--transport",
    type=click.Choice(["stdio", "streamable-http"]),
    default="stdio",
    show_default=True,
    help="Transport mode for generated `.mcp.json` server entry.",
)
@click.option(
    "--http-url",
    default="http://127.0.0.1:8000/mcp",
    show_default=True,
    help="HTTP endpoint used when transport=streamable-http.",
)
@click.option(
    "--mcp-command",
    default="janus-code-mcp",
    show_default=True,
    help="Command used for stdio MCP server registration.",
)
@click.option(
    "--weak-snapshot",
    is_flag=True,
    help="Include weak snapshot flag in generated stdio server args.",
)
@click.option(
    "--disable-sqlite",
    is_flag=True,
    help="Include --disable-sqlite in generated stdio server args.",
)
@click.option(
    "--overwrite-server",
    is_flag=True,
    help="Overwrite existing `.mcp.json` entry for --server-name if present.",
)
def main(
    project: str,
    runtime: str,
    server_name: str,
    transport: str,
    http_url: str,
    mcp_command: str,
    weak_snapshot: bool,
    disable_sqlite: bool,
    overwrite_server: bool,
) -> None:
    """Scaffold Codex/Claude MCP onboarding assets for Janus."""
    project_dir = Path(project).resolve()
    if not project_dir.exists():
        raise click.ClickException(f"Project directory does not exist: {project_dir}")

    result = bootstrap_project(
        project_dir=project_dir,
        runtime=runtime,  # type: ignore[arg-type]
        server_name=server_name,
        transport=transport,  # type: ignore[arg-type]
        http_url=http_url,
        mcp_command=mcp_command,
        weak_snapshot=weak_snapshot,
        disable_sqlite=disable_sqlite,
        overwrite_server=overwrite_server,
    )

    if result.mcp_config_path is not None:
        click.echo(f"Updated MCP config: {result.mcp_config_path}")
    for path in result.written_files:
        click.echo(f"Wrote: {path}")
    for warning in result.warnings:
        click.echo(f"Warning: {warning}", err=True)


if __name__ == "__main__":
    main()
