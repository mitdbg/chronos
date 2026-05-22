"""CLI entrypoint for running Janus as an MCP server."""

from __future__ import annotations

import logging
from pathlib import Path

import click

from janus_code.mcp_server.runtime import JanusMcpRuntime, JanusMcpRuntimeConfig
from janus_code.mcp_server.server import JanusMcpServeConfig, run_mcp_server


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )


@click.command()
@click.option(
    "--project",
    "-p",
    default=".",
    help="Project directory for Janus OverlayFS base (default: current directory).",
)
@click.option(
    "--db-path",
    default=None,
    help="SQLite database path for Janus transactional state (default: .janus-code/mcp.sqlite).",
)
@click.option(
    "--transport",
    type=click.Choice(["stdio", "streamable-http"]),
    default="stdio",
    show_default=True,
    help="MCP transport mode.",
)
@click.option("--host", default=None, help="Host for streamable-http transport.")
@click.option("--port", type=int, default=None, help="Port for streamable-http transport.")
@click.option("--path", default=None, help="Path for streamable-http transport (if supported).")
@click.option(
    "--server-name",
    default="janus",
    show_default=True,
    help="Exposed MCP server name.",
)
@click.option(
    "--weak-snapshot",
    is_flag=True,
    help=(
        "Enable weak snapshot isolation (disable eager conflict checks while keeping snapshot reads)."
    ),
)
@click.option(
    "--disable-sqlite",
    is_flag=True,
    help="Disable janus_sqlite tool exposure.",
)
@click.option(
    "--enforced-session-id",
    default=None,
    help=(
        "If set, this server only accepts this Janus session_id (and maps default "
        "session alias to it). Useful for per-worker branch isolation."
    ),
)
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging.")
def main(
    project: str,
    db_path: str | None,
    transport: str,
    host: str | None,
    port: int | None,
    path: str | None,
    server_name: str,
    weak_snapshot: bool,
    disable_sqlite: bool,
    enforced_session_id: str | None,
    verbose: bool,
) -> None:
    """Run Janus as an MCP server for Codex/Claude-compatible clients."""
    _setup_logging(verbose)

    project_dir = Path(project).resolve()
    if not project_dir.exists():
        raise click.ClickException(f"Project directory does not exist: {project_dir}")

    runtime = JanusMcpRuntime(
        JanusMcpRuntimeConfig(
            project_dir=project_dir,
            db_path=db_path,
            weak_snapshot=weak_snapshot,
            enable_sqlite=not disable_sqlite,
            enable_vectorstore=False,
            enforced_session_id=enforced_session_id,
        )
    )
    run_mcp_server(
        JanusMcpServeConfig(
            runtime=runtime,
            transport=transport,  # type: ignore[arg-type]
            server_name=server_name,
            host=host,
            port=port,
            path=path,
        )
    )


if __name__ == "__main__":
    main()
