"""Command-line entrypoint for the Chronos core MCP server."""

from __future__ import annotations

import argparse
import logging
from typing import Sequence

from chronos_core.mcp.config import ChronosMcpConfigError, load_chronos_mcp_config
from chronos_core.mcp.runtime import ChronosMcpRuntime
from chronos_core.mcp.server import run_mcp_server


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run Chronos core as an MCP server.")
    parser.add_argument(
        "--config",
        default="chronos.toml",
        help="Path to chronos.toml (default: chronos.toml).",
    )
    parser.add_argument(
        "--transport",
        choices=("stdio", "streamable-http"),
        default="stdio",
        help="MCP transport to use.",
    )
    parser.add_argument("--server-name", default="chronos", help="MCP server name.")
    parser.add_argument("--host", default=None, help="Host for streamable-http transport.")
    parser.add_argument("--port", type=int, default=None, help="Port for streamable-http transport.")
    parser.add_argument("--path", default=None, help="Path for streamable-http transport.")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging.")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    try:
        config = load_chronos_mcp_config(args.config)
    except ChronosMcpConfigError as exc:
        raise SystemExit(f"chronos-mcp config error: {exc}") from exc

    runtime = ChronosMcpRuntime(config)
    try:
        run_mcp_server(
            runtime,
            transport=args.transport,
            server_name=args.server_name,
            host=args.host,
            port=args.port,
            path=args.path,
        )
    finally:
        runtime.close()


if __name__ == "__main__":
    main()
