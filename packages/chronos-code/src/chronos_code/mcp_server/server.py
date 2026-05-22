"""MCP server construction for Chronos runtime tools."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Literal

from chronos_code.mcp_server.runtime import ChronosMcpRuntime

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChronosMcpServeConfig:
    """Configuration for running the Chronos MCP server."""

    runtime: ChronosMcpRuntime
    transport: Literal["stdio", "streamable-http"] = "stdio"
    server_name: str = "chronos"
    host: str | None = None
    port: int | None = None
    path: str | None = None


def create_mcp_server(runtime: ChronosMcpRuntime, *, server_name: str = "chronos") -> Any:
    """Create a FastMCP server exposing Chronos transactional tools."""
    FastMCP = _load_fastmcp()
    mcp = FastMCP(server_name)

    @mcp.tool()
    def chronos_txn(action: str, session_id: str = "default", name: str | None = None) -> str:
        """Control Chronos transactions: begin, commit, abort, savepoint, rollback, status, changes."""
        return runtime.chronos_txn(action=action, session_id=session_id, name=name)

    @mcp.tool()
    def chronos_file_editor(
        command: str,
        path: str,
        session_id: str = "default",
        file_text: str | None = None,
        old_str: str | None = None,
        new_str: str | None = None,
        insert_line: int | None = None,
        view_range: list[int] | None = None,
    ) -> str:
        """Transactional file operations over Chronos OverlayFS."""
        return runtime.chronos_file_editor(
            command=command,
            path=path,
            session_id=session_id,
            file_text=file_text,
            old_str=old_str,
            new_str=new_str,
            insert_line=insert_line,
            view_range=view_range,
        )

    @mcp.tool()
    def chronos_bash(
        command: str,
        session_id: str = "default",
        timeout: int | None = None,
    ) -> str:
        """Execute a bash command in the session's transactional workspace."""
        return runtime.chronos_bash(command=command, session_id=session_id, timeout=timeout)

    @mcp.tool()
    def chronos_sqlite(
        command: str,
        session_id: str = "default",
        table: str | None = None,
        row: dict[str, Any] | None = None,
        pk_value: str | None = None,
        columns: list[str] | None = None,
        pk_column: str | None = None,
        filters: dict[str, Any] | None = None,
        order_by: str | None = None,
        limit: int | None = None,
        sql: str | None = None,
        params: list[Any] | None = None,
        seed_data: list[dict[str, Any]] | None = None,
    ) -> str:
        """Transactional SQLite operations under the Chronos 2PC transaction."""
        return runtime.chronos_sqlite(
            command=command,
            session_id=session_id,
            table=table,
            row=row,
            pk_value=pk_value,
            columns=columns,
            pk_column=pk_column,
            filters=filters,
            order_by=order_by,
            limit=limit,
            sql=sql,
            params=params,
            seed_data=seed_data,
        )

    @mcp.tool()
    def tar_sessions() -> str:
        """List active Chronos MCP logical session IDs."""
        sessions = runtime.list_sessions()
        if not sessions:
            return "No sessions yet."
        return "\n".join(sessions)

    @mcp.tool()
    def tar_close_session(session_id: str, abort_active: bool = True) -> str:
        """Close one session and optionally abort any active transaction first."""
        return runtime.close_session(session_id, abort_active=abort_active)

    return mcp


def run_mcp_server(config: ChronosMcpServeConfig) -> None:
    """Run the Chronos MCP server with the requested transport."""
    mcp = create_mcp_server(config.runtime, server_name=config.server_name)
    if config.transport == "stdio":
        mcp.run()
        return

    run_kwargs: dict[str, Any] = {}
    if config.host is not None:
        run_kwargs["host"] = config.host
    if config.port is not None:
        run_kwargs["port"] = config.port
    if config.path is not None:
        run_kwargs["path"] = config.path

    try:
        mcp.run(transport="streamable-http", **run_kwargs)
    except TypeError:
        if run_kwargs:
            logger.warning(
                "FastMCP transport kwargs unsupported by installed mcp version; "
                "falling back to defaults. attempted=%s",
                run_kwargs,
            )
        mcp.run(transport="streamable-http")


def _load_fastmcp() -> Any:
    try:
        from mcp.server.fastmcp import FastMCP
    except Exception as e:
        raise RuntimeError(
            "mcp package is required to run Chronos MCP server. "
            "Install with: pip install 'chronos-code[mcp]'"
        ) from e
    return FastMCP
