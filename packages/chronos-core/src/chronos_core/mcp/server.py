"""FastMCP server registration for Chronos core."""

from __future__ import annotations

import inspect
import logging
from typing import Any, Literal

from chronos_core.mcp.runtime import ChronosMcpRuntime

logger = logging.getLogger(__name__)


def create_mcp_server(runtime: ChronosMcpRuntime, *, server_name: str = "chronos") -> Any:
    """Create a FastMCP server exposing Chronos branch workspace tools."""
    FastMCP = _load_fastmcp()
    mcp = FastMCP(server_name)

    @mcp.tool()
    def chronos_status() -> dict[str, Any]:
        """Return configured Chronos stores, branches, and active mounts."""
        return runtime.status()

    @mcp.tool()
    def chronos_prepare_source(source_dir: str, branch_id: str = "main") -> dict[str, Any]:
        """Import a host source directory into a baseline ChronosFS branch."""
        return runtime.prepare_source(source_dir, branch_id)

    @mcp.tool()
    def chronos_create_sandbox(
        branch_id: str,
        from_branch: str = "main",
        mount: bool = True,
    ) -> dict[str, Any]:
        """Create a branch across configured stores and optionally mount ChronosFS."""
        return runtime.create_sandbox(branch_id, from_branch, mount=mount)

    @mcp.tool()
    def chronos_mount_branch(branch_id: str, mount_path: str | None = None) -> dict[str, Any]:
        """Mount a ChronosFS branch and return the POSIX mount path."""
        return runtime.mount_branch(branch_id, mount_path)

    @mcp.tool()
    def chronos_unmount_branch(branch_id: str) -> dict[str, Any]:
        """Unmount an MCP-managed ChronosFS branch mount."""
        return runtime.unmount_branch(branch_id)

    @mcp.tool()
    def chronos_delete_branch(branch_id: str, unmount: bool = True) -> dict[str, Any]:
        """Delete a branch across configured stores."""
        return runtime.delete_branch(branch_id, unmount=unmount)

    @mcp.tool()
    def chronos_sql_query(
        branch_id: str,
        store: str,
        sql: str,
        params: dict[str, Any] | None = None,
        limit: int = 1000,
    ) -> dict[str, Any]:
        """Run a branch-visible SQL query against a named Chronos SQL store."""
        return runtime.sql_query(branch_id, store, sql, params, limit=limit)

    @mcp.tool()
    def chronos_sql_execute(
        branch_id: str,
        store: str,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run branch-bound SQL DML, or store-level DDL, against a named SQL store."""
        return runtime.sql_execute(branch_id, store, sql, params)

    @mcp.tool()
    def chronos_register_table(
        store: str,
        table: str,
        primary_key: list[str],
    ) -> dict[str, Any]:
        """Register a logical SQL table for Chronos branching."""
        return runtime.register_table(store, table, primary_key)

    @mcp.tool()
    def chronos_diff(left: str, right: str) -> dict[str, Any]:
        """Return per-store diffs between two branches."""
        return runtime.diff(left, right)

    @mcp.tool()
    def chronos_merge_preview(
        source: str,
        target: str = "main",
        policy: Any = "manual_review",
    ) -> dict[str, Any]:
        """Preview a branch transaction commit across configured stores."""
        return runtime.merge_preview(source, target, policy=policy)

    @mcp.tool()
    def chronos_merge_apply(
        source: str,
        target: str = "main",
        policy: Any = "snapshot_isolation",
        resolution: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Apply a branch transaction commit across configured stores."""
        return runtime.merge_apply(source, target, policy=policy, resolution=resolution)

    @mcp.tool()
    def chronos_checkpoint(branch_id: str, checkpoint: str) -> dict[str, Any]:
        """Create a checkpoint across configured stores."""
        return runtime.checkpoint(branch_id, checkpoint)

    return mcp


def run_mcp_server(
    runtime: ChronosMcpRuntime,
    *,
    transport: Literal["stdio", "streamable-http"] = "stdio",
    server_name: str = "chronos",
    host: str | None = None,
    port: int | None = None,
    path: str | None = None,
) -> None:
    """Run the Chronos MCP server with the requested transport."""
    mcp = create_mcp_server(runtime, server_name=server_name)
    if transport == "stdio":
        mcp.run()
        return

    kwargs: dict[str, Any] = {}
    run_sig = inspect.signature(mcp.run)
    if "host" in run_sig.parameters and host is not None:
        kwargs["host"] = host
    if "port" in run_sig.parameters and port is not None:
        kwargs["port"] = port
    if "path" in run_sig.parameters and path is not None:
        kwargs["path"] = path
    try:
        mcp.run(transport="streamable-http", **kwargs)
    except TypeError:
        if kwargs:
            logger.warning(
                "installed mcp package does not accept server host/port/path kwargs; using defaults"
            )
        mcp.run(transport="streamable-http")


def _load_fastmcp() -> Any:
    try:
        from mcp.server.fastmcp import FastMCP
    except Exception as exc:
        raise RuntimeError(
            "mcp package is required to run Chronos MCP. "
            "Install with: pip install 'chronos-core[mcp]'"
        ) from exc
    return FastMCP
