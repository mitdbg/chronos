"""Embedded Janus MCP server bound to orchestrator-managed transactions."""

from __future__ import annotations

import json
import logging
import inspect
import threading
import time
from typing import Any, Callable

logger = logging.getLogger(__name__)
EventSink = Callable[[dict[str, Any]], None] | Callable[..., None]


class EmbeddedJanusMcpServer:
    """Runs Janus MCP server in-process and routes calls to registered contexts.

    This server is designed for unmanaged external workers (Codex/Claude).
    Workers connect over streamable-http, while all operations execute against
    JanusContext instances that are already owned by Janus-code orchestrator.
    """

    def __init__(
        self,
        *,
        server_name: str = "janus",
        host: str = "127.0.0.1",
        port: int = 8765,
        path: str = "/mcp",
        event_sink: EventSink | None = None,
    ) -> None:
        self._server_name = server_name
        self._host = host
        self._port = int(port)
        self._path = path if path.startswith("/") else f"/{path}"
        self._effective_host = self._host
        self._effective_port = self._port
        self._effective_path = self._path
        self._contexts: dict[str, Any] = {}
        self._lock = threading.Lock()
        self._started = False
        self._start_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._event_sink = event_sink

    @property
    def url(self) -> str:
        return (
            f"http://{self._effective_host}:"
            f"{self._effective_port}{self._effective_path}"
        )

    def register_session(self, session_id: str, janus_context: Any) -> None:
        with self._lock:
            self._contexts[session_id] = janus_context
            count = len(self._contexts)
        self._emit_event(
            "mcp_session_registered",
            "Registered MCP session.",
            session_id=session_id,
            session_count=count,
        )

    def unregister_session(self, session_id: str) -> None:
        with self._lock:
            self._contexts.pop(session_id, None)
            count = len(self._contexts)
        self._emit_event(
            "mcp_session_unregistered",
            "Unregistered MCP session.",
            session_id=session_id,
            session_count=count,
        )

    def ensure_started(self) -> None:
        with self._start_lock:
            if self._started:
                return
            self._emit_event(
                "mcp_server_starting",
                "Starting embedded MCP server.",
                host=self._host,
                port=self._port,
                path=self._path,
            )
            try:
                from mcp.server.fastmcp import FastMCP
            except Exception as e:
                raise RuntimeError(
                    "mcp package is required for embedded worker runtime. "
                    "Install with: pip install 'janus-code[mcp]'"
                ) from e

            init_sig = inspect.signature(FastMCP.__init__)
            init_params = set(init_sig.parameters.keys())
            ctor_supports_host = "host" in init_params and "port" in init_params
            ctor_path_arg = (
                "streamable_http_path"
                if "streamable_http_path" in init_params
                else ("path" if "path" in init_params else None)
            )
            ctor_supports_http_args = ctor_supports_host and ctor_path_arg is not None
            ctor_kwargs: dict[str, Any] = {}
            if ctor_supports_http_args:
                ctor_kwargs = {"host": self._host, "port": self._port}
                if ctor_path_arg is not None:
                    ctor_kwargs[ctor_path_arg] = self._path
            mcp = FastMCP(self._server_name, **ctor_kwargs)

            @mcp.tool()
            def janus_txn(
                action: str,
                session_id: str = "default",
                name: str | None = None,
            ) -> str:
                payload: dict[str, Any] = {"action": action}
                if name is not None:
                    payload["name"] = name

                def _execute() -> str:
                    ctx = self._get_context(session_id)
                    if action == "status":
                        return self._status(ctx, session_id)
                    if action == "changes":
                        return self._changes(ctx, session_id)
                    if action == "begin":
                        if bool(getattr(ctx, "is_active", False)):
                            return (
                                "Transaction already active and managed by orchestrator "
                                f"(session={session_id})."
                            )
                        return (
                            "No active transaction for this session yet. "
                            "Waiting for orchestrator to open the branch transaction."
                        )
                    if action in {"commit", "abort", "savepoint", "rollback"}:
                        suffix = f" '{name}'" if name else ""
                        return (
                            f"janus_txn action '{action}{suffix}' acknowledged as no-op in embedded mode. "
                            "Transaction boundaries are controlled by Janus-code orchestrator."
                        )
                    return (
                        f"Error: Unknown action '{action}'. "
                        "Supported actions: begin, commit, abort, savepoint, rollback, status, changes."
                    )

                return self._invoke_with_trace(
                    tool_name="janus_txn",
                    session_id=session_id,
                    payload=payload,
                    invoke_fn=_execute,
                )

            @mcp.tool()
            def janus_file_editor(
                command: str,
                path: str,
                session_id: str = "default",
                file_text: str | None = None,
                old_str: str | None = None,
                new_str: str | None = None,
                insert_line: int | None = None,
                view_range: list[int] | None = None,
            ) -> str:
                payload: dict[str, Any] = {"command": command, "path": path}
                if file_text is not None:
                    payload["file_text"] = file_text
                if old_str is not None:
                    payload["old_str"] = old_str
                if new_str is not None:
                    payload["new_str"] = new_str
                if insert_line is not None:
                    payload["insert_line"] = insert_line
                if view_range is not None:
                    payload["view_range"] = view_range

                def _execute() -> str:
                    ctx = self._get_active_context(session_id)
                    return self._invoke_tool(ctx.file_editor, payload)

                return self._invoke_with_trace(
                    tool_name="janus_file_editor",
                    session_id=session_id,
                    payload=payload,
                    invoke_fn=_execute,
                )

            @mcp.tool()
            def janus_bash(
                command: str,
                session_id: str = "default",
                timeout: int | None = None,
            ) -> str:
                payload: dict[str, Any] = {"command": command}
                if timeout is not None:
                    payload["timeout"] = timeout

                def _execute() -> str:
                    ctx = self._get_active_context(session_id)
                    return self._invoke_tool(ctx.bash, payload)

                return self._invoke_with_trace(
                    tool_name="janus_bash",
                    session_id=session_id,
                    payload=payload,
                    invoke_fn=_execute,
                )

            @mcp.tool()
            def janus_sqlite(
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
                payload: dict[str, Any] = {"command": command}
                if table is not None:
                    payload["table"] = table
                if row is not None:
                    payload["row"] = row
                if pk_value is not None:
                    payload["pk_value"] = pk_value
                if columns is not None:
                    payload["columns"] = columns
                if pk_column is not None:
                    payload["pk_column"] = pk_column
                if filters is not None:
                    payload["filters"] = filters
                if order_by is not None:
                    payload["order_by"] = order_by
                if limit is not None:
                    payload["limit"] = limit
                if sql is not None:
                    payload["sql"] = sql
                if params is not None:
                    payload["params"] = params
                if seed_data is not None:
                    payload["seed_data"] = seed_data

                def _execute() -> str:
                    ctx = self._get_active_context(session_id)
                    sqlite_tool = getattr(ctx, "sqlite", None)
                    if sqlite_tool is None:
                        return "Error: janus_sqlite is disabled for this runtime."
                    return self._invoke_tool(sqlite_tool, payload)

                return self._invoke_with_trace(
                    tool_name="janus_sqlite",
                    session_id=session_id,
                    payload=payload,
                    invoke_fn=_execute,
                )

            @mcp.tool()
            def tar_sessions() -> str:
                def _execute() -> str:
                    with self._lock:
                        sessions = sorted(self._contexts.keys())
                    if not sessions:
                        return "No sessions registered."
                    return "\n".join(sessions)

                return self._invoke_with_trace(
                    tool_name="tar_sessions",
                    session_id="default",
                    payload={},
                    invoke_fn=_execute,
                )

            @mcp.tool()
            def tar_close_session(session_id: str, abort_active: bool = True) -> str:
                payload = {"abort_active": bool(abort_active)}

                def _execute() -> str:
                    ctx = self._get_context(session_id)
                    if abort_active and bool(getattr(ctx, "is_active", False)):
                        return (
                            "Error: Embedded MCP cannot abort orchestrator-managed transactions. "
                            "Close session from orchestrator."
                        )
                    self.unregister_session(session_id)
                    return f"Session '{session_id}' closed."

                return self._invoke_with_trace(
                    tool_name="tar_close_session",
                    session_id=session_id,
                    payload=payload,
                    invoke_fn=_execute,
                )

            run_sig = inspect.signature(mcp.run)
            has_var_kw = any(
                p.kind == inspect.Parameter.VAR_KEYWORD
                for p in run_sig.parameters.values()
            )
            run_supports_host_args = has_var_kw or {
                "host",
                "port",
                "path",
            }.issubset(set(run_sig.parameters.keys()))
            supports_custom_http = run_supports_host_args or ctor_supports_http_args

            if supports_custom_http:
                self._effective_host = self._host
                self._effective_port = self._port
                self._effective_path = self._path
            else:
                self._effective_host = "127.0.0.1"
                self._effective_port = 8000
                self._effective_path = "/mcp"
                logger.warning(
                    "Installed mcp version does not accept host/port/path kwargs; "
                    "using default streamable-http endpoint %s",
                    self.url,
                )
                self._emit_event(
                    "mcp_server_compat_fallback",
                    "Embedded MCP host/port/path unsupported by installed mcp; using default endpoint.",
                    url=self.url,
                )

            def _serve() -> None:
                if run_supports_host_args:
                    mcp.run(
                        transport="streamable-http",
                        host=self._host,
                        port=self._port,
                        path=self._path,
                    )
                else:
                    mcp.run(transport="streamable-http")

            thread = threading.Thread(
                target=_serve,
                name="janus-embedded-mcp",
                daemon=True,
            )
            thread.start()
            self._thread = thread
            self._started = True
            self._emit_event(
                "mcp_server_started",
                "Embedded MCP server started.",
                url=self.url,
            )

    def _get_context(self, session_id: str) -> Any:
        sid = (session_id or "default").strip() or "default"
        with self._lock:
            ctx = self._contexts.get(sid)
        if ctx is None:
            raise RuntimeError(
                f"Unknown Janus MCP session '{sid}'. "
                "Session was not registered by orchestrator."
            )
        return ctx

    def _get_active_context(self, session_id: str) -> Any:
        ctx = self._get_context(session_id)
        if bool(getattr(ctx, "is_active", False)):
            return ctx
        raise RuntimeError(
            f"No active transaction for session '{session_id}'. "
            "Transaction lifecycle is managed by orchestrator."
        )

    @staticmethod
    def _invoke_tool(tool: Any, payload: dict[str, Any]) -> str:
        if hasattr(tool, "invoke"):
            result = tool.invoke(payload)
        elif hasattr(tool, "run"):
            result = tool.run(payload)
        else:
            result = tool._run(**payload)
        return result if isinstance(result, str) else str(result)

    def _invoke_with_trace(
        self,
        *,
        tool_name: str,
        session_id: str,
        payload: dict[str, Any],
        invoke_fn: Callable[[], str],
    ) -> str:
        sid = (session_id or "default").strip() or "default"
        self._emit_event(
            "mcp_tool_call",
            f"MCP tool `{tool_name}` called.",
            tool=tool_name,
            session_id=sid,
            args=self._clip_json(payload, max_chars=220),
        )
        started = time.monotonic()
        try:
            result = invoke_fn()
        except Exception as e:
            self._emit_event(
                "mcp_tool_error",
                f"MCP tool `{tool_name}` failed.",
                tool=tool_name,
                session_id=sid,
                duration_ms=int((time.monotonic() - started) * 1000),
                error=self._clip(str(e), max_chars=220),
            )
            raise
        value = result if isinstance(result, str) else str(result)
        self._emit_event(
            "mcp_tool_result",
            f"MCP tool `{tool_name}` completed.",
            tool=tool_name,
            session_id=sid,
            duration_ms=int((time.monotonic() - started) * 1000),
            result=self._clip(value, max_chars=220),
        )
        return value

    def _emit_event(self, event_type: str, message: str, **details: Any) -> None:
        sink = self._event_sink
        if sink is None:
            return
        payload = {"type": event_type, "message": message, **details}
        try:
            sink(payload)
        except TypeError:
            sink(event_type, message, **details)
        except Exception:
            pass

    @staticmethod
    def _clip(text: str, max_chars: int = 180) -> str:
        value = (text or "").strip().replace("\n", " ")
        if len(value) <= max_chars:
            return value
        return value[: max_chars - 3] + "..."

    @staticmethod
    def _clip_json(payload: dict[str, Any], max_chars: int = 180) -> str:
        try:
            rendered = json.dumps(payload, ensure_ascii=True, default=str)
        except Exception:
            rendered = str(payload)
        return EmbeddedJanusMcpServer._clip(rendered, max_chars=max_chars)

    @staticmethod
    def _status(ctx: Any, session_id: str) -> str:
        if not bool(getattr(ctx, "is_active", False)):
            return f"No active transaction (session={session_id})."
        txn = getattr(ctx, "txn", None)
        txn_id = getattr(txn, "id", "?")
        workdir = getattr(ctx, "working_dir", None)
        changes = getattr(ctx, "get_changes", lambda: [])()
        return (
            f"Transaction active (session={session_id}, id={txn_id})\n"
            f"  Working directory: {workdir}\n"
            f"  Changes: {len(changes)}"
        )

    @staticmethod
    def _changes(ctx: Any, session_id: str) -> str:
        if not bool(getattr(ctx, "is_active", False)):
            return f"No active transaction (session={session_id})."
        changes = getattr(ctx, "get_changes", lambda: [])()
        if not changes:
            return f"No changes made yet (session={session_id})."
        lines = [f"Changes in current transaction (session={session_id}):"]
        for ch in changes:
            ctype = getattr(getattr(ch, "change_type", None), "value", None)
            if ctype is None:
                ctype = getattr(ch, "change_type", "change")
            resource_id = getattr(ch, "resource_id", "<unknown>")
            lines.append(f"  [{ctype}] {resource_id}")
        return "\n".join(lines)
