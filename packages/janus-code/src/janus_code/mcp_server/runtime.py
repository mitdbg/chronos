"""Runtime bridge between MCP tool calls and Janus transactional tools."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


ContextFactory = Callable[..., Any]


@dataclass(frozen=True)
class JanusMcpRuntimeConfig:
    """Configuration for MCP-backed Janus runtime sessions."""

    project_dir: Path
    db_path: str | None = None
    weak_snapshot: bool = False
    enable_sqlite: bool = True
    enable_vectorstore: bool = False
    enforced_session_id: str | None = None


class JanusMcpRuntime:
    """Manages per-session Janus contexts for MCP tools.

    A "session" here is a logical agent identity provided by MCP tool
    arguments. Each session gets its own JanusContext instance and transaction
    lifecycle.
    """

    def __init__(
        self,
        config: JanusMcpRuntimeConfig,
        *,
        context_factory: ContextFactory | None = None,
    ) -> None:
        self._project_dir = Path(config.project_dir).resolve()
        self._state_dir = self._project_dir / ".janus-code"
        self._state_dir.mkdir(parents=True, exist_ok=True)

        self._db_path = config.db_path or str(self._state_dir / "mcp.sqlite")
        self._weak_snapshot = bool(config.weak_snapshot)
        self._enable_sqlite = bool(config.enable_sqlite)
        self._enable_vectorstore = bool(config.enable_vectorstore)
        self._enforced_session_id = (
            self._normalize_session_id(config.enforced_session_id)
            if config.enforced_session_id
            else None
        )
        self._context_factory = context_factory or self._default_context_factory
        self._contexts: dict[str, Any] = {}

    @property
    def project_dir(self) -> Path:
        return self._project_dir

    @property
    def db_path(self) -> str:
        return self._db_path

    def list_sessions(self) -> list[str]:
        """Return known session ids (stable sorted order)."""
        return sorted(self._contexts.keys())

    def close_session(self, session_id: str, *, abort_active: bool = True) -> str:
        """Remove one session and optionally abort an active transaction first."""
        try:
            sid = self._effective_session_id(session_id, allow_default_alias=False)
            ctx = self._contexts.get(sid)
            if ctx is None:
                return f"Session '{sid}' not found."
            if abort_active and bool(getattr(ctx, "is_active", False)):
                ctx.abort()
            self._contexts.pop(sid, None)
            return f"Session '{sid}' closed."
        except Exception as e:
            return f"Error: {e}"

    def janus_txn(
        self,
        *,
        action: str,
        session_id: str = "default",
        name: str | None = None,
    ) -> str:
        """Control Janus transaction lifecycle for one logical session."""
        try:
            sid = self._effective_session_id(session_id)
            ctx = self._context_for(sid)
            if action == "begin":
                if bool(getattr(ctx, "is_active", False)):
                    txn = getattr(ctx, "txn", None)
                    txn_id = getattr(txn, "id", "?")
                    return f"Transaction already active (id={txn_id})."
                txn = ctx.begin()
                return (
                    f"Transaction started (session={sid}, id={txn.id}). "
                    "Use janus_txn(action='commit') to apply or janus_txn(action='abort') to discard."
                )

            if action == "commit":
                self._ensure_active(ctx, sid)
                ctx.commit()
                return f"Transaction committed (session={sid})."

            if action == "abort":
                self._ensure_active(ctx, sid)
                ctx.abort()
                return f"Transaction aborted (session={sid})."

            if action == "savepoint":
                self._ensure_active(ctx, sid)
                if not name:
                    return "Error: 'name' is required for savepoint."
                ctx.savepoint(name)
                return f"Savepoint '{name}' created (session={sid})."

            if action == "rollback":
                self._ensure_active(ctx, sid)
                ctx.rollback(name)
                return (
                    f"Rolled back savepoint"
                    + (f" '{name}'" if name else "")
                    + f" (session={sid})."
                )

            if action == "status":
                return self._status(ctx, sid)

            if action == "changes":
                return self._changes(ctx, sid)

            return (
                f"Error: Unknown action '{action}'. "
                "Supported actions: begin, commit, abort, savepoint, rollback, status, changes."
            )
        except Exception as e:
            return f"Error: {e}"

    def janus_file_editor(
        self,
        *,
        command: str,
        path: str,
        session_id: str = "default",
        file_text: str | None = None,
        old_str: str | None = None,
        new_str: str | None = None,
        insert_line: int | None = None,
        view_range: list[int] | None = None,
    ) -> str:
        """Execute a Janus file editor command against the session overlay."""
        try:
            sid = self._effective_session_id(session_id)
            ctx = self._active_context(sid)
            payload: dict[str, Any] = {
                "command": command,
                "path": path,
            }
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
            return self._invoke_tool(ctx.file_editor, payload)
        except Exception as e:
            return f"Error: {e}"

    def janus_bash(
        self,
        *,
        command: str,
        session_id: str = "default",
        timeout: int | None = None,
    ) -> str:
        """Execute a bash command against the session overlay."""
        try:
            sid = self._effective_session_id(session_id)
            ctx = self._active_context(sid)
            payload: dict[str, Any] = {"command": command}
            if timeout is not None:
                payload["timeout"] = timeout
            return self._invoke_tool(ctx.bash, payload)
        except Exception as e:
            return f"Error: {e}"

    def janus_sqlite(
        self,
        *,
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
        """Execute Janus SQLite command within the active transaction."""
        try:
            sid = self._effective_session_id(session_id)
            ctx = self._active_context(sid)
            sqlite_tool = getattr(ctx, "sqlite", None)
            if sqlite_tool is None:
                return "Error: janus_sqlite is disabled for this runtime."

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
            return self._invoke_tool(sqlite_tool, payload)
        except Exception as e:
            return f"Error: {e}"

    def _context_for(self, session_id: str) -> Any:
        ctx = self._contexts.get(session_id)
        if ctx is not None:
            return ctx
        ctx = self._context_factory(
            self._project_dir,
            db_path=self._db_path,
            enable_sqlite=self._enable_sqlite,
            enable_vectorstore=self._enable_vectorstore,
            weak_snapshot=self._weak_snapshot,
        )
        self._contexts[session_id] = ctx
        return ctx

    def _active_context(self, session_id: str) -> Any:
        ctx = self._context_for(session_id)
        self._ensure_active(ctx, session_id)
        return ctx

    @staticmethod
    def _invoke_tool(tool: Any, payload: dict[str, Any]) -> str:
        if hasattr(tool, "invoke"):
            result = tool.invoke(payload)
        elif hasattr(tool, "run"):
            result = tool.run(payload)
        else:
            result = tool._run(**payload)
        return result if isinstance(result, str) else str(result)

    @staticmethod
    def _normalize_session_id(session_id: str | None) -> str:
        if session_id is None:
            return "default"
        sid = session_id.strip()
        return sid or "default"

    def _effective_session_id(
        self,
        session_id: str | None,
        *,
        allow_default_alias: bool = True,
    ) -> str:
        requested = self._normalize_session_id(session_id)
        enforced = self._enforced_session_id
        if not enforced:
            return requested
        if allow_default_alias and requested == "default":
            return enforced
        if requested != enforced:
            raise RuntimeError(
                f"Session id is enforced as '{enforced}' for this MCP server; got '{requested}'."
            )
        return requested

    @staticmethod
    def _ensure_active(ctx: Any, session_id: str) -> None:
        if bool(getattr(ctx, "is_active", False)):
            return
        raise RuntimeError(
            f"No active transaction for session '{session_id}'. "
            "Call janus_txn(action='begin') first."
        )

    def _status(self, ctx: Any, session_id: str) -> str:
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

    def _changes(self, ctx: Any, session_id: str) -> str:
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

    @staticmethod
    def _default_context_factory(base_path: Path, **kwargs: Any) -> Any:
        from langchain_janus import JanusContext

        return JanusContext(base_path, **kwargs)
