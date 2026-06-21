"""Tool registry — builds and registers tools for the agent.

The runtime always uses the standard Chronos-Code tool names
(`read_file`, `write_file`, `edit_file`, `bash`, `memory`, etc.).
When a ChronosContext transaction is active, these tools are pointed at
``chronos_context.working_dir`` so they operate inside Chronos's OverlayFS
workspace. Outside an active transaction, they point at ``working_dir``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable


class ChronosToolAdapter:
    """Adapts a LangChain BaseTool to the AgentLoop tool protocol.

    AgentLoop calls ``tool.run(**kwargs)``, but LangChain BaseTool's
    ``run()`` takes a single positional string or dict argument.
    This adapter bridges the two conventions.
    """

    def __init__(self, tar_tool: Any) -> None:
        self._tool = tar_tool
        self.name: str = tar_tool.name
        self.description: str = tar_tool.description
        self.args_schema: Any = getattr(tar_tool, "args_schema", None)

    def run(self, **kwargs: Any) -> str:
        """Call the wrapped LangChain tool with kwargs as a dict."""
        return self._tool.run(kwargs)


def build_tools(
    working_dir: Path,
    config: Any,
    chronos_context: Any = None,
    prompt_fn: Callable | None = None,
    task_launcher: Any = None,
    todo_listener: Callable | None = None,
    expose_txn_control: bool = False,
    expose_ask_user: bool = True,
) -> dict[str, Any]:
    """Create all tool instances for Chronos-Code.

    Args:
        working_dir: The project root directory.
        config: Runtime configuration.
        chronos_context: Optional ChronosContext. When provided and active, all
            filesystem/shell tools are rooted in the active overlay workdir.
        prompt_fn: Callback for the AskUser tool (interactive input).
        task_launcher: Callback for the Task tool (sub-agent launch).
        todo_listener: Callback when todos change (for display).
        expose_txn_control: When True, expose ``chronos_txn`` tool for manual
            transaction control (primarily debug/testing). Defaults to False
            so the orchestrator remains the single owner of txn boundaries.
        expose_ask_user: When False, omit the blocking ask-user tool. CLI
            subprocess/non-TTY runs use this so EOF cannot derail a turn.

    Returns:
        Dictionary of tool_name → tool instance.
    """
    from chronos_code.tools.read_file import ReadFileTool
    from chronos_code.tools.write_file import WriteFileTool
    from chronos_code.tools.edit_file import EditFileTool
    from chronos_code.tools.glob_tool import GlobTool
    from chronos_code.tools.ripgrep_tool import RipGrepTool
    from chronos_code.tools.bash_tool import BashTool
    from chronos_code.tools.todo_write import TodoWriteTool
    from chronos_code.tools.ask_user import AskUserTool
    from chronos_code.tools.memory_tool import MemoryTool
    from chronos_code.tools.sqlite_tool import SQLiteToolAdapter
    from chronos_code.tools.task_tool import TaskTool

    tools: dict[str, Any] = {}

    def _register_tool(primary_name: str, tool: Any, *aliases: str) -> None:
        tools[primary_name] = tool
        for alias in aliases:
            tools[alias] = tool

    workdir = working_dir
    if chronos_context is not None and chronos_context.is_active:
        workdir = chronos_context.working_dir

    # Core filesystem/shell tools always use the standard Chronos-Code names.
    _register_tool("read_file", ReadFileTool(working_dir=workdir))
    _register_tool("write_file", WriteFileTool(working_dir=workdir))
    _register_tool("edit_file", EditFileTool(working_dir=workdir))
    glob_tool = GlobTool(working_dir=workdir)
    _register_tool("glob_search", glob_tool, "glob")
    _register_tool("ripgrep", RipGrepTool(working_dir=workdir))
    _register_tool(
        "bash",
        BashTool(
            working_dir=workdir,
            timeout=config.bash_timeout,
        ),
    )
    _register_tool("memory", MemoryTool(working_dir=workdir))
    if chronos_context is not None and getattr(chronos_context, "_sqlite_shim", None) is not None:
        sqlite_tool = SQLiteToolAdapter(chronos_context=chronos_context)
        _register_tool("sqlite", sqlite_tool, "chronos_sqlite")

    # Optional transaction control tool for debug/testing only.
    if chronos_context is not None and expose_txn_control:
        from langchain_chronos.context import ChronosTransactionControl

        txn_ctl = ChronosTransactionControl(chronos_context=chronos_context)
        _register_tool("chronos_txn", ChronosToolAdapter(txn_ctl))

    # ── Tools that are not filesystem-dependent ───────────────────────
    todo = TodoWriteTool()
    if todo_listener:
        todo.add_listener(todo_listener)
    _register_tool("todo_write", todo)

    if expose_ask_user:
        _register_tool("ask_user", AskUserTool(prompt_fn=prompt_fn))
    task_tool = TaskTool(launcher=task_launcher)
    _register_tool("launch_task", task_tool, "task")

    return tools


def build_tool_schemas(tools: dict[str, Any]) -> list[dict[str, Any]]:
    """Generate OpenAI-format tool schemas from tool instances.

    Inspects each tool for:
    1. A ``schema()`` method (highest priority)
    2. ``args_schema`` (Pydantic model) + ``name`` + ``description``
    3. Minimal fallback schema

    Returns a list of OpenAI tool schema dicts.
    """
    schemas: list[dict[str, Any]] = []
    seen_names: set[str] = set()

    for tool_name, tool in tools.items():
        # Option 1: tool has a schema() method
        if hasattr(tool, "schema") and callable(tool.schema):
            schema = tool.schema()
            fn_name = schema.get("function", {}).get("name")
            if fn_name and fn_name in seen_names:
                continue
            if fn_name:
                seen_names.add(fn_name)
            schemas.append(schema)
            continue

        # Option 2: Pydantic args_schema
        name = getattr(tool, "name", tool_name)
        if name in seen_names:
            continue
        seen_names.add(name)
        desc = getattr(tool, "description", f"Tool: {name}")

        if hasattr(tool, "args_schema") and tool.args_schema is not None:
            model = tool.args_schema
            if hasattr(model, "model_json_schema"):
                # Pydantic v2
                params = model.model_json_schema()
            elif hasattr(model, "schema"):
                # Pydantic v1
                params = model.schema()
            else:
                params = {"type": "object", "properties": {}}

            # Clean up pydantic schema for OpenAI format
            params.pop("title", None)
            for prop in params.get("properties", {}).values():
                prop.pop("title", None)

            schemas.append({
                "type": "function",
                "function": {
                    "name": name,
                    "description": desc,
                    "parameters": params,
                },
            })
            continue

        # Option 3: Minimal fallback
        schemas.append({
            "type": "function",
            "function": {
                "name": name,
                "description": desc,
                "parameters": {"type": "object", "properties": {}},
            },
        })

    return schemas
