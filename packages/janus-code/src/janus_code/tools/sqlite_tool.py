"""SQLite tool adapter for Janus-Code.

Exposes JanusContext's transactional SQLite tool under the stable Janus-Code
name ``sqlite`` so agent calls are routed through the same SQLiteShim used
by MessageStore.
"""

from __future__ import annotations

from typing import Any

from langchain_janus.sqlite_tool import JanusSQLiteInput


class SQLiteToolAdapter:
    """Lazy adapter for JanusContext.sqlite following AgentLoop's run(**kwargs)."""

    name: str = "sqlite"
    description: str = (
        "Transactional SQLite database operations backed by SQLiteShim. "
        "Commands: register_table, put, get, query, delete, list_tables, sql."
    )
    args_schema: Any = JanusSQLiteInput

    def __init__(self, janus_context: Any) -> None:
        self._janus_context = janus_context

    def run(self, **kwargs: Any) -> str:
        """Execute a sqlite command using the active Janus transaction."""
        try:
            sqlite_tool = self._janus_context.sqlite
        except Exception as e:
            return f"Error: sqlite tool unavailable: {e}"
        return sqlite_tool.run(kwargs)

