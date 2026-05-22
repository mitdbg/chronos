"""SQLite tool adapter for Chronos-Code.

Exposes ChronosContext's transactional SQLite tool under the stable Chronos-Code
name ``sqlite`` so agent calls are routed through the same SQLiteShim used
by MessageStore.
"""

from __future__ import annotations

from typing import Any

from langchain_chronos.sqlite_tool import ChronosSQLiteInput


class SQLiteToolAdapter:
    """Lazy adapter for ChronosContext.sqlite following AgentLoop's run(**kwargs)."""

    name: str = "sqlite"
    description: str = (
        "Transactional SQLite database operations backed by SQLiteShim. "
        "Commands: register_table, put, get, query, delete, list_tables, sql."
    )
    args_schema: Any = ChronosSQLiteInput

    def __init__(self, chronos_context: Any) -> None:
        self._chronos_context = chronos_context

    def run(self, **kwargs: Any) -> str:
        """Execute a sqlite command using the active Chronos transaction."""
        try:
            sqlite_tool = self._chronos_context.sqlite
        except Exception as e:
            return f"Error: sqlite tool unavailable: {e}"
        return sqlite_tool.run(kwargs)

