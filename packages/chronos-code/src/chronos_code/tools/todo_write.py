"""TodoWrite tool — structured task tracking with SQLite persistence."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, Field


class TodoItem(BaseModel):
    """A single todo item."""

    id: str = Field(description="Unique identifier for the todo.")
    title: str = Field(description="Concise action-oriented label (3-7 words).")
    status: Literal["pending", "in_progress", "completed"] = Field(
        default="pending",
        description="Current status of the todo.",
    )


class TodoWriteInput(BaseModel):
    """Input schema for the TodoWrite tool."""

    todos: list[TodoItem] = Field(
        description=(
            "Complete list of all todo items. Must include ALL items — "
            "both existing and new. States: pending, in_progress, completed."
        )
    )


class TodoWriteTool:
    """Structured task tracking.

    - Maintains an ordered list of todos with status tracking.
    - At most one todo can be ``in_progress`` at a time.
    - State can be persisted to SQLite via the message store.

    The tool keeps an in-memory list that the display layer reads.
    Persistence to SQLite happens through the context management layer.
    """

    name: str = "todo_write"
    description: str = (
        "Create and manage a structured task list. Track progress on "
        "complex multi-step tasks. Max 1 item in_progress at a time."
    )
    args_schema = TodoWriteInput

    def __init__(self) -> None:
        self._todos: list[dict[str, Any]] = []
        self._on_update: list[Any] = []  # callbacks

    @property
    def todos(self) -> list[dict[str, Any]]:
        """Current todo list (read-only copy)."""
        return [t.copy() for t in self._todos]

    def add_listener(self, callback: Any) -> None:
        """Register a callback for todo updates."""
        self._on_update.append(callback)

    def _notify(self) -> None:
        for cb in self._on_update:
            try:
                cb(self._todos)
            except Exception:
                pass

    def run(self, todos: list[dict[str, Any]] | list[TodoItem]) -> str:
        """Execute the todo_write tool."""
        # Normalize input
        items: list[dict[str, Any]] = []
        for t in todos:
            if isinstance(t, TodoItem):
                items.append(t.model_dump())
            elif isinstance(t, dict):
                items.append(t)
            else:
                return f"Error: invalid todo item: {t}"

        # Validate: max 1 in_progress
        in_progress_count = sum(
            1 for item in items if item.get("status") == "in_progress"
        )
        if in_progress_count > 1:
            return "Error: at most 1 todo can be in_progress at a time."

        # Validate required fields
        for item in items:
            if "id" not in item or "title" not in item:
                return f"Error: each todo must have 'id' and 'title'. Got: {item}"
            item.setdefault("status", "pending")
            item["updated_at"] = time.time()

        self._todos = items
        self._notify()

        return self._format_todos()

    def _format_todos(self) -> str:
        """Format the todo list for display."""
        if not self._todos:
            return "Todo list is empty."

        status_icons = {
            "pending": "○",
            "in_progress": "◉",
            "completed": "✓",
        }

        lines = ["Todo List:"]
        for t in self._todos:
            icon = status_icons.get(t["status"], "?")
            lines.append(f"  {icon} [{t['id']}] {t['title']} ({t['status']})")

        completed = sum(1 for t in self._todos if t["status"] == "completed")
        total = len(self._todos)
        lines.append(f"\nProgress: {completed}/{total} completed")

        return "\n".join(lines)

    def to_json(self) -> str:
        """Serialize todo state to JSON for persistence."""
        return json.dumps(self._todos)

    def from_json(self, data: str) -> None:
        """Restore todo state from JSON."""
        self._todos = json.loads(data)
        self._notify()
