"""Todo display — live todo list rendering for the terminal."""

from __future__ import annotations

from typing import Any


# Status icons
STATUS_ICONS = {
    "pending": "○",
    "in_progress": "◉",
    "completed": "✓",
}

STATUS_COLORS = {
    "pending": "white",
    "in_progress": "yellow",
    "completed": "green",
}


class TodoDisplay:
    """Renders the todo list to the terminal.

    Can be used as a listener callback for TodoWriteTool to
    automatically update the display when todos change.
    """

    def __init__(self, use_rich: bool = True) -> None:
        self._console = None
        if use_rich:
            try:
                from rich.console import Console
                self._console = Console()
            except ImportError:
                pass

    def render(self, todos: list[dict[str, Any]]) -> str:
        """Render todos as a formatted string."""
        if not todos:
            return "No tasks."

        lines: list[str] = []
        for todo in todos:
            status = todo.get("status", "pending")
            icon = STATUS_ICONS.get(status, "?")
            title = todo.get("title", "Untitled")
            lines.append(f"  {icon} {title}")

        return "\n".join(lines)

    def display(self, todos: list[dict[str, Any]]) -> None:
        """Print the todo list to terminal."""
        if self._console:
            from rich.table import Table

            table = Table(title="Tasks", show_header=False, box=None)
            table.add_column("Status", width=3)
            table.add_column("Task")

            for todo in todos:
                status = todo.get("status", "pending")
                icon = STATUS_ICONS.get(status, "?")
                color = STATUS_COLORS.get(status, "white")
                title = todo.get("title", "Untitled")
                table.add_row(
                    f"[{color}]{icon}[/{color}]",
                    f"[{color}]{title}[/{color}]",
                )

            self._console.print(table)
        else:
            print(self.render(todos))

    def on_todos_changed(self, todos: list[dict[str, Any]]) -> None:
        """Callback for TodoWriteTool listener."""
        self.display(todos)
