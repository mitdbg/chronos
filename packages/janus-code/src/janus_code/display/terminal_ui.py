"""Terminal UI — Rich-based formatting for Janus-Code output."""

from __future__ import annotations

from typing import Any


class TerminalUI:
    """Rich-based terminal output formatting.

    Falls back to plain text if Rich is not available.
    """

    def __init__(self, use_rich: bool = True) -> None:
        self._console = None
        if use_rich:
            try:
                from rich.console import Console
                self._console = Console()
            except ImportError:
                pass

    @property
    def has_rich(self) -> bool:
        return self._console is not None

    def print(self, text: str, **kwargs: Any) -> None:
        """Print text, optionally with Rich formatting."""
        if self._console:
            self._console.print(text, **kwargs)
        else:
            print(text)

    def print_markdown(self, text: str) -> None:
        """Render markdown content."""
        if self._console:
            from rich.markdown import Markdown
            self._console.print(Markdown(text))
        else:
            print(text)

    def print_diff(self, diff_text: str) -> None:
        """Display a unified diff with syntax highlighting."""
        if self._console:
            from rich.syntax import Syntax
            syntax = Syntax(diff_text, "diff", theme="monokai")
            self._console.print(syntax)
        else:
            print(diff_text)

    def print_error(self, message: str) -> None:
        """Display an error message."""
        if self._console:
            self._console.print(f"[bold red]Error:[/bold red] {message}")
        else:
            print(f"Error: {message}")

    def print_success(self, message: str) -> None:
        """Display a success message."""
        if self._console:
            self._console.print(f"[bold green]✓[/bold green] {message}")
        else:
            print(f"✓ {message}")

    def print_warning(self, message: str) -> None:
        """Display a warning message."""
        if self._console:
            self._console.print(f"[bold yellow]⚠[/bold yellow] {message}")
        else:
            print(f"⚠ {message}")

    def print_info(self, message: str) -> None:
        """Display an info message."""
        if self._console:
            self._console.print(f"[dim]{message}[/dim]")
        else:
            print(message)

    def print_tool_call(self, tool_name: str, args_summary: str) -> None:
        """Display a tool call indicator."""
        if self._console:
            self._console.print(
                f"  [cyan]⚡ {tool_name}[/cyan] {args_summary}"
            )
        else:
            print(f"  ⚡ {tool_name} {args_summary}")

    def print_agent_activity(self, agent_name: str, status: str) -> None:
        """Display sub-agent activity."""
        if self._console:
            self._console.print(
                f"  [magenta]● {agent_name}[/magenta] {status}"
            )
        else:
            print(f"  ● {agent_name} {status}")

    def spinner(self, message: str = "Working..."):
        """Create a spinner context manager."""
        if self._console:
            from rich.console import Console
            return self._console.status(message)
        return _NoOpContext()


class _NoOpContext:
    """No-op context manager for when Rich is unavailable."""
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
