"""Slash command subsystem — /config, /status, /help, etc.

Slash commands are handled outside of transactions. They are
immediate, synchronous operations that modify configuration or
display status.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from janus_code.config import Config, RetryPolicy


@dataclass
class SlashCommandResult:
    """Result from executing a slash command."""

    output: str
    success: bool = True


class SlashCommandHandler:
    """Handles /slash commands in the REPL.

    Commands are parsed by splitting on whitespace. The first token
    is the command name, the rest are arguments.
    """

    def __init__(
        self,
        config: Config,
        get_status_fn: Callable[[], dict[str, Any]] | None = None,
        get_history_fn: Callable[[], str] | None = None,
        get_memory_fn: Callable[[str | None], str] | None = None,
        resume_fn: Callable[[str], str] | None = None,
        debug_fn: Callable[[list[str]], str] | None = None,
    ) -> None:
        self.config = config
        self._get_status = get_status_fn
        self._get_history = get_history_fn
        self._get_memory = get_memory_fn
        self._resume_fn = resume_fn
        self._debug_fn = debug_fn

        self._commands: dict[str, Callable[[list[str]], SlashCommandResult]] = {
            "help": self._cmd_help,
            "config": self._cmd_config,
            "status": self._cmd_status,
            "history": self._cmd_history,
            "memory": self._cmd_memory,
            "resume": self._cmd_resume,
            "abort": self._cmd_abort,
        }
        if self._debug_fn is not None:
            self._commands["debug"] = self._cmd_debug

    @property
    def command_names(self) -> list[str]:
        """Available command names for tab completion."""
        return sorted(self._commands.keys())

    def is_slash_command(self, text: str) -> bool:
        """Check if input is a slash command."""
        return text.strip().startswith("/")

    def handle(self, text: str) -> SlashCommandResult:
        """Parse and execute a slash command."""
        parts = text.strip().lstrip("/").split()
        if not parts:
            return SlashCommandResult(
                "Type /help for available commands.", success=False
            )

        cmd_name = parts[0].lower()
        args = parts[1:]

        handler = self._commands.get(cmd_name)
        if handler is None:
            return SlashCommandResult(
                f"Unknown command: /{cmd_name}. Type /help for available commands.",
                success=False,
            )

        try:
            return handler(args)
        except Exception as e:
            return SlashCommandResult(f"Error: {e}", success=False)

    # ── Command implementations ──────────────────────────────────────

    def _cmd_help(self, args: list[str]) -> SlashCommandResult:
        """Show available commands."""
        lines = [
            "Available commands:",
            "",
            "  /help                          Show this help message",
            "  /config                        Show current configuration",
            "  /config model <name>           Set LLM model",
            "  /config retry <strategy>       Set retry: exponential | none",
            "  /config retry max-retries <n>  Set max retry attempts",
            "  /config retry base-delay <ms>  Set base delay in ms",
            "  /config parallel <on|off>      Toggle parallel execution",
            "  /status                        Show session status",
            "  /history                       Show conversation history",
            "  /memory                        List memory files",
            "  /memory <topic>                Read a memory file",
            "  /resume <session-id>           Resume a previous session",
            "  /abort                         Abort current turn",
        ]
        if self._debug_fn is not None:
            lines.append("  /debug <scenario>              Run deterministic Janus debug scenario")
        return SlashCommandResult("\n".join(lines))

    def _cmd_config(self, args: list[str]) -> SlashCommandResult:
        """Show or modify configuration."""
        if not args:
            return self._show_config()

        subcmd = args[0].lower()

        if subcmd == "model" and len(args) >= 2:
            self.config.model = args[1]
            return SlashCommandResult(f"Model set to: {args[1]}")

        if subcmd == "retry":
            return self._config_retry(args[1:])

        if subcmd == "parallel" and len(args) >= 2:
            val = args[1].lower()
            if val in ("on", "true", "1"):
                self.config.parallel_enabled = True
                return SlashCommandResult("Parallel execution: ON")
            elif val in ("off", "false", "0"):
                self.config.parallel_enabled = False
                return SlashCommandResult("Parallel execution: OFF")
            else:
                return SlashCommandResult(
                    "Usage: /config parallel <on|off>", success=False
                )

        return SlashCommandResult(
            f"Unknown config option: {subcmd}. See /help.", success=False
        )

    def _show_config(self) -> SlashCommandResult:
        """Display current configuration."""
        rp = self.config.retry_policy
        lines = [
            "Current Configuration:",
            f"  Model:             {self.config.model}",
            f"  Max context:       {self.config.max_context_tokens:,} tokens",
            f"  Compaction at:     {self.config.compaction_threshold:.0%}",
            f"  Parallel:          {'ON' if self.config.parallel_enabled else 'OFF'}",
            f"  Bash timeout:      {self.config.bash_timeout}s",
            f"  Ripgrep max:       {self.config.ripgrep_max_results}",
            "",
            "  Retry Policy:",
            f"    Strategy:        {rp.strategy}",
            f"    Max retries:     {rp.max_retries}",
            f"    Base delay:      {rp.base_delay_ms}ms",
            f"    Max delay:       {rp.max_delay_ms}ms",
        ]
        return SlashCommandResult("\n".join(lines))

    def _config_retry(self, args: list[str]) -> SlashCommandResult:
        """Handle /config retry subcommands."""
        if not args:
            return SlashCommandResult(
                "Usage: /config retry <exponential|none>\n"
                "       /config retry max-retries <n>\n"
                "       /config retry base-delay <ms>",
                success=False,
            )

        subcmd = args[0].lower()

        if subcmd in ("exponential", "none"):
            self.config.retry_policy.strategy = subcmd
            return SlashCommandResult(f"Retry strategy: {subcmd}")

        if subcmd == "max-retries" and len(args) >= 2:
            try:
                n = int(args[1])
                self.config.retry_policy.max_retries = n
                return SlashCommandResult(f"Max retries: {n}")
            except ValueError:
                return SlashCommandResult(
                    "Invalid number for max-retries.", success=False
                )

        if subcmd == "base-delay" and len(args) >= 2:
            try:
                ms = int(args[1])
                self.config.retry_policy.base_delay_ms = ms
                return SlashCommandResult(f"Base delay: {ms}ms")
            except ValueError:
                return SlashCommandResult(
                    "Invalid number for base-delay.", success=False
                )

        return SlashCommandResult(
            f"Unknown retry option: {subcmd}. See /help.", success=False
        )

    def _cmd_status(self, args: list[str]) -> SlashCommandResult:
        """Show session status."""
        if self._get_status:
            status = self._get_status()
            lines = [
                "Session Status:",
                f"  Session ID:    {status.get('session_id', 'N/A')}",
                f"  Turn:          {status.get('turn_number', 0)}",
                f"  Active Txn:    {'Yes' if status.get('has_active_txn') else 'No'}",
                f"  Project:       {status.get('project_path', 'N/A')}",
            ]
            return SlashCommandResult("\n".join(lines))
        return SlashCommandResult("Status not available (no active session).")

    def _cmd_history(self, args: list[str]) -> SlashCommandResult:
        """Show conversation history."""
        if self._get_history:
            return SlashCommandResult(self._get_history())
        return SlashCommandResult("History not available.")

    def _cmd_memory(self, args: list[str]) -> SlashCommandResult:
        """List or read memory files."""
        if self._get_memory:
            topic = args[0] if args else None
            return SlashCommandResult(self._get_memory(topic))
        return SlashCommandResult("Memory not available.")

    def _cmd_resume(self, args: list[str]) -> SlashCommandResult:
        """Resume a previous session."""
        if not args:
            return SlashCommandResult(
                "Usage: /resume <session-id>", success=False
            )
        if self._resume_fn:
            return SlashCommandResult(self._resume_fn(args[0]))
        return SlashCommandResult("Resume not available.")

    def _cmd_abort(self, args: list[str]) -> SlashCommandResult:
        """Abort current turn."""
        return SlashCommandResult(
            "Current turn aborted. All uncommitted changes discarded."
        )

    def _cmd_debug(self, args: list[str]) -> SlashCommandResult:
        """Run deterministic debug scenarios."""
        if self._debug_fn is None:
            return SlashCommandResult(
                "Debug scenarios are not enabled.",
                success=False,
            )
        return SlashCommandResult(self._debug_fn(args))
