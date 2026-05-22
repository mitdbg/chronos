"""Janus-Code configuration — model, retry policy, runtime settings."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass
class RetryPolicy:
    """Configurable retry policy for CC-aborted transactions.

    Applies when a parallel transaction aborts due to ``WriteConflictError``
    from a concurrent transaction.
    """

    strategy: Literal["exponential", "none"] = "exponential"
    max_retries: int = 3
    base_delay_ms: int = 100
    max_delay_ms: int = 5000

    def delay_for_attempt(self, attempt: int) -> float:
        """Return the delay in **milliseconds** for the given attempt (1-based)."""
        if self.strategy == "none":
            return 0.0
        delay_ms = min(
            self.base_delay_ms * (2 ** (attempt - 1)),
            self.max_delay_ms,
        )
        return delay_ms


@dataclass
class Config:
    """Runtime configuration for Janus-Code."""

    # LLM
    model: str = "claude-sonnet-4-20250514"
    temperature: float = 0.0
    max_tokens: int = 8192

    # Context management
    max_context_tokens: int = 128_000
    compaction_threshold: float = 0.80  # trigger compaction at 80%
    context_reserve_tokens: int = 4096  # reserve for response

    # Retry / concurrency
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    parallel_enabled: bool = True
    weak_snapshot: bool = False

    # Paths
    memory_dir: str = ".janus-code/memory"
    sessions_dir: str = ".janus-code/sessions"

    # Display
    verbose: bool = False

    # Bash defaults
    bash_timeout: int = 120

    # RipGrep defaults
    ripgrep_max_results: int = 50

    # External worker runtime (for Codex/Claude sub-agents via MCP)
    worker_runtime: Literal["internal", "codex", "claude"] = "internal"
    codex_command: str = "codex"
    claude_command: str = "claude"
    external_worker_timeout_sec: int = 900
    external_worker_mcp_server_name: str = "janus"
    external_worker_mcp_command: str = "janus-code-mcp"
    external_worker_use_embedded_mcp: bool = True
    embedded_mcp_host: str = "127.0.0.1"
    embedded_mcp_port: int = 8765
    embedded_mcp_path: str = "/mcp"
    codex_command_template: str = (
        "{command} exec -C {cwd} --skip-git-repo-check {prompt}"
    )
    claude_command_template: str = (
        "{command} -p --output-format json --mcp-config {mcp_config} {prompt}"
    )
