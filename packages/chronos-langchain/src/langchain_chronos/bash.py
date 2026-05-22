"""Chronos Bash tool — execute shell commands in the transactional overlay.

Runs bash commands with ``cwd`` set to the overlay's merged directory,
so all file-system side effects are captured in the overlay. Agents can
compile code, run tests, execute scripts, etc. in full isolation —
nothing touches the real project until the transaction commits.

Modeled after Anthropic's ``bash`` tool but rooted in the transactional
overlay for explorative execution.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

from langchain_core.callbacks import CallbackManagerForToolRun
from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field


DEFAULT_TIMEOUT = 120  # seconds
MAX_OUTPUT_BYTES = 100_000  # 100 KB truncation


class ChronosBashInput(BaseModel):
    """Input schema for the Chronos bash tool."""

    command: str = Field(
        description=(
            "The bash command to execute in the transactional workspace. "
            "The working directory is the overlay's merged directory, so all "
            "filesystem side effects are isolated within the transaction."
        )
    )
    timeout: int | None = Field(
        default=None,
        description=(
            f"Timeout in seconds. Defaults to {DEFAULT_TIMEOUT}s. "
            "Long-running commands will be terminated."
        ),
    )


class ChronosBash(BaseTool):
    """Execute bash commands inside the transactional overlay filesystem.

    Commands run with ``cwd`` set to the overlay's merged directory so
    all file writes land in the overlay's upperdir. Unix tools (gcc,
    python, pytest, grep, git, etc.) work unmodified.

    Output is captured (combined stdout + stderr) and returned. Long
    outputs are truncated. Non-zero exit codes are reported.

    Example::

        bash = ChronosBash(working_dir=Path("/tmp/tar_overlayfs/.../merged"))
        result = bash.invoke({"command": "ls -la"})
        result = bash.invoke({"command": "python hello.py"})
        result = bash.invoke({"command": "gcc -o prog main.c && ./prog"})
    """

    name: str = "chronos_bash"
    description: str = (
        "Execute bash commands in the transactional workspace. "
        "All filesystem side effects are isolated within the transaction. "
        "Unix tools work unmodified on the overlay filesystem."
    )
    args_schema: type[BaseModel] = ChronosBashInput

    working_dir: Path
    """The overlay merged directory — commands run here."""

    timeout: int = DEFAULT_TIMEOUT
    """Default timeout in seconds for command execution."""

    env_vars: dict[str, str] | None = None
    """Extra environment variables to set for commands."""

    def _run(
        self,
        command: str,
        timeout: int | None = None,
        run_manager: CallbackManagerForToolRun | None = None,
    ) -> str:
        """Execute a bash command in the overlay directory."""
        effective_timeout = timeout or self.timeout

        # Build environment: inherit current env + overlay-specific vars
        env = dict(__import__("os").environ)
        # Ensure PATH includes common tool directories
        env.setdefault("PATH", "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
        # Set HOME to overlay if not set
        env["HOME"] = str(self.working_dir)
        if self.env_vars:
            env.update(self.env_vars)

        try:
            result = subprocess.run(
                ["bash", "-c", command],
                cwd=str(self.working_dir),
                capture_output=True,
                text=True,
                timeout=effective_timeout,
                env=env,
            )
        except subprocess.TimeoutExpired:
            return (
                f"Error: Command timed out after {effective_timeout}s.\n"
                f"Command: {command}"
            )
        except FileNotFoundError:
            return "Error: bash not found. Is /bin/bash available?"
        except Exception as e:
            return f"Error executing command: {e}"

        # Combine stdout and stderr
        output_parts: list[str] = []

        if result.stdout:
            output_parts.append(result.stdout)

        if result.stderr:
            if output_parts:
                output_parts.append("\n--- stderr ---\n")
            output_parts.append(result.stderr)

        output = "".join(output_parts)

        # Truncate if too large
        if len(output) > MAX_OUTPUT_BYTES:
            truncated_at = MAX_OUTPUT_BYTES
            output = (
                output[:truncated_at]
                + f"\n\n... [output truncated at {MAX_OUTPUT_BYTES} bytes] ..."
            )

        # Append exit code info for non-zero
        if result.returncode != 0:
            output += f"\n\n[Exit code: {result.returncode}]"

        return output if output.strip() else "(no output)"
