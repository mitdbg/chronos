"""Bash tool — execute shell commands in the workspace."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from pydantic import BaseModel, Field


DEFAULT_TIMEOUT = 120
MAX_OUTPUT_BYTES = 100_000  # 100 KB


class BashInput(BaseModel):
    """Input schema for the Bash tool."""

    command: str = Field(
        description="The bash command to execute in the workspace."
    )
    timeout: int | None = Field(
        default=None,
        description=f"Timeout in seconds (default: {DEFAULT_TIMEOUT}).",
    )


class BashTool:
    """Execute bash commands in the workspace directory.

    - Commands run with ``cwd`` set to the working directory.
    - Output (stdout + stderr) is captured and returned.
    - Long outputs are truncated.
    - Non-zero exit codes are reported.
    - Never use for grep — use RipGrep tool instead.
    """

    name: str = "bash"
    description: str = (
        "Execute bash commands in the workspace. "
        "Never use for grep — use the RipGrep tool instead."
    )
    args_schema = BashInput

    def __init__(
        self,
        working_dir: Path,
        timeout: int = DEFAULT_TIMEOUT,
        read_only: bool = False,
    ) -> None:
        self.working_dir = working_dir
        self.timeout = timeout
        self.read_only = read_only

    def run(self, command: str, timeout: int | None = None) -> str:
        """Execute a bash command."""
        if self.read_only:
            # Block obvious write commands
            write_indicators = [
                "rm ", "rm\t", "rmdir", "mv ", "mv\t", "cp ",
                "cp\t", "mkdir", "touch", "chmod", "chown",
                ">", ">>", "tee ", "dd ",
            ]
            cmd_lower = command.lower().strip()
            for indicator in write_indicators:
                if indicator in cmd_lower:
                    return (
                        f"Error: write operations are not allowed in read-only mode. "
                        f"Blocked command containing '{indicator.strip()}'."
                    )

        effective_timeout = timeout or self.timeout

        env = dict(os.environ)
        env["HOME"] = str(self.working_dir)

        try:
            result = subprocess.run(
                ["bash", "-c", command],
                capture_output=True,
                text=True,
                timeout=effective_timeout,
                cwd=str(self.working_dir),
                env=env,
            )
        except subprocess.TimeoutExpired:
            return f"Error: command timed out after {effective_timeout} seconds."
        except OSError as e:
            return f"Error executing command: {e}"

        output = result.stdout + result.stderr
        if len(output.encode()) > MAX_OUTPUT_BYTES:
            output = output[: MAX_OUTPUT_BYTES // 2] + (
                "\n\n... (output truncated) ...\n\n"
            ) + output[-MAX_OUTPUT_BYTES // 4 :]

        exit_info = ""
        if result.returncode != 0:
            exit_info = f"\n[exit code: {result.returncode}]"

        return (output.strip() + exit_info) if output.strip() else (
            f"(no output){exit_info}"
        )
