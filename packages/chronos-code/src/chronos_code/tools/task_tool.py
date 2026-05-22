"""Task tool (sub-agent launcher)."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class TaskInput(BaseModel):
    """Input schema for the Task tool."""

    description: str = Field(
        description="A detailed description of the task for the sub-agent."
    )
    type: Literal["explore", "implement", "test"] = Field(
        description=(
            "Sub-agent type. 'explore' = read-only research, "
            "'implement' = speculative edits, 'test' = run tests."
        )
    )
    parallel: bool = Field(
        default=False,
        description=(
            "If true, launch asynchronously and return a task ID for later "
            "aggregation."
        ),
    )


class TaskTool:
    """Launch a specialized sub-agent for a subtask.

    - ``explore``: Read-only agent in parent snapshot.
    - ``implement``: Agent writes in a child subtransaction.
    - ``test``: Agent runs tests and reports results.
    """

    name: str = "launch_task"
    description: str = (
        "Launch a specialized sub-agent. "
        "'explore': read-only research, "
        "'implement': speculative edits (isolated subtransaction), "
        "'test': run tests and report results."
    )
    args_schema = TaskInput

    def __init__(self, launcher: Any = None) -> None:
        """Initialize with optional sub-agent launcher.

        Args:
            launcher: Callable that actually runs sub-agents.
                      None in Phase 1 (stub mode).
        """
        self._launcher = launcher

    def run(
        self,
        description: str,
        type: str,
        parallel: bool = False,
    ) -> str:
        """Execute the launch_task tool.

        If a launcher callback is configured, dispatch to it. The launcher may
        be sync or async; async results are awaited by AgentLoop.
        """
        if self._launcher is not None:
            try:
                return self._launcher(
                    description=description,
                    type=type,
                    parallel=parallel,
                )
            except TypeError:
                # Backward compatibility with launchers that do not accept
                # ``parallel`` yet.
                return self._launcher(description=description, type=type)

        if parallel:
            return (
                "No task launcher configured; cannot queue parallel task. "
                "Run with parallel=false."
            )
        return (
            "No task launcher configured; unable to launch sub-agent. "
            f"Requested type='{type}', task='{description[:200]}'."
        )
