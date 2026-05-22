"""ParallelExecutor — run agents concurrently in independent transactions.

Each agent runs in its own full transaction (NOT a subtransaction).
These peer transactions CAN conflict via write-lock detection.
On conflict, the configurable retry policy determines behavior.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import dataclass
from typing import Any, Callable

from janus_code.config import RetryPolicy
from janus_code.janus_integration.session_manager import SessionManager, TxnContext

logger = logging.getLogger(__name__)


@dataclass
class AgentTask:
    """A task to be executed by a parallel agent."""

    description: str
    agent_type: str = "implement"
    metadata: dict[str, Any] | None = None


@dataclass
class AgentResult:
    """Result from a parallel agent execution."""

    task: AgentTask
    success: bool
    summary: str = ""
    error: str | None = None
    txn_id: str = ""
    attempts: int = 1


class ParallelExecutor:
    """Run multiple agents concurrently in independent transactions.

    Each agent runs in its own transaction (not a subtransaction).
    These are peer transactions that CAN conflict via write-locks.

    On conflict:
      - strategy="exponential": abort, backoff, retry in a fresh txn
        (the fresh txn sees the winner's committed state).
      - strategy="none": abort and report failure to the orchestrator.
    """

    def __init__(
        self,
        session: SessionManager,
        retry_policy: RetryPolicy | None = None,
        run_agent_fn: Callable | None = None,
        event_sink: Callable[..., None] | None = None,
        session_factory: Callable[[AgentTask, int], SessionManager] | None = None,
    ) -> None:
        self.session = session
        self.retry_policy = retry_policy or RetryPolicy()
        self._run_agent_fn = run_agent_fn
        self._event_sink = event_sink
        self._session_factory = session_factory

    async def execute(self, tasks: list[AgentTask]) -> list[AgentResult]:
        """Run tasks concurrently. Returns results (success or failure)."""
        return list(
            await asyncio.gather(
                *[self._run_with_retry(task) for task in tasks]
            )
        )

    async def _run_with_retry(self, task: AgentTask) -> AgentResult:
        """Run a single agent task with retry on CC conflict."""
        attempt = 0

        while True:
            attempt += 1
            active_session = (
                self._session_factory(task, attempt)
                if self._session_factory is not None
                else self.session
            )
            txn = active_session.begin_txn()
            if self._event_sink is not None:
                self._event_sink(
                    "parallel_txn_begin",
                    "Started parallel transaction attempt.",
                    txn_id=txn.txn_id,
                    task=task.description,
                    agent_type=task.agent_type,
                    attempt=attempt,
                )

            try:
                if self._run_agent_fn:
                    try:
                        summary_or_awaitable = self._run_agent_fn(
                            task, txn, active_session
                        )
                    except TypeError:
                        # Backward-compatible two-arg run_agent_fn(task, txn)
                        summary_or_awaitable = self._run_agent_fn(task, txn)
                    if inspect.isawaitable(summary_or_awaitable):
                        summary = await summary_or_awaitable
                    else:
                        summary = str(summary_or_awaitable)
                else:
                    summary = f"[{task.agent_type}] {task.description}"

                active_session.commit_txn(txn)
                if self._event_sink is not None:
                    self._event_sink(
                        "parallel_txn_commit",
                        "Committed parallel transaction attempt.",
                        txn_id=txn.txn_id,
                        task=task.description,
                        agent_type=task.agent_type,
                        attempt=attempt,
                    )

                return AgentResult(
                    task=task,
                    success=True,
                    summary=summary,
                    txn_id=txn.txn_id,
                    attempts=attempt,
                )

            except Exception as e:
                active_session.abort_txn(txn)
                if self._event_sink is not None:
                    self._event_sink(
                        "parallel_txn_abort",
                        "Aborted parallel transaction attempt.",
                        txn_id=txn.txn_id,
                        task=task.description,
                        agent_type=task.agent_type,
                        attempt=attempt,
                        error=str(e),
                    )

                if is_conflict_error(e):
                    if (
                        self.retry_policy.strategy == "none"
                        or attempt >= self.retry_policy.max_retries
                    ):
                        return AgentResult(
                            task=task,
                            success=False,
                            error=f"CC conflict after {attempt} attempt(s): {e}",
                            txn_id=txn.txn_id,
                            attempts=attempt,
                        )

                    # Exponential backoff
                    delay_ms = self.retry_policy.delay_for_attempt(attempt)
                    logger.info(
                        "CC conflict on attempt %d for '%s', retrying in %dms",
                        attempt, task.description[:50], delay_ms,
                    )
                    if self._event_sink is not None:
                        self._event_sink(
                            "parallel_retry",
                            "Conflict detected; retrying parallel transaction.",
                            task=task.description,
                            agent_type=task.agent_type,
                            attempt=attempt,
                            delay_ms=delay_ms,
                        )
                    await asyncio.sleep(delay_ms / 1000.0)
                else:
                    return AgentResult(
                        task=task,
                        success=False,
                        error=str(e),
                        txn_id=txn.txn_id,
                        attempts=attempt,
                    )


def is_conflict_error(e: Exception) -> bool:
    """Check if exception is a write conflict error."""
    # Try importing Janus's standalone transaction conflict type if available.
    try:
        from janus_core.transaction.coordinator import CommitConflictError
        if isinstance(e, CommitConflictError):
            return True
    except ImportError:
        pass
    # Fallback: check exception class name for 'conflict'
    name = type(e).__name__.lower()
    return "conflict" in name or "writeconflict" in name
