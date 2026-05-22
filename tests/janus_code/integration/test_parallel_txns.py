"""Integration tests — parallel transactions, CC conflict, and retry.

Tests N independent transactions running concurrently. When two txns
edit the same file, one gets a conflict. Retry policy determines
whether the loser retries or fails immediately.
"""

import asyncio
import pytest
from pathlib import Path
from unittest.mock import MagicMock

from janus_code.config import Config, RetryPolicy
from janus_code.janus_integration.parallel_executor import (
    ParallelExecutor,
    AgentTask,
    AgentResult,
    is_conflict_error,
)
from janus_code.janus_integration.session_manager import SessionManager, TxnContext


class TestParallelTxns:
    """Test parallel transaction execution."""

    async def test_two_independent_tasks(self, tmp_path):
        """Two tasks editing different files → both succeed."""
        config = Config()
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()

        results_collected: list[str] = []

        async def agent_fn(task: AgentTask, txn: TxnContext):
            results_collected.append(task.description)
            return f"Done: {task.description}"

        executor = ParallelExecutor(
            session=session,
            run_agent_fn=agent_fn,
            retry_policy=config.retry_policy,
        )

        tasks = [
            AgentTask(description="Edit file A"),
            AgentTask(description="Edit file B"),
        ]

        results = await executor.execute(tasks)
        assert len(results) == 2
        assert all(r.success for r in results)
        assert len(results_collected) == 2

    async def test_three_parallel_tasks(self, tmp_path):
        """Three concurrent tasks all succeed."""
        config = Config()
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()

        async def agent_fn(task: AgentTask, txn: TxnContext):
            return f"Result: {task.description}"

        executor = ParallelExecutor(
            session=session,
            run_agent_fn=agent_fn,
            retry_policy=config.retry_policy,
        )

        tasks = [
            AgentTask(description=f"Task {i}") for i in range(3)
        ]

        results = await executor.execute(tasks)
        assert len(results) == 3
        assert all(r.success for r in results)

    async def test_task_failure_non_conflict(self, tmp_path):
        """A task that raises a non-conflict error → failure, no retry."""
        config = Config()
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()

        async def failing_agent(task: AgentTask, txn: TxnContext):
            raise RuntimeError("Unexpected error")

        executor = ParallelExecutor(
            session=session,
            run_agent_fn=failing_agent,
            retry_policy=config.retry_policy,
        )

        results = await executor.execute([AgentTask(description="Will fail")])
        assert len(results) == 1
        assert not results[0].success
        assert "Unexpected error" in results[0].error


class TestParallelRetry:
    """Test CC conflict retry behavior."""

    async def test_conflict_with_exponential_retry_succeeds(self, tmp_path):
        """Conflict on first attempt, success on retry."""
        config = Config()
        config.retry_policy = RetryPolicy(
            strategy="exponential",
            max_retries=3,
            base_delay_ms=10,  # Small delay for fast tests
        )
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()

        attempt_count = 0

        class FakeConflictError(Exception):
            """Simulated WriteConflictError."""
            pass

        # Monkey-patch is_conflict_error for this test
        import janus_code.janus_integration.parallel_executor as pmod
        original_fn = pmod.is_conflict_error
        pmod.is_conflict_error = lambda e: isinstance(e, FakeConflictError)

        try:
            async def sometimes_conflicts(task: AgentTask, txn: TxnContext):
                nonlocal attempt_count
                attempt_count += 1
                if attempt_count == 1:
                    raise FakeConflictError("Write conflict on file.txt")
                return "Success on retry"

            executor = ParallelExecutor(
                session=session,
                run_agent_fn=sometimes_conflicts,
                retry_policy=config.retry_policy,
            )

            results = await executor.execute([AgentTask(description="Conflicting task")])
            assert len(results) == 1
            assert results[0].success
            assert attempt_count == 2
        finally:
            pmod.is_conflict_error = original_fn

    async def test_conflict_with_none_strategy_fails_immediately(self, tmp_path):
        """Conflict with 'none' strategy → immediate failure."""
        config = Config()
        config.retry_policy = RetryPolicy(strategy="none")
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()

        class FakeConflictError(Exception):
            pass

        import janus_code.janus_integration.parallel_executor as pmod
        original_fn = pmod.is_conflict_error
        pmod.is_conflict_error = lambda e: isinstance(e, FakeConflictError)

        try:
            async def always_conflicts(task: AgentTask, txn: TxnContext):
                raise FakeConflictError("Conflict!")

            executor = ParallelExecutor(
                session=session,
                run_agent_fn=always_conflicts,
                retry_policy=config.retry_policy,
            )

            results = await executor.execute([AgentTask(description="Will conflict")])
            assert len(results) == 1
            assert not results[0].success
            assert "CC conflict" in results[0].error
        finally:
            pmod.is_conflict_error = original_fn

    async def test_max_retries_exhausted(self, tmp_path):
        """Conflict persists beyond max_retries → failure."""
        config = Config()
        config.retry_policy = RetryPolicy(
            strategy="exponential",
            max_retries=2,
            base_delay_ms=10,
        )
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()

        class FakeConflictError(Exception):
            pass

        import janus_code.janus_integration.parallel_executor as pmod
        original_fn = pmod.is_conflict_error
        pmod.is_conflict_error = lambda e: isinstance(e, FakeConflictError)

        try:
            async def always_conflicts(task: AgentTask, txn: TxnContext):
                raise FakeConflictError("Persistent conflict")

            executor = ParallelExecutor(
                session=session,
                run_agent_fn=always_conflicts,
                retry_policy=config.retry_policy,
            )

            results = await executor.execute([AgentTask(description="Always conflicts")])
            assert len(results) == 1
            assert not results[0].success
        finally:
            pmod.is_conflict_error = original_fn


class TestConflictDetection:
    """Test is_conflict_error detection."""

    def test_generic_exception_not_conflict(self):
        assert not is_conflict_error(RuntimeError("nope"))

    def test_value_error_not_conflict(self):
        assert not is_conflict_error(ValueError("bad"))

    def test_exception_with_conflict_name(self):
        """Exception class named CommitConflictError is detected."""
        class CommitConflictError(Exception):
            pass
        assert is_conflict_error(CommitConflictError("conflict"))

    def test_exception_with_write_conflict_name(self):
        class WriteConflictError(Exception):
            pass
        assert is_conflict_error(WriteConflictError("conflict"))
