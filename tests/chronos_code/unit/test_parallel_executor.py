"""Unit tests for ParallelExecutor."""

import pytest
import asyncio
from pathlib import Path

from chronos_code.config import Config, RetryPolicy
from chronos_code.chronos_integration.session_manager import SessionManager
from chronos_code.chronos_integration.parallel_executor import (
    AgentTask,
    AgentResult,
    ParallelExecutor,
    is_conflict_error,
)


class ConflictError(Exception):
    """Simulated write conflict error."""
    pass


@pytest.fixture
def session(tmp_path: Path) -> SessionManager:
    sm = SessionManager(project_path=tmp_path, config=Config())
    sm.start_session()
    return sm


class TestParallelExecutor:
    @pytest.mark.asyncio
    async def test_simple_parallel(self, session):
        async def mock_agent(task, txn):
            return f"Done: {task.description}"

        executor = ParallelExecutor(session, run_agent_fn=mock_agent)
        tasks = [
            AgentTask("task A"),
            AgentTask("task B"),
            AgentTask("task C"),
        ]
        results = await executor.execute(tasks)

        assert len(results) == 3
        assert all(r.success for r in results)

    @pytest.mark.asyncio
    async def test_failure_handling(self, session):
        async def failing_agent(task, txn):
            raise RuntimeError("agent crash")

        executor = ParallelExecutor(session, run_agent_fn=failing_agent)
        results = await executor.execute([AgentTask("fail")])

        assert len(results) == 1
        assert not results[0].success
        assert "agent crash" in results[0].error

    @pytest.mark.asyncio
    async def test_no_retry_on_none_strategy(self, session):
        call_count = [0]

        async def conflict_agent(task, txn):
            call_count[0] += 1
            raise ConflictError("write conflict")

        policy = RetryPolicy(strategy="none")
        executor = ParallelExecutor(
            session, retry_policy=policy, run_agent_fn=conflict_agent
        )
        results = await executor.execute([AgentTask("conflict")])

        assert len(results) == 1
        assert not results[0].success
        assert call_count[0] == 1  # no retries

    @pytest.mark.asyncio
    async def test_mixed_success_failure(self, session):
        async def mixed_agent(task, txn):
            if "fail" in task.description:
                raise RuntimeError("failed")
            return "success"

        executor = ParallelExecutor(session, run_agent_fn=mixed_agent)
        tasks = [
            AgentTask("good task"),
            AgentTask("fail task"),
        ]
        results = await executor.execute(tasks)

        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]
        assert len(successes) == 1
        assert len(failures) == 1

    @pytest.mark.asyncio
    async def test_without_agent_fn(self, session):
        """Without run_agent_fn, returns stub summary."""
        executor = ParallelExecutor(session)
        results = await executor.execute([AgentTask("test task")])

        assert len(results) == 1
        assert results[0].success

    @pytest.mark.asyncio
    async def test_session_factory_passed_to_agent_fn(self, session):
        """Executor should use per-task sessions from session_factory."""
        created = {"count": 0}

        def session_factory(task, attempt):
            created["count"] += 1
            sm = SessionManager(project_path=session.project_path, config=session.config)
            sm.start_session()
            return sm

        seen_session_ids = set()

        async def mock_agent(task, txn, active_session):
            seen_session_ids.add(active_session.session_id)
            return f"Done: {task.description}"

        executor = ParallelExecutor(
            session,
            run_agent_fn=mock_agent,
            session_factory=session_factory,
        )
        tasks = [AgentTask("task A"), AgentTask("task B")]
        results = await executor.execute(tasks)

        assert len(results) == 2
        assert all(r.success for r in results)
        assert created["count"] == 2
        assert len(seen_session_ids) == 2


class TestIsConflictError:
    def test_regular_exception(self):
        assert not is_conflict_error(RuntimeError("test"))

    def test_conflict_named_exception(self):
        assert is_conflict_error(ConflictError("test"))

    def test_commit_conflict_by_name(self):
        class CommitConflictError(Exception):
            pass
        assert is_conflict_error(CommitConflictError("test"))
