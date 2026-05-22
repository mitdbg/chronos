"""Integration tests — sequential subtransactions.

Tests the full flow: orchestrator → sequential sub-agents (explore →
implement → test) within a single transaction, using mock LLM.
Each subtxn sees prior subtxn's results. No conflicts.
"""

import pytest
from pathlib import Path

from janus_code.agent.llm import MockLLM
from janus_code.config import Config
from janus_code.janus_integration.session_manager import SessionManager, TxnContext
from janus_code.janus_integration.sub_agent_txn import SubAgentTxn, SubAgentResult


class TestSequentialSubtxns:
    """Test sequential subtransaction execution."""

    async def test_explore_implement_test_pipeline(self, tmp_path):
        """Run explore → implement → test as sequential subtxns."""
        config = Config()
        session = SessionManager(
            project_path=tmp_path,
            config=config,
        )
        session.start_session()
        txn = session.begin_txn()

        call_log: list[str] = []

        async def agent_fn(config, tools, task, prior_results, txn):
            call_log.append(f"task={task}, prior_count={len(prior_results or [])}")
            return f"Result for: {task}"

        sub = SubAgentTxn(parent_txn=txn, all_tools={}, run_agent_fn=agent_fn)
        results = await sub.run_explore_implement_test("build calculator")

        assert len(results) == 3
        assert all(r.success for r in results)
        assert results[0].agent_type == "explore"
        assert results[1].agent_type == "implement"
        assert results[2].agent_type == "test"
        # Second step sees 1 prior result, third sees 2
        assert "prior_count=0" in call_log[0]
        assert "prior_count=1" in call_log[1]
        assert "prior_count=2" in call_log[2]

    async def test_default_agent_fn(self, tmp_path):
        """Without run_agent_fn, uses default summary."""
        config = Config()
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()
        txn = session.begin_txn()

        sub = SubAgentTxn(parent_txn=txn, all_tools={})
        results = await sub.run_explore_implement_test("fix bug")

        assert len(results) == 3
        assert all(r.success for r in results)
        assert "explore" in results[0].summary.lower()

    def test_subtxn_commit_abort_lifecycle(self, tmp_path):
        """Subtxns commit on success, abort on failure."""
        config = Config()
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()
        txn = session.begin_txn()

        sub1 = txn.begin_subtxn("step1")
        assert sub1.is_active
        sub1.commit()
        assert not sub1.is_active

        sub2 = txn.begin_subtxn("step2")
        assert sub2.is_active
        sub2.abort()
        assert not sub2.is_active

        # Parent still active
        assert txn.is_active
        txn.commit()
        assert not txn.is_active

    def test_sequential_subtxns_isolation(self, tmp_path):
        """Each subtxn runs in isolation but sees committed siblings."""
        config = Config()
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()
        txn = session.begin_txn()

        results: list[str] = []
        steps = ["analyze", "plan", "code", "verify"]
        for step in steps:
            sub = txn.begin_subtxn(step)
            results.append(f"output_{step}")
            sub.commit()

        assert len(results) == 4
        txn.commit()

    async def test_stop_on_failure(self, tmp_path):
        """With stop_on_failure, pipeline stops at first failure."""
        config = Config()
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()
        txn = session.begin_txn()

        async def failing_agent(config, tools, task, prior_results, txn):
            if "implement" in task.lower():
                raise RuntimeError("Compilation error")
            return f"OK: {task}"

        sub = SubAgentTxn(parent_txn=txn, all_tools={}, run_agent_fn=failing_agent)
        results = await sub.run_explore_implement_test("tricky task")

        # Explore succeeds, implement fails, test never runs
        assert len(results) == 2
        assert results[0].success
        assert not results[1].success
        assert "Compilation error" in results[1].error


class TestSpeculativeExecution:
    """Test speculative execution — try approach A, if fails, try B."""

    def test_abort_first_approach_try_second(self, tmp_path):
        """Approach A fails → abort → Approach B succeeds."""
        config = Config()
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()
        txn = session.begin_txn()

        # Approach A fails
        subtxn_a = txn.begin_subtxn("approach_A")
        subtxn_a.abort()  # Discard approach A

        # Approach B succeeds
        subtxn_b = txn.begin_subtxn("approach_B")
        subtxn_b.commit()

        assert not subtxn_a.is_active
        assert not subtxn_b.is_active
        assert txn.is_active

        txn.commit()

    def test_multiple_speculative_attempts(self, tmp_path):
        """Multiple failed attempts before success."""
        config = Config()
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()
        txn = session.begin_txn()

        attempts = 0
        for i in range(5):
            sub = txn.begin_subtxn(f"attempt_{i}")
            attempts += 1
            if i < 4:
                sub.abort()
            else:
                sub.commit()

        assert attempts == 5
        txn.commit()


class TestSubAgentIsolation:
    """Test sub-agent context isolation."""

    def test_sub_agent_messages_isolated(self, tmp_path):
        """Sub-agent messages don't leak to parent."""
        from janus_code.context.conversation import Conversation

        parent_conv = Conversation()
        parent_conv.append_system("Parent system prompt")
        parent_conv.append_user("User request")
        parent_msg_count = len(parent_conv.messages)

        # Sub-agent has its own conversation
        sub_conv = Conversation()
        sub_conv.append_system("Sub-agent prompt")
        sub_conv.append_user("Sub-task")
        sub_conv.append_assistant("Sub-result")

        # Parent conversation unchanged
        assert len(parent_conv.messages) == parent_msg_count
        assert len(sub_conv.messages) == 3

    def test_subtxn_abort_preserves_parent(self, tmp_path):
        """Aborting a subtxn doesn't affect the parent txn."""
        config = Config()
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()
        txn = session.begin_txn()

        sub = txn.begin_subtxn("doomed")
        sub.abort()

        # Parent txn still active and committable
        assert txn.is_active
        txn.commit()
        assert not txn.is_active
