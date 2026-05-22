"""Integration tests — aggregator transaction.

After parallel txns commit independently, an aggregator txn reads
all committed results, merges them, and produces the final state.
"""

import pytest
from pathlib import Path

from chronos_code.config import Config
from chronos_code.chronos_integration.aggregator import Aggregator, AggregatorResult
from chronos_code.chronos_integration.parallel_executor import AgentResult, AgentTask
from chronos_code.chronos_integration.session_manager import SessionManager, TxnContext


class TestAggregator:
    """Test aggregator transaction flow."""

    async def test_default_merge(self, tmp_path):
        """Default aggregator concatenates results."""
        config = Config()
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()

        results = [
            AgentResult(
                task=AgentTask(description="Task A"),
                success=True,
                summary="Output A",
                txn_id="txn_1",
            ),
            AgentResult(
                task=AgentTask(description="Task B"),
                success=True,
                summary="Output B",
                txn_id="txn_2",
            ),
            AgentResult(
                task=AgentTask(description="Task C"),
                success=True,
                summary="Output C",
                txn_id="txn_3",
            ),
        ]

        aggregator = Aggregator(session=session)
        result = await aggregator.aggregate(results)

        assert isinstance(result, AggregatorResult)
        assert result.success
        assert "Output A" in result.summary
        assert "Output B" in result.summary
        assert "Output C" in result.summary
        assert result.merged_count == 3

    async def test_custom_merge_fn(self, tmp_path):
        """Custom merge function transforms results."""
        config = Config()
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()

        results = [
            AgentResult(
                task=AgentTask(description="Add feature"),
                success=True,
                summary="Feature added",
                txn_id="txn_1",
            ),
        ]

        async def custom_merge(agent_results: list[AgentResult], txn: TxnContext) -> str:
            return f"Merged {len(agent_results)} results: " + "; ".join(
                r.summary for r in agent_results
            )

        aggregator = Aggregator(session=session, merge_fn=custom_merge)
        result = await aggregator.aggregate(results)

        assert result.success
        assert "Merged 1 results" in result.summary
        assert "Feature added" in result.summary

    async def test_aggregator_skips_failed_results(self, tmp_path):
        """Aggregator handles mix of success and failure."""
        config = Config()
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()

        results = [
            AgentResult(
                task=AgentTask(description="Success task"),
                success=True,
                summary="Good output",
                txn_id="txn_1",
            ),
            AgentResult(
                task=AgentTask(description="Failed task"),
                success=False,
                error="Conflict unresolved",
                txn_id="txn_2",
            ),
        ]

        aggregator = Aggregator(session=session)
        result = await aggregator.aggregate(results)

        assert result.success
        assert result.merged_count == 1
        assert "Good output" in result.summary

    async def test_aggregator_with_no_results(self, tmp_path):
        """Empty result set produces failure."""
        config = Config()
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()

        aggregator = Aggregator(session=session)
        result = await aggregator.aggregate([])
        assert isinstance(result, AggregatorResult)
        assert not result.success

    async def test_llm_aggregator_merge(self, tmp_path):
        """Aggregator can synthesize through llm_fn instead of concatenation."""
        config = Config()
        session = SessionManager(project_path=tmp_path, config=config)
        session.start_session()

        results = [
            AgentResult(
                task=AgentTask(description="Auth task"),
                success=True,
                summary="Auth done",
                txn_id="txn_1",
            ),
            AgentResult(
                task=AgentTask(description="Billing task"),
                success=True,
                summary="Billing done",
                txn_id="txn_2",
            ),
        ]

        calls = {"count": 0}

        def llm_fn(messages, tool_schemas):
            calls["count"] += 1
            assert tool_schemas == []
            return {
                "content": "Synthesized merged output from aggregator LLM.",
                "tool_calls": None,
            }

        aggregator = Aggregator(session=session, llm_fn=llm_fn)
        result = await aggregator.aggregate(results)

        assert result.success
        assert calls["count"] == 1
        assert "Synthesized merged output" in result.summary
