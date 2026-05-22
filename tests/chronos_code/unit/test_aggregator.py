"""Unit tests for Aggregator."""

import pytest
from pathlib import Path

from chronos_code.config import Config
from chronos_code.chronos_integration.session_manager import SessionManager
from chronos_code.chronos_integration.parallel_executor import AgentResult, AgentTask
from chronos_code.chronos_integration.aggregator import Aggregator, AggregatorResult


@pytest.fixture
def session(tmp_path: Path) -> SessionManager:
    sm = SessionManager(project_path=tmp_path, config=Config())
    sm.start_session()
    return sm


def make_result(desc: str, success: bool = True, summary: str = "done") -> AgentResult:
    return AgentResult(
        task=AgentTask(description=desc),
        success=success,
        summary=summary if success else "",
        error=None if success else "failed",
    )


class TestAggregator:
    @pytest.mark.asyncio
    async def test_aggregate_all_success(self, session):
        agg = Aggregator(session)
        results = [
            make_result("task A", summary="A done"),
            make_result("task B", summary="B done"),
        ]

        outcome = await agg.aggregate(results)
        assert outcome.success
        assert outcome.merged_count == 2
        assert "A done" in outcome.summary
        assert "B done" in outcome.summary

    @pytest.mark.asyncio
    async def test_aggregate_with_failures(self, session):
        agg = Aggregator(session)
        results = [
            make_result("task A", summary="A done"),
            make_result("task B", success=False),
        ]

        outcome = await agg.aggregate(results)
        assert outcome.success
        assert outcome.merged_count == 1
        assert "Failed Tasks" in outcome.summary

    @pytest.mark.asyncio
    async def test_aggregate_all_failed(self, session):
        agg = Aggregator(session)
        results = [
            make_result("task A", success=False),
            make_result("task B", success=False),
        ]

        outcome = await agg.aggregate(results)
        assert not outcome.success
        assert "No successful" in outcome.summary

    @pytest.mark.asyncio
    async def test_custom_merge_fn(self, session):
        async def custom_merge(successful, txn):
            return f"Merged {len(successful)} results"

        agg = Aggregator(session, merge_fn=custom_merge)
        results = [make_result("A"), make_result("B")]

        outcome = await agg.aggregate(results)
        assert outcome.success
        assert "Merged 2" in outcome.summary

    @pytest.mark.asyncio
    async def test_llm_merge_path(self, session):
        calls = {"count": 0}

        def llm_fn(messages, tool_schemas):
            calls["count"] += 1
            assert len(messages) == 2
            assert tool_schemas == []
            return {"content": "LLM merged summary", "tool_calls": None}

        agg = Aggregator(session, llm_fn=llm_fn)
        results = [make_result("task A", summary="A done"), make_result("task B", summary="B done")]

        outcome = await agg.aggregate(results)
        assert outcome.success
        assert outcome.summary == "LLM merged summary"
        assert calls["count"] == 1

    @pytest.mark.asyncio
    async def test_llm_merge_empty_falls_back(self, session):
        def llm_fn(messages, tool_schemas):
            return {"content": "", "tool_calls": None}

        agg = Aggregator(session, llm_fn=llm_fn)
        results = [make_result("task A", summary="A done")]

        outcome = await agg.aggregate(results)
        assert outcome.success
        assert "Aggregated Results" in outcome.summary

    @pytest.mark.asyncio
    async def test_merge_fn_failure(self, session):
        async def failing_merge(successful, txn):
            raise RuntimeError("merge failed")

        agg = Aggregator(session, merge_fn=failing_merge)
        results = [make_result("A")]

        outcome = await agg.aggregate(results)
        assert not outcome.success
        assert "merge failed" in outcome.error
