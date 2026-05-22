"""Unit tests for SubAgentTxn."""

import pytest
from pathlib import Path

from janus_code.janus_integration.session_manager import TxnContext
from janus_code.janus_integration.sub_agent_txn import SubAgentTxn, SubAgentResult


class MockTool:
    def __init__(self, name: str, response: str = "ok"):
        self.name = name
        self._response = response
    def run(self, **kwargs):
        return self._response


@pytest.fixture
def parent_txn() -> TxnContext:
    return TxnContext(txn_id="parent-txn")


@pytest.fixture
def tools() -> dict:
    return {
        "read_file": MockTool("read_file", "file content"),
        "edit_file": MockTool("edit_file", "edited"),
        "write_file": MockTool("write_file", "written"),
        "bash": MockTool("bash", "output"),
        "ripgrep": MockTool("ripgrep", "match"),
        "glob_search": MockTool("glob_search", "file.py"),
        "memory": MockTool("memory", "noted"),
    }


class TestSubAgentTxn:
    @pytest.mark.asyncio
    async def test_run_step_success(self, parent_txn, tools):
        async def mock_agent(config, tools, task, prior_results, txn):
            return f"Explored: {task}"

        sat = SubAgentTxn(parent_txn, tools, run_agent_fn=mock_agent)
        result = await sat.run_step("explore", "find main function")

        assert result.success
        assert "find main function" in result.summary
        assert result.agent_type == "explore"

    @pytest.mark.asyncio
    async def test_run_step_failure(self, parent_txn, tools):
        async def failing_agent(config, tools, task, prior_results, txn):
            raise RuntimeError("test failure")

        sat = SubAgentTxn(parent_txn, tools, run_agent_fn=failing_agent)
        result = await sat.run_step("implement", "bad task")

        assert not result.success
        assert "test failure" in result.error

    @pytest.mark.asyncio
    async def test_run_step_without_agent_fn(self, parent_txn, tools):
        """Without run_agent_fn, returns a stub summary."""
        sat = SubAgentTxn(parent_txn, tools)
        result = await sat.run_step("explore", "search code")

        assert result.success
        assert "search code" in result.summary

    @pytest.mark.asyncio
    async def test_run_sequential(self, parent_txn, tools):
        call_order = []

        async def tracking_agent(config, tools, task, prior_results, txn):
            call_order.append(config.type)
            return f"Done: {task}"

        sat = SubAgentTxn(parent_txn, tools, run_agent_fn=tracking_agent)
        results = await sat.run_sequential([
            ("explore", "find stuff"),
            ("implement", "make changes"),
            ("test", "run tests"),
        ])

        assert len(results) == 3
        assert all(r.success for r in results)
        assert call_order == ["explore", "implement", "test"]

    @pytest.mark.asyncio
    async def test_run_sequential_stop_on_failure(self, parent_txn, tools):
        call_count = [0]

        async def sometimes_fails(config, tools, task, prior_results, txn):
            call_count[0] += 1
            if config.type == "implement":
                raise RuntimeError("implementation failed")
            return "ok"

        sat = SubAgentTxn(parent_txn, tools, run_agent_fn=sometimes_fails)
        results = await sat.run_sequential(
            [
                ("explore", "search"),
                ("implement", "edit"),
                ("test", "verify"),
            ],
            stop_on_failure=True,
        )

        assert len(results) == 2  # stopped after implement failure
        assert results[0].success
        assert not results[1].success
        assert call_count[0] == 2  # test never ran

    @pytest.mark.asyncio
    async def test_run_explore_implement_test(self, parent_txn, tools):
        async def mock_agent(config, tools, task, prior_results, txn):
            return f"{config.type}: done"

        sat = SubAgentTxn(parent_txn, tools, run_agent_fn=mock_agent)
        results = await sat.run_explore_implement_test("fix the bug")

        assert len(results) == 3
        assert results[0].agent_type == "explore"
        assert results[1].agent_type == "implement"
        assert results[2].agent_type == "test"

    @pytest.mark.asyncio
    async def test_subtxn_lifecycle(self, parent_txn, tools):
        """Verify subtxn commit/abort on success/failure."""
        committed = []
        aborted = []

        async def track_agent(config, tools, task, prior_results, txn):
            if config.type == "implement":
                raise RuntimeError("fail")
            return "ok"

        sat = SubAgentTxn(parent_txn, tools, run_agent_fn=track_agent)
        results = await sat.run_sequential(
            [("explore", "search"), ("implement", "edit")],
            stop_on_failure=False,
        )

        # First step succeeded (committed), second failed (aborted)
        assert results[0].success
        assert not results[1].success

    @pytest.mark.asyncio
    async def test_prior_results_passed(self, parent_txn, tools):
        received_prior = []

        async def capture_prior(config, tools, task, prior_results, txn):
            received_prior.append(prior_results)
            return "done"

        sat = SubAgentTxn(parent_txn, tools, run_agent_fn=capture_prior)
        await sat.run_sequential([
            ("explore", "step1"),
            ("implement", "step2"),
        ])

        # First step: prior_results from run_sequential starts as empty list []
        assert len(received_prior[0]) == 0
        assert len(received_prior[1]) == 1  # has explore result
