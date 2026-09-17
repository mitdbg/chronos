"""CPU integration tests using verl's real ToolAgentLoop and BaseTool dispatch.

Only model generation/tokenization is replaced with scripted actions. These tests
do not measure model quality, run Ray workers, or perform a training update.
"""

import asyncio
import json

import pytest
from omegaconf import OmegaConf
from verl.tools.schemas import OpenAIFunctionToolSchema

from chronos_core.branching import ChronosBranchContext
from chronos_verl.demo import ScriptedTicketLoop
from chronos_verl.prepare import initialize
from chronos_verl.sandbox import E2BDatabaseEpisode
from chronos_verl.tool import ChronosTicketTool


@pytest.fixture(params=["sqlite", "postgres"])
def tool(tmp_path, monkeypatch, request):
    from pathlib import Path

    if request.param == "postgres":
        import os

        url = os.environ.get("CHRONOS_VERL_TEST_POSTGRES_DSN")
        if not url:
            pytest.skip("set CHRONOS_VERL_TEST_POSTGRES_DSN to a disposable database")
        import psycopg

        # Explicit test-only DSN. Never infer a production database URL.
        with psycopg.connect(url, autocommit=True) as conn:
            conn.execute("DROP SCHEMA public CASCADE")
            conn.execute("CREATE SCHEMA public")
    else:
        url = f"sqlite:///{tmp_path / 'tickets.sqlite'}"
    initialize(url)
    monkeypatch.setenv("CHRONOS_DATABASE_URL", url)
    config = OmegaConf.to_container(
        OmegaConf.load(Path(__file__).with_name("tool_config.yaml")), resolve=True
    )
    spec = config["tools"][0]
    return ChronosTicketTool(
        spec["config"], OpenAIFunctionToolSchema.model_validate(spec["tool_schema"])
    )


def assert_clean(tool):
    assert not tool.calls
    assert not tool.episodes
    ctx = ChronosBranchContext.connect(tool.url)
    try:
        assert [b.branch_id for b in ctx.list_branches()] == ["main"]
        with ctx.checkout_checkpoint("tickets_baseline") as baseline:
            assert baseline.query("SELECT priority FROM tickets ORDER BY id") == [
                {"priority": "normal"},
                {"priority": "normal"},
            ]
        with ctx.checkout("main") as main:
            assert main.query("SELECT priority FROM tickets ORDER BY id") == [
                {"priority": "normal"},
                {"priority": "normal"},
            ]
    finally:
        ctx.close()


@pytest.mark.asyncio
async def test_parallel_rollouts_and_reset(tool):
    for _ in range(3):
        loops = [
            ScriptedTicketLoop(
                tool,
                [
                    {"operation": "list"},
                    {"operation": "set_priority", "ticket_id": 101, "priority": value},
                    {"operation": "list"},
                ],
            )
            for value in ("high", "low", "high", "normal")
        ]
        results = await asyncio.gather(*(loop.run({}, raw_prompt=[]) for loop in loops))
        for loop in loops:
            for response in loop.observed:
                assert not response.startswith("Error"), response
        assert [result.reward_score for result in results] == [1, 0, 1, 0], [
            loop.observed for loop in loops
        ]
        for loop, value in zip(loops, ("high", "low", "high", "normal")):
            assert json.loads(loop.observed[0])[0]["priority"] == "normal"
            assert json.loads(loop.observed[2])[0]["priority"] == value, loop.observed
        assert_clean(tool)


@pytest.mark.asyncio
async def test_model_failure_and_cancellation_cleanup(tool):
    loop = ScriptedTicketLoop(
        tool,
        [
            {"operation": "set_priority", "ticket_id": 101, "priority": "high"},
            RuntimeError("model server failed"),
        ],
    )
    with pytest.raises(RuntimeError, match="model server failed"):
        await loop.run({}, raw_prompt=[])
    assert_clean(tool)
    started = asyncio.Event()
    task = asyncio.create_task(
        ScriptedTicketLoop(tool, [], stop=started).run({}, raw_prompt=[])
    )
    await asyncio.wait_for(started.wait(), 10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert_clean(tool)


@pytest.mark.asyncio
async def test_tool_errors_and_no_model_chosen_branch(tool):
    loop = ScriptedTicketLoop(
        tool,
        [
            {
                "operation": "set_priority",
                "ticket_id": 101,
                "priority": "high",
                "chronos_episode": "main",
            },
            {
                "operation": "set_priority",
                "ticket_id": "101 OR 1=1",
                "priority": "high",
            },
            {"operation": "list"},
        ],
    )
    output = await loop.run(
        {},
        raw_prompt=[],
        tools_kwargs={"tickets": {"create_kwargs": {"chronos_episode": "main"}}},
    )
    assert output.reward_score == 0
    assert all("Error executing" in text for text in loop.observed[:2])
    assert json.loads(loop.observed[2])[0]["priority"] == "normal"
    assert_clean(tool)
    with pytest.raises(ValueError, match="requires ChronosDatabaseAgentLoop"):
        await tool.create(create_kwargs={"chronos_episode": "main"})


@pytest.mark.asyncio
async def test_tool_configuration_selects_e2b_episode(monkeypatch):
    from pathlib import Path

    monkeypatch.setenv("CHRONOS_VERL_BACKEND", "e2b")
    monkeypatch.delenv("CHRONOS_DATABASE_URL", raising=False)
    config = OmegaConf.to_container(
        OmegaConf.load(Path(__file__).with_name("tool_config.yaml")),
        resolve=True,
    )
    spec = config["tools"][0]
    tool = ChronosTicketTool(
        spec["config"],
        OpenAIFunctionToolSchema.model_validate(spec["tool_schema"]),
    )
    episode = tool._new_episode()
    try:
        assert isinstance(episode, E2BDatabaseEpisode)
        assert episode.template == "chronos-agent"
        assert episode.workspace == "training"
        assert episode.database == "tickets"
        assert episode.from_branch == "main"
    finally:
        await episode.close()
