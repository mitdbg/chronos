"""Unit tests for the verl episode that executes inside E2B."""

from __future__ import annotations

import asyncio
import json
import re
import threading
from types import SimpleNamespace

import pytest

from chronos_verl.sandbox import E2BDatabaseEpisode


class FakeBranchSandbox:
    def __init__(self) -> None:
        self.sandbox = SimpleNamespace(sandbox_id="sandbox-42")
        self.tickets = {
            101: ["Payment outage", "normal"],
            102: ["Documentation typo", "normal"],
        }
        self.calls = []
        self.audit = []
        self.started = False
        self.closed = False
        self.block_queries = False
        self.query_started = threading.Event()
        self.release_query = threading.Event()

    def start(self):
        self.started = True
        return self

    def run(self, command, **kwargs):
        self.calls.append((command, kwargs))
        envs = kwargs.get("envs") or {}
        if command.startswith("printf"):
            self.audit.append(json.loads(envs["CHRONOS_TOOL_EVENT"]))
            return SimpleNamespace(exit_code=0, stdout="", stderr="")
        sql = envs["CHRONOS_SQL"]
        if self.block_queries and sql.startswith("SELECT"):
            self.query_started.set()
            if not self.release_query.wait(5):
                raise RuntimeError("test did not release blocked query")
        if sql.startswith("UPDATE"):
            match = re.search(r"priority = '([^']+)' WHERE id = ([0-9]+)", sql)
            assert match
            self.tickets[int(match.group(2))][1] = match.group(1)
            return SimpleNamespace(exit_code=0, stdout="", stderr="")
        selected = self.tickets.items()
        match = re.search(r"WHERE id = ([0-9]+)", sql)
        if match:
            ticket_id = int(match.group(1))
            selected = [(ticket_id, self.tickets[ticket_id])]
        lines = ["id,subject,priority"]
        lines.extend(
            f'{ticket_id},"{value[0]}",{value[1]}' for ticket_id, value in selected
        )
        return SimpleNamespace(
            exit_code=0,
            stdout="\n".join(lines) + "\n",
            stderr="",
        )

    def close(self):
        self.closed = True


class FakeAdapter:
    def __init__(self) -> None:
        self.kwargs = None
        self.rollout = FakeBranchSandbox()

    def branch(self, **kwargs):
        self.kwargs = kwargs
        return self.rollout


@pytest.mark.asyncio
async def test_e2b_episode_executes_and_scores_inside_branch():
    adapter = FakeAdapter()
    prepare = object()
    sandbox_kwargs = {"api_url": "http://127.0.0.1:50001"}
    episode = E2BDatabaseEpisode(
        template="chronos-agent",
        workspace="training",
        database="tickets",
        from_branch="checkpoint",
        filesystem="read_write",
        ttl_seconds=900,
        timeout=600,
        adapter=adapter,
        sandbox_kwargs=sandbox_kwargs,
        prepare=prepare,
    )

    async with episode:
        assert await episode.execute("list") == [
            {"id": 101, "subject": "Payment outage", "priority": "normal"},
            {"id": 102, "subject": "Documentation typo", "priority": "normal"},
        ]
        assert await episode.execute("set_priority", 101, "high") == [
            {"id": 101, "subject": "Payment outage", "priority": "high"}
        ]
        assert await episode.score() == 1.0
        assert episode.trajectory_metadata() == {
            "chronos_branch": episode.branch,
            "chronos_backend": "e2b",
            "chronos_workspace": "training",
            "e2b_sandbox_id": "sandbox-42",
        }

    assert adapter.rollout.started
    assert adapter.rollout.closed
    assert adapter.kwargs == {
        "template": "chronos-agent",
        "workspace": "training",
        "branch_id": episode.branch,
        "from_branch": "checkpoint",
        "databases": {"tickets": "CHRONOS_DATABASE_URL"},
        "filesystem": "read_write",
        "ttl_seconds": 900,
        "timeout": 600,
        "mountpoint": "/mnt/chronos",
        "metadata": {"verl_episode": episode.branch},
        "sandbox_kwargs": sandbox_kwargs,
        "prepare": prepare,
    }
    assert [event["operation"] for event in adapter.rollout.audit] == [
        "list",
        "set_priority",
        "list",
    ]


@pytest.mark.asyncio
async def test_e2b_episode_without_filesystem_skips_audit_log():
    adapter = FakeAdapter()
    async with E2BDatabaseEpisode(
        template="chronos-agent",
        workspace="training",
        database="tickets",
        filesystem="none",
        adapter=adapter,
    ) as episode:
        await episode.execute("list")
    assert adapter.rollout.audit == []


@pytest.mark.asyncio
async def test_e2b_episode_validates_before_tool_execution():
    adapter = FakeAdapter()
    async with E2BDatabaseEpisode(
        template="chronos-agent",
        workspace="training",
        database="tickets",
        adapter=adapter,
    ) as episode:
        with pytest.raises(ValueError, match="integer ticket_id"):
            await episode.execute("set_priority", "101", "high")
    assert adapter.rollout.calls == []


@pytest.mark.asyncio
async def test_cancelled_tool_call_drains_before_sandbox_cleanup():
    adapter = FakeAdapter()
    episode = E2BDatabaseEpisode(
        template="chronos-agent",
        workspace="training",
        database="tickets",
        adapter=adapter,
    )
    await episode.__aenter__()
    adapter.rollout.block_queries = True
    query = asyncio.create_task(episode.execute("list"))
    started = await asyncio.to_thread(adapter.rollout.query_started.wait, 2)
    assert started
    query.cancel()
    adapter.rollout.release_query.set()
    with pytest.raises(asyncio.CancelledError):
        await query
    await episode.close()
    assert adapter.rollout.closed


@pytest.mark.asyncio
async def test_rollout_failure_still_closes_e2b_sandbox():
    adapter = FakeAdapter()
    with pytest.raises(RuntimeError, match="model failed"):
        async with E2BDatabaseEpisode(
            template="chronos-agent",
            workspace="training",
            database="tickets",
            adapter=adapter,
        ):
            raise RuntimeError("model failed")
    assert adapter.rollout.closed
