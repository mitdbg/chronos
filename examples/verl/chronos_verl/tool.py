"""A verl BaseTool that attaches each call to its rollout's database episode."""

from contextlib import asynccontextmanager
import json
from uuid import uuid4

from verl.tools.base_tool import BaseTool
from verl.tools.schemas import ToolResponse

from .sandbox import DatabaseEpisode, E2BDatabaseEpisode


class ChronosTicketTool(BaseTool):
    def __init__(
        self,
        config,
        tool_schema,
        *,
        e2b_adapter=None,
        e2b_prepare=None,
        e2b_sandbox_kwargs=None,
    ):
        super().__init__(config, tool_schema)
        self.backend = config.get("backend", "local")
        self.url = config.get("database_url")
        self.checkpoint = config.get("checkpoint", "tickets_baseline")
        self.e2b = dict(config.get("e2b") or {})
        self.e2b_adapter = e2b_adapter
        self.e2b_prepare = e2b_prepare
        self.e2b_sandbox_kwargs = dict(e2b_sandbox_kwargs or {})
        if self.backend == "local" and not self.url:
            raise ValueError("local backend requires database_url")
        if self.backend == "e2b":
            required = {"template", "workspace", "database"}
            missing = sorted(required.difference(self.e2b))
            if missing:
                raise ValueError("e2b backend requires " + ", ".join(missing))
        elif self.backend != "local":
            raise ValueError("backend must be local or e2b")
        self.episodes = {}
        self.calls = {}

    def _new_episode(self):
        if self.backend == "local":
            return DatabaseEpisode(self.url, self.checkpoint)
        return E2BDatabaseEpisode(
            template=self.e2b["template"],
            workspace=self.e2b["workspace"],
            database=self.e2b["database"],
            from_branch=self.e2b.get("from_branch", "main"),
            filesystem=self.e2b.get("filesystem", "read_write"),
            ttl_seconds=int(self.e2b.get("ttl_seconds", 3600)),
            timeout=int(self.e2b.get("timeout", 3600)),
            mountpoint=self.e2b.get("mountpoint", "/mnt/chronos"),
            command_timeout=int(self.e2b.get("command_timeout", 60)),
            adapter=self.e2b_adapter,
            prepare=self.e2b_prepare,
            sandbox_kwargs=self.e2b_sandbox_kwargs,
        )

    @asynccontextmanager
    async def episode(self):
        async with self._new_episode() as database:
            token = uuid4().hex
            self.episodes[token] = database
            try:
                yield token, database
            finally:
                self.episodes.pop(token, None)
                for call in [
                    key for key, value in self.calls.items() if value is database
                ]:
                    self.calls.pop(call, None)

    async def create(self, instance_id=None, **kwargs):
        # The agent loop supplies this token, not the model's function arguments.
        token = kwargs.get("create_kwargs", {}).get("chronos_episode")
        if token not in self.episodes:
            raise ValueError("ChronosTicketTool requires ChronosDatabaseAgentLoop")
        call = instance_id or uuid4().hex
        if call in self.calls:
            raise ValueError("tool instance already exists")
        self.calls[call] = self.episodes[token]
        return call, ToolResponse()

    async def execute(self, instance_id, parameters, **kwargs):
        unknown = set(parameters) - {"operation", "ticket_id", "priority"}
        if unknown:
            raise ValueError("unknown tool parameters")
        database = self.calls[instance_id]
        rows = await database.execute(
            parameters.get("operation"),
            parameters.get("ticket_id"),
            parameters.get("priority"),
        )
        return ToolResponse(text=json.dumps(rows)), 0.0, {}

    async def calc_reward(self, instance_id, **kwargs):
        return await self.calls[instance_id].score()

    async def release(self, instance_id, **kwargs):
        # Current verl releases after each call. The episode survives until run ends.
        self.calls.pop(instance_id, None)
