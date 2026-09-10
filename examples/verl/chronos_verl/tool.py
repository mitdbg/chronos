"""A verl BaseTool that attaches each call to its rollout's database episode."""
from contextlib import asynccontextmanager
import json
from uuid import uuid4

from verl.tools.base_tool import BaseTool
from verl.tools.schemas import ToolResponse

from .sandbox import DatabaseEpisode


class ChronosTicketTool(BaseTool):
    def __init__(self, config, tool_schema):
        super().__init__(config, tool_schema)
        self.url = config["database_url"]
        self.checkpoint = config.get("checkpoint", "tickets_baseline")
        self.episodes = {}
        self.calls = {}

    @asynccontextmanager
    async def episode(self):
        async with DatabaseEpisode(self.url, self.checkpoint) as database:
            token = uuid4().hex
            self.episodes[token] = database
            try:
                yield token, database
            finally:
                self.episodes.pop(token, None)
                for call in [key for key, value in self.calls.items() if value is database]:
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
            parameters.get("operation"), parameters.get("ticket_id"), parameters.get("priority")
        )
        return ToolResponse(text=json.dumps(rows)), 0.0, {}

    async def calc_reward(self, instance_id, **kwargs):
        return await self.calls[instance_id].score()

    async def release(self, instance_id, **kwargs):
        # Current verl releases after each call. The episode survives until run ends.
        self.calls.pop(instance_id, None)
