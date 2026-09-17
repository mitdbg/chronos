"""One branch per ToolAgentLoop.run, including GRPO samples of the same prompt."""

from copy import deepcopy

from verl.experimental.agent_loop.tool_agent_loop import ToolAgentLoop


class ChronosDatabaseAgentLoop(ToolAgentLoop):
    async def run(self, sampling_params, priority=0, **kwargs):
        tool = self.tools["tickets"]
        async with tool.episode() as (token, database):
            tool_kwargs = deepcopy(kwargs.get("tools_kwargs") or {})
            tool_kwargs.setdefault("tickets", {}).setdefault("create_kwargs", {})[
                "chronos_episode"
            ] = token
            kwargs["tools_kwargs"] = tool_kwargs
            output = await super().run(sampling_params, priority=priority, **kwargs)
            # Evaluate before deletion, once per trajectory, never per tool call.
            output.reward_score = await database.score()
            output.extra_fields["chronos_db_reward"] = output.reward_score
            output.extra_fields.update(database.trajectory_metadata())
            return output
