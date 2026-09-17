"""Run an inspectable trajectory through verl's real tool-agent state machine."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from omegaconf import OmegaConf
from verl.experimental.agent_loop.tool_agent_loop import AgentState
from verl.tools.schemas import OpenAIFunctionToolSchema

from .agent_loop import ChronosDatabaseAgentLoop
from .tool import ChronosTicketTool


class ScriptedTicketLoop(ChronosDatabaseAgentLoop):
    """Replace model generation with fixed actions while retaining verl dispatch."""

    def __init__(
        self, tool: Any, actions: list[Any], stop: asyncio.Event | None = None
    ):
        # No model, tokenizer, or Ray server is constructed for this CPU-checkable
        # trajectory. ToolAgentLoop.run and _call_tool remain the upstream code.
        self.tools = {"tickets": tool}
        self.tool_schemas = [tool.tool_schema.model_dump()]
        self.rollout_config = SimpleNamespace(full_determinism=False)
        self.response_length = 100
        self.max_tool_response_length = 4096
        self.tool_response_truncate_side = "right"
        self.actions = actions
        self.stop = stop
        self.observed: list[str] = []

    async def process_multi_modal_info(self, messages: Any) -> dict[str, Any]:
        return {}

    def _get_mm_processor_kwargs(self, audios: Any) -> dict[str, Any]:
        return {}

    async def _handle_pending_state(self, data: Any, params: Any) -> AgentState:
        data.prompt_ids = [1]
        data.response_mask = []
        data.test_step = 0
        return AgentState.GENERATING

    async def _handle_generating_state(self, data: Any, params: Any) -> AgentState:
        if self.stop:
            self.stop.set()
            await asyncio.Event().wait()
        if data.test_step == len(self.actions):
            return AgentState.TERMINATED
        action = self.actions[data.test_step]
        if isinstance(action, BaseException):
            raise action
        response, reward, _ = await self._call_tool(
            SimpleNamespace(name="tickets", arguments=json.dumps(action)),
            data.tools_kwargs,
            data,
        )
        self.observed.append(response.text)
        if reward != 0:
            raise RuntimeError("ticket tool must score only the final trajectory state")
        data.test_step += 1
        data.prompt_ids.append(2)
        data.response_mask.append(1)
        await asyncio.sleep(0)
        return AgentState.GENERATING


def configured_tool(**kwargs: Any) -> ChronosTicketTool:
    """Build the tutorial tool from its environment-resolved verl config."""

    config_path = Path(__file__).parents[1] / "tool_config.yaml"
    config = OmegaConf.to_container(OmegaConf.load(config_path), resolve=True)
    spec = config["tools"][0]
    return ChronosTicketTool(
        spec["config"],
        OpenAIFunctionToolSchema.model_validate(spec["tool_schema"]),
        **kwargs,
    )


async def run_demo(**tool_kwargs: Any) -> tuple[Any, ScriptedTicketLoop]:
    tool = configured_tool(**tool_kwargs)
    loop = ScriptedTicketLoop(
        tool,
        [
            {"operation": "list"},
            {"operation": "set_priority", "ticket_id": 101, "priority": "high"},
            {"operation": "list"},
        ],
    )
    output = await loop.run({}, raw_prompt=[])
    return output, loop


def main() -> None:
    output, loop = asyncio.run(run_demo())
    print(
        json.dumps(
            {
                "reward": output.reward_score,
                "trajectory": output.extra_fields,
                "tool_results": [json.loads(value) for value in loop.observed],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
