"""Sub-agents — Explore, Implement, Test agent configurations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from chronos_code.agent.prompts import (
    EXPLORE_AGENT_PROMPT,
    IMPLEMENT_AGENT_PROMPT,
    TEST_AGENT_PROMPT,
)


@dataclass
class SubAgentConfig:
    """Configuration for a sub-agent."""

    name: str
    type: str  # "explore", "implement", "test"
    system_prompt: str
    allowed_tools: list[str]
    read_only: bool = False


EXPLORE_CONFIG = SubAgentConfig(
    name="explore",
    type="explore",
    system_prompt=EXPLORE_AGENT_PROMPT,
    allowed_tools=["read_file", "glob_search", "ripgrep", "bash", "sqlite"],
    read_only=True,
)

IMPLEMENT_CONFIG = SubAgentConfig(
    name="implement",
    type="implement",
    system_prompt=IMPLEMENT_AGENT_PROMPT,
    allowed_tools=[
        "read_file", "edit_file", "write_file", "bash",
        "ripgrep", "glob_search", "memory", "sqlite",
    ],
    read_only=False,
)

TEST_CONFIG = SubAgentConfig(
    name="test",
    type="test",
    system_prompt=TEST_AGENT_PROMPT,
    allowed_tools=["bash", "read_file", "glob_search", "ripgrep", "sqlite"],
    read_only=True,
)

SUB_AGENT_CONFIGS = {
    "explore": EXPLORE_CONFIG,
    "implement": IMPLEMENT_CONFIG,
    "test": TEST_CONFIG,
}


def get_sub_agent_config(agent_type: str) -> SubAgentConfig:
    """Get the configuration for a sub-agent type."""
    config = SUB_AGENT_CONFIGS.get(agent_type)
    if config is not None:
        return config

    # LLM-planned subtransaction types are allowed; unknown types get a
    # generic implementation profile with broad tool access.
    return SubAgentConfig(
        name=agent_type,
        type=agent_type,
        system_prompt=(
            "You are a Chronos-Code sub-agent running an LLM-planned "
            f"subtransaction of type '{agent_type}'. "
            "Focus only on the provided task and produce a concise result."
        ),
        allowed_tools=[
            "read_file",
            "edit_file",
            "write_file",
            "glob_search",
            "glob",
            "ripgrep",
            "bash",
            "memory",
            "sqlite",
            "todo_write",
            "ask_user",
            "task",
            "launch_task",
        ],
        read_only=False,
    )


def filter_tools(
    all_tools: dict[str, Any], allowed: list[str]
) -> dict[str, Any]:
    """Filter tools to only those allowed for a sub-agent."""
    return {name: tool for name, tool in all_tools.items() if name in allowed}
