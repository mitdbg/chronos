"""LLM wrapper — litellm-based completion with tool support.

Provides a unified interface for calling any LLM provider through
litellm. Handles tool schema formatting and response parsing.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable

from janus_code.config import Config

logger = logging.getLogger(__name__)


def _coerce_usage(response: Any) -> dict[str, Any] | None:
    """Extract normalized token usage, including cache read tokens when available."""
    usage_obj = getattr(response, "usage", None)
    if usage_obj is None and isinstance(response, dict):
        usage_obj = response.get("usage")
    if usage_obj is None:
        return None

    def _dig(obj: Any, *keys: str) -> Any:
        cur = obj
        for key in keys:
            if cur is None:
                return None
            if isinstance(cur, dict):
                cur = cur.get(key)
            else:
                cur = getattr(cur, key, None)
        return cur

    prompt_tokens = _dig(usage_obj, "prompt_tokens")
    if prompt_tokens is None:
        prompt_tokens = _dig(usage_obj, "input_tokens")

    completion_tokens = _dig(usage_obj, "completion_tokens")
    if completion_tokens is None:
        completion_tokens = _dig(usage_obj, "output_tokens")

    total_tokens = _dig(usage_obj, "total_tokens")
    if total_tokens is None:
        p = int(prompt_tokens or 0)
        c = int(completion_tokens or 0)
        total_tokens = p + c

    cached_prompt_tokens = (
        _dig(usage_obj, "prompt_tokens_details", "cached_tokens")
        or _dig(usage_obj, "input_token_details", "cache_read_input_tokens")
        or _dig(usage_obj, "cache_read_input_tokens")
        or 0
    )
    try:
        cached_prompt_tokens = int(cached_prompt_tokens or 0)
    except Exception:
        cached_prompt_tokens = 0

    try:
        prompt_tokens_int = int(prompt_tokens or 0)
    except Exception:
        prompt_tokens_int = 0
    uncached_prompt_tokens = max(prompt_tokens_int - cached_prompt_tokens, 0)

    try:
        completion_tokens_int = int(completion_tokens or 0)
    except Exception:
        completion_tokens_int = 0
    try:
        total_tokens_int = int(total_tokens or (prompt_tokens_int + completion_tokens_int))
    except Exception:
        total_tokens_int = prompt_tokens_int + completion_tokens_int

    return {
        "prompt_tokens": prompt_tokens_int,
        "completion_tokens": completion_tokens_int,
        "total_tokens": total_tokens_int,
        "cached_prompt_tokens": cached_prompt_tokens,
        "uncached_prompt_tokens": uncached_prompt_tokens,
    }


def build_llm_fn(config: Config) -> Callable:
    """Build an LLM function backed by litellm.

    Returns a callable: ``fn(messages, tool_schemas) -> response_dict``

    The response dict has:
    - ``content``: str — the assistant's text content
    - ``tool_calls``: list[dict] | None — tool calls in OpenAI format

    Raises ``ImportError`` if litellm is not installed.
    """
    try:
        import litellm
    except ImportError as e:
        raise ImportError(
            "litellm is required for LLM calls. Install it with: "
            "pip install litellm"
        ) from e

    async def llm_fn(
        messages: list[dict[str, Any]],
        tool_schemas: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Call the LLM and return a parsed response dict."""
        kwargs: dict[str, Any] = {
            "model": config.model,
            "messages": messages,
            "temperature": config.temperature,
            "max_tokens": config.max_tokens,
        }

        if tool_schemas:
            kwargs["tools"] = tool_schemas
            kwargs["tool_choice"] = "auto"

        logger.debug(
            "LLM call: model=%s, messages=%d, tools=%d",
            config.model,
            len(messages),
            len(tool_schemas),
        )

        # Run the sync litellm call in a worker thread so AgentLoop stays async.
        response = await asyncio.to_thread(litellm.completion, **kwargs)
        choice = response.choices[0]
        message = choice.message

        # Parse tool calls
        tool_calls = None
        if message.tool_calls:
            tool_calls = []
            for tc in message.tool_calls:
                tool_calls.append({
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                })

        usage = _coerce_usage(response)

        return {
            "content": message.content or "",
            "tool_calls": tool_calls,
            "usage": usage,
        }

    return llm_fn


def build_tool_schemas(tools: dict[str, Any]) -> list[dict[str, Any]]:
    """Build OpenAI-format tool schemas from tool instances.

    Each tool must have a ``schema()`` method or ``name``, ``description``,
    and ``parameters`` attributes.
    """
    schemas: list[dict[str, Any]] = []
    for name, tool in tools.items():
        if hasattr(tool, "schema"):
            schema = tool.schema()
        elif hasattr(tool, "description") and hasattr(tool, "parameters"):
            schema = {
                "type": "function",
                "function": {
                    "name": name,
                    "description": getattr(tool, "description", ""),
                    "parameters": getattr(tool, "parameters", {}),
                },
            }
        else:
            # Minimal schema
            schema = {
                "type": "function",
                "function": {
                    "name": name,
                    "description": f"Tool: {name}",
                    "parameters": {"type": "object", "properties": {}},
                },
            }
        schemas.append(schema)
    return schemas


class MockLLM:
    """Mock LLM for deterministic testing.

    Pre-scripted responses allow testing the full agent loop
    without real API calls.
    """

    def __init__(self, responses: list[dict[str, Any]] | None = None) -> None:
        """Initialize with a list of pre-scripted responses.

        Each response is a dict with:
        - ``content``: str
        - ``tool_calls``: list[dict] | None
        """
        self._responses = list(responses or [])
        self._call_count = 0

    def __call__(
        self,
        messages: list[dict[str, Any]],
        tool_schemas: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Return the next scripted response."""
        if self._call_count >= len(self._responses):
            return {
                "content": "[MockLLM: no more scripted responses]",
                "tool_calls": None,
            }
        resp = self._responses[self._call_count]
        self._call_count += 1
        return resp

    @property
    def call_count(self) -> int:
        return self._call_count

    def add_response(self, content: str = "", tool_calls: list | None = None) -> None:
        """Add a scripted response."""
        self._responses.append({
            "content": content,
            "tool_calls": tool_calls,
        })
