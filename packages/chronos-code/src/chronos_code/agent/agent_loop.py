"""AgentLoop — core LLM ↔ tools loop.

Implements the inner loop: send messages to LLM, parse tool calls,
execute tools, append results, repeat until the LLM stops calling tools.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from typing import Any, Callable

from chronos_code.context.conversation import Conversation, Message, ToolCall
from chronos_code.context.context_manager import ContextManager

logger = logging.getLogger(__name__)


class AgentLoop:
    """Core agent loop: LLM → tool calls → tool results → LLM → ...

    The loop runs until:
    - The LLM produces a response with no tool calls (natural end).
    - Max iterations reached.
    - An error occurs.

    Supports pluggable LLM backends via ``llm_fn`` and tool registries
    via ``tools`` dict.
    """

    def __init__(
        self,
        conversation: Conversation,
        context_manager: ContextManager,
        tools: dict[str, Any],
        llm_fn: Callable[[list[dict[str, Any]], list[dict[str, Any]]], dict[str, Any]],
        tool_schemas: list[dict[str, Any]] | None = None,
        max_iterations: int = 50,
        on_assistant_message: Callable[[Message], None] | None = None,
        on_llm_usage: Callable[[dict[str, Any] | None], None] | None = None,
        on_tool_call: Callable[[ToolCall], None] | None = None,
        on_tool_result: Callable[[ToolCall], None] | None = None,
    ) -> None:
        """Initialize the agent loop.

        Args:
            conversation: The conversation state.
            context_manager: Manages token tracking and compaction.
            tools: Map of tool_name → tool instance (must have .run()).
            llm_fn: Function(messages, tool_schemas) → LLM response dict.
            tool_schemas: OpenAI-style function schemas for the LLM.
            max_iterations: Safety cap on loop iterations.
            on_assistant_message: Callback when assistant produces a message.
            on_tool_result: Callback when a tool produces a result.
        """
        self.conversation = conversation
        self.context_manager = context_manager
        self.tools = tools
        self.llm_fn = llm_fn
        self.tool_schemas = tool_schemas or []
        self.max_iterations = max_iterations
        self._on_assistant_message = on_assistant_message
        self._on_llm_usage = on_llm_usage
        self._on_tool_call = on_tool_call
        self._on_tool_result = on_tool_result

    async def run(self) -> Message:
        """Run the agent loop until completion.

        Returns the final assistant message (the one without tool calls).
        """
        for iteration in range(self.max_iterations):
            # Check if compaction is needed
            if self.context_manager.should_compact():
                logger.info("Context window ~80%% full — compacting.")
                self.context_manager.compact()

            # Call LLM
            messages = self.conversation.to_llm_messages()
            response = await self._call_llm(messages)
            if self._on_llm_usage:
                usage = None
                if isinstance(response, dict):
                    usage = response.get("usage")
                self._on_llm_usage(usage)

            # Parse response
            assistant_msg = self._parse_response(response)
            self.conversation.append(assistant_msg)

            if self._on_assistant_message:
                self._on_assistant_message(assistant_msg)

            # If no tool calls, we're done
            if not assistant_msg.tool_calls:
                return assistant_msg

            # Execute tool calls
            for tool_call in assistant_msg.tool_calls:
                if self._on_tool_call:
                    self._on_tool_call(tool_call)
                result = await self._execute_tool(tool_call)
                tool_call.result = result

                # Append tool result message
                self.conversation.append_tool_result(
                    tool_call_id=tool_call.id,
                    content=result,
                )

                if self._on_tool_result:
                    self._on_tool_result(tool_call)

        # Max iterations reached
        final = Message(
            role="assistant",
            content="[Max iterations reached. Stopping agent loop.]",
            turn=self.conversation.turn,
        )
        self.conversation.append(final)
        return final

    async def _call_llm(
        self, messages: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Call the LLM function."""
        try:
            result = self.llm_fn(messages, self.tool_schemas)
            if inspect.isawaitable(result):
                result = await result
            return result
        except Exception as e:
            logger.error("LLM call failed: %s", e)
            raise

    def _parse_response(self, response: dict[str, Any]) -> Message:
        """Parse an LLM response into a Message."""
        content = response.get("content", "")
        tool_calls_raw = response.get("tool_calls")

        tool_calls: list[ToolCall] | None = None
        if tool_calls_raw:
            tool_calls = []
            for tc in tool_calls_raw:
                func = tc.get("function", {})
                args = func.get("arguments", "{}")
                if isinstance(args, str):
                    try:
                        args = json.loads(args)
                    except json.JSONDecodeError:
                        args = {"raw": args}

                tool_calls.append(
                    ToolCall(
                        id=tc.get("id", ""),
                        name=func.get("name", ""),
                        arguments=args,
                    )
                )

        return Message(
            role="assistant",
            content=content or "",
            tool_calls=tool_calls,
            turn=self.conversation.turn,
        )

    async def _execute_tool(self, tool_call: ToolCall) -> str:
        """Execute a single tool call and return the result string."""
        tool = self.tools.get(tool_call.name)
        if tool is None:
            return f"Error: unknown tool '{tool_call.name}'"

        start = time.monotonic()
        try:
            run_fn = getattr(tool, "run")
            if inspect.iscoroutinefunction(run_fn):
                result = await run_fn(**tool_call.arguments)
            else:
                result = await asyncio.to_thread(run_fn, **tool_call.arguments)
            # In case a sync tool returns an awaitable.
            if inspect.isawaitable(result):
                result = await result
        except Exception as e:
            result = f"Error executing {tool_call.name}: {e}"
            logger.error("Tool %s failed: %s", tool_call.name, e)

        tool_call.duration_ms = int((time.monotonic() - start) * 1000)
        return str(result)
