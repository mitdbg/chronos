"""ContextManager — token tracking, context window management, compaction."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

import tiktoken

from janus_code.config import Config
from janus_code.context.conversation import Conversation, Message
from janus_code.context.memory_loader import MemoryLoader


# Compaction summary template (inspired by Claude Code's context compaction)
COMPACTION_TEMPLATE = """\
# Continuation Summary

## Task Overview
{task_overview}

## Current State
{current_state}

## Key Files
{key_files}

## Important Decisions
{decisions}

## Next Steps
{next_steps}
"""


class ContextManager:
    """Manages the context window — tracking tokens, triggering compaction.

    When the context window approaches the threshold (default 80%),
    generates a structured summary and replaces older messages with
    the summary + system prompt + memory.
    """

    def __init__(
        self,
        config: Config,
        memory_loader: MemoryLoader,
        conversation: Conversation,
        summarize_fn: Callable[[list[Message]], str] | None = None,
    ) -> None:
        self.config = config
        self.memory_loader = memory_loader
        self.conversation = conversation
        self._summarize_fn = summarize_fn

        # Token counting
        try:
            self._encoding = tiktoken.encoding_for_model("gpt-4")
        except Exception:
            self._encoding = tiktoken.get_encoding("cl100k_base")

    def count_tokens(self, text: str) -> int:
        """Count tokens in a string."""
        return len(self._encoding.encode(text))

    def count_message_tokens(self, message: Message) -> int:
        """Count tokens in a single message."""
        total = self.count_tokens(message.content)
        if message.tool_calls:
            for tc in message.tool_calls:
                total += self.count_tokens(tc.name)
                total += self.count_tokens(json.dumps(tc.arguments))
                if tc.result:
                    total += self.count_tokens(tc.result)
        return total + 4  # message overhead tokens

    def total_tokens(self) -> int:
        """Count total tokens across all messages."""
        return sum(
            self.count_message_tokens(m) for m in self.conversation.messages
        )

    def tokens_remaining(self) -> int:
        """Tokens remaining before hitting the max."""
        return max(0, self.config.max_context_tokens - self.total_tokens())

    def should_compact(self) -> bool:
        """Check if compaction should be triggered."""
        used = self.total_tokens()
        threshold = int(
            self.config.max_context_tokens * self.config.compaction_threshold
        )
        return used >= threshold

    def compact(self, working_dir: Path | None = None) -> str | None:
        """Perform context compaction if needed.

        1. Generate a structured summary of older messages.
        2. Write summary to session notes file.
        3. Replace conversation with: system prompt + memory + summary + recent turns.

        Returns the summary text if compaction was performed, None otherwise.
        """
        if not self.should_compact():
            return None

        messages = self.conversation.messages

        # Generate summary
        if self._summarize_fn:
            summary = self._summarize_fn(messages)
        else:
            summary = self._default_summary(messages)

        # Write summary to session notes if working_dir provided
        if working_dir:
            notes_dir = working_dir / ".janus-code" / "sessions"
            notes_dir.mkdir(parents=True, exist_ok=True)
            summary_file = notes_dir / "latest_summary.md"
            summary_file.write_text(summary)

        # Rebuild context: system + memory + summary + last few turns
        system_msgs = [m for m in messages if m.role == "system"]
        recent = self.conversation.get_last_n_turns(3)

        self.conversation.clear()

        # Re-add system messages
        for m in system_msgs:
            self.conversation.append(m)

        # Re-load memory
        memory_text = self.memory_loader.load_all()
        if memory_text:
            self.conversation.append_system(
                f"[Context from memory]\n\n{memory_text}"
            )

        # Add summary
        self.conversation.append_system(
            f"[Continuation Summary — older context was compacted]\n\n{summary}"
        )

        # Add recent messages (excluding system)
        for m in recent:
            if m.role != "system":
                self.conversation.append(m)

        return summary

    def _default_summary(self, messages: list[Message]) -> str:
        """Generate a basic summary without LLM (fallback)."""
        # Count by role
        user_msgs = [m for m in messages if m.role == "user"]
        assistant_msgs = [m for m in messages if m.role == "assistant"]
        tool_msgs = [m for m in messages if m.role == "tool"]

        # Collect tool names used
        tool_names: set[str] = set()
        for m in assistant_msgs:
            if m.tool_calls:
                for tc in m.tool_calls:
                    tool_names.add(tc.name)

        # Last user request
        last_user = user_msgs[-1].content[:500] if user_msgs else "(none)"

        return COMPACTION_TEMPLATE.format(
            task_overview=f"User's last request: {last_user}",
            current_state=(
                f"Processed {len(user_msgs)} user messages, "
                f"{len(assistant_msgs)} assistant messages, "
                f"{len(tool_msgs)} tool results."
            ),
            key_files="(See recent tool calls for file paths)",
            decisions="(Preserved in recent messages below)",
            next_steps="Continue working on the user's request.",
        )
