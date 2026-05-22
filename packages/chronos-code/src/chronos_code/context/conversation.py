"""Conversation state — Message and ToolCall dataclasses."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass
class ToolCall:
    """A single tool invocation within an assistant message."""

    id: str = ""
    name: str = ""
    arguments: dict[str, Any] = field(default_factory=dict)
    result: str | None = None
    duration_ms: int = 0

    def __post_init__(self) -> None:
        if not self.id:
            self.id = f"call_{uuid.uuid4().hex[:12]}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "arguments": self.arguments,
            "result": self.result,
            "duration_ms": self.duration_ms,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ToolCall:
        return cls(
            id=data.get("id", ""),
            name=data.get("name", ""),
            arguments=data.get("arguments", {}),
            result=data.get("result"),
            duration_ms=data.get("duration_ms", 0),
        )


@dataclass
class Message:
    """A single message in the conversation."""

    role: Literal["system", "user", "assistant", "tool"] = "user"
    content: str = ""
    tool_calls: list[ToolCall] | None = None  # assistant msgs only
    tool_call_id: str | None = None  # tool result msgs only
    turn: int = 0
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "role": self.role,
            "content": self.content,
            "turn": self.turn,
            "timestamp": self.timestamp,
        }
        if self.tool_calls:
            d["tool_calls"] = [tc.to_dict() for tc in self.tool_calls]
        if self.tool_call_id:
            d["tool_call_id"] = self.tool_call_id
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Message:
        tool_calls = None
        if "tool_calls" in data and data["tool_calls"]:
            tool_calls = [ToolCall.from_dict(tc) for tc in data["tool_calls"]]
        return cls(
            role=data["role"],
            content=data.get("content", ""),
            tool_calls=tool_calls,
            tool_call_id=data.get("tool_call_id"),
            turn=data.get("turn", 0),
            timestamp=data.get("timestamp", 0.0),
        )

    def to_llm_format(self) -> dict[str, Any]:
        """Convert to the format expected by LLM APIs (OpenAI-style)."""
        msg: dict[str, Any] = {"role": self.role, "content": self.content}
        if self.tool_calls:
            msg["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.name,
                        "arguments": json.dumps(tc.arguments),
                    },
                }
                for tc in self.tool_calls
            ]
        if self.tool_call_id:
            msg["tool_call_id"] = self.tool_call_id
        return msg


class Conversation:
    """Ordered list of messages forming the conversation state.

    Supports append, serialization, token counting, and truncation.
    """

    def __init__(self, messages: list[Message] | None = None) -> None:
        self._messages: list[Message] = messages or []
        self._turn: int = 0

    @property
    def messages(self) -> list[Message]:
        return list(self._messages)

    @property
    def turn(self) -> int:
        return self._turn

    def advance_turn(self) -> int:
        """Advance and return the new turn number."""
        self._turn += 1
        return self._turn

    def append(self, message: Message) -> None:
        """Add a message to the conversation."""
        if message.turn == 0:
            message.turn = self._turn
        self._messages.append(message)

    def append_user(self, content: str) -> Message:
        """Convenience: append a user message."""
        msg = Message(role="user", content=content, turn=self._turn)
        self._messages.append(msg)
        return msg

    def append_assistant(
        self, content: str, tool_calls: list[ToolCall] | None = None
    ) -> Message:
        """Convenience: append an assistant message."""
        msg = Message(
            role="assistant",
            content=content,
            tool_calls=tool_calls,
            turn=self._turn,
        )
        self._messages.append(msg)
        return msg

    def append_tool_result(self, tool_call_id: str, content: str) -> Message:
        """Convenience: append a tool result message."""
        msg = Message(
            role="tool",
            content=content,
            tool_call_id=tool_call_id,
            turn=self._turn,
        )
        self._messages.append(msg)
        return msg

    def append_system(self, content: str) -> Message:
        """Convenience: append a system message."""
        msg = Message(role="system", content=content, turn=self._turn)
        self._messages.append(msg)
        return msg

    def to_llm_messages(self) -> list[dict[str, Any]]:
        """Convert all messages to LLM API format."""
        return [m.to_llm_format() for m in self._messages]

    def to_json(self) -> str:
        """Serialize full conversation to JSON."""
        return json.dumps(
            {
                "turn": self._turn,
                "messages": [m.to_dict() for m in self._messages],
            }
        )

    @classmethod
    def from_json(cls, data: str) -> Conversation:
        """Deserialize conversation from JSON."""
        parsed = json.loads(data)
        msgs = [Message.from_dict(m) for m in parsed.get("messages", [])]
        conv = cls(messages=msgs)
        conv._turn = parsed.get("turn", 0)
        return conv

    def get_last_n_turns(self, n: int) -> list[Message]:
        """Get messages from the last N turns."""
        if not self._messages:
            return []
        min_turn = max(0, self._turn - n + 1)
        return [m for m in self._messages if m.turn >= min_turn]

    def truncate_to_turns(self, keep_turns: int) -> list[Message]:
        """Remove older messages, keeping only recent turns + system messages.

        Returns the removed messages for potential summarization.
        """
        if not self._messages:
            return []

        min_turn = max(0, self._turn - keep_turns + 1)
        removed: list[Message] = []
        kept: list[Message] = []

        for m in self._messages:
            if m.role == "system" or m.turn >= min_turn:
                kept.append(m)
            else:
                removed.append(m)

        self._messages = kept
        return removed

    def clear(self) -> None:
        """Clear all messages (keeps turn counter)."""
        self._messages.clear()

    def __len__(self) -> int:
        return len(self._messages)

    def __iter__(self):
        return iter(self._messages)
