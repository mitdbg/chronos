"""Unit tests for ContextManager."""

from pathlib import Path

import pytest

from janus_code.config import Config
from janus_code.context.context_manager import ContextManager
from janus_code.context.conversation import Conversation, Message
from janus_code.context.memory_loader import MemoryLoader


@pytest.fixture
def ctx(tmp_workspace: Path) -> ContextManager:
    config = Config(max_context_tokens=1000, compaction_threshold=0.8)
    loader = MemoryLoader(working_dir=tmp_workspace)
    conv = Conversation()
    return ContextManager(config=config, memory_loader=loader, conversation=conv)


class TestContextManager:
    def test_count_tokens(self, ctx: ContextManager):
        tokens = ctx.count_tokens("hello world")
        assert tokens > 0

    def test_count_message_tokens(self, ctx: ContextManager):
        msg = Message(role="user", content="hello world")
        tokens = ctx.count_message_tokens(msg)
        assert tokens > 0

    def test_total_tokens_empty(self, ctx: ContextManager):
        assert ctx.total_tokens() == 0

    def test_total_tokens_with_messages(self, ctx: ContextManager):
        ctx.conversation.append_user("hello world")
        assert ctx.total_tokens() > 0

    def test_should_compact_false(self, ctx: ContextManager):
        ctx.conversation.append_user("short message")
        assert not ctx.should_compact()

    def test_should_compact_true(self, ctx: ContextManager):
        # Fill context with big messages
        big_msg = "word " * 500
        ctx.conversation.append_user(big_msg)
        ctx.conversation.append_assistant(big_msg)
        assert ctx.should_compact()

    def test_compact_returns_none_when_not_needed(self, ctx: ContextManager):
        ctx.conversation.append_user("hi")
        result = ctx.compact()
        assert result is None

    def test_compact_when_needed(self, ctx: ContextManager, tmp_workspace: Path):
        big_msg = "word " * 500
        ctx.conversation.append_system("system prompt")
        ctx.conversation.advance_turn()
        ctx.conversation.append_user(big_msg)
        ctx.conversation.append_assistant(big_msg)

        result = ctx.compact(working_dir=tmp_workspace)
        assert result is not None
        assert "Continuation Summary" in result

        # After compaction, total tokens should be less
        # (though we added summary, we removed old messages)
        assert len(ctx.conversation) > 0  # has system + summary + recent

    def test_tokens_remaining(self, ctx: ContextManager):
        remaining = ctx.tokens_remaining()
        assert remaining == 1000  # max_context_tokens
        ctx.conversation.append_user("hello")
        assert ctx.tokens_remaining() < 1000

    def test_compact_writes_session_notes(self, ctx: ContextManager, tmp_workspace: Path):
        big_msg = "word " * 500
        ctx.conversation.append_system("sys")
        ctx.conversation.append_user(big_msg)
        ctx.conversation.append_assistant(big_msg)

        ctx.compact(working_dir=tmp_workspace)

        notes = tmp_workspace / ".janus-code" / "sessions" / "latest_summary.md"
        assert notes.exists()
        assert "Continuation Summary" in notes.read_text()
