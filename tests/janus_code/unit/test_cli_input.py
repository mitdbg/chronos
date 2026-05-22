"""Tests for CLI user-input reader — verify it works inside a running event loop.

The original bug: prompt_toolkit's sync ``prompt()`` calls ``asyncio.run()``
internally, which crashes with ``RuntimeError: asyncio.run() cannot be called
from a running event loop`` when the REPL is already inside ``asyncio.run()``.

These tests guarantee that ``_build_user_input_reader`` returns an *async*
callable that can be awaited safely inside an already-running loop.
"""

from __future__ import annotations

import asyncio
import inspect
import sys
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

from janus_code.cli import _build_user_input_reader
from janus_code.slash_commands import SlashCommandHandler


def _make_slash() -> SlashCommandHandler:
    """Create a minimal SlashCommandHandler for tests."""
    return SlashCommandHandler(
        config=MagicMock(),
        get_status_fn=lambda: "ok",
        get_history_fn=lambda: "no history",
        get_memory_fn=lambda t: "no memory",
        resume_fn=lambda sid: f"resumed {sid}",
    )


class TestBuildUserInputReader:
    """_build_user_input_reader must always return an async callable."""

    def test_returns_coroutine_function(self):
        """The reader must be an async def, not a plain function."""
        slash = _make_slash()
        reader = _build_user_input_reader(slash)
        assert inspect.iscoroutinefunction(reader), (
            "_build_user_input_reader must return an async function, "
            f"got {type(reader)}"
        )

    async def test_async_reader_inside_running_loop(self, monkeypatch):
        """Calling the reader inside an already-running event loop must not crash."""
        slash = _make_slash()

        # Force TTY detection so prompt_toolkit path is taken
        monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
        monkeypatch.setattr(sys.stdout, "isatty", lambda: True)

        # Patch PromptSession at its source so the local import picks it up
        mock_session = MagicMock()
        mock_session.prompt_async = AsyncMock(return_value="  hello world  ")
        MockPS = MagicMock(return_value=mock_session)

        with patch("prompt_toolkit.PromptSession", MockPS):
            reader = _build_user_input_reader(slash)
            result = await reader()
            assert result == "hello world"

    async def test_non_tty_fallback_is_async(self, monkeypatch):
        """When stdin is not a TTY, the fallback reader must also be async."""
        monkeypatch.setattr(sys.stdin, "isatty", lambda: False)
        slash = _make_slash()
        reader = _build_user_input_reader(slash)

        assert inspect.iscoroutinefunction(reader)

        # Patch input() via run_in_executor path
        with patch("builtins.input", return_value="  piped input  "):
            result = await reader()
            assert result == "piped input"

    async def test_prompt_toolkit_import_failure_fallback_is_async(self, monkeypatch):
        """If prompt_toolkit fails to import, fallback must still be async."""
        slash = _make_slash()

        # Force the try block to fail by patching the import target
        with patch.dict("sys.modules", {"prompt_toolkit": None}):
            # This will cause an ImportError in the try block
            reader = _build_user_input_reader(slash)
            assert inspect.iscoroutinefunction(reader)

