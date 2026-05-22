"""Unit tests for SlashCommandHandler."""

import pytest

from janus_code.config import Config
from janus_code.slash_commands import SlashCommandHandler


@pytest.fixture
def handler() -> SlashCommandHandler:
    return SlashCommandHandler(config=Config())


class TestSlashCommands:
    def test_is_slash_command(self, handler):
        assert handler.is_slash_command("/help")
        assert handler.is_slash_command("  /config")
        assert not handler.is_slash_command("hello")
        assert not handler.is_slash_command("")

    def test_help(self, handler):
        result = handler.handle("/help")
        assert result.success
        assert "/config" in result.output
        assert "/status" in result.output

    def test_config_show(self, handler):
        result = handler.handle("/config")
        assert result.success
        assert "Model:" in result.output
        assert "Retry Policy:" in result.output

    def test_config_model(self, handler):
        result = handler.handle("/config model gpt-4o")
        assert result.success
        assert handler.config.model == "gpt-4o"

    def test_config_retry_strategy(self, handler):
        result = handler.handle("/config retry none")
        assert result.success
        assert handler.config.retry_policy.strategy == "none"

        result = handler.handle("/config retry exponential")
        assert handler.config.retry_policy.strategy == "exponential"

    def test_config_retry_max_retries(self, handler):
        result = handler.handle("/config retry max-retries 5")
        assert result.success
        assert handler.config.retry_policy.max_retries == 5

    def test_config_retry_base_delay(self, handler):
        result = handler.handle("/config retry base-delay 200")
        assert result.success
        assert handler.config.retry_policy.base_delay_ms == 200

    def test_config_parallel(self, handler):
        result = handler.handle("/config parallel off")
        assert result.success
        assert not handler.config.parallel_enabled

        result = handler.handle("/config parallel on")
        assert handler.config.parallel_enabled

    def test_unknown_command(self, handler):
        result = handler.handle("/nonexistent")
        assert not result.success
        assert "Unknown command" in result.output

    def test_empty_command(self, handler):
        result = handler.handle("/")
        assert not result.success

    def test_status_with_callback(self):
        status = {
            "session_id": "abc123",
            "turn_number": 5,
            "has_active_txn": True,
            "project_path": "/tmp/project",
        }
        handler = SlashCommandHandler(
            config=Config(),
            get_status_fn=lambda: status,
        )
        result = handler.handle("/status")
        assert result.success
        assert "abc123" in result.output
        assert "5" in result.output

    def test_status_without_callback(self, handler):
        result = handler.handle("/status")
        assert "not available" in result.output

    def test_memory_with_callback(self):
        handler = SlashCommandHandler(
            config=Config(),
            get_memory_fn=lambda topic: f"Memory: {topic or 'all'}",
        )
        result = handler.handle("/memory")
        assert "all" in result.output

        result = handler.handle("/memory build")
        assert "build" in result.output

    def test_resume_without_id(self, handler):
        result = handler.handle("/resume")
        assert not result.success
        assert "Usage" in result.output

    def test_abort(self, handler):
        result = handler.handle("/abort")
        assert result.success
        assert "aborted" in result.output

    def test_command_names(self, handler):
        names = handler.command_names
        assert "help" in names
        assert "config" in names
        assert "status" in names

    def test_config_invalid_retry(self, handler):
        result = handler.handle("/config retry max-retries abc")
        assert not result.success

    def test_config_invalid_parallel(self, handler):
        result = handler.handle("/config parallel maybe")
        assert not result.success
