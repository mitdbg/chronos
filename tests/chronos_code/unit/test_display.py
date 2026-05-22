"""Unit tests for display components — terminal_ui and todo_display."""

import pytest

from chronos_code.display.terminal_ui import TerminalUI
from chronos_code.display.todo_display import TodoDisplay, STATUS_ICONS, STATUS_COLORS


class TestTerminalUI:
    """Tests for TerminalUI."""

    def test_create_with_rich(self):
        ui = TerminalUI(use_rich=True)
        assert ui.has_rich is True

    def test_create_without_rich(self):
        ui = TerminalUI(use_rich=False)
        assert ui.has_rich is False

    def test_print_plain(self, capsys):
        ui = TerminalUI(use_rich=False)
        ui.print("hello")
        assert "hello" in capsys.readouterr().out

    def test_print_markdown_plain(self, capsys):
        ui = TerminalUI(use_rich=False)
        ui.print_markdown("# Title")
        assert "Title" in capsys.readouterr().out

    def test_print_diff_plain(self, capsys):
        ui = TerminalUI(use_rich=False)
        ui.print_diff("--- a\n+++ b\n- old\n+ new")
        out = capsys.readouterr().out
        assert "old" in out
        assert "new" in out

    def test_print_error_plain(self, capsys):
        ui = TerminalUI(use_rich=False)
        ui.print_error("something broke")
        assert "Error: something broke" in capsys.readouterr().out

    def test_print_success_plain(self, capsys):
        ui = TerminalUI(use_rich=False)
        ui.print_success("all good")
        assert "all good" in capsys.readouterr().out

    def test_print_warning_plain(self, capsys):
        ui = TerminalUI(use_rich=False)
        ui.print_warning("watch out")
        assert "watch out" in capsys.readouterr().out

    def test_print_info_plain(self, capsys):
        ui = TerminalUI(use_rich=False)
        ui.print_info("fyi")
        assert "fyi" in capsys.readouterr().out

    def test_print_tool_call_plain(self, capsys):
        ui = TerminalUI(use_rich=False)
        ui.print_tool_call("read_file", "path=/a.txt")
        out = capsys.readouterr().out
        assert "read_file" in out

    def test_print_agent_activity_plain(self, capsys):
        ui = TerminalUI(use_rich=False)
        ui.print_agent_activity("explore", "Reading files")
        out = capsys.readouterr().out
        assert "explore" in out

    def test_print_with_rich(self, capsys):
        """Rich mode should not crash."""
        ui = TerminalUI(use_rich=True)
        ui.print("hello")
        # Rich writes to its own console; just ensure no exception

    def test_print_error_with_rich(self):
        ui = TerminalUI(use_rich=True)
        ui.print_error("fail")  # Should not raise

    def test_print_success_with_rich(self):
        ui = TerminalUI(use_rich=True)
        ui.print_success("ok")

    def test_print_markdown_with_rich(self):
        ui = TerminalUI(use_rich=True)
        ui.print_markdown("**bold** text")

    def test_print_diff_with_rich(self):
        ui = TerminalUI(use_rich=True)
        ui.print_diff("--- a\n+++ b\n@@ -1 +1 @@\n-old\n+new")


class TestTodoDisplay:
    """Tests for TodoDisplay."""

    def test_status_icons_complete(self):
        assert "pending" in STATUS_ICONS
        assert "in_progress" in STATUS_ICONS
        assert "completed" in STATUS_ICONS

    def test_status_colors_complete(self):
        assert "pending" in STATUS_COLORS
        assert "in_progress" in STATUS_COLORS
        assert "completed" in STATUS_COLORS

    def test_render_empty(self):
        td = TodoDisplay(use_rich=False)
        assert td.render([]) == "No tasks."

    def test_render_items(self):
        td = TodoDisplay(use_rich=False)
        todos = [
            {"title": "Read code", "status": "completed"},
            {"title": "Write code", "status": "in_progress"},
            {"title": "Test code", "status": "pending"},
        ]
        rendered = td.render(todos)
        assert "✓ Read code" in rendered
        assert "◉ Write code" in rendered
        assert "○ Test code" in rendered

    def test_render_default_status(self):
        td = TodoDisplay(use_rich=False)
        result = td.render([{"title": "Unknown status"}])
        assert "○ Unknown status" in result

    def test_display_plain(self, capsys):
        td = TodoDisplay(use_rich=False)
        td.display([{"title": "Task A", "status": "pending"}])
        assert "○ Task A" in capsys.readouterr().out

    def test_display_rich(self):
        """Rich mode should not crash."""
        td = TodoDisplay(use_rich=True)
        td.display([{"title": "Task A", "status": "completed"}])

    def test_on_todos_changed_callback(self, capsys):
        td = TodoDisplay(use_rich=False)
        td.on_todos_changed([{"title": "Updated", "status": "in_progress"}])
        assert "◉ Updated" in capsys.readouterr().out

    def test_render_unknown_status(self):
        td = TodoDisplay(use_rich=False)
        result = td.render([{"title": "Mystery", "status": "unknown_xyz"}])
        assert "? Mystery" in result
