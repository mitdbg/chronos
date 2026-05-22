"""Unit tests for TodoWrite tool."""

import pytest

from chronos_code.tools.todo_write import TodoWriteTool


@pytest.fixture
def tool() -> TodoWriteTool:
    return TodoWriteTool()


class TestTodoWrite:
    def test_create_todos(self, tool: TodoWriteTool):
        result = tool.run([
            {"id": "1", "title": "Fix bug", "status": "pending"},
            {"id": "2", "title": "Write tests", "status": "pending"},
        ])
        assert "Fix bug" in result
        assert "Write tests" in result
        assert "0/2 completed" in result

    def test_mark_in_progress(self, tool: TodoWriteTool):
        result = tool.run([
            {"id": "1", "title": "Fix bug", "status": "in_progress"},
            {"id": "2", "title": "Write tests", "status": "pending"},
        ])
        assert "in_progress" in result

    def test_mark_completed(self, tool: TodoWriteTool):
        result = tool.run([
            {"id": "1", "title": "Fix bug", "status": "completed"},
            {"id": "2", "title": "Write tests", "status": "pending"},
        ])
        assert "1/2 completed" in result

    def test_max_one_in_progress(self, tool: TodoWriteTool):
        result = tool.run([
            {"id": "1", "title": "Task 1", "status": "in_progress"},
            {"id": "2", "title": "Task 2", "status": "in_progress"},
        ])
        assert "Error" in result
        assert "at most 1" in result

    def test_missing_fields(self, tool: TodoWriteTool):
        result = tool.run([{"status": "pending"}])
        assert "Error" in result

    def test_todos_property(self, tool: TodoWriteTool):
        tool.run([{"id": "1", "title": "Task", "status": "pending"}])
        assert len(tool.todos) == 1
        assert tool.todos[0]["title"] == "Task"

    def test_json_serialization(self, tool: TodoWriteTool):
        tool.run([
            {"id": "1", "title": "Task 1", "status": "completed"},
            {"id": "2", "title": "Task 2", "status": "pending"},
        ])
        json_str = tool.to_json()
        assert "Task 1" in json_str

        # Restore
        new_tool = TodoWriteTool()
        new_tool.from_json(json_str)
        assert len(new_tool.todos) == 2
        assert new_tool.todos[0]["title"] == "Task 1"

    def test_listener_called(self, tool: TodoWriteTool):
        updates = []
        tool.add_listener(lambda todos: updates.append(len(todos)))
        tool.run([{"id": "1", "title": "Task", "status": "pending"}])
        assert updates == [1]

    def test_empty_list(self, tool: TodoWriteTool):
        result = tool.run([])
        assert "empty" in result.lower()

    def test_default_status(self, tool: TodoWriteTool):
        result = tool.run([{"id": "1", "title": "Task"}])
        assert tool.todos[0]["status"] == "pending"
