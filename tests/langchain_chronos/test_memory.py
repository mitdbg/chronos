"""Tests for ChronosMemory — transactional memory tool on OverlayFS.

Run with: sudo python -m pytest tests/test_memory.py -v
"""

from __future__ import annotations

from pathlib import Path

import pytest

from langchain_chronos.memory import ChronosMemory


class TestList:
    """Tests for the 'list' command."""

    def test_list_empty(self, working_dir: Path) -> None:
        """List when no memory files exist."""
        mem = ChronosMemory(working_dir=working_dir)
        result = mem.invoke({"command": "list"})
        assert "empty" in result.lower()

    def test_list_after_create(self, working_dir: Path) -> None:
        """List shows created memory files."""
        mem = ChronosMemory(working_dir=working_dir)
        mem.invoke({"command": "create", "path": "notes.md", "file_text": "# Notes"})
        mem.invoke({"command": "create", "path": "todo.md", "file_text": "# TODO"})

        result = mem.invoke({"command": "list"})
        assert "notes.md" in result
        assert "todo.md" in result

    def test_list_shows_sizes(self, working_dir: Path) -> None:
        """List shows file sizes."""
        mem = ChronosMemory(working_dir=working_dir)
        mem.invoke(
            {"command": "create", "path": "sized.txt", "file_text": "x" * 100}
        )
        result = mem.invoke({"command": "list"})
        assert "bytes" in result


class TestCreate:
    """Tests for the 'create' command."""

    def test_create_memory_file(self, working_dir: Path) -> None:
        """Create a memory file."""
        mem = ChronosMemory(working_dir=working_dir)
        result = mem.invoke(
            {"command": "create", "path": "progress.md", "file_text": "Step 1 done"}
        )
        assert "created" in result.lower()

        # Verify it's in the memories subdirectory
        assert (working_dir / "memories" / "progress.md").exists()

    def test_create_overwrite(self, working_dir: Path) -> None:
        """Creating an existing file overwrites it."""
        mem = ChronosMemory(working_dir=working_dir)
        mem.invoke({"command": "create", "path": "data.txt", "file_text": "v1"})
        mem.invoke({"command": "create", "path": "data.txt", "file_text": "v2"})

        content = (working_dir / "memories" / "data.txt").read_text().strip()
        assert content == "v2"


class TestView:
    """Tests for the 'view' command."""

    def test_view_memory_file(self, working_dir: Path) -> None:
        """View a memory file with line numbers."""
        mem = ChronosMemory(working_dir=working_dir)
        mem.invoke(
            {"command": "create", "path": "view_test.md", "file_text": "line1\nline2"}
        )
        result = mem.invoke({"command": "view", "path": "view_test.md"})
        assert "1|" in result
        assert "line1" in result
        assert "line2" in result

    def test_view_nonexistent(self, working_dir: Path) -> None:
        """View nonexistent file returns error."""
        mem = ChronosMemory(working_dir=working_dir)
        result = mem.invoke({"command": "view", "path": "ghost.md"})
        assert "not found" in result.lower()


class TestStrReplace:
    """Tests for the 'str_replace' command."""

    def test_replace_in_memory(self, working_dir: Path) -> None:
        """Replace a string in a memory file."""
        mem = ChronosMemory(working_dir=working_dir)
        mem.invoke(
            {"command": "create", "path": "repl.md", "file_text": "status: pending"}
        )
        result = mem.invoke(
            {
                "command": "str_replace",
                "path": "repl.md",
                "old_str": "pending",
                "new_str": "done",
            }
        )
        assert "replaced" in result.lower()
        content = (working_dir / "memories" / "repl.md").read_text()
        assert "done" in content

    def test_replace_not_found(self, working_dir: Path) -> None:
        """Replace with nonexistent string returns error."""
        mem = ChronosMemory(working_dir=working_dir)
        mem.invoke(
            {"command": "create", "path": "nf.md", "file_text": "hello"}
        )
        result = mem.invoke(
            {
                "command": "str_replace",
                "path": "nf.md",
                "old_str": "xyz_missing",
                "new_str": "abc",
            }
        )
        assert "not found" in result.lower() or "error" in result.lower()


class TestInsert:
    """Tests for the 'insert' command."""

    def test_insert_into_memory(self, working_dir: Path) -> None:
        """Insert text at a line in a memory file."""
        mem = ChronosMemory(working_dir=working_dir)
        mem.invoke(
            {"command": "create", "path": "ins.md", "file_text": "line1\nline3"}
        )
        mem.invoke(
            {
                "command": "insert",
                "path": "ins.md",
                "insert_line": 1,
                "new_str": "line2",
            }
        )
        content = (working_dir / "memories" / "ins.md").read_text()
        lines = content.strip().split("\n")
        assert lines == ["line1", "line2", "line3"]


class TestDelete:
    """Tests for the 'delete' command."""

    def test_delete_memory_file(self, working_dir: Path) -> None:
        """Delete a memory file."""
        mem = ChronosMemory(working_dir=working_dir)
        mem.invoke({"command": "create", "path": "del.md", "file_text": "bye"})
        assert (working_dir / "memories" / "del.md").exists()

        result = mem.invoke({"command": "delete", "path": "del.md"})
        assert "deleted" in result.lower()
        assert not (working_dir / "memories" / "del.md").exists()

    def test_delete_nonexistent(self, working_dir: Path) -> None:
        """Delete nonexistent file returns error."""
        mem = ChronosMemory(working_dir=working_dir)
        result = mem.invoke({"command": "delete", "path": "ghost.md"})
        assert "not found" in result.lower()


class TestSecurity:
    """Tests for path security in memory tool."""

    def test_rejects_path_traversal(self, working_dir: Path) -> None:
        """Memory file names must not contain path traversal."""
        mem = ChronosMemory(working_dir=working_dir)
        result = mem.invoke(
            {"command": "create", "path": "../escape.txt", "file_text": "bad"}
        )
        assert "error" in result.lower()

    def test_rejects_subdirectory_paths(self, working_dir: Path) -> None:
        """Memory file names must be simple filenames, no subdirectories."""
        mem = ChronosMemory(working_dir=working_dir)
        result = mem.invoke(
            {"command": "create", "path": "sub/dir/file.txt", "file_text": "bad"}
        )
        assert "error" in result.lower()

    def test_requires_path_for_non_list_commands(self, working_dir: Path) -> None:
        """All commands except 'list' require 'path'."""
        mem = ChronosMemory(working_dir=working_dir)
        result = mem.invoke({"command": "view"})
        assert "error" in result.lower()


class TestIsolation:
    """Tests verifying overlay isolation for memory."""

    def test_memory_files_not_in_base(
        self, working_dir: Path, base_dir: Path
    ) -> None:
        """Memory files exist in overlay but not in base directory."""
        mem = ChronosMemory(working_dir=working_dir)
        mem.invoke(
            {"command": "create", "path": "isolated.md", "file_text": "hi"}
        )
        assert (working_dir / "memories" / "isolated.md").exists()
        assert not (base_dir / "memories" / "isolated.md").exists()
