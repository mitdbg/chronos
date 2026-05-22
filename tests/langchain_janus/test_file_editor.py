"""Tests for JanusFileEditor — transactional file editing on OverlayFS.

Run with: sudo python -m pytest tests/test_file_editor.py -v
"""

from __future__ import annotations

from pathlib import Path

import pytest

from langchain_janus.file_editor import JanusFileEditor


# ── View tests ───────────────────────────────────────────────────────


class TestView:
    """Tests for the 'view' command."""

    def test_view_existing_file(self, working_dir: Path) -> None:
        """View a file that exists in the base project."""
        editor = JanusFileEditor(working_dir=working_dir)
        result = editor.invoke({"command": "view", "path": "README.md"})
        assert "# Test Project" in result
        # Should have line numbers
        assert "1|" in result

    def test_view_nested_file(self, working_dir: Path) -> None:
        """View a file in a subdirectory."""
        editor = JanusFileEditor(working_dir=working_dir)
        result = editor.invoke({"command": "view", "path": "src/main.py"})
        assert "hello world" in result
        assert "1|" in result

    def test_view_with_range(self, working_dir: Path) -> None:
        """View specific line range of a file."""
        editor = JanusFileEditor(working_dir=working_dir)
        # First create a multi-line file
        lines = "\n".join(f"line {i}" for i in range(1, 11))
        editor.invoke({"command": "create", "path": "multi.txt", "file_text": lines})

        result = editor.invoke(
            {"command": "view", "path": "multi.txt", "view_range": [3, 5]}
        )
        assert "line 3" in result
        assert "line 5" in result
        assert "line 1" not in result
        assert "line 6" not in result

    def test_view_nonexistent_file(self, working_dir: Path) -> None:
        """Viewing a nonexistent file returns an error."""
        editor = JanusFileEditor(working_dir=working_dir)
        result = editor.invoke({"command": "view", "path": "nonexistent.txt"})
        assert "not found" in result.lower() or "Error" in result

    def test_view_directory(self, working_dir: Path) -> None:
        """Viewing a directory lists its contents."""
        editor = JanusFileEditor(working_dir=working_dir)
        result = editor.invoke({"command": "view", "path": "src"})
        assert "main.py" in result
        assert "utils.py" in result

    def test_view_root_directory(self, working_dir: Path) -> None:
        """Viewing root '' lists top-level contents."""
        editor = JanusFileEditor(working_dir=working_dir)
        result = editor.invoke({"command": "view", "path": ""})
        assert "README.md" in result
        assert "src" in result


# ── Create tests ─────────────────────────────────────────────────────


class TestCreate:
    """Tests for the 'create' command."""

    def test_create_new_file(self, working_dir: Path) -> None:
        """Create a new file in the overlay."""
        editor = JanusFileEditor(working_dir=working_dir)
        result = editor.invoke(
            {"command": "create", "path": "new_file.txt", "file_text": "hello"}
        )
        assert "created" in result.lower()

        # Verify file exists in overlay
        assert (working_dir / "new_file.txt").exists()
        assert (working_dir / "new_file.txt").read_text().strip() == "hello"

    def test_create_with_subdirectory(self, working_dir: Path) -> None:
        """Create a file in a new subdirectory."""
        editor = JanusFileEditor(working_dir=working_dir)
        result = editor.invoke(
            {
                "command": "create",
                "path": "new_dir/sub/file.py",
                "file_text": "x = 1\n",
            }
        )
        assert "created" in result.lower()
        assert (working_dir / "new_dir" / "sub" / "file.py").exists()

    def test_create_overwrites_existing(self, working_dir: Path) -> None:
        """Creating a file that already exists overwrites it."""
        editor = JanusFileEditor(working_dir=working_dir)
        editor.invoke(
            {"command": "create", "path": "overwrite.txt", "file_text": "v1"}
        )
        editor.invoke(
            {"command": "create", "path": "overwrite.txt", "file_text": "v2"}
        )

        content = (working_dir / "overwrite.txt").read_text().strip()
        assert content == "v2"

    def test_create_adds_trailing_newline(self, working_dir: Path) -> None:
        """Create command ensures trailing newline (POSIX compliance)."""
        editor = JanusFileEditor(working_dir=working_dir)
        editor.invoke(
            {"command": "create", "path": "posix.txt", "file_text": "no newline"}
        )
        content = (working_dir / "posix.txt").read_text()
        assert content.endswith("\n")

    def test_create_preserves_existing_trailing_newline(
        self, working_dir: Path
    ) -> None:
        """If file_text already ends with newline, don't double it."""
        editor = JanusFileEditor(working_dir=working_dir)
        editor.invoke(
            {"command": "create", "path": "ok.txt", "file_text": "has newline\n"}
        )
        content = (working_dir / "ok.txt").read_text()
        assert content == "has newline\n"

    def test_create_missing_file_text(self, working_dir: Path) -> None:
        """Create without file_text returns an error."""
        editor = JanusFileEditor(working_dir=working_dir)
        result = editor.invoke({"command": "create", "path": "x.txt"})
        assert "error" in result.lower()


# ── str_replace tests ────────────────────────────────────────────────


class TestStrReplace:
    """Tests for the 'str_replace' command."""

    def test_str_replace_simple(self, working_dir: Path) -> None:
        """Replace a string in a file."""
        editor = JanusFileEditor(working_dir=working_dir)
        result = editor.invoke(
            {
                "command": "str_replace",
                "path": "src/main.py",
                "old_str": "hello world",
                "new_str": "goodbye world",
            }
        )
        assert "replaced" in result.lower()

        content = (working_dir / "src" / "main.py").read_text()
        assert "goodbye world" in content
        assert "hello world" not in content

    def test_str_replace_multiline(self, working_dir: Path) -> None:
        """Replace a multi-line string."""
        editor = JanusFileEditor(working_dir=working_dir)
        result = editor.invoke(
            {
                "command": "str_replace",
                "path": "src/utils.py",
                "old_str": "def add(a, b):\n    return a + b",
                "new_str": "def add(a, b):\n    \"\"\"Add two numbers.\"\"\"\n    return a + b",
            }
        )
        assert "replaced" in result.lower()
        content = (working_dir / "src" / "utils.py").read_text()
        assert '"""Add two numbers."""' in content

    def test_str_replace_not_found(self, working_dir: Path) -> None:
        """Replace a string that doesn't exist returns error."""
        editor = JanusFileEditor(working_dir=working_dir)
        result = editor.invoke(
            {
                "command": "str_replace",
                "path": "src/main.py",
                "old_str": "xyz_not_here",
                "new_str": "abc",
            }
        )
        assert "not found" in result.lower() or "error" in result.lower()

    def test_str_replace_file_not_found(self, working_dir: Path) -> None:
        """Replace in a nonexistent file returns error."""
        editor = JanusFileEditor(working_dir=working_dir)
        result = editor.invoke(
            {
                "command": "str_replace",
                "path": "nonexistent.py",
                "old_str": "x",
                "new_str": "y",
            }
        )
        assert "not found" in result.lower() or "error" in result.lower()

    def test_str_replace_empty_new_str(self, working_dir: Path) -> None:
        """Replace with empty string (deletion)."""
        editor = JanusFileEditor(working_dir=working_dir)
        editor.invoke(
            {
                "command": "create",
                "path": "del_test.txt",
                "file_text": "keep this REMOVE_ME and keep this too",
            }
        )
        result = editor.invoke(
            {
                "command": "str_replace",
                "path": "del_test.txt",
                "old_str": "REMOVE_ME ",
                "new_str": "",
            }
        )
        assert "replaced" in result.lower()
        content = (working_dir / "del_test.txt").read_text()
        assert "REMOVE_ME" not in content
        assert "keep this and keep this too" in content

    def test_str_replace_only_first_occurrence(self, working_dir: Path) -> None:
        """Only the first occurrence is replaced."""
        editor = JanusFileEditor(working_dir=working_dir)
        editor.invoke(
            {
                "command": "create",
                "path": "dups.txt",
                "file_text": "aaa\naaa\naaa",
            }
        )
        editor.invoke(
            {
                "command": "str_replace",
                "path": "dups.txt",
                "old_str": "aaa",
                "new_str": "bbb",
            }
        )
        content = (working_dir / "dups.txt").read_text()
        assert content.count("bbb") == 1
        assert content.count("aaa") == 2


# ── Insert tests ─────────────────────────────────────────────────────


class TestInsert:
    """Tests for the 'insert' command."""

    def test_insert_at_beginning(self, working_dir: Path) -> None:
        """Insert at line 0 (before first line)."""
        editor = JanusFileEditor(working_dir=working_dir)
        editor.invoke(
            {
                "command": "create",
                "path": "ins.txt",
                "file_text": "line1\nline2",
            }
        )
        editor.invoke(
            {
                "command": "insert",
                "path": "ins.txt",
                "insert_line": 0,
                "new_str": "inserted",
            }
        )
        content = (working_dir / "ins.txt").read_text()
        lines = content.strip().split("\n")
        assert lines[0] == "inserted"
        assert lines[1] == "line1"

    def test_insert_at_middle(self, working_dir: Path) -> None:
        """Insert in the middle of a file."""
        editor = JanusFileEditor(working_dir=working_dir)
        editor.invoke(
            {
                "command": "create",
                "path": "ins2.txt",
                "file_text": "line1\nline2\nline3",
            }
        )
        editor.invoke(
            {
                "command": "insert",
                "path": "ins2.txt",
                "insert_line": 2,
                "new_str": "inserted_here",
            }
        )
        content = (working_dir / "ins2.txt").read_text()
        lines = content.strip().split("\n")
        assert lines[2] == "inserted_here"
        assert lines[0] == "line1"
        assert lines[1] == "line2"
        assert lines[3] == "line3"

    def test_insert_at_end(self, working_dir: Path) -> None:
        """Insert after the last line."""
        editor = JanusFileEditor(working_dir=working_dir)
        editor.invoke(
            {
                "command": "create",
                "path": "ins3.txt",
                "file_text": "line1\nline2",
            }
        )
        editor.invoke(
            {
                "command": "insert",
                "path": "ins3.txt",
                "insert_line": 99,  # past end
                "new_str": "at_end",
            }
        )
        content = (working_dir / "ins3.txt").read_text()
        assert content.strip().endswith("at_end")

    def test_insert_multiline(self, working_dir: Path) -> None:
        """Insert multiple lines at once."""
        editor = JanusFileEditor(working_dir=working_dir)
        editor.invoke(
            {"command": "create", "path": "multi_ins.txt", "file_text": "A\nC"}
        )
        editor.invoke(
            {
                "command": "insert",
                "path": "multi_ins.txt",
                "insert_line": 1,
                "new_str": "B1\nB2",
            }
        )
        content = (working_dir / "multi_ins.txt").read_text()
        lines = content.strip().split("\n")
        assert lines == ["A", "B1", "B2", "C"]


# ── Delete tests ─────────────────────────────────────────────────────


class TestDelete:
    """Tests for the 'delete' command."""

    def test_delete_file(self, working_dir: Path) -> None:
        """Delete a file."""
        editor = JanusFileEditor(working_dir=working_dir)
        editor.invoke(
            {"command": "create", "path": "to_delete.txt", "file_text": "bye"}
        )
        assert (working_dir / "to_delete.txt").exists()

        result = editor.invoke({"command": "delete", "path": "to_delete.txt"})
        assert "deleted" in result.lower()
        assert not (working_dir / "to_delete.txt").exists()

    def test_delete_directory(self, working_dir: Path) -> None:
        """Delete a directory recursively."""
        editor = JanusFileEditor(working_dir=working_dir)
        editor.invoke(
            {"command": "create", "path": "del_dir/a.txt", "file_text": "a"}
        )
        editor.invoke(
            {"command": "create", "path": "del_dir/b.txt", "file_text": "b"}
        )
        assert (working_dir / "del_dir").is_dir()

        result = editor.invoke({"command": "delete", "path": "del_dir"})
        assert "deleted" in result.lower()
        assert not (working_dir / "del_dir").exists()

    def test_delete_nonexistent(self, working_dir: Path) -> None:
        """Delete a nonexistent file returns error."""
        editor = JanusFileEditor(working_dir=working_dir)
        result = editor.invoke({"command": "delete", "path": "ghost.txt"})
        assert "not found" in result.lower() or "error" in result.lower()


# ── Security tests ───────────────────────────────────────────────────


class TestSecurity:
    """Tests for path security enforcement."""

    def test_path_traversal_rejected(self, working_dir: Path) -> None:
        """Paths with '..' are rejected."""
        editor = JanusFileEditor(working_dir=working_dir)
        result = editor.invoke({"command": "view", "path": "../../../etc/passwd"})
        assert "error" in result.lower()

    def test_absolute_path_rooted(self, working_dir: Path) -> None:
        """Absolute-looking paths are treated relative to working_dir."""
        editor = JanusFileEditor(working_dir=working_dir)
        editor.invoke(
            {"command": "create", "path": "/test_abs.txt", "file_text": "abs"}
        )
        assert (working_dir / "test_abs.txt").exists()


# ── Isolation tests ──────────────────────────────────────────────────


class TestIsolation:
    """Tests verifying overlay isolation."""

    def test_changes_visible_in_overlay(
        self, working_dir: Path, base_dir: Path
    ) -> None:
        """Changes are visible in overlay but not in base."""
        editor = JanusFileEditor(working_dir=working_dir)
        editor.invoke(
            {
                "command": "create",
                "path": "overlay_only.txt",
                "file_text": "isolated",
            }
        )
        # Visible in overlay
        assert (working_dir / "overlay_only.txt").exists()
        # NOT visible in base
        assert not (base_dir / "overlay_only.txt").exists()

    def test_base_files_readable_through_overlay(
        self, working_dir: Path
    ) -> None:
        """Base project files are readable through the overlay."""
        editor = JanusFileEditor(working_dir=working_dir)
        result = editor.invoke({"command": "view", "path": "README.md"})
        assert "# Test Project" in result

    def test_modify_base_file_in_overlay(
        self, working_dir: Path, base_dir: Path
    ) -> None:
        """Modifying a base file only changes the overlay copy."""
        editor = JanusFileEditor(working_dir=working_dir)
        editor.invoke(
            {
                "command": "str_replace",
                "path": "README.md",
                "old_str": "# Test Project",
                "new_str": "# Modified Project",
            }
        )
        # Overlay has modified version
        assert "Modified" in (working_dir / "README.md").read_text()
        # Base is unchanged
        assert "# Test Project" in (base_dir / "README.md").read_text()
