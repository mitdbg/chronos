"""Tests for JanusBash — transactional bash execution on OverlayFS.

Run with: sudo python -m pytest tests/test_bash.py -v
"""

from __future__ import annotations

from pathlib import Path

import pytest

from langchain_janus.bash import JanusBash


class TestBasicExecution:
    """Tests for basic bash command execution."""

    def test_echo(self, working_dir: Path) -> None:
        """Run a simple echo command."""
        bash = JanusBash(working_dir=working_dir)
        result = bash.invoke({"command": "echo hello"})
        assert "hello" in result

    def test_ls(self, working_dir: Path) -> None:
        """List the overlay directory."""
        bash = JanusBash(working_dir=working_dir)
        result = bash.invoke({"command": "ls"})
        assert "README.md" in result
        assert "src" in result

    def test_cat_file(self, working_dir: Path) -> None:
        """Cat an existing file through the overlay."""
        bash = JanusBash(working_dir=working_dir)
        result = bash.invoke({"command": "cat README.md"})
        assert "# Test Project" in result

    def test_pwd(self, working_dir: Path) -> None:
        """pwd shows the overlay merged directory."""
        bash = JanusBash(working_dir=working_dir)
        result = bash.invoke({"command": "pwd"})
        assert str(working_dir) in result

    def test_multiline_output(self, working_dir: Path) -> None:
        """Handle multiline output."""
        bash = JanusBash(working_dir=working_dir)
        result = bash.invoke({"command": "seq 1 5"})
        for n in range(1, 6):
            assert str(n) in result


class TestFileSystemOperations:
    """Tests for filesystem operations through bash."""

    def test_create_file_via_bash(self, working_dir: Path) -> None:
        """Create a file via bash command."""
        bash = JanusBash(working_dir=working_dir)
        bash.invoke({"command": "echo 'hello from bash' > bash_created.txt"})
        assert (working_dir / "bash_created.txt").exists()
        content = (working_dir / "bash_created.txt").read_text()
        assert "hello from bash" in content

    def test_mkdir_and_tree(self, working_dir: Path) -> None:
        """Create directories and verify structure."""
        bash = JanusBash(working_dir=working_dir)
        bash.invoke(
            {"command": "mkdir -p new_pkg/sub && touch new_pkg/sub/mod.py"}
        )
        assert (working_dir / "new_pkg" / "sub" / "mod.py").exists()

    def test_cp_file(self, working_dir: Path) -> None:
        """Copy a file within the overlay."""
        bash = JanusBash(working_dir=working_dir)
        bash.invoke({"command": "cp src/main.py src/main_backup.py"})
        assert (working_dir / "src" / "main_backup.py").exists()
        original = (working_dir / "src" / "main.py").read_text()
        copy = (working_dir / "src" / "main_backup.py").read_text()
        assert original == copy

    def test_rm_file(self, working_dir: Path) -> None:
        """Remove a file via bash."""
        bash = JanusBash(working_dir=working_dir)
        bash.invoke({"command": "touch to_remove.txt"})
        assert (working_dir / "to_remove.txt").exists()
        bash.invoke({"command": "rm to_remove.txt"})
        # OverlayFS will handle via whiteout
        assert not (working_dir / "to_remove.txt").exists()

    def test_find_files(self, working_dir: Path) -> None:
        """Use find to search files."""
        bash = JanusBash(working_dir=working_dir)
        result = bash.invoke({"command": "find . -name '*.py' | sort"})
        assert "main.py" in result
        assert "utils.py" in result


class TestProgramExecution:
    """Tests for running programs in the overlay."""

    def test_python_script(self, working_dir: Path) -> None:
        """Run a Python script in the overlay."""
        bash = JanusBash(working_dir=working_dir)
        result = bash.invoke({"command": "python3 src/main.py"})
        assert "hello world" in result

    def test_python_inline(self, working_dir: Path) -> None:
        """Run inline Python code."""
        bash = JanusBash(working_dir=working_dir)
        result = bash.invoke({"command": "python3 -c 'print(2 + 2)'"})
        assert "4" in result

    def test_create_and_run_script(self, working_dir: Path) -> None:
        """Create a script via bash and then run it."""
        bash = JanusBash(working_dir=working_dir)
        bash.invoke(
            {
                "command": (
                    "cat > compute.py << 'EOF'\n"
                    "import sys\n"
                    "a, b = int(sys.argv[1]), int(sys.argv[2])\n"
                    "print(f'{a} + {b} = {a + b}')\n"
                    "EOF"
                )
            }
        )
        result = bash.invoke({"command": "python3 compute.py 10 20"})
        assert "10 + 20 = 30" in result

    def test_grep_in_overlay(self, working_dir: Path) -> None:
        """Use grep to search files."""
        bash = JanusBash(working_dir=working_dir)
        result = bash.invoke(
            {"command": "grep -rn 'def' src/ 2>/dev/null || true"}
        )
        assert "add" in result


class TestErrorHandling:
    """Tests for error handling in bash tool."""

    def test_nonzero_exit_code(self, working_dir: Path) -> None:
        """Non-zero exit code is reported."""
        bash = JanusBash(working_dir=working_dir)
        result = bash.invoke({"command": "false"})
        assert "exit code" in result.lower()

    def test_stderr_captured(self, working_dir: Path) -> None:
        """stderr is captured and returned."""
        bash = JanusBash(working_dir=working_dir)
        result = bash.invoke({"command": "echo error >&2"})
        assert "error" in result

    def test_command_not_found(self, working_dir: Path) -> None:
        """Nonexistent command returns error."""
        bash = JanusBash(working_dir=working_dir)
        result = bash.invoke({"command": "nonexistent_command_xyz123"})
        assert "not found" in result.lower() or "exit code" in result.lower()

    def test_timeout(self, working_dir: Path) -> None:
        """Long-running command respects timeout."""
        bash = JanusBash(working_dir=working_dir)
        result = bash.invoke({"command": "sleep 300", "timeout": 2})
        assert "timed out" in result.lower()

    def test_no_output(self, working_dir: Path) -> None:
        """Command with no output returns placeholder."""
        bash = JanusBash(working_dir=working_dir)
        result = bash.invoke({"command": "true"})
        assert result.strip()  # Should return something, not empty


class TestIsolation:
    """Tests verifying overlay isolation for bash."""

    def test_files_created_in_overlay_only(
        self, working_dir: Path, base_dir: Path
    ) -> None:
        """Files created by bash exist in overlay but not in base."""
        bash = JanusBash(working_dir=working_dir)
        bash.invoke({"command": "echo 'isolated' > bash_isolated.txt"})
        assert (working_dir / "bash_isolated.txt").exists()
        assert not (base_dir / "bash_isolated.txt").exists()

    def test_base_files_visible(self, working_dir: Path) -> None:
        """Base project files are visible through the overlay."""
        bash = JanusBash(working_dir=working_dir)
        result = bash.invoke({"command": "cat src/utils.py"})
        assert "def add" in result

    def test_modify_base_file_isolated(
        self, working_dir: Path, base_dir: Path
    ) -> None:
        """Modifying a base file via bash only changes overlay copy."""
        bash = JanusBash(working_dir=working_dir)
        bash.invoke(
            {"command": "sed -i 's/hello world/goodbye/g' src/main.py"}
        )
        overlay_content = (working_dir / "src" / "main.py").read_text()
        base_content = (base_dir / "src" / "main.py").read_text()
        assert "goodbye" in overlay_content
        assert "hello world" in base_content

    def test_pipe_chain(self, working_dir: Path) -> None:
        """Multi-step pipe chain works in the overlay."""
        bash = JanusBash(working_dir=working_dir)
        result = bash.invoke(
            {"command": "find . -name '*.py' | xargs grep -l 'def' | sort"}
        )
        assert "utils.py" in result

    def test_env_vars(self, working_dir: Path) -> None:
        """Custom environment variables are available."""
        bash = JanusBash(working_dir=working_dir, env_vars={"MY_VAR": "test123"})
        result = bash.invoke({"command": "echo $MY_VAR"})
        assert "test123" in result
