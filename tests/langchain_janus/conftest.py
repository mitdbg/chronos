"""Shared fixtures for Janus partner package tests.

These tests require root privileges for OverlayFS mount/umount.
Run with: sudo python -m pytest tests/ -v
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Generator

import pytest

from janus_core.transaction.coordinator import TransactionCoordinator
from janus_core.transaction.shim_fs import OverlayFSShim
from janus_core.transaction.types import TransactionHandle

from langchain_janus.context import JanusContext


def _require_root() -> None:
    """Skip test if not running as root."""
    if os.geteuid() != 0:
        pytest.skip("Requires root for OverlayFS")


@pytest.fixture
def base_dir() -> Generator[Path, None, None]:
    """Create a temporary base project directory with sample files."""
    _require_root()
    d = Path(tempfile.mkdtemp(prefix="janus_test_base_"))
    # Create some initial project files
    (d / "README.md").write_text("# Test Project\n")
    (d / "src").mkdir()
    (d / "src" / "main.py").write_text('print("hello world")\n')
    (d / "src" / "utils.py").write_text(
        "def add(a, b):\n    return a + b\n"
    )
    (d / "tests").mkdir()
    (d / "tests" / "test_main.py").write_text(
        "def test_hello():\n    assert True\n"
    )
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def janus_context(base_dir: Path) -> Generator[JanusContext, None, None]:
    """Create a JanusContext with an active transaction."""
    ctx = JanusContext(base_dir)
    ctx.begin()
    yield ctx
    if ctx.is_active:
        ctx.abort()


@pytest.fixture
def working_dir(janus_context: JanusContext) -> Path:
    """Return the overlay working directory."""
    wd = janus_context.working_dir
    assert wd is not None
    return wd
