"""Deterministic CLI end-to-end tests using /debug scenarios.

These tests run the real CLI subprocess but avoid live-model variance.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest


ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def _strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


def _run_cli_session(
    project_dir: Path,
    inputs: list[str],
    timeout_s: int = 120,
) -> str:
    package_root = Path(__file__).resolve().parents[3]
    cmd = [
        sys.executable,
        "-m",
        "chronos_code.cli",
        "--project",
        str(project_dir),
    ]

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    payload = "\n".join(i.strip() for i in inputs if i is not None).strip() + "\n"
    proc = subprocess.run(
        cmd,
        input=payload,
        text=True,
        capture_output=True,
        cwd=str(package_root),
        env=env,
        timeout=timeout_s,
    )

    output = _strip_ansi((proc.stdout or "") + "\n" + (proc.stderr or ""))
    if "Failed to initialize Chronos context" in output:
        pytest.skip("Chronos context unavailable in this environment.")
    if proc.returncode != 0:
        pytest.fail(
            "CLI process failed.\n"
            f"Command: {' '.join(cmd)}\n"
            f"Exit code: {proc.returncode}\n"
            f"Output:\n{output}"
        )
    return output


class TestCliDebugE2E:
    def test_cli_subagent_speculative_execution(self, tmp_path: Path) -> None:
        output = _run_cli_session(tmp_path, ["/debug subagent-speculative"])

        assert "SPECULATIVE_OK" in output
        assert "rollback_ok=True" in output
        assert "final_ok=True" in output

        text = (tmp_path / "debug_calc.py").read_text()
        assert "return a + b" in text
        assert "return a * b" not in text

    def test_cli_parallel_snapshot_isolation(self, tmp_path: Path) -> None:
        output = _run_cli_session(tmp_path, ["/debug parallel-snapshot"])

        assert "SNAPSHOT_ISOLATION_OK" in output
        assert "consistent=True" in output
        assert "after_same_txn=version=1" in output
        assert "fresh=version=2" in output

    def test_cli_parallel_db_conflict_retry(self, tmp_path: Path) -> None:
        output = _run_cli_session(tmp_path, ["/debug parallel-db-conflict-retry"])

        assert "DB_CONFLICT_RETRY_OK" in output
        assert "conflict_detected=True" in output
        assert "retries=1" in output
        assert "prior=agent-a" in output
        assert "final=agent-b-retry" in output

    def test_cli_debug_help_lists_scenarios(self, tmp_path: Path) -> None:
        output = _run_cli_session(tmp_path, ["/debug help"])

        assert "subagent-speculative" in output
        assert "parallel-snapshot" in output
        assert "parallel-db-conflict-retry" in output
        assert "nested-dag-wave" in output

    def test_cli_nested_dag_wave_execution(self, tmp_path: Path) -> None:
        output = _run_cli_session(tmp_path, ["/debug nested-dag-wave"])

        assert "NESTED_DAG_WAVE_OK" in output
        assert "ok=True" in output
        assert "waves=[1, 2, 1]" in output
        assert "final=final" in output
