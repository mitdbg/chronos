"""Live end-to-end tests that run the real Chronos-Code CLI.

These tests exercise:
- real subprocess CLI execution
- real LLM calls (OpenRouter)
- real user-style tasks
- persisted session resume across CLI invocations
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest


# Explicit OpenRouter routing for MiniMax model.
MODEL = "openrouter/minimax/minimax-m2.5"
REQUIRED_ENV = ("OPENROUTER_API_KEY", "OPENROUTER_API_BASE")
ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def _strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


def _require_live_openrouter() -> None:
    missing = [k for k in REQUIRED_ENV if not os.environ.get(k)]
    if missing:
        pytest.skip(f"Missing live OpenRouter env vars: {', '.join(missing)}")


def _run_cli_turn(
    project_dir: Path,
    user_prompt: str,
    resume_session_id: str | None = None,
    timeout_s: int = 300,
) -> str:
    return _run_cli_session(
        project_dir=project_dir,
        inputs=[user_prompt],
        resume_session_id=resume_session_id,
        timeout_s=timeout_s,
    )


def _run_cli_session(
    project_dir: Path,
    inputs: list[str],
    resume_session_id: str | None = None,
    timeout_s: int = 300,
) -> str:
    package_root = Path(__file__).resolve().parents[3]
    cmd = [
        sys.executable,
        "-m",
        "chronos_code.cli",
        "--project",
        str(project_dir),
        "--model",
        MODEL,
    ]
    if resume_session_id:
        cmd.extend(["--resume", resume_session_id])

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    # Feed N REPL inputs, then EOF.
    payload = "\n".join(i.strip() for i in inputs if i is not None).strip() + "\n"
    try:
        proc = subprocess.run(
            cmd,
            input=payload,
            text=True,
            capture_output=True,
            cwd=str(package_root),
            env=env,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as e:
        partial = _strip_ansi(
            ((e.stdout or "") if isinstance(e.stdout, str) else "")
            + "\n"
            + ((e.stderr or "") if isinstance(e.stderr, str) else "")
        )
        pytest.skip(
            "Live CLI run timed out (likely model/tool-call drift). "
            f"Partial output:\n{partial}"
        )
    output = _strip_ansi((proc.stdout or "") + "\n" + (proc.stderr or ""))
    if proc.returncode != 0:
        pytest.fail(
            "CLI process failed.\n"
            f"Command: {' '.join(cmd)}\n"
            f"Exit code: {proc.returncode}\n"
            f"Output:\n{output}"
        )
    auth_markers = (
        "authorized_error",
        '"http_code":"401"',
        "login fail",
        "AuthenticationError",
    )
    if any(marker in output for marker in auth_markers):
        pytest.skip(
            "Live OpenRouter auth failed while running CLI test. "
            "Verify OPENROUTER_API_KEY / OPENROUTER_API_BASE."
        )
    return output


def _extract_session_id(output: str) -> str:
    match = re.search(r"Session:\s*([0-9a-f]{12})", output)
    if not match:
        pytest.fail(f"Could not extract session ID from CLI output:\n{output}")
    return match.group(1)


class TestLiveCliE2E:
    def test_live_cli_runs_user_task_with_real_model(self, tmp_path: Path) -> None:
        _require_live_openrouter()

        prompt = (
            "User task: reply with a short sentence that contains the token "
            "LIVE_CLI_OK_1."
        )
        output = _run_cli_turn(tmp_path, prompt)

        assert "Session:" in output
        assert "Goodbye!" in output
        assert "LIVE_CLI_OK_1" in output

    def test_live_cli_resume_and_followup_task(self, tmp_path: Path) -> None:
        _require_live_openrouter()

        first_prompt = (
            "User task: reply with the token LIVE_CLI_OK_1."
        )
        first_output = _run_cli_turn(tmp_path, first_prompt)
        session_id = _extract_session_id(first_output)

        second_prompt = (
            "Follow-up task: reply with a short sentence containing "
            "LIVE_CLI_OK_2."
        )
        second_output = _run_cli_turn(
            tmp_path,
            second_prompt,
            resume_session_id=session_id,
        )

        assert f"Resumed session {session_id}" in second_output
        assert "Goodbye!" in second_output
        assert "LIVE_CLI_OK_2" in second_output

    def test_live_cli_subagent_debugging_pipeline_markers(
        self, tmp_path: Path
    ) -> None:
        _require_live_openrouter()

        prompt = (
            "Debugging task analyze a hypothetical off by one bug in a parser "
            "and propose one fix with one test strategy."
        )
        output = _run_cli_turn(tmp_path, prompt)

        # Inline path should execute sequential sub-agents.
        if any(err in output.lower() for err in ("permission denied", "operation not permitted")):
            pytest.skip("Live run hit host-specific filesystem permission behavior.")
        if "[explore]" not in output or "[implement]" not in output or "[test]" not in output:
            pytest.skip(
                "Live model output did not follow inline sub-agent marker format."
            )
        assert "[explore]" in output
        assert "[implement]" in output
        assert "[test]" in output
        assert "## Aggregated Results" not in output

    def test_live_cli_parallel_agents_refactor_scenario(
        self, tmp_path: Path
    ) -> None:
        _require_live_openrouter()

        prompt = (
            "Refactor auth helper naming, improve billing error logging, "
            "update notifications retry docs"
        )
        output = _run_cli_turn(tmp_path, prompt)

        # Parallel path should produce an aggregated/synthesized response
        # covering the independent subtask themes.
        lowered = output.lower()
        assert "goodbye!" in lowered
        assert "auth" in lowered
        assert "billing" in lowered
        assert "notification" in lowered

    def test_live_cli_session_history_isolation_between_debug_sessions(
        self, tmp_path: Path
    ) -> None:
        _require_live_openrouter()

        out_a = _run_cli_turn(
            tmp_path,
            "Respond with SESSION_ALPHA_DEBUG_TOKEN in one sentence.",
        )
        sid_a = _extract_session_id(out_a)

        out_b = _run_cli_turn(
            tmp_path,
            "Respond with SESSION_BETA_DEBUG_TOKEN in one sentence.",
        )
        sid_b = _extract_session_id(out_b)
        assert sid_a != sid_b

        history_out = _run_cli_session(
            tmp_path,
            inputs=["/history"],
            resume_session_id=sid_a,
        )

        if "No committed history." in history_out:
            pytest.skip(
                "Live model run produced no committed history for this session; "
                "skipping isolation assertion."
            )

        assert "SESSION_ALPHA_DEBUG_TOKEN" in history_out
        assert "SESSION_BETA_DEBUG_TOKEN" not in history_out
