"""Unit tests for CLI LLM JSON logging wrapper."""

import asyncio
import json
from pathlib import Path

import pytest

from janus_code.cli import _build_logging_llm_fn


def _read_jsonl(path: Path) -> list[dict]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]


def test_logging_llm_fn_appends_one_json_record_per_call(tmp_path: Path) -> None:
    log_path = tmp_path / "llm.jsonl"

    def llm_fn(messages, tool_schemas):
        return {
            "content": "ok",
            "tool_calls": None,
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 2,
                "total_tokens": 12,
            },
        }

    wrapped = _build_logging_llm_fn(
        llm_fn=llm_fn,
        log_path=log_path,
        get_session_id=lambda: "session_1",
        model="openrouter/minimax/minimax-m2.5",
    )

    async def run_calls() -> None:
        await wrapped([{"role": "user", "content": "hi"}], [])
        await wrapped([{"role": "user", "content": "again"}], [])

    asyncio.run(run_calls())

    records = _read_jsonl(log_path)
    assert len(records) == 2
    assert records[0]["session_id"] == "session_1"
    assert records[0]["model"] == "openrouter/minimax/minimax-m2.5"
    assert records[0]["llm_call_id"] == 1
    assert records[1]["llm_call_id"] == 2
    assert records[0]["response"]["content"] == "ok"
    assert records[0]["usage"]["total_tokens"] == 12


def test_logging_llm_fn_records_error_and_reraises(tmp_path: Path) -> None:
    log_path = tmp_path / "llm.jsonl"

    def llm_fn(messages, tool_schemas):
        raise RuntimeError("boom")

    wrapped = _build_logging_llm_fn(
        llm_fn=llm_fn,
        log_path=log_path,
        get_session_id=lambda: "session_2",
        model="openrouter/minimax/minimax-m2.5",
    )

    async def run_call() -> None:
        await wrapped([{"role": "user", "content": "fail"}], [])

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(run_call())

    records = _read_jsonl(log_path)
    assert len(records) == 1
    assert records[0]["session_id"] == "session_2"
    assert records[0]["response"] is None
    assert records[0]["error"] == "boom"
