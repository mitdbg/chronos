"""Unit tests for embedded MCP trace event emission."""

from __future__ import annotations

import pytest

from chronos_code.mcp_server.embedded import EmbeddedChronosMcpServer


def test_embedded_server_emits_session_registration_events() -> None:
    events: list[dict] = []
    server = EmbeddedChronosMcpServer(event_sink=lambda payload: events.append(payload))

    server.register_session("branch-txn2-implement", object())
    server.unregister_session("branch-txn2-implement")

    event_types = [str(evt.get("type")) for evt in events]
    assert event_types == ["mcp_session_registered", "mcp_session_unregistered"]


def test_invoke_with_trace_emits_call_and_result() -> None:
    events: list[dict] = []
    server = EmbeddedChronosMcpServer(event_sink=lambda payload: events.append(payload))

    result = server._invoke_with_trace(
        tool_name="chronos_txn",
        session_id="branch-txn2-implement",
        payload={"action": "status"},
        invoke_fn=lambda: "ok",
    )

    assert result == "ok"
    assert events[0]["type"] == "mcp_tool_call"
    assert events[1]["type"] == "mcp_tool_result"
    assert events[1]["result"] == "ok"


def test_invoke_with_trace_emits_error_event() -> None:
    events: list[dict] = []
    server = EmbeddedChronosMcpServer(event_sink=lambda payload: events.append(payload))

    def _boom() -> str:
        raise RuntimeError("fail")

    with pytest.raises(RuntimeError, match="fail"):
        server._invoke_with_trace(
            tool_name="chronos_bash",
            session_id="branch-txn9-implement",
            payload={"command": "pwd"},
            invoke_fn=_boom,
        )

    assert events[0]["type"] == "mcp_tool_call"
    assert events[1]["type"] == "mcp_tool_error"
    assert "fail" in str(events[1].get("error"))
