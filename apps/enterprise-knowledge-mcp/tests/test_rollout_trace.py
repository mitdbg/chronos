from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
from typing import Any

import pytest
from chronos_enterprise_knowledge.rollout_trace import (
    EventReplayResult,
    RolloutAnalyzer,
    RolloutTraceError,
    WorkloadEvent,
    WorkloadReplayReport,
    WorkloadReplayer,
    WorkloadTrace,
    _resolve_merge_selection,
    _apply_recorded_patch,
    _extract_nested_actions,
    _mcp_structured_result,
    resolve_memory_timestamps,
)


class _Backend:
    backend_name = "fake"


class _Service:
    def __init__(self, root: Path):
        self.backend = _Backend()
        self.root = root

    def status(self) -> dict[str, Any]:
        return {
            "backend": "fake",
            "branches": ["main"],
            "embedding_model": "test",
            "embedding_dimensions": 3,
            "storage": ["fake"],
        }

    def checkout(
        self,
        branch_id: str,
        *,
        from_branch: str | None = None,
        mount: bool = True,
        mount_path: str | None = None,
    ) -> dict[str, Any]:
        del mount_path
        path = self.root / branch_id.replace("/", "-")
        path.mkdir(parents=True, exist_ok=True)
        result: dict[str, Any] = {
            "branch_id": branch_id,
            "created": True,
            "from_branch": from_branch,
        }
        if mount:
            result["workspace_path"] = str(path)
        return result


def test_successful_shell_replay_can_improve_on_recorded_failure() -> None:
    event = EventReplayResult(
        sequence=0,
        kind="shell",
        name="exec_command",
        elapsed_ns=1,
        status="ok",
        expected_status="error",
        normalized_digest="actual",
        expected_digest="recorded",
        matched=False,
    )

    report = WorkloadReplayReport("trace", "backend", (event,), 1, {})

    assert report.succeeded
    assert not report.matched


def test_memory_timestamp_resolution_retries_final_filesystem_visibility() -> None:
    trace = WorkloadTrace(
        "memory-visibility",
        (
            WorkloadEvent(
                0,
                "mcp",
                "knowledge_remember",
                {"branch_id": "person/alice"},
                expected={
                    "result_summary": {
                        "path": "/memory/semantic_memory/lesson.md",
                        "memory_id": "memory_lesson",
                    }
                },
            ),
        ),
    )

    class DelayedBackend:
        calls = 0

        def read_file(self, branch_id: str, path: str) -> bytes:
            assert branch_id == "person/alice"
            assert path == "/memory/semantic_memory/lesson.md"
            self.calls += 1
            if self.calls < 3:
                raise RuntimeError("path not found")
            return b"# Lesson\n\nRecorded: 2026-07-28T01:12:12Z\n"

    backend = DelayedBackend()
    resolved, count = resolve_memory_timestamps(trace, backend)

    assert count == 1
    assert backend.calls == 3
    assert (
        resolved.events[0].arguments["recorded_at"]
        == "2026-07-28T01:12:12Z"
    )


def test_memory_timestamp_uses_rollout_time_after_branch_deletion() -> None:
    trace = WorkloadTrace(
        "deleted-memory-branch",
        (
            WorkloadEvent(
                0,
                "mcp",
                "knowledge_remember",
                {"branch_id": "task/deleted"},
                timestamp="2026-07-28T03:39:00.125Z",
                expected={
                    "result_summary": {
                        "path": "/memory/episodic_memory/incident.md",
                        "memory_id": "memory_incident",
                    }
                },
            ),
        ),
    )

    class DeletedBranchBackend:
        def read_file(self, branch_id: str, path: str) -> bytes:
            del branch_id, path
            raise RuntimeError("branch not found: task/deleted")

    resolved, count = resolve_memory_timestamps(
        trace,
        DeletedBranchBackend(),
    )

    assert count == 1
    assert (
        resolved.events[0].arguments["recorded_at"]
        == "2026-07-28T03:39:00.125Z"
    )
    assert resolved.metadata["memory_timestamps_from_event"] == 1


def test_analyzer_extracts_nested_shell_and_patch_calls(
    tmp_path: Path,
) -> None:
    rollout = tmp_path / "rollout.jsonl"
    rows = [
        {
            "timestamp": "2026-01-01T00:00:00Z",
            "type": "session_meta",
            "payload": {"session_id": "nested-tools", "cwd": str(tmp_path)},
        },
        {
            "timestamp": "2026-01-01T00:00:01Z",
            "type": "response_item",
            "payload": {
                "type": "custom_tool_call",
                "name": "exec",
                "call_id": "exec-1",
                "input": (
                    "const w=\"" + str(tmp_path) + "\";\n"
                    "const patch=`*** Begin Patch\\n"
                    "*** Add File: ${w}/state\\n"
                    "+value\\n*** End Patch`;\n"
                    "await tools.apply_patch(patch);\n"
                    "await tools.shell_command({command:\"printf value\",workdir:w,timeout_ms:1000});\n"
                ),
            },
        },
        {
            "timestamp": "2026-01-01T00:00:01.010Z",
            "type": "event_msg",
            "payload": {
                "type": "patch_apply_end",
                "success": True,
                "stdout": "Success. Updated the following files:\nA state\n",
            },
        },
        {
            "timestamp": "2026-01-01T00:00:01.020Z",
            "type": "response_item",
            "payload": {
                "type": "custom_tool_call_output",
                "call_id": "exec-1",
                "output": [
                        {"type": "input_text", "text": "Script completed\n"},
                    {"type": "input_text", "text": "{}"},
                    {
                        "type": "input_text",
                        "text": "Exit code: 0\nWall time: 0.0s\nOutput:\nvalue",
                    },
                ],
            },
        },
    ]
    rollout.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    analysis = RolloutAnalyzer().analyze(rollout)

    assert analysis.as_dict()["fully_replayable"]
    assert analysis.skipped_calls == ()
    assert [event.kind for event in analysis.trace.events] == ["patch", "shell"]
    assert analysis.trace.events[0].arguments["patch"].endswith(
        "*** Add File: {{repo}}/state\n+value\n*** End Patch"
    )
    assert analysis.trace.events[1].expected["status"] == "ok"


def test_mcp_structured_result_decodes_nested_text_envelope() -> None:
    result = _mcp_structured_result(
        {
            "Ok": {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "content": [
                                    {
                                        "type": "text",
                                        "text": json.dumps(
                                            {
                                                "preview_token": "token",
                                                "selection_groups": {
                                                    "filesystem_paths": {
                                                        "/artifact": [
                                                            "filesystem:1"
                                                        ]
                                                    }
                                                },
                                            }
                                        ),
                                    }
                                ],
                                "isError": False,
                            }
                        ),
                    }
                ]
            }
        }
    )

    assert result == {
        "preview_token": "token",
        "selection_groups": {
            "filesystem_paths": {"/artifact": ["filesystem:1"]}
        },
    }


def test_analyzer_expands_nested_shell_loop(
    tmp_path: Path,
) -> None:
    rollout = tmp_path / "rollout-loop.jsonl"
    rows = [
        {
            "timestamp": "2026-01-01T00:00:00Z",
            "type": "session_meta",
            "payload": {"session_id": "nested-loop", "cwd": str(tmp_path)},
        },
        {
            "timestamp": "2026-01-01T00:00:01Z",
            "type": "response_item",
            "payload": {
                "type": "custom_tool_call",
                "name": "exec",
                "call_id": "exec-loop",
                "input": (
                    'const cmds=["printf one","printf two"];\n'
                    'for (const command of cmds) {'
                    'const r=await tools.shell_command('
                    '{command,workdir:"/tmp",timeout_ms:1000});'
                    'text(JSON.stringify({command,output:r}));}'
                ),
            },
        },
        {
            "timestamp": "2026-01-01T00:00:01.010Z",
            "type": "response_item",
            "payload": {
                "type": "custom_tool_call_output",
                "call_id": "exec-loop",
                "output": [
                    {"type": "input_text", "text": "Script completed\n"},
                    {
                        "type": "input_text",
                        "text": "{\"command\":\"printf one\",\"output\":"
                        "\"Process exited with code 0\\nOutput: one\"}",
                    },
                    {
                        "type": "input_text",
                        "text": "{\"command\":\"printf two\",\"output\":"
                        "\"Process exited with code 0\\nOutput: two\"}",
                    },
                ],
            },
        },
    ]
    rollout.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    analysis = RolloutAnalyzer().analyze(rollout)

    assert analysis.as_dict()["fully_replayable"]
    assert [event.arguments["cmd"] for event in analysis.trace.events] == [
        "printf one",
        "printf two",
    ]


def test_nested_parser_ignores_array_text_inside_patch_string() -> None:
    source = (
        'const patch="const values = [\\"one\\"];\\n";'
        "await tools.apply_patch(patch);"
    )

    actions = _extract_nested_actions(source)

    assert len(actions) == 1
    assert actions[0].kind == "patch"
    assert actions[0].arguments["patch"] == 'const values = ["one"];\n'


def test_analyzer_records_llm_latency_without_tool_execution(
    tmp_path: Path,
) -> None:
    rows = [
        {
            "timestamp": "2026-01-01T00:00:00.000Z",
            "type": "session_meta",
            "payload": {"session_id": "timed", "cwd": str(tmp_path)},
        },
        {
            "timestamp": "2026-01-01T00:00:01.000Z",
            "type": "event_msg",
            "payload": {"type": "user_message", "message": "inspect"},
        },
        {
            "timestamp": "2026-01-01T00:00:03.500Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "exec_command",
                "call_id": "shell",
                "arguments": json.dumps({"cmd": "true"}),
            },
        },
        {
            "timestamp": "2026-01-01T00:00:08.000Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": "shell",
                "output": "Process exited with code 0\nOutput:\n",
            },
        },
        {
            "timestamp": "2026-01-01T00:00:08.001Z",
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": 100,
                        "output_tokens": 20,
                    }
                },
            },
        },
        {
            "timestamp": "2026-01-01T00:00:09.501Z",
            "type": "event_msg",
            "payload": {
                "type": "agent_message",
                "phase": "final_answer",
                "message": "done",
            },
        },
        {
            "timestamp": "2026-01-01T00:00:09.502Z",
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": 120,
                        "output_tokens": 10,
                    }
                },
            },
        },
        {
            "timestamp": "2026-01-01T00:00:09.503Z",
            "type": "event_msg",
            "payload": {
                "type": "task_complete",
                "duration_ms": 8503,
                "time_to_first_token_ms": 700,
            },
        },
    ]
    rollout = tmp_path / "rollout-timing.jsonl"
    rollout.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    timing = RolloutAnalyzer().analyze(rollout).trace.metadata["llm_timing"]

    assert timing["call_count"] == 2
    assert timing["total_ms"] == 4000.0
    assert timing["p50_ms"] == 1500.0
    assert timing["p95_ms"] == 2500.0
    assert timing["turn_duration_ms"] == 8503
    assert timing["non_llm_ms"] == 4503.0
    assert timing["time_to_first_token_ms"] == 700
    assert timing["calls"][0]["latency_ms"] == 2500.0
    assert timing["calls"][0]["usage"]["output_tokens"] == 20
    assert timing["calls"][1]["latency_ms"] == 1500.0


def test_replayer_can_replay_recorded_llm_latency_without_store_attribution(
    tmp_path: Path,
) -> None:
    trace = WorkloadTrace(
        "llm-replay",
        (
            WorkloadEvent(
                0,
                "mcp",
                "knowledge_status",
                {},
            ),
        ),
        {
            "llm_timing": {
                "total_ms": 30.0,
                "calls": [
                    {"latency_ms": 10.0},
                    {"latency_ms": 20.0},
                ],
            }
        },
    )
    report = WorkloadReplayer(
        _Service(tmp_path),
        repo_dir=tmp_path,
        replay_llm_latency=True,
        llm_latency_scale=0.01,
    ).replay(trace)

    assert report.succeeded
    assert report.llm_latency_enabled
    assert report.llm_recorded_ms == 30.0
    assert report.llm_calls_slept == 2
    assert report.llm_slept_ms == pytest.approx(0.3)
    assert report.events[0].model_inference_ms == pytest.approx(0.3)
    assert report.events[0].timing_ms["others"] == pytest.approx(0.0)


def test_replayer_uses_replay_time_merge_preview_token(tmp_path: Path) -> None:
    class MergeService(_Service):
        def __init__(self, root: Path):
            super().__init__(root)
            self.received_token: str | None = None

        def merge_preview(self, source: str, target: str) -> dict[str, Any]:
            assert (source, target) == ("task/a", "main")
            return {"preview_token": "replay-token", "changes": []}

        def merge(
            self,
            source: str,
            target: str,
            *,
            selected_change_ids: list[str] | None,
            preview_token: str | None,
            conflict_choices: dict[str, str] | None,
            operation_id: str | None,
        ) -> dict[str, Any]:
            del selected_change_ids, conflict_choices, operation_id
            assert (source, target) == ("task/a", "main")
            self.received_token = preview_token
            return {"status": "committed"}

    trace = WorkloadTrace(
        "dynamic-preview-token",
        (
            WorkloadEvent(
                0,
                "mcp",
                "knowledge_merge_preview",
                {"source_branch": "task/a", "target_branch": "main"},
                expected={"status": "ok"},
            ),
            WorkloadEvent(
                1,
                "mcp",
                "knowledge_merge",
                {
                    "source_branch": "task/a",
                    "target_branch": "main",
                    "preview_token": "capture-token",
                    "selected_change_ids": [],
                },
                expected={"status": "ok"},
            ),
        ),
    )
    service = MergeService(tmp_path / "checkouts")

    report = WorkloadReplayer(service, repo_dir=tmp_path).replay(trace)  # type: ignore[arg-type]

    assert report.succeeded
    assert service.received_token == "replay-token"


@pytest.mark.parametrize("merge_retries", [1, -1])
def test_replayer_carries_prepared_preview_and_quiesces_retries_once(
    tmp_path: Path,
    merge_retries: int,
) -> None:
    class PreparedPreview(dict[str, Any]):
        def __init__(self, token: str):
            super().__init__(preview_token=token, changes=[])
            self.atomic_preview = self

    class QuiescingBackend(_Backend):
        def __init__(self) -> None:
            self.quiesce_events: list[str] = []

        @contextlib.contextmanager
        def merge_quiesce(self, source: str, target: str):
            self.quiesce_events.append(f"enter:{source}->{target}")
            try:
                yield
            finally:
                self.quiesce_events.append(f"exit:{source}->{target}")

    class MergeService(_Service):
        def __init__(self, root: Path):
            super().__init__(root)
            self.backend = QuiescingBackend()
            self.preview_count = 0
            self.merge_count = 0
            self.received_previews: list[Any] = []

        def merge_preview(self, source: str, target: str) -> PreparedPreview:
            assert (source, target) == ("task/a", "main")
            self.preview_count += 1
            return PreparedPreview(f"token-{self.preview_count}")

        def merge(
            self,
            source: str,
            target: str,
            *,
            selected_change_ids: list[str] | None,
            preview_token: str | None,
            conflict_choices: dict[str, str] | None,
            operation_id: str | None,
            prepared_preview: Any | None = None,
        ) -> dict[str, Any]:
            del selected_change_ids, conflict_choices, operation_id
            assert (source, target) == ("task/a", "main")
            self.merge_count += 1
            self.received_previews.append(prepared_preview)
            if self.merge_count == 1:
                raise RuntimeError("target branch commit already in progress")
            assert preview_token == "token-2"
            return {"status": "committed"}

    trace = WorkloadTrace(
        "prepared-preview-retry",
        (
            WorkloadEvent(
                0,
                "mcp",
                "knowledge_merge_preview",
                {"source_branch": "task/a", "target_branch": "main"},
                expected={"status": "ok"},
            ),
            WorkloadEvent(
                1,
                "mcp",
                "knowledge_merge",
                {
                    "source_branch": "task/a",
                    "target_branch": "main",
                    "preview_token": "capture-token",
                    "selected_change_ids": [],
                },
                expected={"status": "ok"},
            ),
        ),
    )
    service = MergeService(tmp_path / "checkouts")

    report = WorkloadReplayer(
        service,
        repo_dir=tmp_path,
        merge_retries=merge_retries,
    ).replay(trace)  # type: ignore[arg-type]

    assert report.succeeded
    assert service.preview_count == 2
    assert service.merge_count == 2
    assert all(item is not None for item in service.received_previews)
    assert service.received_previews[0] is not service.received_previews[1]
    assert service.backend.quiesce_events == [
        "enter:task/a->main",
        "exit:task/a->main",
    ]


def test_replayer_does_not_retry_an_expected_merge_error_in_unlimited_mode(
    tmp_path: Path,
) -> None:
    class MergeService(_Service):
        def __init__(self, root: Path):
            super().__init__(root)
            self.preview_count = 0
            self.merge_count = 0

        def merge_preview(self, source: str, target: str) -> dict[str, Any]:
            assert (source, target) == ("task/a", "main")
            self.preview_count += 1
            return {"preview_token": f"token-{self.preview_count}", "changes": []}

        def merge(
            self,
            source: str,
            target: str,
            *,
            selected_change_ids: list[str] | None,
            preview_token: str | None,
            conflict_choices: dict[str, str] | None,
            operation_id: str | None,
        ) -> dict[str, Any]:
            del selected_change_ids, preview_token, conflict_choices, operation_id
            assert (source, target) == ("task/a", "main")
            self.merge_count += 1
            raise RuntimeError("target branch commit already in progress")

    trace = WorkloadTrace(
        "expected-merge-error",
        (
            WorkloadEvent(
                0,
                "mcp",
                "knowledge_merge_preview",
                {"source_branch": "task/a", "target_branch": "main"},
                expected={"status": "ok"},
            ),
            WorkloadEvent(
                1,
                "mcp",
                "knowledge_merge",
                {
                    "source_branch": "task/a",
                    "target_branch": "main",
                    "preview_token": "capture-token",
                    "selected_change_ids": [],
                },
                expected={"status": "error"},
            ),
        ),
    )
    service = MergeService(tmp_path / "checkouts")

    report = WorkloadReplayer(
        service,
        repo_dir=tmp_path,
        merge_retries=-1,
    ).replay(trace)  # type: ignore[arg-type]

    assert report.succeeded
    assert service.preview_count == 1
    assert service.merge_count == 1
    assert report.events[-1].status == "error"
    assert report.events[-1].expected_status == "error"


def test_retryable_merge_error_accepts_machine_readable_progress_marker() -> None:
    from chronos_enterprise_knowledge.rollout_trace import _retryable_merge_error

    assert _retryable_merge_error(
        RuntimeError("chronos_session_barrier_in_progress: team/site-reliability")
    )


def test_replayer_resolves_logical_merge_selection_against_fresh_preview(
    tmp_path: Path,
) -> None:
    class MergeService(_Service):
        def __init__(self, root: Path):
            super().__init__(root)
            self.selected: list[str] | None = None

        def merge_preview(self, source: str, target: str) -> dict[str, Any]:
            del source, target
            return {
                "preview_token": "fresh-token",
                "selection_groups": {
                    "indexed_documents": {
                        "policy": ["sqlite:fresh", "qdrant:fresh"],
                    },
                    "filesystem_paths": {
                        "/knowledge": ["filesystem:fresh"],
                    },
                },
            }

        def merge(
            self,
            source: str,
            target: str,
            *,
            selected_change_ids: list[str] | None,
            preview_token: str | None,
            conflict_choices: dict[str, str] | None,
            operation_id: str | None,
        ) -> dict[str, Any]:
            del source, target, conflict_choices, operation_id
            assert preview_token == "fresh-token"
            self.selected = selected_change_ids
            return {"status": "committed"}

    trace = WorkloadTrace(
        "logical-selection",
        (
            WorkloadEvent(
                0,
                "mcp",
                "knowledge_merge_preview",
                {"source_branch": "task/a", "target_branch": "main"},
                expected={"status": "ok"},
            ),
            WorkloadEvent(
                1,
                "mcp",
                "knowledge_merge",
                {
                    "source_branch": "task/a",
                    "target_branch": "main",
                    "preview_token": "capture-token",
                    "selected_change_ids": ["capture-only"],
                },
                expected={
                    "status": "ok",
                    "merge_selection_groups": [
                        {"category": "indexed_documents", "key": "policy"},
                        {"category": "filesystem_paths", "key": "/knowledge"},
                    ],
                },
            ),
        ),
    )
    service = MergeService(tmp_path / "checkouts")

    report = WorkloadReplayer(service, repo_dir=tmp_path).replay(trace)  # type: ignore[arg-type]

    assert report.succeeded
    assert service.selected == [
        "filesystem:fresh",
        "qdrant:fresh",
        "sqlite:fresh",
    ]


def test_replayer_preserves_failed_partial_merge_selection_shape() -> None:
    preview = {
        "selection_groups": {
            "indexed_documents": {
                "report": ["sqlite:r1", "qdrant:r1"],
                "memory": ["sqlite:m1", "qdrant:m1"],
            },
            "filesystem_paths": {"/artifact": ["filesystem:a1"]},
        }
    }

    selected = _resolve_merge_selection(
        preview,
        [{"category": "indexed_documents", "key": "memory"}],
        [
            {
                "category": "indexed_documents",
                "key": "report",
                "count": 1,
            }
        ],
    )

    assert selected == ["qdrant:m1", "qdrant:r1", "sqlite:m1"]


def test_rollout_analyzer_extracts_mcp_and_portable_shell_paths(
    tmp_path: Path,
) -> None:
    source_cwd = tmp_path / "repo"
    checkout = source_cwd / "state/checkouts/task-a"
    checkout.mkdir(parents=True)
    rows = [
        {
            "timestamp": "2026-01-01T00:00:00Z",
            "type": "session_meta",
            "payload": {
                "session_id": "session-a",
                "cwd": str(source_cwd),
            },
        },
        {
            "timestamp": "2026-01-01T00:00:01Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "knowledge_checkout",
                "call_id": "checkout",
                "arguments": json.dumps(
                    {
                        "branch_id": "task/a",
                        "from_branch": "main",
                        "mount": True,
                    }
                ),
            },
        },
        {
            "timestamp": "2026-01-01T00:00:02Z",
            "type": "event_msg",
            "payload": {
                "type": "mcp_tool_call_end",
                "call_id": "checkout",
                "invocation": {
                    "server": "chronos_enterprise_knowledge",
                    "tool": "knowledge_checkout",
                    "arguments": {
                        "branch_id": "task/a",
                        "from_branch": "main",
                        "mount": True,
                    },
                },
                "result": {
                    "Ok": {
                        "isError": False,
                        "structuredContent": {
                            "branch_id": "task/a",
                            "created": True,
                            "from_branch": "main",
                            "workspace_path": str(checkout),
                        },
                        "content": [],
                    }
                },
            },
        },
        {
            "timestamp": "2026-01-01T00:00:03Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "exec_command",
                "call_id": "shell",
                "arguments": json.dumps(
                    {
                        "cmd": "find state/checkouts/task-a -type f",
                        "workdir": str(source_cwd),
                    }
                ),
            },
        },
        {
            "timestamp": "2026-01-01T00:00:04Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": "shell",
                "output": (
                    "Chunk ID: test\nProcess exited with code 0\n"
                    "Original token count: 0\nOutput:\n"
                ),
            },
        },
    ]
    rollout = tmp_path / "rollout.jsonl"
    rollout.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    analysis = RolloutAnalyzer().analyze(rollout)

    assert analysis.trace.trace_id == "session-a"
    assert analysis.as_dict()["fully_replayable"]
    assert [event.kind for event in analysis.trace.events] == ["mcp", "shell"]
    shell = analysis.trace.events[1]
    assert shell.arguments["cmd"] == "find {{workspace:task/a}} -type f"
    assert shell.arguments["workdir"] == "{{repo}}"
    assert not shell.expected["truncated"]


def test_rollout_analyzer_does_not_treat_dot_as_a_workspace_path(
    tmp_path: Path,
) -> None:
    source_cwd = tmp_path / "checkout"
    source_cwd.mkdir()
    rows = [
        {
            "timestamp": "2026-01-01T00:00:00Z",
            "type": "session_meta",
            "payload": {
                "session_id": "session-dot",
                "cwd": str(source_cwd),
            },
        },
        {
            "timestamp": "2026-01-01T00:00:01Z",
            "type": "event_msg",
            "payload": {
                "type": "mcp_tool_call_end",
                "call_id": "checkout",
                "invocation": {
                    "server": "chronos_enterprise_knowledge",
                    "tool": "knowledge_checkout",
                    "arguments": {
                        "branch_id": "task/a",
                        "from_branch": "main",
                        "mount": True,
                        "mount_path": str(source_cwd),
                    },
                },
                "result": {
                    "Ok": {
                        "isError": False,
                        "structuredContent": {
                            "branch_id": "task/a",
                            "created": True,
                            "from_branch": "main",
                            "workspace_path": str(source_cwd),
                        },
                        "content": [],
                    }
                },
            },
        },
        {
            "timestamp": "2026-01-01T00:00:02Z",
            "type": "event_msg",
            "payload": {
                "type": "mcp_tool_call_end",
                "call_id": "search",
                "invocation": {
                    "server": "chronos_enterprise_knowledge",
                    "tool": "knowledge_search",
                    "arguments": {
                        "branch_id": "task/a",
                        "query": "config.yaml",
                    },
                },
                "result": {
                    "Ok": {
                        "isError": False,
                        "structuredContent": [
                            {
                                "document_id": "doc",
                                "chunk_id": "doc:0",
                                "path": "/code/config.yaml",
                                "title": "config.yaml",
                                "source": "test",
                            }
                        ],
                        "content": [],
                    }
                },
            },
        },
    ]
    rollout = tmp_path / "rollout-dot.jsonl"
    rollout.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    trace = RolloutAnalyzer().analyze(rollout).trace

    assert "mount_path" not in trace.events[0].arguments
    assert trace.events[1].arguments["query"] == "config.yaml"
    assert (
        trace.events[1].expected["result_summary"][0]["path"]
        == "/code/config.yaml"
    )


def test_rollout_analyzer_does_not_replace_workspace_suffix_in_branch_id(
    tmp_path: Path,
) -> None:
    source_cwd = tmp_path / "capture"
    workspace = source_cwd / "flashinfer-capacity-v2"
    workspace.mkdir(parents=True)
    branch_id = "task/lena/flashinfer-capacity-v2"
    rows = [
        {
            "timestamp": "2026-01-01T00:00:00Z",
            "type": "session_meta",
            "payload": {"session_id": "session-suffix", "cwd": str(source_cwd)},
        },
        {
            "timestamp": "2026-01-01T00:00:01Z",
            "type": "event_msg",
            "payload": {
                "type": "mcp_tool_call_end",
                "call_id": "checkout",
                "invocation": {
                    "server": "chronos_enterprise_knowledge",
                    "tool": "knowledge_checkout",
                    "arguments": {
                        "branch_id": branch_id,
                        "from_branch": "main",
                        "mount": True,
                    },
                },
                "result": {
                    "Ok": {
                        "isError": False,
                        "structuredContent": {
                            "branch_id": branch_id,
                            "created": True,
                            "from_branch": "main",
                            "workspace_path": str(workspace),
                        },
                        "content": [],
                    }
                },
            },
        },
    ]
    rollout = tmp_path / "rollout-suffix.jsonl"
    rollout.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    event = RolloutAnalyzer().analyze(rollout).trace.events[0]

    assert event.arguments["branch_id"] == branch_id
    assert event.expected["result_summary"]["branch_id"] == branch_id


def test_incomplete_rollout_is_marked_and_refused(tmp_path: Path) -> None:
    row = {
        "timestamp": "2026-01-01T00:00:00Z",
        "type": "response_item",
        "payload": {
            "type": "function_call",
            "name": "write_stdin",
            "call_id": "interactive",
            "arguments": json.dumps(
                {"session_id": 7, "chars": "yes\n"}
            ),
        },
    }
    rollout = tmp_path / "rollout.jsonl"
    rollout.write_text(json.dumps(row) + "\n", encoding="utf-8")
    analysis = RolloutAnalyzer().analyze(rollout)

    assert not analysis.as_dict()["fully_replayable"]
    assert analysis.trace.metadata["skipped_call_counts"] == {
        "write_stdin": 1
    }
    with pytest.raises(RolloutTraceError, match="unsupported Codex tool calls"):
        WorkloadReplayer(
            _Service(tmp_path / "checkouts"),  # type: ignore[arg-type]
            repo_dir=tmp_path,
        ).replay(analysis.trace)


def test_empty_write_stdin_poll_is_not_a_workload_action(
    tmp_path: Path,
) -> None:
    row = {
        "timestamp": "2026-01-01T00:00:00Z",
        "type": "response_item",
        "payload": {
            "type": "function_call",
            "name": "write_stdin",
            "call_id": "poll",
            "arguments": json.dumps(
                {"session_id": 7, "chars": "", "yield_time_ms": 30_000}
            ),
        },
    }
    rollout = tmp_path / "rollout-poll.jsonl"
    rollout.write_text(json.dumps(row) + "\n", encoding="utf-8")

    analysis = RolloutAnalyzer().analyze(rollout)

    assert analysis.as_dict()["fully_replayable"]
    assert analysis.trace.events == ()


def test_ctrl_c_is_attached_to_its_running_shell_event(
    tmp_path: Path,
) -> None:
    rows = [
        {
            "timestamp": "2026-01-01T00:00:00Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "exec_command",
                "call_id": "exec",
                "arguments": json.dumps(
                    {"cmd": "sleep 30", "yield_time_ms": 10_000}
                ),
            },
        },
        {
            "timestamp": "2026-01-01T00:00:10Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": "exec",
                "output": (
                    "Process running with session ID 62053\n"
                    "Original token count: 0\nOutput:\n"
                ),
            },
        },
        {
            "timestamp": "2026-01-01T00:00:12Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "write_stdin",
                "call_id": "interrupt",
                "arguments": json.dumps(
                    {"session_id": 62053, "chars": "\u0003"}
                ),
            },
        },
        {
            "timestamp": "2026-01-01T00:00:12.01Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": "interrupt",
                "output": (
                    "Process exited with code 130\n"
                    "Original token count: 0\nOutput:\n"
                ),
            },
        },
    ]
    rollout = tmp_path / "rollout-interrupt.jsonl"
    rollout.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    analysis = RolloutAnalyzer().analyze(rollout)

    assert analysis.as_dict()["fully_replayable"]
    assert len(analysis.trace.events) == 1
    event = analysis.trace.events[0]
    assert event.arguments["interrupt_after_seconds"] == 12.0
    assert event.expected["interrupted"] is True
    assert event.expected["exit_code"] == 130
    assert "normalized_digest" not in event.expected


def test_replayer_interrupts_recorded_shell_process_group(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "completed"
    trace = WorkloadTrace(
        "interrupted-shell",
        (
            WorkloadEvent(
                0,
                "shell",
                "exec_command",
                {
                    "cmd": f"sleep 5; touch {marker}",
                    "interrupt_after_seconds": 0.05,
                },
                expected={
                    "status": "error",
                    "interrupted": True,
                    "exit_code": 130,
                },
            ),
        ),
    )

    report = WorkloadReplayer(
        _Service(tmp_path / "checkouts"),  # type: ignore[arg-type]
        repo_dir=tmp_path,
        allow_shell=True,
    ).replay(trace)

    assert report.events[0].shell_exit_code == 130
    assert not marker.exists()


def test_replayer_times_out_shell_process_group_and_reports_latency(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "completed"
    trace = WorkloadTrace(
        "timed-out-shell",
        (
            WorkloadEvent(
                0,
                "shell",
                "exec_command",
                {"cmd": f"sleep 5; touch {marker}"},
                expected={"status": "error", "exit_code": 124},
            ),
        ),
    )

    report = WorkloadReplayer(
        _Service(tmp_path / "checkouts"),  # type: ignore[arg-type]
        repo_dir=tmp_path,
        allow_shell=True,
        shell_timeout_seconds=0.05,
    ).replay(trace)

    assert report.succeeded
    assert report.events[0].shell_exit_code == 124
    assert report.events[0].elapsed_ns >= 40_000_000
    assert not marker.exists()


def test_replayer_caps_recorded_human_interrupt_delay(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "completed"
    trace = WorkloadTrace(
        "capped-interrupted-shell",
        (
            WorkloadEvent(
                0,
                "shell",
                "exec_command",
                {
                    "cmd": f"sleep 5; touch {marker}",
                    "interrupt_after_seconds": 600,
                },
                expected={
                    "status": "error",
                    "interrupted": True,
                    "exit_code": 130,
                },
            ),
        ),
    )

    report = WorkloadReplayer(
        _Service(tmp_path / "checkouts"),  # type: ignore[arg-type]
        repo_dir=tmp_path,
        allow_shell=True,
        max_interrupt_seconds=0.05,
    ).replay(trace)

    assert report.succeeded
    assert report.wall_time_ns < 2_000_000_000
    assert report.events[0].shell_exit_code == 130
    assert not marker.exists()


def test_replayer_isolates_trace_temporary_paths(tmp_path: Path) -> None:
    path_record = tmp_path / "trace-tmp-path"
    trace = WorkloadTrace(
        "isolated-temp",
        (
            WorkloadEvent(
                0,
                "shell",
                "exec_command",
                {
                    "cmd": (
                        "mkdir -p {{tmp}}/state; "
                        "touch {{tmp}}/state/ready; "
                        f"printf '%s' '{{{{tmp}}}}' > {path_record}"
                    ),
                },
                expected={"status": "ok", "exit_code": 0},
            ),
            WorkloadEvent(
                1,
                "shell",
                "exec_command",
                {"cmd": "test -f {{tmp}}/state/ready"},
                expected={"status": "ok", "exit_code": 0},
            ),
        ),
    )

    report = WorkloadReplayer(
        _Service(tmp_path / "checkouts"),  # type: ignore[arg-type]
        repo_dir=tmp_path,
        allow_shell=True,
    ).replay(trace)

    assert report.succeeded
    trace_tmp = Path(path_record.read_text(encoding="utf-8"))
    assert not trace_tmp.exists()


def test_replayer_isolates_literal_absolute_tmp_paths(tmp_path: Path) -> None:
    trace = WorkloadTrace(
        "isolated-absolute-temp",
        (
            WorkloadEvent(
                0,
                "shell",
                "exec_command",
                {
                    "cmd": (
                        "mkdir -p /tmp/chronos-literal-replay/state; "
                        "touch /tmp/chronos-literal-replay/state/ready"
                    )
                },
                expected={"status": "ok", "exit_code": 0},
            ),
            WorkloadEvent(
                1,
                "shell",
                "exec_command",
                {"cmd": "test -f /tmp/chronos-literal-replay/state/ready"},
                expected={"status": "ok", "exit_code": 0},
            ),
        ),
    )

    report = WorkloadReplayer(
        _Service(tmp_path / "checkouts"),  # type: ignore[arg-type]
        repo_dir=tmp_path,
        allow_shell=True,
    ).replay(trace)

    assert report.succeeded
    assert not Path("/tmp/chronos-literal-replay").exists()


def test_replayer_expands_checkout_path_and_runs_trusted_shell(
    tmp_path: Path,
) -> None:
    trace = WorkloadTrace(
        "filesystem-write",
        (
            WorkloadEvent(
                0,
                "mcp",
                "knowledge_checkout",
                {
                    "branch_id": "task/a",
                    "from_branch": "main",
                    "mount": True,
                },
            ),
            WorkloadEvent(
                1,
                "shell",
                "exec_command",
                {
                    "cmd": (
                        "printf 'deterministic\\n' > "
                        "'{{workspace:task/a}}/artifact.txt'"
                    ),
                    "workdir": "{{repo}}",
                },
            ),
        ),
    )
    service = _Service(tmp_path / "checkouts")

    report = WorkloadReplayer(
        service,  # type: ignore[arg-type]
        repo_dir=tmp_path,
        allow_shell=True,
    ).replay(trace)

    assert report.succeeded
    assert len(report.events) == 2
    assert (
        tmp_path / "checkouts/task-a/artifact.txt"
    ).read_text(encoding="utf-8") == "deterministic\n"


def test_replayer_treats_recorded_shell_failure_as_success(
    tmp_path: Path,
) -> None:
    trace = WorkloadTrace(
        "expected-shell-failure",
        (
            WorkloadEvent(
                0,
                "shell",
                "exec_command",
                {"cmd": "exit 7", "workdir": "{{repo}}"},
                expected={"status": "error", "exit_code": 7},
            ),
        ),
    )

    report = WorkloadReplayer(
        _Service(tmp_path / "checkouts"),  # type: ignore[arg-type]
        repo_dir=tmp_path,
        allow_shell=True,
    ).replay(trace)

    assert report.succeeded
    assert report.events[0].status == "error"
    assert report.events[0].expected_status == "error"
    assert report.as_dict()["events"][0]["expected_status"] == "error"


def test_replayer_does_not_inherit_tools_added_to_host_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    host_bin = tmp_path / "host-bin"
    host_bin.mkdir()
    host_tool = host_bin / "added-after-capture"
    host_tool.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    host_tool.chmod(0o755)
    monkeypatch.setenv("PATH", f"{host_bin}:{os.environ['PATH']}")
    trace = WorkloadTrace(
        "stable-shell-tools",
        (
            WorkloadEvent(
                0,
                "shell",
                "exec_command",
                {"cmd": "added-after-capture", "workdir": "{{repo}}"},
                expected={"status": "error", "exit_code": 127},
            ),
        ),
    )

    report = WorkloadReplayer(
        _Service(tmp_path / "checkouts"),  # type: ignore[arg-type]
        repo_dir=tmp_path,
        allow_shell=True,
    ).replay(trace)

    assert report.succeeded
    assert report.events[0].shell_exit_code == 127


def test_replayer_uses_path_extensions_recorded_in_trace_metadata(
    tmp_path: Path,
) -> None:
    recorded_bin = tmp_path / "recorded-bin"
    recorded_bin.mkdir()
    recorded_tool = recorded_bin / "available-during-capture"
    recorded_tool.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    recorded_tool.chmod(0o755)
    trace = WorkloadTrace(
        "recorded-shell-tools",
        (
            WorkloadEvent(
                0,
                "shell",
                "exec_command",
                {
                    "cmd": "available-during-capture",
                    "workdir": "{{repo}}",
                },
                expected={"status": "ok", "exit_code": 0},
            ),
        ),
        {"replay_path_extensions": [str(recorded_bin)]},
    )

    report = WorkloadReplayer(
        _Service(tmp_path / "checkouts"),  # type: ignore[arg-type]
        repo_dir=tmp_path,
        allow_shell=True,
    ).replay(trace)

    assert report.succeeded
    assert report.events[0].shell_exit_code == 0


def test_replayer_accepts_terminal_status_for_unfinished_shell_capture(
    tmp_path: Path,
) -> None:
    trace = WorkloadTrace(
        "unfinished-shell-capture",
        (
            WorkloadEvent(
                0,
                "shell",
                "exec_command",
                {"cmd": "exit 0", "workdir": "{{repo}}"},
                expected={"status": "error", "exit_code": None},
            ),
        ),
    )

    report = WorkloadReplayer(
        _Service(tmp_path / "checkouts"),  # type: ignore[arg-type]
        repo_dir=tmp_path,
        allow_shell=True,
    ).replay(trace)

    assert report.succeeded
    assert report.events[0].status == "ok"
    assert report.events[0].expected_status == "any"


def test_replayer_continues_after_unexpected_exception_with_any_status(
    tmp_path: Path,
) -> None:
    trace = WorkloadTrace(
        "continue-after-any-error",
        (
            WorkloadEvent(
                0,
                "mcp",
                "knowledge_not_a_tool",
                {},
                expected={"status": "any"},
            ),
            WorkloadEvent(
                1,
                "shell",
                "exec_command",
                {"cmd": "exit 0", "workdir": "{{repo}}"},
                expected={"status": "ok", "exit_code": 0},
            ),
        ),
    )

    report = WorkloadReplayer(
        _Service(tmp_path / "checkouts"),  # type: ignore[arg-type]
        repo_dir=tmp_path,
        allow_shell=True,
    ).replay(trace)

    assert len(report.events) == 2
    assert report.events[0].status == "error"
    assert report.events[0].expected_status == "any"
    assert report.events[1].status == "ok"


def test_replayer_remaps_branches_but_preserves_trace_workspace_tokens(
    tmp_path: Path,
) -> None:
    trace = WorkloadTrace(
        "mapped-filesystem-write",
        (
            WorkloadEvent(
                0,
                "mcp",
                "knowledge_checkout",
                {
                    "branch_id": "task/a",
                    "from_branch": "main",
                    "mount": True,
                },
            ),
            WorkloadEvent(
                1,
                "shell",
                "exec_command",
                {
                    "cmd": (
                        "printf 'isolated\\n' > "
                        "'{{workspace:task/a}}/artifact.txt'"
                    ),
                    "workdir": "{{repo}}",
                },
            ),
        ),
    )
    service = _Service(tmp_path / "checkouts")

    report = WorkloadReplayer(
        service,  # type: ignore[arg-type]
        repo_dir=tmp_path,
        allow_shell=True,
        branch_map={
            "main": "benchmark/repeat-0/root",
            "task/a": "benchmark/repeat-0/task-a",
        },
    ).replay(trace)

    assert report.succeeded
    assert report.events[0].result_summary["branch_id"] == "task/a"
    assert (
        tmp_path
        / "checkouts/benchmark-repeat-0-task-a/artifact.txt"
    ).read_text(encoding="utf-8") == "isolated\n"


def test_analyzer_and_replayer_preserve_apply_patch_filesystem_write(
    tmp_path: Path,
) -> None:
    source_cwd = tmp_path / "source"
    checkout = source_cwd / "state/checkouts/task-a"
    checkout.mkdir(parents=True)
    patch = "\n".join(
        [
            "*** Begin Patch",
            f"*** Add File: {checkout}/diagnosis.md",
            "+validated diagnosis",
            "*** End Patch",
            "",
        ]
    )
    rows = [
        {
            "timestamp": "2026-01-01T00:00:00Z",
            "type": "session_meta",
            "payload": {"session_id": "session-patch", "cwd": str(source_cwd)},
        },
        {
            "timestamp": "2026-01-01T00:00:01Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "knowledge_checkout",
                "call_id": "checkout",
                "arguments": json.dumps(
                    {
                        "branch_id": "task/a",
                        "from_branch": "main",
                        "mount": True,
                    }
                ),
            },
        },
        {
            "timestamp": "2026-01-01T00:00:02Z",
            "type": "event_msg",
            "payload": {
                "type": "mcp_tool_call_end",
                "call_id": "checkout",
                "invocation": {
                    "server": "chronos_enterprise_knowledge",
                    "tool": "knowledge_checkout",
                    "arguments": {
                        "branch_id": "task/a",
                        "from_branch": "main",
                        "mount": True,
                    },
                },
                "result": {
                    "Ok": {
                        "isError": False,
                        "structuredContent": {
                            "branch_id": "task/a",
                            "created": True,
                            "from_branch": "main",
                            "workspace_path": str(checkout),
                        },
                        "content": [],
                    }
                },
            },
        },
        {
            "timestamp": "2026-01-01T00:00:03Z",
            "type": "response_item",
            "payload": {
                "type": "custom_tool_call",
                "name": "apply_patch",
                "call_id": "patch",
                "input": patch,
            },
        },
        {
            "timestamp": "2026-01-01T00:00:04Z",
            "type": "response_item",
            "payload": {
                "type": "custom_tool_call_output",
                "call_id": "patch",
                "output": (
                    "Exit code: 0\nOutput:\n"
                    f"Success. Updated the following files:\nA {checkout}/diagnosis.md\n"
                ),
            },
        },
    ]
    rollout = tmp_path / "rollout.jsonl"
    rollout.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )
    analysis = RolloutAnalyzer().analyze(rollout)

    assert analysis.as_dict()["fully_replayable"]
    assert [event.kind for event in analysis.trace.events] == ["mcp", "patch"]
    assert "{{workspace:task/a}}/diagnosis.md" in (
        analysis.trace.events[1].arguments["patch"]
    )

    replay_root = tmp_path / "replay"
    replay_root.mkdir()
    report = WorkloadReplayer(
        _Service(replay_root / "checkouts"),  # type: ignore[arg-type]
        repo_dir=replay_root,
        allow_shell=True,
    ).replay(analysis.trace)

    assert report.succeeded
    assert (
        replay_root / "checkouts/task-a/diagnosis.md"
    ).read_text(encoding="utf-8") == "validated diagnosis\n"


def test_recorded_patch_replayer_applies_multifile_multihunk_patch(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.txt"
    added = tmp_path / "nested/added.txt"
    source.write_text("alpha\nkeep\nold\ntail\n", encoding="utf-8")

    output = _apply_recorded_patch(
        "\n".join(
            [
                "*** Begin Patch",
                f"*** Update File: {source}",
                "@@",
                " alpha",
                "+inserted",
                " keep",
                "@@",
                "-old",
                "+new",
                " tail",
                f"*** Add File: {added}",
                "+created",
                "*** End Patch",
                "",
            ]
        )
    )

    assert source.read_text(encoding="utf-8") == (
        "alpha\ninserted\nkeep\nnew\ntail\n"
    )
    assert added.read_text(encoding="utf-8") == "created\n"
    assert f"M {source}" in output
    assert f"A {added}" in output


def test_real_codex_workload_traces_are_well_formed() -> None:
    trace_root = (
        Path(__file__).parents[1]
        / "workflows"
        / "real"
        / "traces"
    )
    traces = [
        WorkloadTrace.load(path)
        for path in sorted(trace_root.glob("[0-9]*.jsonl"))
    ]

    assert {trace.trace_id for trace in traces} == {
        "create-department-and-team",
        "add-team-member",
        "debug-streaming-handshake",
        "debug-predictive-headroom",
        "debug-throttling-race",
        "enterprise-rag-qa-10",
    }
    assert all(trace.metadata.get("source_rollout") for trace in traces)
    assert all(trace.metadata.get("fully_replayable") for trace in traces)
    assert all(trace.events for trace in traces)
    assert any(
        event.kind == "shell"
        for trace in traces
        for event in trace.events
    )
    assert any(
        event.kind == "patch"
        for trace in traces
        for event in trace.events
    )
    memory_writes = [
        event
        for trace in traces
        for event in trace.events
        if event.kind == "mcp" and event.name == "knowledge_remember"
    ]
    assert len(memory_writes) == 6
