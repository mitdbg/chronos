"""Analyze Codex rollout logs and replay their high-level tool calls.

The replay boundary is intentionally the action selected by Codex: one MCP
tool call or one shell command. Storage-engine syscalls and implementation
details remain opaque so the same logical workload can exercise different
backends.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import subprocess
import tempfile
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from chronos_enterprise_knowledge.models import canonical_json
from chronos_enterprise_knowledge.service import KnowledgeService

_SCHEMA_VERSION = 1
_WORKSPACE_TOKEN = re.compile(r"\{\{workspace:([^}]+)\}\}")
_EXIT_CODE = re.compile(r"Process exited with code (\d+)")
_ORIGINAL_TOKENS = re.compile(r"Original token count: (\d+)")
_RUNNING_SESSION = re.compile(
    r"(?:Process|Script) running with (?:cell ID|session ID) ([^\s]+)"
)
_TRUNCATION_MARKERS = (
    "Warning: truncated output",
    "bytes omitted",
    "tokens truncated",
    "Output exceeded available model context",
)
_ORCHESTRATION_CALLS = {"exec", "wait", "update_plan"}
_SHELL_DIAGNOSTIC_LIMIT = 4_000

EventKind = Literal["mcp", "shell", "patch"]


class RolloutTraceError(RuntimeError):
    """Raised for malformed or unsupported rollout traces."""


def _replay_tool_path(
    recorded_extensions: Sequence[str] = (),
) -> str:
    # Keep replay independent of tools installed after the rollout was
    # captured.  In particular, adding ~/.local/bin made a recorded
    # "uv: command not found" action install and build an entire environment
    # during replay.  Traces that intentionally use a non-system tool record
    # its absolute path (for example, /home/ubuntu/.local/bin/uv).
    directories = [
        Path(value).expanduser()
        for value in recorded_extensions
    ] + [
        Path("/usr/local/sbin"),
        Path("/usr/local/bin"),
        Path("/usr/sbin"),
        Path("/usr/bin"),
        Path("/sbin"),
        Path("/bin"),
    ]
    extension_root = Path.home() / ".vscode-server/extensions"
    for candidate in sorted(
        extension_root.glob("openai.chatgpt-*/bin/linux-x86_64"),
        reverse=True,
    ):
        if (candidate / "rg").is_file():
            directories.insert(0, candidate)
            break
    unique: list[str] = []
    for directory in directories:
        value = str(directory)
        if value not in unique:
            unique.append(value)
    return os.pathsep.join(unique)


@dataclass(frozen=True)
class _FilePatch:
    operation: Literal["add", "update", "delete"]
    path: Path
    body: tuple[str, ...]


def _parse_file_patches(patch: str) -> list[_FilePatch]:
    lines = patch.splitlines()
    if not lines or lines[0] != "*** Begin Patch":
        raise RolloutTraceError("patch must begin with '*** Begin Patch'")
    if lines[-1] != "*** End Patch":
        raise RolloutTraceError("patch must end with '*** End Patch'")

    actions: list[_FilePatch] = []
    index = 1
    headers = {
        "*** Add File: ": "add",
        "*** Update File: ": "update",
        "*** Delete File: ": "delete",
    }
    while index < len(lines) - 1:
        header = lines[index]
        operation = next(
            (
                value
                for prefix, value in headers.items()
                if header.startswith(prefix)
            ),
            None,
        )
        if operation is None:
            raise RolloutTraceError(
                f"expected a file patch header, found: {header!r}"
            )
        prefix = next(
            candidate
            for candidate, value in headers.items()
            if value == operation and header.startswith(candidate)
        )
        path_text = header[len(prefix) :]
        if not path_text:
            raise RolloutTraceError("file patch path cannot be empty")
        index += 1
        body_start = index
        while index < len(lines) - 1 and not any(
            lines[index].startswith(candidate) for candidate in headers
        ):
            index += 1
        actions.append(
            _FilePatch(
                operation=operation,  # type: ignore[arg-type]
                path=Path(path_text),
                body=tuple(lines[body_start:index]),
            )
        )
    return actions


def _find_patch_context(
    lines: Sequence[str],
    pattern: Sequence[str],
    *,
    start: int,
) -> int:
    if not pattern:
        return start
    matchers = (
        lambda value: value,
        lambda value: value.rstrip(),
        lambda value: value.strip(),
    )
    candidates = list(range(start, len(lines) - len(pattern) + 1))
    if start:
        candidates.extend(range(0, min(start, len(lines) - len(pattern) + 1)))
    for normalize in matchers:
        expected = [normalize(value) for value in pattern]
        for position in candidates:
            if [
                normalize(value)
                for value in lines[position : position + len(pattern)]
            ] == expected:
                return position
    excerpt = "\n".join(pattern[:8])
    raise RolloutTraceError(
        "patch context was not found"
        + (f":\n{excerpt}" if excerpt else "")
    )


def _apply_update_hunks(source: str, body: Sequence[str]) -> str:
    lines = source.splitlines()
    trailing_newline = source.endswith("\n")
    cursor = 0
    index = 0
    saw_hunk = False
    while index < len(body):
        if body[index] == "*** End of File":
            index += 1
            continue
        if not body[index].startswith("@@"):
            raise RolloutTraceError(
                f"expected a patch hunk, found: {body[index]!r}"
            )
        saw_hunk = True
        index += 1
        hunk: list[str] = []
        while index < len(body) and not body[index].startswith("@@"):
            if body[index] == "*** End of File":
                index += 1
                break
            hunk.append(body[index])
            index += 1
        old: list[str] = []
        new: list[str] = []
        for line in hunk:
            if not line:
                raise RolloutTraceError(
                    "patch hunk lines must have a context or change prefix"
                )
            prefix, value = line[0], line[1:]
            if prefix == " ":
                old.append(value)
                new.append(value)
            elif prefix == "-":
                old.append(value)
            elif prefix == "+":
                new.append(value)
            else:
                raise RolloutTraceError(
                    f"unsupported patch hunk line: {line!r}"
                )
        position = _find_patch_context(lines, old, start=cursor)
        lines[position : position + len(old)] = new
        cursor = position + len(new)
    if not saw_hunk:
        raise RolloutTraceError("update patch contains no hunks")
    result = "\n".join(lines)
    if trailing_newline:
        result += "\n"
    return result


def _apply_recorded_patch(patch: str) -> str:
    staged: dict[Path, str | None] = {}
    changed: list[tuple[str, Path]] = []
    for action in _parse_file_patches(patch):
        current = (
            staged[action.path]
            if action.path in staged
            else (
                action.path.read_text(encoding="utf-8")
                if action.path.is_file()
                else None
            )
        )
        if action.operation == "add":
            if current is not None:
                raise RolloutTraceError(
                    f"cannot add an existing file: {action.path}"
                )
            content: list[str] = []
            for line in action.body:
                if not line.startswith("+"):
                    raise RolloutTraceError(
                        f"add-file lines must begin with '+': {line!r}"
                    )
                content.append(line[1:])
            staged[action.path] = "\n".join(content) + ("\n" if content else "")
            changed.append(("A", action.path))
        elif action.operation == "update":
            if current is None:
                raise RolloutTraceError(
                    f"cannot update a missing file: {action.path}"
                )
            staged[action.path] = _apply_update_hunks(current, action.body)
            changed.append(("M", action.path))
        else:
            if current is None:
                raise RolloutTraceError(
                    f"cannot delete a missing file: {action.path}"
                )
            if action.body:
                raise RolloutTraceError(
                    f"delete-file patch has unexpected content: {action.path}"
                )
            staged[action.path] = None
            changed.append(("D", action.path))

    for path, content in staged.items():
        if content is None:
            path.unlink()
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            # A recorded patch is complete only when later shell tools can
            # observe the entire new file.  This matters for mounted
            # filesystems whose writes are buffered until FLUSH/FSYNC.
            with path.open("w", encoding="utf-8") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
    details = "\n".join(f"{operation} {path}" for operation, path in changed)
    return (
        "Success. Updated the following files:\n"
        + details
        + ("\n" if details else "")
    )


@dataclass(frozen=True)
class WorkloadEvent:
    sequence: int
    kind: EventKind
    name: str
    arguments: dict[str, Any]
    call_id: str = ""
    timestamp: str = ""
    server: str | None = None
    expected: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "schema_version": _SCHEMA_VERSION,
            "record_type": "event",
            "sequence": self.sequence,
            "kind": self.kind,
            "name": self.name,
            "arguments": self.arguments,
        }
        if self.call_id:
            value["call_id"] = self.call_id
        if self.timestamp:
            value["timestamp"] = self.timestamp
        if self.server is not None:
            value["server"] = self.server
        if self.expected:
            value["expected"] = self.expected
        return value

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> WorkloadEvent:
        if value.get("record_type") != "event":
            raise RolloutTraceError("workload event has an invalid record_type")
        kind = str(value["kind"])
        if kind not in {"mcp", "shell", "patch"}:
            raise RolloutTraceError(f"unsupported workload event kind: {kind}")
        return cls(
            sequence=int(value["sequence"]),
            kind=kind,  # type: ignore[arg-type]
            name=str(value["name"]),
            arguments=dict(value.get("arguments") or {}),
            call_id=str(value.get("call_id") or ""),
            timestamp=str(value.get("timestamp") or ""),
            server=(
                str(value["server"]) if value.get("server") is not None else None
            ),
            expected=dict(value.get("expected") or {}),
        )


@dataclass(frozen=True)
class WorkloadTrace:
    trace_id: str
    events: tuple[WorkloadEvent, ...]
    metadata: dict[str, Any] = field(default_factory=dict)

    def write(self, path: str | Path) -> Path:
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        header = {
            "schema_version": _SCHEMA_VERSION,
            "record_type": "trace",
            "trace_id": self.trace_id,
            **self.metadata,
        }
        with destination.open("w", encoding="utf-8") as handle:
            handle.write(canonical_json(header))
            handle.write("\n")
            for event in self.events:
                handle.write(canonical_json(event.as_dict()))
                handle.write("\n")
        return destination

    @classmethod
    def load(cls, path: str | Path) -> WorkloadTrace:
        source = Path(path)
        records = [
            json.loads(line)
            for line in source.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        if not records:
            raise RolloutTraceError(f"empty workload trace: {source}")
        header = records[0]
        if (
            header.get("schema_version") != _SCHEMA_VERSION
            or header.get("record_type") != "trace"
        ):
            raise RolloutTraceError(f"unsupported workload trace schema: {source}")
        events = tuple(WorkloadEvent.from_dict(record) for record in records[1:])
        sequences = [event.sequence for event in events]
        if sequences != list(range(len(events))):
            raise RolloutTraceError(
                "workload event sequences must be contiguous and start at zero"
            )
        metadata = {
            str(key): value
            for key, value in header.items()
            if key not in {"schema_version", "record_type", "trace_id"}
        }
        return cls(str(header["trace_id"]), events, metadata)

    def summary(self) -> dict[str, Any]:
        names = Counter(f"{event.kind}:{event.name}" for event in self.events)
        return {
            "trace_id": self.trace_id,
            "events": len(self.events),
            "event_counts": dict(sorted(names.items())),
            "shell_events": sum(event.kind == "shell" for event in self.events),
            "patch_events": sum(event.kind == "patch" for event in self.events),
            "mcp_events": sum(event.kind == "mcp" for event in self.events),
            "truncated_shell_results": sum(
                bool(event.expected.get("truncated"))
                for event in self.events
                if event.kind == "shell"
            ),
        }


@dataclass(frozen=True)
class RolloutAnalysis:
    trace: WorkloadTrace
    source_records: int
    skipped_calls: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            **self.trace.summary(),
            "source_records": self.source_records,
            "fully_replayable": not self.skipped_calls,
            "skipped_calls": list(self.skipped_calls),
        }


class RolloutAnalyzer:
    """Extract replayable MCP and shell calls from a Codex rollout JSONL."""

    def analyze(self, path: str | Path, *, trace_id: str | None = None) -> RolloutAnalysis:
        source = Path(path).expanduser().resolve()
        records = [
            json.loads(line)
            for line in source.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        session = next(
            (row.get("payload", {}) for row in records if row.get("type") == "session_meta"),
            {},
        )
        source_cwd = str(session.get("cwd") or "")
        derived_trace_id = trace_id or str(
            session.get("session_id") or session.get("id") or source.stem
        )

        outputs = {
            str(row.get("payload", {}).get("call_id")): row.get("payload", {})
            for row in records
            if row.get("type") == "response_item"
            and row.get("payload", {}).get("type")
            in {"function_call_output", "custom_tool_call_output"}
        }
        mcp_call_ids = {
            str(row.get("payload", {}).get("call_id"))
            for row in records
            if row.get("type") == "event_msg"
            and row.get("payload", {}).get("type") == "mcp_tool_call_end"
        }

        workspace_paths: dict[str, str] = {}
        events: list[WorkloadEvent] = []
        skipped: list[str] = []
        running_shell_events: dict[str, int] = {}
        for row in records:
            payload = row.get("payload", {})
            if (
                row.get("type") == "event_msg"
                and payload.get("type") == "mcp_tool_call_end"
            ):
                call_id = str(payload.get("call_id") or "")
                invocation = dict(payload.get("invocation") or {})
                tool = str(invocation.get("tool") or "")
                server = str(invocation.get("server") or "")
                mcp_arguments = dict(invocation.get("arguments") or {})
                result = _mcp_structured_result(payload.get("result"))
                if tool == "knowledge_checkout" and isinstance(result, Mapping):
                    branch_id = str(result.get("branch_id") or "")
                    workspace_path = result.get("workspace_path")
                    if branch_id and workspace_path:
                        absolute_workspace = str(workspace_path)
                        workspace_paths[absolute_workspace] = branch_id
                        if source_cwd:
                            try:
                                relative_workspace = Path(
                                    absolute_workspace
                                ).relative_to(source_cwd)
                            except ValueError:
                                pass
                            else:
                                relative_text = (
                                    relative_workspace.as_posix()
                                )
                                if relative_text not in {"", "."}:
                                    workspace_paths[
                                        relative_text
                                    ] = branch_id
                normalized = normalize_mcp_result(
                    tool,
                    result,
                    workspace_paths=workspace_paths,
                )
                expected: dict[str, Any] = {
                    "status": (
                        "error"
                        if _mcp_result_is_error(payload.get("result"))
                        else "ok"
                    ),
                    "normalized_digest": _digest(normalized),
                }
                result_summary = _result_summary(tool, normalized)
                if result_summary is not None:
                    expected["result_summary"] = result_summary
                portable_arguments = _replace_paths(
                    mcp_arguments,
                    workspace_paths,
                    source_cwd,
                )
                if tool == "knowledge_checkout":
                    # A capture-specific mount point is neither logical input
                    # nor portable. Replay lets the target backend choose its
                    # own checkout path and records that result for later
                    # workspace placeholders.
                    portable_arguments.pop("mount_path", None)
                events.append(
                    WorkloadEvent(
                        sequence=len(events),
                        kind="mcp",
                        name=tool,
                        server=server,
                        call_id=call_id,
                        timestamp=str(row.get("timestamp") or ""),
                        arguments=portable_arguments,
                        expected=expected,
                    )
                )
                continue
            if row.get("type") != "response_item":
                continue
            payload_type = payload.get("type")
            if payload_type == "custom_tool_call":
                name = str(payload.get("name") or "")
                if name in _ORCHESTRATION_CALLS:
                    continue
                if name != "apply_patch":
                    skipped.append(name or "<unnamed-custom-tool>")
                    continue
                call_id = str(payload.get("call_id") or "")
                patch = str(payload.get("input") or "")
                output = str(outputs.get(call_id, {}).get("output") or "")
                exit_code = _custom_tool_exit_code(output)
                events.append(
                    WorkloadEvent(
                        sequence=len(events),
                        kind="patch",
                        name=name,
                        call_id=call_id,
                        timestamp=str(row.get("timestamp") or ""),
                        arguments={
                            "patch": _replace_paths(
                                patch,
                                workspace_paths,
                                source_cwd,
                            )
                        },
                        expected={
                            "status": "ok" if exit_code == 0 else "error",
                            "exit_code": exit_code,
                        },
                    )
                )
                continue
            if payload_type != "function_call":
                continue
            call_id = str(payload.get("call_id") or "")
            name = str(payload.get("name") or "")
            arguments = _decode_arguments(payload.get("arguments"))
            if call_id in mcp_call_ids:
                continue
            if name == "exec_command":
                shell_arguments = _replace_paths(
                    arguments,
                    workspace_paths,
                    source_cwd,
                )
                expected = _shell_expected(
                    outputs.get(call_id, {}).get("output"),
                    workspace_paths,
                    source_cwd,
                )
                events.append(
                    WorkloadEvent(
                        sequence=len(events),
                        kind="shell",
                        name=name,
                        call_id=call_id,
                        timestamp=str(row.get("timestamp") or ""),
                        arguments=shell_arguments,
                        expected=expected,
                    )
                )
                running = _RUNNING_SESSION.search(
                    str(outputs.get(call_id, {}).get("output") or "")
                )
                if running:
                    running_shell_events[running.group(1)] = len(events) - 1
                continue
            if name == "write_stdin":
                chars = str(arguments.get("chars") or "")
                if not chars:
                    # Empty writes only poll a command already represented by
                    # its exec_command event. Replay runs that command
                    # synchronously.
                    continue
                session_id = str(arguments.get("session_id") or "")
                event_index = running_shell_events.get(session_id)
                if chars == "\x03" and event_index is not None:
                    # A Ctrl-C is not a second workload operation: it
                    # terminates the asynchronous exec_command that Codex
                    # already selected. Preserve that behavior as a timed
                    # interrupt on the originating shell event. Output emitted
                    # before the interrupt is scheduling-dependent, so replay
                    # validates the operation but not a byte-for-byte prefix.
                    event = events[event_index]
                    shell_arguments = dict(event.arguments)
                    shell_arguments["interrupt_after_seconds"] = max(
                        0.01,
                        _elapsed_seconds(
                            event.timestamp,
                            str(row.get("timestamp") or ""),
                        ),
                    )
                    interrupted_expected = _shell_expected(
                        outputs.get(call_id, {}).get("output"),
                        workspace_paths,
                        source_cwd,
                    )
                    interrupted_expected.pop("normalized_digest", None)
                    interrupted_expected["interrupted"] = True
                    events[event_index] = replace(
                        event,
                        arguments=shell_arguments,
                        expected=interrupted_expected,
                    )
                    running_shell_events.pop(session_id, None)
                    continue
            if name in _ORCHESTRATION_CALLS:
                continue
            skipped.append(name or "<unnamed>")

        trace = WorkloadTrace(
            derived_trace_id,
            tuple(events),
            {
                "source_rollout": str(source),
                "source_cwd": source_cwd,
                "description": "Normalized from a Codex rollout session.",
                "fully_replayable": not skipped,
                "skipped_call_counts": dict(sorted(Counter(skipped).items())),
            },
        )
        return RolloutAnalysis(trace, len(records), tuple(skipped))


def combine_workload_traces(
    traces: Sequence[WorkloadTrace],
    *,
    trace_id: str,
) -> WorkloadTrace:
    """Merge real session traces by recorded time into one dependent workload."""

    if not traces:
        raise RolloutTraceError("at least one workload trace is required")
    ordered: list[tuple[str, int, int, WorkloadEvent]] = []
    for trace_index, trace in enumerate(traces):
        for event in trace.events:
            ordered.append(
                (
                    event.timestamp,
                    trace_index,
                    event.sequence,
                    event,
                )
            )
    ordered.sort(key=lambda item: (item[0], item[1], item[2]))

    combined_events: list[WorkloadEvent] = []
    for sequence, (_, trace_index, _, event) in enumerate(ordered):
        expected = dict(event.expected)
        expected["source_trace_id"] = traces[trace_index].trace_id
        combined_events.append(
            replace(event, sequence=sequence, expected=expected)
        )

    required_documents: set[str] = set()
    verify_branches: set[str] = set()
    for event in combined_events:
        if event.kind != "mcp":
            continue
        if event.name == "knowledge_get_document":
            identifier = event.arguments.get("document_id")
            if identifier and _is_snapshot_document_id(str(identifier)):
                required_documents.add(str(identifier))
        if event.name == "knowledge_remember":
            for evidence in event.arguments.get("evidence") or ():
                identifier = str(evidence)
                if _is_snapshot_document_id(identifier):
                    required_documents.add(identifier)
        if event.name == "knowledge_checkout":
            branch_id = event.arguments.get("branch_id")
            if branch_id:
                verify_branches.add(str(branch_id))

    skipped_counts: Counter[str] = Counter()
    fully_replayable = True
    source_rollouts: list[str] = []
    for trace in traces:
        fully_replayable = (
            fully_replayable
            and trace.metadata.get("fully_replayable") is not False
        )
        skipped_counts.update(trace.metadata.get("skipped_call_counts") or {})
        source = trace.metadata.get("source_rollout")
        if source:
            source_rollouts.append(str(source))

    return WorkloadTrace(
        trace_id,
        tuple(combined_events),
        {
            "description": (
                "Combined from real Codex sessions and ordered by recorded "
                "tool-completion time."
            ),
            "component_traces": [trace.trace_id for trace in traces],
            "source_rollouts": source_rollouts,
            "fully_replayable": fully_replayable,
            "skipped_call_counts": dict(sorted(skipped_counts.items())),
            "seed": {"document_ids": sorted(required_documents)},
            "baseline_branches": ["main"],
            "verify_branches": sorted(verify_branches),
        },
    )


def resolve_memory_timestamps(
    trace: WorkloadTrace,
    backend: Any,
) -> tuple[WorkloadTrace, int]:
    """Recover generated memory timestamps from the captured branch state."""

    resolved = 0
    resolved_from_event = 0
    events: list[WorkloadEvent] = []
    for event in trace.events:
        if (
            event.kind != "mcp"
            or event.name != "knowledge_remember"
            or event.arguments.get("recorded_at")
        ):
            events.append(event)
            continue
        summary = event.expected.get("result_summary")
        path = summary.get("path") if isinstance(summary, Mapping) else None
        memory_id = (
            summary.get("memory_id")
            if isinstance(summary, Mapping)
            else None
        )
        branch_id = event.arguments.get("branch_id")
        if not path or not branch_id:
            raise RolloutTraceError(
                f"memory event {event.sequence} lacks its captured path"
            )
        try:
            content = _read_captured_memory(
                backend,
                str(branch_id),
                str(path),
                str(memory_id) if memory_id else None,
            ).decode("utf-8")
        except Exception as exc:
            if "branch not found" not in str(exc).casefold():
                raise
            if not event.timestamp:
                raise RolloutTraceError(
                    "memory was deleted with its branch and its rollout event "
                    f"has no timestamp: {branch_id}:{path}"
                ) from exc
            recorded_at = event.timestamp
            resolved_from_event += 1
        else:
            match = re.search(
                r"^Recorded:\s*(.+)$",
                content,
                flags=re.MULTILINE,
            )
            if match is None:
                raise RolloutTraceError(
                    f"captured memory has no Recorded field: {branch_id}:{path}"
                )
            recorded_at = match.group(1).strip()
        arguments = dict(event.arguments)
        arguments["recorded_at"] = recorded_at
        events.append(replace(event, arguments=arguments))
        resolved += 1
    metadata = dict(trace.metadata)
    metadata["memory_timestamps_resolved"] = resolved
    metadata["memory_timestamps_from_event"] = resolved_from_event
    return WorkloadTrace(trace.trace_id, tuple(events), metadata), resolved


def _read_captured_memory(
    backend: Any,
    branch_id: str,
    path: str,
    memory_id: str | None,
    *,
    timeout_seconds: float = 2.0,
) -> bytes:
    """Read a memory across the MCP/ChronosFS daemon handoff."""

    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            return backend.read_file(branch_id, path)
        except Exception as exc:
            missing = isinstance(exc, FileNotFoundError) or any(
                marker in str(exc).casefold()
                for marker in ("path not found", "no such file")
            )
            if not missing:
                raise
            if time.monotonic() >= deadline:
                if memory_id is None:
                    raise
                indexed = backend.get_document(branch_id, memory_id)
                if indexed is None:
                    raise
                return indexed.document.content.encode("utf-8")
            time.sleep(0.25)


def _expected_replay_status(event: WorkloadEvent) -> str:
    """Return the recorded status, or ``any`` for unfinished shell capture."""

    expected = event.expected
    if event.kind == "shell":
        command = str(event.arguments.get("cmd") or "").lstrip()
        ephemeral_cleanup = (
            command.startswith(("rm ", "mv ", "find "))
            and any(
                marker in command
                for marker in (
                    "__pycache__",
                    ".pytest_cache",
                    ".ruff_cache",
                    "-incomplete-venv",
                    "-pytest-cache",
                    "-ruff-cache",
                    "-pycache",
                )
            )
        )
        if (
            expected.get("interrupted")
            or command.startswith("ps ")
            or ephemeral_cleanup
        ):
            # The command remains part of the replay, but its terminal status
            # depends on capture-local timing, process identifiers, or whether
            # an ephemeral cache entry still exists.
            return "any"
    if (
        event.kind == "shell"
        and expected.get("status") == "error"
        and "exit_code" in expected
        and expected.get("exit_code") is None
        and not expected.get("interrupted")
    ):
        # Codex can yield while a shell command is still running. The
        # corresponding capture record has no terminal exit status, so replay
        # must execute the command but cannot compare its eventual status.
        return "any"
    return str(expected.get("status") or "ok")


def _shell_failure_diagnostic(output: str) -> str:
    """Retain a bounded output tail when a replayed shell status diverges."""

    if len(output) > _SHELL_DIAGNOSTIC_LIMIT:
        output = (
            f"... {len(output) - _SHELL_DIAGNOSTIC_LIMIT} characters omitted ...\n"
            + output[-_SHELL_DIAGNOSTIC_LIMIT:]
        )
    return output.rstrip() or "<shell command produced no output>"


@dataclass(frozen=True)
class EventReplayResult:
    sequence: int
    kind: EventKind
    name: str
    elapsed_ns: int
    status: str
    expected_status: str
    normalized_digest: str | None
    expected_digest: str | None
    matched: bool | None
    result_summary: Any = None
    error: str | None = None
    shell_exit_code: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "kind": self.kind,
            "name": self.name,
            "elapsed_ms": self.elapsed_ns / 1_000_000,
            "status": self.status,
            "expected_status": self.expected_status,
            "normalized_digest": self.normalized_digest,
            "expected_digest": self.expected_digest,
            "matched": self.matched,
            "result_summary": self.result_summary,
            "error": self.error,
            "shell_exit_code": self.shell_exit_code,
        }


@dataclass(frozen=True)
class WorkloadReplayReport:
    trace_id: str
    backend: str
    events: tuple[EventReplayResult, ...]
    wall_time_ns: int
    workspace_paths: dict[str, str]

    @property
    def succeeded(self) -> bool:
        return all(
            event.expected_status == "any"
            or event.status == event.expected_status
            # A shell command that failed in the capture environment may
            # succeed when replayed with more complete tools or repository
            # metadata. Keep the digest mismatch visible, but do not fail the
            # storage benchmark because the command improved.
            or (event.kind == "shell" and event.status == "ok")
            for event in self.events
        )

    @property
    def matched(self) -> bool:
        return all(event.matched is not False for event in self.events)

    def latency_summary(self) -> dict[str, dict[str, float | int]]:
        grouped: dict[str, list[int]] = defaultdict(list)
        for event in self.events:
            grouped[f"{event.kind}:{event.name}"].append(event.elapsed_ns)
        result: dict[str, dict[str, float | int]] = {}
        for name, values in sorted(grouped.items()):
            ordered = sorted(values)
            result[name] = {
                "count": len(ordered),
                "p50_ms": _percentile(ordered, 0.50) / 1_000_000,
                "p95_ms": _percentile(ordered, 0.95) / 1_000_000,
                "max_ms": ordered[-1] / 1_000_000,
            }
        return result

    def as_dict(self, *, include_events: bool = True) -> dict[str, Any]:
        value: dict[str, Any] = {
            "trace_id": self.trace_id,
            "backend": self.backend,
            "succeeded": self.succeeded,
            "matched_recorded_results": self.matched,
            "events_replayed": len(self.events),
            "wall_time_ms": self.wall_time_ns / 1_000_000,
            "latency_by_operation": self.latency_summary(),
            "workspace_paths": dict(sorted(self.workspace_paths.items())),
        }
        if include_events:
            value["events"] = [event.as_dict() for event in self.events]
        return value


class WorkloadReplayer:
    """Replay normalized MCP calls and shell commands against one service."""

    def __init__(
        self,
        service: KnowledgeService,
        *,
        repo_dir: str | Path,
        allow_shell: bool = False,
        shell_timeout_seconds: float = 300,
        max_interrupt_seconds: float | None = None,
        continue_on_error: bool = False,
        branch_map: Mapping[str, str] | None = None,
    ):
        self.service = service
        self.repo_dir = Path(repo_dir).expanduser().resolve()
        self.allow_shell = allow_shell
        self.shell_timeout_seconds = float(shell_timeout_seconds)
        self.max_interrupt_seconds = (
            None
            if max_interrupt_seconds is None
            else float(max_interrupt_seconds)
        )
        if (
            self.max_interrupt_seconds is not None
            and self.max_interrupt_seconds <= 0
        ):
            raise ValueError("max_interrupt_seconds must be positive")
        self.continue_on_error = continue_on_error
        self.branch_map = {
            str(source): str(target)
            for source, target in (branch_map or {}).items()
        }
        if len(set(self.branch_map.values())) != len(self.branch_map):
            raise ValueError("branch_map targets must be unique")
        self._inverse_branch_map = {
            target: source for source, target in self.branch_map.items()
        }
        self.workspace_paths: dict[str, str] = {}
        self._trace_tmpdir: str | None = None
        self._recorded_path_extensions: tuple[str, ...] = ()

    def replay(self, trace: WorkloadTrace) -> WorkloadReplayReport:
        if trace.metadata.get("fully_replayable") is False:
            skipped = trace.metadata.get("skipped_call_counts") or {}
            raise RolloutTraceError(
                "trace contains unsupported Codex tool calls and cannot be "
                f"replayed completely: {skipped}"
            )
        prefix = re.sub(r"[^A-Za-z0-9_.-]+", "-", trace.trace_id)[:48]
        trace_tmpdir = tempfile.TemporaryDirectory(
            prefix=f"chronos-replay-{prefix}-"
        )
        self._trace_tmpdir = trace_tmpdir.name
        path_extensions = trace.metadata.get("replay_path_extensions") or ()
        if not isinstance(path_extensions, Sequence) or isinstance(
            path_extensions,
            (str, bytes),
        ):
            raise RolloutTraceError(
                "replay_path_extensions must be a list of directories"
            )
        self._recorded_path_extensions = tuple(
            str(value) for value in path_extensions
        )
        try:
            results: list[EventReplayResult] = []
            wall_started = time.perf_counter_ns()
            for event in trace.events:
                try:
                    result = self._replay_event(event)
                except Exception as exc:
                    expected_status = _expected_replay_status(event)
                    result = EventReplayResult(
                        sequence=event.sequence,
                        kind=event.kind,
                        name=event.name,
                        elapsed_ns=0,
                        status="error",
                        expected_status=expected_status,
                        normalized_digest=None,
                        expected_digest=(
                            str(event.expected["normalized_digest"])
                            if event.expected.get("normalized_digest")
                            else None
                        ),
                        matched=False,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                    results.append(result)
                    if (
                        not self.continue_on_error
                        and expected_status != "error"
                    ):
                        break
                else:
                    results.append(result)
            report = WorkloadReplayReport(
                trace.trace_id,
                self.service.backend.backend_name,
                tuple(results),
                time.perf_counter_ns() - wall_started,
                dict(self.workspace_paths),
            )
        finally:
            self._trace_tmpdir = None
            self._recorded_path_extensions = ()
            trace_tmpdir.cleanup()
        return report

    def _replay_event(self, event: WorkloadEvent) -> EventReplayResult:
        started = time.perf_counter_ns()
        if event.kind == "mcp":
            arguments = _remap_branch_arguments(
                self._expand(event.arguments),
                self.branch_map,
            )
            value = dispatch_knowledge_tool(self.service, event.name, arguments)
            if event.name == "knowledge_checkout" and isinstance(value, Mapping):
                original_branch_id = str(
                    event.arguments.get("branch_id") or ""
                )
                workspace_path = value.get("workspace_path")
                if original_branch_id and workspace_path:
                    self.workspace_paths[original_branch_id] = str(
                        workspace_path
                    )
            normalized = normalize_mcp_result(
                event.name,
                value,
                branch_paths=self.workspace_paths,
            )
            normalized = _restore_branch_ids(
                normalized,
                self._inverse_branch_map,
            )
            if (
                event.name == "knowledge_status"
                and isinstance(normalized, Mapping)
                and isinstance(normalized.get("branches"), list)
            ):
                # Isolated replays keep the immutable source branches beside
                # private mapped clones. After restoring logical names, hide
                # that implementation detail and report each branch once.
                normalized = dict(normalized)
                normalized["branches"] = sorted(
                    set(str(item) for item in normalized["branches"])
                )
            exit_code = None
        elif event.kind == "shell":
            if not self.allow_shell:
                raise RolloutTraceError(
                    "trace contains shell commands; pass --allow-shell only "
                    "for a trusted trace"
                )
            value, exit_code = self._run_shell(self._expand(event.arguments))
            normalized = normalize_shell_result(
                value,
                exit_code,
                branch_paths=self.workspace_paths,
            )
        else:
            if not self.allow_shell:
                raise RolloutTraceError(
                    "trace contains filesystem patches; pass --allow-shell "
                    "only for a trusted trace"
                )
            value, exit_code = self._run_patch(self._expand(event.arguments))
            normalized = {
                "exit_code": exit_code,
                "applied": exit_code == 0,
            }
        elapsed = time.perf_counter_ns() - started
        actual_digest = _digest(normalized)
        expected_digest = (
            str(event.expected["normalized_digest"])
            if event.expected.get("normalized_digest")
            else None
        )
        expected_status = _expected_replay_status(event)
        status = "ok" if exit_code in {None, 0} else "error"
        error = None
        if (
            event.kind == "shell"
            and expected_status != "any"
            and status != expected_status
        ):
            error = _shell_failure_diagnostic(str(value))
        return EventReplayResult(
            sequence=event.sequence,
            kind=event.kind,
            name=event.name,
            elapsed_ns=elapsed,
            status=status,
            expected_status=expected_status,
            normalized_digest=actual_digest,
            expected_digest=expected_digest,
            matched=(
                actual_digest == expected_digest
                if expected_digest is not None
                else None
            ),
            result_summary=_result_summary(event.name, normalized),
            error=error,
            shell_exit_code=exit_code,
        )

    def _run_shell(self, arguments: Mapping[str, Any]) -> tuple[str, int]:
        command = str(arguments["cmd"])
        workdir = Path(str(arguments.get("workdir") or self.repo_dir)).resolve()
        if not workdir.exists():
            raise RolloutTraceError(f"shell workdir does not exist: {workdir}")
        timeout = min(
            float(arguments.get("timeout_seconds") or self.shell_timeout_seconds),
            self.shell_timeout_seconds,
        )
        environment = dict(os.environ)
        environment.update(
            {
                "LC_ALL": "C",
                "LANG": "C",
                "PATH": _replay_tool_path(self._recorded_path_extensions),
                "TZ": "UTC",
                "PYTHONHASHSEED": "0",
            }
        )
        interrupt_after = arguments.get("interrupt_after_seconds")
        if interrupt_after is None:
            process = subprocess.Popen(
                ["/bin/bash", "-c", command],
                cwd=workdir,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                errors="replace",
                start_new_session=True,
            )
            try:
                stdout, stderr = process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                # Kill the whole tool process group. Killing only the shell can
                # leave package managers and compilers running against a branch
                # after replay has moved on to diff, merge, or deletion.
                os.killpg(process.pid, signal.SIGKILL)
                stdout, stderr = process.communicate()
                return stdout + stderr, 124
            return stdout + stderr, int(process.returncode)

        interrupt_after_seconds = float(interrupt_after)
        if self.max_interrupt_seconds is not None:
            interrupt_after_seconds = min(
                interrupt_after_seconds,
                self.max_interrupt_seconds,
            )
        process = subprocess.Popen(
            ["/bin/bash", "-c", command],
            cwd=workdir,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            errors="replace",
            start_new_session=True,
        )
        interrupted = False
        try:
            stdout, stderr = process.communicate(
                timeout=min(interrupt_after_seconds, timeout)
            )
        except subprocess.TimeoutExpired:
            interrupted = True
            os.killpg(process.pid, signal.SIGINT)
            try:
                stdout, stderr = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                stdout, stderr = process.communicate()
        return_code = int(process.returncode)
        if interrupted and return_code < 0:
            return_code = 128 + abs(return_code)
        return stdout + stderr, return_code

    def _run_patch(self, arguments: Mapping[str, Any]) -> tuple[str, int]:
        try:
            output = _apply_recorded_patch(str(arguments["patch"]))
        except (OSError, UnicodeError, RolloutTraceError) as error:
            return f"Error: {error}\n", 1
        return output, 0

    def _expand(self, value: Any) -> Any:
        if isinstance(value, str):
            value = value.replace("{{repo}}", str(self.repo_dir))
            if "{{tmp}}" in value:
                if self._trace_tmpdir is None:
                    raise RolloutTraceError(
                        "temporary path placeholder used outside trace replay"
                    )
                value = value.replace("{{tmp}}", self._trace_tmpdir)

            def workspace(match: re.Match[str]) -> str:
                branch_id = match.group(1)
                try:
                    return self.workspace_paths[branch_id]
                except KeyError as exc:
                    raise RolloutTraceError(
                        f"workspace placeholder used before checkout: {branch_id}"
                    ) from exc

            return _WORKSPACE_TOKEN.sub(workspace, value)
        if isinstance(value, Mapping):
            return {str(key): self._expand(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._expand(item) for item in value]
        return value


def dispatch_knowledge_tool(
    service: KnowledgeService,
    name: str,
    arguments: Mapping[str, Any],
) -> Any:
    """Execute one MCP tool using the same application-service methods."""

    args = dict(arguments)
    if name == "knowledge_status":
        return service.status()
    if name == "knowledge_checkout":
        return service.checkout(
            str(args["branch_id"]),
            from_branch=(
                str(args["from_branch"]) if args.get("from_branch") is not None else None
            ),
            mount=bool(args.get("mount", True)),
            mount_path=args.get("mount_path"),
        )
    if name == "knowledge_search":
        return [
            hit.as_dict()
            for hit in service.search(
                str(args["branch_id"]),
                str(args["query"]),
                limit=int(args.get("limit", 8)),
            )
        ]
    if name == "knowledge_get_document":
        return service.get_document(
            str(args["branch_id"]),
            str(args["document_id"]),
        )
    if name == "knowledge_update_document":
        return service.update_document(
            str(args["branch_id"]),
            path=str(args["path"]),
            title=str(args["title"]),
            content=str(args["content"]),
            source=str(args["source"]),
            kind=str(args.get("kind", "curated")),  # type: ignore[arg-type]
            document_id=(
                str(args["document_id"])
                if args.get("document_id") is not None
                else None
            ),
            metadata=dict(args.get("metadata") or {}),
        )
    if name == "knowledge_index_workspace_file":
        return service.index_workspace_file(
            str(args["branch_id"]),
            path=str(args["path"]),
            title=str(args["title"]),
            source=str(args["source"]),
            kind=str(args.get("kind", "curated")),  # type: ignore[arg-type]
            document_id=(
                str(args["document_id"])
                if args.get("document_id") is not None
                else None
            ),
            metadata=dict(args.get("metadata") or {}),
        )
    if name == "knowledge_delete_document":
        document_id = str(args["document_id"])
        return {
            "document_id": document_id,
            "deleted": service.delete_document(
                str(args["branch_id"]),
                document_id,
            ),
        }
    if name == "knowledge_write_artifact":
        content = args["content"]
        if isinstance(content, str):
            content = content.encode()
        return service.write_artifact(
            str(args["branch_id"]),
            str(args["path"]),
            bytes(content),
        )
    if name == "knowledge_remember":
        return service.remember(
            str(args["branch_id"]),
            title=str(args["title"]),
            summary=str(args["summary"]),
            kind=str(args["kind"]),  # type: ignore[arg-type]
            evidence=tuple(str(item) for item in args.get("evidence") or ()),
            confidence=float(args.get("confidence", 1.0)),
            owner=(str(args["owner"]) if args.get("owner") is not None else None),
            tags=tuple(str(item) for item in args.get("tags") or ()),
            outcome=(
                str(args["outcome"]) if args.get("outcome") is not None else None
            ),
            supersedes=tuple(str(item) for item in args.get("supersedes") or ()),
            memory_id=(
                str(args["memory_id"])
                if args.get("memory_id") is not None
                else None
            ),
            recorded_at=(
                str(args["recorded_at"])
                if args.get("recorded_at") is not None
                else None
            ),
        )
    if name == "knowledge_diff":
        return service.diff(
            str(args["source_branch"]),
            str(args["target_branch"]),
        )
    if name == "knowledge_merge":
        return service.merge(
            str(args["source_branch"]),
            str(args["target_branch"]),
        )
    if name == "knowledge_delete_branch":
        branch_id = str(args["branch_id"])
        service.delete_branch(branch_id)
        return {"branch_id": branch_id, "deleted": True}
    if name == "knowledge_experiment_tasks":
        return service.experiment_tasks()
    raise RolloutTraceError(f"unsupported knowledge MCP tool: {name}")


def normalize_mcp_result(
    tool: str,
    result: Any,
    *,
    workspace_paths: Mapping[str, str] | None = None,
    branch_paths: Mapping[str, str] | None = None,
) -> Any:
    """Remove backend-specific fields while preserving logical outcomes."""

    path_to_branch = dict(workspace_paths or {})
    path_to_branch.update(
        {path: branch for branch, path in (branch_paths or {}).items()}
    )
    normalized = _replace_paths(result, path_to_branch, "")
    if tool == "knowledge_status" and isinstance(normalized, Mapping):
        return {
            "branches": sorted(str(item) for item in normalized.get("branches", [])),
            "embedding_model": normalized.get("embedding_model"),
            "embedding_dimensions": normalized.get("embedding_dimensions"),
        }
    if tool == "knowledge_search":
        hits = (
            normalized.get("result", [])
            if isinstance(normalized, Mapping)
            else normalized
        )
        return [
            {
                key: hit.get(key)
                for key in (
                    "document_id",
                    "chunk_id",
                    "path",
                    "title",
                    "source",
                )
            }
            for hit in hits or []
            if isinstance(hit, Mapping)
        ]
    return _drop_nondeterministic_fields(normalized)


def normalize_shell_result(
    output: str,
    exit_code: int,
    *,
    branch_paths: Mapping[str, str],
) -> dict[str, Any]:
    path_to_branch = {path: branch for branch, path in branch_paths.items()}
    normalized = str(_replace_paths(output, path_to_branch, ""))
    return {
        "exit_code": int(exit_code),
        "output_sha256": hashlib.sha256(normalized.encode()).hexdigest(),
    }


def _result_summary(name: str, value: Any) -> Any:
    """Keep small logical outcomes in reports without copying document bodies."""

    if name == "knowledge_search":
        return value
    if not isinstance(value, Mapping):
        return None
    keys = (
        "branch_id",
        "from_branch",
        "created",
        "deleted",
        "document_id",
        "path",
        "memory_id",
    )
    summary = {
        key: value[key]
        for key in keys
        if key in value
    }
    return summary or None


def _is_snapshot_document_id(identifier: str) -> bool:
    return identifier.startswith(("dsid_", "enterprise_"))


def _remap_branch_arguments(
    arguments: Mapping[str, Any],
    branch_map: Mapping[str, str],
) -> dict[str, Any]:
    remapped = dict(arguments)
    for key in (
        "branch_id",
        "from_branch",
        "source_branch",
        "target_branch",
    ):
        value = remapped.get(key)
        if value is not None:
            remapped[key] = branch_map.get(str(value), str(value))
    return remapped


def _restore_branch_ids(
    value: Any,
    inverse_branch_map: Mapping[str, str],
) -> Any:
    if isinstance(value, str):
        restored = inverse_branch_map.get(value, value)
        for actual, original in inverse_branch_map.items():
            restored = restored.replace(
                f"{{{{workspace:{actual}}}}}",
                f"{{{{workspace:{original}}}}}",
            )
        return restored
    if isinstance(value, Mapping):
        return {
            str(key): _restore_branch_ids(item, inverse_branch_map)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _restore_branch_ids(item, inverse_branch_map)
            for item in value
        ]
    return value


def _drop_nondeterministic_fields(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _drop_nondeterministic_fields(item)
            for key, item in sorted(value.items())
            if key
            not in {
                "backend",
                "storage",
                "score",
                "embedding",
                "vector",
                "elapsed_ns",
                "duration",
            }
        }
    if isinstance(value, list):
        return [_drop_nondeterministic_fields(item) for item in value]
    return value


def _decode_arguments(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value:
        parsed = json.loads(value)
        if not isinstance(parsed, Mapping):
            raise RolloutTraceError("tool arguments must decode to an object")
        return dict(parsed)
    return {}


def _mcp_structured_result(value: Any) -> Any:
    if not isinstance(value, Mapping):
        return None
    ok = value.get("Ok")
    if not isinstance(ok, Mapping):
        return value.get("Err")
    structured = ok.get("structuredContent")
    if structured is not None:
        return structured
    content = ok.get("content")
    if isinstance(content, list):
        texts = [
            str(item["text"])
            for item in content
            if isinstance(item, Mapping) and item.get("type") == "text"
        ]
        if len(texts) == 1:
            try:
                return json.loads(texts[0])
            except json.JSONDecodeError:
                return texts[0]
        return texts
    return ok


def _mcp_result_is_error(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return True
    if "Err" in value:
        return True
    ok = value.get("Ok")
    return bool(isinstance(ok, Mapping) and ok.get("isError"))


def _shell_expected(
    value: Any,
    workspace_paths: Mapping[str, str],
    source_cwd: str,
) -> dict[str, Any]:
    text = str(value or "")
    exit_match = _EXIT_CODE.search(text)
    token_match = _ORIGINAL_TOKENS.search(text)
    truncated = any(marker in text for marker in _TRUNCATION_MARKERS)
    output = text.split("\nOutput:\n", 1)[1] if "\nOutput:\n" in text else text
    normalized = str(_replace_paths(output, workspace_paths, source_cwd))
    expected: dict[str, Any] = {
        "status": "ok" if exit_match and exit_match.group(1) == "0" else "error",
        "exit_code": int(exit_match.group(1)) if exit_match else None,
        "truncated": truncated,
        "recorded_output_tokens": (
            int(token_match.group(1)) if token_match else None
        ),
    }
    if not truncated:
        expected["normalized_digest"] = _digest(
            {
                "exit_code": expected["exit_code"],
                "output_sha256": hashlib.sha256(normalized.encode()).hexdigest(),
            }
        )
    return expected


def _elapsed_seconds(start: str, end: str) -> float:
    """Return the wall-clock delay between two rollout timestamps."""

    try:
        started = datetime.fromisoformat(start.replace("Z", "+00:00"))
        ended = datetime.fromisoformat(end.replace("Z", "+00:00"))
    except ValueError:
        return 0.01
    return max(0.0, (ended - started).total_seconds())


def _custom_tool_exit_code(output: str) -> int | None:
    match = re.search(r"(?:Exit code|Process exited with code):\s*(\d+)", output)
    return int(match.group(1)) if match else None


def _replace_paths(
    value: Any,
    workspace_paths: Mapping[str, str],
    source_cwd: str,
) -> Any:
    if isinstance(value, str):
        replacements = sorted(
            (
                (path, f"{{{{workspace:{branch}}}}}")
                for path, branch in workspace_paths.items()
                if path
            ),
            key=lambda item: len(item[0]),
            reverse=True,
        )
        for path, placeholder in replacements:
            if Path(path).is_absolute():
                value = value.replace(path, placeholder)
                continue
            # Relative checkout paths may also be suffixes of logical branch
            # IDs (for example ``flashinfer-capacity-v2``). Replace them only
            # where a shell/path token can begin, never in the middle of
            # another slash-delimited identifier.
            value = re.sub(
                rf"(?<![A-Za-z0-9_./-]){re.escape(path)}"
                rf"(?=$|[/\s'\"\\])",
                lambda _match: placeholder,
                value,
            )
        if source_cwd:
            value = value.replace(source_cwd, "{{repo}}")
        return value
    if isinstance(value, Mapping):
        return {
            str(key): _replace_paths(item, workspace_paths, source_cwd)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _replace_paths(item, workspace_paths, source_cwd) for item in value
        ]
    return value


def _digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def _percentile(values: Sequence[int], quantile: float) -> int:
    if not values:
        return 0
    index = max(0, min(len(values) - 1, int(len(values) * quantile + 0.999) - 1))
    return sorted(values)[index]


__all__ = [
    "EventReplayResult",
    "RolloutAnalysis",
    "RolloutAnalyzer",
    "RolloutTraceError",
    "WorkloadEvent",
    "WorkloadReplayReport",
    "WorkloadReplayer",
    "WorkloadTrace",
    "combine_workload_traces",
    "dispatch_knowledge_tool",
    "normalize_mcp_result",
    "resolve_memory_timestamps",
]
