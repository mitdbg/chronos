"""Analyze Codex rollout logs and replay their high-level tool calls.

The replay boundary is intentionally the action selected by Codex: one MCP
tool call or one shell command. Storage-engine syscalls and implementation
details remain opaque so the same logical workload can exercise different
backends.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import math
import os
import random
import re
import signal
import subprocess
import tempfile
import time
import dataclasses
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from chronos_enterprise_knowledge.models import canonical_json
from chronos_enterprise_knowledge.service import KnowledgeService
from chronos_enterprise_knowledge.timing import StoreTimingCollector

_SCHEMA_VERSION = 1
_WORKSPACE_TOKEN = re.compile(r"\{\{workspace:([^}]+)\}\}")
_EXIT_CODE = re.compile(r"(?:Process exited with code|Exit code:)\s*(\d+)")
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
# Codex collaboration calls coordinate the capture session but do not issue
# application operations.  They are intentionally absent from a workload
# trace and must not make an otherwise replayable trace fail validation.
_NON_WORKLOAD_CALLS = {
    "followup_task",
    "interrupt_agent",
    "list_agents",
    "send_message",
    "spawn_agent",
    "wait_agent",
}
_NESTED_STATEFUL_TOOL = re.compile(
    r"\btools\.(exec_command|shell_command|apply_patch|write_stdin)\s*\("
)
_SHELL_DIAGNOSTIC_LIMIT = 4_000
_RETRY_RANDOM = random.SystemRandom()

EventKind = Literal["mcp", "shell", "patch"]


class RolloutTraceError(RuntimeError):
    """Raised for malformed or unsupported rollout traces."""


def _percentile(values: Sequence[int], quantile: float) -> int:
    """Return the nearest-rank percentile used by replay summaries."""

    if not values:
        return 0
    index = max(0, math.ceil(quantile * len(values)) - 1)
    return int(sorted(values)[index])


def _nested_stateful_tool_calls(source: str) -> tuple[str, ...]:
    """Find stateful calls hidden inside a custom orchestration script."""

    return tuple(dict.fromkeys(_NESTED_STATEFUL_TOOL.findall(source)))


@dataclass(frozen=True)
class _NestedAction:
    """One stateful operation issued inside Codex's ``exec`` wrapper."""

    kind: EventKind
    name: str
    arguments: dict[str, Any]


def _matching_delimiter(
    source: str,
    start: int,
    opening: str,
    closing: str,
) -> int:
    """Return the matching delimiter while respecting JavaScript strings."""

    depth = 0
    quote = ""
    escaped = False
    for index in range(start, len(source)):
        character = source[index]
        if quote:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = ""
            continue
        if character in {"'", '"', "`"}:
            quote = character
        elif character == opening:
            depth += 1
        elif character == closing:
            depth -= 1
            if depth == 0:
                return index
    raise RolloutTraceError("unterminated nested Codex tool call")


def _read_js_literal(source: str, start: int) -> tuple[str, int]:
    """Read a quoted JavaScript literal starting at ``start``."""

    quote = source[start]
    if quote not in {"'", '"', "`"}:
        raise RolloutTraceError("expected a JavaScript string literal")
    escaped = False
    for index in range(start + 1, len(source)):
        character = source[index]
        if escaped:
            escaped = False
        elif character == "\\":
            escaped = True
        elif character == quote:
            return source[start : index + 1], index + 1
    raise RolloutTraceError("unterminated JavaScript string literal")


def _decode_js_literal(token: str, variables: Mapping[str, str]) -> str:
    token = token.strip()
    if not token or token[0] not in {"'", '"', "`"}:
        return variables.get(token, token)
    quote = token[0]
    body = token[1:-1]
    if quote == '"':
        try:
            value = json.loads(token)
        except json.JSONDecodeError as exc:
            raise RolloutTraceError("invalid JavaScript string literal") from exc
    elif quote == "'":
        # The wrappers use single-quoted strings only for shell commands.  A
        # small scanner is safer here than interpreting arbitrary escape
        # sequences as Python source.
        value_chars: list[str] = []
        index = 0
        while index < len(body):
            if body[index] != "\\" or index + 1 == len(body):
                value_chars.append(body[index])
                index += 1
                continue
            escaped = body[index + 1]
            value_chars.append({"n": "\n", "r": "\r", "t": "\t"}.get(escaped, escaped))
            index += 2
        value = "".join(value_chars)
    else:
        value_chars = []
        index = 0
        while index < len(body):
            if body[index] != "\\" or index + 1 == len(body):
                value_chars.append(body[index])
                index += 1
                continue
            escaped = body[index + 1]
            if escaped in {"`", "\\", "$"}:
                value_chars.append(escaped)
            else:
                value_chars.append({"n": "\n", "r": "\r", "t": "\t"}.get(escaped, escaped))
            index += 2
        value = "".join(value_chars)
    return re.sub(
        r"\$\{([A-Za-z_$][A-Za-z0-9_$]*)\}",
        lambda match: variables.get(match.group(1), match.group(0)),
        value,
    )


def _split_js_top_level(source: str, separator: str = ",") -> list[str]:
    parts: list[str] = []
    start = 0
    stack: list[str] = []
    quote = ""
    escaped = False
    matching = {"{": "}", "[": "]", "(": ")"}
    for index, character in enumerate(source):
        if quote:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = ""
            continue
        if character in {"'", '"', "`"}:
            quote = character
        elif character in matching:
            stack.append(matching[character])
        elif stack and character == stack[-1]:
            stack.pop()
        elif not stack and character == separator:
            parts.append(source[start:index].strip())
            start = index + 1
    tail = source[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _parse_js_object(source: str, variables: Mapping[str, str]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for part in _split_js_top_level(source):
        if not part:
            continue
        colon = next(
            (
                index
                for index, character in enumerate(part)
                if character == ":"
            ),
            -1,
        )
        if colon < 0:
            shorthand = part.strip()
            if shorthand in variables:
                value[shorthand] = variables[shorthand]
            continue
        key = part[:colon].strip().strip("'\"")
        raw = part[colon + 1 :].strip()
        if raw in {"true", "false"}:
            parsed: Any = raw == "true"
        elif raw == "null":
            parsed = None
        elif re.fullmatch(r"-?\d+(?:\.\d+)?", raw):
            parsed = float(raw) if "." in raw else int(raw)
        else:
            parsed = _decode_js_literal(raw, variables)
        value[key] = parsed
    return value


def _extract_nested_actions(
    source: str,
    *,
    _base_variables: Mapping[str, str] | None = None,
    _expand_loops: bool = True,
) -> tuple[_NestedAction, ...]:
    """Extract shell/patch calls from Codex's persisted orchestration script.

    Current Codex releases expose ``apply_patch`` and ``shell_command`` to the
    model only through a JavaScript ``exec`` wrapper.  The wrapper is not the
    workload boundary: the calls it contains are.  This parser handles the
    generated wrapper subset (string variables, object arguments, and direct
    calls) without executing untrusted JavaScript during trace analysis.
    """

    variables: dict[str, str] = dict(_base_variables or {})
    declaration = re.compile(r"\b(?:const|let|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*")
    for match in declaration.finditer(source):
        position = match.end()
        while position < len(source) and source[position].isspace():
            position += 1
        if position < len(source) and source[position] in {"'", '"', "`"}:
            token, _ = _read_js_literal(source, position)
            variables[match.group(1)] = _decode_js_literal(token, variables)

    # Codex's code-mode host commonly batches shell calls as
    # ``const cmds = [ ... ]; for (const command of cmds) { ... }``.  Expand
    # this small, declarative subset without evaluating JavaScript.  Calls in
    # the loop are then excluded from the outer scan to avoid duplicates.
    array_variables: dict[str, list[str]] = {}
    array_declaration = re.compile(
        r"\b(?:const|let|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*\["
    )
    for match in array_declaration.finditer(source):
        open_position = source.find("[", match.start(), match.end())
        # The declaration regexp also sees text such as ``const values = [``
        # inside a quoted patch passed to ``tools.apply_patch``.  It is not a
        # JavaScript array in the wrapper, and its brackets may be escaped or
        # intentionally incomplete.  Ignore that false positive and let the
        # normal direct-call scan recover the patch action.
        try:
            close_position = _matching_delimiter(
                source, open_position, "[", "]"
            )
        except RolloutTraceError:
            continue
        values: list[str] = []
        for raw in _split_js_top_level(
            source[open_position + 1 : close_position]
        ):
            decoded = _decode_js_literal(raw, variables)
            if decoded != raw or raw.strip() in variables:
                values.append(str(decoded))
        array_variables[match.group(1)] = values

    actions: list[tuple[int, _NestedAction]] = []
    loop_spans: list[tuple[int, int]] = []
    if _expand_loops:
        loop_declaration = re.compile(
            r"for\s*\(\s*(?:const|let|var)\s+"
            r"([A-Za-z_$][A-Za-z0-9_$]*)\s+of\s+"
            r"([A-Za-z_$][A-Za-z0-9_$]*)\s*\)\s*\{"
        )
        for loop in loop_declaration.finditer(source):
            open_position = source.find("{", loop.start(), loop.end())
            close_position = _matching_delimiter(source, open_position, "{", "}")
            values = array_variables.get(loop.group(2), [])
            body = source[open_position + 1 : close_position]
            for value in values:
                loop_variables = dict(variables)
                loop_variables[loop.group(1)] = value
                for action in _extract_nested_actions(
                    body,
                    _base_variables=loop_variables,
                    _expand_loops=False,
                ):
                    actions.append((open_position, action))
            loop_spans.append((loop.start(), close_position + 1))

    calls = re.compile(r"\btools\.(apply_patch|shell_command|exec_command)\s*\(")
    for match in calls.finditer(source):
        if any(start <= match.start() < end for start, end in loop_spans):
            continue
        name = match.group(1)
        open_position = source.find("(", match.start())
        close_position = _matching_delimiter(source, open_position, "(", ")")
        raw = source[open_position + 1 : close_position].strip()
        if name == "apply_patch":
            patch = _decode_js_literal(raw, variables)
            if raw in variables:
                patch = variables[raw]
            actions.append(
                (
                    match.start(),
                    _NestedAction("patch", name, {"patch": patch}),
                )
            )
            continue
        if not raw.startswith("{"):
            raise RolloutTraceError(f"{name} arguments are not an object")
        object_end = _matching_delimiter(raw, 0, "{", "}")
        parsed = _parse_js_object(raw[1:object_end], variables)
        command = parsed.get("command", parsed.get("cmd"))
        if command is None:
            raise RolloutTraceError(f"{name} call has no command")
        arguments: dict[str, Any] = {"cmd": str(command)}
        if parsed.get("workdir") is not None:
            arguments["workdir"] = str(parsed["workdir"])
        timeout_ms = parsed.get("timeout_ms")
        if timeout_ms is not None:
            arguments["timeout_seconds"] = float(timeout_ms) / 1000
        actions.append((match.start(), _NestedAction("shell", name, arguments)))
    return tuple(action for _, action in sorted(actions, key=lambda item: item[0]))


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
    # Detached benchmark services may run as root, so ``Path.home()`` is not
    # necessarily the home directory used when the trace was captured.  Find
    # the same read-only editor tool installation for any local account
    # instead of making command availability depend on the service user.
    extension_roots = {
        Path.home() / ".vscode-server/extensions",
        Path("/root/.vscode-server/extensions"),
    }
    extension_roots.update(Path("/home").glob("*/.vscode-server/extensions"))
    for extension_root in extension_roots:
        try:
            candidates = sorted(
                extension_root.glob("openai.chatgpt-*/bin/linux-x86_64"),
                reverse=True,
            )
        except OSError:
            candidates = []
        for candidate in candidates:
            try:
                available = (candidate / "rg").is_file()
            except OSError:
                available = False
            if available:
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
        patch_results = [
            row.get("payload", {})
            for row in records
            if row.get("type") == "event_msg"
            and row.get("payload", {}).get("type") == "patch_apply_end"
        ]
        patch_result_index = 0
        mcp_call_ids = {
            str(row.get("payload", {}).get("call_id"))
            for row in records
            if row.get("type") == "event_msg"
            and row.get("payload", {}).get("type") == "mcp_tool_call_end"
        }

        workspace_paths: dict[str, str] = {}
        merge_previews: dict[tuple[str, str], Mapping[str, Any]] = {}
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
                merge_pair = _merge_branch_pair(portable_arguments)
                if (
                    tool == "knowledge_merge_preview"
                    and merge_pair is not None
                    and isinstance(normalized, Mapping)
                ):
                    merge_previews[merge_pair] = normalized
                elif (
                    tool == "knowledge_merge"
                    and merge_pair is not None
                    and portable_arguments.get("selected_change_ids") is not None
                ):
                    preview = merge_previews.get(merge_pair)
                    if preview is None:
                        skipped.append("knowledge_merge:missing-preview")
                    else:
                        selection_groups, residual, partial_groups = (
                            _merge_selection_groups(
                                preview,
                                portable_arguments["selected_change_ids"],
                            )
                        )
                        expected["merge_selection_groups"] = selection_groups
                        if partial_groups:
                            # A failed selective merge can intentionally carry
                            # only part of a logical document group.  Preserve
                            # that shape (rather than capture-side IDs) so the
                            # replay can reproduce the dependency error with
                            # fresh change IDs.
                            expected["merge_selection_partial_groups"] = (
                                partial_groups
                            )
                        if residual:
                            expected["merge_selection_residual_ids"] = residual
                            # Unknown IDs cannot be mapped portably.  A
                            # successful merge with such an allow-list is not
                            # replayable; a failed attempt is still replayable
                            # as an invalid-selection attempt and has no state
                            # effect.
                            if expected["status"] != "error":
                                skipped.append(
                                    "knowledge_merge:unmapped-selection"
                                )
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
                    nested_source = str(payload.get("input") or "")
                    nested = _extract_nested_actions(nested_source)
                    nested_results = _custom_tool_output_texts(
                        outputs.get(str(payload.get("call_id") or ""), {})
                    )
                    result_index = 0
                    for action in nested:
                        if action.kind == "patch":
                            patch_result = (
                                patch_results[patch_result_index]
                                if patch_result_index < len(patch_results)
                                else {}
                            )
                            patch_result_index += 1
                            success = bool(patch_result.get("success", False))
                            events.append(
                                WorkloadEvent(
                                    sequence=len(events),
                                    kind="patch",
                                    name=action.name,
                                    timestamp=str(row.get("timestamp") or ""),
                                    arguments={
                                        "patch": _replace_paths(
                                            action.arguments["patch"],
                                            workspace_paths,
                                            source_cwd,
                                        )
                                    },
                                    expected={
                                        "status": "ok" if success else "error",
                                        "exit_code": 0 if success else 1,
                                    },
                                )
                            )
                            # ``apply_patch`` itself usually returns ``{}``;
                            # consume that wrapper result so a following shell
                            # call receives its own output block.
                            if result_index < len(nested_results):
                                result_index += 1
                            continue
                        shell_output = (
                            nested_results[result_index]
                            if result_index < len(nested_results)
                            else ""
                        )
                        result_index += 1
                        shell_arguments = _replace_paths(
                            action.arguments,
                            workspace_paths,
                            source_cwd,
                        )
                        events.append(
                            WorkloadEvent(
                                sequence=len(events),
                                kind="shell",
                                name=action.name,
                                timestamp=str(row.get("timestamp") or ""),
                                arguments=shell_arguments,
                                expected=_shell_expected(
                                    shell_output,
                                    workspace_paths,
                                    source_cwd,
                                ),
                            )
                        )
                    unknown_nested = set(_nested_stateful_tool_calls(nested_source)) - {
                        action.name for action in nested
                    }
                    skipped.extend(f"{name}:{tool}" for tool in sorted(unknown_nested))
                    continue
                if name != "apply_patch":
                    if name in _NON_WORKLOAD_CALLS:
                        continue
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
            if name in _NON_WORKLOAD_CALLS:
                continue
            skipped.append(name or "<unnamed>")

        trace = WorkloadTrace(
            derived_trace_id,
            tuple(events),
            {
                "source_rollout": str(source),
                "source_cwd": source_cwd,
                "description": "Normalized from a Codex rollout session.",
                "llm_timing": _extract_llm_timing(records),
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
            # A branch can be deliberately removed after the agent publishes
            # its memory (for example, when a temporary task branch is
            # cleaned up).  Backends do not all include the same text in the
            # exception; Chronos raises ``BranchNotFoundError`` whose string
            # is only the branch id.  Treat the typed error and the textual
            # variants uniformly, then use the rollout timestamp below.
            error_text = str(exc).casefold()
            missing_branch = (
                type(exc).__name__ == "BranchNotFoundError"
                or "branch not found" in error_text
                or ("branch" in error_text and "not found" in error_text)
            )
            if not missing_branch:
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
        # A Codex shell-tool capture can emit a placeholder invocation while
        # switching back to the repository root (``cmd: cmd`` and
        # ``workdir: root``).  It has no workload semantics and is not
        # portable across shells; replay it for trace fidelity, but do not let
        # its capture-local exit status invalidate the surrounding workflow.
        if command == "cmd" and str(event.arguments.get("workdir") or "") == "root":
            return "any"
        if expected.get("status") == "ok" and _read_only_shell_capture(event):
            # Inspection-only commands are allowed to differ from the source
            # checkout without aborting the stateful replay.  The report still
            # records their digest/status mismatch.
            return "any"
        ephemeral_cleanup = (
            command.startswith(("rm ", "mv ", "find "))
            and any(
                marker in command
                for marker in (
                    "__pycache__",
                    ".pytest_cache",
                    ".ruff_cache",
                    ".test-venv",
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


def _read_only_shell_capture(event: WorkloadEvent) -> bool:
    """Identify capture-local inspection failures that do not change state.

    A trace can inspect optional files or run assertions against a source
    checkout that differs from the pinned replay snapshot.  Such a command
    must remain visible as a status/digest mismatch, but it should not prevent
    the stateful MCP portion of the workflow from being replayed.  Only
    commands composed of inspection primitives are eligible; mutations and
    tests that can alter state remain strict.
    """

    if event.kind != "shell":
        return False
    command = str(event.arguments.get("cmd") or "").strip()
    if not command or any(
        token in command
        for token in (
            "rm ",
            "rm -",
            "mv ",
            "cp ",
            "mkdir ",
            "touch ",
            "chmod ",
            "chown ",
            "git add",
            "git commit",
            "git checkout",
            "git switch",
            "git reset",
            "git clean",
            "python -c",  # executable snippets may mutate a checkout
        )
    ):
        return False
    inspection_prefixes = (
        "cat ",
        "sed ",
        "head ",
        "tail ",
        "find ",
        "rg ",
        "grep ",
        "command -v ",
        "test ",
        "python3 - <<",
        "python - <<",
    )
    return any(command.startswith(prefix) for prefix in inspection_prefixes)


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
    timing_ms: dict[str, float] = field(default_factory=dict)
    model_inference_ms: float = 0.0

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
            "timing_ms": dict(self.timing_ms),
            "model_inference_ms": self.model_inference_ms,
        }


@dataclass(frozen=True)
class WorkloadReplayReport:
    trace_id: str
    backend: str
    events: tuple[EventReplayResult, ...]
    wall_time_ns: int
    workspace_paths: dict[str, str]
    llm_latency_enabled: bool = False
    llm_latency_scale: float = 1.0
    llm_recorded_ms: float = 0.0
    llm_slept_ms: float = 0.0
    llm_calls_slept: int = 0
    # Merge calls are retried inside the replay engine, so they are not
    # represented one-for-one by ``events``.  Keep an explicit audit trail of
    # the initial call and every refresh/apply retry, including failures.
    merge_attempts: tuple[dict[str, Any], ...] = ()

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
            # Checkout captures may contain a failed absolute mount-path
            # attempt (for example, a path below a protected host directory).
            # Replay removes capture-local mount paths and lets each backend
            # choose its own checkout location; a successful checkout is the
            # portable equivalent. The recorded digest remains visible in
            # ``matched_recorded_results``.
            or (
                event.kind == "mcp"
                and event.name == "knowledge_checkout"
                and event.status == "ok"
            )
            # A baseline trace may record a read failure caused by an
            # inconsistent cross-store publication.  A replay that returns a
            # coherent document/search result is a valid (and important)
            # correctness improvement; retain the digest/status mismatch in
            # the report without aborting the benchmark.
            or (
                event.kind == "mcp"
                and event.status == "ok"
                and event.expected_status == "error"
                and event.name
                in {
                    "knowledge_get_document",
                    "knowledge_search",
                    "knowledge_diff",
                    "knowledge_status",
                }
            )
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

    def store_timing_summary(self) -> dict[str, float]:
        """Return measured store time summed over replay events."""

        totals = {
            "relational_db": 0.0,
            "vector_db": 0.0,
            "filesystem": 0.0,
            "others": 0.0,
        }
        for event in self.events:
            for category in totals:
                totals[category] += float(event.timing_ms.get(category, 0.0))
        return totals

    def as_dict(self, *, include_events: bool = True) -> dict[str, Any]:
        value: dict[str, Any] = {
            "trace_id": self.trace_id,
            "backend": self.backend,
            "succeeded": self.succeeded,
            "matched_recorded_results": self.matched,
            "events_replayed": len(self.events),
            "wall_time_ms": self.wall_time_ns / 1_000_000,
            "llm_timing": {
                "enabled": self.llm_latency_enabled,
                "scale": self.llm_latency_scale,
                "recorded_ms": self.llm_recorded_ms,
                "slept_ms": self.llm_slept_ms,
                "calls_slept": self.llm_calls_slept,
            },
            "merge_attempts": [dict(item) for item in self.merge_attempts],
            "latency_by_operation": self.latency_summary(),
            "store_timing_ms": self.store_timing_summary(),
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
        merge_retries: int = 0,
        operation_lock: Any | None = None,
        replay_llm_latency: bool = False,
        llm_latency_scale: float = 1.0,
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
        # ``-1`` is the explicit retry-until-success mode used by the
        # finite incident-response swarm.  Only errors classified as
        # transient merge races enter that loop; semantic conflicts still
        # fail immediately.
        self.merge_retries = int(merge_retries)
        if self.merge_retries < -1:
            raise ValueError(
                "merge_retries must be non-negative or -1 for unlimited"
            )
        # Optional benchmark/runtime coordination.  The replayer does not
        # know which operations need serialization; the caller supplies an
        # object with ``before_operation``/``after_operation`` hooks.  Keeping the
        # hook generic lets a baseline coordinate a complete logical
        # operation without putting backend-specific locking in the replay
        # engine.
        self.operation_lock = operation_lock
        self.replay_llm_latency = bool(replay_llm_latency)
        self.llm_latency_scale = float(llm_latency_scale)
        if self.llm_latency_scale < 0:
            raise ValueError("llm_latency_scale must be non-negative")
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
        self._preview_tokens: dict[tuple[str, str], str] = {}
        self._merge_previews: dict[
            tuple[str, str], Mapping[str, Any]
        ] = {}
        # Keep the backend-native prepared object separately from the plain
        # mapping used for replay-time selection.  The latter is needed for
        # trace normalization, while the former lets merge() reuse work that
        # merge_preview() already performed.
        self._prepared_merge_previews: dict[tuple[str, str], Any] = {}
        self._trace_tmpdir: str | None = None
        self._recorded_path_extensions: tuple[str, ...] = ()
        self._llm_before_event: dict[int, tuple[float, int]] = {}
        self._llm_postlude: tuple[float, int] = (0.0, 0)
        self._llm_recorded_ms = 0.0
        self._llm_slept_ms = 0.0
        self._llm_calls_slept = 0
        self._merge_attempts: list[dict[str, Any]] = []

    def replay(self, trace: WorkloadTrace) -> WorkloadReplayReport:
        if trace.metadata.get("fully_replayable") is False:
            skipped = trace.metadata.get("skipped_call_counts") or {}
            raise RolloutTraceError(
                "trace contains unsupported Codex tool calls and cannot be "
                f"replayed completely: {skipped}"
            )
        replay_lock_token: Any | None = None
        replay_lock_active = False
        replay_succeeded = False
        before_replay = getattr(self.operation_lock, "before_replay", None)
        if callable(before_replay):
            # A coarse-grained baseline may need to protect the complete agent
            # step, including the recorded model delay before its first tool
            # call and the postlude after its last one.  This hook is optional
            # so ordinary operation-scoped coordinators retain their behavior.
            replay_lock_token = before_replay(trace.trace_id)
            replay_lock_active = True
        prefix = re.sub(r"[^A-Za-z0-9_.-]+", "-", trace.trace_id)[:48]
        trace_tmpdir = tempfile.TemporaryDirectory(
            prefix=f"chronos-replay-{prefix}-"
        )
        self._trace_tmpdir = trace_tmpdir.name
        self._preview_tokens = {}
        self._merge_previews = {}
        self._prepared_merge_previews = {}
        self._merge_attempts = []
        try:
            self._configure_llm_latency(trace)
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
            results: list[EventReplayResult] = []
            wall_started = time.perf_counter_ns()
            for event in trace.events:
                event_started = time.perf_counter_ns()
                try:
                    result = self._replay_event(event)
                except Exception as exc:
                    expected_status = _expected_replay_status(event)
                    elapsed_ns = time.perf_counter_ns() - event_started
                    model_ms = (
                        self._llm_before_event.get(event.sequence, (0.0, 0))[0]
                        * self.llm_latency_scale
                    )
                    timing_ms = {
                        "relational_db": 0.0,
                        "vector_db": 0.0,
                        "filesystem": 0.0,
                        "others": max(
                            0.0,
                            elapsed_ns / 1_000_000 - model_ms,
                        ),
                    }
                    result = EventReplayResult(
                        sequence=event.sequence,
                        kind=event.kind,
                        name=event.name,
                        elapsed_ns=elapsed_ns,
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
                        timing_ms=timing_ms,
                        model_inference_ms=model_ms,
                    )
                    results.append(result)
                    if (
                        not self.continue_on_error
                        and expected_status not in {"error", "any"}
                    ):
                        break
                else:
                    results.append(result)
            completed_all_events = len(results) == len(trace.events)
            if completed_all_events:
                self._sleep_llm_postlude()
            report = WorkloadReplayReport(
                trace.trace_id,
                self.service.backend.backend_name,
                tuple(results),
                time.perf_counter_ns() - wall_started,
                dict(self.workspace_paths),
                llm_latency_enabled=self.replay_llm_latency,
                llm_latency_scale=self.llm_latency_scale,
                llm_recorded_ms=self._llm_recorded_ms,
                llm_slept_ms=self._llm_slept_ms,
                llm_calls_slept=self._llm_calls_slept,
                merge_attempts=tuple(self._merge_attempts),
            )
            replay_succeeded = report.succeeded
        finally:
            try:
                if replay_lock_active:
                    after_replay = getattr(
                        self.operation_lock, "after_replay", None
                    )
                    if callable(after_replay):
                        after_replay(
                            replay_lock_token,
                            trace.trace_id,
                            succeeded=replay_succeeded,
                        )
            finally:
                if self.operation_lock is not None:
                    release_all = getattr(
                        self.operation_lock, "release_all", None
                    )
                    if callable(release_all):
                        release_all()
                self._trace_tmpdir = None
                self._recorded_path_extensions = ()
                self._llm_before_event = {}
                self._llm_postlude = (0.0, 0)
                trace_tmpdir.cleanup()
                replay_lock_active = False
        return report

    def _replay_event(self, event: WorkloadEvent) -> EventReplayResult:
        model_inference_ms = self._sleep_llm_before_event(event.sequence)
        started = time.perf_counter_ns()
        collector = StoreTimingCollector()
        lock_token = None
        lock_succeeded = False
        lock_arguments: Mapping[str, Any]
        if event.kind == "mcp":
            arguments = _remap_branch_arguments(
                self._expand(
                    event.arguments,
                    collapse_workspace_tokens=False,
                ),
                self.branch_map,
            )
            arguments = self._logicalize_workspace_paths(arguments)
            preview_key = _merge_branch_pair(arguments)
            if (
                event.name == "knowledge_merge"
                and preview_key is not None
            ):
                if (
                    arguments.get("preview_token") is not None
                    and preview_key in self._preview_tokens
                ):
                    arguments["preview_token"] = self._preview_tokens[preview_key]
                selection_groups = event.expected.get("merge_selection_groups")
                if selection_groups is not None:
                    preview = self._merge_previews.get(preview_key)
                    if preview is None:
                        raise RolloutTraceError(
                            "selective merge has no replay-time preview"
                        )
                    arguments["selected_change_ids"] = _resolve_merge_selection(
                        preview,
                        selection_groups,
                        event.expected.get("merge_selection_partial_groups"),
                        invalid_count=(
                            len(event.expected.get("merge_selection_residual_ids") or [])
                            if _expected_replay_status(event) == "error"
                            else 0
                        ),
                    )
                preview = self._merge_previews.get(preview_key)
                conflict_choices = arguments.get("conflict_choices")
                if (
                    isinstance(conflict_choices, Mapping)
                    and isinstance(preview, Mapping)
                ):
                    actual_conflicts = _merge_conflict_ids(preview)
                    missing = set(str(key) for key in conflict_choices) - set(
                        actual_conflicts
                    )
                    values = [str(value) for value in conflict_choices.values()]
                    # Conflict identifiers include content hashes and can
                    # legitimately differ when a replay snapshot contains a
                    # newer source checkout.  When the captured decision is
                    # uniform (as with a reviewed "keep target" merge),
                    # preserve that decision for the replay-time conflicts.
                    if missing and actual_conflicts and len(set(values)) == 1:
                        arguments["conflict_choices"] = {
                            conflict_id: values[0]
                            for conflict_id in actual_conflicts
                        }
            lock_arguments = arguments
            if self.operation_lock is not None:
                before_operation = getattr(
                    self.operation_lock, "before_operation", None
                )
                if callable(before_operation):
                    lock_token = before_operation(event.name, arguments)
            merge_context = (
                _merge_quiesce_context(self.service, preview_key)
                if event.name == "knowledge_merge" and preview_key is not None
                else contextlib.nullcontext()
            )
            with merge_context:
                with collector.active():
                    merge_started = (
                        time.perf_counter_ns()
                        if event.name == "knowledge_merge"
                        else None
                    )
                    try:
                        value = dispatch_knowledge_tool(
                            self.service,
                            event.name,
                            arguments,
                            prepared_preview=(
                                self._prepared_merge_previews.get(preview_key)
                                if event.name == "knowledge_merge"
                                and preview_key is not None
                                else None
                            ),
                        )
                    except Exception as exc:
                        if merge_started is not None and preview_key is not None:
                            self._record_merge_attempt(
                                event,
                                preview_key,
                                attempt=0,
                                merge_elapsed_ns=time.perf_counter_ns()
                                - merge_started,
                                status="error",
                                error=exc,
                            )
                        if (
                            event.name != "knowledge_merge"
                            or preview_key is None
                            # A trace may intentionally record a merge that
                            # fails (for example, an invalid selective
                            # promotion).  That failure is part of the
                            # workload contract; retrying it in unlimited
                            # mode can turn a deterministic expected error
                            # into an endless loop when its message also
                            # contains a transient-looking marker.
                            or _expected_replay_status(event) == "error"
                            or self.merge_retries == 0
                            or not _retryable_merge_error(exc)
                        ):
                            raise
                        value = self._retry_merge(
                            event,
                            arguments,
                            preview_key,
                            exc,
                        )
                        lock_succeeded = True
                    else:
                        if merge_started is not None and preview_key is not None:
                            self._record_merge_attempt(
                                event,
                                preview_key,
                                attempt=0,
                                merge_elapsed_ns=time.perf_counter_ns()
                                - merge_started,
                                status="ok",
                            )
                        lock_succeeded = True
                    finally:
                        if lock_token is not None:
                            after_operation = getattr(
                                self.operation_lock, "after_operation", None
                            )
                            if callable(after_operation):
                                after_operation(
                                    lock_token,
                                    event.name,
                                    arguments,
                                    succeeded=lock_succeeded,
                                )
            if event.name == "knowledge_merge_preview" and preview_key is not None:
                preview_payload = _plain_dataclass(value)
                if (
                    isinstance(preview_payload, Mapping)
                    and preview_payload.get("preview_token")
                ):
                    self._preview_tokens[preview_key] = str(
                        preview_payload["preview_token"]
                    )
                    self._merge_previews[preview_key] = preview_payload
                    if _is_prepared_merge_preview(value):
                        self._prepared_merge_previews[preview_key] = value
                    else:
                        self._prepared_merge_previews.pop(preview_key, None)
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
            lock_arguments = self._expand(event.arguments)
            if self.operation_lock is not None:
                before_operation = getattr(
                    self.operation_lock, "before_operation", None
                )
                if callable(before_operation):
                    lock_token = before_operation(event.name, lock_arguments)
            try:
                with collector.active():
                    value, exit_code = self._run_shell(lock_arguments)
                lock_succeeded = True
            finally:
                if lock_token is not None:
                    after_operation = getattr(
                        self.operation_lock, "after_operation", None
                    )
                    if callable(after_operation):
                        after_operation(
                            lock_token,
                            event.name,
                            lock_arguments,
                            succeeded=lock_succeeded,
                        )
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
            lock_arguments = self._expand(event.arguments)
            if self.operation_lock is not None:
                before_operation = getattr(
                    self.operation_lock, "before_operation", None
                )
                if callable(before_operation):
                    lock_token = before_operation(event.name, lock_arguments)
            try:
                with collector.active():
                    value, exit_code = self._run_patch(lock_arguments)
                lock_succeeded = True
            finally:
                if lock_token is not None:
                    after_operation = getattr(
                        self.operation_lock, "after_operation", None
                    )
                    if callable(after_operation):
                        after_operation(
                            lock_token,
                            event.name,
                            lock_arguments,
                            succeeded=lock_succeeded,
                        )
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
            event.kind in {"shell", "patch"}
            and expected_status != "any"
            and status != expected_status
        ):
            error = _shell_failure_diagnostic(str(value))
        timing_ms = collector.snapshot_ms()
        measured_ms = sum(timing_ms.values())
        timing_ms["others"] = max(
            0.0,
            elapsed / 1_000_000 - measured_ms - model_inference_ms,
        )
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
            timing_ms=timing_ms,
            model_inference_ms=model_inference_ms,
        )

    def _configure_llm_latency(self, trace: WorkloadTrace) -> None:
        self._llm_before_event = {}
        self._llm_postlude = (0.0, 0)
        self._llm_recorded_ms = 0.0
        self._llm_slept_ms = 0.0
        self._llm_calls_slept = 0
        if not self.replay_llm_latency:
            return
        (
            self._llm_before_event,
            self._llm_postlude,
            self._llm_recorded_ms,
        ) = _build_llm_replay_schedule(trace)

    def _sleep_llm_before_event(self, sequence: int) -> float:
        delay_ms, call_count = self._llm_before_event.get(sequence, (0.0, 0))
        scaled_ms = delay_ms * self.llm_latency_scale
        if scaled_ms > 0:
            time.sleep(scaled_ms / 1000.0)
        self._llm_slept_ms += scaled_ms
        self._llm_calls_slept += call_count
        return scaled_ms

    def _sleep_llm_postlude(self) -> None:
        delay_ms, call_count = self._llm_postlude
        scaled_ms = delay_ms * self.llm_latency_scale
        if scaled_ms > 0:
            time.sleep(scaled_ms / 1000.0)
        self._llm_slept_ms += scaled_ms
        self._llm_calls_slept += call_count

    def _record_merge_attempt(
        self,
        event: WorkloadEvent,
        preview_key: tuple[str, str],
        *,
        attempt: int,
        merge_elapsed_ns: int,
        status: str,
        error: BaseException | None = None,
        preview_elapsed_ns: int = 0,
        backoff_ms: float = 0.0,
    ) -> None:
        self._merge_attempts.append(
            {
                "sequence": event.sequence,
                "source_branch": preview_key[0],
                "target_branch": preview_key[1],
                "attempt": int(attempt),
                "status": status,
                "merge_elapsed_ms": merge_elapsed_ns / 1_000_000,
                "preview_elapsed_ms": preview_elapsed_ns / 1_000_000,
                "backoff_ms": float(backoff_ms),
                "retryable_error": (
                    _retryable_merge_error(error) if error is not None else False
                ),
                "error": (
                    f"{type(error).__name__}: {error}"
                    if error is not None
                    else None
                ),
            }
        )

    def _retry_merge(
        self,
        event: WorkloadEvent,
        arguments: dict[str, Any],
        preview_key: tuple[str, str],
        first_error: Exception,
    ) -> Any:
        """Refresh a stale preview before retrying a reviewed merge.

        Concurrent replay is the one setting in which a captured preview can
        legitimately age between the preview and apply calls.  Refreshing the
        preview is a backend-neutral retry of the same merge interface; it is
        not an application-level epoch or conflict policy.
        """

        last_error: Exception = first_error
        source_branch, target_branch = preview_key
        attempt = 0
        while self.merge_retries == -1 or attempt < self.merge_retries:
            # A competing atomic publication may still be finishing its
            # metadata transaction.  Back off briefly before refreshing the
            # preview; retrying immediately only makes every contender race
            # the same reservation again.
            backoff_ms = 0.0
            if attempt:
                # Full jitter prevents a process swarm from refreshing and
                # reserving the same target at the same instant.  The cap is
                # five seconds so retries can wait for a short publication
                # transaction without turning a transient race into an
                # unbounded replay stall.
                cap_ms = min(1.0 * (2 ** min(attempt, 13)), 10_000.0)
                backoff_ms = _RETRY_RANDOM.uniform(0.0, cap_ms)
                time.sleep(backoff_ms / 1000.0)
            preview_started = time.perf_counter_ns()
            try:
                refreshed = dispatch_knowledge_tool(
                    self.service,
                    "knowledge_merge_preview",
                    {
                        "source_branch": source_branch,
                        "target_branch": target_branch,
                    },
                )
            except Exception as exc:
                self._record_merge_attempt(
                    event,
                    preview_key,
                    attempt=attempt + 1,
                    merge_elapsed_ns=0,
                    preview_elapsed_ns=time.perf_counter_ns() - preview_started,
                    backoff_ms=backoff_ms,
                    status="preview-error",
                    error=exc,
                )
                last_error = exc
                if not _retryable_merge_error(exc):
                    break
                attempt += 1
                continue
            preview_elapsed_ns = time.perf_counter_ns() - preview_started
            preview = _plain_dataclass(refreshed)
            if not isinstance(preview, Mapping):
                last_error = RolloutTraceError(
                    "merge retry returned a non-mapping preview"
                )
                attempt += 1
                continue
            self._merge_previews[preview_key] = preview
            if _is_prepared_merge_preview(refreshed):
                self._prepared_merge_previews[preview_key] = refreshed
            else:
                self._prepared_merge_previews.pop(preview_key, None)
            token = preview.get("preview_token")
            if token is not None:
                self._preview_tokens[preview_key] = str(token)
                arguments["preview_token"] = str(token)
            selection_groups = event.expected.get("merge_selection_groups")
            if selection_groups is not None:
                arguments["selected_change_ids"] = _resolve_merge_selection(
                    preview,
                    selection_groups,
                    event.expected.get("merge_selection_partial_groups"),
                    invalid_count=(
                        len(event.expected.get("merge_selection_residual_ids") or [])
                        if _expected_replay_status(event) == "error"
                        else 0
                    ),
                )
            conflict_choices = arguments.get("conflict_choices")
            if isinstance(conflict_choices, Mapping):
                actual_conflicts = _merge_conflict_ids(preview)
                if actual_conflicts:
                    values = [str(value) for value in conflict_choices.values()]
                    if values and len(set(values)) == 1:
                        arguments["conflict_choices"] = {
                            conflict_id: values[0]
                            for conflict_id in actual_conflicts
                        }
            try:
                merge_started = time.perf_counter_ns()
                value = dispatch_knowledge_tool(
                    self.service,
                    event.name,
                    arguments,
                    prepared_preview=self._prepared_merge_previews.get(preview_key),
                )
            except Exception as exc:
                self._record_merge_attempt(
                    event,
                    preview_key,
                    attempt=attempt + 1,
                    merge_elapsed_ns=time.perf_counter_ns() - merge_started,
                    preview_elapsed_ns=preview_elapsed_ns,
                    backoff_ms=backoff_ms,
                    status="error",
                    error=exc,
                )
                last_error = exc
                if not _retryable_merge_error(exc):
                    break
            else:
                self._record_merge_attempt(
                    event,
                    preview_key,
                    attempt=attempt + 1,
                    merge_elapsed_ns=time.perf_counter_ns() - merge_started,
                    preview_elapsed_ns=preview_elapsed_ns,
                    backoff_ms=backoff_ms,
                    status="ok",
                )
                return value
            attempt += 1
        raise last_error

    def _run_shell(self, arguments: Mapping[str, Any]) -> tuple[str, int]:
        command = self._rewrite_replay_tool_paths(str(arguments["cmd"]))
        raw_workdir = str(arguments.get("workdir") or "")
        workdir_path = Path(raw_workdir).expanduser()
        if not workdir_path.is_absolute():
            candidate = self.repo_dir / workdir_path
            # ``root`` is a capture-side label for the session repository,
            # rather than a child directory named ``root``.
            if raw_workdir == "root" and not candidate.exists():
                candidate = self.repo_dir
            workdir_path = candidate
        workdir = workdir_path.resolve()
        if not workdir.exists():
            raise RolloutTraceError(f"shell workdir does not exist: {workdir}")
        command = _rewrite_workspace_relative_paths(workdir, command)
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

    def _rewrite_literal_tmp_paths(self, value: str) -> str:
        """Map captured absolute temporary paths into this replay's tmpdir.

        Leave paths below ``repo_dir`` untouched: tests and trace tooling may
        deliberately use a host-side marker there.  Only external ``/tmp``
        paths are replay scratch state and need per-replay isolation.
        """

        if self._trace_tmpdir is None:
            return value
        pattern = re.compile(
            r"(?<![A-Za-z0-9_])(/tmp/[A-Za-z0-9._+%=-]+(?:/[A-Za-z0-9._+%=@-]+)*)"
        )

        def replace(match: re.Match[str]) -> str:
            original = match.group(1)
            try:
                Path(original).resolve().relative_to(self.repo_dir)
            except ValueError:
                return self._trace_tmpdir + original.removeprefix("/tmp")
            return original

        return pattern.sub(replace, value)

    def _logicalize_workspace_paths(self, value: Any) -> Any:
        """Convert expanded checkout paths back to workspace-relative paths.

        MCP file APIs address logical workspace paths (for example
        ``/code/vllm/...``), whereas shell commands need the absolute FUSE
        mount path.  Keep the latter unchanged and strip only MCP argument
        prefixes after all replay path resolution has completed.
        """

        if isinstance(value, str):
            for workspace_path in self.workspace_paths.values():
                prefix = str(workspace_path).rstrip("/")
                value = _rewrite_workspace_relative_paths(
                    Path(workspace_path),
                    value,
                    rewrite_relative=False,
                )
                if value == prefix:
                    return "/"
                if value.startswith(prefix + "/"):
                    return value[len(prefix) :]
            return value
        if isinstance(value, Mapping):
            return {
                str(key): self._logicalize_workspace_paths(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self._logicalize_workspace_paths(item) for item in value]
        return value

    def _rewrite_replay_tool_paths(self, command: str) -> str:
        """Resolve explicit ``.venv/bin`` tools in the replay environment.

        A captured workspace may omit its transient virtual environment even
        though the command used tools from it.  Reuse the benchmark runner's
        pinned environment without creating files in the workspace.  Commands
        for tools that are unavailable remain unchanged and report their
        original failure status.
        """

        tool_dirs: list[Path] = []
        virtual_env = os.environ.get("VIRTUAL_ENV")
        if virtual_env:
            tool_dirs.append(Path(virtual_env) / "bin")
        for root in (Path.cwd(), Path(__file__).resolve().parent, *Path.cwd().parents):
            candidate = root / ".venv" / "bin"
            if candidate not in tool_dirs:
                tool_dirs.append(candidate)
        # Match the complete path, including an absolute capture-time
        # prefix.  Matching only the ``.venv/bin`` suffix would leave that
        # prefix in place and turn ``/capture/.venv/bin/python`` into
        # ``/capture/<host>/.venv/bin/python``.
        pattern = re.compile(
            r"(?P<absolute>(?<![A-Za-z0-9_.-])/(?:[A-Za-z0-9._~+@%-]+/)*"
            r"\.venv/bin/(?P<absolute_tool>[A-Za-z0-9._+-]+))"
            r"|(?P<relative>(?<![A-Za-z0-9_.-])(?:\./)?\.venv/bin/"
            r"(?P<relative_tool>[A-Za-z0-9._+-]+))"
        )

        def replace(match: re.Match[str]) -> str:
            tool = match.group("absolute_tool") or match.group("relative_tool")
            for tool_dir in tool_dirs:
                candidate = tool_dir / tool
                if candidate.is_file() and os.access(candidate, os.X_OK):
                    return str(candidate)
            return match.group(0)

        command = pattern.sub(replace, command)

        # Package-manager commands are often captured from ``~/.local/bin``
        # while replay intentionally excludes that mutable user directory.
        # Resolve them to the same pinned benchmark environment when present;
        # this does not install anything or change the captured command's
        # arguments.  Handle ``uvx ruff`` directly so replay never invokes a
        # network-backed ephemeral environment.
        def pinned_tool(name: str) -> str | None:
            for tool_dir in tool_dirs:
                candidate = tool_dir / name
                if candidate.is_file() and os.access(candidate, os.X_OK):
                    return str(candidate)
            return None

        ruff = pinned_tool("ruff")
        if ruff is not None:
            command = re.sub(
                r"(?<![A-Za-z0-9_./-])uvx\s+ruff(?=\s|$)",
                ruff,
                command,
            )
        for name in ("uvx", "uv"):
            resolved = pinned_tool(name)
            if resolved is not None:
                command = re.sub(
                    rf"(?<![A-Za-z0-9_./-]){name}(?=\s|$)",
                    resolved,
                    command,
                )
        return command

    def _expand(
        self,
        value: Any,
        *,
        collapse_workspace_tokens: bool = True,
    ) -> Any:
        if isinstance(value, str):
            value = value.replace("{{repo}}", str(self.repo_dir))
            # Codex can concatenate the same workspace token when it builds a
            # path from two shell fragments.  The capture normalizer keeps
            # those fragments portable, but expanding both would duplicate
            # the checkout path and make an otherwise valid replay fail.
            if collapse_workspace_tokens:
                value = re.sub(
                    r"(\{\{workspace:[^}]+\}\})(?:\1)+",
                    r"\1",
                    value,
                )
            # Codex sometimes records a literal absolute /tmp path instead of
            # the explicit ``{{tmp}}`` placeholder.  Keep external replay
            # scratch isolated while preserving paths under the configured
            # repository and workspaces.
            value = self._rewrite_literal_tmp_paths(value)
            if "{{tmp}}" in value:
                if self._trace_tmpdir is None:
                    raise RolloutTraceError(
                        "temporary path placeholder used outside trace replay"
                    )
                value = value.replace("{{tmp}}", self._trace_tmpdir)

            def workspace(match: re.Match[str]) -> str:
                branch_id = match.group(1)
                try:
                    path = self.workspace_paths[branch_id]
                except KeyError as exc:
                    raise RolloutTraceError(
                        f"workspace placeholder used before checkout: {branch_id}"
                    ) from exc
                return path

            return _WORKSPACE_TOKEN.sub(workspace, value)
        if isinstance(value, Mapping):
            return {
                str(key): self._expand(
                    item,
                    collapse_workspace_tokens=collapse_workspace_tokens,
                )
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [
                self._expand(
                    item,
                    collapse_workspace_tokens=collapse_workspace_tokens,
                )
                for item in value
            ]
        return value


_WORKSPACE_RELATIVE_PATH = re.compile(
    r"(?<![A-Za-z0-9_./-])"
    r"(?P<path>[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.@+-]+)+)"
)


def _rewrite_workspace_relative_paths(
    workspace_path: Path,
    value: str,
    *,
    rewrite_relative: bool = True,
) -> str:
    """Map captured repository-root paths to the stored ``/code`` tree.

    Captures made from a source worktree use paths such as ``vllm/...`` and
    ``tests/...``.  The enterprise workspace stores the same files below
    ``/code/<repository>``.  Resolve only paths that are absent at the replay
    root and have exactly one existing match under ``code``; all other command
    text remains byte-for-byte unchanged.
    """

    code_root = workspace_path / "code"
    # Commands already running from a repository checkout (for example
    # ``.../code/litellm``) are relative to that repository, not the
    # enterprise workspace root.  They must not be prefixed with another
    # ``code/<repository>`` component.
    if not code_root.is_dir():
        return value

    def rewrite(match: re.Match[str]) -> str:
        relative = match.group("path")
        root_candidate = workspace_path / relative
        if root_candidate.exists():
            return relative
        direct = code_root / relative
        if direct.exists():
            return f"code/{relative}"
        matches: list[Path] = []
        try:
            for repository in code_root.iterdir():
                candidate = repository / relative
                if candidate.exists():
                    matches.append(candidate)
                    if len(matches) > 1:
                        break
        except OSError:
            return relative
        if len(matches) != 1:
            return relative
        return f"code/{matches[0].relative_to(code_root).as_posix()}"

    if rewrite_relative:
        value = _WORKSPACE_RELATIVE_PATH.sub(rewrite, value)
    # MCP paths are absolute after placeholder expansion.  Apply the same
    # unique-match rule to the suffix following a workspace mount.
    prefix = str(workspace_path).rstrip("/") + "/"
    if prefix in value:
        suffix_pattern = re.compile(
            re.escape(prefix)
            + r"(?P<suffix>[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.@+-]+)+)"
        )

        def rewrite_absolute(match: re.Match[str]) -> str:
            suffix = match.group("suffix")
            candidate = workspace_path / suffix
            if candidate.exists():
                return match.group(0)
            rewritten = rewrite(
                re.match(
                    r"(?P<path>.*)",
                    suffix,
                )
            )
            if rewritten == suffix:
                return match.group(0)
            return prefix + rewritten

        value = suffix_pattern.sub(rewrite_absolute, value)
    return value


def dispatch_knowledge_tool(
    service: KnowledgeService,
    name: str,
    arguments: Mapping[str, Any],
    *,
    prepared_preview: Any | None = None,
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
    if name == "knowledge_merge_preview":
        return service.merge_preview(
            str(args["source_branch"]),
            str(args["target_branch"]),
        )
    if name == "knowledge_merge":
        merge_arguments: dict[str, Any] = {
            "selected_change_ids": (
                [str(value) for value in args["selected_change_ids"]]
                if args.get("selected_change_ids") is not None
                else None
            ),
            "preview_token": (
                str(args["preview_token"])
                if args.get("preview_token") is not None
                else None
            ),
            "conflict_choices": (
                {
                    str(key): str(value)
                    for key, value in args["conflict_choices"].items()
                }
                if args.get("conflict_choices") is not None
                else None
            ),
            "operation_id": (
                str(args["operation_id"])
                if args.get("operation_id") is not None
                else None
            ),
        }
        if prepared_preview is not None:
            merge_arguments["prepared_preview"] = prepared_preview
        return service.merge(
            str(args["source_branch"]),
            str(args["target_branch"]),
            **merge_arguments,
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


def _merge_branch_pair(arguments: Mapping[str, Any]) -> tuple[str, str] | None:
    source = arguments.get("source_branch")
    target = arguments.get("target_branch")
    if source is None or target is None:
        return None
    return str(source), str(target)


def _retryable_merge_error(error: BaseException) -> bool:
    """Recognize transient preview/publication races without hiding conflicts."""

    # Some backends expose the race in the exception type while keeping the
    # message deliberately short (for example,
    # ``StaleAtomicMergePreviewError("source or target changed while
    # reserving the merge")``).  Include both forms so the retry policy does
    # not depend on a particular backend's wording.
    text = f"{type(error).__name__}: {error}".casefold()
    return any(
        marker in text
        for marker in (
            "stale",
            "advanced",
            "branch has moved",
            "head changed",
            "changed after preview",
            "source or target changed",
            "target advanced",
            "preview token",
            "serialization",
            "database is locked",
            "temporarily unavailable",
            "in progress",
            # Chronos surfaces some transient publication races with
            # machine-readable underscore-delimited markers.  Treat these
            # spellings the same as the human-readable form above so an
            # unlimited retry policy can make progress under contention.
            "in_progress",
            "already_in_progress",
            "barrier_in_progress",
            "try again",
        )
    )


def _is_prepared_merge_preview(value: Any) -> bool:
    """Identify a backend-native preview object that can be reused by merge."""

    return hasattr(value, "atomic_preview") or hasattr(value, "plan")


def _merge_quiesce_context(
    service: KnowledgeService,
    preview_key: tuple[str, str],
) -> Any:
    """Keep backend state quiescent across one merge and its retries."""

    quiesce = getattr(getattr(service, "backend", None), "merge_quiesce", None)
    if callable(quiesce):
        return quiesce(*preview_key)
    return contextlib.nullcontext()


def _plain_dataclass(value: Any) -> Any:
    """Convert a backend preview dataclass into replay-local plain values."""

    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return _plain_dataclass(dataclasses.asdict(value))
    if isinstance(value, Mapping):
        return {
            str(key): _plain_dataclass(item) for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_plain_dataclass(item) for item in value]
    return value


def _merge_conflict_ids(preview: Mapping[str, Any]) -> list[str]:
    """Collect conflict identifiers from a backend-neutral merge preview."""

    result: list[str] = []

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            conflicts = value.get("conflicts")
            if isinstance(conflicts, Sequence) and not isinstance(
                conflicts, (str, bytes)
            ):
                for conflict in conflicts:
                    if not isinstance(conflict, Mapping):
                        continue
                    # Conflict resolution is keyed by the store's
                    # conflict_id; change_id identifies the selectable row
                    # and is not accepted by merge resolution.
                    identifier = conflict.get("conflict_id") or conflict.get(
                        "change_id"
                    )
                    if identifier is not None:
                        result.append(str(identifier))
            for key, item in value.items():
                if key != "conflicts":
                    visit(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                visit(item)

    visit(preview)
    return list(dict.fromkeys(result))


def _merge_selection_groups(
    preview: Mapping[str, Any],
    selected_change_ids: Any,
) -> tuple[list[dict[str, str]], list[str], list[dict[str, Any]]]:
    """Describe a captured allow-list by logical document and path groups."""

    selected = {str(value) for value in selected_change_ids}
    covered: set[str] = set()
    chosen: list[dict[str, str]] = []
    partial: list[dict[str, Any]] = []
    raw_groups = preview.get("selection_groups") or {}
    if not isinstance(raw_groups, Mapping):
        return [], sorted(selected), []
    for category in ("indexed_documents", "filesystem_paths"):
        category_groups = raw_groups.get(category) or {}
        if not isinstance(category_groups, Mapping):
            continue
        for key, values in sorted(category_groups.items()):
            group = {str(value) for value in values}
            selected_in_group = (group & selected) - covered
            if not selected_in_group:
                continue
            if group and group <= selected:
                chosen.append({"category": category, "key": str(key)})
                covered.update(group)
            elif group:
                partial.append(
                    {
                        "category": category,
                        "key": str(key),
                        "count": len(selected_in_group),
                    }
                )
                covered.update(selected_in_group)
    return chosen, sorted(selected - covered), partial


def _resolve_merge_selection(
    preview: Mapping[str, Any],
    selection_groups: Any,
    partial_groups: Any = None,
    *,
    invalid_count: int = 0,
) -> list[str]:
    """Resolve complete and intentionally partial groups against a fresh preview.

    Complete groups are the normal selective-merge representation.  Partial
    groups are retained only for captured calls that returned an error: taking
    a deterministic prefix of a fresh group reproduces the same dependency
    violation without depending on capture-time change IDs.  An unmapped
    failed selection is represented by an invalid sentinel so it remains a
    no-op while preserving the recorded error status.
    """

    selected = set(_resolve_merge_selection_groups(preview, selection_groups))
    raw_groups = preview.get("selection_groups") or {}
    if not isinstance(raw_groups, Mapping):
        raw_groups = {}
    for item in partial_groups or ():
        if not isinstance(item, Mapping):
            raise RolloutTraceError("invalid partial merge selection group")
        category = str(item.get("category") or "")
        key = str(item.get("key") or "")
        category_groups = raw_groups.get(category)
        group = (
            category_groups.get(key)
            if isinstance(category_groups, Mapping)
            else None
        )
        if not isinstance(group, Sequence) or isinstance(group, (str, bytes)):
            raise RolloutTraceError(
                "replay merge preview is missing partial selection group "
                f"{category}:{key}"
            )
        values = sorted(str(value) for value in group)
        count = int(item.get("count") or 0)
        if count <= 0 or count >= len(values):
            raise RolloutTraceError(
                f"invalid partial selection count for {category}:{key}"
            )
        selected.update(values[:count])
    if invalid_count:
        selected.update(
            f"chronos-replay-unmapped-selection-{index}"
            for index in range(invalid_count)
        )
    return sorted(selected)


def _resolve_merge_selection_groups(
    preview: Mapping[str, Any],
    selection_groups: Any,
) -> list[str]:
    """Resolve a logical captured selection against a fresh merge preview."""

    raw_groups = preview.get("selection_groups") or {}
    if not isinstance(raw_groups, Mapping):
        raise RolloutTraceError("replay merge preview has no selection groups")
    selected: set[str] = set()
    for item in selection_groups:
        if not isinstance(item, Mapping):
            raise RolloutTraceError("invalid merge selection group in trace")
        category = str(item.get("category") or "")
        key = str(item.get("key") or "")
        category_groups = raw_groups.get(category)
        if not isinstance(category_groups, Mapping) or key not in category_groups:
            raise RolloutTraceError(
                f"replay merge preview is missing selection group {category}:{key}"
            )
        selected.update(str(value) for value in category_groups[key])
    return sorted(selected)


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

    # Codex's current JSON event stream stores the MCP result directly and
    # spells the field ``structured_content``.  Older captures wrapped the
    # result in ``Ok`` and used the MCP SDK's ``structuredContent`` spelling.
    # Accept both encodings so trace normalization does not lose merge
    # selection groups or other structured data.
    structured_direct = value.get("structured_content")
    if structured_direct is not None:
        return structured_direct
    structured_camel = value.get("structuredContent")
    if structured_camel is not None:
        return structured_camel
    ok = value.get("Ok")
    if not isinstance(ok, Mapping):
        if "Err" in value:
            return value.get("Err")
        content = value.get("content")
        if not isinstance(content, list):
            return value
        # Direct MCP results in the current Codex stream use the same text
        # content blocks as the wrapped representation below.
        texts = [
            str(item["text"])
            for item in content
            if isinstance(item, Mapping) and item.get("type") == "text"
        ]
        if len(texts) != 1:
            return texts
        try:
            parsed = json.loads(texts[0])
        except json.JSONDecodeError:
            return texts[0]
        if isinstance(parsed, Mapping):
            return _mcp_structured_result(parsed)
        return parsed
    structured = ok.get("structuredContent")
    if structured is not None:
        return structured
    structured = ok.get("structured_content")
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
                parsed = json.loads(texts[0])
            except json.JSONDecodeError:
                return texts[0]
            # Some Codex MCP captures contain a second MCP result envelope
            # inside the text block. Decode that envelope as well so merge
            # previews retain their structured selection groups.
            if isinstance(parsed, Mapping):
                return _mcp_structured_result(parsed)
            return parsed
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


def _timestamp_seconds(value: Any) -> float | None:
    """Parse a persisted rollout timestamp for replay scheduling."""

    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError, OverflowError):
        return None


def _build_llm_replay_schedule(
    trace: WorkloadTrace,
) -> tuple[dict[int, tuple[float, int]], tuple[float, int], float]:
    """Map recorded model-response delays onto replayed tool-event groups.

    One model response can issue several tool calls, such as a ``Promise.all``
    batch.  The response latency is therefore charged once, before the first
    event emitted by that response, rather than once per nested tool call. Any
    model calls that do not contain replayable state operations remain in the
    preceding/following delay so the replay preserves the recorded turn time.
    """

    raw_timing = trace.metadata.get("llm_timing")
    if not isinstance(raw_timing, Mapping):
        return {}, (0.0, 0), 0.0
    raw_calls = raw_timing.get("calls")
    if not isinstance(raw_calls, Sequence) or isinstance(raw_calls, (str, bytes)):
        return {}, (0.0, 0), float(raw_timing.get("total_ms") or 0.0)

    calls: list[tuple[float, float | None, float | None]] = []
    for raw_call in raw_calls:
        if not isinstance(raw_call, Mapping):
            continue
        try:
            latency_ms = max(0.0, float(raw_call.get("latency_ms") or 0.0))
        except (TypeError, ValueError):
            latency_ms = 0.0
        calls.append(
            (
                latency_ms,
                _timestamp_seconds(raw_call.get("started_at")),
                _timestamp_seconds(raw_call.get("completed_at")),
            )
        )
    if not calls:
        try:
            recorded_ms = float(raw_timing.get("total_ms") or 0.0)
        except (TypeError, ValueError):
            recorded_ms = 0.0
        return {}, (max(0.0, recorded_ms), 1 if recorded_ms > 0 else 0), recorded_ms

    events = list(trace.events)
    if not events:
        recorded_ms = sum(item[0] for item in calls)
        return {}, (recorded_ms, len(calls)), recorded_ms

    event_times = [_timestamp_seconds(event.timestamp) for event in events]
    assignments: dict[int, int] = {}
    tolerance_seconds = 1.0
    for event, event_time in zip(events, event_times):
        if event_time is None:
            continue
        candidates: list[int] = []
        for index, (_latency, _started, completed) in enumerate(calls):
            if completed is None or event_time < completed - tolerance_seconds:
                continue
            next_started = (
                calls[index + 1][1] if index + 1 < len(calls) else None
            )
            if next_started is None or event_time <= next_started + tolerance_seconds:
                candidates.append(index)
        if candidates:
            assignments[event.sequence] = candidates[-1]

    # Some synthetic traces omit timestamps. In that case, retaining all model
    # delay as a prelude is conservative and avoids silently dropping it.
    if not assignments:
        recorded_ms = sum(item[0] for item in calls)
        return {
            events[0].sequence: (recorded_ms, len(calls))
        }, (0.0, 0), recorded_ms

    first_event_for_call: dict[int, int] = {}
    for event in events:
        call_index = assignments.get(event.sequence)
        if call_index is not None:
            first_event_for_call.setdefault(call_index, event.sequence)

    before_event: dict[int, tuple[float, int]] = {}
    assigned_calls = sorted(first_event_for_call)
    first_call = assigned_calls[0]
    first_sequence = first_event_for_call[first_call]
    before_event[first_sequence] = (
        sum(item[0] for item in calls[: first_call + 1]),
        first_call + 1,
    )
    previous_call = first_call
    for call_index in assigned_calls[1:]:
        sequence = first_event_for_call[call_index]
        delay_ms, call_count = before_event.get(sequence, (0.0, 0))
        delay_ms += sum(item[0] for item in calls[previous_call + 1 : call_index + 1])
        call_count += call_index - previous_call
        before_event[sequence] = (delay_ms, call_count)
        previous_call = call_index

    postlude = (
        sum(item[0] for item in calls[previous_call + 1 :]),
        len(calls) - previous_call - 1,
    )
    recorded_ms = sum(item[0] for item in calls)
    return before_event, postlude, recorded_ms


def _extract_llm_timing(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Measure model-response intervals from persisted Codex timestamps.

    A Codex model request begins after the user message or the preceding tool
    result. It ends when the model emits the next tool call or its final
    answer. Codex writes one ``token_count`` record after each such response;
    that record supplies the per-request usage and starts the next request
    after any intervening tool execution. This excludes MCP, shell, and patch
    execution from the reported model latency.
    """

    request_start = ""
    response_end = ""
    response_kind = ""
    calls: list[dict[str, Any]] = []
    task: Mapping[str, Any] = {}

    for row in records:
        timestamp = str(row.get("timestamp") or "")
        payload = row.get("payload")
        if not isinstance(payload, Mapping):
            payload = {}
        row_type = row.get("type")
        payload_type = payload.get("type")

        if row_type == "event_msg" and payload_type == "user_message":
            request_start = timestamp
            response_end = ""
            response_kind = ""
            continue

        if row_type == "response_item" and payload_type in {
            "function_call",
            "custom_tool_call",
        }:
            response_end = timestamp
            response_kind = "tool_call"
            continue

        if row_type == "event_msg" and payload_type == "agent_message":
            response_end = timestamp
            response_kind = str(payload.get("phase") or "message")
            continue

        if row_type == "event_msg" and payload_type == "token_count":
            usage_info = payload.get("info")
            if not isinstance(usage_info, Mapping):
                usage_info = {}
            usage = usage_info.get("last_token_usage")
            if not isinstance(usage, Mapping):
                usage = {}
            if request_start and response_end:
                latency_ms = round(
                    _elapsed_seconds(request_start, response_end) * 1000,
                    3,
                )
                calls.append(
                    {
                        "sequence": len(calls),
                        "started_at": request_start,
                        "completed_at": response_end,
                        "latency_ms": latency_ms,
                        "response_kind": response_kind,
                        "usage": {
                            str(key): int(value)
                            for key, value in usage.items()
                            if isinstance(value, int)
                        },
                    }
                )
            # Tool results precede this record in the rollout, so the model
            # can issue its next request only after this timestamp.
            request_start = timestamp
            response_end = ""
            response_kind = ""
            continue

        if row_type == "event_msg" and payload_type == "task_complete":
            task = payload

    latencies_us = [
        int(round(float(call["latency_ms"]) * 1000)) for call in calls
    ]
    total_ms = round(sum(latencies_us) / 1000, 3)
    turn_duration_ms = int(task.get("duration_ms") or 0)
    result: dict[str, Any] = {
        "measurement": (
            "wall-clock time from user/tool result to the next model-emitted "
            "tool call or final response; tool execution excluded"
        ),
        "call_count": len(calls),
        "total_ms": total_ms,
        "p50_ms": round(_percentile(latencies_us, 0.50) / 1000, 3),
        "p95_ms": round(_percentile(latencies_us, 0.95) / 1000, 3),
        "max_ms": round(max(latencies_us, default=0) / 1000, 3),
        "time_to_first_token_ms": int(task.get("time_to_first_token_ms") or 0),
        "turn_duration_ms": turn_duration_ms,
        "non_llm_ms": max(0, round(turn_duration_ms - total_ms, 3)),
        "calls": calls,
    }
    return result


def _custom_tool_exit_code(output: str) -> int | None:
    match = re.search(r"(?:Exit code|Process exited with code):\s*(\d+)", output)
    return int(match.group(1)) if match else None


def _custom_tool_output_texts(payload: Mapping[str, Any]) -> list[str]:
    """Return result blocks emitted by ``text(r)`` inside an exec wrapper."""

    output = payload.get("output")
    if isinstance(output, list):
        values: list[str] = []
        for item in output:
            if not isinstance(item, Mapping) or item.get("type") != "input_text":
                continue
            text = item.get("text")
            if text is not None:
                text = str(text)
                # The wrapper emits a bookkeeping block before the nested
                # tool results.  It is not a command result and must not make
                # a successful nested shell call look like a failure.
                if not values and re.match(r"^Script (?:completed|failed)\n", text):
                    continue
                values.append(text)
        return values
    if isinstance(output, str):
        return [output]
    return []


def _replace_paths(
    value: Any,
    workspace_paths: Mapping[str, str],
    source_cwd: str,
) -> Any:
    """Replace capture-local workspace paths with portable placeholders."""

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
