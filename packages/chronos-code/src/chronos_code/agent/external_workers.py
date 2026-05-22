"""External worker adapters for Codex/Claude runtimes.

Launches unmanaged worker CLIs with a worker-local MCP config that points
to Chronos MCP server with an enforced session id.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import re
import shlex
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Literal

from chronos_code.config import Config


WorkerRuntime = Literal["codex", "claude"]
EventSink = Callable[[str, str], None] | Callable[..., None]
LineSink = Callable[[str], None]


@dataclass
class CommandResult:
    """Captured command execution output."""

    returncode: int
    stdout: str
    stderr: str


CommandRunner = Callable[..., Awaitable[CommandResult]]


class ExternalWorkerAdapter:
    """Adapter layer for launching external worker agents."""

    _DEFAULT_MCP_TOOLS = ["chronos_txn", "chronos_file_editor", "chronos_bash", "chronos_sqlite"]

    def __init__(
        self,
        config: Config,
        root_dir: Path,
        *,
        event_sink: EventSink | None = None,
        command_runner: CommandRunner | None = None,
    ) -> None:
        self._config = config
        self._root_dir = Path(root_dir).resolve()
        self._event_sink = event_sink
        self._run_cmd = command_runner or self._run_command

    @property
    def enabled(self) -> bool:
        return self._config.worker_runtime in ("codex", "claude")

    @property
    def runtime(self) -> str:
        return self._config.worker_runtime

    async def run_worker(
        self,
        *,
        runtime: WorkerRuntime,
        agent_type: str,
        objective: str,
        task_contract: dict[str, Any],
        branch_session_id: str,
        working_dir: Path,
        txn_id: str,
        mcp_url: str | None = None,
    ) -> str:
        """Launch one unmanaged worker process and return its summary text."""
        run_id = uuid.uuid4().hex[:10]
        run_dir = self._root_dir / ".chronos-code" / "external-workers" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        mcp_config_path = run_dir / ".mcp.json"
        prompt_path = run_dir / "prompt.md"
        stdout_path = run_dir / "stdout.log"
        stderr_path = run_dir / "stderr.log"

        mcp_payload = self._build_mcp_config(
            working_dir=working_dir,
            branch_session_id=branch_session_id,
            mcp_url=mcp_url,
        )
        mcp_config_path.write_text(
            json.dumps(mcp_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        prompt_text = self._build_prompt(
            runtime=runtime,
            agent_type=agent_type,
            objective=objective,
            task_contract=task_contract,
            branch_session_id=branch_session_id,
            mcp_url=mcp_url,
        )
        prompt_path.write_text(prompt_text, encoding="utf-8")

        command = self._build_runtime_command(
            runtime=runtime,
            working_dir=working_dir,
            mcp_config_path=mcp_config_path,
            prompt_text=prompt_text,
            mcp_url=mcp_url,
        )
        self._emit_event(
            "external_worker_start",
            "Launching external worker process.",
            runtime=runtime,
            txn_id=txn_id,
            agent_type=agent_type,
            session_id=branch_session_id,
            command=self._clip(" ".join(command), max_chars=220),
            artifacts=str(run_dir),
        )

        emit_mcp_tool_events = not bool(mcp_url)
        on_stdout_line: LineSink = lambda line: self._handle_worker_stdout_line(
            runtime=runtime,
            line=line,
            txn_id=txn_id,
            agent_type=agent_type,
            session_id=branch_session_id,
            emit_mcp_tool_events=emit_mcp_tool_events,
        )
        on_stderr_line: LineSink = lambda line: self._handle_worker_stderr_line(
            runtime=runtime,
            line=line,
            txn_id=txn_id,
            agent_type=agent_type,
            session_id=branch_session_id,
        )
        timeout_sec = int(self._config.external_worker_timeout_sec)
        supports_line_hooks = self._runner_supports_line_hooks(self._run_cmd)
        if supports_line_hooks:
            result = await self._run_cmd(
                command,
                run_dir,
                timeout_sec,
                on_stdout_line=on_stdout_line,
                on_stderr_line=on_stderr_line,
            )
        else:
            result = await self._run_cmd(command, run_dir, timeout_sec)
            for raw in result.stdout.splitlines():
                on_stdout_line(raw)
            for raw in result.stderr.splitlines():
                on_stderr_line(raw)

        stdout_path.write_text(result.stdout, encoding="utf-8")
        stderr_path.write_text(result.stderr, encoding="utf-8")

        if result.returncode != 0:
            raise RuntimeError(
                f"External worker failed (runtime={runtime}, rc={result.returncode}). "
                f"stderr={self._clip(result.stderr, max_chars=220)}"
            )

        summary = self._extract_summary(runtime=runtime, stdout=result.stdout)
        if not summary:
            summary = "(external worker finished with no stdout)"
        self._emit_event(
            "external_worker_done",
            "External worker process finished.",
            runtime=runtime,
            txn_id=txn_id,
            agent_type=agent_type,
            session_id=branch_session_id,
            summary=self._clip(summary, max_chars=160),
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
        )
        return summary

    def build_branch_session_id(self, *, txn_id: str, agent_type: str) -> str:
        """Deterministically map transaction branch to MCP session id."""
        base = f"{txn_id}_{agent_type}"
        sanitized = re.sub(r"[^a-zA-Z0-9_.-]+", "-", base).strip("-")
        if not sanitized:
            sanitized = "worker"
        return f"branch-{sanitized}"[:96]

    def _build_mcp_config(
        self,
        *,
        working_dir: Path,
        branch_session_id: str,
        mcp_url: str | None,
    ) -> dict[str, Any]:
        if mcp_url:
            return {
                "mcpServers": {
                    self._config.external_worker_mcp_server_name: {
                        "url": mcp_url
                    }
                }
            }
        args = [
            "--project",
            str(working_dir),
            "--transport",
            "stdio",
            "--enforced-session-id",
            branch_session_id,
        ]
        if self._config.weak_snapshot:
            args.append("--weak-snapshot")
        return {
            "mcpServers": {
                self._config.external_worker_mcp_server_name: {
                    "command": self._config.external_worker_mcp_command,
                    "args": args,
                }
            }
        }

    def _build_prompt(
        self,
        *,
        runtime: WorkerRuntime,
        agent_type: str,
        objective: str,
        task_contract: dict[str, Any],
        branch_session_id: str,
        mcp_url: str | None = None,
    ) -> str:
        allowed = task_contract.get("allowed_tools")
        if not isinstance(allowed, list) or not allowed:
            allowed = list(self._DEFAULT_MCP_TOOLS)
        contract = dict(task_contract)
        contract["allowed_tools"] = allowed
        contract["session_id"] = branch_session_id
        contract_json = json.dumps(contract, indent=2, sort_keys=True)
        server_name = self._config.external_worker_mcp_server_name
        txn_rules = (
            "- Transaction boundaries are managed by orchestrator.\n"
            "- Do not call chronos_txn commit/abort/savepoint/rollback.\n"
            "- You may call chronos_txn status/changes for inspection.\n"
            if mcp_url
            else
            "- Begin with chronos_txn action=begin.\n"
            "- Execute the contract, verify results, then chronos_txn changes/status.\n"
            "- If checks fail, chronos_txn abort. If checks pass, chronos_txn commit.\n"
        )
        return (
            f"You are a {runtime} worker ({agent_type}) launched by Chronos-code.\n\n"
            "Rules:\n"
            f"- Use MCP server '{server_name}' only for Chronos transactional work.\n"
            f"- The required Chronos session_id is '{branch_session_id}'.\n"
            "- This session_id is enforced by the Chronos MCP server.\n"
            f"{txn_rules}"
            "- Return concise plain-text summary only.\n\n"
            f"Objective:\n{objective}\n\n"
            "Task contract JSON:\n"
            f"{contract_json}\n"
        )

    def _build_runtime_command(
        self,
        *,
        runtime: WorkerRuntime,
        working_dir: Path,
        mcp_config_path: Path,
        prompt_text: str,
        mcp_url: str | None,
    ) -> list[str]:
        if runtime == "codex":
            return self._build_codex_command(
                working_dir=working_dir,
                prompt_text=prompt_text,
                mcp_url=mcp_url,
            )
        return self._build_claude_command(
            working_dir=working_dir,
            mcp_config_path=mcp_config_path,
            prompt_text=prompt_text,
        )

    def _build_codex_command(
        self,
        *,
        working_dir: Path,
        prompt_text: str,
        mcp_url: str | None,
    ) -> list[str]:
        command = self._config.codex_command
        if mcp_url:
            return [
                command,
                "exec",
                "-C",
                str(working_dir),
                "--skip-git-repo-check",
                "--json",
                "-c",
                (
                    f'mcp_servers.{self._config.external_worker_mcp_server_name}.url='
                    f'"{mcp_url}"'
                ),
                prompt_text,
            ]
        template = self._config.codex_command_template
        rendered = template.format(
            command=command,
            cwd=str(working_dir),
            mcp_config="",
            prompt_file="",
            prompt=prompt_text,
        )
        tokens = shlex.split(rendered)
        if not tokens:
            raise RuntimeError("Codex command template resolved to empty command.")
        return tokens

    def _build_claude_command(
        self,
        *,
        working_dir: Path,
        mcp_config_path: Path,
        prompt_text: str,
    ) -> list[str]:
        command = self._config.claude_command
        template = self._config.claude_command_template
        rendered = template.format(
            command=command,
            cwd=str(working_dir),
            mcp_config=str(mcp_config_path),
            prompt_file="",
            prompt=prompt_text,
        )
        tokens = shlex.split(rendered)
        if not tokens:
            raise RuntimeError("Claude command template resolved to empty command.")
        return tokens

    @staticmethod
    def _extract_summary(runtime: WorkerRuntime, stdout: str) -> str:
        text = (stdout or "").strip()
        if runtime != "codex" or not text:
            return text
        last_agent_text = ""
        for line in text.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            if item.get("type") != "item.completed":
                continue
            payload = item.get("item") or {}
            if payload.get("type") == "agent_message":
                candidate = str(payload.get("text", "")).strip()
                if candidate:
                    last_agent_text = candidate
        return last_agent_text or text

    async def _run_command(
        self,
        command: list[str],
        cwd: Path,
        timeout_sec: int,
        on_stdout_line: LineSink | None = None,
        on_stderr_line: LineSink | None = None,
    ) -> CommandResult:
        proc = await asyncio.create_subprocess_exec(
            *command,
            cwd=str(cwd),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        async def _drain_stream(
            stream: asyncio.StreamReader | None,
            sink: list[str],
            line_sink: LineSink | None,
        ) -> None:
            if stream is None:
                return
            while True:
                chunk = await stream.readline()
                if not chunk:
                    return
                decoded = chunk.decode("utf-8", errors="replace")
                sink.append(decoded)
                if line_sink is None:
                    continue
                line = decoded.rstrip("\r\n")
                if not line:
                    continue
                try:
                    line_sink(line)
                except Exception:
                    continue

        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []
        stdout_task = asyncio.create_task(
            _drain_stream(proc.stdout, stdout_chunks, on_stdout_line)
        )
        stderr_task = asyncio.create_task(
            _drain_stream(proc.stderr, stderr_chunks, on_stderr_line)
        )
        wait_task = asyncio.create_task(proc.wait())
        try:
            await asyncio.wait_for(
                asyncio.gather(stdout_task, stderr_task, wait_task),
                timeout=timeout_sec,
            )
        except asyncio.TimeoutError as e:
            proc.kill()
            await proc.wait()
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            raise RuntimeError(
                f"External worker command timed out after {timeout_sec}s: {' '.join(command)}"
            ) from e
        stdout = "".join(stdout_chunks)
        stderr = "".join(stderr_chunks)
        return CommandResult(
            returncode=int(proc.returncode or 0),
            stdout=stdout,
            stderr=stderr,
        )

    @staticmethod
    def _runner_supports_line_hooks(runner: CommandRunner) -> bool:
        try:
            sig = inspect.signature(runner)
        except (TypeError, ValueError):
            return False
        params = sig.parameters
        if any(
            p.kind == inspect.Parameter.VAR_KEYWORD
            for p in params.values()
        ):
            return True
        return "on_stdout_line" in params and "on_stderr_line" in params

    def _handle_worker_stdout_line(
        self,
        *,
        runtime: WorkerRuntime,
        line: str,
        txn_id: str,
        agent_type: str,
        session_id: str,
        emit_mcp_tool_events: bool,
    ) -> None:
        text = (line or "").strip()
        if not text:
            return

        if runtime == "codex":
            payload = self._parse_json_line(text)
            if isinstance(payload, dict):
                self._emit_codex_stream_event(
                    payload=payload,
                    txn_id=txn_id,
                    agent_type=agent_type,
                    session_id=session_id,
                    emit_mcp_tool_events=emit_mcp_tool_events,
                )
                return

        if runtime == "claude":
            payload = self._parse_json_line(text)
            if isinstance(payload, dict):
                self._emit_claude_stream_event(
                    payload=payload,
                    txn_id=txn_id,
                    agent_type=agent_type,
                    session_id=session_id,
                    emit_mcp_tool_events=emit_mcp_tool_events,
                )
                return

        self._emit_event(
            "external_worker_output",
            "Worker output.",
            runtime=runtime,
            txn_id=txn_id,
            agent_type=agent_type,
            session_id=session_id,
            preview=self._clip(text, max_chars=220),
        )

    def _handle_worker_stderr_line(
        self,
        *,
        runtime: WorkerRuntime,
        line: str,
        txn_id: str,
        agent_type: str,
        session_id: str,
    ) -> None:
        text = (line or "").strip()
        if not text:
            return
        self._emit_event(
            "external_worker_stderr",
            "Worker stderr.",
            runtime=runtime,
            txn_id=txn_id,
            agent_type=agent_type,
            session_id=session_id,
            preview=self._clip(text, max_chars=220),
        )

    def _emit_codex_stream_event(
        self,
        *,
        payload: dict[str, Any],
        txn_id: str,
        agent_type: str,
        session_id: str,
        emit_mcp_tool_events: bool,
    ) -> None:
        etype = str(payload.get("type", "")).strip()
        if etype in {"item.started", "item.completed"}:
            item = payload.get("item")
            if not isinstance(item, dict):
                return
            item_type = str(item.get("type", "")).strip()
            if item_type == "agent_message" and etype == "item.completed":
                text = str(item.get("text", "")).strip()
                if not text:
                    return
                self._emit_event(
                    "external_worker_message",
                    "Worker update.",
                    runtime="codex",
                    txn_id=txn_id,
                    agent_type=agent_type,
                    session_id=session_id,
                    preview=self._clip(text, max_chars=220),
                )
                return

            if item_type == "mcp_tool_call" and emit_mcp_tool_events:
                tool_name = str(item.get("tool", "")).strip() or "mcp_tool"
                if etype == "item.started":
                    self._emit_event(
                        "external_worker_tool_call",
                        f"Worker calling `{tool_name}`.",
                        runtime="codex",
                        txn_id=txn_id,
                        agent_type=agent_type,
                        session_id=session_id,
                        tool=tool_name,
                        args=self._clip_json(item.get("arguments"), max_chars=180),
                    )
                    return
                error = item.get("error")
                if error:
                    self._emit_event(
                        "external_worker_tool_result",
                        f"`{tool_name}` failed.",
                        runtime="codex",
                        txn_id=txn_id,
                        agent_type=agent_type,
                        session_id=session_id,
                        tool=tool_name,
                        error=self._clip(str(error), max_chars=200),
                    )
                    return
                self._emit_event(
                    "external_worker_tool_result",
                    f"`{tool_name}` completed.",
                    runtime="codex",
                    txn_id=txn_id,
                    agent_type=agent_type,
                    session_id=session_id,
                    tool=tool_name,
                    result=self._clip(
                        self._extract_mcp_result_text(item.get("result")),
                        max_chars=200,
                    ),
                )
            return

        if etype == "turn.completed":
            usage = payload.get("usage")
            if not isinstance(usage, dict):
                return
            self._emit_event(
                "external_worker_usage",
                "Worker turn usage.",
                runtime="codex",
                txn_id=txn_id,
                agent_type=agent_type,
                session_id=session_id,
                input_tokens=int(usage.get("input_tokens", 0) or 0),
                output_tokens=int(usage.get("output_tokens", 0) or 0),
                cached_input_tokens=int(usage.get("cached_input_tokens", 0) or 0),
            )

    def _emit_claude_stream_event(
        self,
        *,
        payload: dict[str, Any],
        txn_id: str,
        agent_type: str,
        session_id: str,
        emit_mcp_tool_events: bool,
    ) -> None:
        etype = str(payload.get("type", "")).strip().lower()
        text = payload.get("text")
        if isinstance(text, str) and text.strip() and etype in {
            "assistant",
            "message",
            "result",
            "final",
        }:
            self._emit_event(
                "external_worker_message",
                "Worker update.",
                runtime="claude",
                txn_id=txn_id,
                agent_type=agent_type,
                session_id=session_id,
                preview=self._clip(text, max_chars=220),
            )
            return

        if emit_mcp_tool_events and etype in {"tool_use", "tool_call"}:
            tool_name = str(payload.get("name") or payload.get("tool") or "").strip()
            if not tool_name:
                tool_name = "tool"
            self._emit_event(
                "external_worker_tool_call",
                f"Worker calling `{tool_name}`.",
                runtime="claude",
                txn_id=txn_id,
                agent_type=agent_type,
                session_id=session_id,
                tool=tool_name,
                args=self._clip_json(
                    payload.get("input") or payload.get("arguments"),
                    max_chars=180,
                ),
            )

    @staticmethod
    def _parse_json_line(line: str) -> dict[str, Any] | None:
        if not line.startswith("{"):
            return None
        try:
            payload = json.loads(line)
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _extract_mcp_result_text(result: Any) -> str:
        if isinstance(result, str):
            return result
        if isinstance(result, dict):
            structured = result.get("structured_content")
            if isinstance(structured, dict):
                candidate = structured.get("result")
                if isinstance(candidate, str) and candidate.strip():
                    return candidate
            content = result.get("content")
            if isinstance(content, list):
                for item in content:
                    if not isinstance(item, dict):
                        continue
                    candidate = item.get("text")
                    if isinstance(candidate, str) and candidate.strip():
                        return candidate
        return json.dumps(result, ensure_ascii=True, default=str)

    @staticmethod
    def _clip_json(payload: Any, max_chars: int = 160) -> str:
        if payload is None:
            return "{}"
        try:
            rendered = json.dumps(payload, ensure_ascii=True, default=str)
        except Exception:
            rendered = str(payload)
        return ExternalWorkerAdapter._clip(rendered, max_chars=max_chars)

    def _emit_event(self, event_type: str, message: str, **details: Any) -> None:
        sink = self._event_sink
        if sink is None:
            return
        payload = {"type": event_type, "message": message, **details}
        try:
            sink(payload)
        except TypeError:
            sink(event_type, message, **details)
        except Exception:
            pass

    @staticmethod
    def _clip(text: str, max_chars: int = 160) -> str:
        value = (text or "").strip().replace("\n", " ")
        if len(value) <= max_chars:
            return value
        return value[: max_chars - 3] + "..."
