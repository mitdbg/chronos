"""CLI REPL — main entry point for Chronos-Code.

Provides an interactive loop: user input → orchestrator → display.
Handles slash commands, interrupts, and session management.

Each user turn is wrapped in a Chronos transaction:
  - begin() mounts the OverlayFS overlay
  - Agent tools operate on the isolated overlay
  - commit() merges changes to the real project (or abort() discards)
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
import shutil
import sys
import threading
import time
from itertools import count
from pathlib import Path
from typing import Any, Callable

import click

from chronos_code.config import Config
from chronos_code.slash_commands import SlashCommandHandler

logger = logging.getLogger(__name__)


def _build_banner() -> str:
    return (
        "╭──────────────────────────────────────────╮\n"
        "│           Chronos-Code v0.1.0                │\n"
        "│   Transactional CLI Coding Agent         │\n"
        "│   Type /help for commands, Ctrl+C to     │\n"
        "│   abort, Ctrl+D to exit.                 │\n"
        "╰──────────────────────────────────────────╯"
    )


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level, format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )


def _build_prompt_fn():
    """Build the interactive prompt function for AskUser tool."""
    def prompt_fn(question: str, options: list[dict[str, Any]] | None, allow_freeform: bool) -> str:
        print(f"\n🤖 Agent asks: {question}")
        if options:
            for i, opt in enumerate(options, 1):
                label = opt.get("label", opt.get("text", f"Option {i}"))
                print(f"  {i}. {label}")
        response = input("Your answer: ").strip()
        return response
    return prompt_fn


def _prepare_chronos_state_dir(working_dir: Path, fresh: bool) -> Path:
    """Ensure project-local Chronos state directory exists.

    When ``fresh`` is True, remove any existing state first.
    """
    chronos_state_dir = working_dir / ".chronos-code"
    if fresh and chronos_state_dir.exists():
        shutil.rmtree(chronos_state_dir)
    chronos_state_dir.mkdir(parents=True, exist_ok=True)
    return chronos_state_dir


def _build_txn_labeler() -> Callable[[str | None], str]:
    """Create stable transaction labels for display.

    - If a txn id already follows ``txnN`` or ``txnN_M``, keep it.
    - Otherwise map it to synthetic sequential ``txnN`` labels.
    """
    mapping: dict[str, str] = {}
    counter = count(1)
    canonical = re.compile(r"^txn\d+(?:_\d+)?$")

    def label(txn_id: str | None) -> str:
        if not txn_id:
            return "-"
        if canonical.match(txn_id):
            return txn_id
        existing = mapping.get(txn_id)
        if existing:
            return existing
        assigned = f"txn{next(counter)}"
        mapping[txn_id] = assigned
        return assigned

    return label


def _build_user_input_reader(slash: SlashCommandHandler):
    """Create async REPL input function with slash-command completion.

    Returns an *async* callable so it can be awaited inside the already-running
    asyncio event loop (prompt_toolkit's sync ``prompt()`` calls
    ``asyncio.run()`` internally which crashes with
    ``RuntimeError: asyncio.run() cannot be called from a running event loop``).
    """
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        async def read_user_input() -> str:
            # Non-interactive: just read from stdin via the event loop
            loop = asyncio.get_event_loop()
            line = await loop.run_in_executor(None, lambda: input("you> "))
            return line.strip()
        return read_user_input

    try:
        from prompt_toolkit import PromptSession
        from prompt_toolkit.completion import WordCompleter

        completer = WordCompleter(
            [f"/{name}" for name in slash.command_names],
            ignore_case=True,
            sentence=True,
        )
        _pt_session = PromptSession("you> ", completer=completer)

        async def read_user_input() -> str:
            return (await _pt_session.prompt_async()).strip()

        return read_user_input
    except Exception:
        async def read_user_input() -> str:
            loop = asyncio.get_event_loop()
            line = await loop.run_in_executor(None, lambda: input("you> "))
            return line.strip()
        return read_user_input


def _create_chronos_context(working_dir: Path, *, weak_snapshot: bool = False) -> Any:
    """Create a ChronosContext for the session.

    Chronos-Code is Chronos-only and requires a working ChronosContext.
    """
    try:
        from langchain_chronos import ChronosContext
        chronos_dir = working_dir / ".chronos-code"
        chronos_dir.mkdir(exist_ok=True)
        db_path = str(chronos_dir / "session.db")
        ctx = ChronosContext(
            str(working_dir),
            db_path=db_path,
            enable_sqlite=True,
            enable_vectorstore=False,
            weak_snapshot=weak_snapshot,
        )
        return ctx
    except Exception as e:
        raise click.ClickException(
            "Failed to initialize Chronos context. Chronos-Code requires Chronos-enabled "
            f"execution.\nUnderlying error: {e}\n"
            "Install and configure fuse-overlayfs (or run with equivalent "
            "OverlayFS support), then retry."
        )


def _build_logging_llm_fn(
    llm_fn: Callable,
    log_path: Path,
    get_session_id: Callable[[], str],
    model: str,
) -> Callable:
    """Wrap llm_fn to append one JSON line per LLM call."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    write_lock = threading.Lock()
    call_counter = count(1)

    async def logged_llm_fn(
        messages: list[dict[str, Any]],
        tool_schemas: list[dict[str, Any]],
    ) -> dict[str, Any]:
        call_id = next(call_counter)
        started = time.time()
        response: dict[str, Any] | None = None
        error: str | None = None

        try:
            result = llm_fn(messages, tool_schemas)
            if inspect.isawaitable(result):
                result = await result
            response = result if isinstance(result, dict) else {"raw": str(result)}
            return response
        except Exception as e:
            error = str(e)
            raise
        finally:
            record = {
                "ts": started,
                "duration_ms": int((time.time() - started) * 1000),
                "session_id": get_session_id(),
                "model": model,
                "llm_call_id": call_id,
                "request": {
                    "messages": messages,
                    "tool_schemas": tool_schemas,
                },
                "response": response,
                "usage": (response or {}).get("usage") if isinstance(response, dict) else None,
                "error": error,
            }
            with write_lock:
                with log_path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(record, ensure_ascii=False, default=str))
                    f.write("\n")

    return logged_llm_fn


async def _repl_loop(
    working_dir: Path,
    config: Config,
    session_id: str | None = None,
    show_events: bool = False,
    log_path: str | None = None,
    fresh: bool = False,
) -> None:
    """Run the interactive REPL loop."""
    from chronos_code.agent.orchestrator import Orchestrator
    from chronos_code.display.terminal_ui import TerminalUI
    from chronos_code.display.todo_display import TodoDisplay
    from chronos_code.debug_scenarios import DebugScenarioRunner
    from chronos_code.chronos_integration.session_manager import SessionManager
    from chronos_code.tools.memory_tool import MemoryTool
    from chronos_code.tools.registry import build_tools, build_tool_schemas

    ui = TerminalUI(use_rich=True)
    todo_display = TodoDisplay(use_rich=True)
    txn_label = _build_txn_labeler()

    def render_event(event: dict[str, Any]) -> None:
        if not show_events:
            return
        event_type = str(event.get("type", "")).strip()
        message = str(event.get("message", "")).strip()

        prefix_map = {
            "turn_start": "turn",
            "llm_plan": "plan",
            "llm_plan_empty": "plan",
            "llm_plan_invalid": "plan",
            "llm_plan_repaired": "plan",
            "llm_plan_error": "plan",
            "plan_result": "plan",
            "plan_dag": "plan",
            "inline_mode": "inline",
            "parallel_start": "parallel",
            "parallel_done": "parallel",
            "parallel_retry": "parallel",
            "parallel_session_created": "parallel",
            "parallel_session_fallback": "parallel",
            "parallel_tools_fallback": "parallel",
            "parallel_txn_begin": "parallel",
            "parallel_txn_commit": "parallel",
            "parallel_txn_abort": "parallel",
            "parallel_worker_start": "worker",
            "parallel_worker_done": "worker",
            "external_worker_start": "worker",
            "external_worker_done": "worker",
            "external_worker_message": "worker",
            "external_worker_output": "worker",
            "external_worker_stderr": "worker",
            "external_worker_tool_call": "worker",
            "external_worker_tool_result": "worker",
            "external_worker_usage": "worker",
            "mcp_server_starting": "mcp",
            "mcp_server_started": "mcp",
            "mcp_server_compat_fallback": "mcp",
            "mcp_session_registered": "mcp",
            "mcp_session_unregistered": "mcp",
            "mcp_tool_call": "mcp",
            "mcp_tool_result": "mcp",
            "mcp_tool_error": "mcp",
            "aggregator_start": "agg",
            "aggregator_done": "agg",
            "txn_begin": "txn",
            "txn_commit": "txn",
            "txn_abort": "txn",
            "subtxn_begin": "subtxn",
            "subtxn_commit": "subtxn",
            "subtxn_abort": "subtxn",
            "subtxn_agent_start": "subagent",
            "subtxn_agent_done": "subagent",
            "subagent_prompt": "prompt",
            "subagent_prompt_ready": "prompt",
            "subagent_prompt_fallback": "prompt",
            "tool_call": "tool",
            "tool_result": "tool",
            "llm_output": "llm",
            "token_usage_final": "usage",
        }
        prefix = prefix_map.get(event_type, "event")

        parts: list[str] = [f"{prefix}: {message}"]
        raw_txn_id = event.get("txn_id")
        parts.append(f"txn={txn_label(str(raw_txn_id) if raw_txn_id is not None else None)}")
        scope = event.get("scope")
        if scope:
            parts.append(f"scope={scope}")
        agent_type = event.get("agent_type")
        if agent_type:
            parts.append(f"agent={agent_type}")
        runtime = event.get("runtime")
        if runtime:
            parts.append(f"runtime={runtime}")
        session_id = event.get("session_id")
        if session_id:
            parts.append(f"session={session_id}")
        tool = event.get("tool")
        if tool:
            parts.append(f"tool={tool}")
        attempt = event.get("attempt")
        if attempt:
            parts.append(f"attempt={attempt}")
        duration_ms = event.get("duration_ms")
        if duration_ms is not None:
            parts.append(f"dur={duration_ms}ms")
        if event_type == "token_usage_final":
            parts.append(
                "tokens="
                f"{int(event.get('total_tokens', 0))} "
                f"(prompt={int(event.get('prompt_tokens', 0))}, "
                f"completion={int(event.get('completion_tokens', 0))}, "
                f"cached={int(event.get('cached_prompt_tokens', 0))}, "
                f"uncached={int(event.get('uncached_prompt_tokens', 0))})"
            )
        if event_type == "external_worker_usage":
            parts.append(
                "tokens="
                f"in:{int(event.get('input_tokens', 0))}, "
                f"out:{int(event.get('output_tokens', 0))}, "
                f"cached_in:{int(event.get('cached_input_tokens', 0))}"
            )

        # Include one concise detail snippet if available.
        detail = (
            event.get("preview")
            or event.get("task")
            or event.get("result")
            or event.get("summary")
            or event.get("url")
            or event.get("assessment")
            or event.get("error")
            or event.get("command")
            or event.get("args")
            or event.get("stdout_path")
            or event.get("stderr_path")
        )
        line = " | ".join(parts)
        if detail:
            line = f"{line} | {detail}"
        ui.print_info(line)

    chronos_state_dir = _prepare_chronos_state_dir(working_dir, fresh=fresh)
    if fresh:
        ui.print_info(f"Fresh mode: reset state at {chronos_state_dir}")

    # Chronos-only execution
    chronos_ctx = _create_chronos_context(
        working_dir,
        weak_snapshot=config.weak_snapshot,
    )
    if config.weak_snapshot:
        ui.print_warning(
            "Weak snapshot mode enabled: write-lock/conflict detection disabled."
        )

    # Initialize session
    session_db_path = str(chronos_state_dir / "messages.db")

    session = SessionManager(
        project_path=working_dir,
        config=config,
        chronos_context=chronos_ctx,
        db_path=session_db_path,
    )

    resumed_messages = []
    if session_id:
        resumed_messages = session.resume_session(session_id)
        ui.print_success(
            f"Resumed session {session_id} ({len(resumed_messages)} messages)"
        )
    else:
        sid = session.start_session()
        ui.print_info(f"Session: {sid}")

    # Build prompt function and LLM
    prompt_fn = _build_prompt_fn()

    try:
        from chronos_code.agent.llm import build_llm_fn
        llm_fn = build_llm_fn(config)
    except ImportError:
        ui.print_warning(
            "litellm not installed. Using mock LLM. "
            "Install with: pip install litellm"
        )
        from chronos_code.agent.llm import MockLLM
        llm_fn = MockLLM([
            {"content": "I'm a mock LLM. Install litellm for real model access.", "tool_calls": None}
        ])

    if log_path:
        resolved_log_path = Path(log_path)
        if not resolved_log_path.is_absolute():
            resolved_log_path = working_dir / resolved_log_path
        llm_fn = _build_logging_llm_fn(
            llm_fn=llm_fn,
            log_path=resolved_log_path,
            get_session_id=lambda: session.session_id or "",
            model=config.model,
        )
        ui.print_info(f"LLM JSONL log: {resolved_log_path}")

    # Build initial tools (rooted at project until a txn begins)
    initial_tools = build_tools(
        working_dir=working_dir,
        config=config,
        chronos_context=chronos_ctx,
        prompt_fn=prompt_fn,
        todo_listener=todo_display.on_todos_changed,
    )
    initial_schemas = build_tool_schemas(initial_tools)

    orchestrator: Orchestrator

    def refresh_tools_for_txn(_txn):
        tools = build_tools(
            working_dir=working_dir,
            config=config,
            chronos_context=chronos_ctx,
            prompt_fn=prompt_fn,
            task_launcher=orchestrator.launch_task,
            todo_listener=todo_display.on_todos_changed,
        )
        return tools, build_tool_schemas(tools)

    def build_tools_for_context(context):
        tools = build_tools(
            working_dir=working_dir,
            config=config,
            chronos_context=context,
            prompt_fn=prompt_fn,
            task_launcher=orchestrator.launch_task,
            todo_listener=todo_display.on_todos_changed,
        )
        return tools, build_tool_schemas(tools)

    # Build orchestrator
    orchestrator = Orchestrator(
        config=config,
        working_dir=working_dir,
        tools=initial_tools,
        llm_fn=llm_fn,
        tool_schemas=initial_schemas,
        refresh_tools_for_txn=refresh_tools_for_txn,
        parallel_tools_for_context=build_tools_for_context,
        event_sink=render_event,
    )

    if resumed_messages:
        orchestrator.restore_from_messages(resumed_messages)

    def get_history() -> str:
        messages = session.load_messages()
        if not messages:
            return "No committed history."
        lines: list[str] = []
        for m in messages:
            body = (m.content or "").strip().replace("\n", "\n    ")
            lines.append(f"[turn {m.turn}] {m.role}: {body}")
        return "\n".join(lines)

    memory_tool = MemoryTool(working_dir=working_dir)

    def get_memory(topic: str | None) -> str:
        if topic:
            return memory_tool.run(command="read", path=topic)
        return memory_tool.run(command="list")

    def resume_existing(target_session_id: str) -> str:
        msgs = session.resume_session(target_session_id)
        orchestrator.restore_from_messages(msgs)
        return f"Resumed session {target_session_id} ({len(msgs)} messages)."

    debug_runner = DebugScenarioRunner(
        working_dir=working_dir,
        session=session,
    )

    # Slash command handler
    slash = SlashCommandHandler(
        config=config,
        get_status_fn=session.get_status,
        get_history_fn=get_history,
        get_memory_fn=get_memory,
        resume_fn=resume_existing,
        debug_fn=debug_runner.run,
    )
    read_user_input = _build_user_input_reader(slash)

    ui.print(_build_banner())
    print()

    while True:
        try:
            user_input = await read_user_input()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not user_input:
            continue

        # Handle slash commands
        if slash.is_slash_command(user_input):
            result = slash.handle(user_input)
            if result.success:
                ui.print_info(result.output)
            else:
                ui.print_error(result.output)
            continue

        try:
            if show_events:
                response = await orchestrator.process(user_input, session=session)
                usage = orchestrator.get_last_turn_usage()
                render_event({
                    "type": "token_usage_final",
                    "message": "Turn token usage.",
                    **usage,
                })
            else:
                with ui.spinner("Thinking..."):
                    response = await orchestrator.process(user_input, session=session)
            ui.print_markdown(response)
            print()

        except KeyboardInterrupt:
            ui.print_warning("Turn aborted. All uncommitted changes discarded.")
            if session.current_txn and session.current_txn.is_active:
                session.abort_txn(session.current_txn)
            # Safety: ensure ChronosContext is clean even if abort_txn missed it
            if chronos_ctx and chronos_ctx.is_active:
                try:
                    chronos_ctx.abort()
                except Exception:
                    pass
        except Exception as e:
            ui.print_error(f"{e}. All uncommitted changes rolled back.")
            logger.exception("Error processing message")
            if session.current_txn and session.current_txn.is_active:
                session.abort_txn(session.current_txn)
            if chronos_ctx and chronos_ctx.is_active:
                try:
                    chronos_ctx.abort()
                except Exception:
                    pass


@click.command()
@click.option(
    "--project",
    "-p",
    default=".",
    help="Project directory (default: current directory)",
)
@click.option(
    "--model",
    "-m",
    default='openrouter/minimax/minimax-m2.5',
    #default='openrouter/google/gemini-3-flash-preview',
    #default='openrouter/anthropic/claude-opus-4.6',
    help="LLM model to use (e.g., claude-sonnet-4-20250514)",
)
@click.option(
    "--resume",
    "-r",
    default=None,
    help="Resume a previous session by ID",
)
@click.option(
    "--show-events/--no-show-events",
    default=False,
    help=(
        "Show concise intermediate progress events (planner/txn/subtxn/"
        "parallel/tool/LLM output) during each turn."
    ),
)
@click.option(
    "--log",
    "log_path",
    default=None,
    help="Append per-LLM-call JSON lines to this file.",
)
@click.option(
    "--fresh",
    is_flag=True,
    help="Reset project-local .chronos-code state before starting.",
)
@click.option(
    "--weak-snapshot",
    is_flag=True,
    help=(
        "Enable weak snapshot isolation (disable write-lock/conflict detection "
        "while keeping snapshot reads)."
    ),
)
@click.option(
    "--worker-runtime",
    type=click.Choice(["internal", "codex", "claude"]),
    default="internal",
    show_default=True,
    help="Sub-agent execution runtime.",
)
@click.option(
    "--codex-command",
    default="codex",
    show_default=True,
    help="Codex CLI executable name/path for --worker-runtime codex.",
)
@click.option(
    "--claude-command",
    default="claude",
    show_default=True,
    help="Claude CLI executable name/path for --worker-runtime claude.",
)
@click.option(
    "--worker-timeout-sec",
    type=int,
    default=900,
    show_default=True,
    help="Timeout for external worker commands in seconds.",
)
@click.option(
    "--embedded-mcp/--no-embedded-mcp",
    default=True,
    show_default=True,
    help="Use in-process embedded Chronos MCP server for external workers.",
)
@click.option(
    "--embedded-mcp-host",
    default="127.0.0.1",
    show_default=True,
    help="Embedded Chronos MCP host.",
)
@click.option(
    "--embedded-mcp-port",
    type=int,
    default=8765,
    show_default=True,
    help="Embedded Chronos MCP port.",
)
@click.option(
    "--embedded-mcp-path",
    default="/mcp",
    show_default=True,
    help="Embedded Chronos MCP HTTP path.",
)
@click.option(
    "--codex-command-template",
    default="{command} exec -C {cwd} --skip-git-repo-check {prompt}",
    show_default=True,
    help="Template for launching Codex workers.",
)
@click.option(
    "--claude-command-template",
    default="{command} -p --output-format json --mcp-config {mcp_config} {prompt}",
    show_default=True,
    help="Template for launching Claude workers.",
)
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
def main(
    project: str,
    model: str | None,
    resume: str | None,
    show_events: bool,
    log_path: str | None,
    fresh: bool,
    weak_snapshot: bool,
    worker_runtime: str,
    codex_command: str,
    claude_command: str,
    worker_timeout_sec: int,
    embedded_mcp: bool,
    embedded_mcp_host: str,
    embedded_mcp_port: int,
    embedded_mcp_path: str,
    codex_command_template: str,
    claude_command_template: str,
    verbose: bool,
) -> None:
    """Chronos-Code: A transactional CLI coding agent."""
    _setup_logging(verbose)

    working_dir = Path(project).resolve()
    if not working_dir.exists():
        click.echo(f"Error: {working_dir} does not exist", err=True)
        sys.exit(1)

    config = Config()
    if model:
        config.model = model
    config.weak_snapshot = weak_snapshot
    config.worker_runtime = worker_runtime  # type: ignore[assignment]
    config.codex_command = codex_command
    config.claude_command = claude_command
    config.external_worker_timeout_sec = int(worker_timeout_sec)
    config.external_worker_use_embedded_mcp = bool(embedded_mcp)
    config.embedded_mcp_host = embedded_mcp_host
    config.embedded_mcp_port = int(embedded_mcp_port)
    config.embedded_mcp_path = embedded_mcp_path
    config.codex_command_template = codex_command_template
    config.claude_command_template = claude_command_template

    asyncio.run(
        _repl_loop(
            working_dir,
            config,
            session_id=resume,
            show_events=show_events,
            log_path=log_path,
            fresh=fresh,
        )
    )


if __name__ == "__main__":
    main()
