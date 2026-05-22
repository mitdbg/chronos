"""Bootstrap helpers for configuring Codex/Claude to use Janus MCP."""

from __future__ import annotations

import json
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal


RuntimeTarget = Literal["codex", "claude", "both"]
TransportMode = Literal["stdio", "streamable-http"]


@dataclass
class BootstrapResult:
    """Result details for bootstrap operations."""

    mcp_config_path: Path | None = None
    written_files: list[Path] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def bootstrap_project(
    *,
    project_dir: Path,
    runtime: RuntimeTarget = "both",
    server_name: str = "janus",
    transport: TransportMode = "stdio",
    http_url: str = "http://127.0.0.1:8000/mcp",
    mcp_command: str = "janus-code-mcp",
    weak_snapshot: bool = False,
    disable_sqlite: bool = False,
    overwrite_server: bool = False,
) -> BootstrapResult:
    """Generate project-level MCP/instruction assets for Codex/Claude."""
    resolved_project_dir = Path(project_dir).resolve()
    result = BootstrapResult()

    entry = build_mcp_server_entry(
        project_dir=resolved_project_dir,
        transport=transport,
        http_url=http_url,
        mcp_command=mcp_command,
        weak_snapshot=weak_snapshot,
        disable_sqlite=disable_sqlite,
    )
    mcp_config_path, warning = upsert_project_mcp_server(
        project_dir=resolved_project_dir,
        server_name=server_name,
        entry=entry,
        overwrite_server=overwrite_server,
    )
    result.mcp_config_path = mcp_config_path
    if warning:
        result.warnings.append(warning)

    bootstrap_root = resolved_project_dir / ".janus-code" / "bootstrap"
    if runtime in ("codex", "both"):
        codex_dir = bootstrap_root / "codex"
        result.written_files.append(
            _write_text(
                codex_dir / "AGENTS.janus.md",
                build_agents_instruction(server_name=server_name),
            )
        )
        result.written_files.append(
            _write_text(
                codex_dir / "skills" / "janus-transaction-protocol" / "SKILL.md",
                build_skill_md(server_name=server_name),
            )
        )
        result.written_files.append(
            _write_text(
                codex_dir / "install_codex_skill.sh",
                build_codex_skill_install_script(
                    project_dir=resolved_project_dir,
                    skill_rel_path=Path(".janus-code/bootstrap/codex/skills/janus-transaction-protocol"),
                ),
                executable=True,
            )
        )

    if runtime in ("claude", "both"):
        claude_dir = bootstrap_root / "claude"
        result.written_files.append(
            _write_text(
                claude_dir / "CLAUDE.janus.md",
                build_claude_instruction(server_name=server_name),
            )
        )
        result.written_files.append(
            _write_text(
                claude_dir / "register_mcp.sh",
                build_claude_register_script(
                    project_dir=resolved_project_dir,
                    server_name=server_name,
                    mcp_command=mcp_command,
                    weak_snapshot=weak_snapshot,
                    disable_sqlite=disable_sqlite,
                ),
                executable=True,
            )
        )

    return result


def build_mcp_server_entry(
    *,
    project_dir: Path,
    transport: TransportMode,
    http_url: str,
    mcp_command: str,
    weak_snapshot: bool,
    disable_sqlite: bool,
) -> dict[str, Any]:
    """Build a `.mcp.json` server entry compatible with Codex/Claude clients."""
    if transport == "streamable-http":
        return {"url": http_url}

    args = [
        "--project",
        str(project_dir),
        "--transport",
        "stdio",
    ]
    if weak_snapshot:
        args.append("--weak-snapshot")
    if disable_sqlite:
        args.append("--disable-sqlite")

    return {
        "command": mcp_command,
        "args": args,
    }


def upsert_project_mcp_server(
    *,
    project_dir: Path,
    server_name: str,
    entry: dict[str, Any],
    overwrite_server: bool,
) -> tuple[Path, str | None]:
    """Insert/update one MCP server entry in project `.mcp.json`."""
    mcp_path = project_dir / ".mcp.json"
    data: dict[str, Any] = {}
    if mcp_path.exists():
        raw = json.loads(mcp_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError(f"Expected JSON object in {mcp_path}")
        data = raw

    servers = data.get("mcpServers")
    if servers is None:
        servers = {}
        data["mcpServers"] = servers
    if not isinstance(servers, dict):
        raise ValueError(f"Expected 'mcpServers' object in {mcp_path}")

    warning: str | None = None
    existing = servers.get(server_name)
    if existing is not None and existing != entry and not overwrite_server:
        warning = (
            f"MCP server '{server_name}' already exists in {mcp_path}. "
            "Kept existing entry (use overwrite_server=True to replace)."
        )
    else:
        servers[server_name] = entry

    mcp_path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return mcp_path, warning


def build_agents_instruction(*, server_name: str) -> str:
    return f"""# Janus MCP Workflow (Codex)

Use MCP server `{server_name}` for filesystem/database operations.

1. Start each task with `janus_txn` action `begin` and set a stable `session_id`.
2. Use the same `session_id` for all `janus_file_editor`, `janus_bash`, and `janus_sqlite` calls.
3. Before finalizing, call `janus_txn` action `changes` and `status`.
4. If checks fail, call `janus_txn` action `abort`; otherwise call `commit`.
5. Never commit when tests or validation are failing.

If the client namespaces MCP tools, the tool names may appear as:
`mcp__{server_name}__janus_txn`, `mcp__{server_name}__janus_file_editor`, etc.
"""


def build_claude_instruction(*, server_name: str) -> str:
    return f"""# Janus MCP Workflow (Claude)

Use MCP server `{server_name}` for transactional file/database actions.

1. Begin with `janus_txn` action `begin` using a consistent `session_id`.
2. Keep all tool calls for the task on that same `session_id`.
3. Use `janus_txn` action `changes` before finalizing.
4. Abort on failed checks; commit only when validation passes.

If your client surfaces namespaced MCP tools, these may appear as:
`mcp__{server_name}__janus_txn`, `mcp__{server_name}__janus_file_editor`, etc.
"""


def build_skill_md(*, server_name: str) -> str:
    return f"""---
name: janus-transaction-protocol
description: Use Janus MCP tools safely with explicit transaction boundaries.
---

# Janus Transaction Protocol

When MCP server `{server_name}` is available:

- Always call `janus_txn` with `action="begin"` first.
- Reuse one `session_id` across all Janus calls in a task.
- Use `janus_file_editor`/`janus_bash`/`janus_sqlite` for changes and checks.
- Call `janus_txn` with `action="changes"` before finalizing.
- If anything is wrong, call `janus_txn` with `action="abort"`.
- Call `janus_txn` with `action="commit"` only after validations pass.
"""


def build_codex_skill_install_script(*, project_dir: Path, skill_rel_path: Path) -> str:
    skill_src = (project_dir / skill_rel_path).resolve()
    return f"""#!/usr/bin/env bash
set -euo pipefail

SKILL_SRC="{skill_src}"
SKILL_DST="${{CODEX_HOME:-$HOME/.codex}}/skills/janus-transaction-protocol"

mkdir -p "$(dirname "$SKILL_DST")"
rm -rf "$SKILL_DST"
cp -R "$SKILL_SRC" "$SKILL_DST"

echo "Installed Codex skill to: $SKILL_DST"
"""


def build_claude_register_script(
    *,
    project_dir: Path,
    server_name: str,
    mcp_command: str,
    weak_snapshot: bool,
    disable_sqlite: bool,
) -> str:
    args = [mcp_command, "--project", str(project_dir), "--transport", "stdio"]
    if weak_snapshot:
        args.append("--weak-snapshot")
    if disable_sqlite:
        args.append("--disable-sqlite")
    quoted_args = " ".join(_shell_quote(arg) for arg in args)

    return f"""#!/usr/bin/env bash
set -euo pipefail

# Registers project-scoped Janus MCP server in Claude Code.
claude mcp add --scope project {server_name} -- {quoted_args}
claude mcp list
"""


def _write_text(path: Path, content: str, *, executable: bool = False) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    if executable:
        mode = path.stat().st_mode
        path.chmod(mode | stat.S_IXUSR)
    return path


def _shell_quote(value: str) -> str:
    if value == "":
        return "''"
    if all(ch.isalnum() or ch in "@%_+=:,./-" for ch in value):
        return value
    return "'" + value.replace("'", "'\"'\"'") + "'"
