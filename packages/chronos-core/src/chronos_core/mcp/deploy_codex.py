"""Deploy Chronos core MCP server configuration for Codex."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Sequence

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python 3.10.
    import tomli as tomllib  # type: ignore[no-redef]


RunnerMode = Literal["auto", "source", "installed", "uv"]
TransportMode = Literal["stdio", "streamable-http"]


class ChronosMcpDeployError(RuntimeError):
    """Raised when Codex MCP deployment cannot update project config."""


@dataclass(frozen=True)
class CodexDeployResult:
    project_dir: Path
    mcp_config_path: Path
    chronos_config_path: Path
    server_name: str
    entry: dict[str, Any]
    warning: str | None = None
    created_chronos_config: bool = False


def deploy_codex_mcp(
    *,
    project_dir: str | Path = ".",
    chronos_config: str | Path = "chronos.toml",
    codex_config: str | Path | None = None,
    server_name: str = "chronos",
    runner: RunnerMode = "auto",
    transport: TransportMode = "stdio",
    overwrite: bool = False,
    create_chronos_config: bool = True,
    chronos_core_project: str | Path = "packages/chronos-core",
) -> CodexDeployResult:
    """Create/update a Codex `config.toml` MCP entry for Chronos."""
    resolved_project = Path(project_dir).expanduser().resolve()
    if not resolved_project.exists():
        raise ChronosMcpDeployError(f"project directory does not exist: {resolved_project}")
    if not resolved_project.is_dir():
        raise ChronosMcpDeployError(f"project path is not a directory: {resolved_project}")

    config_path = _resolve_under_project(resolved_project, chronos_config)
    created_config = False
    if create_chronos_config and not config_path.exists():
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(_default_chronos_toml(resolved_project), encoding="utf-8")
        created_config = True

    active_runner = _select_runner(resolved_project, runner, chronos_core_project)
    entry = build_codex_mcp_entry(
        project_dir=resolved_project,
        chronos_config=config_path,
        runner=active_runner,
        transport=transport,
        chronos_core_project=chronos_core_project,
    )
    codex_config_path = (
        Path(codex_config).expanduser().resolve()
        if codex_config is not None
        else Path.home() / ".codex" / "config.toml"
    )
    mcp_path, warning = upsert_codex_mcp_server(
        codex_config_path=codex_config_path,
        server_name=server_name,
        entry=entry,
        overwrite=overwrite,
    )
    return CodexDeployResult(
        project_dir=resolved_project,
        mcp_config_path=mcp_path,
        chronos_config_path=config_path,
        server_name=server_name,
        entry=entry,
        warning=warning,
        created_chronos_config=created_config,
    )


def build_codex_mcp_entry(
    *,
    project_dir: Path,
    chronos_config: Path,
    runner: Literal["installed", "uv"],
    transport: TransportMode,
    chronos_core_project: str | Path = "packages/chronos-core",
) -> dict[str, Any]:
    """Build a Codex-compatible `config.toml` MCP server entry."""
    if transport != "stdio":
        raise ChronosMcpDeployError("Codex deployment currently supports stdio transport only")
    config_arg = _display_path(project_dir, chronos_config)
    if runner == "installed":
        return {
            "command": "chronos-mcp",
            "args": ["--config", config_arg, "--transport", "stdio"],
            "cwd": str(project_dir),
            "startup_timeout_sec": 30,
            "tool_timeout_sec": 300,
            "default_tools_approval_mode": "approve",
        }
    if runner == "source":
        script = _source_server_script(project_dir)
        if not script.exists():
            raise ChronosMcpDeployError(f"source runner script does not exist: {script}")
        return {
            "command": str(script),
            "args": ["--config", config_arg, "--transport", "stdio"],
            "cwd": str(project_dir),
            "startup_timeout_sec": 30,
            "tool_timeout_sec": 300,
            "default_tools_approval_mode": "approve",
        }
    if runner == "uv":
        core_project = _resolve_under_project(project_dir, chronos_core_project)
        return {
            "command": "uv",
            "args": [
                "run",
                "--project",
                _display_path(project_dir, core_project),
                "--extra",
                "mcp",
                "chronos-mcp",
                "--config",
                config_arg,
                "--transport",
                "stdio",
            ],
            "cwd": str(project_dir),
            "startup_timeout_sec": 30,
            "tool_timeout_sec": 300,
            "default_tools_approval_mode": "approve",
        }
    raise ChronosMcpDeployError(f"unknown runner: {runner}")


def upsert_codex_mcp_server(
    *,
    codex_config_path: Path,
    server_name: str,
    entry: dict[str, Any],
    overwrite: bool,
) -> tuple[Path, str | None]:
    """Insert or update one server entry in Codex `config.toml`."""
    if not server_name:
        raise ChronosMcpDeployError("server_name must be non-empty")
    codex_config_path.parent.mkdir(parents=True, exist_ok=True)
    text = ""
    data: dict[str, Any] = {}
    if codex_config_path.exists():
        text = codex_config_path.read_text(encoding="utf-8")
        try:
            raw = tomllib.loads(text)
        except tomllib.TOMLDecodeError as exc:
            raise ChronosMcpDeployError(f"invalid TOML in {codex_config_path}: {exc}") from exc
        if not isinstance(raw, dict):
            raise ChronosMcpDeployError(f"expected TOML table in {codex_config_path}")
        data = raw

    servers = data.get("mcp_servers")
    if servers is None:
        servers = {}
    if not isinstance(servers, dict):
        raise ChronosMcpDeployError(f"expected mcp_servers table in {codex_config_path}")

    warning: str | None = None
    existing = _server_entry_from_toml(servers.get(server_name))
    if existing is not None and existing != entry and not overwrite:
        warning = (
            f"MCP server '{server_name}' already exists in {codex_config_path}; "
            "kept existing entry. Re-run with --overwrite to replace it."
        )
    else:
        updated = _remove_server_block(text, server_name)
        if updated and not updated.endswith("\n"):
            updated += "\n"
        if updated.strip():
            updated += "\n"
        updated += _render_server_block(server_name, entry)
        codex_config_path.write_text(updated, encoding="utf-8")
    return codex_config_path, warning


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Deploy Chronos MCP tools to Codex.")
    parser.add_argument("--project", default=".", help="Project directory for Chronos state/config.")
    parser.add_argument("--config", default="chronos.toml", help="Path to chronos.toml.")
    parser.add_argument(
        "--codex-config",
        default=None,
        help="Codex config.toml path (default: ~/.codex/config.toml).",
    )
    parser.add_argument("--server-name", default="chronos", help="Codex MCP server name.")
    parser.add_argument(
        "--runner",
        choices=("auto", "source", "installed", "uv"),
        default="auto",
        help="How Codex should launch chronos-mcp.",
    )
    parser.add_argument(
        "--chronos-core-project",
        default="packages/chronos-core",
        help="Path to chronos-core project when --runner uv is used.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Replace an existing server entry.")
    parser.add_argument(
        "--no-create-config",
        action="store_true",
        help="Do not create a starter chronos.toml when it is missing.",
    )
    args = parser.parse_args(argv)

    result = deploy_codex_mcp(
        project_dir=args.project,
        chronos_config=args.config,
        codex_config=args.codex_config,
        server_name=args.server_name,
        runner=args.runner,
        overwrite=args.overwrite,
        create_chronos_config=not args.no_create_config,
        chronos_core_project=args.chronos_core_project,
    )
    print(f"Wrote Codex MCP config: {result.mcp_config_path}")
    if result.created_chronos_config:
        print(f"Created starter Chronos config: {result.chronos_config_path}")
    else:
        print(f"Using Chronos config: {result.chronos_config_path}")
    if result.warning:
        print(f"Warning: {result.warning}")
    print(f"Server name: {result.server_name}")
    print("Entry:")
    print(_render_server_block(result.server_name, result.entry).rstrip())


def _server_entry_from_toml(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        return {}
    result: dict[str, Any] = {}
    for key in (
        "command",
        "args",
        "cwd",
        "startup_timeout_sec",
        "tool_timeout_sec",
        "default_tools_approval_mode",
    ):
        if key in value:
            result[key] = value[key]
    return result


def _render_server_block(server_name: str, entry: dict[str, Any]) -> str:
    lines = [f"[mcp_servers.{_toml_key(server_name)}]"]
    for key in (
        "command",
        "args",
        "cwd",
        "startup_timeout_sec",
        "tool_timeout_sec",
        "default_tools_approval_mode",
    ):
        if key not in entry:
            continue
        lines.extend(_render_toml_value(key, entry[key]))
    return "\n".join(lines) + "\n"


def _render_toml_value(key: str, value: Any) -> list[str]:
    if isinstance(value, str):
        return [f"{key} = {_toml_string(value)}"]
    if isinstance(value, bool):
        return [f"{key} = {'true' if value else 'false'}"]
    if isinstance(value, int):
        return [f"{key} = {value}"]
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        if not value:
            return [f"{key} = []"]
        lines = [f"{key} = ["]
        lines.extend(f"    {_toml_string(item)}," for item in value)
        lines.append("]")
        return lines
    raise ChronosMcpDeployError(f"unsupported MCP config value for {key}: {value!r}")


def _remove_server_block(text: str, server_name: str) -> str:
    if not text:
        return ""
    lines = text.splitlines(keepends=True)
    result: list[str] = []
    skipping = False
    for line in lines:
        table = _table_header(line)
        if table is not None:
            skipping = _is_server_table(table, server_name)
        if not skipping:
            result.append(line)
    return "".join(result).rstrip() + ("\n" if result else "")


def _table_header(line: str) -> str | None:
    stripped = line.strip()
    if not stripped.startswith("[") or not stripped.endswith("]"):
        return None
    if stripped.startswith("[["):
        return None
    return stripped[1:-1].strip()


def _is_server_table(table: str, server_name: str) -> bool:
    bare = f"mcp_servers.{server_name}"
    quoted = f"mcp_servers.{_toml_key(server_name)}"
    return table == bare or table.startswith(bare + ".") or table == quoted or table.startswith(quoted + ".")


def _toml_key(value: str) -> str:
    if value.replace("_", "-").replace("-", "").isalnum() and value:
        return value
    return _toml_string(value)


def _toml_string(value: str) -> str:
    return json.dumps(value)


def _select_runner(
    project_dir: Path,
    runner: RunnerMode,
    chronos_core_project: str | Path,
) -> Literal["source", "installed", "uv"]:
    if runner in ("installed", "uv"):
        return runner
    if runner == "source":
        return "source"
    if _source_server_script(project_dir).exists():
        return "source"
    core_project = _resolve_under_project(project_dir, chronos_core_project)
    if (core_project / "pyproject.toml").exists():
        return "uv"
    return "installed"


def _source_server_script(project_dir: Path) -> Path:
    return project_dir / "scripts" / "chronos-mcp-server"


def _resolve_under_project(project_dir: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        candidate = project_dir / candidate
    return candidate.resolve()


def _display_path(project_dir: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(project_dir.resolve()))
    except ValueError:
        return str(path.resolve())


def _default_chronos_toml(project_dir: Path) -> str:
    return f"""[workspace]
name = "{project_dir.name}"
default_branch = "main"
state_dir = ".chronos"
mount_root = ".chronos/mounts"
allowed_source_roots = ["."]

[filesystem]
database_url = "sqlite:///.chronos/chronosfs.sqlite"
block_size = 8192

[stores.sqlite]
kind = "sqlite"
database_url = "sqlite:///.chronos/app.sqlite"
"""


if __name__ == "__main__":
    main()
