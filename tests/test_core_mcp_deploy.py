from __future__ import annotations

from pathlib import Path

from chronos_core.mcp.deploy_codex import (
    build_codex_mcp_entry,
    deploy_codex_mcp,
)

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10.
    import tomli as tomllib  # type: ignore[no-redef]


def _read_toml(path: Path) -> dict[str, object]:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def test_deploy_codex_mcp_creates_starter_config_and_codex_toml_entry(tmp_path: Path) -> None:
    codex_config = tmp_path / "codex-config.toml"
    result = deploy_codex_mcp(
        project_dir=tmp_path,
        codex_config=codex_config,
        runner="installed",
    )

    assert result.created_chronos_config is True
    assert (tmp_path / "chronos.toml").exists()
    data = _read_toml(codex_config)
    assert data["mcp_servers"]["chronos"] == {
        "command": "chronos-mcp",
        "args": ["--config", "chronos.toml", "--transport", "stdio"],
        "cwd": str(tmp_path),
        "startup_timeout_sec": 30,
        "tool_timeout_sec": 300,
        "default_tools_approval_mode": "approve",
    }
    assert 'name = "' + tmp_path.name + '"' in (tmp_path / "chronos.toml").read_text(
        encoding="utf-8"
    )


def test_deploy_codex_mcp_auto_uses_repo_local_uv_runner(tmp_path: Path) -> None:
    codex_config = tmp_path / "codex-config.toml"
    core_project = tmp_path / "packages" / "chronos-core"
    core_project.mkdir(parents=True)
    (core_project / "pyproject.toml").write_text("[project]\nname='chronos-core'\n", encoding="utf-8")

    result = deploy_codex_mcp(project_dir=tmp_path, codex_config=codex_config)

    assert result.entry["command"] == "uv"
    assert result.entry["args"][:5] == [
        "run",
        "--project",
        "packages/chronos-core",
        "--extra",
        "mcp",
    ]
    assert "chronos-mcp" in result.entry["args"]


def test_deploy_codex_mcp_auto_prefers_source_wrapper(tmp_path: Path) -> None:
    codex_config = tmp_path / "codex-config.toml"
    script = tmp_path / "scripts" / "chronos-mcp-server"
    script.parent.mkdir(parents=True)
    script.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    core_project = tmp_path / "packages" / "chronos-core"
    core_project.mkdir(parents=True)
    (core_project / "pyproject.toml").write_text("[project]\nname='chronos-core'\n", encoding="utf-8")

    result = deploy_codex_mcp(project_dir=tmp_path, codex_config=codex_config)

    assert result.entry["command"] == str(script)
    assert result.entry["args"] == ["--config", "chronos.toml", "--transport", "stdio"]


def test_deploy_codex_mcp_preserves_existing_entry_without_overwrite(tmp_path: Path) -> None:
    codex_config = tmp_path / "codex-config.toml"
    codex_config.write_text(
        """
[mcp_servers.chronos]
command = "custom"
""",
        encoding="utf-8",
    )

    result = deploy_codex_mcp(
        project_dir=tmp_path,
        codex_config=codex_config,
        runner="installed",
    )

    assert result.warning is not None
    assert _read_toml(codex_config)["mcp_servers"]["chronos"] == {"command": "custom"}


def test_deploy_codex_mcp_overwrites_existing_entry_when_requested(tmp_path: Path) -> None:
    codex_config = tmp_path / "codex-config.toml"
    codex_config.write_text(
        """
[mcp_servers.other]
command = "keep"

[mcp_servers.chronos]
command = "custom"

[mcp_servers.chronos.env]
OLD = "1"
""",
        encoding="utf-8",
    )

    result = deploy_codex_mcp(
        project_dir=tmp_path,
        codex_config=codex_config,
        runner="installed",
        overwrite=True,
    )

    assert result.warning is None
    data = _read_toml(codex_config)
    assert data["mcp_servers"]["other"]["command"] == "keep"
    assert data["mcp_servers"]["chronos"]["command"] == "chronos-mcp"
    assert "env" not in data["mcp_servers"]["chronos"]


def test_build_codex_mcp_entry_uses_relative_paths_inside_project(tmp_path: Path) -> None:
    entry = build_codex_mcp_entry(
        project_dir=tmp_path,
        chronos_config=tmp_path / "configs" / "chronos.toml",
        runner="uv",
        transport="stdio",
        chronos_core_project="vendor/chronos-core",
    )

    assert entry == {
        "command": "uv",
        "args": [
            "run",
            "--project",
            "vendor/chronos-core",
            "--extra",
            "mcp",
            "chronos-mcp",
            "--config",
            "configs/chronos.toml",
            "--transport",
            "stdio",
        ],
        "cwd": str(tmp_path),
        "startup_timeout_sec": 30,
        "tool_timeout_sec": 300,
        "default_tools_approval_mode": "approve",
    }


def test_build_codex_mcp_entry_source_runner_uses_checkout_script(tmp_path: Path) -> None:
    script = tmp_path / "scripts" / "chronos-mcp-server"
    script.parent.mkdir()
    script.write_text("#!/usr/bin/env bash\n", encoding="utf-8")

    entry = build_codex_mcp_entry(
        project_dir=tmp_path,
        chronos_config=tmp_path / "chronos.toml",
        runner="source",
        transport="stdio",
    )

    assert entry == {
        "command": str(script),
        "args": ["--config", "chronos.toml", "--transport", "stdio"],
        "cwd": str(tmp_path),
        "startup_timeout_sec": 30,
        "tool_timeout_sec": 300,
        "default_tools_approval_mode": "approve",
    }
