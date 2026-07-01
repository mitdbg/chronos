from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

import pytest

from chronos_core.mcp.config import ChronosMcpConfigError, load_chronos_mcp_config
from chronos_core.mcp.runtime import ChronosMcpRuntime
from chronos_core.mcp.server import create_mcp_server


def _write_config(root: Path, body: str) -> Path:
    path = root / "chronos.toml"
    path.write_text(body, encoding="utf-8")
    return path


def _sqlite_url(path: Path) -> str:
    return f"sqlite:///{path}"


def _base_config(root: Path, *, with_table: bool = True) -> str:
    table_block = (
        """
[[stores.sqlite.tables]]
name = "docs"
primary_key = ["id"]
"""
        if with_table
        else ""
    )
    return f"""
[workspace]
name = "mcp-test"
default_branch = "main"
state_dir = ".chronos"
mount_root = ".chronos/mounts"
allowed_source_roots = ["src"]

[filesystem]
database_url = "sqlite:///.chronos/chronosfs.sqlite"
block_size = 8

[stores.sqlite]
kind = "sqlite"
database_url = "{_sqlite_url(root / '.chronos' / 'app.sqlite')}"
{table_block}
"""


def _create_docs_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT NOT NULL)")
        conn.execute("INSERT INTO docs VALUES (?, ?)", ("d1", "main"))
        conn.commit()
    finally:
        conn.close()


def test_chronos_mcp_config_parses_project_relative_paths(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        """
[workspace]
name = "mcp-test"
default_branch = "main"
state_dir = ".chronos"
mount_root = ".chronos/mounts"
allowed_source_roots = ["src"]

[filesystem]
database_url = "sqlite:///.chronos/chronosfs.sqlite"
block_size = 8

[stores.sqlite]
kind = "sqlite"
database_url = "sqlite:///.chronos/app.sqlite"

[[stores.sqlite.tables]]
name = "docs"
primary_key = ["id"]

[stores.duckdb]
kind = "duckdb"
data_url = ".chronos/analytics.duckdb"
metadata_url = "sqlite:///.chronos/metadata.sqlite"
""",
    )

    config = load_chronos_mcp_config(config_path)

    assert config.workspace.name == "mcp-test"
    assert config.workspace.state_dir == tmp_path / ".chronos"
    assert config.workspace.mount_root == tmp_path / ".chronos" / "mounts"
    assert config.workspace.allowed_source_roots == (tmp_path / "src",)
    assert config.filesystem is not None
    assert config.filesystem.database_url == f"sqlite:///{tmp_path / '.chronos' / 'chronosfs.sqlite'}"
    assert config.stores["sqlite"].database_url == f"sqlite:///{tmp_path / '.chronos' / 'app.sqlite'}"
    assert config.stores["sqlite"].tables[0].primary_key == ("id",)
    assert config.stores["duckdb"].data_url == f"duckdb:///{tmp_path / '.chronos' / 'analytics.duckdb'}"
    assert config.stores["duckdb"].metadata_url == f"sqlite:///{tmp_path / '.chronos' / 'metadata.sqlite'}"


def test_chronos_mcp_config_rejects_invalid_store_kind(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        """
[stores.bad]
kind = "mongodb"
database_url = "sqlite:///bad.sqlite"
""",
    )

    with pytest.raises(ChronosMcpConfigError):
        load_chronos_mcp_config(config_path)


def test_chronos_mcp_runtime_imports_source_and_merges_sql_and_fs(tmp_path: Path) -> None:
    source = tmp_path / "src"
    source.mkdir()
    (source / "README.md").write_text("main\n", encoding="utf-8")
    _create_docs_db(tmp_path / ".chronos" / "app.sqlite")
    runtime = ChronosMcpRuntime(load_chronos_mcp_config(_write_config(tmp_path, _base_config(tmp_path))))
    try:
        prepared = runtime.prepare_source(str(source), "main")
        assert prepared["ok"] is True
        assert runtime.prepare_source(str(source), "main")["ok"] is False

        created = runtime.create_sandbox("agent", from_branch="main", mount=False)
        assert created == {"ok": True, "branch_id": "agent", "from_branch": "main"}
        updated = runtime.sql_execute(
            "agent",
            "sqlite",
            "UPDATE docs SET body = :body WHERE id = :id",
            {"id": "d1", "body": "agent"},
        )
        assert updated["ok"] is True
        runtime.filesystem.write_file("agent", "/README.md", "agent\n", parents=True)

        assert runtime.sql_query(
            "main",
            "sqlite",
            "SELECT body FROM docs WHERE id = :id",
            {"id": "d1"},
        )["rows"] == [{"body": "main"}]
        assert runtime.sql_query(
            "agent",
            "sqlite",
            "SELECT body FROM docs WHERE id = :id",
            {"id": "d1"},
        )["rows"] == [{"body": "agent"}]

        preview = runtime.merge_preview("agent", "main", policy="manual_review")
        assert preview["ok"] is True
        assert preview["preview"]["sqlite"]["changes"]
        assert preview["preview"]["filesystem"]["changes"]
        fs_changes = preview["preview"]["filesystem"]["changes"]
        assert any(change["key"].get("path") == "/README.md" for change in fs_changes)

        applied = runtime.merge_apply("agent", "main", policy="snapshot_isolation")
        assert applied["ok"] is True
        assert runtime.sql_query(
            "main",
            "sqlite",
            "SELECT body FROM docs WHERE id = :id",
            {"id": "d1"},
        )["rows"] == [{"body": "agent"}]
        assert runtime.filesystem.read_text("main", "/README.md") == "agent\n"
    finally:
        runtime.close()


def test_chronos_mcp_runtime_enforces_allowed_source_roots(tmp_path: Path) -> None:
    source = tmp_path / "outside"
    source.mkdir()
    runtime = ChronosMcpRuntime(load_chronos_mcp_config(_write_config(tmp_path, _base_config(tmp_path, with_table=False))))
    try:
        result = runtime.prepare_source(str(source), "main")
        assert result["ok"] is False
        assert "allowed source root" in result["error"]["message"]
    finally:
        runtime.close()


def test_chronos_mcp_runtime_requires_explicit_branch_id(tmp_path: Path) -> None:
    _create_docs_db(tmp_path / ".chronos" / "app.sqlite")
    runtime = ChronosMcpRuntime(load_chronos_mcp_config(_write_config(tmp_path, _base_config(tmp_path))))
    try:
        result = runtime.sql_query("", "sqlite", "SELECT body FROM docs")
        assert result["ok"] is False
        assert "branch_id" in result["error"]["message"]
    finally:
        runtime.close()


def test_chronos_mcp_runtime_sql_ddl_then_register_table(tmp_path: Path) -> None:
    runtime = ChronosMcpRuntime(load_chronos_mcp_config(_write_config(tmp_path, _base_config(tmp_path, with_table=False))))
    try:
        created = runtime.sql_execute(
            "main",
            "sqlite",
            "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT NOT NULL)",
        )
        assert created["ok"] is True
        registered = runtime.register_table("sqlite", "notes", ["id"])
        assert registered["ok"] is True
        inserted = runtime.sql_execute(
            "main",
            "sqlite",
            "INSERT INTO notes (id, body) VALUES (:id, :body)",
            {"id": "n1", "body": "hello"},
        )
        assert inserted["ok"] is True
        rows = runtime.sql_query("main", "sqlite", "SELECT body FROM notes WHERE id = :id", {"id": "n1"})
        assert rows["rows"] == [{"body": "hello"}]
    finally:
        runtime.close()


def test_chronos_mcp_runtime_mount_uses_public_helper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[Path, str]] = []

    def fake_start(_store: object, mountpoint: str | Path, *, branch_id: str, **_kwargs: object) -> Path:
        path = Path(mountpoint)
        path.mkdir(parents=True, exist_ok=True)
        calls.append((path, branch_id))
        return path

    monkeypatch.setattr("chronos_core.mcp.runtime.start_chronosfs_mount", fake_start)
    runtime = ChronosMcpRuntime(load_chronos_mcp_config(_write_config(tmp_path, _base_config(tmp_path, with_table=False))))
    try:
        created = runtime.create_sandbox("agent", mount=True)
        assert created["ok"] is True
        assert Path(created["mount_path"]) == tmp_path / ".chronos" / "mounts" / "agent"
        assert calls == [(tmp_path / ".chronos" / "mounts" / "agent", "agent")]
        status = runtime.status()
        assert status["active_mounts"]["agent"] == str(tmp_path / ".chronos" / "mounts" / "agent")
    finally:
        runtime.close()


def test_chronos_mcp_server_can_be_constructed(tmp_path: Path) -> None:
    pytest.importorskip("mcp.server.fastmcp")
    runtime = ChronosMcpRuntime(load_chronos_mcp_config(_write_config(tmp_path, _base_config(tmp_path, with_table=False))))
    try:
        server = create_mcp_server(runtime)
        assert server is not None
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_chronos_mcp_stdio_smoke_status(tmp_path: Path) -> None:
    pytest.importorskip("mcp.client.stdio")
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    config_path = _write_config(
        tmp_path,
        f"""
[workspace]
name = "stdio-smoke"
state_dir = ".chronos"
mount_root = ".chronos/mounts"

[stores.sqlite]
kind = "sqlite"
database_url = "{_sqlite_url(tmp_path / '.chronos' / 'app.sqlite')}"
""",
    )
    env = os.environ.copy()
    src_path = str(Path.cwd() / "packages" / "chronos-core" / "src")
    env["PYTHONPATH"] = src_path + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "chronos_core.mcp.cli", "--config", str(config_path), "--transport", "stdio"],
        cwd=str(Path.cwd()),
        env=env,
    )

    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool("chronos_status", {})

    assert not result.isError
    text = "\n".join(
        getattr(item, "text", "")
        for item in result.content
        if getattr(item, "type", None) == "text"
    )
    payload = json.loads(text)
    assert payload["ok"] is True
    assert payload["workspace"]["name"] == "stdio-smoke"
    assert "sqlite" in payload["stores"]
