"""CLI argument forwarding, terminal behavior, and cleanup without real services."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock
from contextlib import contextmanager

import pytest
import typer
from typer.testing import CliRunner

from chronos_core.branching import BranchingError, ChronosBranchContext
from chronos_core.cli import _qdrant, _workspace
from chronos_core.cli._relational import DatabaseConfig, connect
from chronos_core.cli._terminal import print_result
from chronos_core.cli._workspace_config import (
    WorkspaceConfigError, load_workspace_config, open_workspace,
)
from chronos_core.cli.main import app

runner = CliRunner()


@pytest.fixture
def workspace_connection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CHRONOS_WORKSPACE_CONFIG", raising=False)
    (tmp_path / "chronos.toml").write_text(
        '[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///app.sqlite"\n'
        '[stores.other]\nkind="sqlite"\ndatabase_url="sqlite:///other.sqlite"\n'
    )
    workspace = MagicMock()
    for method in ("create_branch", "create_branch_from_checkpoint", "delete_branch", "create_checkpoint",
                   "diff", "merge_preview", "merge_apply", "merge_atomic_preview", "merge_atomic"):
        getattr(workspace, method).return_value = None
    workspace.list_branches.return_value = ["main"]
    workspace.stores = {"app": MagicMock(), "other": MagicMock()}
    workspace.stores["app"].register_table.return_value = None
    workspace.stores["app"].db.commit.return_value = None
    session = MagicMock()
    session.query.return_value = [{"id": 1}]
    session.execute.return_value = None
    workspace.checkout.return_value.stores = {"app": session, "other": MagicMock()}

    @contextmanager
    def opened(config):
        try:
            yield workspace
        finally:
            workspace.close()

    monkeypatch.setattr(_workspace, "open_workspace", MagicMock(side_effect=opened))
    return workspace


@pytest.fixture
def connection(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    database = MagicMock()
    database.list_branches.return_value = []
    database.checkout.return_value.__enter__.return_value.query.return_value = []
    monkeypatch.setattr(ChronosBranchContext, "connect", MagicMock(return_value=database))
    monkeypatch.delenv("CHRONOS_DATABASE_URL", raising=False)
    monkeypatch.delenv("CHRONOS_METADATA_URL", raising=False)
    return database


@pytest.fixture
def factory(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    pytest.importorskip("qdrant_client")
    for name in ("CHRONOS_METADATA_URL", "CHRONOS_QDRANT_PATH", "CHRONOS_QDRANT_URL",
                 "CHRONOS_QDRANT_API_KEY", "CHRONOS_QDRANT_COLLECTION_PREFIX"):
        monkeypatch.delenv(name, raising=False)
    factory = MagicMock()
    factory.remote.return_value = factory.local.return_value
    monkeypatch.setattr(_qdrant, "_store_class", lambda: factory)
    return factory


def _invoke_qdrant(*args: str, **kwargs):
    return runner.invoke(app, ["qdrant", "--path", "vectors", "--metadata-url", "sqlite:///metadata.sqlite", *args], **kwargs)


class TestRelationalCommands:
    def test_configuration_and_help(self, connection: MagicMock) -> None:
        for args in (["--help"], ["branch", "--help"], ["query", "--help"]):
            assert runner.invoke(app, args).exit_code == 0
        ChronosBranchContext.connect.assert_not_called()
        result = runner.invoke(app, ["branch", "list"])
        assert result.exit_code == 2
        ChronosBranchContext.connect.assert_not_called()
        result = runner.invoke(app, ["branch", "list"], env={"CHRONOS_DATABASE_URL": "sqlite:///env.db"})
        assert result.exit_code == 0
        ChronosBranchContext.connect.assert_called_with("sqlite:///env.db")
        result = runner.invoke(app, ["--database-url", "sqlite:///flag.db", "branch", "list"],
                               env={"CHRONOS_DATABASE_URL": "sqlite:///env.db"})
        assert result.exit_code == 0
        ChronosBranchContext.connect.assert_called_with("sqlite:///flag.db")
        assert json.loads(result.stdout) == []
        assert connection.close.call_count == 2

    @pytest.mark.parametrize(("args", "method", "expected_args", "expected_kwargs"), [
        (["branch", "list"], "list_branches", (), {}),
        (["branch", "create", "trial"], "create_branch", ("trial",), {"from_branch": "main"}),
        (["branch", "create", "trial", "--from", "parent"], "create_branch", ("trial",), {"from_branch": "parent"}),
        (["branch", "delete", "trial"], "delete_branch", ("trial",), {}),
        (["table", "register", "items", "--primary-key", "id", "--primary-key", "tenant"], "register_table", ("items", ["id", "tenant"]), {}),
        (["checkpoint", "create", "saved", "--branch", "trial"], "create_checkpoint", ("saved",), {"branch": "trial"}),
        (["checkpoint", "list"], "list_checkpoints", (), {"branch": None}),
        (["checkpoint", "list", "--branch", "trial"], "list_checkpoints", (), {"branch": "trial"}),
        (["diff", "main", "trial"], "diff", ("main", "trial"), {}),
        (["diff", "main", "trial", "--table", "items"], "diff_rows", ("main", "trial", "items"), {}),
        (["merge", "preview", "trial", "main"], "merge_preview", ("trial", "main"), {"policy": "abort_on_conflict"}),
        (["merge", "apply", "trial", "main", "--policy", "snapshot_isolation"], "merge_apply", ("trial", "main"), {"policy": "snapshot_isolation"}),
    ])
    def test_api_forwarding(self, connection: MagicMock, args: list[str], method: str,
                            expected_args: tuple, expected_kwargs: dict) -> None:
        getattr(connection, method).return_value = None
        result = runner.invoke(app, ["--database-url", "sqlite:///test.db", *args])
        assert result.exit_code == 0, result.output
        getattr(connection, method).assert_called_once_with(*expected_args, **expected_kwargs)
        assert result.stdout == ""
        connection.close.assert_called_once()

    @pytest.mark.parametrize("command", ["query", "execute"])
    @pytest.mark.parametrize("source", ["string", "file", "stdin", "pipe"])
    def test_sql_sources(self, connection: MagicMock, tmp_path: Path, command: str, source: str) -> None:
        sql = "SELECT *\nFROM items WHERE id = :id\n"
        args = ["--database-url", "sqlite:///test.db", command, "--branch", "trial", "--params", '{"id": 3}']
        input_text = None
        if source == "string":
            args.append(sql)
        elif source == "file":
            path = tmp_path / "query.sql"
            path.write_text(sql, encoding="utf-8")
            args.extend(["--file", str(path)])
        else:
            input_text = sql
            if source == "stdin":
                args.append("--stdin")
        session = connection.checkout.return_value.__enter__.return_value
        getattr(session, command).return_value = [{"id": 3}] if command == "query" else {"rowcount": 1}
        result = runner.invoke(app, args, input=input_text)
        assert result.exit_code == 0, result.output
        connection.checkout.assert_called_once_with("trial")
        getattr(session, command).assert_called_once_with(sql, {"id": 3})
        assert json.loads(result.stdout) == getattr(session, command).return_value
        connection.checkout.return_value.__exit__.assert_called_once()
        connection.close.assert_called_once()

    @pytest.mark.parametrize("args", [
        ["query"], ["query", " "], ["query", "SELECT 1", "--stdin"],
        ["query", "SELECT 1", "--file", "missing.sql"],
        ["query", "--file", "missing.sql"],
        ["query", "SELECT 1", "--params", "[]"],
        ["execute", "SELECT 1", "--params", "broken"],
        ["merge", "apply", "a", "b", "--policy", "unknown"],
        ["table", "register", "items"],
    ])
    def test_bad_inputs_do_not_connect(self, connection: MagicMock, args: list[str]) -> None:
        result = runner.invoke(app, ["--database-url", "sqlite:///test.db", *args])
        assert result.exit_code == 2, result.output
        ChronosBranchContext.connect.assert_not_called()

    def test_interactive_missing_sql(self, connection: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
        from chronos_core.cli._relational import _sql_input
        import typer
        with pytest.raises(typer.BadParameter, match="Provide SQL"):
            _sql_input(None, None, False)
        ChronosBranchContext.connect.assert_not_called()

    def test_cleanup_and_redaction(self, connection: MagicMock) -> None:
        url = "postgresql://user:secret@localhost/db"
        session = connection.checkout.return_value.__enter__.return_value
        session.query.side_effect = BranchingError(f"Cannot use {url}; password=secret")
        result = runner.invoke(app, ["--database-url", url, "query", "SELECT 1"])
        assert result.exit_code == 1
        assert "secret" not in result.output
        assert "Error:" in result.stderr
        connection.checkout.return_value.__exit__.assert_called_once()
        connection.close.assert_called_once()

    def test_connection_failure_does_not_close_unassigned_context(self, connection: MagicMock) -> None:
        ChronosBranchContext.connect.side_effect = ValueError("unsupported database")
        result = runner.invoke(app, ["--database-url", "bad", "branch", "list"])
        assert result.exit_code == 1
        assert "unsupported database" in result.stderr
        connection.close.assert_not_called()

    def test_installed_entrypoints(self) -> None:
        executable = Path(sys.executable).parent / "chronos"
        for args in ([str(executable), "--help"], [sys.executable, "-m", "chronos_core.cli", "--help"]):
            env = dict(os.environ)
            env.pop("CHRONOS_DATABASE_URL", None)
            env.pop("CHRONOS_METADATA_URL", None)
            result = subprocess.run(args, capture_output=True, text=True, env=env, timeout=30)
            assert result.returncode == 0, result.stderr
            assert "--database-url" in result.stdout

    def test_create_table_forwarding(self, connection: MagicMock) -> None:
        connection.db.commit.return_value = None
        statement = "CREATE TABLE items (id INTEGER PRIMARY KEY)"
        result = runner.invoke(app, ["--database-url", "sqlite:///test.db", "table", "create", statement])
        assert result.exit_code == 0, result.output
        connection.db.execute.assert_called_once_with(statement)
        connection.db.commit.assert_called_once()
        connection.checkout.assert_not_called()
        assert result.stdout == ""

    @pytest.mark.parametrize("statement", [
        "DELETE FROM items", "CREATE INDEX idx ON items(id)",
        "CREATE TABLE items (id INTEGER); DROP TABLE items", "not sql",
    ])
    def test_create_table_rejects_other_sql(self, connection: MagicMock, statement: str) -> None:
        result = runner.invoke(app, ["--database-url", "sqlite:///test.db", "table", "create", statement])
        assert result.exit_code == 2, result.output
        ChronosBranchContext.connect.assert_not_called()

    def test_native_error_is_reported_without_traceback(self, connection: MagicMock) -> None:
        session = connection.checkout.return_value.__enter__.return_value
        session.execute.side_effect = RuntimeError("INSERT must specify a column list")
        result = runner.invoke(app, [
            "--database-url", "sqlite:///test.db", "execute", "INSERT INTO items VALUES (1)",
        ])
        assert result.exit_code == 1
        assert result.stdout == ""
        assert result.stderr.strip() == "Error: INSERT must specify a column list"
        connection.checkout.return_value.__exit__.assert_called_once()
        connection.close.assert_called_once()

    @pytest.mark.parametrize("source", ["flag", "environment", "override"])
    def test_split_connection_configuration(self, connection: MagicMock, monkeypatch: pytest.MonkeyPatch, source: str) -> None:
        split = MagicMock(return_value=connection)
        monkeypatch.setattr(ChronosBranchContext, "connect_split", split)
        args = ["--database-url", "duckdb:////tmp/data.duckdb"]
        environment = {}
        if source != "flag":
            environment["CHRONOS_METADATA_URL"] = "sqlite:////tmp/env.sqlite"
        if source != "environment":
            args.extend(["--metadata-url", "sqlite:////tmp/flag.sqlite"])
        result = runner.invoke(app, [*args, "branch", "list"], env=environment)
        assert result.exit_code == 0, result.output
        expected_metadata = "sqlite:////tmp/env.sqlite" if source == "environment" else "sqlite:////tmp/flag.sqlite"
        split.assert_called_once_with("duckdb:////tmp/data.duckdb", expected_metadata)
        ChronosBranchContext.connect.assert_not_called()
        connection.close.assert_called_once()

    def test_duckdb_requires_metadata_before_connecting(self, connection: MagicMock) -> None:
        result = runner.invoke(app, ["--database-url", "duckdb:////tmp/data.duckdb", "branch", "list"])
        assert result.exit_code == 2
        assert "--metadata-url" in result.output
        ChronosBranchContext.connect.assert_not_called()

    def test_split_connection_redacts_metadata_credentials(self, connection: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        metadata = "postgresql://user:secret@localhost/metadata"
        split = MagicMock(side_effect=ValueError(f"Cannot connect to {metadata}"))
        monkeypatch.setattr(ChronosBranchContext, "connect_split", split)
        result = runner.invoke(app, ["--database-url", "duckdb:////tmp/data.duckdb",
                                     "--metadata-url", metadata, "branch", "list"])
        assert result.exit_code == 1
        assert "secret" not in result.output
        assert "<database>" in result.stderr
        connection.close.assert_not_called()

    def test_unavailable_duckdb_driver_reports_api_error(self, connection: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        split = MagicMock(side_effect=RuntimeError("DuckDB support requires CHRONOS_WITH_DUCKDB=ON"))
        monkeypatch.setattr(ChronosBranchContext, "connect_split", split)
        result = runner.invoke(app, ["--database-url", "duckdb:////tmp/data.duckdb",
                                     "--metadata-url", "sqlite:////tmp/metadata.sqlite", "branch", "list"])
        assert result.exit_code == 1
        assert result.stdout == ""
        assert "CHRONOS_WITH_DUCKDB=ON" in result.stderr
        connection.close.assert_not_called()

    @pytest.mark.parametrize("failure", [False, True])
    def test_connection_cleanup(self, monkeypatch: pytest.MonkeyPatch, failure: bool) -> None:
        database = MagicMock()
        monkeypatch.setattr(ChronosBranchContext, "connect", MagicMock(return_value=database))
        if failure:
            with pytest.raises(typer.Exit):
                with connect(DatabaseConfig("sqlite:///:memory:")):
                    raise ValueError("operation failed")
        else:
            with connect(DatabaseConfig("sqlite:///:memory:")) as context:
                assert context is database
        database.close.assert_called_once()


class TestQdrantCommands:
    def test_help_does_not_connect(self, factory: MagicMock) -> None:
        for args in (["--help"], ["qdrant", "--help"], ["qdrant", "point", "--help"],
                     ["qdrant", "search", "--help"]):
            assert runner.invoke(app, args).exit_code == 0
        factory.local.assert_not_called()
        factory.remote.assert_not_called()

    @pytest.mark.parametrize(("args", "method", "positional", "keywords"), [
        (["collection", "register", "docs", "--dimensions", "3"], "register_collection", ("docs", 3), {"distance": "cosine"}),
        (["collection", "register", "docs", "--dimensions", "3", "--distance", "dot"], "register_collection", ("docs", 3), {"distance": "dot"}),
        (["collection", "list"], "list_collections", (), {}),
        (["collection", "info", "docs"], "collection_info", ("docs",), {}),
        (["branch", "create", "trial", "--from", "parent"], "create_branch", ("trial",), {"from_branch": "parent"}),
        (["branch", "delete", "trial"], "delete_branch", ("trial",), {}),
        (["checkpoint", "create", "saved", "--branch", "trial"], "create_checkpoint", ("saved",), {"branch": "trial"}),
        (["diff", "main", "trial"], "diff", ("main", "trial"), {}),
        (["merge", "preview", "trial", "main"], "merge_preview", ("trial", "main"), {"policy": "abort_on_conflict"}),
        (["merge", "apply", "trial", "main", "--policy", "snapshot_isolation"], "merge_apply", ("trial", "main"), {"policy": "snapshot_isolation"}),
    ])
    def test_store_api_forwarding(self, factory: MagicMock, args, method, positional, keywords) -> None:
        store = factory.local.return_value
        getattr(store, method).return_value = {"result": "from API"}
        result = _invoke_qdrant(*args)
        assert result.exit_code == 0, result.output
        getattr(store, method).assert_called_once_with(*positional, **keywords)
        assert json.loads(result.stdout) == {"result": "from API"}
        store.close.assert_called_once()

    @pytest.mark.parametrize(("args", "method", "keywords"), [
        (["branch", "list"], "list_branches", {}),
        (["checkpoint", "list", "--branch", "trial"], "list_checkpoints", {"branch": "trial"}),
    ])
    def test_metadata_context_calls(self, factory: MagicMock, args, method, keywords) -> None:
        store = factory.local.return_value
        getattr(store.context, method).return_value = []
        result = _invoke_qdrant(*args)
        assert result.exit_code == 0, result.output
        getattr(store.context, method).assert_called_once_with(**keywords)
        assert json.loads(result.stdout) == []
        store.close.assert_called_once()

    @pytest.mark.parametrize(("command", "method", "positional"), [
        (["point", "get", "docs", "one"], "get", ("docs", "one")),
        (["point", "list", "docs"], "list_points", ("docs",)),
        (["point", "delete", "docs", "one"], "delete", ("docs", "one")),
    ])
    def test_point_session_calls(self, factory: MagicMock, command, method, positional) -> None:
        store = factory.local.return_value
        session = store.checkout_control.return_value
        getattr(session, method).return_value = False if method == "delete" else None
        result = _invoke_qdrant(*command, "--branch", "trial")
        assert result.exit_code == 0, result.output
        store.context.checkout.assert_called_once_with("trial")
        store.checkout_control.assert_called_once_with(store.context.checkout.return_value.__enter__.return_value)
        getattr(session, method).assert_called_once_with(*positional)
        assert result.stdout == ("false\n" if method == "delete" else "")
        store.context.checkout.return_value.__exit__.assert_called_once()
        store.close.assert_called_once()

    @pytest.mark.parametrize("source", ["text", "file", "stdin", "pipe"])
    @pytest.mark.parametrize("command", ["upsert", "search"])
    def test_json_sources(self, factory: MagicMock, tmp_path: Path, source: str, command: str) -> None:
        text = '{"vector": [1, 0, 0], "payload": {"title": "hello"}}' if command == "upsert" else "[1, 0, 0]"
        args = ["point", "upsert", "docs", "one", "--revision", "r1"] if command == "upsert" else ["search", "docs", "--limit", "2", "--score-threshold", "0.5"]
        input_text = None
        if source == "text":
            args.append(text)
        elif source == "file":
            path = tmp_path / "input.json"
            path.write_text(text, encoding="utf-8")
            args.extend(["--file", str(path)])
        else:
            input_text = text
            if source == "stdin":
                args.append("--stdin")
        store = factory.local.return_value
        session = store.checkout_control.return_value
        getattr(session, command).return_value = []
        result = _invoke_qdrant(*args, input=input_text)
        assert result.exit_code == 0, result.output
        if command == "upsert":
            session.upsert.assert_called_once_with("docs", "one", [1, 0, 0], {"title": "hello"}, revision="r1")
        else:
            session.search.assert_called_once_with("docs", [1, 0, 0], limit=2, score_threshold=0.5)
        assert json.loads(result.stdout) == []
        store.close.assert_called_once()

    def test_connection_flags_and_environment(self, factory: MagicMock) -> None:
        store = factory.local.return_value
        store.list_collections.return_value = []
        env = {"CHRONOS_METADATA_URL": "sqlite:///env.sqlite", "CHRONOS_QDRANT_URL": "http://localhost:6333",
               "CHRONOS_QDRANT_API_KEY": "test-secret"}
        result = runner.invoke(app, ["qdrant", "--metadata-url", "sqlite:///flag.sqlite", "--collection-prefix", "demo_",
                                     "--timeout", "12", "collection", "list"], env=env)
        assert result.exit_code == 0, result.output
        factory.remote.assert_called_once_with("sqlite:///flag.sqlite", url="http://localhost:6333",
                                               api_key="test-secret", collection_prefix="demo_", timeout=12.0)
        factory.local.assert_not_called()
        store.close.assert_called_once()

    @pytest.mark.parametrize("args", [
        ["collection", "list"],
        ["--metadata-url", "sqlite:///m.sqlite", "collection", "list"],
        ["--metadata-url", "sqlite:///m.sqlite", "--path", "vectors", "--url", "http://localhost:6333", "collection", "list"],
        ["--metadata-url", "sqlite:///:memory:", "--path", "vectors", "collection", "list"],
        ["--metadata-url", "sqlite:///m.sqlite", "--path", ":memory:", "collection", "list"],
        ["--metadata-url", "sqlite:///m.sqlite", "--path", "vectors", "--api-key", "secret", "collection", "list"],
    ])
    def test_invalid_configuration_does_not_connect(self, factory: MagicMock, args) -> None:
        assert runner.invoke(app, ["qdrant", *args]).exit_code == 2
        factory.local.assert_not_called()
        factory.remote.assert_not_called()

    @pytest.mark.parametrize("args", [
        ["search", "docs"], ["search", "docs", "not JSON"], ["search", "docs", "[NaN]"],
        ["search", "docs", "[true]"], ["search", "docs", '{}'], ["search", "docs", "[1]", "--stdin"],
        ["search", "docs", "--file", "missing.json"],
        ["point", "upsert", "docs", "one", '{"vector": [1], "payload": []}'],
        ["point", "upsert", "docs", "one", '{"vector": [1], "unexpected": 1}'],
        ["collection", "register", "docs", "--dimensions", "0"],
    ])
    def test_invalid_input_does_not_connect(self, factory: MagicMock, args) -> None:
        result = _invoke_qdrant(*args)
        assert result.exit_code == 2, result.output
        factory.local.assert_not_called()

    def test_failure_closes_session_before_store_and_redacts_secrets(self, factory: MagicMock) -> None:
        store = factory.local.return_value
        session = store.checkout_control.return_value
        events = []
        store.context.checkout.return_value.__exit__.side_effect = lambda *args: events.append("session")
        store.close.side_effect = lambda: events.append("store")
        session.search.side_effect = RuntimeError("Failed with api-key test-secret at postgresql://user:pw@host/meta")
        result = runner.invoke(app, ["qdrant", "--url", "http://localhost:6333", "--api-key", "test-secret",
                                     "--metadata-url", "postgresql://user:pw@host/meta", "search", "docs", "[1,0,0]"])
        assert result.exit_code == 1
        assert result.stdout == ""
        assert "test-secret" not in result.stderr
        assert "user:pw" not in result.stderr
        assert events == ["session", "store"]

    def test_factory_failure_does_not_close_unassigned_store(self, factory: MagicMock) -> None:
        factory.local.side_effect = ValueError("Cannot open storage")
        result = _invoke_qdrant("collection", "list")
        assert result.exit_code == 1
        factory.local.return_value.close.assert_not_called()

    def test_optional_dependency_is_not_needed_for_help(self, tmp_path: Path) -> None:
        code = '''
    import importlib.abc
    import sys
    class Block(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname == "qdrant_client" or fullname.startswith("qdrant_client."):
                raise ModuleNotFoundError("simulated missing qdrant-client")
    sys.meta_path.insert(0, Block())
    from chronos_core.cli.main import app
    sys.argv = ["chronos", *sys.argv[1:]]
    app()
    '''
        env = {key: value for key, value in os.environ.items() if not key.startswith("CHRONOS_")}
        for args in (["--help"], ["qdrant", "--help"]):
            result = subprocess.run([sys.executable, "-c", textwrap.dedent(code), *args], capture_output=True, text=True, env=env)
            assert result.returncode == 0, result.stderr
        result = subprocess.run([sys.executable, "-c", textwrap.dedent(code), "qdrant", "--metadata-url", f"sqlite:///{tmp_path / 'metadata.sqlite'}",
                                 "--path", str(tmp_path / "vectors"), "collection", "list"], capture_output=True, text=True, env=env)
        assert result.returncode == 1
        assert "chronos-core[qdrant]" in result.stderr
        assert list(tmp_path.iterdir()) == []
        result = subprocess.run([sys.executable, "-c", textwrap.dedent(code), "--database-url", f"sqlite:///{tmp_path / 'sql.sqlite'}",
                                 "branch", "list"], capture_output=True, text=True, env=env)
        assert result.returncode == 0, result.stderr
        assert [branch["branch_id"] for branch in json.loads(result.stdout)] == ["main"]


class TestTerminalBehavior:
    def test_json_values(self, capsys: pytest.CaptureFixture[str]) -> None:
        @dataclass
        class Result:
            values: list
        print_result(Result([date(2026, 1, 2), datetime(2026, 1, 2, 3, 4), time(3, 4), Decimal("1.20"), b"hi"]))
        assert json.loads(capsys.readouterr().out) == {"values": [
            "2026-01-02", "2026-01-02T03:04:00", "03:04:00", "1.20", {"encoding": "base64", "data": "aGk="}
        ]}
        print_result(None)
        assert capsys.readouterr().out == ""
        with pytest.raises(TypeError, match="Unsupported JSON value"):
            print_result(object())


class TestWorkspaceConfiguration:
    def test_path_precedence_and_relative_storage(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("CHRONOS_WORKSPACE_CONFIG", raising=False)
        default = tmp_path / "chronos.toml"
        default.write_text('[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///.chronos/app.sqlite"\n')
        assert load_workspace_config().stores["app"].metadata_url == f"sqlite:///{tmp_path / '.chronos/app.sqlite'}"
        other = tmp_path / "project"
        other.mkdir()
        configured = other / "workspace.toml"
        configured.write_text('[stores.analytics]\nkind="duckdb"\ndatabase_url="duckdb:///data.duckdb"\nmetadata_url="sqlite:///meta.sqlite"\n')
        monkeypatch.setenv("CHRONOS_WORKSPACE_CONFIG", str(configured))
        selected = load_workspace_config()
        assert selected.config_path == configured
        assert selected.stores["analytics"].connection == DatabaseConfig(
            f"duckdb:///{other / 'data.duckdb'}", f"sqlite:///{other / 'meta.sqlite'}",
        )
        assert load_workspace_config(default).config_path == default
        assert not (tmp_path / ".chronos").exists()
        assert sorted(item.name for item in other.iterdir()) == ["workspace.toml"]

    def test_environment_values_and_atomic_metadata(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("WORKSPACE_DATABASE", "postgres://user:secret@localhost/test")
        monkeypatch.setenv("VECTOR_KEY", "vector-secret")
        config = tmp_path / "chronos.toml"
        config.write_text('''
[workspace]
shared_metadata_url = "${WORKSPACE_DATABASE}"
[stores.app]
kind = "postgresql"
database_url = "${WORKSPACE_DATABASE}"
[stores.vectors]
kind = "qdrant"
metadata_url = "${WORKSPACE_DATABASE}"
url = "http://localhost:6333"
api_key = "${VECTOR_KEY}"
timeout = 2.5
''')
        loaded = load_workspace_config(config)
        assert loaded.shared_metadata_url == "postgresql://user:secret@localhost/test"
        assert loaded.stores["vectors"].connection == _qdrant.QdrantConfig(
            loaded.shared_metadata_url, url="http://localhost:6333", api_key="vector-secret", timeout=2.5,
        )
        monkeypatch.delenv("VECTOR_KEY")
        with pytest.raises(WorkspaceConfigError, match="unset environment variable 'VECTOR_KEY'") as failure:
            load_workspace_config(config)
        assert "secret" not in str(failure.value)

    @pytest.mark.parametrize("source", [
        '', '[stores.app]\nkind="other"',
        '[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///:memory:"',
        '[stores.app]\nkind="sqlite"\ndatabase_url="postgresql://user:secret@host/test"',
        '[stores.app]\nkind="duckdb"\ndatabase_url="duckdb:///app.duckdb"',
        '[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///app.sqlite"\ntypo="secret"',
        '[stores.fs]\nkind="sqlite"\ndatabase_url="sqlite:///app.sqlite"',
        '[stores.vectors]\nkind="qdrant"\nmetadata_url="sqlite:///meta.sqlite"',
        '[stores.vectors]\nkind="qdrant"\nmetadata_url="sqlite:///meta.sqlite"\npath=":memory:"',
        '[stores.vectors]\nkind="qdrant"\nmetadata_url="sqlite:///meta.sqlite"\npath="data"\nurl="http://localhost:6333"',
        '[stores.vectors]\nkind="qdrant"\nmetadata_url="sqlite:///meta.sqlite"\npath="data"\napi_key="secret"',
        '[stores.vectors]\nkind="qdrant"\nmetadata_url="sqlite:///meta.sqlite"\nurl="http://localhost:6333"\ntimeout=nan',
        '[stores.vectors]\nkind="qdrant"\nmetadata_url="sqlite:///meta.sqlite"\nurl="http://localhost:6333"\ntimeout=true',
        '[workspace]\nshared_metadata_url="sqlite:///shared.sqlite"\n[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///other.sqlite"',
        '[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///same.sqlite"\n[stores.vectors]\nkind="qdrant"\nmetadata_url="sqlite:///same.sqlite"\npath="data"',
        '[stores.app]\nkind="sqlite"\ndatabase_url="secret',
    ])
    def test_invalid_configuration_does_not_create_storage(self, source: str, tmp_path: Path) -> None:
        config = tmp_path / "chronos.toml"
        config.write_text(source)
        with pytest.raises(WorkspaceConfigError) as failure:
            load_workspace_config(config)
        assert "secret" not in str(failure.value)
        assert list(tmp_path.iterdir()) == [config]

    def test_store_selection(self, tmp_path: Path) -> None:
        config = tmp_path / "chronos.toml"
        config.write_text('[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///app.sqlite"')
        loaded = load_workspace_config(config)
        assert loaded.store("app").kind == "sqlite"
        with pytest.raises(WorkspaceConfigError, match="Unknown store"):
            loaded.store("missing")
        with pytest.raises(WorkspaceConfigError, match="not a Qdrant store"):
            loaded.store("app", qdrant=True)

    @pytest.mark.parametrize("failure_stage", ["none", "open", "workspace", "operation", "close"])
    def test_resource_ownership(self, failure_stage: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        import chronos_core.workspace as workspace_api

        config_path = tmp_path / "chronos.toml"
        # Declaration order must not affect shared-context construction or cleanup.
        config_path.write_text('''
[workspace]
shared_metadata_url="sqlite:///app.sqlite"
[stores.vectors]
kind="qdrant"
metadata_url="sqlite:///app.sqlite"
path="vectors"
[stores.app]
kind="sqlite"
database_url="sqlite:///app.sqlite"
''')
        loaded = load_workspace_config(config_path)
        database = MagicMock()
        vectors = MagicMock()
        factory = MagicMock()
        factory.local.return_value = vectors
        workspace_factory = MagicMock()
        monkeypatch.setattr(ChronosBranchContext, "connect", MagicMock(return_value=database))
        monkeypatch.setattr(_qdrant, "_store_class", lambda: factory)
        monkeypatch.setattr(workspace_api, "ChronosWorkspaceContext", workspace_factory)
        events = []
        database.close.side_effect = lambda: events.append("database")

        def close_vectors() -> None:
            events.append("vectors")
            if failure_stage in {"operation", "close"}:
                raise RuntimeError("close failed")

        vectors.close.side_effect = close_vectors
        if failure_stage == "open":
            factory.local.side_effect = RuntimeError("open failed")
        if failure_stage == "workspace":
            workspace_factory.side_effect = RuntimeError("workspace failed")
        if failure_stage == "none":
            with open_workspace(loaded) as workspace:
                assert workspace is workspace_factory.return_value
        else:
            expected = "operation failed" if failure_stage == "operation" else f"{failure_stage} failed"
            with pytest.raises(RuntimeError, match=expected):
                with open_workspace(loaded):
                    if failure_stage == "operation":
                        raise RuntimeError("operation failed")
        assert events == (["database"] if failure_stage == "open" else ["vectors", "database"])
        if failure_stage != "open":
            factory.local.assert_called_once_with(
                loaded.shared_metadata_url, path=tmp_path / "vectors", collection_prefix="chronos_", context=database,
            )
            workspace_factory.assert_called_once_with(
                stores={"vectors": vectors, "app": database}, shared_metadata_url=loaded.shared_metadata_url,
            )
            workspace_factory.return_value.close.assert_not_called()

    def test_split_sql_and_remote_qdrant(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        import chronos_core.workspace as workspace_api

        config_file = tmp_path / "chronos.toml"
        config_file.write_text('''
[workspace]
shared_metadata_url="postgresql://localhost/metadata"
[stores.analytics]
kind="duckdb"
database_url="duckdb:///analytics.duckdb"
metadata_url="postgresql://localhost/metadata"
[stores.vectors]
kind="qdrant"
metadata_url="postgresql://localhost/metadata"
url="http://localhost:6333"
api_key="test-key"
collection_prefix="project_"
timeout=3
''')
        config = load_workspace_config(config_file)
        split = MagicMock()
        monkeypatch.setattr(ChronosBranchContext, "connect_split", split)
        factory = MagicMock()
        monkeypatch.setattr(_qdrant, "_store_class", lambda: factory)
        workspace_factory = MagicMock()
        monkeypatch.setattr(workspace_api, "ChronosWorkspaceContext", workspace_factory)
        with open_workspace(config):
            pass
        split.assert_called_once_with(f"duckdb:///{tmp_path / 'analytics.duckdb'}", "postgresql://localhost/metadata")
        factory.remote.assert_called_once_with(
            "postgresql://localhost/metadata", url="http://localhost:6333", api_key="test-key",
            collection_prefix="project_", timeout=3, context=None,
        )
        split.return_value.close.assert_called_once_with()
        factory.remote.return_value.close.assert_called_once_with()

    def test_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(WorkspaceConfigError, match="Cannot read workspace configuration"):
            load_workspace_config(tmp_path / "missing.toml")


class TestWorkspaceCommands:
    @pytest.mark.parametrize(("args", "method", "expected_args", "expected_kwargs"), [
        (["branch", "list"], "list_branches", (), {}),
        (["branch", "create", "trial", "--from", "parent"], "create_branch", ("trial",), {"from_branch": "parent"}),
        (["branch", "create-from-checkpoint", "trial", "saved"], "create_branch_from_checkpoint", ("trial", "saved"), {}),
        (["branch", "delete", "trial"], "delete_branch", ("trial",), {}),
        (["checkpoint", "create", "saved", "--branch", "trial"], "create_checkpoint", ("saved",), {"branch": "trial"}),
        (["diff", "main", "trial"], "diff", ("main", "trial"), {}),
        (["merge", "preview", "trial", "main"], "merge_preview", ("trial", "main"), {"policy": "abort_on_conflict"}),
        (["merge", "apply", "trial", "main", "--policy", "snapshot_isolation"], "merge_apply", ("trial", "main"), {"policy": "snapshot_isolation"}),
        (["merge", "atomic-preview", "trial", "main"], "merge_atomic_preview", ("trial", "main"), {"policy": "abort_on_conflict"}),
        (["merge", "atomic-apply", "trial", "main", "--operation-id", "merge-1", "--preview-token", "reviewed"],
         "merge_atomic", ("trial", "main"), {"policy": "abort_on_conflict", "operation_id": "merge-1", "preview_token": "reviewed"}),
    ])
    def test_workspace_forwarding(self, workspace_connection: MagicMock, args, method, expected_args, expected_kwargs) -> None:
        result = runner.invoke(app, ["workspace", *args])
        assert result.exit_code == 0, result.output
        getattr(workspace_connection, method).assert_called_once_with(*expected_args, **expected_kwargs)
        workspace_connection.close.assert_called_once_with()
        if method == "list_branches":
            assert json.loads(result.stdout) == ["main"]
        else:
            assert result.stdout == ""

    def test_named_relational_commands(self, workspace_connection: MagicMock) -> None:
        workspace = workspace_connection
        result = runner.invoke(app, ["workspace", "table", "create", "--store", "app", "CREATE TABLE items (id INTEGER)"])
        assert result.exit_code == 0, result.output
        workspace.stores["app"].db.execute.assert_called_once_with("CREATE TABLE items (id INTEGER)")
        workspace.stores["app"].db.commit.assert_called_once_with()
        result = runner.invoke(app, ["workspace", "table", "register", "items", "--store", "app", "--primary-key", "id", "--primary-key", "tenant"])
        assert result.exit_code == 0, result.output
        workspace.stores["app"].register_table.assert_called_once_with("items", ["id", "tenant"])
        result = runner.invoke(app, ["workspace", "query", "SELECT :id", "--store", "app", "--branch", "trial", "--params", '{"id":1}'])
        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout) == [{"id": 1}]
        session = workspace.checkout.return_value.stores["app"]
        session.query.assert_called_once_with("SELECT :id", {"id": 1})
        workspace.checkout.assert_called_once_with("trial")
        for checked_out in workspace.checkout.return_value.stores.values():
            checked_out.close.assert_called_once_with()
        result = runner.invoke(app, ["workspace", "execute", "UPDATE items SET id = :id", "--store", "app", "--params", '{"id":2}'])
        assert result.exit_code == 0, result.output
        session.execute.assert_called_once_with("UPDATE items SET id = :id", {"id": 2})

    def test_sql_input_modes(self, workspace_connection: MagicMock, tmp_path: Path) -> None:
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT :id")
        session = workspace_connection.checkout.return_value.stores["app"]
        for args, input_text in [(["--file", str(sql_file)], None), (["--stdin"], "SELECT :id"), ([], "SELECT :id")]:
            result = runner.invoke(app, ["workspace", "query", "--store", "app", "--params", '{"id":3}', *args], input=input_text)
            assert result.exit_code == 0, result.output
            assert session.query.call_args.args == ("SELECT :id", {"id": 3})

    @pytest.mark.parametrize("args", [
        ["query", "SELECT 1", "--store", "missing"],
        ["query", "SELECT 1", "--store", "app", "--params", "[]"],
        ["query", "SELECT 1", "--store", "app", "--stdin"],
        ["query", "--store", "app", "--stdin"],
        ["table", "create", "--store", "app", "DELETE FROM items"],
        ["qdrant", "--store", "app", "collection", "list"],
        ["qdrant", "collection", "list"],
        ["merge", "atomic-apply", "trial", "main"],
    ])
    def test_invalid_arguments_do_not_open_stores(self, workspace_connection: MagicMock, args) -> None:
        result = runner.invoke(app, ["workspace", *args], input="")
        assert result.exit_code == 2, result.output
        _workspace.open_workspace.assert_not_called()

    def test_help_and_configuration_precedence(self, workspace_connection: MagicMock, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        for args in ([], ["branch"], ["query"], ["qdrant"], ["qdrant", "point"], ["qdrant", "point", "upsert"], ["merge", "atomic-apply"]):
            result = runner.invoke(app, ["workspace", "--config", "missing.toml", *args, "--help"])
            assert result.exit_code == 0, result.output
        _workspace.open_workspace.assert_not_called()
        monkeypatch.setenv("CHRONOS_WORKSPACE_CONFIG", str(tmp_path / "missing.toml"))
        result = runner.invoke(app, ["workspace", "branch", "list"])
        assert result.exit_code == 2
        _workspace.open_workspace.assert_not_called()
        result = runner.invoke(app, ["workspace", "--config", str(tmp_path / "chronos.toml"), "branch", "list"])
        assert result.exit_code == 0, result.output
        assert _workspace.open_workspace.call_args.args[0].config_path == tmp_path / "chronos.toml"

    def test_api_error_and_session_cleanup(self, workspace_connection: MagicMock) -> None:
        workspace = workspace_connection
        session = workspace.checkout.return_value.stores["app"]
        session.query.side_effect = BranchingError("query failed")
        session.close.side_effect = RuntimeError("close failed")
        result = runner.invoke(app, ["workspace", "query", "SELECT 1", "--store", "app"])
        assert result.exit_code == 1
        assert result.stdout == ""
        assert "query failed" in result.stderr
        assert "close failed" not in result.stderr
        session.close.assert_called_once_with()
        workspace.checkout.return_value.stores["other"].close.assert_called_once_with()
        workspace.close.assert_called_once_with()

    def test_api_result_field_names_are_preserved(self, workspace_connection: MagicMock) -> None:
        from chronos_core.workspace import AtomicMergeResult, WorkspaceBranchToken
        token = WorkspaceBranchToken("main", 1)
        result = AtomicMergeResult("merge-1", "trial", "main", "committed", token, token, token, 2, 0, {"app": 1, "other": 1})
        workspace_connection.merge_atomic.return_value = result
        invoked = runner.invoke(app, ["workspace", "merge", "atomic-apply", "trial", "main", "--operation-id", "merge-1"])
        assert invoked.exit_code == 0, invoked.output
        assert json.loads(invoked.stdout) == {
            "operation_id": "merge-1", "source": "trial", "target": "main", "status": "committed",
            "source_token": {"branch_id": "main", "current_segment_id": 1},
            "old_target_token": {"branch_id": "main", "current_segment_id": 1},
            "new_target_token": {"branch_id": "main", "current_segment_id": 1},
            "selected": 2, "skipped": 0, "stores": {"app": 1, "other": 1},
        }


class TestWorkspaceQdrantCommands:
    @pytest.fixture
    def workspace(self, workspace_connection: MagicMock, tmp_path: Path) -> MagicMock:
        pytest.importorskip("qdrant_client")
        (tmp_path / "chronos.toml").write_text('''
[stores.app]
kind="sqlite"
database_url="sqlite:///app.sqlite"
[stores.vectors]
kind="qdrant"
metadata_url="sqlite:///vectors.sqlite"
path="vectors"
''')
        vector_store = MagicMock()
        for method in ("register_collection", "list_collections", "collection_info"):
            getattr(vector_store, method).return_value = None
        vector_session = MagicMock()
        for method in ("upsert", "get", "list_points", "delete", "search"):
            getattr(vector_session, method).return_value = None
        workspace_connection.stores["vectors"] = vector_store
        workspace_connection.checkout.return_value.stores["vectors"] = vector_session
        return workspace_connection

    @pytest.mark.parametrize(("args", "method", "positional", "keywords", "session"), [
        (["collection", "register", "docs", "--dimensions", "3", "--distance", "dot"], "register_collection", ("docs", 3), {"distance": "dot"}, False),
        (["collection", "list"], "list_collections", (), {}, False),
        (["collection", "info", "docs"], "collection_info", ("docs",), {}, False),
        (["point", "upsert", "docs", "one", '{"vector":[1,0,0],"payload":{"value":"one"}}', "--revision", "v1"],
         "upsert", ("docs", "one", [1, 0, 0], {"value": "one"}), {"revision": "v1"}, True),
        (["point", "get", "docs", "one"], "get", ("docs", "one"), {}, True),
        (["point", "list", "docs"], "list_points", ("docs",), {}, True),
        (["point", "delete", "docs", "one"], "delete", ("docs", "one"), {}, True),
        (["search", "docs", "[1,0,0]", "--limit", "3", "--score-threshold", "0.5"], "search", ("docs", [1, 0, 0]), {"limit": 3, "score_threshold": 0.5}, True),
    ])
    def test_forwarding(self, workspace: MagicMock, args, method, positional, keywords, session: bool) -> None:
        result = runner.invoke(app, ["workspace", "qdrant", "--store", "vectors", *args, *(["--branch", "trial"] if session else [])])
        assert result.exit_code == 0, result.output
        assert result.stdout == ""
        target = workspace.checkout.return_value.stores["vectors"] if session else workspace.stores["vectors"]
        getattr(target, method).assert_called_once_with(*positional, **keywords)
        if session:
            workspace.checkout.assert_called_once_with("trial")
        workspace.close.assert_called_once_with()

    @pytest.mark.parametrize("data", ['{"vector":[true]}', '{"vector":[NaN]}', '{"vector":[1],"payload":[]}'])
    def test_invalid_point_does_not_connect(self, workspace: MagicMock, data: str) -> None:
        result = runner.invoke(app, ["workspace", "qdrant", "--store", "vectors", "point", "upsert", "docs", "one", data])
        assert result.exit_code == 2, result.output
        _workspace.open_workspace.assert_not_called()

    def test_transport_error_redacts_credentials(self, workspace: MagicMock, tmp_path: Path) -> None:
        import httpx
        (tmp_path / "chronos.toml").write_text('''
[stores.app]
kind="postgresql"
database_url="postgresql://user:sql-secret@localhost/app"
[stores.vectors]
kind="qdrant"
metadata_url="sqlite:///vectors.sqlite"
url="http://localhost:6333"
api_key="vector-secret"
''')
        workspace.checkout.return_value.stores["vectors"].search.side_effect = httpx.ConnectError(
            "Failed using vector-secret and postgresql://user:sql-secret@localhost/app"
        )
        result = runner.invoke(app, ["workspace", "qdrant", "--store", "vectors", "search", "docs", "[1,0,0]"])
        assert result.exit_code == 1
        assert result.stdout == ""
        assert "secret" not in result.stderr
        assert "<redacted>" in result.stderr
        workspace.close.assert_called_once_with()

    def test_json_file_and_stdin(self, workspace: MagicMock, tmp_path: Path) -> None:
        data = tmp_path / "point.json"
        data.write_text('{"vector":[1,0,0],"payload":{"source":"file"}}')
        result = runner.invoke(app, ["workspace", "qdrant", "--store", "vectors", "point", "upsert", "docs", "one", "--file", str(data)])
        assert result.exit_code == 0, result.output
        session = workspace.checkout.return_value.stores["vectors"]
        session.upsert.assert_called_once_with("docs", "one", [1, 0, 0], {"source": "file"}, revision=None)
        for args in (["--stdin"], []):
            result = runner.invoke(app, ["workspace", "qdrant", "--store", "vectors", "search", "docs", *args], input="[0,1,0]")
            assert result.exit_code == 0, result.output
            assert session.search.call_args.args == ("docs", [0, 1, 0])
        data.write_text("[1,0,0]")
        result = runner.invoke(app, ["workspace", "qdrant", "--store", "vectors", "search", "docs", "--file", str(data)])
        assert result.exit_code == 0, result.output
        assert session.search.call_args.args == ("docs", [1, 0, 0])
