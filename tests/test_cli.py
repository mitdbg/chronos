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

import pytest
import typer
from typer.testing import CliRunner

from chronos_core.branching import BranchingError, ChronosBranchContext
from chronos_core.cli import _qdrant
from chronos_core.cli._relational import DatabaseConfig, connect
from chronos_core.cli._terminal import print_result
from chronos_core.cli.main import app

runner = CliRunner()


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
