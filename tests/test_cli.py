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
from rich.text import Text
from typer.testing import CliRunner

from chronos_core.branching import BranchingError, ChronosBranchContext
from chronos_core.cli import _workspace
from chronos_core.cli._terminal import print_result
from chronos_core.cli._config import (
    DatabaseConfig, QdrantConfig, WorkspaceConfigError, load_workspace_config, open_workspace,
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
    @pytest.mark.parametrize("source", [
        '[stores.vectors]\nkind="qdrant"\nmetadata_url="sqlite:///meta.sqlite"\nurl="http://localhost:6333"\ntimeout=' + '9' * 400,
        '[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///~chronos_no_such_user_987654/app.sqlite"',
        '[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///shared.sqlite"\n[stores.vectors]\nkind="qdrant"\nmetadata_url="sqlite:///shared%2Esqlite"\npath="vectors"',
        '[workspace]\nshared_metadata_url="sqlite:///shared.sqlite"\n[stores.first]\nkind="sqlite"\ndatabase_url="sqlite:///shared.sqlite"\n[stores.second]\nkind="sqlite"\ndatabase_url="sqlite:///shared.sqlite"',
        '[workspace]\nshared_metadata_url="sqlite:///shared.sqlite"\n[stores.first]\nkind="duckdb"\ndatabase_url="duckdb:///first.duckdb"\nmetadata_url="sqlite:///shared.sqlite"\n[stores.second]\nkind="duckdb"\ndatabase_url="duckdb:///second.duckdb"\nmetadata_url="sqlite:///shared.sqlite"',
    ])
    def test_reviewed_config_errors_fail_before_connecting(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str) -> None:
        config = tmp_path / "chronos.toml"
        config.write_text(source)
        opened = MagicMock()
        monkeypatch.setattr(_workspace, "open_workspace", opened)
        result = runner.invoke(app, ["--config", str(config), "branch", "list"])
        assert result.exit_code == 2, result.output
        assert "Invalid value" in result.stderr
        opened.assert_not_called()
        assert list(tmp_path.iterdir()) == [config]

    def test_encoded_filename_preserves_api_url(self, tmp_path: Path) -> None:
        config = tmp_path / "chronos.toml"
        config.write_text('[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///literal%252Ename.sqlite"')
        loaded = load_workspace_config(config)
        assert loaded.stores["app"].connection.database_url == f"sqlite:///{tmp_path / 'literal%252Ename.sqlite'}"

    def test_invalid_config_path_is_a_usage_error(self) -> None:
        result = runner.invoke(app, ["--config", "~chronos_no_such_user_987654/chronos.toml", "branch", "list"])
        assert result.exit_code == 2, result.output
        assert "valid local path" in result.stderr

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
        assert loaded.stores["vectors"].connection == QdrantConfig(
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
        from chronos_core.workspace.qdrant import ChronosQdrantStore

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
        monkeypatch.setattr(ChronosQdrantStore, "local", factory.local)
        monkeypatch.setattr(ChronosQdrantStore, "remote", factory.remote)
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
        from chronos_core.workspace.qdrant import ChronosQdrantStore

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
        monkeypatch.setattr(ChronosQdrantStore, "local", factory.local)
        monkeypatch.setattr(ChronosQdrantStore, "remote", factory.remote)
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
    @pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
    def test_nonfinite_query_result_fails_clearly(self, workspace_connection: MagicMock, value: float) -> None:
        session = workspace_connection.checkout.return_value.stores["app"]
        session.query.return_value = [{"value": value}]
        result = runner.invoke(app, ["query", "--store", "app", "SELECT 1"])
        assert result.exit_code == 1, result.output
        assert result.stdout == ""
        assert "JSON compliant" in result.stderr
        session.close.assert_called_once_with()
        workspace_connection.close.assert_called_once_with()

    @pytest.mark.parametrize("credentials", [
        'review_user:review_secret%zz@127.0.0.1/db',
        'review_user:review%40secret%zz@127.0.0.1/db',
        'review_user@127.0.0.1/db?password=review%40secret%zz',
    ])
    def test_postgres_parser_error_hides_password(self, tmp_path: Path, credentials: str) -> None:
        config = tmp_path / "chronos.toml"
        config.write_text(f'[stores.app]\nkind="postgresql"\ndatabase_url="postgresql://{credentials}"')
        # Invalid percent escapes fail in the real driver's parser, before any network call.
        result = runner.invoke(app, ["--config", str(config), "branch", "list"])
        assert result.exit_code == 1, result.output
        assert result.stdout == ""
        assert "invalid percent-encoded token" in result.stderr
        assert "secret" not in result.stderr
        assert "<redacted>" in result.stderr

    def test_installed_entrypoints(self) -> None:
        executable = Path(sys.executable).parent / "chronos"
        for args in ([str(executable), "--help"], [sys.executable, "-m", "chronos_core.cli", "--help"]):
            environment = {key: value for key, value in os.environ.items() if not key.startswith("CHRONOS_")}
            result = subprocess.run(args, capture_output=True, text=True, env=environment, timeout=30)
            assert result.returncode == 0, result.stderr
            help_text = Text.from_ansi(result.stdout).plain
            assert "--config" in help_text
            assert "--database-url" not in help_text

    def test_help_and_sql_without_qdrant_dependency(self, tmp_path: Path) -> None:
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
        environment = {key: value for key, value in os.environ.items() if not key.startswith("CHRONOS_")}
        for args in (["--help"], ["qdrant", "--help"], ["qdrant", "point", "upsert", "--help"]):
            result = subprocess.run([sys.executable, "-c", textwrap.dedent(code), *args], capture_output=True, text=True, env=environment)
            assert result.returncode == 0, result.stderr
        config = tmp_path / "chronos.toml"
        config.write_text('[stores.vectors]\nkind="qdrant"\nmetadata_url="sqlite:///meta.sqlite"\npath="vectors"\n')
        result = subprocess.run(
            [sys.executable, "-c", textwrap.dedent(code), "--config", str(config), "qdrant", "--store", "vectors", "collection", "list"],
            capture_output=True, text=True, env=environment,
        )
        assert result.returncode == 1
        assert "chronos-core[qdrant]" in result.stderr
        assert list(tmp_path.iterdir()) == [config]
        config.write_text('[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///app.sqlite"\n')
        result = subprocess.run(
            [sys.executable, "-c", textwrap.dedent(code), "--config", str(config), "branch", "list"],
            capture_output=True, text=True, env=environment,
        )
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == ["main"]

    def test_open_failure_reports_credentials_safely(self, workspace_connection: MagicMock) -> None:
        _workspace.open_workspace.side_effect = RuntimeError("Backend unavailable at postgresql://user:secret@host/meta")
        result = runner.invoke(app, ["branch", "list"])
        assert result.exit_code == 1
        assert result.stdout == ""
        assert "secret" not in result.stderr
        assert "Backend unavailable" in result.stderr
        workspace_connection.close.assert_not_called()

    def test_unsupported_result_closes_workspace(self, workspace_connection: MagicMock) -> None:
        workspace_connection.diff.return_value = object()
        result = runner.invoke(app, ["diff", "main", "trial"])
        assert result.exit_code == 1
        assert result.stdout == ""
        assert "Unsupported JSON value type" in result.stderr
        workspace_connection.close.assert_called_once_with()

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
        result = runner.invoke(app, [*args])
        assert result.exit_code == 0, result.output
        getattr(workspace_connection, method).assert_called_once_with(*expected_args, **expected_kwargs)
        workspace_connection.close.assert_called_once_with()
        if method == "list_branches":
            assert json.loads(result.stdout) == ["main"]
        else:
            assert result.stdout == ""

    def test_named_relational_commands(self, workspace_connection: MagicMock) -> None:
        workspace = workspace_connection
        result = runner.invoke(app, ["table", "create", "--store", "app", "CREATE TABLE items (id INTEGER)"])
        assert result.exit_code == 0, result.output
        workspace.stores["app"].db.execute.assert_called_once_with("CREATE TABLE items (id INTEGER)")
        workspace.stores["app"].db.commit.assert_called_once_with()
        result = runner.invoke(app, ["table", "register", "items", "--store", "app", "--primary-key", "id", "--primary-key", "tenant"])
        assert result.exit_code == 0, result.output
        workspace.stores["app"].register_table.assert_called_once_with("items", ["id", "tenant"])
        result = runner.invoke(app, ["query", "SELECT :id", "--store", "app", "--branch", "trial", "--params", '{"id":1}'])
        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout) == [{"id": 1}]
        session = workspace.checkout.return_value.stores["app"]
        session.query.assert_called_once_with("SELECT :id", {"id": 1})
        workspace.checkout.assert_called_once_with("trial")
        for checked_out in workspace.checkout.return_value.stores.values():
            checked_out.close.assert_called_once_with()
        result = runner.invoke(app, ["execute", "UPDATE items SET id = :id", "--store", "app", "--params", '{"id":2}'])
        assert result.exit_code == 0, result.output
        session.execute.assert_called_once_with("UPDATE items SET id = :id", {"id": 2})

    def test_sql_input_modes(self, workspace_connection: MagicMock, tmp_path: Path) -> None:
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT :id")
        session = workspace_connection.checkout.return_value.stores["app"]
        for args, input_text in [(["--file", str(sql_file)], None), (["--stdin"], "SELECT :id"), ([], "SELECT :id")]:
            result = runner.invoke(app, ["query", "--store", "app", "--params", '{"id":3}', *args], input=input_text)
            assert result.exit_code == 0, result.output
            assert session.query.call_args.args == ("SELECT :id", {"id": 3})

    @pytest.mark.parametrize("args", [
        ["query", "SELECT 1", "--store", "missing"],
        ["query", "SELECT 1", "--store", "app", "--params", "[]"],
        ["execute", "SELECT 1", "--store", "app", "--params", "broken"],
        ["query", "SELECT 1", "--store", "app", "--file", "missing.sql"],
        ["query", "--store", "app", "--file", "missing.sql"],
        ["query", "SELECT 1", "--store", "app", "--stdin"],
        ["query", "--store", "app", "--stdin"],
        ["table", "create", "--store", "app", "DELETE FROM items"],
        ["table", "create", "--store", "app", "CREATE INDEX idx ON items(id)"],
        ["table", "create", "--store", "app", "CREATE TABLE items (id INTEGER); DROP TABLE items"],
        ["table", "create", "--store", "app", "not sql"],
        ["table", "register", "--store", "app", "items"],
        ["qdrant", "--store", "app", "collection", "list"],
        ["qdrant", "collection", "list"],
        ["merge", "atomic-apply", "trial", "main"],
        ["merge", "apply", "trial", "main", "--policy", "unknown"],
    ])
    def test_invalid_arguments_do_not_open_stores(self, workspace_connection: MagicMock, args) -> None:
        result = runner.invoke(app, [*args], input="")
        assert result.exit_code == 2, result.output
        _workspace.open_workspace.assert_not_called()

    def test_help_and_configuration_precedence(self, workspace_connection: MagicMock, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        for args in ([], ["branch"], ["query"], ["qdrant"], ["qdrant", "point"], ["qdrant", "point", "upsert"], ["merge", "atomic-apply"]):
            result = runner.invoke(app, ["--config", "missing.toml", *args, "--help"])
            assert result.exit_code == 0, result.output
        _workspace.open_workspace.assert_not_called()
        monkeypatch.setenv("CHRONOS_WORKSPACE_CONFIG", str(tmp_path / "missing.toml"))
        result = runner.invoke(app, ["branch", "list"])
        assert result.exit_code == 2
        _workspace.open_workspace.assert_not_called()
        result = runner.invoke(app, ["--config", str(tmp_path / "chronos.toml"), "branch", "list"])
        assert result.exit_code == 0, result.output
        assert _workspace.open_workspace.call_args.args[0].config_path == tmp_path / "chronos.toml"

    def test_api_error_and_session_cleanup(self, workspace_connection: MagicMock) -> None:
        workspace = workspace_connection
        session = workspace.checkout.return_value.stores["app"]
        session.query.side_effect = BranchingError("query failed")
        session.close.side_effect = RuntimeError("close failed")
        result = runner.invoke(app, ["query", "SELECT 1", "--store", "app"])
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
        invoked = runner.invoke(app, ["merge", "atomic-apply", "trial", "main", "--operation-id", "merge-1"])
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
        result = runner.invoke(app, ["qdrant", "--store", "vectors", *args, *(["--branch", "trial"] if session else [])])
        assert result.exit_code == 0, result.output
        assert result.stdout == ""
        target = workspace.checkout.return_value.stores["vectors"] if session else workspace.stores["vectors"]
        getattr(target, method).assert_called_once_with(*positional, **keywords)
        if session:
            workspace.checkout.assert_called_once_with("trial")
        workspace.close.assert_called_once_with()

    @pytest.mark.parametrize("data", ['{"vector":[true]}', '{"vector":[NaN]}', '{"vector":[1],"payload":[]}'])
    def test_invalid_point_does_not_connect(self, workspace: MagicMock, data: str) -> None:
        result = runner.invoke(app, ["qdrant", "--store", "vectors", "point", "upsert", "docs", "one", data])
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
        result = runner.invoke(app, ["qdrant", "--store", "vectors", "search", "docs", "[1,0,0]"])
        assert result.exit_code == 1
        assert result.stdout == ""
        assert "secret" not in result.stderr
        assert "<redacted>" in result.stderr
        workspace.close.assert_called_once_with()

    def test_json_file_and_stdin(self, workspace: MagicMock, tmp_path: Path) -> None:
        data = tmp_path / "point.json"
        data.write_text('{"vector":[1,0,0],"payload":{"source":"file"}}')
        result = runner.invoke(app, ["qdrant", "--store", "vectors", "point", "upsert", "docs", "one", "--file", str(data)])
        assert result.exit_code == 0, result.output
        session = workspace.checkout.return_value.stores["vectors"]
        session.upsert.assert_called_once_with("docs", "one", [1, 0, 0], {"source": "file"}, revision=None)
        for args in (["--stdin"], []):
            result = runner.invoke(app, ["qdrant", "--store", "vectors", "search", "docs", *args], input="[0,1,0]")
            assert result.exit_code == 0, result.output
            assert session.search.call_args.args == ("docs", [0, 1, 0])
        data.write_text("[1,0,0]")
        result = runner.invoke(app, ["qdrant", "--store", "vectors", "search", "docs", "--file", str(data)])
        assert result.exit_code == 0, result.output
        assert session.search.call_args.args == ("docs", [1, 0, 0])


class TestWorkspaceFilesystemCommands:
    def test_config_and_resource_ownership(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        from chronos_core import workspace as api

        config_file = tmp_path / 'chronos.toml'
        config_file.write_text('[filesystem]\nroot="base"\nstate_dir="state"\n')
        config = load_workspace_config(config_file)
        assert config.stores == {}
        assert config.filesystem.root == tmp_path / 'base'
        assert config.filesystem.state_dir == tmp_path / 'state'
        factory = MagicMock()
        monkeypatch.setattr(api, 'ChronosFilesystemStore', factory)
        with open_workspace(config) as workspace:
            assert workspace.filesystem is factory.return_value
        factory.assert_called_once_with(tmp_path / 'base', state_dir=tmp_path / 'state')
        factory.return_value.close.assert_called_once_with()
        factory.return_value.cleanup.assert_not_called()

    @pytest.mark.parametrize('extra', [
        '[workspace]\nshared_metadata_url="sqlite:///meta.sqlite"\n',
        '[stores.filesystem]\nkind="sqlite"\ndatabase_url="sqlite:///app.sqlite"\n',
    ])
    def test_unsupported_config(self, tmp_path: Path, extra: str) -> None:
        config_file = tmp_path / 'chronos.toml'
        config_file.write_text('[filesystem]\nroot="base"\nstate_dir="state"\n' + extra)
        with pytest.raises(WorkspaceConfigError):
            load_workspace_config(config_file)

    def test_state_directory_is_required(self, tmp_path: Path) -> None:
        config_file = tmp_path / 'chronos.toml'
        config_file.write_text('[filesystem]\nroot="base"\n')
        with pytest.raises(WorkspaceConfigError, match='state_dir'):
            load_workspace_config(config_file)

    @pytest.mark.parametrize('as_json', [False, True])
    def test_run_forwards_arguments_and_output(self, workspace_connection: MagicMock, as_json: bool) -> None:
        workspace = workspace_connection
        command = ['example', '--child-option', 'two words']
        filesystem = workspace.checkout.return_value.fs
        filesystem.run.return_value = subprocess.CompletedProcess(command, 7, '\x1b[32moutput\x1b[0m\n', '\x1b[31merror\x1b[0m\n')
        args = ['fs', 'run', '--branch', 'feature', '--cwd', 'nested', '--env', '{"MODE":"test"}', '--timeout', '2']
        if as_json:
            args.append('--json')
        result = runner.invoke(app, [*args, '--', *command])
        assert result.exit_code == 7, result.output
        filesystem.run.assert_called_once_with(command, cwd='nested', env={'MODE': 'test'}, timeout=2.0)
        workspace.checkout.assert_called_once_with('feature')
        if as_json:
            assert json.loads(result.stdout) == dict(args=command, returncode=7, stdout='\x1b[32moutput\x1b[0m\n', stderr='\x1b[31merror\x1b[0m\n')
            assert result.stderr == ''
        else:
            assert result.stdout == '\x1b[32moutput\x1b[0m\n'
            assert result.stderr == '\x1b[31merror\x1b[0m\n'
        for session in workspace.checkout.return_value.stores.values():
            session.close.assert_called_once_with()
        workspace.close.assert_called_once_with()

    @pytest.mark.parametrize('args', [[], ['--env', '[]', '--', 'echo'], ['--env', '{"X":1}', '--', 'echo'],
                                      ['--timeout', 'nan', '--', 'echo']])
    def test_invalid_input_before_connection(self, workspace_connection: MagicMock, args: list[str]) -> None:
        result = runner.invoke(app, ['fs', 'run', *args])
        assert result.exit_code == 2
        _workspace.open_workspace.assert_not_called()

    @pytest.mark.parametrize('error', [FileNotFoundError('missing command'), subprocess.TimeoutExpired(['secret'], 1)])
    def test_run_failure_closes_resources(self, workspace_connection: MagicMock, error: Exception) -> None:
        workspace_connection.checkout.return_value.fs.run.side_effect = error
        result = runner.invoke(app, ['fs', 'run', '--', 'secret'])
        assert result.exit_code == 1
        assert 'secret' not in result.stderr
        workspace_connection.close.assert_called_once_with()
        for session in workspace_connection.checkout.return_value.stores.values():
            session.close.assert_called_once_with()

    def test_no_filesystem(self, workspace_connection: MagicMock) -> None:
        workspace_connection.filesystem = None
        result = runner.invoke(app, ['fs', 'run', '--', 'echo'])
        assert result.exit_code == 2
        workspace_connection.checkout.assert_not_called()
        workspace_connection.close.assert_called_once_with()

    @pytest.mark.parametrize('operation', ['preview', 'apply', 'atomic-preview', 'atomic-apply'])
    def test_api_default_merge_policy(self, workspace_connection: MagicMock, operation: str) -> None:
        args = ['merge', operation, 'feature', 'main', '--policy', 'api-default']
        if operation == 'atomic-apply':
            args.extend(['--operation-id', 'operation'])
        result = runner.invoke(app, args)
        assert result.exit_code == 0, result.output
        name = {'preview': 'merge_preview', 'apply': 'merge_apply',
                'atomic-preview': 'merge_atomic_preview', 'atomic-apply': 'merge_atomic'}[operation]
        assert getattr(workspace_connection, name).call_args.kwargs['policy'] is None

    def test_signal_exit_and_api_defaults(self, workspace_connection: MagicMock) -> None:
        filesystem = workspace_connection.checkout.return_value.fs
        filesystem.run.return_value = subprocess.CompletedProcess(['command'], -15, '', '')
        result = runner.invoke(app, ['fs', 'run', '--', 'command'])
        assert result.exit_code == 143
        filesystem.run.assert_called_once_with(['command'], cwd='.', env=None, timeout=None)
        workspace_connection.checkout.assert_called_once_with('main')

    def test_help_needs_no_config(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv('CHRONOS_WORKSPACE_CONFIG', raising=False)
        result = runner.invoke(app, ['fs', 'run', '--help'])
        assert result.exit_code == 0
        assert "--json" in Text.from_ansi(result.stdout).plain

    def test_filesystem_error_is_reported(self, workspace_connection: MagicMock) -> None:
        from chronos_core.workspace.filesystem import FilesystemStoreError

        workspace_connection.checkout.side_effect = FilesystemStoreError('failed to mount branch')
        result = runner.invoke(app, ['fs', 'run', '--', 'command'])
        assert result.exit_code == 1
        assert 'failed to mount branch' in result.stderr
        workspace_connection.close.assert_called_once_with()


def test_native_filesystem_commands(workspace_connection: MagicMock, tmp_path: Path) -> None:
    from chronos_core.workspace import ChronosFSStore

    workspace_connection.filesystem = MagicMock(spec=ChronosFSStore)
    session = workspace_connection.checkout.return_value.fs
    payload = b"\x00\xff\n"
    source = tmp_path / "payload"
    source.write_bytes(payload)
    session.write_file.return_value = 12
    result = runner.invoke(app, ["fs", "write", "/dir/file", "--branch", "draft", "--file", str(source), "--parents"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == 12
    workspace_connection.checkout.assert_called_with("draft")
    session.write_file.assert_called_once_with("/dir/file", payload, parents=True)

    session.read_file.return_value = payload
    result = runner.invoke(app, ["fs", "read", "/dir/file"])
    assert result.exit_code == 0, result.output
    assert result.stdout_bytes == payload
    session.read_file.assert_called_once_with("/dir/file")

    session.listdir.return_value = ["file"]
    result = runner.invoke(app, ["fs", "ls", "/dir"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == ["file"]
    session.listdir.assert_called_once_with("/dir")

    result = runner.invoke(app, ["fs", "run", "--", "true"])
    assert result.exit_code == 2
    assert "requires the OverlayFS backend" in result.stderr


@pytest.mark.parametrize("args", [[], ["--file", __file__, "--stdin"]])
def test_native_write_requires_one_input(workspace_connection: MagicMock, args: list[str]) -> None:
    result = runner.invoke(app, ["fs", "write", "/file", *args])
    assert result.exit_code == 2
    workspace_connection.checkout.assert_not_called()


def test_native_filesystem_requires_matching_atomic_metadata(tmp_path: Path) -> None:
    config = tmp_path / "chronos.toml"
    config.write_text(
        '[workspace]\nshared_metadata_url="sqlite:///shared.sqlite"\n'
        '[filesystem]\nkind="chronosfs"\ndatabase_url="sqlite:///files.sqlite"\n'
    )
    with pytest.raises(WorkspaceConfigError, match="Every store must use"):
        load_workspace_config(config)


@pytest.mark.parametrize("scope", ["", "merge_tables=[]\n"])
def test_native_atomic_split_sql_requires_explicit_scope(tmp_path: Path, scope: str) -> None:
    config = tmp_path / "chronos.toml"
    config.write_text(
        '[workspace]\nshared_metadata_url="sqlite:///app.sqlite"\n'
        '[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///app.sqlite"\n'
        + scope + '[filesystem]\nkind="chronosfs"\ndatabase_url="sqlite:///files.sqlite"\n'
        'metadata_url="sqlite:///app.sqlite"\n'
    )
    with pytest.raises(WorkspaceConfigError, match="require nonempty merge_tables"):
        load_workspace_config(config)


@pytest.mark.parametrize("scope", ["", "merge_tables=[]\n"])
def test_default_merge_scope_includes_all_tables(tmp_path: Path, scope: str) -> None:
    config = tmp_path / "chronos.toml"
    config.write_text('[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///app.sqlite"\n' + scope)
    assert load_workspace_config(config).stores["app"].merge_tables is None


@pytest.mark.parametrize("mount_error", [False, True])
def test_native_mount_delegates_and_closes(
    workspace_connection: MagicMock, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mount_error: bool,
) -> None:
    from chronos_core import workspace as api

    filesystem = MagicMock(spec=api.ChronosFSStore)
    workspace_connection.filesystem = filesystem
    mountpoint = tmp_path / "mount"
    mountpoint.mkdir()
    failure = api.ChronosFSMountError("mount failed") if mount_error else None
    mount = MagicMock(side_effect=failure)
    monkeypatch.setattr(api, "mount_chronosfs", mount)

    result = runner.invoke(app, [
        "fs", "mount", str(mountpoint), "--branch", "draft", "-o", "ro", "--option", "default_permissions",
    ])
    assert result.exit_code == (1 if mount_error else 0), result.output
    if mount_error:
        assert "mount failed" in result.stderr
    mount.assert_called_once_with(
        filesystem, mountpoint.resolve(), branch_id="draft", options={"ro", "default_permissions"},
    )
    workspace_connection.checkout.assert_not_called()
    workspace_connection.close.assert_called_once_with()
